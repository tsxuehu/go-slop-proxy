package pool

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/edy/slop-proxy-go/internal/oauth"
	"github.com/edy/slop-proxy-go/internal/provider"
	"github.com/edy/slop-proxy-go/internal/store"
)

func testStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestRendezvousStable(t *testing.T) {
	a := rendezvousScore("session-1", 42)
	b := rendezvousScore("session-1", 42)
	if a != b {
		t.Fatal("rendezvous score must be stable")
	}
	if a == rendezvousScore("session-1", 43) {
		t.Fatal("different accounts must hash differently")
	}
}

func TestUsageBand(t *testing.T) {
	now := time.Now().Unix()
	u := &AccountUsage{Windows: []UsageWindow{
		{Name: "5h", Utilization: 0.95, ResetsAt: now + 3600},
	}}
	if got := u.Band(0.9, now); got != BandSpent {
		t.Fatalf("expected Spent, got %v", got)
	}
	u.Windows[0].Utilization = 0.1
	if got := u.Band(0.9, now); got != BandAmple {
		t.Fatalf("expected Ample, got %v", got)
	}
	u.Locked = true
	if got := u.Band(0.9, now); got != BandSpent {
		t.Fatalf("locked account must be Spent, got %v", got)
	}
}

type fakeBackend struct {
	failFirst int
	calls     int
	softLimit float64
}

func (f *fakeBackend) Provider() provider.Provider     { return provider.OpenAI }
func (f *fakeBackend) SoftLimit() float64              { return f.softLimit }
func (f *fakeBackend) Attempts() int                   { return 3 }
func (f *fakeBackend) RateLimitMax() int64             { return 60 }
func (f *fakeBackend) RateLimitBase() int64            { return 1 }
func (f *fakeBackend) RefreshOnce() bool               { return true }
func (f *fakeBackend) StickyWaitSecs() int64           { return 0 }
func (f *fakeBackend) SessionAffinity() bool           { return true }
func (f *fakeBackend) RetryBudget() time.Duration      { return 0 }
func (f *fakeBackend) UsageFrom(any) *AccountUsage     { return nil }
func (f *fakeBackend) RetryableBadRequest(string) bool { return false }
func (f *fakeBackend) Reason(s string) string          { return s }
func (f *fakeBackend) Send(_ context.Context, _ string, slot *Slot, _ Route, _ any) (any, error) {
	f.calls++
	if f.calls <= f.failFirst {
		return nil, &SendError{Kind: SendRateLimited, Body: "quota"}
	}
	return slot.ID, nil
}

func TestPoolFailover(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		acc := store.Account{Provider: "openai", ProviderAccountID: fmt.Sprintf("acct-%d", i), Label: "acct", Email: "a@b.c", AuthMode: "apikey", Status: "active", AccessToken: "tok"}
		if err := st.InsertAccount(ctx, &acc); err != nil {
			t.Fatal(err)
		}
	}
	slots := NewSlots(provider.OpenAI, st, oauth.NewCodexRefresher())
	if err := slots.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	backend := &fakeBackend{failFirst: 1, softLimit: 0.9}
	p := NewPool(slots, backend)
	served, err := p.Execute(ctx, Route{SessionKey: "s", User: "u"}, nil)
	if err != nil {
		t.Fatalf("expected failover to succeed: %v", err)
	}
	if served.AccountID == nil {
		t.Fatal("expected an account")
	}
	if backend.calls < 2 {
		t.Fatalf("expected at least 2 attempts, got %d", backend.calls)
	}
}

func TestCooldownPersists(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	acc := store.Account{Provider: "openai", ProviderAccountID: "acct-0", Label: "acct", Email: "a@b.c", AuthMode: "apikey", Status: "active", AccessToken: "tok"}
	if err := st.InsertAccount(ctx, &acc); err != nil {
		t.Fatal(err)
	}
	slots := NewSlots(provider.OpenAI, st, nil)
	if err := slots.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	slot := slots.List()[0]
	slots.Cool(ctx, slot, 60, "test")
	if !slots.IsCooling(slot) {
		t.Fatal("slot should be cooling")
	}
	accounts, err := st.ListAccounts(ctx, "openai")
	if err != nil {
		t.Fatal(err)
	}
	if accounts[0].Status != "cooldown" {
		t.Fatalf("cooldown not persisted: %s", accounts[0].Status)
	}
}

func TestRankedPrefersCapacityAndSession(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		acc := store.Account{Provider: "openai", ProviderAccountID: fmt.Sprintf("acct-%d", i), Label: "acct", Email: "a@b.c", AuthMode: "apikey", Status: "active", AccessToken: "tok"}
		if err := st.InsertAccount(ctx, &acc); err != nil {
			t.Fatal(err)
		}
	}
	slots := NewSlots(provider.OpenAI, st, nil)
	if err := slots.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	p := NewPool(slots, &fakeBackend{softLimit: 0.9})
	p.bindSession("session-A", 2)
	got := p.ranked(Route{SessionKey: "session-A", User: "u"})
	if len(got) != 3 {
		t.Fatalf("expected 3 candidates, got %d", len(got))
	}
	if got[0].ID != 2 {
		t.Fatalf("bound account should rank first, got %d", got[0].ID)
	}
}

func TestNoAccountsError(t *testing.T) {
	st := testStore(t)
	slots := NewSlots(provider.OpenAI, st, nil)
	p := NewPool(slots, &fakeBackend{})
	_, err := p.Execute(context.Background(), Route{SessionKey: "s"}, nil)
	var pe *PoolError
	if !errors.As(err, &pe) || pe.Kind != ErrNoAccounts {
		t.Fatalf("expected NoAccounts, got %v", err)
	}
}

type anonBackend struct {
	fakeBackend
	calls int
}

func (b *anonBackend) Anonymous() bool { return true }
func (b *anonBackend) SendAnonymous(_ context.Context, _ Route, _ any) (any, error) {
	b.calls++
	return "anonymous", nil
}

func TestAnonymousPoolWithoutAccounts(t *testing.T) {
	st := testStore(t)
	slots := NewSlots(provider.Zen, st, nil)
	backend := &anonBackend{}
	p := NewPool(slots, backend)
	served, err := p.Execute(context.Background(), Route{}, nil)
	if err != nil {
		t.Fatalf("anonymous pool should serve: %v", err)
	}
	if served.Response != "anonymous" || backend.calls != 1 {
		t.Fatalf("unexpected anonymous result: %v, calls=%d", served.Response, backend.calls)
	}
}

func TestAPIKeyModeCompatibility(t *testing.T) {
	if !IsAPIKey("api_key") || !IsAPIKey("apikey") || IsAPIKey("oauth") {
		t.Fatal("both spellings of API-key auth must be accepted")
	}
}

func TestSlotProviderAccountIDSurvivesSnapshot(t *testing.T) {
	account := store.Account{
		Provider: "openai", ProviderAccountID: "acct-1", Email: "user@example.com",
		Label: "label", AuthMode: "oauth", Status: "active",
	}
	slot := NewSlot(account)
	if slot.ProviderAccountID != "acct-1" || slot.Email != "user@example.com" {
		t.Fatalf("slot lost identity: %+v", slot)
	}
	snapshot := slot.Snapshot()
	if snapshot.ProviderAccountID != "acct-1" || snapshot.Email != "user@example.com" {
		t.Fatalf("snapshot lost identity: %+v", snapshot)
	}
}

// tieredFakeBackend opts into the optional Tiered capability so ranking
// exercises the mismatch key and service-tier filter.
type tieredFakeBackend struct{ fakeBackend }

func (f *tieredFakeBackend) Tiered() bool { return true }

func TestExplicitTier(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"", ""}, {"auto", ""}, {"default", ""},
		{"priority", "priority"}, {"ultrafast", "ultrafast"},
	} {
		if got := (Route{ServiceTier: tc.in}).ExplicitTier(); got != tc.want {
			t.Errorf("ExplicitTier(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestRankedMismatchPrefersTrusted(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		acc := store.Account{Provider: "openai", ProviderAccountID: fmt.Sprintf("trust-%d", i),
			Label: "l", Email: "e", AuthMode: "apikey", Status: "active", AccessToken: "tok", Trusted: i == 0}
		if err := st.InsertAccount(ctx, &acc); err != nil {
			t.Fatal(err)
		}
	}
	slots := NewSlots(provider.OpenAI, st, nil)
	if err := slots.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	p := NewPool(slots, &tieredFakeBackend{fakeBackend{softLimit: 0.9}})
	if got := p.ranked(Route{SessionKey: "s", PreferTrusted: true}); len(got) != 2 || !got[0].Trusted {
		t.Fatalf("trusted account should rank first when preferred: %+v", got)
	}
	if got := p.ranked(Route{SessionKey: "s", PreferTrusted: false}); len(got) != 2 || got[0].Trusted {
		t.Fatalf("untrusted account should rank first when not preferred: %+v", got)
	}
}

func TestRankedRendezvousLargestHashWins(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		acc := store.Account{Provider: "openai", ProviderAccountID: fmt.Sprintf("hash-%d", i),
			Label: "l", Email: "e", AuthMode: "apikey", Status: "active", AccessToken: "tok"}
		if err := st.InsertAccount(ctx, &acc); err != nil {
			t.Fatal(err)
		}
	}
	slots := NewSlots(provider.OpenAI, st, nil)
	if err := slots.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	// Non-tiered backend: mismatch disabled, leaving pure rendezvous order.
	p := NewPool(slots, &fakeBackend{softLimit: 0.9})
	got := p.ranked(Route{SessionKey: "stable-session"})
	if len(got) != 3 {
		t.Fatalf("want 3 accounts, got %d", len(got))
	}
	for i := 1; i < len(got); i++ {
		if rendezvousScore("stable-session", got[i-1].ID) < rendezvousScore("stable-session", got[i].ID) {
			t.Fatal("largest rendezvous hash must rank first (Reverse ordering)")
		}
	}
}

func TestServiceTierFilterAndEmptyPool(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		acc := store.Account{Provider: "openai", ProviderAccountID: fmt.Sprintf("tier-%d", i),
			Label: "l", Email: "e", AuthMode: "apikey", Status: "active", AccessToken: "tok"}
		if err := st.InsertAccount(ctx, &acc); err != nil {
			t.Fatal(err)
		}
	}
	slots := NewSlots(provider.OpenAI, st, nil)
	if err := slots.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	list := slots.List()
	slots.SetServiceTiers(list[0], map[string][]string{"gpt-5": {"priority"}})
	slots.SetServiceTiers(list[1], map[string][]string{"gpt-5": {"default"}})
	p := NewPool(slots, &tieredFakeBackend{fakeBackend{softLimit: 0.9}})

	got := p.ranked(Route{SessionKey: "s", Model: "gpt-5", ServiceTier: "priority"})
	if len(got) != 1 || got[0].ID != list[0].ID {
		t.Fatalf("only the priority-tier account should be eligible: %+v", got)
	}
	// "ultrafast" is only synthesized for gpt-5.6-sol; here no account has it.
	_, err := p.Execute(ctx, Route{SessionKey: "s", Model: "gpt-5", ServiceTier: "ultrafast"}, nil)
	var pe *PoolError
	if !errors.As(err, &pe) || pe.Kind != ErrRefused || !strings.Contains(pe.Message, "ultrafast") {
		t.Fatalf("expected ErrRefused naming the tier, got %v", err)
	}
	// The synthesized tier bypasses catalog metadata entirely.
	if !slots.ServesTier(list[1], "gpt-5.6-sol", "ultrafast") {
		t.Fatal("gpt-5.6-sol must always serve the synthesized ultrafast tier")
	}
}
