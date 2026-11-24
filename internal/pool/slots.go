package pool

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/edy/slop-proxy-go/internal/oauth"
	"github.com/edy/slop-proxy-go/internal/provider"
	"github.com/edy/slop-proxy-go/internal/store"
)

type Slots struct {
	provider  provider.Provider
	store     *store.Store
	mu        sync.RWMutex
	slots     []*Slot
	reloadMu  sync.Mutex
	refresher oauth.Refresher
}

func NewSlots(p provider.Provider, s *store.Store, refresher oauth.Refresher) *Slots {
	return &Slots{provider: p, store: s, refresher: refresher}
}

func (ss *Slots) Provider() provider.Provider { return ss.provider }

func (ss *Slots) Reload(ctx context.Context) error {
	ss.reloadMu.Lock()
	defer ss.reloadMu.Unlock()
	accounts, err := ss.store.ListAccounts(ctx, ss.provider.String())
	if err != nil {
		return err
	}
	byID := make(map[int64]*Slot)
	ss.mu.RLock()
	for _, slot := range ss.slots {
		byID[slot.ID] = slot
	}
	ss.mu.RUnlock()

	next := make([]*Slot, 0, len(accounts))
	for _, acc := range accounts {
		existing := byID[acc.ID]
		if existing == nil {
			next = append(next, NewSlot(acc))
			continue
		}
		// A re-login rotates the refresh token; rebuild the slot so the stale
		// grant cannot be used. Otherwise keep the in-memory cooldown intact.
		snapshot := existing.Snapshot()
		if snapshot.RefreshToken != acc.RefreshToken || snapshot.AccessToken != acc.AccessToken {
			next = append(next, NewSlot(acc))
			continue
		}
		existing.mu.Lock()
		if acc.Status == "disabled" {
			existing.state.Status = StatusDisabled
		} else if acc.Status == "active" && existing.state.Status == StatusDisabled {
			existing.state.Status = StatusActive
		}
		existing.Plan = acc.Plan
		existing.mu.Unlock()
		next = append(next, existing)
	}
	ss.mu.Lock()
	ss.slots = next
	ss.mu.Unlock()
	return nil
}

func (ss *Slots) List() []*Slot {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	out := make([]*Slot, len(ss.slots))
	copy(out, ss.slots)
	return out
}

func (ss *Slots) ByID(id int64) *Slot {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	for _, slot := range ss.slots {
		if slot.ID == id {
			return slot
		}
	}
	return nil
}

func (ss *Slots) Len() int {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return len(ss.slots)
}

func (ss *Slots) tryClaim(slot *Slot) bool {
	now := time.Now().Unix()
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.state.Status == StatusDisabled {
		return false
	}
	if slot.state.Status == StatusCooldown && slot.state.CooldownUntil > now {
		return false
	}
	return true
}

func (ss *Slots) IsCooling(slot *Slot) bool {
	now := time.Now().Unix()
	slot.mu.Lock()
	defer slot.mu.Unlock()
	return slot.state.Status == StatusCooldown && slot.state.CooldownUntil > now
}

func (ss *Slots) CooldownLeft(slot *Slot) int64 {
	now := time.Now().Unix()
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.state.Status == StatusCooldown && slot.state.CooldownUntil > now {
		return slot.state.CooldownUntil - now
	}
	return 0
}

func (ss *Slots) MinCooldown() int64 {
	now := time.Now().Unix()
	var min int64 = 1 << 62
	for _, slot := range ss.List() {
		slot.mu.Lock()
		if slot.state.Status == StatusCooldown && slot.state.CooldownUntil > now {
			if d := slot.state.CooldownUntil - now; d < min {
				min = d
			}
		}
		slot.mu.Unlock()
	}
	if min == 1<<62 {
		return 30
	}
	if min < 1 {
		return 1
	}
	return min
}

func (ss *Slots) MarkOK(slot *Slot) {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	slot.state.ConsecutiveFails = 0
}

func (ss *Slots) NoteUsage(slot *Slot, usage *AccountUsage) {
	slot.SetUsage(usage.Clone())
}

func (u *AccountUsage) Clone() *AccountUsage {
	if u == nil {
		return nil
	}
	c := *u
	c.Windows = append([]UsageWindow(nil), u.Windows...)
	return &c
}

func (ss *Slots) Band(slot *Slot, softLimit float64) Band {
	now := time.Now().Unix()
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.state.Usage == nil {
		return BandSteady
	}
	return slot.state.Usage.Band(softLimit, now)
}

func (ss *Slots) ServesModel(slot *Slot, model string) bool {
	if model == "" {
		return true
	}
	slot.catalogMu.Lock()
	defer slot.catalogMu.Unlock()
	if len(slot.state.Catalog) == 0 {
		return true
	}
	for _, m := range slot.state.Catalog {
		if m == model {
			return true
		}
	}
	return false
}

func (ss *Slots) SetCatalog(slot *Slot, models []string) {
	slot.catalogMu.Lock()
	defer slot.catalogMu.Unlock()
	slot.state.Catalog = models
	slot.state.CatalogAt = time.Now().Unix()
}

// SetServiceTiers stores the per-model service tiers advertised by the
// account's catalog probe.
func (ss *Slots) SetServiceTiers(slot *Slot, tiers map[string][]string) {
	slot.catalogMu.Lock()
	defer slot.catalogMu.Unlock()
	slot.state.ServiceTiers = tiers
}

// ServesTier reports whether an account's cached catalog advertises the
// requested service tier for the model, mirroring Rust Slots::serves_tier
// (including the synthesized "ultrafast" tier for gpt-5.6-sol). Accounts
// whose catalog has not been probed yet are not excluded.
func (ss *Slots) ServesTier(slot *Slot, model, tier string) bool {
	if model == "gpt-5.6-sol" && tier == "ultrafast" {
		return true
	}
	slot.catalogMu.Lock()
	defer slot.catalogMu.Unlock()
	if len(slot.state.ServiceTiers) == 0 {
		return true
	}
	for _, t := range slot.state.ServiceTiers[model] {
		if t == tier {
			return true
		}
	}
	return false
}

func (ss *Slots) ModelCooling(slot *Slot, model string) bool {
	now := time.Now().Unix()
	slot.mu.Lock()
	defer slot.mu.Unlock()
	for modelName, until := range slot.state.ModelCooldowns {
		if until <= now {
			delete(slot.state.ModelCooldowns, modelName)
		}
	}
	_, cooling := slot.state.ModelCooldowns[model]
	return cooling
}

func (ss *Slots) CoolModel(slot *Slot, model string, secs int64) {
	slot.mu.Lock()
	slot.state.ModelCooldowns[model] = time.Now().Unix() + secs
	slot.mu.Unlock()
	slog.Warn("model cooling", "account", slot.Display, "model", model, "secs", secs)
}

func (ss *Slots) Cool(ctx context.Context, slot *Slot, secs int64, why string) {
	now := time.Now().Unix()
	until := now + secs
	slot.mu.Lock()
	extend := true
	if slot.state.Status == StatusCooldown && slot.state.CooldownUntil >= until {
		extend = false
	}
	if slot.state.Status != StatusDisabled && !(slot.state.Status == StatusCooldown && slot.state.CooldownUntil > now) {
		slot.state.ConsecutiveFails++
	}
	if extend && slot.state.Status != StatusDisabled {
		slot.state.Status = StatusCooldown
		slot.state.CooldownUntil = until
	} else {
		extend = false
	}
	slot.mu.Unlock()
	slog.Warn("account cooling", "account", slot.Display, "secs", secs, "why", why)
	if extend {
		if _, err := ss.store.SetAccountCooldown(ctx, slot.ID, until); err != nil {
			slog.Warn("persist cooldown failed", "error", err)
		}
	}
}

func (ss *Slots) CoolRateLimited(ctx context.Context, slot *Slot, retryAfter *int64, max, base int64) {
	slot.mu.Lock()
	fails := slot.state.ConsecutiveFails
	slot.mu.Unlock()
	var secs int64
	if retryAfter != nil {
		secs = *retryAfter
	} else {
		shift := uint(fails)
		if shift > 6 {
			shift = 6
		}
		secs = base * (1 << shift)
	}
	lower := base
	if lower > 30 {
		lower = 30
	}
	if secs < lower {
		secs = lower
	}
	if secs > max {
		secs = max
	}
	ss.Cool(ctx, slot, secs, "rate limited")
}

func (ss *Slots) CoolFailure(ctx context.Context, slot *Slot, why string) {
	slot.mu.Lock()
	fails := slot.state.ConsecutiveFails
	slot.mu.Unlock()
	shift := uint(fails)
	if shift > 6 {
		shift = 6
	}
	secs := int64(15) * (1 << shift)
	if secs > 900 {
		secs = 900
	}
	ss.Cool(ctx, slot, secs, why)
}

// FreshToken serializes refresh per slot: refresh tokens rotate, so two
// concurrent refreshes would invalidate each other.
func (ss *Slots) FreshToken(ctx context.Context, slot *Slot, force bool) (string, error) {
	slot.mu.Lock()
	defer slot.mu.Unlock()

	if IsAPIKey(slot.AuthMode) || ss.refresher == nil || !ss.provider.Refreshable() {
		return slot.creds.AccessToken, nil
	}
	now := time.Now().Unix()
	if !force && slot.creds.ExpiresAt > 0 && slot.creds.ExpiresAt-now > 60 {
		return slot.creds.AccessToken, nil
	}
	// Adopt a token another process already rotated rather than spending ours.
	stored, err := ss.store.FindAccount(ctx, itoa(slot.ID))
	if err == nil && stored.RefreshToken != slot.creds.RefreshToken {
		slot.creds = Credentials{stored.AccessToken, stored.RefreshToken, stored.AccessExpiresAt}
		return slot.creds.AccessToken, nil
	}

	slog.Info("refreshing access token", "provider", ss.provider, "account", slot.Display)
	tokens, err := ss.refresher.Refresh(ctx, ss.provider, slot.creds.RefreshToken)
	if err != nil {
		if oauth.IsTerminal(err) {
			slog.Error("refresh token dead; disabling", "account", slot.Display, "error", err)
			slot.state.Status = StatusDisabled
			_ = ss.store.SetAccountStatus(ctx, slot.ID, "disabled", 0, err.Error())
		} else {
			slog.Warn("refresh failed transiently", "account", slot.Display, "error", err)
			if slot.state.Status != StatusDisabled {
				slot.state.Status = StatusCooldown
				slot.state.CooldownUntil = now + 30
			}
		}
		return "", fmt.Errorf("refresh: %w", err)
	}
	if err := ss.store.UpdateAccountTokens(ctx, slot.ID, tokens.AccessToken, tokens.RefreshToken, tokens.ExpiresAt); err != nil {
		slog.Error("persist refreshed tokens failed", "error", err)
		return "", err
	}
	slot.creds = Credentials{tokens.AccessToken, tokens.RefreshToken, tokens.ExpiresAt}
	if tokens.Plan != "" && tokens.Plan != slot.Plan {
		slot.Plan = tokens.Plan
		_ = ss.store.SetAccountField(ctx, slot.ID, "plan_type", tokens.Plan)
	}
	return slot.creds.AccessToken, nil
}

type AccountSnapshot struct {
	Slot     *Slot
	Status   Status
	Cooldown int64
	Failures int32
	Usage    *AccountUsage
}

func (ss *Slots) Snapshots() []AccountSnapshot {
	now := time.Now().Unix()
	var out []AccountSnapshot
	for _, slot := range ss.List() {
		slot.mu.Lock()
		snap := AccountSnapshot{Slot: slot, Status: slot.state.Status, Failures: slot.state.ConsecutiveFails, Usage: slot.state.Usage.Clone()}
		if slot.state.Status == StatusCooldown && slot.state.CooldownUntil > now {
			snap.Cooldown = slot.state.CooldownUntil - now
		}
		slot.mu.Unlock()
		out = append(out, snap)
	}
	return out
}

func (ss *Slots) PoolWindows(user string, pinned *int64, softLimit float64) []UsageWindow {
	slots := ss.List()
	sums := make(map[string]float64)
	counts := make(map[string]int)
	resets := make(map[string]int64)
	for _, slot := range slots {
		if !slot.Serves(user) {
			continue
		}
		if pinned != nil && *pinned != slot.ID {
			continue
		}
		slot.mu.Lock()
		utilization := float64(1.0)
		if !(slot.state.Status == StatusCooldown && slot.state.CooldownUntil > time.Now().Unix()) && slot.state.Usage != nil {
			for _, w := range slot.state.Usage.Windows {
				sums[w.Name] += w.Utilization
				counts[w.Name]++
				if w.ResetsAt > 0 && (resets[w.Name] == 0 || w.ResetsAt < resets[w.Name]) {
					resets[w.Name] = w.ResetsAt
				}
			}
		} else if slot.state.Usage != nil {
			for _, w := range slot.state.Usage.Windows {
				sums[w.Name] += utilization
				counts[w.Name]++
			}
		}
		slot.mu.Unlock()
	}
	names := make([]string, 0, len(sums))
	for name := range sums {
		names = append(names, name)
	}
	sort.Strings(names)
	var out []UsageWindow
	for _, name := range names {
		out = append(out, UsageWindow{Name: name, Utilization: sums[name] / float64(max(counts[name], 1)), ResetsAt: resets[name]})
	}
	return out
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
