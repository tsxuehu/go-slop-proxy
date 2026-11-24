package pool

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/edy/slop-proxy-go/internal/provider"
)

type Route struct {
	SessionKey    string
	Model         string
	ServiceTier   string
	User          string
	PinnedAccount *int64
	PreferTrusted bool
	ReservedOnly  bool
	APIKeyOnly    bool
}

// ExplicitTier returns the requested service tier, or "" when the client left
// it unspecified ("auto"/"default"/empty), matching Rust Route::explicit_tier.
func (r Route) ExplicitTier() string {
	switch r.ServiceTier {
	case "", "auto", "default":
		return ""
	}
	return r.ServiceTier
}

// tieredBackend is optional: only the Codex backend advertises per-model
// service tiers, so other pools ignore the tier entirely.
type tieredBackend interface{ Tiered() bool }

func (p *Pool) tiered() bool {
	b, ok := p.backend.(tieredBackend)
	return ok && b.Tiered()
}

type Backend interface {
	Provider() provider.Provider
	Send(ctx context.Context, token string, slot *Slot, route Route, req any) (any, error)
	SoftLimit() float64
	Attempts() int
	RateLimitMax() int64
	RateLimitBase() int64
	RefreshOnce() bool
	StickyWaitSecs() int64
	SessionAffinity() bool
	RetryBudget() time.Duration
	UsageFrom(resp any) *AccountUsage
	RetryableBadRequest(body string) bool
	Reason(body string) string
}

// AnonymousBackend is implemented by pools that can serve without an account,
// such as Zen's free tier. The optional interface keeps the common Backend
// contract unchanged for every other provider.
type AnonymousBackend interface {
	Anonymous() bool
	SendAnonymous(ctx context.Context, route Route, req any) (any, error)
}

// IsAPIKey accepts both the original spelling and the Rust original's
// "api_key", so databases written by either version keep working.
func IsAPIKey(mode string) bool { return mode == "api_key" || mode == "apikey" }

type PoolErrorKind int

const (
	ErrNoAccounts PoolErrorKind = iota
	ErrAllCooling
	ErrBadRequest
	ErrUpstream
	ErrRefused
)

type PoolError struct {
	Kind       PoolErrorKind
	Message    string
	RetryAfter int64
	Attempts   int
	Provider   provider.Provider
}

func (e *PoolError) Error() string { return e.Message }

type SendErrorKind int

const (
	SendAuth SendErrorKind = iota
	SendRateLimited
	SendModelLimited
	SendBadRequest
	SendNetwork
	SendOther
)

type SendError struct {
	Kind       SendErrorKind
	RetryAfter *int64
	Body       string
	Headers    http.Header
}

func (e *SendError) Error() string { return e.Body }

type Served struct {
	AccountID *int64
	Response  any
	Attempts  int
}

type bound struct {
	accountID int64
	seen      int64
}

const (
	bindingTTL  = 12 * 3600
	maxBindings = 50000
)

type Pool struct {
	slots   *Slots
	backend Backend
	boundMu sync.Mutex
	bound   map[string]bound
}

func NewPool(slots *Slots, backend Backend) *Pool {
	return &Pool{slots: slots, backend: backend, bound: make(map[string]bound)}
}

func (p *Pool) Slots() *Slots { return p.slots }

// FirstAnswer asks each claimable account in turn until one can answer a
// non-chat auxiliary call (catalog, usage report, version probe). Failures
// here never cool an account: they are informational, not user traffic.
func (p *Pool) FirstAnswer(ctx context.Context, f func(context.Context, string, *Slot) (any, error)) (any, error) {
	for _, slot := range p.slots.List() {
		if !p.slots.tryClaim(slot) {
			continue
		}
		token, err := p.slots.FreshToken(ctx, slot, false)
		if err != nil {
			continue
		}
		v, err := f(ctx, token, slot)
		if err == nil {
			return v, nil
		}
		slog.Debug("auxiliary probe failed", "account", slot.Display, "error", err)
	}
	return nil, errors.New("no account could answer")
}

func (p *Pool) boundAccount(sessionKey string) *int64 {
	if !p.backend.SessionAffinity() || sessionKey == "" {
		return nil
	}
	p.boundMu.Lock()
	defer p.boundMu.Unlock()
	entry, ok := p.bound[sessionKey]
	if !ok || time.Now().Unix()-entry.seen >= bindingTTL {
		return nil
	}
	id := entry.accountID
	return &id
}

func (p *Pool) bindSession(sessionKey string, accountID int64) {
	if !p.backend.SessionAffinity() || sessionKey == "" {
		return
	}
	now := time.Now().Unix()
	p.boundMu.Lock()
	defer p.boundMu.Unlock()
	if len(p.bound) >= maxBindings {
		for k, v := range p.bound {
			if now-v.seen >= bindingTTL {
				delete(p.bound, k)
			}
		}
	}
	p.bound[sessionKey] = bound{accountID: accountID, seen: now}
}

func (p *Pool) waitOutOwnCooldown(preferred *Slot) {
	wait := p.backend.StickyWaitSecs()
	if wait <= 0 || preferred == nil {
		return
	}
	left := p.slots.CooldownLeft(preferred)
	if left >= 1 && left <= wait {
		slog.Debug("waiting for session's own account", "account", preferred.Display, "left", left)
		time.Sleep(time.Duration(left+1) * time.Second)
	}
}

// ranked sorts accounts by eligibility and preference. Capacity comes before
// preference; within a band a session sticks to one account via rendezvous.
func (p *Pool) ranked(route Route) []*Slot {
	slots := p.slots.List()
	var pinned *int64
	for _, slot := range slots {
		if route.PinnedAccount != nil && slot.ID == *route.PinnedAccount {
			id := slot.ID
			pinned = &id
			break
		}
	}
	bound := p.boundAccount(route.SessionKey)
	for _, slot := range slots {
		if bound != nil && slot.ID == *bound {
			id := slot.ID
			bound = &id
			break
		}
	}

	tiered := p.tiered()
	tier := route.ExplicitTier()
	type scored struct {
		missing  bool
		band     Band
		away     bool
		mismatch bool
		score    uint64
		slot     *Slot
	}
	var candidates []scored
	for _, slot := range slots {
		if pinned != nil && slot.ID != *pinned {
			continue
		}
		if !slot.Serves(route.User) {
			continue
		}
		if slot.Reserved != route.ReservedOnly {
			continue
		}
		if route.APIKeyOnly && !IsAPIKey(slot.AuthMode) {
			continue
		}
		// A tiered backend hard-excludes accounts that do not advertise the
		// requested tier for this model (Rust ranked() 358-363).
		if tiered && tier != "" && !p.slots.ServesTier(slot, route.Model, tier) {
			continue
		}
		missing := route.Model != "" && !p.slots.ServesModel(slot, route.Model)
		band := p.slots.Band(slot, p.backend.SoftLimit())
		away := bound != nil && slot.ID != *bound
		mismatch := tiered && slot.Trusted != route.PreferTrusted
		candidates = append(candidates, scored{missing, band, away, mismatch, rendezvousScore(route.SessionKey, slot.ID), slot})
	}

	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if a.missing != b.missing {
			return !a.missing
		}
		if a.band != b.band {
			return a.band < b.band
		}
		if a.away != b.away {
			return !a.away
		}
		if a.mismatch != b.mismatch {
			return !a.mismatch
		}
		// Rust sorts on Reverse(rendezvous_score): the larger hash wins.
		return a.score > b.score
	})

	out := make([]*Slot, len(candidates))
	for i, c := range candidates {
		out[i] = c.slot
	}
	return out
}

// Execute runs the backend's normal Send path. ExecuteWebSocket reuses the
// same ranking, token refresh, 401 retry and cooldown machinery for a
// websocket handshake, where the "send" is a dial rather than an HTTP POST.
func (p *Pool) Execute(ctx context.Context, route Route, req any) (*Served, error) {
	return p.executeWith(ctx, route, req, func(ctx context.Context, token string, slot *Slot) (any, error) {
		return p.backend.Send(ctx, token, slot, route, req)
	}, false)
}

func (p *Pool) ExecuteWebSocket(ctx context.Context, route Route, dial func(context.Context, string, *Slot) (any, error)) (*Served, error) {
	return p.executeWith(ctx, route, nil, dial, true)
}

func (p *Pool) executeWith(ctx context.Context, route Route, req any, send func(context.Context, string, *Slot) (any, error), handshake bool) (*Served, error) {
	budget := p.backend.RetryBudget()
	deadline := time.Now().Add(budget)
	var priorAttempts int
	for {
		served, err := p.sweepWith(ctx, route, req, send, handshake, priorAttempts)
		if err == nil {
			return served, nil
		}
		var pe *PoolError
		if !errors.As(err, &pe) || pe.Kind != ErrAllCooling || budget <= 0 {
			return nil, err
		}
		priorAttempts = pe.Attempts
		left := time.Until(deadline)
		if left <= 0 {
			return nil, err
		}
		wait := time.Duration(p.slots.MinCooldown()) * time.Second
		if wait > left {
			wait = left
		}
		wait += time.Duration(rand.Int64N(1500)) * time.Millisecond
		slog.Info("pool empty, holding request", "provider", p.backend.Provider(), "wait_ms", wait.Milliseconds())
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
}

func (p *Pool) sweep(ctx context.Context, route Route, req any, priorAttempts int) (*Served, error) {
	return p.sweepWith(ctx, route, req, func(ctx context.Context, token string, slot *Slot) (any, error) {
		return p.backend.Send(ctx, token, slot, route, req)
	}, false, priorAttempts)
}

func (p *Pool) sweepWith(ctx context.Context, route Route, req any, send func(context.Context, string, *Slot) (any, error), handshake bool, priorAttempts int) (*Served, error) {
	ranked := p.ranked(route)
	if len(ranked) == 0 {
		if p.tiered() && route.ExplicitTier() != "" {
			return nil, &PoolError{Kind: ErrRefused,
				Message:  "no eligible account advertises service tier " + route.ExplicitTier() + " for " + route.Model,
				Provider: p.backend.Provider()}
		}
		if route.APIKeyOnly {
			return nil, &PoolError{Kind: ErrBadRequest, Message: "fast mode is served only by API-key accounts and none is available to this token"}
		}
		if ab, ok := p.backend.(AnonymousBackend); ok && ab.Anonymous() {
			resp, err := ab.SendAnonymous(ctx, route, req)
			if err == nil {
				return &Served{Response: resp, Attempts: priorAttempts}, nil
			}
			if se := toSendError(err); se != nil {
				switch se.Kind {
				case SendBadRequest:
					return nil, &PoolError{Kind: ErrBadRequest, Message: p.backend.Reason(se.Body)}
				case SendRateLimited, SendModelLimited:
					retry := int64(30)
					if se.RetryAfter != nil {
						retry = *se.RetryAfter
					}
					return nil, &PoolError{Kind: ErrAllCooling, RetryAfter: retry, Attempts: priorAttempts}
				case SendAuth:
					return nil, &PoolError{Kind: ErrRefused, Message: se.Body, Provider: p.backend.Provider()}
				default:
					return nil, &PoolError{Kind: ErrUpstream, Message: se.Body}
				}
			}
		}
		return nil, &PoolError{Kind: ErrNoAccounts, Message: "no usable accounts; run login"}
	}
	p.waitOutOwnCooldown(ranked[0])

	var lastErr *SendError
	attempts := 0
	serverCooling := false
	for _, slot := range ranked {
		if attempts >= p.backend.Attempts() {
			break
		}
		serves := p.slots.ServesModel(slot, route.Model)
		if !serves && serverCooling {
			break
		}
		if p.slots.ModelCooling(slot, route.Model) || !p.slots.tryClaim(slot) {
			if serves {
				serverCooling = true
			}
			continue
		}
		attempts++
		token, err := p.slots.FreshToken(ctx, slot, false)
		if err != nil {
			continue
		}
		resp, sendErr := send(ctx, token, slot)
		if sendErr == nil {
			p.bindSession(route.SessionKey, slot.ID)
			// A handshake has not served a turn yet, so it must not clear the
			// failure counter (Rust is_handshake() skips mark_ok).
			if !handshake {
				p.slots.MarkOK(slot)
			}
			if usage := p.backend.UsageFrom(resp); usage != nil {
				p.slots.NoteUsage(slot, usage)
			}
			return &Served{AccountID: &slot.ID, Response: resp, Attempts: priorAttempts + attempts}, nil
		}

		var se *SendError
		if !errors.As(sendErr, &se) {
			se = &SendError{Kind: SendOther, Body: sendErr.Error()}
		}
		switch se.Kind {
		case SendAuth:
			if p.backend.RefreshOnce() && slot.AuthMode == "oauth" {
				slog.Warn("got 401, forcing refresh", "account", slot.Display)
				if fresh, err := p.slots.FreshToken(ctx, slot, true); err == nil {
					resp, err2 := send(ctx, fresh, slot)
					if err2 == nil {
						p.bindSession(route.SessionKey, slot.ID)
						if !handshake {
							p.slots.MarkOK(slot)
						}
						if usage := p.backend.UsageFrom(resp); usage != nil {
							p.slots.NoteUsage(slot, usage)
						}
						return &Served{AccountID: &slot.ID, Response: resp, Attempts: priorAttempts + attempts}, nil
					}
					p.slots.Cool(ctx, slot, 60, "post-refresh failure")
					lastErr = toSendError(err2)
				} else {
					lastErr = se
				}
			} else {
				p.slots.Cool(ctx, slot, 15*60, "key rejected")
				lastErr = se
			}
		case SendRateLimited:
			p.slots.CoolRateLimited(ctx, slot, se.RetryAfter, p.backend.RateLimitMax(), p.backend.RateLimitBase())
			if serves {
				serverCooling = true
			}
			lastErr = se
		case SendModelLimited:
			secs := p.backend.RateLimitBase()
			if se.RetryAfter != nil {
				secs = *se.RetryAfter
			}
			if secs > p.backend.RateLimitMax() {
				secs = p.backend.RateLimitMax()
			}
			p.slots.CoolModel(slot, route.Model, secs)
			if serves {
				serverCooling = true
			}
			lastErr = se
		case SendBadRequest:
			if p.backend.RetryableBadRequest(se.Body) {
				slog.Warn("account cannot serve model, trying another", "account", slot.Display)
				lastErr = se
			} else {
				return nil, &PoolError{Kind: ErrBadRequest, Message: p.backend.Reason(se.Body)}
			}
		case SendNetwork:
			slog.Warn("network error, account not cooled", "account", slot.Display, "error", se.Body)
			lastErr = se
		default:
			p.slots.CoolFailure(ctx, slot, se.Body)
			lastErr = se
		}
	}

	if lastErr != nil && lastErr.Kind == SendBadRequest {
		return nil, &PoolError{Kind: ErrBadRequest, Message: p.backend.Reason(lastErr.Body)}
	}
	if lastErr != nil && lastErr.Kind != SendRateLimited && lastErr.Kind != SendModelLimited {
		return nil, &PoolError{Kind: ErrUpstream, Message: lastErr.Body}
	}
	retry := p.slots.MinCooldown()
	if retry < 30 {
		retry = 30
	}
	return nil, &PoolError{
		Kind: ErrAllCooling, RetryAfter: retry, Attempts: priorAttempts + attempts,
		Message: "all upstream accounts are cooling down",
	}
}

func toSendError(err error) *SendError {
	var se *SendError
	if errors.As(err, &se) {
		return se
	}
	return &SendError{Kind: SendOther, Body: err.Error()}
}
