package pool

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"
)

const WSExhaustedCooldown = 15 * 60

// WebSocketServes reports whether the connection's current account can serve
// this turn. Only an explicit service tier can force a redial.
func (p *Pool) WebSocketServes(accountID *int64, route Route) bool {
	if route.ExplicitTier() == "" {
		return true
	}
	if accountID == nil {
		return false
	}
	slot := p.slots.ByID(*accountID)
	if slot == nil || !slot.Serves(route.User) || p.slots.IsDisabled(slot) {
		return false
	}
	return p.slots.ServesTier(slot, route.Model, route.ExplicitTier())
}

func (ss *Slots) IsDisabled(slot *Slot) bool {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	return slot.state.Status == StatusDisabled
}

// WSCompleted clears the failure counter after a fully generated turn.
func (ss *Slots) WSCompleted(slot *Slot) { ss.MarkOK(slot) }

// WSFailed applies the exponential failure backoff (15..900s).
func (ss *Slots) WSFailed(ctx context.Context, slot *Slot, why string) {
	ss.CoolFailure(ctx, slot, why)
}

// WSExhausted benches a spent account until its earliest future window reset,
// clamped to [900s, 6h]. Rust pool/codex.rs::websocket_exhausted.
func (ss *Slots) WSExhausted(ctx context.Context, slot *Slot) {
	now := time.Now().Unix()
	secs := int64(WSExhaustedCooldown)
	windows := ss.LimitWindows(slot, "")
	for _, w := range windows {
		if w.ResetsAt > now && (secs == WSExhaustedCooldown || w.ResetsAt-now < secs) {
			candidate := w.ResetsAt - now
			if candidate < WSExhaustedCooldown {
				candidate = WSExhaustedCooldown
			}
			if candidate > 6*3600 {
				candidate = 6 * 3600
			}
			secs = candidate
		}
	}
	ss.Cool(ctx, slot, secs, "usage limit reached")
}

// NoteLimitWindows records the windows from a codex.rate_limits event. An
// empty limit name writes the account's main quota windows; a named limit is
// kept separately, like Rust's SlotState::limit_windows.
func (ss *Slots) NoteLimitWindows(slot *Slot, limit string, windows []UsageWindow) {
	if len(windows) == 0 {
		return
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.state.LimitWindows == nil {
		slot.state.LimitWindows = make(map[string][]UsageWindow)
	}
	apply := func(stored []UsageWindow) []UsageWindow {
		for _, window := range windows {
			found := false
			for i := range stored {
				if stored[i].Name == window.Name {
					stored[i] = window
					found = true
					break
				}
			}
			if !found {
				stored = append(stored, window)
			}
		}
		return stored
	}
	if limit == "" {
		if slot.state.Usage == nil {
			slot.state.Usage = &AccountUsage{}
		}
		slot.state.Usage.Windows = apply(slot.state.Usage.Windows)
	} else {
		slot.state.LimitWindows[limit] = apply(slot.state.LimitWindows[limit])
	}
	if limit == "" && slot.state.Usage != nil {
		slot.state.Usage.Locked = slot.state.Usage.Peak() >= 1.0
		slot.state.Usage.ObservedAt = time.Now().Unix()
	}
}

func (ss *Slots) LimitWindows(slot *Slot, limit string) []UsageWindow {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.state.Status == StatusDisabled {
		return nil
	}
	var stored []UsageWindow
	if limit == "" {
		if slot.state.Usage != nil {
			stored = slot.state.Usage.Windows
		}
	} else {
		stored = slot.state.LimitWindows[limit]
	}
	return append([]UsageWindow(nil), stored...)
}

// PoolWindowsNamed averages the same window across accounts so a caller's
// figures do not jump when routing moves it. Cooling accounts count as fully
// used (1.0); resets_at is the earliest sample.
func (ss *Slots) PoolWindowsNamed(user string, pinned *int64, limit string) []UsageWindow {
	slots := ss.List()
	pinnedID := int64(0)
	hasPinned := false
	if pinned != nil {
		for _, slot := range slots {
			if slot.ID == *pinned {
				pinnedID, hasPinned = *pinned, true
				break
			}
		}
	}
	type aggregate struct {
		sum    float64
		count  int
		reset  int64
		hasRes bool
	}
	byName := make(map[string]*aggregate)
	for _, slot := range slots {
		if !slot.Serves(user) || (hasPinned && slot.ID != pinnedID) {
			continue
		}
		cooling := ss.IsCooling(slot)
		windows := ss.LimitWindows(slot, limit)
		for _, w := range windows {
			a := byName[w.Name]
			if a == nil {
				a = &aggregate{}
				byName[w.Name] = a
			}
			if cooling {
				a.sum += 1.0
			} else {
				a.sum += w.Utilization
			}
			a.count++
			if w.ResetsAt > 0 && (!a.hasRes || w.ResetsAt < a.reset) {
				a.reset, a.hasRes = w.ResetsAt, true
			}
		}
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]UsageWindow, 0, len(names))
	for _, name := range names {
		a := byName[name]
		out = append(out, UsageWindow{
			Name: name, Utilization: a.sum / float64(max(a.count, 1)), ResetsAt: a.reset,
		})
	}
	return out
}

func windowNameFor(minutes int64) string {
	switch {
	case minutes%1440 == 0:
		return itoa(minutes/1440) + "d"
	case minutes%60 == 0:
		return itoa(minutes/60) + "h"
	default:
		return itoa(minutes) + "m"
	}
}

// RewriteRateLimits replaces the per-account figures in a codex.rate_limits
// event with the pool-wide average, so a client cannot infer which account
// served the turn. Rust pool/codex.rs::rewrite_rate_limits.
func (p *Pool) RewriteRateLimits(accountID *int64, user string, pinned *int64, event map[string]any) {
	limit := "codex"
	for _, key := range []string{"metered_limit_name", "limit_name"} {
		if raw, ok := event[key].(string); ok && strings.TrimSpace(raw) != "" {
			limit = strings.ToLower(strings.TrimSpace(raw))
			break
		}
	}
	limit = strings.ReplaceAll(limit, "-", "_")
	named := ""
	if limit != "codex" {
		named = limit
	}

	var windows []UsageWindow
	if rateLimits, ok := event["rate_limits"].(map[string]any); ok {
		for _, tier := range []string{"primary", "secondary"} {
			win, ok := rateLimits[tier].(map[string]any)
			if !ok {
				continue
			}
			minutes, okM := jsonInt(win["window_minutes"])
			percent, okP := jsonFloat(win["used_percent"])
			if !okM || !okP || minutes <= 0 || percent < 0 {
				continue
			}
			var reset int64
			if v, ok := jsonInt(win["reset_at"]); ok {
				reset = v
			}
			windows = append(windows, UsageWindow{
				Name: windowNameFor(minutes), Utilization: percent / 100.0, ResetsAt: reset,
			})
		}
	}
	if accountID != nil {
		if slot := p.slots.ByID(*accountID); slot != nil {
			p.slots.NoteLimitWindows(slot, named, windows)
		} else {
			slog.Debug("rate-limits event has no known account", "account", *accountID)
		}
	}
	pooled := p.slots.PoolWindowsNamed(user, pinned, named)
	sort.SliceStable(pooled, func(i, j int) bool {
		si, oki := WindowSeconds(pooled[i].Name)
		sj, okj := WindowSeconds(pooled[j].Name)
		if !oki {
			si = int64(^uint64(0) >> 1)
		}
		if !okj {
			sj = int64(^uint64(0) >> 1)
		}
		return si < sj
	})
	out := map[string]any{}
	for i, tier := range []string{"primary", "secondary"} {
		if i >= len(pooled) {
			break
		}
		seconds, ok := WindowSeconds(pooled[i].Name)
		if !ok {
			continue
		}
		var reset any
		if pooled[i].ResetsAt > 0 {
			reset = pooled[i].ResetsAt
		}
		out[tier] = map[string]any{
			"used_percent":   pooled[i].Utilization * 100.0,
			"window_minutes": seconds / 60,
			"reset_at":       reset,
		}
	}
	event["rate_limits"] = out
	delete(event, "credits")
	delete(event, "plan_type")
}

func jsonInt(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	case int:
		return int64(n), true
	case string:
		i, err := strconv.ParseInt(strings.TrimSpace(n), 10, 64)
		return i, err == nil
	}
	return 0, false
}

func jsonFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case int:
		return float64(n), true
	}
	return 0, false
}
