package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/edy/slop-proxy-go/internal/pool"
	"github.com/edy/slop-proxy-go/internal/store"
)

// Prometheus text is rendered off the scrape path: a background refresher
// folds incremental usage_log slices once a minute, and /metrics concatenates
// the live account gauges with that cached history. This mirrors Rust's
// server/metrics.rs exactly (including unconditional HELP/TYPE lines).

type metricFamily struct{ name, kind, help string }

var accountFamilies = []metricFamily{
	{"slop_account_info", "gauge", "Constant 1, carrying the account's plan and trusted access as labels"},
	{"slop_account_capacity", "gauge", "Share of the provider's largest plan this account is worth, for weighting a pooled average. Emitted only where quota is reported, so a provider that reports none stays out of the average entirely"},
	{"slop_account_status", "gauge", "Account state: 0 active, 1 cooling down, 2 disabled"},
	{"slop_account_cooldown_seconds", "gauge", "Seconds until the account is retried, 0 when active"},
	{"slop_account_consecutive_fails", "gauge", "Consecutive upstream failures feeding the backoff"},
	{"slop_account_usage_age_seconds", "gauge", "Age of the quota sample. Codex only reports quota on a served response, so an idle account's figures stop advancing"},
	{"slop_account_locked", "gauge", "1 when the provider has locked the account out until a window resets"},
	{"slop_account_utilization_ratio", "gauge", "Fraction of the account's rolling limit window consumed"},
	{"slop_account_window_reset_seconds", "gauge", "Seconds until the account's limit window rolls over"},
	{"slop_account_model_utilization_ratio", "gauge", "Fraction of a model's own sub-limit consumed. Measured against that model's allowance rather than the account's, so it is not comparable to slop_account_utilization_ratio"},
	{"slop_account_model_window_reset_seconds", "gauge", "Seconds until the model's own limit window rolls over"},
}

var historyFamilies = []metricFamily{
	{"slop_requests_total", "counter", "Requests served"},
	{"slop_request_errors_total", "counter", "Requests that failed"},
	{"slop_request_duration_seconds_sum", "counter", "Total time served, divide by slop_requests_total for the mean"},
	{"slop_cost_usd_total", "counter", "What the traffic actually cost"},
	{"slop_list_cost_usd_total", "counter", "What the same traffic would have cost at the model's list rate, so a free tier is worth something rather than reading as zero spend"},
	{"slop_tokens_total", "counter", "Tokens by kind"},
	{"slop_cache_hit_ratio", "gauge", "Share of all recorded prompt tokens served from cache per label set"},
	{"slop_errors_total", "counter", "Failed requests by cause"},
	{"slop_tool_turns_total", "counter", "Turns that used each tool. Names only, never their arguments. A turn calling one tool repeatedly counts once"},
	{"slop_tool_errors_total", "counter", "Failed turns that used each tool. Names only, never their arguments or results"},
	{"slop_stop_reason_total", "counter", "Requests by how the turn ended"},
	{"slop_request_bytes_total", "counter", "Request bodies as the handler parsed them"},
	{"slop_response_bytes_total", "counter", "Response bytes relayed"},
	{"slop_turns_total", "counter", "Messages carried in, summed. Divide by requests for conversation depth"},
	{"slop_images_total", "counter", "Image parts carried in"},
	{"slop_thinking_budget_total", "counter", "Thinking tokens asked for, against reasoning tokens actually spent"},
	{"slop_tools_declared_total", "counter", "Tools offered to the model, summed. Every one costs prompt on every turn"},
	{"slop_attempts_total", "counter", "Upstream account claims, summed. Divide by slop_stop_reason_total for attempts per request"},
	{"slop_ttft_ms_total", "counter", "Time to first byte, summed. Divide by slop_ttft_samples_total"},
	{"slop_ttft_samples_total", "counter", "Requests that produced a first byte"},
	{"slop_sessions", "gauge", "Distinct conversations seen. Divide slop_requests_total by it for turns per session"},
	{"slop_session_account_switches", "gauge", "Times a conversation moved to a different account. Each move re-bills the whole prefix upstream, so this is what a cache hit rate collapse looks like before the cost shows up"},
	{"slop_session_tokens_max", "gauge", "Tokens the largest single conversation has billed over its life, counting every turn it ever resent rather than what fits in the context now. Reasoning is excluded, being a subset of output"},
	{"slop_session_deepest_turn", "gauge", "Longest conversation seen, in messages carried"},
}

var tokenKinds = []struct {
	name string
	get  func(store.MetricUsageRow) int64
}{
	{"input", func(r store.MetricUsageRow) int64 { return r.InputTokens }},
	{"output", func(r store.MetricUsageRow) int64 { return r.OutputTokens }},
	{"cache_read", func(r store.MetricUsageRow) int64 { return r.CacheReadTokens }},
	{"cache_write", func(r store.MetricUsageRow) int64 { return r.CacheWriteTokens }},
	{"reasoning", func(r store.MetricUsageRow) int64 { return r.ReasoningTokens }},
}

var usageCounters = []struct {
	name string
	get  func(store.MetricUsageRow) string
}{
	{"slop_requests_total", func(r store.MetricUsageRow) string { return strconv.FormatInt(r.Requests, 10) }},
	{"slop_request_errors_total", func(r store.MetricUsageRow) string { return strconv.FormatInt(r.Errors, 10) }},
	{"slop_request_duration_seconds_sum", func(r store.MetricUsageRow) string { return strconv.FormatFloat(r.DurationMS/1000.0, 'f', -1, 64) }},
	{"slop_cost_usd_total", func(r store.MetricUsageRow) string { return strconv.FormatFloat(r.CostUSD, 'f', -1, 64) }},
	{"slop_list_cost_usd_total", func(r store.MetricUsageRow) string { return strconv.FormatFloat(r.ListCostUSD, 'f', -1, 64) }},
}

var insightCounters = []struct {
	name string
	get  func(store.MetricInsightRow) int64
}{
	{"slop_stop_reason_total", func(r store.MetricInsightRow) int64 { return r.Requests }},
	{"slop_request_bytes_total", func(r store.MetricInsightRow) int64 { return r.RequestBytes }},
	{"slop_response_bytes_total", func(r store.MetricInsightRow) int64 { return r.ResponseBytes }},
	{"slop_turns_total", func(r store.MetricInsightRow) int64 { return r.Turns }},
	{"slop_images_total", func(r store.MetricInsightRow) int64 { return r.Images }},
	{"slop_thinking_budget_total", func(r store.MetricInsightRow) int64 { return r.ThinkingBudget }},
	{"slop_tools_declared_total", func(r store.MetricInsightRow) int64 { return r.ToolsDeclared }},
	{"slop_attempts_total", func(r store.MetricInsightRow) int64 { return r.Attempts }},
	{"slop_ttft_ms_total", func(r store.MetricInsightRow) int64 { return r.TTFTMS }},
	{"slop_ttft_samples_total", func(r store.MetricInsightRow) int64 { return r.TTTTSamples }},
}

type metricsState struct {
	mu          sync.RWMutex
	history     string
	totals      *metricsTotals
	rewrites    uint64
	initialized bool
	serverRef   *Server
}

type metricsTotals struct {
	lastID      int64
	accounts    string
	rewrites    uint64
	usage       map[[8]string]*store.MetricUsageRow
	usageKeys   [][8]string
	errors      map[[3]string]*store.MetricErrorRow
	errorKeys   [][3]string
	tools       map[[2]string]*store.MetricToolRow
	toolKeys    [][2]string
	insights    map[[3]string]*store.MetricInsightRow
	insightKeys [][3]string
	sessions    map[[2]string]*sessionAgg
}

type sessionAgg struct {
	accounts map[int64]bool
	deepest  int64
	tokens   int64
}

func newMetricsTotals() *metricsTotals {
	return &metricsTotals{
		usage:    make(map[[8]string]*store.MetricUsageRow),
		errors:   make(map[[3]string]*store.MetricErrorRow),
		tools:    make(map[[2]string]*store.MetricToolRow),
		insights: make(map[[3]string]*store.MetricInsightRow),
		sessions: make(map[[2]string]*sessionAgg),
	}
}

func (m *metricsState) invalidate() {
	m.mu.Lock()
	m.rewrites++
	m.mu.Unlock()
}

func (m *metricsState) renderHistory() string {
	if m == nil {
		return ""
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.history
}

// refresh folds one incremental slice of usage_log into the cached text.
// A pricing rewrite or a changed account fingerprint forces a full rebuild.
func (m *metricsState) refresh(ctx context.Context) {
	s := m.serverRef
	m.mu.Lock()
	defer m.mu.Unlock()
	rewrites := m.rewrites
	t := m.totals
	if t == nil || t.rewrites != rewrites {
		t = newMetricsTotals()
		t.rewrites = rewrites
		m.totals = t
	}
	delta, err := s.store.MetricsSince(ctx, t.lastID)
	if err != nil {
		slog.Error("reading usage metrics", "error", err)
		return
	}
	if t.lastID > 0 && delta.Accounts != t.accounts {
		t = newMetricsTotals()
		t.rewrites = rewrites
		m.totals = t
		delta, err = s.store.MetricsSince(ctx, 0)
		if err != nil {
			slog.Error("rebuilding usage metrics", "error", err)
			return
		}
	}
	t.fold(delta)
	m.history = renderHistoryMetrics(t)
	m.initialized = true
}

// ensureFresh folds the first slice synchronously, matching Rust's immediate
// first interval tick, so the very first scrape is not empty.
func (m *metricsState) ensureFresh(ctx context.Context) {
	if m == nil {
		return
	}
	m.mu.RLock()
	ready := m.initialized
	m.mu.RUnlock()
	if !ready {
		m.refresh(ctx)
	}
}

func (t *metricsTotals) fold(d store.MetricsDelta) {
	t.lastID, t.accounts = d.LastID, d.Accounts
	for _, row := range d.Usage {
		key := row.Dimensions
		if cur := t.usage[key]; cur != nil {
			cur.Requests += row.Requests
			cur.Errors += row.Errors
			cur.InputTokens += row.InputTokens
			cur.OutputTokens += row.OutputTokens
			cur.CacheReadTokens += row.CacheReadTokens
			cur.CacheWriteTokens += row.CacheWriteTokens
			cur.ReasoningTokens += row.ReasoningTokens
			cur.CostUSD += row.CostUSD
			cur.ListCostUSD += row.ListCostUSD
			cur.DurationMS += row.DurationMS
		} else {
			copy := row
			t.usage[key] = &copy
			t.usageKeys = append(t.usageKeys, key)
		}
	}
	for _, row := range d.Errors {
		key := [3]string{row.User, row.Provider, row.Kind}
		if cur := t.errors[key]; cur != nil {
			cur.Count += row.Count
		} else {
			copy := row
			t.errors[key] = &copy
			t.errorKeys = append(t.errorKeys, key)
		}
	}
	for _, row := range d.Tools {
		key := [2]string{row.User, row.Tool}
		if cur := t.tools[key]; cur != nil {
			cur.Count += row.Count
			cur.Errors += row.Errors
		} else {
			copy := row
			t.tools[key] = &copy
			t.toolKeys = append(t.toolKeys, key)
		}
	}
	for _, row := range d.Insights {
		key := [3]string{row.User, row.Account, row.StopReason}
		if cur := t.insights[key]; cur != nil {
			cur.Requests += row.Requests
			cur.RequestBytes += row.RequestBytes
			cur.ResponseBytes += row.ResponseBytes
			cur.Turns += row.Turns
			cur.Images += row.Images
			cur.ThinkingBudget += row.ThinkingBudget
			cur.ToolsDeclared += row.ToolsDeclared
			cur.TTFTMS += row.TTFTMS
			cur.TTTTSamples += row.TTTTSamples
			cur.Attempts += row.Attempts
		} else {
			copy := row
			t.insights[key] = &copy
			t.insightKeys = append(t.insightKeys, key)
		}
	}
	for _, part := range d.Sessions {
		key := [2]string{part.User, part.SessionKey}
		agg := t.sessions[key]
		if agg == nil {
			agg = &sessionAgg{accounts: make(map[int64]bool)}
			t.sessions[key] = agg
		}
		if part.AccountID != nil {
			agg.accounts[*part.AccountID] = true
		}
		if part.Deepest > agg.deepest {
			agg.deepest = part.Deepest
		}
		agg.tokens += part.Tokens
	}
}

// startMetrics serves Prometheus text on the dedicated metrics listener. It
// is a separate port so scraping cannot consume a caller's API quota.
func (s *Server) startMetrics() {
	if s.cfg.MetricsBind == "" {
		return
	}
	s.startMetricsOn(nil)
}

// startMetricsOn serves /metrics on ln. A nil listener means bind
// cfg.MetricsBind ourselves; a non-nil one is a listener inherited across a
// hot handoff, so the new process keeps serving on the very same socket.
func (s *Server) startMetricsOn(ln net.Listener) {
	if s.metrics == nil {
		s.metrics = &metricsState{serverRef: s}
	}
	go s.metricsLoop(context.Background())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", s.serveMetrics)
	ms := &http.Server{Addr: s.cfg.MetricsBind, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	s.metricsMu.Lock()
	s.metricsServer = ms
	s.metricsMu.Unlock()
	go func() {
		slog.Info("metrics listening", "bind", s.cfg.MetricsBind)
		var err error
		if ln != nil {
			err = ms.Serve(ln)
		} else {
			err = ms.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server failed", "error", err)
		}
	}()
}

func (s *Server) metricsLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		s.metrics.refresh(ctx)
		<-ticker.C
	}
}

func label(s string) string {
	if s == "" {
		return "unset"
	}
	r := strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "\n", "\\n")
	return r.Replace(s)
}

func family(out *strings.Builder, f metricFamily) {
	fmt.Fprintf(out, "# HELP %s %s\n# TYPE %s %s\n", f.name, f.help, f.name, f.kind)
}

func sample(out *strings.Builder, name string, labels [][2]string, value string) {
	out.WriteString(name)
	out.WriteByte('{')
	for i, kv := range labels {
		if i > 0 {
			out.WriteByte(',')
		}
		fmt.Fprintf(out, `%s="%s"`, kv[0], label(kv[1]))
	}
	out.WriteString("} ")
	out.WriteString(value)
	out.WriteByte('\n')
}

func usageLabels(row *store.MetricUsageRow) [][2]string {
	out := make([][2]string, 0, 9)
	for i, dim := range store.UsageDimensions {
		out = append(out, [2]string{dim.Label, label(row.Dimensions[i])})
	}
	return out
}

func cacheHitRatio(input, cacheRead, cacheWrite int64) string {
	cached := float64(max64(cacheRead, 0))
	prompt := float64(max64(input, 0)) + cached + float64(max64(cacheWrite, 0))
	if prompt <= 0 {
		return "0"
	}
	return strconv.FormatFloat(cached/prompt, 'f', -1, 64)
}

func planCapacity(snap pool.AccountSnapshot) string {
	if snap.Usage != nil && snap.Usage.Capacity != 0 {
		return strconv.FormatFloat(snap.Usage.Capacity, 'f', -1, 64)
	}
	if snap.Slot.Plan == "default_claude_max_5x" {
		return "0.5"
	}
	return "1"
}

func (s *Server) serveMetrics(w http.ResponseWriter, r *http.Request) {
	var out strings.Builder
	out.Grow(8192)
	now := time.Now().Unix()
	renderAccountMetrics(&out, s, now)
	metrics := s.metrics
	if metrics == nil {
		// Servers built by tests without the background refresher still get
		// an accurate first scrape.
		metrics = &metricsState{serverRef: s}
	}
	metrics.ensureFresh(r.Context())
	out.WriteString(metrics.renderHistory())
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(out.String()))
}

func renderAccountMetrics(out *strings.Builder, s *Server, now int64) {
	type snapWithProvider struct {
		p    string
		snap pool.AccountSnapshot
	}
	var snaps []snapWithProvider
	for p, pl := range s.pools {
		if pl == nil {
			continue
		}
		for _, snap := range pl.Slots().Snapshots() {
			snaps = append(snaps, snapWithProvider{p.String(), snap})
		}
	}
	sort.Slice(snaps, func(i, j int) bool {
		if snaps[i].p != snaps[j].p {
			return snaps[i].p < snaps[j].p
		}
		return snaps[i].snap.Slot.Display < snaps[j].snap.Slot.Display
	})
	base := func(item snapWithProvider) [][2]string {
		return [][2]string{{"provider", label(item.p)}, {"account", label(item.snap.Slot.Display)}}
	}
	status := func(v pool.Status) string {
		switch v {
		case pool.StatusCooldown:
			return "1"
		case pool.StatusDisabled:
			return "2"
		}
		return "0"
	}
	for _, f := range accountFamilies {
		family(out, f)
		for _, item := range snaps {
			snap := item.snap
			labels := base(item)
			switch f.name {
			case "slop_account_info":
				info := append(append([][2]string{}, labels...),
					[2]string{"plan", label(snap.Slot.Plan)},
					[2]string{"trusted", strconv.FormatBool(snap.Slot.Trusted)})
				sample(out, f.name, info, "1")
			case "slop_account_status":
				sample(out, f.name, labels, status(snap.Status))
			case "slop_account_cooldown_seconds":
				sample(out, f.name, labels, strconv.FormatInt(snap.Cooldown, 10))
			case "slop_account_consecutive_fails":
				sample(out, f.name, labels, strconv.FormatInt(int64(snap.Failures), 10))
			case "slop_account_capacity":
				if snap.Usage != nil {
					sample(out, f.name, labels, planCapacity(snap))
				}
			case "slop_account_usage_age_seconds":
				if snap.Usage != nil {
					sample(out, f.name, labels, strconv.FormatInt(max64(now-snap.Usage.ObservedAt, 0), 10))
				}
			case "slop_account_locked":
				if snap.Usage != nil {
					v := "0"
					if snap.Usage.Locked {
						v = "1"
					}
					sample(out, f.name, labels, v)
				}
			case "slop_account_utilization_ratio", "slop_account_window_reset_seconds":
				if snap.Usage == nil {
					continue
				}
				for _, window := range snap.Usage.Windows {
					wl := append(append([][2]string{}, labels...), [2]string{"window", label(window.Name)})
					if f.name == "slop_account_utilization_ratio" {
						sample(out, f.name, wl, strconv.FormatFloat(window.Utilization, 'f', -1, 64))
					} else if window.ResetsAt > 0 {
						sample(out, f.name, wl, strconv.FormatInt(max64(window.ResetsAt-now, 0), 10))
					}
				}
			case "slop_account_model_utilization_ratio", "slop_account_model_window_reset_seconds":
				// Model sub-limit windows are provider-specific and only
				// present when an Anthropic usage probe reported them.
			}
		}
	}
}

func renderHistoryMetrics(t *metricsTotals) string {
	var out strings.Builder
	out.Grow(4096)
	if t == nil {
		for _, f := range historyFamilies {
			family(&out, f)
		}
		return out.String()
	}
	byName := make(map[string]metricFamily)
	for _, f := range historyFamilies {
		byName[f.name] = f
	}
	emit := func(name string, write func()) {
		family(&out, byName[name])
		write()
	}
	for _, counter := range usageCounters {
		c := counter
		emit(c.name, func() {
			for _, key := range t.usageKeys {
				row := t.usage[key]
				sample(&out, c.name, usageLabels(row), c.get(*row))
			}
		})
	}
	emit("slop_tokens_total", func() {
		for _, kind := range tokenKinds {
			for _, key := range t.usageKeys {
				row := t.usage[key]
				labels := append(append([][2]string{}, usageLabels(row)...), [2]string{"kind", kind.name})
				sample(&out, "slop_tokens_total", labels, strconv.FormatInt(kind.get(*row), 10))
			}
		}
	})
	emit("slop_cache_hit_ratio", func() {
		for _, key := range t.usageKeys {
			row := t.usage[key]
			sample(&out, "slop_cache_hit_ratio", usageLabels(row),
				cacheHitRatio(row.InputTokens, row.CacheReadTokens, row.CacheWriteTokens))
		}
	})
	emit("slop_errors_total", func() {
		for _, key := range t.errorKeys {
			row := t.errors[key]
			sample(&out, "slop_errors_total", [][2]string{
				{"user", label(row.User)}, {"provider", label(row.Provider)}, {"kind", label(row.Kind)},
			}, strconv.FormatInt(row.Count, 10))
		}
	})
	emit("slop_tool_turns_total", func() {
		for _, key := range t.toolKeys {
			row := t.tools[key]
			sample(&out, "slop_tool_turns_total", [][2]string{
				{"user", label(row.User)}, {"tool", label(row.Tool)},
			}, strconv.FormatInt(row.Count, 10))
		}
	})
	emit("slop_tool_errors_total", func() {
		for _, key := range t.toolKeys {
			row := t.tools[key]
			sample(&out, "slop_tool_errors_total", [][2]string{
				{"user", label(row.User)}, {"tool", label(row.Tool)},
			}, strconv.FormatInt(row.Errors, 10))
		}
	})
	for _, counter := range insightCounters {
		c := counter
		emit(c.name, func() {
			for _, key := range t.insightKeys {
				row := t.insights[key]
				sample(&out, c.name, [][2]string{
					{"user", label(row.User)}, {"account", label(row.Account)},
					{"stop_reason", label(row.StopReason)},
				}, strconv.FormatInt(c.get(*row), 10))
			}
		})
	}
	type sessionRow struct {
		user     string
		sessions int64
		deepest  int64
		switches int64
		tokens   int64
	}
	perUser := map[string]*sessionRow{}
	// Iterating the map keys keeps (user, session) pairs together.
	for key, agg := range t.sessions {
		row := perUser[key[0]]
		if row == nil {
			row = &sessionRow{user: key[0]}
			perUser[key[0]] = row
		}
		row.sessions++
		if agg.deepest > row.deepest {
			row.deepest = agg.deepest
		}
		row.switches += int64(len(agg.accounts))
		if len(agg.accounts) > 0 {
			row.switches--
		}
		if agg.tokens > row.tokens {
			row.tokens = agg.tokens
		}
	}
	users := make([]string, 0, len(perUser))
	for u := range perUser {
		users = append(users, u)
	}
	sort.Strings(users)
	sessionGauges := []struct {
		name string
		get  func(*sessionRow) int64
	}{
		{"slop_sessions", func(r *sessionRow) int64 { return r.sessions }},
		{"slop_session_account_switches", func(r *sessionRow) int64 { return r.switches }},
		{"slop_session_tokens_max", func(r *sessionRow) int64 { return r.tokens }},
		{"slop_session_deepest_turn", func(r *sessionRow) int64 { return r.deepest }},
	}
	for _, gauge := range sessionGauges {
		g := gauge
		emit(g.name, func() {
			for _, u := range users {
				row := perUser[u]
				sample(&out, g.name, [][2]string{{"user", label(u)}}, strconv.FormatInt(g.get(row), 10))
			}
		})
	}
	return out.String()
}
