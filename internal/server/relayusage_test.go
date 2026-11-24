package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/edy/slop-proxy-go/internal/pool"
	"github.com/edy/slop-proxy-go/internal/provider"
)

// A complete Anthropic Messages stream with the four bookkeeping frames Rust's
// SseScan parses. The repeated Bash block checks tool de-duplication.
const relaySSE = "event: message_start\n" +
	`data: {"type":"message_start","message":{"usage":{"input_tokens":120,"output_tokens":1,"cache_read_input_tokens":80,"cache_creation_input_tokens":20}}}` + "\n\n" +
	"event: content_block_start\n" +
	`data: {"type":"content_block_start","content_block":{"type":"tool_use","name":"Bash","input":{}}}` + "\n\n" +
	"event: content_block_start\n" +
	`data: {"type":"content_block_start","content_block":{"type":"tool_use","name":"Bash","input":{}}}` + "\n\n" +
	"event: content_block_delta\n" +
	`data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"hello"}}` + "\n\n" +
	"event: message_delta\n" +
	`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":42}}` + "\n\n" +
	"event: message_stop\n" +
	`data: {"type":"message_stop"}` + "\n\n"

func TestRelayUsageSSEFrames(t *testing.T) {
	u := newRelayUsage()
	// Split in the middle of a data line to prove the line buffer survives
	// arbitrary chunk boundaries, just like an upstream TCP stream.
	cut := strings.Index(relaySSE, `"output_tokens":42`) + 5
	u.feed([]byte(relaySSE[:cut]))
	u.feed([]byte(relaySSE[cut:]))
	u.finish(200)
	snap := u.snapshot()

	if snap.InputTokens != 120 || snap.OutputTokens != 42 {
		t.Errorf("tokens = %d/%d, want 120/42", snap.InputTokens, snap.OutputTokens)
	}
	if snap.CacheReadTokens != 80 || snap.CacheWriteTokens != 20 {
		t.Errorf("cache = %d/%d, want 80/20", snap.CacheReadTokens, snap.CacheWriteTokens)
	}
	if !snap.Completed || !snap.UpstreamEOF {
		t.Errorf("completed/eof = %v/%v", snap.Completed, snap.UpstreamEOF)
	}
	if snap.StopReason == nil || *snap.StopReason != "end_turn" {
		t.Errorf("stop_reason = %v", snap.StopReason)
	}
	if len(snap.ToolsCalled) != 1 || snap.ToolsCalled[0] != "Bash" {
		t.Errorf("tools_called = %v, want [Bash]", snap.ToolsCalled)
	}
	if snap.ErrorKind != nil {
		t.Errorf("clean stream got error_kind %q", *snap.ErrorKind)
	}
	if snap.ResponseBytes != int64(len(relaySSE)) || snap.FirstByteAt == nil {
		t.Errorf("byte accounting = %d, first=%v", snap.ResponseBytes, snap.FirstByteAt)
	}
}

func TestRelayUsageNonStreamingBody(t *testing.T) {
	u := newRelayUsage()
	u.applyJSON([]byte(`{"id":"msg_1","stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":3,"cache_read_input_tokens":4,"cache_creation_input_tokens":1}}`))
	u.noteBytes(256)
	snap := u.snapshot()
	if snap.InputTokens != 10 || snap.OutputTokens != 3 ||
		snap.CacheReadTokens != 4 || snap.CacheWriteTokens != 1 {
		t.Errorf("json usage = %+v", snap)
	}
	if snap.ResponseBytes != 256 {
		t.Errorf("response_bytes = %d", snap.ResponseBytes)
	}
}

func TestRelayUsageFailureKinds(t *testing.T) {
	cases := []struct {
		name  string
		apply func(*relayUsage)
		want  string
	}{
		{"clean upstream eof", func(u *relayUsage) {
			u.feed([]byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":1}}}\n\n"))
			u.finish(200)
		}, "upstream_eof"},
		{"rejected status", func(u *relayUsage) { u.finish(400) }, "upstream_rejected"},
		{"client disconnect", func(u *relayUsage) { u.disconnect() }, "client_disconnect"},
		{"stream error", func(u *relayUsage) { u.streamError() }, "upstream_stream_error"},
		{"error frame", func(u *relayUsage) {
			u.feed([]byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"message\":\"overloaded\"}}\n\n"))
			u.finish(200)
		}, "upstream_error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := newRelayUsage()
			tc.apply(u)
			snap := u.snapshot()
			if snap.ErrorKind == nil || *snap.ErrorKind != tc.want {
				t.Errorf("error_kind = %v, want %q", snap.ErrorKind, tc.want)
			}
		})
	}
}

func TestPoolErrorClassification(t *testing.T) {
	cases := []struct {
		err    error
		status int
		kind   string
	}{
		{&pool.PoolError{Kind: pool.ErrNoAccounts}, http.StatusServiceUnavailable, "pool_no_accounts"},
		{&pool.PoolError{Kind: pool.ErrAllCooling, Attempts: 2}, http.StatusTooManyRequests, "pool_cooling_down"},
		{&pool.PoolError{Kind: pool.ErrBadRequest}, http.StatusBadRequest, "pool_bad_request"},
		{&pool.PoolError{Kind: pool.ErrRefused, Provider: provider.Anthropic}, http.StatusForbidden, "pool_refused"},
		{&pool.PoolError{Kind: pool.ErrUpstream}, http.StatusBadGateway, "pool_upstream"},
	}
	for _, tc := range cases {
		if got := poolErrorStatus(tc.err); got != tc.status {
			t.Errorf("%v: status = %d, want %d", tc.err, got, tc.status)
		}
		if got := poolErrorKind(tc.err); got != tc.kind {
			t.Errorf("%v: kind = %q, want %q", tc.err, got, tc.kind)
		}
	}
	if got := poolErrorAttempts(&pool.PoolError{Kind: pool.ErrAllCooling, Attempts: 3}); got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
}

func relayMessagesRequest(srv *Server, key string) *httptest.ResponseRecorder {
	body := `{"model":"claude-sonnet","max_tokens":64,"session_id":"sess-relay-1","service_tier":"standard",` +
		`"thinking":{"type":"enabled","budget_tokens":1024},` +
		`"tools":[{"name":"Bash"},{"name":"Read"}],` +
		`"messages":[{"role":"user","content":[{"type":"text","text":"hello"}]}]}`
	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/v1/messages", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("anthropic-beta", "claude-code-20250219,oauth-2025-04-20")
	req.Header.Set("user-agent", "claude-cli/2.0.0")
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)
	return rec
}

// TestRelayMessagesLogsFullUsage drives the real Router -> messages ->
// relayMessages -> pool.Execute path and proves the verbatim relay now writes
// every usage_log dimension the Prometheus families aggregate.
func TestRelayMessagesLogsFullUsage(t *testing.T) {
	var upstreamPath string
	srv, key, _ := countTokensTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		upstreamPath = r.URL.Path
		// Keep duration/ttft above the millisecond rounding floor so the
		// timing assertions are not flaky on a fast local loopback.
		time.Sleep(3 * time.Millisecond)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("request-id", "req_relay_1")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, relaySSE)
	})

	rec := relayMessagesRequest(srv, key)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != relaySSE {
		t.Error("relay rewrote the upstream SSE bytes")
	}
	if rec.Header().Get("request-id") != "req_relay_1" {
		t.Error("upstream headers were not forwarded")
	}
	if upstreamPath != "/v1/messages" {
		t.Errorf("upstream path = %q", upstreamPath)
	}

	delta, err := srv.store.MetricsSince(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(delta.Usage) != 1 {
		t.Fatalf("usage rows = %d: %+v", len(delta.Usage), delta.Usage)
	}
	u := delta.Usage[0]
	if u.Dimensions[7] != "messages" || u.Dimensions[3] != "claude-sonnet" {
		t.Errorf("dialect/requested = %q/%q", u.Dimensions[7], u.Dimensions[3])
	}
	if u.InputTokens != 120 || u.OutputTokens != 42 || u.CacheReadTokens != 80 || u.CacheWriteTokens != 20 {
		t.Errorf("token dims = %+v", u)
	}
	if u.Requests != 1 || u.Errors != 0 || u.DurationMS == 0 {
		t.Errorf("request dims = %+v", u)
	}
	if len(delta.Tools) != 1 || delta.Tools[0].Tool != "Bash" || delta.Tools[0].Count != 1 {
		t.Errorf("tool rows = %+v", delta.Tools)
	}
	if len(delta.Errors) != 0 {
		t.Errorf("clean stream logged errors: %+v", delta.Errors)
	}
	if len(delta.Insights) == 0 {
		t.Fatal("no insight rows")
	}
	ins := delta.Insights[0]
	if ins.StopReason != "end_turn" || ins.ToolsDeclared != 2 || ins.ThinkingBudget != 1024 {
		t.Errorf("insight dims = %+v", ins)
	}
	if ins.ResponseBytes != int64(len(relaySSE)) || ins.Attempts == 0 || ins.TTFTMS == 0 {
		t.Errorf("insight response dims = %+v", ins)
	}
	if len(delta.Sessions) != 1 || delta.Sessions[0].SessionKey != "sess-relay-1" {
		t.Errorf("session rows = %+v", delta.Sessions)
	}
}

func TestRelayMessagesLogsNonStreamingUsage(t *testing.T) {
	body := `{"id":"msg_1","type":"message","usage":{"input_tokens":11,"output_tokens":7,"cache_read_input_tokens":5,"cache_creation_input_tokens":2}}`
	srv, key, _ := countTokensTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, body)
	})
	rec := relayMessagesRequest(srv, key)
	if rec.Code != http.StatusOK || rec.Body.String() != body {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	delta, err := srv.store.MetricsSince(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(delta.Usage) != 1 {
		t.Fatalf("usage rows = %d", len(delta.Usage))
	}
	u := delta.Usage[0]
	if u.InputTokens != 11 || u.OutputTokens != 7 || u.CacheReadTokens != 5 || u.CacheWriteTokens != 2 {
		t.Errorf("json token dims = %+v", u)
	}
	if len(delta.Insights) == 0 || delta.Insights[0].ResponseBytes != int64(len(body)) {
		t.Errorf("response bytes = %+v", delta.Insights)
	}
}

// TestRelayMessagesLogsUpstreamRejection proves a non-2xx verbatim relay
// still writes a usage_log row (Rust's forward() logs it unconditionally) and
// that the upstream body is passed through unchanged.
func TestRelayMessagesLogsUpstreamRejection(t *testing.T) {
	errBody := `{"type":"error","error":{"type":"invalid_request_error","message":"bad relay"}}`
	srv, key, _ := countTokensTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, errBody)
	})
	rec := relayMessagesRequest(srv, key)
	if rec.Code != http.StatusBadRequest || rec.Body.String() != errBody {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	delta, err := srv.store.MetricsSince(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(delta.Usage) != 1 || delta.Usage[0].InputTokens != 0 {
		t.Errorf("rejected request missing its forced usage row: %+v", delta.Usage)
	}
	if len(delta.Errors) != 1 || delta.Errors[0].Kind != "upstream_error" {
		t.Errorf("error rows = %+v, want upstream_error", delta.Errors)
	}
}

// TestRelayMessagesLogsPoolFailure covers the dispatch_failed branch: a 401
// drains the only account and the pool returns ErrUpstream.
func TestRelayMessagesLogsPoolFailure(t *testing.T) {
	srv, key, _ := countTokensTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"type":"error","error":{"message":"key rejected"}}`)
	})
	rec := relayMessagesRequest(srv, key)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	delta, err := srv.store.MetricsSince(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(delta.Usage) != 1 {
		t.Fatalf("pool failure missing its forced usage row: %+v", delta.Usage)
	}
	if len(delta.Errors) != 1 || delta.Errors[0].Kind != "pool_upstream" {
		t.Errorf("error rows = %+v, want pool_upstream", delta.Errors)
	}
}

func TestRelayRateLimitHeaders(t *testing.T) {
	got := relayRateLimitHeaders([]pool.UsageWindow{
		{Name: "5h", Utilization: 0.1234, ResetsAt: 200},
		{Name: "7d", Utilization: 0.5, ResetsAt: 100},
	})
	checks := map[string]string{
		"anthropic-ratelimit-unified-5h-status":      "allowed",
		"anthropic-ratelimit-unified-5h-utilization": "0.12",
		"anthropic-ratelimit-unified-5h-reset":       "200",
		"anthropic-ratelimit-unified-7d-status":      "allowed",
		"anthropic-ratelimit-unified-7d-utilization": "0.50",
		"anthropic-ratelimit-unified-status":         "allowed",
		"anthropic-ratelimit-unified-reset":          "100",
	}
	for name, want := range checks {
		if v := got.Get(name); v != want {
			t.Errorf("%s = %q, want %q", name, v, want)
		}
	}
	if len(relayRateLimitHeaders(nil)) != 0 {
		t.Error("no windows must produce no headers")
	}
}

func TestCopyRelayHeadersWhitelist(t *testing.T) {
	rec := httptest.NewRecorder()
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("Request-Id", "req_1")
	h.Set("Retry-After", "7")
	h.Set("Anthropic-Version", "2023-06-01")
	h.Set("Anthropic-Ratelimit-Unified-5h-Utilization", "0.99")
	h.Set("X-Unrelated", "drop-me")
	h.Set("Content-Length", "99")
	copyRelayHeaders(rec, h)
	for _, want := range []string{"Content-Type", "Request-Id", "Retry-After", "Anthropic-Version"} {
		if rec.Header().Get(want) == "" {
			t.Errorf("whitelisted header %q was dropped", want)
		}
	}
	for _, banned := range []string{"X-Unrelated", "Content-Length", "Anthropic-Ratelimit-Unified-5h-Utilization"} {
		if rec.Header().Get(banned) != "" {
			t.Errorf("header %q leaked through the relay", banned)
		}
	}
}

// TestRelayMessagesRefusesNonClaudeCode covers the subscription gate that
// Rust applies before dispatch, including the rejected usage_log row.
func TestRelayMessagesRefusesNonClaudeCode(t *testing.T) {
	srv, key, _ := countTokensTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("upstream must not be contacted for a refused request")
		w.WriteHeader(http.StatusOK)
	})
	body := `{"model":"claude-sonnet","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/v1/messages", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+key)
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "only cover Claude Code") {
		t.Errorf("wrong error envelope: %s", rec.Body.String())
	}
	delta, err := srv.store.MetricsSince(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(delta.Usage) != 1 {
		t.Fatalf("refused request missing its forced usage row: %+v", delta.Usage)
	}
	if len(delta.Errors) != 1 || delta.Errors[0].Kind != "not_claude_code" {
		t.Errorf("error rows = %+v, want not_claude_code", delta.Errors)
	}
}
