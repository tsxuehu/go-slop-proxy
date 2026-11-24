package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/edy/slop-proxy-go/internal/backend"
	"github.com/edy/slop-proxy-go/internal/config"
	"github.com/edy/slop-proxy-go/internal/oauth"
	"github.com/edy/slop-proxy-go/internal/pool"
	"github.com/edy/slop-proxy-go/internal/provider"
	"github.com/edy/slop-proxy-go/internal/store"
)

// A Responses SSE stream whose terminal frame carries usage and whose output
// item names a tool. The bytes are relayed verbatim; the observer only reads.
const responsesSSE = "event: response.output_item.done\n" +
	`data: {"type":"response.output_item.done","item":{"type":"function_call","name":"shell"}}` + "\n\n" +
	"event: response.completed\n" +
	`data: {"type":"response.completed","response":{"id":"resp_1","status":"completed","usage":{"input_tokens":100,"output_tokens":20,"input_tokens_details":{"cached_tokens":40},"output_tokens_details":{"reasoning_tokens":8}}}}` + "\n\n"

func responsesTestServer(t *testing.T, upstream http.HandlerFunc) (*Server, string) {
	t.Helper()
	up := httptest.NewServer(upstream)
	t.Cleanup(up.Close)
	st := newSmokeStore(t)
	ctx := context.Background()
	key, _ := store.GenerateToken()
	if _, err := st.CreateToken(ctx, key, "alice", store.TokenLimits{}); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertAccount(ctx, &store.Account{
		Provider: "openai", ProviderAccountID: "acct-1", Label: "test@example.com",
		AuthMode: "oauth", AccessToken: "at", RefreshToken: "rt",
		AccessExpiresAt: time.Now().Unix() + 3600, Status: "active",
	}); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Codex.BaseURL = up.URL + "/backend-api/codex"
	be := backend.NewCodex(cfg.Codex)
	slots := pool.NewSlots(provider.OpenAI, st, oauth.NewCodexRefresher())
	if err := slots.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	pl := pool.NewPool(slots, be)
	srv := &Server{
		cfg: cfg, store: st, backend: be,
		pools:    map[provider.Provider]*pool.Pool{provider.OpenAI: pl},
		catalogs: map[provider.Provider][]string{},
	}
	return srv, key
}

func responsesRequest(srv *Server, key string) *httptest.ResponseRecorder {
	body := `{"model":"gpt-5","stream":true,"session_id":"sess-resp-1","reasoning":{"effort":"high"},` +
		`"tools":[{"type":"function","name":"shell"}],` +
		`"input":[{"role":"user","content":"hello"}]}`
	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/v1/responses", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+key)
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)
	return rec
}

func TestResponsesRateLimitHeaders(t *testing.T) {
	got := responsesRateLimitHeaders([]pool.UsageWindow{
		{Name: "7d", Utilization: 0.42, ResetsAt: 200},
		{Name: "5h", Utilization: 0.105, ResetsAt: 100},
	})
	checks := map[string]string{
		"x-codex-primary-window-minutes":   "300",
		"x-codex-primary-used-percent":     "11",
		"x-codex-primary-reset-at":         "100",
		"x-codex-secondary-window-minutes": "10080",
		"x-codex-secondary-used-percent":   "42",
		"x-codex-secondary-reset-at":       "200",
	}
	for name, want := range checks {
		if v := got.Get(name); v != want {
			t.Errorf("%s = %q, want %q", name, v, want)
		}
	}
	if len(responsesRateLimitHeaders(nil)) != 0 {
		t.Error("no windows must produce no headers")
	}
}

func TestResponsesUsageObserver(t *testing.T) {
	u := newResponsesUsage()
	cut := strings.Index(responsesSSE, `"input_tokens":100`) + 6
	u.feed([]byte(responsesSSE[:cut]))
	u.feed([]byte(responsesSSE[cut:]))
	u.finish(200)
	snap := u.snapshot()
	if snap.InputTokens != 60 || snap.OutputTokens != 20 ||
		snap.CacheReadTokens != 40 || snap.ReasoningTokens != 8 {
		t.Errorf("usage = %+v, want fresh=60 out=20 cached=40 reasoning=8", snap)
	}
	if !snap.Completed || snap.StopReason == nil || !snap.UpstreamEOF {
		t.Errorf("terminal/eof = %v/%v/%v", snap.Completed, snap.StopReason, snap.UpstreamEOF)
	}
	if len(snap.ToolsCalled) != 1 || snap.ToolsCalled[0] != "shell" {
		t.Errorf("tools = %v", snap.ToolsCalled)
	}
	if snap.ErrorKind != nil || snap.ResponseBytes != int64(len(responsesSSE)) {
		t.Errorf("clean stream = %+v", snap)
	}
}

func TestResponsesUsageFailureKinds(t *testing.T) {
	cases := []struct {
		name  string
		apply func(*responsesUsage)
		want  string
	}{
		{"clean upstream eof", func(u *responsesUsage) {
			u.feed([]byte("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{}}\n\n"))
			u.finish(200)
		}, "upstream_eof"},
		{"rejected status", func(u *responsesUsage) { u.finish(429) }, "upstream_rejected"},
		{"client disconnect", func(u *responsesUsage) { u.disconnect() }, "client_disconnect"},
		{"stream error", func(u *responsesUsage) { u.streamError() }, "upstream_sse_error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := newResponsesUsage()
			tc.apply(u)
			snap := u.snapshot()
			if snap.ErrorKind == nil || *snap.ErrorKind != tc.want {
				t.Errorf("error_kind = %v, want %q", snap.ErrorKind, tc.want)
			}
		})
	}
}

// TestResponsesPassthroughLogsUsage proves POST /v1/responses now behaves like
// Rust's responses_passthrough: bytes are untouched, but a full usage_log row
// with tool/insight/session dimensions is written for the turn.
func TestResponsesPassthroughLogsUsage(t *testing.T) {
	var upstreamPath string
	srv, key := responsesTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		upstreamPath = r.URL.Path
		time.Sleep(3 * time.Millisecond)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, responsesSSE)
	})
	rec := responsesRequest(srv, key)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != responsesSSE {
		t.Error("passthrough rewrote the upstream SSE bytes")
	}
	if upstreamPath != "/backend-api/codex/responses" {
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
	if u.Dimensions[7] != "responses" {
		t.Errorf("dialect = %q, want responses", u.Dimensions[7])
	}
	if u.InputTokens != 60 || u.OutputTokens != 20 || u.CacheReadTokens != 40 {
		t.Errorf("token dims = %+v", u)
	}
	if u.Requests != 1 || u.Errors != 0 || u.DurationMS == 0 {
		t.Errorf("request dims = %+v", u)
	}
	if len(delta.Tools) != 1 || delta.Tools[0].Tool != "shell" {
		t.Errorf("tool rows = %+v", delta.Tools)
	}
	if len(delta.Errors) != 0 {
		t.Errorf("clean stream logged errors: %+v", delta.Errors)
	}
	if len(delta.Insights) == 0 {
		t.Fatal("no insight rows")
	}
	ins := delta.Insights[0]
	if ins.StopReason == "" || ins.ResponseBytes != int64(len(responsesSSE)) ||
		ins.Attempts == 0 || ins.TTFTMS == 0 || ins.ToolsDeclared != 1 {
		t.Errorf("insight dims = %+v", ins)
	}
	if len(delta.Sessions) != 1 || delta.Sessions[0].SessionKey != "sess-resp-1" {
		t.Errorf("session rows = %+v", delta.Sessions)
	}
}

func TestResponsesPassthroughLogsNonStreamingAndRejection(t *testing.T) {
	t.Run("non-streaming json", func(t *testing.T) {
		body := `{"type":"response.completed","response":{"usage":{"input_tokens":12,"output_tokens":5}}}`
		srv, key := responsesTestServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, body)
		})
		rec := responsesRequest(srv, key)
		if rec.Code != http.StatusOK || rec.Body.String() != body {
			t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
		}
		delta, err := srv.store.MetricsSince(context.Background(), 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(delta.Usage) != 1 || delta.Usage[0].InputTokens != 12 || delta.Usage[0].OutputTokens != 5 {
			t.Errorf("json usage = %+v", delta.Usage)
		}
	})
	t.Run("upstream rejection", func(t *testing.T) {
		// Rust's Codex RULES also use Classify::STRICT (pass: |_| false), so a
		// 4xx never reaches pipeline::forward as a response: it becomes a
		// PoolError::BadRequest, logged with kind pool_bad_request.
		srv, key := responsesTestServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":{"message":"bad responses request"}}`)
		})
		rec := responsesRequest(srv, key)
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "bad responses request") {
			t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
		}
		delta, err := srv.store.MetricsSince(context.Background(), 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(delta.Usage) != 1 {
			t.Fatalf("rejected request missing its forced usage row: %+v", delta.Usage)
		}
		if len(delta.Errors) != 1 || delta.Errors[0].Kind != "pool_bad_request" {
			t.Errorf("error rows = %+v, want pool_bad_request", delta.Errors)
		}
	})
}
