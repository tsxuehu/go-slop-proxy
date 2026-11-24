package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/edy/slop-proxy-go/internal/backend"
	"github.com/edy/slop-proxy-go/internal/config"
	"github.com/edy/slop-proxy-go/internal/oauth"
	"github.com/edy/slop-proxy-go/internal/pool"
	"github.com/edy/slop-proxy-go/internal/provider"
	"github.com/edy/slop-proxy-go/internal/store"
)

func TestIsClaudeCode(t *testing.T) {
	h := http.Header{}
	if isClaudeCode(h) {
		t.Fatal("empty headers must not pass the Claude Code gate")
	}
	h.Set("anthropic-beta", "claude-code-20250219")
	if isClaudeCode(h) {
		t.Fatal("beta alone must not pass")
	}
	h.Set("user-agent", "claude-cli/2.0.0")
	if !isClaudeCode(h) {
		t.Fatal("both markers must pass")
	}
	h.Set("user-agent", "curl/8.0")
	if isClaudeCode(h) {
		t.Fatal("a non-Claude CLI user agent must not pass")
	}
}

func TestNormalizedCountBody(t *testing.T) {
	body := []byte(`{"model":"claude-sonnet","max_tokens":16,"messages":[{"role":"user","content":"hi"}],"extra":{"keep":true}}`)
	if same := normalizedCountBody(body, "claude-sonnet", "claude-sonnet"); string(same) != string(body) {
		t.Fatalf("unchanged model should keep the body verbatim, got %s", same)
	}
	out := normalizedCountBody(body, "claude-sonnet", "claude-sonnet-4-5")
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(out, &obj); err != nil {
		t.Fatalf("rewritten body is not JSON: %v", err)
	}
	var model string
	_ = json.Unmarshal(obj["model"], &model)
	if model != "claude-sonnet-4-5" {
		t.Errorf("upstream model = %q", model)
	}
	for _, key := range []string{"max_tokens", "messages", "extra"} {
		if _, ok := obj[key]; !ok {
			t.Errorf("rewritten body dropped %q", key)
		}
	}
	bad := []byte(`not json`)
	if got := normalizedCountBody(bad, "a", "b"); string(got) != string(bad) {
		t.Errorf("malformed JSON should be returned unchanged, got %s", got)
	}
}

func TestForwardAnthropicHeaders(t *testing.T) {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("request-id", "req_cnt_1")
	h.Set("retry-after", "7")
	h.Set("anthropic-version", "2023-06-01")
	h.Set("anthropic-ratelimit-unified-status", "allowed")
	h.Set("x-unrelated", "drop-me")
	rec := httptest.NewRecorder()
	forwardAnthropicHeaders(rec, h)
	for _, want := range []string{"Content-Type", "Request-Id", "Retry-After", "Anthropic-Version"} {
		if rec.Header().Get(want) == "" {
			t.Errorf("header %q was not forwarded", want)
		}
	}
	if rec.Header().Get("anthropic-ratelimit-unified-status") != "" {
		t.Error("rate-limit headers must not be forwarded")
	}
	if rec.Header().Get("x-unrelated") != "" {
		t.Error("unrelated upstream headers must not be forwarded")
	}
}

// countTokensTestServer wires a single Anthropic account backed by a mock
// upstream, enough to drive the real Router -> countTokens -> pool.Execute path.
func countTokensTestServer(t *testing.T, upstream http.HandlerFunc) (*Server, string, *httptest.Server) {
	t.Helper()
	up := httptest.NewServer(upstream)
	t.Cleanup(up.Close)

	cfg := config.Default()
	cfg.Anthropic.BaseURL = up.URL
	cfg.Anthropic.RequireClaudeCode = true
	cfg.Models.AnthropicPatterns = []string{"claude-*"}
	cfg.Models.Aliases = map[string]config.ModelAlias{
		"claude-sonnet": {Model: "claude-sonnet-4-5"},
	}

	st := newSmokeStore(t)
	ctx := context.Background()
	key, _ := store.GenerateToken()
	if _, err := st.CreateToken(ctx, key, "alice", store.TokenLimits{}); err != nil {
		t.Fatal(err)
	}
	acc := &store.Account{
		Provider: provider.Anthropic.String(), ProviderAccountID: "acc-1",
		Label: "test-anthropic", Status: "active", AuthMode: "api_key",
		AccessToken: "sk-upstream",
	}
	if err := st.InsertAccount(ctx, acc); err != nil {
		t.Fatal(err)
	}
	slots := pool.NewSlots(provider.Anthropic, st, oauth.NewCodexRefresher())
	if err := slots.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	client, err := backend.NewAnthropic(cfg.Anthropic)
	if err != nil {
		t.Fatal(err)
	}
	pl := pool.NewPool(slots, backend.NewAnthropicBackend(client, cfg.Anthropic.SoftUtilizationLimit))
	srv := &Server{
		cfg: cfg, store: st,
		pools:    map[provider.Provider]*pool.Pool{provider.Anthropic: pl},
		catalogs: map[provider.Provider][]string{},
	}
	return srv, key, up
}

func countTokensRequest(srv *Server, key string, claudeCode bool) *httptest.ResponseRecorder {
	body := `{"model":"claude-sonnet","max_tokens":16,"messages":[{"role":"user","content":"hello"}]}`
	req := httptest.NewRequest(http.MethodPost, "http://proxy.local/v1/messages/count_tokens", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("anthropic-version", "2023-06-01")
	if claudeCode {
		req.Header.Set("anthropic-beta", "claude-code-20250219,oauth-2025-04-20")
		req.Header.Set("user-agent", "claude-cli/2.0.0")
	}
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)
	return rec
}

// TestCountTokensRelaysAnthropicUpstream proves the Anthropic route is a real
// upstream relay (not the local estimator): path, normalized model, upstream
// headers and the upstream response body all pass through verbatim.
func TestCountTokensRelaysAnthropicUpstream(t *testing.T) {
	var got struct {
		path, body, beta, ua, version, key string
	}
	srv, key, _ := countTokensTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got.path, got.body = r.URL.Path, string(b)
		got.beta = r.Header.Get("anthropic-beta")
		got.ua = r.Header.Get("user-agent")
		got.version = r.Header.Get("anthropic-version")
		got.key = r.Header.Get("x-api-key")
		w.Header().Set("request-id", "req_cnt_1")
		w.Header().Set("retry-after", "7")
		w.Header().Set("anthropic-ratelimit-unified-status", "allowed")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"input_tokens":42}`)
	})

	rec := countTokensRequest(srv, key, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"input_tokens":42`) {
		t.Errorf("upstream count was not relayed: %s", rec.Body.String())
	}
	if got.path != "/v1/messages/count_tokens" {
		t.Errorf("upstream path = %q", got.path)
	}
	var sent map[string]json.RawMessage
	if err := json.Unmarshal([]byte(got.body), &sent); err != nil {
		t.Fatalf("upstream body not JSON: %v", err)
	}
	var model string
	_ = json.Unmarshal(sent["model"], &model)
	if model != "claude-sonnet-4-5" {
		t.Errorf("upstream model = %q, want alias-resolved model", model)
	}
	if _, ok := sent["max_tokens"]; !ok {
		t.Error("request body lost max_tokens")
	}
	if got.beta != "claude-code-20250219,oauth-2025-04-20" || got.ua != "claude-cli/2.0.0" || got.version != "2023-06-01" {
		t.Errorf("upstream headers: beta=%q ua=%q version=%q", got.beta, got.ua, got.version)
	}
	if got.key != "sk-upstream" {
		t.Errorf("upstream key = %q", got.key)
	}
	if rec.Header().Get("request-id") != "req_cnt_1" || rec.Header().Get("retry-after") != "7" {
		t.Errorf("relayed headers missing: %v", rec.Header())
	}
	if rec.Header().Get("anthropic-ratelimit-unified-status") != "" {
		t.Error("rate-limit header should have been stripped")
	}
}

// TestCountTokensRequiresClaudeCode enforces the subscription gate.
func TestCountTokensRequiresClaudeCode(t *testing.T) {
	calls := 0
	srv, key, _ := countTokensTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"input_tokens":1}`)
	})
	rec := countTokensRequest(srv, key, false)
	if rec.Code != http.StatusForbidden {
		t.Errorf("status %d body %s", rec.Code, rec.Body.String())
	}
	if calls != 0 {
		t.Errorf("upstream was called %d times despite the gate", calls)
	}
}

// TestCountTokensDoesNotFallBackToEstimator: an upstream error is relayed
// verbatim, never replaced by a local token estimate.
func TestCountTokensDoesNotFallBackToEstimator(t *testing.T) {
	srv, key, _ := countTokensTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("request-id", "req_err_1")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"type":"error","error":{"type":"invalid_request_error","message":"bad count request"}}`)
	})
	rec := countTokensRequest(srv, key, true)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "bad count request") {
		t.Errorf("upstream error body not relayed: %s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "input_tokens") {
		t.Errorf("fell back to the local estimator: %s", rec.Body.String())
	}
	if rec.Header().Get("request-id") != "req_err_1" {
		t.Errorf("upstream error headers not relayed: %v", rec.Header())
	}
}
