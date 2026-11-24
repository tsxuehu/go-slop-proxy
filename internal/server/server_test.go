package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/edy/slop-proxy-go/internal/config"
	"github.com/edy/slop-proxy-go/internal/pool"
	"github.com/edy/slop-proxy-go/internal/provider"
	"github.com/edy/slop-proxy-go/internal/store"
)

func newSmokeStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir() + "/smoke.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// TestRouterRegistersWithoutPanic is the regression guard for ServeMux
// wildcard conflicts. Router() registers every pattern at call time, and two
// competing {wild} patterns panic during registration (i.e. at server startup),
// NOT at build time -- so a plain `go build`/`go test` that never calls
// Router() would miss it entirely.
func TestRouterRegistersWithoutPanic(t *testing.T) {
	if h := (&Server{}).Router(); h == nil {
		t.Fatal("Router returned a nil handler")
	}
}

// TestMatchDesktopStatic verifies the in-handler segment matching used for
// static reply keys that contain a {wild} segment.
func TestMatchDesktopStatic(t *testing.T) {
	cases := []struct {
		path     string
		wantOK   bool
		contains string
	}{
		{"/backend-api/accounts/xyz/settings", true, "beta_settings"},
		{"/backend-api/ps/plugin-categories/foo/plugins", true, "pagination"},
		{"/backend-api/accounts/xyz/settings/extra", false, ""},     // wrong segment count
		{"/backend-api/settings/user", true, "custom_instructions"}, // exact key
	}
	for _, c := range cases {
		body, ok := matchDesktopStatic(desktopReads, c.path)
		if ok != c.wantOK {
			t.Errorf("%s: ok=%v want %v", c.path, ok, c.wantOK)
			continue
		}
		if c.contains != "" && !strings.Contains(body, c.contains) {
			t.Errorf("%s: body %q missing %q", c.path, body, c.contains)
		}
	}
}

// TestDesktopAndConfigRoutes exercises real dispatch through the assembled
// mux (with auth), including a wildcard static path and the unknown-probe
// fallback, to confirm the subtree handler coexists with the remaining
// ServeMux {wild} patterns.
func TestDesktopAndConfigRoutes(t *testing.T) {
	st := newSmokeStore(t)
	key, _ := store.GenerateToken()
	if _, err := st.CreateToken(context.Background(), key, "smoke", store.TokenLimits{}); err != nil {
		t.Fatal(err)
	}
	srv := &Server{cfg: config.Default(), store: st}
	h := srv.Router()

	cases := []struct {
		path, contains string
	}{
		{"/config/codex/config.toml", "chatgpt_base_url"},
		{"/config/codex/auth.json", "tokens"},
		{"/backend-api/me", ""},
		{"/v1/models", "gpt-5"},
		{"/backend-api/accounts/abc123/settings", "beta_settings"},
		{"/backend-api/ps/plugin-categories/foo/plugins", "pagination"},
		{"/backend-api/accounts/check/v1", ""},
		{"/backend-api/unknown/probe", "{}"},
		{"/backend-api/wham/usage", ""},
	}
	for _, c := range cases {
		req := httptest.NewRequest(http.MethodGet, "http://proxy.local"+c.path, nil)
		req.Header.Set("Authorization", "Bearer "+key)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("%s: status %d, body %s", c.path, rec.Code, rec.Body.String())
			continue
		}
		if c.contains != "" && !strings.Contains(rec.Body.String(), c.contains) {
			t.Errorf("%s: body %q missing %q", c.path, rec.Body.String(), c.contains)
		}
	}
}

// TestMetricsAlwaysDeclaresFamilies verifies an empty scrape still emits
// HELP/TYPE for every family (the Rust original does this unconditionally).
func TestMetricsAlwaysDeclaresFamilies(t *testing.T) {
	st := newSmokeStore(t)
	srv := &Server{store: st}
	rec := httptest.NewRecorder()
	srv.serveMetrics(rec, httptest.NewRequest(http.MethodGet, "http://metrics.local/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	for _, want := range []string{
		"# HELP slop_requests_total",
		"# TYPE slop_requests_total counter",
		"# TYPE slop_account_status gauge",
		"# TYPE slop_tokens_total counter",
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("empty-scrape metrics missing %q", want)
		}
	}
}

// TestInfraRoutes exercises the endpoints added for the Codex/Gemini client
// surfaces, including the 426 websocket fallback contract.
func TestInfraRoutes(t *testing.T) {
	st := newSmokeStore(t)
	key, _ := store.GenerateToken()
	if _, err := st.CreateToken(context.Background(), key, "smoke", store.TokenLimits{}); err != nil {
		t.Fatal(err)
	}
	srv := &Server{
		cfg: config.Default(), store: st,
		pools: map[provider.Provider]*pool.Pool{}, catalogs: map[provider.Provider][]string{},
	}
	h := srv.Router()

	get := func(path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "http://proxy.local"+path, nil)
		req.Header.Set("Authorization", "Bearer "+key)
		h.ServeHTTP(rec, req)
		return rec
	}
	post := func(path, body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "http://proxy.local"+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+key)
		h.ServeHTTP(rec, req)
		return rec
	}

	if rec := get("/v1/responses"); rec.Code != http.StatusUpgradeRequired {
		t.Errorf("GET /v1/responses: want 426, got %d (%s)", rec.Code, rec.Body.String())
	}
	if rec := get("/v1beta/models"); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"object":"list"`) {
		t.Errorf("GET /v1beta/models: %d %s", rec.Code, rec.Body.String())
	}
	if rec := post("/v1/messages/count_tokens", `{"model":"gpt-5","max_tokens":16,"messages":[{"role":"user","content":"hello"}]}`); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "input_tokens") {
		t.Errorf("count_tokens: %d %s", rec.Code, rec.Body.String())
	}
	// A model not routed to Gemini is rejected before any upstream call.
	if rec := post("/v1beta/models/gpt-5:generateContent", `{"contents":[]}`); rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "gemini backend") {
		t.Errorf("non-gemini native: %d %s", rec.Code, rec.Body.String())
	}
	// Malformed spec is a 404, not a 500/panic.
	if rec := post("/v1beta/models/nocolon", `{}`); rec.Code != http.StatusNotFound {
		t.Errorf("bad native spec: %d %s", rec.Code, rec.Body.String())
	}
	// alpha/search must be routed (no accounts -> 503), never a 404.
	if rec := post("/v1/alpha/search", `{"model":"gpt-5","query":"x"}`); rec.Code == http.StatusNotFound {
		t.Errorf("alpha/search fell through to 404: %s", rec.Body.String())
	}
}

func TestScanNativeUsage(t *testing.T) {
	jsonBody := []byte(`{"usageMetadata":{"promptTokenCount":12,"candidatesTokenCount":7}}`)
	if in, out := scanNativeUsage(jsonBody); in != 12 || out != 7 {
		t.Errorf("json usage = %d/%d", in, out)
	}
	sse := []byte("data: {\"usageMetadata\":{\"promptTokenCount\":1,\"candidatesTokenCount\":2}}\n\n" +
		"data: {\"usageMetadata\":{\"promptTokenCount\":30,\"candidatesTokenCount\":9}}\n\n")
	if in, out := scanNativeUsage(sse); in != 30 || out != 9 {
		t.Errorf("sse usage = %d/%d (want last frame)", in, out)
	}
	if in, out := scanNativeUsage([]byte("no usage here")); in != 0 || out != 0 {
		t.Errorf("empty usage = %d/%d", in, out)
	}
}

// TestMetricsEmitsUsageSamples verifies recorded usage becomes series.
func TestMetricsEmitsUsageSamples(t *testing.T) {
	st := newSmokeStore(t)
	if err := st.LogUsage(context.Background(), store.UsageLog{
		User: "alice", Provider: "openai", UpstreamModel: "gpt-5",
		InputTokens: 12, OutputTokens: 4, Status: 200,
	}); err != nil {
		t.Fatal(err)
	}
	srv := &Server{store: st}
	rec := httptest.NewRecorder()
	srv.serveMetrics(rec, httptest.NewRequest(http.MethodGet, "http://metrics.local/metrics", nil))
	body := rec.Body.String()
	if !strings.Contains(body, `slop_requests_total{user="alice"`) {
		t.Errorf("missing request sample: %s", body)
	}
	if !strings.Contains(body, `slop_tokens_total{`) {
		t.Errorf("missing token samples: %s", body)
	}
}
