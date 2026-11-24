package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/edy/slop-proxy-go/internal/backend"
	"github.com/edy/slop-proxy-go/internal/config"
	"github.com/edy/slop-proxy-go/internal/oauth"
	"github.com/edy/slop-proxy-go/internal/pool"
	"github.com/edy/slop-proxy-go/internal/provider"
	"github.com/edy/slop-proxy-go/internal/store"
	"github.com/edy/slop-proxy-go/internal/upstream"
)

func requestJSON(handler http.Handler, method, path, key, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, "http://proxy.local"+path, strings.NewReader(body))
	if key != "" {
		request.Header.Set("Authorization", "Bearer "+key)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func TestAdminAuthentication(t *testing.T) {
	cfg := config.Default()
	srv := &Server{cfg: cfg, store: newSmokeStore(t)}
	if reply := requestJSON(srv.Router(), "GET", "/admin/tokens", "", ""); reply.Code != 404 {
		t.Fatalf("admin API without config status = %d", reply.Code)
	}
	srv.cfg.AdminToken = "test-admin"
	handler := srv.Router()
	key, _ := store.GenerateToken()
	if _, err := srv.store.CreateToken(context.Background(), key, "client", store.TokenLimits{}); err != nil {
		t.Fatal(err)
	}
	for _, credential := range []string{"", key} {
		reply := requestJSON(handler, "GET", "/admin/tokens", credential, "")
		if reply.Code != 401 {
			t.Fatalf("admin API without admin credential status = %d", reply.Code)
		}
	}
	reply := requestJSON(handler, "POST", "/admin/token", srv.cfg.AdminToken, `{"user":"alice"}`)
	if reply.Code != 200 || !strings.Contains(reply.Body.String(), "alice") {
		t.Fatalf("admin API with valid credential = %d %s", reply.Code, reply.Body.String())
	}
}

func TestResponsesReturnsTerminalJSON(t *testing.T) {
	srv, key := responsesTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Stream bool `json:"stream"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil || !input.Stream {
			t.Errorf("upstream Responses must stream, request = %+v, error = %v", input, err)
		}
		if r.Header.Get("X-Api-Key") != "" {
			t.Error("proxy key was forwarded to Codex")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, responsesSSE)
	})
	req := httptest.NewRequest("POST", "http://proxy.local/v1/responses",
		strings.NewReader(`{"model":"gpt-5","input":"hello","stream":false}`))
	req.Header.Set("X-Api-Key", key)
	response := httptest.NewRecorder()
	srv.Router().ServeHTTP(response, req)
	var final struct{ ID string }
	err := json.Unmarshal(response.Body.Bytes(), &final)
	if response.Code != 200 || response.Header().Get("Content-Type") != "application/json" || err != nil || final.ID != "resp_1" {
		t.Fatalf("non-stream response = %d %s, %v", response.Code, response.Body.String(), err)
	}
}

func TestTranslatedStreamsEmitBeforeUpstreamCloses(t *testing.T) {
	cases := []struct{ path, request string }{
		{"/v1/chat/completions", `{"model":"gpt-5","stream":true,"messages":[{"role":"user","content":"hi"}]}`},
		{"/v1/messages", `{"model":"gpt-5","stream":true,"max_tokens":32,"messages":[{"role":"user","content":"hi"}]}`},
	}
	for _, item := range cases {
		t.Run(item.path, func(t *testing.T) {
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			srv, key := responsesTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n")
				w.(http.Flusher).Flush()
				<-release
				_, _ = io.WriteString(w, responsesSSE)
			})
			proxy := httptest.NewServer(srv.Router())
			defer proxy.Close()
			defer unblock()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, "POST", proxy.URL+item.path, strings.NewReader(item.request))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer "+key)
			reply, err := proxy.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer reply.Body.Close()
			scanner := bufio.NewScanner(reply.Body)
			for scanner.Scan() {
				if strings.Contains(scanner.Text(), "partial") {
					unblock()
					_, _ = io.Copy(io.Discard, reply.Body)
					return
				}
			}
			t.Fatalf("missing partial output before upstream close: %v", scanner.Err())
		})
	}
}

func TestProviderScopeAndQuota(t *testing.T) {
	srv, _ := responsesTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("rejected request reached the upstream")
	})
	ctx := context.Background()
	create := func(limits store.TokenLimits) (string, store.Token) {
		key, _ := store.GenerateToken()
		token, err := srv.store.CreateToken(ctx, key, "alice", limits)
		if err != nil {
			t.Fatal(err)
		}
		return key, token
	}
	srv.cfg.Models.Aliases["shortcut"] = config.ModelAlias{Model: "gemini-test"}
	scoped, _ := create(store.TokenLimits{Providers: []string{"openai"}})
	for _, path := range []string{"/v1/responses", "/v1/chat/completions", "/v1/messages", "/v1/messages/count_tokens"} {
		rec := requestJSON(srv.Router(), "POST", path, scoped, `{"model":"shortcut:high","input":[],"messages":[],"max_tokens":32}`)
		if rec.Code != 403 {
			t.Errorf("%s model alias scope = %d %s", path, rec.Code, rec.Body.String())
		}
	}
	key, token := create(store.TokenLimits{Tokens: 1, Metered: []string{"openai"}})
	seed, err := srv.store.Admit(ctx, token.ID, store.TokenLimits{WindowSecs: 3600})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.store.RecordUsageForMeter(ctx, seed.MeterID, 2, 0); err != nil {
		t.Fatal(err)
	}
	rec := requestJSON(srv.Router(), "POST", "/v1/responses", key, `{"model":"gpt-5","input":"hi"}`)
	if rec.Code != 429 || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("metered route = %d %s", rec.Code, rec.Body.String())
	}
	unmetered := requestJSON(srv.Router(), "POST", "/v1/messages/count_tokens", key,
		`{"model":"gemini-test","messages":[{"role":"user","content":"hi"}],"max_tokens":16}`)
	if unmetered.Code != 200 {
		t.Fatalf("unmetered route should stay usable = %d %s", unmetered.Code, unmetered.Body.String())
	}
}

func TestGeminiBridge(t *testing.T) {
	for _, referer := range []string{"", "https://example.invalid/"} {
		t.Run(fmt.Sprintf("native=%v", referer != ""), func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				if referer == "" {
					if r.URL.Path != "/openai/chat/completions" {
						t.Errorf("compatible upstream path = %s", r.URL.Path)
					}
					_, _ = io.WriteString(w, "data: {\"id\":\"chat-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"from gemini\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":2}}\n\n")
				} else {
					if r.URL.Path != "/models/gemini-test:streamGenerateContent" || r.Header.Get("Referer") != referer {
						t.Errorf("native upstream path = %s referer = %s", r.URL.Path, r.Header.Get("Referer"))
					}
					_, _ = io.WriteString(w, "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"from gemini\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":4,\"candidatesTokenCount\":2}}\n\n")
				}
			}))
			defer up.Close()
			ctx := context.Background()
			st := newSmokeStore(t)
			key, _ := store.GenerateToken()
			token, err := st.CreateToken(ctx, key, "alice", store.TokenLimits{})
			if err != nil {
				t.Fatal(err)
			}
			if err := st.InsertAccount(ctx, &store.Account{
				Provider: "gemini", ProviderAccountID: "gemini-test", AuthMode: "api_key",
				AccessToken: "fixture", HTTPReferer: referer,
			}); err != nil {
				t.Fatal(err)
			}
			cfg := config.Default()
			cfg.Gemini.BaseURL = up.URL + "/openai"
			client, err := backend.NewGemini(cfg.Gemini)
			if err != nil {
				t.Fatal(err)
			}
			slots := pool.NewSlots(provider.Gemini, st, oauth.NewCodexRefresher())
			if err := slots.Reload(ctx); err != nil {
				t.Fatal(err)
			}
			srv := &Server{cfg: cfg, store: st, pools: map[provider.Provider]*pool.Pool{
				provider.Gemini: pool.NewPool(slots, backend.NewGeminiBackend(client, 0.9)),
			}}
			cases := []struct{ path, input string }{
				{"/v1/responses", `{"model":"gemini-test","input":"hello"}`},
				{"/v1/messages", `{"model":"gemini-test","messages":[{"role":"user","content":"hi"}],"max_tokens":16}`},
				{"/v1/chat/completions", `{"model":"gemini-test","stream":true,"messages":[{"role":"user","content":"hi"}]}`},
			}
			for _, item := range cases {
				got := requestJSON(srv.Router(), "POST", item.path, key, item.input)
				if got.Code != 200 || !strings.Contains(got.Body.String(), "from gemini") {
					t.Errorf("%s = %d %s", item.path, got.Code, got.Body.String())
				}
			}
			_, input, output, err := st.TokenUsage(ctx, token.ID, 3600)
			if err != nil || input != 12 || output != 6 {
				t.Fatalf("Gemini accounting = %d/%d, %v", input, output, err)
			}
		})
	}
}

func TestZenResponsesUsesConfiguredUpstreamDialect(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		switch r.URL.Path {
		case "/responses":
			_, _ = io.WriteString(w, responsesSSE)
		case "/chat/completions":
			_, _ = io.WriteString(w, "data: {\"id\":\"zen-chat\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"zen output\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2}}\n\n")
		default:
			t.Errorf("unexpected Zen upstream path %q", r.URL.Path)
		}
	}))
	defer up.Close()
	ctx := context.Background()
	cfg := config.Default()
	cfg.Zen.GoBaseURL = up.URL
	cfg.Zen.BaseURLs = nil
	cfg.Models.ZenPatterns = []string{"zen-responses"}
	cfg.Models.ZenChatPatterns = []string{"zen-chat"}
	client, err := backend.NewZen(cfg.Zen)
	if err != nil {
		t.Fatal(err)
	}
	st := newSmokeStore(t)
	key, _ := store.GenerateToken()
	if _, err := st.CreateToken(ctx, key, "alice", store.TokenLimits{}); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertAccount(ctx, &store.Account{
		Provider: "zen", ProviderAccountID: "zen", AuthMode: "api_key", AccessToken: "fixture",
	}); err != nil {
		t.Fatal(err)
	}
	slots := pool.NewSlots(provider.Zen, st, oauth.NewCodexRefresher())
	if err := slots.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	relay := backend.NewRelayBackend(provider.Zen, func(ctx context.Context, token string, req *backend.RelayRequest) (*http.Response, *upstream.SendError) {
		return client.Post(ctx, &token, req.SessionKey, req.Path, req.Body)
	})
	srv := &Server{cfg: cfg, store: st, pools: map[provider.Provider]*pool.Pool{
		provider.Zen: pool.NewPool(slots, relay),
	}}
	for _, tc := range []struct{ model, expected string }{{"zen-responses", "resp_1"}, {"zen-chat", "zen output"}} {
		r := requestJSON(srv.Router(), "POST", "/v1/responses", key, fmt.Sprintf(`{"model":%q,"input":"hello"}`, tc.model))
		if r.Code != 200 || !strings.Contains(r.Body.String(), tc.expected) {
			t.Errorf("%s results %d %s", tc.model, r.Code, r.Body.String())
		}
	}
}

func TestModelCatalogDialects(t *testing.T) {
	srv, key := responsesTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/backend-api/codex/models" {
			t.Errorf("catalog path = %s", r.URL.Path)
		}
		_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5","context_window":400000,"input_modalities":["text","image"],"custom_field":"kept"}]}`)
	})
	srv.anthropicCatalog = []backend.AnthropicModel{{ID: "claude-test", Type: "model", DisplayName: "Claude Test"}}
	h := srv.Router()
	client := requestJSON(h, "GET", "/v1/models?client_version=0.168.0", key, "")
	if client.Code != 200 || !strings.Contains(client.Body.String(), `"custom_field":"kept"`) {
		t.Fatalf("Codex catalog = %d %s", client.Code, client.Body.String())
	}
	openai := requestJSON(h, "GET", "/v1/models", key, "")
	if openai.Code != 200 || !strings.Contains(openai.Body.String(), `"context_length":400000`) {
		t.Fatalf("OpenAI catalog = %d %s", openai.Code, openai.Body.String())
	}
	req := httptest.NewRequest("GET", "http://proxy.local/v1/models", nil)
	req.Header.Set("Anthropic-Version", "2023-06-01")
	req.Header.Set("Authorization", "Bearer "+key)
	anthropic := httptest.NewRecorder()
	h.ServeHTTP(anthropic, req)
	if anthropic.Code != 200 || !strings.Contains(anthropic.Body.String(), `"first_id":"claude-test"`) {
		t.Fatalf("Anthropic catalog = %d %s", anthropic.Code, anthropic.Body.String())
	}
}
