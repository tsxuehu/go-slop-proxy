package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/edy/slop-proxy-go/internal/backend"
	"github.com/edy/slop-proxy-go/internal/config"
	"github.com/edy/slop-proxy-go/internal/oauth"
	"github.com/edy/slop-proxy-go/internal/pool"
	"github.com/edy/slop-proxy-go/internal/provider"
	"github.com/edy/slop-proxy-go/internal/store"
)

// wsUpstream is a mock Codex websocket endpoint that records the handshake
// headers and every request frame, then replies with a terminal event.
type wsUpstream struct {
	server   *httptest.Server
	requests chan map[string]any
	headers  chan http.Header
	reply    func(map[string]any) []map[string]any
}

func newWSUpstream(t *testing.T, reply func(map[string]any) []map[string]any) *wsUpstream {
	t.Helper()
	u := &wsUpstream{
		requests: make(chan map[string]any, 16),
		headers:  make(chan http.Header, 4),
		reply:    reply,
	}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	u.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/backend-api/codex/responses" {
			http.NotFound(w, r)
			return
		}
		echo := http.Header{}
		echo.Set("x-codex-turn-state", "state-from-upstream")
		echo.Set("x-reasoning-included", "true")
		echo.Set("x-models-etag", "catalog-v1")
		conn, err := upgrader.Upgrade(w, r, echo)
		if err != nil {
			t.Errorf("upstream upgrade: %v", err)
			return
		}
		defer conn.Close()
		u.headers <- r.Header.Clone()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var frame map[string]any
			if json.Unmarshal(data, &frame) != nil {
				continue
			}
			u.requests <- frame
			for _, response := range u.reply(frame) {
				payload, _ := json.Marshal(response)
				if conn.WriteMessage(websocket.TextMessage, payload) != nil {
					return
				}
			}
		}
	}))
	t.Cleanup(u.server.Close)
	return u
}

func wsTestServer(t *testing.T, upstream *wsUpstream) (*httptest.Server, string) {
	t.Helper()
	st := newSmokeStore(t)
	key, _ := store.GenerateToken()
	if _, err := st.CreateToken(context.Background(), key, "alice", store.TokenLimits{}); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertAccount(context.Background(), &store.Account{
		Provider: "openai", ProviderAccountID: "acct-1", Label: "test@example.com",
		AuthMode: "oauth", AccessToken: "at", RefreshToken: "rt",
		AccessExpiresAt: time.Now().Unix() + 3600, Status: "active",
	}); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Codex.BaseURL = upstream.server.URL + "/backend-api/codex"
	be := backend.NewCodex(cfg.Codex)
	slots := pool.NewSlots(provider.OpenAI, st, oauth.NewCodexRefresher())
	if err := slots.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	pl := pool.NewPool(slots, be)
	srv := &Server{
		cfg: cfg, store: st, backend: be,
		pools:    map[provider.Provider]*pool.Pool{provider.OpenAI: pl},
		catalogs: map[provider.Provider][]string{},
	}
	proxy := httptest.NewServer(srv.Router())
	t.Cleanup(proxy.Close)
	return proxy, key
}

func wsDial(t *testing.T, proxy *httptest.Server, key string) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(proxy.URL, "http") + "/v1/responses"
	h := http.Header{}
	h.Set("Authorization", "Bearer "+key)
	h.Set("chatgpt-account-id", "untrusted-account")
	h.Set("cookie", "do-not-forward=secret")
	h.Set("openai-beta", "responses_websockets=2026-02-06")
	h.Set("session-id", "session-from-client")
	h.Set("thread-id", "thread-from-client")
	h.Set("x-codex-routing-hint", "model=gpt-5")
	conn, resp, err := websocket.DefaultDialer.Dial(url, h)
	if err != nil {
		if resp != nil {
			t.Fatalf("dial failed: %v (status %d)", err, resp.StatusCode)
		}
		t.Fatalf("dial failed: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func wsSend(t *testing.T, conn *websocket.Conn, frame map[string]any) {
	t.Helper()
	payload, _ := json.Marshal(frame)
	if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatal(err)
	}
}

func wsReceive(t *testing.T, conn *websocket.Conn) map[string]any {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var event map[string]any
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatalf("bad frame %q: %v", data, err)
	}
	return event
}

func TestWebSocketHandshakeAndTurn(t *testing.T) {
	upstream := newWSUpstream(t, func(frame map[string]any) []map[string]any {
		return []map[string]any{{
			"type": "response.completed", "stream_id": frame["stream_id"],
			"response": map[string]any{
				"id": "r1", "status": "completed",
				"usage": map[string]any{"input_tokens": 12, "output_tokens": 5},
			},
		}}
	})
	proxy, key := wsTestServer(t, upstream)
	conn := wsDial(t, proxy, key)

	// The upstream handshake must carry the pool's credentials, never the
	// caller's, and must forward the Codex metadata headers.
	select {
	case h := <-upstream.headers:
		if h.Get("Authorization") != "Bearer at" {
			t.Errorf("upstream authorization = %q", h.Get("Authorization"))
		}
		if h.Get("Chatgpt-Account-Id") != "acct-1" {
			t.Errorf("upstream account = %q", h.Get("Chatgpt-Account-Id"))
		}
		if h.Get("Cookie") != "" {
			t.Error("client cookie was forwarded upstream")
		}
		if h.Get("Session-Id") != "session-from-client" || h.Get("Thread-Id") != "thread-from-client" {
			t.Error("session/thread metadata was not forwarded")
		}
		if h.Get("Openai-Beta") != "responses_websockets=2026-02-06" {
			t.Errorf("openai-beta = %q", h.Get("Openai-Beta"))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("upstream never saw a handshake")
	}

	wsSend(t, conn, map[string]any{
		"type": "response.create", "model": "gpt-5",
		"stream": true, "background": true,
		"stream_id": "lane-1", "previous_response_id": "resp-99",
		"future_request_field": map[string]any{"preserved": true},
	})
	select {
	case frame := <-upstream.requests:
		if _, ok := frame["stream"]; ok {
			t.Error("stream was not removed from the upstream frame")
		}
		if _, ok := frame["background"]; ok {
			t.Error("background was not removed from the upstream frame")
		}
		if frame["store"] != false {
			t.Errorf("store = %v, want false", frame["store"])
		}
		if frame["instructions"] == nil || frame["instructions"] == "" {
			t.Error("instructions were not injected")
		}
		if frame["previous_response_id"] != "resp-99" {
			t.Error("continuation history was dropped")
		}
		if _, ok := frame["future_request_field"]; !ok {
			t.Error("unknown fields were not preserved")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("upstream never saw a turn")
	}

	event := wsReceive(t, conn)
	if event["type"] != "response.completed" || event["stream_id"] != "lane-1" {
		t.Errorf("unexpected client event: %v", event)
	}
}

func TestWebSocketRewritesRateLimitsAndErrors(t *testing.T) {
	upstream := newWSUpstream(t, func(frame map[string]any) []map[string]any {
		return []map[string]any{
			{
				"type": "codex.rate_limits", "plan_type": "plus", "credits": map[string]any{"balance": 10},
				"rate_limits": map[string]any{"primary": map[string]any{"window_minutes": 300, "used_percent": 42}},
			},
			{
				"type": "error", "status": 429,
				"error":   map[string]any{"type": "server_error", "code": "slow_down", "message": "busy"},
				"headers": map[string]string{"retry-after": "17"},
			},
		}
	})
	proxy, key := wsTestServer(t, upstream)
	conn := wsDial(t, proxy, key)
	wsSend(t, conn, map[string]any{"type": "response.create", "model": "gpt-5", "stream_id": "lane-2"})

	limits := wsReceive(t, conn)
	if limits["type"] != "codex.rate_limits" {
		t.Fatalf("first frame = %v", limits)
	}
	if _, ok := limits["credits"]; ok {
		t.Error("credits should be stripped from a pooled rate-limits event")
	}
	if _, ok := limits["plan_type"]; ok {
		t.Error("plan_type should be stripped from a pooled rate-limits event")
	}
	if _, ok := limits["rate_limits"]; !ok {
		t.Error("rate_limits should be replaced with the pool average")
	}

	errFrame := wsReceive(t, conn)
	if errFrame["status"] != float64(429) {
		t.Errorf("error status = %v", errFrame["status"])
	}
	if errObj, ok := errFrame["error"].(map[string]any); !ok || errObj["code"] != "slow_down" {
		t.Errorf("error code was normalised away: %v", errFrame["error"])
	}
}

func TestWebSocketUpgradeFallsBackWithoutHeaders(t *testing.T) {
	upstream := newWSUpstream(t, func(map[string]any) []map[string]any { return nil })
	proxy, key := wsTestServer(t, upstream)
	req, _ := http.NewRequest(http.MethodGet, proxy.URL+"/v1/responses", nil)
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUpgradeRequired {
		t.Errorf("status = %d, want 426", resp.StatusCode)
	}
	select {
	case <-upstream.headers:
		t.Error("upstream was dialled for a request without upgrade headers")
	default:
	}
}

func TestWebSocketRejectsNonOpenAIModel(t *testing.T) {
	upstream := newWSUpstream(t, func(map[string]any) []map[string]any { return nil })
	proxy, key := wsTestServer(t, upstream)
	url := "ws" + strings.TrimPrefix(proxy.URL, "http") + "/v1/responses"
	h := http.Header{}
	h.Set("Authorization", "Bearer "+key)
	h.Set("x-codex-routing-hint", "model=gemini-test")
	_, resp, err := websocket.DefaultDialer.Dial(url, h)
	if err == nil {
		t.Fatal("expected dial to be rejected")
	}
	if resp == nil || resp.StatusCode != http.StatusUpgradeRequired {
		t.Fatalf("want 426, got %v", resp)
	}
}

func TestTurnStateParsing(t *testing.T) {
	// 10 Fernet ciphertext blocks => 57 + 160 = 217 bytes => 290 base64url
	// characters without padding.
	ts, ok := pool.ParseTurnState("gAAAA" + strings.Repeat("A", 285))
	if !ok {
		t.Fatal("valid turn-state was rejected")
	}
	if ts.Blocks != 10 {
		t.Errorf("blocks = %d, want 10", ts.Blocks)
	}
	if _, ok := pool.ParseTurnState("gAAAA" + strings.Repeat("A", 10)); ok {
		t.Error("malformed turn-state was accepted")
	}
	cleaner := pool.TurnState{Token: "clean", Blocks: 10}
	dirtier := pool.TurnState{Token: "dirty", Blocks: 11}
	if !cleaner.Supersedes(dirtier) || dirtier.Supersedes(cleaner) {
		t.Error("supersedes comparison is inverted")
	}
}

func TestPrepareWSRequestPreservesUnknownFields(t *testing.T) {
	srv := &Server{cfg: config.Default()}
	raw := []byte(`{"type":"response.create","model":"gpt-5","stream":true,"background":true,"generate":false,"stream_id":"s1","custom":{"k":[1,2,3]}}`)
	p, err := srv.prepareWSRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	var frame map[string]json.RawMessage
	if json.Unmarshal(p.Encoded, &frame) != nil {
		t.Fatal("encoded frame is not JSON")
	}
	for _, dropped := range []string{"stream", "background"} {
		if _, ok := frame[dropped]; ok {
			t.Errorf("%s should be removed", dropped)
		}
	}
	if string(frame["store"]) != "false" {
		t.Error("store should be forced to false")
	}
	if _, ok := frame["custom"]; !ok {
		t.Error("unknown fields were dropped")
	}
	if p.Generate {
		t.Error("generate:false should mark the turn as prewarm")
	}
	if string(frame["generate"]) != "false" {
		t.Error("generate:false should be preserved upstream")
	}
}

func TestRestoreReservedNamespace(t *testing.T) {
	got := restoreReservedNamespace(`{"namespace":"slop_collab","name":"spawn"}`)
	if !strings.Contains(got, `"namespace":"collaboration"`) {
		t.Errorf("namespace was not restored: %s", got)
	}
}
