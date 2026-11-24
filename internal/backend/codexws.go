package backend

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/edy/slop-proxy-go/internal/pool"
	"github.com/edy/slop-proxy-go/internal/upstream"
)

// Constants mirrored from Rust codex/websocket.rs and
// server/openai/websocket.rs.
const (
	WSMaxMessageSize = 192 * 1024 * 1024 // 192 MiB
	WSSendTimeout    = 30 * time.Second
	WSPingInterval   = 25 * time.Second
	WSHandshakeLimit = 30 * time.Second

	TurnStateHeader     = "x-codex-turn-state"
	wsOpenAIBetaDefault = "responses_websockets=2026-02-06"
	wsOpenAIBetaInitial = "responses=experimental"
	wsUserAgentFallback = "0.168.0"
)

// wsEchoHeaders are the upstream 101 response headers echoed to the client.
var wsEchoHeaders = []string{
	"x-codex-turn-state", "x-reasoning-included", "x-models-etag",
	"openai-model", "x-request-id",
	"x-codex-safety-buffering-enabled", "x-codex-safety-buffering-faster-model",
}

// WSEchoHeaders are the only upstream 101 headers echoed to the client.
func WSEchoHeaders() []string { return append([]string(nil), wsEchoHeaders...) }

// wsPassHeaders are client headers forwarded to the upstream handshake.
var wsPassHeaders = []string{
	"openai-beta", "originator", "version", "user-agent",
	"session_id", "session-id", "thread_id", "thread-id",
	"x-codex-turn-metadata", "x-codex-turn-state",
	"x-codex-beta-features", "x-codex-window-id", "x-client-request-id",
}

// WSConnection is an upstream Codex websocket plus the 101 response headers.
type WSConnection struct {
	Conn    *websocket.Conn
	Headers http.Header
}

// ResponseHeader lets the pool read quota headers from a websocket handshake
// the same way it does from an HTTP response.
func (c *WSConnection) ResponseHeader() http.Header { return c.Headers }

// WSResponsesURL converts the configured HTTP base URL into its ws/wss form.
func (b *CodexBackend) WSResponsesURL() (string, error) {
	raw := strings.TrimRight(b.cfg.BaseURL, "/") + "/responses"
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("websocket upstream URL: %w", err)
	}
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	case "http":
		u.Scheme = "ws"
	default:
		return "", fmt.Errorf("unsupported WebSocket upstream scheme %q", u.Scheme)
	}
	return u.String(), nil
}

// WSHeaders builds the upstream handshake headers. The caller's credentials
// and cookie are never forwarded; the pool's account credentials win.
func (b *CodexBackend) WSHeaders(incoming http.Header, token, accountID, session, model string) http.Header {
	callerSession := firstHeader(incoming, "session-id", "session_id")
	if callerSession == "" {
		callerSession = session
	}
	thread := firstHeader(incoming, "thread-id", "thread_id")
	if thread == "" {
		thread = callerSession
	}
	version := b.cfg.Version
	if version == "" {
		version = wsUserAgentFallback
	}
	originator := b.cfg.Originator
	if originator == "" {
		originator = "codex_cli_rs"
	}

	routing := "model=" + model
	if hint := incoming.Get("x-codex-routing-hint"); hint != "" {
		parts := strings.Split(hint, ",")
		for i, part := range parts {
			if strings.HasPrefix(strings.TrimSpace(part), "model=") {
				parts[i] = routing
			}
		}
		routing = strings.Join(parts, ",")
	}

	h := make(http.Header)
	for name, value := range map[string]string{
		"Authorization":        "Bearer " + token,
		"ChatGPT-Account-Id":   accountID,
		"OpenAI-Beta":          wsOpenAIBetaInitial,
		"Originator":           originator,
		"Version":              version,
		"User-Agent":           originator + "/" + version + " (Linux; x86_64)",
		"Session_id":           callerSession,
		"Session-Id":           callerSession,
		"Thread_id":            thread,
		"Thread-Id":            thread,
		"X-Codex-Routing-Hint": routing,
	} {
		h.Set(name, value)
	}
	for _, name := range wsPassHeaders {
		if v := incoming.Get(name); v != "" {
			h.Set(name, v)
		}
	}
	if incoming.Get("openai-beta") == "" {
		h.Set("OpenAI-Beta", wsOpenAIBetaDefault)
	}
	return h
}

func firstHeader(h http.Header, names ...string) string {
	for _, name := range names {
		if v := h.Get(name); v != "" {
			return v
		}
	}
	return ""
}

// DialWebSocket opens the upstream Codex websocket. A non-101 HTTP response
// is classified into a pool.SendError so the pool can cool the account.
func (b *CodexBackend) DialWebSocket(ctx context.Context, incoming http.Header, token, accountID, session, model string) (*WSConnection, error) {
	u, err := b.WSResponsesURL()
	if err != nil {
		return nil, &pool.SendError{Kind: pool.SendNetwork, Body: err.Error()}
	}
	headers := b.WSHeaders(incoming, token, accountID, session, model)

	dialer := websocket.Dialer{
		HandshakeTimeout: WSHandshakeLimit,
		ReadBufferSize:   64 * 1024,
		WriteBufferSize:  64 * 1024,
	}
	handshakeCtx, cancel := context.WithTimeout(ctx, WSHandshakeLimit)
	defer cancel()
	conn, resp, err := dialer.DialContext(handshakeCtx, u, headers)
	if err != nil {
		if resp != nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
			_ = resp.Body.Close()
			return nil, classifyWSHandshake(resp.StatusCode, resp.Header, string(body))
		}
		return nil, &pool.SendError{Kind: pool.SendNetwork, Body: err.Error()}
	}
	conn.SetReadLimit(WSMaxMessageSize)
	h := resp.Header.Clone()
	return &WSConnection{Conn: conn, Headers: h}, nil
}

func classifyWSHandshake(status int, hdr http.Header, body string) *pool.SendError {
	reason := (&CodexBackend{}).Reason(body)
	switch status {
	case http.StatusUnauthorized:
		return &pool.SendError{Kind: pool.SendAuth, Body: reason}
	case http.StatusTooManyRequests:
		retry := upstream.RetryAfterSecs(hdr, []string{"x-codex-primary-reset-at"}, time.Now().Unix())
		return &pool.SendError{Kind: pool.SendRateLimited, RetryAfter: retry, Body: reason, Headers: hdr.Clone()}
	case http.StatusBadRequest:
		return &pool.SendError{Kind: pool.SendBadRequest, Body: reason, Headers: hdr.Clone()}
	default:
		return &pool.SendError{Kind: pool.SendOther, Body: fmt.Sprintf("upstream %d: %s", status, reason), Headers: hdr.Clone()}
	}
}

// SessionUUID derives a stable upstream conversation id from the proxy's
// session key, matching Rust's session_uuid (SHA-256 prefix as a v4 UUID).
func SessionUUID(sessionKey string) string {
	if sessionKey == "" {
		return uuid.NewString()
	}
	sum := sha256.Sum256([]byte(sessionKey))
	var b [16]byte
	copy(b[:], sum[:16])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return uuid.UUID(b).String()
}

// WSFault classifies what an upstream error frame says about the account.
type WSFault int

const (
	WSFaultCaller WSFault = iota
	WSFaultExhausted
	WSFaultTransient
)

// WSResponseError is a classified error / response.failed frame.
type WSResponseError struct {
	Status int
	Fault  WSFault
}

func hasString(set []string, value string) bool {
	for _, item := range set {
		if item == value {
			return true
		}
	}
	return false
}

// WSResponseErrorFrom mirrors codex::websocket::response_error.
func WSResponseErrorFrom(event map[string]any) (WSResponseError, bool) {
	kind, _ := event["type"].(string)
	var errObj map[string]any
	switch kind {
	case "error":
		errObj, _ = event["error"].(map[string]any)
	case "response.failed":
		respObj, _ := event["response"].(map[string]any)
		errObj, _ = respObj["error"].(map[string]any)
	default:
		return WSResponseError{}, false
	}
	if errObj == nil {
		return WSResponseError{}, false
	}
	code, _ := errObj["code"].(string)
	errType, _ := errObj["type"].(string)

	status := 0
	fault := WSFaultTransient
	switch {
	case hasString([]string{"cyber_policy", "invalid_prompt", "context_length_exceeded",
		"invalid_encrypted_content", "previous_response_not_found"}, code):
		status, fault = 400, WSFaultCaller
	case hasString(exhaustedCodes, code):
		status, fault = 429, WSFaultExhausted
	case code == "rate_limit_exceeded":
		status, fault = 429, WSFaultTransient
	case hasString([]string{"server_is_overloaded", "slow_down", "model_at_capacity"}, code):
		status, fault = 503, WSFaultTransient
	default:
		switch {
		case errType == "invalid_request_error":
			status, fault = 400, WSFaultCaller
		case errType == "authentication_error":
			status, fault = 401, WSFaultCaller
		case errType == "permission_error":
			status, fault = 403, WSFaultCaller
		case hasString(exhaustedCodes, errType):
			status, fault = 429, WSFaultExhausted
		case errType == "rate_limit_error" || errType == "rate_limit_exceeded":
			status, fault = 429, WSFaultTransient
		case hasString([]string{"server_error", "api_error", "internal_server_error"}, errType):
			status, fault = 500, WSFaultTransient
		case code == "server_error":
			status, fault = 500, WSFaultTransient
		}
	}
	if v, ok := numericStatus(event["status"]); ok {
		status = v
	} else if v, ok := numericStatus(event["status_code"]); ok {
		status = v
	}
	if fault == WSFaultTransient && status < 500 && status != 429 {
		fault = WSFaultCaller
	}
	if status < 400 || status >= 600 {
		return WSResponseError{}, false
	}
	return WSResponseError{Status: status, Fault: fault}, true
}

func numericStatus(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), true
	case json.Number:
		i, err := strconv.ParseInt(string(n), 10, 32)
		return int(i), err == nil
	case int:
		return n, true
	case string:
		i, err := strconv.Atoi(n)
		return i, err == nil
	}
	return 0, false
}

// Normalize applies the in-frame normalisation Rust does before relaying an
// upstream error to the client.
func (e WSResponseError) Normalize(event map[string]any) {
	kind, _ := event["type"].(string)
	if kind == "error" {
		event["status"] = e.Status
		delete(event, "status_code")
	}
	var errObj map[string]any
	if kind == "error" {
		errObj, _ = event["error"].(map[string]any)
	} else {
		respObj, _ := event["response"].(map[string]any)
		errObj, _ = respObj["error"].(map[string]any)
	}
	if e.Fault == WSFaultTransient && e.Status >= 500 && errObj != nil {
		code, _ := errObj["code"].(string)
		if code == "server_is_overloaded" || code == "slow_down" {
			errObj["upstream_code"] = code
			errObj["code"] = "upstream_unavailable"
		}
	}
}

// WindowName mirrors pool::codex::window_name.
func WindowName(minutes int64) string { return windowName(minutes) }
