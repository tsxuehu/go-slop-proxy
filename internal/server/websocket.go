package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/edy/slop-proxy-go/internal/backend"
	"github.com/edy/slop-proxy-go/internal/pool"
	"github.com/edy/slop-proxy-go/internal/pricing"
	"github.com/edy/slop-proxy-go/internal/provider"
	"github.com/edy/slop-proxy-go/internal/store"
	"github.com/edy/slop-proxy-go/internal/translate"
)

// The Codex client probes GET /v1/responses for a websocket upgrade before
// falling back to HTTP POST. This file implements that websocket transport:
// handshake → upstream dial → bidirectional relay, with per-turn admission,
// pinned turn-state, service-tier redial and pooled rate-limit rewriting.

var wsUpgrader = websocket.Upgrader{
	ReadBufferSize:  64 * 1024,
	WriteBufferSize: 64 * 1024,
	// Rust's axum WebSocketUpgrade does no Origin check; adding one would
	// break the Codex CLI.
	CheckOrigin: func(*http.Request) bool { return true },
}

func wsUpgradeRequested(r *http.Request) bool {
	if !strings.EqualFold(strings.TrimSpace(r.Header.Get("Upgrade")), "websocket") {
		return false
	}
	connection := strings.ToLower(r.Header.Get("Connection"))
	found := false
	for _, part := range strings.Split(connection, ",") {
		if strings.TrimSpace(part) == "upgrade" {
			found = true
			break
		}
	}
	return found && r.Header.Get("Sec-WebSocket-Key") != "" &&
		r.Header.Get("Sec-WebSocket-Version") == "13"
}

func wsUpgradeRequired(w http.ResponseWriter) {
	w.WriteHeader(http.StatusUpgradeRequired)
	_, _ = io.WriteString(w, "WebSocket unavailable for this request, use HTTP POST")
}

func wsModelFromHint(h http.Header, fallback string) string {
	for _, part := range strings.Split(h.Get("x-codex-routing-hint"), ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "model=") {
			return strings.TrimPrefix(part, "model=")
		}
	}
	return fallback
}

func wsSessionKey(h http.Header, fallback string) string {
	for _, name := range []string{"session-id", "session_id", "thread-id", "thread_id"} {
		if v := h.Get(name); v != "" {
			return v
		}
	}
	return fallback
}

// websocketResponses handles the GET /v1/responses upgrade. Admission at the
// handshake is deliberately quota-free (withToken treats GET as a local read);
// each response.create frame is admitted separately.
func (s *Server) websocketResponses(w http.ResponseWriter, r *http.Request, rc *requestContext) {
	requested := wsModelFromHint(r.Header, s.cfg.Models.Default)
	resolved := translate.Resolve(&s.cfg, requested)
	if s.cfg.Models.Blocked(resolved.Model) {
		writeAPIError(w, dialectOpenAI, http.StatusForbidden, "permission_error",
			resolved.Model+" is blocked on this proxy")
		return
	}
	target := s.cfg.Models.Route(resolved.Model)
	if target != provider.OpenAI {
		wsUpgradeRequired(w)
		return
	}
	if !s.tokenMayUse(rc, target) {
		writeAPIError(w, dialectOpenAI, http.StatusForbidden, "permission_error",
			"this token is not scoped to the "+target.String()+" backend")
		return
	}
	if rc.tokensSpent != nil {
		writeAPIError(w, dialectOpenAI, http.StatusTooManyRequests, "rate_limit_error", "API token token limit exceeded")
		return
	}
	if !wsUpgradeRequested(r) {
		wsUpgradeRequired(w)
		return
	}
	pl := s.pools[provider.OpenAI]
	if pl == nil || s.backend == nil {
		writePoolError(w, &pool.PoolError{Kind: pool.ErrNoAccounts, Message: "no codex accounts; run login"})
		return
	}
	sessionKey := wsSessionKey(r.Header, rc.user)
	route := s.wsRoute(r, rc, sessionKey, resolved.Model, "")

	served, err := pl.ExecuteWebSocket(r.Context(), route, s.makeWSDial(r.Context(), r.Header.Clone(), sessionKey, resolved.Model))
	if err != nil {
		writePoolError(w, err)
		return
	}
	upstream, ok := served.Response.(*backend.WSConnection)
	if !ok || upstream == nil {
		writePoolError(w, &pool.PoolError{Kind: pool.ErrUpstream, Message: "unexpected websocket reply"})
		return
	}

	// Only seven upstream 101 headers are echoed back to the client.
	echo := make(http.Header)
	for _, name := range backend.WSEchoHeaders() {
		if v := upstream.Headers.Get(name); v != "" {
			echo.Set(name, v)
		}
	}
	client, err := wsUpgrader.Upgrade(w, r, echo)
	if err != nil {
		slog.Warn("websocket upgrade failed", "error", err)
		_ = upstream.Conn.Close()
		return
	}
	client.SetReadLimit(backend.WSMaxMessageSize)

	relay := &wsRelay{
		s: s, pl: pl, client: client, upstream: upstream,
		token: rc.apiKey, headers: r.Header.Clone(), sessionKey: sessionKey,
		auth: rc, accountID: served.AccountID, attempts: served.Attempts,
		turnStateBlocks: s.observeTurnState(r.Context(), pl, served.AccountID, upstream.Headers),
		pending:         make(map[string][]*wsPending),
	}
	relay.run(r.Context())
}

func (s *Server) wsRoute(r *http.Request, rc *requestContext, sessionKey, model, serviceTier string) pool.Route {
	return pool.Route{
		SessionKey: sessionKey, Model: model, ServiceTier: serviceTier,
		User: rc.user, ReservedOnly: rc.token.ReservedOnly,
		PreferTrusted: rc.token.PreferTrusted, PinnedAccount: rc.token.PinnedAccount,
	}
}

// makeWSDial returns the pool's dial function for a websocket handshake. It
// substitutes the account's cleaner turn-state token when one is available,
// falling back to the caller's headers once if the backend refuses it.
func (s *Server) makeWSDial(ctx context.Context, incoming http.Header, sessionKey, model string) func(context.Context, string, *pool.Slot) (any, error) {
	pl := s.pools[provider.OpenAI]
	session := backend.SessionUUID(sessionKey)
	return func(dialCtx context.Context, token string, slot *pool.Slot) (any, error) {
		try := func(h http.Header) (*backend.WSConnection, error) {
			return s.backend.DialWebSocket(dialCtx, h, token, slot.ProviderAccountID, session, model)
		}
		if s.backend.PinTurnState() {
			if presented, ok := pool.ParseTurnState(incoming.Get(backend.TurnStateHeader)); ok {
				if pinned, ok := pl.Slots().PreferredTurnState(slot, presented); ok {
					swapped := incoming.Clone()
					swapped.Set(backend.TurnStateHeader, pinned.Token)
					if conn, err := try(swapped); err == nil {
						return conn, nil
					}
					pl.Slots().RefuseTurnState(slot)
					slog.Warn("backend refused pinned turn-state, dialling with caller's",
						"account", slot.Display, "blocks", pinned.Blocks)
				}
			}
		}
		return try(incoming)
	}
}

func (s *Server) observeTurnState(ctx context.Context, pl *pool.Pool, accountID *int64, h http.Header) *int64 {
	observed, ok := pool.ParseTurnState(h.Get(backend.TurnStateHeader))
	if !ok {
		return nil
	}
	if accountID != nil {
		if slot := pl.Slots().ByID(*accountID); slot != nil {
			pl.Slots().NoteTurnState(ctx, slot, observed)
		}
	}
	blocks := observed.Blocks
	return &blocks
}

// admitTurn performs the per-turn token admission that Rust's Relay::request
// runs on every response.create frame.
func (s *Server) admitTurn(ctx context.Context, key string) (*requestContext, int, string, map[string]string, error) {
	tok, err := s.store.AuthToken(ctx, key)
	if err != nil {
		return nil, http.StatusUnauthorized, "authentication_error", nil,
			fmt.Errorf("invalid or revoked API token")
	}
	admission, err := s.store.Admit(ctx, tok.ID, store.TokenLimits{
		Requests: tok.Requests, Tokens: tok.Tokens, WindowSecs: tok.WindowSecs,
		SlowdownMS: tok.SlowdownMS, Metered: decodeList(tok.MeteredProviders),
	})
	if err != nil {
		var denied *store.AdmissionError
		if errors.As(err, &denied) {
			msg := "API token request limit exceeded"
			if denied.Kind == "token" {
				msg = "API token token limit exceeded"
			}
			headers := map[string]string{"retry-after": strconv.FormatInt(denied.RetryAfter, 10)}
			return nil, http.StatusTooManyRequests, "rate_limit_error", headers, fmt.Errorf("%s", msg)
		}
		return nil, http.StatusInternalServerError, "api_error", nil, err
	}
	return &requestContext{
		token: tok, apiKey: key, user: tok.User,
		meterID: admission.MeterID, tokensSpent: admission.TokensSpent,
	}, 0, "", nil, nil
}

func wsErrorFrame(status int, kind, message string, headers map[string]string, streamID string) []byte {
	body := map[string]any{
		"error": map[string]any{"type": kind, "message": message, "code": nil, "param": nil},
		"type":  "error", "status": status,
	}
	if headers == nil {
		headers = map[string]string{}
	}
	body["headers"] = headers
	if streamID != "" {
		body["stream_id"] = streamID
	}
	return mustMarshal(body)
}

// ---------------------------------------------------------------------------
// Relay
// ---------------------------------------------------------------------------

type wsPending struct {
	rc              *requestContext
	requested       string
	upstream        string
	serviceTier     string
	effort          string
	accountID       *int64
	attempts        int
	turnStateBlocks *int64
	requestBytes    int64
	responseBytes   int64
	input           int64
	output          int64
	cacheRead       int64
	cacheWrite      int64
	reasoning       int64
	generated       bool
	streamID        string
	started         time.Time
	completed       bool
	errorKind       string
	stopReason      string
	ttftMS          int64
}

type wsEvent struct {
	side      int // 0 client, 1 upstream
	typ       int
	data      []byte
	err       error
	closeCode int
	closeText string
}

type wsRelay struct {
	s               *Server
	pl              *pool.Pool
	client          *websocket.Conn
	upstream        *backend.WSConnection
	token           string
	headers         http.Header
	auth            *requestContext
	accountID       *int64
	attempts        int
	turnStateBlocks *int64
	sessionKey      string
	pending         map[string][]*wsPending
	upstreamFailed  bool

	clientIn   <-chan wsEvent
	clientDone chan struct{}
	upIn       <-chan wsEvent
	upDone     chan struct{}
}

func wsWrite(conn *websocket.Conn, typ int, data []byte) bool {
	_ = conn.SetWriteDeadline(time.Now().Add(backend.WSSendTimeout))
	return conn.WriteMessage(typ, data) == nil
}

func wsControl(conn *websocket.Conn, typ int, data []byte) {
	_ = conn.WriteControl(typ, data, time.Now().Add(backend.WSSendTimeout))
}

func wsClose(conn *websocket.Conn, code int, reason string) {
	wsControl(conn, websocket.CloseMessage, websocket.FormatCloseMessage(code, reason))
}

func (r *wsRelay) upstreamConn() *websocket.Conn { return r.upstream.Conn }

func (r *wsRelay) run(ctx context.Context) {
	clientIn := make(chan wsEvent, 32)
	r.clientIn, r.clientDone = clientIn, make(chan struct{})
	r.startReader(r.client, 0, clientIn, r.clientDone)
	r.openUpstreamReader()
	defer func() {
		close(r.clientDone)
		close(r.upDone)
	}()

	keepalive := time.NewTicker(backend.WSPingInterval)
	defer keepalive.Stop()
	breakLoop := false
	for !breakLoop {
		// Re-evaluated every iteration: a service-tier redial swaps the
		// upstream connection and its reader channel.
		upIn := r.upIn
		select {
		case <-ctx.Done():
			breakLoop = true
		case <-keepalive.C:
			if !wsControl2(r.upstreamConn(), websocket.PingMessage, nil) {
				r.upstreamClosed()
				breakLoop = true
				continue
			}
			wsControl2(r.client, websocket.PingMessage, nil)
		case ev := <-clientIn:
			if action := r.consume(ctx, ev); action == 2 {
				return
			} else if action == 1 {
				breakLoop = true
			}
		case ev := <-upIn:
			if action := r.consume(ctx, ev); action == 2 {
				return
			} else if action == 1 {
				breakLoop = true
			}
		}
	}
	wsClose(r.client, 1011, "WebSocket connection ended")
	wsClose(r.upstreamConn(), websocket.CloseNormalClosure, "")
}

// consume handles one reader event. 0 = keep looping, 1 = break, 2 = return.
func (r *wsRelay) consume(ctx context.Context, ev wsEvent) int {
	if ev.err != nil {
		if ev.side == 1 {
			r.upstreamClosed()
		}
		return 1
	}
	switch ev.typ {
	case websocket.PingMessage:
		other := r.upstreamConn()
		if ev.side == 1 {
			other = r.client
		}
		wsControl(other, websocket.PongMessage, ev.data)
		return 0
	case websocket.PongMessage:
		return 0
	case websocket.CloseMessage:
		if ev.side == 0 {
			wsClose(r.upstreamConn(), ev.closeCode, ev.closeText)
		} else {
			r.upstreamClosed()
			wsClose(r.client, ev.closeCode, ev.closeText)
		}
		return 2
	}
	if ev.side == 0 {
		if !r.handleClientFrame(ctx, ev) {
			return 2
		}
	} else if !r.handleUpstreamFrame(ctx, ev) {
		return 2
	}
	return 0
}

func (r *wsRelay) openUpstreamReader() {
	ch := make(chan wsEvent, 32)
	r.upIn, r.upDone = ch, make(chan struct{})
	r.startReader(r.upstreamConn(), 1, ch, r.upDone)
}

func (r *wsRelay) redialReader() {
	close(r.upDone)
	r.openUpstreamReader()
}

func wsControl2(conn *websocket.Conn, typ int, data []byte) bool {
	_ = conn.SetWriteDeadline(time.Now().Add(backend.WSSendTimeout))
	return conn.WriteControl(typ, data, time.Now().Add(backend.WSSendTimeout)) == nil
}

func (r *wsRelay) startReader(conn *websocket.Conn, side int, in chan<- wsEvent, done <-chan struct{}) {
	conn.SetPingHandler(func(appData string) error {
		select {
		case in <- wsEvent{side: side, typ: websocket.PingMessage, data: []byte(appData)}:
		case <-done:
		}
		return nil
	})
	conn.SetPongHandler(func(string) error { return nil })
	conn.SetCloseHandler(func(code int, text string) error {
		select {
		case in <- wsEvent{side: side, typ: websocket.CloseMessage, closeCode: code, closeText: text}:
		case <-done:
		}
		return nil
	})
	go func() {
		for {
			typ, data, err := conn.ReadMessage()
			if err != nil {
				select {
				case in <- wsEvent{side: side, err: err}:
				case <-done:
				}
				return
			}
			select {
			case in <- wsEvent{side: side, typ: typ, data: data}:
			case <-done:
				return
			}
		}
	}()
}

func (r *wsRelay) handleClientFrame(ctx context.Context, ev wsEvent) bool {
	if ev.typ == websocket.BinaryMessage {
		wsClose(r.client, 1003, "Responses requests must be text messages")
		wsClose(r.upstreamConn(), websocket.CloseNormalClosure, "")
		return false
	}
	if ev.typ != websocket.TextMessage {
		return true
	}
	prepared, err := r.s.prepareWSRequest(ev.data)
	if err != nil {
		wsWrite(r.client, websocket.TextMessage,
			wsErrorFrame(400, "invalid_request_error", err.Error(), nil, wsStreamID(ev.data)))
		return true
	}
	if !prepared.IsCreate {
		// Not a response.create frame: verbatim passthrough.
		if !wsWrite(r.upstreamConn(), websocket.TextMessage, ev.data) {
			r.upstreamClosed()
			return false
		}
		return true
	}

	auth, status, kind, headers, admitErr := r.s.admitTurn(ctx, r.token)
	if admitErr != nil {
		wsWrite(r.client, websocket.TextMessage,
			wsErrorFrame(status, kind, admitErr.Error(), headers, prepared.StreamID))
		return true
	}
	if auth.token.PinnedAccount != nil && (r.accountID == nil || *auth.token.PinnedAccount != *r.accountID) {
		wsWrite(r.client, websocket.TextMessage,
			wsErrorFrame(403, "permission_error", "reconnect to use the pinned account", nil, prepared.StreamID))
		return true
	}

	route := r.s.wsRoute(nil, auth, r.sessionKey, prepared.ResolvedModel, prepared.ServiceTier)
	if !r.pl.WebSocketServes(r.accountID, route) {
		if len(r.pending) > 0 || prepared.HasPrevious {
			wsWrite(r.client, websocket.TextMessage, wsErrorFrame(400, "invalid_request_error",
				"changing service tier requires a new connection with the complete input history",
				nil, prepared.StreamID))
			return true
		}
		served, dialErr := r.pl.ExecuteWebSocket(ctx, route,
			r.s.makeWSDial(ctx, r.headers, r.sessionKey, prepared.ResolvedModel))
		if dialErr != nil {
			wsWrite(r.client, websocket.TextMessage, wsErrorFrame(429, "rate_limit_error",
				dialErr.Error(), nil, prepared.StreamID))
			return true
		}
		redialed, ok := served.Response.(*backend.WSConnection)
		if !ok {
			wsWrite(r.client, websocket.TextMessage, wsErrorFrame(502, "api_error",
				"unexpected websocket reply", nil, prepared.StreamID))
			return true
		}
		wsClose(r.upstreamConn(), websocket.CloseNormalClosure, "")
		r.upstream = redialed
		r.accountID = served.AccountID
		r.attempts = served.Attempts
		r.turnStateBlocks = r.s.observeTurnState(ctx, r.pl, served.AccountID, redialed.Headers)
		r.upstreamFailed = false
		// The upstream reader is bound to the old connection; stop it and
		// start a fresh one for the redialed socket.
		redialed.Conn.SetReadLimit(backend.WSMaxMessageSize)
		r.redialReader()
	}

	p := &wsPending{
		rc: auth, requested: prepared.RequestedModel, upstream: prepared.ResolvedModel,
		serviceTier: prepared.ServiceTier, effort: prepared.Effort,
		accountID: r.accountID, attempts: r.attempts, turnStateBlocks: r.turnStateBlocks,
		requestBytes: prepared.RequestBytes, generated: prepared.Generate,
		streamID: prepared.StreamID, started: time.Now(),
	}
	r.pending[prepared.StreamID] = append(r.pending[prepared.StreamID], p)
	r.auth = auth
	if !wsWrite(r.upstreamConn(), websocket.TextMessage, prepared.Encoded) {
		r.upstreamClosed()
		return false
	}
	return true
}

func wsStreamID(raw []byte) string {
	var frame map[string]json.RawMessage
	if json.Unmarshal(raw, &frame) != nil {
		return ""
	}
	v, _ := jsonString(frame["stream_id"])
	return v
}

func (r *wsRelay) handleUpstreamFrame(ctx context.Context, ev wsEvent) bool {
	if ev.typ == websocket.BinaryMessage {
		return wsWrite(r.client, websocket.BinaryMessage, ev.data)
	}
	if ev.typ != websocket.TextMessage {
		return true
	}
	text := string(ev.data)
	var event map[string]any
	if json.Unmarshal(ev.data, &event) == nil {
		if kind, _ := event["type"].(string); kind == "codex.rate_limits" {
			r.pl.RewriteRateLimits(r.accountID, r.auth.user, r.auth.token.PinnedAccount, event)
			text = string(mustMarshal(event))
		} else {
			kind, _ := event["type"].(string)
			if classified, ok := backend.WSResponseErrorFrom(event); ok {
				switch classified.Fault {
				case backend.WSFaultExhausted:
					r.upstreamFailed = true
					if slot := r.pl.Slots().ByID(derefInt(r.accountID)); slot != nil {
						r.pl.Slots().WSExhausted(ctx, slot)
					}
				case backend.WSFaultTransient:
					r.failUpstream(ctx, fmt.Sprintf("websocket error frame %d: %s",
						classified.Status, truncateRunes(text, 300)))
				}
			}
			r.notePending(event, kind)
			if classified, ok := backend.WSResponseErrorFrom(event); ok {
				classified.Normalize(event)
				text = string(mustMarshal(event))
			}
		}
	}
	return wsWrite(r.client, websocket.TextMessage, []byte(restoreReservedNamespace(text)))
}

func truncateRunes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func derefInt(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

func (r *wsRelay) notePending(event map[string]any, kind string) {
	lane, _ := event["stream_id"].(string)
	queue := r.pending[lane]
	if len(queue) == 0 {
		return
	}
	p := queue[0]
	p.responseBytes += int64(len(mustMarshal(event)))
	usage := wsUsage(event)
	p.input = max64(p.input, usage.input)
	p.output = max64(p.output, usage.output)
	p.cacheRead = max64(p.cacheRead, usage.cacheRead)
	p.cacheWrite = max64(p.cacheWrite, usage.cacheWrite)
	p.reasoning = max64(p.reasoning, usage.reasoning)
	if p.ttftMS == 0 {
		p.ttftMS = time.Since(p.started).Milliseconds()
	}
	if kind == "error" {
		p.errorKind = "upstream_rejected"
		p.stopReason = "error"
	}
	if kind == "response.completed" && p.generated {
		p.completed = true
		p.stopReason = "completed"
		if !r.upstreamFailed {
			if slot := r.pl.Slots().ByID(derefInt(r.accountID)); slot != nil {
				r.pl.Slots().WSCompleted(slot)
			}
		}
	}
	switch kind {
	case "response.completed", "response.failed", "response.incomplete", "error":
		if p.stopReason == "" {
			p.stopReason = kind
		}
		r.finishPending(p)
		queue = queue[1:]
		if len(queue) == 0 {
			delete(r.pending, lane)
		} else {
			r.pending[lane] = queue
		}
	}
}

type wsUsageNumbers struct{ input, output, cacheRead, cacheWrite, reasoning int64 }

func wsUsage(event map[string]any) wsUsageNumbers {
	var obj map[string]any
	if resp, ok := event["response"].(map[string]any); ok {
		obj, _ = resp["usage"].(map[string]any)
	}
	if obj == nil {
		obj, _ = event["usage"].(map[string]any)
	}
	get := func(key string) int64 {
		switch v := obj[key].(type) {
		case float64:
			return int64(v)
		case json.Number:
			n, _ := v.Int64()
			return n
		}
		return 0
	}
	out := wsUsageNumbers{input: get("input_tokens"), output: get("output_tokens")}
	if details, ok := obj["input_tokens_details"].(map[string]any); ok {
		if v, ok := details["cached_tokens"].(float64); ok {
			out.cacheRead = int64(v)
		}
	}
	if details, ok := obj["output_tokens_details"].(map[string]any); ok {
		if v, ok := details["reasoning_tokens"].(float64); ok {
			out.reasoning = int64(v)
		}
	}
	return out
}

func (r *wsRelay) finishPending(p *wsPending) {
	if p == nil || p.rc == nil {
		return
	}
	status := http.StatusOK
	if p.errorKind != "" {
		status = http.StatusInternalServerError
	}
	tokens := pricing.Tokens{Input: p.input, Output: p.output, CacheRead: p.cacheRead, CacheWrite: p.cacheWrite}
	var cost, listCost float64
	if r.s.prices != nil {
		cost = r.s.prices.Cost(p.upstream, tokens)
		listCost = r.s.prices.ListCost(p.upstream, tokens)
	}
	if p.rc.token.Meters(provider.OpenAI) && p.rc.meterID > 0 {
		_ = r.s.store.RecordUsageForMeter(context.Background(), p.rc.meterID, p.input, p.output)
	}
	blocks := p.turnStateBlocks
	if err := r.s.store.LogUsage(context.Background(), store.UsageLog{
		User: p.rc.user, Provider: provider.OpenAI.String(), Dialect: "responses",
		RequestedModel: p.requested, UpstreamModel: p.upstream, AccountID: p.accountID,
		Effort: p.effort, ServiceTier: p.serviceTier,
		InputTokens: p.input, OutputTokens: p.output, CacheRead: p.cacheRead,
		CacheWrite: p.cacheWrite, ReasoningTokens: p.reasoning,
		Status: status, ErrorKind: p.errorKind, SessionKey: r.sessionKey,
		RequestBytes: p.requestBytes, ResponseBytes: p.responseBytes,
		TTFTMS: p.ttftMS, StopReason: p.stopReason, Attempts: int64(p.attempts),
		DurationMS: time.Since(p.started).Milliseconds(),
		CostUSD:    cost, ListCostUSD: listCost, TurnStateBlocks: blocks,
	}); err != nil {
		slog.Warn("websocket usage log failed", "error", err)
	}
}

func (r *wsRelay) failUpstream(ctx context.Context, why string) {
	if r.upstreamFailed {
		return
	}
	r.upstreamFailed = true
	if slot := r.pl.Slots().ByID(derefInt(r.accountID)); slot != nil {
		r.pl.Slots().WSFailed(ctx, slot, why)
	}
}

func (r *wsRelay) upstreamClosed() {
	count := len(r.pending)
	for lane, queue := range r.pending {
		for _, p := range queue {
			p.errorKind = "upstream_eof"
			r.finishPending(p)
		}
		delete(r.pending, lane)
	}
	if count > 0 {
		r.failUpstream(context.Background(),
			fmt.Sprintf("websocket closed with %d streams pending", count))
	}
}
