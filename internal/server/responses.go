package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/edy/slop-proxy-go/internal/backend"
	"github.com/edy/slop-proxy-go/internal/config"
	"github.com/edy/slop-proxy-go/internal/pool"
	"github.com/edy/slop-proxy-go/internal/provider"
	"github.com/edy/slop-proxy-go/internal/translate"
	"github.com/google/uuid"
)

type responsesUpstream struct {
	body      io.ReadCloser
	header    http.Header
	status    int
	accountID *int64
	attempts  int64
	events    backend.Events
	bridged   bool
}

func (s *Server) executeResponses(ctx context.Context, target provider.Provider, route pool.Route,
	request *translate.ResponsesRequest, encoded []byte, headers http.Header, capture *translate.UsageCapture) (*responsesUpstream, error) {
	pl := s.pools[target]
	if pl == nil {
		return nil, &pool.PoolError{Kind: pool.ErrNoAccounts, Message: "no usable " + target.String() + " backend"}
	}
	useChat := target == provider.Gemini ||
		(target == provider.Zen && s.cfg.Models.ZenDialect(route.Model) == config.ZenChat)
	var payload any
	var custom map[string]bool
	switch {
	case useChat:
		chat, tools := translate.ResponsesToChat(request)
		custom = tools
		if target == provider.Gemini {
			payload = chat
		} else {
			body := mustMarshal(chat)
			if gated := backend.SatisfyChatToolGate(body); gated != nil {
				body = gated
			}
			payload = &backend.RelayRequest{Path: "/chat/completions", Body: body}
		}
	case target == provider.OpenAI:
		if encoded == nil {
			encoded = mustMarshal(request)
		}
		payload = &backend.Request{Body: encoded, Headers: headers, Path: "/responses"}
	case target == provider.Zen:
		if encoded == nil {
			encoded = mustMarshal(request)
		}
		if gated := backend.SatisfyToolGate(encoded); gated != nil {
			encoded = gated
		}
		payload = &backend.RelayRequest{Path: "/responses", Body: encoded}
	default:
		return nil, &pool.PoolError{Kind: pool.ErrBadRequest, Message: "this model is not served over the Responses API"}
	}

	served, err := pl.Execute(ctx, route, payload)
	if err != nil {
		return nil, err
	}
	var response *http.Response
	protocol := translate.BridgeChat
	switch value := served.Response.(type) {
	case *backend.Response:
		response = &http.Response{StatusCode: value.StatusCode, Header: value.Header, Body: value.Body}
	case *http.Response:
		response = value
	case *backend.GeminiResponse:
		response, protocol = value.Response, value.Protocol
	default:
		return nil, &pool.PoolError{Kind: pool.ErrUpstream, Message: "unexpected upstream response"}
	}

	if useChat && (response.StatusCode < 200 || response.StatusCode >= 300) {
		defer response.Body.Close()
		data, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		return nil, &pool.PoolError{Kind: pool.ErrBadRequest, Message: translate.ChatReason(string(data)), Attempts: served.Attempts}
	}
	events := backend.ResponsesEvents(response.Body, capture)
	if useChat {
		events = backend.ChatEvents(response, protocol, route.Model, custom, capture)
	}
	return &responsesUpstream{
		body: response.Body, header: response.Header, status: response.StatusCode,
		accountID: served.AccountID, attempts: int64(served.Attempts), events: events, bridged: useChat,
	}, nil
}

func (s *Server) responses(w http.ResponseWriter, r *http.Request, rc *requestContext) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeAPIError(w, dialectOpenAI, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		writeAPIError(w, dialectOpenAI, http.StatusBadRequest, "invalid_request_error", "invalid Responses request")
		return
	}
	requested, _ := jsonString(fields["model"])
	if requested == "" {
		requested = s.cfg.Models.Default
	}
	resolved := translate.Resolve(&s.cfg, requested)
	target := s.cfg.Models.Route(resolved.Model)
	if !s.admitModel(w, r, rc, resolved.Model, target) {
		return
	}
	if target != provider.OpenAI && target != provider.Gemini &&
		(target != provider.Zen || s.cfg.Models.ZenDialect(resolved.Model) == config.ZenMessages) {
		writeAPIError(w, dialectOpenAI, http.StatusBadRequest, "invalid_request_error", "this model is not served over the Responses API")
		return
	}
	var clientStreams bool
	if raw, ok := fields["stream"]; ok && json.Unmarshal(raw, &clientStreams) != nil {
		writeAPIError(w, dialectOpenAI, http.StatusBadRequest, "invalid_request_error", "stream must be a boolean")
		return
	}
	fields["model"] = jsonStringValue(resolved.Model)
	fields["stream"] = json.RawMessage("true")
	fields["store"] = json.RawMessage("false")
	delete(fields, "background")
	if _, ok := fields["instructions"]; !ok && target == provider.OpenAI {
		fields["instructions"] = jsonStringValue(s.codexInstructions())
	}
	var reasoning map[string]json.RawMessage
	_ = json.Unmarshal(fields["reasoning"], &reasoning)
	effort, _ := jsonString(reasoning["effort"])
	if effort == "" && resolved.Effort != "" {
		if reasoning == nil {
			reasoning = make(map[string]json.RawMessage)
		}
		effort = translate.ClampEffort(resolved.Model, resolved.Effort)
		reasoning["effort"] = jsonStringValue(effort)
		fields["reasoning"] = mustMarshal(reasoning)
	}
	var cap uint64
	if json.Unmarshal(fields["max_output_tokens"], &cap) == nil && cap < 16 {
		delete(fields, "max_output_tokens")
	}

	encoded := mustMarshal(fields)
	var ir *translate.ResponsesRequest
	if target == provider.Gemini || (target == provider.Zen && s.cfg.Models.ZenDialect(resolved.Model) == config.ZenChat) {
		if text, ok := jsonString(fields["input"]); ok {
			fields["input"] = mustMarshal([]translate.InputItem{{Type: "message", Role: "user",
				Content: []translate.ContentPart{{Type: "input_text", Text: text}}}})
		}
		ir = new(translate.ResponsesRequest)
		if err := json.Unmarshal(mustMarshal(fields), ir); err != nil {
			writeAPIError(w, dialectOpenAI, http.StatusBadRequest, "invalid_request_error", "invalid Responses input: "+err.Error())
			return
		}
	}

	route := s.route(r, body, rc, resolved.Model)
	if cacheKey, _ := jsonString(fields["prompt_cache_key"]); rc.sessionKey == "" && cacheKey != "" {
		route.SessionKey, rc.sessionKey = cacheKey, cacheKey
	}
	if route.SessionKey == "" {
		route.SessionKey, rc.sessionKey = rc.user, rc.user
	}
	if target == provider.OpenAI {
		encoded = renameReservedNamespace(encoded)
	}
	scan := newResponsesUsage()
	reply, err := s.executeResponses(r.Context(), target, route, ir, encoded, r.Header, scan.capture)
	if err != nil {
		s.logDispatchFailure(rc, target, requested, resolved.Model, "responses", body, err)
		writePoolError(w, err)
		return
	}
	defer reply.body.Close()

	status := s.writeResponsesReply(w, rc, target, resolved.Model, clientStreams, reply, scan)
	snap := scan.snapshot()
	s.recordUsage(rc, target, requested, resolved.Model, "responses", reply.accountID, snap.InputTokens, snap.OutputTokens,
		UsageMeta{Body: body, Captured: snap, Effort: effort, Status: status, Attempts: reply.attempts, Force: true})
}

func (s *Server) writeResponsesReply(w http.ResponseWriter, rc *requestContext, target provider.Provider,
	model string, streaming bool, reply *responsesUpstream, scan *responsesUsage) int {
	if reply.bridged {
		return writeBridgedResponses(w, model, streaming, reply.events, scan.capture)
	}
	eventStream := strings.Contains(strings.ToLower(reply.header.Get("Content-Type")), "text/event-stream")
	if streaming && eventStream {
		w.Header().Set("Content-Type", "text/event-stream")
		for _, name := range backend.WSEchoHeaders() {
			if value := reply.header.Get(name); strings.HasPrefix(name, "x-codex-safety-") && value != "" {
				w.Header().Set(name, value)
			}
		}
		if target == provider.OpenAI {
			windows := s.pools[target].Slots().PoolWindows(rc.user, rc.token.PinnedAccount, s.cfg.Codex.SoftUtilizationLimit)
			for name, values := range responsesRateLimitHeaders(windows) {
				w.Header()[name] = values
			}
		}
		w.WriteHeader(reply.status)
		if target == provider.OpenAI {
			streamResponseFrames(w, reply.body, scan)
		} else {
			streamObserved(w, reply.body, scan, reply.status)
		}
		return reply.status
	}

	var body []byte
	var err error
	if eventStream {
		body, err = finalResponseJSON(reply.body, scan)
	} else {
		body, err = io.ReadAll(reply.body)
		if err == nil {
			scan.applyJSON(body)
		}
	}
	if err != nil {
		scan.capture.Fail("upstream_read")
		writeAPIError(w, dialectOpenAI, http.StatusBadGateway, "api_error", "reading upstream response: "+err.Error())
		return http.StatusBadGateway
	}
	if len(body) == 0 {
		scan.capture.Fail("upstream_eof")
		writeAPIError(w, dialectOpenAI, http.StatusBadGateway, "api_error", "upstream stream ended unexpectedly")
		return http.StatusBadGateway
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(reply.status)
	_, _ = io.WriteString(w, restoreReservedNamespace(string(body)))
	return reply.status
}

func streamResponseFrames(w http.ResponseWriter, body io.Reader, scan *responsesUsage) {
	flusher, _ := w.(http.Flusher)
	disconnected := false
	err := translate.ParseSSE(body, func(event, data string) error {
		size := len(data) + len("data: \n\n") + strings.Count(data, "\n")*len("data: ")
		if event != "" {
			size += len(event) + len("event: \n")
		}
		scan.noteBytes(size)
		scan.observe(data)
		if err := backend.WriteSSE(w, event, restoreReservedNamespace(data)); err != nil {
			disconnected = true
			return err
		}
		if flusher != nil {
			flusher.Flush()
		}
		return nil
	})
	switch {
	case disconnected:
		scan.disconnect()
	case err != nil:
		scan.streamError()
	default:
		scan.finish(http.StatusOK)
	}
}

func finalResponseJSON(body io.Reader, scan *responsesUsage) ([]byte, error) {
	var result json.RawMessage
	err := translate.ParseSSE(body, func(_ string, data string) error {
		scan.observe(data)
		var terminal struct {
			Type     string          `json:"type"`
			Response json.RawMessage `json:"response"`
		}
		if json.Unmarshal([]byte(data), &terminal) == nil && len(terminal.Response) > 0 &&
			string(terminal.Response) != "null" {
			switch terminal.Type {
			case translate.EvCompleted, translate.EvIncomplete, translate.EvFailed:
				result = terminal.Response
			}
		}
		return nil
	})
	if err == nil {
		scan.capture.NoteUpstreamEOF()
	}
	scan.noteBytes(len(result))
	return result, err
}

func writeBridgedResponses(w http.ResponseWriter, model string, stream bool, events backend.Events, capture *translate.UsageCapture) int {
	if stream {
		flusher, _ := w.(http.Flusher)
		backend.BeginSSE(w)
		if err := events(func(event *translate.ResponsesEvent) error {
			capture.Observe(event)
			if err := backend.WriteSSE(w, event.Type, string(mustMarshal(event))); err != nil {
				capture.Fail("client_disconnect")
				return err
			}
			if flusher != nil {
				flusher.Flush()
			}
			return nil
		}); err != nil {
			capture.Fail("upstream_stream_error")
		} else {
			capture.NoteUpstreamEOF()
			if !capture.Snapshot().Completed {
				capture.Fail("upstream_eof")
			}
		}
		return http.StatusOK
	}

	var final *translate.ResponseObj
	items := make(map[uint64]*translate.OutputItem)
	err := events(func(event *translate.ResponsesEvent) error {
		capture.Observe(event)
		if kind, response, ok := event.Terminal(); ok && response != nil {
			value := *response
			status := kind.AsStr()
			value.Status = &status
			final = &value
		}
		if event.Type == translate.EvOutputItemDone && event.Item != nil {
			items[event.OutputIndex] = event.Item
		}
		return nil
	})
	if err != nil || final == nil {
		capture.Fail("upstream_eof")
		writeAPIError(w, dialectOpenAI, http.StatusBadGateway, "api_error", "upstream stream ended unexpectedly")
		return http.StatusBadGateway
	}
	if final.ID == nil || *final.ID == "" {
		id := "resp_" + uuid.NewString()
		final.ID = &id
	}
	indexes := make([]uint64, 0, len(items))
	for index := range items {
		indexes = append(indexes, index)
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i] < indexes[j] })
	output := make([]*translate.OutputItem, 0, len(indexes))
	for _, index := range indexes {
		output = append(output, items[index])
	}
	result := struct {
		*translate.ResponseObj
		Object string                  `json:"object"`
		Model  string                  `json:"model"`
		Output []*translate.OutputItem `json:"output"`
	}{final, "response", model, output}
	writeJSON(w, result)
	return http.StatusOK
}

func (s *Server) logDispatchFailure(rc *requestContext, p provider.Provider, requested, model, dialect string, body []byte, err error) {
	s.recordUsage(rc, p, requested, model, dialect, nil, 0, 0, UsageMeta{
		Body: body, Force: true, Status: poolErrorStatus(err), ErrorKind: poolErrorKind(err), Attempts: poolErrorAttempts(err),
	})
}

func writePoolErrorFor(w http.ResponseWriter, d dialect, err error) {
	if d == dialectAnthropic {
		writeAnthropicPoolError(w, err)
	} else {
		writePoolError(w, err)
	}
}
