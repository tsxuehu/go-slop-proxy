package backend

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/edy/slop-proxy-go/internal/config"
	"github.com/edy/slop-proxy-go/internal/pool"
	"github.com/edy/slop-proxy-go/internal/provider"
	"github.com/edy/slop-proxy-go/internal/upstream"
)

type CodexBackend struct {
	cfg    config.CodexConfig
	client *http.Client
	limit  float64
}

func NewCodex(cfg config.CodexConfig) *CodexBackend {
	return &CodexBackend{
		cfg: cfg,
		client: &http.Client{
			Timeout: 0,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		limit: cfg.SoftUtilizationLimit,
	}
}

func (b *CodexBackend) Provider() provider.Provider { return provider.OpenAI }
func (b *CodexBackend) SoftLimit() float64          { return b.limit }
func (b *CodexBackend) Attempts() int               { return 3 }
func (b *CodexBackend) RateLimitMax() int64         { return 6 * 3600 }
func (b *CodexBackend) RateLimitBase() int64        { return 60 }
func (b *CodexBackend) RefreshOnce() bool           { return true }
func (b *CodexBackend) StickyWaitSecs() int64       { return 0 }
func (b *CodexBackend) SessionAffinity() bool       { return true }
func (b *CodexBackend) RetryBudget() time.Duration  { return 0 }
func (b *CodexBackend) RetryableBadRequest(body string) bool {
	return strings.Contains(body, "is not supported")
}

func (b *CodexBackend) Reason(body string) string {
	var envelope struct {
		Detail string `json:"detail"`
		Error  struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal([]byte(body), &envelope) == nil {
		if envelope.Detail != "" {
			return envelope.Detail
		}
		if envelope.Error.Message != "" {
			return envelope.Error.Message
		}
	}
	return body
}

type Request struct {
	Body    []byte
	Headers http.Header
	Path    string
	// SkipModelQuery suppresses the ?model= suffix for upstream endpoints
	// (e.g. /alpha/search) that take the model only in the JSON body while
	// still letting the pool use the model for account ranking.
	SkipModelQuery bool
}

type Response struct {
	StatusCode int
	Header     http.Header
	Body       io.ReadCloser
}

func (b *CodexBackend) UsageFrom(resp any) *pool.AccountUsage {
	var hdr http.Header
	switch v := resp.(type) {
	case *Response:
		if v == nil {
			return nil
		}
		hdr = v.Header
	case interface{ ResponseHeader() http.Header }:
		hdr = v.ResponseHeader()
	default:
		return nil
	}
	if hdr == nil {
		return nil
	}
	get := func(name string) (int64, bool) {
		v := hdr.Get(name)
		if v == "" {
			return 0, false
		}
		n, err := strconv.ParseInt(v, 10, 64)
		return n, err == nil
	}
	var windows []pool.UsageWindow
	for _, tier := range []string{"primary", "secondary"} {
		minutes, okM := get("x-codex-" + tier + "-window-minutes")
		percent, okP := get("x-codex-" + tier + "-used-percent")
		if !okM || !okP || minutes <= 0 {
			continue
		}
		resetsAt, _ := get("x-codex-" + tier + "-reset-at")
		windows = append(windows, pool.UsageWindow{
			Name: windowName(minutes), Utilization: float64(percent) / 100.0, ResetsAt: resetsAt,
		})
	}
	if len(windows) == 0 {
		return nil
	}
	return &pool.AccountUsage{Windows: windows, ObservedAt: time.Now().Unix()}
}

func windowName(minutes int64) string {
	if minutes%1440 == 0 {
		return fmt.Sprintf("%dd", minutes/1440)
	}
	if minutes%60 == 0 {
		return fmt.Sprintf("%dh", minutes/60)
	}
	return fmt.Sprintf("%dm", minutes)
}

func (b *CodexBackend) Send(ctx context.Context, token string, slot *pool.Slot, route pool.Route, req any) (any, error) {
	r, ok := req.(*Request)
	if !ok {
		return nil, fmt.Errorf("unexpected request type")
	}
	url := b.cfg.BaseURL + r.Path
	if route.Model != "" && !r.SkipModelQuery && !strings.Contains(r.Path, "model=") {
		if strings.Contains(url, "?") {
			url += "&model=" + route.Model
		} else {
			url += "?model=" + route.Model
		}
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(r.Body))
	if err != nil {
		return nil, err
	}
	for k, vals := range r.Headers {
		if sensitive(k) {
			continue
		}
		for _, v := range vals {
			httpReq.Header.Add(k, v)
		}
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)
	httpReq.Header.Set("chatgpt-account-id", slot.ProviderAccountID)
	httpReq.Header.Set("originator", b.cfg.Originator)
	if b.cfg.Version != "" {
		httpReq.Header.Set("version", b.cfg.Version)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := b.client.Do(httpReq)
	if err != nil {
		return nil, &pool.SendError{Kind: pool.SendNetwork, Body: err.Error()}
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return refuseEarly(resp)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	text := string(body)
	switch resp.StatusCode {
	case 401:
		return nil, &pool.SendError{Kind: pool.SendAuth, Body: b.Reason(text)}
	case 429:
		retryAfter := upstream.RetryAfterSecs(resp.Header, []string{"x-codex-primary-reset-at"}, time.Now().Unix())
		if strings.Contains(strings.ToLower(text), "model") {
			return nil, &pool.SendError{Kind: pool.SendModelLimited, RetryAfter: retryAfter, Body: b.Reason(text)}
		}
		return nil, &pool.SendError{Kind: pool.SendRateLimited, RetryAfter: retryAfter, Body: b.Reason(text)}
	case 400:
		return nil, &pool.SendError{Kind: pool.SendBadRequest, Body: b.Reason(text)}
	default:
		return nil, &pool.SendError{Kind: pool.SendOther, Body: fmt.Sprintf("upstream %d: %s", resp.StatusCode, b.Reason(text))}
	}
}

func sensitive(k string) bool {
	switch strings.ToLower(k) {
	case "authorization", "x-api-key", "x-goog-api-key", "chatgpt-account-id", "originator", "version", "host", "content-length", "connection", "accept-encoding":
		return true
	default:
		return false
	}
}

// Usage reads the account's quota without spending an inference request.
func (b *CodexBackend) Usage(ctx context.Context, token, accountID string) (*pool.AccountUsage, *upstream.SendError) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(b.cfg.BaseURL, "/")+"/usage", nil)
	if err != nil {
		return nil, upstream.NetworkError(err)
	}
	b.authed(req, token, accountID)
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, upstream.NetworkError(err)
	}
	ok, se := upstream.ClassifyResponse(resp, upstream.Strict)
	if se != nil {
		return nil, se
	}
	defer ok.Body.Close()
	type window struct {
		UsedPercent        float64 `json:"used_percent"`
		LimitWindowSeconds int64   `json:"limit_window_seconds"`
		ResetAt            *int64  `json:"reset_at"`
	}
	var doc struct {
		RateLimit struct {
			LimitReached    bool    `json:"limit_reached"`
			PrimaryWindow   *window `json:"primary_window"`
			SecondaryWindow *window `json:"secondary_window"`
		} `json:"rate_limit"`
	}
	if err := json.NewDecoder(ok.Body).Decode(&doc); err != nil {
		return nil, &upstream.SendError{Kind: upstream.ErrUpstream, Body: err.Error()}
	}
	var windows []pool.UsageWindow
	for _, w := range []*window{doc.RateLimit.PrimaryWindow, doc.RateLimit.SecondaryWindow} {
		if w == nil || w.LimitWindowSeconds <= 0 {
			continue
		}
		var resetAt int64
		if w.ResetAt != nil {
			resetAt = *w.ResetAt
		}
		windows = append(windows, pool.UsageWindow{
			Name: windowName(w.LimitWindowSeconds / 60), Utilization: w.UsedPercent / 100, ResetsAt: resetAt,
		})
	}
	if len(windows) == 0 {
		return nil, nil
	}
	return &pool.AccountUsage{Windows: windows, Locked: doc.RateLimit.LimitReached, ObservedAt: time.Now().Unix()}, nil
}

// CatalogSnapshot is the result of a /models probe: the servable model ids
// plus, for each model, the service tiers the account advertises.
type CodexModel struct {
	Slug            string   `json:"slug"`
	ContextWindow   *int64   `json:"context_window"`
	InputModalities []string `json:"input_modalities"`
	Visibility      *string  `json:"visibility"`
	SupportedInAPI  *bool    `json:"supported_in_api"`
	ServiceTiers    []struct {
		ID string `json:"id"`
	} `json:"service_tiers"`
}

type CatalogSnapshot struct {
	Raw          map[string]json.RawMessage
	Entries      []CodexModel
	Models       []string
	ServiceTiers map[string][]string
}

// Tiered marks the Codex backend as the only one that advertises per-model
// service tiers (Rust Backend::TIERED).
func (b *CodexBackend) Tiered() bool { return true }

// PinTurnState reports whether this account's cleanest turn-state token may
// replace the caller's dirtier one on a websocket handshake.
func (b *CodexBackend) PinTurnState() bool { return b.cfg.PinTurnState }

// Catalog lists the models this account can actually serve, together with the
// service tiers each model advertises.
func (b *CodexBackend) Catalog(ctx context.Context, token, accountID string) (*CatalogSnapshot, *upstream.SendError) {
	url := strings.TrimRight(b.cfg.BaseURL, "/") + "/models?client_version=" + b.cfg.Version
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, upstream.NetworkError(err)
	}
	b.authed(req, token, accountID)
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, upstream.NetworkError(err)
	}
	ok, se := upstream.ClassifyResponse(resp, upstream.Strict)
	if se != nil {
		return nil, se
	}
	defer ok.Body.Close()
	snap := &CatalogSnapshot{ServiceTiers: make(map[string][]string)}
	if err := json.NewDecoder(ok.Body).Decode(&snap.Raw); err != nil {
		return nil, &upstream.SendError{Kind: upstream.ErrUpstream, Body: err.Error()}
	}
	var models []CodexModel
	if err := json.Unmarshal(snap.Raw["models"], &models); err != nil {
		return nil, &upstream.SendError{Kind: upstream.ErrUpstream, Body: err.Error()}
	}
	for _, m := range models {
		if m.Visibility != nil && *m.Visibility == "hidden" {
			continue
		}
		if m.SupportedInAPI != nil && !*m.SupportedInAPI {
			continue
		}
		snap.Entries = append(snap.Entries, m)
		snap.Models = append(snap.Models, m.Slug)
		if len(m.ServiceTiers) > 0 {
			tiers := make([]string, 0, len(m.ServiceTiers))
			for _, t := range m.ServiceTiers {
				if t.ID != "" {
					tiers = append(tiers, t.ID)
				}
			}
			snap.ServiceTiers[m.Slug] = tiers
		}
	}
	return snap, nil
}

func (b *CodexBackend) authed(req *http.Request, token, accountID string) {
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("chatgpt-account-id", accountID)
	req.Header.Set("originator", b.cfg.Originator)
	if b.cfg.Version != "" {
		req.Header.Set("version", b.cfg.Version)
	}
}

// TranslateChat converts an OpenAI chat request to the Codex responses shape.
func TranslateChat(body []byte, instructions string) ([]byte, string, error) {
	var in struct {
		Model    string `json:"model"`
		Messages []struct {
			Role    string `json:"role"`
			Content any    `json:"content"`
		} `json:"messages"`
		Stream      bool     `json:"stream"`
		MaxTokens   int      `json:"max_tokens"`
		Temperature *float64 `json:"temperature"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, "", err
	}
	var items []map[string]any
	for _, m := range in.Messages {
		content := m.Content
		switch v := content.(type) {
		case string:
			role := "user"
			if m.Role == "assistant" {
				role = "assistant"
			} else if m.Role == "system" || m.Role == "developer" {
				continue
			}
			items = append(items, map[string]any{"type": "message", "role": role, "content": v})
		case []any:
			var parts []map[string]any
			for _, p := range v {
				if pm, ok := p.(map[string]any); ok && pm["type"] == "text" {
					parts = append(parts, map[string]any{"type": "input_text", "text": pm["text"]})
				}
			}
			if len(parts) > 0 {
				items = append(items, map[string]any{"type": "message", "role": "user", "content": parts})
			}
		}
	}
	if instructions == "" {
		instructions = "You are a helpful coding assistant."
	}
	out := map[string]any{
		"model":        in.Model,
		"input":        items,
		"instructions": instructions,
		"stream":       in.Stream,
		"store":        false,
	}
	b, err := json.Marshal(out)
	return b, in.Model, err
}

type SSEEvent struct {
	Event string
	Data  string
}

func ReadSSE(r io.Reader, handle func(SSEEvent) error) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var event, data string
	flush := func() error {
		if data == "" {
			return nil
		}
		return handle(SSEEvent{Event: event, Data: strings.TrimPrefix(data, "\n")})
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if err := flush(); err != nil {
				return err
			}
			event, data = "", ""
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		if strings.HasPrefix(line, "event:") {
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			data += strings.TrimPrefix(line, "data:") + "\n"
		}
	}
	if err := flush(); err != nil {
		return err
	}
	return scanner.Err()
}

// SSEToChat translates Codex responses events into OpenAI chat chunks.
func SSEToChat(r io.Reader, w http.ResponseWriter, flusher http.Flusher, model string) (int, int, error) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	var inputTokens, outputTokens int
	send := func(payload map[string]any) {
		b, _ := json.Marshal(payload)
		fmt.Fprintf(w, "data: %s\n\n", b)
		flusher.Flush()
	}
	id := "chatcmpl-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	send(map[string]any{
		"id": id, "object": "chat.completion.chunk", "created": time.Now().Unix(), "model": model,
		"choices": []any{map[string]any{"index": 0, "delta": map[string]any{"role": "assistant"}, "finish_reason": nil}},
	})

	err := ReadSSE(r, func(ev SSEEvent) error {
		switch ev.Event {
		case "response.output_text.delta":
			var d struct {
				Delta string `json:"delta"`
			}
			if json.Unmarshal([]byte(ev.Data), &d) == nil {
				send(map[string]any{
					"id": id, "object": "chat.completion.chunk", "created": time.Now().Unix(), "model": model,
					"choices": []any{map[string]any{"index": 0, "delta": map[string]any{"content": d.Delta}, "finish_reason": nil}},
				})
			}
		case "response.completed":
			var d struct {
				Response struct {
					Usage struct {
						InputTokens  int `json:"input_tokens"`
						OutputTokens int `json:"output_tokens"`
					} `json:"usage"`
				} `json:"response"`
			}
			if json.Unmarshal([]byte(ev.Data), &d) == nil {
				inputTokens = d.Response.Usage.InputTokens
				outputTokens = d.Response.Usage.OutputTokens
			}
			send(map[string]any{
				"id": id, "object": "chat.completion.chunk", "created": time.Now().Unix(), "model": model,
				"choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": "stop"}},
			})
			fmt.Fprint(w, "data: [DONE]\n\n")
			flusher.Flush()
		}
		return nil
	})
	return inputTokens, outputTokens, err
}

// NonStreamChat collects a non-streaming Codex SSE response into one JSON object.
func NonStreamChat(r io.Reader, model string) ([]byte, int, int, error) {
	var text strings.Builder
	var inputTokens, outputTokens int
	err := ReadSSE(r, func(ev SSEEvent) error {
		switch ev.Event {
		case "response.output_text.delta", "response.output_text.done":
			var d struct {
				Delta string `json:"delta"`
				Text  string `json:"text"`
			}
			if json.Unmarshal([]byte(ev.Data), &d) == nil {
				if d.Delta != "" {
					text.WriteString(d.Delta)
				} else {
					text.WriteString(d.Text)
				}
			}
		case "response.completed":
			var d2 struct {
				Response struct {
					Usage struct {
						InputTokens  int `json:"input_tokens"`
						OutputTokens int `json:"output_tokens"`
					} `json:"usage"`
				} `json:"response"`
			}
			if json.Unmarshal([]byte(ev.Data), &d2) == nil {
				inputTokens, outputTokens = d2.Response.Usage.InputTokens, d2.Response.Usage.OutputTokens
			}
		}
		return nil
	})
	if err != nil {
		return nil, 0, 0, err
	}
	out := map[string]any{
		"id":      "chatcmpl-" + strconv.FormatInt(time.Now().UnixNano(), 36),
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []any{map[string]any{
			"index":         0,
			"message":       map[string]any{"role": "assistant", "content": text.String()},
			"finish_reason": "stop",
		}},
		"usage": map[string]any{"prompt_tokens": inputTokens, "completion_tokens": outputTokens, "total_tokens": inputTokens + outputTokens},
	}
	b, err := json.Marshal(out)
	return b, inputTokens, outputTokens, err
}

var _ = slog.Debug
