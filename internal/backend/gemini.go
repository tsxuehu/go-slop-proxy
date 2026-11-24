package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/edy/slop-proxy-go/internal/config"
	"github.com/edy/slop-proxy-go/internal/translate"
	"github.com/edy/slop-proxy-go/internal/upstream"
)

var geminiRules = upstream.Classify{
	Pass:         func(s int) bool { return s != 401 && s != 403 && s != 429 && !(s >= 500 && s <= 599) },
	Auth:         []int{401, 403},
	ResetHeaders: []string{"x-ratelimit-reset-requests", "x-ratelimit-reset-tokens"},
	DeadKey:      []string{"API key not valid"},
}

type GeminiClient struct {
	base        string
	native      string
	headers     map[string]string
	pool        *upstream.EgressPool
	retryBudget time.Duration
}

func NewGemini(cfg config.GeminiConfig) (*GeminiClient, error) {
	base := strings.TrimRight(cfg.BaseURL, "/")
	native := base
	if strings.HasSuffix(native, "/openai") {
		native = strings.TrimSuffix(native, "/openai")
	}
	pool, err := upstream.NewEgressPool(cfg.EgressConfig, "gemini", "")
	if err != nil {
		return nil, err
	}
	return &GeminiClient{base: base, native: native, headers: cfg.Headers, pool: pool, retryBudget: time.Duration(cfg.RetryBudgetSecs) * time.Second}, nil
}

func (c *GeminiClient) ReloadEgress() error { return c.pool.Reload() }

func (c *GeminiClient) referer(accountReferer *string) *string {
	if accountReferer != nil && *accountReferer != "" {
		return accountReferer
	}
	for k, v := range c.headers {
		if strings.EqualFold(k, "referer") && v != "" {
			return &v
		}
	}
	return nil
}

func (c *GeminiClient) withHeaders(h map[string]string, hasReferer bool) {
	for k, v := range c.headers {
		if hasReferer && strings.EqualFold(k, "referer") {
			continue
		}
		h[k] = v
	}
}

type GeminiResponse struct {
	Response *http.Response
	Protocol translate.BridgeProtocol
}

// Post sends a Chat request via either the native or OpenAI-compatible surface.
func (c *GeminiClient) Post(ctx context.Context, key string, accountReferer *string, chat *translate.ChatRequest) (*GeminiResponse, *upstream.SendError) {
	ref := c.referer(accountReferer)
	protocol := translate.BridgeChat

	var url, body string
	headers := map[string]string{"Content-Type": "application/json"}
	if ref != nil {
		translated, nativeBody := translateToNative(chat)
		if translated == nil {
			return nil, &upstream.SendError{Kind: upstream.ErrBadRequest, Body: "request cannot be represented on Gemini's native endpoint"}
		}
		protocol = translate.BridgeGeminiNative
		action := "generateContent"
		if chat.Stream != nil && *chat.Stream {
			action = "streamGenerateContent?alt=sse"
		}
		url = c.native + "/models/" + translated.Model + ":" + action
		headers["x-goog-api-key"] = key
		headers["referer"] = *ref
		body = string(nativeBody)
	} else {
		url = c.base + "/chat/completions"
		headers["Authorization"] = "Bearer " + key
		b, _ := json.Marshal(chat)
		body = string(b)
	}
	c.withHeaders(headers, ref != nil)

	build := func(client *http.Client) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
		if err != nil {
			return nil, err
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		return req, nil
	}
	resp, se := c.pool.Send(ctx, build)
	if se != nil {
		return nil, se
	}
	checked, se := upstream.ClassifyResponse(resp, geminiRules)
	if se != nil {
		return nil, se
	}
	return &GeminiResponse{Response: checked, Protocol: protocol}, nil
}

// GeminiNativeRequest is a byte-for-byte passthrough for the native
// generateContent / streamGenerateContent surface. The server does no Chat
// translation here; Google's response is returned unchanged.
type GeminiNativeRequest struct {
	Model  string
	Action string
	Query  string
	Body   []byte
}

// PostNative forwards a raw native-surface request without translation.
func (c *GeminiClient) PostNative(ctx context.Context, key string, referer *string, r *GeminiNativeRequest) (*GeminiResponse, *upstream.SendError) {
	ref := c.referer(referer)
	url := c.native + "/models/" + r.Model + ":" + r.Action
	if r.Query != "" {
		url += "?" + r.Query
	}
	headers := map[string]string{"Content-Type": "application/json"}
	if ref != nil {
		headers["referer"] = *ref
	}
	if key != "" {
		headers["x-goog-api-key"] = key
	}
	c.withHeaders(headers, ref != nil)
	build := func(client *http.Client) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(r.Body))
		if err != nil {
			return nil, err
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		return req, nil
	}
	resp, se := c.pool.Send(ctx, build)
	if se != nil {
		return nil, se
	}
	checked, se := upstream.ClassifyResponse(resp, geminiRules)
	if se != nil {
		return nil, se
	}
	return &GeminiResponse{Response: checked, Protocol: translate.BridgeGeminiNative}, nil
}

func (c *GeminiClient) Models(ctx context.Context, key string, accountReferer *string) ([]GeminiModel, *upstream.SendError) {
	headers := map[string]string{"x-goog-api-key": key}
	if accountReferer != nil {
		headers["referer"] = *accountReferer
	}
	c.withHeaders(headers, accountReferer != nil)
	resp, se := c.pool.Send(ctx, func(_ *http.Client) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.native+"/models?pageSize=1000", nil)
		if err == nil {
			for name, value := range headers {
				req.Header.Set(name, value)
			}
		}
		return req, err
	})
	if se != nil {
		return nil, se
	}
	var list struct {
		Models []struct {
			Name                       *string  `json:"name"`
			SupportedGenerationMethods []string `json:"supportedGenerationMethods"`
			InputTokenLimit            *int64   `json:"inputTokenLimit"`
		} `json:"models"`
	}
	list, se = upstream.JSON[struct {
		Models []struct {
			Name                       *string  `json:"name"`
			SupportedGenerationMethods []string `json:"supportedGenerationMethods"`
			InputTokenLimit            *int64   `json:"inputTokenLimit"`
		} `json:"models"`
	}](resp, upstream.Strict, "/models")
	if se != nil {
		return nil, se
	}
	var out []GeminiModel
	for _, m := range list.Models {
		if m.Name == nil {
			continue
		}
		supported := len(m.SupportedGenerationMethods) == 0
		for _, method := range m.SupportedGenerationMethods {
			if method == "generateContent" {
				supported = true
			}
		}
		if !supported {
			continue
		}
		out = append(out, GeminiModel{ID: strings.TrimPrefix(*m.Name, "models/"), ContextWindow: m.InputTokenLimit})
	}
	return out, nil
}

type GeminiModel struct {
	ID            string
	ContextWindow *int64
}

type nativeRequest struct {
	Model             string           `json:"-"`
	Contents          []nativeContent  `json:"contents"`
	SystemInstruction *nativeContent   `json:"systemInstruction,omitempty"`
	GenerationConfig  json.RawMessage  `json:"generationConfig,omitempty"`
	Tools             []map[string]any `json:"tools,omitempty"`
	ToolConfig        map[string]any   `json:"toolConfig,omitempty"`
}

type nativeContent struct {
	Role  string            `json:"role,omitempty"`
	Parts []json.RawMessage `json:"parts"`
}

// translateToNative performs the Chat → Gemini native request conversion.
func translateToNative(chat *translate.ChatRequest) (*nativeRequest, []byte) {
	model := strings.TrimPrefix(chat.Model, "models/")
	if model == "" {
		return nil, nil
	}
	for _, r := range model {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.') {
			return nil, nil
		}
	}

	nr := &nativeRequest{Model: model}
	callNames := map[string]string{}
	for i := range chat.Messages {
		m := &chat.Messages[i]
		role := m.Role
		if role == "system" || role == "developer" {
			part, _ := json.Marshal(map[string]any{"text": m.Text()})
			si := &nativeContent{Parts: []json.RawMessage{part}}
			nr.SystemInstruction = si
			continue
		}
		if role == "assistant" {
			role = "model"
		}
		var parts []json.RawMessage
		if m.Content != nil {
			if m.Content.Parts == nil && m.Content.Text != "" {
				part, _ := json.Marshal(map[string]string{"text": m.Content.Text})
				parts = append(parts, part)
			}
			for _, part := range m.Content.Parts {
				var nativePart map[string]any
				switch {
				case part.Type == "text":
					nativePart = map[string]any{"text": part.Text}
				case part.ImageURL != nil:
					media, err := geminiMedia(part.ImageURL.Value)
					if err != nil {
						return nil, nil
					}
					nativePart = media
				case part.InputAudio != nil:
					format := "wav"
					if part.InputAudio.Format != nil {
						format = *part.InputAudio.Format
					}
					nativePart = map[string]any{"inlineData": map[string]string{"mimeType": "audio/" + format, "data": part.InputAudio.Data}}
				default:
					continue
				}
				encoded, _ := json.Marshal(nativePart)
				parts = append(parts, encoded)
			}
		}
		if m.ToolCalls != nil {
			for j := range *m.ToolCalls {
				call := &(*m.ToolCalls)[j]
				fn := call.Function
				name, args := "", "{}"
				if fn.Name != nil {
					name = *fn.Name
				}
				if fn.Arguments != nil && *fn.Arguments != "" {
					args = *fn.Arguments
				}
				if call.ID != nil {
					callNames[*call.ID] = name
				}
				var argsObj map[string]any
				_ = json.Unmarshal([]byte(args), &argsObj)
				fc := map[string]any{"name": name, "args": argsObj}
				p := map[string]any{"functionCall": fc}
				if sig := call.ThoughtSignature(); sig != nil {
					p["thoughtSignature"] = *sig
				}
				b, _ := json.Marshal(p)
				parts = append(parts, b)
			}
		}
		if role == "tool" {
			name := ""
			if m.Name != nil {
				name = *m.Name
			}
			if m.ToolCallID != nil && callNames[*m.ToolCallID] != "" {
				name = callNames[*m.ToolCallID]
			}
			if name == "" {
				return nil, nil
			}
			p, _ := json.Marshal(map[string]any{
				"functionResponse": map[string]any{"name": name, "response": map[string]any{"text": m.Text()}},
			})
			parts = []json.RawMessage{p}
			role = "user"
		}
		if len(parts) == 0 {
			p, _ := json.Marshal(map[string]any{"text": ""})
			parts = append(parts, p)
		}
		nr.Contents = append(nr.Contents, nativeContent{Role: role, Parts: parts})
	}

	gc := map[string]any{}
	if chat.Temperature != nil {
		gc["temperature"] = *chat.Temperature
	}
	if chat.TopP != nil {
		gc["topP"] = *chat.TopP
	}
	if chat.MaxCompletionTokens != nil {
		gc["maxOutputTokens"] = *chat.MaxCompletionTokens
	} else if chat.MaxTokens != nil {
		gc["maxOutputTokens"] = *chat.MaxTokens
	}
	if chat.ChoiceCount != nil {
		gc["candidateCount"] = *chat.ChoiceCount
	}
	if chat.ReasoningEffort != nil {
		level := *chat.ReasoningEffort
		if strings.HasPrefix(model, "gemini-3") {
			if level == "none" {
				level = "minimal"
			}
			gc["thinkingConfig"] = map[string]any{"thinkingLevel": level, "includeThoughts": true}
		} else {
			budget := int64(24576)
			switch level {
			case "none":
				budget = 0
			case "minimal", "low":
				budget = 1024
			case "medium":
				budget = 8192
			}
			gc["thinkingConfig"] = map[string]any{"thinkingBudget": budget, "includeThoughts": true}
		}
	}
	if chat.Stop != nil {
		gc["stopSequences"] = chat.Stop.Values
	}
	if chat.ResponseFormat != nil {
		switch chat.ResponseFormat.Kind {
		case "json_object", "json_schema":
			gc["responseMimeType"] = "application/json"
			if chat.ResponseFormat.JSONSchema != nil {
				gc["responseJsonSchema"] = chat.ResponseFormat.JSONSchema.Schema
			}
		}
	}
	gcBytes, _ := json.Marshal(gc)
	nr.GenerationConfig = gcBytes

	if chat.Tools != nil {
		var declarations []map[string]any
		for _, tool := range *chat.Tools {
			def := tool.Def()
			if def == nil || def.Name == "" {
				continue
			}
			declaration := map[string]any{"name": def.Name}
			if def.Description != nil {
				declaration["description"] = *def.Description
			}
			if len(def.Parameters) > 0 {
				declaration["parametersJsonSchema"] = def.Parameters
			}
			declarations = append(declarations, declaration)
		}
		if len(declarations) > 0 {
			nr.Tools = []map[string]any{{"functionDeclarations": declarations}}
		}
	}
	if choice := chat.ToolChoice; choice != nil {
		mode := "AUTO"
		selection := make(map[string]any)
		switch {
		case choice.Name != "":
			mode = "ANY"
			selection["allowedFunctionNames"] = []string{choice.Name}
		case choice.Mode == "required":
			mode = "ANY"
		case choice.Mode == "none":
			mode = "NONE"
		}
		selection["mode"] = mode
		nr.ToolConfig = map[string]any{"functionCallingConfig": selection}
	}

	body, _ := json.Marshal(nr)
	return nr, body
}

func geminiMedia(value string) (map[string]any, error) {
	head, data, inline := strings.Cut(value, ",")
	if strings.HasPrefix(head, "data:") {
		mime, ok := strings.CutSuffix(strings.TrimPrefix(head, "data:"), ";base64")
		if !inline || !ok || mime == "" || data == "" {
			return nil, errors.New("invalid inline media")
		}
		return map[string]any{"inlineData": map[string]string{"mimeType": mime, "data": data}}, nil
	}
	return map[string]any{"fileData": map[string]string{"fileUri": value}}, nil
}
