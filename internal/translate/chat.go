package translate

import (
	"bytes"
	"encoding/json"
	"strings"
)

// ---------- Chat request wire ----------

type ChatRequest struct {
	Model               string          `json:"model"`
	Messages            []ChatMessage   `json:"messages"`
	Tools               *[]ChatToolDef  `json:"tools,omitempty"`
	ToolChoice          *ChatToolChoice `json:"tool_choice,omitempty"`
	ParallelToolCalls   *bool           `json:"parallel_tool_calls,omitempty"`
	ReasoningEffort     *string         `json:"reasoning_effort,omitempty"`
	MaxTokens           *uint64         `json:"max_tokens,omitempty"`
	MaxCompletionTokens *uint64         `json:"max_completion_tokens,omitempty"`
	Temperature         *float64        `json:"temperature,omitempty"`
	TopP                *float64        `json:"top_p,omitempty"`
	ChoiceCount         *uint64         `json:"n,omitempty"`
	PresencePenalty     *float64        `json:"presence_penalty,omitempty"`
	FrequencyPenalty    *float64        `json:"frequency_penalty,omitempty"`
	Seed                *int64          `json:"seed,omitempty"`
	Stop                *StopSequences  `json:"stop,omitempty"`
	ResponseFormat      *ResponseFormat `json:"response_format,omitempty"`
	Stream              *bool           `json:"stream,omitempty"`
	StreamOptions       *StreamOptions  `json:"stream_options,omitempty"`
}

func (c *ChatRequest) IncludeUsage() bool {
	return c.StreamOptions != nil && c.StreamOptions.IncludeUsage
}

type StreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type StopSequences struct {
	Values []string
}

func (s *StopSequences) UnmarshalJSON(data []byte) error {
	var single string
	if json.Unmarshal(data, &single) == nil {
		s.Values = []string{single}
		return nil
	}
	return json.Unmarshal(data, &s.Values)
}

func (s StopSequences) MarshalJSON() ([]byte, error) { return json.Marshal(s.Values) }

type ResponseFormat struct {
	Kind       string            `json:"type"`
	JSONSchema *JsonSchemaFormat `json:"json_schema,omitempty"`
}

type JsonSchemaFormat struct {
	Name   *string         `json:"name,omitempty"`
	Schema json.RawMessage `json:"schema,omitempty"`
	Strict *bool           `json:"strict,omitempty"`
}

type ChatMessage struct {
	Role             string          `json:"role"`
	Content          *ChatContent    `json:"content,omitempty"`
	ToolCalls        *[]ChatToolCall `json:"tool_calls,omitempty"`
	ToolCallID       *string         `json:"tool_call_id,omitempty"`
	Name             *string         `json:"name,omitempty"`
	ReasoningContent *string         `json:"reasoning_content,omitempty"`
	Images           *[]ChatPart     `json:"images,omitempty"`
}

// Text returns the concatenation of all text parts, ignoring images/audio.
func (m *ChatMessage) Text() string {
	if m.Content == nil {
		return ""
	}
	return m.Content.TextValue()
}

type ChatContent struct {
	Text  string
	Parts []ChatPart
}

func (c *ChatContent) UnmarshalJSON(data []byte) error {
	var s string
	if json.Unmarshal(data, &s) == nil {
		c.Text = s
		c.Parts = nil
		return nil
	}
	if err := json.Unmarshal(data, &c.Parts); err != nil {
		return err
	}
	c.Text = ""
	return nil
}

func (c ChatContent) MarshalJSON() ([]byte, error) {
	if c.Parts == nil {
		return json.Marshal(c.Text)
	}
	return json.Marshal(c.Parts)
}

func (c *ChatContent) TextValue() string {
	if c == nil {
		return ""
	}
	if c.Parts == nil {
		return c.Text
	}
	var sb strings.Builder
	for _, p := range c.Parts {
		if p.Type == "text" && p.Text != "" {
			sb.WriteString(p.Text)
		}
	}
	return sb.String()
}

type ChatPart struct {
	Type       string     `json:"type"`
	Text       string     `json:"text,omitempty"`
	ImageURL   *ImageRef  `json:"image_url,omitempty"`
	InputAudio *AudioData `json:"input_audio,omitempty"`
}

func (p *ChatPart) UnmarshalJSON(data []byte) error {
	var raw struct {
		Type       string          `json:"type"`
		Text       string          `json:"text"`
		ImageURL   json.RawMessage `json:"image_url"`
		Image      json.RawMessage `json:"image"`
		InputAudio *AudioData      `json:"input_audio"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	p.Type, p.Text, p.InputAudio = raw.Type, raw.Text, raw.InputAudio
	switch raw.Type {
	case "text", "input_text":
		p.Type = "text"
	case "image_url", "input_image", "image":
		p.Type = "image_url"
		data := raw.ImageURL
		if len(data) == 0 {
			data = raw.Image
		}
		if len(data) > 0 {
			p.ImageURL = &ImageRef{}
			if err := json.Unmarshal(data, p.ImageURL); err != nil {
				p.ImageURL = nil
			}
		}
	}
	return nil
}

type ImageRef struct {
	Value string
}

func (i *ImageRef) UnmarshalJSON(data []byte) error {
	var s string
	if json.Unmarshal(data, &s) == nil {
		i.Value = s
		return nil
	}
	var obj struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return err
	}
	i.Value = obj.URL
	return nil
}

func (i ImageRef) MarshalJSON() ([]byte, error) { return json.Marshal(i.Value) }

type AudioData struct {
	Data   string  `json:"data"`
	Format *string `json:"format,omitempty"`
}

type ChatToolCall struct {
	ID       *string       `json:"id,omitempty"`
	Index    *uint64       `json:"index,omitempty"`
	Kind     *string       `json:"type,omitempty"`
	Function FunctionBody  `json:"function"`
	Extra    *ExtraContent `json:"extra_content,omitempty"`
}

type FunctionBody struct {
	Name      *string `json:"name,omitempty"`
	Arguments *string `json:"arguments,omitempty"`
}

type ExtraContent struct {
	Google *GoogleExtra `json:"google,omitempty"`
}

type GoogleExtra struct {
	ThoughtSignature *string `json:"thought_signature,omitempty"`
}

func (c *ChatToolCall) ThoughtSignature() *string {
	if c.Extra != nil && c.Extra.Google != nil {
		return c.Extra.Google.ThoughtSignature
	}
	return nil
}

type ChatToolDef struct {
	Kind     *string      `json:"type,omitempty"`
	Function *FunctionDef `json:"function,omitempty"`
	Flat     *FunctionDef `json:"-"`
}

type FunctionDef struct {
	Name        string          `json:"name"`
	Description *string         `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
	Strict      *bool           `json:"strict,omitempty"`
}

func (d *ChatToolDef) Def() *FunctionDef {
	if d.Function != nil {
		return d.Function
	}
	return d.Flat
}

func (d *ChatToolDef) UnmarshalJSON(data []byte) error {
	var raw struct {
		Kind        *string         `json:"type"`
		Function    *FunctionDef    `json:"function"`
		Name        string          `json:"name"`
		Description *string         `json:"description"`
		Parameters  json.RawMessage `json:"parameters"`
		Strict      *bool           `json:"strict"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	d.Kind, d.Function = raw.Kind, raw.Function
	if raw.Name != "" || len(raw.Parameters) > 0 || raw.Description != nil {
		d.Flat = &FunctionDef{Name: raw.Name, Description: raw.Description, Parameters: bufferedRaw(raw.Parameters), Strict: raw.Strict}
	}
	return nil
}

type ChatToolChoice struct {
	Mode string
	Name string
}

func (c *ChatToolChoice) UnmarshalJSON(data []byte) error {
	var s string
	if json.Unmarshal(data, &s) == nil {
		c.Mode, c.Name = s, ""
		return nil
	}
	var obj struct {
		Function *struct {
			Name *string `json:"name"`
		} `json:"function"`
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return err
	}
	c.Mode = ""
	if obj.Function != nil && obj.Function.Name != nil {
		c.Name = *obj.Function.Name
	}
	return nil
}

func (c ChatToolChoice) MarshalJSON() ([]byte, error) {
	if c.Mode != "" {
		return json.Marshal(c.Mode)
	}
	return json.Marshal(map[string]any{"type": "function", "function": map[string]string{"name": c.Name}})
}

func ChatToolChoiceFunction(name string) *ChatToolChoice {
	return &ChatToolChoice{Name: name}
}

// bufferedRaw normalizes RawValue so it can survive flatten buffers.
func bufferedRaw(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return nil
	}
	b, _ := json.Marshal(v)
	return b
}

// ---------- Chat response wire ----------

type ChatUsage struct {
	PromptTokens            int64                    `json:"prompt_tokens"`
	CompletionTokens        int64                    `json:"completion_tokens"`
	TotalTokens             int64                    `json:"total_tokens"`
	PromptTokensDetails     *PromptTokensDetails     `json:"prompt_tokens_details,omitempty"`
	CompletionTokensDetails *CompletionTokensDetails `json:"completion_tokens_details,omitempty"`
}

type PromptTokensDetails struct {
	CachedTokens     int64 `json:"cached_tokens"`
	CacheWriteTokens int64 `json:"cache_write_tokens,omitempty"`
}

type CompletionTokensDetails struct {
	ReasoningTokens int64 `json:"reasoning_tokens"`
}

func ChatUsageFrom(u *Usage) ChatUsage {
	if u == nil {
		return ChatUsage{}
	}
	return ChatUsage{
		PromptTokens:     u.InputTokens,
		CompletionTokens: u.OutputTokens,
		TotalTokens:      u.InputTokens + u.OutputTokens,
		PromptTokensDetails: &PromptTokensDetails{
			CachedTokens:     u.InputTokensDetails.CachedTokens,
			CacheWriteTokens: u.InputTokensDetails.CacheWriteTokens,
		},
		CompletionTokensDetails: &CompletionTokensDetails{
			ReasoningTokens: u.OutputTokensDetails.ReasoningTokens,
		},
	}
}

func UsageFromChat(u *ChatUsage) Usage {
	if u == nil {
		return Usage{}
	}
	billedOutput := u.TotalTokens - u.PromptTokens
	if u.CompletionTokens > billedOutput {
		billedOutput = u.CompletionTokens
	}
	if billedOutput < 0 {
		billedOutput = 0
	}
	reasoning := int64(0)
	if u.CompletionTokensDetails != nil {
		reasoning = u.CompletionTokensDetails.ReasoningTokens
	}
	if reasoning == 0 && billedOutput > u.CompletionTokens {
		reasoning = billedOutput - u.CompletionTokens
	}
	var cached, written int64
	if u.PromptTokensDetails != nil {
		cached = u.PromptTokensDetails.CachedTokens
		written = u.PromptTokensDetails.CacheWriteTokens
	}
	return Usage{
		InputTokens: u.PromptTokens, OutputTokens: billedOutput,
		TotalTokens:         u.PromptTokens + billedOutput,
		InputTokensDetails:  TokenDetails{CachedTokens: cached, CacheWriteTokens: written},
		OutputTokensDetails: TokenDetails{ReasoningTokens: reasoning},
	}
}

type ChatCompletion struct {
	ID      string       `json:"id"`
	Object  string       `json:"object"`
	Created int64        `json:"created"`
	Model   string       `json:"model"`
	Choices []ChatChoice `json:"choices"`
	Usage   *ChatUsage   `json:"usage,omitempty"`
}

type ChatChoice struct {
	Index        uint64      `json:"index"`
	Message      ChatMessage `json:"message"`
	FinishReason *string     `json:"finish_reason"`
	Logprobs     any         `json:"logprobs"`
}

type ChatChunk struct {
	ID      string         `json:"id"`
	Object  string         `json:"object"`
	Created int64          `json:"created"`
	Model   string         `json:"model"`
	Choices []ChunkChoice  `json:"choices"`
	Usage   *ChatUsage     `json:"usage,omitempty"`
	Error   *ChatErrorBody `json:"error,omitempty"`
}

type ChunkChoice struct {
	Index        uint64    `json:"index"`
	Delta        ChatDelta `json:"delta"`
	FinishReason *string   `json:"finish_reason"`
	Logprobs     any       `json:"logprobs"`
}

type ChatDelta struct {
	Role             string          `json:"role,omitempty"`
	Content          string          `json:"content,omitempty"`
	ReasoningContent string          `json:"reasoning_content,omitempty"`
	ToolCalls        *[]ChatToolCall `json:"tool_calls,omitempty"`
	Images           *[]ChatPart     `json:"images,omitempty"`
}

type ChatError struct {
	Error ChatErrorBody `json:"error"`
}

type ChatErrorBody struct {
	Message string          `json:"message"`
	Kind    *string         `json:"type,omitempty"`
	Code    json.RawMessage `json:"code"` // no omitempty: null when absent
}

func ChatReason(body string) string {
	var e ChatError
	if json.Unmarshal([]byte(body), &e) == nil && e.Error.Message != "" {
		return e.Error.Message
	}
	return body
}

var _ = bytes.MinRead
