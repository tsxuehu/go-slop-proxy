package translate

import (
	"encoding/base64"
	"encoding/json"
	"strings"
)

// ---------- ResponsesRequest (IR) ----------

type ResponsesRequest struct {
	Model             string           `json:"model"`
	Instructions      string           `json:"instructions,omitempty"`
	Input             []InputItem      `json:"input"`
	Tools             []ToolDef        `json:"tools,omitempty"`
	ToolChoice        *ToolChoice      `json:"tool_choice,omitempty"`
	ParallelToolCalls *bool            `json:"parallel_tool_calls,omitempty"`
	Reasoning         *ReasoningConfig `json:"reasoning,omitempty"`
	MaxOutputTokens   *uint64          `json:"max_output_tokens,omitempty"`
	Store             bool             `json:"store"`
	Stream            bool             `json:"stream"`
	Include           []string         `json:"include,omitempty"`
	PromptCacheKey    *string          `json:"prompt_cache_key,omitempty"`
	ServiceTier       *string          `json:"service_tier,omitempty"`
}

type ReasoningConfig struct {
	Effort  string `json:"effort,omitempty"`
	Summary string `json:"summary,omitempty"`
}

func NewResponses(model, instructions string) ResponsesRequest {
	return ResponsesRequest{
		Model: model, Instructions: instructions,
		Store: false, Stream: true,
		Include: []string{"reasoning.encrypted_content"},
	}
}

type InputItem struct {
	Type string `json:"type"`

	// message
	Role    string        `json:"role,omitempty"`
	Content []ContentPart `json:"content,omitempty"`

	// function_call / custom_tool_call
	CallID    string `json:"call_id,omitempty"`
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"` // function_call
	Input     string `json:"input,omitempty"`     // custom_tool_call

	// function_call_output / custom_tool_call_output
	Output *ToolOutput `json:"output,omitempty"`

	// reasoning
	ID               *string       `json:"id,omitempty"`
	Summary          []SummaryPart `json:"summary,omitempty"`
	EncryptedContent *string       `json:"encrypted_content,omitempty"`

	// additional_tools
	Tools []ToolDef `json:"tools,omitempty"`
}

type ContentPart struct {
	Type     string `json:"type"`
	Text     string `json:"text,omitempty"`
	ImageURL string `json:"image_url,omitempty"`
}

type ToolOutput struct {
	Text  string
	Parts []ContentPart
}

func (t ToolOutput) TextValue() string {
	if t.Parts == nil {
		return t.Text
	}
	var sb strings.Builder
	for _, p := range t.Parts {
		if p.Text != "" {
			sb.WriteString(p.Text)
		}
	}
	return sb.String()
}

func (t ToolOutput) MarshalJSON() ([]byte, error) {
	if t.Parts == nil {
		return json.Marshal(t.Text)
	}
	return json.Marshal(t.Parts)
}

type SummaryPart struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

type ToolDef struct {
	Kind        string          `json:"type"`
	Name        string          `json:"name,omitempty"`
	Description *string         `json:"description,omitempty"`
	Strict      bool            `json:"strict"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

func EmptySchema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{}}`)
}

func FunctionTool(name string, desc *string, params json.RawMessage) ToolDef {
	if len(params) == 0 {
		params = EmptySchema()
	}
	return ToolDef{Kind: "function", Name: name, Description: desc, Strict: false, Parameters: params}
}

type ToolChoice struct {
	Mode string // "auto"/"none"/"required"/"" 当 mode 为空时用 Function
	Name string
}

func (t ToolChoice) MarshalJSON() ([]byte, error) {
	if t.Mode != "" {
		return json.Marshal(t.Mode)
	}
	return json.Marshal(map[string]any{"type": "function", "function": map[string]string{"name": t.Name}})
}

// ---------- ResponsesEvent (IR) ----------

type ResponsesEvent struct {
	Type string `json:"type"`

	// created / in_progress / completed / incomplete / failed
	Response *ResponseObj `json:"response,omitempty"`

	// output_item.added / done
	OutputIndex uint64      `json:"output_index,omitempty"`
	Item        *OutputItem `json:"item,omitempty"`

	// content_part.added
	ItemID       *string            `json:"item_id,omitempty"`
	ContentIndex uint64             `json:"content_index,omitempty"`
	Part         *OutputContentPart `json:"part,omitempty"`

	// delta 事件
	Delta     string `json:"delta,omitempty"`
	Text      string `json:"text,omitempty"`
	Arguments string `json:"arguments,omitempty"`
	Input     string `json:"input,omitempty"`
}

type ResponseObj struct {
	ID     *string        `json:"id,omitempty"`
	Status *string        `json:"status,omitempty"`
	Usage  *Usage         `json:"usage,omitempty"`
	Error  *UpstreamError `json:"error"`
}

type UpstreamError struct {
	Code    *string `json:"code"`
	Message *string `json:"message"`
}

type OutputItem struct {
	Type string `json:"type"`

	// message
	ID      *string              `json:"id,omitempty"`
	Role    *string              `json:"role,omitempty"`
	Status  *string              `json:"status,omitempty"`
	Content *[]OutputContentPart `json:"content,omitempty"`

	// reasoning
	Summary          *[]SummaryPart `json:"summary,omitempty"`
	EncryptedContent *string        `json:"encrypted_content,omitempty"`

	// function_call / custom_tool_call
	CallID    string  `json:"call_id,omitempty"`
	Name      string  `json:"name,omitempty"`
	Arguments *string `json:"arguments,omitempty"`
	InputVal  string  `json:"-"`
}

func (o OutputItem) MarshalJSON() ([]byte, error) {
	type alias OutputItem
	b, err := json.Marshal((*alias)(&o))
	if err != nil {
		return nil, err
	}
	if o.Type == "custom_tool_call" {
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		m["input"] = o.InputVal
		return json.Marshal(m)
	}
	return b, nil
}

type OutputContentPart struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

type Usage struct {
	InputTokens         int64        `json:"input_tokens"`
	OutputTokens        int64        `json:"output_tokens"`
	TotalTokens         int64        `json:"total_tokens"`
	InputTokensDetails  TokenDetails `json:"input_tokens_details"`
	OutputTokensDetails TokenDetails `json:"output_tokens_details"`
}

type TokenDetails struct {
	CachedTokens     int64 `json:"cached_tokens"`
	CacheWriteTokens int64 `json:"cache_write_tokens"`
	ReasoningTokens  int64 `json:"reasoning_tokens"`
}

// event kind strings (the wire "type" values)
const (
	EvCreated                    = "response.created"
	EvInProgress                 = "response.in_progress"
	EvOutputItemAdded            = "response.output_item.added"
	EvOutputItemDone             = "response.output_item.done"
	EvContentPartAdded           = "response.content_part.added"
	EvContentPartDone            = "response.content_part.done"
	EvOutputTextDelta            = "response.output_text.delta"
	EvOutputTextDone             = "response.output_text.done"
	EvReasoningSummaryPartAdded  = "response.reasoning_summary_part.added"
	EvReasoningSummaryPartDone   = "response.reasoning_summary_part.done"
	EvReasoningSummaryTextDelta  = "response.reasoning_summary_text.delta"
	EvReasoningSummaryTextDone   = "response.reasoning_summary_text.done"
	EvReasoningTextDelta         = "response.reasoning_text.delta"
	EvReasoningTextDone          = "response.reasoning_text.done"
	EvFunctionCallArgumentsDelta = "response.function_call_arguments.delta"
	EvFunctionCallArgumentsDone  = "response.function_call_arguments.done"
	EvCustomToolCallInputDone    = "response.custom_tool_call_input.done"
	EvCompleted                  = "response.completed"
	EvIncomplete                 = "response.incomplete"
	EvFailed                     = "response.failed"
)

type TerminalKind int

const (
	TerminalCompleted TerminalKind = iota
	TerminalIncomplete
	TerminalFailed
)

func (t TerminalKind) AsStr() string {
	switch t {
	case TerminalCompleted:
		return "completed"
	case TerminalIncomplete:
		return "incomplete"
	default:
		return "failed"
	}
}

func (e *ResponsesEvent) Terminal() (TerminalKind, *ResponseObj, bool) {
	switch e.Type {
	case EvCompleted:
		return TerminalCompleted, e.Response, true
	case EvIncomplete:
		return TerminalIncomplete, e.Response, true
	case EvFailed:
		return TerminalFailed, e.Response, true
	default:
		return 0, nil, false
	}
}

// ---------- signature round-trip ----------

type signaturePayload struct {
	ID        *string `json:"id,omitempty"`
	Encrypted *string `json:"ec,omitempty"`
}

func EncodeSignature(id *string, encrypted string) string {
	payload := signaturePayload{ID: id, Encrypted: &encrypted}
	b, _ := json.Marshal(payload)
	return base64.RawURLEncoding.EncodeToString(b)
}

func DecodeSignature(sig string) (*string, string) {
	b, err := base64.RawURLEncoding.DecodeString(sig)
	if err != nil {
		return nil, sig
	}
	var p signaturePayload
	if json.Unmarshal(b, &p) != nil || p.Encrypted == nil {
		return nil, sig
	}
	return p.ID, *p.Encrypted
}
