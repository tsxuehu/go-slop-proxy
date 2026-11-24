package translate

import (
	"encoding/json"
	"strings"

	"github.com/edy/slop-proxy-go/internal/config"
	"github.com/edy/slop-proxy-go/internal/provider"
)

type AnthropicRequest struct {
	Model      string          `json:"model"`
	MaxTokens  *uint64         `json:"max_tokens,omitempty"`
	Messages   []AnthMessage   `json:"messages"`
	System     *SystemPrompt   `json:"system,omitempty"`
	Tools      *[]AnthToolDef  `json:"tools,omitempty"`
	ToolChoice *AnthToolChoice `json:"tool_choice,omitempty"`
	Thinking   *ThinkingConfig `json:"thinking,omitempty"`
	Stream     *bool           `json:"stream,omitempty"`
}

type AnthMessage struct {
	Role    string         `json:"role"`
	Content MessageContent `json:"content"`
}

type MessageContent struct {
	Text   string
	Blocks []ContentBlock
}

func (m *MessageContent) UnmarshalJSON(data []byte) error {
	var s string
	if json.Unmarshal(data, &s) == nil {
		m.Text, m.Blocks = s, nil
		return nil
	}
	if err := json.Unmarshal(data, &m.Blocks); err != nil {
		return nil // null / missing -> empty
	}
	m.Text = ""
	return nil
}

type ContentBlock struct {
	Type         string             `json:"type"`
	Text         string             `json:"text,omitempty"`
	Source       *ImageSource       `json:"source,omitempty"`
	ID           string             `json:"id,omitempty"`
	Name         string             `json:"name,omitempty"`
	Input        json.RawMessage    `json:"input,omitempty"`
	ToolUseID    string             `json:"tool_use_id,omitempty"`
	Content      *ToolResultContent `json:"content,omitempty"`
	IsError      bool               `json:"is_error,omitempty"`
	Thinking     string             `json:"thinking,omitempty"`
	Signature    *string            `json:"signature,omitempty"`
	CacheControl *CacheControl      `json:"cache_control,omitempty"`
}

type ImageSource struct {
	Type      string `json:"type"`
	MediaType string `json:"media_type,omitempty"`
	Data      string `json:"data,omitempty"`
	URL       string `json:"url,omitempty"`
}

type ToolResultContent struct {
	Text   string
	Blocks []ToolResultBlock
	Raw    json.RawMessage
}

func (t *ToolResultContent) UnmarshalJSON(data []byte) error {
	var s string
	if json.Unmarshal(data, &s) == nil {
		t.Text, t.Blocks = s, nil
		return nil
	}
	if err := json.Unmarshal(data, &t.Blocks); err == nil && len(t.Blocks) > 0 {
		t.Text = ""
		return nil
	}
	t.Raw = append(json.RawMessage(nil), data...)
	return nil
}

type ToolResultBlock struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

type SystemPrompt struct {
	Text   string
	Blocks []SystemBlock
}

func (s *SystemPrompt) UnmarshalJSON(data []byte) error {
	var str string
	if json.Unmarshal(data, &str) == nil {
		s.Text, s.Blocks = str, nil
		return nil
	}
	_ = json.Unmarshal(data, &s.Blocks)
	s.Text = ""
	return nil
}

type SystemBlock struct {
	Text         *string       `json:"text,omitempty"`
	CacheControl *CacheControl `json:"cache_control,omitempty"`
}

type CacheControl struct {
	TTL *string `json:"ttl,omitempty"`
}

type AnthToolDef struct {
	Name        string          `json:"name"`
	Description *string         `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema,omitempty"`
}

type AnthToolChoice struct {
	Kind                   *string `json:"type,omitempty"`
	Name                   *string `json:"name,omitempty"`
	DisableParallelToolUse *bool   `json:"disable_parallel_tool_use,omitempty"`
}

type ThinkingConfig struct {
	Kind         *string `json:"type,omitempty"`
	BudgetTokens *uint64 `json:"budget_tokens,omitempty"`
}

func (a *AnthropicRequest) ThinkingEnabled() bool {
	return a.Thinking != nil && (ptrEq(a.Thinking.Kind, "enabled") || ptrEq(a.Thinking.Kind, "adaptive"))
}

func (a *AnthropicRequest) CacheTTLSecs() *int64 {
	if a.System == nil || len(a.System.Blocks) == 0 {
		return nil
	}
	var max int64
	for _, b := range a.System.Blocks {
		if b.CacheControl != nil {
			secs := int64(300)
			if b.CacheControl.TTL != nil && *b.CacheControl.TTL == "1h" {
				secs = 3600
			}
			if secs > max {
				max = secs
			}
		}
	}
	if max == 0 {
		return nil
	}
	return &max
}

func ptrEq(p *string, s string) bool { return p != nil && *p == s }

// AnthropicToResponses converts an Anthropic Messages request into IR.
func AnthropicToResponses(req *AnthropicRequest, cfg *config.Config, p provider.Provider) (ResponsesRequest, error) {
	var effort string
	if req.Thinking != nil {
		if !req.ThinkingEnabled() {
			effort = "low"
		} else if req.Thinking.BudgetTokens != nil {
			budget := *req.Thinking.BudgetTokens
			switch {
			case budget < 4096:
				effort = "low"
			case budget < 16384:
				effort = "medium"
			default:
				effort = "high"
			}
		}
	}
	out := responsesRequest(cfg, req.Model, effort, req.MaxTokens)
	replayReasoning := p == provider.OpenAI

	if system := systemText(req.System); system != "" {
		out.Input = append(out.Input, InputItem{
			Type: "message", Role: "developer",
			Content: []ContentPart{{Type: "input_text", Text: system}},
		})
	}

	for i := range req.Messages {
		convertAnthMessage(&req.Messages[i], &out, replayReasoning)
	}

	if req.Tools != nil {
		for i := range *req.Tools {
			t := &(*req.Tools)[i]
			if t.Name == "" {
				continue
			}
			params := t.InputSchema
			if len(params) == 0 {
				params = EmptySchema()
			}
			out.Tools = append(out.Tools, FunctionTool(t.Name, t.Description, params))
		}
	}

	if req.ToolChoice != nil {
		kind := "auto"
		if req.ToolChoice.Kind != nil {
			kind = *req.ToolChoice.Kind
		}
		switch kind {
		case "any":
			out.ToolChoice = &ToolChoice{Mode: "required"}
		case "tool":
			name := ""
			if req.ToolChoice.Name != nil {
				name = *req.ToolChoice.Name
			}
			out.ToolChoice = &ToolChoice{Name: name}
		case "none":
			out.ToolChoice = &ToolChoice{Mode: "none"}
		default:
			out.ToolChoice = &ToolChoice{Mode: "auto"}
		}
		if req.ToolChoice.DisableParallelToolUse != nil && *req.ToolChoice.DisableParallelToolUse {
			f := false
			out.ParallelToolCalls = &f
		}
	}
	return out, nil
}

func systemText(s *SystemPrompt) string {
	if s == nil {
		return ""
	}
	if s.Text != "" {
		return s.Text
	}
	var parts []string
	for _, b := range s.Blocks {
		if b.Text != nil {
			parts = append(parts, *b.Text)
		}
	}
	return strings.Join(parts, "\n\n")
}

func convertAnthMessage(msg *AnthMessage, out *ResponsesRequest, replayReasoning bool) {
	assistant := msg.Role == "assistant"
	role := "user"
	if assistant {
		role = "assistant"
	}
	blocks := msg.Content.Blocks
	if msg.Content.Text != "" {
		blocks = []ContentBlock{{Type: "text", Text: msg.Content.Text}}
	}

	var parts []ContentPart
	flush := func() {
		if len(parts) > 0 {
			out.Input = append(out.Input, InputItem{Type: "message", Role: role, Content: parts})
			parts = nil
		}
	}

	for i := range blocks {
		b := &blocks[i]
		switch b.Type {
		case "text":
			pt := "input_text"
			if assistant {
				pt = "output_text"
			}
			parts = append(parts, ContentPart{Type: pt, Text: b.Text})
		case "image":
			if b.Source == nil {
				continue
			}
			url := ""
			switch b.Source.Type {
			case "base64":
				media := b.Source.MediaType
				if media == "" {
					media = "image/png"
				}
				url = "data:" + media + ";base64," + b.Source.Data
			case "url":
				url = b.Source.URL
			}
			if url != "" {
				parts = append(parts, ContentPart{Type: "input_image", ImageURL: url})
			}
		case "tool_use":
			flush()
			args := string(b.Input)
			if args == "" {
				args = "{}"
			}
			out.Input = append(out.Input, InputItem{Type: "function_call", CallID: b.ID, Name: b.Name, Arguments: args})
		case "tool_result":
			flush()
			output := toolResultText(b.Content)
			if b.IsError {
				output = "[tool error] " + output
			}
			out.Input = append(out.Input, InputItem{
				Type: "function_call_output", CallID: b.ToolUseID, Output: &ToolOutput{Text: output},
			})
		case "thinking":
			if b.Signature == nil || !replayReasoning {
				continue
			}
			id, encrypted := DecodeSignature(*b.Signature)
			flush()
			var summary []SummaryPart
			if b.Thinking != "" {
				summary = []SummaryPart{{Type: "summary_text", Text: b.Thinking}}
			}
			enc := encrypted
			out.Input = append(out.Input, InputItem{
				Type: "reasoning", ID: id, Summary: summary, EncryptedContent: &enc,
			})
		}
	}
	flush()
}

func toolResultText(c *ToolResultContent) string {
	if c == nil {
		return ""
	}
	if c.Text != "" {
		return c.Text
	}
	if len(c.Blocks) > 0 {
		var parts []string
		for _, b := range c.Blocks {
			switch b.Type {
			case "text":
				parts = append(parts, b.Text)
			case "image":
				parts = append(parts, "[image omitted]")
			}
		}
		return strings.Join(parts, "\n")
	}
	if len(c.Raw) > 0 {
		return string(c.Raw)
	}
	return ""
}
