package translate

import (
	"encoding/json"
	"strings"
	"time"
)

type AnthUsage struct {
	InputTokens              int64 `json:"input_tokens"`
	OutputTokens             int64 `json:"output_tokens"`
	CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
	CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
}

func anthropicUsage(u *Usage) AnthUsage {
	fresh, cached, written := inputTokenPartition(u)
	out := AnthUsage{CacheReadInputTokens: cached, CacheCreationInputTokens: written}
	if u != nil {
		out.OutputTokens = u.OutputTokens
	}
	out.InputTokens = fresh
	return out
}

type AnthBinding struct {
	Index   int
	Channel Channel
}

type AnthropicStream struct {
	Model          string
	EstInputTokens int64
	EmitThinking   bool
	NextIndex      int
	Blocks         []*AnthBinding
	Walker         *Walker
}

func NewAnthropicStream(model string, estInput int64, emitThinking bool, capture *UsageCapture) *AnthropicStream {
	return &AnthropicStream{
		Model: model, EstInputTokens: estInput, EmitThinking: emitThinking,
		Walker: NewWalker(capture),
	}
}

type OutEvent struct {
	Name string
	Body string
}

func (s *AnthropicStream) Feed(event *ResponsesEvent) []OutEvent {
	var out []OutEvent
	for _, step := range s.Walker.Step(event) {
		out = append(out, s.render(step)...)
	}
	return out
}

func (s *AnthropicStream) Finalize() []OutEvent {
	var out []OutEvent
	for _, step := range s.Walker.EOF() {
		if step.Kind == StepFailed {
			out = append(out, s.event("error", map[string]any{
				"type":  "error",
				"error": map[string]any{"type": "overloaded_error", "message": step.Message},
			}))
		}
	}
	return out
}

func (s *AnthropicStream) event(name string, body any) OutEvent {
	b, _ := json.Marshal(body)
	return OutEvent{Name: name, Body: string(b)}
}

func (s *AnthropicStream) render(step Step) []OutEvent {
	switch step.Kind {
	case StepStart:
		id := "msg_" + randomHex(16)
		if step.ID != nil {
			id = *step.ID
		}
		msg := map[string]any{
			"id": id, "type": "message", "role": "assistant", "model": s.Model,
			"content": []any{}, "stop_reason": nil, "stop_sequence": nil,
			"usage": AnthUsage{InputTokens: s.EstInputTokens, OutputTokens: 1},
		}
		return []OutEvent{
			s.event("message_start", map[string]any{"type": "message_start", "message": msg}),
			s.event("ping", map[string]any{"type": "ping"}),
		}
	case StepBlock:
		switch step.Event {
		case EvOpen:
			switch step.Block.Kind {
			case BlockThinking:
				if !s.EmitThinking {
					s.Blocks = append(s.Blocks, nil) // hidden: reserve slot, no index
					return nil
				}
				idx := s.NextIndex
				s.NextIndex++
				s.Blocks = append(s.Blocks, &AnthBinding{Index: idx, Channel: Channel{Kind: ChannelThinking}})
				return []OutEvent{s.event("content_block_start", map[string]any{
					"type": "content_block_start", "index": idx,
					"content_block": map[string]any{"type": "thinking", "thinking": ""},
				})}
			case BlockText:
				idx := s.NextIndex
				s.NextIndex++
				s.Blocks = append(s.Blocks, &AnthBinding{Index: idx, Channel: Channel{Kind: ChannelText}})
				return []OutEvent{s.event("content_block_start", map[string]any{
					"type": "content_block_start", "index": idx,
					"content_block": map[string]any{"type": "text", "text": ""},
				})}
			case BlockToolCall:
				idx := s.NextIndex
				s.NextIndex++
				s.Blocks = append(s.Blocks, &AnthBinding{Index: idx, Channel: Channel{Kind: ChannelCall}})
				return []OutEvent{s.event("content_block_start", map[string]any{
					"type": "content_block_start", "index": idx,
					"content_block": map[string]any{
						"type": "tool_use", "id": step.Block.ID, "name": step.Block.Name,
						"input": map[string]any{},
					},
				})}
			}
		case EvAppend:
			if step.Index >= len(s.Blocks) || s.Blocks[step.Index] == nil {
				return nil
			}
			binding := s.Blocks[step.Index]
			var delta map[string]any
			switch binding.Channel.Kind {
			case ChannelThinking:
				delta = map[string]any{"type": "thinking_delta", "thinking": step.Payload}
			case ChannelText:
				delta = map[string]any{"type": "text_delta", "text": step.Payload}
			case ChannelCall:
				delta = map[string]any{"type": "input_json_delta", "partial_json": step.Payload}
			}
			return []OutEvent{s.event("content_block_delta", map[string]any{
				"type": "content_block_delta", "index": binding.Index, "delta": delta,
			})}
		case EvSignature:
			if step.Index >= len(s.Blocks) || s.Blocks[step.Index] == nil {
				return nil
			}
			binding := s.Blocks[step.Index]
			return []OutEvent{s.event("content_block_delta", map[string]any{
				"type": "content_block_delta", "index": binding.Index,
				"delta": map[string]any{"type": "signature_delta", "signature": step.Payload},
			})}
		case EvClose:
			if step.Index >= len(s.Blocks) || s.Blocks[step.Index] == nil {
				return nil
			}
			binding := s.Blocks[step.Index]
			return []OutEvent{s.event("content_block_stop", map[string]any{
				"type": "content_block_stop", "index": binding.Index,
			})}
		}
	case StepStop:
		u := anthropicUsage(&step.Usage)
		var out []OutEvent
		out = append(out, s.event("message_delta", map[string]any{
			"type":  "message_delta",
			"delta": map[string]any{"stop_reason": step.Stop.AsStr(), "stop_sequence": nil},
			"usage": u,
		}))
		out = append(out, s.event("message_stop", map[string]any{"type": "message_stop"}))
		return out
	case StepFailed:
		kind := "api_error"
		if step.Code != nil && strings.Contains(*step.Code, "rate_limit") || strings.Contains(step.Message, "rate limit") {
			kind = "rate_limit_error"
		}
		return []OutEvent{s.event("error", map[string]any{
			"type":  "error",
			"error": map[string]any{"type": kind, "message": step.Message},
		})}
	}
	return nil
}

type RenderedBlock struct {
	Type      string          `json:"type"`
	Thinking  string          `json:"thinking,omitempty"`
	Signature string          `json:"signature,omitempty"`
	Text      string          `json:"text,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
}

type RenderedMessage struct {
	ID           string          `json:"id"`
	Type         string          `json:"type"`
	Role         string          `json:"role"`
	Model        string          `json:"model"`
	Content      []RenderedBlock `json:"content"`
	StopReason   string          `json:"stop_reason"`
	StopSequence *string         `json:"stop_sequence"`
	Usage        AnthUsage       `json:"usage"`
}

func RenderAggregatedAnthropic(agg *Aggregated, model string, emitThinking bool) []byte {
	var blocks []RenderedBlock
	for _, b := range agg.Blocks {
		switch b.Kind {
		case BlockThinking:
			if !emitThinking {
				continue
			}
			sig := ""
			if b.Signature != nil {
				sig = *b.Signature
			}
			blocks = append(blocks, RenderedBlock{Type: "thinking", Thinking: b.Text, Signature: sig})
		case BlockText:
			blocks = append(blocks, RenderedBlock{Type: "text", Text: b.Text})
		case BlockToolCall:
			input := json.RawMessage(b.Arguments)
			if !json.Valid(input) {
				input = json.RawMessage("{}")
			}
			blocks = append(blocks, RenderedBlock{Type: "tool_use", ID: b.ID, Name: b.Name, Input: input})
		}
	}
	msg := RenderedMessage{
		ID: agg.ID, Type: "message", Role: "assistant", Model: model,
		Content: blocks, StopReason: agg.Stop.AsStr(), StopSequence: nil,
		Usage: anthropicUsage(&agg.Usage),
	}
	b, _ := json.Marshal(msg)
	return b
}

var _ = time.Now
