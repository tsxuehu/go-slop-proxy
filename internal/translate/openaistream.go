package translate

import (
	"encoding/json"
	"strings"
	"time"
)

type ChannelKind int

const (
	ChannelText ChannelKind = iota
	ChannelThinking
	ChannelCall
)

type Channel struct {
	Kind  ChannelKind
	Index uint64 // for ChannelCall
}

type OpenAiStream struct {
	Model, ID     string
	Created       int64
	IncludeUsage  bool
	Channels      []Channel
	ToolCount     uint64
	ReasoningSeen bool
	SeparatorDue  bool
	Walker        *Walker
}

func NewOpenAiStream(model string, includeUsage bool, capture *UsageCapture) *OpenAiStream {
	return &OpenAiStream{
		Model: model, ID: "chatcmpl-" + randomHex(16),
		Created: time.Now().Unix(), IncludeUsage: includeUsage,
		Walker: NewWalker(capture),
	}
}

// Feed converts one upstream ResponsesEvent into zero or more Chat SSE JSON frames.
func (s *OpenAiStream) Feed(event *ResponsesEvent) []string {
	var out []string
	for _, step := range s.Walker.Step(event) {
		out = append(out, s.render(step)...)
	}
	return out
}

func (s *OpenAiStream) Finalize() []string {
	var out []string
	for _, step := range s.Walker.EOF() {
		out = append(out, s.render(step)...)
	}
	return out
}

func (s *OpenAiStream) render(step Step) []string {
	switch step.Kind {
	case StepStart:
		if step.ID != nil {
			s.ID = "chatcmpl-" + *step.ID
		}
		return []string{s.chunk(ChatDelta{Role: "assistant"}, nil, nil)}
	case StepBlock:
		switch step.Event {
		case EvOpen:
			switch step.Block.Kind {
			case BlockText:
				s.Channels = append(s.Channels, Channel{Kind: ChannelText})
			case BlockThinking:
				s.Channels = append(s.Channels, Channel{Kind: ChannelThinking})
				s.SeparatorDue = s.ReasoningSeen
			case BlockToolCall:
				idx := s.ToolCount
				s.ToolCount++
				s.Channels = append(s.Channels, Channel{Kind: ChannelCall, Index: idx})
				calls := []ChatToolCall{{
					Index: &idx, Kind: strPtr("function"),
					Function: FunctionBody{Name: strPtr(step.Block.Name), Arguments: strPtr("")},
				}}
				return []string{s.chunk(ChatDelta{ToolCalls: &calls}, nil, nil)}
			}
		case EvAppend:
			if step.Index >= len(s.Channels) {
				return nil
			}
			ch := s.Channels[step.Index]
			switch ch.Kind {
			case ChannelText:
				return []string{s.chunk(ChatDelta{Content: step.Payload}, nil, nil)}
			case ChannelThinking:
				text := step.Payload
				if s.SeparatorDue {
					text = "\n\n" + text
				}
				s.SeparatorDue = false
				s.ReasoningSeen = true
				return []string{s.chunk(ChatDelta{ReasoningContent: text}, nil, nil)}
			case ChannelCall:
				calls := []ChatToolCall{{
					Index:    &ch.Index,
					Function: FunctionBody{Arguments: strPtr(step.Payload)},
				}}
				return []string{s.chunk(ChatDelta{ToolCalls: &calls}, nil, nil)}
			}
		}
		// Signature / Close -> no frame
	case StepStop:
		fr := step.Stop.ChatFinishReason()
		var out []string
		out = append(out, s.chunk(ChatDelta{}, &fr, nil))
		if s.IncludeUsage {
			usage := ChatUsageFrom(&step.Usage)
			b, _ := json.Marshal(map[string]any{
				"id": s.ID, "object": "chat.completion.chunk", "created": s.Created,
				"model": s.Model, "choices": []any{}, "usage": usage,
			})
			out = append(out, string(b))
		}
		return out
	case StepFailed:
		b, _ := json.Marshal(map[string]any{
			"id": s.ID, "object": "chat.completion.chunk", "created": s.Created,
			"model": s.Model, "choices": []any{},
			"error": ChatErrorBody{Message: step.Message, Kind: strPtr("api_error")},
		})
		return []string{string(b)}
	}
	return nil
}

func (s *OpenAiStream) chunk(delta ChatDelta, finish *string, usage *ChatUsage) string {
	b, _ := json.Marshal(ChatChunk{
		ID: s.ID, Object: "chat.completion.chunk", Created: s.Created, Model: s.Model,
		Choices: []ChunkChoice{{Index: 0, Delta: delta, FinishReason: finish, Logprobs: nil}},
		Usage:   usage,
	})
	return string(b)
}

func strPtr(s string) *string { return &s }

func RenderAggregatedChat(agg *Aggregated, model string) []byte {
	var text strings.Builder
	var reasoning []string
	var calls []ChatToolCall
	for _, b := range agg.Blocks {
		switch b.Kind {
		case BlockText:
			text.WriteString(b.Text)
		case BlockThinking:
			if b.Text != "" {
				reasoning = append(reasoning, b.Text)
			}
		case BlockToolCall:
			args := b.Arguments
			if json.Valid([]byte(args)) == false {
				args = "{}"
			}
			calls = append(calls, ChatToolCall{
				ID: strPtr(b.ID), Kind: strPtr("function"),
				Function: FunctionBody{Name: strPtr(b.Name), Arguments: &args},
			})
		}
	}
	msg := ChatMessage{Role: "assistant", Content: &ChatContent{Text: text.String()}}
	if len(reasoning) > 0 {
		r := strings.Join(reasoning, "\n\n")
		msg.ReasoningContent = &r
	}
	if len(calls) > 0 {
		msg.ToolCalls = &calls
	}
	fr := agg.Stop.ChatFinishReason()
	usage := ChatUsageFrom(&agg.Usage)
	result := ChatCompletion{
		ID: "chatcmpl-" + agg.ID, Object: "chat.completion", Created: time.Now().Unix(),
		Model:   model,
		Choices: []ChatChoice{{Index: 0, Message: msg, FinishReason: &fr, Logprobs: nil}},
		Usage:   &usage,
	}
	b, _ := json.Marshal(result)
	return b
}
