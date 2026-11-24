package translate

import (
	"encoding/json"
	"sort"
)

type BridgeProtocol int

const (
	BridgeChat BridgeProtocol = iota
	BridgeGeminiNative
)

type OpenCall struct {
	ID, ItemID, Name, Arguments string
	Index                       uint64
	Announced                   bool
}

type TextOutput struct {
	ID    string
	Index uint64
	Text  string
}

func newTextOutput(prefix string, next *uint64) TextOutput {
	idx := *next
	*next++
	return TextOutput{ID: prefix + "_" + randomHex(16), Index: idx}
}

type ChatToResponses struct {
	Custom       map[string]bool
	ResponseID   *string
	Calls        map[uint64]*OpenCall
	Text         *TextOutput
	Reasoning    *TextOutput
	NextIndex    uint64
	FinishReason *string
	Usage        *Usage
	Completed    bool
}

func NewChatToResponses(custom map[string]bool) *ChatToResponses {
	if custom == nil {
		custom = make(map[string]bool)
	}
	return &ChatToResponses{Custom: custom, Calls: make(map[uint64]*OpenCall)}
}

func (b *ChatToResponses) Feed(chunk *ChatChunk) []*ResponsesEvent {
	var out []*ResponsesEvent
	if b.Completed {
		return out
	}
	firstFrame := b.ResponseID == nil
	id := "resp_" + randomHex(16)
	if b.ResponseID != nil {
		id = *b.ResponseID
	} else if chunk.ID != "" {
		id = chunk.ID
	}
	b.ResponseID = &id

	resp := func(status string, usage *Usage, err *UpstreamError) *ResponseObj {
		return &ResponseObj{ID: &id, Status: &status, Usage: usage, Error: err}
	}

	if chunk.Error != nil {
		b.Completed = true
		b.Calls = make(map[uint64]*OpenCall)
		var code, msg *string
		msg = &chunk.Error.Message
		var codeStr string
		if len(chunk.Error.Code) > 0 {
			codeStr = string(chunk.Error.Code)
		}
		if codeStr != "" {
			code = &codeStr
		}
		var usage *Usage
		if chunk.Usage != nil {
			u := UsageFromChat(chunk.Usage)
			usage = &u
		} else if b.Usage != nil {
			usage = b.Usage
		}
		out = append(out, &ResponsesEvent{Type: EvFailed, Response: resp("failed", usage, &UpstreamError{Code: code, Message: msg})})
		return out
	}

	if firstFrame {
		status := "in_progress"
		out = append(out, &ResponsesEvent{Type: EvCreated, Response: resp(status, nil, nil)})
	}

	if len(chunk.Choices) == 0 {
		return b.finishIfReady(chunk, out)
	}
	choice := &chunk.Choices[0]
	delta := &choice.Delta

	if delta.Images != nil && len(*delta.Images) > 0 {
		b.Completed = true
		var usage *Usage
		if chunk.Usage != nil {
			u := UsageFromChat(chunk.Usage)
			usage = &u
		}
		code, msg := "unsupported_output", "image output cannot be represented on this endpoint"
		return append(out, &ResponsesEvent{Type: EvFailed, Response: resp("failed", usage, &UpstreamError{Code: &code, Message: &msg})})
	}

	if delta.ReasoningContent != "" {
		first := b.Reasoning == nil
		if first {
			r := newTextOutput("rs", &b.NextIndex)
			b.Reasoning = &r
			emptySummary := []SummaryPart{}
			rid := r.ID
			out = append(out, &ResponsesEvent{
				Type: EvOutputItemAdded, OutputIndex: r.Index,
				Item: &OutputItem{Type: "reasoning", ID: &rid, Summary: &emptySummary},
			})
			out = append(out, &ResponsesEvent{Type: EvReasoningSummaryPartAdded, OutputIndex: r.Index})
		}
		b.Reasoning.Text += delta.ReasoningContent
		out = append(out, &ResponsesEvent{
			Type: EvReasoningSummaryTextDelta, OutputIndex: b.Reasoning.Index, Delta: delta.ReasoningContent,
		})
	}

	if delta.Content != "" {
		first := b.Text == nil
		if first {
			m := newTextOutput("msg", &b.NextIndex)
			b.Text = &m
			emptyContent := []OutputContentPart{}
			mid := m.ID
			role, status := "assistant", "in_progress"
			out = append(out, &ResponsesEvent{
				Type: EvOutputItemAdded, OutputIndex: m.Index,
				Item: &OutputItem{Type: "message", ID: &mid, Role: &role, Status: &status, Content: &emptyContent},
			})
			out = append(out, &ResponsesEvent{
				Type: EvContentPartAdded, ItemID: &mid, OutputIndex: m.Index,
				Part: &OutputContentPart{Type: "output_text", Text: ""},
			})
		}
		b.Text.Text += delta.Content
		out = append(out, &ResponsesEvent{
			Type: EvOutputTextDelta, ItemID: &b.Text.ID, OutputIndex: b.Text.Index,
			Delta: delta.Content,
		})
	}

	if delta.ToolCalls != nil {
		for i := range *delta.ToolCalls {
			out = append(out, b.toolCall(&(*delta.ToolCalls)[i])...)
		}
	}

	return b.finishIfReady(chunk, out)
}

func (b *ChatToResponses) finishIfReady(chunk *ChatChunk, out []*ResponsesEvent) []*ResponsesEvent {
	if len(chunk.Choices) > 0 && b.FinishReason == nil && chunk.Choices[0].FinishReason != nil {
		fr := *chunk.Choices[0].FinishReason
		b.FinishReason = &fr
		keys := make([]uint64, 0, len(b.Calls))
		for k := range b.Calls {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		for _, k := range keys {
			call := b.Calls[k]
			if !call.Announced {
				continue
			}
			body := call.Arguments
			custom := b.Custom[call.Name]
			if custom {
				var m map[string]string
				if json.Unmarshal([]byte(call.Arguments), &m) == nil {
					if v, ok := m["input"]; ok {
						body = v
					}
				}
				out = append(out, &ResponsesEvent{Type: EvCustomToolCallInputDone, ItemID: &call.ItemID, OutputIndex: call.Index, Input: body})
			} else {
				out = append(out, &ResponsesEvent{Type: EvFunctionCallArgumentsDone, ItemID: &call.ItemID, OutputIndex: call.Index, Arguments: body})
			}
			status := "completed"
			out = append(out, &ResponsesEvent{Type: EvOutputItemDone, OutputIndex: call.Index, Item: callItem(call, custom, body, &status)})
		}
		b.Calls = make(map[uint64]*OpenCall)

		if b.Text != nil {
			status := "completed"
			text := b.Text.Text
			out = append(out, &ResponsesEvent{Type: EvOutputTextDone, ItemID: &b.Text.ID, OutputIndex: b.Text.Index, Text: text})
			content := []OutputContentPart{{Type: "output_text", Text: text}}
			role := "assistant"
			out = append(out, &ResponsesEvent{
				Type: EvOutputItemDone, OutputIndex: b.Text.Index,
				Item: &OutputItem{Type: "message", ID: &b.Text.ID, Role: &role, Status: &status, Content: &content},
			})
		}
		if b.Reasoning != nil {
			status := "completed"
			summary := []SummaryPart{{Type: "summary_text", Text: b.Reasoning.Text}}
			out = append(out, &ResponsesEvent{
				Type: EvOutputItemDone, OutputIndex: b.Reasoning.Index,
				Item: &OutputItem{Type: "reasoning", ID: &b.Reasoning.ID, Summary: &summary, Status: &status},
			})
		}
	}
	if chunk.Usage != nil {
		u := UsageFromChat(chunk.Usage)
		b.Usage = &u
	}
	if b.FinishReason != nil && !b.Completed && b.Usage != nil {
		b.Completed = true
		out = append(out, b.terminal())
	}
	return out
}

func callItem(call *OpenCall, custom bool, body string, status *string) *OutputItem {
	if custom {
		return &OutputItem{Type: "custom_tool_call", ID: &call.ItemID, CallID: call.ID, Name: call.Name, InputVal: body, Status: status}
	}
	return &OutputItem{Type: "function_call", ID: &call.ItemID, CallID: call.ID, Name: call.Name, Arguments: &body, Status: status}
}

func (b *ChatToResponses) toolCall(call *ChatToolCall) []*ResponsesEvent {
	var out []*ResponsesEvent
	idx := uint64(0)
	if call.Index != nil {
		idx = *call.Index
	}
	slot := b.Calls[idx]
	if slot == nil {
		slot = &OpenCall{Index: idx}
		b.Calls[idx] = slot
	}
	if slot.ID == "" {
		if call.ID != nil && *call.ID != "" {
			slot.ID = *call.ID
		} else {
			slot.ID = "call_" + randomHex(16)
		}
	}
	if call.Function.Name != nil {
		slot.Name = *call.Function.Name
	}
	wasAnnounced := slot.Announced
	if call.Function.Arguments != nil {
		slot.Arguments += *call.Function.Arguments
	}
	if !slot.Announced && slot.Name != "" {
		slot.Announced = true
		slot.ItemID = "fc_" + randomHex(16)
		slot.Index = b.NextIndex
		b.NextIndex++
		out = append(out, &ResponsesEvent{
			Type: EvOutputItemAdded, OutputIndex: slot.Index,
			Item: callItem(slot, b.Custom[slot.Name], "", nil),
		})
	}
	args := ""
	if call.Function.Arguments != nil {
		args = *call.Function.Arguments
		if !wasAnnounced {
			args = slot.Arguments
		}
	}
	if slot.Announced && args != "" {
		out = append(out, &ResponsesEvent{
			Type: EvFunctionCallArgumentsDelta, ItemID: &slot.ItemID, OutputIndex: slot.Index, Delta: args,
		})
	}
	return out
}

func (b *ChatToResponses) terminal() *ResponsesEvent {
	id := ""
	if b.ResponseID != nil {
		id = *b.ResponseID
	}
	usage := &Usage{}
	if b.Usage != nil {
		usage = b.Usage
	}
	status, err := "completed", (*UpstreamError)(nil)
	switch {
	case b.FinishReason == nil || (*b.FinishReason != "stop" && *b.FinishReason != "tool_calls"):
		if b.FinishReason != nil && (*b.FinishReason == "length" || *b.FinishReason == "content_filter") {
			status = "incomplete"
		} else {
			status = "failed"
			code, msg := "upstream_eof", "upstream ended without a supported finish reason"
			err = &UpstreamError{Code: &code, Message: &msg}
		}
	}
	resp := &ResponseObj{ID: &id, Status: &status, Usage: usage, Error: err}
	switch status {
	case "incomplete":
		return &ResponsesEvent{Type: EvIncomplete, Response: resp}
	case "failed":
		return &ResponsesEvent{Type: EvFailed, Response: resp}
	default:
		return &ResponsesEvent{Type: EvCompleted, Response: resp}
	}
}

func (b *ChatToResponses) Finalize() []*ResponsesEvent {
	if b.Completed {
		return nil
	}
	b.Completed = true
	return []*ResponsesEvent{b.terminal()}
}
