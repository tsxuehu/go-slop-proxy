package translate

import (
	"fmt"

	"github.com/edy/slop-proxy-go/internal/config"
)

type TranslateError struct {
	Kind    string
	Message string
}

func (e *TranslateError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return e.Kind
}

// ToResponses converts an OpenAI Chat request into the IR ResponsesRequest.
func ToResponses(req *ChatRequest, cfg *config.Config) (ResponsesRequest, error) {
	var cap *uint64
	if req.MaxCompletionTokens != nil {
		cap = req.MaxCompletionTokens
	} else {
		cap = req.MaxTokens
	}
	effort := ""
	if req.ReasoningEffort != nil {
		effort = *req.ReasoningEffort
	}
	out := responsesRequest(cfg, req.Model, effort, cap)

	for i := range req.Messages {
		if err := convertChatMessage(&req.Messages[i], &out); err != nil {
			return out, err
		}
	}

	if req.Tools != nil {
		for i := range *req.Tools {
			def := (*req.Tools)[i].Def()
			if def == nil || def.Name == "" {
				continue
			}
			var desc *string
			if def.Description != nil {
				desc = def.Description
			}
			out.Tools = append(out.Tools, FunctionTool(def.Name, desc, def.Parameters))
		}
	}

	if req.ToolChoice != nil {
		if req.ToolChoice.Mode != "" {
			out.ToolChoice = &ToolChoice{Mode: req.ToolChoice.Mode}
		} else {
			out.ToolChoice = &ToolChoice{Name: req.ToolChoice.Name}
		}
	}
	out.ParallelToolCalls = req.ParallelToolCalls
	return out, nil
}

func convertChatMessage(msg *ChatMessage, out *ResponsesRequest) error {
	switch msg.Role {
	case "system", "developer":
		text := msg.Text()
		if text != "" {
			out.Input = append(out.Input, InputItem{
				Type: "message", Role: "developer",
				Content: []ContentPart{{Type: "input_text", Text: text}},
			})
		}
	case "user":
		parts := userParts(msg)
		if len(parts) > 0 {
			out.Input = append(out.Input, InputItem{Type: "message", Role: "user", Content: parts})
		}
	case "assistant":
		text := msg.Text()
		if text != "" {
			out.Input = append(out.Input, InputItem{
				Type: "message", Role: "assistant",
				Content: []ContentPart{{Type: "output_text", Text: text}},
			})
		}
		if msg.ToolCalls != nil {
			for i := range *msg.ToolCalls {
				call := &(*msg.ToolCalls)[i]
				id, name, args := "", "", "{}"
				if call.ID != nil {
					id = *call.ID
				}
				if call.Function.Name != nil {
					name = *call.Function.Name
				}
				if call.Function.Arguments != nil && *call.Function.Arguments != "" {
					args = *call.Function.Arguments
				}
				out.Input = append(out.Input, InputItem{
					Type: "function_call", CallID: id, Name: name, Arguments: args,
				})
			}
		}
	case "tool":
		if msg.ToolCallID == nil {
			return &TranslateError{Kind: "ToolMessageWithoutCallId", Message: "tool message without tool_call_id"}
		}
		out.Input = append(out.Input, InputItem{
			Type: "function_call_output", CallID: *msg.ToolCallID,
			Output: &ToolOutput{Text: msg.Text()},
		})
	default:
		return &TranslateError{Kind: "UnsupportedRole", Message: fmt.Sprintf("unsupported role: %s", msg.Role)}
	}
	return nil
}

func userParts(msg *ChatMessage) []ContentPart {
	if msg.Content == nil {
		return nil
	}
	if msg.Content.Parts == nil {
		if msg.Content.TextValue() == "" {
			return nil
		}
		return []ContentPart{{Type: "input_text", Text: msg.Content.TextValue()}}
	}
	var out []ContentPart
	for i := range msg.Content.Parts {
		p := &msg.Content.Parts[i]
		switch p.Type {
		case "text":
			out = append(out, ContentPart{Type: "input_text", Text: p.Text})
		case "image_url":
			if p.ImageURL != nil && p.ImageURL.Value != "" {
				out = append(out, ContentPart{Type: "input_image", ImageURL: p.ImageURL.Value})
			}
		}
	}
	return out
}
