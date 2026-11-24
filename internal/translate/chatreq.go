package translate

import (
	"encoding/json"
	"strings"
)

func ResponsesToChat(req *ResponsesRequest) (*ChatRequest, map[string]bool) {
	stream := true
	chat := &ChatRequest{
		Model: req.Model, Stream: &stream, StreamOptions: &StreamOptions{IncludeUsage: true},
		MaxTokens: req.MaxOutputTokens, ParallelToolCalls: req.ParallelToolCalls,
	}
	if req.Instructions != "" {
		chat.Messages = append(chat.Messages, ChatMessage{Role: "system", Content: &ChatContent{Text: req.Instructions}})
	}
	if req.Reasoning != nil && req.Reasoning.Effort != "" {
		effort := chatEffort(req.Model, req.Reasoning.Effort)
		chat.ReasoningEffort = &effort
	}
	if req.ToolChoice != nil {
		chat.ToolChoice = &ChatToolChoice{Mode: req.ToolChoice.Mode, Name: req.ToolChoice.Name}
	}

	var thought *string
	var tools []ToolDef
	tools = append(tools, req.Tools...)
	for _, item := range req.Input {
		var previous *ChatMessage
		if n := len(chat.Messages); n > 0 {
			previous = &chat.Messages[n-1]
		}
		switch item.Type {
		case "message":
			role := item.Role
			if role == "developer" {
				role = "system"
			}
			content := chatParts(item.Content)
			if role == "assistant" && previous != nil && previous.Role == "assistant" {
				if previous.Content == nil {
					previous.Content = content
				} else {
					previous.Content.Parts = append(previous.Content.Parts, content.Parts...)
				}
				continue
			}
			if role != "assistant" {
				thought = nil
			}
			chat.Messages = append(chat.Messages, ChatMessage{Role: role, Content: content})
		case "function_call", "custom_tool_call":
			if previous == nil || previous.Role != "assistant" {
				chat.Messages = append(chat.Messages, ChatMessage{Role: "assistant"})
				previous = &chat.Messages[len(chat.Messages)-1]
			}
			if previous.ReasoningContent == nil {
				previous.ReasoningContent, thought = thought, nil
			}
			if previous.ToolCalls == nil {
				previous.ToolCalls = new([]ChatToolCall)
			}
			args, kind := item.Arguments, "function"
			if item.Type == "custom_tool_call" {
				encoded, _ := json.Marshal(map[string]string{"input": item.Input})
				args = string(encoded)
			}
			*previous.ToolCalls = append(*previous.ToolCalls, ChatToolCall{
				ID: &item.CallID, Kind: &kind,
				Function: FunctionBody{Name: &item.Name, Arguments: &args},
			})
		case "function_call_output", "custom_tool_call_output":
			if item.Output != nil {
				chat.Messages = append(chat.Messages, ChatMessage{
					Role: "tool", ToolCallID: &item.CallID, Content: &ChatContent{Text: item.Output.TextValue()},
				})
			}
		case "reasoning":
			var summary strings.Builder
			for _, part := range item.Summary {
				summary.WriteString(part.Text)
			}
			if text := summary.String(); text != "" {
				thought = &text
				if previous != nil && previous.ToolCalls != nil && previous.ReasoningContent == nil {
					previous.ReasoningContent, thought = thought, nil
				}
			}
		case "additional_tools":
			tools = append(tools, item.Tools...)
		}
	}
	chat.Messages = sequenceToolReplies(chat.Messages)
	custom := make(map[string]bool)
	var defs []ChatToolDef
	for _, tool := range tools {
		if tool.Name == "" || (tool.Kind != "function" && tool.Kind != "custom") {
			continue
		}
		parameters := tool.Parameters
		if tool.Kind == "custom" {
			custom[tool.Name] = true
			parameters = json.RawMessage(`{"type":"object","properties":{"input":{"type":"string"}},"required":["input"]}`)
		} else if len(parameters) == 0 {
			parameters = EmptySchema()
		}
		kind := "function"
		defs = append(defs, ChatToolDef{Kind: &kind, Function: &FunctionDef{
			Name: tool.Name, Description: tool.Description, Parameters: parameters,
		}})
	}
	if len(defs) > 0 {
		chat.Tools = &defs
	}
	return chat, custom
}

func sequenceToolReplies(messages []ChatMessage) []ChatMessage {
	ordered := make([]ChatMessage, 0, len(messages))
	var held []ChatMessage
	pending := make(map[string]bool)
	for _, message := range messages {
		if message.Role == "tool" && message.ToolCallID != nil && pending[*message.ToolCallID] {
			delete(pending, *message.ToolCallID)
			ordered = append(ordered, message)
			if len(pending) == 0 {
				ordered = append(ordered, held...)
				held = nil
			}
			continue
		}
		if message.Role == "tool" {
			message = ChatMessage{Role: "user", Content: &ChatContent{Text: message.Text()}}
		}
		if len(pending) > 0 && message.ToolCalls == nil {
			held = append(held, message)
			continue
		}
		ordered = append(ordered, held...)
		held = nil
		clear(pending)
		if message.ToolCalls != nil {
			for _, call := range *message.ToolCalls {
				if call.ID != nil {
					pending[*call.ID] = true
				}
			}
		}
		ordered = append(ordered, message)
	}
	return append(ordered, held...)
}

func chatParts(parts []ContentPart) *ChatContent {
	content := &ChatContent{Parts: make([]ChatPart, 0, len(parts))}
	for _, part := range parts {
		if part.Type == "input_image" {
			content.Parts = append(content.Parts, ChatPart{Type: "image_url", ImageURL: &ImageRef{Value: part.ImageURL}})
		} else {
			content.Parts = append(content.Parts, ChatPart{Type: "text", Text: part.Text})
		}
	}
	return content
}

func chatEffort(model, effort string) string {
	if strings.HasPrefix(model, "fledge") {
		switch effort {
		case "none", "minimal", "low":
			return "low"
		case "medium", "high":
			return "high"
		default:
			return "max"
		}
	}
	switch effort {
	case "none", "minimal":
		return "none"
	case "low", "medium":
		return effort
	default:
		return "high"
	}
}
