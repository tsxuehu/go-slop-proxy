package backend

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/edy/slop-proxy-go/internal/translate"
	"github.com/google/uuid"
)

func ChatEvents(resp *http.Response, protocol translate.BridgeProtocol, model string, custom map[string]bool, capture *translate.UsageCapture) Events {
	return func(emit func(*translate.ResponsesEvent) error) error {
		bridge := translate.NewChatToResponses(custom)
		emitAll := func(events []*translate.ResponsesEvent) error {
			for _, event := range events {
				if err := emit(event); err != nil {
					return err
				}
			}
			return nil
		}
		err := ReadChatChunks(resp, protocol, model, func(chunk *translate.ChatChunk, size int) error {
			capture.NoteBytes(int64(size))
			return emitAll(bridge.Feed(chunk))
		})
		if err != nil {
			return err
		}
		if bridge.ResponseID == nil {
			return nil
		}
		return emitAll(bridge.Finalize())
	}
}

// ReadChatChunks decodes Chat or Gemini frames into the same intermediate form.
func ReadChatChunks(resp *http.Response, protocol translate.BridgeProtocol, model string, emit func(*translate.ChatChunk, int) error) error {
	native := newGeminiChatStream(model)
	if !strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream") {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if protocol == translate.BridgeGeminiNative {
			chunk, err := native.decode(body)
			if err != nil {
				return err
			}
			return emit(chunk, len(body))
		}
		var completion struct {
			translate.ChatCompletion
			Error *translate.ChatErrorBody `json:"error"`
		}
		if err := json.Unmarshal(body, &completion); err != nil {
			return err
		}
		chunk := translate.ChatChunk{ID: completion.ID, Model: completion.Model, Created: completion.Created, Usage: completion.Usage, Error: completion.Error}
		for _, choice := range completion.Choices {
			chunk.Choices = append(chunk.Choices, translate.ChunkChoice{
				Index: choice.Index, Delta: messageDelta(choice.Message), FinishReason: choice.FinishReason,
			})
		}
		return emit(&chunk, len(body))
	}
	return translate.ParseSSE(resp.Body, func(_ string, data string) error {
		var chunk *translate.ChatChunk
		var err error
		if protocol == translate.BridgeGeminiNative {
			chunk, err = native.decode([]byte(data))
		} else {
			chunk = new(translate.ChatChunk)
			err = json.Unmarshal([]byte(data), chunk)
		}
		if err != nil {
			return nil
		}
		return emit(chunk, len(data))
	})
}

func messageDelta(message translate.ChatMessage) translate.ChatDelta {
	delta := translate.ChatDelta{
		Role: message.Role, Content: message.Text(), ToolCalls: message.ToolCalls, Images: message.Images,
	}
	if message.ReasoningContent != nil {
		delta.ReasoningContent = *message.ReasoningContent
	}
	return delta
}

type geminiContentResponse struct {
	ID         string `json:"responseId"`
	Candidates []struct {
		Index   *uint64 `json:"index"`
		Finish  string  `json:"finishReason"`
		Content struct {
			Parts []struct {
				Text      string  `json:"text"`
				Thought   bool    `json:"thought"`
				Signature *string `json:"thoughtSignature"`
				Call      *struct {
					ID   string          `json:"id"`
					Name string          `json:"name"`
					Args json.RawMessage `json:"args"`
				} `json:"functionCall"`
				InlineData *struct {
					MIME string `json:"mimeType"`
					Data string `json:"data"`
				} `json:"inlineData"`
			} `json:"parts"`
		} `json:"content"`
	} `json:"candidates"`
	Usage *struct {
		Prompt    int64  `json:"promptTokenCount"`
		Output    int64  `json:"candidatesTokenCount"`
		Reasoning int64  `json:"thoughtsTokenCount"`
		Cached    int64  `json:"cachedContentTokenCount"`
		Total     *int64 `json:"totalTokenCount"`
	} `json:"usageMetadata"`
	Error *translate.ChatErrorBody `json:"error"`
}

type geminiChatStream struct {
	id, model string
	created   int64
	seen      map[uint64]bool
	toolCount map[uint64]uint64
}

func newGeminiChatStream(model string) *geminiChatStream {
	return &geminiChatStream{
		id: "chatcmpl-" + uuid.NewString(), model: model, created: time.Now().Unix(),
		seen: make(map[uint64]bool), toolCount: make(map[uint64]uint64),
	}
}

func (g *geminiChatStream) decode(body []byte) (*translate.ChatChunk, error) {
	var response geminiContentResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}
	if response.ID != "" {
		g.id = "chatcmpl-" + response.ID
	}
	chunk := &translate.ChatChunk{
		ID: g.id, Model: g.model, Created: g.created, Object: "chat.completion.chunk",
		Choices: make([]translate.ChunkChoice, 0, len(response.Candidates)), Error: response.Error,
	}
	chunk.Usage = response.usage()
	for i, candidate := range response.Candidates {
		index := uint64(i)
		if candidate.Index != nil {
			index = *candidate.Index
		}
		choice := translate.ChunkChoice{Index: index}
		if !g.seen[index] {
			choice.Delta.Role = "assistant"
			g.seen[index] = true
		}
		var calls []translate.ChatToolCall
		var images []translate.ChatPart
		for _, part := range candidate.Content.Parts {
			if part.Thought {
				choice.Delta.ReasoningContent += part.Text
				continue
			}
			choice.Delta.Content += part.Text
			if call := part.Call; call != nil {
				id, arguments, kind := call.ID, string(call.Args), "function"
				if id == "" {
					id = "call_" + uuid.NewString()
				}
				if arguments == "" || arguments == "null" {
					arguments = "{}"
				}
				position := g.toolCount[index]
				g.toolCount[index]++
				tool := translate.ChatToolCall{ID: &id, Index: &position, Kind: &kind,
					Function: translate.FunctionBody{Name: &call.Name, Arguments: &arguments}}
				if part.Signature != nil {
					tool.Extra = &translate.ExtraContent{Google: &translate.GoogleExtra{ThoughtSignature: part.Signature}}
				}
				calls = append(calls, tool)
			}
			if image := part.InlineData; image != nil && image.Data != "" {
				mime := image.MIME
				if mime == "" {
					mime = "application/octet-stream"
				}
				images = append(images, translate.ChatPart{
					Type: "image_url", ImageURL: &translate.ImageRef{Value: "data:" + mime + ";base64," + image.Data},
				})
			}
		}
		if len(calls) > 0 {
			choice.Delta.ToolCalls = &calls
		}
		if len(images) > 0 {
			choice.Delta.Images = &images
		}
		if finish := geminiFinish(candidate.Finish, g.toolCount[index] > 0); finish != "" {
			choice.FinishReason = &finish
		}
		chunk.Choices = append(chunk.Choices, choice)
	}
	return chunk, nil
}

func GeminiChatUsage(data []byte) *translate.ChatUsage {
	var response geminiContentResponse
	if json.Unmarshal(data, &response) != nil {
		return nil
	}
	return response.usage()
}

func (r *geminiContentResponse) usage() *translate.ChatUsage {
	u := r.Usage
	if u == nil {
		return nil
	}
	total := u.Prompt + u.Output + u.Reasoning
	if u.Total != nil {
		total = *u.Total
	}
	return &translate.ChatUsage{
		PromptTokens: u.Prompt, CompletionTokens: u.Output, TotalTokens: total,
		PromptTokensDetails:     &translate.PromptTokensDetails{CachedTokens: u.Cached},
		CompletionTokensDetails: &translate.CompletionTokensDetails{ReasoningTokens: u.Reasoning},
	}
}

func geminiFinish(reason string, tools bool) string {
	switch reason {
	case "", "FINISH_REASON_UNSPECIFIED":
		return ""
	case "STOP":
		if tools {
			return "tool_calls"
		}
		return "stop"
	case "MAX_TOKENS":
		return "length"
	case "MALFORMED_FUNCTION_CALL", "UNEXPECTED_TOOL_CALL":
		return "other"
	default:
		return "content_filter"
	}
}
