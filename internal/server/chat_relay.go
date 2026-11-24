package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/edy/slop-proxy-go/internal/backend"
	"github.com/edy/slop-proxy-go/internal/pool"
	"github.com/edy/slop-proxy-go/internal/provider"
	"github.com/edy/slop-proxy-go/internal/translate"
)

func (s *Server) relayChat(w http.ResponseWriter, rc *requestContext, body []byte, p provider.Provider,
	requested, model, effort string, stream bool, served *pool.Served) {
	switch reply := served.Response.(type) {
	case *http.Response:
		defer reply.Body.Close()
		s.relayChatBody(w, rc, body, p, requested, model, "chat", effort, served, reply, false)
	case *backend.GeminiResponse:
		defer reply.Response.Body.Close()
		if reply.Protocol == translate.BridgeGeminiNative && reply.Response.StatusCode < 300 {
			capture := &translate.UsageCapture{}
			status := writeNativeChat(w, reply.Response, requested, stream, capture)
			snap := capture.Snapshot()
			s.recordUsage(rc, p, requested, model, "chat", served.AccountID, snap.InputTokens, snap.OutputTokens,
				UsageMeta{Body: body, Captured: &snap, Effort: effort, Status: status, Attempts: int64(served.Attempts), Force: true})
			return
		}
		s.relayChatBody(w, rc, body, p, requested, model, "chat", effort, served, reply.Response, false)
	default:
		writeAPIError(w, dialectOpenAI, http.StatusBadGateway, "api_error", "unexpected upstream response")
	}
}

func (s *Server) relayChatBody(w http.ResponseWriter, rc *requestContext, body []byte, p provider.Provider,
	requested, model, dialectName, effort string, served *pool.Served, response *http.Response, native bool) {
	scan := &chatRelayUsage{capture: &translate.UsageCapture{}, native: native}
	if contentType := response.Header.Get("Content-Type"); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	status := response.StatusCode
	if strings.Contains(strings.ToLower(response.Header.Get("Content-Type")), "text/event-stream") {
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(status)
		streamObserved(w, response.Body, scan, status)
	} else {
		data, err := io.ReadAll(response.Body)
		if err != nil {
			status = http.StatusBadGateway
			scan.streamError()
			writeAPIError(w, dialectOpenAI, status, "api_error", "reading upstream response: "+err.Error())
		} else {
			scan.capture.NoteBytes(int64(len(data)))
			scan.observe(data)
			scan.finish(status)
			w.WriteHeader(status)
			_, _ = w.Write(data)
		}
	}
	snap := scan.capture.Snapshot()
	s.recordUsage(rc, p, requested, model, dialectName, served.AccountID, snap.InputTokens, snap.OutputTokens,
		UsageMeta{Body: body, Captured: &snap, Effort: effort, Status: status, Attempts: int64(served.Attempts), Force: true})
}

type chatRelayUsage struct {
	capture *translate.UsageCapture
	buf     string
	native  bool
}

func (s *chatRelayUsage) feed(chunk []byte) {
	s.capture.NoteBytes(int64(len(chunk)))
	s.buf += string(chunk)
	for {
		line, rest, ok := strings.Cut(s.buf, "\n")
		if !ok {
			return
		}
		s.buf = rest
		if data, ok := strings.CutPrefix(line, "data:"); ok {
			s.observe([]byte(strings.TrimSpace(data)))
		}
	}
}

func (s *chatRelayUsage) observe(data []byte) {
	if s.native {
		noteChatUsage(s.capture, backend.GeminiChatUsage(data))
		return
	}
	var chunk translate.ChatChunk
	if json.Unmarshal(data, &chunk) != nil {
		return
	}
	noteChatUsage(s.capture, chunk.Usage)
	for _, choice := range chunk.Choices {
		if choice.FinishReason != nil {
			s.capture.NoteStopReason(*choice.FinishReason)
		}
	}
	if chunk.Error != nil {
		s.capture.Fail("upstream_sse_error")
	}
}

func noteChatUsage(capture *translate.UsageCapture, usage *translate.ChatUsage) {
	if usage != nil {
		value := translate.UsageFromChat(usage)
		capture.Record(&value)
	}
}

func (s *chatRelayUsage) finish(status int) {
	s.capture.NoteUpstreamEOF()
	if status >= 400 {
		s.capture.Fail("upstream_rejected")
	}
}

func (s *chatRelayUsage) disconnect()  { s.capture.Fail("client_disconnect") }
func (s *chatRelayUsage) streamError() { s.capture.Fail("upstream_stream_error") }

func writeNativeChat(w http.ResponseWriter, response *http.Response, model string, stream bool,
	capture *translate.UsageCapture) int {
	var completion translate.ChatCompletion
	var responseError *translate.ChatErrorBody
	if stream {
		backend.BeginSSE(w)
	}
	flusher, _ := w.(http.Flusher)
	err := backend.ReadChatChunks(response, translate.BridgeGeminiNative, model, func(chunk *translate.ChatChunk, size int) error {
		capture.NoteBytes(int64(size))
		noteChatUsage(capture, chunk.Usage)
		responseError = chunk.Error
		if chunk.Error != nil {
			capture.Fail("upstream_failed")
		}
		if stream {
			if err := backend.WriteSSE(w, "", string(mustMarshal(chunk))); err != nil {
				capture.Fail("client_disconnect")
				return err
			}
			if flusher != nil {
				flusher.Flush()
			}
			return nil
		}
		completion = translate.ChatCompletion{
			ID: chunk.ID, Object: "chat.completion", Created: chunk.Created, Model: model, Usage: chunk.Usage,
			Choices: make([]translate.ChatChoice, 0, len(chunk.Choices)),
		}
		for _, choice := range chunk.Choices {
			message := translate.ChatMessage{Role: "assistant", ToolCalls: choice.Delta.ToolCalls, Images: choice.Delta.Images}
			if choice.Delta.Content != "" {
				message.Content = &translate.ChatContent{Text: choice.Delta.Content}
			}
			if choice.Delta.ReasoningContent != "" {
				message.ReasoningContent = &choice.Delta.ReasoningContent
			}
			completion.Choices = append(completion.Choices, translate.ChatChoice{Index: choice.Index, Message: message, FinishReason: choice.FinishReason})
		}
		return nil
	})
	if err != nil {
		capture.Fail("upstream_stream_error")
		if !stream {
			writeAPIError(w, dialectOpenAI, http.StatusBadGateway, "api_error", "reading Gemini response: "+err.Error())
			return http.StatusBadGateway
		}
	} else {
		capture.NoteUpstreamEOF()
	}
	if stream {
		if err := backend.WriteSSE(w, "", "[DONE]"); err == nil && flusher != nil {
			flusher.Flush()
		}
		return http.StatusOK
	}
	if responseError != nil {
		writeAPIError(w, dialectOpenAI, http.StatusBadGateway, "api_error", responseError.Message)
		return http.StatusBadGateway
	}
	writeJSON(w, completion)
	return http.StatusOK
}
