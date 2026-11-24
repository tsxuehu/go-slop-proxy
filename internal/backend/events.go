package backend

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/edy/slop-proxy-go/internal/translate"
)

// ParseResponsesEvents reads an upstream Responses SSE stream into IR events.
func ParseResponsesEvents(r io.Reader, capture *translate.UsageCapture, onBytes func(int64)) []*translate.ResponsesEvent {
	var events []*translate.ResponsesEvent
	_ = translate.ParseSSE(r, func(eventType, data string) error {
		if onBytes != nil {
			onBytes(int64(len(data)))
		}
		if capture != nil {
			capture.NoteUpstreamHead([]byte(data))
		}
		var ev translate.ResponsesEvent
		if json.Unmarshal([]byte(data), &ev) != nil {
			return nil // skip malformed frames
		}
		if ev.Type == "" {
			ev.Type = eventType
		}
		events = append(events, &ev)
		return nil
	})
	return events
}

// StreamChatResponses renders a Responses event stream as OpenAI Chat SSE.
func StreamChatResponses(events []*translate.ResponsesEvent, model string, includeUsage bool, capture *translate.UsageCapture, w http.ResponseWriter, flusher http.Flusher) (int64, int64) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	stream := translate.NewOpenAiStream(model, includeUsage, capture)
	write := func(frames []string) {
		for _, frame := range frames {
			_, _ = io.WriteString(w, "data: "+frame+"\n\n")
		}
		if len(frames) > 0 {
			flusher.Flush()
		}
	}
	for _, e := range events {
		write(stream.Feed(e))
	}
	write(stream.Finalize())
	_, _ = io.WriteString(w, "data: [DONE]\n\n")
	flusher.Flush()

	snap := capture.Snapshot()
	return snap.InputTokens, snap.OutputTokens
}

// CollectChatResponses aggregates a Responses event stream into a ChatCompletion.
func CollectChatResponses(events []*translate.ResponsesEvent, model string, capture *translate.UsageCapture) []byte {
	agg := translate.Aggregate(events, capture)
	return translate.RenderAggregatedChat(&agg, model)
}

// StreamAnthropicResponses renders a Responses event stream as Anthropic SSE.
func StreamAnthropicResponses(events []*translate.ResponsesEvent, model string, estInput int64, emitThinking bool, capture *translate.UsageCapture, w http.ResponseWriter, flusher http.Flusher) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	stream := translate.NewAnthropicStream(model, estInput, emitThinking, capture)
	write := func(evs []translate.OutEvent) {
		for _, e := range evs {
			_, _ = io.WriteString(w, "event: "+e.Name+"\ndata: "+e.Body+"\n\n")
		}
		if len(evs) > 0 {
			flusher.Flush()
		}
	}
	for _, e := range events {
		write(stream.Feed(e))
	}
	write(stream.Finalize())
}

var _ = time.Now
