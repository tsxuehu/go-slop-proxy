package backend

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/edy/slop-proxy-go/internal/translate"
)

// Events delivers each parsed event while the upstream body is still open.
type Events func(func(*translate.ResponsesEvent) error) error

func ResponsesEvents(r io.Reader, capture *translate.UsageCapture) Events {
	return func(emit func(*translate.ResponsesEvent) error) error {
		return translate.ParseSSE(r, func(eventType, data string) error {
			capture.NoteBytes(int64(len(data)))
			capture.NoteUpstreamHead([]byte(data))
			var event translate.ResponsesEvent
			if json.Unmarshal([]byte(data), &event) != nil {
				return nil
			}
			if event.Type == "" {
				event.Type = eventType
			}
			return emit(&event)
		})
	}
}

func CollectResponses(events Events, capture *translate.UsageCapture) translate.Aggregated {
	var received []*translate.ResponsesEvent
	if err := events(func(event *translate.ResponsesEvent) error {
		received = append(received, event)
		return nil
	}); err != nil {
		capture.Fail("upstream_stream_error")
	} else {
		capture.NoteUpstreamEOF()
	}
	return translate.Aggregate(received, capture)
}

func WriteSSE(w http.ResponseWriter, event, data string) error {
	if event != "" {
		if _, err := io.WriteString(w, "event: "+event+"\n"); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, "data: "+strings.ReplaceAll(data, "\n", "\ndata: ")+"\n\n")
	return err
}

func BeginSSE(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
}

func StreamChatResponses(events Events, model string, includeUsage bool, capture *translate.UsageCapture, w http.ResponseWriter, flusher http.Flusher) {
	BeginSSE(w)
	stream := translate.NewOpenAiStream(model, includeUsage, capture)
	write := func(frames []string) error {
		for _, frame := range frames {
			if err := WriteSSE(w, "", frame); err != nil {
				capture.Fail("client_disconnect")
				return err
			}
		}
		if len(frames) > 0 {
			flusher.Flush()
		}
		return nil
	}
	if err := events(func(event *translate.ResponsesEvent) error { return write(stream.Feed(event)) }); err != nil {
		capture.Fail("upstream_stream_error")
	} else {
		capture.NoteUpstreamEOF()
	}
	if write(stream.Finalize()) == nil {
		_ = write([]string{"[DONE]"})
	}
}

func StreamAnthropicResponses(events Events, model string, estInput int64, emitThinking bool, capture *translate.UsageCapture, w http.ResponseWriter, flusher http.Flusher) {
	BeginSSE(w)
	stream := translate.NewAnthropicStream(model, estInput, emitThinking, capture)
	write := func(frames []translate.OutEvent) error {
		for _, frame := range frames {
			if err := WriteSSE(w, frame.Name, frame.Body); err != nil {
				capture.Fail("client_disconnect")
				return err
			}
		}
		if len(frames) > 0 {
			flusher.Flush()
		}
		return nil
	}
	if err := events(func(event *translate.ResponsesEvent) error { return write(stream.Feed(event)) }); err != nil {
		capture.Fail("upstream_stream_error")
	} else {
		capture.NoteUpstreamEOF()
	}
	_ = write(stream.Finalize())
}
