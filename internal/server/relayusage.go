package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/edy/slop-proxy-go/internal/translate"
)

// relayNumbers is the usage object Rust's RelayEvent parses out of a Messages
// response. The same shape shows up in message_start.message.usage,
// message_delta.usage and a non-streaming response's top-level usage.
type relayNumbers struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
	CacheRead    int64 `json:"cache_read_input_tokens"`
	CacheWrite   int64 `json:"cache_creation_input_tokens"`
}

type relayMessageEnvelope struct {
	Usage *relayNumbers `json:"usage"`
}

type relayStopDelta struct {
	StopReason *string `json:"stop_reason"`
}

type relayContentBlock struct {
	Name string `json:"name"`
}

// relayEvent mirrors the tagged RelayEvent enum in server/relay.rs: only the
// bookkeeping event kinds are parsed, every content delta passes through.
type relayEvent struct {
	Type         string                `json:"type"`
	Message      *relayMessageEnvelope `json:"message"`
	Usage        *relayNumbers         `json:"usage"`
	Delta        *relayStopDelta       `json:"delta"`
	ContentBlock *relayContentBlock    `json:"content_block"`
}

// relayUsage is a pure observer of the bytes relayed back to the caller. It
// never rewrites them; it only collects the dimensions Rust's SseScan,
// UsageCapture and LogGuard fold into UsageRecord.
type relayUsage struct {
	buf         string
	interesting bool
	eof         bool

	input, output, cacheRead, cacheWrite int64
	toolsCalled                          []string
	stopReason                           string
	errorKind                            string
	completed                            bool
	firstByteAt                          *time.Time
	responseBytes                        int64
}

func newRelayUsage() *relayUsage { return &relayUsage{} }

func (u *relayUsage) noteBytes(n int) {
	if n <= 0 {
		return
	}
	u.responseBytes += int64(n)
	if u.firstByteAt == nil {
		now := time.Now()
		u.firstByteAt = &now
	}
}

// feed sees each chunk before it reaches the client, so a caller that goes
// away mid-stream still leaves a row with the bytes already read.
func (u *relayUsage) feed(chunk []byte) {
	u.noteBytes(len(chunk))
	u.buf += string(chunk)
	for {
		nl := strings.IndexByte(u.buf, '\n')
		if nl < 0 {
			return
		}
		line := strings.TrimSpace(u.buf[:nl])
		u.buf = u.buf[nl+1:]
		if name, ok := strings.CutPrefix(line, "event:"); ok {
			name = strings.TrimSpace(name)
			u.interesting = name == "message_start" || name == "message_delta" ||
				name == "message_stop" || name == "content_block_start" || name == "error"
			continue
		}
		if !u.interesting {
			continue
		}
		if data, ok := strings.CutPrefix(line, "data:"); ok {
			u.apply(strings.TrimSpace(data))
		}
	}
}

// applyJSON reads the usage object out of a non-streaming Messages response,
// the equivalent of SseScan::body.
func (u *relayUsage) applyJSON(body []byte) {
	var env relayMessageEnvelope
	if json.Unmarshal(body, &env) == nil && env.Usage != nil {
		u.setUsage(env.Usage)
	}
}

func (u *relayUsage) apply(data string) {
	var ev relayEvent
	if json.Unmarshal([]byte(data), &ev) != nil {
		return
	}
	switch ev.Type {
	case "message_start":
		if ev.Message != nil && ev.Message.Usage != nil {
			u.setUsage(ev.Message.Usage)
		}
	case "message_delta":
		if ev.Usage != nil {
			u.output = ev.Usage.OutputTokens
			// Zen reports a streamed turn's input as zero in message_start and
			// only settles it here, where Anthropic sends nothing.
			if ev.Usage.InputTokens > 0 {
				u.input = ev.Usage.InputTokens
				u.cacheRead = ev.Usage.CacheRead
				u.cacheWrite = ev.Usage.CacheWrite
			}
		}
		if ev.Delta != nil && ev.Delta.StopReason != nil && *ev.Delta.StopReason != "" {
			u.stopReason = *ev.Delta.StopReason
		}
	case "content_block_start":
		if ev.ContentBlock != nil && ev.ContentBlock.Name != "" {
			u.noteTool(ev.ContentBlock.Name)
		}
	case "message_stop":
		u.completed = true
	case "error":
		if u.errorKind == "" {
			u.errorKind = "upstream_error"
		}
	}
}

func (u *relayUsage) setUsage(n *relayNumbers) {
	u.input = n.InputTokens
	u.output = n.OutputTokens
	u.cacheRead = n.CacheRead
	u.cacheWrite = n.CacheWrite
}

func (u *relayUsage) noteTool(name string) {
	for _, n := range u.toolsCalled {
		if n == name {
			return
		}
	}
	u.toolsCalled = append(u.toolsCalled, name)
}

// finish marks a clean upstream EOF; LogGuard::drop turns an unfinished turn
// into upstream_eof rather than client_disconnect in that case.
func (u *relayUsage) finish(status int) {
	u.eof = true
	if !u.completed && u.stopReason == "" && u.errorKind == "" {
		if status >= 400 {
			u.errorKind = "upstream_rejected"
		} else {
			u.errorKind = "upstream_eof"
		}
	}
}

func (u *relayUsage) disconnect() {
	if !u.completed && u.stopReason == "" && u.errorKind == "" {
		u.errorKind = "client_disconnect"
	}
}

func (u *relayUsage) streamError() {
	if u.errorKind == "" {
		u.errorKind = "upstream_stream_error"
	}
}

func (u *relayUsage) snapshot() *translate.CapturedUsage {
	out := &translate.CapturedUsage{
		InputTokens: u.input, OutputTokens: u.output,
		CacheReadTokens: u.cacheRead, CacheWriteTokens: u.cacheWrite,
		Completed: u.completed, UpstreamEOF: u.eof,
		FirstByteAt: u.firstByteAt, ResponseBytes: u.responseBytes,
		ToolsCalled: append([]string(nil), u.toolsCalled...),
	}
	if u.stopReason != "" {
		s := u.stopReason
		out.StopReason = &s
	}
	if u.errorKind != "" {
		s := u.errorKind
		out.ErrorKind = &s
	}
	return out
}

// streamObserver is implemented by both verbatim observers. Keeping the
// interface here lets relayMessages and responses share one relay loop.
type streamObserver interface {
	feed([]byte)
	finish(int)
	disconnect()
	streamError()
}

// streamObserved relays bytes verbatim while letting an observer see every
// chunk first. A client that disappears mid-stream still leaves a usage row
// for the bytes already read, matching Rust's LogGuard drop behaviour.
func streamObserved(w http.ResponseWriter, body io.Reader, obs streamObserver, status int) {
	flusher, _ := w.(http.Flusher)
	chunk := make([]byte, 32*1024)
	for {
		n, readErr := body.Read(chunk)
		if n > 0 {
			obs.feed(chunk[:n])
			if _, err := w.Write(chunk[:n]); err != nil {
				obs.disconnect()
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				obs.finish(status)
			} else {
				obs.streamError()
			}
			return
		}
	}
}
