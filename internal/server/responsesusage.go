package server

import (
	"encoding/json"
	"strings"

	"github.com/edy/slop-proxy-go/internal/translate"
)

// responsesUsage is the verbatim observer for the Responses wire. It reuses
// translate.UsageCapture, whose Observe() already folds completed/incomplete/
// failed usage, tool calls and stop reasons out of ResponsesEvent frames.
type responsesUsage struct {
	buf     string
	capture *translate.UsageCapture
}

func newResponsesUsage() *responsesUsage {
	return &responsesUsage{capture: &translate.UsageCapture{}}
}

func (u *responsesUsage) noteBytes(n int) {
	if n > 0 {
		u.capture.NoteBytes(int64(n))
	}
}

// feed sees every chunk before it is written to the caller. Only data: lines
// are parsed; event: names and content deltas pass through untouched.
func (u *responsesUsage) feed(chunk []byte) {
	u.noteBytes(len(chunk))
	u.buf += string(chunk)
	for {
		nl := strings.IndexByte(u.buf, '\n')
		if nl < 0 {
			return
		}
		line := strings.TrimSpace(u.buf[:nl])
		u.buf = u.buf[nl+1:]
		if data, ok := strings.CutPrefix(line, "data:"); ok {
			u.observe(strings.TrimSpace(data))
		}
	}
}

// applyJSON reads a non-streaming Responses body, the counterpart of Rust's
// raw_response scanning for the terminal response.completed object.
func (u *responsesUsage) applyJSON(body []byte) {
	u.noteBytes(len(body))
	u.observe(string(body))
}

func (u *responsesUsage) observe(data string) {
	if data == "" || data == "[DONE]" {
		return
	}
	var ev translate.ResponsesEvent
	if json.Unmarshal([]byte(data), &ev) != nil || ev.Type == "" {
		return
	}
	u.capture.Observe(&ev)
}

func (u *responsesUsage) finish(status int) {
	u.capture.NoteUpstreamEOF()
	snap := u.capture.Snapshot()
	if !snap.Completed && snap.StopReason == nil && snap.ErrorKind == nil {
		if status >= 400 {
			u.capture.Fail("upstream_rejected")
		} else {
			u.capture.Fail("upstream_eof")
		}
	}
}

func (u *responsesUsage) disconnect() {
	u.capture.Fail("client_disconnect")
}

func (u *responsesUsage) streamError() {
	u.capture.Fail("upstream_sse_error")
}

func (u *responsesUsage) snapshot() *translate.CapturedUsage {
	snap := u.capture.Snapshot()
	return &snap
}
