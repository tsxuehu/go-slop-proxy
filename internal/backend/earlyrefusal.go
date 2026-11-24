package backend

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/edy/slop-proxy-go/internal/pool"
)

const (
	earlyRefusalCooldown     = 60
	exhaustedRefusalCooldown = 15 * 60 // 900
	verdictHeadLimit         = 256 * 1024
	verdictDeadline          = 35 * time.Second
)

// Codes that mean the subscription is spent rather than momentarily busy;
// those get the longer cooldown so we stop hammering a drained account.
var exhaustedCodes = []string{
	"usage_limit_reached", "usage_not_included", "insufficient_quota",
}

type openingVerdictKind int

const (
	verdictPending openingVerdictKind = iota
	verdictServe
	verdictRefused
	verdictUndecryptable
)

// refuseEarly inspects the opening frames of an HTTP 200 stream. Codex can
// report an account-level refusal as an `error` event inside a successful SSE
// response; treating that as a rate limit lets the pool cool the account and
// fail over instead of handing the error to the caller. The buffered head is
// replayed verbatim whenever the stream is safe to serve.
func refuseEarly(resp *http.Response) (*Response, error) {
	head, pending := readVerdictHead(resp.Body, verdictDeadline)
	replay := func() *Response {
		var readers []io.Reader
		readers = append(readers, bytes.NewReader(head))
		if pending != nil {
			readers = append(readers, &pendingBodyRead{result: pending})
		}
		readers = append(readers, resp.Body)
		return &Response{
			StatusCode: resp.StatusCode,
			Header:     resp.Header,
			Body: struct {
				io.Reader
				io.Closer
			}{io.MultiReader(readers...), resp.Body},
		}
	}
	switch openingVerdict(head) {
	case verdictRefused:
		_ = resp.Body.Close()
		secs := int64(earlyRefusalCooldown)
		for _, code := range exhaustedCodes {
			if strings.Contains(string(head), code) {
				secs = exhaustedRefusalCooldown
				break
			}
		}
		return nil, &pool.SendError{Kind: pool.SendRateLimited, RetryAfter: &secs, Body: string(head)}
	case verdictUndecryptable:
		_ = resp.Body.Close()
		// The Rust original retries after dropping encrypted payloads. We do
		// not carry that retry here, so surface it as a bad request instead of
		// cooling an otherwise healthy account.
		return nil, &pool.SendError{Kind: pool.SendBadRequest, Body: "invalid_encrypted_content"}
	default:
		return replay(), nil
	}
}

// readVerdictHead buffers the stream prefix until a decision can be made, the
// 256 KiB head budget is exhausted, or the 35s verdict deadline elapses. A
// single reader goroutine is driven one chunk at a time so it never consumes
// bytes beyond the prefix we are going to replay.
type bodyRead struct {
	data []byte
	err  error
}

// pendingBodyRead owns the read already in progress when the verdict times
// out. Replay must finish this read before starting another on the same body.
type pendingBodyRead struct {
	result <-chan bodyRead
	buffer *bytes.Reader
	err    error
}

func (r *pendingBodyRead) Read(p []byte) (int, error) {
	if r.result != nil {
		result := <-r.result
		r.result = nil
		r.buffer, r.err = bytes.NewReader(result.data), result.err
	}
	if r.buffer.Len() > 0 {
		return r.buffer.Read(p)
	}
	if r.err != nil {
		err := r.err
		r.err = nil
		return 0, err
	}
	return 0, io.EOF
}

func readVerdictHead(body io.Reader, deadline time.Duration) ([]byte, <-chan bodyRead) {
	request := make(chan struct{}, 1)
	results := make(chan bodyRead, 1)
	go func() {
		buffer := make([]byte, 8192)
		for range request {
			n, err := body.Read(buffer)
			results <- bodyRead{data: bytes.Clone(buffer[:n]), err: err}
			if err != nil {
				return
			}
		}
	}()
	defer close(request)

	timer := time.NewTimer(deadline)
	defer timer.Stop()
	var head []byte
	for {
		request <- struct{}{}
		select {
		case result := <-results:
			head = append(head, result.data...)
			if result.err != nil || len(head) >= verdictHeadLimit || openingVerdict(head) != verdictPending {
				return head, nil
			}
		case <-timer.C:
			return head, results
		}
	}
}

// openingVerdict classifies complete `\n\n`-delimited SSE frames by their JSON
// `type` field (not the SSE `event:` line), mirroring the Rust opening().
func openingVerdict(head []byte) openingVerdictKind {
	for _, frame := range bytes.Split(head, []byte("\n\n")) {
		var data []byte
		for _, line := range bytes.Split(frame, []byte("\n")) {
			if bytes.HasPrefix(line, []byte("data:")) {
				data = append(data, bytes.TrimSpace(line[len("data:"):])...)
				data = append(data, '\n')
			}
		}
		data = bytes.TrimSpace(data)
		if len(data) == 0 {
			continue
		}
		var ev struct {
			Type  string `json:"type"`
			Error *struct {
				Code    string `json:"code"`
				Type    string `json:"type"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(data, &ev) != nil {
			continue
		}
		switch ev.Type {
		case "response.created", "response.in_progress", "keepalive":
			continue
		case "error":
			if ev.Error == nil {
				return verdictRefused
			}
			if ev.Error.Code == "invalid_encrypted_content" {
				return verdictUndecryptable
			}
			// Caller errors and the transient "protection unavailable" case
			// are safe to serve; everything else is an account refusal.
			if ev.Error.Type == "invalid_request_error" ||
				ev.Error.Message == "response protection is unavailable" {
				return verdictServe
			}
			return verdictRefused
		default:
			// Includes response.failed, which carries billable usage and is
			// intentionally served rather than cooled.
			return verdictServe
		}
	}
	return verdictPending
}
