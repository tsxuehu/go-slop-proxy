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
	head := readVerdictHead(resp.Body)
	replay := func() *Response {
		return &Response{
			StatusCode: resp.StatusCode,
			Header:     resp.Header,
			Body:       io.NopCloser(io.MultiReader(bytes.NewReader(head), resp.Body)),
		}
	}
	switch openingVerdict(head) {
	case verdictRefused:
		secs := int64(earlyRefusalCooldown)
		for _, code := range exhaustedCodes {
			if strings.Contains(string(head), code) {
				secs = exhaustedRefusalCooldown
				break
			}
		}
		return nil, &pool.SendError{Kind: pool.SendRateLimited, RetryAfter: &secs, Body: string(head)}
	case verdictUndecryptable:
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
func readVerdictHead(body io.Reader) []byte {
	type readResult struct {
		b   []byte
		err error
	}
	reqCh := make(chan struct{}, 1)
	resCh := make(chan readResult, 1)
	go func() {
		buf := make([]byte, 8192)
		for range reqCh {
			n, err := body.Read(buf)
			var b []byte
			if n > 0 {
				b = append([]byte(nil), buf[:n]...)
			}
			resCh <- readResult{b: b, err: err}
			if err != nil {
				return
			}
		}
	}()
	defer close(reqCh)

	timer := time.NewTimer(verdictDeadline)
	defer timer.Stop()
	var head []byte
	for {
		reqCh <- struct{}{}
		select {
		case r := <-resCh:
			if len(r.b) > 0 {
				head = append(head, r.b...)
				if openingVerdict(head) != verdictPending {
					return head
				}
				if len(head) > verdictHeadLimit {
					return head
				}
			}
			if r.err != nil {
				return head
			}
		case <-timer.C:
			return head
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
