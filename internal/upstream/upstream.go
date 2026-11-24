package upstream

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type SendErrorKind int

const (
	ErrAuth SendErrorKind = iota
	ErrRateLimited
	ErrModelLimited
	ErrUpstream
	ErrBadRequest
	ErrNetwork
)

type SendError struct {
	Kind       SendErrorKind
	Message    string
	Status     int
	RetryAfter *int64
	Body       string
	Headers    http.Header
}

func (e *SendError) Error() string {
	switch e.Kind {
	case ErrAuth:
		return "upstream auth failed: " + e.Message
	case ErrRateLimited:
		return "upstream rate limited"
	case ErrModelLimited:
		return "upstream rate limited this model"
	case ErrUpstream:
		return fmt.Sprintf("upstream error %d: %s", e.Status, e.Body)
	case ErrBadRequest:
		return "bad request upstream: " + e.Message
	default:
		return "network error: " + e.Message
	}
}

type Classify struct {
	Pass          func(int) bool
	Auth          []int
	ResetHeaders  []string
	AccountFaults []string
	DeadKey       []string
}

var Strict = Classify{Auth: []int{401, 403}}

var AccountFaultRetrySecs int64 = 3600

func unixSeconds(s string) *int64 {
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339, s)
	}
	if err != nil {
		return nil
	}
	v := t.Unix()
	return &v
}

func containsAny(s string, list []string) bool {
	for _, item := range list {
		if item != "" && strings.Contains(s, item) {
			return true
		}
	}
	return false
}

// RetryAfterSecs reads a numeric retry-after or an absolute reset header,
// expressed either as RFC3339 or as epoch seconds.
func RetryAfterSecs(h http.Header, resetHeaders []string, now int64) *int64 {
	return retryAfterSecs(h, resetHeaders, now)
}

func retryAfterSecs(h http.Header, resetHeaders []string, now int64) *int64 {
	if raw := h.Get("retry-after"); raw != "" {
		if v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64); err == nil {
			return &v
		}
	}
	for _, name := range resetHeaders {
		reset := h.Get(name)
		if reset == "" {
			continue
		}
		if ts := unixSeconds(reset); ts != nil {
			v := *ts - now
			if v < 1 {
				v = 1
			}
			return &v
		}
		if f, err := strconv.ParseFloat(strings.TrimSpace(reset), 64); err == nil {
			v := int64(f)
			if v > now {
				v -= now
			}
			if v < 1 {
				v = 1
			}
			return &v
		}
	}
	return nil
}

func truncateRunes(s string, n int) string {
	runes := []rune(s)
	if len(runes) > n {
		return string(runes[:n])
	}
	return s
}

// ClassifyResponse maps an upstream response into a SendError or returns the
// response unchanged when it should be relayed verbatim.
func ClassifyResponse(resp *http.Response, rules Classify) (*http.Response, *SendError) {
	status := resp.StatusCode
	passed := rules.Pass != nil && rules.Pass(status)
	inspect := (status == 400 || status == 429) && (len(rules.AccountFaults) > 0 || len(rules.DeadKey) > 0)

	if (status >= 200 && status < 300) || (passed && !inspect) {
		return resp, nil
	}

	now := time.Now().Unix()
	retryAfter := retryAfterSecs(resp.Header, rules.ResetHeaders, now)
	headers := resp.Header.Clone()

	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	text := truncateRunes(string(body), 2000)
	rebuild := func() *http.Response {
		r := *resp
		r.Body = io.NopCloser(bytes.NewReader(body))
		return &r
	}

	if inspect && containsAny(text, rules.DeadKey) {
		return nil, &SendError{Kind: ErrAuth, Message: text, Headers: headers}
	}
	if status == 400 && containsAny(text, rules.AccountFaults) {
		secs := AccountFaultRetrySecs
		return nil, &SendError{Kind: ErrRateLimited, RetryAfter: &secs, Body: text, Headers: headers}
	}
	if passed {
		return rebuild(), nil
	}
	for _, code := range rules.Auth {
		if status == code {
			return nil, &SendError{Kind: ErrAuth, Message: text, Headers: headers}
		}
	}
	switch status {
	case 407:
		return nil, &SendError{Kind: ErrNetwork, Message: "proxy authentication failed", Headers: headers}
	case 429:
		return nil, &SendError{Kind: ErrRateLimited, RetryAfter: retryAfter, Body: text, Headers: headers}
	case 400:
		return nil, &SendError{Kind: ErrBadRequest, Message: text, Headers: headers}
	default:
		return nil, &SendError{Kind: ErrUpstream, Status: status, Body: text, Headers: headers}
	}
}

func JSON[T any](resp *http.Response, rules Classify, path string) (T, *SendError) {
	var zero T
	ok, err := ClassifyResponse(resp, rules)
	if err != nil {
		return zero, err
	}
	defer ok.Body.Close()
	if err := json.NewDecoder(ok.Body).Decode(&zero); err != nil {
		return zero, &SendError{Kind: ErrUpstream, Status: ok.StatusCode, Body: fmt.Sprintf("parsing %s: %v", path, err)}
	}
	return zero, nil
}

func NetworkError(err error) *SendError {
	return &SendError{Kind: ErrNetwork, Message: err.Error()}
}
