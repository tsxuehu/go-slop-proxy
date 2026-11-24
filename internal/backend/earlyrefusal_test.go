package backend

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/edy/slop-proxy-go/internal/pool"
)

func sseResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func frame(v string) string { return "data: " + v + "\n\n" }

func TestRefuseEarlyRateLimitsAnErrorFrame(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantSec int64
	}{
		{"busy", frame(`{"type":"error","error":{"code":"model_at_capacity","message":"busy"}}`), earlyRefusalCooldown},
		{"exhausted", frame(`{"type":"error","error":{"code":"usage_limit_reached"}}`), exhaustedRefusalCooldown},
		{"insufficient_quota", frame(`{"type":"error","error":{"message":"insufficient_quota"}}`), exhaustedRefusalCooldown},
	}
	for _, c := range cases {
		resp, err := refuseEarly(sseResponse(c.body))
		if resp != nil {
			t.Errorf("%s: expected no response, got one", c.name)
			continue
		}
		se, ok := err.(*pool.SendError)
		if !ok || se.Kind != pool.SendRateLimited || se.RetryAfter == nil || *se.RetryAfter != c.wantSec {
			t.Errorf("%s: err = %v, want RateLimited/%ds", c.name, err, c.wantSec)
		}
	}
}

func TestRefuseEarlyServesAndReplays(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"created then delta",
			frame(`{"type":"response.created","response":{"id":"r1"}}`) +
				frame(`{"type":"response.output_text.delta","delta":"hi"}`)},
		{"response.failed is billable", frame(`{"type":"response.failed","response":{"id":"r1"}}`)},
		{"caller error", frame(`{"type":"error","error":{"type":"invalid_request_error","message":"bad"}}`)},
		{"protection unavailable", frame(`{"type":"error","error":{"message":"response protection is unavailable"}}`)},
		{"plain json", `{"object":"response","output":[]}`},
	}
	for _, c := range cases {
		resp, err := refuseEarly(sseResponse(c.body))
		if err != nil || resp == nil {
			t.Errorf("%s: expected a response, got resp=%v err=%v", c.name, resp, err)
			continue
		}
		got, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(got) != c.body {
			t.Errorf("%s: replayed %q, want %q", c.name, string(got), c.body)
		}
	}
}

func TestRefuseEarlyUndecryptableIsBadRequest(t *testing.T) {
	resp, err := refuseEarly(sseResponse(frame(`{"type":"error","error":{"code":"invalid_encrypted_content"}}`)))
	if resp != nil {
		t.Fatal("expected no response")
	}
	se, ok := err.(*pool.SendError)
	if !ok || se.Kind != pool.SendBadRequest {
		t.Fatalf("err = %v, want SendBadRequest", err)
	}
}

func TestOpeningVerdictKeepaliveStaysPending(t *testing.T) {
	head := frame(`{"type":"response.created"}`) + frame(`{"type":"keepalive"}`)
	if v := openingVerdict([]byte(head)); v != verdictPending {
		t.Fatalf("verdict = %v, want pending", v)
	}
}
