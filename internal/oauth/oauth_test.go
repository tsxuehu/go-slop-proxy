package oauth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestParseIntervalAcceptsNumberStringAndMissing(t *testing.T) {
	cases := []struct {
		raw  string
		want int64
	}{
		{`"7"`, 7}, {`3`, 3}, {``, 5}, {`0`, 1}, {`999`, 60},
	}
	for _, c := range cases {
		got := parseInterval(json.RawMessage(c.raw))
		if got != c.want {
			t.Errorf("parseInterval(%s) = %d, want %d", c.raw, got, c.want)
		}
	}
}

func TestTerminalRefreshClassification(t *testing.T) {
	if !terminalRefresh(403, `{"error":"bad"}`) {
		t.Error("403 should be terminal")
	}
	if !terminalRefresh(400, `{"error":"invalid_grant"}`) {
		t.Error("invalid_grant should be terminal")
	}
	if terminalRefresh(500, "temporary outage") {
		t.Error("a transient 5xx should not be terminal")
	}
}

func TestTokenSetCarriesPriorRefresh(t *testing.T) {
	now := int64(1_700_000_000)
	resp := tokenResponse{AccessToken: "access", ExpiresIn: 3600}
	set, err := tokenSetFrom(resp, "old-refresh", now)
	if err != nil {
		t.Fatal(err)
	}
	if set.RefreshToken != "old-refresh" || set.ExpiresAt != now+3600 {
		t.Errorf("unexpected token set: %+v", set)
	}
	if _, err := tokenSetFrom(resp, "", now); err == nil {
		t.Error("a response with no refresh token should fail")
	}
}

func TestJWTExp(t *testing.T) {
	claims, _ := json.Marshal(map[string]any{"exp": 1_900_000_000})
	token := "e30." + base64.RawURLEncoding.EncodeToString(claims) + ".sig"
	if got := jwtExp(token); got != 1_900_000_000 {
		t.Errorf("jwtExp = %d", got)
	}
	if got := jwtExp("not-a-jwt"); got != 0 {
		t.Errorf("jwtExp on garbage = %d", got)
	}
}

func TestGLMBizEnvelopeValidation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/ok":
			_, _ = w.Write([]byte(`{"code":200,"msg":"","data":{"accessToken":"biz-token"}}`))
		case "/zero":
			_, _ = w.Write([]byte(`{"code":"0","data":{"accessToken":"biz-token"}}`))
		case "/bad":
			_, _ = w.Write([]byte(`{"code":500,"msg":"nope","data":null}`))
		case "/nodata":
			_, _ = w.Write([]byte(`{"code":0,"data":null}`))
		}
	}))
	defer server.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	for _, path := range []string{"/ok", "/zero"} {
		got, err := glmBiz[struct {
			AccessToken string `json:"accessToken"`
		}](context.Background(), client, http.MethodGet, server.URL+path, nil, "", "test")
		if err != nil || got.AccessToken != "biz-token" {
			t.Errorf("%s: got %+v err %v", path, got, err)
		}
	}
	if _, err := glmBiz[any](context.Background(), client, http.MethodGet, server.URL+"/bad", nil, "", "test"); err == nil ||
		!strings.Contains(err.Error(), "nope") {
		t.Errorf("bad envelope should fail with msg: %v", err)
	}
	if _, err := glmBiz[any](context.Background(), client, http.MethodGet, server.URL+"/nodata", nil, "", "test"); err == nil {
		t.Error("missing data should fail")
	}
}

func TestCopilotPollErrorBranches(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("accept") != "application/json" {
			t.Error("missing accept: application/json")
		}
		switch r.URL.Path {
		case "/pending":
			_, _ = w.Write([]byte(`{"error":"authorization_pending"}`))
		case "/slow":
			_, _ = w.Write([]byte(`{"error":"slow_down"}`))
		case "/ready":
			_, _ = w.Write([]byte(`{"access_token":"gho_test"}`))
		case "/expired":
			_, _ = w.Write([]byte(`{"error":"expired_token"}`))
		}
	}))
	defer server.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	ctx := context.Background()
	for _, path := range []string{"/pending", "/slow"} {
		got, err := copilotPollAt(ctx, client, server.URL+path, "device-code")
		if err != nil || got != "" {
			t.Errorf("%s: got %q err %v", path, got, err)
		}
	}
	got, err := copilotPollAt(ctx, client, server.URL+"/ready", "device-code")
	if err != nil || got != "gho_test" {
		t.Errorf("ready: got %q err %v", got, err)
	}
	_, err = copilotPollAt(ctx, client, server.URL+"/expired", "device-code")
	if err == nil || !strings.Contains(err.Error(), "device code expired") {
		t.Errorf("expired: %v", err)
	}
}
