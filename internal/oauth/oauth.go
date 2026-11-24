package oauth

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/edy/slop-proxy-go/internal/provider"
)

const (
	TokenURL       = "https://auth.openai.com/oauth/token"
	ClientID       = "app_EMoamEEZ73f0CkXaXp7hrann"
	DeviceURL      = "https://auth.openai.com/api/accounts/deviceauth/usercode"
	DevicePoll     = "https://auth.openai.com/api/accounts/deviceauth/token"
	DeviceVerify   = "https://auth.openai.com/codex/device"
	DeviceRedirect = "https://auth.openai.com/deviceauth/callback"
)

type TokenSet struct {
	AccessToken  string
	RefreshToken string
	ExpiresAt    int64
	IDToken      string
	Plan         string
	AccountID    string
	Email        string
}

type Refresher interface {
	Refresh(ctx context.Context, p provider.Provider, refreshToken string) (TokenSet, error)
}

type TerminalError struct{ msg string }

func (e *TerminalError) Error() string { return e.msg }
func IsTerminal(err error) bool {
	var t *TerminalError
	return errors.As(err, &t)
}

type CodexRefresher struct{ client *http.Client }

func NewCodexRefresher() *CodexRefresher {
	return &CodexRefresher{client: &http.Client{Timeout: 30 * time.Second}}
}

func (r *CodexRefresher) Refresh(ctx context.Context, p provider.Provider, refreshToken string) (TokenSet, error) {
	switch p {
	case provider.OpenAI:
		return r.refreshOpenAI(ctx, refreshToken)
	case provider.Anthropic:
		return r.refreshAnthropic(ctx, refreshToken)
	case provider.Copilot:
		return r.mintCopilot(ctx, refreshToken)
	default:
		return TokenSet{}, &TerminalError{"provider issues static keys"}
	}
}

// terminalRefresh reports whether a refresh response means the refresh token
// is dead, mirroring Rust's RefreshError::Terminal classification.
func terminalRefresh(status int, body string) bool {
	if status == http.StatusForbidden {
		return true
	}
	for _, code := range []string{
		"refresh_token_expired", "refresh_token_reused",
		"refresh_token_invalidated", "invalid_grant",
	} {
		if strings.Contains(body, code) {
			return true
		}
	}
	return false
}

func (r *CodexRefresher) refreshAt(ctx context.Context, endpoint, clientID, refreshToken, prior string) (TokenSet, error) {
	payload, _ := json.Marshal(map[string]string{
		"client_id": clientID, "grant_type": "refresh_token", "refresh_token": refreshToken,
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return TokenSet{}, err
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		detail := fmt.Sprintf("%d: %s", resp.StatusCode, truncate(string(body), 200))
		if terminalRefresh(resp.StatusCode, string(body)) {
			return TokenSet{}, &TerminalError{detail}
		}
		return TokenSet{}, fmt.Errorf("token refresh failed: %s", detail)
	}
	var parsed tokenResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return TokenSet{}, fmt.Errorf("bad token response: %w", err)
	}
	set, err := tokenSetFrom(parsed, prior, time.Now().Unix())
	if err != nil {
		return TokenSet{}, err
	}
	return set, nil
}

func (r *CodexRefresher) refreshOpenAI(ctx context.Context, refreshToken string) (TokenSet, error) {
	set, err := r.refreshAt(ctx, TokenURL, ClientID, refreshToken, "")
	if err != nil {
		return TokenSet{}, err
	}
	set.Plan = planFromIDToken(set.IDToken)
	return set, nil
}

func (r *CodexRefresher) refreshAnthropic(ctx context.Context, refreshToken string) (TokenSet, error) {
	// Anthropic does not always rotate its refresh token, so the old one is
	// carried over when the response omits it.
	return r.refreshAt(ctx, anthropicTokenURL, anthropicClientID, refreshToken, refreshToken)
}

func (r *CodexRefresher) mintCopilot(ctx context.Context, githubToken string) (TokenSet, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, copilotTokenURL, nil)
	req.Header.Set("Authorization", "token "+githubToken)
	req.Header.Set("Accept", "application/json")
	copilotEditorHeaders(req)
	resp, err := r.client.Do(req)
	if err != nil {
		return TokenSet{}, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == 401 || resp.StatusCode == 403 || resp.StatusCode == 404 {
		return TokenSet{}, &TerminalError{fmt.Sprintf("github grant rejected: %s", truncate(string(body), 200))}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return TokenSet{}, fmt.Errorf("%d: %s", resp.StatusCode, truncate(string(body), 200))
	}
	var out struct {
		Token     string `json:"token"`
		ExpiresAt int64  `json:"expires_at"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return TokenSet{}, fmt.Errorf("parsing copilot token: %w", err)
	}
	return TokenSet{AccessToken: out.Token, RefreshToken: githubToken, ExpiresAt: out.ExpiresAt}, nil
}

func parseIDToken(idToken string) (plan, accountID, email string) {
	parts := strings.Split(idToken, ".")
	if len(parts) != 3 {
		return "", "", ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", ""
	}
	var claims struct {
		Auth struct {
			Plan      string `json:"chatgpt_plan_type"`
			AccountID string `json:"chatgpt_account_id"`
			UserID    string `json:"user_id"`
		} `json:"https://api.openai.com/auth"`
		Email string `json:"email"`
	}
	if json.Unmarshal(payload, &claims) != nil {
		return "", "", ""
	}
	accountID = claims.Auth.AccountID
	if accountID == "" {
		accountID = claims.Auth.UserID
	}
	return claims.Auth.Plan, accountID, claims.Email
}

func planFromIDToken(idToken string) string {
	plan, _, _ := parseIDToken(idToken)
	return plan
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

type DeviceAuth struct {
	UserCode     string
	VerifyURL    string
	Interval     int64
	DeviceAuthID string
}

type devicePollResult struct {
	AuthorizationCode string `json:"authorization_code"`
	CodeVerifier      string `json:"code_verifier"`
}

func StartDeviceAuth(ctx context.Context, client *http.Client) (DeviceAuth, error) {
	payload, _ := json.Marshal(map[string]string{"client_id": ClientID})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, DeviceURL, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return DeviceAuth{}, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == 404 {
		return DeviceAuth{}, &TerminalError{"device login not enabled for this account; a workspace admin must enable Codex device auth"}
	}
	if resp.StatusCode >= 400 {
		return DeviceAuth{}, fmt.Errorf("device auth http %d: %s", resp.StatusCode, truncate(string(body), 200))
	}
	var out struct {
		DeviceAuthID string          `json:"device_auth_id"`
		UserCode     string          `json:"user_code"`
		Interval     json.RawMessage `json:"interval"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return DeviceAuth{}, err
	}
	if out.UserCode == "" {
		return DeviceAuth{}, fmt.Errorf("device auth response missing user_code")
	}
	interval := parseInterval(out.Interval)
	return DeviceAuth{UserCode: out.UserCode, VerifyURL: DeviceVerify, Interval: interval, DeviceAuthID: out.DeviceAuthID}, nil
}

func parseInterval(raw json.RawMessage) int64 {
	// Rust's parse_interval: default 5, clamped to [1, 60]. The endpoint has
	// been observed sending the interval both as a number and as a string.
	secs := int64(5)
	if len(raw) > 0 {
		var n int64
		if json.Unmarshal(raw, &n) == nil {
			secs = n
		} else if json.Unmarshal(raw, new(string)) == nil {
			var s string
			_ = json.Unmarshal(raw, &s)
			if v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil {
				secs = v
			}
		}
	}
	if secs < 1 {
		secs = 1
	}
	if secs > 60 {
		secs = 60
	}
	return secs
}

func PollDeviceAuth(ctx context.Context, client *http.Client, auth DeviceAuth) (TokenSet, error) {
	payload, _ := json.Marshal(map[string]string{
		"device_auth_id": auth.DeviceAuthID,
		"user_code":      auth.UserCode,
	})
	deadline := time.NewTimer(15 * time.Minute)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Duration(auth.Interval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return TokenSet{}, ctx.Err()
		case <-deadline.C:
			return TokenSet{}, &TerminalError{"device authorization timed out after 15 minutes"}
		case <-ticker.C:
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, DevicePoll, bytes.NewReader(payload))
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			switch resp.StatusCode {
			case 200:
				var code devicePollResult
				if err := json.Unmarshal(body, &code); err != nil {
					return TokenSet{}, fmt.Errorf("parse device poll: %w", err)
				}
				return exchangeDeviceCode(ctx, client, code)
			case 403, 404:
				// Authorization not completed yet.
				continue
			default:
				return TokenSet{}, fmt.Errorf("device poll http %d: %s", resp.StatusCode, truncate(string(body), 200))
			}
		}
	}
}

func exchangeDeviceCode(ctx context.Context, client *http.Client, code devicePollResult) (TokenSet, error) {
	form := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code.AuthorizationCode},
		"redirect_uri":  {DeviceRedirect},
		"client_id":     {ClientID},
		"code_verifier": {code.CodeVerifier},
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, TokenURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client.Do(req)
	if err != nil {
		return TokenSet{}, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return TokenSet{}, fmt.Errorf("token exchange http %d: %s", resp.StatusCode, truncate(string(body), 200))
	}
	var out struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		IDToken      string `json:"id_token"`
		ExpiresIn    int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return TokenSet{}, err
	}
	if out.RefreshToken == "" || out.IDToken == "" {
		return TokenSet{}, &TerminalError{"token exchange missing refresh_token or id_token"}
	}
	plan, accountID, email := parseIDToken(out.IDToken)
	return TokenSet{
		AccessToken: out.AccessToken, RefreshToken: out.RefreshToken,
		IDToken: out.IDToken, ExpiresAt: time.Now().Unix() + out.ExpiresIn,
		Plan: plan, AccountID: accountID, Email: email,
	}, nil
}
