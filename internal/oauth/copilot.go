package oauth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/edy/slop-proxy-go/internal/store"
)

// GitHub device flow used by VS Code Copilot Chat.
const (
	copilotClientID    = "Iv1.b507a08c87ecfe98"
	copilotDeviceURL   = "https://github.com/login/device/code"
	copilotAccessToken = "https://github.com/login/oauth/access_token"
	copilotTokenURL    = "https://api.github.com/copilot_internal/v2/token"
	copilotUserURL     = "https://api.github.com/user"
	copilotUserAgent   = "GitHubCopilotChat/0.26.7"
	copilotEditorVer   = "vscode/1.105.0"
	copilotPluginVer   = "copilot-chat/0.26.7"
	copilotAPIVersion  = "2025-04-01"
)

func copilotEditorHeaders(req *http.Request) {
	req.Header.Set("editor-version", copilotEditorVer)
	req.Header.Set("editor-plugin-version", copilotPluginVer)
	req.Header.Set("user-agent", copilotUserAgent)
	req.Header.Set("x-github-api-version", copilotAPIVersion)
}

// GithubLogin reads the GitHub username for a grant. The API expects a
// "token " prefix here, not "Bearer ".
func GithubLogin(ctx context.Context, client *http.Client, githubToken string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, copilotUserURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("authorization", "token "+githubToken)
	req.Header.Set("user-agent", copilotUserAgent)
	req.Header.Set("accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("reading github user: %w", err)
	}
	user, err := okJSON[struct {
		Login string `json:"login"`
	}](resp, "reading github user")
	if err != nil {
		return "", err
	}
	return user.Login, nil
}

func CopilotLogin(ctx context.Context, client *http.Client, st *store.Store, label string) error {
	body, _ := json.Marshal(map[string]string{"client_id": copilotClientID, "scope": "read:user"})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, copilotDeviceURL, bytes.NewReader(body))
	req.Header.Set("accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("requesting device code: %w", err)
	}
	code, err := okJSON[struct {
		DeviceCode      string `json:"device_code"`
		UserCode        string `json:"user_code"`
		VerificationURI string `json:"verification_uri"`
		Interval        int64  `json:"interval"`
	}](resp, "device code")
	if err != nil {
		return err
	}
	fmt.Printf("To authorize, open this URL in a browser:\n\n    %s\n\nand enter this code:\n\n    %s\n\n",
		code.VerificationURI, code.UserCode)
	fmt.Println("Waiting for authorization (up to 15 minutes)...")

	interval := code.Interval
	if interval < 5 {
		interval = 5
	}
	if interval > 30 {
		interval = 30
	}
	deadline := time.Now().Add(15 * time.Minute)
	var githubToken string
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("device authorization timed out")
		}
		time.Sleep(time.Duration(interval) * time.Second)
		grant, pollErr := copilotPoll(ctx, client, code.DeviceCode)
		if pollErr != nil {
			return pollErr
		}
		if grant != "" {
			githubToken = grant
			break
		}
	}
	login, err := GithubLogin(ctx, client, githubToken)
	if err != nil {
		return err
	}
	tokens := TokenSet{AccessToken: githubToken, RefreshToken: githubToken, ExpiresAt: 0}
	return finishLogin(ctx, st, "copilot", login, login, label, "", tokens)
}

func copilotPoll(ctx context.Context, client *http.Client, deviceCode string) (string, error) {
	return copilotPollAt(ctx, client, copilotAccessToken, deviceCode)
}

func copilotPollAt(ctx context.Context, client *http.Client, endpoint, deviceCode string) (string, error) {
	body, _ := json.Marshal(map[string]string{
		"client_id":   copilotClientID,
		"device_code": deviceCode,
		"grant_type":  "urn:ietf:params:oauth:grant-type:device_code",
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	req.Header.Set("accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("polling device authorization: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var grant struct {
		AccessToken      string `json:"access_token"`
		Error            string `json:"error"`
		ErrorDescription string `json:"error_description"`
	}
	_ = json.Unmarshal(raw, &grant)
	if grant.AccessToken != "" {
		return grant.AccessToken, nil
	}
	switch grant.Error {
	case "", "authorization_pending", "slow_down":
		// slow_down deliberately does not increase the interval here, matching
		// the Rust implementation.
		return "", nil
	case "expired_token":
		return "", fmt.Errorf("device code expired, run login again")
	case "access_denied":
		return "", fmt.Errorf("authorization denied")
	default:
		return "", fmt.Errorf("device authorization failed: %s: %s", grant.Error, grant.ErrorDescription)
	}
}
