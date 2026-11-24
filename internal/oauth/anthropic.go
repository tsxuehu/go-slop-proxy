package oauth

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/edy/slop-proxy-go/internal/store"
)

// Anthropic's Claude Code PKCE flow. There is no local callback server: the
// console page displays "code#state" and the user pastes it on stdin.
const (
	anthropicClientID   = "9d1c250a-e61b-44d9-88ed-5944d1962f5e"
	anthropicAuthorize  = "https://claude.ai/oauth/authorize"
	anthropicTokenURL   = "https://console.anthropic.com/v1/oauth/token"
	anthropicRedirect   = "https://console.anthropic.com/oauth/code/callback"
	anthropicScopes     = "org:create_api_key user:profile user:inference"
	anthropicProfileURL = "https://api.anthropic.com/api/oauth/profile"
)

func AnthropicLogin(ctx context.Context, client *http.Client, st *store.Store, label string) error {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return fmt.Errorf("reading the OS random source: %w", err)
	}
	verifier := base64.RawURLEncoding.EncodeToString(raw[:])
	digest := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(digest[:])

	q := url.Values{}
	q.Set("code", "true")
	q.Set("client_id", anthropicClientID)
	q.Set("response_type", "code")
	q.Set("redirect_uri", anthropicRedirect)
	q.Set("scope", anthropicScopes)
	q.Set("code_challenge", challenge)
	q.Set("code_challenge_method", "S256")
	q.Set("state", verifier)
	fmt.Printf("To authorize, open this URL in a browser:\n\n    %s?%s\n\n", anthropicAuthorize, q.Encode())
	fmt.Println("then paste the code shown on the callback page (looks like code#state):")

	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		return fmt.Errorf("reading authorization code: %w", err)
	}
	pasted := strings.TrimSpace(line)
	if pasted == "" {
		return fmt.Errorf("no authorization code provided")
	}
	code, state := pasted, verifier
	if idx := strings.IndexByte(pasted, '#'); idx >= 0 {
		code, state = pasted[:idx], pasted[idx+1:]
	}
	if state != verifier {
		return fmt.Errorf("state mismatch, paste the whole code")
	}

	body, _ := json.Marshal(map[string]string{
		"grant_type": "authorization_code", "code": code, "state": state,
		"client_id": anthropicClientID, "redirect_uri": anthropicRedirect,
		"code_verifier": verifier,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, anthropicTokenURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("token exchange request failed: %w", err)
	}
	parsed, err := okJSON[tokenResponse](resp, "token exchange")
	if err != nil {
		return err
	}
	if parsed.Account == nil || parsed.Account.UUID == "" {
		return fmt.Errorf("token response has no account uuid")
	}
	tokens, err := tokenSetFrom(parsed, "", time.Now().Unix())
	if err != nil {
		return err
	}
	plan := anthropicSubscriptionTier(ctx, client, parsed.AccessToken)
	return finishLogin(ctx, st, "anthropic", parsed.Account.UUID, parsed.Account.EmailAddress,
		label, plan, tokens)
}

// anthropicSubscriptionTier is best-effort: any failure yields "" and must
// not interrupt the login.
func anthropicSubscriptionTier(ctx context.Context, client *http.Client, accessToken string) string {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, anthropicProfileURL, nil)
	if err != nil {
		return ""
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("anthropic-beta", "oauth-2025-04-20")
	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	var profile struct {
		Organization *struct {
			RateLimitTier string `json:"rate_limit_tier"`
		} `json:"organization"`
	}
	if json.NewDecoder(resp.Body).Decode(&profile) != nil || profile.Organization == nil {
		return ""
	}
	return profile.Organization.RateLimitTier
}
