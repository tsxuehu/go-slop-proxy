package oauth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/edy/slop-proxy-go/internal/store"
)

// Shared helpers for the four CLI login flows. These mirror Rust's
// oauth/mod.rs (ok_json, finish_login, jwt parsing) and oauth/refresh.rs.

func okJSON[T any](resp *http.Response, what string) (T, error) {
	var zero T
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return zero, fmt.Errorf("%s: %w", what, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return zero, fmt.Errorf("%s failed: %d: %s", what, resp.StatusCode, truncate(string(body), 2000))
	}
	var out T
	if err := json.Unmarshal(body, &out); err != nil {
		return zero, fmt.Errorf("parsing %s response: %w", what, err)
	}
	return out, nil
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	IDToken      string `json:"id_token"`
	ExpiresIn    int64  `json:"expires_in"`
	Account      *struct {
		UUID         string `json:"uuid"`
		EmailAddress string `json:"email_address"`
	} `json:"account"`
}

// jwtExp reads the exp claim of an access token without verifying its
// signature, matching Rust's jwt::exp.
func jwtExp(token string) int64 {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return 0
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return 0
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if json.Unmarshal(payload, &claims) != nil {
		return 0
	}
	return claims.Exp
}

func tokenSetFrom(resp tokenResponse, priorRefresh string, now int64) (TokenSet, error) {
	refresh := resp.RefreshToken
	if refresh == "" {
		refresh = priorRefresh
	}
	if refresh == "" {
		return TokenSet{}, fmt.Errorf("no refresh_token in token response")
	}
	expires := jwtExp(resp.AccessToken)
	if expires == 0 && resp.ExpiresIn > 0 {
		expires = now + resp.ExpiresIn
	}
	return TokenSet{
		AccessToken: resp.AccessToken, RefreshToken: refresh,
		IDToken: resp.IDToken, ExpiresAt: expires,
	}, nil
}

// finishLogin is the shared store/print tail of the OpenAI, Anthropic and
// Copilot logins.
func finishLogin(ctx context.Context, st *store.Store, provider, accountID, email, label, plan string, tokens TokenSet) error {
	if label == "" {
		label = email
	}
	acc := &store.Account{
		Provider: provider, ProviderAccountID: accountID, Email: email, Label: label,
		Plan: plan, AuthMode: "oauth", Status: "active",
		AccessToken: tokens.AccessToken, RefreshToken: tokens.RefreshToken,
		AccessExpiresAt: tokens.ExpiresAt,
	}
	if err := st.InsertAccount(ctx, acc); err != nil {
		return err
	}
	planText := ""
	if plan != "" {
		planText = ", plan " + plan
	}
	if email == "" {
		email = "unknown email"
	}
	fmt.Printf("logged in: %s account %d (%s%s)\n", provider, acc.ID, email, planText)
	return nil
}
