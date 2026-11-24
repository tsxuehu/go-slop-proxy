package oauth

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

// z.ai's CLI OAuth flow. There is no local QR generation: the authorize_url
// is printed and opened in a browser, and the remote page shows the code.
const (
	glmCLIOAuth     = "https://zcode.z.ai/api/v1/oauth/cli"
	glmBizHost      = "https://api.z.ai"
	glmKeyName      = "zcode-api-key"
	glmDefaultOrg   = "默认机构"
	glmDefaultProj  = "默认项目"
	glmFlowProvider = "zai"
)

type glmEnvelope struct {
	Code json.RawMessage `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

func glmBiz[T any](ctx context.Context, client *http.Client, method, url string, body []byte, bearer, what string) (T, error) {
	var zero T
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return zero, fmt.Errorf("%s request failed: %w", what, err)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return zero, fmt.Errorf("%s request failed: %w", what, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return zero, fmt.Errorf("%s: %w", what, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return zero, fmt.Errorf("%s failed: %d: %s", what, resp.StatusCode, truncate(string(raw), 2000))
	}
	var env glmEnvelope
	if json.Unmarshal(raw, &env) != nil {
		return zero, fmt.Errorf("parsing %s response", what)
	}
	if len(env.Code) > 0 && string(env.Code) != "null" {
		code := strings.Trim(string(env.Code), `"`)
		if code != "0" && code != "200" {
			return zero, fmt.Errorf("%s failed: %s", what, env.Msg)
		}
	}
	if len(env.Data) == 0 || string(env.Data) == "null" {
		return zero, fmt.Errorf("%s response carried no data", what)
	}
	var out T
	if err := json.Unmarshal(env.Data, &out); err != nil {
		return zero, fmt.Errorf("parsing %s data: %w", what, err)
	}
	return out, nil
}

// GLMLogin runs the whole z.ai flow and returns the static API key
// ("{apiKey}.{secretKey}") that the CLI stores via accounts add-key.
func GLMLogin(ctx context.Context, client *http.Client) (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("reading the OS random source: %w", err)
	}
	pollToken := hex.EncodeToString(raw[:])
	initBody, _ := json.Marshal(map[string]string{"provider": glmFlowProvider})
	flow, err := glmBiz[struct {
		FlowID          string `json:"flow_id"`
		AuthorizeURL    string `json:"authorize_url"`
		ExpiresAt       int64  `json:"expires_at"`
		PollIntervalSec int64  `json:"poll_interval_sec"`
	}](ctx, client, http.MethodPost, glmCLIOAuth+"/init", initBody, pollToken, "oauth init")
	if err != nil {
		return "", err
	}
	fmt.Printf("To authorize, open this URL in a browser:\n\n    %s\n\n", flow.AuthorizeURL)
	opener := "xdg-open"
	if runtime.GOOS == "darwin" {
		opener = "open"
	}
	_ = exec.Command(opener, flow.AuthorizeURL).Start()
	fmt.Println("Waiting for authorization...")

	remaining := flow.ExpiresAt - time.Now().Unix()
	if remaining < 0 {
		remaining = 0
	}
	if remaining > 300 {
		remaining = 300
	}
	deadline := time.Now().Add(time.Duration(remaining) * time.Second)
	interval := flow.PollIntervalSec
	if interval < 1 {
		interval = 1
	}
	var grant string
	for {
		if time.Now().After(deadline) {
			return "", fmt.Errorf("authorization timed out")
		}
		time.Sleep(time.Duration(interval) * time.Second)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
			glmCLIOAuth+"/poll/"+flow.FlowID, nil)
		req.Header.Set("Authorization", "Bearer "+pollToken)
		resp, err := client.Do(req)
		if err != nil {
			return "", fmt.Errorf("oauth poll request failed: %w", err)
		}
		status := resp.StatusCode
		raw, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		switch status {
		case http.StatusTooManyRequests:
			continue
		case http.StatusBadRequest:
			return "", fmt.Errorf("the sign-in link expired, run login again")
		}
		var env glmEnvelope
		if json.Unmarshal(raw, &env) != nil {
			return "", fmt.Errorf("oauth poll failed: invalid response")
		}
		if status < 200 || status >= 300 {
			return "", fmt.Errorf("oauth poll failed: %d: %s", status, truncate(string(raw), 2000))
		}
		var state struct {
			Status string `json:"status"`
			ZAI    *struct {
				AccessToken string `json:"access_token"`
			} `json:"zai"`
		}
		if json.Unmarshal(env.Data, &state) != nil {
			return "", fmt.Errorf("oauth poll failed: invalid state")
		}
		switch state.Status {
		case "pending":
		case "failed":
			return "", fmt.Errorf("authorization failed")
		case "ready":
			if state.ZAI == nil {
				return "", fmt.Errorf("authorization failed")
			}
			grant = state.ZAI.AccessToken
		default:
			return "", fmt.Errorf("authorization failed")
		}
		if grant != "" {
			break
		}
	}

	loginBody, _ := json.Marshal(map[string]string{"token": grant})
	session, err := glmBiz[struct {
		AccessToken string `json:"accessToken"`
		AccessTok2  string `json:"access_token"`
	}](ctx, client, http.MethodPost, glmBizHost+"/api/auth/z/login", loginBody, "", "z.ai login")
	if err != nil {
		return "", err
	}
	bearer := session.AccessToken
	if bearer == "" {
		bearer = session.AccessTok2
	}
	customer, err := glmBiz[struct {
		Organizations []struct {
			OrganizationID   string `json:"organizationId"`
			OrganizationName string `json:"organizationName"`
			Projects         []struct {
				ProjectID   string `json:"projectId"`
				ProjectName string `json:"projectName"`
			} `json:"projects"`
		} `json:"organizations"`
	}](ctx, client, http.MethodGet, glmBizHost+"/api/biz/customer/getCustomerInfo", nil, bearer, "customer info")
	if err != nil {
		return "", err
	}
	if len(customer.Organizations) == 0 {
		return "", fmt.Errorf("no organizations on this account")
	}
	org := customer.Organizations[0]
	for _, candidate := range customer.Organizations {
		if strings.Contains(candidate.OrganizationName, glmDefaultOrg) {
			org = candidate
			break
		}
	}
	if len(org.Projects) == 0 {
		return "", fmt.Errorf("no projects in organization %s", org.OrganizationID)
	}
	project := org.Projects[0]
	for _, candidate := range org.Projects {
		if strings.Contains(candidate.ProjectName, glmDefaultProj) {
			project = candidate
			break
		}
	}
	keysURL := fmt.Sprintf("%s/api/biz/v1/organization/%s/projects/%s/api_keys",
		glmBizHost, org.OrganizationID, project.ProjectID)
	keys, err := glmBiz[[]struct {
		Name   string `json:"name"`
		APIKey string `json:"apiKey"`
	}](ctx, client, http.MethodGet, keysURL, nil, bearer, "api key list")
	if err != nil {
		return "", err
	}
	apiKey := ""
	for _, k := range keys {
		if k.Name == glmKeyName {
			apiKey = k.APIKey
			break
		}
	}
	if apiKey == "" {
		createBody, _ := json.Marshal(map[string]string{"name": glmKeyName})
		created, err := glmBiz[struct {
			APIKey string `json:"apiKey"`
		}](ctx, client, http.MethodPost, keysURL, createBody, bearer, "api key creation")
		if err != nil {
			return "", err
		}
		apiKey = created.APIKey
	}
	secret, err := glmBiz[struct {
		SecretKey string `json:"secretKey"`
	}](ctx, client, http.MethodGet, keysURL+"/copy/"+apiKey, nil, bearer, "api key secret")
	if err != nil {
		return "", err
	}
	return apiKey + "." + secret.SecretKey, nil
}
