package backend

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/edy/slop-proxy-go/internal/config"
	"github.com/edy/slop-proxy-go/internal/upstream"
)

const oauthBeta = "oauth-2025-04-20"

var anthropicRules = upstream.Classify{
	Pass: func(s int) bool { return s != 401 && s != 429 && !(s >= 500 && s <= 599) },
	Auth: []int{401},
	ResetHeaders: []string{
		"anthropic-ratelimit-unified-reset", "anthropic-ratelimit-requests-reset",
	},
	AccountFaults: []string{"Your credit balance is too low"},
}

type AnthropicClient struct {
	base   string
	pool   *upstream.EgressPool
	client *http.Client
}

func NewAnthropic(cfg config.AnthropicConfig) (*AnthropicClient, error) {
	pool, err := upstream.NewEgressPool(cfg.EgressConfig, "anthropic", "")
	if err != nil {
		return nil, err
	}
	return &AnthropicClient{base: strings.TrimRight(cfg.BaseURL, "/"), pool: pool, client: sharedClient}, nil
}

type RelayHeaders struct {
	Version   string
	Beta      string
	UserAgent string
}

func (c *AnthropicClient) beta(h RelayHeaders, oauthMode bool) string {
	if !oauthMode {
		return h.Beta
	}
	for _, part := range strings.Split(h.Beta, ",") {
		if strings.TrimSpace(part) == oauthBeta {
			return h.Beta
		}
	}
	if h.Beta != "" {
		return oauthBeta + "," + h.Beta
	}
	return oauthBeta
}

// Post relays a Messages body verbatim to the Anthropic API.
func (c *AnthropicClient) Post(ctx context.Context, credential, authMode string, viaEgress bool, path string, body []byte, h RelayHeaders) (*http.Response, *upstream.SendError) {
	oauthMode := authMode == "oauth"
	headers := map[string]string{
		"anthropic-version": h.Version,
		"Content-Type":      "application/json",
	}
	if headers["anthropic-version"] == "" {
		headers["anthropic-version"] = "2023-06-01"
	}
	if oauthMode {
		headers["Authorization"] = "Bearer " + credential
	} else {
		headers["x-api-key"] = credential
	}
	if beta := c.beta(h, oauthMode); beta != "" {
		headers["anthropic-beta"] = beta
	}
	if h.UserAgent != "" {
		headers["user-agent"] = h.UserAgent
	}

	build := func(client *http.Client) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, strings.NewReader(string(body)))
		if err != nil {
			return nil, err
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		return req, nil
	}

	if viaEgress {
		return c.pool.Send(ctx, build)
	}
	req, err := build(sharedClient)
	if err != nil {
		return nil, upstream.NetworkError(err)
	}
	resp, err := sharedClient.Do(req)
	if err != nil {
		return nil, upstream.NetworkError(err)
	}
	checked, se := upstream.ClassifyResponse(resp, anthropicRules)
	return checked, se
}

// SubLimitRejected reports whether a 429 is due to a per-model sub-limit
// rather than the account-level 5h/7d windows.
func SubLimitRejected(h http.Header) bool {
	claimRejected := func(claim string) bool {
		return h.Get("anthropic-ratelimit-unified-"+claim+"-status") == "rejected"
	}
	found := false
	const prefix = "anthropic-ratelimit-unified-"
	const suffix = "-status"
	for name, values := range h {
		if len(values) == 0 || values[0] != "rejected" {
			continue
		}
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
			continue
		}
		claim := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
		if claim != "5h" && claim != "7d" && claim != "overage" {
			found = true
		}
	}
	return found && !claimRejected("5h") && !claimRejected("7d")
}

// UsageReport polls the account's OAuth usage endpoint.
func (c *AnthropicClient) UsageReport(ctx context.Context, accessToken string) (*AnthUsageReport, *upstream.SendError) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/api/oauth/usage", nil)
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("anthropic-beta", oauthBeta)
	resp, err := sharedClient.Do(req)
	if err != nil {
		return nil, upstream.NetworkError(err)
	}
	report, se := upstream.JSON[AnthUsageReport](resp, upstream.Strict, "/api/oauth/usage")
	if se != nil {
		return nil, se
	}
	return &report, nil
}

func (r *AnthUsageReport) WindowName(l AnthLimit) string {
	if l.Group == "session" {
		return "5h"
	}
	return "7d"
}

type AnthUsageWindow struct {
	Name        string
	Utilization float64
	ResetsAt    *string
}

func (r *AnthUsageReport) Windows() []AnthUsageWindow {
	dormant := map[string]bool{}
	for _, l := range r.Limits {
		if l.Scope == nil && l.IsActive != nil && !*l.IsActive && l.Percent == 0 {
			dormant[r.WindowName(l)] = true
		}
	}
	var out []AnthUsageWindow
	if r.FiveHour != nil && !dormant["5h"] {
		out = append(out, AnthUsageWindow{"5h", r.FiveHour.Utilization, r.FiveHour.ResetsAt})
	}
	if r.SevenDay != nil && !dormant["7d"] {
		out = append(out, AnthUsageWindow{"7d", r.SevenDay.Utilization, r.SevenDay.ResetsAt})
	}
	return out
}

func (r *AnthUsageReport) Locked() bool {
	return (r.FiveHour != nil && r.FiveHour.LockedReason != nil) ||
		(r.SevenDay != nil && r.SevenDay.LockedReason != nil)
}

func (c *AnthropicClient) Models(ctx context.Context, accessToken string) ([]string, *upstream.SendError) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/v1/models?limit=100", nil)
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("anthropic-beta", oauthBeta)
	req.Header.Set("anthropic-version", "2023-06-01")
	resp, err := sharedClient.Do(req)
	if err != nil {
		return nil, upstream.NetworkError(err)
	}
	catalog, se := upstream.JSON[struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}](resp, upstream.Strict, "/v1/models")
	if se != nil {
		return nil, se
	}
	ids := make([]string, 0, len(catalog.Data))
	for _, m := range catalog.Data {
		ids = append(ids, m.ID)
	}
	return ids, nil
}

var _ = time.Now

// AnthWindow models one reported quota window.
type AnthWindow struct {
	Utilization  float64 `json:"utilization"`
	LockedReason *string `json:"locked_reason"`
	ResetsAt     *string `json:"resets_at"`
}

type AnthUsageReport struct {
	FiveHour *AnthWindow `json:"five_hour"`
	SevenDay *AnthWindow `json:"seven_day"`
	Limits   []AnthLimit `json:"limits"`
}

type AnthLimit struct {
	Group   string  `json:"group"`
	Percent float64 `json:"percent"`
	Scope   *struct {
		Model *struct {
			DisplayName *string `json:"display_name"`
		} `json:"model"`
	} `json:"scope"`
	IsActive *bool   `json:"is_active"`
	ResetsAt *string `json:"resets_at"`
}
