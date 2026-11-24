package backend

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/edy/slop-proxy-go/internal/config"
	"github.com/edy/slop-proxy-go/internal/upstream"
)

var copilotRules = upstream.Classify{
	Auth:         []int{401, 403, 404},
	ResetHeaders: []string{"retry-after", "x-ratelimit-reset", "x-quota-reset"},
}

type CopilotClient struct {
	base string
}

func NewCopilot(cfg config.CopilotConfig) *CopilotClient {
	return &CopilotClient{base: strings.TrimRight(cfg.BaseURL, "/")}
}

func copilotEditorHeaders() map[string]string {
	return map[string]string{
		"editor-version":        "vscode/1.105.0",
		"editor-plugin-version": "copilot-chat/0.26.7",
		"user-agent":            "GitHubCopilotChat/0.26.7",
		"x-github-api-version":  "2025-04-01",
	}
}

func (c *CopilotClient) Post(ctx context.Context, token string, body []byte, agent, vision bool) (*http.Response, *upstream.SendError) {
	h := copilotEditorHeaders()
	h["Authorization"] = "Bearer " + token
	h["copilot-integration-id"] = "vscode-chat"
	h["openai-intent"] = "conversation-panel"
	if agent {
		h["x-initiator"] = "agent"
	} else {
		h["x-initiator"] = "user"
	}
	h["x-request-id"] = newUUID()
	if vision {
		h["copilot-vision-request"] = "true"
	}
	resp, se := doPost(ctx, c.base+"/chat/completions", h, body)
	if se != nil {
		return nil, se
	}
	checked, se := upstream.ClassifyResponse(resp, copilotRules)
	return checked, se
}

func (c *CopilotClient) Models(ctx context.Context, token string) ([]string, *upstream.SendError) {
	h := copilotEditorHeaders()
	h["Authorization"] = "Bearer " + token
	resp, se := doGet(ctx, c.base+"/models", h)
	if se != nil {
		return nil, se
	}
	return parseIDList(resp)
}

type CopilotQuota struct {
	QuotaResetDateUTC *string `json:"quota_reset_date_utc"`
	QuotaSnapshots    struct {
		PremiumInteractions *struct {
			Entitlement int64 `json:"entitlement"`
			Remaining   int64 `json:"remaining"`
			Unlimited   bool  `json:"unlimited"`
		} `json:"premium_interactions"`
	} `json:"quota_snapshots"`
}

func (c *CopilotClient) Quota(ctx context.Context, token string) (*CopilotQuota, *upstream.SendError) {
	h := copilotEditorHeaders()
	h["Authorization"] = "Bearer " + token
	resp, se := doGet(ctx, "https://api.github.com/copilot_internal/user", h)
	if se != nil {
		return nil, se
	}
	q, se := upstream.JSON[CopilotQuota](resp, copilotRules, "quota")
	if se != nil {
		return nil, se
	}
	return &q, nil
}

func (q *CopilotQuota) Utilization() float64 {
	d := q.QuotaSnapshots.PremiumInteractions
	if d == nil || d.Unlimited || d.Entitlement <= 0 {
		return 0
	}
	used := d.Entitlement - d.Remaining
	if used < 0 {
		used = 0
	}
	v := float64(used) / float64(d.Entitlement)
	if v > 1 {
		v = 1
	}
	return v
}

var _ = time.Now
