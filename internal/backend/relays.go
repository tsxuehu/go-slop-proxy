package backend

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/edy/slop-proxy-go/internal/config"
	"github.com/edy/slop-proxy-go/internal/pool"
	"github.com/edy/slop-proxy-go/internal/upstream"
)

var sharedClient = &http.Client{Transport: &http.Transport{
	MaxIdleConns: 100, MaxIdleConnsPerHost: 20, IdleConnTimeout: 90 * time.Second,
}}

func doPost(ctx context.Context, url string, headers map[string]string, body []byte) (*http.Response, *upstream.SendError) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, upstream.NetworkError(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := sharedClient.Do(req)
	if err != nil {
		return nil, upstream.NetworkError(err)
	}
	return resp, nil
}

func doGet(ctx context.Context, url string, headers map[string]string) (*http.Response, *upstream.SendError) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, upstream.NetworkError(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := sharedClient.Do(req)
	if err != nil {
		return nil, upstream.NetworkError(err)
	}
	return resp, nil
}

// ---------- OpenRouter ----------

type OpenRouterClient struct{ base string }

func NewOpenRouter(cfg config.RelayConfig) *OpenRouterClient {
	return &OpenRouterClient{base: strings.TrimRight(cfg.BaseURL, "/")}
}

func (c *OpenRouterClient) Post(ctx context.Context, key, path string, body []byte) (*http.Response, *upstream.SendError) {
	return doPost(ctx, c.base+path, map[string]string{"Authorization": "Bearer " + key}, body)
}

// ---------- Experiential ----------

type ExperientialClient struct{ base string }

func NewExperiential(cfg config.RelayConfig) *ExperientialClient {
	return &ExperientialClient{base: strings.TrimRight(cfg.BaseURL, "/")}
}

func (c *ExperientialClient) Post(ctx context.Context, key, path string, body []byte) (*http.Response, *upstream.SendError) {
	return doPost(ctx, c.base+path, map[string]string{"Authorization": "Bearer " + key}, body)
}

// ---------- Radiance ----------

type RadianceClient struct{ base string }

func NewRadiance(cfg config.RelayConfig) *RadianceClient {
	return &RadianceClient{base: strings.TrimRight(cfg.BaseURL, "/")}
}

func (c *RadianceClient) Post(ctx context.Context, key, path string, body []byte) (*http.Response, *upstream.SendError) {
	return doPost(ctx, c.base+path, map[string]string{"Authorization": "Bearer " + key}, body)
}

func (c *RadianceClient) Models(ctx context.Context, key string) ([]string, *upstream.SendError) {
	resp, se := doGet(ctx, c.base+"/v1/models", map[string]string{"Authorization": "Bearer " + key})
	if se != nil {
		return nil, se
	}
	return parseIDList(resp)
}

type idList struct {
	Data []struct {
		ID string `json:"id"`
	} `json:"data"`
}

func parseIDList(resp *http.Response) ([]string, *upstream.SendError) {
	list, se := upstream.JSON[idList](resp, upstream.Strict, resp.Request.URL.Path)
	if se != nil {
		return nil, se
	}
	ids := make([]string, 0, len(list.Data))
	for _, m := range list.Data {
		ids = append(ids, m.ID)
	}
	return ids, nil
}

// ---------- DeepSeek ----------

type DeepSeekClient struct{ root string }

func NewDeepSeek(cfg config.RelayConfig) *DeepSeekClient {
	return &DeepSeekClient{root: strings.TrimRight(cfg.BaseURL, "/")}
}

func (c *DeepSeekClient) Post(ctx context.Context, key, path string, body []byte) (*http.Response, *upstream.SendError) {
	return doPost(ctx, c.root+"/anthropic"+path, map[string]string{
		"x-api-key": key, "anthropic-version": "2023-06-01",
	}, body)
}

func (c *DeepSeekClient) Models(ctx context.Context, key string) ([]string, *upstream.SendError) {
	resp, se := doGet(ctx, c.root+"/models", map[string]string{"Authorization": "Bearer " + key})
	if se != nil {
		return nil, se
	}
	return parseIDList(resp)
}

// ---------- GLM / Z.ai ----------

var glmRules = upstream.Classify{
	Auth:    []int{401, 403},
	DeadKey: []string{"Insufficient balance"},
}

type GLMClient struct {
	base     string
	quotaURL string
}

func NewGLM(cfg config.RelayConfig) *GLMClient {
	return &GLMClient{base: strings.TrimRight(cfg.BaseURL, "/"), quotaURL: "https://api.z.ai/api/monitor/usage/quota/limit"}
}

func glmHeaders(key string) map[string]string {
	return map[string]string{
		"x-api-key":           key,
		"anthropic-version":   "2023-06-01",
		"user-agent":          "ZCode/3.14.4",
		"x-zcode-app-version": "3.14.4",
		"x-title":             "Z Code@cli",
		"x-zcode-agent":       "glm",
		"http-referer":        "https://zcode.z.ai",
	}
}

func (c *GLMClient) Post(ctx context.Context, key, path string, body []byte, session string) (*http.Response, *upstream.SendError) {
	h := glmHeaders(key)
	h["x-request-id"] = newUUID()
	h["x-zcode-trace-id"] = newUUID()
	h["x-session-id"] = session
	resp, se := doPost(ctx, c.base+path, h, body)
	if se != nil {
		return resp, se
	}
	checked, se := upstream.ClassifyResponse(resp, glmRules)
	return checked, se
}

func (c *GLMClient) Models(ctx context.Context, key string) ([]string, *upstream.SendError) {
	resp, se := doGet(ctx, c.base+"/v1/models", glmHeaders(key))
	if se != nil {
		return nil, se
	}
	return parseIDList(resp)
}

type quotaEnvelope struct {
	Data struct {
		Limits []struct {
			Unit        int     `json:"unit"`
			Number      uint32  `json:"number"`
			Allowance   float64 `json:"usage"`
			Used        float64 `json:"currentValue"`
			Remaining   float64 `json:"remaining"`
			NextResetMS *int64  `json:"nextResetTime"`
		} `json:"limits"`
	} `json:"data"`
}

// WeeklyAllowance is the largest weekly quota the account reports.
func (q *quotaEnvelope) WeeklyAllowance() float64 {
	var largest float64
	for _, l := range q.Data.Limits {
		if l.Unit == 6 && l.Allowance > largest {
			largest = l.Allowance
		}
	}
	return largest
}

// Usage converts a GLM quota report into the pool's account usage shape.
// largestWeekly is the biggest weekly allowance seen across the pool, used to
// rank accounts of different subscription sizes against each other.
func (q *quotaEnvelope) Usage(largestWeekly float64) *pool.AccountUsage {
	var windows []pool.UsageWindow
	var weekly float64
	locked := false
	for _, l := range q.Data.Limits {
		var name string
		switch l.Unit {
		case 3:
			name = fmt.Sprintf("%dh", l.Number)
		case 6:
			name = fmt.Sprintf("%dd", l.Number*7)
			if weekly == 0 {
				weekly = l.Allowance
			}
		default:
			continue
		}
		utilization := 0.0
		if l.Allowance > 0 {
			utilization = l.Used / l.Allowance
		}
		var resetsAt int64
		if l.NextResetMS != nil {
			resetsAt = *l.NextResetMS / 1000
		}
		windows = append(windows, pool.UsageWindow{Name: name, Utilization: utilization, ResetsAt: resetsAt})
		if l.Remaining <= 0 {
			locked = true
		}
	}
	capacity := 0.0
	if largestWeekly > 0 && weekly > 0 {
		capacity = weekly / largestWeekly
	}
	return &pool.AccountUsage{Windows: windows, Locked: locked, Capacity: capacity, ObservedAt: time.Now().Unix()}
}

func (c *GLMClient) Quota(ctx context.Context, key string) (*quotaEnvelope, *upstream.SendError) {
	resp, se := doGet(ctx, c.quotaURL, map[string]string{"authorization": key})
	if se != nil {
		return nil, se
	}
	q, se := upstream.JSON[quotaEnvelope](resp, upstream.Strict, "quota")
	if se != nil {
		return nil, se
	}
	return &q, nil
}

var _ = io.Discard
