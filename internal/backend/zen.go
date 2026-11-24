package backend

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/edy/slop-proxy-go/internal/config"
	"github.com/edy/slop-proxy-go/internal/pool"
	"github.com/edy/slop-proxy-go/internal/upstream"
)

const (
	contextRefreshSecs = 12 * 3600
	drainedSecs        = 60
	replayRejection    = "thinking mode must be passed back"
	replayTries        = 3
)

type zenHost struct {
	BaseURL      string
	Pool         *upstream.EgressPool
	DrainedUntil atomic.Int64
}

type ZenClient struct {
	hosts          []*zenHost
	goHost         *zenHost
	modelsDevURL   string
	contextFetched atomic.Int64
	contextWindows map[string]int64
}

func NewZen(cfg config.ZenConfig) (*ZenClient, error) {
	z := &ZenClient{modelsDevURL: cfg.ModelsDevURL, contextWindows: map[string]int64{}}
	for _, raw := range cfg.BaseURLs {
		pool, err := upstream.NewEgressPool(cfg.EgressConfig, "zen", cfg.UserAgent)
		if err != nil {
			return nil, err
		}
		z.hosts = append(z.hosts, &zenHost{BaseURL: strings.TrimRight(raw, "/"), Pool: pool})
	}
	if cfg.GoBaseURL != "" {
		pool, err := upstream.NewEgressPool(cfg.EgressConfig, "zen", cfg.UserAgent)
		if err != nil {
			return nil, err
		}
		z.goHost = &zenHost{BaseURL: strings.TrimRight(cfg.GoBaseURL, "/"), Pool: pool}
	}
	return z, nil
}

// ZenSession derives the stable free-tier session id from the caller's
// session key, falling back to user and model when no key is available.
func ZenSession(route pool.Route) string {
	msg := route.SessionKey
	if msg == "" {
		msg = route.User + "\x00" + route.Model
	}
	sum := sha256.Sum256([]byte(msg))
	t := ^(time.Now().UnixMilli()*0x1000 + 1) & 0xffffffffffff
	prefix := fmt.Sprintf("%012x", t)
	const alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	var sb strings.Builder
	for i := 0; i < 14 && i < len(sum); i++ {
		sb.WriteByte(alphabet[int(sum[i])%len(alphabet)])
	}
	return "ses_" + prefix + sb.String()
}

func zenSession(sessionKey string) string {
	return ZenSession(pool.Route{SessionKey: sessionKey})
}

func traceContext() (string, string) {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	h := hex.EncodeToString(b)
	trace, span, parent := h[0:32], h[32:48], h[48:64]
	return "00-" + trace + "-" + span + "-01", trace + "-" + span + "-1-" + parent
}

// Post sends to a zen host; key==nil uses the anonymous free tier.
func (c *ZenClient) Post(ctx context.Context, key *string, session, path string, body []byte) (*http.Response, *upstream.SendError) {
	hosts := c.hosts
	anonymous := key == nil || *key == ""
	if !anonymous {
		if c.goHost == nil {
			return nil, &upstream.SendError{Kind: upstream.ErrNetwork, Message: "no zen subscription host"}
		}
		hosts = []*zenHost{c.goHost}
	}
	now := time.Now().Unix()
	var outcome *upstream.SendError
	for i, host := range hosts {
		if anonymous && i < len(hosts)-1 && host.DrainedUntil.Load() > now {
			continue
		}
		traceparent, b3 := traceContext()
		auth := "Bearer public"
		if !anonymous {
			auth = "Bearer " + *key
		}
		build := func(client *http.Client) (*http.Request, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, host.BaseURL+path, bytes.NewReader(body))
			if err != nil {
				return nil, err
			}
			req.Header.Set("Accept", "*/*")
			req.Header.Set("x-opencode-session", session)
			req.Header.Set("x-opencode-session-id", session)
			req.Header.Set("x-session-affinity", session)
			req.Header.Set("x-session-id", session)
			req.Header.Set("x-opencode-client", "cli")
			req.Header.Set("x-opencode-project", "global")
			req.Header.Set("traceparent", traceparent)
			req.Header.Set("b3", b3)
			req.Header.Set("Authorization", auth)
			if strings.HasSuffix(path, "/messages") {
				req.Header.Set("anthropic-version", "2023-06-01")
			}
			req.Header.Set("Content-Type", "application/json")
			return req, nil
		}

		var resp *http.Response
		if anonymous {
			resp, outcome = host.Pool.SendAnonymous(ctx, build)
		} else {
			resp, outcome = host.Pool.Send(ctx, build)
		}
		if outcome == nil {
			return resp, nil
		}
		if outcome.Kind == upstream.ErrBadRequest {
			if strings.Contains(outcome.Message, replayRejection) {
				// Replay with the same egress a few times before giving up.
				for tries := 1; tries < replayTries; tries++ {
					time.Sleep(250 * time.Millisecond)
					req, err := build(sharedClient)
					if err != nil {
						break
					}
					r, err := sharedClient.Do(req)
					if err != nil {
						break
					}
					checked, se := upstream.ClassifyResponse(r, upstream.Strict)
					if se == nil {
						return checked, nil
					}
					outcome = se
					if !strings.Contains(se.Message, replayRejection) {
						break
					}
				}
			} else if strings.Contains(outcome.Message, "Model is unavailable") {
				secs := retryAfterMs(outcome.Message)
				if secs == nil {
					def := int64(60)
					secs = &def
				}
				return nil, &upstream.SendError{Kind: upstream.ErrModelLimited, RetryAfter: secs, Body: outcome.Message}
			}
		}
		if outcome.Kind != upstream.ErrRateLimited {
			break
		}
		if anonymous {
			secs := int64(drainedSecs)
			if outcome.RetryAfter != nil && *outcome.RetryAfter > secs {
				secs = *outcome.RetryAfter
			}
			host.DrainedUntil.Store(now + secs)
		} else {
			break
		}
	}
	if outcome == nil {
		outcome = &upstream.SendError{Kind: upstream.ErrNetwork, Message: "no zen hosts"}
	}
	return nil, outcome
}

func retryAfterMs(body string) *int64 {
	idx := strings.Index(body, "retry-after-ms=")
	if idx < 0 {
		return nil
	}
	rest := body[idx+len("retry-after-ms="):]
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	if end == 0 {
		return nil
	}
	millis, err := strconv.ParseInt(rest[:end], 10, 64)
	if err != nil || millis <= 0 {
		return nil
	}
	secs := (millis + 999) / 1000
	if secs > 60 {
		secs = 60
	}
	return &secs
}

func (c *ZenClient) Models(ctx context.Context) ([]string, *upstream.SendError) {
	if len(c.hosts) == 0 {
		return nil, &upstream.SendError{Kind: upstream.ErrNetwork, Message: "no zen hosts"}
	}
	host := c.hosts[0]
	resp, se := host.Pool.SendAnonymous(ctx, func(client *http.Client) (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodGet, host.BaseURL+"/models", nil)
	})
	if se != nil {
		return nil, se
	}
	return parseIDList(resp)
}

// SatisfyToolGate injects the three placeholder tools the free tier requires.
func SatisfyToolGate(body []byte) []byte {
	var req map[string]any
	if json.Unmarshal(body, &req) != nil {
		return nil
	}
	declared := map[string]bool{}
	tools, _ := req["tools"].([]any)
	for _, t := range tools {
		if m, ok := t.(map[string]any); ok {
			if name, ok := m["name"].(string); ok {
				declared[name] = true
			}
		}
	}
	missing := gateToolsMissing(declared)
	untagged := 0
	if input, ok := req["input"].([]any); ok {
		for _, item := range input {
			if m, ok := item.(map[string]any); ok {
				if m["type"] == "message" {
					delete(m, "type")
					untagged++
				}
			}
		}
	}
	if len(missing) == 0 && untagged == 0 {
		return nil
	}
	for _, tool := range missing {
		tools = append(tools, tool)
	}
	req["tools"] = tools
	out, _ := json.Marshal(req)
	return out
}

// SatisfyChatToolGate applies the same free-tier tool gate to a Chat wire
// body, where the tool name lives under "function" rather than at the top.
func SatisfyChatToolGate(body []byte) []byte {
	var req map[string]any
	if json.Unmarshal(body, &req) != nil {
		return nil
	}
	tools, _ := req["tools"].([]any)
	declared := map[string]bool{}
	for _, t := range tools {
		if m, ok := t.(map[string]any); ok {
			if f, ok := m["function"].(map[string]any); ok {
				if name, ok := f["name"].(string); ok {
					declared[name] = true
				}
			}
		}
	}
	missing := gateToolsMissing(declared)
	if len(missing) == 0 {
		return nil
	}
	for _, tool := range missing {
		tools = append(tools, map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        tool["name"],
				"description": tool["description"],
				"parameters":  tool["parameters"],
			},
		})
	}
	req["tools"] = tools
	out, _ := json.Marshal(req)
	return out
}

var gateToolDefs = []map[string]any{
	{"type": "function", "name": "bash", "description": "Deprecated placeholder. Never call this tool.", "strict": false,
		"parameters": map[string]any{"type": "object", "properties": map[string]any{
			"command": map[string]any{"description": "The command to execute", "type": "string"},
			"timeout": map[string]any{"description": "Optional timeout in milliseconds", "type": "integer"},
			"workdir": map[string]any{"description": "The working directory to run the command in. Defaults to the current directory. Use this instead of 'cd' commands.", "type": "string"},
		}, "required": []string{"command"}}},
	{"type": "function", "name": "edit", "description": "Deprecated placeholder. Never call this tool.", "strict": false,
		"parameters": map[string]any{"type": "object", "properties": map[string]any{
			"filePath":   map[string]any{"description": "The absolute path to the file to modify", "type": "string"},
			"oldString":  map[string]any{"description": "The text to replace", "type": "string"},
			"newString":  map[string]any{"description": "The text to replace it with (must be different from oldString)", "type": "string"},
			"replaceAll": map[string]any{"description": "Replace all occurrences of oldString (default false)", "type": "boolean"},
		}, "required": []string{"filePath", "oldString", "newString"}}},
	{"type": "function", "name": "read", "description": "Deprecated placeholder. Never call this tool.", "strict": false,
		"parameters": map[string]any{"type": "object", "properties": map[string]any{
			"filePath": map[string]any{"description": "The absolute path to the file or directory to read", "type": "string"},
			"offset":   map[string]any{"description": "The line number to start reading from (1-indexed)", "type": "integer"},
			"limit":    map[string]any{"description": "The maximum number of lines to read (defaults to 2000)", "type": "integer"},
		}, "required": []string{"filePath"}}},
}

func gateToolsMissing(declared map[string]bool) []map[string]any {
	var out []map[string]any
	for _, t := range gateToolDefs {
		if !declared[t["name"].(string)] {
			out = append(out, t)
		}
	}
	return out
}
