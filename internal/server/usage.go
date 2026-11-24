package server

import (
	"encoding/json"
	"strings"

	"github.com/edy/slop-proxy-go/internal/translate"
)

// UsageMeta carries the per-request dimensions that back the tool, insight and
// session Prometheus families. It is optional at every call site, so the
// websocket relay (which builds its own UsageLog) is unaffected.
type UsageMeta struct {
	Body          []byte
	Captured      *translate.CapturedUsage
	Effort        string
	ServiceTier   string
	ResponseBytes int64
	Attempts      int64
	Status        int
	ErrorKind     string
	// Force writes a usage_log row even when the upstream reported no tokens.
	// Rust's pipeline::forward logs rejected and failed requests too; those
	// rows are what the error/tool insight families aggregate.
	Force bool
}

type usageInsights struct {
	ToolsDeclared  int64
	ToolsCalled    string
	ThinkingBudget int64
	ImageCount     int64
	Effort         string
	ServiceTier    string
}

// inspectUsage pulls the request-side dimensions out of a caller body. The
// shape is deliberately tolerant: Chat, Responses and Messages all spell the
// same concepts slightly differently.
func inspectUsage(body []byte) usageInsights {
	var out usageInsights
	if len(body) == 0 {
		return out
	}
	var root map[string]json.RawMessage
	if json.Unmarshal(body, &root) != nil {
		return out
	}
	for _, key := range []string{"tools", "functions"} {
		var list []json.RawMessage
		if json.Unmarshal(root[key], &list) == nil {
			out.ToolsDeclared += int64(len(list))
		}
	}
	out.Effort = firstString(root, "reasoning_effort")
	if out.Effort == "" {
		var reasoning map[string]json.RawMessage
		if json.Unmarshal(root["reasoning"], &reasoning) == nil {
			out.Effort = firstString(reasoning, "effort")
		}
	}
	out.ServiceTier = firstString(root, "service_tier")
	out.ThinkingBudget = firstInt(root, "thinking_budget", "max_thinking_tokens")
	if out.ThinkingBudget == 0 {
		var thinking map[string]json.RawMessage
		if json.Unmarshal(root["thinking"], &thinking) == nil {
			out.ThinkingBudget = firstInt(thinking, "budget_tokens")
		}
	}

	var calls []string
	var images int64
	var walk func(v any)
	walk = func(v any) {
		switch node := v.(type) {
		case map[string]any:
			typ, _ := node["type"].(string)
			isImage := typ == "image" || typ == "image_url"
			if _, ok := node["image_url"]; ok {
				isImage = true
			}
			if _, ok := node["inline_data"]; ok {
				isImage = true
			}
			if isImage {
				images++
			}
			if typ == "tool_use" || typ == "function_call" {
				if name, ok := node["name"].(string); ok && name != "" {
					calls = append(calls, name)
				}
			}
			if tcs, ok := node["tool_calls"].([]any); ok {
				for _, tc := range tcs {
					if m, ok := tc.(map[string]any); ok {
						if fn, ok := m["function"].(map[string]any); ok {
							if name, ok := fn["name"].(string); ok && name != "" {
								calls = append(calls, name)
							}
						}
					}
				}
			}
			for _, child := range node {
				walk(child)
			}
		case []any:
			for _, child := range node {
				walk(child)
			}
		}
	}
	var doc any
	if json.Unmarshal(body, &doc) == nil {
		walk(doc)
	}
	out.ImageCount = images
	out.ToolsCalled = strings.Join(calls, ",")
	return out
}

func firstString(m map[string]json.RawMessage, keys ...string) string {
	for _, k := range keys {
		var s string
		if json.Unmarshal(m[k], &s) == nil && s != "" {
			return s
		}
	}
	return ""
}

func firstInt(m map[string]json.RawMessage, keys ...string) int64 {
	for _, k := range keys {
		var n int64
		if json.Unmarshal(m[k], &n) == nil && n > 0 {
			return n
		}
	}
	return 0
}
