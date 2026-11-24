package translate

import (
	"os"
	"sort"
	"strings"

	"github.com/edy/slop-proxy-go/internal/config"
)

var efforts = map[string]bool{
	"minimal": true, "low": true, "medium": true, "high": true,
	"xhigh": true, "max": true, "ultra": true,
}

type ResolvedModel struct {
	Model  string
	Effort string
}

func splitSuffix(requested string) (string, string) {
	idx := strings.LastIndex(requested, ":")
	if idx < 0 {
		return requested, ""
	}
	suffix := requested[idx+1:]
	if efforts[suffix] {
		return requested[:idx], suffix
	}
	return requested, ""
}

func Resolve(cfg *config.Config, requested string) ResolvedModel {
	name, suffixEffort := splitSuffix(requested)

	var bestScore int
	var bestModel, bestEffort string
	matched := false

	patterns := make([]string, 0, len(cfg.Models.Aliases))
	for p := range cfg.Models.Aliases {
		patterns = append(patterns, p)
	}
	sort.Strings(patterns)

	for _, pattern := range patterns {
		score, ok := patternSpecificity(pattern, name)
		if ok && (!matched || score > bestScore) {
			alias := cfg.Models.Aliases[pattern]
			bestScore, bestModel, bestEffort = score, alias.Model, alias.Effort
			matched = true
		}
	}

	effort := suffixEffort
	if effort == "" {
		effort = cfg.Models.DefaultEffort
	}
	if matched {
		if suffixEffort != "" {
			effort = suffixEffort
		} else if bestEffort != "" {
			effort = bestEffort
		}
		return ResolvedModel{Model: bestModel, Effort: effort}
	}
	return ResolvedModel{Model: name, Effort: effort}
}

func patternSpecificity(pattern, model string) (int, bool) {
	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		if strings.HasPrefix(model, prefix) {
			return len(prefix), true
		}
		return 0, false
	}
	if model == pattern {
		return int(^uint(0) >> 1), true
	}
	return 0, false
}

func ClampEffort(model, effort string) string {
	if strings.Contains(model, "codex") && effort == "minimal" {
		return "low"
	}
	return effort
}

const minMaxOutputTokens uint64 = 16

func usableCap(cap *uint64) *uint64 {
	if cap != nil && *cap >= minMaxOutputTokens {
		return cap
	}
	return nil
}

func instructionsOf(cfg *config.Config) string {
	if cfg.Codex.InstructionsFile != "" {
		if b, err := os.ReadFile(cfg.Codex.InstructionsFile); err == nil && strings.TrimSpace(string(b)) != "" {
			return string(b)
		}
	}
	if cfg.Codex.Instructions != "" {
		return cfg.Codex.Instructions
	}
	return "You are Codex, based on GPT-5. You are running as a coding agent on a user's computer. Answer the user's requests directly and concisely."
}

// responses_request builds an IR request with alias resolution, effort and instructions.
func responsesRequest(cfg *config.Config, model, passedEffort string, cap *uint64) ResponsesRequest {
	resolved := Resolve(cfg, model)
	out := NewResponses(resolved.Model, instructionsOf(cfg))
	effort := passedEffort
	if effort == "" {
		effort = resolved.Effort
	}
	if effort == "" {
		effort = "medium"
	}
	out.Reasoning = &ReasoningConfig{Effort: ClampEffort(out.Model, effort), Summary: "auto"}
	if cfg.Codex.ForwardMaxTokens {
		out.MaxOutputTokens = usableCap(cap)
	}
	return out
}
