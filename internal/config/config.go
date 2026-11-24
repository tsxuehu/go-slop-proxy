package config

import (
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/edy/slop-proxy-go/internal/provider"
	"github.com/pelletier/go-toml/v2"
)

type Config struct {
	DB           string          `toml:"db"`
	AdminToken   string          `toml:"-"`
	Bind         string          `toml:"bind"`
	MetricsBind  string          `toml:"metrics_bind"`
	Codex        CodexConfig     `toml:"codex"`
	Anthropic    AnthropicConfig `toml:"anthropic"`
	Gemini       GeminiConfig    `toml:"gemini"`
	Zen          ZenConfig       `toml:"zen"`
	GLM          RelayConfig     `toml:"glm"`
	DeepSeek     RelayConfig     `toml:"deepseek"`
	Experiential RelayConfig     `toml:"experiential"`
	OpenRouter   RelayConfig     `toml:"openrouter"`
	Radiance     RelayConfig     `toml:"radiance"`
	Copilot      CopilotConfig   `toml:"copilot"`
	Pricing      PricingConfig   `toml:"pricing"`
	Models       ModelsConfig    `toml:"models"`
}

type CodexConfig struct {
	BaseURL              string          `toml:"base_url"`
	Originator           string          `toml:"originator"`
	Version              string          `toml:"version"`
	Instructions         string          `toml:"instructions"`
	InstructionsFile     string          `toml:"instructions_file"`
	ForwardMaxTokens     bool            `toml:"forward_max_tokens"`
	PinTurnState         bool            `toml:"pin_turn_state"`
	SoftUtilizationLimit float64         `toml:"soft_utilization_limit"`
	DesktopGates         map[string]bool `toml:"desktop_gates"`
}

type AnthropicConfig struct {
	BaseURL              string  `toml:"base_url"`
	RequireClaudeCode    bool    `toml:"require_claude_code"`
	SoftUtilizationLimit float64 `toml:"soft_utilization_limit"`
	EgressConfig
}

type GeminiConfig struct {
	BaseURL              string            `toml:"base_url"`
	Headers              map[string]string `toml:"headers"`
	SoftUtilizationLimit float64           `toml:"soft_utilization_limit"`
	RetryBudgetSecs      int64             `toml:"retry_budget_secs"`
	EgressConfig
}

type ZenConfig struct {
	BaseURLs     []string `toml:"base_urls"`
	GoBaseURL    string   `toml:"go_base_url"`
	ModelsDevURL string   `toml:"models_dev_url"`
	UserAgent    string   `toml:"user_agent"`
	EgressConfig
}

type RelayConfig struct {
	BaseURL string `toml:"base_url"`
	EgressConfig
}

type CopilotConfig struct {
	BaseURL              string  `toml:"base_url"`
	SoftUtilizationLimit float64 `toml:"soft_utilization_limit"`
}

type PricingConfig struct {
	URL string `toml:"url"`
}

type EgressConfig struct {
	ProxyURLs      []string     `toml:"proxy_urls"`
	ProxyURLsFile  string       `toml:"proxy_urls_file"`
	SourcePrefixes []string     `toml:"source_prefixes"`
	Exits          []ExitConfig `toml:"exits"`
}

type ExitConfig struct {
	ProxyURL      string `toml:"proxy_url"`
	RotateCommand string `toml:"rotate_command"`
}

type ModelAlias struct {
	Model  string `toml:"model"`
	Effort string `toml:"effort"`
}

type ModelsConfig struct {
	Default              string                `toml:"default"`
	DefaultEffort        string                `toml:"default_effort"`
	Known                []string              `toml:"known"`
	Aliases              map[string]ModelAlias `toml:"aliases"`
	AnthropicPatterns    []string              `toml:"anthropic_patterns"`
	GeminiPatterns       []string              `toml:"gemini_patterns"`
	ZenPatterns          []string              `toml:"zen_patterns"`
	ZenMessagesPatterns  []string              `toml:"zen_messages_patterns"`
	ZenChatPatterns      []string              `toml:"zen_chat_patterns"`
	GLMPatterns          []string              `toml:"glm_patterns"`
	DeepSeekPatterns     []string              `toml:"deepseek_patterns"`
	ExperientialPatterns []string              `toml:"experiential_patterns"`
	CopilotPatterns      []string              `toml:"copilot_patterns"`
	OpenRouterPatterns   []string              `toml:"openrouter_patterns"`
	RadiancePatterns     []string              `toml:"radiance_patterns"`
	BlockedPatterns      []string              `toml:"blocked_patterns"`
}

type ZenDialect int

const (
	ZenResponses ZenDialect = iota
	ZenMessages
	ZenChat
)

func Default() Config {
	return Config{
		Bind: "[::1]:8484",
		Codex: CodexConfig{
			BaseURL:              "https://chatgpt.com/backend-api/codex",
			Originator:           "codex_cli_rs",
			Version:              "0.168.0",
			ForwardMaxTokens:     true,
			PinTurnState:         true,
			SoftUtilizationLimit: 0.9,
			DesktopGates:         make(map[string]bool),
		},
		Anthropic: AnthropicConfig{
			BaseURL:              "https://api.anthropic.com",
			RequireClaudeCode:    true,
			SoftUtilizationLimit: 0.9,
		},
		Gemini: GeminiConfig{
			BaseURL:              "https://generativelanguage.googleapis.com/v1beta/openai",
			SoftUtilizationLimit: 0.9,
			RetryBudgetSecs:      90,
			Headers:              make(map[string]string),
		},
		Zen: ZenConfig{
			BaseURLs: []string{
				"https://opencode.ai/inference/v1",
				"https://dev.opencode.ai/inference/v1",
			},
			GoBaseURL:    "https://opencode.ai/inference/go/openai/v1",
			ModelsDevURL: "https://models.dev/api.json",
			UserAgent:    "opencode/latest/2.1.4/cli",
		},
		GLM:          RelayConfig{BaseURL: "https://api.z.ai/api/anthropic"},
		DeepSeek:     RelayConfig{BaseURL: "https://api.deepseek.com"},
		Experiential: RelayConfig{BaseURL: "https://api.experientiallabs.ai"},
		OpenRouter:   RelayConfig{BaseURL: "https://openrouter.ai/api"},
		Radiance:     RelayConfig{BaseURL: "http://[::1]:8080"},
		Copilot: CopilotConfig{
			BaseURL:              "https://api.githubcopilot.com",
			SoftUtilizationLimit: 0.9,
		},
		Pricing: PricingConfig{
			URL: "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json",
		},
		Models: ModelsConfig{
			Aliases:            make(map[string]ModelAlias),
			AnthropicPatterns:  []string{"claude-*"},
			GeminiPatterns:     []string{"gemini-*"},
			GLMPatterns:        []string{"glm-*"},
			DeepSeekPatterns:   []string{"deepseek-*"},
			OpenRouterPatterns: []string{"minimax/*"},
		},
	}
}

func homeDir() string {
	if h, err := os.UserHomeDir(); err == nil {
		return h
	}
	return "."
}

func ConfigPath() string {
	if p := os.Getenv("SLOP_CONFIG"); p != "" {
		return p
	}
	if x := os.Getenv("XDG_CONFIG_HOME"); x != "" {
		return filepath.Join(x, "slop-proxy-go", "config.toml")
	}
	return filepath.Join(homeDir(), ".config", "slop-proxy-go", "config.toml")
}

func DBPath() string {
	if p := os.Getenv("SLOP_DB"); p != "" {
		return p
	}
	if x := os.Getenv("XDG_DATA_HOME"); x != "" {
		return filepath.Join(x, "slop-proxy-go", "slop.db")
	}
	return filepath.Join(homeDir(), ".local", "share", "slop-proxy-go", "slop.db")
}

func Load(path string) (Config, error) {
	cfg := Default()
	cfg.AdminToken = os.Getenv("SLOP_ADMIN_TOKEN")
	if path == "" {
		path = ConfigPath()
	}
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		if cfg.DB == "" {
			cfg.DB = DBPath()
		}
		if b := os.Getenv("SLOP_BIND"); b != "" {
			cfg.Bind = b
		}
		return cfg, nil
	}
	if err != nil {
		return cfg, err
	}
	if err := toml.Unmarshal(b, &cfg); err != nil {
		return cfg, err
	}
	if db := os.Getenv("SLOP_DB"); db != "" {
		cfg.DB = db
	} else if cfg.DB == "" {
		cfg.DB = DBPath()
	}
	if cfg.Bind == "" {
		cfg.Bind = Default().Bind
	}
	if b := os.Getenv("SLOP_BIND"); b != "" {
		cfg.Bind = b
	}
	return cfg, nil
}

// patternSpecificity: an exact match wins outright; a trailing * matches by prefix length.
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

func bestMatch(patterns []string, model string) (int, bool) {
	best, found := 0, false
	for _, p := range patterns {
		if score, ok := patternSpecificity(p, model); ok && (!found || score > best) {
			best, found = score, true
		}
	}
	return best, found
}

func (m *ModelsConfig) Blocked(model string) bool {
	_, ok := bestMatch(m.BlockedPatterns, model)
	return ok
}

func (m *ModelsConfig) sets() []struct {
	provider provider.Provider
	patterns []string
} {
	return []struct {
		provider provider.Provider
		patterns []string
	}{
		{provider.Anthropic, m.AnthropicPatterns},
		{provider.Gemini, m.GeminiPatterns},
		{provider.Zen, m.ZenPatterns},
		{provider.Zen, m.ZenMessagesPatterns},
		{provider.Zen, m.ZenChatPatterns},
		{provider.GLM, m.GLMPatterns},
		{provider.DeepSeek, m.DeepSeekPatterns},
		{provider.Experiential, m.ExperientialPatterns},
		{provider.Copilot, m.CopilotPatterns},
		{provider.OpenRouter, m.OpenRouterPatterns},
		{provider.Radiance, m.RadiancePatterns},
	}
}

func (m *ModelsConfig) Matched(model string) *provider.Provider {
	var bestScore int
	var bestProvider *provider.Provider
	for _, set := range m.sets() {
		score, ok := bestMatch(set.patterns, model)
		if ok && (bestProvider == nil || score > bestScore) {
			s, p := score, set.provider
			bestScore, bestProvider = s, &p
		}
	}
	return bestProvider
}

func (m *ModelsConfig) Route(model string) provider.Provider {
	if p := m.Matched(model); p != nil {
		return *p
	}
	return provider.OpenAI
}

func (m *ModelsConfig) ZenDialect(model string) ZenDialect {
	type candidate struct {
		dialect ZenDialect
		score   int
	}
	var candidates []candidate
	if s, ok := bestMatch(m.ZenChatPatterns, model); ok {
		candidates = append(candidates, candidate{ZenChat, s})
	}
	if s, ok := bestMatch(m.ZenMessagesPatterns, model); ok {
		candidates = append(candidates, candidate{ZenMessages, s})
	}
	if s, ok := bestMatch(m.ZenPatterns, model); ok {
		candidates = append(candidates, candidate{ZenResponses, s})
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].score <= candidates[j].score
	})
	if len(candidates) > 0 {
		return candidates[len(candidates)-1].dialect
	}
	return ZenResponses
}

func (m *ModelsConfig) Resolve(model string) (string, string) {
	upstream, effort := model, m.DefaultEffort
	if a, ok := m.Aliases[model]; ok {
		upstream = a.Model
		if a.Effort != "" {
			effort = a.Effort
		}
	}
	if idx := strings.LastIndex(upstream, ":"); idx > 0 {
		effort = upstream[idx+1:]
		upstream = upstream[:idx]
	}
	return upstream, effort
}
