package provider

import "strings"

type Provider string

const (
	OpenAI       Provider = "openai"
	Anthropic    Provider = "anthropic"
	Gemini       Provider = "gemini"
	GLM          Provider = "glm"
	DeepSeek     Provider = "deepseek"
	Experiential Provider = "experiential"
	OpenRouter   Provider = "openrouter"
	Radiance     Provider = "radiance"
	Zen          Provider = "zen"
	Copilot      Provider = "copilot"
)

func Parse(s string) (Provider, bool) {
	p := Provider(strings.ToLower(strings.TrimSpace(s)))
	switch p {
	case OpenAI, Anthropic, Gemini, GLM, DeepSeek, Experiential, OpenRouter, Radiance, Zen, Copilot:
		return p, true
	default:
		return "", false
	}
}

func (p Provider) String() string { return string(p) }

func (p Provider) Refreshable() bool {
	switch p {
	case OpenAI, Anthropic, Copilot:
		return true
	default:
		return false
	}
}
