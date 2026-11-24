package backend

import (
	"context"
	"time"

	"github.com/edy/slop-proxy-go/internal/pool"
	"github.com/edy/slop-proxy-go/internal/provider"
	"github.com/edy/slop-proxy-go/internal/translate"
	"github.com/edy/slop-proxy-go/internal/upstream"
)

// AnthropicBackend relays Messages requests verbatim over pooled Max accounts.
type AnthropicBackend struct {
	client *AnthropicClient
	limit  float64
}

func NewAnthropicBackend(c *AnthropicClient, softLimit float64) *AnthropicBackend {
	return &AnthropicBackend{client: c, limit: softLimit}
}

func (b *AnthropicBackend) Provider() provider.Provider      { return provider.Anthropic }
func (b *AnthropicBackend) SoftLimit() float64               { return b.limit }
func (b *AnthropicBackend) Attempts() int                    { return 3 }
func (b *AnthropicBackend) RateLimitMax() int64              { return 7 * 24 * 3600 }
func (b *AnthropicBackend) RateLimitBase() int64             { return 60 }
func (b *AnthropicBackend) RefreshOnce() bool                { return true }
func (b *AnthropicBackend) StickyWaitSecs() int64            { return 0 }
func (b *AnthropicBackend) SessionAffinity() bool            { return false }
func (b *AnthropicBackend) RetryBudget() time.Duration       { return 0 }
func (b *AnthropicBackend) RetryableBadRequest(string) bool  { return false }
func (b *AnthropicBackend) Reason(s string) string           { return translate.ChatReason(s) }
func (b *AnthropicBackend) UsageFrom(any) *pool.AccountUsage { return nil }

type AnthRequest struct {
	Path      string
	Body      []byte
	Version   string
	Beta      string
	UserAgent string
}

func (b *AnthropicBackend) Send(ctx context.Context, token string, slot *pool.Slot, route pool.Route, req any) (any, error) {
	r := req.(*AnthRequest)
	h := RelayHeaders{Version: r.Version, Beta: r.Beta, UserAgent: r.UserAgent}
	resp, se := b.client.Post(ctx, token, slot.AuthMode, slot.Egress, r.Path, r.Body, h)
	if se == nil {
		return resp, nil
	}
	if se.Kind == upstream.ErrRateLimited && SubLimitRejected(se.Headers) {
		se.Kind = upstream.ErrModelLimited
	}
	return nil, mapSendError(se)
}

// GeminiBackend sends Chat requests through the Gemini client.
type GeminiBackend struct {
	client *GeminiClient
	limit  float64
}

func NewGeminiBackend(c *GeminiClient, softLimit float64) *GeminiBackend {
	return &GeminiBackend{client: c, limit: softLimit}
}

func (b *GeminiBackend) Provider() provider.Provider     { return provider.Gemini }
func (b *GeminiBackend) SoftLimit() float64              { return b.limit }
func (b *GeminiBackend) Attempts() int                   { return 3 }
func (b *GeminiBackend) RateLimitMax() int64             { return 3600 }
func (b *GeminiBackend) RateLimitBase() int64            { return 40 }
func (b *GeminiBackend) RefreshOnce() bool               { return false }
func (b *GeminiBackend) StickyWaitSecs() int64           { return 40 }
func (b *GeminiBackend) SessionAffinity() bool           { return false }
func (b *GeminiBackend) RetryBudget() time.Duration      { return b.client.retryBudget }
func (b *GeminiBackend) RetryableBadRequest(string) bool { return false }
func (b *GeminiBackend) Reason(s string) string          { return translate.ChatReason(s) }
func (b *GeminiBackend) UsageFrom(resp any) *pool.AccountUsage {
	if r, ok := resp.(*GeminiResponse); ok && r != nil {
		return usageFromHeaders(r.Response.Header)
	}
	return nil
}

func (b *GeminiBackend) Send(ctx context.Context, token string, slot *pool.Slot, route pool.Route, req any) (any, error) {
	var referer *string
	if slot.HTTPReferer != "" {
		referer = &slot.HTTPReferer
	}
	switch r := req.(type) {
	case *GeminiNativeRequest:
		result, se := b.client.PostNative(ctx, token, referer, r)
		if se != nil {
			return nil, mapSendError(se)
		}
		return result, nil
	case *translate.ChatRequest:
		result, se := b.client.Post(ctx, token, referer, r)
		if se != nil {
			return nil, mapSendError(se)
		}
		return result, nil
	default:
		return nil, &pool.SendError{Kind: pool.SendNetwork, Body: "unexpected gemini request type"}
	}
}
