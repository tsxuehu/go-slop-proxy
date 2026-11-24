package backend

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/edy/slop-proxy-go/internal/pool"
	"github.com/edy/slop-proxy-go/internal/provider"
	"github.com/edy/slop-proxy-go/internal/translate"
	"github.com/edy/slop-proxy-go/internal/upstream"
)

// RelayBackend adapts a verbatim-pass-through provider client to pool.Backend.
type RelayBackend struct {
	prov        provider.Provider
	post        func(ctx context.Context, key string, r *RelayRequest) (*http.Response, *upstream.SendError)
	softLimit   float64
	authRefresh bool
	session     func(pool.Route) string
}

func (b *RelayBackend) Provider() provider.Provider     { return b.prov }
func (b *RelayBackend) SoftLimit() float64              { return b.softLimit }
func (b *RelayBackend) Attempts() int                   { return 3 }
func (b *RelayBackend) RateLimitMax() int64             { return 3600 }
func (b *RelayBackend) RateLimitBase() int64            { return 60 }
func (b *RelayBackend) RefreshOnce() bool               { return b.authRefresh }
func (b *RelayBackend) StickyWaitSecs() int64           { return 0 }
func (b *RelayBackend) SessionAffinity() bool           { return false }
func (b *RelayBackend) RetryBudget() time.Duration      { return 0 }
func (b *RelayBackend) RetryableBadRequest(string) bool { return false }
func (b *RelayBackend) Reason(s string) string          { return translate.ChatReason(s) }

func (b *RelayBackend) UsageFrom(any) *pool.AccountUsage { return nil }

type RelayRequest struct {
	Body       []byte
	Path       string
	Headers    map[string]string
	SessionKey string
	Agent      bool
	Vision     bool
}

// WithSession installs the provider-specific session id derivation (Zen and
// GLM both key their free-tier state on the routed session).
func (b *RelayBackend) WithSession(f func(pool.Route) string) *RelayBackend {
	b.session = f
	return b
}

func (b *RelayBackend) sessionKey(route pool.Route, r *RelayRequest) {
	if b.session != nil && r.SessionKey == "" {
		r.SessionKey = b.session(route)
	}
}

func (b *RelayBackend) Send(ctx context.Context, token string, slot *pool.Slot, route pool.Route, req any) (any, error) {
	r, ok := req.(*RelayRequest)
	if !ok {
		return nil, &pool.SendError{Kind: pool.SendNetwork, Body: "bad relay request"}
	}
	b.sessionKey(route, r)
	resp, se := b.post(ctx, token, r)
	if se == nil {
		return resp, nil
	}
	return nil, mapSendError(se)
}

func mapSendError(se *upstream.SendError) *pool.SendError {
	out := &pool.SendError{Body: se.Body, Headers: se.Headers}
	if se.Body == "" {
		out.Body = se.Message
	}
	switch se.Kind {
	case upstream.ErrAuth:
		out.Kind = pool.SendAuth
	case upstream.ErrRateLimited:
		out.Kind = pool.SendRateLimited
		out.RetryAfter = se.RetryAfter
	case upstream.ErrModelLimited:
		out.Kind = pool.SendModelLimited
		out.RetryAfter = se.RetryAfter
	case upstream.ErrBadRequest:
		out.Kind = pool.SendBadRequest
	case upstream.ErrNetwork:
		out.Kind = pool.SendNetwork
	default:
		out.Kind = pool.SendOther
	}
	return out
}

func NewRelayBackend(p provider.Provider, post func(context.Context, string, *RelayRequest) (*http.Response, *upstream.SendError)) *RelayBackend {
	return &RelayBackend{prov: p, post: post, softLimit: 1.0}
}

// NewAuthRelayBackend marks a relay whose OAuth credential can be refreshed
// once in place after a 401 (Copilot).
func NewAuthRelayBackend(p provider.Provider, post func(context.Context, string, *RelayRequest) (*http.Response, *upstream.SendError)) *RelayBackend {
	b := NewRelayBackend(p, post)
	b.authRefresh = true
	return b
}

// AnonymousRelayBackend adds the pool.AnonymousBackend contract to a relay
// that can also send without any stored credential.
type AnonymousRelayBackend struct {
	*RelayBackend
	postAnonymous func(context.Context, *RelayRequest) (*http.Response, *upstream.SendError)
}

func NewAnonymousRelayBackend(b *RelayBackend, post func(context.Context, *RelayRequest) (*http.Response, *upstream.SendError)) *AnonymousRelayBackend {
	return &AnonymousRelayBackend{RelayBackend: b, postAnonymous: post}
}

func (b *AnonymousRelayBackend) Anonymous() bool { return true }

func (b *AnonymousRelayBackend) SendAnonymous(ctx context.Context, route pool.Route, req any) (any, error) {
	r, ok := req.(*RelayRequest)
	if !ok {
		return nil, &pool.SendError{Kind: pool.SendNetwork, Body: "bad relay request"}
	}
	b.sessionKey(route, r)
	resp, se := b.postAnonymous(ctx, r)
	if se != nil {
		return nil, mapSendError(se)
	}
	return resp, nil
}

// usageFromHeaders parses x-codex-style usage headers (shared by relay clients
// that report per-response quota).
func usageFromHeaders(h map[string][]string) *pool.AccountUsage {
	var windows []pool.UsageWindow
	get := func(name string) *int64 {
		vals := h[name]
		if len(vals) == 0 {
			return nil
		}
		v, err := strconv.ParseInt(vals[0], 10, 64)
		if err != nil {
			return nil
		}
		return &v
	}
	for _, tier := range []string{"primary", "secondary"} {
		minutes := get("x-codex-" + tier + "-window-minutes")
		percent := get("x-codex-" + tier + "-used-percent")
		if minutes == nil || percent == nil || *minutes <= 0 {
			continue
		}
		resets := get("x-codex-" + tier + "-reset-at")
		var resetAt int64
		if resets != nil {
			resetAt = *resets
		}
		windows = append(windows, pool.UsageWindow{Name: windowName(*minutes), Utilization: float64(*percent) / 100, ResetsAt: resetAt})
	}
	if len(windows) == 0 {
		return nil
	}
	return &pool.AccountUsage{Windows: windows}
}

var _ = io.Discard
var _ = strings.TrimSpace
