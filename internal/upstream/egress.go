package upstream

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/edy/slop-proxy-go/internal/config"
)

const (
	egressAttempts      = 8
	unreachableCooldown = 30
)

type Egress struct {
	Client           *http.Client
	ProxyURL         string
	UnavailableUntil int64
	AnonymousUntil   int64
	FreeTierFlagged  bool
}

type EgressPool struct {
	egresses       []*Egress
	next           atomic.Uint64
	applyUserAgent string
}

func NewEgressPool(cfg config.EgressConfig, label, userAgent string) (*EgressPool, error) {
	urls := append([]string(nil), cfg.ProxyURLs...)

	pool := &EgressPool{}
	direct := &http.Client{Timeout: 0, Transport: &http.Transport{
		MaxIdleConns: 100, MaxIdleConnsPerHost: 20, IdleConnTimeout: 90 * time.Second,
	}}
	pool.egresses = append(pool.egresses, &Egress{Client: direct})
	if userAgent != "" {
		// A default UA is applied per request in applyUA; keep it on the pool.
		pool.applyUserAgent = userAgent
	}
	for _, raw := range urls {
		proxyURL, err := url.Parse(raw)
		if err != nil {
			return nil, err
		}
		tr := &http.Transport{Proxy: http.ProxyURL(proxyURL), MaxIdleConns: 50, IdleConnTimeout: 90 * time.Second}
		pool.egresses = append(pool.egresses, &Egress{Client: &http.Client{Transport: tr}, ProxyURL: raw})
	}
	return pool, nil
}

func (p *EgressPool) applyUA(req *http.Request) {
	if p.applyUserAgent != "" && req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", p.applyUserAgent)
	}
}

func (p *EgressPool) order() []*Egress {
	n := len(p.egresses)
	start := int(p.next.Add(1) - 1)
	out := make([]*Egress, n)
	for i := 0; i < n; i++ {
		out[i] = p.egresses[(start+i)%n]
	}
	return out
}

// Send tries egresses; only network errors cause a switch. Any upstream
// response terminates the loop.
func (p *EgressPool) Send(ctx context.Context, build func(*http.Client) (*http.Request, error)) (*http.Response, *SendError) {
	var lastNetwork *SendError
	for _, eg := range p.order() {
		req, err := build(eg.Client)
		if err != nil {
			return nil, NetworkError(err)
		}
		p.applyUA(req)
		resp, err := eg.Client.Do(req)
		if err != nil {
			lastNetwork = NetworkError(err)
			continue
		}
		return resp, nil
	}
	if lastNetwork != nil {
		return nil, lastNetwork
	}
	return nil, &SendError{Kind: ErrNetwork, Message: "no egress available"}
}

// SendAnonymous additionally benches an egress on rate limits / free-tier
// errors, and cools unreachable egresses.
func (p *EgressPool) SendAnonymous(ctx context.Context, build func(*http.Client) (*http.Request, error)) (*http.Response, *SendError) {
	now := time.Now().Unix()
	var rateLimitedUntil int64
	var lastNetwork *SendError
	flagged := false

	for _, eg := range p.order() {
		if eg.UnavailableUntil > now {
			continue
		}
		req, err := build(eg.Client)
		if err != nil {
			return nil, NetworkError(err)
		}
		p.applyUA(req)
		resp, err := eg.Client.Do(req)
		if err != nil {
			eg.UnavailableUntil = now + unreachableCooldown
			lastNetwork = NetworkError(err)
			continue
		}
		checked, se := ClassifyResponse(resp, Strict)
		if se == nil {
			return checked, nil
		}
		switch se.Kind {
		case ErrRateLimited:
			secs := int64(60)
			if se.RetryAfter != nil {
				secs = *se.RetryAfter
			}
			eg.UnavailableUntil = now + secs
			if rateLimitedUntil == 0 || eg.UnavailableUntil < rateLimitedUntil {
				rateLimitedUntil = eg.UnavailableUntil
			}
		case ErrAuth:
			if strings.Contains(se.Message, "FreeTierError") {
				eg.FreeTierFlagged = true
				flagged = true
			} else {
				return nil, se
			}
		case ErrNetwork:
			eg.UnavailableUntil = now + unreachableCooldown
			lastNetwork = se
		default:
			return nil, se
		}
	}

	if flagged {
		tomorrow := time.Now().UTC().Truncate(24 * time.Hour).Add(24 * time.Hour).Unix()
		for _, eg := range p.egresses {
			if eg.FreeTierFlagged {
				eg.UnavailableUntil = tomorrow
				eg.FreeTierFlagged = false
			}
		}
	}
	if rateLimitedUntil > 0 {
		left := rateLimitedUntil - time.Now().Unix()
		if left < 1 {
			left = 1
		}
		return nil, &SendError{Kind: ErrRateLimited, RetryAfter: &left, Body: "all egresses rate limited"}
	}
	if lastNetwork != nil {
		return nil, lastNetwork
	}
	return nil, &SendError{Kind: ErrNetwork, Message: "no egress available"}
}
