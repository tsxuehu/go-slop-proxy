package upstream

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edy/slop-proxy-go/internal/config"
)

const (
	egressAttempts      = 8
	unreachableCooldown = 30
	rotationInterval    = 60 * time.Second
	rotationTimeout     = 2 * time.Minute
)

type egress struct {
	client         *http.Client
	proxy          string
	unavailable    atomic.Int64
	anonymousUntil atomic.Int64
	exit           *Exit
	generation     uint64
}

// Exit shares rotation state across a provider's hosts.
type Exit struct {
	cfg        config.ExitConfig
	mu         sync.Mutex
	next       time.Time
	running    bool
	generation atomic.Uint64
}

func NewExits(cfg config.EgressConfig) []*Exit {
	exits := make([]*Exit, 0, len(cfg.Exits))
	for _, item := range cfg.Exits {
		exits = append(exits, &Exit{cfg: item})
	}
	return exits
}

func (e *Exit) rotate() {
	e.mu.Lock()
	now := time.Now()
	if e.running || now.Before(e.next) {
		e.mu.Unlock()
		return
	}
	e.running, e.next = true, now.Add(rotationInterval)
	e.mu.Unlock()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), rotationTimeout)
		defer cancel()
		err := exec.CommandContext(ctx, "/bin/sh", "-c", e.cfg.RotateCommand).Run()
		if err == nil {
			e.generation.Add(1)
		} else {
			slog.Warn("egress rotation hook failed", "error", err)
		}
		e.mu.Lock()
		e.running = false
		e.mu.Unlock()
	}()
}

type EgressPool struct {
	cfg                     config.EgressConfig
	label, userAgent        string
	mu                      sync.Mutex
	sources, exits, proxies []*egress
	next                    atomic.Uint64
}

func NewEgressPool(cfg config.EgressConfig, label, userAgent string) (*EgressPool, error) {
	return NewSharedEgressPool(cfg, label, userAgent, NewExits(cfg))
}

func NewSharedEgressPool(cfg config.EgressConfig, label, userAgent string, exits []*Exit) (*EgressPool, error) {
	p := &EgressPool{cfg: cfg, label: label, userAgent: userAgent}
	for _, raw := range cfg.SourcePrefixes {
		prefix, err := netip.ParsePrefix(raw)
		if err != nil || !prefix.Addr().Is6() || prefix.Bits() < 32 || prefix.Bits() > 120 {
			return nil, fmt.Errorf("%s source prefix must be IPv6 and between /32 and /120: %q", label, raw)
		}
		source := prefix.Masked().Addr().Next()
		p.sources = append(p.sources, &egress{client: newEgressClient(nil, source)})
	}
	for _, exit := range exits {
		if strings.TrimSpace(exit.cfg.RotateCommand) == "" {
			return nil, fmt.Errorf("%s exit requires a rotation command", label)
		}
		entry, err := newProxyEgress(exit.cfg.ProxyURL)
		if err != nil {
			return nil, fmt.Errorf("%s exit: %w", label, err)
		}
		entry.exit = exit
		p.exits = append(p.exits, entry)
	}
	if len(cfg.ProxyURLs) == 0 && cfg.ProxyURLsFile == "" && len(p.sources) == 0 && len(p.exits) == 0 {
		p.proxies = []*egress{{client: newEgressClient(nil, netip.Addr{})}}
		return p, nil
	}
	if err := p.reload(); err != nil {
		return nil, err
	}
	return p, nil
}

func newEgressClient(proxy *url.URL, source netip.Addr) *http.Client {
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	dial := dialer.DialContext
	if source.IsValid() {
		dialer.LocalAddr = &net.TCPAddr{IP: source.AsSlice()}
		dial = func(ctx context.Context, _ string, address string) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp6", address)
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	if proxy != nil {
		transport.Proxy = http.ProxyURL(proxy)
	}
	transport.DialContext = dial
	transport.ResponseHeaderTimeout = 30 * time.Second
	return &http.Client{Transport: transport}
}

func newProxyEgress(raw string) (*egress, error) {
	proxy, err := url.Parse(raw)
	if err != nil || proxy.Host == "" {
		return nil, fmt.Errorf("invalid egress proxy URL")
	}
	switch proxy.Scheme {
	case "http", "https", "socks5", "socks5h":
	default:
		return nil, fmt.Errorf("unsupported egress proxy scheme %q", proxy.Scheme)
	}
	return &egress{client: newEgressClient(proxy, netip.Addr{}), proxy: raw}, nil
}

// Reload picks up changes to a proxy list file. Validation is completed before
// replacing the live list so a bad update does not interrupt active traffic.
func (p *EgressPool) Reload() error {
	if p.cfg.ProxyURLsFile == "" {
		return nil
	}
	return p.reload()
}

func (p *EgressPool) reload() error {
	urls := append([]string{}, p.cfg.ProxyURLs...)
	if filename := p.cfg.ProxyURLsFile; filename != "" {
		content, err := os.ReadFile(filename)
		if err != nil {
			return fmt.Errorf("read %s proxy list: %w", p.label, err)
		}
		for _, line := range strings.Split(string(content), "\n") {
			if value := strings.TrimSpace(line); value != "" && !strings.HasPrefix(value, "#") {
				urls = append(urls, value)
			}
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	previous := make(map[string]*egress, len(p.proxies))
	for _, item := range p.proxies {
		previous[item.proxy] = item
	}
	next := make([]*egress, 0, len(urls))
	for _, raw := range urls {
		if existing := previous[raw]; existing != nil {
			next = append(next, existing)
			delete(previous, raw)
			continue
		}
		entry, err := newProxyEgress(raw)
		if err != nil {
			return fmt.Errorf("%s proxy %d: %w", p.label, len(next)+1, err)
		}
		next = append(next, entry)
	}
	p.proxies = next
	for _, dropped := range previous {
		dropped.client.CloseIdleConnections()
	}
	return nil
}

func (p *EgressPool) order(anonymous bool) []*egress {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now().Unix()
	for i, entry := range p.exits {
		if generation := entry.exit.generation.Load(); generation != entry.generation {
			fresh, err := newProxyEgress(entry.proxy)
			if err == nil {
				entry.client.CloseIdleConnections()
				fresh.exit, fresh.generation = entry.exit, generation
				p.exits[i] = fresh
				entry = fresh
			}
		}
		if anonymous && entry.anonymousUntil.Load() > now {
			entry.exit.rotate()
		}
	}
	start := p.next.Add(1) - 1
	var ordered []*egress
	for _, group := range [][]*egress{p.sources, p.exits, p.proxies} {
		if n := len(group); n > 0 {
			offset := int(start % uint64(n))
			ordered = append(ordered, group[offset:]...)
			ordered = append(ordered, group[:offset]...)
		}
	}
	return ordered
}

func (p *EgressPool) Send(ctx context.Context, build func(*http.Client) (*http.Request, error)) (*http.Response, *SendError) {
	return p.send(ctx, build, false)
}

func (p *EgressPool) SendAnonymous(ctx context.Context, build func(*http.Client) (*http.Request, error)) (*http.Response, *SendError) {
	return p.send(ctx, build, true)
}

func (p *EgressPool) send(ctx context.Context, build func(*http.Client) (*http.Request, error), anonymous bool) (*http.Response, *SendError) {
	var lastNetwork, refusal *SendError
	var retryAt int64
	attempts := 0
	for _, item := range p.order(anonymous) {
		if ctx.Err() != nil {
			return nil, NetworkError(ctx.Err())
		}
		now := time.Now().Unix()
		if item.unavailable.Load() > now {
			continue
		}
		if anonymous && item.anonymousUntil.Load() > now {
			until := item.anonymousUntil.Load()
			if retryAt == 0 || until < retryAt {
				retryAt = until
			}
			continue
		}
		if attempts >= egressAttempts {
			break
		}
		attempts++
		req, err := build(item.client)
		if err != nil {
			return nil, NetworkError(err)
		}
		if p.userAgent != "" && req.Header.Get("User-Agent") == "" {
			req.Header.Set("User-Agent", p.userAgent)
		}
		resp, err := item.client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, NetworkError(ctx.Err())
			}
			item.unavailable.Store(now + unreachableCooldown)
			lastNetwork = NetworkError(err)
			continue
		}
		if !anonymous {
			return resp, nil
		}
		checked, failure := ClassifyResponse(resp, Strict)
		if failure == nil {
			return checked, nil
		}
		switch {
		case failure.Kind == ErrRateLimited:
			delay := int64(60)
			if failure.RetryAfter != nil {
				delay = max(1, *failure.RetryAfter)
			}
			until := now + delay
			item.anonymousUntil.Store(until)
			if retryAt == 0 || until < retryAt {
				retryAt = until
			}
		case (failure.Kind == ErrAuth || failure.Kind == ErrBadRequest) &&
			strings.Contains(failure.Body+" "+failure.Message, "FreeTierError"):
			item.anonymousUntil.Store(time.Now().UTC().Truncate(24 * time.Hour).Add(24 * time.Hour).Unix())
			refusal = failure
		default:
			return nil, failure
		}
		if item.exit != nil {
			item.exit.rotate()
		}
	}
	if refusal != nil {
		return nil, refusal
	}
	if retryAt > 0 {
		delay := max(1, retryAt-time.Now().Unix())
		return nil, &SendError{Kind: ErrRateLimited, RetryAfter: &delay, Body: "all egresses rate limited"}
	}
	if lastNetwork != nil {
		return nil, lastNetwork
	}
	return nil, &SendError{Kind: ErrNetwork, Message: "no egress available"}
}
