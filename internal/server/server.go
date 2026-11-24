package server

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/edy/slop-proxy-go/internal/backend"
	"github.com/edy/slop-proxy-go/internal/config"
	"github.com/edy/slop-proxy-go/internal/oauth"
	"github.com/edy/slop-proxy-go/internal/pool"
	"github.com/edy/slop-proxy-go/internal/pricing"
	"github.com/edy/slop-proxy-go/internal/provider"
	"github.com/edy/slop-proxy-go/internal/store"
	"github.com/edy/slop-proxy-go/internal/translate"
	"github.com/edy/slop-proxy-go/internal/upstream"
)

type egressReloader interface{ ReloadEgress() error }

type Server struct {
	egressClients []egressReloader
	cfg           config.Config
	store         *store.Store
	pools         map[provider.Provider]*pool.Pool
	backend       *backend.CodexBackend
	server        *http.Server

	metricsMu     sync.Mutex
	metricsServer *http.Server

	anthropicClient *backend.AnthropicClient
	geminiClient    *backend.GeminiClient
	zenClient       *backend.ZenClient
	glmClient       *backend.GLMClient
	copilotClient   *backend.CopilotClient
	deepseekClient  *backend.DeepSeekClient
	radianceClient  *backend.MessagesRelay

	catalogsMu       sync.RWMutex
	catalogs         map[provider.Provider][]string
	codexCatalogs    map[int64]cachedCodexCatalog
	anthropicCatalog []backend.AnthropicModel
	geminiCatalog    []backend.GeminiModel
	prices           *pricing.Prices
	metrics          *metricsState
}

func makePool(p provider.Provider, st *store.Store, refresher oauth.Refresher, b pool.Backend) *pool.Pool {
	slots := pool.NewSlots(p, st, refresher)
	if err := slots.Reload(context.Background()); err != nil {
		slog.Warn("initial pool reload failed", "error", err, "provider", p)
	}
	return pool.NewPool(slots, b)
}

func NewServer(cfg config.Config, st *store.Store, refresher oauth.Refresher) *Server {
	srv := &Server{
		cfg: cfg, store: st,
		pools:    make(map[provider.Provider]*pool.Pool),
		catalogs: make(map[provider.Provider][]string),
	}

	codexBackend := backend.NewCodex(cfg.Codex)
	srv.backend = codexBackend
	srv.pools[provider.OpenAI] = makePool(provider.OpenAI, st, refresher, codexBackend)

	// Verbatim relay providers, adapted to the pool.Backend contract.
	openRouter := backend.NewMessagesRelay(cfg.OpenRouter, "openrouter")
	experiential := backend.NewMessagesRelay(cfg.Experiential, "experiential")
	radiance := backend.NewMessagesRelay(cfg.Radiance, "radiance")
	srv.egressClients = append(srv.egressClients, openRouter, experiential, radiance)
	srv.radianceClient = radiance
	relays := []struct {
		p    provider.Provider
		post func(context.Context, string, *backend.RelayRequest) (*http.Response, *upstream.SendError)
	}{
		{provider.OpenRouter, func(ctx context.Context, key string, r *backend.RelayRequest) (*http.Response, *upstream.SendError) {
			return openRouter.Post(ctx, key, r.Path, r.Body)
		}},
		{provider.Experiential, func(ctx context.Context, key string, r *backend.RelayRequest) (*http.Response, *upstream.SendError) {
			return experiential.Post(ctx, key, r.Path, r.Body)
		}},
		{provider.Radiance, func(ctx context.Context, key string, r *backend.RelayRequest) (*http.Response, *upstream.SendError) {
			return radiance.Post(ctx, key, r.Path, r.Body)
		}},
	}
	for _, r := range relays {
		srv.pools[r.p] = makePool(r.p, st, refresher, backend.NewRelayBackend(r.p, r.post))
	}
	if ds := backend.NewDeepSeek(cfg.DeepSeek); ds != nil {
		srv.deepseekClient = ds
		srv.egressClients = append(srv.egressClients, ds)
		srv.pools[provider.DeepSeek] = makePool(provider.DeepSeek, st, refresher, backend.NewRelayBackend(provider.DeepSeek,
			func(ctx context.Context, key string, r *backend.RelayRequest) (*http.Response, *upstream.SendError) {
				return ds.Post(ctx, key, r.Path, r.Body)
			}))
	}
	if glm := backend.NewGLM(cfg.GLM); glm != nil {
		srv.glmClient = glm
		srv.egressClients = append(srv.egressClients, glm)
		b := backend.NewRelayBackend(provider.GLM, func(ctx context.Context, key string, r *backend.RelayRequest) (*http.Response, *upstream.SendError) {
			return glm.Post(ctx, key, r.Path, r.Body, r.SessionKey)
		})
		b.WithSession(func(route pool.Route) string { return route.SessionKey })
		srv.pools[provider.GLM] = makePool(provider.GLM, st, refresher, b)
	}
	if co := backend.NewCopilot(cfg.Copilot); co != nil {
		srv.copilotClient = co
		b := backend.NewAuthRelayBackend(provider.Copilot, func(ctx context.Context, key string, r *backend.RelayRequest) (*http.Response, *upstream.SendError) {
			return co.Post(ctx, key, r.Body, r.Agent, r.Vision)
		})
		srv.pools[provider.Copilot] = makePool(provider.Copilot, st, refresher, b)
	}
	if zen, err := backend.NewZen(cfg.Zen); err == nil {
		srv.zenClient = zen
		srv.egressClients = append(srv.egressClients, zen)
		b := backend.NewRelayBackend(provider.Zen, func(ctx context.Context, key string, r *backend.RelayRequest) (*http.Response, *upstream.SendError) {
			var k *string
			if key != "" {
				k = &key
			}
			return zen.Post(ctx, k, r.SessionKey, r.Path, r.Body)
		}).WithSession(backend.ZenSession)
		srv.pools[provider.Zen] = makePool(provider.Zen, st, refresher, backend.NewAnonymousRelayBackend(b, func(ctx context.Context, r *backend.RelayRequest) (*http.Response, *upstream.SendError) {
			return zen.Post(ctx, nil, backend.ZenSession(pool.Route{SessionKey: r.SessionKey}), r.Path, r.Body)
		}))
	}
	if gem, err := backend.NewGemini(cfg.Gemini); err == nil {
		srv.geminiClient = gem
		srv.egressClients = append(srv.egressClients, gem)
		srv.pools[provider.Gemini] = makePool(provider.Gemini, st, refresher, backend.NewGeminiBackend(gem, cfg.Gemini.SoftUtilizationLimit))
	}
	if anth, err := backend.NewAnthropic(cfg.Anthropic); err == nil {
		srv.anthropicClient = anth
		srv.egressClients = append(srv.egressClients, anth)
		srv.pools[provider.Anthropic] = makePool(provider.Anthropic, st, refresher, backend.NewAnthropicBackend(anth, cfg.Anthropic.SoftUtilizationLimit))
	}

	srv.prices = pricing.New(cfg.DB, cfg.Pricing.URL)
	srv.prices.Refresh()
	srv.metrics = &metricsState{serverRef: srv}

	go srv.backgroundReload(context.Background())
	go srv.backgroundCatalogs(context.Background())
	go srv.backgroundPrices(context.Background())
	return srv
}

func (s *Server) providerPool(w http.ResponseWriter, dialect dialect, target provider.Provider) *pool.Pool {
	pl := s.pools[target]
	if pl == nil {
		writeAPIError(w, dialect, http.StatusServiceUnavailable, "api_error", "no usable "+target.String()+" backend")
	}
	return pl
}

func isVerbatimProvider(p provider.Provider) bool {
	switch p {
	case provider.Anthropic, provider.GLM, provider.DeepSeek,
		provider.OpenRouter, provider.Radiance, provider.Experiential:
		return true
	default:
		return false
	}
}

// relayMessages forwards a Messages body verbatim through a relay provider
// pool. It mirrors server/relay.rs::messages + pipeline::forward: the response
// bytes are never rewritten, but every exit (pool failure, upstream rejection,
// clean success, truncated stream) folds a full UsageRecord-style row into
// usage_log so the Prometheus error/tool/insight/session families see it.
func (s *Server) relayMessages(w http.ResponseWriter, r *http.Request, body []byte, rc *requestContext, p provider.Provider, model string) {
	resolved := translate.Resolve(&s.cfg, model)
	upstream, effort := resolved.Model, resolved.Effort
	scan := newRelayUsage()

	// log is the single bookkeeping exit. Status, error_kind, attempts and
	// account_id are the Rust record fields that vary per branch.
	log := func(status int, errorKind string, accountID *int64, attempts int64) {
		snap := scan.snapshot()
		s.recordUsage(rc, p, model, upstream, "messages", accountID,
			snap.InputTokens, snap.OutputTokens,
			UsageMeta{
				Body: body, Captured: snap, Effort: effort,
				Status: status, ErrorKind: errorKind, Attempts: attempts, Force: true,
			})
	}

	// An Anthropic subscription only covers Claude Code. Rust refuses before
	// dispatch and still logs the rejected row with error_kind not_claude_code.
	if p == provider.Anthropic && s.cfg.Anthropic.RequireClaudeCode &&
		!rc.token.ReservedOnly && !isClaudeCode(r.Header) {
		log(http.StatusForbidden, "not_claude_code", nil, 1)
		writeAPIError(w, dialectAnthropic, http.StatusForbidden, "permission_error",
			"this proxy serves Anthropic subscriptions, which only cover Claude Code")
		return
	}

	pPool := s.providerPool(w, dialectAnthropic, p)
	if pPool == nil {
		log(http.StatusServiceUnavailable, "pool_no_accounts", nil, 1)
		return
	}
	normalized := normalizedCountBody(body, model, upstream)
	route := s.anthropicRoute(r, body, rc, upstream)
	var req any
	if p == provider.Anthropic {
		req = &backend.AnthRequest{
			Path: "/v1/messages", Body: normalized,
			Version:   r.Header.Get("anthropic-version"),
			Beta:      r.Header.Get("anthropic-beta"),
			UserAgent: r.Header.Get("user-agent"),
		}
	} else {
		path := "/v1/messages"
		if p == provider.Zen {
			path = "/messages"
		}
		req = &backend.RelayRequest{Body: normalized, Path: path}
	}
	served, err := pPool.Execute(r.Context(), route, req)
	if err != nil {
		log(poolErrorStatus(err), poolErrorKind(err), nil, poolErrorAttempts(err))
		writePoolErrorFor(w, dialectAnthropic, err)
		return
	}
	resp, ok := served.Response.(*http.Response)
	if !ok {
		log(http.StatusInternalServerError, "upstream_decode", served.AccountID, int64(served.Attempts))
		writeError(w, http.StatusInternalServerError, "unexpected upstream response")
		return
	}
	defer resp.Body.Close()
	attempts := int64(served.Attempts)

	// mirrored from Rust's forwarded_response + pool_rate_limit_headers: only a
	// whitelist of upstream headers crosses the relay, and the per-account
	// ratelimit headers are replaced with pool-wide averages.
	copyRelayHeaders(w, resp.Header)
	if p == provider.Anthropic {
		windows := pPool.Slots().PoolWindowsNamed(rc.user, rc.token.PinnedAccount, "")
		for name, vals := range relayRateLimitHeaders(windows) {
			for _, v := range vals {
				w.Header().Add(name, v)
			}
		}
	}
	w.WriteHeader(resp.StatusCode)
	streaming := strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream")
	success := resp.StatusCode >= 200 && resp.StatusCode < 300

	if !success || !streaming {
		buf, readErr := io.ReadAll(resp.Body)
		scan.noteBytes(len(buf))
		if success {
			scan.applyJSON(buf)
		}
		_, _ = w.Write(buf)
		kind := ""
		if !success {
			kind = "upstream_error"
		} else if readErr != nil {
			kind = "upstream_read"
		}
		log(resp.StatusCode, kind, served.AccountID, attempts)
		return
	}

	streamObserved(w, resp.Body, scan, resp.StatusCode)
	log(resp.StatusCode, "", served.AccountID, attempts)
}

func (s *Server) backgroundReload(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		for _, p := range s.pools {
			if err := p.Slots().Reload(ctx); err != nil {
				slog.Warn("pool reload failed", "error", err)
			}
		}
		s.reloadEgress()
		s.pollUsage(ctx)
	}
}

func (s *Server) reloadEgress() {
	for _, client := range s.egressClients {
		if err := client.ReloadEgress(); err != nil {
			slog.Warn("egress reload failed", "error", err)
		}
	}
}

func (s *Server) backgroundPrices(ctx context.Context) {
	ticker := time.NewTicker(12 * time.Hour)
	defer ticker.Stop()
	for range ticker.C {
		s.prices.Refresh()
		// Repricing rewrites logged rows after the fact, so every folded
		// running total must be rebuilt from id 0.
		s.invalidateMetrics()
	}
}

// invalidateMetrics forces the metrics refresher to rebuild its folded
// totals (used after a pricing rewrite). Safe before startMetrics too.
func (s *Server) invalidateMetrics() {
	if s.metrics != nil {
		s.metrics.invalidate()
	}
}

func (s *Server) backgroundCatalogs(ctx context.Context) {
	s.refreshCatalogs(ctx)
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		s.refreshCatalogs(ctx)
	}
}

// refreshCatalogs keeps per-provider model lists. A provider that fails to
// answer keeps its previous list, so one flaky upstream cannot empty every
// client's model picker.
func (s *Server) refreshCatalogs(ctx context.Context) {
	storeIDs := func(p provider.Provider, ids []string) {
		s.catalogsMu.Lock()
		s.catalogs[p] = ids
		s.catalogsMu.Unlock()
		if pl := s.pools[p]; pl != nil {
			for _, slot := range pl.Slots().List() {
				pl.Slots().SetCatalog(slot, ids)
			}
		}
	}
	ask := func(p provider.Provider, f func(context.Context, string, *pool.Slot) (any, error)) {
		pl := s.pools[p]
		if pl == nil || pl.Slots().Len() == 0 {
			return
		}
		v, err := pl.FirstAnswer(ctx, f)
		if err != nil {
			slog.Debug("catalog probe failed", "provider", p, "error", err)
			return
		}
		if ids, ok := v.([]string); ok {
			storeIDs(p, ids)
		}
	}
	if s.anthropicClient != nil {
		ask(provider.Anthropic, func(ctx context.Context, token string, slot *pool.Slot) (any, error) {
			models, se := s.anthropicClient.Models(ctx, token, slot.AuthMode)
			if se != nil {
				return nil, se
			}
			s.catalogsMu.Lock()
			s.anthropicCatalog = models
			s.catalogsMu.Unlock()
			ids := make([]string, 0, len(models))
			for _, model := range models {
				ids = append(ids, model.ID)
			}
			return ids, nil
		})
	}
	if s.geminiClient != nil {
		ask(provider.Gemini, func(ctx context.Context, token string, slot *pool.Slot) (any, error) {
			var ref *string
			if slot.HTTPReferer != "" {
				ref = &slot.HTTPReferer
			}
			models, se := s.geminiClient.Models(ctx, token, ref)
			if se != nil {
				return nil, se
			}
			s.catalogsMu.Lock()
			s.geminiCatalog = models
			s.catalogsMu.Unlock()
			ids := make([]string, 0, len(models))
			for _, model := range models {
				ids = append(ids, model.ID)
			}
			return ids, nil
		})
	}
	if s.zenClient != nil {
		if ids, se := s.zenClient.Models(ctx); se == nil {
			storeIDs(provider.Zen, ids)
		}
	}
	if s.copilotClient != nil {
		ask(provider.Copilot, func(ctx context.Context, token string, _ *pool.Slot) (any, error) {
			models, se := s.copilotClient.Models(ctx, token)
			if se != nil {
				return nil, se
			}
			return models, nil
		})
	}
	if s.glmClient != nil {
		ask(provider.GLM, func(ctx context.Context, token string, _ *pool.Slot) (any, error) {
			ids, se := s.glmClient.Models(ctx, token)
			if se != nil {
				return nil, se
			}
			return ids, nil
		})
	}
	catalogOf := func(p provider.Provider, f func(context.Context, string) ([]string, *upstream.SendError)) {
		ask(p, func(ctx context.Context, token string, _ *pool.Slot) (any, error) {
			ids, se := f(ctx, token)
			if se != nil {
				return nil, se
			}
			return ids, nil
		})
	}
	if s.deepseekClient != nil {
		catalogOf(provider.DeepSeek, s.deepseekClient.Models)
	}
	if s.radianceClient != nil {
		catalogOf(provider.Radiance, s.radianceClient.Models)
	}
}

func (s *Server) catalog(p provider.Provider) []string {
	s.catalogsMu.RLock()
	defer s.catalogsMu.RUnlock()
	return append([]string(nil), s.catalogs[p]...)
}

// pollUsage refreshes quota figures for idle accounts, so scheduling does not
// rely solely on what a response header happened to report during traffic.
func (s *Server) pollUsage(ctx context.Context) {
	if pl := s.pools[provider.OpenAI]; pl != nil && s.backend != nil {
		for _, slot := range pl.Slots().List() {
			token, err := pl.Slots().FreshToken(ctx, slot, false)
			if err != nil {
				continue
			}
			s.codexAccountCatalog(ctx, pl, slot)
			usage, se := s.backend.Usage(ctx, token, slot.ProviderAccountID)
			if se != nil || usage == nil {
				continue
			}
			if usage.Locked {
				continue
			}
			pl.Slots().NoteUsage(slot, usage)
		}
	}
	if s.anthropicClient != nil {
		s.pollAnthropicUsage(ctx)
	}
	if s.copilotClient != nil {
		s.pollCopilotUsage(ctx)
	}
	if s.glmClient != nil {
		s.pollGLMUsage(ctx)
	}
}

func (s *Server) pollAnthropicUsage(ctx context.Context) {
	pl := s.pools[provider.Anthropic]
	if pl == nil {
		return
	}
	for _, slot := range pl.Slots().List() {
		token, err := pl.Slots().FreshToken(ctx, slot, false)
		if err != nil {
			continue
		}
		report, se := s.anthropicClient.UsageReport(ctx, token)
		if se != nil {
			continue
		}
		var windows []pool.UsageWindow
		for _, w := range report.Windows() {
			var resetsAt int64
			if w.ResetsAt != nil {
				if t, err := time.Parse(time.RFC3339, *w.ResetsAt); err == nil {
					resetsAt = t.Unix()
				}
			}
			windows = append(windows, pool.UsageWindow{Name: w.Name, Utilization: w.Utilization, ResetsAt: resetsAt})
		}
		if len(windows) == 0 {
			continue
		}
		pl.Slots().NoteUsage(slot, &pool.AccountUsage{Windows: windows, Locked: report.Locked(), ObservedAt: time.Now().Unix()})
	}
}

func (s *Server) pollCopilotUsage(ctx context.Context) {
	pl := s.pools[provider.Copilot]
	if pl == nil {
		return
	}
	for _, slot := range pl.Slots().List() {
		token, err := pl.Slots().FreshToken(ctx, slot, false)
		if err != nil {
			continue
		}
		report, se := s.copilotClient.Quota(ctx, token)
		if se != nil {
			continue
		}
		premium := report.QuotaSnapshots.PremiumInteractions
		if premium == nil {
			continue
		}
		pl.Slots().NoteUsage(slot, &pool.AccountUsage{
			Windows:    []pool.UsageWindow{{Name: "30d", Utilization: report.Utilization(), ResetsAt: 0}},
			Locked:     !premium.Unlimited && premium.Remaining <= 0,
			ObservedAt: time.Now().Unix(),
		})
	}
}

func (s *Server) pollGLMUsage(ctx context.Context) {
	pl := s.pools[provider.GLM]
	if pl == nil {
		return
	}
	type entry struct {
		slot   *pool.Slot
		usage  *pool.AccountUsage
		weekly float64
	}
	var polled []entry
	var largest float64
	for _, slot := range pl.Slots().List() {
		token, err := pl.Slots().FreshToken(ctx, slot, false)
		if err != nil {
			continue
		}
		report, se := s.glmClient.Quota(ctx, token)
		if se != nil {
			continue
		}
		polled = append(polled, entry{slot: slot, usage: report.Usage(0), weekly: report.WeeklyAllowance()})
		if allowance := report.WeeklyAllowance(); allowance > largest {
			largest = allowance
		}
	}
	// Capacity is relative to the largest weekly allowance in the pool.
	for _, e := range polled {
		if largest > 0 && e.weekly > 0 {
			e.usage.Capacity = e.weekly / largest
		}
		pl.Slots().NoteUsage(e.slot, e.usage)
	}
}

func (s *Server) Router() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", s.withToken(s.chatCompletions))
	mux.HandleFunc("POST /v1/messages", s.withToken(s.messages))
	mux.HandleFunc("POST /v1/messages/count_tokens", s.withToken(s.countTokens))
	mux.HandleFunc("POST /v1/alpha/search", s.withToken(s.alphaSearch))
	mux.HandleFunc("GET /v1/cache/{session}", s.withToken(s.cacheStatus))
	mux.HandleFunc("POST /v1/responses", s.withToken(s.responses))
	// The Codex client probes GET /v1/responses for a websocket upgrade first;
	// a 426 (missing upgrade headers or a non-OpenAI model) tells it to fall
	// back to HTTP POST. Otherwise the full websocket relay takes over.
	mux.HandleFunc("GET /v1/responses", s.withToken(s.websocketResponses))
	mux.HandleFunc("GET /v1/models", s.withToken(s.models))
	mux.HandleFunc("GET /v1beta/models", s.withToken(s.geminiModels))
	mux.HandleFunc("POST /v1beta/models/{spec}", s.withToken(s.geminiNative))
	mux.HandleFunc("GET /config/codex/auth.json", s.withToken(s.codexAuth))
	mux.HandleFunc("GET /config/codex/config.toml", s.withToken(s.codexConfig))
	mux.HandleFunc("POST /oauth/token", s.oauthToken)
	s.registerDesktopRoutes(mux)
	s.registerProfileRoutes(mux)
	if s.cfg.AdminToken != "" {
		mux.HandleFunc("POST /admin/token", s.withAdminToken(s.createToken))
		mux.HandleFunc("GET /admin/tokens", s.withAdminToken(s.listTokens))
		mux.HandleFunc("GET /admin/accounts", s.withAdminToken(s.listAccounts))
	}
	mux.HandleFunc("GET /health", s.health)
	// Cap all bodies and inflate zstd-encoded ones before routing, matching
	// the Rust DefaultBodyLimit + decompress::zstd_requests middleware.
	return decompressMiddleware(mux)
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.cfg.Bind)
	if err != nil {
		return err
	}
	return s.ServeOn(ln)
}

// ServeOn serves on an already-bound listener, e.g. one inherited via
// systemd LISTEN_FDS during a hot handoff.
func (s *Server) ServeOn(ln net.Listener) error {
	return s.ServeOnWithMetrics(ln, nil)
}

// ServeOnWithMetrics serves the API on ln and /metrics on metricsLn. A nil
// metricsLn means bind cfg.MetricsBind ourselves; a non-nil one is a listener
// inherited across a handoff.
func (s *Server) ServeOnWithMetrics(ln, metricsLn net.Listener) error {
	if metricsLn != nil {
		s.startMetricsOn(metricsLn)
	} else {
		s.startMetrics()
	}
	s.server = &http.Server{Addr: s.cfg.Bind, Handler: s.Router(), ReadHeaderTimeout: 10 * time.Second}
	slog.Info("serving", "bind", ln.Addr().String())
	return s.server.Serve(ln)
}

// Shutdown gracefully stops accepting on both listeners and waits (up to the
// caller's deadline) for in-flight streams to finish.
func (s *Server) Shutdown(ctx context.Context) error {
	var first error
	if s.server != nil {
		first = s.server.Shutdown(ctx)
	}
	s.metricsMu.Lock()
	ms := s.metricsServer
	s.metricsMu.Unlock()
	if ms != nil {
		if err := ms.Shutdown(ctx); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Close force-closes both listeners once the drain deadline has been reached.
func (s *Server) Close() error {
	var first error
	if s.server != nil {
		first = s.server.Close()
	}
	s.metricsMu.Lock()
	ms := s.metricsServer
	s.metricsMu.Unlock()
	if ms != nil {
		if err := ms.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

type requestContext struct {
	token       store.Token
	apiKey      string
	user        string
	meterID     int64
	tokensSpent *int64
	sessionKey  string
	started     time.Time
}

type dialect int

const (
	dialectOpenAI dialect = iota
	dialectAnthropic
)

func dialectOf(path string) dialect {
	if strings.HasPrefix(path, "/v1/messages") {
		return dialectAnthropic
	}
	return dialectOpenAI
}

// writeAPIError renders the two error envelopes Codex and Claude Code expect.
func writeAPIError(w http.ResponseWriter, d dialect, status int, kind, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if d == dialectAnthropic {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"type": "error", "error": map[string]any{"type": kind, "message": msg},
		})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"type": kind, "message": msg, "code": nil, "param": nil},
	})
}

func (s *Server) withToken(next func(http.ResponseWriter, *http.Request, *requestContext)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		d := dialectOf(r.URL.Path)
		key := extractKey(r)
		if key == "" {
			writeAPIError(w, d, http.StatusUnauthorized, "authentication_error", "missing API token (x-api-key or Authorization: Bearer)")
			return
		}
		tok, err := s.store.AuthToken(r.Context(), key)
		if err != nil {
			writeAPIError(w, d, http.StatusUnauthorized, "authentication_error", "invalid or revoked API token")
			return
		}
		// Desktop account reads must keep working after a quota is spent,
		// otherwise the user cannot even see why requests stopped.
		path := r.URL.Path
		localRead := (strings.HasPrefix(path, "/backend-api/") && path != "/backend-api/aura/site_status") ||
			(r.Method == http.MethodGet && (path == "/v1/responses" || strings.HasPrefix(path, "/v1/cache/")))
		var admission store.Admission
		if !localRead {
			admission, err = s.store.Admit(r.Context(), tok.ID, store.TokenLimits{
				Requests: tok.Requests, Tokens: tok.Tokens, WindowSecs: tok.WindowSecs,
				SlowdownMS: tok.SlowdownMS, Metered: decodeList(tok.MeteredProviders),
			})
			if err != nil {
				var denied *store.AdmissionError
				if errors.As(err, &denied) {
					msg := "API token request limit exceeded"
					if denied.Kind == "token" {
						msg = "API token token limit exceeded"
					}
					w.Header().Set("retry-after", strconv.FormatInt(denied.RetryAfter, 10))
					writeAPIError(w, d, http.StatusTooManyRequests, "rate_limit_error", msg)
					return
				}
				writeAPIError(w, d, http.StatusInternalServerError, "api_error", "internal error")
				return
			}
		}
		setInt := func(name string, v *int64) {
			if v != nil {
				w.Header().Set(name, strconv.FormatInt(*v, 10))
			}
		}
		setInt("x-ratelimit-limit-requests", admission.RequestLimit)
		setInt("x-ratelimit-remaining-requests", admission.RequestsRemaining)
		setInt("x-ratelimit-limit-tokens", admission.TokenLimit)
		setInt("x-ratelimit-remaining-tokens", admission.TokensRemaining)
		if !localRead {
			w.Header().Set("x-ratelimit-reset", strconv.FormatInt(admission.ResetAfter, 10))
		}
		if admission.SlowdownMS > 0 {
			w.Header().Set("x-slop-slowdown-ms", strconv.FormatInt(admission.SlowdownMS, 10))
			time.Sleep(time.Duration(admission.SlowdownMS) * time.Millisecond)
		}
		next(w, r, &requestContext{
			token: tok, apiKey: key, user: tok.User,
			meterID: admission.MeterID, tokensSpent: admission.TokensSpent,
			started: started,
		})
	}
}

func decodeList(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}

const accessKeyJWT = "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0"

// unwrapAccessKey accepts the unsigned JWT the Codex config hands back to
// clients, pulling the proxy key out of its api_key claim.
func unwrapAccessKey(raw string) string {
	if !strings.HasPrefix(raw, accessKeyJWT+".") {
		return raw
	}
	parts := strings.Split(raw, ".")
	if len(parts) < 2 {
		return raw
	}
	decoded, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return raw
	}
	var claim struct {
		APIKey string `json:"api_key"`
	}
	if json.Unmarshal(decoded, &claim) == nil && claim.APIKey != "" {
		return claim.APIKey
	}
	return raw
}

func extractKey(r *http.Request) string {
	for _, name := range []string{"x-api-key", "x-goog-api-key"} {
		if v := r.Header.Get(name); v != "" {
			return unwrapAccessKey(v)
		}
	}
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		return unwrapAccessKey(strings.TrimPrefix(h, "Bearer "))
	}
	if v := r.URL.Query().Get("key"); v != "" {
		return unwrapAccessKey(v)
	}
	if v := r.URL.Query().Get("api_key"); v != "" {
		return unwrapAccessKey(v)
	}
	return ""
}

func sessionKeyFrom(r *http.Request, body []byte) string {
	if h := r.Header.Get("X-Session-Key"); h != "" {
		return h
	}
	var v struct {
		SessionID string `json:"session_id"`
		User      string `json:"user"`
	}
	if json.Unmarshal(body, &v) == nil {
		if v.SessionID != "" {
			return v.SessionID
		}
		if v.User != "" {
			return v.User
		}
	}
	return ""
}

func (s *Server) route(r *http.Request, body []byte, rc *requestContext, model string) pool.Route {
	session := sessionKeyFrom(r, body)
	rc.sessionKey = session
	var peek struct {
		ServiceTier string `json:"service_tier"`
	}
	if json.Unmarshal(body, &peek) != nil {
		peek.ServiceTier = ""
	}
	return pool.Route{
		SessionKey:    session,
		Model:         model,
		ServiceTier:   peek.ServiceTier,
		User:          rc.user,
		ReservedOnly:  rc.token.ReservedOnly,
		PreferTrusted: rc.token.PreferTrusted,
		PinnedAccount: rc.token.PinnedAccount,
	}
}

func (s *Server) chatCompletions(w http.ResponseWriter, r *http.Request, rc *requestContext) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var chat translate.ChatRequest
	if err := json.Unmarshal(body, &chat); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	requested := chat.Model
	resolved := translate.Resolve(&s.cfg, requested)
	upstream, effort := resolved.Model, resolved.Effort
	target := s.cfg.Models.Route(upstream)
	if !s.admitModel(w, r, rc, upstream, target) {
		return
	}
	switch target {
	case provider.Anthropic:
		writeError(w, http.StatusBadRequest, "this model is relayed to Anthropic and only available on /v1/messages")
		return
	case provider.Gemini, provider.Copilot:
		s.chatViaChatUpstream(w, r, body, rc, &chat, requested, upstream, effort, target)
		return
	case provider.GLM, provider.DeepSeek, provider.Experiential, provider.OpenRouter, provider.Radiance:
		writeError(w, http.StatusBadRequest, "this model is served over /v1/messages")
		return
	case provider.Zen:
		switch s.cfg.Models.ZenDialect(upstream) {
		case config.ZenMessages:
			writeError(w, http.StatusBadRequest, "this model is served over /v1/messages")
			return
		case config.ZenChat:
			s.zenChat(w, r, body, rc, &chat, requested, upstream)
			return
		}
	}

	irReq, err := translate.ToResponses(&chat, &s.cfg)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	route := s.translatedRoute(r, body, rc, &irReq)
	capture := &translate.UsageCapture{}
	reply, err := s.executeResponses(r.Context(), target, route, &irReq, nil, r.Header, capture)
	if err != nil {
		s.logDispatchFailure(rc, target, requested, upstream, "chat", body, err)
		writePoolError(w, err)
		return
	}
	defer reply.body.Close()

	stream := chat.Stream != nil && *chat.Stream
	status := http.StatusOK
	var result []byte
	if stream {
		flusher, ok := w.(http.Flusher)
		if !ok {
			writeError(w, http.StatusInternalServerError, "streaming unsupported")
			return
		}
		backend.StreamChatResponses(reply.events, requested, chat.IncludeUsage(), capture, w, flusher)
	} else {
		agg := backend.CollectResponses(reply.events, capture)
		if agg.Error != nil {
			status = http.StatusBadGateway
			writeAPIError(w, dialectOpenAI, status, "api_error", *agg.Error)
		} else {
			result = translate.RenderAggregatedChat(&agg, requested)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(result)
		}
	}
	snap := capture.Snapshot()
	s.recordUsage(rc, target, requested, upstream, "chat", reply.accountID, snap.InputTokens, snap.OutputTokens,
		UsageMeta{Body: body, Captured: &snap, ResponseBytes: int64(len(result)), Attempts: reply.attempts, Status: status, Force: true})
}

// chatViaChatUpstream serves models whose upstream speaks the Chat wire
// (Gemini's compatible surface and Copilot). Their replies are already in the
// caller's dialect, so they pass through without a second translation.
func (s *Server) chatViaChatUpstream(w http.ResponseWriter, r *http.Request, body []byte, rc *requestContext, chat *translate.ChatRequest, requested, upstream, effort string, p provider.Provider) {
	chat.Model = upstream
	if chat.ReasoningEffort == nil && effort != "" {
		effortCopy := effort
		chat.ReasoningEffort = &effortCopy
	}
	stream := chat.Stream != nil && *chat.Stream
	if stream {
		chat.StreamOptions = &translate.StreamOptions{IncludeUsage: true}
	}
	var req any
	if p == provider.Gemini {
		req = chat
	} else {
		encoded, err := json.Marshal(chat)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		agent, vision := copilotFlags(encoded)
		req = &backend.RelayRequest{Body: encoded, Path: "/chat/completions", Agent: agent, Vision: vision}
	}
	pl := s.providerPool(w, dialectOpenAI, p)
	if pl == nil {
		return
	}
	served, err := pl.Execute(r.Context(), s.route(r, body, rc, upstream), req)
	if err != nil {
		s.logDispatchFailure(rc, p, requested, upstream, "chat", body, err)
		writePoolError(w, err)
		return
	}
	s.relayChat(w, rc, body, p, requested, upstream, effort, stream, served)
}

// zenChat serves models configured on Zen's Chat wire, padding the request
// with the free tier's required tool definitions first.
func (s *Server) zenChat(w http.ResponseWriter, r *http.Request, body []byte, rc *requestContext, chat *translate.ChatRequest, requested, upstream string) {
	pl := s.pools[provider.Zen]
	if pl == nil {
		writeError(w, http.StatusServiceUnavailable, "zen backend unavailable")
		return
	}
	chat.Model = upstream
	stream := chat.Stream != nil && *chat.Stream
	if stream {
		chat.StreamOptions = &translate.StreamOptions{IncludeUsage: true}
	}
	encoded, err := json.Marshal(chat)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if gated := backend.SatisfyChatToolGate(encoded); gated != nil {
		encoded = gated
	}
	served, err := pl.Execute(r.Context(), s.route(r, body, rc, upstream), &backend.RelayRequest{Body: encoded, Path: "/chat/completions"})
	if err != nil {
		s.logDispatchFailure(rc, provider.Zen, requested, upstream, "chat", body, err)
		writePoolError(w, err)
		return
	}
	s.relayChat(w, rc, body, provider.Zen, requested, upstream, "", stream, served)
}

func copilotFlags(body []byte) (agent, vision bool) {
	var v struct {
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if json.Unmarshal(body, &v) != nil {
		return false, false
	}
	for _, m := range v.Messages {
		if m.Role == "assistant" || m.Role == "tool" {
			agent = true
		}
		var parts []struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(m.Content, &parts) == nil {
			for _, part := range parts {
				if part.Type == "image_url" {
					vision = true
				}
			}
		}
	}
	return agent, vision
}

func (s *Server) messages(w http.ResponseWriter, r *http.Request, rc *requestContext) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var anth translate.AnthropicRequest
	if err := json.Unmarshal(body, &anth); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	requested := anth.Model
	upstream := translate.Resolve(&s.cfg, requested).Model
	target := s.cfg.Models.Route(upstream)
	if !s.admitModel(w, r, rc, upstream, target) {
		return
	}
	if isVerbatimProvider(target) || (target == provider.Zen && s.cfg.Models.ZenDialect(upstream) == config.ZenMessages) {
		s.relayMessages(w, r, body, rc, target, requested)
		return
	}
	if target == provider.Copilot {
		writeAPIError(w, dialectAnthropic, http.StatusBadRequest, "invalid_request_error", "this model is served over /v1/chat/completions")
		return
	}

	irReq, err := translate.AnthropicToResponses(&anth, &s.cfg, target)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	route := s.translatedRoute(r, body, rc, &irReq)
	capture := &translate.UsageCapture{}
	reply, err := s.executeResponses(r.Context(), target, route, &irReq, nil, r.Header, capture)
	if err != nil {
		s.logDispatchFailure(rc, target, requested, upstream, "messages", body, err)
		writePoolErrorFor(w, dialectAnthropic, err)
		return
	}
	defer reply.body.Close()

	emitThinking := anth.ThinkingEnabled()
	stream := anth.Stream != nil && *anth.Stream
	status := http.StatusOK
	var result []byte
	if stream {
		flusher, ok := w.(http.Flusher)
		if !ok {
			writeError(w, http.StatusInternalServerError, "streaming unsupported")
			return
		}
		backend.StreamAnthropicResponses(reply.events, upstream, translate.Estimate(&irReq), emitThinking, capture, w, flusher)
	} else {
		agg := backend.CollectResponses(reply.events, capture)
		if agg.Error != nil {
			status = http.StatusBadGateway
			writeAPIError(w, dialectAnthropic, status, "api_error", *agg.Error)
		} else {
			result = translate.RenderAggregatedAnthropic(&agg, upstream, emitThinking)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(result)
		}
	}
	snap := capture.Snapshot()
	s.recordUsage(rc, target, requested, upstream, "messages", reply.accountID, snap.InputTokens, snap.OutputTokens,
		UsageMeta{Body: body, Captured: &snap, ResponseBytes: int64(len(result)), Attempts: reply.attempts, Status: status, Force: true})
}

// alphaSearch relays Codex's web-search tool call. It always goes to the Codex
// pool whatever model the turn names, mirroring `state.pools.codex.search`.
func (s *Server) alphaSearch(w http.ResponseWriter, r *http.Request, rc *requestContext) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeAPIError(w, dialectOpenAI, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	var peek struct {
		ID    *string `json:"id"`
		Model string  `json:"model"`
	}
	if json.Unmarshal(body, &peek) != nil {
		writeAPIError(w, dialectOpenAI, http.StatusBadRequest, "invalid_request_error", "invalid request")
		return
	}
	rt := s.route(r, body, rc, peek.Model)
	if peek.ID != nil && *peek.ID != "" {
		rt.SessionKey = *peek.ID
	}
	req := &backend.Request{Body: body, Headers: r.Header.Clone(), Path: "/alpha/search", SkipModelQuery: true}
	pl := s.pools[provider.OpenAI]
	if pl == nil {
		writeError(w, http.StatusServiceUnavailable, "no codex accounts; run login")
		return
	}
	served, err := pl.Execute(r.Context(), rt, req)
	if err != nil {
		writePoolError(w, err)
		return
	}
	relayJSON(w, served)
}

// relayJSON writes an upstream response back verbatim with a JSON content
// type, mirroring the Rust relay_json helper.
func relayJSON(w http.ResponseWriter, served *pool.Served) {
	resp, ok := served.Response.(*backend.Response)
	if !ok {
		writeError(w, http.StatusInternalServerError, "unexpected upstream response")
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// websocketFallback preserves the Codex client's "try websocket, then fall
// back to POST" contract without porting the full websocket relay subsystem.
func (s *Server) websocketFallback(w http.ResponseWriter, r *http.Request, rc *requestContext) {
	writeAPIError(w, dialectOpenAI, http.StatusUpgradeRequired, "invalid_request_error",
		"websocket transport unavailable; use POST /v1/responses")
}

// geminiModels answers the /v1beta discovery surface in the same shape as
// /v1/models, narrowed to models routed to Gemini.
// countTokens relays to Anthropic's own endpoint when the model routes there,
// and falls back to the local estimator for every other provider. This is a
// routing branch, not a failure fallback: a failed relay returns the upstream
// error verbatim, matching the Rust original.
func (s *Server) countTokens(w http.ResponseWriter, r *http.Request, rc *requestContext) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeAPIError(w, dialectAnthropic, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	var peek struct {
		Model string `json:"model"`
	}
	if json.Unmarshal(body, &peek) != nil {
		writeAPIError(w, dialectAnthropic, http.StatusBadRequest, "invalid_request_error", "invalid request")
		return
	}
	resolved := translate.Resolve(&s.cfg, peek.Model).Model
	target := s.cfg.Models.Route(resolved)
	if !s.admitModel(w, r, rc, resolved, target) {
		return
	}

	if target == provider.Anthropic {
		s.relayCountTokens(w, r, rc, body, peek.Model)
		return
	}

	var anth translate.AnthropicRequest
	if json.Unmarshal(body, &anth) != nil {
		writeAPIError(w, dialectAnthropic, http.StatusBadRequest, "invalid_request_error", "invalid request")
		return
	}
	ir, err := translate.AnthropicToResponses(&anth, &s.cfg, target)
	if err != nil {
		writeAPIError(w, dialectAnthropic, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	writeJSON(w, map[string]any{"input_tokens": translate.Estimate(&ir)})
}

// relayCountTokens mirrors server/relay.rs::count_tokens.
func (s *Server) relayCountTokens(w http.ResponseWriter, r *http.Request, rc *requestContext, body []byte, requested string) {
	if s.cfg.Anthropic.RequireClaudeCode && !rc.token.ReservedOnly && !isClaudeCode(r.Header) {
		writeAPIError(w, dialectAnthropic, http.StatusForbidden, "permission_error",
			"this proxy serves Anthropic subscriptions, which only cover Claude Code")
		return
	}
	pl := s.pools[provider.Anthropic]
	if pl == nil {
		writeAPIError(w, dialectAnthropic, http.StatusServiceUnavailable, "api_error",
			"no usable anthropic accounts; run `login`")
		return
	}
	resolved := translate.Resolve(&s.cfg, requested)
	normalized := normalizedCountBody(body, requested, resolved.Model)
	route := s.anthropicRoute(r, body, rc, resolved.Model)
	served, err := pl.Execute(r.Context(), route, &backend.AnthRequest{
		Path: "/v1/messages/count_tokens", Body: normalized,
		Version: r.Header.Get("anthropic-version"), Beta: r.Header.Get("anthropic-beta"),
		UserAgent: r.Header.Get("user-agent"),
	})
	if err != nil {
		writeAnthropicPoolError(w, err)
		return
	}
	resp, ok := served.Response.(*http.Response)
	if !ok || resp == nil {
		writeAPIError(w, dialectAnthropic, http.StatusBadGateway, "api_error", "unexpected upstream response")
		return
	}
	defer resp.Body.Close()
	forwardAnthropicHeaders(w, resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func isClaudeCode(h http.Header) bool {
	has := func(name, want string) bool {
		return strings.Contains(h.Get(name), want)
	}
	return has("anthropic-beta", "claude-code-") && has("user-agent", "claude-cli/")
}

func normalizedCountBody(body []byte, requested, upstream string) []byte {
	if requested == upstream {
		return body
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(body, &obj) != nil {
		return body
	}
	obj["model"] = mustMarshal(upstream)
	out, err := json.Marshal(obj)
	if err != nil {
		return body
	}
	return out
}

func forwardAnthropicHeaders(w http.ResponseWriter, h http.Header) {
	for name, values := range h {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "anthropic-ratelimit-") {
			continue
		}
		if lower == "content-type" || lower == "request-id" || lower == "retry-after" || strings.HasPrefix(lower, "anthropic-") {
			for _, v := range values {
				w.Header().Add(name, v)
			}
		}
	}
}

func writeAnthropicPoolError(w http.ResponseWriter, err error) {
	if pe, ok := err.(*pool.PoolError); ok {
		switch pe.Kind {
		case pool.ErrNoAccounts:
			writeAPIError(w, dialectAnthropic, http.StatusServiceUnavailable, "api_error", pe.Message)
		case pool.ErrAllCooling:
			if pe.RetryAfter > 0 {
				w.Header().Set("retry-after", strconv.FormatInt(pe.RetryAfter, 10))
			}
			writeAPIError(w, dialectAnthropic, http.StatusTooManyRequests, "rate_limit_error", pe.Message)
		case pool.ErrBadRequest:
			writeAPIError(w, dialectAnthropic, http.StatusBadRequest, "invalid_request_error", pe.Message)
		case pool.ErrRefused:
			writeAPIError(w, dialectAnthropic, http.StatusForbidden, "permission_error", pe.Message)
		default:
			writeAPIError(w, dialectAnthropic, http.StatusBadGateway, "api_error", pe.Message)
		}
		return
	}
	writeAPIError(w, dialectAnthropic, http.StatusInternalServerError, "api_error", err.Error())
}

// geminiNative serves the native Gemini surface (POST /v1beta/models/{spec}).
// Body and response pass through untranslated; only usageMetadata is read on
// the way past for metering.
func (s *Server) geminiNative(w http.ResponseWriter, r *http.Request, rc *requestContext) {
	spec := r.PathValue("spec")
	idx := strings.LastIndex(spec, ":")
	if idx < 0 {
		writeAPIError(w, dialectOpenAI, http.StatusNotFound, "invalid_request_error",
			"expected /v1beta/models/{model}:{generateContent|streamGenerateContent}")
		return
	}
	rawModel, action := spec[:idx], spec[idx+1:]
	if action != "generateContent" && action != "streamGenerateContent" {
		writeAPIError(w, dialectOpenAI, http.StatusNotFound, "invalid_request_error", "unsupported action on the native surface")
		return
	}
	resolved := translate.Resolve(&s.cfg, rawModel).Model
	target := s.cfg.Models.Route(resolved)
	if target != provider.Gemini {
		writeAPIError(w, dialectOpenAI, http.StatusBadRequest, "invalid_request_error", "this model is not served by the gemini backend")
		return
	}
	if !s.admitModel(w, r, rc, resolved, target) {
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeAPIError(w, dialectOpenAI, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	if !json.Valid(body) {
		writeAPIError(w, dialectOpenAI, http.StatusBadRequest, "invalid_request_error", "invalid request")
		return
	}
	pl := s.pools[provider.Gemini]
	if pl == nil {
		writeError(w, http.StatusServiceUnavailable, "no gemini accounts; run login")
		return
	}
	query := r.URL.Query()
	query.Del("key")
	query.Del("api_key")
	if action == "streamGenerateContent" {
		query.Set("alt", "sse")
	}
	req := &backend.GeminiNativeRequest{Model: resolved, Action: action, Query: query.Encode(), Body: body}
	served, err := pl.Execute(r.Context(), s.route(r, body, rc, resolved), req)
	if err != nil {
		writePoolError(w, err)
		return
	}
	result, ok := served.Response.(*backend.GeminiResponse)
	if !ok {
		writeError(w, http.StatusBadGateway, "unexpected upstream response")
		return
	}
	resp := result.Response
	defer resp.Body.Close()
	s.relayChatBody(w, rc, body, provider.Gemini, rawModel, resolved, "native", "", served, resp, true)
}

// Desktop reads and writes for ChatGPT features the proxy has no part in.
// Each returns the empty answer the client expects so it stops retrying.
var desktopReads = map[string]string{
	"/backend-api/settings/user":                            `{"custom_instructions":null,"settings":{},"flags":{},"announcements":{},"eligible_announcements":[]}`,
	"/backend-api/wham/settings/user":                       `{"custom_instructions":null,"settings":{},"flags":{},"announcements":{},"eligible_announcements":[]}`,
	"/backend-api/accounts/{account}/settings":              `{"beta_settings":{},"permissions":[],"admin_work_mode_enabled":false,"admin_work_local_enabled":true,"usage_limit_increase_request":{"kind":"disabled"}}`,
	"/backend-api/accounts/verified_access":                 `{"programs":[{"program":"cyber","state":"unavailable","grants":[]}]}`,
	"/backend-api/wham/onboarding/context":                  `{"desktop_onboarding_completed_at":"1970-01-01T00:00:00Z","role":"engineering","entrypoints":{}}`,
	"/backend-api/wham/tasks/list":                          `{"items":[],"cursor":null}`,
	"/backend-api/pins":                                     `{"items":[]}`,
	"/backend-api/wham/sites/access":                        `{"enabled":false}`,
	"/backend-api/plugins/featured":                         `[]`,
	"/backend-api/ps/plugins/installed":                     `{"plugins":[],"pagination":{"next_page_token":null}}`,
	"/backend-api/ps/plugins/list":                          `{"plugins":[],"pagination":{"next_page_token":null}}`,
	"/backend-api/ps/plugin-categories/{category}/plugins":  `{"plugins":[],"pagination":{"next_page_token":null}}`,
	"/backend-api/profiles/me/page/pin_options":             `{"items":[],"pinned_ids":[],"next_cursor":null}`,
	"/backend-api/wham/rate-limit-reset-credits":            `{"credits":[],"available_count":0}`,
	"/backend-api/referrals/invite/eligibility":             `{"should_show":false}`,
	"/backend-api/gift-credits/senders/eligibility":         `{"eligible":false}`,
	"/backend-api/subscriptions":                            `{"is_processor_stripe":false,"will_renew":false,"subscription_id":null,"scheduled_billing_period":null}`,
	"/backend-api/subscriptions/credits/discount-offer":     `{"offer":null}`,
	"/backend-api/subscriptions/auto_top_up/settings":       `{"is_enabled":false,"payment_method":null,"recharge_threshold":null,"recharge_target":null,"recharge_monthly_limit":null,"auto_reload_credit_discount_policy":null}`,
	"/backend-api/payments/payment_methods":                 `{"payment_methods":[]}`,
	"/backend-api/wham/analytics/daily-code-review-metrics": `{"units":"tokens","data":[],"data_freshness_ts":0}`,
}

var desktopWrites = map[string]string{
	"/backend-api/wham/onboarding/desktop/complete": `{"desktop_onboarding_completed_at":"1970-01-01T00:00:00Z","role":"engineering","entrypoints":{}}`,
	"/backend-api/codex/analytics-events/events":    `{"success":true}`,
	"/backend-api/wham/analytics-events/events":     `{"success":true}`,
}

func (s *Server) registerDesktopRoutes(mux *http.ServeMux) {
	answer := func(body string) http.HandlerFunc {
		return s.withToken(func(w http.ResponseWriter, r *http.Request, rc *requestContext) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, body)
		})
	}
	// Exact static paths are registered directly. Wildcard-bearing keys are
	// matched inside a single subtree handler instead: registering two
	// competing {wild} patterns on ServeMux panics at startup because neither
	// is more specific than the other (e.g. accounts/{account}/settings vs
	// accounts/check/{version} both match /backend-api/accounts/check/settings).
	for path, body := range desktopReads {
		if !strings.Contains(path, "{") {
			mux.HandleFunc("GET "+path, answer(body))
		}
	}
	for path, body := range desktopWrites {
		if !strings.Contains(path, "{") {
			mux.HandleFunc("POST "+path, answer(body))
		}
	}
	mux.HandleFunc("GET /backend-api/", s.withToken(s.desktopFallback(desktopReads)))
	mux.HandleFunc("POST /backend-api/", s.withToken(s.desktopFallback(desktopWrites)))
	mux.HandleFunc("GET /backend-api/me", s.withToken(s.desktopUser))
	mux.HandleFunc("GET /backend-api/wham/accounts/check", s.withToken(s.desktopAccountsCheck))
	mux.HandleFunc("GET /backend-api/accounts/check/{version}", s.withToken(s.desktopAccounts))
	mux.HandleFunc("GET /backend-api/accounts/optimized/check", s.withToken(s.desktopAccount))
	mux.HandleFunc("POST /backend-api/wham/statsig/bootstrap", s.withToken(s.desktopBootstrap))
}

// desktopFallback serves the wildcard-bearing static reply keys that cannot be
// registered on ServeMux without conflicting with other {wild} patterns.
// Unknown backend-api probes get a benign empty JSON object so the desktop
// client does not treat an unimplemented endpoint as a hard failure.
func (s *Server) desktopFallback(table map[string]string) func(http.ResponseWriter, *http.Request, *requestContext) {
	return func(w http.ResponseWriter, r *http.Request, rc *requestContext) {
		body, ok := matchDesktopStatic(table, r.URL.Path)
		if !ok {
			body = "{}"
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}
}

// matchDesktopStatic looks up path in a static reply table. Exact keys win;
// keys containing a {wild} segment are matched segment-by-segment.
func matchDesktopStatic(table map[string]string, path string) (string, bool) {
	if body, ok := table[path]; ok {
		return body, true
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for pattern, body := range table {
		if !strings.Contains(pattern, "{") {
			continue
		}
		segs := strings.Split(strings.Trim(pattern, "/"), "/")
		if len(segs) != len(parts) {
			continue
		}
		matched := true
		for i := range segs {
			if strings.HasPrefix(segs[i], "{") {
				continue
			}
			if segs[i] != parts[i] {
				matched = false
				break
			}
		}
		if matched {
			return body, true
		}
	}
	return "", false
}

func desktopUserID(rc *requestContext) string {
	return "slop-proxy-token-" + strconv.FormatInt(rc.token.ID, 10)
}

func desktopWorkspace(rc *requestContext) map[string]any {
	id := desktopUserID(rc)
	return map[string]any{
		"account_id": codexAccountID, "account_user_id": id,
		"account_user_role": "account-owner", "structure": "personal",
		"plan_type": codexPlan, "name": rc.user,
		"is_deactivated": false, "is_zdr": false, "is_openai_internal": false,
		"is_fedramp_compliant_workspace": false,
		"workspace_backend_origin":       "NO_CONSTRAINT",
		"account_routing_override":       "NO_CONSTRAINT",
	}
}

func desktopMembership(rc *requestContext) map[string]any {
	return map[string]any{
		"account": desktopWorkspace(rc),
		"account_user": map[string]any{
			"is_trial": false, "seat_type": "default", "trial_state": nil,
			"trial_expires_at": nil, "pending_seat_upgrade_request": false,
		},
		"can_access_with_session": true,
		"features":                []any{},
		"entitlement": map[string]any{
			"has_active_subscription": false, "billing_currency": nil,
			"subscription_plan": nil, "scheduled_plan_change": nil, "cancels_at": nil,
		},
		"last_active_subscription": map[string]any{"subscription_id": nil, "purchase_origin_platform": nil},
	}
}

func (s *Server) desktopUser(w http.ResponseWriter, r *http.Request, rc *requestContext) {
	writeJSON(w, map[string]any{
		"id": desktopUserID(rc), "object": "user", "email": rc.user, "name": rc.user,
	})
}

func (s *Server) desktopAccountsCheck(w http.ResponseWriter, r *http.Request, rc *requestContext) {
	entry := desktopWorkspace(rc)
	entry["can_access_with_session"] = true
	writeJSON(w, map[string]any{
		"accounts":           []any{entry},
		"default_account_id": codexAccountID,
		"account_ordering":   []string{codexAccountID},
	})
}

func (s *Server) desktopAccounts(w http.ResponseWriter, r *http.Request, rc *requestContext) {
	writeJSON(w, map[string]any{
		"account_ordering": []string{codexAccountID},
		"accounts":         map[string]any{codexAccountID: desktopMembership(rc)},
	})
}

func (s *Server) desktopAccount(w http.ResponseWriter, r *http.Request, rc *requestContext) {
	writeJSON(w, desktopMembership(rc))
}

const cloudGate = "1315865107"

func (s *Server) desktopBootstrap(w http.ResponseWriter, r *http.Request, rc *requestContext) {
	var context struct {
		Beta       bool   `json:"desktop_app_beta_enabled"`
		StableID   string `json:"stable_id"`
		AppVersion string `json:"app_version"`
		Locale     string `json:"locale"`
	}
	_ = json.NewDecoder(r.Body).Decode(&context)
	gates := map[string]any{
		cloudGate: map[string]any{"name": cloudGate, "value": false, "secondary_exposures": []any{}, "rule_id": "slop-proxy"},
	}
	for name, value := range s.cfg.Codex.DesktopGates {
		gates[name] = map[string]any{"name": name, "value": value, "secondary_exposures": []any{}, "rule_id": "slop-proxy-config"}
	}
	customIDs := map[string]any{"account_id": codexAccountID, "stableID": context.StableID, "source_surface_stable_id": context.StableID}
	user := map[string]any{
		"userID": desktopUserID(rc), "email": rc.user, "customIDs": customIDs,
		"custom": map[string]any{"plan_type": codexPlan, "desktop_app_beta_enabled": context.Beta},
	}
	if context.AppVersion != "" {
		user["appVersion"] = context.AppVersion
	}
	if context.Locale != "" {
		user["locale"] = context.Locale
	}
	payload := map[string]any{
		"user": user, "feature_gates": gates,
		"dynamic_configs": map[string]any{}, "layer_configs": map[string]any{},
		"has_updates": true, "hash_used": "none", "time": time.Now().UnixMilli(),
	}
	encoded, _ := json.Marshal(payload)
	writeJSON(w, map[string]any{"statsigPayload": string(encoded)})
}

func (s *Server) registerProfileRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /backend-api/wham/usage", s.withToken(s.profileUsage))
	mux.HandleFunc("GET /backend-api/wham/profiles/me", s.withToken(s.profileStats))
	mux.HandleFunc("GET /backend-api/profiles/me", s.withToken(s.profileIdentity))
	mux.HandleFunc("GET /backend-api/profiles/me/page", s.withToken(s.profilePage))
	mux.HandleFunc("GET /backend-api/profiles/{username}/page", s.withToken(s.profilePage))
	mux.HandleFunc("GET /backend-api/wham/usage/daily-token-usage-breakdown", s.withToken(s.profileHistory))
	mux.HandleFunc("GET /backend-api/wham/usage/daily-workspace-user-token-usage-breakdown", s.withToken(s.profileHistory))
}

func profileDetails(rc *requestContext) map[string]any {
	return map[string]any{
		"display_name": rc.user, "username": rc.user,
		"profile_picture_url": nil, "description": nil,
		"photo_frame_style": "circle", "profile_visibility": "private",
	}
}

func (s *Server) profileUsage(w http.ResponseWriter, r *http.Request, rc *requestContext) {
	var windows []pool.UsageWindow
	if pl := s.pools[provider.OpenAI]; pl != nil {
		windows = pl.Slots().PoolWindows(rc.user, rc.token.PinnedAccount, s.cfg.Codex.SoftUtilizationLimit)
	}
	sort.Slice(windows, func(i, j int) bool {
		a, _ := pool.WindowSeconds(windows[i].Name)
		b, _ := pool.WindowSeconds(windows[j].Name)
		return a < b
	})
	now := time.Now().Unix()
	reached := false
	asWindow := func(w pool.UsageWindow) map[string]any {
		if w.Utilization >= 1 {
			reached = true
		}
		resetAt := w.ResetsAt
		if resetAt == 0 {
			resetAt = now
		}
		percent := math.Round(w.Utilization * 100)
		if percent < 0 {
			percent = 0
		}
		if percent > 100 {
			percent = 100
		}
		span, _ := pool.WindowSeconds(w.Name)
		return map[string]any{
			"used_percent": int64(percent), "limit_window_seconds": span,
			"reset_after_seconds": max64(resetAt-now, 0), "reset_at": resetAt,
		}
	}
	limit := map[string]any{"allowed": true, "limit_reached": false, "primary_window": nil, "secondary_window": nil}
	if len(windows) > 0 {
		limit["primary_window"] = asWindow(windows[0])
	}
	if len(windows) > 1 {
		limit["secondary_window"] = asWindow(windows[1])
	}
	limit["allowed"] = !reached
	limit["limit_reached"] = reached
	writeJSON(w, map[string]any{
		"plan_type": codexPlan, "rate_limit": limit, "credits": nil,
		"additional_rate_limits":   []any{},
		"rate_limit_reset_credits": map[string]any{"available_count": 0},
	})
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// profileActivity folds the usage log into the daily totals and streaks the
// desktop profile reads.
func (s *Server) profileActivity(rc *requestContext) (days []map[string]any, totals, streaks map[string]any) {
	usage, err := s.store.DailyUsage(context.Background(), rc.user)
	if err == nil {
		var active []string
		var lifetime, peak int64
		byDay := map[string]int64{}
		for _, u := range usage {
			total := u.Input + u.Cached + u.Output
			byDay[u.Day] += total
		}
		keys := make([]string, 0, len(byDay))
		for day := range byDay {
			keys = append(keys, day)
		}
		sort.Strings(keys)
		for _, day := range keys {
			total := byDay[day]
			active = append(active, day)
			lifetime += total
			if total > peak {
				peak = total
			}
			days = append(days, map[string]any{"date": day, "total_tokens": total})
		}
		current, longest, run := int64(0), int64(0), int64(0)
		for i, day := range active {
			if i > 0 {
				prev, _ := time.Parse("2006-01-02", active[i-1])
				next, _ := time.Parse("2006-01-02", day)
				if next.Sub(prev) > 24*time.Hour {
					run = 0
				}
			}
			run++
			if run > longest {
				longest = run
			}
			current = run
		}
		totals = map[string]any{
			"lifetime_tokens": lifetime, "peak_daily_tokens": peak, "longest_running_turn_sec": nil,
		}
		streaks = map[string]any{"current_streak_days": current, "longest_streak_days": longest}
	}
	if days == nil {
		days = []map[string]any{}
	}
	if totals == nil {
		totals = map[string]any{"lifetime_tokens": 0, "peak_daily_tokens": 0, "longest_running_turn_sec": nil}
	}
	if streaks == nil {
		streaks = map[string]any{"current_streak_days": 0, "longest_streak_days": 0}
	}
	return days, totals, streaks
}

func (s *Server) profileIdentity(w http.ResponseWriter, r *http.Request, rc *requestContext) {
	writeJSON(w, map[string]any{"profile_details": profileDetails(rc)})
}

func (s *Server) profileStats(w http.ResponseWriter, r *http.Request, rc *requestContext) {
	days, totals, streaks := s.profileActivity(rc)
	writeJSON(w, map[string]any{
		"stats": map[string]any{
			"totals": totals, "streaks": streaks, "daily_usage_buckets": days,
		},
		"profile":  profileDetails(rc),
		"metadata": map[string]any{"stats_error": nil},
	})
}

func (s *Server) profilePage(w http.ResponseWriter, r *http.Request, rc *requestContext) {
	days, totals, streaks := s.profileActivity(rc)
	writeJSON(w, map[string]any{
		"is_self": true, "can_edit": false, "has_public_personal_profile": false,
		"profile_details": profileDetails(rc), "page_version": 0,
		"page": map[string]any{
			"workspace_id": nil, "workspace_name": nil,
			"visibility": map[string]any{"value": "private", "can_edit": false},
			"display_settings": map[string]any{
				"show_usage_stats_section": true, "show_activity_graph_section": true,
				"show_insights_section": false, "show_top_plugins_section": false,
			},
			"stats": map[string]any{"streaks": streaks, "agentic": totals},
			"activity_graph": map[string]any{
				"daily_usage_buckets": days, "weekly_usage_buckets": []any{},
				"cumulative_daily_usage_buckets": []any{},
			},
			"insights": nil, "top_plugins": []any{}, "pins": map[string]any{"items": []any{}},
		},
	})
}

func (s *Server) profileHistory(w http.ResponseWriter, r *http.Request, rc *requestContext) {
	q := r.URL.Query()
	start := q.Get("start_date")
	end := q.Get("end_date")
	if end == "" {
		end = time.Now().UTC().Format("2006-01-02")
	}
	if start == "" {
		t, _ := time.Parse("2006-01-02", end)
		start = t.AddDate(0, 0, -29).Format("2006-01-02")
	}
	if start > end {
		writeAPIError(w, dialectOpenAI, http.StatusBadRequest, "invalid_request_error", "the range runs backwards or spans more than 366 days")
		return
	}
	usage, err := s.store.DailyUsage(context.Background(), rc.user)
	if err != nil {
		writeAPIError(w, dialectOpenAI, http.StatusInternalServerError, "api_error", "internal error")
		return
	}
	byDay := map[string]map[string]*store.DayUsage{}
	for i := range usage {
		u := &usage[i]
		if u.Day < start || u.Day > end {
			continue
		}
		if byDay[u.Day] == nil {
			byDay[u.Day] = map[string]*store.DayUsage{}
		}
		if existing := byDay[u.Day][u.Model]; existing != nil {
			existing.Input += u.Input
			existing.Cached += u.Cached
			existing.Output += u.Output
		} else {
			copyUsage := *u
			byDay[u.Day][u.Model] = &copyUsage
		}
	}
	days := make([]string, 0, len(byDay))
	for day := range byDay {
		days = append(days, day)
	}
	sort.Strings(days)
	data := make([]any, 0, len(days))
	for _, day := range days {
		models := make([]any, 0, len(byDay[day]))
		var total int64
		names := make([]string, 0, len(byDay[day]))
		for model := range byDay[day] {
			names = append(names, model)
		}
		sort.Strings(names)
		for _, name := range names {
			u := byDay[day][name]
			sum := u.Input + u.Cached + u.Output
			total += sum
			models = append(models, map[string]any{
				"model": name, "credits": sum,
				"uncached_text_input_tokens": u.Input, "cached_text_input_tokens": u.Cached,
				"text_output_tokens": u.Output, "text_total_tokens": sum,
			})
		}
		data = append(data, map[string]any{
			"date": day, "models": models,
			"product_surface_usage_values": map[string]any{"codex": total},
		})
	}
	writeJSON(w, map[string]any{"units": "tokens", "data": data, "data_freshness_ts": time.Now().Unix()})
}

const (
	codexAccountID = "slop-proxy"
	codexPlan      = "pro"
	codexLifetime  = 10 * 365 * 24 * 3600
)

func unsignedJWT(claims any) string {
	payload, _ := json.Marshal(claims)
	return accessKeyJWT + "." + base64.RawURLEncoding.EncodeToString(payload) + ".slop"
}

// mintCodexTokens issues the unsigned JWT pair the Codex desktop reads from
// auth.json. The refresh token is the access token: its api_key claim is all
// the refresh endpoint needs to reissue the pair.
func mintCodexTokens(rc *requestContext, apiKey string) map[string]any {
	now := time.Now().Unix()
	userID := "slop-proxy-token-" + strconv.FormatInt(rc.token.ID, 10)
	claims := map[string]any{
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_account_id":      codexAccountID,
			"chatgpt_plan_type":       codexPlan,
			"user_id":                 userID,
			"chatgpt_user_id":         userID,
			"chatgpt_account_user_id": userID,
		},
		"https://api.openai.com/profile": map[string]any{"email": rc.user, "name": rc.user},
		"sub":                            userID, "email": rc.user, "name": rc.user,
		"iat": now, "exp": now + codexLifetime,
	}
	accessClaims := make(map[string]any, len(claims)+1)
	for k, v := range claims {
		accessClaims[k] = v
	}
	accessClaims["api_key"] = apiKey
	accessToken := unsignedJWT(accessClaims)
	return map[string]any{
		"id_token":      unsignedJWT(claims),
		"access_token":  accessToken,
		"refresh_token": accessToken,
		"account_id":    codexAccountID,
	}
}

func (s *Server) codexAuth(w http.ResponseWriter, r *http.Request, rc *requestContext) {
	writeJSON(w, map[string]any{
		"OPENAI_API_KEY": nil,
		"tokens":         mintCodexTokens(rc, rc.apiKey),
		"last_refresh":   time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *Server) codexConfig(w http.ResponseWriter, r *http.Request, rc *requestContext) {
	host := r.Host
	if host == "" {
		host = "localhost"
	}
	scheme := r.Header.Get("x-forwarded-proto")
	if scheme == "" {
		scheme = "https"
	}
	body := "openai_base_url = \"" + scheme + "://" + host + "/v1\"\n" +
		"chatgpt_base_url = \"" + scheme + "://" + host + "/backend-api\"\n\n" +
		"[features]\napps = false\n"
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, body)
}

func (s *Server) oauthToken(w http.ResponseWriter, r *http.Request) {
	var in struct {
		RefreshToken string `json:"refresh_token"`
	}
	if json.NewDecoder(r.Body).Decode(&in) != nil {
		writeAPIError(w, dialectOpenAI, http.StatusBadRequest, "invalid_request_error", "refresh_token required")
		return
	}
	key := unwrapAccessKey(in.RefreshToken)
	tok, err := s.store.AuthToken(r.Context(), key)
	if err != nil {
		writeAPIError(w, dialectOpenAI, http.StatusUnauthorized, "authentication_error", "invalid or revoked API token")
		return
	}
	writeJSON(w, mintCodexTokens(&requestContext{token: tok, user: tok.User}, key))
}

func (s *Server) admitModel(w http.ResponseWriter, r *http.Request, rc *requestContext, model string, target provider.Provider) bool {
	d := dialectOf(r.URL.Path)
	if s.cfg.Models.Blocked(model) {
		writeAPIError(w, d, http.StatusForbidden, "permission_error", model+" is blocked on this proxy")
		return false
	}
	if !rc.token.MayUse(target) {
		writeAPIError(w, d, http.StatusForbidden, "permission_error", "this token is not scoped to the "+target.String()+" backend")
		return false
	}
	if rc.tokensSpent != nil && rc.token.Meters(target) {
		w.Header().Set("Retry-After", strconv.FormatInt(*rc.tokensSpent, 10))
		writeAPIError(w, d, http.StatusTooManyRequests, "rate_limit_error", "API token token limit exceeded for the "+target.String()+" backend")
		return false
	}
	return true
}

func (s *Server) tokenMayUse(rc *requestContext, p provider.Provider) bool {
	return rc == nil || rc.token.MayUse(p)
}

func (s *Server) cacheStatus(w http.ResponseWriter, r *http.Request, rc *requestContext) {
	session := r.PathValue("session")
	last, found, err := s.store.SessionLast(r.Context(), rc.user, session)
	if err != nil {
		writeAPIError(w, dialectOpenAI, http.StatusInternalServerError, "api_error", "internal error")
		return
	}
	if !found {
		writeAPIError(w, dialectOpenAI, http.StatusNotFound, "not_found_error", "no recent requests for this session")
		return
	}
	now := time.Now().Unix()
	idle := now - last
	if idle < 0 {
		idle = 0
	}
	ttl := int64(12 * 3600)
	expires := ttl - idle
	if expires < 0 {
		expires = 0
	}
	writeJSON(w, map[string]any{
		"session_key": session, "last_seen": last,
		"idle_secs": idle, "expires_in_secs": expires,
	})
}

// recordUsage writes the full Rust UsageRecord row: token counters plus the
// tool/insight/session dimensions that the Prometheus families aggregate.
// extra is optional so every existing call site keeps working; handlers that
// have a request body and a UsageCapture pass them for the richer dimensions.
func (s *Server) recordUsage(rc *requestContext, p provider.Provider, requested, upstream, dialect string,
	accountID *int64, input, output int64, extra ...UsageMeta) {
	meta := UsageMeta{}
	if len(extra) > 0 {
		meta = extra[0]
	}
	if rc == nil || (input+output <= 0 && !meta.Force) {
		return
	}
	ctx := context.Background()
	if rc.token.Meters(p) {
		var err error
		if rc.meterID > 0 {
			err = s.store.RecordUsageForMeter(ctx, rc.meterID, input, output)
		} else {
			err = s.store.RecordUsage(ctx, rc.token.ID, input, output)
		}
		if err != nil {
			slog.Warn("record usage failed", "error", err)
		}
	}

	ins := inspectUsage(meta.Body)
	effort, tier := meta.Effort, meta.ServiceTier
	if effort == "" {
		effort = ins.Effort
	}
	if tier == "" {
		tier = ins.ServiceTier
	}
	status := meta.Status
	if status == 0 {
		status = http.StatusOK
	}
	attempts := meta.Attempts
	if attempts == 0 {
		attempts = 1
	}
	var duration int64
	if !rc.started.IsZero() {
		duration = time.Since(rc.started).Milliseconds()
	}

	var tokenID *int64
	if rc.token.ID > 0 {
		id := rc.token.ID
		tokenID = &id
	}
	var cacheRead, cacheWrite, reasoning, responseBytes, ttft int64
	toolsCalled, stopReason, errorKind := ins.ToolsCalled, "", meta.ErrorKind
	responseBytes = meta.ResponseBytes
	if captured := meta.Captured; captured != nil {
		cacheRead = captured.CacheReadTokens
		cacheWrite = captured.CacheWriteTokens
		reasoning = captured.ReasoningTokens
		responseBytes = captured.ResponseBytes
		if captured.FirstByteAt != nil && !rc.started.IsZero() {
			ttft = captured.FirstByteAt.Sub(rc.started).Milliseconds()
			if ttft < 0 {
				ttft = 0
			}
		}
		if captured.StopReason != nil {
			stopReason = *captured.StopReason
		}
		if captured.ErrorKind != nil && errorKind == "" {
			errorKind = *captured.ErrorKind
		}
		if len(captured.ToolsCalled) > 0 {
			toolsCalled = strings.Join(captured.ToolsCalled, ",")
		}
	}
	turnIndex := s.store.NextTurnIndex(ctx, rc.user, rc.sessionKey)

	var cost, listCost float64
	if s.prices != nil {
		tokens := pricing.Tokens{Input: input, Output: output}
		cost = s.prices.Cost(upstream, tokens)
		listCost = s.prices.ListCost(upstream, tokens)
	}
	if err := s.store.LogUsage(ctx, store.UsageLog{
		TokenID: tokenID, User: rc.user, Provider: p.String(), Dialect: dialect,
		RequestedModel: requested, UpstreamModel: upstream, AccountID: accountID,
		Effort: effort, ServiceTier: tier,
		InputTokens: input, OutputTokens: output,
		CacheRead: cacheRead, CacheWrite: cacheWrite, ReasoningTokens: reasoning,
		Status: status, ErrorKind: errorKind,
		SessionKey: rc.sessionKey, TurnIndex: turnIndex,
		ToolsDeclared: ins.ToolsDeclared, ToolsCalled: toolsCalled,
		ThinkingBudget: ins.ThinkingBudget, ImageCount: ins.ImageCount,
		RequestBytes: int64(len(meta.Body)), ResponseBytes: responseBytes,
		TTFTMS: ttft, StopReason: stopReason, Attempts: attempts,
		DurationMS: duration, CostUSD: cost, ListCostUSD: listCost,
	}); err != nil {
		slog.Warn("log usage failed", "error", err)
	}
}

func (s *Server) withAdminToken(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") ||
			subtle.ConstantTimeCompare([]byte(token), []byte(s.cfg.AdminToken)) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeAPIError(w, dialectOpenAI, http.StatusUnauthorized, "authentication_error", "valid admin token required")
			return
		}
		next(w, r)
	}
}

func (s *Server) createToken(w http.ResponseWriter, r *http.Request) {
	var in struct {
		User         string `json:"user"`
		ReservedOnly bool   `json:"reserved_only"`
		Requests     int64  `json:"requests"`
		Tokens       int64  `json:"tokens"`
		WindowSecs   int64  `json:"window_seconds"`
		SlowdownMS   int64  `json:"slowdown_ms"`
	}
	if json.NewDecoder(r.Body).Decode(&in) != nil || in.User == "" {
		writeError(w, http.StatusBadRequest, "user required")
		return
	}
	key, _ := store.GenerateToken()
	tok, err := s.store.CreateToken(r.Context(), key, in.User, store.TokenLimits{
		Requests: in.Requests, Tokens: in.Tokens, WindowSecs: in.WindowSecs,
		SlowdownMS: in.SlowdownMS, ReservedOnly: in.ReservedOnly,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]any{"id": tok.ID, "key": key, "user": in.User})
}

func (s *Server) listTokens(w http.ResponseWriter, r *http.Request) {
	tokens, err := s.store.ListTokens(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	type view struct {
		ID           int64  `json:"id"`
		User         string `json:"user"`
		ReservedOnly bool   `json:"reserved_only"`
		Requests     int64  `json:"requests"`
		Tokens       int64  `json:"tokens"`
		WindowSecs   int64  `json:"window_seconds"`
	}
	var out []view
	for _, t := range tokens {
		out = append(out, view{t.ID, t.User, t.ReservedOnly, t.Requests, t.Tokens, t.WindowSecs})
	}
	writeJSON(w, out)
}

func (s *Server) listAccounts(w http.ResponseWriter, r *http.Request) {
	accounts, err := s.store.ListAccounts(r.Context(), "")
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	type view struct {
		ID       int64  `json:"id"`
		Provider string `json:"provider"`
		Label    string `json:"label"`
		Status   string `json:"status"`
		Plan     string `json:"plan"`
		Trusted  bool   `json:"trusted"`
		Reserved bool   `json:"reserved"`
	}
	var out []view
	for _, a := range accounts {
		out = append(out, view{a.ID, a.Provider, a.Label, a.Status, a.Plan, a.Trusted, a.Reserved})
	}
	writeJSON(w, out)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{"ok": true, "accounts": s.pools[provider.OpenAI].Slots().Len()})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// writeError renders the full OpenAI error envelope the Rust original emits:
// type, message, code and param are all always present. Dropping the outer
// fields breaks clients that read error.type or error.code.
func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	kind := "api_error"
	switch status {
	case http.StatusBadRequest:
		kind = "invalid_request_error"
	case http.StatusUnauthorized:
		kind = "authentication_error"
	case http.StatusForbidden:
		kind = "permission_error"
	case http.StatusNotFound:
		kind = "not_found_error"
	case http.StatusTooManyRequests:
		kind = "rate_limit_error"
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"type": kind, "message": msg, "code": nil, "param": nil,
	}})
}

// poolErrorStatus mirrors the status mapping in writePoolError so the usage
// row and the response always agree on the outcome of a failed dispatch.
func poolErrorStatus(err error) int {
	pe, ok := err.(*pool.PoolError)
	if !ok {
		return http.StatusInternalServerError
	}
	switch pe.Kind {
	case pool.ErrNoAccounts:
		return http.StatusServiceUnavailable
	case pool.ErrBadRequest:
		return http.StatusBadRequest
	case pool.ErrAllCooling:
		return http.StatusTooManyRequests
	case pool.ErrRefused:
		return http.StatusForbidden
	default:
		return http.StatusBadGateway
	}
}

// poolErrorKind mirrors pipeline::dispatch_failed's error_kind taxonomy.
func poolErrorKind(err error) string {
	pe, ok := err.(*pool.PoolError)
	if !ok {
		return "pool_upstream"
	}
	switch pe.Kind {
	case pool.ErrNoAccounts:
		return "pool_no_accounts"
	case pool.ErrAllCooling:
		return "pool_cooling_down"
	case pool.ErrBadRequest:
		return "pool_bad_request"
	case pool.ErrRefused:
		return "pool_refused"
	default:
		return "pool_upstream"
	}
}

func poolErrorAttempts(err error) int64 {
	if pe, ok := err.(*pool.PoolError); ok {
		return int64(pe.Attempts)
	}
	return 1
}

func writePoolError(w http.ResponseWriter, err error) {
	if pe, ok := err.(*pool.PoolError); ok {
		if pe.Kind == pool.ErrAllCooling && pe.RetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.FormatInt(pe.RetryAfter, 10))
		}
		if pe.Kind == pool.ErrRefused {
			pe.Message = "the " + pe.Provider.String() + " free tier refused every egress: " + pe.Message
		}
		writeError(w, poolErrorStatus(err), pe.Message)
		return
	}
	writeError(w, http.StatusInternalServerError, err.Error())
}

func copyHeaders(w http.ResponseWriter, h http.Header) {
	for k, vals := range h {
		lower := strings.ToLower(k)
		if lower == "content-length" || lower == "transfer-encoding" || lower == "connection" {
			continue
		}
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}
}

// copyRelayHeaders mirrors forwarded_response: only a whitelist of upstream
// headers crosses a verbatim relay, and the serving account's own quota
// headers never leak to the caller.
func copyRelayHeaders(w http.ResponseWriter, h http.Header) {
	for k, vals := range h {
		lower := strings.ToLower(k)
		if strings.HasPrefix(lower, "anthropic-ratelimit-") {
			continue
		}
		if lower == "content-type" || lower == "request-id" ||
			lower == "retry-after" || strings.HasPrefix(lower, "anthropic-") {
			for _, v := range vals {
				w.Header().Add(k, v)
			}
		}
	}
}

// relayRateLimitHeaders renders the pool's averaged quota windows into the
// headers Claude Code reads, so routing across accounts shows one steady
// figure instead of whichever account happened to serve the turn.
func relayRateLimitHeaders(windows []pool.UsageWindow) http.Header {
	out := http.Header{}
	var soonest int64
	for _, window := range windows {
		if window.Name == "" {
			continue
		}
		prefix := "anthropic-ratelimit-unified-" + window.Name
		out.Set(prefix+"-status", "allowed")
		out.Set(prefix+"-utilization", strconv.FormatFloat(window.Utilization, 'f', 2, 64))
		if window.ResetsAt > 0 {
			out.Set(prefix+"-reset", strconv.FormatInt(window.ResetsAt, 10))
			if soonest == 0 || window.ResetsAt < soonest {
				soonest = window.ResetsAt
			}
		}
	}
	if len(out) > 0 {
		out.Set("anthropic-ratelimit-unified-status", "allowed")
		if soonest > 0 {
			out.Set("anthropic-ratelimit-unified-reset", strconv.FormatInt(soonest, 10))
		}
	}
	return out
}

// responsesRateLimitHeaders mirrors rate_limit_headers in server/openai/mod.rs:
// the two shortest quota windows become x-codex-primary/secondary-* headers
// so a streamed Codex turn exposes pool-wide figures to /status readers.
func responsesRateLimitHeaders(windows []pool.UsageWindow) http.Header {
	out := http.Header{}
	sorted := append([]pool.UsageWindow(nil), windows...)
	const unknown = int64(1 << 62)
	sort.SliceStable(sorted, func(i, j int) bool {
		a, oka := pool.WindowSeconds(sorted[i].Name)
		b, okb := pool.WindowSeconds(sorted[j].Name)
		if !oka {
			a = unknown
		}
		if !okb {
			b = unknown
		}
		return a < b
	})
	for i, tier := range []string{"primary", "secondary"} {
		if i >= len(sorted) {
			break
		}
		span, ok := pool.WindowSeconds(sorted[i].Name)
		if !ok {
			continue
		}
		prefix := "x-codex-" + tier + "-"
		out.Set(prefix+"window-minutes", strconv.FormatInt(span/60, 10))
		out.Set(prefix+"used-percent", strconv.FormatInt(int64(math.Round(sorted[i].Utilization*100)), 10))
		if sorted[i].ResetsAt > 0 {
			out.Set(prefix+"reset-at", strconv.FormatInt(sorted[i].ResetsAt, 10))
		}
	}
	return out
}
