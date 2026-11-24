package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/edy/slop-proxy-go/internal/backend"
	"github.com/edy/slop-proxy-go/internal/config"
	"github.com/edy/slop-proxy-go/internal/handoff"
	"github.com/edy/slop-proxy-go/internal/oauth"
	"github.com/edy/slop-proxy-go/internal/provider"
	"github.com/edy/slop-proxy-go/internal/server"
	"github.com/edy/slop-proxy-go/internal/store"
)

// globalArgs are the flags a reload must forward to its successor so the new
// process opens the same config and database.
type globalArgs struct {
	config string
	db     string
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	verbose := flag.Bool("verbose", false, "enable debug logging")
	flag.BoolVar(verbose, "v", false, "enable debug logging")
	cfgPath := flag.String("config", "", "path to config.toml")
	dbPath := flag.String("db", "", "path to the sqlite database")
	flag.Parse()
	if *verbose {
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	}
	args := flag.Args()
	if len(args) == 0 {
		usage()
		return
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fatal(err)
	}
	if *dbPath != "" {
		cfg.DB = *dbPath
	}
	st, err := store.Open(cfg.DB)
	if err != nil {
		fatal(err)
	}
	defer st.Close()

	ga := globalArgs{config: *cfgPath, db: *dbPath}
	ctx := context.Background()
	switch args[0] {
	case "login":
		login(ctx, st, args[1:])
	case "accounts":
		accounts(ctx, st, args[1:])
	case "token":
		token(ctx, st, args[1:])
	case "serve":
		serveflags(ctx, cfg, st, args[1:])
	case "stats":
		stats(ctx, st, args[1:])
	case "models":
		models(ctx, cfg, st)
	case "handoff":
		runHandoff(ga, args[1:])
	case "help", "-h":
		usage()
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Println(`slop-proxy-go — Go account pool for Codex subscriptions

Usage:
  slop-proxy-go [--config PATH] [--db PATH] <command>

  slop-proxy-go login [--provider openai|anthropic|copilot|glm] [--label NAME]
  slop-proxy-go accounts list|add-key|trust|reserve|users|disable|enable|remove
  slop-proxy-go token create|list|revoke|limits|usage
  slop-proxy-go serve [--bind ADDR]
  slop-proxy-go handoff [--bind ADDR]   # ExecReload: ask the running server to spawn a successor
  slop-proxy-go stats [--since 24h] [--until RFC3339]
  slop-proxy-go models
  slop-proxy-go help`)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func login(ctx context.Context, st *store.Store, args []string) {
	fs := flag.NewFlagSet("login", flag.ExitOnError)
	p := fs.String("provider", "openai", "provider")
	label := fs.String("label", "", "account label")
	_ = fs.Parse(args)
	prov, ok := provider.Parse(*p)
	if !ok {
		fatal(fmt.Errorf("unknown provider %q", *p))
	}
	client := &http.Client{Timeout: 30 * time.Second}
	switch prov {
	case provider.OpenAI:
		loginOpenAI(ctx, st, client, *label)
	case provider.Anthropic:
		if err := oauth.AnthropicLogin(ctx, client, st, *label); err != nil {
			fatal(err)
		}
	case provider.Copilot:
		if err := oauth.CopilotLogin(ctx, client, st, *label); err != nil {
			fatal(err)
		}
	case provider.GLM:
		key, err := oauth.GLMLogin(ctx, client)
		if err != nil {
			fatal(err)
		}
		if err := addKeyAccount(ctx, st, client, prov, key, *label, "", false); err != nil {
			fatal(err)
		}
	default:
		fatal(fmt.Errorf("%s has no login flow, use `accounts add-key --provider %s`", prov, prov))
	}
}

func loginOpenAI(ctx context.Context, st *store.Store, client *http.Client, label string) {
	da, err := oauth.StartDeviceAuth(ctx, client)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("To authorize, open this URL on any device:\n\n    %s\n\nand enter this code:\n\n    %s\n\n",
		da.VerifyURL, da.UserCode)
	fmt.Println("Waiting for authorization (up to 15 minutes)...")
	tokens, err := oauth.PollDeviceAuth(ctx, client, da)
	if err != nil {
		fatal(err)
	}
	email := tokens.Email
	if email == "" {
		email = tokens.AccountID
	}
	accountID := tokens.AccountID
	if accountID == "" {
		accountID = email
	}
	if accountID == "" {
		fatal(fmt.Errorf("id_token has no chatgpt account id"))
	}
	acc := store.Account{
		Provider: provider.OpenAI.String(), Label: label, Email: email,
		ProviderAccountID: accountID, AuthMode: "oauth",
		AccessToken: tokens.AccessToken, RefreshToken: tokens.RefreshToken,
		AccessExpiresAt: tokens.ExpiresAt, Status: "active", Plan: tokens.Plan,
	}
	if err := st.InsertAccount(ctx, &acc); err != nil {
		fatal(err)
	}
	if email == "" {
		email = "unknown email"
	}
	planText := ""
	if tokens.Plan != "" {
		planText = ", plan " + tokens.Plan
	}
	fmt.Printf("logged in: openai account %d (%s%s)\n", acc.ID, email, planText)
}

// addKeyAccount mirrors Rust's cli::accounts_add_key, including the SHA-256
// prefix id and Copilot's github_login lookup.
func addKeyAccount(ctx context.Context, st *store.Store, client *http.Client, prov provider.Provider,
	key, label, referer string, egress bool) error {
	if referer != "" && prov != provider.Gemini {
		return fmt.Errorf("--referer is only supported for gemini keys")
	}
	var accountID, email, refresh, authMode, token string
	if prov == provider.Copilot {
		token = strings.TrimSpace(key)
		login, err := oauth.GithubLogin(ctx, client, token)
		if err != nil {
			return err
		}
		accountID, email, refresh, authMode = login, login, token, "oauth"
	} else {
		sum := sha256.Sum256([]byte(key))
		accountID = hex.EncodeToString(sum[:8])
		authMode, token = "api_key", key
	}
	if label == "" {
		label = prov.String() + "-key"
	}
	acc := &store.Account{
		Provider: prov.String(), Label: label, Email: email,
		ProviderAccountID: accountID, AuthMode: authMode, Status: "active",
		AccessToken: token, RefreshToken: refresh,
		HTTPReferer: referer, Egress: egress,
	}
	if err := st.InsertAccount(ctx, acc); err != nil {
		return err
	}
	fmt.Printf("stored %s account %d (%s)\n", prov, acc.ID, accountID)
	return nil
}

// reorderFlags moves options ahead of positional arguments so Go's flag
// package accepts the same "accounts trust 1 --off" syntax clap accepts in
// the Rust original. Go's flag.Parse stops at the first non-flag argument.
func reorderFlags(args []string, boolFlags map[string]bool) []string {
	var opts, positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "-") || a == "-" {
			positional = append(positional, a)
			continue
		}
		opts = append(opts, a)
		if strings.Contains(a, "=") {
			continue
		}
		if boolFlags[a] {
			continue
		}
		if i+1 < len(args) {
			opts = append(opts, args[i+1])
			i++
		}
	}
	return append(opts, positional...)
}

var offIsBool = map[string]bool{"--off": true, "-off": true}

func accounts(ctx context.Context, st *store.Store, args []string) {
	if len(args) == 0 {
		fatal(fmt.Errorf("accounts needs a subcommand"))
	}
	sub := args[0]
	switch sub {
	case "list":
		list, err := st.ListAccounts(ctx, "")
		if err != nil {
			fatal(err)
		}
		for _, a := range list {
			fmt.Printf("#%d  %-10s %-24s %-10s plan=%s trusted=%v reserved=%v\n",
				a.ID, a.Provider, a.Label, a.Status, a.Plan, a.Trusted, a.Reserved)
		}
	case "import-codex":
		importCodex(ctx, st, args[1:])
	case "add-key":
		fs := flag.NewFlagSet("add-key", flag.ExitOnError)
		p := fs.String("provider", "", "provider")
		key := fs.String("key", "", "API key")
		label := fs.String("label", "", "label")
		referer := fs.String("referer", "", "HTTP referer for this account")
		egress := fs.Bool("egress", false, "send this account through egress proxies")
		_ = fs.Parse(args[1:])
		prov, ok := provider.Parse(*p)
		if !ok || *key == "" {
			fatal(fmt.Errorf("--provider and --key required"))
		}
		if err := addKeyAccount(ctx, st, &http.Client{Timeout: 30 * time.Second},
			prov, *key, *label, *referer, *egress); err != nil {
			fatal(err)
		}
	case "trust", "reserve", "egress":
		fs := flag.NewFlagSet(sub, flag.ExitOnError)
		off := fs.Bool("off", false, "clear flag")
		_ = fs.Parse(reorderFlags(args[1:], offIsBool))
		if fs.NArg() == 0 {
			fatal(fmt.Errorf("account id or email required"))
		}
		acc, err := st.FindAccount(ctx, fs.Arg(0))
		if err != nil {
			fatal(err)
		}
		field := "trusted"
		if sub == "reserve" {
			field = "reserved"
		} else if sub == "egress" {
			field = "egress"
		}
		value := 1
		if *off {
			value = 0
		}
		if err := st.SetAccountField(ctx, acc.ID, field, value); err != nil {
			fatal(err)
		}
		fmt.Printf("ok (%s=%d)\n", field, value)
	case "users":
		fs := flag.NewFlagSet("users", flag.ExitOnError)
		allow := fs.String("allow", "", "comma-separated users")
		_ = fs.Parse(reorderFlags(args[1:], nil))
		if fs.NArg() == 0 {
			fatal(fmt.Errorf("account required"))
		}
		acc, err := st.FindAccount(ctx, fs.Arg(0))
		if err != nil {
			fatal(err)
		}
		if err := st.SetAccountField(ctx, acc.ID, "allowed_users", *allow); err != nil {
			fatal(err)
		}
		fmt.Println("ok")
	case "disable":
		fs := flag.NewFlagSet("disable", flag.ExitOnError)
		reason := fs.String("reason", "", "reason")
		_ = fs.Parse(reorderFlags(args[1:], nil))
		if fs.NArg() == 0 {
			fatal(fmt.Errorf("account required"))
		}
		acc, err := st.FindAccount(ctx, fs.Arg(0))
		if err != nil {
			fatal(err)
		}
		if err := st.SetAccountStatus(ctx, acc.ID, "disabled", 0, *reason); err != nil {
			fatal(err)
		}
		fmt.Println("disabled")
	case "enable":
		if len(args) < 2 {
			fatal(fmt.Errorf("account required"))
		}
		acc, err := st.FindAccount(ctx, args[1])
		if err != nil {
			fatal(err)
		}
		if err := st.SetAccountStatus(ctx, acc.ID, "active", 0, ""); err != nil {
			fatal(err)
		}
		fmt.Println("enabled")
	case "remove":
		if len(args) < 2 {
			fatal(fmt.Errorf("account required"))
		}
		acc, err := st.FindAccount(ctx, args[1])
		if err != nil {
			fatal(err)
		}
		if err := st.RemoveAccount(ctx, acc.ID); err != nil {
			fatal(err)
		}
		fmt.Println("removed")
	default:
		fatal(fmt.Errorf("unknown accounts command %q", sub))
	}
}

// bindLimits wires the original proxy's full LimitArgs flag set, shared by
// token create and token limits.
func bindLimits(fs *flag.FlagSet, ctx context.Context, st *store.Store) func() (store.TokenLimits, error) {
	requests := fs.Int64("requests", 0, "request limit")
	tokens := fs.Int64("tokens", 0, "token limit")
	window := fs.Int64("window-seconds", 3600, "rolling window")
	slow := fs.Int64("slowdown-ms", 0, "delay per admitted request")
	prefer := fs.Bool("prefer-trusted", false, "prefer trusted accounts")
	reserved := fs.Bool("reserved-only", false, "reserved accounts only")
	providers := fs.String("providers", "", "comma-separated allowed providers")
	metered := fs.String("metered", "", "comma-separated metered providers")
	pinned := fs.String("pin-account", "", "account ID, email or label to pin")
	return func() (store.TokenLimits, error) {
		if *requests < 0 || *tokens < 0 || *window < 1 || *slow < 0 {
			return store.TokenLimits{}, fmt.Errorf("invalid request, token, window or delay limit")
		}
		limits := store.TokenLimits{
			Requests: *requests, Tokens: *tokens, WindowSecs: *window,
			SlowdownMS: *slow, PreferTrusted: *prefer, ReservedOnly: *reserved,
		}
		var err error
		if limits.Providers, err = parseProviderList(*providers); err != nil {
			return store.TokenLimits{}, err
		}
		if limits.Metered, err = parseProviderList(*metered); err != nil {
			return store.TokenLimits{}, err
		}
		if *pinned != "" {
			account, err := st.FindAccount(ctx, *pinned)
			if err != nil {
				return store.TokenLimits{}, fmt.Errorf("pin account %q: %w", *pinned, err)
			}
			limits.PinnedAccount = &account.ID
		}
		return limits, nil
	}
}

func parseProviderList(raw string) ([]string, error) {
	if raw == "" {
		return nil, nil
	}
	var providers []string
	for _, name := range strings.Split(raw, ",") {
		if p, ok := provider.Parse(strings.TrimSpace(name)); ok {
			providers = append(providers, p.String())
		} else {
			return nil, fmt.Errorf("unknown provider %q", name)
		}
	}
	return providers, nil
}

func token(ctx context.Context, st *store.Store, args []string) {
	if len(args) == 0 {
		fatal(fmt.Errorf("token needs a subcommand"))
	}
	switch args[0] {
	case "create":
		fs := flag.NewFlagSet("create", flag.ExitOnError)
		user := fs.String("user", "", "user")
		limitsOf := bindLimits(fs, ctx, st)
		_ = fs.Parse(args[1:])
		if *user == "" {
			fatal(fmt.Errorf("--user required"))
		}
		key, _ := store.GenerateToken()
		limits, err := limitsOf()
		if err != nil {
			fatal(err)
		}
		t, err := st.CreateToken(ctx, key, *user, limits)
		if err != nil {
			fatal(err)
		}
		fmt.Printf("#%d %s\n", t.ID, key)
	case "list":
		list, err := st.ListTokens(ctx)
		if err != nil {
			fatal(err)
		}
		for _, t := range list {
			fmt.Printf("#%d user=%s requests=%d tokens=%d window=%ds\n", t.ID, t.User, t.Requests, t.Tokens, t.WindowSecs)
		}
	case "revoke":
		if len(args) < 2 {
			fatal(fmt.Errorf("token required"))
		}
		if err := st.RevokeToken(ctx, args[1]); err != nil {
			fatal(err)
		}
		fmt.Println("revoked")
	case "limits":
		fs := flag.NewFlagSet("limits", flag.ExitOnError)
		limitsOf := bindLimits(fs, ctx, st)
		_ = fs.Parse(reorderFlags(args[1:], map[string]bool{"prefer-trusted": true, "reserved-only": true}))
		if fs.NArg() != 1 {
			fatal(fmt.Errorf("token ID or prefix required"))
		}
		tok, err := st.FindToken(ctx, fs.Arg(0))
		if err != nil {
			fatal(err)
		}
		limits, err := limitsOf()
		if err != nil {
			fatal(err)
		}
		if err := st.SetTokenLimits(ctx, tok.ID, limits); err != nil {
			fatal(err)
		}
		fmt.Println("ok")
	case "usage":
		if len(args) < 2 {
			fatal(fmt.Errorf("token id required"))
		}
		tok, err := st.FindToken(ctx, args[1])
		if err != nil {
			fatal(err)
		}
		reqs, input, output, err := st.TokenUsage(ctx, tok.ID, tok.WindowSecs)
		if err != nil {
			fatal(err)
		}
		fmt.Printf("requests=%d input=%d output=%d (last hour)\n", reqs, input, output)
	default:
		fatal(fmt.Errorf("unknown token command %q", args[0]))
	}
}

// serve binds the API (and optional metrics) listener, adopting inherited
// sockets from a predecessor when present, then runs until signalled. This
// mirrors Rust's server::serve: a control socket accepts exactly one
// successor, and SIGTERM starts a drain that keeps in-flight streams alive
// for up to 15 minutes before dropping them.
func serve(ctx context.Context, cfg config.Config, st *store.Store) {
	inherited := handoff.Take()
	apiLn, err := inherited.Listen(handoff.SocketAPI, cfg.Bind)
	if err != nil {
		fatal(err)
	}
	var metricsLn net.Listener
	if cfg.MetricsBind != "" {
		metricsLn, err = inherited.Listen(handoff.SocketMetrics, cfg.MetricsBind)
		if err != nil {
			fatal(err)
		}
	}
	inherited.CloseRemaining()

	sockets := []handoff.NamedListener{{Socket: handoff.SocketAPI, Listener: apiLn}}
	if metricsLn != nil {
		sockets = append(sockets, handoff.NamedListener{Socket: handoff.SocketMetrics, Listener: metricsLn})
	}
	drain := make(chan struct{})
	control, err := handoff.Accept(sockets, drain)
	if err != nil {
		// Without a usable control socket the server still runs normally; a
		// reload just cannot hand its listeners over.
		slog.Warn("handoff control socket unavailable", "error", err)
	} else {
		defer control.Close()
	}

	srv := server.NewServer(cfg, st, oauth.NewCodexRefresher())
	errCh := make(chan error, 1)
	go func() {
		if metricsLn != nil {
			errCh <- srv.ServeOnWithMetrics(apiLn, metricsLn)
		} else {
			errCh <- srv.ServeOn(apiLn)
		}
	}()

	// systemd MAINPID/READY=1 retargets the unit and makes it SIGTERM the
	// predecessor; RelievePredecessor is the belt-and-braces local SIGTERM.
	handoff.NotifyReady()
	handoff.RelievePredecessor()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	select {
	case sig := <-sigCh:
		slog.Info("draining open connections", "signal", sig, "limit", handoff.DrainLimit)
		// Wakes a pending handoff so its successor gets the "ok" reply before
		// we stop accepting, and mirrors Rust's drain watch channel.
		close(drain)
		shutdownCtx, cancel := context.WithTimeout(ctx, handoff.DrainLimit)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Warn("drain deadline reached, dropping open connections", "error", err)
			_ = srv.Close()
		}
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			fatal(err)
		}
	}
}

func serveflags(ctx context.Context, cfg config.Config, st *store.Store, args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	bind := fs.String("bind", "", "listen address")
	_ = fs.Parse(args)
	if *bind != "" {
		cfg.Bind = *bind
	}
	serve(ctx, cfg, st)
}

// runHandoff is the ExecReload client. It asks the running server, over its
// Unix control socket, to spawn this binary as its successor on the same
// sockets. Only --db/--config/--bind are forwarded, matching the Rust CLI.
func runHandoff(ga globalArgs, args []string) {
	fs := flag.NewFlagSet("handoff", flag.ExitOnError)
	bind := fs.String("bind", "", "listen address when no socket is inherited")
	_ = fs.Parse(args)
	exe, err := os.Executable()
	if err != nil {
		fatal(err)
	}
	argv := []string{exe}
	if ga.config != "" {
		argv = append(argv, "--config", ga.config)
	}
	if ga.db != "" {
		argv = append(argv, "--db", ga.db)
	}
	argv = append(argv, "serve")
	if *bind != "" {
		argv = append(argv, "--bind", *bind)
	}
	if err := handoff.Request(argv); err != nil {
		fatal(err)
	}
	fmt.Println("ok")
}

func models(ctx context.Context, cfg config.Config, st *store.Store) {
	accounts, err := st.ListAccounts(ctx, provider.OpenAI.String())
	if err != nil {
		fatal(err)
	}
	client := backend.NewCodex(cfg.Codex)
	for _, account := range accounts {
		if account.AccessToken == "" {
			continue
		}
		snap, se := client.Catalog(ctx, account.AccessToken, account.ProviderAccountID)
		if se != nil {
			continue
		}
		data := make([]any, 0, len(snap.Models))
		for _, id := range snap.Models {
			data = append(data, map[string]any{"id": id, "object": "model", "owned_by": "openai"})
		}
		b, _ := json.MarshalIndent(map[string]any{"object": "list", "data": data}, "", "  ")
		fmt.Println(string(b))
		return
	}
	fmt.Println(`{"object":"list","data":[]}`)
}

// parseWindow resolves a relative duration (30m/24h/7d/2w) or RFC3339
// timestamp to a unix second, matching the Rust stats::parse_time semantics.
func parseWindow(s string, now int64) (int64, error) {
	if s == "" {
		return 0, nil
	}
	if len(s) > 1 {
		unit := s[len(s)-1]
		if value, err := strconv.ParseInt(s[:len(s)-1], 10, 64); err == nil {
			var mult int64
			switch unit {
			case 'm':
				mult = 60
			case 'h':
				mult = 3600
			case 'd':
				mult = 86400
			case 'w':
				mult = 7 * 86400
			}
			if mult > 0 {
				return now - value*mult, nil
			}
		}
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return 0, fmt.Errorf("cannot parse time %q; use RFC3339 or 30m/24h/7d/2w", s)
	}
	return t.Unix(), nil
}

func stamp(ts int64) string {
	return time.Unix(ts, 0).UTC().Format(time.RFC3339)
}

// statsReport mirrors the Rust stats::Report JSON shape.
type statsReport struct {
	Since     string           `json:"since"`
	Until     string           `json:"until"`
	Totals    store.UsageAgg   `json:"totals"`
	ByUser    []store.UsageAgg `json:"by_user"`
	ByAccount []store.UsageAgg `json:"by_account"`
	ByModel   []store.UsageAgg `json:"by_model"`
}

func stats(ctx context.Context, st *store.Store, args []string) {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	since := fs.String("since", "", "window start: 24h, 7d, 30m, 2w or RFC3339")
	until := fs.String("until", "", "window end (RFC3339)")
	_ = fs.Parse(args)
	now := time.Now().Unix()
	sinceTS, err := parseWindow(*since, now)
	if err != nil {
		fatal(err)
	}
	untilTS, err := parseWindow(*until, now)
	if err != nil {
		fatal(err)
	}
	if *until == "" {
		untilTS = now + 1
	}
	totals, err := st.UsageTotals(ctx, sinceTS, untilTS)
	if err != nil {
		fatal(err)
	}
	byUser, err := st.UsageBy(ctx, store.DimUser, sinceTS, untilTS)
	if err != nil {
		fatal(err)
	}
	byAccount, err := st.UsageBy(ctx, store.DimAccount, sinceTS, untilTS)
	if err != nil {
		fatal(err)
	}
	byModel, err := st.UsageBy(ctx, store.DimModel, sinceTS, untilTS)
	if err != nil {
		fatal(err)
	}
	report := statsReport{
		Since: stamp(sinceTS), Until: stamp(untilTS), Totals: totals,
		ByUser: byUser, ByAccount: byAccount, ByModel: byModel,
	}
	b, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(b))
}

func decodeJWTPayload(token string) map[string]any {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil
	}
	var claims map[string]any
	if json.Unmarshal(payload, &claims) != nil {
		return nil
	}
	return claims
}

func importCodex(ctx context.Context, st *store.Store, args []string) {
	fs := flag.NewFlagSet("import-codex", flag.ExitOnError)
	label := fs.String("label", "", "account label")
	_ = fs.Parse(args)
	if fs.NArg() == 0 {
		fatal(fmt.Errorf("auth.json path required, use - for stdin"))
	}
	var body []byte
	var err error
	if fs.Arg(0) == "-" {
		body, err = io.ReadAll(os.Stdin)
	} else {
		body, err = os.ReadFile(fs.Arg(0))
	}
	if err != nil {
		fatal(err)
	}
	var doc struct {
		Tokens struct {
			IDToken      string `json:"id_token"`
			AccessToken  string `json:"access_token"`
			RefreshToken string `json:"refresh_token"`
		} `json:"tokens"`
	}
	if json.Unmarshal(body, &doc) != nil {
		fatal(fmt.Errorf("invalid auth.json"))
	}
	claims := decodeJWTPayload(doc.Tokens.IDToken)
	email, _ := claims["email"].(string)
	accountID := ""
	if auth, ok := claims["https://api.openai.com/auth"].(map[string]any); ok {
		accountID, _ = auth["chatgpt_account_id"].(string)
	}
	if email == "" {
		email = accountID
	}
	if email == "" {
		email = "imported-codex"
	}
	if accountID == "" {
		accountID = email
	}
	l := *label
	if l == "" {
		l = email
	}
	acc := store.Account{
		Provider: provider.OpenAI.String(), Label: l, Email: email,
		ProviderAccountID: accountID, AuthMode: "oauth",
		AccessToken: doc.Tokens.AccessToken, RefreshToken: doc.Tokens.RefreshToken,
		Status: "active",
	}
	if err := st.InsertAccount(ctx, &acc); err != nil {
		fatal(err)
	}
	fmt.Printf("Imported account #%d (%s)\n", acc.ID, l)
}
