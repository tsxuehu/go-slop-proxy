package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/edy/slop-proxy-go/internal/provider"

	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

type Account struct {
	ID                int64
	Provider          string
	ProviderAccountID string
	Trusted           bool
	Reserved          bool
	Egress            bool
	AuthMode          string // oauth / apikey
	Email             string
	Label             string
	Plan              string
	HTTPReferer       string
	TurnState         string
	AccessToken       string
	RefreshToken      string
	AccessExpiresAt   int64
	LastRefreshAt     int64
	Status            string // active / cooldown / disabled
	CooldownUntil     int64
	DisabledReason    string
	AllowedUsers      string
	CreatedAt         int64
	UpdatedAt         int64
}

type Token struct {
	ID               int64
	User             string
	KeyHash          []byte
	KeyPrefix        string
	Requests         int64 // 0 = unlimited
	Tokens           int64
	WindowSecs       int64
	SlowdownMS       int64
	PreferTrusted    bool
	ReservedOnly     bool
	PinnedAccount    *int64
	AllowedProviders string
	MeteredProviders string
	CreatedAt        int64
	RevokedAt        int64
}

type TokenLimits struct {
	Requests      int64
	Tokens        int64
	WindowSecs    int64
	SlowdownMS    int64
	PreferTrusted bool
	ReservedOnly  bool
	PinnedAccount *int64
	Providers     []string
	Metered       []string
}

// Admission is the persisted decision that lets a request through. The
// / headers in the original proxy are derived from these exact fields.
type Admission struct {
	MeterID           int64
	RequestLimit      *int64
	RequestsRemaining *int64
	TokenLimit        *int64
	TokensRemaining   *int64
	ResetAfter        int64
	SlowdownMS        int64
	TokensSpent       *int64
}

// AdmissionError carries the provider-specific retry delay of a rejection.
type AdmissionError struct {
	Kind       string
	RetryAfter int64
}

func (e *AdmissionError) Error() string { return e.Kind }

// MayUse reports whether this token is allowed to reach the provider.
func (t Token) MayUse(p provider.Provider) bool {
	if t.AllowedProviders == "" {
		return true
	}
	for _, name := range decodeProviders(t.AllowedProviders) {
		if name == p.String() {
			return true
		}
	}
	return false
}

// Meters reports whether traffic to the provider spends this token's quota.
func (t Token) Meters(p provider.Provider) bool {
	if t.MeteredProviders == "" {
		return true
	}
	for _, name := range decodeProviders(t.MeteredProviders) {
		if name == p.String() {
			return true
		}
	}
	return false
}

var ErrNotFound = errors.New("not found")

func Open(path string) (*Store, error) {
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(`PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;`); err != nil {
		return nil, err
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	schema := `
CREATE TABLE IF NOT EXISTS accounts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider TEXT NOT NULL,
  provider_account_id TEXT NOT NULL DEFAULT '',
  trusted INTEGER NOT NULL DEFAULT 0,
  reserved INTEGER NOT NULL DEFAULT 0,
  egress INTEGER NOT NULL DEFAULT 0,
  auth_mode TEXT NOT NULL DEFAULT 'oauth',
  email TEXT NOT NULL DEFAULT '',
  label TEXT NOT NULL DEFAULT '',
  plan_type TEXT NOT NULL DEFAULT '',
  access_token TEXT NOT NULL DEFAULT '',
  refresh_token TEXT NOT NULL DEFAULT '',
  http_referer TEXT NOT NULL DEFAULT '',
  turn_state TEXT NOT NULL DEFAULT '',
  access_expires_at INTEGER NOT NULL DEFAULT 0,
  last_refresh_at INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'active',
  cooldown_until INTEGER NOT NULL DEFAULT 0,
  disabled_reason TEXT NOT NULL DEFAULT '',
  allowed_users TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL DEFAULT (unixepoch()),
  updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
  UNIQUE (provider, provider_account_id)
);
CREATE TABLE IF NOT EXISTS api_tokens (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user TEXT NOT NULL,
  token_hash BLOB NOT NULL UNIQUE,
  token_prefix TEXT NOT NULL DEFAULT '',
  request_limit INTEGER NOT NULL DEFAULT 0,
  token_limit INTEGER NOT NULL DEFAULT 0,
  window_seconds INTEGER NOT NULL DEFAULT 3600,
  slowdown_ms INTEGER NOT NULL DEFAULT 0,
  prefer_trusted INTEGER NOT NULL DEFAULT 0,
  reserved_only INTEGER NOT NULL DEFAULT 0,
  allowed_providers TEXT NOT NULL DEFAULT '',
  metered_providers TEXT NOT NULL DEFAULT '',
  pinned_account INTEGER,
  created_at INTEGER NOT NULL DEFAULT (unixepoch()),
  revoked_at INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS api_meter (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  token_id INTEGER NOT NULL REFERENCES api_tokens(id),
  ts_ms INTEGER NOT NULL,
  input_tokens INTEGER NOT NULL DEFAULT 0,
  output_tokens INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_api_meter_token_ts ON api_meter(token_id, ts_ms);
CREATE TABLE IF NOT EXISTS usage_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts INTEGER NOT NULL DEFAULT (unixepoch()),
  token_id INTEGER REFERENCES api_tokens(id),
  user TEXT NOT NULL DEFAULT '',
  account_id INTEGER REFERENCES accounts(id),
  provider TEXT NOT NULL DEFAULT '',
  dialect TEXT NOT NULL DEFAULT '',
  requested_model TEXT NOT NULL DEFAULT '',
  upstream_model TEXT NOT NULL DEFAULT '',
  effort TEXT NOT NULL DEFAULT '',
  service_tier TEXT NOT NULL DEFAULT '',
  input_tokens INTEGER NOT NULL DEFAULT 0,
  output_tokens INTEGER NOT NULL DEFAULT 0,
  cache_read_tokens INTEGER NOT NULL DEFAULT 0,
  cache_write_tokens INTEGER NOT NULL DEFAULT 0,
  reasoning_tokens INTEGER NOT NULL DEFAULT 0,
  cost_usd REAL NOT NULL DEFAULT 0,
  list_cost_usd REAL NOT NULL DEFAULT 0,
  status INTEGER NOT NULL DEFAULT 0,
  error_kind TEXT NOT NULL DEFAULT '',
  duration_ms INTEGER NOT NULL DEFAULT 0,
  session_key TEXT NOT NULL DEFAULT '',
  turn_index INTEGER NOT NULL DEFAULT 0,
  tools_declared INTEGER NOT NULL DEFAULT 0,
  tools_called TEXT NOT NULL DEFAULT '',
  thinking_budget INTEGER NOT NULL DEFAULT 0,
  image_count INTEGER NOT NULL DEFAULT 0,
  request_bytes INTEGER NOT NULL DEFAULT 0,
  response_bytes INTEGER NOT NULL DEFAULT 0,
  ttft_ms INTEGER NOT NULL DEFAULT 0,
  stop_reason TEXT NOT NULL DEFAULT '',
  attempts INTEGER NOT NULL DEFAULT 0,
  turn_state_blocks INTEGER,
  cache_ttl_secs INTEGER
);
CREATE INDEX IF NOT EXISTS idx_usage_ts ON usage_log(ts);
CREATE INDEX IF NOT EXISTS idx_usage_user_ts ON usage_log(user, ts);
CREATE INDEX IF NOT EXISTS idx_usage_account_ts ON usage_log(account_id, ts);
CREATE INDEX IF NOT EXISTS idx_usage_session_ts ON usage_log(session_key, ts);

CREATE TABLE IF NOT EXISTS account_usage (
  account_id INTEGER NOT NULL,
  window TEXT NOT NULL,
  utilization REAL NOT NULL DEFAULT 0,
  resets_at INTEGER NOT NULL DEFAULT 0,
  observed_at INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (account_id, window)
);
`
	for _, stmt := range strings.Split(schema, ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("migrate %q: %w", stmt[:min(len(stmt), 60)], err)
		}
	}
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// hashKey stores the same 32-byte digest as the Rust original, so a database
// written by either implementation can be read by the other.
func hashKey(key string) []byte {
	sum := sha256.Sum256([]byte(key))
	return sum[:]
}

func GenerateToken() (string, string) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic("OS random source unavailable: " + err.Error())
	}
	raw := "sp-" + base64.RawURLEncoding.EncodeToString(b)
	if len(raw) > 12 {
		return raw, raw[:12]
	}
	return raw, raw
}

const accountCols = `id, provider, provider_account_id, trusted, reserved, egress,
	auth_mode, email, label, plan_type, access_token, refresh_token, http_referer,
	turn_state, access_expires_at, last_refresh_at, status, cooldown_until,
	disabled_reason, allowed_users, created_at, updated_at`

func scanAccount(row interface{ Scan(...any) error }) (Account, error) {
	var a Account
	var trusted, reserved, egress int
	err := row.Scan(&a.ID, &a.Provider, &a.ProviderAccountID, &trusted, &reserved, &egress,
		&a.AuthMode, &a.Email, &a.Label, &a.Plan, &a.AccessToken, &a.RefreshToken,
		&a.HTTPReferer, &a.TurnState, &a.AccessExpiresAt, &a.LastRefreshAt,
		&a.Status, &a.CooldownUntil, &a.DisabledReason, &a.AllowedUsers,
		&a.CreatedAt, &a.UpdatedAt)
	a.Trusted = trusted != 0
	a.Reserved = reserved != 0
	a.Egress = egress != 0
	return a, err
}

func (s *Store) ListAccounts(ctx context.Context, provider string) ([]Account, error) {
	q := `SELECT ` + accountCols + ` FROM accounts`
	var args []any
	if provider != "" {
		q += ` WHERE provider = ?`
		args = append(args, provider)
	}
	q += ` ORDER BY id`
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Account
	for rows.Next() {
		a, err := scanAccount(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) FindAccount(ctx context.Context, key string) (Account, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+accountCols+` FROM accounts
		WHERE CAST(id AS TEXT) = ? OR email = ? OR provider_account_id = ? LIMIT 1`, key, key, key)
	a, err := scanAccount(row)
	if errors.Is(err, sql.ErrNoRows) {
		return a, ErrNotFound
	}
	return a, err
}

func (s *Store) UpsertAccount(ctx context.Context, a *Account) error {
	if a.AuthMode == "" {
		a.AuthMode = "oauth"
	}
	if a.Status == "" {
		a.Status = "active"
	}
	now := time.Now().Unix()
	b := func(v bool) int {
		if v {
			return 1
		}
		return 0
	}
	res, err := s.db.ExecContext(ctx, `INSERT INTO accounts
		(provider,provider_account_id,trusted,reserved,egress,auth_mode,email,label,plan_type,
		 access_token,refresh_token,http_referer,turn_state,access_expires_at,last_refresh_at,
		 status,cooldown_until,disabled_reason,allowed_users,created_at,updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(provider, provider_account_id) DO UPDATE SET
		 trusted=excluded.trusted, reserved=excluded.reserved, egress=excluded.egress,
		 auth_mode=excluded.auth_mode, email=excluded.email, label=excluded.label,
		 plan_type=excluded.plan_type, access_token=excluded.access_token,
		 refresh_token=excluded.refresh_token, http_referer=excluded.http_referer,
		 turn_state=excluded.turn_state, access_expires_at=excluded.access_expires_at,
		 last_refresh_at=excluded.last_refresh_at, status=excluded.status,
		 cooldown_until=excluded.cooldown_until, disabled_reason=excluded.disabled_reason,
		 allowed_users=excluded.allowed_users, updated_at=excluded.updated_at`,
		a.Provider, a.ProviderAccountID, b(a.Trusted), b(a.Reserved), b(a.Egress), a.AuthMode,
		a.Email, a.Label, a.Plan, a.AccessToken, a.RefreshToken, a.HTTPReferer, a.TurnState,
		a.AccessExpiresAt, a.LastRefreshAt, a.Status, a.CooldownUntil, a.DisabledReason,
		a.AllowedUsers, now, now)
	if err != nil {
		return err
	}
	a.ID, err = res.LastInsertId()
	if a.ID == 0 {
		found, ferr := s.FindAccount(ctx, a.ProviderAccountID)
		if ferr == nil {
			a.ID = found.ID
		}
	}
	return err
}

func (s *Store) InsertAccount(ctx context.Context, a *Account) error {
	return s.UpsertAccount(ctx, a)
}

func (s *Store) RemoveAccount(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM accounts WHERE id = ?`, id)
	return err
}

func (s *Store) UpdateAccountTokens(ctx context.Context, id int64, access, refresh string, expires int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE accounts SET access_token=?, refresh_token=?,
		access_expires_at=?, last_refresh_at=?, updated_at=? WHERE id=?`,
		access, refresh, expires, time.Now().Unix(), time.Now().Unix(), id)
	return err
}

func (s *Store) SetAccountStatus(ctx context.Context, id int64, status string, cooldownUntil int64, reason string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE accounts SET status=?, cooldown_until=?, disabled_reason=?, updated_at=? WHERE id=?`,
		status, cooldownUntil, reason, time.Now().Unix(), id)
	return err
}

func (s *Store) SetAccountCooldown(ctx context.Context, id, until int64) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE accounts SET status='cooldown', cooldown_until=?, updated_at=?
		WHERE id=? AND (status != 'cooldown' OR cooldown_until < ?)`, until, time.Now().Unix(), id, until)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (s *Store) ClearAccountCooldown(ctx context.Context, id, until int64) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE accounts SET status='active', cooldown_until=0, updated_at=?
		WHERE id=? AND status='cooldown' AND cooldown_until=?`, time.Now().Unix(), id, until)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (s *Store) SetAccountField(ctx context.Context, id int64, field string, value any) error {
	allowed := map[string]bool{
		"plan_type": true, "turn_state": true, "trusted": true, "reserved": true,
		"egress": true, "allowed_users": true, "label": true, "http_referer": true,
	}
	if !allowed[field] {
		return fmt.Errorf("field %q not updatable", field)
	}
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`UPDATE accounts SET %s=?, updated_at=? WHERE id=?`, field), value, time.Now().Unix(), id)
	return err
}

type UsageWindow struct {
	Name        string
	Utilization float64
	ResetsAt    int64
}

func (s *Store) SetUsageWindows(ctx context.Context, accountID int64, windows []UsageWindow) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, w := range windows {
		_, err := tx.ExecContext(ctx, `INSERT INTO account_usage (account_id,window,utilization,resets_at,observed_at)
			VALUES (?,?,?,?,?) ON CONFLICT(account_id,window) DO UPDATE SET
			utilization=excluded.utilization, resets_at=excluded.resets_at, observed_at=excluded.observed_at`,
			accountID, w.Name, w.Utilization, w.ResetsAt, time.Now().Unix())
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) UsageWindows(ctx context.Context, accountID int64) ([]UsageWindow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT window,utilization,resets_at FROM account_usage WHERE account_id=?`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []UsageWindow
	for rows.Next() {
		var w UsageWindow
		if err := rows.Scan(&w.Name, &w.Utilization, &w.ResetsAt); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

func encodeProviders(p []string) string { return strings.Join(p, ",") }
func decodeProviders(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}

func (s *Store) CreateToken(ctx context.Context, key, user string, limits TokenLimits) (Token, error) {
	if limits.WindowSecs <= 0 {
		limits.WindowSecs = 3600
	}
	t := Token{
		User: user, KeyHash: hashKey(key), KeyPrefix: prefixOf(key),
		Requests: limits.Requests, Tokens: limits.Tokens, WindowSecs: limits.WindowSecs,
		SlowdownMS: limits.SlowdownMS, PreferTrusted: limits.PreferTrusted,
		ReservedOnly: limits.ReservedOnly, PinnedAccount: limits.PinnedAccount,
		AllowedProviders: encodeProviders(limits.Providers),
		MeteredProviders: encodeProviders(limits.Metered),
		CreatedAt:        time.Now().Unix(),
	}
	var pinned any
	if limits.PinnedAccount != nil {
		pinned = *limits.PinnedAccount
	}
	res, err := s.db.ExecContext(ctx, `INSERT INTO api_tokens
		(user,token_hash,token_prefix,request_limit,token_limit,window_seconds,slowdown_ms,
		 prefer_trusted,reserved_only,pinned_account,allowed_providers,metered_providers,created_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		user, t.KeyHash, t.KeyPrefix, t.Requests, t.Tokens, t.WindowSecs, t.SlowdownMS,
		boolInt(limits.PreferTrusted), boolInt(limits.ReservedOnly), pinned,
		t.AllowedProviders, t.MeteredProviders, t.CreatedAt)
	if err != nil {
		return t, err
	}
	t.ID, err = res.LastInsertId()
	return t, err
}

func prefixOf(key string) string {
	if len(key) > 12 {
		return key[:12]
	}
	return key
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

const tokenCols = `id, user, token_hash, token_prefix, request_limit, token_limit,
	window_seconds, slowdown_ms, prefer_trusted, reserved_only, pinned_account,
	allowed_providers, metered_providers, created_at, revoked_at`

func scanToken(row interface{ Scan(...any) error }) (Token, error) {
	var t Token
	var prefer, reserved int
	var pinned sql.NullInt64
	err := row.Scan(&t.ID, &t.User, &t.KeyHash, &t.KeyPrefix, &t.Requests, &t.Tokens,
		&t.WindowSecs, &t.SlowdownMS, &prefer, &reserved, &pinned,
		&t.AllowedProviders, &t.MeteredProviders, &t.CreatedAt, &t.RevokedAt)
	t.PreferTrusted = prefer != 0
	t.ReservedOnly = reserved != 0
	if pinned.Valid {
		id := pinned.Int64
		t.PinnedAccount = &id
	}
	return t, err
}

func (s *Store) AuthToken(ctx context.Context, key string) (Token, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+tokenCols+` FROM api_tokens WHERE token_hash=? AND revoked_at=0`, hashKey(key))
	t, err := scanToken(row)
	if errors.Is(err, sql.ErrNoRows) {
		return t, ErrNotFound
	}
	return t, err
}

// FindToken resolves a token by id, prefix, or raw key.
func (s *Store) FindToken(ctx context.Context, key string) (Token, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+tokenCols+` FROM api_tokens WHERE CAST(id AS TEXT)=? OR token_prefix=? OR token_hash=?`,
		key, key, hashKey(key))
	t, err := scanToken(row)
	if errors.Is(err, sql.ErrNoRows) {
		return t, ErrNotFound
	}
	return t, err
}

func (s *Store) ListTokens(ctx context.Context) ([]Token, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+tokenCols+` FROM api_tokens ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Token
	for rows.Next() {
		t, err := scanToken(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) RevokeToken(ctx context.Context, key string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE api_tokens SET revoked_at=? WHERE token_hash=? OR token_prefix=? OR CAST(id AS TEXT)=?`,
		time.Now().Unix(), hashKey(key), key, key)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) SetTokenLimits(ctx context.Context, id int64, limits TokenLimits) error {
	if limits.WindowSecs <= 0 {
		limits.WindowSecs = 3600
	}
	var pinned any
	if limits.PinnedAccount != nil {
		pinned = *limits.PinnedAccount
	}
	_, err := s.db.ExecContext(ctx, `UPDATE api_tokens SET request_limit=?, token_limit=?, window_seconds=?,
		slowdown_ms=?, prefer_trusted=?, reserved_only=?, pinned_account=?, allowed_providers=?, metered_providers=? WHERE id=?`,
		limits.Requests, limits.Tokens, limits.WindowSecs, limits.SlowdownMS,
		boolInt(limits.PreferTrusted), boolInt(limits.ReservedOnly), pinned,
		encodeProviders(limits.Providers), encodeProviders(limits.Metered), id)
	return err
}

// AdmitRequest atomically records an admission inside a transaction so
// concurrent requests cannot race past the request limit.
func (s *Store) AdmitRequest(ctx context.Context, tokenID, windowSecs, reqLimit int64) (bool, error) {
	if reqLimit <= 0 {
		return true, nil
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return true, err
	}
	defer tx.Rollback()
	cutoff := time.Now().UnixMilli() - windowSecs*1000
	var count int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM api_meter WHERE token_id=? AND ts_ms>=?`, tokenID, cutoff).Scan(&count); err != nil {
		return true, err
	}
	if count >= reqLimit {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO api_meter(token_id,ts_ms) VALUES (?,?)`, tokenID, time.Now().UnixMilli()); err != nil {
		return true, err
	}
	return true, tx.Commit()
}

// Admit is the original proxy's atomic request admission: it counts the
// rolling window, rejects over-limit tokens, persists the meter row, and
// returns the values used for x-ratelimit-* response headers.
func (s *Store) Admit(ctx context.Context, tokenID int64, limits TokenLimits) (Admission, error) {
	var out Admission
	now := time.Now().UnixMilli()
	window := limits.WindowSecs * 1000
	since := now - window
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()

	var requests, tokens int64
	var oldestRequest, oldestTokens sql.NullInt64
	err = tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(input_tokens+output_tokens),0), MIN(ts_ms),
		MIN(CASE WHEN input_tokens+output_tokens>0 THEN ts_ms END)
		FROM api_meter WHERE token_id=? AND ts_ms>?`, tokenID, since).
		Scan(&requests, &tokens, &oldestRequest, &oldestTokens)
	if err != nil {
		return out, err
	}
	retry := func(v sql.NullInt64) int64 {
		if !v.Valid {
			return 1
		}
		left := (v.Int64 + window - now) / 1000
		if left < 1 {
			return 1
		}
		return left
	}
	if limits.Requests > 0 && requests >= limits.Requests {
		return out, &AdmissionError{Kind: "request", RetryAfter: retry(oldestRequest)}
	}
	if limits.Tokens > 0 && tokens >= limits.Tokens {
		r := retry(oldestTokens)
		if !oldestTokens.Valid {
			r = retry(oldestRequest)
		}
		if len(limits.Metered) == 0 {
			return out, &AdmissionError{Kind: "token", RetryAfter: r}
		}
		out.TokensSpent = &r
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM api_meter WHERE token_id=? AND ts_ms<=?`, tokenID, since); err != nil {
		return out, err
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO api_meter(token_id,ts_ms) VALUES (?,?)`, tokenID, now)
	if err != nil {
		return out, err
	}
	out.MeterID, err = res.LastInsertId()
	if err != nil {
		return out, err
	}
	if limits.Requests > 0 {
		remaining := limits.Requests - requests - 1
		if remaining < 0 {
			remaining = 0
		}
		out.RequestLimit, out.RequestsRemaining = &limits.Requests, &remaining
	}
	if limits.Tokens > 0 {
		remaining := limits.Tokens - tokens
		if remaining < 0 {
			remaining = 0
		}
		out.TokenLimit, out.TokensRemaining = &limits.Tokens, &remaining
	}
	oldest := oldestRequest
	if !oldest.Valid {
		oldest = sql.NullInt64{Int64: now, Valid: true}
	}
	out.ResetAfter = retry(oldest)
	out.SlowdownMS = limits.SlowdownMS
	return out, tx.Commit()
}

// UsageLog is one completed request in the proxy's own history. The extra
// metadata columns mirror Rust's UsageRecord so the metrics families can be
// aggregated from the same dimensions.
type UsageLog struct {
	TokenID         *int64
	User            string
	Provider        string
	Dialect         string
	RequestedModel  string
	UpstreamModel   string
	AccountID       *int64
	Effort          string
	ServiceTier     string
	InputTokens     int64
	OutputTokens    int64
	CacheRead       int64
	CacheWrite      int64
	ReasoningTokens int64
	Status          int
	ErrorKind       string
	SessionKey      string
	TurnIndex       int64
	ToolsDeclared   int64
	ToolsCalled     string
	ThinkingBudget  int64
	ImageCount      int64
	RequestBytes    int64
	ResponseBytes   int64
	TTFTMS          int64
	StopReason      string
	Attempts        int64
	DurationMS      int64
	CostUSD         float64
	ListCostUSD     float64
	TurnStateBlocks *int64
	CacheTTLSecs    *int64
}

// LogUsage appends to the usage history that backs the profile endpoints and
// the Prometheus metric families.
func (s *Store) LogUsage(ctx context.Context, rec UsageLog) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO usage_log
		(token_id,user,provider,dialect,requested_model,upstream_model,account_id,effort,service_tier,
		 input_tokens,output_tokens,cache_read_tokens,cache_write_tokens,reasoning_tokens,
		 status,error_kind,duration_ms,session_key,turn_index,tools_declared,tools_called,
		 thinking_budget,image_count,request_bytes,response_bytes,ttft_ms,stop_reason,attempts,
		 cost_usd,list_cost_usd,turn_state_blocks,cache_ttl_secs)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		rec.TokenID, rec.User, rec.Provider, rec.Dialect, rec.RequestedModel, rec.UpstreamModel,
		rec.AccountID, rec.Effort, rec.ServiceTier, rec.InputTokens, rec.OutputTokens,
		rec.CacheRead, rec.CacheWrite, rec.ReasoningTokens, rec.Status, rec.ErrorKind,
		rec.DurationMS, rec.SessionKey, rec.TurnIndex, rec.ToolsDeclared, rec.ToolsCalled,
		rec.ThinkingBudget, rec.ImageCount, rec.RequestBytes, rec.ResponseBytes, rec.TTFTMS,
		rec.StopReason, rec.Attempts, rec.CostUSD, rec.ListCostUSD, rec.TurnStateBlocks, rec.CacheTTLSecs)
	return err
}

// MetricRow is one aggregated usage_log series for the Prometheus endpoint.
type MetricRow struct {
	User           string
	Provider       string
	RequestedModel string
	UpstreamModel  string
	Dialect        string
	Requests       int64
	Errors         int64
	Input          int64
	Output         int64
	CacheRead      int64
	CacheWrite     int64
	Cost           float64
	ListCost       float64
}

func (s *Store) MetricRows(ctx context.Context) ([]MetricRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT user, provider, requested_model, upstream_model, dialect,
		COUNT(*), SUM(CASE WHEN status>=400 THEN 1 ELSE 0 END),
		COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0),
		COALESCE(SUM(cache_read_tokens),0), COALESCE(SUM(cache_write_tokens),0),
		COALESCE(SUM(cost_usd),0), COALESCE(SUM(list_cost_usd),0)
		FROM usage_log GROUP BY 1,2,3,4,5`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MetricRow
	for rows.Next() {
		var m MetricRow
		if err := rows.Scan(&m.User, &m.Provider, &m.RequestedModel, &m.UpstreamModel, &m.Dialect,
			&m.Requests, &m.Errors, &m.Input, &m.Output, &m.CacheRead, &m.CacheWrite, &m.Cost, &m.ListCost); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// UsageAgg mirrors the Rust stats/usage aggregate. JSON field names match the
// original `stats` output so downstream consumers see the same shape.
type UsageAgg struct {
	Key              string  `json:"name,omitempty"`
	Requests         int64   `json:"requests"`
	Errors           int64   `json:"errors"`
	InputTokens      int64   `json:"input_tokens"`
	OutputTokens     int64   `json:"output_tokens"`
	CacheReadTokens  int64   `json:"cache_read_tokens"`
	CacheWriteTokens int64   `json:"cache_write_tokens"`
	CacheHitRatio    float64 `json:"cache_hit_ratio"`
	ReasoningTokens  int64   `json:"reasoning_tokens"`
}

// accountLabelSQL reproduces the Rust ACCOUNT_LABEL expression: prefer the
// linked account's label/email, fall back to the provider name when no
// account row exists for it, otherwise "none".
const accountLabelSQL = `COALESCE((SELECT COALESCE(NULLIF(a.label,''), NULLIF(a.email,''), 'account#' || a.id)
	FROM accounts a WHERE a.id = u.account_id),
	CASE WHEN u.provider <> '' AND NOT EXISTS
		(SELECT 1 FROM accounts a2 WHERE a2.provider = u.provider)
		THEN u.provider END, 'none')`

// UsageDim selects the grouping dimension for UsageBy.
type UsageDim string

const (
	DimUser    UsageDim = "user"
	DimAccount UsageDim = "account"
	DimModel   UsageDim = "model"
)

func usageHitRatio(input, cacheRead, cacheWrite int64) float64 {
	prompt := float64(input) + float64(cacheRead) + float64(cacheWrite)
	if prompt <= 0 {
		return 0
	}
	return float64(cacheRead) / prompt
}

// UsageTotals aggregates the whole window, mirroring Db::usage_totals.
func (s *Store) UsageTotals(ctx context.Context, since, until int64) (UsageAgg, error) {
	var a UsageAgg
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),
		COALESCE(SUM(CASE WHEN status>=400 THEN 1 ELSE 0 END),0),
		COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0),
		COALESCE(SUM(cache_read_tokens),0), COALESCE(SUM(cache_write_tokens),0),
		COALESCE(SUM(reasoning_tokens),0)
		FROM usage_log WHERE ts >= ? AND ts < ?`, since, until).
		Scan(&a.Requests, &a.Errors, &a.InputTokens, &a.OutputTokens,
			&a.CacheReadTokens, &a.CacheWriteTokens, &a.ReasoningTokens)
	a.CacheHitRatio = usageHitRatio(a.InputTokens, a.CacheReadTokens, a.CacheWriteTokens)
	return a, err
}

// UsageBy aggregates usage grouped by one dimension, mirroring Db::usage_by.
func (s *Store) UsageBy(ctx context.Context, dim UsageDim, since, until int64) ([]UsageAgg, error) {
	var keyExpr string
	switch dim {
	case DimUser:
		keyExpr = "u.user"
	case DimAccount:
		keyExpr = accountLabelSQL
	case DimModel:
		keyExpr = "u.upstream_model"
	default:
		return nil, fmt.Errorf("unknown usage dimension %q", dim)
	}
	query := fmt.Sprintf(`SELECT %s AS k, COUNT(*),
		COALESCE(SUM(CASE WHEN u.status>=400 OR u.error_kind<>'' THEN 1 ELSE 0 END),0),
		COALESCE(SUM(u.input_tokens),0), COALESCE(SUM(u.output_tokens),0),
		COALESCE(SUM(u.cache_read_tokens),0), COALESCE(SUM(u.cache_write_tokens),0),
		COALESCE(SUM(u.reasoning_tokens),0)
		FROM usage_log u WHERE u.ts >= ? AND u.ts < ?
		GROUP BY k ORDER BY SUM(u.input_tokens) + SUM(u.output_tokens) DESC`, keyExpr)
	rows, err := s.db.QueryContext(ctx, query, since, until)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []UsageAgg
	for rows.Next() {
		var a UsageAgg
		if err := rows.Scan(&a.Key, &a.Requests, &a.Errors, &a.InputTokens, &a.OutputTokens,
			&a.CacheReadTokens, &a.CacheWriteTokens, &a.ReasoningTokens); err != nil {
			return nil, err
		}
		a.CacheHitRatio = usageHitRatio(a.InputTokens, a.CacheReadTokens, a.CacheWriteTokens)
		out = append(out, a)
	}
	return out, rows.Err()
}

// SessionLast returns the most recent activity for a user's cache session.
func (s *Store) SessionLast(ctx context.Context, user, session string) (last int64, found bool, err error) {
	var count int64
	err = s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(ts),0), COUNT(*) FROM usage_log WHERE user=? AND session_key=?`,
		user, session).Scan(&last, &count)
	return last, count > 0, err
}

// NextTurnIndex returns the 1-based position of a request within its session.
// The session metrics use MAX(turn_index) as the deepest conversation seen.
func (s *Store) NextTurnIndex(ctx context.Context, user, session string) int64 {
	if session == "" {
		return 0
	}
	var n int64
	if err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(turn_index),0)+1 FROM usage_log WHERE user=? AND session_key=?`,
		user, session).Scan(&n); err != nil {
		return 1
	}
	return n
}

// DayUsage is one model's tokens for one UTC day.
type DayUsage struct {
	Day    string
	Model  string
	Input  int64
	Cached int64
	Output int64
}

func (s *Store) DailyUsage(ctx context.Context, user string) ([]DayUsage, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT strftime('%Y-%m-%d',ts,'unixepoch'), upstream_model,
		input_tokens, cache_read_tokens+cache_write_tokens, output_tokens
		FROM usage_log WHERE user=? AND status<400 ORDER BY 1,2`, user)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DayUsage
	for rows.Next() {
		var d DayUsage
		if err := rows.Scan(&d.Day, &d.Model, &d.Input, &d.Cached, &d.Output); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// RecordUsage settles input/output tokens into the most recent admission.
func (s *Store) RecordUsage(ctx context.Context, tokenID int64, input, output int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE api_meter SET input_tokens=input_tokens+?, output_tokens=output_tokens+?
		WHERE id=(SELECT id FROM api_meter WHERE token_id=? ORDER BY ts_ms DESC, id DESC LIMIT 1)`,
		input, output, tokenID)
	return err
}

// RecordUsageForMeter settles tokens against the exact admission row, so two
// concurrent requests cannot credit each other's meter.
func (s *Store) RecordUsageForMeter(ctx context.Context, meterID, input, output int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE api_meter SET input_tokens=input_tokens+?, output_tokens=output_tokens+? WHERE id=?`,
		input, output, meterID)
	return err
}

func (s *Store) TokenUsage(ctx context.Context, tokenID, windowSecs int64) (requests, input, output int64, err error) {
	cutoff := time.Now().UnixMilli() - windowSecs*1000
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0)
		FROM api_meter WHERE token_id=? AND ts_ms>=?`, tokenID, cutoff).Scan(&requests, &input, &output)
	return
}

func (s *Store) RetryAfter(ctx context.Context, tokenID, windowSecs, limit int64) int64 {
	cutoff := time.Now().UnixMilli() - windowSecs*1000
	var oldest int64
	err := s.db.QueryRowContext(ctx, `SELECT MIN(ts_ms) FROM api_meter WHERE token_id=? AND ts_ms>=?`, tokenID, cutoff).Scan(&oldest)
	if err != nil || oldest == 0 {
		return 60
	}
	left := (oldest + windowSecs*1000 - time.Now().UnixMilli()) / 1000
	if left < 1 {
		return 1
	}
	return left
}
