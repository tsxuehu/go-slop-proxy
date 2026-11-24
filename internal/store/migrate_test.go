package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

func TestOpenRustStyleDatabase(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "rust.db")
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()

	// Rust leaves optional text and numeric fields NULL. Older versions also
	// lack columns that the Rust startup migration would normally add.
	_, err = legacy.Exec(`
CREATE TABLE accounts (
	id INTEGER PRIMARY KEY, provider TEXT NOT NULL, provider_account_id TEXT NOT NULL,
	trusted INTEGER NOT NULL DEFAULT 0, email TEXT, label TEXT, plan_type TEXT,
	access_token TEXT NOT NULL, refresh_token TEXT NOT NULL, access_expires_at INTEGER,
	last_refresh_at INTEGER, status TEXT NOT NULL DEFAULT 'active', cooldown_until INTEGER,
	disabled_reason TEXT, created_at INTEGER NOT NULL DEFAULT (unixepoch()),
	updated_at INTEGER NOT NULL DEFAULT (unixepoch()), UNIQUE(provider, provider_account_id)
);
CREATE TABLE api_tokens (
	id INTEGER PRIMARY KEY, user TEXT NOT NULL, token_hash BLOB NOT NULL UNIQUE,
	token_prefix TEXT NOT NULL, request_limit INTEGER, token_limit INTEGER,
	window_seconds INTEGER NOT NULL DEFAULT 3600, slowdown_ms INTEGER NOT NULL DEFAULT 0,
	prefer_trusted INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL DEFAULT (unixepoch()), revoked_at INTEGER
);
INSERT INTO accounts (provider, provider_account_id, access_token, refresh_token)
VALUES ('openai', 'account', 'fixture', 'fixture');
`)
	if err != nil {
		t.Fatal(err)
	}
	key, prefix := GenerateToken()
	if _, err := legacy.Exec("INSERT INTO api_tokens(user, token_hash, token_prefix) VALUES (?, ?, ?)", "alice", hashKey(key), prefix); err != nil {
		t.Fatal(err)
	}
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	accounts, err := st.ListAccounts(ctx, "openai")
	if err != nil || len(accounts) != 1 || accounts[0].ProviderAccountID != "account" {
		t.Fatalf("read legacy accounts: %v %v", accounts, err)
	}
	token, err := st.AuthToken(ctx, key)
	if err != nil || token.User != "alice" || token.Requests != 0 || token.Tokens != 0 {
		t.Fatalf("authenticate legacy token: %+v, %v", token, err)
	}
	if err := st.RevokeToken(ctx, prefix); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AuthToken(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("revoked legacy token authentication error = %v", err)
	}
}
