package store

import (
	"database/sql"
	"fmt"
)

// Rust releases added these columns in place. Existing SQLite tables keep
// their definitions when CREATE TABLE IF NOT EXISTS runs.
func migrateLegacyColumns(tx *sql.Tx) error {
	columns := map[string][]struct{ name, definition string }{
		"accounts": {
			{"http_referer", "TEXT"},
			{"turn_state", "TEXT"},
			{"auth_mode", "TEXT NOT NULL DEFAULT 'oauth'"},
			{"allowed_users", "TEXT NOT NULL DEFAULT ''"},
			{"reserved", "INTEGER NOT NULL DEFAULT 0"},
			{"egress", "INTEGER NOT NULL DEFAULT 0"},
		},
		"api_tokens": {
			{"allowed_providers", "TEXT NOT NULL DEFAULT ''"},
			{"pinned_account", "INTEGER"},
			{"reserved_only", "INTEGER NOT NULL DEFAULT 0"},
			{"metered_providers", "TEXT NOT NULL DEFAULT ''"},
		},
		"usage_log": {
			{"provider", "TEXT NOT NULL DEFAULT ''"},
			{"list_cost_usd", "REAL NOT NULL DEFAULT 0"},
			{"service_tier", "TEXT NOT NULL DEFAULT ''"},
			{"session_key", "TEXT NOT NULL DEFAULT ''"},
			{"turn_index", "INTEGER NOT NULL DEFAULT 0"},
			{"tools_declared", "INTEGER NOT NULL DEFAULT 0"},
			{"tools_called", "TEXT NOT NULL DEFAULT ''"},
			{"thinking_budget", "INTEGER NOT NULL DEFAULT 0"},
			{"image_count", "INTEGER NOT NULL DEFAULT 0"},
			{"request_bytes", "INTEGER NOT NULL DEFAULT 0"},
			{"response_bytes", "INTEGER NOT NULL DEFAULT 0"},
			{"ttft_ms", "INTEGER"},
			{"stop_reason", "TEXT NOT NULL DEFAULT ''"},
			{"attempts", "INTEGER NOT NULL DEFAULT 0"},
			{"turn_state_blocks", "INTEGER"},
			{"cache_ttl_secs", "INTEGER"},
		},
	}
	for table, wanted := range columns {
		for _, column := range wanted {
			var present bool
			err := tx.QueryRow("SELECT EXISTS(SELECT 1 FROM pragma_table_info(?) WHERE name=?)",
				table, column.name).Scan(&present)
			if err != nil {
				return err
			}
			if present {
				continue
			}
			query := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column.name, column.definition)
			if _, err := tx.Exec(query); err != nil {
				return fmt.Errorf("adding %s.%s: %w", table, column.name, err)
			}
		}
	}
	return nil
}
