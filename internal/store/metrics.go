package store

import (
	"context"
	"fmt"
)

// The eight usage label dimensions, in the order Rust's USAGE_DIMENSIONS
// declares them. Every counter that groups by usage uses exactly these.
var UsageDimensions = [8]struct{ Label, Expr string }{
	{"user", "u.user"},
	{"account", accountLabelSQL},
	{"provider", providerLabelSQL},
	{"requested_model", "u.requested_model"},
	{"model", "u.upstream_model"},
	{"effort", "u.effort"},
	{"service_tier", "u.service_tier"},
	{"dialect", "u.dialect"},
}

const providerLabelSQL = `COALESCE(NULLIF(u.provider, ''),
	(SELECT a.provider FROM accounts a WHERE a.id = u.account_id), 'none')`

// errorKindSQL mirrors Rust's COALESCE(u.error_kind, 'http_Nxx'); this schema
// stores error_kind as NOT NULL DEFAULT ” instead of a nullable column.
const errorKindSQL = `CASE WHEN u.error_kind <> '' THEN u.error_kind
	ELSE 'http_' || (u.status / 100) || 'xx' END`

// failedSQL is the shared "this request failed" predicate.
const failedSQL = `(u.status >= 400 OR u.error_kind <> '')`

type MetricUsageRow struct {
	Dimensions       [8]string
	Requests         int64
	Errors           int64
	InputTokens      int64
	OutputTokens     int64
	CacheReadTokens  int64
	CacheWriteTokens int64
	ReasoningTokens  int64
	CostUSD          float64
	ListCostUSD      float64
	DurationMS       float64
}

type MetricErrorRow struct {
	User     string
	Provider string
	Kind     string
	Count    int64
}

type MetricToolRow struct {
	User   string
	Tool   string
	Count  int64
	Errors int64
}

type MetricInsightRow struct {
	User           string
	Account        string
	StopReason     string
	Requests       int64
	RequestBytes   int64
	ResponseBytes  int64
	Turns          int64
	Images         int64
	ThinkingBudget int64
	ToolsDeclared  int64
	TTFTMS         int64
	TTTTSamples    int64
	Attempts       int64
}

type MetricSessionPart struct {
	User       string
	SessionKey string
	AccountID  *int64
	Deepest    int64
	Tokens     int64
}

// MetricsDelta is one incremental slice of usage_log, read in a single
// snapshot so every family agrees on where it ends.
type MetricsDelta struct {
	LastID   int64
	Accounts string
	Usage    []MetricUsageRow
	Errors   []MetricErrorRow
	Tools    []MetricToolRow
	Insights []MetricInsightRow
	Sessions []MetricSessionPart
}

// MetricsSince reads everything logged after `after` in one read transaction.
func (s *Store) MetricsSince(ctx context.Context, after int64) (MetricsDelta, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return MetricsDelta{}, err
	}
	defer tx.Rollback()
	var delta MetricsDelta
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(id), 0) FROM usage_log`).Scan(&delta.LastID); err != nil {
		return MetricsDelta{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(group_concat(id || ':' || COALESCE(label,'') || ':'
		|| COALESCE(email,'') || ':' || provider, ','), '')
		FROM (SELECT * FROM accounts ORDER BY id)`).Scan(&delta.Accounts); err != nil {
		return MetricsDelta{}, err
	}
	read := func(query string, scan func(rows scanner) error) error {
		rows, err := tx.QueryContext(ctx, query, after, delta.LastID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			if err := scan(rows); err != nil {
				return err
			}
		}
		return rows.Err()
	}

	selected := make([]string, 0, len(UsageDimensions))
	grouped := make([]string, 0, len(UsageDimensions))
	for _, dim := range UsageDimensions {
		selected = append(selected, dim.Expr+" AS "+dim.Label)
		grouped = append(grouped, dim.Label)
	}
	usageSQL := fmt.Sprintf(`SELECT %s, COUNT(*),
		COALESCE(SUM(%s),0),
		COALESCE(SUM(u.input_tokens),0), COALESCE(SUM(u.output_tokens),0),
		COALESCE(SUM(u.cache_read_tokens),0), COALESCE(SUM(u.cache_write_tokens),0),
		COALESCE(SUM(u.reasoning_tokens),0), COALESCE(SUM(u.cost_usd),0),
		COALESCE(SUM(u.list_cost_usd),0), COALESCE(SUM(u.duration_ms),0)
		FROM usage_log u WHERE u.id > ?1 AND u.id <= ?2 GROUP BY %s`,
		joinComma(selected), failedSQL, joinComma(grouped))
	if err := read(usageSQL, func(rows scanner) error {
		var m MetricUsageRow
		dest := make([]any, 0, 19)
		for i := range m.Dimensions {
			dest = append(dest, &m.Dimensions[i])
		}
		dest = append(dest, &m.Requests, &m.Errors, &m.InputTokens, &m.OutputTokens,
			&m.CacheReadTokens, &m.CacheWriteTokens, &m.ReasoningTokens,
			&m.CostUSD, &m.ListCostUSD, &m.DurationMS)
		if err := rows.Scan(dest...); err != nil {
			return err
		}
		delta.Usage = append(delta.Usage, m)
		return nil
	}); err != nil {
		return MetricsDelta{}, err
	}

	errorSQL := fmt.Sprintf(`SELECT u.user, %s AS provider, %s AS kind, COUNT(*)
		FROM usage_log u WHERE %s AND u.id > ?1 AND u.id <= ?2
		GROUP BY u.user, provider, kind`, providerLabelSQL, errorKindSQL, failedSQL)
	if err := read(errorSQL, func(rows scanner) error {
		var m MetricErrorRow
		if err := rows.Scan(&m.User, &m.Provider, &m.Kind, &m.Count); err != nil {
			return err
		}
		delta.Errors = append(delta.Errors, m)
		return nil
	}); err != nil {
		return MetricsDelta{}, err
	}

	toolSQL := `WITH RECURSIVE split(user, failed, tool, rest) AS (
		SELECT user, (status >= 400 OR error_kind <> ''), '', tools_called || ','
		FROM usage_log WHERE tools_called <> '' AND id > ?1 AND id <= ?2
		UNION ALL
		SELECT user, failed, substr(rest, 1, instr(rest, ',') - 1), substr(rest, instr(rest, ',') + 1)
		FROM split WHERE rest <> ''
	)
	SELECT user, tool, COUNT(*), COALESCE(SUM(failed),0)
	FROM split WHERE tool <> '' GROUP BY user, tool`
	if err := read(toolSQL, func(rows scanner) error {
		var m MetricToolRow
		if err := rows.Scan(&m.User, &m.Tool, &m.Count, &m.Errors); err != nil {
			return err
		}
		delta.Tools = append(delta.Tools, m)
		return nil
	}); err != nil {
		return MetricsDelta{}, err
	}

	stopReasonSQL := fmt.Sprintf(`COALESCE(NULLIF(u.stop_reason, ''), NULLIF(u.error_kind, ''),
		CASE WHEN u.status >= 400 THEN 'http_' || (u.status / 100) || 'xx' END, 'unrecorded')`)
	insightSQL := fmt.Sprintf(`SELECT u.user, %s AS account, %s AS stop_reason,
		COUNT(*), COALESCE(SUM(u.request_bytes),0), COALESCE(SUM(u.response_bytes),0),
		COALESCE(SUM(u.turn_index),0), COALESCE(SUM(u.image_count),0),
		COALESCE(SUM(u.thinking_budget),0), COALESCE(SUM(u.tools_declared),0),
		COALESCE(SUM(u.ttft_ms),0), COALESCE(SUM(u.ttft_ms IS NOT NULL),0),
		COALESCE(SUM(u.attempts),0)
		FROM usage_log u WHERE u.id > ?1 AND u.id <= ?2
		GROUP BY u.user, account, stop_reason`, accountLabelSQL, stopReasonSQL)
	if err := read(insightSQL, func(rows scanner) error {
		var m MetricInsightRow
		if err := rows.Scan(&m.User, &m.Account, &m.StopReason, &m.Requests,
			&m.RequestBytes, &m.ResponseBytes, &m.Turns, &m.Images,
			&m.ThinkingBudget, &m.ToolsDeclared, &m.TTFTMS, &m.TTTTSamples, &m.Attempts); err != nil {
			return err
		}
		delta.Insights = append(delta.Insights, m)
		return nil
	}); err != nil {
		return MetricsDelta{}, err
	}

	sessionSQL := `SELECT user, session_key, account_id,
		COALESCE(MAX(turn_index),0),
		COALESCE(SUM(input_tokens + output_tokens + cache_read_tokens + cache_write_tokens),0)
		FROM usage_log WHERE session_key <> '' AND (account_id IS NOT NULL OR provider <> '')
		AND id > ?1 AND id <= ?2 GROUP BY user, session_key, account_id`
	if err := read(sessionSQL, func(rows scanner) error {
		var m MetricSessionPart
		if err := rows.Scan(&m.User, &m.SessionKey, &m.AccountID, &m.Deepest, &m.Tokens); err != nil {
			return err
		}
		delta.Sessions = append(delta.Sessions, m)
		return nil
	}); err != nil {
		return MetricsDelta{}, err
	}
	return delta, tx.Commit()
}

type scanner interface{ Scan(dest ...any) error }

func joinComma(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += ", "
		}
		out += p
	}
	return out
}
