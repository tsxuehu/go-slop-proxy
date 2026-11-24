package store

import (
	"context"
	"testing"
)

func TestUsageAggregatesWithWindow(t *testing.T) {
	st, err := Open(t.TempDir() + "/agg.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()

	// Explicit timestamps let us exercise the [since, until) filter exactly.
	rows := []struct {
		ts          int64
		user, model string
		in, out     int64
		status      int
	}{
		{1000, "alice", "gpt-5", 10, 5, 200},
		{2000, "alice", "gpt-5", 20, 7, 200},
		{3000, "bob", "gpt-5", 30, 9, 500},
		{9000, "carol", "o3", 40, 11, 200},
	}
	for _, r := range rows {
		if _, err := st.db.ExecContext(ctx, `INSERT INTO usage_log
			(ts,user,provider,upstream_model,input_tokens,output_tokens,status)
			VALUES (?,?,?,?,?,?,?)`, r.ts, r.user, "openai", r.model, r.in, r.out, r.status); err != nil {
			t.Fatal(err)
		}
	}

	// [1000, 4000) covers exactly the first three rows.
	totals, err := st.UsageTotals(ctx, 1000, 4000)
	if err != nil {
		t.Fatal(err)
	}
	if totals.Requests != 3 || totals.Errors != 1 || totals.InputTokens != 60 || totals.OutputTokens != 21 {
		t.Fatalf("totals = %+v", totals)
	}
	if totals.CacheHitRatio != 0 {
		t.Fatalf("unexpected cache hit ratio %v", totals.CacheHitRatio)
	}

	// until is exclusive: ts=1000 is not included in [0, 1000).
	empty, err := st.UsageTotals(ctx, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if empty.Requests != 0 {
		t.Fatalf("expected empty window, got %+v", empty)
	}

	all, err := st.UsageTotals(ctx, 0, 10000)
	if err != nil {
		t.Fatal(err)
	}
	if all.Requests != 4 {
		t.Fatalf("all-window requests = %d", all.Requests)
	}

	byUser, err := st.UsageBy(ctx, DimUser, 1000, 4000)
	if err != nil {
		t.Fatal(err)
	}
	if len(byUser) != 2 || byUser[0].Key != "alice" || byUser[0].Requests != 2 {
		t.Fatalf("by_user = %+v", byUser)
	}

	// accountLabelSQL: no accounts row exists for provider "openai", so all
	// rows collapse into a single "openai" group.
	byAccount, err := st.UsageBy(ctx, DimAccount, 1000, 4000)
	if err != nil {
		t.Fatal(err)
	}
	if len(byAccount) != 1 || byAccount[0].Key != "openai" || byAccount[0].Requests != 3 {
		t.Fatalf("by_account = %+v", byAccount)
	}

	byModel, err := st.UsageBy(ctx, DimModel, 1000, 4000)
	if err != nil {
		t.Fatal(err)
	}
	if len(byModel) != 1 || byModel[0].Key != "gpt-5" {
		t.Fatalf("by_model = %+v", byModel)
	}
}
