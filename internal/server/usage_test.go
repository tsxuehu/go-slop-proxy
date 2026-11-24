package server

import (
	"context"
	"testing"
	"time"

	"github.com/edy/slop-proxy-go/internal/provider"
	"github.com/edy/slop-proxy-go/internal/store"
	"github.com/edy/slop-proxy-go/internal/translate"
)

func TestInspectUsageChatShape(t *testing.T) {
	body := []byte(`{
		"model":"gpt-5",
		"reasoning_effort":"high",
		"service_tier":"flex",
		"thinking":{"budget_tokens":4096},
		"tools":[{"type":"function","function":{"name":"search"}},{"type":"function","function":{"name":"calc"}}],
		"messages":[
			{"role":"user","content":[{"type":"text","text":"hi"},{"type":"image_url","image_url":{"url":"data:image/png;base64,x"}}]},
			{"role":"assistant","tool_calls":[{"type":"function","function":{"name":"search"}}]}
		]
	}`)
	ins := inspectUsage(body)
	if ins.ToolsDeclared != 2 {
		t.Errorf("tools_declared = %d, want 2", ins.ToolsDeclared)
	}
	if ins.ThinkingBudget != 4096 {
		t.Errorf("thinking_budget = %d, want 4096", ins.ThinkingBudget)
	}
	if ins.ImageCount != 1 {
		t.Errorf("image_count = %d, want 1", ins.ImageCount)
	}
	if ins.Effort != "high" || ins.ServiceTier != "flex" {
		t.Errorf("effort/tier = %q/%q", ins.Effort, ins.ServiceTier)
	}
	if ins.ToolsCalled != "search" {
		t.Errorf("tools_called = %q, want search", ins.ToolsCalled)
	}
}

func TestInspectUsageAnthropicShape(t *testing.T) {
	body := []byte(`{
		"model":"claude-x",
		"thinking":{"type":"enabled","budget_tokens":2048},
		"tools":[{"name":"bash"}],
		"messages":[{"role":"assistant","content":[{"type":"tool_use","name":"bash","input":{}}]}]
	}`)
	ins := inspectUsage(body)
	if ins.ToolsDeclared != 1 || ins.ThinkingBudget != 2048 {
		t.Errorf("declared/budget = %d/%d", ins.ToolsDeclared, ins.ThinkingBudget)
	}
	if ins.ToolsCalled != "bash" {
		t.Errorf("tools_called = %q, want bash", ins.ToolsCalled)
	}
}

// TestRecordUsageFeedsToolInsightSessionMetrics proves a normal translated
// request now writes every dimension the Prometheus families aggregate.
func TestRecordUsageFeedsToolInsightSessionMetrics(t *testing.T) {
	st := newSmokeStore(t)
	srv := &Server{store: st}
	ctx := context.Background()
	key, _ := store.GenerateToken()
	tok, err := st.CreateToken(ctx, key, "alice", store.TokenLimits{})
	if err != nil {
		t.Fatal(err)
	}
	stop := "end_turn"
	now := time.Now()
	first := now.Add(-80 * time.Millisecond)
	captured := &translate.CapturedUsage{
		ToolsCalled:   []string{"search", "calculator"},
		ResponseBytes: 512,
		StopReason:    &stop,
		FirstByteAt:   &first,
	}
	body := []byte(`{"model":"gpt-5","reasoning_effort":"medium","tools":[{"type":"function","function":{"name":"search"}}],"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"x"}}]}]}`)
	rc := &requestContext{
		token:      tok,
		user:       "alice",
		sessionKey: "sess-1",
		started:    now.Add(-120 * time.Millisecond),
	}
	meta := UsageMeta{Body: body, Captured: captured, Attempts: 2}
	// Two turns in the same session, so turn_index increments.
	srv.recordUsage(rc, provider.OpenAI, "gpt-5", "gpt-5", "chat", nil, 10, 4, meta)
	srv.recordUsage(rc, provider.OpenAI, "gpt-5", "gpt-5", "chat", nil, 8, 3, meta)

	delta, err := st.MetricsSince(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(delta.Tools) != 2 {
		t.Errorf("tool families = %d rows, want 2: %+v", len(delta.Tools), delta.Tools)
	}
	if len(delta.Insights) == 0 {
		t.Fatal("no insight rows")
	}
	ins := delta.Insights[0]
	if ins.StopReason != "end_turn" {
		t.Errorf("stop_reason = %q", ins.StopReason)
	}
	if ins.ToolsDeclared != 2 || ins.Images != 2 || ins.RequestBytes != 2*int64(len(body)) {
		t.Errorf("insight request dims = %+v", ins)
	}
	if ins.ResponseBytes != 1024 || ins.Attempts != 4 {
		t.Errorf("insight response dims = %+v", ins)
	}
	if ins.TTFTMS == 0 || ins.TTTTSamples != 2 {
		t.Errorf("ttft = %d samples = %d", ins.TTFTMS, ins.TTTTSamples)
	}
	if len(delta.Sessions) != 1 || delta.Sessions[0].Deepest != 2 {
		t.Errorf("session rows = %+v, want deepest turn 2", delta.Sessions)
	}
	if len(delta.Usage) == 0 {
		t.Fatal("no usage rows")
	}
	u := delta.Usage[0]
	if u.Dimensions[5] != "medium" {
		t.Errorf("effort dimension = %q, want medium", u.Dimensions[5])
	}
}
