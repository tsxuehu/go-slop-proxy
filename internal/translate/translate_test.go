package translate

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/edy/slop-proxy-go/internal/config"
	"github.com/edy/slop-proxy-go/internal/provider"
)

func TestPatternSpecificity(t *testing.T) {
	if s, ok := patternSpecificity("claude-*", "claude-3-5-sonnet"); !ok || s != 7 {
		t.Fatalf("prefix specificity: %d %v", s, ok)
	}
	if s, ok := patternSpecificity("gpt-5", "gpt-5"); !ok || s < 1000 {
		t.Fatalf("exact match should win: %d %v", s, ok)
	}
	if _, ok := patternSpecificity("claude-*", "gpt-5"); ok {
		t.Fatal("non-matching pattern")
	}
}

func TestResolveAliasAndEffort(t *testing.T) {
	cfg := config.Default()
	cfg.Models.DefaultEffort = "medium"
	cfg.Models.Aliases = map[string]config.ModelAlias{
		"my-gpt": {Model: "gpt-5-codex", Effort: "high"},
	}
	r := Resolve(&cfg, "my-gpt")
	if r.Model != "gpt-5-codex" || r.Effort != "high" {
		t.Fatalf("alias: %+v", r)
	}
	r = Resolve(&cfg, "gpt-5:low")
	if r.Model != "gpt-5" || r.Effort != "low" {
		t.Fatalf("suffix: %+v", r)
	}
	r = Resolve(&cfg, "unknown")
	if r.Model != "unknown" || r.Effort != "medium" {
		t.Fatalf("default: %+v", r)
	}
}

func TestClampEffort(t *testing.T) {
	if got := ClampEffort("gpt-5-codex", "minimal"); got != "low" {
		t.Fatalf("codex minimal should clamp to low, got %s", got)
	}
	if got := ClampEffort("gpt-5", "minimal"); got != "minimal" {
		t.Fatalf("non-codex minimal should stay, got %s", got)
	}
}

func TestSignatureRoundTrip(t *testing.T) {
	id := "rs_123"
	sig := EncodeSignature(&id, "encrypted-blob")
	gotID, gotEnc := DecodeSignature(sig)
	if gotID == nil || *gotID != id || gotEnc != "encrypted-blob" {
		t.Fatalf("round trip failed: %v %s", gotID, gotEnc)
	}
	// Invalid input passes through unchanged.
	gotID, gotEnc = DecodeSignature("not-base64!!!")
	if gotID != nil || gotEnc != "not-base64!!!" {
		t.Fatalf("passthrough failed: %v %s", gotID, gotEnc)
	}
}

func TestChatUsageConversion(t *testing.T) {
	u := &ChatUsage{
		PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150,
		CompletionTokensDetails: &CompletionTokensDetails{ReasoningTokens: 10},
	}
	ir := UsageFromChat(u)
	if ir.InputTokens != 100 || ir.OutputTokens != 50 || ir.OutputTokensDetails.ReasoningTokens != 10 {
		t.Fatalf("chat->ir: %+v", ir)
	}
	back := ChatUsageFrom(&ir)
	if back.PromptTokens != 100 || back.CompletionTokens != 50 {
		t.Fatalf("ir->chat: %+v", back)
	}
}

func TestChatToolDefFlatForm(t *testing.T) {
	var def ChatToolDef
	data := []byte(`{"type":"function","name":"get_weather","parameters":{"type":"object"}}`)
	if err := json.Unmarshal(data, &def); err != nil {
		t.Fatal(err)
	}
	d := def.Def()
	if d == nil || d.Name != "get_weather" || len(d.Parameters) == 0 {
		t.Fatalf("flat tool not parsed: %+v", d)
	}
}

func TestChatPartAliases(t *testing.T) {
	var part ChatPart
	if err := json.Unmarshal([]byte(`{"type":"input_image","image_url":"https://x/y.png"}`), &part); err != nil {
		t.Fatal(err)
	}
	if part.Type != "image_url" || part.ImageURL == nil || part.ImageURL.Value != "https://x/y.png" {
		t.Fatalf("input_image alias: %+v", part)
	}
}

func TestToResponsesMessages(t *testing.T) {
	cfg := config.Default()
	req := &ChatRequest{
		Model: "gpt-5",
		Messages: []ChatMessage{
			{Role: "system", Content: &ChatContent{Text: "be helpful"}},
			{Role: "user", Content: &ChatContent{Text: "hello"}},
			{Role: "assistant", ToolCalls: &[]ChatToolCall{{
				ID: strPtr("call_1"), Function: FunctionBody{Name: strPtr("f"), Arguments: strPtr(`{"a":1}`)},
			}}},
			{Role: "tool", ToolCallID: strPtr("call_1"), Content: &ChatContent{Text: "result"}},
		},
	}
	out, err := ToResponses(req, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Input) != 4 {
		t.Fatalf("expected 4 input items, got %d: %+v", len(out.Input), out.Input)
	}
	if out.Input[0].Role != "developer" || out.Input[0].Content[0].Text != "be helpful" {
		t.Fatalf("system mapping: %+v", out.Input[0])
	}
	if out.Input[2].Type != "function_call" || out.Input[2].CallID != "call_1" {
		t.Fatalf("tool call mapping: %+v", out.Input[2])
	}
	if out.Input[3].Type != "function_call_output" || out.Input[3].Output.TextValue() != "result" {
		t.Fatalf("tool output mapping: %+v", out.Input[3])
	}
}

func TestWalkerTextAndTool(t *testing.T) {
	capture := &UsageCapture{}
	events := []*ResponsesEvent{
		{Type: EvCreated, Response: &ResponseObj{ID: strPtr("resp_1")}},
		{Type: EvOutputTextDelta, OutputIndex: 0, ContentIndex: 0, Delta: "Hello"},
		{Type: EvOutputTextDelta, OutputIndex: 0, ContentIndex: 0, Delta: " world"},
		{Type: EvOutputItemAdded, OutputIndex: 1, Item: &OutputItem{Type: "function_call", CallID: "call_1", Name: "f"}},
		{Type: EvFunctionCallArgumentsDelta, OutputIndex: 1, Delta: `{"a":`},
		{Type: EvFunctionCallArgumentsDelta, OutputIndex: 1, Delta: `1}`},
		{Type: EvCompleted, Response: &ResponseObj{Usage: &Usage{InputTokens: 10, OutputTokens: 5}}},
	}
	agg := Aggregate(events, capture)
	if agg.ID != "resp_1" {
		t.Fatalf("id: %s", agg.ID)
	}
	if len(agg.Blocks) != 2 {
		t.Fatalf("expected 2 blocks, got %d", len(agg.Blocks))
	}
	if agg.Blocks[0].Kind != BlockText || agg.Blocks[0].Text != "Hello world" {
		t.Fatalf("text block: %+v", agg.Blocks[0])
	}
	if agg.Blocks[1].Kind != BlockToolCall || agg.Blocks[1].Arguments != `{"a":1}` {
		t.Fatalf("tool block: %+v", agg.Blocks[1])
	}
	if agg.Stop != ToolUse {
		t.Fatalf("stop should be tool_use, got %v", agg.Stop)
	}
	if capture.Snapshot().InputTokens != 10 || capture.Snapshot().OutputTokens != 5 {
		t.Fatalf("capture: %+v", capture.Snapshot())
	}
}

func TestWalkerEmptyToolArguments(t *testing.T) {
	events := []*ResponsesEvent{
		{Type: EvOutputItemAdded, OutputIndex: 0, Item: &OutputItem{Type: "function_call", CallID: "c", Name: "f"}},
		{Type: EvCompleted, Response: &ResponseObj{}},
	}
	agg := Aggregate(events, nil)
	if len(agg.Blocks) != 1 || agg.Blocks[0].Arguments != "{}" {
		t.Fatalf("empty tool args should become {}: %+v", agg.Blocks)
	}
}

func TestOpenAiStreamToolAndUsage(t *testing.T) {
	s := NewOpenAiStream("gpt-5", true, &UsageCapture{})
	s.Feed(&ResponsesEvent{Type: EvCreated, Response: &ResponseObj{ID: strPtr("resp_x")}})
	frames := s.Feed(&ResponsesEvent{Type: EvOutputItemAdded, OutputIndex: 0, Item: &OutputItem{Type: "function_call", CallID: "c1", Name: "f"}})
	if len(frames) != 1 || !strings.Contains(frames[0], `"tool_calls"`) {
		t.Fatalf("tool announce frame: %v", frames)
	}
	frames = s.Feed(&ResponsesEvent{Type: EvCompleted, Response: &ResponseObj{Usage: &Usage{InputTokens: 10, OutputTokens: 5}}})
	if len(frames) != 2 {
		t.Fatalf("expected finish + usage frames, got %d", len(frames))
	}
	if !strings.Contains(frames[0], `"tool_calls"`) || !strings.Contains(frames[1], `"usage"`) {
		t.Fatalf("finish frames wrong: %v", frames)
	}
}

func TestAnthropicStreamHiddenThinkingNoGap(t *testing.T) {
	s := NewAnthropicStream("claude", 100, false, &UsageCapture{})
	s.Feed(&ResponsesEvent{Type: EvCreated, Response: &ResponseObj{ID: strPtr("r")}})
	s.Feed(&ResponsesEvent{Type: EvOutputItemAdded, OutputIndex: 0, Item: &OutputItem{Type: "reasoning"}})
	events := s.Feed(&ResponsesEvent{Type: EvOutputTextDelta, OutputIndex: 1, ContentIndex: 0, Delta: "hi"})
	var found bool
	for _, e := range events {
		if strings.Contains(e.Body, `"index":0`) && strings.Contains(e.Body, "text") {
			found = true
		}
	}
	if !found {
		t.Fatalf("text block should use index 0 with hidden thinking: %+v", events)
	}
}

func TestBridgeTextAndFinish(t *testing.T) {
	b := NewChatToResponses(nil)
	events := b.Feed(&ChatChunk{
		ID: "chunk_1", Object: "chat.completion.chunk",
		Choices: []ChunkChoice{{Index: 0, Delta: ChatDelta{Role: "assistant"}}},
	})
	if len(events) == 0 || events[0].Type != EvCreated {
		t.Fatalf("first frame should be created: %+v", events)
	}
	events = b.Feed(&ChatChunk{
		ID: "chunk_1", Choices: []ChunkChoice{{Index: 0, Delta: ChatDelta{Content: "hi"}}},
	})
	var hasDelta bool
	for _, e := range events {
		if e.Type == EvOutputTextDelta && e.Delta == "hi" {
			hasDelta = true
		}
	}
	if !hasDelta {
		t.Fatalf("missing text delta: %+v", events)
	}
	fr := "stop"
	usage := &ChatUsage{PromptTokens: 10, CompletionTokens: 2, TotalTokens: 12}
	events = b.Feed(&ChatChunk{
		ID: "chunk_1", Choices: []ChunkChoice{{Index: 0, Delta: ChatDelta{}, FinishReason: &fr}}, Usage: usage,
	})
	var hasCompleted bool
	for _, e := range events {
		if e.Type == EvCompleted {
			hasCompleted = true
		}
	}
	if !hasCompleted {
		t.Fatalf("missing completed on finish+usage: %+v", events)
	}
}

func TestEstimate(t *testing.T) {
	req := NewResponses("gpt-5", "hello")
	req.Input = []InputItem{
		{Type: "message", Role: "user", Content: []ContentPart{{Type: "input_text", Text: strings.Repeat("a", 40)}}},
		{Type: "message", Role: "user", Content: []ContentPart{{Type: "input_image", ImageURL: "u"}}},
	}
	got := Estimate(&req)
	want := int64((len("hello")+40)/4) + 8 + 1000 + 30
	if got != want {
		t.Fatalf("estimate: got %d want %d", got, want)
	}
}

func TestDecodeSignatureBase64(t *testing.T) {
	id, enc := DecodeSignature(base64.RawURLEncoding.EncodeToString([]byte(`{"id":"x","ec":"y"}`)))
	if id == nil || *id != "x" || enc != "y" {
		t.Fatalf("decode: %v %s", id, enc)
	}
}

var _ = provider.OpenAI
