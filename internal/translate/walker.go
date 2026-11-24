package translate

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// ---------- UsageCapture ----------

type CapturedUsage struct {
	InputTokens, OutputTokens, CacheReadTokens, CacheWriteTokens, ReasoningTokens int64
	Completed                                                                     bool
	ErrorKind                                                                     *string
	UpstreamEOF                                                                   bool
	LastEvent                                                                     *string
	FirstByteAt, LastByteAt                                                       *time.Time
	Egress                                                                        *int
	ResponseBytes                                                                 int64
	StopReason                                                                    *string
	ToolsCalled                                                                   []string
	UpstreamHead                                                                  *string
}

type UsageCapture struct {
	mu sync.Mutex
	u  CapturedUsage
}

func (c *UsageCapture) Snapshot() CapturedUsage {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.u
	out.ToolsCalled = append([]string(nil), c.u.ToolsCalled...)
	return out
}

func inputTokenPartition(u *Usage) (fresh, cached, written int64) {
	if u == nil {
		return 0, 0, 0
	}
	cached = u.InputTokensDetails.CachedTokens
	if cached < 0 {
		cached = 0
	}
	written = u.InputTokensDetails.CacheWriteTokens
	if written < 0 {
		written = 0
	}
	fresh = u.InputTokens
	if fresh < 0 {
		fresh = 0
	}
	fresh -= cached
	if fresh < 0 {
		fresh = 0
	}
	fresh -= written
	if fresh < 0 {
		fresh = 0
	}
	return
}

func (c *UsageCapture) Observe(event *ResponsesEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.noteEventLocked(event.Type)
	if kind, resp, ok := event.Terminal(); ok {
		if resp != nil && resp.Usage != nil {
			c.recordPartialLocked(resp.Usage)
		}
		c.u.Completed = true
		s := kind.AsStr()
		c.u.StopReason = &s
		if kind == TerminalFailed {
			k := "upstream_failed"
			c.u.ErrorKind = &k
		}
	}
	if event.Type == EvOutputItemDone && event.Item != nil {
		if event.Item.Type == "function_call" || event.Item.Type == "custom_tool_call" {
			c.noteToolCallLocked(event.Item.Name)
		}
	}
}

func (c *UsageCapture) Record(u *Usage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recordPartialLocked(u)
	c.u.Completed = true
}

func (c *UsageCapture) recordPartialLocked(u *Usage) {
	fresh, cached, written := inputTokenPartition(u)
	c.u.InputTokens, c.u.CacheReadTokens, c.u.CacheWriteTokens = fresh, cached, written
	c.u.OutputTokens = u.OutputTokens
	c.u.ReasoningTokens = u.OutputTokensDetails.ReasoningTokens
}

func (c *UsageCapture) noteEventLocked(name string) {
	now := time.Now()
	c.u.LastEvent = &name
	if c.u.FirstByteAt == nil {
		c.u.FirstByteAt = &now
	}
	c.u.LastByteAt = &now
}

func (c *UsageCapture) NoteBytes(n int64) {
	if n == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.u.ResponseBytes += n
	now := time.Now()
	if c.u.FirstByteAt == nil {
		c.u.FirstByteAt = &now
	}
	c.u.LastByteAt = &now
}

func (c *UsageCapture) NoteEgress(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.u.Egress = &i
}

func (c *UsageCapture) NoteStopReason(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.u.StopReason = &s
}

func (c *UsageCapture) NoteCutoff(status string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := "upstream_cutoff"
	c.u.ErrorKind = &k
	s := strings.ToLower(status)
	c.u.StopReason = &s
}

func (c *UsageCapture) noteToolCallLocked(name string) {
	if name == "" {
		return
	}
	for _, n := range c.u.ToolsCalled {
		if n == name {
			return
		}
	}
	c.u.ToolsCalled = append(c.u.ToolsCalled, name)
}

func (c *UsageCapture) NoteUpstreamHead(b []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.u.UpstreamHead != nil {
		return
	}
	runes := []rune(string(b))
	if len(runes) > 400 {
		runes = runes[:400]
	}
	s := string(runes)
	c.u.UpstreamHead = &s
}

func (c *UsageCapture) NoteUpstreamEOF() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.u.UpstreamEOF = true
}

func (c *UsageCapture) Fail(kind string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.u.ErrorKind == nil {
		c.u.ErrorKind = &kind
	}
}

// ---------- Walker state machine ----------

type BlockKind int

const (
	BlockThinking BlockKind = iota
	BlockText
	BlockToolCall
)

type Block struct {
	Kind                BlockKind
	Text                string  // Thinking / Text
	Signature           *string // Thinking
	ID, Name, Arguments string  // ToolCall
}

type StopKind int

const (
	EndTurn StopKind = iota
	MaxTokens
	ToolUse
	StopError
)

func (s StopKind) AsStr() string {
	switch s {
	case EndTurn:
		return "end_turn"
	case MaxTokens:
		return "max_tokens"
	case ToolUse:
		return "tool_use"
	default:
		return "error"
	}
}

func (s StopKind) ChatFinishReason() string {
	switch s {
	case ToolUse:
		return "tool_calls"
	case MaxTokens:
		return "length"
	default:
		return "stop"
	}
}

type StepKind int

const (
	StepStart StepKind = iota
	StepBlock
	StepStop
	StepFailed
)

type BlockEventKind int

const (
	EvOpen BlockEventKind = iota
	EvAppend
	EvSignature
	EvClose
)

type Step struct {
	Kind    StepKind
	ID      *string        // Start
	Index   int            // Block
	Event   BlockEventKind // Block
	Block   Block          // Block + Open payload
	Payload string         // Append / Signature
	Stop    StopKind       // Stop
	Usage   Usage          // Stop
	Message string         // Failed
	Code    *string        // Failed
}

type blockKey struct {
	output, content uint64
}

type trackedBlock struct {
	index  int
	seen   bool
	closed bool
}

type Walker struct {
	blocks  map[blockKey]*trackedBlock
	keys    []blockKey // insertion order for deterministic iteration
	sawTool bool
	done    bool
	capture *UsageCapture
}

func NewWalker(capture *UsageCapture) *Walker {
	return &Walker{blocks: make(map[blockKey]*trackedBlock), capture: capture}
}

func (w *Walker) Step(event *ResponsesEvent) []Step {
	if w.done {
		return nil
	}
	if w.capture != nil {
		w.capture.Observe(event)
	}
	var out []Step
	switch event.Type {
	case EvCreated:
		if event.Response != nil {
			out = append(out, Step{Kind: StepStart, ID: event.Response.ID})
		}
	case EvOutputItemAdded:
		if event.Item == nil {
			break
		}
		key := blockKey{event.OutputIndex, 0}
		switch event.Item.Type {
		case "reasoning":
			out = append(out, w.open(key, Block{Kind: BlockThinking})...)
		case "function_call":
			b := Block{Kind: BlockToolCall, ID: event.Item.CallID, Name: event.Item.Name}
			out = append(out, w.open(key, b)...)
		}
	case EvReasoningSummaryPartAdded:
		key := blockKey{event.OutputIndex, 0}
		if _, ok := w.blocks[key]; ok {
			out = append(out, w.append(key, "\n\n")...)
		}
	case EvReasoningSummaryTextDelta, EvReasoningTextDelta:
		key := blockKey{event.OutputIndex, 0}
		out = append(out, w.open(key, Block{Kind: BlockThinking})...)
		out = append(out, w.append(key, event.Delta)...)
	case EvOutputTextDelta:
		key := blockKey{event.OutputIndex, event.ContentIndex}
		out = append(out, w.open(key, Block{Kind: BlockText})...)
		out = append(out, w.append(key, event.Delta)...)
	case EvFunctionCallArgumentsDelta:
		key := blockKey{event.OutputIndex, 0}
		out = append(out, w.append(key, event.Delta)...)
	case EvOutputItemDone:
		if event.Item != nil {
			out = append(out, w.finishItem(event.OutputIndex, event.Item)...)
		}
	case EvCompleted:
		var usage Usage
		if event.Response != nil && event.Response.Usage != nil {
			usage = *event.Response.Usage
		}
		kind := EndTurn
		if w.sawTool {
			kind = ToolUse
		}
		out = append(out, w.stop(kind, usage)...)
	case EvIncomplete:
		var usage Usage
		if event.Response != nil && event.Response.Usage != nil {
			usage = *event.Response.Usage
		}
		out = append(out, w.stop(MaxTokens, usage)...)
	case EvFailed:
		msg := "upstream response failed"
		var code *string
		if event.Response != nil && event.Response.Error != nil {
			if event.Response.Error.Message != nil {
				msg = *event.Response.Error.Message
			}
			code = event.Response.Error.Code
		}
		if w.capture != nil {
			w.capture.Fail("upstream_failed")
			w.capture.NoteStopReason("error")
		}
		w.done = true
		out = append(out, Step{Kind: StepFailed, Message: msg, Code: code})
	}
	return out
}

func (w *Walker) finishItem(oi uint64, item *OutputItem) []Step {
	var out []Step
	key := blockKey{oi, 0}
	switch item.Type {
	case "reasoning":
		out = append(out, w.open(key, Block{Kind: BlockThinking})...)
		var parts []string
		if item.Summary != nil {
			for _, s := range *item.Summary {
				if s.Type == "summary_text" {
					parts = append(parts, s.Text)
				}
			}
		}
		text := strings.Join(parts, "\n\n")
		tracked := w.blocks[key]
		if tracked != nil && !tracked.seen && text != "" {
			out = append(out, w.append(key, text)...)
		}
		if item.EncryptedContent != nil {
			sig := EncodeSignature(item.ID, *item.EncryptedContent)
			out = append(out, w.signature(key, sig)...)
		}
		out = append(out, w.close(key)...)
	case "message":
		if item.Content != nil {
			for ci, part := range *item.Content {
				if part.Type == "output_text" {
					k := blockKey{oi, uint64(ci)}
					out = append(out, w.open(k, Block{Kind: BlockText})...)
					tracked := w.blocks[k]
					if tracked != nil && !tracked.seen {
						out = append(out, w.append(k, part.Text)...)
					}
				}
			}
		}
		// Close all unclosed blocks under this output_index, in key order.
		for _, k := range w.sortedKeys() {
			if k.output == oi {
				out = append(out, w.close(k)...)
			}
		}
	case "function_call":
		b := Block{Kind: BlockToolCall, ID: item.CallID, Name: item.Name}
		out = append(out, w.open(key, b)...)
		tracked := w.blocks[key]
		if tracked != nil && !tracked.seen && item.Arguments != nil && *item.Arguments != "" {
			out = append(out, w.append(key, *item.Arguments)...)
		}
		out = append(out, w.close(key)...)
	}
	return out
}

func (w *Walker) open(key blockKey, b Block) []Step {
	if _, ok := w.blocks[key]; ok {
		return nil
	}
	idx := len(w.blocks)
	if b.Kind == BlockToolCall {
		w.sawTool = true
		if w.capture != nil {
			w.capture.mu.Lock()
			w.capture.noteToolCallLocked(b.Name)
			w.capture.mu.Unlock()
		}
	}
	w.blocks[key] = &trackedBlock{index: idx}
	w.keys = append(w.keys, key)
	return []Step{{Kind: StepBlock, Index: idx, Event: EvOpen, Block: b}}
}

func (w *Walker) append(key blockKey, text string) []Step {
	t := w.blocks[key]
	if t == nil || t.closed || text == "" {
		return nil
	}
	t.seen = true
	return []Step{{Kind: StepBlock, Index: t.index, Event: EvAppend, Payload: text}}
}

func (w *Walker) signature(key blockKey, sig string) []Step {
	t := w.blocks[key]
	if t == nil || t.closed {
		return nil
	}
	return []Step{{Kind: StepBlock, Index: t.index, Event: EvSignature, Payload: sig}}
}

func (w *Walker) close(key blockKey) []Step {
	t := w.blocks[key]
	if t == nil || t.closed {
		return nil
	}
	t.closed = true
	return []Step{{Kind: StepBlock, Index: t.index, Event: EvClose}}
}

func (w *Walker) stop(kind StopKind, usage Usage) []Step {
	var out []Step
	for _, k := range w.sortedKeys() {
		out = append(out, w.close(k)...)
	}
	if w.capture != nil {
		w.capture.NoteStopReason(kind.AsStr())
	}
	w.done = true
	out = append(out, Step{Kind: StepStop, Stop: kind, Usage: usage})
	return out
}

func (w *Walker) sortedKeys() []blockKey {
	keys := append([]blockKey(nil), w.keys...)
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].output != keys[j].output {
			return keys[i].output < keys[j].output
		}
		return keys[i].content < keys[j].content
	})
	return keys
}

func (w *Walker) EOF() []Step {
	if w.done {
		return nil
	}
	w.done = true
	if w.capture != nil {
		w.capture.Fail("upstream_eof")
	}
	return []Step{{Kind: StepFailed, Message: "upstream stream ended unexpectedly"}}
}

// ---------- aggregate ----------

type Aggregated struct {
	ID     string
	Blocks []Block
	Stop   StopKind
	Usage  Usage
	Error  *string
}

func newID(prefix string) string {
	return prefix + randomHex(16)
}

func Aggregate(events []*ResponsesEvent, capture *UsageCapture) Aggregated {
	agg := Aggregated{ID: newID("resp_")}
	w := NewWalker(capture)
	for _, e := range events {
		for _, step := range w.Step(e) {
			foldStep(&agg, step)
		}
	}
	for _, step := range w.EOF() {
		foldStep(&agg, step)
	}
	return agg
}

func foldStep(agg *Aggregated, s Step) {
	switch s.Kind {
	case StepStart:
		if s.ID != nil {
			agg.ID = *s.ID
		}
	case StepBlock:
		switch s.Event {
		case EvOpen:
			agg.Blocks = append(agg.Blocks, s.Block)
		case EvAppend:
			idx := blockIndex(agg, s.Index)
			if idx < 0 {
				return
			}
			b := &agg.Blocks[idx]
			switch b.Kind {
			case BlockThinking, BlockText:
				b.Text += s.Payload
			case BlockToolCall:
				b.Arguments += s.Payload
			}
		case EvSignature:
			idx := blockIndex(agg, s.Index)
			if idx >= 0 && agg.Blocks[idx].Kind == BlockThinking {
				sig := s.Payload
				agg.Blocks[idx].Signature = &sig
			}
		case EvClose:
			idx := blockIndex(agg, s.Index)
			if idx >= 0 && agg.Blocks[idx].Kind == BlockToolCall && agg.Blocks[idx].Arguments == "" {
				agg.Blocks[idx].Arguments = "{}"
			}
		}
	case StepStop:
		agg.Stop, agg.Usage = s.Stop, s.Usage
	case StepFailed:
		msg := s.Message
		agg.Error, agg.Stop = &msg, StopError
	}
}

// blockIndex maps a walker step index to the matching open block in the agg.
// Since blocks are opened in index order, s.Index is the position in Blocks.
func blockIndex(agg *Aggregated, index int) int {
	if index >= 0 && index < len(agg.Blocks) {
		return index
	}
	return -1
}
