// Package pricing loads LiteLLM's model price table and computes the cost of
// a request. Rates are per token; a long-context tier replaces the entire
// rate set once the whole context crosses its threshold, as both vendors
// bill it.
package pricing

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Rates struct {
	Input      float64
	Output     float64
	CacheWrite float64
	CacheRead  float64
}

func (r Rates) empty() bool {
	return r.Input == 0 && r.Output == 0 && r.CacheWrite == 0 && r.CacheRead == 0
}

type Tokens struct {
	Input      int64
	Output     int64
	CacheRead  int64
	CacheWrite int64
}

func (t Tokens) context() int64 { return t.Input + t.CacheRead + t.CacheWrite }

type ModelPrice struct {
	Base        Rates
	LongContext *longContext
}

type longContext struct {
	Threshold int64
	Rates     Rates
}

func (m ModelPrice) Cost(t Tokens) float64 {
	rates := m.Base
	if m.LongContext != nil && t.context() > m.LongContext.Threshold {
		rates = m.LongContext.Rates
	}
	return float64(t.Input)*rates.Input +
		float64(t.Output)*rates.Output +
		float64(t.CacheRead)*rates.CacheRead +
		float64(t.CacheWrite)*rates.CacheWrite
}

// Table is an immutable snapshot; refresh swaps the pointer under a mutex.
type Table struct {
	models map[string]ModelPrice
}

func (t *Table) find(model string) (ModelPrice, bool) {
	if t == nil {
		return ModelPrice{}, false
	}
	if price, ok := t.models[model]; ok {
		return price, true
	}
	suffix, dotted := "/"+model, "."+model
	for key, price := range t.models {
		if strings.HasSuffix(key, suffix) || strings.HasSuffix(key, dotted) {
			return price, true
		}
	}
	if strings.HasPrefix(model, "gpt-") {
		return t.find(model[len("gpt-"):])
	}
	return ModelPrice{}, false
}

// Cost is the actual price of the request.
func (t *Table) Cost(model string, tokens Tokens) float64 {
	price, ok := t.find(model)
	if !ok {
		return 0
	}
	return price.Cost(tokens)
}

// ListCost values the same tokens at the model's own list price, so a free or
// contributor tier can report what it saved.
func (t *Table) ListCost(model string, tokens Tokens) float64 {
	paid := strings.TrimSuffix(model, "-free")
	if cost := t.Cost(paid, tokens); cost > 0 {
		return cost
	}
	if i := strings.LastIndex(paid, "-contributor"); i > 0 {
		return t.Cost(paid[:i], tokens)
	}
	return 0
}

func parse(body []byte) (*Table, error) {
	var raw map[string]map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	models := make(map[string]ModelPrice, len(raw))
	for name, entry := range raw {
		base := Rates{}
		num := func(key string, dst *float64) {
			var v float64
			if b, ok := entry[key]; ok && json.Unmarshal(b, &v) == nil {
				*dst = v
			}
		}
		num("input_cost_per_token", &base.Input)
		num("output_cost_per_token", &base.Output)
		num("cache_creation_input_token_cost", &base.CacheWrite)
		num("cache_read_input_token_cost", &base.CacheRead)
		if base.empty() {
			continue
		}
		price := ModelPrice{Base: base}
		var threshold int64
		above := base
		keys := make([]string, 0, len(entry))
		for key := range entry {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			at := strings.Index(key, "_above_")
			if at < 0 || !strings.HasSuffix(key, "k_tokens") {
				continue
			}
			limit, err := strconv.ParseInt(key[at+len("_above_"):len(key)-len("k_tokens")], 10, 64)
			if err != nil {
				continue
			}
			var rate float64
			if json.Unmarshal(entry[key], &rate) != nil {
				continue
			}
			slot := &above.Input
			switch key[:at] {
			case "output_cost_per_token":
				slot = &above.Output
			case "cache_creation_input_token_cost":
				slot = &above.CacheWrite
			case "cache_read_input_token_cost":
				slot = &above.CacheRead
			case "input_cost_per_token":
			default:
				continue
			}
			*slot = rate
			if threshold == 0 || limit < threshold {
				threshold = limit
			}
		}
		if threshold > 0 {
			price.LongContext = &longContext{Threshold: threshold * 1000, Rates: above}
		}
		models[name] = price
	}
	return &Table{models: models}, nil
}

// Prices keeps the last good table on disk beside the database, so a restart
// without network still bills correctly.
type Prices struct {
	mu        sync.RWMutex
	table     *Table
	url       string
	cachePath string
	client    *http.Client
}

func New(dbPath, url string) *Prices {
	cache := filepath.Join(filepath.Dir(dbPath), "litellm-prices.json")
	return &Prices{url: url, cachePath: cache, client: &http.Client{Timeout: 20 * time.Second}}
}

func (p *Prices) Table() *Table {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.table
}

func (p *Prices) Cost(model string, tokens Tokens) float64 {
	return p.Table().Cost(model, tokens)
}

func (p *Prices) ListCost(model string, tokens Tokens) float64 {
	return p.Table().ListCost(model, tokens)
}

// Refresh reads the cached table first, then tries the network. A failed
// network fetch leaves the cached table in place.
func (p *Prices) Refresh() {
	if body, err := os.ReadFile(p.cachePath); err == nil {
		if table, err := parse(body); err == nil {
			p.mu.Lock()
			p.table = table
			p.mu.Unlock()
		}
	}
	if p.url == "" {
		return
	}
	resp, err := p.client.Get(p.url)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	table, err := parse(body)
	if err != nil {
		return
	}
	p.mu.Lock()
	p.table = table
	p.mu.Unlock()
	_ = os.WriteFile(p.cachePath, body, 0o600)
}
