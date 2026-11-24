package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/edy/slop-proxy-go/internal/backend"
	"github.com/edy/slop-proxy-go/internal/config"
	"github.com/edy/slop-proxy-go/internal/pool"
	"github.com/edy/slop-proxy-go/internal/provider"
)

type cachedCodexCatalog struct {
	at    time.Time
	value *backend.CatalogSnapshot
}

func (s *Server) codexAccountCatalog(ctx context.Context, pl *pool.Pool, slot *pool.Slot) *backend.CatalogSnapshot {
	s.catalogsMu.RLock()
	cached := s.codexCatalogs[slot.ID]
	s.catalogsMu.RUnlock()
	if cached.value != nil && time.Since(cached.at) < 5*time.Minute {
		return cached.value
	}
	token, err := pl.Slots().FreshToken(ctx, slot, false)
	if err != nil {
		return cached.value
	}
	snapshot, upstreamErr := s.backend.Catalog(ctx, token, slot.ProviderAccountID)
	if upstreamErr != nil {
		return cached.value
	}
	pl.Slots().SetCatalog(slot, snapshot.Models)
	pl.Slots().SetServiceTiers(slot, snapshot.ServiceTiers)
	s.catalogsMu.Lock()
	if s.codexCatalogs == nil {
		s.codexCatalogs = make(map[int64]cachedCodexCatalog)
	}
	s.codexCatalogs[slot.ID] = cachedCodexCatalog{time.Now(), snapshot}
	s.catalogsMu.Unlock()
	return snapshot
}

func (s *Server) codexCatalog(ctx context.Context, rc *requestContext) []*backend.CatalogSnapshot {
	pl := s.pools[provider.OpenAI]
	if pl == nil || s.backend == nil || !s.tokenMayUse(rc, provider.OpenAI) {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var out []*backend.CatalogSnapshot
	for _, slot := range pl.Slots().List() {
		if pl.Slots().IsDisabled(slot) || !slot.Serves(rc.user) || slot.Reserved != rc.token.ReservedOnly ||
			(rc.token.PinnedAccount != nil && slot.ID != *rc.token.PinnedAccount) {
			continue
		}
		if model := s.codexAccountCatalog(ctx, pl, slot); model != nil {
			out = append(out, model)
		}
	}
	return out
}

type modelEntry struct {
	ID            string   `json:"id"`
	Object        string   `json:"object"`
	Created       int64    `json:"created"`
	OwnedBy       string   `json:"owned_by"`
	ContextLength *int64   `json:"context_length,omitempty"`
	Input         []string `json:"input,omitempty"`
}

func newModelEntry(id, owner string, contextLength *int64, input []string) modelEntry {
	return modelEntry{ID: id, Object: "model", Created: time.Now().Unix(), OwnedBy: owner, ContextLength: contextLength, Input: input}
}

func (s *Server) models(w http.ResponseWriter, r *http.Request, rc *requestContext) {
	if _, ok := r.Header[http.CanonicalHeaderKey("anthropic-version")]; ok {
		s.messagesModels(w, rc)
		return
	}
	catalogs := s.codexCatalog(r.Context(), rc)
	if r.URL.Query().Has("client_version") {
		if len(catalogs) == 0 {
			writeAPIError(w, dialectOpenAI, http.StatusServiceUnavailable, "api_error", "no usable codex account to read the model catalog from")
			return
		}
		s.writeCodexCatalog(w, rc, catalogs)
		return
	}

	entries := make([]modelEntry, 0)
	seen := make(map[string]bool)
	add := func(entry modelEntry) {
		if entry.ID != "" && !seen[entry.ID] && !s.cfg.Models.Blocked(entry.ID) {
			entries = append(entries, entry)
			seen[entry.ID] = true
		}
	}
	for _, catalog := range catalogs {
		for _, model := range catalog.Entries {
			if s.cfg.Models.Route(model.Slug) == provider.OpenAI {
				add(newModelEntry(model.Slug, "openai", model.ContextWindow, model.InputModalities))
			}
		}
	}
	if len(catalogs) == 0 && s.tokenMayUse(rc, provider.OpenAI) {
		for _, id := range append(append([]string{}, s.cfg.Models.Known...), s.cfg.Models.Default) {
			if s.cfg.Models.Route(id) == provider.OpenAI {
				add(newModelEntry(id, "slop-proxy", nil, nil))
			}
		}
	}
	for _, entry := range s.geminiEntries(rc) {
		add(entry)
	}
	if s.tokenMayUse(rc, provider.Zen) {
		for _, id := range s.catalog(provider.Zen) {
			if s.cfg.Models.Route(id) == provider.Zen && s.cfg.Models.ZenDialect(id) != config.ZenMessages {
				add(newModelEntry(id, "opencode", nil, nil))
			}
		}
	}
	if s.tokenMayUse(rc, provider.Copilot) {
		for _, id := range s.catalog(provider.Copilot) {
			if s.cfg.Models.Route(id) == provider.Copilot {
				add(newModelEntry(id, "github", nil, nil))
			}
		}
	}
	writeJSON(w, map[string]any{"object": "list", "data": entries})
}

func (s *Server) writeCodexCatalog(w http.ResponseWriter, rc *requestContext, catalogs []*backend.CatalogSnapshot) {
	document := make(map[string]json.RawMessage)
	for field, value := range catalogs[0].Raw {
		document[field] = value
	}
	models := make([]json.RawMessage, 0)
	seen := make(map[string]bool)
	for _, catalog := range catalogs {
		var entries []json.RawMessage
		_ = json.Unmarshal(catalog.Raw["models"], &entries)
		for _, entry := range entries {
			var model backend.CodexModel
			if json.Unmarshal(entry, &model) != nil || model.Slug == "" || seen[model.Slug] ||
				s.cfg.Models.Blocked(model.Slug) || s.cfg.Models.Route(model.Slug) != provider.OpenAI {
				continue
			}
			models = append(models, entry)
			seen[model.Slug] = true
		}
	}
	if s.tokenMayUse(rc, provider.Zen) {
		for _, id := range s.catalog(provider.Zen) {
			if id != "" && !seen[id] && !s.cfg.Models.Blocked(id) && s.cfg.Models.Route(id) == provider.Zen &&
				s.cfg.Models.ZenDialect(id) != config.ZenMessages {
				models = append(models, mustMarshal(map[string]any{
					"slug": id, "display_name": id, "description": "OpenCode Zen", "visibility": "list",
					"supported_in_api": true, "input_modalities": []string{"text"},
				}))
				seen[id] = true
			}
		}
	}
	document["models"] = mustMarshal(models)
	writeJSON(w, document)
}

func (s *Server) messagesModels(w http.ResponseWriter, rc *requestContext) {
	s.catalogsMu.RLock()
	anthropic := append([]backend.AnthropicModel(nil), s.anthropicCatalog...)
	s.catalogsMu.RUnlock()
	entries := make([]backend.AnthropicModel, 0)
	seen := make(map[string]bool)
	add := func(item backend.AnthropicModel) {
		if item.ID != "" && !seen[item.ID] && !s.cfg.Models.Blocked(item.ID) {
			seen[item.ID] = true
			entries = append(entries, item)
		}
	}
	if s.tokenMayUse(rc, provider.Anthropic) {
		for _, model := range anthropic {
			if s.cfg.Models.Route(model.ID) == provider.Anthropic {
				add(model)
			}
		}
	}
	created := time.Now().UTC().Format(time.RFC3339)
	for _, p := range []provider.Provider{provider.GLM, provider.DeepSeek, provider.Radiance, provider.Zen} {
		if !s.tokenMayUse(rc, p) {
			continue
		}
		for _, id := range s.catalog(p) {
			if s.cfg.Models.Route(id) == p && (p != provider.Zen || s.cfg.Models.ZenDialect(id) == config.ZenMessages) {
				add(backend.AnthropicModel{ID: id, DisplayName: id, CreatedAt: created, Type: "model"})
			}
		}
	}
	var first, last *string
	if len(entries) > 0 {
		first, last = &entries[0].ID, &entries[len(entries)-1].ID
	}
	writeJSON(w, map[string]any{"data": entries, "has_more": false, "first_id": first, "last_id": last})
}

func (s *Server) geminiModels(w http.ResponseWriter, r *http.Request, rc *requestContext) {
	writeJSON(w, map[string]any{"object": "list", "data": s.geminiEntries(rc)})
}

func (s *Server) geminiEntries(rc *requestContext) []modelEntry {
	entries := make([]modelEntry, 0)
	if !s.tokenMayUse(rc, provider.Gemini) {
		return entries
	}
	s.catalogsMu.RLock()
	defer s.catalogsMu.RUnlock()
	for _, item := range s.geminiCatalog {
		id := strings.TrimPrefix(item.ID, "models/")
		if id != "" && !s.cfg.Models.Blocked(id) && s.cfg.Models.Route(id) == provider.Gemini {
			entries = append(entries, newModelEntry(id, "google", item.ContextWindow, nil))
		}
	}
	return entries
}
