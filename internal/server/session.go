package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/edy/slop-proxy-go/internal/pool"
	"github.com/edy/slop-proxy-go/internal/translate"
	"github.com/google/uuid"
)

func stableSession(user string, payload []byte) string {
	digest := sha256.Sum256(append(append([]byte(user), 0), payload...))
	return hex.EncodeToString(digest[:8])
}

func (s *Server) translatedRoute(r *http.Request, body []byte, rc *requestContext, request *translate.ResponsesRequest) pool.Route {
	route := s.route(r, body, rc, request.Model)
	if route.SessionKey == "" {
		seed := []byte(rc.user + request.Instructions)
		if len(request.Input) > 0 {
			seed = append(seed, mustMarshal(request.Input[0])...)
		}
		digest := sha256.Sum256(seed)
		var key uuid.UUID
		copy(key[:], digest[:])
		key[6] = (key[6] & 0x0f) | 0x40
		key[8] = (key[8] & 0x3f) | 0x80
		route.SessionKey, rc.sessionKey = key.String(), key.String()
	}
	request.PromptCacheKey = &route.SessionKey
	if route.ServiceTier != "" {
		request.ServiceTier = &route.ServiceTier
	}
	return route
}

func (s *Server) anthropicRoute(r *http.Request, body []byte, rc *requestContext, model string) pool.Route {
	route := s.route(r, body, rc, model)
	var peek struct {
		Speed    string            `json:"speed"`
		System   json.RawMessage   `json:"system"`
		Messages []json.RawMessage `json:"messages"`
		Metadata struct {
			UserID string `json:"user_id"`
		} `json:"metadata"`
	}
	if json.Unmarshal(body, &peek) != nil {
		return route
	}
	route.APIKeyOnly = peek.Speed == "fast"
	if rc.sessionKey == "" {
		rc.sessionKey = peek.Metadata.UserID
		if rc.sessionKey == "" {
			rc.sessionKey = "sys-" + stableSession(rc.user, peek.System)
		}
		route.SessionKey = rc.sessionKey
	}
	if len(peek.Messages) > 0 {
		var opening struct {
			Content json.RawMessage `json:"content"`
		}
		if json.Unmarshal(peek.Messages[0], &opening) == nil && len(opening.Content) > 0 {
			route.SessionKey = stableSession(rc.user, opening.Content)
		}
	}
	return route
}
