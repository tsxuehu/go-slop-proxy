package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/edy/slop-proxy-go/internal/provider"
	"github.com/edy/slop-proxy-go/internal/translate"
)

const (
	reservedNamespace = "collaboration"
	proxyNamespace    = "slop_collab"
)

type wsPrepared struct {
	IsCreate       bool
	Encoded        []byte
	RequestedModel string
	ResolvedModel  string
	ServiceTier    string
	Effort         string
	Generate       bool
	StreamID       string
	HasPrevious    bool
	RequestBytes   int64
}

// prepareWSRequest applies the same rewrites Rust's prepare_request applies
// to a response.create frame, while preserving every unknown field through
// json.RawMessage (the Go equivalent of serde's #[serde(flatten)]).
func (s *Server) prepareWSRequest(raw []byte) (*wsPrepared, error) {
	var frame map[string]json.RawMessage
	if err := json.Unmarshal(raw, &frame); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}
	kind, _ := jsonString(frame["type"])
	if kind != "response.create" {
		return &wsPrepared{Encoded: raw}, nil
	}

	requested, _ := jsonString(frame["model"])
	if requested == "" {
		requested = s.cfg.Models.Default
	}
	resolved := translate.Resolve(&s.cfg, requested)
	if s.cfg.Models.Blocked(resolved.Model) {
		return nil, fmt.Errorf("%s is blocked on this proxy", resolved.Model)
	}
	target := s.cfg.Models.Route(resolved.Model)
	if target != provider.OpenAI {
		return nil, fmt.Errorf("this connection only serves OpenAI models")
	}

	serviceTier, _ := jsonString(frame["service_tier"])
	effort := ""
	if _, present := frame["reasoning"]; present {
		var reasoning map[string]json.RawMessage
		if json.Unmarshal(frame["reasoning"], &reasoning) == nil {
			if v, ok := jsonString(reasoning["effort"]); ok && v != "" {
				effort = v
			}
			if effort == "" && resolved.Effort != "" {
				reasoning["effort"] = jsonStringValue(translate.ClampEffort(resolved.Model, resolved.Effort))
				frame["reasoning"] = mustMarshal(reasoning)
				effort = resolved.Effort
			}
		}
	} else if resolved.Effort != "" {
		frame["reasoning"] = mustMarshal(map[string]any{"effort": translate.ClampEffort(resolved.Model, resolved.Effort)})
		effort = resolved.Effort
	}

	if _, present := frame["instructions"]; !present {
		frame["instructions"] = jsonStringValue(s.codexInstructions())
	}
	frame["model"] = jsonStringValue(resolved.Model)
	frame["store"] = json.RawMessage("false")
	delete(frame, "stream")
	delete(frame, "background")

	encoded := renameReservedNamespace(mustMarshal(frame))

	streamID, _ := jsonString(frame["stream_id"])
	generate := true
	if v, ok := frame["generate"]; ok {
		var b bool
		if json.Unmarshal(v, &b) == nil && !b {
			generate = false
		}
	}
	_, hasPrevious := frame["previous_response_id"]

	return &wsPrepared{
		IsCreate: true, Encoded: encoded, RequestedModel: requested, ResolvedModel: resolved.Model,
		ServiceTier: serviceTier, Effort: effort, Generate: generate, StreamID: streamID,
		HasPrevious: hasPrevious, RequestBytes: int64(len(raw)),
	}, nil
}

func renameReservedNamespace(encoded []byte) []byte {
	encoded = bytes.ReplaceAll(encoded,
		[]byte(`"namespace":"`+reservedNamespace+`"`),
		[]byte(`"namespace":"`+proxyNamespace+`"`))
	return bytes.ReplaceAll(encoded,
		[]byte("functions."+reservedNamespace+"."),
		[]byte("functions."+proxyNamespace+"."))
}

// restoreReservedNamespace puts the collaboration namespace back on upstream
// frames, matching Rust's pure string substitution.
func restoreReservedNamespace(text string) string {
	return strings.ReplaceAll(text, `"namespace":"`+proxyNamespace+`"`, `"namespace":"`+reservedNamespace+`"`)
}

func (s *Server) codexInstructions() string {
	if s.cfg.Codex.Instructions != "" {
		return s.cfg.Codex.Instructions
	}
	if s.cfg.Codex.InstructionsFile != "" {
		if b, err := os.ReadFile(s.cfg.Codex.InstructionsFile); err == nil && strings.TrimSpace(string(b)) != "" {
			return string(b)
		}
	}
	return "You are Codex, based on GPT-5. You are running as a coding agent on a user's computer. Answer the user's requests directly and concisely."
}

func jsonString(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 {
		return "", false
	}
	var v string
	if json.Unmarshal(raw, &v) != nil {
		return "", false
	}
	return v, true
}

func jsonStringValue(v string) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func mustMarshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
