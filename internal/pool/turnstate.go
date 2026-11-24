package pool

import (
	"context"
	"log/slog"
	"strings"
	"time"
)

// TurnState is the Fernet-shaped x-codex-turn-state token. The backend routes
// on its ciphertext block count: 10 blocks for a personal account, 11 for a
// flagged one. Parsing is integer arithmetic on the base64 length only.
type TurnState struct {
	Token  string
	Blocks int64
}

const TurnStateSnoozeSecs = 15 * 60

func ParseTurnState(token string) (TurnState, bool) {
	core := strings.TrimRight(token, "=")
	bytes := int64(len(core)) * 3 / 4
	if !strings.HasPrefix(core, "gAAAA") || bytes < 73 || (bytes-57)%16 != 0 {
		return TurnState{}, false
	}
	return TurnState{Token: token, Blocks: (bytes - 57) / 16}, true
}

// Supersedes reports whether this token is at least as clean as the held one
// (a shorter ciphertext wins; an equal one refreshes a stale pin).
func (t TurnState) Supersedes(held TurnState) bool { return t.Blocks <= held.Blocks }

// HeldTurnState returns the cleanest turn-state stored on this account.
func (ss *Slots) HeldTurnState(slot *Slot) (TurnState, bool) {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	raw := slot.state.TurnState
	if raw == "" {
		return TurnState{}, false
	}
	return ParseTurnState(raw)
}

// PreferredTurnState returns the account's own cleaner token when the caller
// presents a dirtier one and a refused pin has gone quiet for 15 minutes.
func (ss *Slots) PreferredTurnState(slot *Slot, presented TurnState) (TurnState, bool) {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	held, ok := ParseTurnState(slot.state.TurnState)
	if !ok || presented.Blocks <= held.Blocks {
		return TurnState{}, false
	}
	if time.Now().Unix() < slot.state.TurnStateRefusedAt+TurnStateSnoozeSecs {
		return TurnState{}, false
	}
	return held, true
}

// RefuseTurnState records that the backend rejected a handshake carrying the
// pinned token; it snoozes pinning on this account for 15 minutes.
func (ss *Slots) RefuseTurnState(slot *Slot) {
	slot.mu.Lock()
	slot.state.TurnStateRefusedAt = time.Now().Unix()
	slot.mu.Unlock()
}

// NoteTurnState persists the cleanest turn-state an account's handshakes have
// returned, across restarts. A successful write clears the snooze.
func (ss *Slots) NoteTurnState(ctx context.Context, slot *Slot, observed TurnState) {
	slot.mu.Lock()
	held, has := ParseTurnState(slot.state.TurnState)
	supersedes := !has || observed.Supersedes(held)
	if supersedes {
		slot.state.TurnState = observed.Token
		slot.state.TurnStateBlocks = observed.Blocks
		slot.state.TurnStateRefusedAt = 0
	}
	slot.mu.Unlock()
	if !supersedes || ss.store == nil {
		return
	}
	if err := ss.store.SetAccountField(ctx, slot.ID, "turn_state", observed.Token); err != nil {
		slog.Warn("persisting turn-state failed", "account", slot.Display, "error", err)
	}
}

// TurnStateBlocks returns the block count of the account's held token.
func (ss *Slots) TurnStateBlocks(slot *Slot) *int64 {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.state.TurnState == "" {
		return nil
	}
	blocks := slot.state.TurnStateBlocks
	return &blocks
}
