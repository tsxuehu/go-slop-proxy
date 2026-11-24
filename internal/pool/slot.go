package pool

import (
	"crypto/sha256"
	"encoding/binary"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/edy/slop-proxy-go/internal/store"
)

type Status uint8

const (
	StatusActive Status = iota
	StatusCooldown
	StatusDisabled
)

type Band uint8

const (
	BandAmple Band = iota
	BandSteady
	BandBehind
	BandSpent
)

type UsageWindow struct {
	Name        string
	Utilization float64
	ResetsAt    int64
}

type AccountUsage struct {
	Windows    []UsageWindow
	Locked     bool
	Capacity   float64
	ObservedAt int64
}

func (u *AccountUsage) Peak() float64 {
	var peak float64
	for _, w := range u.Windows {
		if w.Utilization > peak {
			peak = w.Utilization
		}
	}
	return peak
}

// WindowSeconds parses a quota window name such as "5h" or "30d".
func WindowSeconds(name string) (int64, bool) { return windowSeconds(name) }

func windowSeconds(name string) (int64, bool) {
	if len(name) < 2 {
		return 0, false
	}
	unit := name[len(name)-1]
	var value int64
	for i := 0; i < len(name)-1; i++ {
		c := name[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		value = value*10 + int64(c-'0')
	}
	switch unit {
	case 'm':
		return value * 60, true
	case 'h':
		return value * 3600, true
	case 'd':
		return value * 86400, true
	default:
		return 0, false
	}
}

const minResetSecs = 300

func (u *AccountUsage) Band(softLimit float64, now int64) Band {
	if u.Locked || u.Peak() >= softLimit {
		return BandSpent
	}
	var longest *UsageWindow
	var longestSpan int64
	for i := range u.Windows {
		w := &u.Windows[i]
		if w.ResetsAt == 0 {
			continue
		}
		if span, ok := windowSeconds(w.Name); ok && (longest == nil || span > longestSpan) {
			longest, longestSpan = w, span
		}
	}
	if longest == nil {
		return BandSteady
	}
	resetsIn := longest.ResetsAt - now
	if resetsIn < minResetSecs {
		resetsIn = minResetSecs
	}
	headroom := 1.0 - longest.Utilization
	if headroom < 0 {
		headroom = 0
	}
	slack := headroom * float64(longestSpan) / float64(resetsIn)
	switch {
	case slack >= 2:
		return BandAmple
	case slack >= 1:
		return BandSteady
	default:
		return BandBehind
	}
}

type Credentials struct {
	AccessToken  string
	RefreshToken string
	ExpiresAt    int64
}

type SlotState struct {
	Status             Status
	CooldownUntil      int64
	ConsecutiveFails   int32
	Usage              *AccountUsage
	ModelCooldowns     map[string]int64
	Catalog            []string
	ServiceTiers       map[string][]string
	CatalogAt          int64
	TurnState          string
	TurnStateBlocks    int64
	TurnStateRefusedAt int64
	// LimitWindows holds named sub-limits (metered_limit_name) separately
	// from the account's main quota windows in Usage.
	LimitWindows map[string][]UsageWindow
}

type Slot struct {
	ID                int64
	ProviderAccountID string
	Email             string
	Display           string
	Trusted           bool
	Reserved          bool
	Egress            bool
	AllowedUsers      []string
	AuthMode          string
	Plan              string
	HTTPReferer       string

	mu        sync.Mutex
	creds     Credentials
	state     SlotState
	catalogMu sync.Mutex
}

func NewSlot(a store.Account) *Slot {
	s := &Slot{
		ID: a.ID, ProviderAccountID: a.ProviderAccountID, Email: a.Email, Display: a.Label,
		Trusted: a.Trusted, Reserved: a.Reserved, Egress: a.Egress,
		AuthMode: a.AuthMode, Plan: a.Plan, HTTPReferer: a.HTTPReferer,
		creds: Credentials{a.AccessToken, a.RefreshToken, a.AccessExpiresAt},
		state: SlotState{
			ModelCooldowns: make(map[string]int64),
			LimitWindows:   make(map[string][]UsageWindow),
			TurnState:      a.TurnState,
		},
	}
	if held, ok := ParseTurnState(a.TurnState); ok {
		s.state.TurnStateBlocks = held.Blocks
	}
	if a.Status == "disabled" {
		s.state.Status = StatusDisabled
	} else if a.Status == "cooldown" && a.CooldownUntil > time.Now().Unix() {
		s.state.Status = StatusCooldown
		s.state.CooldownUntil = a.CooldownUntil
	}
	if a.AllowedUsers != "" {
		s.AllowedUsers = strings.Split(a.AllowedUsers, ",")
	}
	return s
}

func (s *Slot) Serves(user string) bool {
	if len(s.AllowedUsers) == 0 {
		return true
	}
	for _, allowed := range s.AllowedUsers {
		if allowed == user {
			return true
		}
	}
	return false
}

func (s *Slot) Snapshot() store.Account {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := store.Account{
		ID: s.ID, Label: s.Display, Email: s.Email, ProviderAccountID: s.ProviderAccountID,
		AuthMode: s.AuthMode, AccessToken: s.creds.AccessToken,
		RefreshToken: s.creds.RefreshToken, AccessExpiresAt: s.creds.ExpiresAt,
		Plan: s.Plan, Trusted: s.Trusted, Reserved: s.Reserved, Egress: s.Egress,
		TurnState: s.state.TurnState,
	}
	switch s.state.Status {
	case StatusDisabled:
		a.Status = "disabled"
	case StatusCooldown:
		a.Status = "cooldown"
		if s.state.CooldownUntil > time.Now().Unix() {
			a.CooldownUntil = s.state.CooldownUntil
		}
	default:
		a.Status = "active"
	}
	return a
}

func (s *Slot) SetUsage(u *AccountUsage) {
	if u == nil {
		return
	}
	u.ObservedAt = time.Now().Unix()
	for _, w := range u.Windows {
		if w.Utilization >= 1.0 {
			u.Locked = true
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Usage = u
}

func rendezvousScore(session string, id int64) uint64 {
	h := sha256.New()
	h.Write([]byte(session))
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(id))
	h.Write(b[:])
	sum := h.Sum(nil)
	return binary.LittleEndian.Uint64(sum[:8])
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }
