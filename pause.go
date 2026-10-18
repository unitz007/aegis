package aegis

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// ── The pause switch ──────────────────────────────────────────────────────────
//
// Aegis is an execution layer: it places what it is told to place. It makes no trading
// decisions, so it has no size ceilings, loss limits or trade counts: those belong to
// whoever decides to trade (the agent, on the person's behalf). The one thing aegis owns
// is an operator's stop: a switch that halts every new trade until it is turned back on.

// Refusal is an executor "no" that the sender should hear about in plain words. It is
// returned as an error so the verdict says why nothing was placed.
type Refusal struct{ Why string }

func (r *Refusal) Error() string { return r.Why }

// AsRefusal reports whether err is (or wraps) a Refusal.
func AsRefusal(err error) (*Refusal, bool) {
	var r *Refusal
	if errors.As(err, &r) {
		return r, true
	}
	return nil, false
}

func refuse(format string, a ...any) error { return &Refusal{Why: fmt.Sprintf(format, a...)} }

// Guard holds the pause switch. One Guard is shared by every executor, including the
// per-request account ones, so the switch stops everything.
type Guard struct {
	mu     sync.Mutex
	paused bool
	reason string
	since  time.Time
	clock  func() time.Time
}

// NewGuard returns a Guard. clock may be nil (real time).
func NewGuard(clock func() time.Time) *Guard {
	if clock == nil {
		clock = time.Now
	}
	return &Guard{clock: clock}
}

// defaultGuard backs any Executor that wasn't given one, so the pause switch always works
// even for a library user who wired nothing.
var defaultGuard = NewGuard(nil)

func (g *Guard) now() time.Time { return g.clock().UTC() }

// Pause stops all new trades until Resume. Closing existing trades is unaffected.
func (g *Guard) Pause(reason string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if reason == "" {
		reason = "paused"
	}
	g.paused, g.reason, g.since = true, reason, g.now()
}

// Resume allows new trades again.
func (g *Guard) Resume() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.paused, g.reason = false, ""
}

// Status reports whether trading is paused, and why and since when.
func (g *Guard) Status() (paused bool, reason string, since time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.paused, g.reason, g.since
}

func (e *Executor) guard() *Guard {
	if e.Guard != nil {
		return e.Guard
	}
	return defaultGuard
}

// checkPaused is the first thing every trade meets.
func (e *Executor) checkPaused() error {
	if paused, reason, _ := e.guard().Status(); paused {
		if reason != "" && reason != "paused" {
			return refuse("Trading is paused right now (%s), so nothing was placed.", reason)
		}
		return refuse("Trading is paused right now, so nothing was placed.")
	}
	return nil
}
