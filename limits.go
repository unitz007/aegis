package aegis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/unitz007/aegis/broker"
)

// ── Safety limits ─────────────────────────────────────────────────────────────
//
// Whatever a sender asks, aegis will not go past these. They exist so that a
// mistake upstream (a wrong size, a runaway loop, a bad day) costs a little, not
// everything. Zero switches a limit off.

// Limits are the hard ceilings on what aegis will place.
type Limits struct {
	// MaxUnits is the largest single forex or stock trade, in whole units.
	MaxUnits int
	// MaxCryptoValue is the largest single crypto trade by value: quantity × entry
	// price, in the instrument's quote currency (USD for the usual pairs).
	MaxCryptoValue float64
	// MaxOpenPositions is the most open trades plus waiting orders one account may have.
	MaxOpenPositions int
	// MaxTradesPerDay is the most new trades one account may get per UTC day.
	MaxTradesPerDay int
	// MaxDailyLoss, in the account's own currency, stops new trades for the rest of the
	// UTC day once the account is down this much from the value aegis first saw today.
	// (Aegis keeps no history, so a restart starts a fresh day-start value.)
	MaxDailyLoss float64
}

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

// ── Guard: the state limits and the pause switch share ────────────────────────

// Guard holds what must outlive a single request: the pause switch and each account's
// count for the day. One Guard is shared by every executor, including the per-request
// account ones, so the switch stops everything and an account's count is one count.
type Guard struct {
	mu     sync.Mutex
	paused bool
	reason string
	since  time.Time
	days   map[string]*dayState
	clock  func() time.Time
}

type dayState struct {
	day      string // UTC date, e.g. 2026-10-18
	trades   int
	startNAV float64
}

// NewGuard returns a Guard. clock may be nil (real time).
func NewGuard(clock func() time.Time) *Guard {
	if clock == nil {
		clock = time.Now
	}
	return &Guard{days: map[string]*dayState{}, clock: clock}
}

// defaultGuard backs any Executor that wasn't given one, so the pause switch and the
// day counts always work even for a library user who wired nothing.
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

// today returns the account's record for the current UTC day, starting a fresh one
// when the day has changed. Caller holds g.mu.
func (g *Guard) today(account string) *dayState {
	day := g.now().Format("2006-01-02")
	d := g.days[account]
	if d == nil || d.day != day {
		d = &dayState{day: day}
		g.days[account] = d
	}
	return d
}

func (g *Guard) recordPlaced(account string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.today(account).trades++
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

// checkLimits applies the size and account limits to a trade that is otherwise ready
// to go. It returns the account key to record the trade against once it is placed.
// Anything it cannot check it refuses: a limit that might not hold is not a limit.
func (e *Executor) checkLimits(ctx context.Context, intent OrderIntent, openTrades []broker.OpenTrade, openOrders []broker.OpenOrder) (account string, err error) {
	lim := e.Limits

	if intent.AssetClass == "crypto" {
		if lim.MaxCryptoValue > 0 {
			if value := intent.Size * intent.EntryPrice; value > lim.MaxCryptoValue {
				return "", refuse("That trade is bigger than the largest allowed (about %.0f in value), so nothing was placed.", lim.MaxCryptoValue)
			}
		}
	} else if lim.MaxUnits > 0 && intent.Size > float64(lim.MaxUnits) {
		return "", refuse("That trade is bigger than the largest allowed (%d units), so nothing was placed.", lim.MaxUnits)
	}

	if lim.MaxOpenPositions > 0 && len(openTrades)+len(openOrders) >= lim.MaxOpenPositions {
		return "", refuse("You already have the most open trades allowed (%d), so nothing was placed.", lim.MaxOpenPositions)
	}

	if lim.MaxTradesPerDay <= 0 && lim.MaxDailyLoss <= 0 {
		return "", nil
	}
	acct, aerr := e.Broker.Account(ctx)
	if aerr != nil {
		e.logger().Printf("executor: %s cannot read the account to check limits — skip: %v", intent.Symbol, aerr)
		return "", refuse("I couldn't check your account just now, so nothing was placed. Please try again in a moment.")
	}
	account = acct.AccountID
	if account == "" {
		account = e.BrokerName
	}

	g := e.guard()
	g.mu.Lock()
	defer g.mu.Unlock()
	d := g.today(account)
	if lim.MaxTradesPerDay > 0 && d.trades >= lim.MaxTradesPerDay {
		return "", refuse("The daily limit of %d new trades has been reached, so nothing was placed. It resets at midnight UTC.", lim.MaxTradesPerDay)
	}
	if lim.MaxDailyLoss > 0 {
		if d.startNAV == 0 {
			d.startNAV = acct.NAV
		}
		if loss := d.startNAV - acct.NAV; loss >= lim.MaxDailyLoss {
			return "", refuse("The daily loss limit (%.0f %s) has been reached, so no new trades will be placed today.", lim.MaxDailyLoss, acct.Currency)
		}
	}
	return account, nil
}
