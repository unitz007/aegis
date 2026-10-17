package aegis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/unitz007/aegis/broker"
)

// ── Safety limits ─────────────────────────────────────────────────────────────
//
// Aegis has no limits of its own, just as it has no reward-to-risk floor and never
// sizes a trade. The person trading chooses their limits and the sender passes them
// with each trade request, exactly like min_rr; aegis holds the line on whatever it
// is given. A mistake upstream (a wrong size, a runaway loop, a bad day) then costs
// no more than the person said they could afford.

// Limits are one person's own ceilings, sent with a trade request. Zero or absent means
// that limit is not set.
type Limits struct {
	// MaxUnits is the largest single forex or stock trade, in whole units.
	MaxUnits int `json:"max_units,omitempty"`
	// MaxCryptoValue is the largest single crypto trade by value: quantity × entry
	// price, in the instrument's quote currency (USD for the usual pairs).
	MaxCryptoValue float64 `json:"max_crypto_value,omitempty"`
	// MaxOpenPositions is the most open trades plus waiting orders the account may have.
	MaxOpenPositions int `json:"max_open_positions,omitempty"`
	// MaxTradesPerDay is the most new trades the account may get per UTC day.
	MaxTradesPerDay int `json:"max_trades_per_day,omitempty"`
	// MaxDailyLoss, in the account's own currency, stops new trades for the rest of the
	// UTC day once the account is down this much from the value aegis first saw today.
	// (Aegis keeps no history, so a restart starts a fresh day-start value.)
	MaxDailyLoss float64 `json:"max_daily_loss,omitempty"`
}

// Validate reports a limit that is negative or not a number.
func (l Limits) Validate() error {
	bad := func(name string, v float64) error {
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			return fmt.Errorf("%s must be a number, 0 or more", name)
		}
		return nil
	}
	for _, c := range []struct {
		name string
		v    float64
	}{
		{"max_units", float64(l.MaxUnits)}, {"max_crypto_value", l.MaxCryptoValue},
		{"max_open_positions", float64(l.MaxOpenPositions)}, {"max_trades_per_day", float64(l.MaxTradesPerDay)},
		{"max_daily_loss", l.MaxDailyLoss},
	} {
		if err := bad(c.name, c.v); err != nil {
			return err
		}
	}
	return nil
}

// tighten combines two sets of limits, keeping the stricter value of each that is set.
// Used when a library user configures ceilings on the Executor as well as the sender
// passing the person's own.
func tighten(a, b Limits) Limits {
	minInt := func(x, y int) int {
		switch {
		case x == 0:
			return y
		case y == 0:
			return x
		case x < y:
			return x
		}
		return y
	}
	minF := func(x, y float64) float64 {
		switch {
		case x == 0:
			return y
		case y == 0:
			return x
		case x < y:
			return x
		}
		return y
	}
	return Limits{
		MaxUnits:         minInt(a.MaxUnits, b.MaxUnits),
		MaxCryptoValue:   minF(a.MaxCryptoValue, b.MaxCryptoValue),
		MaxOpenPositions: minInt(a.MaxOpenPositions, b.MaxOpenPositions),
		MaxTradesPerDay:  minInt(a.MaxTradesPerDay, b.MaxTradesPerDay),
		MaxDailyLoss:     minF(a.MaxDailyLoss, b.MaxDailyLoss),
	}
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

// checkLimits applies the person's limits (sent with the request) to a trade that is
// otherwise ready to go. It returns the account key to record the trade against once it is placed.
// Anything it cannot check it refuses: a limit that might not hold is not a limit.
func (e *Executor) checkLimits(ctx context.Context, intent OrderIntent, openTrades []broker.OpenTrade, openOrders []broker.OpenOrder) (account string, err error) {
	lim := tighten(e.Limits, intent.Limits)

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
