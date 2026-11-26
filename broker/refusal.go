package broker

import (
	"context"
	"errors"
	"time"
)

// RefusalKind is why a broker turned a trade or a change down, the same for every broker: each adapter sorts its own
// error codes into these, so whatever calls it can say why without knowing any broker's wording.
type RefusalKind string

const (
	RefusedMarketClosed    RefusalKind = "market_closed"     // the market isn't open now
	RefusedSizeTooSmall    RefusalKind = "size_too_small"    // below the broker's smallest deal (Limit)
	RefusedSizeTooLarge    RefusalKind = "size_too_large"    // above its largest deal (Limit)
	RefusedNotEnoughMargin RefusalKind = "not_enough_margin" // the account hasn't the margin or funds for it
	RefusedStopTooClose    RefusalKind = "stop_too_close"    // the stop is nearer the price than the broker allows (Limit: the nearest it takes)
	RefusedLevel           RefusalKind = "level"             // a stop, target or entry the broker won't take
	RefusedOther           RefusalKind = "other"             // a "no" the adapter can't place: BrokerSaid has the broker's words
)

// Refused is a broker's "no" to a trade or a change (never a fault of the connection). Error() is the adapter's whole
// error, as before, so anything reading its text still can.
type Refused struct {
	Kind       RefusalKind
	Limit      float64 // the broker's limit when it gave one: the smallest or largest size, the nearest stop
	BrokerSaid string  // the broker's own words or code
	Err        string  // the error as the adapter reported it
}

func (r *Refused) Error() string {
	if r.Err != "" {
		return r.Err
	}
	return string(r.Kind) + ": " + r.BrokerSaid
}

// AsRefused reports whether err is (or wraps) a broker's refusal.
func AsRefused(err error) (*Refused, bool) {
	var r *Refused
	if errors.As(err, &r) {
		return r, true
	}
	return nil, false
}

// ErrAccessGone is an account whose access the broker no longer accepts (a sign-in that ran out or was withdrawn): the
// person must connect it again. Adapters wrap it, so a caller can tell it from a refusal or a fault.
var ErrAccessGone = errors.New("broker: the account's access is gone")

// MarketHours is whether a market is open now and, when it isn't, when it next opens (zero when the broker can't
// say).
type MarketHours struct {
	Open     bool
	NextOpen time.Time
}

// MarketHoursProvider is an optional capability a Port may expose: whether a market is open, and when it next opens.
type MarketHoursProvider interface {
	MarketHours(ctx context.Context, symbol string) (MarketHours, error)
}

// StreamEvent is something an account pushes as it happens: a pair's bid and ask, or something happening to its trades
// (an order filled or cancelled, a position opened, changed or closed: Trade, with nothing else, since what changed is
// read from the account).
type StreamEvent struct {
	Symbol   string
	Bid, Ask float64
	Trade    bool
}

// Streamer is an adapter whose broker pushes prices and what happens to an account's trades (cTrader's Open API).
// Stream gives the prices of the symbols asked for and every trade event of the account, until ctx is done; the channel
// is closed then, or when the connection drops (open it again to carry on). Events are dropped rather than queued when
// the reader falls behind: each price supersedes the last.
type Streamer interface {
	Stream(ctx context.Context, symbols []string) (<-chan StreamEvent, error)
}
