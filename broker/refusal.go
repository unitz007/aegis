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
