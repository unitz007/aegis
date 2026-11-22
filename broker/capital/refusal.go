package capital

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/unitz007/aegis/broker"
)

// capitalRefusal sorts Capital.com's own error codes (errorCode on a refused request, the reason on a refused
// confirmation) into the kinds every broker shares. Capital.com's codes, as its API gives them:
// error.invalid.size.minvalue: 100, error.invalid.stoploss.minvalue: 1.0712, RC_NOT_ENOUGH_MARGIN, ….
func capitalRefusal(code, whole string) *broker.Refused {
	r := &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: strings.TrimSpace(code), Err: whole}
	c := strings.ToLower(code)
	limit := func(key string) float64 {
		v, _ := parseCapitalMinValue(c, key)
		return v
	}
	switch {
	case strings.Contains(c, "size.minvalue"):
		r.Kind, r.Limit = broker.RefusedSizeTooSmall, limit("size.minvalue")
	case strings.Contains(c, "size.maxvalue"):
		r.Kind, r.Limit = broker.RefusedSizeTooLarge, limit("size.maxvalue")
	case strings.Contains(c, "margin"), strings.Contains(c, "insufficient"), strings.Contains(c, "funds"):
		r.Kind = broker.RefusedNotEnoughMargin
	case strings.Contains(c, "stoploss.minvalue"):
		r.Kind, r.Limit = broker.RefusedStopTooClose, limit("stoploss.minvalue")
	case strings.Contains(c, "stoploss.maxvalue"):
		r.Kind, r.Limit = broker.RefusedStopTooClose, limit("stoploss.maxvalue")
	case strings.Contains(c, "stoploss"), strings.Contains(c, "stop.distance"), strings.Contains(c, "profitlevel"),
		strings.Contains(c, "takeprofit"), strings.Contains(c, "profit.distance"), strings.Contains(c, "invalid.level"):
		r.Kind = broker.RefusedLevel
	case strings.Contains(c, "market.closed"), strings.Contains(c, "market_closed"):
		r.Kind = broker.RefusedMarketClosed
	}
	return r
}

// refusalOf is a refused HTTP answer (4xx with a body) as a refusal, its code read from errorCode.
func refusalOf(raw []byte, whole string) *broker.Refused {
	var body struct {
		ErrorCode string `json:"errorCode"`
	}
	code := strings.TrimSpace(string(raw))
	if json.Unmarshal(raw, &body) == nil && body.ErrorCode != "" {
		code = body.ErrorCode
	}
	return capitalRefusal(code, whole)
}

// rejectedDeal is a refused confirmation (dealStatus REJECTED) as a refusal.
func rejectedDeal(format, why string, args ...any) error {
	return capitalRefusal(why, fmt.Sprintf(format, append(args, why)...))
}

// MarketHours says whether a market is open now (Capital.com's marketStatus) and when it next opens, from its opening
// hours (instrument.openingHours: each day's ranges, "22:05 - 00:00", in its zone). Implements broker.MarketHoursProvider.
func (a *Adapter) MarketHours(ctx context.Context, symbol string) (broker.MarketHours, error) {
	if err := a.ensureSession(ctx); err != nil {
		return broker.MarketHours{}, err
	}
	var resp struct {
		Instrument struct {
			OpeningHours map[string]json.RawMessage `json:"openingHours"`
		} `json:"instrument"`
		Snapshot struct {
			MarketStatus string `json:"marketStatus"`
		} `json:"snapshot"`
	}
	if err := a.get(ctx, "/api/v1/markets/"+capitalEpic(symbol), &resp); err != nil {
		return broker.MarketHours{}, fmt.Errorf("capital market hours %s: %w", symbol, err)
	}
	h := broker.MarketHours{Open: strings.EqualFold(resp.Snapshot.MarketStatus, "TRADEABLE")}
	if !h.Open {
		h.NextOpen = nextOpening(resp.Instrument.OpeningHours, time.Now())
	}
	return h, nil
}

// nextOpening is the first time after now that one of the day's ranges starts, in the hours' own zone (UTC when it
// names none); zero when the hours can't be read. Closed now, the next range to start is the opening.
func nextOpening(hours map[string]json.RawMessage, now time.Time) time.Time {
	loc := time.UTC
	if raw, ok := hours["zone"]; ok {
		var zone string
		if json.Unmarshal(raw, &zone) == nil && zone != "" {
			if l, err := time.LoadLocation(zone); err == nil {
				loc = l
			}
		}
	}
	days := []string{"sun", "mon", "tue", "wed", "thu", "fri", "sat"}
	local := now.In(loc)
	for i := 0; i < 8; i++ {
		day := local.AddDate(0, 0, i)
		var ranges []string
		if raw, ok := hours[days[day.Weekday()]]; !ok || json.Unmarshal(raw, &ranges) != nil {
			continue
		}
		for _, r := range ranges {
			start, ok := clock(strings.TrimSpace(strings.SplitN(r, "-", 2)[0]))
			if !ok {
				continue
			}
			at := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, loc).Add(start)
			if at.After(local) {
				return at
			}
		}
	}
	return time.Time{}
}

// clock reads "22:05" or "21:59:50" as a time of day.
func clock(s string) (time.Duration, bool) {
	parts := strings.Split(s, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, false
	}
	var d time.Duration
	for i, unit := range []time.Duration{time.Hour, time.Minute, time.Second}[:len(parts)] {
		n, err := strconv.Atoi(parts[i])
		if err != nil || n < 0 {
			return 0, false
		}
		d += time.Duration(n) * unit
	}
	return d, true
}
