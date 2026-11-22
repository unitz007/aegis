package capital

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/unitz007/aegis/broker"
)

// Capital.com's own codes are sorted into the kinds every broker shares; what it can't place stays "other" with its words.
func TestCapitalRefusal_Kinds(t *testing.T) {
	for _, c := range []struct {
		code  string
		kind  broker.RefusalKind
		limit float64
	}{
		{"error.invalid.size.minvalue: 100", broker.RefusedSizeTooSmall, 100},
		{"error.invalid.size.maxvalue: 5000000", broker.RefusedSizeTooLarge, 5000000},
		{"RC_NOT_ENOUGH_MARGIN", broker.RefusedNotEnoughMargin, 0},
		{"error.invalid.stoploss.minvalue: 1.0712", broker.RefusedStopTooClose, 1.0712},
		{"error.invalid.takeprofit.maxvalue: 1.2", broker.RefusedLevel, 0},
		{"Rejected. EURUSD is currently closed. Timetable in place: UTC", broker.RefusedOther, 0},
	} {
		r := capitalRefusal(c.code, "whole")
		if r.Kind != c.kind || r.Limit != c.limit || r.BrokerSaid != c.code || r.Error() != "whole" {
			t.Errorf("%q: %+v", c.code, r)
		}
	}
}

// A refused request reaches the caller as a refusal through the adapter's own wrapping; a fault of the service doesn't.
func TestCapital_RefusedRequestsAreRefusals(t *testing.T) {
	status := http.StatusBadRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"errorCode":"error.invalid.size.minvalue: 1000"}`))
	}))
	defer srv.Close()
	a := newTestAdapter(srv.URL)
	_, err := a.placeMarketOrder(context.Background(), broker.TradeRequest{Symbol: "EUR/USD", Units: 1000})
	r, ok := broker.AsRefused(err)
	if !ok || r.Kind != broker.RefusedSizeTooSmall || r.Limit != 1000 || !strings.Contains(err.Error(), "HTTP 400") {
		t.Fatalf("refused: %v %+v", err, r)
	}
	status = http.StatusInternalServerError
	_, err = a.placeMarketOrder(context.Background(), broker.TradeRequest{Symbol: "EUR/USD", Units: 1000})
	if _, ok := broker.AsRefused(err); ok || err == nil {
		t.Errorf("a fault isn't a refusal: %v", err)
	}
}

// Closed on a Sunday afternoon, forex next opens at 22:00 that evening; in the gap between Monday's two ranges, at 22:05.
func TestNextOpening(t *testing.T) {
	var hours map[string]json.RawMessage
	_ = json.Unmarshal([]byte(`{"fri":["00:00 - 21:59:50"],"mon":["00:00 - 21:59:50","22:05 - 00:00"],"sat":[],"sun":["22:00 - 00:00"],
		"thu":["00:00 - 21:59:50","22:05 - 00:00"],"tue":["00:00 - 21:59:50","22:05 - 00:00"],"wed":["00:00 - 21:59:50","22:05 - 00:00"],"zone":"UTC"}`), &hours)
	sunday := time.Date(2026, 11, 22, 17, 6, 0, 0, time.UTC)
	if got := nextOpening(hours, sunday); !got.Equal(time.Date(2026, 11, 22, 22, 0, 0, 0, time.UTC)) {
		t.Errorf("Sunday: %v", got)
	}
	saturday := time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC)
	if got := nextOpening(hours, saturday); !got.Equal(time.Date(2026, 11, 22, 22, 0, 0, 0, time.UTC)) {
		t.Errorf("Saturday: %v", got)
	}
	gap := time.Date(2026, 11, 23, 22, 1, 0, 0, time.UTC)
	if got := nextOpening(hours, gap); !got.Equal(time.Date(2026, 11, 23, 22, 5, 0, 0, time.UTC)) {
		t.Errorf("Monday's gap: %v", got)
	}
	if got := nextOpening(nil, sunday); !got.IsZero() {
		t.Errorf("no hours: %v", got)
	}
}
