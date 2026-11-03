package capital

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// History reads what each deal did and why from Capital.com's activity, and what closed trades made from its
// transactions, asking for as far back as was asked (a day at most).
func TestHistory_WhatEachDealDidAndWhatItMade(t *testing.T) {
	var periods []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		periods = append(periods, r.URL.Query().Get("lastPeriod"))
		switch r.URL.Path {
		case "/api/v1/history/activity":
			_ = json.NewEncoder(w).Encode(map[string]any{"activities": []map[string]any{
				{"date": "2026-11-02T18:10:05.123", "dateUTC": "2026-11-02T17:10:05.123", "epic": "EURGBP", "dealId": "d1", "source": "SL", "type": "POSITION", "status": "ACCEPTED"},
				{"dateUTC": "2026-11-02T17:00:00.000", "epic": "USDJPY", "dealId": "o1", "source": "SYSTEM", "type": "WORKING_ORDER", "status": "EXECUTED"},
				{"dateUTC": "2026-11-02T16:00:00", "epic": "GOLD", "dealId": "d2", "source": "TP", "type": "POSITION", "status": "ACCEPTED"},
			}})
		case "/api/v1/history/transactions":
			if r.URL.Query().Get("type") != "TRADE" {
				t.Errorf("only trades are asked for: %s", r.URL.RawQuery)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"transactions": []map[string]any{
				{"dateUtc": "2026-11-02T17:10:05.200", "instrumentName": "EUR/GBP", "transactionType": "TRADE", "note": "Trade closed", "reference": "r1", "size": "-12.40", "currency": "EUR"},
				{"dateUtc": "2026-11-02T17:11:00.000", "instrumentName": "EUR/GBP", "transactionType": "DEPOSIT", "size": "100", "currency": "EUR"},
			}})
		default:
			t.Errorf("unexpected %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	h, err := newTestAdapter(srv.URL).History(context.Background(), time.Now().Add(-3*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(periods) != 2 || periods[0] != "86400" {
		t.Errorf("a day back at most: %v", periods)
	}
	if len(h.Activities) != 3 {
		t.Fatalf("activities: %+v", h.Activities)
	}
	a := h.Activities[0]
	if a.DealID != "d1" || a.Symbol != "EUR/GBP" || a.Kind != "position" || a.By != "stop_loss" || a.Status != "accepted" ||
		!a.At.Equal(time.Date(2026, 11, 2, 17, 10, 5, 123e6, time.UTC)) {
		t.Errorf("a stop hit: %+v", a)
	}
	if o := h.Activities[1]; o.Kind != "order" || o.Status != "executed" || o.By != "broker" || o.Symbol != "USD/JPY" {
		t.Errorf("an order filled: %+v", o)
	}
	if g := h.Activities[2]; g.By != "take_profit" || g.Symbol != "GOLD" || g.At.IsZero() {
		t.Errorf("a target hit, time without milliseconds: %+v", g)
	}
	if len(h.Results) != 1 || h.Results[0].Amount != -12.40 || h.Results[0].Currency != "EUR" || h.Results[0].Symbol != "EUR/GBP" {
		t.Errorf("what the closed trade made, trades only: %+v", h.Results)
	}

	// A short look back still asks for a minute.
	periods = nil
	if _, err := newTestAdapter(srv.URL).History(context.Background(), time.Now().Add(-5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if periods[0] != "60" {
		t.Errorf("a minute at least: %v", periods)
	}
}
