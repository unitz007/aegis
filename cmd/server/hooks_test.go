package main

import (
	"testing"
	"time"
)

// TestStockAISessionGate_FullMode tests the default "full" Asian-session gate
// (FOREX_ASIAN_GATE unset or "full"): both 00:00–07:00 AND 21:00–24:00 are blocked
// for non-Asian forex pairs.
func TestStockAISessionGate_FullMode(t *testing.T) {
	// Save and restore the package-level mode var so parallel tests are safe.
	saved := asianGateLateOnly
	asianGateLateOnly = false // explicit full mode
	defer func() { asianGateLateOnly = saved }()

	cases := []struct {
		name       string
		symbol     string
		assetClass string
		now        time.Time
		want       bool // true = allowed to execute, false = gated/vetoed
	}{
		// EURUSD — not Asian-exempt —————————————————————————————————————————
		{
			name:       "EURUSD_02:00_gated",
			symbol:     "EURUSD",
			assetClass: "forex",
			now:        time.Date(2026, 6, 10, 2, 0, 0, 0, time.UTC),
			want:       false, // 00:00–07:00 window — blocked in full mode
		},
		{
			name:       "EURUSD_22:00_gated",
			symbol:     "EURUSD",
			assetClass: "forex",
			now:        time.Date(2026, 6, 10, 22, 0, 0, 0, time.UTC),
			want:       false, // 21:00–24:00 window — always blocked
		},
		{
			name:       "EURUSD_12:00_allowed",
			symbol:     "EURUSD",
			assetClass: "forex",
			now:        time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC),
			want:       true, // London session — allowed
		},
		// USDJPY — Asian-exempt in ALL modes ————————————————————————————————
		{
			name:       "USDJPY_02:00_allowed",
			symbol:     "USDJPY",
			assetClass: "forex",
			now:        time.Date(2026, 6, 10, 2, 0, 0, 0, time.UTC),
			want:       true, // JPY-leg pair always allowed during Asian hours
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := StockAISessionGate(tc.symbol, tc.assetClass, tc.now)
			if got != tc.want {
				t.Errorf("StockAISessionGate(%q, %q, %s) full mode = %v, want %v",
					tc.symbol, tc.assetClass, tc.now.UTC().Format("15:04"), got, tc.want)
			}
		})
	}
}

// TestStockAISessionGate_LateOnlyMode tests the surgical "late_only" mode
// (FOREX_ASIAN_GATE=late_only): ONLY 21:00–24:00 is blocked; 00:00–07:00 is
// fully allowed for all pairs including non-Asian ones.
func TestStockAISessionGate_LateOnlyMode(t *testing.T) {
	saved := asianGateLateOnly
	asianGateLateOnly = true // surgical mode
	defer func() { asianGateLateOnly = saved }()

	cases := []struct {
		name       string
		symbol     string
		assetClass string
		now        time.Time
		want       bool
	}{
		// EURUSD — the recovered early-morning window ————————————————————————
		{
			name:       "EURUSD_02:00_allowed",
			symbol:     "EURUSD",
			assetClass: "forex",
			now:        time.Date(2026, 6, 10, 2, 0, 0, 0, time.UTC),
			want:       true, // 00:00–07:00 — UNBLOCKED in late_only mode
		},
		{
			name:       "EURUSD_06:59_allowed",
			symbol:     "EURUSD",
			assetClass: "forex",
			now:        time.Date(2026, 6, 10, 6, 59, 0, 0, time.UTC),
			want:       true, // still in early window — allowed
		},
		{
			name:       "EURUSD_07:00_allowed",
			symbol:     "EURUSD",
			assetClass: "forex",
			now:        time.Date(2026, 6, 10, 7, 0, 0, 0, time.UTC),
			want:       true, // 07:00 — London pre-market, allowed in both modes
		},
		{
			name:       "EURUSD_12:00_allowed",
			symbol:     "EURUSD",
			assetClass: "forex",
			now:        time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC),
			want:       true, // London session — always allowed
		},
		{
			name:       "EURUSD_20:59_allowed",
			symbol:     "EURUSD",
			assetClass: "forex",
			now:        time.Date(2026, 6, 10, 20, 59, 0, 0, time.UTC),
			want:       true, // one minute before the evening gate — allowed
		},
		{
			name:       "EURUSD_22:00_gated",
			symbol:     "EURUSD",
			assetClass: "forex",
			now:        time.Date(2026, 6, 10, 22, 0, 0, 0, time.UTC),
			want:       false, // 21:00–24:00 — STILL gated in late_only mode
		},
		// USDJPY during the evening gate — Asian-exempt, so still allowed ———
		{
			name:       "USDJPY_22:00_allowed",
			symbol:     "USDJPY",
			assetClass: "forex",
			now:        time.Date(2026, 6, 10, 22, 0, 0, 0, time.UTC),
			want:       true, // JPY-leg always allowed within any gated window
		},
		// Crypto / stocks are always allowed regardless of mode ——————————————
		{
			name:       "BTCUSD_02:00_allowed",
			symbol:     "BTCUSD",
			assetClass: "crypto",
			now:        time.Date(2026, 6, 10, 2, 0, 0, 0, time.UTC),
			want:       true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := StockAISessionGate(tc.symbol, tc.assetClass, tc.now)
			if got != tc.want {
				t.Errorf("StockAISessionGate(%q, %q, %s) late_only mode = %v, want %v",
					tc.symbol, tc.assetClass, tc.now.UTC().Format("15:04"), got, tc.want)
			}
		})
	}
}
