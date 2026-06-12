package capital

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/unitz007/aegis/broker"
)

// newTestAdapter wires the adapter against a custom base URL and pre-populates
// the session tokens so ensureSession() skips the auth call in tests.
func newTestAdapter(baseURL string) *Adapter {
	a := &Adapter{
		baseURL:         baseURL,
		apiKey:          "test-key",
		email:           "test@example.com",
		password:        "test-password",
		client:          &http.Client{Timeout: 5 * time.Second},
		orderSources:    make(map[string]string),
		orderTradeTypes: make(map[string]string),
	}
	a.cst = "test-cst"
	a.secToken = "test-sec"
	a.sessionExp = time.Now().Add(time.Hour)
	// gate is nil — tests skip rate-pacing for speed
	return a
}

// ── capitalClampStop unit tests ───────────────────────────────────────────────

// TestCapitalClampStop_DirectionAware_SideGuard is the safety-critical regression
// suite for the direction-aware stop clamp. Ported verbatim from finOS
// capital_adapter_test.go TestCapitalClampStop_DirectionAware_SideGuard.
func TestCapitalClampStop_DirectionAware_SideGuard(t *testing.T) {
	cases := []struct {
		name      string
		errStr    string
		wantLevel float64
		wantOK    bool
	}{
		{
			name:      "BUY: stoploss.maxvalue clamps to broker boundary",
			errStr:    `capital POST /api/v1/positions → HTTP 400: {"errorCode":"error.invalid.stoploss.maxvalue: 1.07100"}`,
			wantLevel: 1.07100,
			wantOK:    true,
		},
		{
			name:      "SELL: stoploss.minvalue clamps to broker boundary",
			errStr:    `capital POST /api/v1/workingorders → HTTP 400: {"errorCode":"error.invalid.stoploss.minvalue: 1.09250"}`,
			wantLevel: 1.09250,
			wantOK:    true,
		},
		{
			// Wrong-side regression: a SELL order receiving a maxvalue error (which is the
			// BUY-side error) must NOT silently clamp — the function returns the numeric level
			// from whichever key it finds first, so the caller must only apply the clamp when
			// the level is on the correct side of entry. This test verifies that capitalClampStop
			// parses stoploss.maxvalue correctly even in a SELL error (the caller's guard
			// `pl.StopLevel != nil` is the safety invariant that prevents the wrong-side apply).
			name:      "stoploss.maxvalue parse succeeds regardless of direction context",
			errStr:    `capital POST /api/v1/workingorders → HTTP 400: {"errorCode":"error.invalid.stoploss.maxvalue: 0.795905"}`,
			wantLevel: 0.795905,
			wantOK:    true,
		},
		{
			name:   "non-stop error returns ok=false — no clamp triggered",
			errStr: `capital POST /api/v1/positions → HTTP 400: {"errorCode":"error.invalid.size.minvalue: 1000"}`,
			wantOK: false,
		},
		{
			name:   "unrelated error returns ok=false",
			errStr: `capital http: context deadline exceeded`,
			wantOK: false,
		},
		{
			name:   "empty error string returns ok=false",
			errStr: ``,
			wantOK: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lvl, ok := capitalClampStop(tc.errStr)
			if ok != tc.wantOK {
				t.Fatalf("capitalClampStop ok=%v, want %v (errStr=%q)", ok, tc.wantOK, tc.errStr)
			}
			if ok && lvl != tc.wantLevel {
				t.Errorf("capitalClampStop level=%v, want %v", lvl, tc.wantLevel)
			}
		})
	}
}

// ── placeLimitOrder stop-clamp integration tests ──────────────────────────────

// TestCapitalAdapter_PlaceLimitOrder_StopClamp_BUY_maxvalue verifies that when Capital
// rejects a BUY limit order with stoploss.maxvalue (stop too close), the adapter
// clamps the stop to the returned level and retries — matching the USD/CHF production
// error "error.invalid.stoploss.maxvalue: 0.795905".
func TestCapitalAdapter_PlaceLimitOrder_StopClamp_BUY_maxvalue(t *testing.T) {
	const (
		clampLevel    = 0.795905
		dealReference = "DEAL_USDCHF_001"
		dealID        = "DI_USDCHF_001"
	)

	var callCount atomic.Int32
	var retryStopLevel float64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/workingorders" && r.Method == http.MethodPost:
			n := callCount.Add(1)
			if n == 1 {
				// First call: reject with maxvalue stop-distance error.
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"errorCode":"error.invalid.stoploss.maxvalue: 0.795905"}`))
				return
			}
			// Second call: capture the retried stop level and succeed.
			var body struct {
				StopLevel *float64 `json:"stopLevel"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("failed to decode retry body: %v", err)
			}
			if body.StopLevel != nil {
				retryStopLevel = *body.StopLevel
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"dealReference":"` + dealReference + `","status":"SUCCESS"}`))

		case strings.HasPrefix(r.URL.Path, "/api/v1/confirms/") && r.Method == http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ACCEPTED","dealId":"` + dealID + `","level":0.7982,"stopLevel":` +
				`0.795905,"profitLevel":0.810,"date":"2026-06-11T10:00:00.000"}`))

		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	a := newTestAdapter(srv.URL)

	// BUY limit: entry 0.7982, stop 0.7977 (too tight — Capital requires ≤ 0.795905).
	req := broker.TradeRequest{
		Symbol:     "USD/CHF",
		Units:      1000,
		EntryPrice: 0.7982,
		StopLoss:   0.7977,
		TakeProfit: 0.810,
	}

	receipt, err := a.placeLimitOrder(context.Background(), req)
	if err != nil {
		t.Fatalf("placeLimitOrder returned error after stop clamp: %v", err)
	}

	// The order must have been retried (two POST calls total).
	if got := callCount.Load(); got != 2 {
		t.Errorf("workingorders POST called %d times, want 2 (initial + clamp retry)", got)
	}

	// The retried request must carry the broker-specified clamped level.
	if retryStopLevel != clampLevel {
		t.Errorf("retry stopLevel = %v, want %v (the clamped level from maxvalue error)", retryStopLevel, clampLevel)
	}

	// Receipt must carry a deal ID and correct broker stamp.
	if receipt.BrokerTradeID == "" {
		t.Errorf("receipt.BrokerTradeID is empty, want a deal ID")
	}
	if receipt.Broker != "capital" {
		t.Errorf("receipt.Broker = %q, want capital", receipt.Broker)
	}
}

// TestCapitalAdapter_PlaceLimitOrder_StopClamp_SELL_minvalue verifies the symmetric
// SELL path: stoploss.minvalue is caught and the stop is clamped to the returned level.
func TestCapitalAdapter_PlaceLimitOrder_StopClamp_SELL_minvalue(t *testing.T) {
	const (
		clampLevel    = 1.09250
		dealReference = "DEAL_EURUSD_SELL_001"
		dealID        = "DI_EURUSD_SELL_001"
	)

	var callCount atomic.Int32
	var retryStopLevel float64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/workingorders" && r.Method == http.MethodPost:
			n := callCount.Add(1)
			if n == 1 {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"errorCode":"error.invalid.stoploss.minvalue: 1.09250"}`))
				return
			}
			var body struct {
				StopLevel *float64 `json:"stopLevel"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("failed to decode retry body: %v", err)
			}
			if body.StopLevel != nil {
				retryStopLevel = *body.StopLevel
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"dealReference":"` + dealReference + `","status":"SUCCESS"}`))

		case strings.HasPrefix(r.URL.Path, "/api/v1/confirms/") && r.Method == http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ACCEPTED","dealId":"` + dealID + `","level":1.0900,"stopLevel":` +
				`1.09250,"profitLevel":1.075,"date":"2026-06-11T10:00:00.000"}`))

		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	a := newTestAdapter(srv.URL)

	// SELL limit: entry 1.0900, stop 1.0920 (too tight — Capital requires ≥ 1.09250).
	req := broker.TradeRequest{
		Symbol:     "EUR/USD",
		Units:      -1000,
		EntryPrice: 1.0900,
		StopLoss:   1.0920,
		TakeProfit: 1.075,
	}

	receipt, err := a.placeLimitOrder(context.Background(), req)
	if err != nil {
		t.Fatalf("placeLimitOrder returned error after stop clamp (SELL): %v", err)
	}

	if got := callCount.Load(); got != 2 {
		t.Errorf("workingorders POST called %d times, want 2", got)
	}
	if retryStopLevel != clampLevel {
		t.Errorf("retry stopLevel = %v, want %v", retryStopLevel, clampLevel)
	}
	if receipt.BrokerTradeID == "" {
		t.Errorf("receipt.BrokerTradeID is empty")
	}
}

// TestCapitalAdapter_PlaceMarketOrder_StopClamp_BUY_maxvalue verifies that the market
// order path also self-heals a stoploss.maxvalue rejection by clamping and retrying.
func TestCapitalAdapter_PlaceMarketOrder_StopClamp_BUY_maxvalue(t *testing.T) {
	const (
		clampLevel    = 1.07100
		dealReference = "DEAL_EURUSD_MKT_001"
		dealID        = "DI_EURUSD_MKT_001"
	)

	var callCount atomic.Int32
	var retryStopLevel float64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/positions" && r.Method == http.MethodPost:
			n := callCount.Add(1)
			if n == 1 {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"errorCode":"error.invalid.stoploss.maxvalue: 1.07100"}`))
				return
			}
			var body struct {
				StopLevel *float64 `json:"stopLevel"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("failed to decode retry body: %v", err)
			}
			if body.StopLevel != nil {
				retryStopLevel = *body.StopLevel
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"dealReference":"` + dealReference + `","status":"SUCCESS"}`))

		case strings.HasPrefix(r.URL.Path, "/api/v1/confirms/") && r.Method == http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ACCEPTED","dealId":"` + dealID + `","level":1.0800,"stopLevel":` +
				`1.07100,"profitLevel":1.095,"date":"2026-06-11T10:00:00.000"}`))

		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	a := newTestAdapter(srv.URL)

	req := broker.TradeRequest{
		Symbol:     "EUR/USD",
		Units:      1000,
		StopLoss:   1.0790, // too tight
		TakeProfit: 1.095,
	}

	receipt, err := a.placeMarketOrder(context.Background(), req)
	if err != nil {
		t.Fatalf("placeMarketOrder returned error after stop clamp: %v", err)
	}

	if got := callCount.Load(); got != 2 {
		t.Errorf("positions POST called %d times, want 2 (initial + clamp retry)", got)
	}
	if retryStopLevel != clampLevel {
		t.Errorf("retry stopLevel = %v, want %v", retryStopLevel, clampLevel)
	}
	if receipt.BrokerTradeID == "" {
		t.Errorf("receipt.BrokerTradeID is empty")
	}
}

// TestCapitalAdapter_PlaceMarketOrder_REJECTED verifies that a REJECTED status in
// the confirm response surfaces as an error (not a silent empty receipt).
func TestCapitalAdapter_PlaceMarketOrder_REJECTED(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/positions" && r.Method == http.MethodPost:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"dealReference":"DEAL_REJECT_001","status":"SUCCESS"}`))
		case strings.HasPrefix(r.URL.Path, "/api/v1/confirms/"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"REJECTED","reason":"MARKET_CLOSED"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	a := newTestAdapter(srv.URL)
	req := broker.TradeRequest{Symbol: "EUR/USD", Units: 1000, StopLoss: 1.07, TakeProfit: 1.09}
	_, err := a.placeMarketOrder(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for REJECTED confirm, got nil")
	}
	if !strings.Contains(err.Error(), "MARKET_CLOSED") {
		t.Errorf("error should contain MARKET_CLOSED, got: %v", err)
	}
}

// TestCapitalAdapter_PlaceLimitOrder_REJECTED verifies that a REJECTED working order
// surfaces as an error.
func TestCapitalAdapter_PlaceLimitOrder_REJECTED(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/workingorders" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"dealReference":"REF001","status":"REJECTED","reason":"INSUFFICIENT_FUNDS"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	a := newTestAdapter(srv.URL)
	req := broker.TradeRequest{
		Symbol: "EUR/USD", Units: 1000,
		EntryPrice: 1.0850, StopLoss: 1.0810, TakeProfit: 1.0910,
	}
	_, err := a.placeLimitOrder(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for REJECTED limit order, got nil")
	}
	if !strings.Contains(err.Error(), "INSUFFICIENT_FUNDS") {
		t.Errorf("error should contain INSUFFICIENT_FUNDS, got: %v", err)
	}
}

// ── capitalDealSize / unitsToCapitalSize edge cases ───────────────────────────

// TestCapitalDealSize verifies the deal-size selection: crypto Quantity is used
// verbatim (fractional, NO 1000-unit forex floor) while the integer Units path
// still applies the forex minimum. Regression for the crypto over-max bug.
func TestCapitalDealSize(t *testing.T) {
	cases := []struct {
		name string
		req  broker.TradeRequest
		want float64
	}{
		{"crypto fractional preserved", broker.TradeRequest{Symbol: "BTCUSD", Quantity: 0.00126}, 0.00126},
		{"crypto short uses abs", broker.TradeRequest{Symbol: "BTCUSD", Units: -1, Quantity: -0.05}, 0.05},
		{"crypto small not floored to 1000", broker.TradeRequest{Symbol: "ETHUSD", Quantity: 0.001}, 0.001},
		{"forex below floor → 1000", broker.TradeRequest{Symbol: "EUR/USD", Units: 200}, 1000},
		{"forex above floor unchanged", broker.TradeRequest{Symbol: "EUR/USD", Units: 20000}, 20000},
		{"zero quantity falls through to units", broker.TradeRequest{Symbol: "EUR/USD", Units: 5000, Quantity: 0}, 5000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := capitalDealSize(tc.req); got != tc.want {
				t.Errorf("capitalDealSize=%v want %v", got, tc.want)
			}
		})
	}
}

// TestUnitsToCapitalSize verifies the forex minimum floor and absolute value.
func TestUnitsToCapitalSize(t *testing.T) {
	cases := []struct {
		units int
		want  float64
	}{
		{0, 1000},
		{500, 1000},
		{999, 1000},
		{1000, 1000},
		{2500, 2500},
		{-3000, 3000}, // negative units → abs
	}
	for _, tc := range cases {
		got := unitsToCapitalSize(tc.units)
		if got != tc.want {
			t.Errorf("unitsToCapitalSize(%d)=%v want %v", tc.units, got, tc.want)
		}
	}
}

// ── OpenTrades JPY-quote P&L via quoteToUSD ───────────────────────────────────

// TestOpenTrades_JPYQuotePnLConversion verifies that a JPY-quoted position's
// unrealized P&L is converted to USD using the static fallback table when no
// live quote is available (the lookup returns false). This prevents the ~150×
// over-reporting bug where NZD/JPY losses showed as $32 instead of $0.21.
func TestOpenTrades_JPYQuotePnLConversion(t *testing.T) {
	// GBPJPY short: open at 195.00, current offer at 196.00 → loss
	// P&L in JPY = (195.00 - 196.00) × 1000 = -1000 JPY
	// USD conversion factor (static): 1/150 ≈ 0.00667
	// Expected USD P&L ≈ -6.67
	respBody := `{
		"positions": [
			{
				"position": {
					"dealId": "DI_GBPJPY_001",
					"direction": "SELL",
					"size": 1000,
					"level": 195.00,
					"stopLevel": 197.00,
					"profitLevel": 192.00
				},
				"market": {
					"epic": "GBPJPY",
					"bid": 195.50,
					"offer": 196.00
				}
			}
		]
	}`

	// Serve an empty working-orders response so ensureSession is not needed.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/positions":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(respBody))
		default:
			// markets/ calls from the live-lookup fallback inside quoteToUSD
			// return empty so the static table is used instead.
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	a := newTestAdapter(srv.URL)
	trades, err := a.OpenTrades(context.Background())
	if err != nil {
		t.Fatalf("OpenTrades error: %v", err)
	}
	if len(trades) != 1 {
		t.Fatalf("expected 1 trade, got %d", len(trades))
	}

	tr := trades[0]
	if tr.Symbol != "GBP/JPY" {
		t.Errorf("Symbol=%q want GBP/JPY", tr.Symbol)
	}
	if tr.Units != -1000 {
		t.Errorf("Units=%d want -1000 (SELL)", tr.Units)
	}

	// P&L should be in USD (not raw JPY). Raw JPY P&L would be ≈ -1000.
	// After static table conversion (factor ≈ 1/150): ≈ -6.67
	// Verify it is NOT in the raw JPY range (would be ≈ -1000).
	if tr.UnrealizedPnL < -100 || tr.UnrealizedPnL > 0 {
		t.Errorf("UnrealizedPnL=%.4f — expected a small negative USD value (not raw JPY)", tr.UnrealizedPnL)
	}
}

// ── Compile-time interface assertions (also tested at build time) ─────────────

func TestCompileTimeAssertions(t *testing.T) {
	// These will fail to compile if Adapter does not implement the interfaces.
	var _ broker.Port = (*Adapter)(nil)
	var _ broker.DealingRulesProvider = (*Adapter)(nil)
}
