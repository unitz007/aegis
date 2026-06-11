package aegis_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/unitz007/aegis"
	"github.com/unitz007/aegis/broker"
	"github.com/unitz007/aegis/broker/paper"
)

// ── Helpers ───────────────────────────────────────────────────────────────────

func stubPlace(t *testing.T, wantPlaced bool) aegis.PlaceFunc {
	t.Helper()
	return func(_ context.Context, _ aegis.OrderIntent, _ string) (string, bool, error) {
		if wantPlaced {
			return "broker-order-001", true, nil
		}
		return "", false, nil
	}
}

func defaultGW(t *testing.T, place aegis.PlaceFunc) aegis.Gateway {
	t.Helper()
	gw, err := aegis.NewGateway(place, aegis.GatewayConfig{
		AllowedSources: []string{"test-agent"},
		MinRR:          1.5,
	})
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	return gw
}

func buySignal(id string) aegis.TradeSignal {
	return aegis.TradeSignal{
		Source:     "test-agent",
		Strategy:   "smc",
		SignalID:   id,
		Symbol:     "EURUSD",
		Direction:  "BUY",
		OrderType:  "limit",
		Entry:      1.0850,
		StopLoss:   1.0820,
		TakeProfit: 1.0895, // RR = 45/30 = 1.5
		Timestamp:  time.Now().Unix(),
	}
}

// ── Allowlist tests ───────────────────────────────────────────────────────────

func TestAllowlist_AcceptsConfiguredSource(t *testing.T) {
	gw := defaultGW(t, stubPlace(t, true))
	result, err := gw.Submit(context.Background(), buySignal("sig-001"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Accepted {
		t.Errorf("expected Accepted=true, got: %s", result.Reason)
	}
}

func TestAllowlist_RejectsUnknownSource(t *testing.T) {
	gw := defaultGW(t, stubPlace(t, false))
	sig := buySignal("sig-002")
	sig.Source = "rogue-agent"
	result, err := gw.Submit(context.Background(), sig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Accepted {
		t.Error("expected Accepted=false for unknown source")
	}
	if result.Code != aegis.ReasonValidation {
		t.Errorf("expected ReasonValidation, got %q", result.Code)
	}
}

// TestAllowlist_AcceptsArbitraryStrategy verifies that strategy is free-text:
// any non-empty strategy string (including novel experiment names) is accepted
// end-to-end. SOURCE validation is the real auth boundary — strategy is provenance.
func TestAllowlist_AcceptsArbitraryStrategy(t *testing.T) {
	for _, strat := range []string{"topdown", "some_experiment_v3", "zone_internal", "my-new-algo"} {
		t.Run(strat, func(t *testing.T) {
			gw := defaultGW(t, stubPlace(t, true))
			sig := buySignal("sig-arbitrary-" + strat)
			sig.Strategy = strat
			result, err := gw.Submit(context.Background(), sig)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !result.Accepted {
				t.Errorf("arbitrary strategy %q should be accepted, got: %s", strat, result.Reason)
			}
		})
	}
}

func TestAllowlist_EmptySourcesRejectsAll(t *testing.T) {
	gw, err := aegis.NewGateway(stubPlace(t, true), aegis.GatewayConfig{
		AllowedSources: []string{},
	})
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	result, _ := gw.Submit(context.Background(), buySignal("sig-004"))
	if result.Accepted {
		t.Error("empty AllowedSources should reject all")
	}
}

// TestAllowlist_UnknownSourceRejected_AnyStrategyAccepted: unknown SOURCE is still
// rejected even when an arbitrary strategy string is supplied. Source is the only
// auth boundary; strategy is free-text provenance.
func TestAllowlist_UnknownSourceRejected_AnyStrategyAccepted(t *testing.T) {
	gw := defaultGW(t, stubPlace(t, false))
	sig := buySignal("sig-005")
	sig.Source = "rogue-agent"
	sig.Strategy = "topdown" // arbitrary strategy
	result, err := gw.Submit(context.Background(), sig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Accepted {
		t.Error("unknown source must still be rejected regardless of strategy")
	}
	if result.Code != aegis.ReasonValidation {
		t.Errorf("expected ReasonValidation, got %q", result.Code)
	}
}

// ── Validation tests ──────────────────────────────────────────────────────────

func TestValidation_EmptySignalID(t *testing.T) {
	gw := defaultGW(t, stubPlace(t, false))
	sig := buySignal("")
	result, _ := gw.Submit(context.Background(), sig)
	if result.Accepted {
		t.Error("expected rejection for empty signal_id")
	}
	if result.Code != aegis.ReasonValidation {
		t.Errorf("expected ReasonValidation, got %q", result.Code)
	}
}

func TestValidation_RRBelowMinimum(t *testing.T) {
	gw := defaultGW(t, stubPlace(t, false))
	sig := buySignal("sig-rr-fail")
	sig.TakeProfit = 1.0855 // RR = 5/30 < 1.5
	result, _ := gw.Submit(context.Background(), sig)
	if result.Accepted {
		t.Error("expected rejection for RR below minimum")
	}
	if result.Code != aegis.ReasonValidation {
		t.Errorf("expected ReasonValidation, got %q", result.Code)
	}
}

func TestValidation_BadDirectionSell(t *testing.T) {
	gw := defaultGW(t, stubPlace(t, false))
	sig := buySignal("sig-dir")
	sig.Direction = "SELL"
	// SL and TP are wrong side for SELL — should fail direction-consistent check.
	result, _ := gw.Submit(context.Background(), sig)
	if result.Accepted {
		t.Error("expected rejection for direction-inconsistent levels")
	}
}

// ── Idempotency tests ─────────────────────────────────────────────────────────

func TestIdempotency_DuplicateReturnsOriginalResult(t *testing.T) {
	placed := 0
	place := func(_ context.Context, _ aegis.OrderIntent, _ string) (string, bool, error) {
		placed++
		return "order-abc", true, nil
	}
	gw := defaultGW(t, place)
	ctx := context.Background()

	r1, _ := gw.Submit(ctx, buySignal("sig-idem"))
	r2, _ := gw.Submit(ctx, buySignal("sig-idem"))

	if placed != 1 {
		t.Errorf("PlaceFunc should be called exactly once, got %d", placed)
	}
	if r1.Code != r2.Code {
		t.Errorf("idempotent results should have same code: %q vs %q", r1.Code, r2.Code)
	}
}

func TestIdempotency_ValidationRejectDoesNotConsumeSlot(t *testing.T) {
	placed := 0
	place := func(_ context.Context, _ aegis.OrderIntent, _ string) (string, bool, error) {
		placed++
		return "order-xyz", true, nil
	}
	gw := defaultGW(t, place)
	ctx := context.Background()

	// First: submit a bad signal (wrong source) — should be rejected, slot not burned.
	bad := buySignal("sig-noburn")
	bad.Source = "rogue"
	r1, _ := gw.Submit(ctx, bad)
	if r1.Accepted {
		t.Fatal("expected rejection")
	}
	if placed != 0 {
		t.Fatal("PlaceFunc must not be called for rejected signals")
	}

	// Second: submit a valid signal with the same signal_id — must succeed.
	good := buySignal("sig-noburn") // same signal_id, but valid source
	r2, _ := gw.Submit(ctx, good)
	if !r2.Accepted {
		t.Errorf("valid resubmit with same signal_id should be accepted after validation reject: %s", r2.Reason)
	}
	if placed != 1 {
		t.Errorf("PlaceFunc should be called once for the valid submission, got %d", placed)
	}
}

// ── SignalReasonCode constants ─────────────────────────────────────────────────

func TestSignalReasonCodes_Present(t *testing.T) {
	codes := []aegis.SignalReasonCode{
		aegis.ReasonPlaced,
		aegis.ReasonExecutorSkip,
		aegis.ReasonInFlight,
		aegis.ReasonValidation,
	}
	want := map[aegis.SignalReasonCode]string{
		aegis.ReasonPlaced:       "placed",
		aegis.ReasonExecutorSkip: "executor_skip",
		aegis.ReasonInFlight:     "in_flight",
		aegis.ReasonValidation:   "validation",
	}
	for _, c := range codes {
		if string(c) != want[c] {
			t.Errorf("SignalReasonCode %q: want %q", c, want[c])
		}
	}
}

// ── ClassifySymbolDefault tests ───────────────────────────────────────────────

func TestClassifySymbolDefault(t *testing.T) {
	cases := []struct {
		symbol string
		want   string
	}{
		{"EURUSD", "forex"},
		{"EUR/USD", "forex"},
		{"GBP/JPY", "forex"},
		{"USDJPY", "forex"},
		{"AUDCAD", "forex"},
		{"BTC/USD", "crypto"},
		{"BTCUSDT", "crypto"},
		{"ETHUSDT", "crypto"},
		{"SOLBTC", "crypto"},
		{"XRPUSD", "crypto"},
		{"ADAUSDT", "crypto"},
		{"AAPL", "stock"},
		{"TSLA", "stock"},
		{"NVDA", "stock"},
	}
	for _, tc := range cases {
		got := aegis.ClassifySymbolDefault(tc.symbol)
		if got != tc.want {
			t.Errorf("ClassifySymbolDefault(%q) = %q, want %q", tc.symbol, got, tc.want)
		}
	}
}

func TestRegisterCryptoBases_ExtendsClassifier(t *testing.T) {
	// Register a fictional coin.
	aegis.RegisterCryptoBases(map[string]struct{}{"FICTIONALCOIN": {}})
	got := aegis.ClassifySymbolDefault("FICTIONALCOINUSDT")
	if got != "crypto" {
		t.Errorf("expected crypto after RegisterCryptoBases, got %q", got)
	}
}

// ── Gateway placed result ─────────────────────────────────────────────────────

func TestGateway_PlacedResult(t *testing.T) {
	gw := defaultGW(t, stubPlace(t, true))
	result, err := gw.Submit(context.Background(), buySignal("sig-placed"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Accepted {
		t.Error("expected Accepted=true")
	}
	if !result.Placed {
		t.Error("expected Placed=true")
	}
	if result.BrokerOrderID == "" {
		t.Error("expected BrokerOrderID to be set")
	}
	if result.Code != aegis.ReasonPlaced {
		t.Errorf("expected ReasonPlaced, got %q", result.Code)
	}
}

func TestGateway_SkippedResult(t *testing.T) {
	gw := defaultGW(t, stubPlace(t, false))
	result, err := gw.Submit(context.Background(), buySignal("sig-skip"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Accepted {
		t.Error("expected Accepted=true even when not placed")
	}
	if result.Placed {
		t.Error("expected Placed=false")
	}
	if result.Code != aegis.ReasonExecutorSkip {
		t.Errorf("expected ReasonExecutorSkip, got %q", result.Code)
	}
}

// ── NewGateway error cases ────────────────────────────────────────────────────

func TestNewGateway_NilPlaceReturnsError(t *testing.T) {
	_, err := aegis.NewGateway(nil, aegis.GatewayConfig{})
	if err == nil {
		t.Error("expected error for nil place func")
	}
}

// ── Sizing helpers ────────────────────────────────────────────────────────────

func TestSizeForex_Basic(t *testing.T) {
	// balance=10000, risk=1%, entry=1.0850, sl=1.0820, factor=1.0
	// slDist≈0.003 (IEEE-754), riskAmount=100
	// rawUnits = 100/slDist ≈ 33333.333... → math.Round = 33333
	// (Floor and Round agree here; the fractional part is ~0.333, below 0.5)
	units, err := aegis.SizeForex(10000, 0.01, 1.0850, 1.0820, 1.0)
	if err != nil {
		t.Fatalf("SizeForex: %v", err)
	}
	if units != 33333 {
		t.Errorf("SizeForex units = %d, want 33333", units)
	}
}

func TestSizeForex_JPYFactor(t *testing.T) {
	// USDJPY: factor = 1/150 ≈ 0.00667. Entry=150, SL=149.5, slDist=0.5 (exact)
	// riskAmount = 10000 * 0.01 = 100
	// rawUnits = 100 / (0.5 * (1/150)) ≈ 29999.999... (fp), math.Round = 30000
	factor := 1.0 / 150.0
	units, err := aegis.SizeForex(10000, 0.01, 150.0, 149.5, factor)
	if err != nil {
		t.Fatalf("SizeForex JPY: %v", err)
	}
	// math.Round(29999.999...) = 30000 — very different from 66 (which you'd get with factor=1).
	if units != 30000 {
		t.Errorf("SizeForex JPY units = %d, want 30000", units)
	}
}

func TestSizeCrypto_Basic(t *testing.T) {
	// balance=10000, risk=1%, entry=50000, sl=49000
	// slPct = |50000-49000| / 50000 = 0.02
	// riskAmount = 10000 * 0.01 = 100
	// qty = 100 / 0.02 / 50000 = 5000 / 50000 = 0.1
	qty, err := aegis.SizeCrypto(10000, 0.01, 50000, 49000)
	if err != nil {
		t.Fatalf("SizeCrypto: %v", err)
	}
	const want = 0.1
	diff := qty - want
	if diff < 0 {
		diff = -diff
	}
	if diff > 0.000001 {
		t.Errorf("SizeCrypto qty = %.8f, want %.8f", qty, want)
	}
}

func TestClampCryptoQuantity_AboveMax(t *testing.T) {
	clamped, ok := aegis.ClampCryptoQuantity(0.05, 0.001, 0.01)
	if !ok {
		t.Error("expected ok=true when qty > min")
	}
	if clamped != 0.01 {
		t.Errorf("expected clamped to max 0.01, got %f", clamped)
	}
}

func TestClampCryptoQuantity_BelowMin(t *testing.T) {
	_, ok := aegis.ClampCryptoQuantity(0.0001, 0.001, 0.01)
	if ok {
		t.Error("expected ok=false when qty < min (must skip, not upsize)")
	}
}

func TestClampCryptoQuantity_InRange(t *testing.T) {
	clamped, ok := aegis.ClampCryptoQuantity(0.005, 0.001, 0.01)
	if !ok {
		t.Error("expected ok=true for in-range qty")
	}
	if clamped != 0.005 {
		t.Errorf("expected unchanged qty 0.005, got %f", clamped)
	}
}

// ── ExecutabilityCheck hook ───────────────────────────────────────────────────

func TestExecutabilityCheck_PreReservationReject(t *testing.T) {
	placed := 0
	place := func(_ context.Context, _ aegis.OrderIntent, _ string) (string, bool, error) {
		placed++
		return "", false, nil
	}
	gw, err := aegis.NewGateway(place, aegis.GatewayConfig{
		AllowedSources: []string{"test-agent"},
		ExecutabilityCheck: func(symbol, _ string) (bool, string) {
			if strings.HasPrefix(symbol, "BLOCKED") {
				return false, "symbol is on the denylist"
			}
			return true, ""
		},
	})
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}

	sig := buySignal("sig-blocked")
	sig.Symbol = "BLOCKEDUSDT"
	// Adjust levels to be plausible for crypto.
	sig.Entry = 100.0
	sig.StopLoss = 95.0
	sig.TakeProfit = 110.0

	result, _ := gw.Submit(context.Background(), sig)
	if result.Accepted {
		t.Error("expected rejection for blocked symbol")
	}
	if result.Code != aegis.ReasonValidation {
		t.Errorf("expected ReasonValidation, got %q", result.Code)
	}
	if placed != 0 {
		t.Error("PlaceFunc must not be called for executability-rejected symbols")
	}
}

// ── Paper broker tests ────────────────────────────────────────────────────────

func TestPaperBroker_RoundTrip_Market(t *testing.T) {
	pb := paper.New(10_000, nil)
	ctx := context.Background()

	// Place a market trade.
	receipt, err := pb.PlaceTrade(ctx, broker.TradeRequest{
		Symbol:     "EURUSD",
		Units:      1000,
		OrderType:  "market",
		EntryPrice: 1.0850,
		TakeProfit: 1.0900,
		StopLoss:   1.0820,
	})
	if err != nil {
		t.Fatalf("PlaceTrade: %v", err)
	}
	if receipt.BrokerTradeID == "" {
		t.Error("expected non-empty BrokerTradeID")
	}

	// Appears in OpenTrades.
	trades, err := pb.OpenTrades(ctx)
	if err != nil {
		t.Fatalf("OpenTrades: %v", err)
	}
	if len(trades) != 1 {
		t.Fatalf("expected 1 open trade, got %d", len(trades))
	}
	if trades[0].Symbol != "EURUSD" {
		t.Errorf("expected EURUSD, got %s", trades[0].Symbol)
	}

	// Close trade.
	if err := pb.CloseTrade(ctx, receipt.BrokerTradeID); err != nil {
		t.Fatalf("CloseTrade: %v", err)
	}

	// No longer in OpenTrades.
	trades, _ = pb.OpenTrades(ctx)
	if len(trades) != 0 {
		t.Errorf("expected 0 trades after close, got %d", len(trades))
	}
}

func TestPaperBroker_RoundTrip_LimitOrder(t *testing.T) {
	pb := paper.New(10_000, nil)
	ctx := context.Background()

	// Place a limit order.
	receipt, err := pb.PlaceTrade(ctx, broker.TradeRequest{
		Symbol:     "GBPUSD",
		Units:      500,
		OrderType:  "limit",
		EntryPrice: 1.2700,
		TakeProfit: 1.2800,
		StopLoss:   1.2640,
	})
	if err != nil {
		t.Fatalf("PlaceTrade limit: %v", err)
	}

	// Appears in OpenOrders (not OpenTrades — not yet triggered).
	orders, err := pb.OpenOrders(ctx)
	if err != nil {
		t.Fatalf("OpenOrders: %v", err)
	}
	if len(orders) != 1 {
		t.Fatalf("expected 1 open order, got %d", len(orders))
	}
	if orders[0].Symbol != "GBPUSD" {
		t.Errorf("expected GBPUSD, got %s", orders[0].Symbol)
	}

	// Not yet in OpenTrades.
	trades, _ := pb.OpenTrades(ctx)
	if len(trades) != 0 {
		t.Errorf("expected 0 open trades for unfilled limit, got %d", len(trades))
	}

	// Cancel the order.
	if err := pb.CancelOrder(ctx, receipt.BrokerTradeID); err != nil {
		t.Fatalf("CancelOrder: %v", err)
	}

	orders, _ = pb.OpenOrders(ctx)
	if len(orders) != 0 {
		t.Errorf("expected 0 orders after cancel, got %d", len(orders))
	}
}

func TestPaperBroker_Account_ReflectsBalance(t *testing.T) {
	pb := paper.New(10_000, nil)
	ctx := context.Background()

	acct, err := pb.Account(ctx)
	if err != nil {
		t.Fatalf("Account: %v", err)
	}
	if acct.Balance != 10_000 {
		t.Errorf("expected balance 10000, got %.2f", acct.Balance)
	}
	if acct.Currency != "USD" {
		t.Errorf("expected USD currency, got %q", acct.Currency)
	}
}

func TestPaperBroker_CloseTrade_AdjustsBalance(t *testing.T) {
	pb := paper.New(10_000, nil)
	ctx := context.Background()

	// Buy 1000 units @ 1.0850, close @ 1.0900 (profitable).
	// Use a quoteFetcher so close price can be supplied.
	closePrice := 1.0900
	pb2 := paper.New(10_000, func(_ context.Context, _ string) (float64, error) {
		return closePrice, nil
	})

	receipt, err := pb2.PlaceTrade(ctx, broker.TradeRequest{
		Symbol:     "EURUSD",
		Units:      1000,
		OrderType:  "market",
		EntryPrice: 1.0850,
		TakeProfit: 1.0950,
		StopLoss:   1.0800,
	})
	if err != nil {
		t.Fatalf("PlaceTrade: %v", err)
	}

	if err := pb2.CloseTrade(ctx, receipt.BrokerTradeID); err != nil {
		t.Fatalf("CloseTrade: %v", err)
	}

	acct, _ := pb2.Account(ctx)
	// Expected P&L = (1.0900 - 1.0850) * 1000 = 5.00
	expectedBalance := 10_000 + 5.00
	if acct.Balance != expectedBalance {
		t.Errorf("expected balance %.2f after close, got %.2f", expectedBalance, acct.Balance)
	}
	_ = pb // suppress unused warning
}

func TestPaperBroker_DoubleClose_Errors(t *testing.T) {
	pb := paper.New(10_000, nil)
	ctx := context.Background()

	receipt, _ := pb.PlaceTrade(ctx, broker.TradeRequest{
		Symbol:     "EURUSD",
		Units:      100,
		OrderType:  "market",
		EntryPrice: 1.0850,
		TakeProfit: 1.0900,
		StopLoss:   1.0820,
	})
	pb.CloseTrade(ctx, receipt.BrokerTradeID)
	err := pb.CloseTrade(ctx, receipt.BrokerTradeID)
	if err == nil {
		t.Error("expected error on double-close")
	}
}

func TestPaperBroker_DealingRules(t *testing.T) {
	pb := paper.New(10_000, nil)
	pb.DealingRules["BTCUSD"] = broker.DealingRules{MinDealSize: 0.001, MaxDealSize: 5.0}
	ctx := context.Background()

	rules, err := pb.MarketDealingRules(ctx, "BTCUSD")
	if err != nil {
		t.Fatalf("MarketDealingRules: %v", err)
	}
	if rules.MinDealSize != 0.001 {
		t.Errorf("expected MinDealSize=0.001, got %f", rules.MinDealSize)
	}
	if rules.MaxDealSize != 5.0 {
		t.Errorf("expected MaxDealSize=5.0, got %f", rules.MaxDealSize)
	}
}

func TestPaperBroker_AmendTrade(t *testing.T) {
	pb := paper.New(10_000, nil)
	ctx := context.Background()

	receipt, _ := pb.PlaceTrade(ctx, broker.TradeRequest{
		Symbol: "EURUSD", Units: 1000, OrderType: "market",
		EntryPrice: 1.0850, TakeProfit: 1.0900, StopLoss: 1.0820,
	})
	if err := pb.AmendTrade(ctx, receipt.BrokerTradeID, 1.0815, 1.0920); err != nil {
		t.Fatalf("AmendTrade: %v", err)
	}
	trades, _ := pb.OpenTrades(ctx)
	if len(trades) != 1 {
		t.Fatalf("expected 1 trade")
	}
	if trades[0].StopLoss != 1.0815 {
		t.Errorf("expected SL=1.0815, got %f", trades[0].StopLoss)
	}
	if trades[0].TakeProfit != 1.0920 {
		t.Errorf("expected TP=1.0920, got %f", trades[0].TakeProfit)
	}
}

