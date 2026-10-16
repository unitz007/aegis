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
		Size:       10000,
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

// ── Manual signal tests ───────────────────────────────────────────────────────

// manualGW returns a gateway configured with MinRR=2.0 and a single allowed
// source "finos" — matching the production aegis-exec default — so manual-path
// tests run against realistic settings.
func manualGW(t *testing.T, place aegis.PlaceFunc) aegis.Gateway {
	t.Helper()
	gw, err := aegis.NewGateway(place, aegis.GatewayConfig{
		AllowedSources:     []string{"finos"},
		MinRR:              2.0,
		ExecutabilityCheck: aegis.AllExecutable, // focus the tests on the manual gates
	})
	if err != nil {
		t.Fatalf("manualGW: NewGateway: %v", err)
	}
	return gw
}

// manualBuySignal builds a BUY signal with RR ≈ 1.2 (below the 2.0 MinRR floor)
// from source "finos". Setting Manual=true should cause this to be accepted;
// leaving it false should cause rejection.
func manualBuySignal(id string, manual bool) aegis.TradeSignal {
	// entry=1.0850, sl=1.0820 (30 pip stop), tp=1.0886 (36 pip target) → RR ≈ 1.2
	return aegis.TradeSignal{
		Source:     "finos",
		Strategy:   "manual_discretionary",
		SignalID:   id,
		Symbol:     "EURUSD",
		Direction:  "BUY",
		OrderType:  "limit",
		Entry:      1.0850,
		StopLoss:   1.0820,
		TakeProfit: 1.0886,
		Size:       10000,
		Timestamp:  time.Now().Unix(),
		Manual:     manual,
	}
}

// TestManual_LowRR_Accepted: Manual=true with RR below MinRR must be accepted
// (RR floor bypassed for manual signals).
func TestManual_LowRR_Accepted(t *testing.T) {
	gw := manualGW(t, stubPlaceSource(t, "finos", true))
	result, err := gw.Submit(context.Background(), manualBuySignal("manual-rr-ok-001", true))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Accepted {
		t.Errorf("Manual=true with low RR must be accepted, got: %s (code=%s)", result.Reason, result.Code)
	}
	if result.Code != aegis.ReasonPlaced {
		t.Errorf("expected ReasonPlaced, got %q", result.Code)
	}
}

// TestManual_LowRR_NonManual_Rejected: regression guard — Manual=false with the
// same low-RR levels must still be rejected by the RR floor.
func TestManual_LowRR_NonManual_Rejected(t *testing.T) {
	gw := manualGW(t, stubPlaceSource(t, "finos", false))
	result, err := gw.Submit(context.Background(), manualBuySignal("manual-rr-reject-001", false))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Accepted {
		t.Error("Manual=false with low RR must be rejected (RR floor active)")
	}
	if result.Code != aegis.ReasonValidation {
		t.Errorf("expected ReasonValidation, got %q", result.Code)
	}
	if !strings.Contains(result.Reason, "RR") {
		t.Errorf("expected reason to mention RR, got: %q", result.Reason)
	}
}

// TestManual_BadStructure_Rejected: Manual=true with stop on wrong side of entry
// must be rejected (structural checks still enforced).
func TestManual_BadStructure_Rejected(t *testing.T) {
	gw := manualGW(t, stubPlaceSource(t, "finos", false))
	sig := aegis.TradeSignal{
		Source:     "finos",
		Strategy:   "manual_discretionary",
		SignalID:   "manual-bad-struct-001",
		Symbol:     "EURUSD",
		Direction:  "BUY",
		OrderType:  "limit",
		Entry:      1.0850,
		StopLoss:   1.0880, // ABOVE entry for a BUY — structurally invalid
		TakeProfit: 1.0920,
		Size:       10000,
		Timestamp:  time.Now().Unix(),
		Manual:     true,
	}
	result, err := gw.Submit(context.Background(), sig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Accepted {
		t.Error("Manual=true with stop above entry (BUY) must be rejected — structural check still applies")
	}
	if result.Code != aegis.ReasonValidation {
		t.Errorf("expected ReasonValidation, got %q", result.Code)
	}
}

// TestManual_UnknownSource_Rejected: Manual=true from an unknown source must
// still be rejected — the allowlist is not bypassed.
func TestManual_UnknownSource_Rejected(t *testing.T) {
	gw := manualGW(t, stubPlaceSource(t, "finos", false))
	sig := manualBuySignal("manual-badsrc-001", true)
	sig.Source = "rogue-agent" // not in AllowedSources
	result, err := gw.Submit(context.Background(), sig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Accepted {
		t.Error("Manual=true from unknown source must be rejected — source allowlist still enforced")
	}
	if result.Code != aegis.ReasonValidation {
		t.Errorf("expected ReasonValidation, got %q", result.Code)
	}
}

// TestManual_ZeroEntry_Rejected: Manual=true with entry<=0 must be rejected
// (basic level validity still enforced).
func TestManual_ZeroEntry_Rejected(t *testing.T) {
	gw := manualGW(t, stubPlaceSource(t, "finos", false))
	sig := manualBuySignal("manual-zero-entry-001", true)
	sig.Entry = 0
	result, err := gw.Submit(context.Background(), sig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Accepted {
		t.Error("Manual=true with entry=0 must be rejected — level validity still enforced")
	}
	if result.Code != aegis.ReasonValidation {
		t.Errorf("expected ReasonValidation, got %q", result.Code)
	}
}

// TestManual_SessionBypass: a Manual signal submitted during Asian session hours
// for a non-Asian pair (e.g. EURUSD at 03:00 UTC) must be placed. This test
// wires the executor directly with StockAISessionGate to verify the bypass.
//
// Session bypass wiring: intent.Manual=true causes innerExecute to skip the
// `e.sessionOK(...)` call unconditionally. The gate is still registered on the
// Executor (StockAISessionGate); it is simply not invoked for manual intents.
func TestManual_SessionBypass(t *testing.T) {
	pb := paper.New(10_000, nil)

	// sessionGate that always returns false — simulates a hard Asian-session veto
	// for any symbol. A non-manual intent would be blocked; a manual one must pass.
	alwaysVeto := func(_, _ string, _ time.Time) bool { return false }

	ex := &aegis.Executor{
		Broker:      pb,
		SessionGate: alwaysVeto,
	}

	intent := aegis.OrderIntent{
		Symbol:     "EURUSD",
		AssetClass: "forex",
		Direction:  "BUY",
		OrderType:  "limit",
		EntryPrice: 1.0850,
		StopLoss:   1.0820,
		TakeProfit: 1.0886,
		Size:       10000,
		Confidence: 60,
		Manual:     true, // must bypass the always-veto session gate
	}

	_, placed, err := ex.ExecuteWithResult(context.Background(), intent, "manual-session-bypass-001")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !placed {
		t.Error("Manual=true must bypass session veto — trade must be placed even when gate returns false")
	}

	// Sanity check: same intent without Manual=false IS vetoed.
	intent.Manual = false
	intent2 := intent
	_, placed2, err2 := ex.ExecuteWithResult(context.Background(), intent2, "manual-session-veto-002")
	if err2 != nil {
		t.Fatalf("unexpected error on non-manual: %v", err2)
	}
	if placed2 {
		t.Error("Manual=false must be vetoed by the always-veto session gate")
	}
}

// TestManual_ConfidenceBypass: a Manual signal with confidence below MinConfidence
// for a non-algo pair must still be placed.
func TestManual_ConfidenceBypass(t *testing.T) {
	pb := paper.New(10_000, nil)

	ex := &aegis.Executor{
		Broker:        pb,
		MinConfidence: 80,
		// IsAlgoPair: nil → DefaultIsAlgoPair → crypto is NOT algo, confidence applies.
	}

	// Crypto intent with confidence below MinConfidence.
	intent := aegis.OrderIntent{
		Symbol:     "BTCUSD",
		AssetClass: "crypto",
		Direction:  "BUY",
		OrderType:  "limit",
		EntryPrice: 50_000,
		StopLoss:   49_000,
		TakeProfit: 53_000,
		Size:       0.05,
		Confidence: 30, // below MinConfidence=80
		Manual:     true,
	}

	// Need dealing rules for crypto.
	pb.DealingRules["BTCUSD"] = broker.DealingRules{MinDealSize: 0.001, MaxDealSize: 5.0}

	_, placed, err := ex.ExecuteWithResult(context.Background(), intent, "manual-conf-bypass-001")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !placed {
		t.Error("Manual=true must bypass confidence gate — trade must be placed even with confidence=30")
	}

	// Sanity: same intent without Manual is confidence-gated.
	intent.Manual = false
	pb2 := paper.New(10_000, nil)
	pb2.DealingRules["BTCUSD"] = broker.DealingRules{MinDealSize: 0.001, MaxDealSize: 5.0}
	ex2 := &aegis.Executor{
		Broker:        pb2,
		MinConfidence: 80,
	}
	_, placed2, err2 := ex2.ExecuteWithResult(context.Background(), intent, "manual-conf-veto-002")
	if err2 != nil {
		t.Fatalf("unexpected error on non-manual confidence test: %v", err2)
	}
	if placed2 {
		t.Error("Manual=false with confidence=30 below MinConfidence=80 must be skipped")
	}
}

// stubPlaceSource is like stubPlace but for a specific source. The source
// parameter is unused in the stub (the gateway stamps source before calling
// PlaceFunc), but is kept for clarity of intent.
func stubPlaceSource(t *testing.T, _ string, wantPlaced bool) aegis.PlaceFunc {
	t.Helper()
	return func(_ context.Context, _ aegis.OrderIntent, _ string) (string, bool, error) {
		if wantPlaced {
			return "broker-manual-001", true, nil
		}
		return "", false, nil
	}
}
