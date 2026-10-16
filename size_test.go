package aegis_test

// size_test.go — aegis never decides how big a trade is. The caller's size is
// used exactly as given, or the trade is skipped. It is never rounded, raised
// or lowered, and it does not depend on the account balance.

import (
	"context"
	"math"
	"strings"
	"testing"

	"github.com/unitz007/aegis"
	"github.com/unitz007/aegis/broker"
)

func openStub() *stubBroker {
	return &stubBroker{openTradesResp: []broker.OpenTrade{}, openOrdersResp: []broker.OpenOrder{}}
}

// ── Gateway: a size is required and must make sense ──────────────────────────

func TestGateway_SizeIsRequiredAndMustBeUsable(t *testing.T) {
	cases := []struct {
		name   string
		symbol string
		size   float64
		reason string
	}{
		{"missing", "EURUSD", 0, "size must be > 0"},
		{"negative", "EURUSD", -1000, "size must be > 0"},
		{"not a number", "EURUSD", math.NaN(), "size must be > 0"},
		{"infinite", "EURUSD", math.Inf(1), "size must be > 0"},
		{"fractional units of a currency pair", "EURUSD", 1000.5, "whole number"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			placed := 0
			gw := defaultGW(t, func(context.Context, aegis.OrderIntent, string) (string, bool, error) {
				placed++
				return "x", true, nil
			})
			sig := buySignal("sig-size-" + strings.ReplaceAll(tc.name, " ", "-"))
			sig.Symbol, sig.Size = tc.symbol, tc.size
			res, err := gw.Submit(context.Background(), sig)
			if err != nil {
				t.Fatal(err)
			}
			if res.Accepted || res.Code != aegis.ReasonValidation || !strings.Contains(res.Reason, tc.reason) {
				t.Fatalf("got %+v, want a validation rejection mentioning %q", res, tc.reason)
			}
			if placed != 0 {
				t.Fatal("a signal without a usable size must never reach the broker")
			}
			// A rejected signal does not use up its id: the corrected one goes through.
			sig.Size = 10000
			if res, _ := gw.Submit(context.Background(), sig); !res.Accepted {
				t.Fatalf("corrected signal with the same id should be accepted: %+v", res)
			}
		})
	}
}

func TestGateway_PassesTheCallersSizeThroughUntouched(t *testing.T) {
	var got []aegis.OrderIntent
	gw := defaultGW(t, func(_ context.Context, in aegis.OrderIntent, _ string) (string, bool, error) {
		got = append(got, in)
		return "ok", true, nil
	})
	sig := buySignal("sig-pass-fx")
	sig.Size = 12345
	gw.Submit(context.Background(), sig) //nolint:errcheck

	// Crypto takes a fractional quantity of the base asset.
	crypto := aegis.TradeSignal{Source: "test-agent", Strategy: "smc", SignalID: "sig-pass-btc", Symbol: "BTCUSD",
		Direction: "BUY", OrderType: "limit", Entry: 50000, StopLoss: 49000, TakeProfit: 52000, Size: 0.0375}
	gw.Submit(context.Background(), crypto) //nolint:errcheck

	if len(got) != 2 || got[0].Size != 12345 || got[1].Size != 0.0375 {
		t.Fatalf("sizes were not passed through exactly as given: %+v", got)
	}
}

// ── Executor: the size is used exactly, or the trade is skipped ──────────────

func TestExecutor_ForexUnitsAreExactlyTheCallersSize(t *testing.T) {
	for _, dir := range []string{"BUY", "SELL"} {
		sb := openStub()
		in := forexIntent("EURUSD", dir)
		in.Size = 7777
		_, placed, err := (&aegis.Executor{Broker: sb}).ExecuteWithResult(context.Background(), in, "dec-"+dir)
		if err != nil || !placed {
			t.Fatalf("%s: placed=%v err=%v", dir, placed, err)
		}
		want := 7777
		if dir == "SELL" {
			want = -7777
		}
		if sb.lastReq.Units != want {
			t.Errorf("%s: sent %d units, want exactly %d", dir, sb.lastReq.Units, want)
		}
	}
}

func TestExecutor_DoesNotDependOnTheAccountBalance(t *testing.T) {
	sb := openStub()
	sb.accountErr = context.DeadlineExceeded // the balance can't even be read
	in := forexIntent("EURUSD", "BUY")
	in.Size = 500
	_, placed, err := (&aegis.Executor{Broker: sb}).ExecuteWithResult(context.Background(), in, "dec-nobalance")
	if err != nil || !placed || sb.lastReq.Units != 500 {
		t.Fatalf("placed=%v err=%v units=%d: the size must not depend on reading the account", placed, err, sb.lastReq.Units)
	}
}

func TestExecutor_SkipsAnUnusableSize(t *testing.T) {
	for name, size := range map[string]float64{"zero": 0, "negative": -5, "fractional": 100.5, "nan": math.NaN(), "too large": 1e12} {
		sb := openStub()
		in := forexIntent("EURUSD", "BUY")
		in.Size = size
		_, placed, err := (&aegis.Executor{Broker: sb}).ExecuteWithResult(context.Background(), in, "dec-bad-"+name)
		if err != nil || placed || sb.placeCalled != 0 {
			t.Errorf("%s: placed=%v err=%v calls=%d — must be skipped without touching the broker", name, placed, err, sb.placeCalled)
		}
	}
}

func TestExecutor_CryptoQuantityIsExact_AndOutOfRangeIsSkippedNotResized(t *testing.T) {
	rules := broker.DealingRules{MinDealSize: 0.01, MaxDealSize: 1}
	run := func(size float64) (*stubBroker, bool) {
		sb := openStub()
		sb.dealingRulesResp, sb.hasDealingRules = rules, true
		in := cryptoIntent("BTCUSD", "BUY")
		in.Size = size
		_, placed, err := (&aegis.Executor{Broker: sb}).ExecuteWithResult(context.Background(), in, "dec-c")
		if err != nil {
			t.Fatal(err)
		}
		return sb, placed
	}

	if sb, placed := run(0.0375); !placed || sb.lastReq.Quantity != 0.0375 {
		t.Errorf("in range: placed=%v quantity=%v, want exactly 0.0375", placed, sb.lastReq.Quantity)
	}
	if sb, placed := run(0.001); placed || sb.placeCalled != 0 {
		t.Error("below the minimum must be skipped, never raised to the minimum")
	}
	if sb, placed := run(5); placed || sb.placeCalled != 0 {
		t.Error("above the maximum must be skipped, never cut down to the maximum")
	}
}

func TestExecutor_ExposureCapStillAppliesToTheCallersSize(t *testing.T) {
	sb := openStub()
	in := forexIntent("EURUSD", "BUY")
	in.Size = 60_000
	_, placed, _ := (&aegis.Executor{Broker: sb, MaxCurrencyUnits: 50_000}).ExecuteWithResult(context.Background(), in, "dec-cap")
	if placed || sb.placeCalled != 0 {
		t.Error("a size over the exposure cap must be skipped")
	}
	in.Size = 40_000
	if _, placed, _ := (&aegis.Executor{Broker: sb, MaxCurrencyUnits: 50_000}).ExecuteWithResult(context.Background(), in, "dec-cap2"); !placed {
		t.Error("a size within the cap should be placed")
	}
}
