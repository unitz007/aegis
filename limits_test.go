package aegis_test

// limits_test.go — the hard ceilings hold whatever the sender asks, say why in plain
// words, and fail closed when they can't be checked.

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/unitz007/aegis"
	"github.com/unitz007/aegis/broker"
)

func limitedExec(b *stubBroker, lim aegis.Limits, g *aegis.Guard) *aegis.Executor {
	return &aegis.Executor{Broker: b, BrokerName: "stub", MinConfidence: 45, Limits: lim, Guard: g}
}

func fxIntent(size float64) aegis.OrderIntent {
	return aegis.OrderIntent{Symbol: "EURUSD", AssetClass: "forex", Direction: "BUY", OrderType: "limit",
		EntryPrice: 1.085, StopLoss: 1.082, TakeProfit: 1.0895, Size: size, Manual: true}
}

func mustRefuse(t *testing.T, err error, want string) {
	t.Helper()
	r, ok := aegis.AsRefusal(err)
	if !ok {
		t.Fatalf("want a refusal mentioning %q, got %v", want, err)
	}
	if !strings.Contains(r.Why, want) {
		t.Fatalf("refusal %q should mention %q", r.Why, want)
	}
	if strings.Contains(strings.ToLower(r.Why), "executor") || strings.Contains(r.Why, "{") {
		t.Fatalf("a refusal must be plain words: %q", r.Why)
	}
}

func TestLimits_ATradeBiggerThanTheMaximumIsRefusedNotShrunk(t *testing.T) {
	b := openStub()
	e := limitedExec(b, aegis.Limits{MaxUnits: 100_000}, nil)

	_, placed, err := e.ExecuteWithResult(context.Background(), fxIntent(250_000), "d1")
	mustRefuse(t, err, "100000 units")
	if placed || b.placeCalled != 0 {
		t.Fatalf("an oversize trade must never reach the broker (placed=%v calls=%d)", placed, b.placeCalled)
	}
	// Exactly at the ceiling is fine, and is placed as asked.
	if _, placed, err := e.ExecuteWithResult(context.Background(), fxIntent(100_000), "d2"); err != nil || !placed {
		t.Fatalf("a trade at the ceiling is allowed: placed=%v err=%v", placed, err)
	}
	if b.lastReq.Units != 100_000 {
		t.Errorf("the size is used exactly as asked: %d", b.lastReq.Units)
	}
}

func TestLimits_CryptoIsLimitedByValueNotQuantity(t *testing.T) {
	b := openStub()
	b.hasDealingRules = true
	b.dealingRulesResp = broker.DealingRules{MinDealSize: 0.0001, MaxDealSize: 1_000_000}
	e := limitedExec(b, aegis.Limits{MaxCryptoValue: 10_000}, nil)
	btc := func(qty, price float64) aegis.OrderIntent {
		return aegis.OrderIntent{Symbol: "BTCUSD", AssetClass: "crypto", Direction: "BUY", OrderType: "limit", EntryPrice: price, StopLoss: price * 0.98, TakeProfit: price * 1.04, Size: qty, Manual: true}
	}
	// 0.5 BTC at 60,000 is 30,000 of value: refused. 0.1 BTC is 6,000: fine.
	_, _, err := e.ExecuteWithResult(context.Background(), btc(0.5, 60_000), "c1")
	mustRefuse(t, err, "largest allowed")
	if _, placed, err := e.ExecuteWithResult(context.Background(), btc(0.1, 60_000), "c2"); err != nil || !placed {
		t.Fatalf("a small crypto trade is allowed: placed=%v err=%v", placed, err)
	}
	// The same quantity of a cheap coin is a different value.
	if _, placed, err := e.ExecuteWithResult(context.Background(), btc(5000, 0.5), "c3"); err != nil || !placed {
		t.Fatalf("value, not quantity, is what counts: placed=%v err=%v", placed, err)
	}
}

func TestLimits_OpenPositionsCountTradesAndWaitingOrders(t *testing.T) {
	b := openStub()
	b.openTradesResp = []broker.OpenTrade{{Symbol: "GBPUSD", Units: 1000}, {Symbol: "USDJPY", Units: 1000}}
	b.openOrdersResp = []broker.OpenOrder{{Symbol: "AUDUSD", Direction: "BUY"}}
	e := limitedExec(b, aegis.Limits{MaxOpenPositions: 3}, nil)

	_, _, err := e.ExecuteWithResult(context.Background(), fxIntent(10_000), "p1")
	mustRefuse(t, err, "most open trades allowed (3)")
	if b.placeCalled != 0 {
		t.Fatal("nothing placed at the cap")
	}
	e.Limits.MaxOpenPositions = 4
	if _, placed, err := e.ExecuteWithResult(context.Background(), fxIntent(10_000), "p2"); err != nil || !placed {
		t.Fatalf("one below the cap is allowed: placed=%v err=%v", placed, err)
	}
}

func TestLimits_TradesPerDayArePerAccountAndResetAtMidnightUTC(t *testing.T) {
	now := time.Date(2026, 10, 18, 22, 0, 0, 0, time.UTC)
	g := aegis.NewGuard(func() time.Time { return now })
	a := openStub()
	a.accountResp = broker.BrokerAccount{AccountID: "acct-A", NAV: 10_000, Balance: 10_000, Currency: "USD"}
	c := openStub()
	c.accountResp = broker.BrokerAccount{AccountID: "acct-B", NAV: 10_000, Balance: 10_000, Currency: "USD"}
	ea := limitedExec(a, aegis.Limits{MaxTradesPerDay: 2}, g)
	ec := limitedExec(c, aegis.Limits{MaxTradesPerDay: 2}, g)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, placed, err := ea.ExecuteWithResult(ctx, fxIntent(1000), "a"); err != nil || !placed {
			t.Fatalf("trade %d for A: placed=%v err=%v", i+1, placed, err)
		}
	}
	_, _, err := ea.ExecuteWithResult(ctx, fxIntent(1000), "a3")
	mustRefuse(t, err, "daily limit of 2")

	// Another account has its own count, even through the same guard.
	if _, placed, err := ec.ExecuteWithResult(ctx, fxIntent(1000), "b1"); err != nil || !placed {
		t.Fatalf("B is unaffected by A's trades: placed=%v err=%v", placed, err)
	}

	// A new UTC day starts fresh.
	now = now.Add(3 * time.Hour)
	if _, placed, err := ea.ExecuteWithResult(ctx, fxIntent(1000), "a4"); err != nil || !placed {
		t.Fatalf("the count resets after midnight UTC: placed=%v err=%v", placed, err)
	}
}

func TestLimits_ARefusedOrSkippedTradeDoesNotUseUpTheDay(t *testing.T) {
	g := aegis.NewGuard(nil)
	b := openStub()
	b.accountResp = broker.BrokerAccount{AccountID: "acct", NAV: 10_000, Balance: 10_000}
	e := limitedExec(b, aegis.Limits{MaxTradesPerDay: 1, MaxUnits: 5000}, g)
	ctx := context.Background()
	if _, _, err := e.ExecuteWithResult(ctx, fxIntent(50_000), "big"); err == nil {
		t.Fatal("oversize refused")
	}
	b.placeErr = errors.New("broker down")
	if _, _, err := e.ExecuteWithResult(ctx, fxIntent(1000), "fails"); err == nil {
		t.Fatal("broker error surfaces")
	}
	b.placeErr = nil
	if _, placed, err := e.ExecuteWithResult(ctx, fxIntent(1000), "ok"); err != nil || !placed {
		t.Fatalf("only a trade that really went through counts: placed=%v err=%v", placed, err)
	}
}

func TestLimits_DailyLossStopsNewTradesAndFailsClosed(t *testing.T) {
	g := aegis.NewGuard(nil)
	b := openStub()
	b.accountResp = broker.BrokerAccount{AccountID: "acct", NAV: 10_000, Balance: 10_000, Currency: "USD"}
	e := limitedExec(b, aegis.Limits{MaxDailyLoss: 500}, g)
	ctx := context.Background()

	// First sight of the account today fixes the day's starting value (10,000).
	if _, placed, err := e.ExecuteWithResult(ctx, fxIntent(1000), "l1"); err != nil || !placed {
		t.Fatalf("first trade: placed=%v err=%v", placed, err)
	}
	b.accountResp.NAV = 9_600 // down 400: still inside the limit
	if _, placed, err := e.ExecuteWithResult(ctx, fxIntent(1000), "l2"); err != nil || !placed {
		t.Fatalf("inside the limit: placed=%v err=%v", placed, err)
	}
	b.accountResp.NAV = 9_480 // down 520: over
	calls := b.placeCalled
	_, _, err := e.ExecuteWithResult(ctx, fxIntent(1000), "l3")
	mustRefuse(t, err, "daily loss limit (500 USD)")
	if b.placeCalled != calls {
		t.Fatal("nothing placed once the loss limit is hit")
	}

	// Can't read the account: can't know the loss, so nothing goes.
	b.accountErr = errors.New("timeout")
	_, _, err = e.ExecuteWithResult(ctx, fxIntent(1000), "l4")
	mustRefuse(t, err, "couldn't check your account")
}

func TestPause_StopsEverythingFirstAndResumeLetsItGo(t *testing.T) {
	g := aegis.NewGuard(nil)
	b := openStub()
	e := limitedExec(b, aegis.Limits{}, g)
	other := limitedExec(openStub(), aegis.Limits{}, g) // another executor, same guard
	ctx := context.Background()

	g.Pause("checking an odd fill")
	for _, ex := range []*aegis.Executor{e, other} {
		_, placed, err := ex.ExecuteWithResult(ctx, fxIntent(1000), "x")
		mustRefuse(t, err, "paused right now (checking an odd fill)")
		if placed {
			t.Fatal("paused means nothing is placed, on any executor sharing the guard")
		}
	}
	if b.placeCalled != 0 {
		t.Fatal("the broker is not even asked while paused")
	}
	if paused, reason, since := g.Status(); !paused || reason != "checking an odd fill" || since.IsZero() {
		t.Fatalf("status: %v %q %v", paused, reason, since)
	}

	g.Resume()
	if _, placed, err := e.ExecuteWithResult(ctx, fxIntent(1000), "y"); err != nil || !placed {
		t.Fatalf("after resume: placed=%v err=%v", placed, err)
	}
	if paused, _, _ := g.Status(); paused {
		t.Fatal("resumed")
	}
}

func TestLimits_ZeroMeansOffAndNothingChangesForExistingUsers(t *testing.T) {
	b := openStub()
	e := &aegis.Executor{Broker: b, BrokerName: "stub", MinConfidence: 45}
	if _, placed, err := e.ExecuteWithResult(context.Background(), fxIntent(5_000_000), "huge"); err != nil || !placed {
		t.Fatalf("no limits configured, no limits applied: placed=%v err=%v", placed, err)
	}
}

// Through the gateway the sender reads plain words, not "executor error: …".
func TestGateway_ALimitRefusalReadsInPlainWords(t *testing.T) {
	b := openStub()
	e := limitedExec(b, aegis.Limits{MaxUnits: 1000}, nil)
	gw := defaultGW(t, e.ExecuteWithResult)
	sig := buySignal("sig-limit-1")
	sig.Size = 50_000
	res, err := gw.Submit(context.Background(), sig)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Accepted || res.Placed || res.Code != aegis.ReasonExecutorSkip {
		t.Fatalf("an accepted signal that the limits stopped: %+v", res)
	}
	if !strings.HasPrefix(res.Reason, "That trade is bigger than the largest allowed") || strings.Contains(res.Reason, "executor error") {
		t.Errorf("plain reason expected: %q", res.Reason)
	}
}

// ── The limits are the person's own: they arrive with the trade ──────────────

func TestLimits_TheSendersOwnLimitsAreEnforcedWithNoServerLimitsAtAll(t *testing.T) {
	b := openStub()
	e := &aegis.Executor{Broker: b, BrokerName: "stub", MinConfidence: 45} // nothing configured on aegis
	intent := fxIntent(250_000)
	intent.Limits = aegis.Limits{MaxUnits: 100_000}
	_, placed, err := e.ExecuteWithResult(context.Background(), intent, "own1")
	mustRefuse(t, err, "100000 units")
	if placed || b.placeCalled != 0 {
		t.Fatal("the person's own ceiling must hold")
	}
	intent.Size = 100_000
	if _, placed, err := e.ExecuteWithResult(context.Background(), intent, "own2"); err != nil || !placed {
		t.Fatalf("within their own ceiling: placed=%v err=%v", placed, err)
	}
}

func TestLimits_WhenBothAreSetTheStricterWins(t *testing.T) {
	b := openStub()
	e := limitedExec(b, aegis.Limits{MaxUnits: 50_000}, nil) // a library user's ceiling
	intent := fxIntent(30_000)
	intent.Limits = aegis.Limits{MaxUnits: 20_000} // the person's, stricter
	_, _, err := e.ExecuteWithResult(context.Background(), intent, "s1")
	mustRefuse(t, err, "20000 units")
	intent.Limits = aegis.Limits{MaxUnits: 90_000} // the person's, looser: the library's still holds
	intent.Size = 60_000
	_, _, err = e.ExecuteWithResult(context.Background(), intent, "s2")
	mustRefuse(t, err, "50000 units")
}

func TestGateway_LimitsAreValidatedAndHandedToTheExecutor(t *testing.T) {
	var got aegis.Limits
	gw := defaultGW(t, func(_ context.Context, in aegis.OrderIntent, _ string) (string, bool, error) {
		got = in.Limits
		return "o1", true, nil
	})
	sig := buySignal("sig-lim-ok")
	sig.Limits = aegis.Limits{MaxUnits: 5000, MaxTradesPerDay: 2}
	if res, _ := gw.Submit(context.Background(), sig); !res.Placed || got != sig.Limits {
		t.Fatalf("limits must reach the executor: %+v got=%+v", res, got)
	}
	for name, bad := range map[string]aegis.Limits{
		"negative units": {MaxUnits: -1},
		"negative loss":  {MaxDailyLoss: -5},
		"NaN value":      {MaxCryptoValue: math.NaN()},
		"infinite loss":  {MaxDailyLoss: math.Inf(1)},
	} {
		s := buySignal("sig-lim-" + strings.ReplaceAll(name, " ", "-"))
		s.Limits = bad
		res, _ := gw.Submit(context.Background(), s)
		if res.Accepted || res.Code != aegis.ReasonValidation {
			t.Errorf("%s must be rejected, got %+v", name, res)
		}
	}
}
