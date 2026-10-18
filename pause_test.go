package aegis_test

// pause_test.go — the operator's stop halts every new trade, on every executor that
// shares it, before the broker is asked, and says why in plain words.

import (
	"context"
	"strings"
	"testing"

	"github.com/unitz007/aegis"
)

func pausableExec(b *stubBroker, g *aegis.Guard) *aegis.Executor {
	return &aegis.Executor{Broker: b, BrokerName: "stub", MinConfidence: 45, Guard: g}
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

func TestPause_StopsEverythingFirstAndResumeLetsItGo(t *testing.T) {
	g := aegis.NewGuard(nil)
	b := openStub()
	e := pausableExec(b, g)
	other := pausableExec(openStub(), g) // another executor, same guard
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

// Aegis sizes and limits nothing: a very large trade goes through untouched, because
// whether it is too big is the sender's decision, not the execution layer's.
func TestNoLimitsOfItsOwn_ATradeIsPlacedAsSent(t *testing.T) {
	b := openStub()
	e := &aegis.Executor{Broker: b, BrokerName: "stub", MinConfidence: 45}
	if _, placed, err := e.ExecuteWithResult(context.Background(), fxIntent(5_000_000), "huge"); err != nil || !placed {
		t.Fatalf("aegis has no ceilings of its own: placed=%v err=%v", placed, err)
	}
}

// Through the gateway the sender reads plain words, not "executor error: …".
func TestGateway_APauseReadsInPlainWords(t *testing.T) {
	g := aegis.NewGuard(nil)
	g.Pause("")
	gw := defaultGW(t, pausableExec(openStub(), g).ExecuteWithResult)
	res, err := gw.Submit(context.Background(), buySignal("sig-paused-1"))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Accepted || res.Placed || res.Code != aegis.ReasonExecutorSkip {
		t.Fatalf("an accepted signal that the pause stopped: %+v", res)
	}
	if !strings.HasPrefix(res.Reason, "Trading is paused right now") || strings.Contains(res.Reason, "executor error") {
		t.Errorf("plain reason expected: %q", res.Reason)
	}
}
