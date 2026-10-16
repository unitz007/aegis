package aegis

import (
	"context"
	"fmt"
	"log"
	"math"
	"strings"
	"time"

	"github.com/unitz007/aegis/broker"
)

// ── Policy hook types ─────────────────────────────────────────────────────────

// IsAlgoPairFunc returns true when a symbol is driven by a deterministic algo
// engine (rather than an LLM). Algo pairs skip the MinConfidence gate and are
// stamped as source "algo" rather than "ai" in the TradeRequest.
//
// Safe standalone default: forex is inherently algo-driven; crypto and stocks
// default to LLM (confidence gated).
type IsAlgoPairFunc func(symbol, assetClass string) bool

// SessionGateFunc returns true when the symbol may be executed right now.
// Called by the executor after dup-check, before the order is built. Returning false defers
// the trade without consuming the signal_id reservation (the gateway reservation
// is already committed; only the broker order is withheld).
//
// Safe standalone default: AlwaysAllow — always returns true.
type SessionGateFunc func(symbol, assetClass string, now time.Time) bool

// AlwaysAllow is the default SessionGateFunc — all symbols are allowed at all times.
func AlwaysAllow(_, _ string, _ time.Time) bool { return true }

// DefaultIsAlgoPair is the default IsAlgoPairFunc. Forex is inherently
// algo-driven in the standalone case; crypto and stocks require confidence gating.
func DefaultIsAlgoPair(_, assetClass string) bool { return assetClass == "forex" }

// ── Store interfaces ──────────────────────────────────────────────────────────

// TradeStore optionally persists trade receipts and closed trade history.
// Nil is a valid value — the executor silently skips persistence when nil.
type TradeStore interface {
	SaveReceipt(ctx context.Context, r broker.TradeReceipt) error
	SaveClosedTrade(ctx context.Context, t broker.ClosedTrade) error
}

// TradeJournal optionally persists live trade records in a backtest-compatible
// schema so live and backtest performance can be analysed together.
// Nil is a valid value — the executor silently skips journal writes when nil.
type TradeJournal interface {
	SaveLiveTrade(ctx context.Context, r LiveTradeRecord) error
	CloseLiveTrade(ctx context.Context, decisionID, result string, pnlR, maeR, mfeR float64, exitTime int64) error
}

// LiveTradeRecord is the asset-class-agnostic journal record written at trade
// entry and closed by an outcome monitor.
type LiveTradeRecord struct {
	DecisionID    string
	BrokerTradeID string
	BrokerName    string
	Symbol        string
	EntryTime     int64  // Unix seconds
	Direction     string // "long" | "short"
	TradeType     string
	Entry         float64
	SL            float64
	TP            float64
	RR            float64
	Result        string // "open" | "win" | "loss"
	Source        string
	Strategy      string
}

// ── Executor ──────────────────────────────────────────────────────────────────

// Executor bridges validated OrderIntents to a live broker. It enforces:
//   - Minimum confidence gate (skipped for algo pairs)
//   - Dup-check against open positions and pending orders (fail-closed on error)
//   - Flip-close of opposite-direction positions (terminal — no new entry)
//   - Flip-cancel of opposite-direction pending orders (human orders exempt)
//   - Session gating via SessionGate hook
//   - Dealing-rules check of the requested crypto size (fail-safe: skip if provider
//     unavailable; an out-of-range size is skipped, never resized)
//   - Currency-exposure cap (optional)
//   - Trade journal and receipt persistence (both optional)
//
// Safe to call concurrently.
type Executor struct {
	Broker     broker.Port
	Store      TradeStore   // optional
	Journal    TradeJournal // optional
	BrokerName string

	MinConfidence    int     // minimum confidence for non-algo-pair decisions; default 55
	MaxCurrencyUnits float64 // per-currency exposure cap; 0 = disabled

	// Hooks — all optional; safe defaults are used when nil.
	IsAlgoPair  IsAlgoPairFunc
	SessionGate SessionGateFunc

	Logger *log.Logger
}

func (e *Executor) logger() *log.Logger {
	if e.Logger != nil {
		return e.Logger
	}
	return log.Default()
}

func (e *Executor) isAlgoPair(symbol, assetClass string) bool {
	if e.IsAlgoPair != nil {
		return e.IsAlgoPair(symbol, assetClass)
	}
	return DefaultIsAlgoPair(symbol, assetClass)
}

func (e *Executor) sessionOK(symbol, assetClass string, now time.Time) bool {
	if e.SessionGate != nil {
		return e.SessionGate(symbol, assetClass, now)
	}
	return AlwaysAllow(symbol, assetClass, now)
}

func (e *Executor) minConfidence() int {
	if e.MinConfidence > 0 {
		return e.MinConfidence
	}
	return 55
}

// Execute evaluates an intent and, if eligible, places a trade at the broker.
// Safe to call concurrently.
func (e *Executor) Execute(ctx context.Context, intent OrderIntent, decisionID string) error {
	_, _, err := e.ExecuteWithResult(ctx, intent, decisionID)
	return err
}

// ExecuteWithResult is Execute but returns the broker order ID and a placed flag
// so callers can distinguish "trade opened" from "gated/skipped" without parsing logs.
func (e *Executor) ExecuteWithResult(ctx context.Context, intent OrderIntent, decisionID string) (brokerOrderID string, placed bool, err error) {
	return e.innerExecute(ctx, intent, decisionID)
}

func (e *Executor) innerExecute(ctx context.Context, intent OrderIntent, decisionID string) (string, bool, error) {
	log := e.logger()
	sym := intent.Symbol
	dir := intent.Direction
	assetClass := intent.AssetClass

	// ── Algo-pair / confidence gate ───────────────────────────────────────────
	// Manual signals bypass the confidence gate — the human placing the trade
	// owns the conviction decision. The gate is also skipped for algo pairs.
	isAlgo := e.isAlgoPair(sym, assetClass)
	if !intent.Manual && !isAlgo && intent.Confidence < e.minConfidence() {
		log.Printf("executor: %s confidence %d below minimum %d — skip", sym, intent.Confidence, e.minConfidence())
		return "", false, nil
	}

	// ── Dup-check: open trades (fail-closed on error) ─────────────────────────
	// Safety invariant #1: if OpenTrades returns an error we MUST NOT place —
	// proceeding would defeat duplicate detection.
	openTrades, err := e.Broker.OpenTrades(ctx)
	if err != nil {
		log.Printf("executor: %s cannot fetch open trades — skip: %v", sym, err)
		return "", false, nil
	}
	for _, t := range openTrades {
		if !strings.EqualFold(t.Symbol, sym) {
			continue
		}
		existingDir := "BUY"
		if t.Units < 0 {
			existingDir = "SELL"
		}
		if existingDir == dir {
			log.Printf("executor: %s already has open %s position %s — skip", sym, dir, t.BrokerTradeID)
			return "", false, nil
		}
		// Opposite direction → flip-close.
		// Safety invariant #5: flip-close is terminal; no new entry is opened.
		// Fidelity: mirror original trade_executor.go:203-224 — write a
		// ClosedTrade with ExitReason "algo_flip" to the store after close.
		log.Printf("executor: %s flip-close opposite position %s before %s entry", sym, t.BrokerTradeID, dir)
		if cerr := e.Broker.CloseTrade(ctx, t.BrokerTradeID); cerr != nil {
			log.Printf("executor: %s flip-close %s failed: %v — skip", sym, t.BrokerTradeID, cerr)
			return "", false, nil
		}
		if e.Store != nil {
			ct := broker.ClosedTrade{
				BrokerName:  e.BrokerName,
				Symbol:      t.Symbol,
				Units:       t.Units,
				OpenPrice:   t.OpenPrice,
				ClosePrice:  t.CurrentPrice,
				RealizedPnL: t.UnrealizedPnL,
				OpenedAt:    t.OpenedAt,
				ClosedAt:    time.Now().UTC(),
				ExitReason:  "algo_flip",
			}
			if saveErr := e.Store.SaveClosedTrade(ctx, ct); saveErr != nil {
				log.Printf("executor: %s warn — save flip-close record: %v", sym, saveErr)
			}
		}
		return "", false, nil // flip-close is terminal
	}

	// ── Dup-check: open orders (fail-closed on error) ─────────────────────────
	openOrders, err := e.Broker.OpenOrders(ctx)
	if err != nil {
		log.Printf("executor: %s cannot fetch open orders — skip: %v", sym, err)
		return "", false, nil
	}
	for _, o := range openOrders {
		if !strings.EqualFold(o.Symbol, sym) {
			continue
		}
		if o.Direction == dir {
			log.Printf("executor: %s already has pending %s order %s — skip", sym, dir, o.BrokerOrderID)
			return "", false, nil
		}
		// Opposite direction → flip-cancel (skip human-placed orders).
		// Safety invariant #4: human-placed orders are never flip-cancelled.
		// Fidelity: mirror original trade_executor.go:246-250 — an opposite
		// HUMAN order causes a hard return (do not place the new entry).
		if strings.EqualFold(o.Source, "human") {
			log.Printf("executor: %s opposite human-placed order %s — skip flip-cancel, do not place new entry", sym, o.BrokerOrderID)
			return "", false, nil
		}
		log.Printf("executor: %s flip-cancel opposite order %s", sym, o.BrokerOrderID)
		if cerr := e.Broker.CancelOrder(ctx, o.BrokerOrderID); cerr != nil {
			log.Printf("executor: %s flip-cancel %s failed: %v", sym, o.BrokerOrderID, cerr)
		}
	}

	// ── Session gate ──────────────────────────────────────────────────────────
	// Manual signals bypass the session veto — the human has consciously chosen
	// to place outside the preferred session window.
	if !intent.Manual && !e.sessionOK(sym, assetClass, time.Now().UTC()) {
		log.Printf("executor: %s outside session window — defer", sym)
		return "", false, nil
	}

	// ── Order ─────────────────────────────────────────────────────────────────
	// aegis never decides how big a trade is. The caller's size is used exactly
	// as asked or the trade is skipped; it is never raised, lowered or rounded.
	req := broker.TradeRequest{
		Symbol:     sym,
		OrderType:  intent.OrderType,
		EntryPrice: intent.EntryPrice,
		TakeProfit: intent.TakeProfit,
		StopLoss:   intent.StopLoss,
		DecisionID: decisionID,
		TradeType:  intent.TradeType,
	}

	// Stamp source: algo pair → "algo", LLM-sourced → "ai".
	if isAlgo {
		req.Source = "algo"
	} else {
		req.Source = "ai"
	}

	if intent.Size <= 0 || math.IsNaN(intent.Size) || math.IsInf(intent.Size, 0) {
		log.Printf("executor: %s no valid size (%v) — skip", sym, intent.Size)
		return "", false, nil
	}

	switch assetClass {
	case "crypto":
		// Safety invariant #2: Capital.com crypto CFD "size" is in base-asset
		// units, not USD notional, so Size is a base-asset quantity here.
		//
		// Safety invariant #6: fail-safe if dealing-rules provider is absent or errors.
		drp, ok := e.Broker.(broker.DealingRulesProvider)
		if !ok {
			log.Printf("executor: %s broker does not implement DealingRulesProvider — skip crypto trade", sym)
			return "", false, nil
		}
		rules, err := drp.MarketDealingRules(ctx, sym)
		if err != nil {
			log.Printf("executor: %s dealing rules error: %v — skip", sym, err)
			return "", false, nil
		}

		// Safety invariant #7: a size outside the broker's limits is skipped, not
		// adjusted — the caller asked for exactly this much risk.
		if rules.MinDealSize > 0 && intent.Size < rules.MinDealSize {
			log.Printf("executor: %s size %.6f below min deal size %.6f — skip", sym, intent.Size, rules.MinDealSize)
			return "", false, nil
		}
		if rules.MaxDealSize > 0 && intent.Size > rules.MaxDealSize {
			log.Printf("executor: %s size %.6f above max deal size %.6f — skip", sym, intent.Size, rules.MaxDealSize)
			return "", false, nil
		}
		req.Quantity = intent.Size
		// Sign of Units still determines direction; value is 1 or -1 for crypto.
		if dir == "BUY" {
			req.Units = 1
		} else {
			req.Units = -1
		}

	default: // forex and stocks: Size is a whole number of units
		if intent.Size != math.Trunc(intent.Size) || intent.Size > math.MaxInt32 {
			log.Printf("executor: %s size %v is not a whole number of units — skip", sym, intent.Size)
			return "", false, nil
		}
		units := int(intent.Size)

		// Currency-exposure cap.
		if e.MaxCurrencyUnits > 0 {
			existing := BuildCurrencyExposure(openTrades)
			if err := CheckCurrencyExposure(existing, sym, units, e.MaxCurrencyUnits); err != nil {
				log.Printf("executor: %s exposure cap: %v — skip", sym, err)
				return "", false, nil
			}
		}

		if dir == "BUY" {
			req.Units = units
		} else {
			req.Units = -units
		}
	}

	// ── Place trade ───────────────────────────────────────────────────────────
	receipt, err := e.Broker.PlaceTrade(ctx, req)
	if err != nil {
		return "", false, fmt.Errorf("executor: place trade for %s: %w", sym, err)
	}
	log.Printf("executor: placed %s %s %s order=%s units=%d qty=%.6f",
		dir, sym, intent.OrderType, receipt.BrokerTradeID, receipt.Units, req.Quantity)

	// ── Persist receipt (optional) ────────────────────────────────────────────
	if e.Store != nil {
		if err := e.Store.SaveReceipt(ctx, receipt); err != nil {
			log.Printf("executor: warn — save receipt %s: %v", receipt.BrokerTradeID, err)
		}
	}

	// ── Journal entry (optional) ──────────────────────────────────────────────
	if e.Journal != nil {
		direction := "long"
		if dir == "SELL" {
			direction = "short"
		}
		rec := LiveTradeRecord{
			DecisionID:    decisionID,
			BrokerTradeID: receipt.BrokerTradeID,
			BrokerName:    e.BrokerName,
			Symbol:        sym,
			EntryTime:     receipt.OpenedAt.Unix(),
			Direction:     direction,
			TradeType:     intent.TradeType,
			Entry:         intent.EntryPrice,
			SL:            intent.StopLoss,
			TP:            intent.TakeProfit,
			RR:            intent.RiskReward,
			Result:        "open",
			Source:        intent.Source,
			Strategy:      intent.Strategy,
		}
		if err := e.Journal.SaveLiveTrade(ctx, rec); err != nil {
			log.Printf("executor: warn — journal save %s: %v", receipt.BrokerTradeID, err)
		}
	}

	return receipt.BrokerTradeID, true, nil
}
