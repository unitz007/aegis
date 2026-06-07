package aegis

import (
	"context"
	"fmt"
	"log"
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
// Called by the executor after dup-check, before sizing. Returning false defers
// the trade without consuming the signal_id reservation (the gateway reservation
// is already committed; only the broker order is withheld).
//
// Safe standalone default: AlwaysAllow — always returns true.
type SessionGateFunc func(symbol, assetClass string, now time.Time) bool

// QuoteConverterFunc returns how many account-currency units equal one unit of
// the pair's QUOTE currency at the given entry price. Used to convert a
// quote-currency SL distance to account-currency risk for forex position sizing.
//
// Returns (factor, exact) where exact=false signals a static fallback was used.
// factor must always be > 0.
//
// Safe standalone default: IdentityConverter — always returns (1.0, false).
// This is correct for USD-quoted pairs (EURUSD, GBPUSD, etc.) but ~150× wrong
// for USD-base pairs like USDJPY. Inject a custom converter for JPY pairs.
type QuoteConverterFunc func(ctx context.Context, symbol string, entryPrice float64) (factor float64, exact bool)

// AlwaysAllow is the default SessionGateFunc — all symbols are allowed at all times.
func AlwaysAllow(_, _ string, _ time.Time) bool { return true }

// IdentityConverter is a QuoteConverterFunc that always returns (1.0, false).
// Correct only for USD-quoted pairs (EURUSD, GBPUSD, etc.). For USD-base pairs
// such as USDJPY this produces sizing ~150× too small. Use DefaultQuoteConverter
// as the executor default; inject IdentityConverter only when you genuinely need
// a hard 1.0 factor for all pairs.
func IdentityConverter(_ context.Context, _ string, _ float64) (float64, bool) { return 1.0, false }

// DefaultQuoteConverter is the default QuoteConverterFunc. It mirrors the
// semantics of domain.QuoteToUSD (StockAI pairs.go:159) using only the static
// rate table — no live broker quote is needed:
//
//   - QUOTE == "USD" (e.g. EURUSD, GBPUSD) → factor 1.0, exact=true
//   - BASE  == "USD" (e.g. USDJPY, USDCHF) → factor 1/entry, exact=true
//   - Cross pair (no USD leg)               → static table lookup, exact=false
//   - Non-fiat / unknown                    → 1.0, exact=false (safe default)
//
// This corrects the ~150× USDJPY under-sizing that IdentityConverter causes.
// Callers who need live-rate cross-pair accuracy should inject a custom
// QuoteConverterFunc that calls the broker's LiveQuote for the cross rate.
func DefaultQuoteConverter(_ context.Context, symbol string, entryPrice float64) (float64, bool) {
	return defaultQuoteConvert(symbol, entryPrice)
}

// defaultQuoteConvert is the pure (non-method) implementation of
// DefaultQuoteConverter, callable from sizing tests without a context.
func defaultQuoteConvert(symbol string, entryPrice float64) (float64, bool) {
	// Normalise: strip separators, upper-case.
	clean := strings.NewReplacer("/", "", "-", "", "_", "").Replace(strings.ToUpper(strings.TrimSpace(symbol)))

	// We need a 6-letter fiat pair to apply the correction.
	if len(clean) != 6 {
		return 1.0, false
	}
	base := clean[:3]
	quote := clean[3:]

	switch {
	case quote == "USD":
		return 1.0, true
	case base == "USD":
		if entryPrice > 0 {
			return 1.0 / entryPrice, true
		}
	}

	// Cross pair: use static table for the quote leg.
	if r, ok := staticQuoteUSD[quote]; ok && r > 0 {
		return r, false
	}
	return 1.0, false
}

// staticQuoteUSD is a coarse "USD per 1 unit of quote currency" table used as a
// last-resort fallback for cross-pair conversion. Values mirror the StockAI
// domain.staticQuoteUSD table (pairs.go:121-136) and are intentionally
// conservative — a live lookup always takes precedence when available.
var staticQuoteUSD = map[string]float64{
	"USD": 1.00,
	"EUR": 1.08,
	"GBP": 1.27,
	"JPY": 1.0 / 150.0,
	"CHF": 1.13,
	"CAD": 0.73,
	"AUD": 0.66,
	"NZD": 0.60,
	"SEK": 0.095,
	"NOK": 0.093,
	"DKK": 0.145,
	"SGD": 0.74,
	"HKD": 0.128,
	"CNY": 0.139,
}

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
//   - Position sizing via SizeForex / SizeCrypto
//   - Dealing-rules clamping for crypto (fail-safe: skip if provider unavailable)
//   - Currency-exposure cap (optional)
//   - Trade journal and receipt persistence (both optional)
//
// Safe to call concurrently.
type Executor struct {
	Broker     broker.Port
	Store      TradeStore   // optional
	Journal    TradeJournal // optional
	BrokerName string

	RiskPct          float64 // e.g. 0.01 for 1% risk per trade
	MinConfidence    int     // minimum confidence for non-algo-pair decisions; default 55
	MaxCurrencyUnits float64 // per-currency exposure cap; 0 = disabled

	// Hooks — all optional; safe defaults are used when nil.
	IsAlgoPair      IsAlgoPairFunc
	SessionGate     SessionGateFunc
	QuoteConverter  QuoteConverterFunc

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

func (e *Executor) quoteFactor(ctx context.Context, symbol string, entryPrice float64) (float64, bool) {
	if e.QuoteConverter != nil {
		return e.QuoteConverter(ctx, symbol, entryPrice)
	}
	return DefaultQuoteConverter(ctx, symbol, entryPrice)
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
	isAlgo := e.isAlgoPair(sym, assetClass)
	if !isAlgo && intent.Confidence < e.minConfidence() {
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
	if !e.sessionOK(sym, assetClass, time.Now().UTC()) {
		log.Printf("executor: %s outside session window — defer", sym)
		return "", false, nil
	}

	// ── Account ───────────────────────────────────────────────────────────────
	account, err := e.Broker.Account(ctx)
	if err != nil {
		return "", false, fmt.Errorf("executor: account fetch: %w", err)
	}

	// ── Position sizing ───────────────────────────────────────────────────────
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

	switch assetClass {
	case "crypto":
		// Safety invariant #2: Capital.com crypto CFD "size" is in base-asset
		// units, not USD notional. SizeCrypto uses the correct formula.
		qty, err := SizeCrypto(account.Balance, e.RiskPct, intent.EntryPrice, intent.StopLoss)
		if err != nil {
			log.Printf("executor: %s crypto sizing error: %v — skip", sym, err)
			return "", false, nil
		}

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

		// Safety invariant #7: clamp skips below minDealSize; never upsizes.
		clamped, ok := ClampCryptoQuantity(qty, rules.MinDealSize, rules.MaxDealSize)
		if !ok {
			log.Printf("executor: %s quantity %.6f below min deal size %.6f — skip", sym, qty, rules.MinDealSize)
			return "", false, nil
		}
		req.Quantity = clamped
		// Sign of Units still determines direction; value is 1 or -1 for crypto.
		if dir == "BUY" {
			req.Units = 1
		} else {
			req.Units = -1
		}

	default: // forex and stocks
		factor, _ := e.quoteFactor(ctx, sym, intent.EntryPrice)
		units, err := SizeForex(account.Balance, e.RiskPct, intent.EntryPrice, intent.StopLoss, factor)
		if err != nil {
			log.Printf("executor: %s forex sizing error: %v — skip", sym, err)
			return "", false, nil
		}
		if units == 0 {
			log.Printf("executor: %s position size rounded to 0 — skip", sym)
			return "", false, nil
		}

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
