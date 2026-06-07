// Package aegis is a standalone trade-execution harness. It accepts TradeSignals
// from any producer, validates them against caller-configured allowlists,
// enforces idempotency, delegates to an Executor for sizing and broker routing,
// and returns a machine-readable SignalResult.
//
// Core safety invariants (preserved from origin implementation):
//
//  1. Validation rejects never consume a signal_id slot — the idempotency
//     reservation occurs only after all validation passes.
//  2. An in-flight race loser returns ReasonInFlight (not ReasonValidation).
//  3. A PlaceFunc error is surfaced as Accepted=true/Placed=false/ReasonExecutorSkip
//     so the signal_id is finalized and retries return the same terminal state.
//  4. Empty AllowedSources or AllowedStrategies means reject-all (fail-closed).
package aegis

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// ── Signal types ─────────────────────────────────────────────────────────────

// TradeSignal is the unified, agent-agnostic trade request submitted to the
// Gateway. All price levels are absolute prices in the symbol's quote units.
// Source and Strategy are plain strings; the Gateway validates them against
// caller-configured allowlists in GatewayConfig.
type TradeSignal struct {
	Source     string  `json:"source"`     // who produced it — validated against AllowedSources
	Strategy   string  `json:"strategy"`   // how levels were derived — validated against AllowedStrategies
	SignalID   string  `json:"signal_id"`  // idempotency key — required, non-empty
	Symbol     string  `json:"symbol"`
	Direction  string  `json:"direction"`  // "BUY" | "SELL"
	OrderType  string  `json:"order_type"` // "limit" | "market" | "" (→ "limit")
	Entry      float64 `json:"entry"`
	StopLoss   float64 `json:"stop_loss"`
	TakeProfit float64 `json:"take_profit"`
	Note       string  `json:"note"`
	Timestamp  int64   `json:"timestamp"` // unix seconds
}

// SignalReasonCode is a stable, machine-readable verdict classification.
// Transport adapters (MCP, webhook) switch on this — NOT on the human-readable
// Reason string. The codes are the single source of truth for HTTP status
// mapping (ReasonInFlight → 409, ReasonValidation → 400, others → 200).
type SignalReasonCode string

const (
	// ReasonPlaced — accepted and the executor placed a broker order.
	ReasonPlaced SignalReasonCode = "placed"

	// ReasonExecutorSkip — accepted, but the executor placed no new order
	// (safety gate, broker-side dup-check, flip-close, or execution error).
	// Still a terminal accepted outcome; never a conflict.
	ReasonExecutorSkip SignalReasonCode = "executor_skip"

	// ReasonInFlight — a duplicate (source, signal_id) is currently being
	// processed by another in-flight submission. Race losers must NOT place.
	// Maps to HTTP 409.
	ReasonInFlight SignalReasonCode = "in_flight"

	// ReasonValidation — failed allowlist / symbol / level / RR / signal_id
	// validation. Maps to HTTP 400.
	ReasonValidation SignalReasonCode = "validation"
)

// SignalResult is the Gateway's verdict for a submitted TradeSignal.
type SignalResult struct {
	Accepted      bool            `json:"accepted"`        // passed validation + idempotency
	Placed        bool            `json:"placed"`          // executor actually placed a broker order
	BrokerOrderID string          `json:"broker_order_id"` // set when Placed=true
	Reason        string          `json:"reason"`          // human-readable — for logs only; may embed caller input
	Code          SignalReasonCode `json:"code"`            // machine-readable — switch on this
}

// ── Executor-facing intent ────────────────────────────────────────────────────

// OrderIntent is the executor's view of a trade to place. It is what the
// Gateway constructs from a validated TradeSignal and passes to PlaceFunc. It
// carries all fields needed for sizing, dup-check, session gating, and journal
// attribution, but none of the pipeline-specific fields (grade, TA fingerprint,
// TF narrations) present in a full pipeline decision.
type OrderIntent struct {
	Symbol     string
	AssetClass string  // "forex" | "crypto" | "stock" — populated by SymbolClassifier hook
	Direction  string  // "BUY" | "SELL"
	OrderType  string  // "limit" | "market"
	EntryPrice float64
	StopLoss   float64
	TakeProfit float64
	RiskReward float64
	TradeType  string // caller-supplied tag (e.g. "smc_engine", "external_signal")
	Source     string // provenance axis 1: who produced the signal
	Strategy   string // provenance axis 2: how levels were derived
	// Confidence is used by the executor only when IsAlgoPair returns false
	// (LLM-sourced decisions). Gateway-submitted signals default to 60.
	Confidence int
	// ReasonText is a single audit string surfaced in the trade journal.
	ReasonText string
}

// ── Gateway interface and config ──────────────────────────────────────────────

// Gateway is the unified submission interface for trade signals.
// A single Submit call owns: allowlist validation, signal content validation,
// RR floor, idempotency (atomic reserve), OrderIntent construction, and
// delegation to the Executor via PlaceFunc.
//
// AllowedStrategies returns the list of strategy strings the gateway accepts.
// Transport adapters (e.g. MCP) call this to advertise and pre-validate the
// strategy enum without duplicating the allowlist. The slice is a snapshot;
// callers must not mutate it.
type Gateway interface {
	Submit(ctx context.Context, sig TradeSignal) (SignalResult, error)
	AllowedStrategies() []string
}

// PlaceFunc is the executor callback injected into the Gateway.
// Returning an error causes Accepted=true/Placed=false/ReasonExecutorSkip.
// The signature matches Executor.ExecuteWithResult so the gateway can be wired
// directly without an adapter layer.
type PlaceFunc func(ctx context.Context, intent OrderIntent, decisionID string) (brokerOrderID string, placed bool, err error)

// SymbolClassifier classifies a symbol into an asset-class string.
// Return value must be one of "forex", "crypto", or "stock".
// The default is ClassifySymbolDefault — a pure function that needs no network.
type SymbolClassifier func(symbol string) string

// ExecutabilityCheck reports whether a symbol (already classified) is actually
// executable by the downstream executor. Returning false causes a
// pre-reservation validation reject in the Gateway with a descriptive reason
// string, preventing the signal_id slot from being consumed.
// The default is AllExecutable — always returns (true, "").
type ExecutabilityCheck func(symbol, assetClass string) (ok bool, reason string)

// GatewayConfig holds all tunable Gateway knobs. All fields have safe defaults.
type GatewayConfig struct {
	// AllowedSources is the set of source strings the Gateway accepts.
	// Empty = reject all (fail-closed). Callers must populate this.
	AllowedSources []string

	// AllowedStrategies is the set of strategy strings the Gateway accepts.
	// Empty = reject all (fail-closed). Callers must populate this.
	AllowedStrategies []string

	// MinRR is the minimum reward-to-risk ratio a signal must satisfy.
	// Default: 1.5.
	MinRR float64

	// IdempotencyTTL bounds how long a completed signal_id reservation is
	// remembered. Default: 10 minutes.
	IdempotencyTTL time.Duration

	// SymbolClassifier classifies a symbol as "forex", "crypto", or "stock".
	// Default: ClassifySymbolDefault.
	SymbolClassifier SymbolClassifier

	// ExecutabilityCheck reports whether the Gateway should accept a symbol
	// for execution. Default: DefaultExecutability (forex + core-algo crypto
	// only; stocks and non-core crypto are rejected before the slot is consumed).
	// Inject AllExecutable to allow every symbol.
	ExecutabilityCheck ExecutabilityCheck

	// Logger may be nil; falls back to log.Default().
	Logger *log.Logger

	// Clock may be nil; falls back to time.Now.
	Clock func() time.Time
}

// AllowedSources returns a copy of the configured sources slice.
func (c GatewayConfig) allowedSourcesSet() map[string]struct{} {
	m := make(map[string]struct{}, len(c.AllowedSources))
	for _, s := range c.AllowedSources {
		m[s] = struct{}{}
	}
	return m
}

func (c GatewayConfig) allowedStrategiesSet() map[string]struct{} {
	m := make(map[string]struct{}, len(c.AllowedStrategies))
	for _, s := range c.AllowedStrategies {
		m[s] = struct{}{}
	}
	return m
}

// NewGateway constructs a Gateway. place must be non-nil.
// Default values are applied for any zero-valued config fields.
func NewGateway(place PlaceFunc, cfg GatewayConfig) (Gateway, error) {
	if place == nil {
		return nil, fmt.Errorf("aegis: NewGateway: place func must be non-nil")
	}
	if cfg.MinRR <= 0 {
		cfg.MinRR = 1.5
	}
	if cfg.IdempotencyTTL <= 0 {
		cfg.IdempotencyTTL = 10 * time.Minute
	}
	if cfg.SymbolClassifier == nil {
		cfg.SymbolClassifier = ClassifySymbolDefault
	}
	if cfg.ExecutabilityCheck == nil {
		cfg.ExecutabilityCheck = DefaultExecutability
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	return &signalGateway{
		place:    place,
		cfg:      cfg,
		sources:  cfg.allowedSourcesSet(),
		strats:   cfg.allowedStrategiesSet(),
		inflight: make(map[string]inflightEntry),
		done:     make(map[string]signalEntry),
	}, nil
}

// ── idempotency store ─────────────────────────────────────────────────────────

type signalEntry struct {
	result    SignalResult
	expiresAt time.Time
}

// inflightTTL is the maximum time an in-flight reservation is considered live.
// If g.place panics after getOrReserve, the deferred finalize call cleans up
// immediately. This TTL is an additional backstop: stale entries (e.g. from a
// goroutine that was hard-killed without running defers) are evicted on the next
// getOrReserve call so the slot is never permanently wedged.
const inflightTTL = 5 * time.Minute

type inflightEntry struct {
	reservedAt time.Time
}

type signalGateway struct {
	place   PlaceFunc
	cfg     GatewayConfig
	sources map[string]struct{}
	strats  map[string]struct{}

	mu       sync.Mutex
	inflight map[string]inflightEntry // signal_ids currently being placed
	done     map[string]signalEntry   // completed signal_ids with cached results
}

// Submit validates the signal, reserves an idempotency slot, builds an
// OrderIntent, calls PlaceFunc, and returns the verdict.
//
// Safety invariants honored:
//  1. All validation runs BEFORE getOrReserve — rejected signals never burn a slot.
//  2. Race losers on getOrReserve return ReasonInFlight, not ReasonValidation.
//  3. PlaceFunc errors → Accepted=true/Placed=false/ReasonExecutorSkip so the
//     slot is finalized and retries return the same terminal state.
func (g *signalGateway) Submit(ctx context.Context, sig TradeSignal) (SignalResult, error) {
	// ── Phase 1: validate BEFORE touching idempotency state ──────────────────
	if result, ok := g.validateSignal(sig); !ok {
		return result, nil
	}

	slotKey := sig.Source + ":" + sig.SignalID

	// ── Phase 2: idempotency check / reserve ─────────────────────────────────
	if result, verdict := g.getOrReserve(slotKey); verdict != "" {
		switch verdict {
		case "cached":
			return result, nil
		case "inflight":
			return SignalResult{
				Accepted: false,
				Reason:   fmt.Sprintf("signal %s already in flight", sig.SignalID),
				Code:     ReasonInFlight,
			}, nil
		}
	}

	// ── Phase 3: build OrderIntent ────────────────────────────────────────────
	assetClass := g.cfg.SymbolClassifier(sig.Symbol)
	var rr float64
	slDist := sig.Entry - sig.StopLoss
	if slDist < 0 {
		slDist = -slDist
	}
	tpDist := sig.TakeProfit - sig.Entry
	if tpDist < 0 {
		tpDist = -tpDist
	}
	if slDist > 0 {
		rr = tpDist / slDist
	}

	ot := sig.OrderType
	if ot == "" {
		ot = "limit"
	}

	intent := OrderIntent{
		Symbol:     sig.Symbol,
		AssetClass: assetClass,
		Direction:  sig.Direction,
		OrderType:  ot,
		EntryPrice: sig.Entry,
		StopLoss:   sig.StopLoss,
		TakeProfit: sig.TakeProfit,
		RiskReward: rr,
		TradeType:  "external_signal",
		Source:     sig.Source,
		Strategy:   sig.Strategy,
		Confidence: 60, // default for gateway-submitted signals
		ReasonText: sig.Note,
	}

	decisionID := slotKey

	// ── Phase 4: delegate to executor ────────────────────────────────────────
	// Safety invariant #3 (panic-safe inflight): finalize MUST run even if
	// g.place panics, so the slot is never permanently wedged. The deferred
	// finalize writes a terminal ReasonExecutorSkip result (set below) and then
	// the panic propagates normally. The TTL-based eviction in getOrReserve is a
	// secondary backstop for goroutines that are hard-killed without running defers.
	result := SignalResult{
		Accepted: true,
		Placed:   false,
		Reason:   "executor skipped (dup-check, gate, or flip-close)",
		Code:     ReasonExecutorSkip,
	}
	defer func() {
		g.finalize(slotKey, result)
	}()

	brokerOrderID, placed, err := g.place(ctx, intent, decisionID)

	if err != nil {
		g.cfg.Logger.Printf("aegis gateway: executor error for %s/%s: %v", sig.Source, sig.SignalID, err)
		result = SignalResult{
			Accepted: true,
			Placed:   false,
			Reason:   fmt.Sprintf("executor error: %v", err),
			Code:     ReasonExecutorSkip,
		}
	} else if placed {
		result = SignalResult{
			Accepted:      true,
			Placed:        true,
			BrokerOrderID: brokerOrderID,
			Reason:        "order placed",
			Code:          ReasonPlaced,
		}
	}
	// else: result keeps its default ReasonExecutorSkip value.

	return result, nil
}

// validateSignal runs all allowlist and content checks. Returns (zero, true) on
// pass, or (rejection result, false) on any failure.
func (g *signalGateway) validateSignal(sig TradeSignal) (SignalResult, bool) {
	reject := func(msg string) (SignalResult, bool) {
		return SignalResult{
			Accepted: false,
			Reason:   msg,
			Code:     ReasonValidation,
		}, false
	}

	if sig.SignalID == "" {
		return reject("signal_id must be non-empty")
	}
	if _, ok := g.sources[sig.Source]; !ok {
		return reject(fmt.Sprintf("unknown source %q — not in AllowedSources", sig.Source))
	}
	if _, ok := g.strats[sig.Strategy]; !ok {
		return reject(fmt.Sprintf("unknown strategy %q — not in AllowedStrategies", sig.Strategy))
	}
	if sig.Symbol == "" {
		return reject("symbol must be non-empty")
	}
	if sig.Direction != "BUY" && sig.Direction != "SELL" {
		return reject(fmt.Sprintf("direction must be BUY or SELL, got %q", sig.Direction))
	}
	if sig.Entry <= 0 {
		return reject("entry price must be > 0")
	}
	if sig.StopLoss <= 0 {
		return reject("stop_loss must be > 0")
	}
	if sig.TakeProfit <= 0 {
		return reject("take_profit must be > 0")
	}

	// Direction-consistent level checks.
	if sig.Direction == "BUY" {
		if sig.StopLoss >= sig.Entry {
			return reject("BUY signal: stop_loss must be below entry")
		}
		if sig.TakeProfit <= sig.Entry {
			return reject("BUY signal: take_profit must be above entry")
		}
	} else {
		if sig.StopLoss <= sig.Entry {
			return reject("SELL signal: stop_loss must be above entry")
		}
		if sig.TakeProfit >= sig.Entry {
			return reject("SELL signal: take_profit must be below entry")
		}
	}

	// RR floor.
	slDist := sig.Entry - sig.StopLoss
	if slDist < 0 {
		slDist = -slDist
	}
	tpDist := sig.TakeProfit - sig.Entry
	if tpDist < 0 {
		tpDist = -tpDist
	}
	if slDist > 0 {
		rr := tpDist / slDist
		if rr < g.cfg.MinRR {
			return reject(fmt.Sprintf("RR %.2f below minimum %.2f", rr, g.cfg.MinRR))
		}
	}

	// Executability predicate (runs before slot reservation).
	assetClass := g.cfg.SymbolClassifier(sig.Symbol)
	if ok, reason := g.cfg.ExecutabilityCheck(sig.Symbol, assetClass); !ok {
		return reject(fmt.Sprintf("symbol %s not executable: %s", sig.Symbol, reason))
	}

	return SignalResult{}, true
}

// getOrReserve checks whether the slot is cached (return "cached"), in-flight
// (return "inflight"), or new (reserve it and return "").
//
// Stale in-flight entries (older than inflightTTL) are evicted so a panicking
// PlaceFunc that never ran its defer cannot wedge a slot permanently.
func (g *signalGateway) getOrReserve(key string) (SignalResult, string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.cfg.Clock()

	if entry, ok := g.done[key]; ok {
		if now.Before(entry.expiresAt) {
			return entry.result, "cached"
		}
		delete(g.done, key)
	}
	if e, ok := g.inflight[key]; ok {
		// Evict stale in-flight entries (safety net for panics that bypass defer).
		if now.Sub(e.reservedAt) < inflightTTL {
			return SignalResult{}, "inflight"
		}
		// Stale — treat as gone so a new attempt can proceed.
		delete(g.inflight, key)
	}
	g.inflight[key] = inflightEntry{reservedAt: now}
	return SignalResult{}, ""
}

// AllowedStrategies returns a snapshot of the configured strategy allowlist.
// The returned slice is a copy; callers may not mutate it.
func (g *signalGateway) AllowedStrategies() []string {
	s := make([]string, 0, len(g.strats))
	for k := range g.strats {
		s = append(s, k)
	}
	return s
}

// finalize promotes a slot from in-flight to done with the final result.
func (g *signalGateway) finalize(key string, result SignalResult) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.inflight, key)
	g.done[key] = signalEntry{
		result:    result,
		expiresAt: g.cfg.Clock().Add(g.cfg.IdempotencyTTL),
	}
}

// ── Default hook implementations ──────────────────────────────────────────────

// AllExecutable is an ExecutabilityCheck that accepts every symbol.
// Use this when your broker supports all asset classes and you need no
// restriction. It is NOT the executor's default — see DefaultExecutability.
func AllExecutable(_, _ string) (bool, string) { return true, "" }

// coreAlgoCryptoSymbols is the canonical set of crypto symbols handled by the
// deterministic SMC engine. Mirrors StockAI's coreCryptoAlgoSymbols map.
// All keys are upper-case, separator-free.
var coreAlgoCryptoSymbols = map[string]bool{
	"BTCUSD": true,
	"ETHUSD": true,
	"SOLUSD": true,
	"BNBUSD": true,
	"XRPUSD": true,
	"ADAUSD": true,
}

// DefaultExecutability is the default ExecutabilityCheck. It mirrors the hard
// gate in StockAI's trade_executor.go (line 103) and signal_gateway.go (line 222):
// only forex pairs and the 6 core-algo crypto symbols are accepted. Everything
// else (stocks, non-core crypto) is rejected before a signal_id slot is consumed.
//
// This makes a default-configured executor fail CLOSED — a stock symbol such as
// "AAPL" never reaches PlaceTrade. Callers who need unrestricted execution must
// explicitly inject AllExecutable.
func DefaultExecutability(symbol, assetClass string) (bool, string) {
	switch assetClass {
	case "forex":
		return true, ""
	case "crypto":
		norm := strings.ToUpper(strings.NewReplacer("/", "", "-", "", "_", "").Replace(symbol))
		if coreAlgoCryptoSymbols[norm] {
			return true, ""
		}
		// Build the allowed list for the rejection message.
		allowed := make([]string, 0, len(coreAlgoCryptoSymbols))
		for s := range coreAlgoCryptoSymbols {
			allowed = append(allowed, s)
		}
		return false, fmt.Sprintf("only core algo crypto supported: %s", strings.Join(allowed, "/"))
	default:
		return false, fmt.Sprintf("asset class %q is not executable by the default executor (forex and core-algo crypto only)", assetClass)
	}
}
