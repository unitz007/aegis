// Command examples demonstrates wiring the aegis Gateway end-to-end using the
// in-memory paper broker — no real broker account or network connection needed.
//
// It shows four canonical outcomes callers will encounter at runtime:
//
//  1. ACCEPT + PLACE  — a valid EURUSD BUY limit that passes all checks.
//  2. DEDUP (cached)  — the identical signal_id is re-submitted; the gateway
//                       returns the cached verdict without calling the broker.
//  3. REJECT (stock)  — AAPL is classified as "stock", which DefaultExecutability
//                       rejects before the signal_id slot is even reserved.
//  4. REJECT (RR)     — a GBPUSD signal with RR below the 1.5 floor is
//                       rejected during content validation.
//
// Additionally the example constructs (but does not serve) the MCP and webhook
// HTTP handlers to show how transports are mounted on a standard http.ServeMux.
//
// Run with:
//
//	go run ./examples
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/unitz007/aegis"
	"github.com/unitz007/aegis/broker/paper"
	aegismcp "github.com/unitz007/aegis/transport/mcp"
	"github.com/unitz007/aegis/transport/webhook"
)

func main() {
	ctx := context.Background()

	// ── 1. Construct a paper broker ─────────────────────────────────────────────
	//
	// paper.New takes a starting balance and an optional QuoteFetcher (used for
	// live P&L, trigger fills, and auto-exit). For this example we pass nil — the
	// broker fills limit orders at their trigger level when PlaceTrade is called
	// (the lazy fill path runs on the next OpenTrades / OpenOrders call).
	pb := paper.New(10_000, nil)

	// ── 2. Build the Executor ────────────────────────────────────────────────────
	//
	// Executor bridges an OrderIntent to the broker.  It handles:
	//   • confidence gating (skipped for algo pairs / forex)
	//   • dup-check against open trades and orders (fail-closed on error)
	//   • flip-close / flip-cancel for opposite positions
	//   • session gating (AlwaysAllow by default)
	//   • position sizing (SizeForex / SizeCrypto)
	//   • dealing-rules clamping for crypto (type-asserts DealingRulesProvider)
	//   • optional receipt + journal persistence
	exec := &aegis.Executor{
		Broker:     pb,
		BrokerName: "paper",
		RiskPct:    0.01, // risk 1% of balance per trade
		// All hook fields (IsAlgoPair, SessionGate, QuoteConverter) are nil here,
		// which causes the executor to use the safe standalone defaults:
		//   IsAlgoPair      → DefaultIsAlgoPair   (forex = algo, crypto/stocks = ai)
		//   SessionGate     → AlwaysAllow          (no session restriction)
		//   QuoteConverter  → DefaultQuoteConverter (correct for USD-quoted pairs)
	}

	// ── 3. Build the Gateway ─────────────────────────────────────────────────────
	//
	// NewGateway accepts a PlaceFunc (the executor's ExecuteWithResult) and a
	// GatewayConfig.  All zero GatewayConfig fields apply safe defaults:
	//   MinRR              → 1.5
	//   IdempotencyTTL     → 10 minutes
	//   SymbolClassifier   → ClassifySymbolDefault   (pure function, no network)
	//   ExecutabilityCheck → DefaultExecutability    (forex + core-algo crypto only;
	//                                                 stocks + non-core crypto rejected)
	gw, err := aegis.NewGateway(
		// PlaceFunc wraps Executor.ExecuteWithResult so the gateway can call it
		// without knowing anything about the executor's internal structure.
		func(ctx context.Context, intent aegis.OrderIntent, decisionID string) (string, bool, error) {
			return exec.ExecuteWithResult(ctx, intent, decisionID)
		},
		aegis.GatewayConfig{
			AllowedSources: []string{
				"demo-agent", // the source tag used in the signals below
			},
			AllowedStrategies: []string{
				"smc",      // Smart Money Concepts structural analysis
				"momentum", // any additional strategy the caller uses
			},
			MinRR: 1.5, // reject signals whose reward:risk < 1.5
			// Logger and Clock left nil → use log.Default() and time.Now.
		},
	)
	if err != nil {
		log.Fatalf("aegis: NewGateway: %v", err)
	}

	logger := log.New(os.Stdout, "", 0)

	// ── Helper: pretty-print a SignalResult ──────────────────────────────────────
	printResult := func(label string, result aegis.SignalResult, submitErr error) {
		if submitErr != nil {
			logger.Printf("[%s] ERROR: %v\n", label, submitErr)
			return
		}
		b, _ := json.MarshalIndent(result, "  ", "  ")
		logger.Printf("[%s]\n  %s\n", label, b)
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// Scenario 1 — ACCEPT + PLACE
	//   EURUSD BUY limit.  Levels produce RR = (1.0910-1.0850)/(1.0850-1.0810)
	//   = 0.0060/0.0040 = 1.5 — exactly at the floor.
	//   DefaultExecutability accepts "forex".
	//   The paper broker queues a LIMIT order (no QuoteFetcher → no auto-fill yet).
	// ═══════════════════════════════════════════════════════════════════════════
	fmt.Println("─── Scenario 1: valid EURUSD BUY limit ───────────────────────────────")
	r1, err := gw.Submit(ctx, aegis.TradeSignal{
		Source:     "demo-agent",
		Strategy:   "smc",
		SignalID:   "sig-eurusd-001",
		Symbol:     "EURUSD",
		Direction:  "BUY",
		OrderType:  "limit",
		Entry:      1.0850,
		StopLoss:   1.0810,
		TakeProfit: 1.0910,
		Note:       "BOS retest at H4 OB, London session",
		Timestamp:  time.Now().Unix(),
	})
	printResult("ACCEPT+PLACE", r1, err)

	// ═══════════════════════════════════════════════════════════════════════════
	// Scenario 2 — DEDUP (cached result)
	//   Re-submit the identical signal_id "sig-eurusd-001" with the same source.
	//   The gateway's idempotency store holds the finalized result from Scenario 1
	//   and returns it immediately — the broker is never called a second time.
	// ═══════════════════════════════════════════════════════════════════════════
	fmt.Println("─── Scenario 2: duplicate signal_id (dedup) ──────────────────────────")
	r2, err := gw.Submit(ctx, aegis.TradeSignal{
		Source:    "demo-agent",
		Strategy:  "smc",
		SignalID:  "sig-eurusd-001", // same id — triggers dedup
		Symbol:    "EURUSD",
		Direction: "BUY",
		OrderType: "limit",
		Entry:     1.0850,
		StopLoss:  1.0810,
		TakeProfit: 1.0910,
		Timestamp: time.Now().Unix(),
	})
	printResult("DEDUP", r2, err)

	// ═══════════════════════════════════════════════════════════════════════════
	// Scenario 3 — REJECT: stock symbol
	//   AAPL is classified as "stock" by ClassifySymbolDefault.
	//   DefaultExecutability rejects it with:
	//     "asset class \"stock\" is not executable by the default executor"
	//   The signal_id slot is NEVER reserved (validation runs pre-reservation).
	//   Inject AllExecutable into GatewayConfig.ExecutabilityCheck to allow stocks.
	// ═══════════════════════════════════════════════════════════════════════════
	fmt.Println("─── Scenario 3: stock symbol rejected (DefaultExecutability) ─────────")
	r3, err := gw.Submit(ctx, aegis.TradeSignal{
		Source:     "demo-agent",
		Strategy:   "smc",
		SignalID:   "sig-aapl-001",
		Symbol:     "AAPL",
		Direction:  "BUY",
		OrderType:  "limit",
		Entry:      220.00,
		StopLoss:   215.00,
		TakeProfit: 232.50,
		Timestamp:  time.Now().Unix(),
	})
	printResult("REJECT(stock)", r3, err)

	// ═══════════════════════════════════════════════════════════════════════════
	// Scenario 4 — REJECT: insufficient RR
	//   GBPUSD BUY with RR = (1.2680-1.2640)/(1.2640-1.2620) = 0.0040/0.0020
	//   = 2.0 — passes.  We deliberately set TP too close so RR = 1.2 < 1.5.
	//   RR = (1.2652-1.2640)/(1.2640-1.2630) = 0.0012/0.0010 = 1.2 → rejected.
	// ═══════════════════════════════════════════════════════════════════════════
	fmt.Println("─── Scenario 4: GBPUSD BUY rejected (RR below floor) ─────────────────")
	r4, err := gw.Submit(ctx, aegis.TradeSignal{
		Source:     "demo-agent",
		Strategy:   "smc",
		SignalID:   "sig-gbpusd-001",
		Symbol:     "GBPUSD",
		Direction:  "BUY",
		OrderType:  "limit",
		Entry:      1.2640,
		StopLoss:   1.2630, // 10-pip SL
		TakeProfit: 1.2652, // 12-pip TP → RR 1.2 < 1.5
		Timestamp:  time.Now().Unix(),
	})
	printResult("REJECT(RR)", r4, err)

	// ── Paper broker state after the scenarios ───────────────────────────────────
	fmt.Println("─── Paper broker state ───────────────────────────────────────────────")
	acct, _ := pb.Account(ctx)
	fmt.Printf("  balance=%.2f  open_trades=%d  open_orders=%d\n\n",
		acct.Balance, pb.TradeCount(), pb.OrderCount())

	// ═══════════════════════════════════════════════════════════════════════════
	// Transport wiring (handlers constructed but not served)
	//
	// In production you would call http.ListenAndServe or integrate these handlers
	// into your existing Gin/Chi/stdlib mux.  Here we construct them and register
	// them on a ServeMux just to show the wiring — we never call ListenAndServe.
	// ═══════════════════════════════════════════════════════════════════════════
	fmt.Println("─── Transport wiring (mux constructed, not served) ───────────────────")

	mux := http.NewServeMux()

	// MCP Streamable-HTTP handler — exposes a "place_trade" MCP tool.
	// Fail-closed: returns 503 if authToken is empty or gateway is nil.
	// When enabled every request passes constant-time bearer-auth before any
	// tool dispatch.  In production set MCP_AUTH_TOKEN from a secrets manager.
	mcpAuthToken := "demo-secret-token" // non-empty → handler enabled
	mcpHandler := aegismcp.Handler(
		mcpAuthToken,
		gw,
		log.Default(),
		aegismcp.WithSourceTag("mcp-agent"),
		aegismcp.WithDefaultStrategy("smc"),
	)
	mux.Handle("/mcp/", mcpHandler)
	fmt.Printf("  MCP handler mounted at /mcp/  (enabled=%t)\n",
		aegismcp.Enabled(mcpAuthToken, gw))

	// Webhook handler — HMAC-SHA256 signed POST /signal/trade.
	// Fail-closed: returns 503 if secret is empty or gateway is nil.
	// 60-second replay window prevents stale-signal injection.
	webhookSecret := "demo-webhook-secret"
	whHandler := webhook.Handler(
		webhookSecret,
		gw,
		webhook.WithSourceTag("webhook-agent"),
		webhook.WithDefaultStrategy("smc"),
		webhook.WithTimestampTolerance(60*time.Second),
	)
	mux.Handle("/signal/trade", whHandler)
	fmt.Printf("  Webhook handler mounted at /signal/trade  (secret non-empty=%t)\n",
		webhookSecret != "")

	// Demonstrate that gw.AllowedStrategies() can be used to advertise the
	// strategy enum without duplicating the allowlist (e.g. in API docs / MCP
	// tool schema).  The MCP handler uses this internally.
	fmt.Printf("  Gateway allowed strategies: %v\n", gw.AllowedStrategies())
	fmt.Println()
	fmt.Println("Done. Run `go run ./examples` to see this output.")

	// Suppress the "declared and not used" error for mux in environments that
	// optimize away the variable.
	_ = mux
}
