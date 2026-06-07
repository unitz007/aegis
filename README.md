# aegis

**Status: pre-release (v0) — API may change before v1.0. No stable release tag has been cut yet.**

aegis is a standalone, open-source trade-execution safety harness for algorithmic trading systems. It lets any agent or signal producer — whether a language model talking over MCP, a pipeline posting a signed webhook, or in-process Go code — place trades through a pluggable broker adapter, with server-side position sizing, dealing-rules clamping, dedup/idempotency, allowlist-based authentication, and fail-closed safety defaults baked in. aegis defines its own types and imports nothing from any trading-system internals: drop it in, wire your `broker.Port`, populate an `AllowedSources` / `AllowedStrategies` allowlist, and submit `TradeSignal`s.

## Package layout

| Package | Contents |
|---|---|
| `github.com/unitz007/aegis` | Root: `Gateway`, `Executor`, `TradeSignal`, `OrderIntent`, `SignalResult`, sizing helpers (`SizeForex`, `SizeCrypto`, `ClampCryptoQuantity`), currency-exposure helpers, `ClassifySymbolDefault`, and the five injected-hook defaults |
| `github.com/unitz007/aegis/broker` | `Port` interface all broker adapters must satisfy; shared value types (`TradeRequest`, `TradeReceipt`, `OpenTrade`, `OpenOrder`, `BrokerAccount`, `ClosedTrade`); `DealingRulesProvider` optional capability |
| `github.com/unitz007/aegis/broker/paper` | In-memory `Broker` for tests and simulation; implements `broker.Port` and `broker.DealingRulesProvider`; lazy trigger-fill and auto-exit via optional `QuoteFetcher` |
| `github.com/unitz007/aegis/transport/mcp` | MCP Streamable-HTTP `Handler` exposing a `place_trade` tool; bearer-token auth (constant-time); fail-closed when `authToken` is empty or gateway is nil |
| `github.com/unitz007/aegis/transport/webhook` | HMAC-SHA256 signed POST handler; 60-second timestamp replay window; fail-closed when secret is empty or gateway is nil |
| `github.com/unitz007/aegis/examples` | Runnable `main.go` wiring the full harness with the paper broker |

## Quickstart

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/unitz007/aegis"
    "github.com/unitz007/aegis/broker/paper"
)

func main() {
    ctx := context.Background()

    // 1. Paper broker — 10 000 USD starting balance, no live quote feed.
    pb := paper.New(10_000, nil)

    // 2. Executor handles sizing, dup-check, session gating, and broker calls.
    exec := &aegis.Executor{
        Broker:     pb,
        BrokerName: "paper",
        RiskPct:    0.01, // 1% of balance risked per trade
        // All hook fields nil → safe standalone defaults apply (see below).
    }

    // 3. Gateway owns allowlist validation, idempotency, and OrderIntent construction.
    gw, err := aegis.NewGateway(
        func(ctx context.Context, intent aegis.OrderIntent, id string) (string, bool, error) {
            return exec.ExecuteWithResult(ctx, intent, id)
        },
        aegis.GatewayConfig{
            AllowedSources:    []string{"my-agent"},
            AllowedStrategies: []string{"smc"},
            MinRR:             1.5,
            // SymbolClassifier   → ClassifySymbolDefault (pure function, no network)
            // ExecutabilityCheck → DefaultExecutability  (forex + core-algo crypto only)
        },
    )
    if err != nil {
        log.Fatal(err)
    }

    // 4. Submit a signal — validation, idempotency, sizing, and execution in one call.
    result, err := gw.Submit(ctx, aegis.TradeSignal{
        Source:     "my-agent",
        Strategy:   "smc",
        SignalID:   "sig-001",       // idempotency key
        Symbol:     "EURUSD",
        Direction:  "BUY",
        OrderType:  "limit",
        Entry:      1.0850,
        StopLoss:   1.0810,         // 40-pip SL
        TakeProfit: 1.0910,         // 60-pip TP → RR 1.5
        Timestamp:  time.Now().Unix(),
    })
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("accepted=%t placed=%t code=%s order=%s\n",
        result.Accepted, result.Placed, result.Code, result.BrokerOrderID)
}
```

Run the full wired demo (paper broker, dedup, rejection examples, transport wiring):

```bash
go run github.com/unitz007/aegis/examples@latest
# or from a local checkout:
go run ./examples
```

## The injected-hooks model

Five hooks can be injected into `GatewayConfig` (for the gateway layer) and `Executor` (for the execution layer). Every hook has a safe standalone default that is used when the field is nil. **All defaults fail closed** — they restrict rather than permit.

| Hook | Type | Where injected | Safe default | Default behaviour |
|---|---|---|---|---|
| `SymbolClassifier` | `func(symbol string) string` | `GatewayConfig` | `ClassifySymbolDefault` | Pure function; classifies 6-letter fiat pairs as `"forex"`, known crypto bases as `"crypto"`, everything else as `"stock"`. No network call needed. |
| `ExecutabilityCheck` | `func(symbol, assetClass string) (bool, string)` | `GatewayConfig` | `DefaultExecutability` | **Fail-closed.** Accepts only `"forex"` and the 6 core-algo crypto symbols (`BTCUSD`, `ETHUSD`, `SOLUSD`, `BNBUSD`, `XRPUSD`, `ADAUSD`). Stocks and non-core crypto are **rejected before the signal_id slot is consumed**. Inject `AllExecutable` to allow every asset class. |
| `IsAlgoPair` | `IsAlgoPairFunc` = `func(symbol, assetClass string) bool` | `Executor` | `DefaultIsAlgoPair` | Forex is algo-driven (skips confidence gate); crypto and stocks default to AI-sourced (confidence gate applies). |
| `SessionGate` | `SessionGateFunc` = `func(symbol, assetClass string, now time.Time) bool` | `Executor` | `AlwaysAllow` | All symbols allowed at all times. Inject a custom gate to restrict trading to London/NY sessions. |
| `QuoteConverter` | `QuoteConverterFunc` = `func(ctx, symbol, entry) (factor float64, exact bool)` | `Executor` | `DefaultQuoteConverter` | Converts the quote-currency SL distance to account-currency units for forex sizing. Returns `1/entry` for USD-base pairs (e.g. USDJPY) — **corrects the ~150× under-sizing** that `IdentityConverter` causes. Cross pairs use a static rate table (`exact=false`). |

The defaults for `SymbolClassifier` and `ExecutabilityCheck` were chosen to match the hardened gates in production StockAI. `DefaultExecutability` rejects a stock symbol such as `"AAPL"` before calling `PlaceFunc`, so a misconfigured agent cannot accidentally route equity orders to a CFD broker.

## Safety guarantees

- **Server-side sizing only.** Position size is computed from account balance, `RiskPct`, and the signal's entry/SL levels inside the `Executor`. Signal producers never dictate units or quantity.

- **Dup-check fails closed.** If `Broker.OpenTrades` or `Broker.OpenOrders` returns an error, the executor skips rather than assuming "no positions" and risking duplicate stacking.

- **Idempotency / no-replay.** A `signal_id` is atomically reserved before any broker call. A second submission with the same `(source, signal_id)` returns the cached terminal result without calling the broker. A `PlaceFunc` error finalizes the slot as `ReasonExecutorSkip` so retries return the same outcome instead of re-placing.

- **Dealing-rules clamping for crypto.** The executor type-asserts `broker.DealingRulesProvider` and fetches per-instrument min/max deal sizes. Quantities below `MinDealSize` are skipped; quantities above `MaxDealSize` are clamped. **Never upsized** (upsizing would exceed the configured risk percentage).

- **Flip-close / flip-cancel is terminal.** When an opposite-direction open position or pending order exists, the executor closes/cancels it and returns without opening a new entry. Human-placed orders (`Source == "human"`) are never flip-cancelled.

- **Fail-closed auth on every transport.** The MCP handler returns 503 when `authToken` is empty; the webhook handler returns 503 when the signing secret is empty. Neither handler dispatches any tool logic in the disabled state.

- **Validation runs before slot reservation.** Allowlist, content, RR-floor, and executability checks all run before `getOrReserve` — a rejected signal never burns an idempotency slot.

## License

Apache-2.0. See [LICENSE](LICENSE).
