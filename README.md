# aegis

**Status: v0.1.0 — first tagged release. The API may still change before v1.0.**

aegis is a standalone, open-source trade-execution safety harness for algorithmic trading systems. It lets any agent or signal producer — whether a language model talking over MCP, a pipeline posting a signed webhook, or in-process Go code — place trades through a pluggable broker adapter, with dealing-rules checks, dedup/idempotency, allowlist-based authentication, and fail-closed safety defaults baked in. aegis defines its own types and imports nothing from any trading-system internals: drop it in, wire your `broker.Port`, populate an `AllowedSources` / `AllowedStrategies` allowlist, and submit `TradeSignal`s.

## Package layout

| Package | Contents |
|---|---|
| `github.com/unitz007/aegis` | Root: `Gateway`, `Executor`, `TradeSignal`, `OrderIntent`, `SignalResult`, currency-exposure helpers, `ClassifySymbolDefault`, and the five injected-hook defaults |
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

    // 2. Executor handles dup-check, session gating, and broker calls.
    exec := &aegis.Executor{
        Broker:     pb,
        BrokerName: "paper",
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
            // SymbolClassifier   → ClassifySymbolDefault (pure function, no network)
            // ExecutabilityCheck → DefaultExecutability  (forex + core-algo crypto only)
        },
    )
    if err != nil {
        log.Fatal(err)
    }

    // 4. Submit a signal — validation, idempotency, and execution in one call.
    result, err := gw.Submit(ctx, aegis.TradeSignal{
        Source:     "my-agent",
        Strategy:   "smc",
        SignalID:   "sig-001",       // idempotency key
        Symbol:     "EURUSD",
        Direction:  "BUY",
        OrderType:  "limit",
        Entry:      1.0850,
        StopLoss:   1.0810,         // 40-pip SL
        TakeProfit: 1.0910,         // 60-pip TP → RR 1.5 (aegis checks this only if you set MinRR)
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

## Mounting the transports

The same gateway can be exposed to remote agents over MCP (a `place_trade` tool) and/or a signed webhook. Both handlers are fail-closed — pass an empty token/secret and they serve `503`.

```go
import (
    "net/http"
    "os"

    aegismcp "github.com/unitz007/aegis/transport/mcp"
    aegiswebhook "github.com/unitz007/aegis/transport/webhook"
)

mux := http.NewServeMux()

// MCP: language models call the place_trade tool (bearer auth).
mux.Handle("/mcp", aegismcp.Handler(
    os.Getenv("MCP_AUTH_TOKEN"), gw, nil,
    aegismcp.WithSourceTag("claude"),
))

// Webhook: pipelines POST an HMAC-SHA256 signed signal.
mux.Handle("/signal/trade", aegiswebhook.Handler(
    os.Getenv("WEBHOOK_SIGNING_SECRET"), gw,
    aegiswebhook.WithSourceTag("ci"),
))

http.ListenAndServe(":8080", mux)
```

## Protocol reference

Remote producers reach the gateway through one of two transports. Both validate, dedup, and return the same `SignalResult`. **The caller always sends the position size** (`size`) and aegis never works one out: it is used exactly as given, or the trade is skipped. Forex and stocks: a whole number of units. Crypto: a quantity of the base asset.

### MCP — the `place_trade` tool

- **Endpoint:** `POST /mcp` (MCP Streamable HTTP). **Auth:** `Authorization: Bearer <token>`.
- **Tool:** `place_trade`

| Argument | Type | Required | Notes |
|---|---|---|---|
| `symbol` | string | ✓ | e.g. `EURUSD`, `GBPJPY`, `BTCUSD` |
| `direction` | string | ✓ | `BUY` \| `SELL` |
| `order_type` | string | ✓ | `limit` \| `market` |
| `entry` | number | ✓ | absolute price, in the symbol's quote units |
| `stop_loss` | number | ✓ | BUY: below entry · SELL: above entry |
| `take_profit` | number | ✓ | BUY: above entry · SELL: below entry |
| `min_rr` | number |  | The smallest reward-to-risk you will accept for this trade (e.g. `2`). aegis has no ratio of its own: omit it or send `0` and nothing is checked. A trade whose levels give less is rejected. |
| `strategy` | string | ✓ | one of the gateway's `AllowedStrategies` (advertised as an enum in the tool schema) |
| `note` | string | – | free-text rationale, recorded with the trade |
| `idempotency_key` | string | – | `[A-Za-z0-9._-]`, ≤ 64 chars → stable `signal_id`; omit for a server-generated one |

**Result** (JSON text content):

```json
{ "accepted": true, "placed": true, "code": "placed",
  "reason": "", "signal_id": "claude-ab12", "broker_order_id": "DIATF8U2" }
```

A gateway rejection (e.g. below the `min_rr` you set, symbol not executable, duplicate) is a **normal result with `accepted: false`** and a `code`/`reason` — not a tool error. Only an internal failure is an MCP tool error.

### Webhook — signed POST

- **Endpoint:** a path you mount (e.g. `POST /signal/trade`).
- **Signature header:** `X-Signature: sha256=<hex>` — HMAC-SHA256 of the **raw request body**, keyed by the shared secret, constant-time verified.
- **Body** (`application/json`, ≤ 64 KiB):

| Field | Type | Notes |
|---|---|---|
| `signal_id` | string | idempotency key |
| `timestamp` | int64 | unix seconds; must be within ±60 s of now (replay window, configurable) |
| `symbol`, `direction`, `order_type` | string | as in the MCP tool |
| `entry`, `stop_loss`, `take_profit` | number | as in the MCP tool |
| `size` | number | required: how much to trade, as in the MCP tool |
| `min_rr` | number | optional: the smallest reward-to-risk you will accept, as in the MCP tool |
| `strategy` | string | allowlisted |
| `note` | string | optional |

**Response status:** `200` accepted/placed · `400` validation reject or stale timestamp · `409` duplicate in-flight · `401` bad/missing signature. The body carries the same `SignalResult` fields.

Signing example:

```bash
BODY='{"signal_id":"sig-001","timestamp":1733580000,"symbol":"EURUSD","direction":"BUY","order_type":"limit","entry":1.0850,"stop_loss":1.0810,"take_profit":1.0910,"size":10000,"strategy":"smc"}'
SIG=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "$WEBHOOK_SIGNING_SECRET" | awk '{print $2}')
curl -X POST https://host/signal/trade \
  -H "X-Signature: sha256=$SIG" -H 'Content-Type: application/json' -d "$BODY"
```

### Per-request broker accounts (webhook)

One aegis server can trade for many people, each on their own broker account. A signed webhook request may carry an `account` object that names the broker and carries that account's login, **sealed** so nothing in between can read it. aegis opens it, trades, and answers with the usual `SignalResult`. Requests without `account` use the server's default broker.

```json
{ "signal_id": "sig-001", "timestamp": 1733580000, "symbol": "EURUSD", "direction": "BUY",
  "order_type": "limit", "entry": 1.0850, "stop_loss": 1.0810, "take_profit": 1.0910, "size": 10000,
  "account": { "broker": "capital", "env": "demo", "credentials": "<base64 sealed login>" } }
```

- **`GET /brokers`** (public) lists the brokers aegis will trade on, the environments each offers right now (`live` is left out unless the server allows it), and the details a login for each needs (name, label, whether it is secret). A connect screen should be built from this list rather than hard-coding any broker. Each broker also says **how a person connects** (`strategy`). Today that is always `fields` (type in the listed details); brokers that connect another way, such as signing in on the broker's own page, get their own strategy, and a client simply skips strategies it doesn't know. Registering a new broker adds it here automatically.
- **`broker` must be registered.** Only brokers listed in the server's registry (`brokerRegistry()` in `cmd/server/wiring.go`; today `capital`) can be named. Anything else is refused with the list of what is available.
- **`env`** must be one of that broker's environments (`demo` or `live`). `live` is refused unless the server runs with `ALLOW_ACCOUNT_LIVE=true`.
- **`credentials`** is the login (for Capital.com: `{"api_key", "email", "password"}`) as JSON, sealed to aegis's public key and base64 encoded. Every field the broker needs must be present and nothing extra. A login sent in the clear, or any stray plaintext field next to `credentials`, is refused.
- **Sealing.** Fetch the key from `GET /account/key` (public, safe to cache), then use the `sealed` package: `sealed.Seal(publicKey, []byte("aegis/account/v1|"+broker+"|"+env), loginJSON)` with the broker name lower case. The context ties the value to its broker and environment, so demo details can't be replayed as live and one broker's can't be used for another. Scheme: X25519 + HKDF-SHA256 + AES-256-GCM.
- **Keys.** `go run ./cmd/sealkey` makes a key pair. Set the private half on aegis (`fly secrets set ACCOUNT_SEAL_KEY=...`). Without it the feature is off and any request naming an account is refused.
- Logins are opened in memory only (one connection per account, so the broker session is reused). They are never stored, logged or returned in errors.
- Duplicate protection is namespaced per account, so two accounts can reuse a `signal_id`.
- `POST /account/check` (same signature scheme, body `{"timestamp": …, "account": {…}}`) logs in and returns `{"ok": true, "account": {"currency", "balance", "env"}}` without placing anything. Use it to verify a login before trading.

## The injected-hooks model

Four hooks can be injected into `GatewayConfig` (for the gateway layer) and `Executor` (for the execution layer). Every hook has a standalone default that is used when the field is nil. The **security-critical** defaults fail closed: `ExecutabilityCheck` rejects out-of-universe symbols (no accidental equity orders to a CFD broker)  The `SessionGate` and `IsAlgoPair` defaults are **permissive** (trade any session; treat forex as algo-driven) — inject your own to restrict.

| Hook | Type | Where injected | Safe default | Default behaviour |
|---|---|---|---|---|
| `SymbolClassifier` | `func(symbol string) string` | `GatewayConfig` | `ClassifySymbolDefault` | Pure function; classifies 6-letter fiat pairs as `"forex"`, known crypto bases as `"crypto"`, everything else as `"stock"`. No network call needed. |
| `ExecutabilityCheck` | `func(symbol, assetClass string) (bool, string)` | `GatewayConfig` | `DefaultExecutability` | **Fail-closed.** Accepts only `"forex"` and the 6 core-algo crypto symbols (`BTCUSD`, `ETHUSD`, `SOLUSD`, `BNBUSD`, `XRPUSD`, `ADAUSD`). Stocks and non-core crypto are **rejected before the signal_id slot is consumed**. Inject `AllExecutable` to allow every asset class. |
| `IsAlgoPair` | `IsAlgoPairFunc` = `func(symbol, assetClass string) bool` | `Executor` | `DefaultIsAlgoPair` | Forex is algo-driven (skips confidence gate); crypto and stocks default to AI-sourced (confidence gate applies). |
| `SessionGate` | `SessionGateFunc` = `func(symbol, assetClass string, now time.Time) bool` | `Executor` | `AlwaysAllow` | All symbols allowed at all times. Inject a custom gate to restrict trading to London/NY sessions. |

The defaults for `SymbolClassifier` and `ExecutabilityCheck` were chosen to match the hardened gates in production StockAI. `DefaultExecutability` rejects a stock symbol such as `"AAPL"` before calling `PlaceFunc`, so a misconfigured agent cannot accidentally route equity orders to a CFD broker.

## Safety guarantees

- **No auto sizing.** aegis never decides how big a trade is. The caller's `size` is used exactly as given; a size that is missing, not positive, not a whole number of units (forex/stocks) or outside the broker's crypto limits is skipped, never rounded or adjusted.

- **Dup-check fails closed.** If `Broker.OpenTrades` or `Broker.OpenOrders` returns an error, the executor skips rather than assuming "no positions" and risking duplicate stacking.

- **Idempotency / no-replay.** A `signal_id` is atomically reserved before any broker call. A second submission with the same `(source, signal_id)` returns the cached terminal result without calling the broker. A `PlaceFunc` error finalizes the slot as `ReasonExecutorSkip` so retries return the same outcome instead of re-placing.

- **Dealing-rules check for crypto.** The executor type-asserts `broker.DealingRulesProvider` and fetches per-instrument min/max deal sizes. A quantity below `MinDealSize` or above `MaxDealSize` is skipped, **never resized**.

- **Flip-close / flip-cancel is terminal.** When an opposite-direction open position or pending order exists, the executor closes/cancels it and returns without opening a new entry. Human-placed orders (`Source == "human"`) are never flip-cancelled.

- **Fail-closed auth on every transport.** The MCP handler returns 503 when `authToken` is empty; the webhook handler returns 503 when the signing secret is empty. Neither handler dispatches any tool logic in the disabled state.

- **Validation runs before slot reservation.** Allowlist, content, RR-floor, and executability checks all run before `getOrReserve` — a rejected signal never burns an idempotency slot.

## Running in production

aegis was extracted from [StockAI](https://github.com/unitz007/finOS), which consumes it live: external agents place trades over the MCP and webhook transports, and the internal strategy pipeline routes through the same gateway. The safety defaults and invariants documented above are the ones that gate real broker orders there.

## License

Apache-2.0. See [LICENSE](LICENSE).
