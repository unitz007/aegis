// Package mcp provides the MCP Streamable-HTTP handler for the aegis Gateway.
//
// Handler builds an authenticated HTTP handler that exposes a place_trade MCP
// tool. bearer-auth constant-time check runs before any tool dispatch.
// Fail-closed: returns 503 when authToken is empty or gateway is nil.
//
// SECURITY MODEL — this is a public, money-moving endpoint, so it is fail-closed:
//
//   - Disabled unless authToken is non-empty. The caller must not mount the
//     handler (or must mount the 503 response from Handler) when the token is empty.
//   - When enabled, every request is wrapped in bearer-token auth middleware that
//     runs a constant-time comparison BEFORE any tool dispatch.
//   - Disabled (503) when gateway is nil (executor not wired).
//
// EXECUTION MODEL — the MCP server is purely a transport adapter. It owns NO
// business logic: the place_trade tool maps its arguments to an aegis.TradeSignal
// and hands it to the Gateway. ALL validation, sizing, dedup/idempotency, and the
// single money path to the executor live in the gateway. This package never
// touches a broker and never re-implements any of that.
package mcp

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/unitz007/aegis"
)

// config holds the options applied to a Handler.
type config struct {
	source          string
	defaultStrategy string
}

// Option configures the MCP handler.
type Option func(*config)

// WithSourceTag fixes the source field stamped on every MCP-submitted signal.
// Default: "external".
func WithSourceTag(tag string) Option {
	return func(c *config) {
		c.source = tag
	}
}

// WithDefaultStrategy sets the strategy used when the caller supplies none.
// Default: first entry of the gateway's AllowedStrategies list (or "smc_engine"
// when the list is empty).
func WithDefaultStrategy(s string) Option {
	return func(c *config) {
		c.defaultStrategy = s
	}
}

// Handler builds the authenticated MCP HTTP handler.
//
// Returns a fail-closed handler in every degraded configuration:
//   - authToken empty  → 503 on every request (endpoint disabled).
//   - gateway nil      → 503 on every request (executor not wired).
//
// When fully configured it returns the bearer-auth-wrapped Streamable-HTTP MCP
// transport. The bearer check (constant-time) runs before the transport sees the
// request, so no tool can dispatch without a valid token.
func Handler(authToken string, gateway aegis.Gateway, logger *log.Logger, opts ...Option) http.Handler {
	if logger == nil {
		logger = log.Default()
	}
	if strings.TrimSpace(authToken) == "" {
		logger.Println("mcp: handler disabled (no authToken)")
		return disabledHandler("mcp handler disabled: authToken not configured")
	}
	if gateway == nil {
		logger.Println("mcp: handler disabled (gateway not wired)")
		return disabledHandler("mcp handler disabled: gateway not wired")
	}

	cfg := &config{source: "external"}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.defaultStrategy == "" {
		strats := gateway.AllowedStrategies()
		if len(strats) > 0 {
			cfg.defaultStrategy = strats[0]
		} else {
			cfg.defaultStrategy = "smc_engine"
		}
	}

	mcpSrv := server.NewMCPServer(
		"aegis-mcp",
		"1.0.0",
		server.WithToolCapabilities(true),
	)
	registerPlaceTrade(mcpSrv, gateway, cfg, logger)

	// Stateless Streamable HTTP: each request is self-contained, which suits a
	// stateless external session and avoids per-session bookkeeping.
	httpTransport := server.NewStreamableHTTPServer(
		mcpSrv,
		server.WithStateLess(true),
	)

	logger.Printf("mcp: handler enabled (bearer auth required, source=%q)", cfg.source)
	return bearerAuth(authToken, httpTransport, logger)
}

// Enabled reports whether the handler would be active for the given config.
func Enabled(authToken string, gateway aegis.Gateway) bool {
	return strings.TrimSpace(authToken) != "" && gateway != nil
}

// registerPlaceTrade wires the place_trade tool onto the MCP server. The handler
// is a pure transport mapping: build an aegis.TradeSignal (source stamped from
// config, server-generated signal_id) and submit it to the gateway. The gateway's
// verdict — including validation rejections — is returned as a tool result (JSON
// text), never as a transport error, so the agent can self-correct.
func registerPlaceTrade(mcpSrv *server.MCPServer, gateway aegis.Gateway, cfg *config, logger *log.Logger) {
	// Build the strategy enum from the gateway's allowlist so the tool schema
	// always matches the gateway's validation. Fall back to a minimal hint if
	// the list is empty (the gateway will still reject unknown strategies).
	strats := gateway.AllowedStrategies()
	stratEnum := make([]string, len(strats))
	copy(stratEnum, strats)

	stratDescription := "Strategy that derived the levels."
	if len(stratEnum) > 0 {
		stratDescription = fmt.Sprintf("Strategy that derived the levels. Allowed: %s.", strings.Join(stratEnum, ", "))
	}

	toolOpts := []mcp.ToolOption{
		mcp.WithDescription(
			"Submit a forex/crypto/stock trade signal to the aegis gateway. The signal is " +
				"validated, sized, deduplicated and (if accepted) routed to the live broker " +
				"by the server-side Gateway. Position size is decided server-side and is NOT " +
				"an input. Returns the gateway verdict; a validation rejection is a normal " +
				"result (accepted=false with a reason), not an error.",
		),
		mcp.WithString("symbol", mcp.Required(),
			mcp.Description("Tradeable symbol, e.g. EURUSD, GBPJPY, BTCUSD.")),
		mcp.WithString("direction", mcp.Required(), mcp.Enum("BUY", "SELL"),
			mcp.Description("Trade direction.")),
		mcp.WithString("order_type", mcp.Required(), mcp.Enum("limit", "market"),
			mcp.Description("Order type.")),
		mcp.WithNumber("entry", mcp.Required(),
			mcp.Description("Entry price (absolute, in the symbol's quote units).")),
		mcp.WithNumber("stop_loss", mcp.Required(),
			mcp.Description("Stop-loss price. BUY: below entry. SELL: above entry.")),
		mcp.WithNumber("take_profit", mcp.Required(),
			mcp.Description("Take-profit price. BUY: above entry. SELL: below entry. RR must be >= 1.5.")),
		mcp.WithString("note",
			mcp.Description("Optional free-text rationale recorded with the trade.")),
		mcp.WithString("idempotency_key",
			mcp.Description("Optional caller-supplied idempotency key (A-Z a-z 0-9 . _ -, max 64 chars). "+
				"When provided, it is sanitized and used as the signal_id so repeated calls "+
				"with the same key return the cached gateway verdict without re-placing.")),
	}

	// Add strategy field: with enum if we have strategies, without if not.
	if len(stratEnum) > 0 {
		toolOpts = append(toolOpts, mcp.WithString("strategy", mcp.Required(),
			mcp.Enum(stratEnum...),
			mcp.Description(stratDescription)))
	} else {
		toolOpts = append(toolOpts, mcp.WithString("strategy", mcp.Required(),
			mcp.Description(stratDescription)))
	}

	tool := mcp.NewTool("place_trade", toolOpts...)
	mcpSrv.AddTool(tool, placeTradeHandler(gateway, cfg, logger))
}

// placeTradeHandler returns the ToolHandlerFunc for place_trade. Extracted so it
// can be unit-tested directly against a stub gateway, with no MCP transport and
// no real broker.
func placeTradeHandler(gateway aegis.Gateway, cfg *config, logger *log.Logger) server.ToolHandlerFunc {
	if logger == nil {
		logger = log.Default()
	}
	// Build the allowed-strategy set for pre-validation before gateway call.
	allowedStrats := make(map[string]struct{})
	for _, s := range gateway.AllowedStrategies() {
		allowedStrats[strings.ToLower(s)] = struct{}{}
	}

	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		// ── Required arg extraction ───────────────────────────────────────────
		symbol, err := req.RequireString("symbol")
		if err != nil {
			return mcp.NewToolResultError("symbol is required"), nil
		}
		direction, err := req.RequireString("direction")
		if err != nil {
			return mcp.NewToolResultError("direction is required (BUY|SELL)"), nil
		}
		orderType, err := req.RequireString("order_type")
		if err != nil {
			return mcp.NewToolResultError("order_type is required (limit|market)"), nil
		}
		entry, err := req.RequireFloat("entry")
		if err != nil {
			return mcp.NewToolResultError("entry is required (number)"), nil
		}
		stopLoss, err := req.RequireFloat("stop_loss")
		if err != nil {
			return mcp.NewToolResultError("stop_loss is required (number)"), nil
		}
		takeProfit, err := req.RequireFloat("take_profit")
		if err != nil {
			return mcp.NewToolResultError("take_profit is required (number)"), nil
		}
		strategyArg, err := req.RequireString("strategy")
		if err != nil {
			return mcp.NewToolResultError("strategy is required"), nil
		}

		// ── Strategy pre-validation (before gateway, so gateway is never called
		//    for a clearly invalid strategy) ────────────────────────────────────
		normalizedStrategy := strings.ToLower(strings.TrimSpace(strategyArg))
		if len(allowedStrats) > 0 {
			if _, ok := allowedStrats[normalizedStrategy]; !ok {
				strats := gateway.AllowedStrategies()
				return mcp.NewToolResultError(fmt.Sprintf(
					"strategy %q is not in the allowed list: %s",
					strategyArg, strings.Join(strats, ", "))), nil
			}
		}
		// Use original (non-lowercased) value — the gateway allowlist may be case-sensitive.
		strategy := strings.TrimSpace(strategyArg)

		note := req.GetString("note", "")

		// ── Idempotency key / signal_id ───────────────────────────────────────
		// If the caller supplies an idempotency_key, sanitize it and use it as
		// the signal_id. Otherwise generate a fresh server-side random id.
		idempKey := req.GetString("idempotency_key", "")
		var signalID string
		if idempKey != "" {
			sanitized := sanitizeIdempotencyKey(idempKey)
			if sanitized == "" {
				return mcp.NewToolResultError(
					"idempotency_key contains no valid characters (allowed: A-Za-z0-9._-)"), nil
			}
			signalID = cfg.source + "-" + sanitized
		} else {
			var genErr error
			signalID, genErr = newSignalID(cfg.source)
			if genErr != nil {
				// Cannot guarantee idempotency without an id — fail the call rather
				// than risk an unattributable / non-deduplicable submission.
				logger.Printf("mcp: failed to generate signal_id: %v", genErr)
				return mcp.NewToolResultError("internal error: could not generate signal id"), nil
			}
		}

		sig := aegis.TradeSignal{
			Source:     cfg.source,
			Strategy:   strategy,
			SignalID:   signalID,
			Symbol:     symbol,
			Direction:  direction,
			OrderType:  orderType,
			Entry:      entry,
			StopLoss:   stopLoss,
			TakeProfit: takeProfit,
			Note:       note,
			Timestamp:  time.Now().Unix(),
		}

		// Single business path. The gateway owns validation/sizing/dedup; this
		// adapter only maps the verdict back to the agent.
		subCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		res, gwErr := gateway.Submit(subCtx, sig)
		if gwErr != nil {
			// Submit returns a non-nil error only for unexpected internal faults;
			// validation/idempotency outcomes are carried in res.
			logger.Printf("mcp: GATEWAY_ERROR signal_id=%s symbol=%s error=%v", signalID, symbol, gwErr)
			return mcp.NewToolResultError(fmt.Sprintf("gateway error: %v", gwErr)), nil
		}

		logger.Printf("mcp: place_trade signal_id=%s symbol=%s dir=%s strategy=%s -> accepted=%t placed=%t code=%s",
			signalID, symbol, direction, strategy, res.Accepted, res.Placed, res.Code)

		return toolResult(signalID, res), nil
	}
}

// toolResult marshals a gateway verdict into a JSON text tool result. A
// validation/idempotency rejection is a NORMAL result (accepted=false + reason),
// not a transport error, so the agent can read the reason and self-correct.
func toolResult(signalID string, res aegis.SignalResult) *mcp.CallToolResult {
	payload := map[string]any{
		"signal_id":       signalID,
		"accepted":        res.Accepted,
		"placed":          res.Placed,
		"broker_order_id": res.BrokerOrderID,
		"reason":          res.Reason,
		"code":            string(res.Code),
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to encode result: %v", err))
	}
	return mcp.NewToolResultText(string(b))
}

// newSignalID returns a random 128-bit hex id (UUID-grade entropy) used as the
// gateway idempotency key for MCP-submitted signals. The prefix is derived from
// the configured source tag, making the origin of the id visible in logs.
func newSignalID(source string) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	prefix := source
	if prefix == "" {
		prefix = "mcp"
	}
	return prefix + "-" + hex.EncodeToString(b[:]), nil
}

// sanitizeIdempotencyKey strips disallowed characters from a caller-supplied
// idempotency key, keeping only [A-Za-z0-9._-] up to 64 characters.
func sanitizeIdempotencyKey(key string) string {
	var b strings.Builder
	for _, r := range key {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') ||
			(r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-' {
			b.WriteRune(r)
			if b.Len() >= 64 {
				break
			}
		}
	}
	return b.String()
}

// bearerAuth wraps next with constant-time bearer-token authentication. The
// token check runs before next sees the request, so no MCP method (including
// tool dispatch) executes without a valid Authorization: Bearer <token> header.
func bearerAuth(token string, next http.Handler, logger *log.Logger) http.Handler {
	expected := []byte(token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authz := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(authz, prefix) {
			unauthorized(w)
			return
		}
		presented := []byte(strings.TrimSpace(authz[len(prefix):]))
		// Constant-time comparison; ConstantTimeCompare returns 0 on length
		// mismatch, so this does not leak length via timing.
		if subtle.ConstantTimeCompare(presented, expected) != 1 {
			logger.Printf("mcp: REJECTED bad bearer token from %s", r.RemoteAddr)
			unauthorized(w)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// disabledHandler returns a handler that always responds 503 with reason. Used
// when the MCP handler is not fully configured (no token / no gateway) so the
// route is safe to mount unconditionally and never dispatches a tool.
func disabledHandler(reason string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": reason})
	})
}

func unauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
