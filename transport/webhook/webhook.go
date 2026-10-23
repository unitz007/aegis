// Package webhook provides the HMAC-SHA256 webhook HTTP handler for the aegis Gateway.
//
// Handler returns a POST handler. Fail-closed: returns 503 when the signing
// secret is empty or the gateway is nil. All trade logic stays in the gateway;
// this package owns only transport security and payload mapping.
//
// Security model:
//   - HMAC-SHA256 signature over the raw body, compared constant-time.
//   - 60-second timestamp replay window (configurable via WithTimestampTolerance).
//   - Raw body read exactly once; capped at 64 KiB (configurable via WithBodyLimit).
//   - Secret absent → 503. Gateway not wired → 503.
//
// HTTP status mapping:
//   - ReasonInFlight → 409
//   - Any other rejection → 400
//   - Accepted (placed or executor-skip) → 200
//   - Gateway internal error → 500
package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/unitz007/aegis"
)

const (
	defaultTimestampTolerance = 60 * time.Second
	defaultBodyLimit          = 64 * 1024 // 64 KiB
)

// config holds the options applied to a Handler.
type config struct {
	timestampTolerance time.Duration
	source             string
	defaultStrategy    string
	bodyLimit          int64
	logger             *log.Logger
	accountResolver    AccountResolver
}

// Option configures the webhook handler.
type Option func(*config)

// WithTimestampTolerance sets the maximum allowed age (or future-drift) of the
// timestamp field in the request body. Default: 60 seconds.
func WithTimestampTolerance(d time.Duration) Option {
	return func(c *config) { c.timestampTolerance = d }
}

// WithSourceTag sets the source field stamped on every webhook-submitted signal.
// Default: "external". The caller must ensure this value is in the gateway's
// AllowedSources list.
func WithSourceTag(tag string) Option {
	return func(c *config) { c.source = tag }
}

// WithDefaultStrategy sets the strategy stamped on every webhook-submitted signal
// when the request body omits the strategy field. Default: "smc_engine".
// Strategy is free-text provenance — it is never validated by the gateway.
func WithDefaultStrategy(s string) Option {
	return func(c *config) { c.defaultStrategy = s }
}

// WithBodyLimit sets the maximum request body size in bytes. Default: 65536 (64 KiB).
func WithBodyLimit(n int64) Option {
	return func(c *config) { c.bodyLimit = n }
}

// WithLogger sets the logger used for audit messages. Default: log.Default().
func WithLogger(l *log.Logger) Option {
	return func(c *config) { c.logger = l }
}

// AccountResolver turns the optional "account" object of a signed request into
// the context the trade runs under (typically carrying that account's broker)
// and a stable, non-secret account key. The key namespaces idempotency so two
// accounts can never collide on a signal_id. Return an error to reject the
// request; its text is returned to the caller, so it must never contain secrets.
type AccountResolver func(ctx context.Context, raw json.RawMessage) (context.Context, string, error)

// WithAccountResolver enables per-request accounts. Without it, a request that
// carries an "account" is rejected rather than silently using the default broker.
func WithAccountResolver(r AccountResolver) Option {
	return func(c *config) { c.accountResolver = r }
}

// signalRequest is the JSON body expected from external callers.
type signalRequest struct {
	SignalID   string  `json:"signal_id"`
	Timestamp  int64   `json:"timestamp"`
	Symbol     string  `json:"symbol"`
	Direction  string  `json:"direction"`
	OrderType  string  `json:"order_type"`
	Entry      float64 `json:"entry"`
	StopLoss   float64 `json:"stop_loss"`
	TakeProfit float64 `json:"take_profit"`
	// NOTE: no `source` field — the transport must not trust a self-asserted
	// source. The handler always stamps the configured source below.
	Strategy string `json:"strategy"`
	Note     string `json:"note"`
	// Manual=true marks a human-placed discretionary trade. When set, the gateway
	// bypasses the session veto and confidence gate in the executor. Structural validity and the source allowlist still apply.
	Manual bool `json:"manual,omitempty"`
	// MinRR is the smallest reward-to-risk the sender will accept for this trade.
	// aegis has no ratio of its own; absent or 0 means no limit.
	MinRR float64 `json:"min_rr,omitempty"`
	// Size is how much to trade, chosen by the caller (aegis never sizes a trade):
	// whole units for forex and stocks, a base-asset quantity for crypto. Required.
	Size float64 `json:"size"`
	// Account optionally names the broker account to trade on (provider-specific
	// JSON, interpreted by the AccountResolver). Absent → the server's default
	// broker, so existing producers are unaffected.
	Account json.RawMessage `json:"account,omitempty"`
}

// handler is the concrete webhook http.Handler.
type handler struct {
	secret  string        // HMAC-SHA256 key; empty → 503
	gateway aegis.Gateway // single business path; nil → 503
	cfg     config
}

// Handler returns the HMAC-SHA256 webhook http.Handler for POST /signal/trade.
//
// Fail-closed: 503 when secret is empty or gateway is nil.
// Timestamp replay window defaults to 60 seconds (override with WithTimestampTolerance).
func Handler(secret string, gateway aegis.Gateway, opts ...Option) http.Handler {
	cfg := config{
		timestampTolerance: defaultTimestampTolerance,
		source:             "external",
		defaultStrategy:    "smc_engine",
		bodyLimit:          defaultBodyLimit,
		logger:             log.Default(),
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.logger == nil {
		cfg.logger = log.Default()
	}
	return &handler{
		secret:  secret,
		gateway: gateway,
		cfg:     cfg,
	}
}

// ServeHTTP handles POST requests.
//
// Transport guards run first (method, secret-configured, gateway-wired,
// body-read, HMAC, replay window). Then the payload maps to a
// aegis.TradeSignal and the gateway's verdict maps to HTTP status.
func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// ── Transport guard: method ───────────────────────────────────────────────
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{
			"accepted": false,
			"reason":   "method not allowed; POST required",
		})
		return
	}

	// ── Transport guard: secret must be configured (fail-closed) ─────────────
	if h.secret == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"accepted": false,
			"reason":   "webhook disabled: signing secret not configured",
		})
		return
	}

	// ── Transport guard: gateway must be wired (fail-closed) ─────────────────
	if h.gateway == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"accepted": false,
			"reason":   "webhook disabled: gateway not wired",
		})
		return
	}

	// Read the raw body exactly once — needed for both HMAC verification and JSON decode.
	rawBody, err := io.ReadAll(io.LimitReader(r.Body, h.cfg.bodyLimit))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"accepted": false,
			"reason":   "failed to read request body",
		})
		return
	}
	// Reject bodies that hit the limit exactly — they may have been truncated.
	if int64(len(rawBody)) == h.cfg.bodyLimit {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{
			"accepted": false,
			"reason":   fmt.Sprintf("request body exceeds maximum size (%d bytes)", h.cfg.bodyLimit),
		})
		return
	}

	// ── Transport guard: HMAC-SHA256 signature ────────────────────────────────
	sigHeader := r.Header.Get("X-Signature")
	sigHeader = strings.TrimPrefix(sigHeader, "sha256=")
	if !verifyHMAC([]byte(h.secret), rawBody, sigHeader) {
		h.cfg.logger.Printf("webhook: REJECTED bad signature from %s", r.RemoteAddr)
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"accepted": false,
			"reason":   "invalid signature",
		})
		return
	}

	// ── Decode JSON ───────────────────────────────────────────────────────────
	var req signalRequest
	if decErr := json.Unmarshal(rawBody, &req); decErr != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"accepted": false,
			"reason":   fmt.Sprintf("invalid JSON: %v", decErr),
		})
		return
	}

	// ── Transport guard: timestamp replay window ──────────────────────────────
	ts := time.Unix(req.Timestamp, 0)
	age := time.Since(ts)
	if age < 0 {
		age = -age // abs; future timestamps are also rejected
	}
	if age > h.cfg.timestampTolerance {
		h.cfg.logger.Printf("webhook: REJECTED stale timestamp (age=%s signal_id=%s)",
			age.Round(time.Second), strings.TrimSpace(req.SignalID))
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"accepted": false,
			"reason": fmt.Sprintf("timestamp out of tolerance window (age=%s, max=%s)",
				age.Round(time.Second), h.cfg.timestampTolerance),
		})
		return
	}

	// ── Map payload → aegis.TradeSignal ──────────────────────────────────────
	// The gateway owns ALL validation (including signal_id presence, symbol,
	// levels, RR, allowlists) and idempotency. The handler does not pre-validate.
	strategy := strings.TrimSpace(req.Strategy)
	if strategy == "" {
		strategy = h.cfg.defaultStrategy
	}

	sig := aegis.TradeSignal{
		Source:     h.cfg.source,
		Strategy:   strategy,
		SignalID:   strings.TrimSpace(req.SignalID),
		Symbol:     req.Symbol,
		Direction:  req.Direction,
		OrderType:  req.OrderType,
		Entry:      req.Entry,
		StopLoss:   req.StopLoss,
		TakeProfit: req.TakeProfit,
		Note:       req.Note,
		Timestamp:  req.Timestamp,
		Manual:     req.Manual,
		Size:       req.Size,
		MinRR:      req.MinRR,
	}

	// ── Submit to the gateway (single business path) ──────────────────────────
	pCtx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Per-request account: resolved before the gateway so the broker travels in
	// the context, and the id is namespaced so idempotency is per account.
	if hasAccount(req.Account) {
		if h.cfg.accountResolver == nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"accepted": false,
				"reason":   "per-account trading is not enabled on this server",
			})
			return
		}
		acctCtx, key, aerr := h.cfg.accountResolver(pCtx, req.Account)
		if aerr != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"accepted": false,
				"reason":   fmt.Sprintf("invalid account: %v", aerr),
			})
			return
		}
		pCtx = acctCtx
		sig.SignalID = key + ":" + sig.SignalID
	}

	res, gwErr := h.gateway.Submit(pCtx, sig)
	if gwErr != nil {
		// Submit only returns a non-nil error for unexpected internal faults;
		// validation/idempotency outcomes are carried in res, not err.
		h.cfg.logger.Printf("webhook: GATEWAY_ERROR signal_id=%s symbol=%s error=%v",
			sig.SignalID, sig.Symbol, gwErr)
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"accepted": false,
			"reason":   fmt.Sprintf("gateway error: %v", gwErr),
		})
		return
	}

	statusCode := statusFor(res)
	h.cfg.logger.Printf("webhook: VERDICT signal_id=%s source=%s strategy=%s symbol=%s -> http=%d accepted=%t placed=%t reason=%q",
		sig.SignalID, sig.Source, sig.Strategy, sig.Symbol, statusCode, res.Accepted, res.Placed, res.Reason)

	writeJSON(w, statusCode, map[string]any{
		"accepted":        res.Accepted,
		"placed":          res.Placed,
		"broker_order_id": res.BrokerOrderID,
		"reason":          res.Reason,
	})
}

// statusFor maps a gateway SignalResult to an HTTP status code. This is the only
// business→transport mapping the handler performs; it makes no decisions of its own.
//
//   - In-flight duplicate (ReasonInFlight)         → 409 conflict.
//   - Any other rejection (ReasonValidation, etc.) → 400.
//   - Accepted (placed or executor-skip)           → 200.
//
// The decision is driven by the typed res.Code, NOT by substring-matching the
// human Reason string (which may embed caller-supplied, craftable values).
func statusFor(res aegis.SignalResult) int {
	if res.Code == aegis.ReasonInFlight {
		return http.StatusConflict
	}
	if !res.Accepted {
		return http.StatusBadRequest
	}
	return http.StatusOK
}

// verifyHMAC computes HMAC-SHA256(secret, body) and compares it to the provided
// hex-encoded signature using a constant-time comparison.
func verifyHMAC(secret, body []byte, sigHex string) bool {
	if sigHex == "" {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	expected := mac.Sum(nil)

	got, err := hex.DecodeString(sigHex)
	if err != nil {
		return false
	}
	// hmac.Equal uses constant-time compare internally.
	return hmac.Equal(expected, got)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func hasAccount(raw json.RawMessage) bool {
	t := strings.TrimSpace(string(raw))
	return t != "" && t != "null"
}

// AccountCheckFunc inspects the account resolved from a request (for example,
// confirming the credentials work) and returns a JSON-serialisable summary.
type AccountCheckFunc func(ctx context.Context) (any, error)

type accountRequestKey struct{}

// AccountRequestBody is the signed request an AccountCheckFunc is answering, for reads that take more than the
// account (such as which symbols to read). It is already verified when the func runs.
// WithAccountRequestBody is how the handler passes the verified request on (and how tests stand in for it).
func WithAccountRequestBody(ctx context.Context, body []byte) context.Context {
	return context.WithValue(ctx, accountRequestKey{}, body)
}

func AccountRequestBody(ctx context.Context) []byte {
	b, _ := ctx.Value(accountRequestKey{}).([]byte)
	return b
}

// AccountCheckHandler returns the signed POST handler used to verify an account
// before trading on it. Body: {"timestamp": <unix>, "account": {...}}, signed
// exactly like the trade webhook. It never places an order.
func AccountCheckHandler(secret string, resolve AccountResolver, check AccountCheckFunc, opts ...Option) http.Handler {
	cfg := config{
		timestampTolerance: defaultTimestampTolerance,
		bodyLimit:          defaultBodyLimit,
		logger:             log.Default(),
	}
	for _, o := range opts {
		o(&cfg)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"ok": false, "reason": "method not allowed; POST required"})
			return
		}
		if secret == "" || resolve == nil || check == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ok": false, "reason": "account check disabled"})
			return
		}
		raw, err := io.ReadAll(io.LimitReader(r.Body, cfg.bodyLimit))
		if err != nil || int64(len(raw)) == cfg.bodyLimit {
			writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "reason": "invalid request body"})
			return
		}
		if !verifyHMAC([]byte(secret), raw, strings.TrimPrefix(r.Header.Get("X-Signature"), "sha256=")) {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"ok": false, "reason": "invalid signature"})
			return
		}
		var req struct {
			Timestamp int64           `json:"timestamp"`
			Account   json.RawMessage `json:"account"`
		}
		if json.Unmarshal(raw, &req) != nil || !hasAccount(req.Account) {
			writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "reason": "account is required"})
			return
		}
		age := time.Since(time.Unix(req.Timestamp, 0))
		if age < 0 {
			age = -age
		}
		if age > cfg.timestampTolerance {
			writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "reason": "timestamp out of tolerance window"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		ctx, _, err = resolve(ctx, req.Account)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "reason": fmt.Sprintf("invalid account: %v", err)})
			return
		}
		ctx = WithAccountRequestBody(ctx, raw)
		info, err := check(ctx)
		if err != nil {
			cfg.logger.Printf("webhook: account check failed: %v", err)
			writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "reason": "the broker rejected these credentials or could not be reached"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "account": info})
	})
}
