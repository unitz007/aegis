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
}

// handler is the concrete webhook http.Handler.
type handler struct {
	secret  string         // HMAC-SHA256 key; empty → 503
	gateway aegis.Gateway  // single business path; nil → 503
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
	}

	// ── Submit to the gateway (single business path) ──────────────────────────
	pCtx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

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
