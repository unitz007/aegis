package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	mcpgo "github.com/mark3labs/mcp-go/mcp"

	"github.com/unitz007/aegis"
)

// ── Stub gateway ─────────────────────────────────────────────────────────────

// stubGateway is an in-memory Gateway stub. It NEVER reaches a real broker or
// network — it records each submitted signal and returns a canned verdict.
type stubGateway struct {
	mu      sync.Mutex
	calls   []aegis.TradeSignal
	res     aegis.SignalResult
	err     error
	callCnt int
}

func (s *stubGateway) Submit(_ context.Context, sig aegis.TradeSignal) (aegis.SignalResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.callCnt++
	s.calls = append(s.calls, sig)
	return s.res, s.err
}

func (s *stubGateway) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.callCnt
}

func (s *stubGateway) last() aegis.TradeSignal {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[len(s.calls)-1]
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// callReq builds a CallToolRequest with the given argument map.
func callReq(args map[string]any) mcpgo.CallToolRequest {
	var req mcpgo.CallToolRequest
	req.Params.Name = "place_trade"
	req.Params.Arguments = args
	return req
}

func validArgs() map[string]any {
	return map[string]any{
		"symbol":      "EURUSD",
		"direction":   "BUY",
		"order_type":  "limit",
		"entry":       1.1000,
		"stop_loss":   1.0950,
		"take_profit": 1.1100,
		"strategy":    "smc_engine",
		"note":        "test setup",
	}
}

// decodeResult extracts the JSON payload from a text tool result.
func decodeResult(t *testing.T, res *mcpgo.CallToolResult) map[string]any {
	t.Helper()
	if len(res.Content) == 0 {
		t.Fatalf("tool result has no content")
	}
	tc, ok := res.Content[0].(mcpgo.TextContent)
	if !ok {
		t.Fatalf("expected TextContent, got %T", res.Content[0])
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(tc.Text), &out); err != nil {
		t.Fatalf("result text is not JSON: %v (text=%q)", err, tc.Text)
	}
	return out
}

func newStubGateway(_ []string, res aegis.SignalResult, err error) *stubGateway {
	return &stubGateway{res: res, err: err}
}

func defaultConfig(source string) *config {
	return &config{source: source, defaultStrategy: "smc_engine"}
}

// ── Tool handler tests ────────────────────────────────────────────────────────

func TestPlaceTrade_ValidCallSubmitsOnceAndMapsResult(t *testing.T) {
	gw := newStubGateway([]string{"smc_engine", "zone_internal"}, aegis.SignalResult{
		Accepted: true, Placed: true, BrokerOrderID: "BRK-123", Code: aegis.ReasonPlaced,
	}, nil)
	h := placeTradeHandler(gw, defaultConfig("claude"), nil)

	res, err := h(context.Background(), callReq(validArgs()))
	if err != nil {
		t.Fatalf("handler returned transport error: %v", err)
	}
	if gw.count() != 1 {
		t.Fatalf("expected exactly 1 gateway.Submit call, got %d", gw.count())
	}

	// Verify the handler stamped provenance and a server-generated signal_id.
	sig := gw.last()
	if sig.Source != "claude" {
		t.Errorf("source = %q, want %q", sig.Source, "claude")
	}
	if sig.Strategy != "smc_engine" {
		t.Errorf("strategy = %q, want %q", sig.Strategy, "smc_engine")
	}
	// signal_id should start with the source prefix.
	if !strings.HasPrefix(sig.SignalID, "claude-") || len(sig.SignalID) < 20 {
		t.Errorf("signal_id %q not server-generated with expected prefix", sig.SignalID)
	}
	if sig.Symbol != "EURUSD" || sig.Direction != "BUY" || sig.OrderType != "limit" {
		t.Errorf("unexpected mapped fields: %+v", sig)
	}
	if sig.Entry != 1.1000 || sig.StopLoss != 1.0950 || sig.TakeProfit != 1.1100 {
		t.Errorf("unexpected mapped levels: %+v", sig)
	}
	if sig.Timestamp == 0 {
		t.Errorf("timestamp not set")
	}

	out := decodeResult(t, res)
	if out["accepted"] != true || out["placed"] != true {
		t.Errorf("result accepted/placed wrong: %+v", out)
	}
	if out["broker_order_id"] != "BRK-123" {
		t.Errorf("broker_order_id = %v", out["broker_order_id"])
	}
	if out["code"] != string(aegis.ReasonPlaced) {
		t.Errorf("code = %v", out["code"])
	}
	if out["signal_id"] != sig.SignalID {
		t.Errorf("result signal_id %v != submitted %v", out["signal_id"], sig.SignalID)
	}
}

func TestPlaceTrade_GatewayRejectionSurfacesAsResult(t *testing.T) {
	gw := newStubGateway([]string{"smc_engine"}, aegis.SignalResult{
		Accepted: false, Placed: false,
		Reason: "risk_reward 0.80 below floor 1.50", Code: aegis.ReasonValidation,
	}, nil)
	h := placeTradeHandler(gw, defaultConfig("claude"), nil)

	res, err := h(context.Background(), callReq(validArgs()))
	if err != nil {
		t.Fatalf("rejection must not be a transport error, got: %v", err)
	}
	if res.IsError {
		t.Fatalf("gateway rejection must be a normal result, not IsError")
	}
	if gw.count() != 1 {
		t.Fatalf("expected 1 Submit call, got %d", gw.count())
	}
	out := decodeResult(t, res)
	if out["accepted"] != false {
		t.Errorf("accepted should be false: %+v", out)
	}
	if out["reason"] != "risk_reward 0.80 below floor 1.50" {
		t.Errorf("reason not surfaced: %v", out["reason"])
	}
	if out["code"] != string(aegis.ReasonValidation) {
		t.Errorf("code = %v", out["code"])
	}
}

func TestPlaceTrade_GatewayInternalErrorIsToolError(t *testing.T) {
	gw := newStubGateway([]string{"smc_engine"}, aegis.SignalResult{}, errors.New("boom"))
	h := placeTradeHandler(gw, defaultConfig("claude"), nil)

	res, err := h(context.Background(), callReq(validArgs()))
	if err != nil {
		t.Fatalf("handler should not return transport error: %v", err)
	}
	if !res.IsError {
		t.Fatalf("gateway internal error should map to an error tool result")
	}
}

func TestPlaceTrade_MissingRequiredArgDoesNotSubmit(t *testing.T) {
	gw := newStubGateway([]string{"smc_engine"}, aegis.SignalResult{}, nil)
	h := placeTradeHandler(gw, defaultConfig("claude"), nil)

	args := validArgs()
	delete(args, "symbol")
	res, err := h(context.Background(), callReq(args))
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}
	if !res.IsError {
		t.Fatalf("missing symbol should be an error result")
	}
	if gw.count() != 0 {
		t.Fatalf("gateway must not be called on missing arg, got %d", gw.count())
	}
}

// TestPlaceTrade_ArbitraryStrategyAccepted: strategy is free-text provenance.
// Any non-empty string must pass through to the gateway without pre-rejection.
func TestPlaceTrade_ArbitraryStrategyAccepted(t *testing.T) {
	gw := newStubGateway(nil, aegis.SignalResult{
		Accepted: true, Placed: true, BrokerOrderID: "BRK-XYZ", Code: aegis.ReasonPlaced,
	}, nil)
	h := placeTradeHandler(gw, defaultConfig("claude"), nil)

	for _, strat := range []string{"bogus_strategy", "topdown", "some_experiment_v3", "my-new-algo"} {
		gw.mu.Lock()
		gw.callCnt = 0
		gw.calls = nil
		gw.mu.Unlock()

		args := validArgs()
		args["strategy"] = strat
		res, err := h(context.Background(), callReq(args))
		if err != nil {
			t.Fatalf("unexpected transport error for strategy %q: %v", strat, err)
		}
		if res.IsError {
			t.Fatalf("arbitrary strategy %q must not be an error result", strat)
		}
		if gw.count() != 1 {
			t.Fatalf("gateway must be called once for strategy %q, got %d", strat, gw.count())
		}
		sig := gw.last()
		if sig.Strategy != strat {
			t.Errorf("strategy %q not passed through to gateway, got %q", strat, sig.Strategy)
		}
	}
}

func TestPlaceTrade_IdempotencyKeyUsedAsSignalID(t *testing.T) {
	gw := newStubGateway([]string{"smc_engine"}, aegis.SignalResult{
		Accepted: true, Placed: true, Code: aegis.ReasonPlaced,
	}, nil)
	h := placeTradeHandler(gw, defaultConfig("mcp"), nil)

	args := validArgs()
	args["idempotency_key"] = "my-key-123"
	_, err := h(context.Background(), callReq(args))
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if gw.count() != 1 {
		t.Fatalf("expected 1 Submit call, got %d", gw.count())
	}
	sig := gw.last()
	// The signal_id should incorporate the sanitized idempotency key.
	if !strings.Contains(sig.SignalID, "my-key-123") {
		t.Errorf("signal_id %q should contain sanitized idempotency_key", sig.SignalID)
	}
}

func TestPlaceTrade_IdempotencyKeyInvalidCharsStripped(t *testing.T) {
	gw := newStubGateway([]string{"smc_engine"}, aegis.SignalResult{
		Accepted: true, Placed: true, Code: aegis.ReasonPlaced,
	}, nil)
	h := placeTradeHandler(gw, defaultConfig("mcp"), nil)

	args := validArgs()
	args["idempotency_key"] = "valid!@#$only-letters"
	_, err := h(context.Background(), callReq(args))
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if gw.count() != 1 {
		t.Fatalf("expected 1 Submit call, got %d", gw.count())
	}
	sig := gw.last()
	// Special chars stripped; only valid chars remain.
	if strings.ContainsAny(sig.SignalID, "!@#$") {
		t.Errorf("signal_id %q still contains disallowed characters", sig.SignalID)
	}
}

func TestPlaceTrade_AllInvalidIdempotencyKeyIsError(t *testing.T) {
	gw := newStubGateway([]string{"smc_engine"}, aegis.SignalResult{}, nil)
	h := placeTradeHandler(gw, defaultConfig("mcp"), nil)

	args := validArgs()
	args["idempotency_key"] = "!@#$%^&*()" // all disallowed
	res, err := h(context.Background(), callReq(args))
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if !res.IsError {
		t.Fatalf("all-invalid idempotency_key should be an error result")
	}
	if gw.count() != 0 {
		t.Fatalf("gateway must not be called for invalid idempotency_key")
	}
}

func TestPlaceTrade_ConfigurableSource(t *testing.T) {
	gw := newStubGateway([]string{"smc_engine"}, aegis.SignalResult{
		Accepted: true, Placed: false, Code: aegis.ReasonExecutorSkip,
	}, nil)
	cfg := defaultConfig("my-system")
	h := placeTradeHandler(gw, cfg, nil)

	_, err := h(context.Background(), callReq(validArgs()))
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	sig := gw.last()
	if sig.Source != "my-system" {
		t.Errorf("source = %q, want %q", sig.Source, "my-system")
	}
}

// ── Auth middleware + fail-closed mounting ────────────────────────────────────

func TestHandler_DisabledWithoutToken(t *testing.T) {
	gw := newStubGateway([]string{"smc_engine"}, aegis.SignalResult{}, nil)
	h := Handler("", gw, nil)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/mcp", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("no token: want 503, got %d", rec.Code)
	}
	if Enabled("", gw) {
		t.Errorf("Enabled should be false without a token")
	}
}

func TestHandler_DisabledWithoutGateway(t *testing.T) {
	h := Handler("secret-token", nil, nil)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/mcp", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil gateway: want 503, got %d", rec.Code)
	}
	if Enabled("secret-token", nil) {
		t.Errorf("Enabled should be false with a nil gateway")
	}
}

func TestHandler_MissingBearerIs401(t *testing.T) {
	gw := newStubGateway([]string{"smc_engine"}, aegis.SignalResult{}, nil)
	h := Handler("secret-token", gw, nil)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader("{}")))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing bearer: want 401, got %d", rec.Code)
	}
	if gw.count() != 0 {
		t.Fatalf("no tool may dispatch without auth, got %d Submit calls", gw.count())
	}
}

func TestHandler_BadBearerIs401(t *testing.T) {
	gw := newStubGateway([]string{"smc_engine"}, aegis.SignalResult{}, nil)
	h := Handler("secret-token", gw, nil)

	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader("{}"))
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("bad bearer: want 401, got %d", rec.Code)
	}
	if gw.count() != 0 {
		t.Fatalf("no tool may dispatch with a bad token, got %d Submit calls", gw.count())
	}
}

func TestHandler_GoodBearerPassesAuth(t *testing.T) {
	gw := newStubGateway([]string{"smc_engine"}, aegis.SignalResult{}, nil)
	h := Handler("secret-token", gw, nil)

	// A correct bearer passes the auth gate and reaches the MCP transport. We
	// send an MCP initialize request so the transport responds 200 (not 401),
	// proving auth let us through. No tool is dispatched, so the gateway stays untouched.
	body := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`
	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret-token")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code == http.StatusUnauthorized || rec.Code == http.StatusServiceUnavailable {
		t.Fatalf("correct bearer should pass auth, got %d (body=%s)", rec.Code, rec.Body.String())
	}
}

func TestHandler_WithSourceTagOption(t *testing.T) {
	gw := newStubGateway([]string{"smc_engine"}, aegis.SignalResult{}, nil)
	// Verify WithSourceTag option is accepted without panic.
	h := Handler("tok", gw, nil, WithSourceTag("custom-source"))
	if h == nil {
		t.Error("Handler should return non-nil handler")
	}
}

func TestHandler_StrategyFreeText_HandlerNonNil(t *testing.T) {
	// Strategy is free-text: Handler builds without any strategy allowlist.
	// Verify it is non-nil for a configured gateway.
	gw := newStubGateway(nil, aegis.SignalResult{}, nil)
	h := Handler("tok", gw, nil)
	if h == nil {
		t.Error("Handler should return non-nil handler")
	}
}

// ── sanitizeIdempotencyKey unit tests ─────────────────────────────────────────

func TestSanitizeIdempotencyKey_ValidChars(t *testing.T) {
	got := sanitizeIdempotencyKey("abc-123.XY_z")
	if got != "abc-123.XY_z" {
		t.Errorf("got %q, want unchanged", got)
	}
}

func TestSanitizeIdempotencyKey_StripsBadChars(t *testing.T) {
	got := sanitizeIdempotencyKey("abc!@#def")
	if got != "abcdef" {
		t.Errorf("got %q, want %q", got, "abcdef")
	}
}

func TestSanitizeIdempotencyKey_CapsAt64(t *testing.T) {
	long := strings.Repeat("a", 100)
	got := sanitizeIdempotencyKey(long)
	if len(got) != 64 {
		t.Errorf("expected len=64, got %d", len(got))
	}
}

func TestSanitizeIdempotencyKey_AllInvalid(t *testing.T) {
	got := sanitizeIdempotencyKey("!@#$%")
	if got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}
