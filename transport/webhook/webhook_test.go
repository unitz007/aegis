package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/unitz007/aegis"
)

// ── Stub gateway ──────────────────────────────────────────────────────────────

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

// ── Test helpers ──────────────────────────────────────────────────────────────

const testSecret = "test-signing-secret"

func signBody(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func validBody() map[string]any {
	return map[string]any{
		"signal_id":   "sig-webhook-001",
		"timestamp":   time.Now().Unix(),
		"symbol":      "EURUSD",
		"direction":   "BUY",
		"order_type":  "limit",
		"entry":       1.1000,
		"stop_loss":   1.0950,
		"take_profit": 1.1100,
		"size":        10000,
		"strategy":    "smc_engine",
		"note":        "test",
	}
}

func makeRequest(t *testing.T, secret string, body map[string]any) *http.Request {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	sig := signBody(secret, b)
	req := httptest.NewRequest(http.MethodPost, "/signal/trade", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Signature", "sha256="+sig)
	return req
}

func decodeResponse(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v (body=%s)", err, rec.Body.String())
	}
	return out
}

func newGW(res aegis.SignalResult, err error) *stubGateway {
	return &stubGateway{res: res, err: err}
}

// ── Core security tests ───────────────────────────────────────────────────────

func TestWebhook_GoodSignatureSubmitsToGateway(t *testing.T) {
	gw := newGW(aegis.SignalResult{
		Accepted: true, Placed: true,
		BrokerOrderID: "BRK-001", Code: aegis.ReasonPlaced,
	}, nil)
	h := Handler(testSecret, gw)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, makeRequest(t, testSecret, validBody()))

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d (body=%s)", rec.Code, rec.Body.String())
	}
	if gw.count() != 1 {
		t.Fatalf("want 1 gateway call, got %d", gw.count())
	}
	out := decodeResponse(t, rec)
	if out["accepted"] != true {
		t.Errorf("expected accepted=true: %+v", out)
	}
	if out["placed"] != true {
		t.Errorf("expected placed=true: %+v", out)
	}
	// The caller's size reaches the gateway exactly as sent.
	if got := gw.last().Size; got != 10000 {
		t.Errorf("size = %v, want 10000 passed through", got)
	}
}

func TestWebhook_BadSignatureIs401NoSubmit(t *testing.T) {
	gw := newGW(aegis.SignalResult{}, nil)
	h := Handler(testSecret, gw)

	b, _ := json.Marshal(validBody())
	req := httptest.NewRequest(http.MethodPost, "/signal/trade", bytes.NewReader(b))
	req.Header.Set("X-Signature", "sha256=deadbeef0000000000000000000000000000000000000000000000000000dead")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", rec.Code)
	}
	if gw.count() != 0 {
		t.Fatalf("gateway must not be called with bad signature, got %d calls", gw.count())
	}
}

func TestWebhook_MissingSignatureIs401(t *testing.T) {
	gw := newGW(aegis.SignalResult{}, nil)
	h := Handler(testSecret, gw)

	b, _ := json.Marshal(validBody())
	req := httptest.NewRequest(http.MethodPost, "/signal/trade", bytes.NewReader(b))
	// No X-Signature header.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("want 401 for missing signature, got %d", rec.Code)
	}
	if gw.count() != 0 {
		t.Fatalf("gateway must not be called without signature")
	}
}

func TestWebhook_StaleTimestampRejected(t *testing.T) {
	gw := newGW(aegis.SignalResult{}, nil)
	h := Handler(testSecret, gw, WithTimestampTolerance(60*time.Second))

	body := validBody()
	body["timestamp"] = time.Now().Add(-2 * time.Minute).Unix() // 2 minutes old

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, makeRequest(t, testSecret, body))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("stale timestamp: want 400, got %d", rec.Code)
	}
	if gw.count() != 0 {
		t.Fatalf("gateway must not be called for stale timestamp")
	}
}

func TestWebhook_FutureTimestampRejected(t *testing.T) {
	gw := newGW(aegis.SignalResult{}, nil)
	h := Handler(testSecret, gw, WithTimestampTolerance(60*time.Second))

	body := validBody()
	body["timestamp"] = time.Now().Add(2 * time.Minute).Unix() // 2 minutes in future

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, makeRequest(t, testSecret, body))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("future timestamp: want 400, got %d", rec.Code)
	}
	if gw.count() != 0 {
		t.Fatalf("gateway must not be called for future timestamp")
	}
}

func TestWebhook_OversizedBodyRejected(t *testing.T) {
	gw := newGW(aegis.SignalResult{}, nil)
	// Body limit of 100 bytes — our payload is larger.
	h := Handler(testSecret, gw, WithBodyLimit(100))

	// Build a body that exceeds 100 bytes.
	body := validBody()
	body["note"] = strings.Repeat("x", 200)

	b, _ := json.Marshal(body)
	sig := signBody(testSecret, b)
	req := httptest.NewRequest(http.MethodPost, "/signal/trade", bytes.NewReader(b))
	req.Header.Set("X-Signature", "sha256="+sig)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	// Should be rejected due to body limit.
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body: want 413, got %d (body=%s)", rec.Code, rec.Body.String())
	}
	if gw.count() != 0 {
		t.Fatalf("gateway must not be called for oversized body")
	}
}

func TestWebhook_DisabledWithoutSecret(t *testing.T) {
	gw := newGW(aegis.SignalResult{}, nil)
	h := Handler("", gw) // empty secret

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, makeRequest(t, testSecret, validBody()))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("no secret: want 503, got %d", rec.Code)
	}
	if gw.count() != 0 {
		t.Fatalf("gateway must not be called when disabled")
	}
}

func TestWebhook_DisabledWithoutGateway(t *testing.T) {
	h := Handler(testSecret, nil)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, makeRequest(t, testSecret, validBody()))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil gateway: want 503, got %d", rec.Code)
	}
}

func TestWebhook_MethodNotAllowedIs405(t *testing.T) {
	gw := newGW(aegis.SignalResult{}, nil)
	h := Handler(testSecret, gw)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/signal/trade", nil))

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET: want 405, got %d", rec.Code)
	}
	if gw.count() != 0 {
		t.Fatalf("gateway must not be called for non-POST")
	}
}

// ── Status mapping tests ──────────────────────────────────────────────────────

func TestWebhook_InFlightIs409(t *testing.T) {
	gw := newGW(aegis.SignalResult{
		Accepted: false, Code: aegis.ReasonInFlight,
		Reason: "signal already in flight",
	}, nil)
	h := Handler(testSecret, gw)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, makeRequest(t, testSecret, validBody()))

	if rec.Code != http.StatusConflict {
		t.Fatalf("in-flight: want 409, got %d", rec.Code)
	}
}

func TestWebhook_ValidationRejectionIs400(t *testing.T) {
	gw := newGW(aegis.SignalResult{
		Accepted: false, Code: aegis.ReasonValidation,
		Reason: "RR 0.80 below minimum 1.50",
	}, nil)
	h := Handler(testSecret, gw)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, makeRequest(t, testSecret, validBody()))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("validation reject: want 400, got %d", rec.Code)
	}
}

func TestWebhook_ExecutorSkipIs200(t *testing.T) {
	gw := newGW(aegis.SignalResult{
		Accepted: true, Placed: false, Code: aegis.ReasonExecutorSkip,
	}, nil)
	h := Handler(testSecret, gw)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, makeRequest(t, testSecret, validBody()))

	if rec.Code != http.StatusOK {
		t.Fatalf("executor skip: want 200, got %d", rec.Code)
	}
	out := decodeResponse(t, rec)
	if out["accepted"] != true {
		t.Errorf("expected accepted=true for executor skip")
	}
	if out["placed"] != false {
		t.Errorf("expected placed=false for executor skip")
	}
}

func TestWebhook_GatewayErrorIs500(t *testing.T) {
	gw := &stubGateway{err: fmt.Errorf("internal boom")}
	h := Handler(testSecret, gw)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, makeRequest(t, testSecret, validBody()))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("gateway error: want 500, got %d", rec.Code)
	}
}

// ── Payload mapping tests ─────────────────────────────────────────────────────

func TestWebhook_SourceStampedFromConfig(t *testing.T) {
	gw := newGW(aegis.SignalResult{Accepted: true, Code: aegis.ReasonExecutorSkip}, nil)
	h := Handler(testSecret, gw, WithSourceTag("my-system"))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, makeRequest(t, testSecret, validBody()))

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	if gw.count() != 1 {
		t.Fatalf("expected 1 gateway call")
	}
	sig := gw.last()
	if sig.Source != "my-system" {
		t.Errorf("source = %q, want %q", sig.Source, "my-system")
	}
}

func TestWebhook_DefaultStrategyUsedWhenOmitted(t *testing.T) {
	gw := newGW(aegis.SignalResult{Accepted: true, Code: aegis.ReasonExecutorSkip}, nil)
	h := Handler(testSecret, gw, WithDefaultStrategy("zone_internal"))

	body := validBody()
	delete(body, "strategy") // omit strategy

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, makeRequest(t, testSecret, body))

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	sig := gw.last()
	if sig.Strategy != "zone_internal" {
		t.Errorf("strategy = %q, want zone_internal", sig.Strategy)
	}
}

func TestWebhook_SignalIDTrimmed(t *testing.T) {
	gw := newGW(aegis.SignalResult{Accepted: true, Code: aegis.ReasonExecutorSkip}, nil)
	h := Handler(testSecret, gw)

	body := validBody()
	body["signal_id"] = "  sig-with-spaces  "

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, makeRequest(t, testSecret, body))

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	sig := gw.last()
	if sig.SignalID != "sig-with-spaces" {
		t.Errorf("signal_id = %q, want trimmed", sig.SignalID)
	}
}

func TestWebhook_InvalidJSONIs400(t *testing.T) {
	gw := newGW(aegis.SignalResult{}, nil)
	h := Handler(testSecret, gw)

	rawBody := []byte("not valid json{{{")
	sig := signBody(testSecret, rawBody)
	req := httptest.NewRequest(http.MethodPost, "/signal/trade", bytes.NewReader(rawBody))
	req.Header.Set("X-Signature", "sha256="+sig)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid JSON: want 400, got %d", rec.Code)
	}
	if gw.count() != 0 {
		t.Fatalf("gateway must not be called for invalid JSON")
	}
}

// ── verifyHMAC unit tests ─────────────────────────────────────────────────────

func TestVerifyHMAC_CorrectSignature(t *testing.T) {
	secret := []byte("my-secret")
	body := []byte(`{"hello":"world"}`)
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	sig := hex.EncodeToString(mac.Sum(nil))

	if !verifyHMAC(secret, body, sig) {
		t.Error("expected verifyHMAC to return true for correct signature")
	}
}

func TestVerifyHMAC_WrongSecret(t *testing.T) {
	body := []byte(`{"hello":"world"}`)
	mac := hmac.New(sha256.New, []byte("correct-secret"))
	mac.Write(body)
	sig := hex.EncodeToString(mac.Sum(nil))

	if verifyHMAC([]byte("wrong-secret"), body, sig) {
		t.Error("expected verifyHMAC to return false for wrong secret")
	}
}

func TestVerifyHMAC_EmptySig(t *testing.T) {
	if verifyHMAC([]byte("secret"), []byte("body"), "") {
		t.Error("expected false for empty signature")
	}
}

func TestVerifyHMAC_TamperedBody(t *testing.T) {
	secret := []byte("my-secret")
	original := []byte(`{"amount":100}`)
	mac := hmac.New(sha256.New, secret)
	mac.Write(original)
	sig := hex.EncodeToString(mac.Sum(nil))

	tampered := []byte(`{"amount":999}`)
	if verifyHMAC(secret, tampered, sig) {
		t.Error("expected false for tampered body")
	}
}

// ── Per-request accounts ──────────────────────────────────────────────────────

type ctxKey struct{}

// ctxGateway records the context and signal it was handed.
type ctxGateway struct {
	ctx context.Context
	sig aegis.TradeSignal
}

func (g *ctxGateway) Submit(ctx context.Context, sig aegis.TradeSignal) (aegis.SignalResult, error) {
	g.ctx, g.sig = ctx, sig
	return aegis.SignalResult{Accepted: true, Placed: true, BrokerOrderID: "B1"}, nil
}

func post(h http.Handler, body map[string]any) *httptest.ResponseRecorder {
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/signal/trade", bytes.NewReader(raw))
	req.Header.Set("X-Signature", "sha256="+signBody(testSecret, raw))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func resolver(ctx context.Context, raw json.RawMessage) (context.Context, string, error) {
	var a struct {
		ID, Password string
	}
	_ = json.Unmarshal(raw, &a)
	if a.ID == "" {
		return nil, "", fmt.Errorf("account id missing")
	}
	if a.ID == "bad" {
		return nil, "", fmt.Errorf("unsupported account %q", a.ID)
	}
	return context.WithValue(ctx, ctxKey{}, a.ID), "acct-" + a.ID, nil
}

func TestAccount_RoutedAndNamespaced(t *testing.T) {
	gw := &ctxGateway{}
	h := Handler(testSecret, gw, WithSourceTag("finos"), WithAccountResolver(resolver))
	body := validBody()
	body["account"] = map[string]any{"id": "u1", "password": "hunter2"}
	rec := post(h, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if got := gw.sig.SignalID; got != "acct-u1:sig-webhook-001" {
		t.Errorf("signal id not namespaced per account: %q", got)
	}
	if gw.ctx.Value(ctxKey{}) != "u1" {
		t.Errorf("the account's context did not reach the gateway")
	}
}

func TestAccount_RejectedWhenNotEnabled(t *testing.T) {
	gw := &stubGateway{res: aegis.SignalResult{Accepted: true}}
	h := Handler(testSecret, gw, WithSourceTag("finos")) // no resolver
	body := validBody()
	body["account"] = map[string]any{"id": "u1"}
	rec := post(h, body)
	if rec.Code != http.StatusBadRequest || gw.count() != 0 {
		t.Fatalf("want 400 and no gateway call, got %d calls=%d body=%s", rec.Code, gw.count(), rec.Body)
	}
}

func TestAccount_ResolverErrorRejectsWithoutSecrets(t *testing.T) {
	gw := &stubGateway{res: aegis.SignalResult{Accepted: true}}
	h := Handler(testSecret, gw, WithSourceTag("finos"), WithAccountResolver(resolver))
	body := validBody()
	body["account"] = map[string]any{"id": "bad", "password": "hunter2"}
	rec := post(h, body)
	if rec.Code != http.StatusBadRequest || gw.count() != 0 {
		t.Fatalf("want 400 and no gateway call, got %d calls=%d", rec.Code, gw.count())
	}
	if strings.Contains(rec.Body.String(), "hunter2") {
		t.Errorf("response leaked the password: %s", rec.Body)
	}
}

func TestAccount_AbsentLeavesExistingBehaviour(t *testing.T) {
	gw := &ctxGateway{}
	h := Handler(testSecret, gw, WithSourceTag("finos"), WithAccountResolver(resolver))
	rec := post(h, validBody())
	if rec.Code != http.StatusOK || gw.sig.SignalID != "sig-webhook-001" || gw.ctx.Value(ctxKey{}) != nil {
		t.Fatalf("a request without an account must behave exactly as before: code=%d id=%q", rec.Code, gw.sig.SignalID)
	}
	body := validBody()
	body["account"] = nil // explicit null counts as absent
	if rec := post(h, body); rec.Code != http.StatusOK {
		t.Errorf("null account: status %d", rec.Code)
	}
}

func TestAccount_DifferentAccountsDoNotShareSignalIDs(t *testing.T) {
	gw := &ctxGateway{}
	h := Handler(testSecret, gw, WithSourceTag("finos"), WithAccountResolver(resolver))
	ids := map[string]bool{}
	for _, u := range []string{"u1", "u2"} {
		body := validBody()
		body["account"] = map[string]any{"id": u}
		post(h, body)
		ids[gw.sig.SignalID] = true
	}
	if len(ids) != 2 {
		t.Errorf("same signal_id from two accounts must map to two distinct ids, got %v", ids)
	}
}

// ── Account check ─────────────────────────────────────────────────────────────

func postCheck(h http.Handler, secret string, body map[string]any) *httptest.ResponseRecorder {
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/account/check", bytes.NewReader(raw))
	req.Header.Set("X-Signature", "sha256="+signBody(secret, raw))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestAccountCheck(t *testing.T) {
	var checked context.Context
	check := func(ctx context.Context) (any, error) {
		checked = ctx
		if ctx.Value(ctxKey{}) == "broken" {
			return nil, fmt.Errorf("login failed for hunter2")
		}
		return map[string]any{"currency": "USD"}, nil
	}
	h := AccountCheckHandler(testSecret, resolver, check)
	now := time.Now().Unix()

	if rec := postCheck(h, testSecret, map[string]any{"timestamp": now, "account": map[string]any{"id": "u1"}}); rec.Code != 200 ||
		!strings.Contains(rec.Body.String(), `"ok":true`) || !strings.Contains(rec.Body.String(), "USD") || checked.Value(ctxKey{}) != "u1" {
		t.Errorf("good account: %d %s", rec.Code, rec.Body)
	}
	if rec := postCheck(h, "wrong-secret", map[string]any{"timestamp": now, "account": map[string]any{"id": "u1"}}); rec.Code != http.StatusUnauthorized {
		t.Errorf("bad signature: %d", rec.Code)
	}
	if rec := postCheck(h, testSecret, map[string]any{"timestamp": now - 3600, "account": map[string]any{"id": "u1"}}); rec.Code != http.StatusBadRequest {
		t.Errorf("stale timestamp: %d", rec.Code)
	}
	if rec := postCheck(h, testSecret, map[string]any{"timestamp": now}); rec.Code != http.StatusBadRequest {
		t.Errorf("missing account: %d", rec.Code)
	}
	rec := postCheck(h, testSecret, map[string]any{"timestamp": now, "account": map[string]any{"id": "broken"}})
	if rec.Code != http.StatusBadGateway || strings.Contains(rec.Body.String(), "hunter2") {
		t.Errorf("broker failure must be 502 and leak nothing: %d %s", rec.Code, rec.Body)
	}
	if rec := postCheck(AccountCheckHandler("", resolver, check), "", map[string]any{"timestamp": now, "account": map[string]any{"id": "u1"}}); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("no secret must fail closed: %d", rec.Code)
	}
}

// The minimum reward-to-risk the sender asks for reaches the gateway unchanged,
// and when they don't ask, none is invented.
func TestWebhook_MinRRIsPassedThroughAsSent(t *testing.T) {
	for _, tc := range []struct {
		name string
		body func(m map[string]any)
		want float64
	}{
		{"asked", func(m map[string]any) { m["min_rr"] = 2.5 }, 2.5},
		{"not asked", func(m map[string]any) {}, 0},
	} {
		gw := newGW(aegis.SignalResult{Accepted: true, Placed: true, Code: aegis.ReasonPlaced}, nil)
		h := Handler(testSecret, gw)
		body := validBody()
		tc.body(body)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, makeRequest(t, testSecret, body))
		if gw.count() != 1 {
			t.Fatalf("%s: want 1 gateway call, got %d (%d %s)", tc.name, gw.count(), rec.Code, rec.Body.String())
		}
		if got := gw.last().MinRR; got != tc.want {
			t.Errorf("%s: min_rr reached the gateway as %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The person's own limits reach the gateway exactly as sent, and when none are sent none
// are invented: aegis has no limits of its own.
func TestWebhook_LimitsArePassedThroughAsSentAndNeverInvented(t *testing.T) {
	gw := newGW(aegis.SignalResult{Accepted: true, Placed: true, Code: aegis.ReasonPlaced}, nil)
	h := Handler(testSecret, gw)
	body := validBody()
	body["limits"] = map[string]any{"max_units": 50000, "max_crypto_value": 2500.5, "max_open_positions": 3, "max_trades_per_day": 4, "max_daily_loss": 300}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, makeRequest(t, testSecret, body))
	if gw.count() != 1 {
		t.Fatalf("want 1 gateway call: %d %s", rec.Code, rec.Body)
	}
	want := aegis.Limits{MaxUnits: 50000, MaxCryptoValue: 2500.5, MaxOpenPositions: 3, MaxTradesPerDay: 4, MaxDailyLoss: 300}
	if got := gw.last().Limits; got != want {
		t.Errorf("limits reached the gateway as %+v, want %+v", got, want)
	}

	gw2 := newGW(aegis.SignalResult{Accepted: true, Placed: true, Code: aegis.ReasonPlaced}, nil)
	rec = httptest.NewRecorder()
	Handler(testSecret, gw2).ServeHTTP(rec, makeRequest(t, testSecret, validBody()))
	if got := gw2.last().Limits; got != (aegis.Limits{}) {
		t.Errorf("no limits sent must mean none: %+v", got)
	}
}
