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
