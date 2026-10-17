package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/unitz007/aegis"
	"github.com/unitz007/aegis/broker/paper"
	"github.com/unitz007/aegis/sealed"
	"github.com/unitz007/aegis/transport/webhook"
)

// A signed request carrying an account must place the order on THAT account's
// broker and never on the server's default broker.
func TestEndToEnd_TradeLandsOnTheRequestedAccount(t *testing.T) {
	defaultBroker := paper.New(100_000, nil)
	def := &aegis.Executor{Broker: defaultBroker, BrokerName: "paper", MinConfidence: 45}

	r := newRig(t, false)
	accts := r.a
	brokers := r.brokers
	place := func(ctx context.Context, in aegis.OrderIntent, id string) (string, bool, error) {
		if e, ok := executorFrom(ctx); ok {
			return e.ExecuteWithResult(ctx, in, id)
		}
		return def.ExecuteWithResult(ctx, in, id)
	}
	gw, err := aegis.NewGateway(place, aegis.GatewayConfig{
		AllowedSources:   []string{"finos"},
		SymbolClassifier: aegis.ClassifySymbolDefault, ExecutabilityCheck: aegis.DefaultExecutability,
	})
	if err != nil {
		t.Fatal(err)
	}
	const secret = "s3cret"
	h := webhook.Handler(secret, gw, webhook.WithSourceTag("finos"), webhook.WithAccountResolver(accts.resolve))

	send := func(account json.RawMessage) *httptest.ResponseRecorder {
		body := map[string]any{
			"signal_id": "same-id", "timestamp": time.Now().Unix(), "symbol": "EURUSD", "direction": "BUY",
			"order_type": "limit", "entry": 1.1000, "stop_loss": 1.0950, "take_profit": 1.1100, "size": 10000, "strategy": "agent",
		}
		if account != nil {
			body["account"] = account
		}
		raw, _ := json.Marshal(body)
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write(raw)
		req := httptest.NewRequest(http.MethodPost, "/signal/trade", bytes.NewReader(raw))
		req.Header.Set("X-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	acct := func(email string) json.RawMessage {
		return r.account(t, "capital", "demo", map[string]string{"api_key": "k", "email": email, "password": "p"})
	}

	// Same signal_id from two different accounts: both must be placed.
	for _, email := range []string{"u1@x.co", "u2@x.co"} {
		if rec := send(acct(email)); rec.Code != 200 {
			t.Fatalf("%s: %d %s", email, rec.Code, rec.Body)
		}
	}
	count := func(b *paper.Broker) int {
		orders, _ := b.OpenOrders(context.Background())
		trades, _ := b.OpenTrades(context.Background())
		return len(orders) + len(trades)
	}
	if count(brokers["u1@x.co"]) != 1 || count(brokers["u2@x.co"]) != 1 {
		t.Errorf("each account should hold exactly one order: u1=%d u2=%d", count(brokers["u1@x.co"]), count(brokers["u2@x.co"]))
	}
	if count(defaultBroker) != 0 {
		t.Errorf("the default broker must not receive account trades (has %d)", count(defaultBroker))
	}

	// Repeating a signal_id on the same account is deduplicated.
	if rec := send(acct("u1@x.co")); rec.Code != 200 || count(brokers["u1@x.co"]) != 1 {
		t.Errorf("retry on the same account must not place twice: %d count=%d", rec.Code, count(brokers["u1@x.co"]))
	}

	// A request without an account still uses the default broker, as before.
	if rec := send(nil); rec.Code != 200 || count(defaultBroker) != 1 {
		t.Errorf("no-account request should use the default broker: %d count=%d", rec.Code, count(defaultBroker))
	}
}

// The wire format never carries a readable login, and a request naming a broker
// that isn't registered is refused before anything happens.
func TestEndToEnd_LoginIsNeverReadableOnTheWire_AndUnknownBrokersAreRefused(t *testing.T) {
	r := newRig(t, false)
	place := func(ctx context.Context, in aegis.OrderIntent, id string) (string, bool, error) {
		if e, ok := executorFrom(ctx); ok {
			return e.ExecuteWithResult(ctx, in, id)
		}
		return "", false, nil
	}
	gw, _ := aegis.NewGateway(place, aegis.GatewayConfig{AllowedSources: []string{"finos"},
		SymbolClassifier: aegis.ClassifySymbolDefault, ExecutabilityCheck: aegis.DefaultExecutability})
	const secret = "s3cret"
	h := webhook.Handler(secret, gw, webhook.WithSourceTag("finos"), webhook.WithAccountResolver(r.a.resolve))

	post := func(account json.RawMessage) (*httptest.ResponseRecorder, []byte) {
		raw, _ := json.Marshal(map[string]any{
			"signal_id": "sig-wire", "timestamp": time.Now().Unix(), "symbol": "EURUSD", "direction": "BUY",
			"order_type": "limit", "entry": 1.1000, "stop_loss": 1.0950, "take_profit": 1.1100, "size": 10000, "account": account,
		})
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write(raw)
		req := httptest.NewRequest(http.MethodPost, "/signal/trade", bytes.NewReader(raw))
		req.Header.Set("X-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec, raw
	}

	rec, wire := post(r.account(t, "capital", "demo", map[string]string{"api_key": "KEY-123", "email": "who@x.co", "password": "hunter2"}))
	if rec.Code != 200 {
		t.Fatalf("a sealed login should work: %d %s", rec.Code, rec.Body)
	}
	for _, secretBit := range []string{"KEY-123", "who@x.co", "hunter2"} {
		if bytes.Contains(wire, []byte(secretBit)) || bytes.Contains(rec.Body.Bytes(), []byte(secretBit)) {
			t.Errorf("%q must not appear in the request or the response", secretBit)
		}
	}

	rec, _ = post(r.account(t, "oanda", "demo", good()))
	if rec.Code != 400 || !bytes.Contains(rec.Body.Bytes(), []byte("not registered")) {
		t.Errorf("an unregistered broker should be a 400 saying so: %d %s", rec.Code, rec.Body)
	}
}

// aegis publishes the key to seal to, and what is sealed to it opens.
func TestKeyHandler_PublishesTheKeySendersSealTo(t *testing.T) {
	priv, pub, _ := sealed.GenerateKey()
	rec := httptest.NewRecorder()
	keyHandler(pub).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/account/key", nil))
	var out map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || rec.Code != 200 {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	got, err := base64.StdEncoding.DecodeString(out["public_key"])
	if err != nil || out["key_id"] == "" || out["algorithm"] == "" {
		t.Fatalf("incomplete key document: %v", out)
	}
	blob, err := sealed.Seal(got, []byte("c"), []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if plain, err := sealed.Open(priv, []byte("c"), blob); err != nil || string(plain) != "hello" {
		t.Fatalf("what is sealed to the published key must open with the private key: %v", err)
	}
	post := httptest.NewRecorder()
	keyHandler(pub).ServeHTTP(post, httptest.NewRequest(http.MethodPost, "/account/key", nil))
	if post.Code != http.StatusMethodNotAllowed {
		t.Errorf("only GET: %d", post.Code)
	}
}
