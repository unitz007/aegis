package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/unitz007/aegis"
	"github.com/unitz007/aegis/broker"
	"github.com/unitz007/aegis/broker/paper"
	"github.com/unitz007/aegis/transport/webhook"
)

// A signed request carrying an account must place the order on THAT account's
// broker and never on the server's default broker.
func TestEndToEnd_TradeLandsOnTheRequestedAccount(t *testing.T) {
	defaultBroker := paper.New(100_000, nil)
	def := &aegis.Executor{Broker: defaultBroker, BrokerName: "paper", MinConfidence: 45}

	accts := newAccounts(*def, false)
	brokers := map[string]*paper.Broker{}
	accts.newBroker = func(s accountSpec) broker.Port {
		b := paper.New(10_000, nil)
		brokers[s.Email] = b
		return b
	}
	place := func(ctx context.Context, in aegis.OrderIntent, id string) (string, bool, error) {
		if e, ok := executorFrom(ctx); ok {
			return e.ExecuteWithResult(ctx, in, id)
		}
		return def.ExecuteWithResult(ctx, in, id)
	}
	gw, err := aegis.NewGateway(place, aegis.GatewayConfig{
		AllowedSources: []string{"finos"}, MinRR: 1.5,
		SymbolClassifier: aegis.ClassifySymbolDefault, ExecutabilityCheck: aegis.DefaultExecutability,
	})
	if err != nil {
		t.Fatal(err)
	}
	const secret = "s3cret"
	h := webhook.Handler(secret, gw, webhook.WithSourceTag("finos"), webhook.WithAccountResolver(accts.resolve))

	send := func(account map[string]string) *httptest.ResponseRecorder {
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
	acct := func(email string) map[string]string {
		return map[string]string{"provider": "capital", "env": "demo", "api_key": "k", "email": email, "password": "p"}
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
