package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/unitz007/aegis/transport/webhook"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/unitz007/aegis"
	"github.com/unitz007/aegis/broker"
	"github.com/unitz007/aegis/broker/paper"
	"github.com/unitz007/aegis/sealed"
)

// A registry with one broker, "capital", whose connection is a paper broker so
// no network is involved. `built` counts connections made.
func testRegistry(built *int, record map[string]*paper.Broker) *broker.Registry {
	r := broker.NewRegistry()
	err := r.Register(broker.Spec{
		Name: "capital", Envs: []string{"demo", "live"},
		Fields: []broker.Field{{Name: "api_key"}, {Name: "email"}, {Name: "password"}}, Identity: []string{"api_key", "email"},
		New: func(env string, c broker.Credentials) (broker.Port, error) {
			*built++
			b := paper.New(5_000, nil)
			if record != nil {
				record[c["email"]] = b
			}
			return b, nil
		},
	})
	if err != nil {
		panic(err)
	}
	return r
}

type rig struct {
	a       *accounts
	public  []byte
	built   *int
	brokers map[string]*paper.Broker
}

func newRig(t *testing.T, allowLive bool) *rig {
	t.Helper()
	priv, pub, err := sealed.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	built := new(int)
	brokers := map[string]*paper.Broker{}
	return &rig{a: newAccounts(aegis.Executor{MinConfidence: 45}, testRegistry(built, brokers), priv, allowLive), public: pub, built: built, brokers: brokers}
}

func good() map[string]string {
	return map[string]string{"api_key": "k1", "email": "a@b.co", "password": "hunter2"}
}

// account builds the `account` object a sender would put in the signed body.
func (r *rig) account(t *testing.T, brokerName, env string, creds map[string]string) json.RawMessage {
	t.Helper()
	plain, _ := json.Marshal(creds)
	blob, err := sealed.Seal(r.public, sealContext(strings.ToLower(strings.TrimSpace(brokerName)), env), plain)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]string{"broker": brokerName, "env": env, "credentials": base64.StdEncoding.EncodeToString(blob)})
	return raw
}

func TestResolve_OpensTheSealedLoginAndBuildsAnExecutorForIt(t *testing.T) {
	r := newRig(t, false)
	ctx, key, err := r.a.resolve(context.Background(), r.account(t, "capital", "demo", good()))
	if err != nil || key == "" {
		t.Fatalf("resolve: key=%q err=%v", key, err)
	}
	e, ok := executorFrom(ctx)
	if !ok || e.Broker == nil || e.MinConfidence != 45 || e.BrokerName != "capital:demo" {
		t.Fatalf("executor missing or not built from the template: %+v ok=%v", e, ok)
	}
	if strings.Contains(key, "hunter2") || strings.Contains(key, "k1") || strings.Contains(key, "a@b.co") {
		t.Errorf("account key must not contain credentials: %q", key)
	}
	if r.brokers["a@b.co"] == nil {
		t.Error("the broker should have been connected with the opened login")
	}
}

func TestResolve_BrokerNameIsCaseInsensitive(t *testing.T) {
	r := newRig(t, false)
	if _, _, err := r.a.resolve(context.Background(), r.account(t, "  CAPITAL ", "demo", good())); err != nil {
		t.Fatalf("broker name should ignore case: %v", err)
	}
}

func TestResolve_ReusesTheBrokerAcrossRequests(t *testing.T) {
	r := newRig(t, false)
	ctx1, k1, _ := r.a.resolve(context.Background(), r.account(t, "capital", "demo", good()))
	ctx2, k2, _ := r.a.resolve(context.Background(), r.account(t, "capital", "demo", good())) // sealed afresh: different bytes
	e1, _ := executorFrom(ctx1)
	e2, _ := executorFrom(ctx2)
	if k1 != k2 || e1 != e2 || *r.built != 1 {
		t.Errorf("same account should reuse one broker: keys %q/%q built=%d", k1, k2, *r.built)
	}
}

func TestResolve_DifferentAccountsAreIsolated(t *testing.T) {
	r := newRig(t, false)
	other := good()
	other["email"] = "other@b.co"
	c1, k1, _ := r.a.resolve(context.Background(), r.account(t, "capital", "demo", good()))
	c2, k2, _ := r.a.resolve(context.Background(), r.account(t, "capital", "demo", other))
	e1, _ := executorFrom(c1)
	e2, _ := executorFrom(c2)
	if k1 == k2 || e1 == e2 || e1.Broker == e2.Broker {
		t.Errorf("two accounts must never share a key or broker")
	}
}

func TestResolve_DemoAndLiveOfTheSameLoginAreDifferentAccounts(t *testing.T) {
	r := newRig(t, true)
	_, kd, _ := r.a.resolve(context.Background(), r.account(t, "capital", "demo", good()))
	_, kl, _ := r.a.resolve(context.Background(), r.account(t, "capital", "live", good()))
	if kd == kl {
		t.Error("demo and live must never share an account key")
	}
}

func TestResolve_ChangedPasswordRebuildsTheBrokerButKeepsTheAccount(t *testing.T) {
	r := newRig(t, false)
	_, k1, _ := r.a.resolve(context.Background(), r.account(t, "capital", "demo", good()))
	changed := good()
	changed["password"] = "new-password"
	_, k2, _ := r.a.resolve(context.Background(), r.account(t, "capital", "demo", changed))
	if k1 != k2 || *r.built != 2 {
		t.Errorf("a new password should keep the key but rebuild the broker: built=%d", *r.built)
	}
}

func TestResolve_OnlyRegisteredBrokersAreAccepted(t *testing.T) {
	r := newRig(t, false)
	_, _, err := r.a.resolve(context.Background(), r.account(t, "oanda", "demo", good()))
	if err == nil || !strings.Contains(err.Error(), "not registered") || !strings.Contains(err.Error(), "capital") {
		t.Fatalf("an unregistered broker must be refused and the registered ones named: %v", err)
	}
	if *r.built != 0 {
		t.Error("nothing may be connected for an unregistered broker")
	}
}

func TestResolve_RefusesPlaintextLogins(t *testing.T) {
	r := newRig(t, false)
	plain, _ := json.Marshal(map[string]string{"broker": "capital", "env": "demo", "api_key": "k1", "email": "a@b.co", "password": "hunter2"})
	if _, _, err := r.a.resolve(context.Background(), plain); err == nil {
		t.Fatal("a login sent in the clear must be refused")
	} else if strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("error leaked the password: %v", err)
	}
	// Even next to a valid sealed login, a stray plaintext field is refused, not ignored.
	var m map[string]any
	json.Unmarshal(r.account(t, "capital", "demo", good()), &m) //nolint:errcheck
	m["password"] = "hunter2"
	mixed, _ := json.Marshal(m)
	if _, _, err := r.a.resolve(context.Background(), mixed); err == nil {
		t.Fatal("plaintext fields beside the sealed login must be refused")
	}
	if *r.built != 0 {
		t.Error("nothing may be connected from a refused request")
	}
}

func TestResolve_SealedForOneEnvOrBrokerCannotBeUsedForAnother(t *testing.T) {
	r := newRig(t, true)
	// Sealed for demo, presented as live.
	var m map[string]string
	json.Unmarshal(r.account(t, "capital", "demo", good()), &m) //nolint:errcheck
	m["env"] = "live"
	raw, _ := json.Marshal(m)
	if _, _, err := r.a.resolve(context.Background(), raw); err == nil {
		t.Fatal("demo credentials must not open as live")
	}
	if *r.built != 0 {
		t.Error("nothing may be connected")
	}
}

func TestResolve_AValueSealedToAnotherKeyIsRefused(t *testing.T) {
	r := newRig(t, false)
	_, otherPub, _ := sealed.GenerateKey()
	plain, _ := json.Marshal(good())
	blob, _ := sealed.Seal(otherPub, sealContext("capital", "demo"), plain)
	raw, _ := json.Marshal(map[string]string{"broker": "capital", "env": "demo", "credentials": base64.StdEncoding.EncodeToString(blob)})
	if _, _, err := r.a.resolve(context.Background(), raw); err == nil {
		t.Fatal("a login sealed to somebody else's key must be refused")
	}
}

func TestResolve_Rejections(t *testing.T) {
	r := newRig(t, false)
	seal := func(creds map[string]string) json.RawMessage { return r.account(t, "capital", "demo", creds) }
	without := func(field string) map[string]string { m := good(); delete(m, field); return m }
	blank := func(field string) map[string]string { m := good(); m[field] = "  "; return m }
	extra := func() map[string]string { m := good(); m["pin"] = "1234"; return m }
	cases := map[string]json.RawMessage{
		"bad env":               r.account(t, "capital", "staging", good()),
		"live disabled":         r.account(t, "capital", "live", good()),
		"missing key":           seal(without("api_key")),
		"missing email":         seal(without("email")),
		"blank password":        seal(blank("password")),
		"unknown login field":   seal(extra()),
		"not an object":         json.RawMessage(`"not an object"`),
		"no credentials":        json.RawMessage(`{"broker":"capital","env":"demo"}`),
		"credentials not b64":   json.RawMessage(`{"broker":"capital","env":"demo","credentials":"%%%"}`),
		"credentials gibberish": json.RawMessage(fmt.Sprintf(`{"broker":"capital","env":"demo","credentials":%q}`, base64.StdEncoding.EncodeToString([]byte("not sealed at all, just some bytes here")))),
	}
	for name, raw := range cases {
		_, _, err := r.a.resolve(context.Background(), raw)
		if err == nil {
			t.Errorf("%s: expected a rejection", name)
		} else if strings.Contains(err.Error(), "hunter2") || strings.Contains(err.Error(), "a@b.co") {
			t.Errorf("%s: error leaked credentials: %v", name, err)
		}
	}
	if *r.built != 0 {
		t.Errorf("rejected requests must not build a broker (built=%d)", *r.built)
	}
}

func TestResolve_LiveAllowedOnlyWhenEnabled(t *testing.T) {
	r := newRig(t, true)
	ctx, _, err := r.a.resolve(context.Background(), r.account(t, "capital", "live", good()))
	if err != nil {
		t.Fatalf("live should be allowed when enabled: %v", err)
	}
	if e, _ := executorFrom(ctx); e.BrokerName != "capital:live" {
		t.Errorf("broker name %q", e.BrokerName)
	}
}

func TestResolve_CacheIsBounded(t *testing.T) {
	r := newRig(t, false)
	r.a.maxSize = 3
	for i := 0; i < 10; i++ {
		m := good()
		m["email"] = string(rune('a'+i)) + "@b.co"
		if _, _, err := r.a.resolve(context.Background(), r.account(t, "capital", "demo", m)); err != nil {
			t.Fatal(err)
		}
	}
	if len(r.a.cache) > 3 {
		t.Errorf("cache grew past its bound: %d", len(r.a.cache))
	}
}

func TestCheck_ReportsTheAccountWithoutTrading(t *testing.T) {
	r := newRig(t, false)
	ctx, _, _ := r.a.resolve(context.Background(), r.account(t, "capital", "demo", good()))
	info, err := r.a.check(ctx)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	m := info.(map[string]any)
	if m["env"] != "demo" || m["balance"] != 5000.0 {
		t.Errorf("unexpected account info: %v", m)
	}
	if _, err := r.a.check(context.Background()); err == nil {
		t.Errorf("check without an account in context must fail")
	}
}

func TestLoadSealKey(t *testing.T) {
	if priv, pub, err := loadSealKey("  "); priv != nil || pub != nil || err != nil {
		t.Error("an empty setting means the feature is off, not an error")
	}
	priv, pub, _ := sealed.GenerateKey()
	gotPriv, gotPub, err := loadSealKey(base64.StdEncoding.EncodeToString(priv))
	if err != nil || string(gotPriv) != string(priv) || string(gotPub) != string(pub) {
		t.Errorf("a valid key should load and give its public half: %v", err)
	}
	for _, bad := range []string{"%%%", base64.StdEncoding.EncodeToString([]byte("short"))} {
		if _, _, err := loadSealKey(bad); err == nil {
			t.Errorf("%q should be refused", bad)
		}
	}
}

func TestBrokersHandler_PublishesTheOptionsAndNeverAnAccount(t *testing.T) {
	built := new(int)
	reg := testRegistry(built, nil)
	get := func(allowLive bool) map[string]any {
		rec := httptest.NewRecorder()
		brokersHandler(reg, allowLive).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/brokers", nil))
		if rec.Code != 200 {
			t.Fatalf("%d", rec.Code)
		}
		var out map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	envsOf := func(out map[string]any) []any {
		return out["brokers"].([]any)[0].(map[string]any)["envs"].([]any)
	}
	if e := envsOf(get(false)); len(e) != 1 || e[0] != "demo" {
		t.Errorf("live must not be offered while it is off: %v", e)
	}
	if e := envsOf(get(true)); len(e) != 2 {
		t.Errorf("live should be offered when allowed: %v", e)
	}
	b := get(false)["brokers"].([]any)[0].(map[string]any)
	if b["name"] != "capital" || len(b["fields"].([]any)) != 3 {
		t.Errorf("name and the three login fields should be listed: %v", b)
	}
	post := httptest.NewRecorder()
	brokersHandler(reg, false).ServeHTTP(post, httptest.NewRequest(http.MethodPost, "/brokers", nil))
	if post.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET only: %d", post.Code)
	}
	if *built != 0 {
		t.Error("listing brokers must never connect to one")
	}
}

// The published list is a wire format other programs read: its keys are fixed,
// lower case, and say nothing beyond what a connect screen needs.
func TestBrokersHandler_WireFormatIsStable(t *testing.T) {
	reg := testRegistry(new(int), nil)
	rec := httptest.NewRecorder()
	brokersHandler(reg, false).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/brokers", nil))
	var raw struct {
		Brokers []map[string]json.RawMessage `json:"brokers"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil || len(raw.Brokers) != 1 {
		t.Fatalf("%v %s", err, rec.Body.String())
	}
	for _, k := range []string{"name", "label", "strategy", "envs", "fields"} {
		if _, ok := raw.Brokers[0][k]; !ok {
			t.Errorf("broker is missing %q: %s", k, rec.Body.String())
		}
	}
	var fields []map[string]any
	_ = json.Unmarshal(raw.Brokers[0]["fields"], &fields)
	for _, f := range fields {
		for _, k := range []string{"name", "label", "secret"} {
			if _, ok := f[k]; !ok {
				t.Errorf("field is missing lower-case %q: %v", k, f)
			}
		}
		for k := range f {
			if k != strings.ToLower(k) {
				t.Errorf("field key %q must be lower case", k)
			}
		}
	}
}

// A person's own account gets its own executor, but the pause switch is the server's: it
// must stop every account at once.
func TestResolve_EveryAccountSharesThePauseSwitch(t *testing.T) {
	priv, pub, err := sealed.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	built := new(int)
	guard := aegis.NewGuard(nil)
	template := aegis.Executor{MinConfidence: 45, Guard: guard}
	r := &rig{a: newAccounts(template, testRegistry(built, map[string]*paper.Broker{}), priv, false), public: pub, built: built}

	ctx, _, err := r.a.resolve(context.Background(), r.account(t, "capital", "demo", good()))
	if err != nil {
		t.Fatal(err)
	}
	e, _ := executorFrom(ctx)
	intent := aegis.OrderIntent{Symbol: "EURUSD", AssetClass: "forex", Direction: "BUY", OrderType: "market", EntryPrice: 1.1, StopLoss: 1.09, TakeProfit: 1.12, Size: 1_000, Manual: true}

	guard.Pause("")
	if _, placed, err := e.ExecuteWithResult(ctx, intent, "d2"); placed || err == nil || !strings.Contains(err.Error(), "paused") {
		t.Fatalf("pausing the server must stop a person's account too: placed=%v err=%v", placed, err)
	}
}

// Status reports what is open on the account, as the broker says it: the balance, each open
// position with how it is doing, and the orders still waiting. It is a read: nothing is placed,
// changed or cancelled, and one account never sees another's.
func TestStatus_ReportsOpenTradesAndOrdersWithoutChangingAnything(t *testing.T) {
	r := newRig(t, false)
	ctx, _, err := r.a.resolve(context.Background(), r.account(t, "capital", "demo", good()))
	if err != nil {
		t.Fatal(err)
	}
	pb := r.brokers["a@b.co"]
	if _, err := pb.PlaceTrade(ctx, broker.TradeRequest{Symbol: "EURUSD", Units: 10000, OrderType: "market", EntryPrice: 1.1, StopLoss: 1.09, TakeProfit: 1.13}); err != nil {
		t.Fatal(err)
	}
	if _, err := pb.PlaceTrade(ctx, broker.TradeRequest{Symbol: "USDCHF", Units: -5000, OrderType: "limit", EntryPrice: 0.93584, StopLoss: 0.94, TakeProfit: 0.92}); err != nil {
		t.Fatal(err)
	}
	trades, orders := pb.TradeCount(), pb.OrderCount()

	info, err := r.a.status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	m := info.(map[string]any)
	if m["env"] != "demo" || m["currency"] == nil || m["balance"] == nil || m["equity"] == nil {
		t.Errorf("the account: %v", m)
	}
	open := m["open_trades"].([]map[string]any)
	if len(open) != 1 || open[0]["symbol"] != "EURUSD" || open[0]["direction"] != "BUY" || open[0]["units"] != 10000 || open[0]["stop_loss"] != 1.09 || open[0]["take_profit"] != 1.13 {
		t.Errorf("the open position, with its levels: %v", open)
	}
	if _, ok := open[0]["unrealized_pnl"]; !ok {
		t.Errorf("how it is doing: %v", open[0])
	}
	pending := m["pending_orders"].([]map[string]any)
	if len(pending) != 1 || pending[0]["symbol"] != "USDCHF" || pending[0]["direction"] != "SELL" || pending[0]["units"] != 5000 || pending[0]["level"] != 0.93584 || pending[0]["order_type"] != "limit" {
		t.Errorf("the waiting order: %v", pending)
	}
	if pb.TradeCount() != trades || pb.OrderCount() != orders {
		t.Errorf("a read must change nothing: trades %d→%d, orders %d→%d", trades, pb.TradeCount(), orders, pb.OrderCount())
	}

	// Another account sees none of it.
	other := map[string]string{"api_key": "k2", "email": "other@b.co", "password": "x"}
	ctx2, _, _ := r.a.resolve(context.Background(), r.account(t, "capital", "demo", other))
	info2, err := r.a.status(ctx2)
	if err != nil {
		t.Fatal(err)
	}
	if m2 := info2.(map[string]any); len(m2["open_trades"].([]map[string]any)) != 0 || len(m2["pending_orders"].([]map[string]any)) != 0 {
		t.Errorf("another account must not see this account's positions: %v", m2)
	}
	if _, err := r.a.status(context.Background()); err == nil {
		t.Errorf("status without an account in context must fail")
	}
}

func TestStatus_NoAccountMeansEmptyListsNotNull(t *testing.T) {
	r := newRig(t, false)
	ctx, _, _ := r.a.resolve(context.Background(), r.account(t, "capital", "demo", good()))
	info, _ := r.a.status(ctx)
	raw, _ := json.Marshal(info)
	if !strings.Contains(string(raw), `"open_trades":[]`) || !strings.Contains(string(raw), `"pending_orders":[]`) {
		t.Errorf("empty lists, never null (a client counts them): %s", raw)
	}
}

// /market/info reads a few symbols' prices and deal-size rules, without changing anything; a symbol with no price says
// so on its own.
func TestMarket_PricesAndDealSizeRules(t *testing.T) {
	r := newRig(t, false)
	acct := r.account(t, "capital", "demo", good())
	ctx, _, err := r.a.resolve(context.Background(), acct)
	if err != nil {
		t.Fatal(err)
	}
	pb := r.brokers["a@b.co"]
	pb.DealingRules = map[string]broker.DealingRules{"EURUSD": {MinDealSize: 1000, MaxDealSize: 5_000_000}}
	body, _ := json.Marshal(map[string]any{"symbols": []string{"eurusd", "GBPJPY", "A", "B", "C", "D"}})
	ctx = webhook.WithAccountRequestBody(ctx, body)
	info, err := r.a.market(ctx)
	if err != nil {
		t.Fatal(err)
	}
	markets := info.(map[string]any)["markets"].(map[string]any)
	eur := markets["EURUSD"].(map[string]any)
	if eur["min_size"] != 1000.0 || eur["max_size"] != 5_000_000.0 {
		t.Errorf("the broker's deal-size rules: %v", eur)
	}
	if _, ok := eur["bid"]; !ok {
		t.Errorf("a price (0 from the paper broker): %v", eur)
	}
	if len(markets) != maxMarketSymbols {
		t.Errorf("at most %d symbols are read: %d", maxMarketSymbols, len(markets))
	}
	if pb.TradeCount() != 0 || pb.OrderCount() != 0 {
		t.Error("a read changes nothing")
	}
}
