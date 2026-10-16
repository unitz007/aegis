package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/unitz007/aegis"
	"github.com/unitz007/aegis/broker"
	"github.com/unitz007/aegis/broker/paper"
)

func spec(m map[string]string) json.RawMessage {
	b, _ := json.Marshal(m)
	return b
}

func good() map[string]string {
	return map[string]string{"provider": "capital", "env": "demo", "api_key": "k1", "email": "a@b.co", "password": "hunter2"}
}

func testAccounts(allowLive bool) (*accounts, *int) {
	a := newAccounts(aegis.Executor{MinConfidence: 45}, allowLive)
	built := 0
	a.newBroker = func(accountSpec) broker.Port { built++; return paper.New(5_000, nil) }
	return a, &built
}

func TestResolve_BuildsAnExecutorForTheAccount(t *testing.T) {
	a, _ := testAccounts(false)
	ctx, key, err := a.resolve(context.Background(), spec(good()))
	if err != nil || key == "" {
		t.Fatalf("resolve: key=%q err=%v", key, err)
	}
	e, ok := executorFrom(ctx)
	if !ok || e.Broker == nil || e.MinConfidence != 45 || e.BrokerName != "capital:demo" {
		t.Fatalf("executor missing or not built from the template: %+v ok=%v", e, ok)
	}
	if strings.Contains(key, "hunter2") || strings.Contains(key, "k1") {
		t.Errorf("account key must not contain credentials: %q", key)
	}
}

func TestResolve_ReusesTheBrokerAcrossRequests(t *testing.T) {
	a, built := testAccounts(false)
	ctx1, k1, _ := a.resolve(context.Background(), spec(good()))
	ctx2, k2, _ := a.resolve(context.Background(), spec(good()))
	e1, _ := executorFrom(ctx1)
	e2, _ := executorFrom(ctx2)
	if k1 != k2 || e1 != e2 || *built != 1 {
		t.Errorf("same account should reuse one broker: keys %q/%q built=%d", k1, k2, *built)
	}
}

func TestResolve_DifferentAccountsAreIsolated(t *testing.T) {
	a, _ := testAccounts(false)
	other := good()
	other["email"] = "other@b.co"
	c1, k1, _ := a.resolve(context.Background(), spec(good()))
	c2, k2, _ := a.resolve(context.Background(), spec(other))
	e1, _ := executorFrom(c1)
	e2, _ := executorFrom(c2)
	if k1 == k2 || e1 == e2 || e1.Broker == e2.Broker {
		t.Errorf("two accounts must never share a key or broker")
	}
}

func TestResolve_ChangedPasswordRebuildsTheBroker(t *testing.T) {
	a, built := testAccounts(false)
	_, k1, _ := a.resolve(context.Background(), spec(good()))
	changed := good()
	changed["password"] = "new-password"
	_, k2, _ := a.resolve(context.Background(), spec(changed))
	if k1 != k2 || *built != 2 {
		t.Errorf("a new password should reuse the key but rebuild the broker: built=%d", *built)
	}
}

func TestResolve_Rejections(t *testing.T) {
	a, built := testAccounts(false)
	cases := map[string]func(m map[string]string){
		"unsupported provider": func(m map[string]string) { m["provider"] = "other" },
		"bad env":              func(m map[string]string) { m["env"] = "staging" },
		"missing key":          func(m map[string]string) { m["api_key"] = "" },
		"missing email":        func(m map[string]string) { m["email"] = "" },
		"missing password":     func(m map[string]string) { m["password"] = "" },
		"live disabled":        func(m map[string]string) { m["env"] = "live" },
	}
	for name, mutate := range cases {
		m := good()
		mutate(m)
		if _, _, err := a.resolve(context.Background(), spec(m)); err == nil {
			t.Errorf("%s: expected a rejection", name)
		} else if strings.Contains(err.Error(), "hunter2") || strings.Contains(err.Error(), "a@b.co") {
			t.Errorf("%s: error leaked credentials: %v", name, err)
		}
	}
	if _, _, err := a.resolve(context.Background(), json.RawMessage(`"not an object"`)); err == nil {
		t.Errorf("non-object account should be rejected")
	}
	if *built != 0 {
		t.Errorf("rejected requests must not build a broker (built=%d)", *built)
	}
}

func TestResolve_LiveAllowedOnlyWhenEnabled(t *testing.T) {
	m := good()
	m["env"] = "live"
	a, _ := testAccounts(true)
	ctx, _, err := a.resolve(context.Background(), spec(m))
	if err != nil {
		t.Fatalf("live should be allowed when enabled: %v", err)
	}
	if e, _ := executorFrom(ctx); e.BrokerName != "capital:live" {
		t.Errorf("broker name %q", e.BrokerName)
	}
}

func TestResolve_CacheIsBounded(t *testing.T) {
	a, _ := testAccounts(false)
	a.maxSize = 3
	for i := 0; i < 10; i++ {
		m := good()
		m["email"] = string(rune('a'+i)) + "@b.co"
		if _, _, err := a.resolve(context.Background(), spec(m)); err != nil {
			t.Fatal(err)
		}
	}
	if len(a.cache) > 3 {
		t.Errorf("cache grew past its bound: %d", len(a.cache))
	}
}

func TestCheck_ReportsTheAccountWithoutTrading(t *testing.T) {
	a, _ := testAccounts(false)
	ctx, _, _ := a.resolve(context.Background(), spec(good()))
	info, err := a.check(ctx)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	m := info.(map[string]any)
	if m["env"] != "demo" || m["balance"] != 5000.0 {
		t.Errorf("unexpected account info: %v", m)
	}
	if _, err := a.check(context.Background()); err == nil {
		t.Errorf("check without an account in context must fail")
	}
}
