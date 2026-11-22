package ig

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/unitz007/aegis/broker"
)

// pretendIG is IG's REST API as the trading-ig client's recorded responses show it: a login with a spread bet account
// (current) and a CFD account, EUR/USD quoted in points (scaling factor 10000, contracts of 100,000), a closed USD/JPY.
type pretendIG struct {
	mu       sync.Mutex
	switched bool
	sessions int
	pos      map[string]map[string]any
	orders   map[string]map[string]any
	confirms map[string]map[string]any
	got      []map[string]any
	n        int
}

func (s *pretendIG) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	reply := func(code int, v any) {
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}
	if r.Header.Get("X-IG-API-KEY") != "key" {
		reply(403, map[string]any{"errorCode": "error.security.api-key-invalid"})
		return
	}
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	if r.URL.Path == "/session" && r.Method == http.MethodPost {
		if body["identifier"] != "ada" || body["password"] != "pw" {
			reply(401, map[string]any{"errorCode": "error.security.invalid-details"})
			return
		}
		s.sessions++
		w.Header().Set("CST", "cst1")
		w.Header().Set("X-SECURITY-TOKEN", "xst1")
		reply(200, map[string]any{"currentAccountId": "SB1", "currencyIsoCode": "GBP", "accountType": "SPREADBET",
			"accounts": []any{map[string]any{"accountId": "SB1", "accountType": "SPREADBET"}, map[string]any{"accountId": "CFD1", "accountType": "CFD"}}})
		return
	}
	if r.Header.Get("CST") != "cst1" || r.Header.Get("X-SECURITY-TOKEN") != "xst1" {
		reply(401, map[string]any{"errorCode": "error.security.client-token-invalid"})
		return
	}
	deleting := r.Header.Get("_method") == "DELETE"
	deal := func(typ string) {
		s.n++
		ref := "REF" + string(rune('0'+s.n))
		reply(200, map[string]any{"dealReference": ref})
		_ = typ
	}
	switch {
	case r.URL.Path == "/session" && r.Method == http.MethodPut:
		s.switched = body["accountId"] == "CFD1"
		reply(200, map[string]any{})
	case r.URL.Path == "/markets" && r.Method == http.MethodGet:
		q := r.URL.Query().Get("searchTerm")
		var ms []any
		switch q {
		case "EURUSD":
			ms = []any{map[string]any{"epic": "CS.D.EURUSD.TODAY.IP", "instrumentName": "EUR/USD", "expiry": "DFB"},
				map[string]any{"epic": "CS.D.EURUSD.CFD.IP", "instrumentName": "EUR/USD", "expiry": "-"}}
		case "USDJPY":
			ms = []any{map[string]any{"epic": "CS.D.USDJPY.CFD.IP", "instrumentName": "USD/JPY", "expiry": "-"}}
		}
		reply(200, map[string]any{"markets": ms})
	case strings.HasPrefix(r.URL.Path, "/markets/"):
		epic := strings.TrimPrefix(r.URL.Path, "/markets/")
		status, bid, offer, scale := "TRADEABLE", 10845.6, 10846.2, 10000.0
		if strings.Contains(epic, "USDJPY") {
			status, bid, offer, scale = "CLOSED", 15859.0, 15861.0, 100.0
		}
		reply(200, map[string]any{
			"instrument":   map[string]any{"epic": epic, "lotSize": 1.0, "contractSize": "100000", "marginFactor": 3.33, "marginFactorUnit": "PERCENTAGE", "currencies": []any{map[string]any{"code": "USD", "isDefault": true}}},
			"dealingRules": map[string]any{"minDealSize": map[string]any{"unit": "POINTS", "value": 0.5}},
			"snapshot":     map[string]any{"marketStatus": status, "bid": bid, "offer": offer, "scalingFactor": scale}})
	case r.URL.Path == "/positions/otc" && r.Method == http.MethodPost && !deleting:
		s.got = append(s.got, body)
		if strings.Contains(body["epic"].(string), "USDJPY") {
			s.n++
			ref := "REF" + string(rune('0'+s.n))
			s.confirms[ref] = map[string]any{"dealStatus": "REJECTED", "reason": "MARKET_CLOSED_WITH_EDITS"}
			reply(200, map[string]any{"dealReference": ref})
			return
		}
		s.n++
		ref := "REF" + string(rune('0'+s.n))
		id := "DEAL" + string(rune('0'+s.n))
		s.pos[id] = map[string]any{"position": map[string]any{"contractSize": 100000.0, "dealId": id, "size": body["size"], "direction": body["direction"], "level": 10846.2,
			"stopLevel": body["stopLevel"], "limitLevel": body["limitLevel"], "currency": "USD", "createdDateUTC": "2026-11-23T08:00:00"},
			"market": map[string]any{"epic": body["epic"], "instrumentName": "EUR/USD", "bid": 10850.0, "offer": 10850.6, "scalingFactor": 10000.0}}
		s.confirms[ref] = map[string]any{"dealStatus": "ACCEPTED", "dealId": "X" + id, "level": 10846.2, "affectedDeals": []any{map[string]any{"dealId": id, "status": "OPENED"}}}
		reply(200, map[string]any{"dealReference": ref})
	case r.URL.Path == "/positions/otc" && deleting:
		s.got = append(s.got, body)
		delete(s.pos, body["dealId"].(string))
		s.n++
		ref := "REF" + string(rune('0'+s.n))
		s.confirms[ref] = map[string]any{"dealStatus": "ACCEPTED", "dealId": body["dealId"]}
		reply(200, map[string]any{"dealReference": ref})
	case strings.HasPrefix(r.URL.Path, "/positions/otc/") && r.Method == http.MethodPut:
		id := strings.TrimPrefix(r.URL.Path, "/positions/otc/")
		p := s.pos[id]["position"].(map[string]any)
		p["stopLevel"], p["limitLevel"] = body["stopLevel"], body["limitLevel"]
		s.n++
		ref := "REF" + string(rune('0'+s.n))
		s.confirms[ref] = map[string]any{"dealStatus": "ACCEPTED", "dealId": id}
		reply(200, map[string]any{"dealReference": ref})
	case r.URL.Path == "/positions" && r.Method == http.MethodGet:
		var ps []any
		for _, p := range s.pos {
			ps = append(ps, p)
		}
		reply(200, map[string]any{"positions": ps})
	case r.URL.Path == "/workingorders/otc" && r.Method == http.MethodPost:
		s.got = append(s.got, body)
		s.n++
		ref := "REF" + string(rune('0'+s.n))
		id := "ORD" + string(rune('0'+s.n))
		level := body["level"].(float64)
		s.orders[id] = map[string]any{"workingOrderData": map[string]any{"dealId": id, "direction": body["direction"], "epic": body["epic"], "orderSize": body["size"],
			"orderLevel": level, "orderType": body["type"], "stopDistance": level - body["stopLevel"].(float64), "limitDistance": body["limitLevel"].(float64) - level},
			"marketData": map[string]any{"instrumentName": "EUR/USD", "lotSize": 1.0, "scalingFactor": 10000.0}}
		s.confirms[ref] = map[string]any{"dealStatus": "ACCEPTED", "dealId": id}
		reply(200, map[string]any{"dealReference": ref})
	case r.URL.Path == "/workingorders" && r.Method == http.MethodGet:
		var os []any
		for _, o := range s.orders {
			os = append(os, o)
		}
		reply(200, map[string]any{"workingOrders": os})
	case strings.HasPrefix(r.URL.Path, "/workingorders/otc/") && deleting:
		id := strings.TrimPrefix(r.URL.Path, "/workingorders/otc/")
		delete(s.orders, id)
		s.n++
		ref := "REF" + string(rune('0'+s.n))
		s.confirms[ref] = map[string]any{"dealStatus": "ACCEPTED", "dealId": id}
		reply(200, map[string]any{"dealReference": ref})
	case strings.HasPrefix(r.URL.Path, "/workingorders/otc/") && r.Method == http.MethodPut:
		id := strings.TrimPrefix(r.URL.Path, "/workingorders/otc/")
		d := s.orders[id]["workingOrderData"].(map[string]any)
		d["orderLevel"] = body["level"]
		d["stopDistance"] = body["level"].(float64) - body["stopLevel"].(float64)
		s.n++
		ref := "REF" + string(rune('0'+s.n))
		s.confirms[ref] = map[string]any{"dealStatus": "ACCEPTED", "dealId": id}
		reply(200, map[string]any{"dealReference": ref})
	case strings.HasPrefix(r.URL.Path, "/confirms/"):
		reply(200, s.confirms[strings.TrimPrefix(r.URL.Path, "/confirms/")])
	case r.URL.Path == "/accounts":
		reply(200, map[string]any{"accounts": []any{
			map[string]any{"accountId": "SB1", "currency": "GBP", "balance": map[string]any{"balance": 50.0}},
			map[string]any{"accountId": "CFD1", "currency": "USD", "balance": map[string]any{"balance": 10000.0, "deposit": 361.0, "profitLoss": 38.0, "available": 9677.0}}}})
	default:
		deal("")
	}
}

func pretend(t *testing.T) (*pretendIG, string) {
	t.Helper()
	s := &pretendIG{pos: map[string]map[string]any{}, orders: map[string]map[string]any{}, confirms: map[string]map[string]any{}}
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	env := "test-" + t.Name()
	Bases[env] = srv.URL
	t.Cleanup(func() { delete(Bases, env) })
	return s, env
}

// A person's IG login, traded on its CFD account: placed, read, changed and closed as approved, in units and real prices
// whatever IG's own scale; refusals as every broker says them.
func TestAdapter_TradesOnTheCFDAccount(t *testing.T) {
	ctx := context.Background()
	s, env := pretend(t)
	a, err := New(env, "key", "ada", "pw")
	if err != nil {
		t.Fatal(err)
	}
	bid, ask, err := a.LiveQuote(ctx, "EUR/USD")
	if err != nil || bid != 1.08456 || ask != 1.08462 || !s.switched {
		t.Fatalf("price in real terms, on the CFD account: %v %v %v switched=%v", bid, ask, err, s.switched)
	}
	r, err := a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "EURUSD", Units: 100000, OrderType: "market", StopLoss: 1.0835, TakeProfit: 1.0866})
	if err != nil || r.BrokerTradeID != "DEAL1" || r.FilledPrice != 1.08462 {
		t.Fatalf("market: %+v %v", r, err)
	}
	if g := s.got[0]; g["epic"] != "CS.D.EURUSD.CFD.IP" || g["size"] != 1.0 || g["stopLevel"] != 10835.0 || g["limitLevel"] != 10866.0 || g["orderType"] != "MARKET" || g["currencyCode"] != "USD" {
		t.Errorf("as sent: %v", g)
	}
	trades, err := a.OpenTrades(ctx)
	if err != nil || len(trades) != 1 || trades[0].Units != 100000 || trades[0].OpenPrice != 1.08462 || trades[0].StopLoss != 1.0835 || trades[0].Symbol != "EURUSD" ||
		trades[0].CurrentPrice != 1.085 || trades[0].UnrealizedPnL != 38 {
		t.Fatalf("read: %+v %v", trades, err)
	}
	acct, err := a.Account(ctx)
	if err != nil || acct.Balance != 10000 || acct.Currency != "USD" || acct.MarginUsed != 361 || acct.NAV != 10038 {
		t.Errorf("the CFD account: %+v %v", acct, err)
	}
	if err := a.AmendTrade(ctx, "DEAL1", 1.084, 0); err != nil {
		t.Fatal(err)
	}
	if p := s.pos["DEAL1"]["position"].(map[string]any); p["stopLevel"] != 10840.0 || p["limitLevel"] != 10866.0 {
		t.Errorf("stop moved, target kept: %v", p)
	}
	if err := a.CloseTrade(ctx, "DEAL1"); err != nil || len(s.pos) != 0 {
		t.Fatalf("closed: %v", err)
	}
	if g := s.got[len(s.got)-1]; g["direction"] != "SELL" || g["size"] != 1.0 {
		t.Errorf("closed with the opposite side: %v", g)
	}
	// an entry order: below the market a limit, waiting; its stop and target read back as levels
	if _, err := a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "EURUSD", Units: 50000, OrderType: "limit", EntryPrice: 1.08, StopLoss: 1.075, TakeProfit: 1.09}); err != nil {
		t.Fatal(err)
	}
	orders, _ := a.OpenOrders(ctx)
	if len(orders) != 1 || orders[0].OrderType != "LIMIT" || orders[0].Level != 1.08 || orders[0].StopLoss != 1.075 || orders[0].TakeProfit != 1.09 || orders[0].Units != 50000 {
		t.Fatalf("order: %+v", orders)
	}
	if err := a.AmendOrder(ctx, orders[0].BrokerOrderID, 0, 1.074, 0); err != nil {
		t.Fatal(err)
	}
	if err := a.CancelOrder(ctx, orders[0].BrokerOrderID); err != nil || len(s.orders) != 0 {
		t.Fatalf("cancelled: %v", err)
	}
	// refusals
	_, err = a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "USDJPY", Units: 100000})
	if rf, ok := broker.AsRefused(err); !ok || rf.Kind != broker.RefusedMarketClosed {
		t.Errorf("closed: %v", err)
	}
	_, err = a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "EURUSD", Units: 1000})
	if rf, ok := broker.AsRefused(err); !ok || rf.Kind != broker.RefusedSizeTooSmall || rf.Limit != 50000 {
		t.Errorf("below the smallest deal (half a contract), never raised: %v", err)
	}
	if h, _ := a.MarketHours(ctx, "USDJPY"); h.Open {
		t.Error("USD/JPY is closed")
	}
	if rules, _ := a.MarketDealingRules(ctx, "EURUSD"); rules.MinDealSize != 50000 || rules.MarginPercent != 3.33 {
		t.Errorf("rules: %+v", rules)
	}
	wrong, _ := New(env, "key", "ada", "nope")
	if _, err := wrong.Account(ctx); err == nil {
		t.Error("wrong password")
	}
}
