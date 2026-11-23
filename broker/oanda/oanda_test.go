package oanda

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/unitz007/aegis/broker"
)

// pretendOANDA is OANDA's v20 API as its reference shows it: account 101-1, EUR_USD (whole units, 5 decimals) and
// USD_JPY (halted), decimals as strings.
type pretendOANDA struct {
	mu     sync.Mutex
	trades map[string]map[string]any
	orders map[string]map[string]any
	got    []map[string]any
	n      int
}

func (s *pretendOANDA) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	reply := func(code int, v any) {
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}
	if r.Header.Get("Authorization") != "Bearer tok" {
		reply(401, map[string]any{"errorMessage": "Insufficient authorization to perform request."})
		return
	}
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	base := "/v3/accounts/101-1"
	if !strings.HasPrefix(r.URL.Path, base) {
		reply(404, map[string]any{"errorMessage": "The Account specified does not exist."})
		return
	}
	p := strings.TrimPrefix(r.URL.Path, base)
	next := func() string { s.n++; return strconv.Itoa(s.n) }
	switch {
	case p == "/instruments":
		reply(200, map[string]any{"instruments": []any{
			map[string]any{"name": "EUR_USD", "displayPrecision": 5, "tradeUnitsPrecision": 0, "minimumTradeSize": "1", "maximumOrderUnits": "100000000", "marginRate": "0.0333"},
			map[string]any{"name": "USD_JPY", "displayPrecision": 3, "tradeUnitsPrecision": 0, "minimumTradeSize": "1", "maximumOrderUnits": "100000000", "marginRate": "0.04"},
			map[string]any{"name": "XAU_USD", "displayPrecision": 3, "tradeUnitsPrecision": 0, "minimumTradeSize": "10", "maximumOrderUnits": "5000", "marginRate": "0.05"}}})
	case p == "/pricing":
		var ps []any
		for _, n := range strings.Split(r.URL.Query().Get("instruments"), ",") {
			switch n {
			case "EUR_USD":
				ps = append(ps, map[string]any{"instrument": n, "tradeable": true, "bids": []any{map[string]any{"price": "1.08456"}}, "asks": []any{map[string]any{"price": "1.08462"}}})
			case "USD_JPY":
				ps = append(ps, map[string]any{"instrument": n, "tradeable": false, "bids": []any{map[string]any{"price": "158.590"}}, "asks": []any{map[string]any{"price": "158.610"}}})
			}
		}
		reply(200, map[string]any{"prices": ps})
	case p == "/summary":
		reply(200, map[string]any{"account": map[string]any{"id": "101-1", "currency": "USD", "balance": "10000.0000", "NAV": "10038.0000",
			"unrealizedPL": "38.0000", "marginUsed": "361.0000", "marginAvailable": "9677.0000"}})
	case p == "/orders" && r.Method == http.MethodPost:
		o := body["order"].(map[string]any)
		s.got = append(s.got, o)
		if o["instrument"] == "USD_JPY" {
			reply(201, map[string]any{"orderCreateTransaction": map[string]any{"id": next()}, "orderCancelTransaction": map[string]any{"reason": "MARKET_HALTED"}})
			return
		}
		if o["stopLossOnFill"] != nil && o["stopLossOnFill"].(map[string]any)["price"] == "1.20000" {
			reply(400, map[string]any{"errorCode": "STOP_LOSS_ON_FILL_LOSS", "errorMessage": "stop loss on fill would lose", "orderRejectTransaction": map[string]any{"rejectReason": "STOP_LOSS_ON_FILL_LOSS"}})
			return
		}
		id := next()
		if o["type"] == "MARKET" {
			tid := next()
			t := map[string]any{"id": tid, "instrument": o["instrument"], "price": "1.08462", "openTime": "2026-11-23T08:00:00.000000000Z", "currentUnits": o["units"], "unrealizedPL": "38.0000"}
			if sl, ok := o["stopLossOnFill"].(map[string]any); ok {
				t["stopLossOrder"] = map[string]any{"price": sl["price"]}
			}
			if tp, ok := o["takeProfitOnFill"].(map[string]any); ok {
				t["takeProfitOrder"] = map[string]any{"price": tp["price"]}
			}
			s.trades[tid] = t
			reply(201, map[string]any{"orderCreateTransaction": map[string]any{"id": id}, "orderFillTransaction": map[string]any{"id": next(), "price": "1.08462",
				"tradeOpened": map[string]any{"tradeID": tid, "price": "1.08462"}}})
			return
		}
		s.orders[id] = map[string]any{"id": id, "type": o["type"], "instrument": o["instrument"], "units": o["units"], "price": o["price"], "timeInForce": "GTC",
			"createTime": "2026-11-23T09:00:00Z", "stopLossOnFill": o["stopLossOnFill"], "takeProfitOnFill": o["takeProfitOnFill"]}
		reply(201, map[string]any{"orderCreateTransaction": map[string]any{"id": id}})
	case p == "/openTrades":
		var ts []any
		for _, t := range s.trades {
			ts = append(ts, t)
		}
		reply(200, map[string]any{"trades": ts})
	case p == "/pendingOrders":
		os := []any{map[string]any{"id": "99", "type": "STOP_LOSS", "tradeID": "x", "price": "1.07"}} // a trade's own stop: not an entry
		for _, o := range s.orders {
			os = append(os, o)
		}
		reply(200, map[string]any{"orders": os})
	case strings.HasSuffix(p, "/close") && strings.HasPrefix(p, "/trades/"):
		s.got = append(s.got, body)
		delete(s.trades, strings.TrimSuffix(strings.TrimPrefix(p, "/trades/"), "/close"))
		reply(200, map[string]any{"orderFillTransaction": map[string]any{"id": next()}})
	case strings.HasSuffix(p, "/orders") && strings.HasPrefix(p, "/trades/"):
		s.got = append(s.got, body)
		t := s.trades[strings.TrimSuffix(strings.TrimPrefix(p, "/trades/"), "/orders")]
		if sl, ok := body["stopLoss"].(map[string]any); ok {
			t["stopLossOrder"] = map[string]any{"price": sl["price"]}
		}
		if tp, ok := body["takeProfit"].(map[string]any); ok {
			t["takeProfitOrder"] = map[string]any{"price": tp["price"]}
		}
		reply(200, map[string]any{})
	case strings.HasSuffix(p, "/cancel") && strings.HasPrefix(p, "/orders/"):
		delete(s.orders, strings.TrimSuffix(strings.TrimPrefix(p, "/orders/"), "/cancel"))
		reply(200, map[string]any{"orderCancelTransaction": map[string]any{"reason": "CLIENT_REQUEST"}})
	case strings.HasPrefix(p, "/orders/") && r.Method == http.MethodPut:
		old := strings.TrimPrefix(p, "/orders/")
		o := body["order"].(map[string]any)
		s.got = append(s.got, o)
		delete(s.orders, old)
		id := next()
		o["id"] = id
		s.orders[id] = o
		reply(201, map[string]any{"orderCancelTransaction": map[string]any{"replacedByOrderID": id}, "orderCreateTransaction": map[string]any{"id": id}})
	default:
		reply(404, map[string]any{"errorMessage": "no such path " + p})
	}
}

func pretend(t *testing.T) (*pretendOANDA, string) {
	t.Helper()
	s := &pretendOANDA{trades: map[string]map[string]any{}, orders: map[string]map[string]any{}}
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	env := "test-" + t.Name()
	Bases[env] = srv.URL
	t.Cleanup(func() { delete(Bases, env) })
	return s, env
}

// An OANDA account: placed, read, changed and closed as approved, never netted against an opposite trade; refusals as
// every broker says them.
func TestAdapter_TradesAsApproved(t *testing.T) {
	ctx := context.Background()
	s, env := pretend(t)
	a, err := New(env, "101-1", "tok")
	if err != nil {
		t.Fatal(err)
	}
	bid, ask, err := a.LiveQuote(ctx, "EUR/USD")
	if err != nil || bid != 1.08456 || ask != 1.08462 {
		t.Fatalf("price: %v %v %v", bid, ask, err)
	}
	r, err := a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "EURUSD", Units: -10000, StopLoss: 1.09, TakeProfit: 1.08})
	if err != nil || r.BrokerTradeID == "" || r.FilledPrice != 1.08462 {
		t.Fatalf("market: %+v %v", r, err)
	}
	g := s.got[0]
	if g["type"] != "MARKET" || g["units"] != "-10000" || g["instrument"] != "EUR_USD" || g["positionFill"] != "OPEN_ONLY" || g["timeInForce"] != "FOK" ||
		g["stopLossOnFill"].(map[string]any)["price"] != "1.09000" || g["takeProfitOnFill"].(map[string]any)["price"] != "1.08000" {
		t.Errorf("as sent: %v", g)
	}
	trades, err := a.OpenTrades(ctx)
	if err != nil || len(trades) != 1 || trades[0].Units != -10000 || trades[0].Symbol != "EURUSD" || trades[0].StopLoss != 1.09 || trades[0].TakeProfit != 1.08 ||
		trades[0].CurrentPrice != 1.08462 || trades[0].UnrealizedPnL != 38 || trades[0].PnLCurrency != "USD" || trades[0].OpenedAt.IsZero() {
		t.Fatalf("read: %+v %v", trades, err)
	}
	acct, err := a.Account(ctx)
	if err != nil || acct.Balance != 10000 || acct.NAV != 10038 || acct.MarginAvailable != 9677 || acct.Currency != "USD" {
		t.Errorf("account: %+v %v", acct, err)
	}
	if err := a.AmendTrade(ctx, r.BrokerTradeID, 1.0895, 0); err != nil {
		t.Fatal(err)
	}
	if g := s.got[len(s.got)-1]; g["takeProfit"] != nil || g["stopLoss"].(map[string]any)["price"] != "1.08950" {
		t.Errorf("only the stop moved: %v", g)
	}
	if err := a.CloseTrade(ctx, r.BrokerTradeID); err != nil || len(s.trades) != 0 {
		t.Fatalf("closed: %v", err)
	}
	if g := s.got[len(s.got)-1]; g["units"] != "ALL" {
		t.Errorf("closed whole: %v", g)
	}
	// entry orders: below the ask a limit, above it a stop; their stop and target on fill read back
	if _, err := a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "EURUSD", Units: 5000, OrderType: "limit", EntryPrice: 1.08, StopLoss: 1.075, TakeProfit: 1.09}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "EURUSD", Units: 5000, OrderType: "limit", EntryPrice: 1.09, StopLoss: 1.085}); err != nil {
		t.Fatal(err)
	}
	orders, _ := a.OpenOrders(ctx)
	if len(orders) != 2 {
		t.Fatalf("two entries, the trade's own stop left out: %+v", orders)
	}
	var lim broker.OpenOrder
	for _, o := range orders {
		if o.Level == 1.08 {
			lim = o
		} else if o.OrderType != "STOP" {
			t.Errorf("above the market, a stop entry: %+v", o)
		}
	}
	if lim.OrderType != "LIMIT" || lim.StopLoss != 1.075 || lim.TakeProfit != 1.09 || lim.Units != 5000 || lim.Direction != "BUY" {
		t.Fatalf("limit: %+v", lim)
	}
	if err := a.AmendOrder(ctx, lim.BrokerOrderID, 1.079, 0, 0); err != nil {
		t.Fatal(err)
	}
	if g := s.got[len(s.got)-1]; g["price"] != "1.07900" || g["stopLossOnFill"].(map[string]any)["price"] != "1.07500" || g["positionFill"] != "OPEN_ONLY" {
		t.Errorf("replaced, its stop kept: %v", g)
	}
	orders, _ = a.OpenOrders(ctx)
	for _, o := range orders {
		if err := a.CancelOrder(ctx, o.BrokerOrderID); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.orders) != 0 {
		t.Fatalf("cancelled: %v", s.orders)
	}
	// refusals
	_, err = a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "USDJPY", Units: 1000})
	if rf, ok := broker.AsRefused(err); !ok || rf.Kind != broker.RefusedMarketClosed {
		t.Errorf("halted: %v", err)
	}
	_, err = a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "EURUSD", Units: 1000, StopLoss: 1.2})
	if rf, ok := broker.AsRefused(err); !ok || rf.Kind != broker.RefusedLevel {
		t.Errorf("a stop on the wrong side: %v", err)
	}
	_, err = a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "XAUUSD", Units: 5})
	if rf, ok := broker.AsRefused(err); !ok || rf.Kind != broker.RefusedSizeTooSmall || rf.Limit != 10 {
		t.Errorf("below the smallest trade, never raised: %v", err)
	}
	_, err = a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "XAUUSD", Units: 9000})
	if rf, ok := broker.AsRefused(err); !ok || rf.Kind != broker.RefusedSizeTooLarge {
		t.Errorf("above the largest order: %v", err)
	}
	if h, _ := a.MarketHours(ctx, "USD/JPY"); h.Open {
		t.Error("USD/JPY is halted")
	}
	if h, _ := a.MarketHours(ctx, "EURUSD"); !h.Open {
		t.Error("EUR/USD trades")
	}
	if rules, _ := a.MarketDealingRules(ctx, "XAU_USD"); rules.MinDealSize != 10 || rules.MaxDealSize != 5000 || rules.MarginPercent != 5 {
		t.Errorf("rules: %+v", rules)
	}
	wrong, _ := New(env, "101-1", "nope")
	if _, err := wrong.Account(ctx); err == nil {
		t.Error("a token OANDA refuses")
	} else if _, ok := broker.AsRefused(err); !ok {
		t.Errorf("a refused token is a refusal: %v", err)
	}
	if _, err := a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "NOPE", Units: 1}); err == nil {
		t.Error("an instrument the account doesn't have")
	}
}

func TestSpec(t *testing.T) {
	r := broker.NewRegistry()
	if err := r.Register(Spec()); err != nil {
		t.Fatal(err)
	}
	if _, err := Spec().New("demo", broker.Credentials{"account_id": "101-1", "token": "t"}); err != nil {
		t.Fatal(err)
	}
	if _, err := Spec().New("demo", broker.Credentials{"account_id": "101-1"}); err == nil {
		t.Error("a token is needed")
	}
}
