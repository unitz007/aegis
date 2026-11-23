package alpaca

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

// pretendAlpaca is Alpaca's Trading and market data APIs as the docs show them: AAPL (fractionable), TSLA and BTC/USD
// (crypto, 0.0001 at least), decimals as strings, a market clock that can be shut, brackets with their legs.
type pretendAlpaca struct {
	mu     sync.Mutex
	open   bool
	pos    map[string]map[string]any
	orders map[string]map[string]any
	got    []map[string]any
	paths  []string
	n      int
}

func (s *pretendAlpaca) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	reply := func(code int, v any) {
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}
	if r.Header.Get("APCA-API-KEY-ID") != "PKTEST" || r.Header.Get("APCA-API-SECRET-KEY") != "sec" {
		reply(401, map[string]any{"message": "request is not authorized"})
		return
	}
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	s.paths = append(s.paths, r.Method+" "+r.URL.Path)
	next := func() string { s.n++; return "o" + strconv.Itoa(s.n) }
	p := r.URL.Path
	switch {
	case strings.HasPrefix(p, "/v2/assets/"):
		switch strings.TrimPrefix(p, "/v2/assets/") {
		case "AAPL":
			reply(200, map[string]any{"symbol": "AAPL", "class": "us_equity", "tradable": true, "fractionable": true})
		case "TSLA":
			reply(200, map[string]any{"symbol": "TSLA", "class": "us_equity", "tradable": true, "fractionable": false})
		case "BTCUSD", "BTC/USD":
			reply(200, map[string]any{"symbol": "BTC/USD", "class": "crypto", "tradable": true, "fractionable": true, "min_order_size": "0.0001", "min_trade_increment": "0.000000001"})
		default:
			reply(404, map[string]any{"code": 40410000, "message": "asset not found"})
		}
	case p == "/v2/clock":
		reply(200, map[string]any{"is_open": s.open, "next_open": "2026-11-24T14:30:00Z"})
	case strings.HasPrefix(p, "/v2/stocks/"):
		reply(200, map[string]any{"symbol": "AAPL", "quote": map[string]any{"bp": 230.10, "ap": 230.20}})
	case p == "/v1beta3/crypto/us/latest/quotes":
		reply(200, map[string]any{"quotes": map[string]any{"BTC/USD": map[string]any{"bp": 90000.0, "ap": 90010.0}}})
	case p == "/v2/account":
		reply(200, map[string]any{"id": "uuid", "account_number": "PA3ABC", "currency": "USD", "equity": "100050", "buying_power": "200000", "initial_margin": "11505"})
	case p == "/v2/orders" && r.Method == http.MethodPost:
		s.got = append(s.got, body)
		if body["qty"] == "100000" {
			reply(403, map[string]any{"code": 40310000, "message": "insufficient buying power"})
			return
		}
		id := next()
		o := map[string]any{"id": id, "symbol": body["symbol"], "qty": body["qty"], "side": body["side"], "type": body["type"], "order_class": body["order_class"],
			"limit_price": body["limit_price"], "stop_price": body["stop_price"], "status": "new", "created_at": "2026-11-23T15:00:00Z"}
		var legs []any
		exit := "sell"
		if body["side"] == "sell" {
			exit = "buy"
		}
		if tp, ok := body["take_profit"].(map[string]any); ok {
			legs = append(legs, map[string]any{"id": next(), "symbol": body["symbol"], "qty": body["qty"], "side": exit, "type": "limit", "limit_price": tp["limit_price"], "status": "held"})
		}
		if sl, ok := body["stop_loss"].(map[string]any); ok {
			legs = append(legs, map[string]any{"id": next(), "symbol": body["symbol"], "qty": body["qty"], "side": exit, "type": "stop", "stop_price": sl["stop_price"], "status": "held"})
		}
		if body["type"] == "market" && body["order_class"] != "oco" {
			// fills at once: a position, and its legs come alive as orders of their own
			sym := strings.ReplaceAll(body["symbol"].(string), "/", "")
			side := "long"
			if body["side"] == "sell" {
				side = "short"
			}
			s.pos[sym] = map[string]any{"symbol": sym, "asset_class": "us_equity", "qty": body["qty"], "side": side, "avg_entry_price": "230.20", "current_price": "231.00", "unrealized_pl": "8.00"}
			for _, l := range legs {
				lm := l.(map[string]any)
				lm["status"] = "new"
				s.orders[lm["id"].(string)] = lm
			}
			reply(200, map[string]any{"id": id, "status": "filled", "filled_avg_price": "230.20"})
			return
		}
		if body["order_class"] == "oco" {
			legs = []any{map[string]any{"id": next(), "symbol": body["symbol"], "qty": body["qty"], "side": body["side"], "type": "stop", "stop_price": body["stop_loss"].(map[string]any)["stop_price"], "status": "new"}}
			o["limit_price"] = body["take_profit"].(map[string]any)["limit_price"]
		}
		o["legs"] = legs
		s.orders[id] = o
		reply(200, o)
	case p == "/v2/orders" && r.Method == http.MethodGet:
		if r.URL.Query().Get("nested") != "true" || r.URL.Query().Get("status") != "open" {
			reply(400, map[string]any{"message": "want nested open"})
			return
		}
		var os []any
		for _, o := range s.orders {
			os = append(os, o)
		}
		reply(200, os)
	case strings.HasPrefix(p, "/v2/orders/") && r.Method == http.MethodPatch:
		id := strings.TrimPrefix(p, "/v2/orders/")
		s.got = append(s.got, body)
		o := s.find(id)
		if o == nil {
			reply(404, map[string]any{"message": "order not found"})
			return
		}
		for k, v := range body {
			o[k] = v
		}
		reply(200, o)
	case strings.HasPrefix(p, "/v2/orders/") && r.Method == http.MethodDelete:
		delete(s.orders, strings.TrimPrefix(p, "/v2/orders/"))
		w.WriteHeader(204)
	case p == "/v2/positions":
		var ps []any
		for _, x := range s.pos {
			ps = append(ps, x)
		}
		reply(200, ps)
	case strings.HasPrefix(p, "/v2/positions/") && r.Method == http.MethodDelete:
		sym := strings.TrimPrefix(p, "/v2/positions/")
		for _, o := range s.orders {
			if strings.ReplaceAll(o["symbol"].(string), "/", "") == sym {
				reply(403, map[string]any{"code": 40310000, "message": "insufficient qty available for order (requested: 10, available: 0)"})
				return
			}
		}
		delete(s.pos, sym)
		reply(200, map[string]any{"id": next(), "status": "accepted"})
	default:
		reply(404, map[string]any{"message": "no route " + p})
	}
}

func (s *pretendAlpaca) find(id string) map[string]any {
	if o := s.orders[id]; o != nil {
		return o
	}
	for _, o := range s.orders {
		if legs, ok := o["legs"].([]any); ok {
			for _, l := range legs {
				if lm := l.(map[string]any); lm["id"] == id {
					return lm
				}
			}
		}
	}
	return nil
}

func pretend(t *testing.T) (*pretendAlpaca, string) {
	t.Helper()
	s := &pretendAlpaca{open: true, pos: map[string]map[string]any{}, orders: map[string]map[string]any{}}
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	env := "test-" + t.Name()
	Bases[env], DataBases[env] = srv.URL, srv.URL
	t.Cleanup(func() { delete(Bases, env); delete(DataBases, env) })
	return s, env
}

// An Alpaca account: a stock bought with its bracket, read with the stop and target its legs hold, the stop moved, the
// position closed (its legs taken off first); entries with their legs; crypto without a stop; refusals as every broker
// says them.
func TestAdapter_StocksAndCrypto(t *testing.T) {
	ctx := context.Background()
	s, env := pretend(t)
	a, err := New(env, "PKTEST", "sec")
	if err != nil {
		t.Fatal(err)
	}
	bid, ask, err := a.LiveQuote(ctx, "AAPL")
	if err != nil || bid != 230.10 || ask != 230.20 {
		t.Fatalf("price: %v %v %v", bid, ask, err)
	}
	r, err := a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "AAPL", Units: 10, StopLoss: 225, TakeProfit: 240})
	if err != nil || r.BrokerTradeID != "AAPL" || r.FilledPrice != 230.20 {
		t.Fatalf("market: %+v %v", r, err)
	}
	g := s.got[0]
	if g["type"] != "market" || g["qty"] != "10" || g["side"] != "buy" || g["order_class"] != "bracket" || g["time_in_force"] != "gtc" ||
		g["take_profit"].(map[string]any)["limit_price"] != "240" || g["stop_loss"].(map[string]any)["stop_price"] != "225" {
		t.Errorf("as sent: %v", g)
	}
	trades, err := a.OpenTrades(ctx)
	if err != nil || len(trades) != 1 || trades[0].Units != 10 || trades[0].StopLoss != 225 || trades[0].TakeProfit != 240 || trades[0].OpenPrice != 230.20 || trades[0].UnrealizedPnL != 8 {
		t.Fatalf("read with its legs: %+v %v", trades, err)
	}
	if orders, _ := a.OpenOrders(ctx); len(orders) != 0 {
		t.Errorf("a position's legs aren't entries: %+v", orders)
	}
	acct, err := a.Account(ctx)
	if err != nil || acct.NAV != 100050 || acct.Balance != 100042 || acct.MarginAvailable != 200000 || acct.AccountID != "PA3ABC" {
		t.Errorf("account: %+v %v", acct, err)
	}
	if err := a.AmendTrade(ctx, "AAPL", 228, 0); err != nil {
		t.Fatal(err)
	}
	if g := s.got[len(s.got)-1]; g["stop_price"] != "228" || len(g) != 1 {
		t.Errorf("the stop leg replaced, nothing else: %v", g)
	}
	if err := a.CloseTrade(ctx, "AAPL"); err != nil || len(s.pos) != 0 || len(s.orders) != 0 {
		t.Fatalf("closed, legs first: %v pos=%v orders=%v", err, s.pos, s.orders)
	}
	// an entry: a buy below the market is a limit, with its bracket; its legs say its stop and target
	if _, err := a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "AAPL", Units: 5, OrderType: "limit", EntryPrice: 220, StopLoss: 215, TakeProfit: 235}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "AAPL", Units: 5, OrderType: "limit", EntryPrice: 235, StopLoss: 230}); err != nil {
		t.Fatal(err)
	}
	if g := s.got[len(s.got)-1]; g["type"] != "stop" || g["stop_price"] != "235" || g["order_class"] != "oto" {
		t.Errorf("above the market a stop entry, one leg as OTO: %v", g)
	}
	orders, _ := a.OpenOrders(ctx)
	if len(orders) != 2 {
		t.Fatalf("two entries: %+v", orders)
	}
	var lim broker.OpenOrder
	for _, o := range orders {
		if o.OrderType == "LIMIT" {
			lim = o
		}
	}
	if lim.Level != 220 || lim.StopLoss != 215 || lim.TakeProfit != 235 || lim.Units != 5 || lim.Direction != "BUY" {
		t.Fatalf("limit with its legs: %+v", lim)
	}
	if err := a.AmendOrder(ctx, lim.BrokerOrderID, 221, 216, 0); err != nil {
		t.Fatal(err)
	}
	for _, o := range orders {
		if err := a.CancelOrder(ctx, o.BrokerOrderID); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.orders) != 0 {
		t.Fatalf("cancelled: %v", s.orders)
	}
	// crypto: a market buy without levels; a stop is refused, never dropped; no going short
	if _, err := a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "BTCUSD", Units: 1, Quantity: 0.0005}); err != nil {
		t.Fatal(err)
	}
	if g := s.got[len(s.got)-1]; g["symbol"] != "BTC/USD" || g["qty"] != "0.0005" || g["order_class"] != nil {
		t.Errorf("crypto as sent: %v", g)
	}
	_, err = a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "BTC/USD", Units: 1, Quantity: 0.001, StopLoss: 85000})
	if rf, ok := broker.AsRefused(err); !ok || rf.Kind != broker.RefusedLevel {
		t.Errorf("crypto with a stop: %v", err)
	}
	_, err = a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "BTCUSD", Units: -1, Quantity: 0.01})
	if rf, ok := broker.AsRefused(err); !ok || rf.BrokerSaid != "CRYPTO_NO_SHORT" {
		t.Errorf("selling more crypto than held: %v", err)
	}
	_, err = a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "BTCUSD", Units: 1, Quantity: 0.00005})
	if rf, ok := broker.AsRefused(err); !ok || rf.Kind != broker.RefusedSizeTooSmall || rf.Limit != 0.0001 {
		t.Errorf("below crypto's smallest order: %v", err)
	}
	// stock refusals
	_, err = a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "AAPL", Units: 1, Quantity: 0.5, StopLoss: 220})
	if rf, ok := broker.AsRefused(err); !ok || rf.Kind != broker.RefusedSizeTooSmall {
		t.Errorf("a stop on a fraction of a share: %v", err)
	}
	_, err = a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "TSLA", Units: 100000})
	if rf, ok := broker.AsRefused(err); !ok || rf.Kind != broker.RefusedNotEnoughMargin {
		t.Errorf("buying power: %v", err)
	}
	s.open = false
	_, err = a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "TSLA", Units: 1})
	if rf, ok := broker.AsRefused(err); !ok || rf.Kind != broker.RefusedMarketClosed {
		t.Errorf("a market order while shut: %v", err)
	}
	if h, _ := a.MarketHours(ctx, "TSLA"); h.Open || h.NextOpen.IsZero() {
		t.Errorf("shut, with when it opens: %+v", h)
	}
	if h, _ := a.MarketHours(ctx, "BTC/USD"); !h.Open {
		t.Error("crypto always trades")
	}
	if rules, _ := a.MarketDealingRules(ctx, "TSLA"); rules.MinDealSize != 1 || rules.MarginPercent != 100 {
		t.Errorf("rules: %+v", rules)
	}
	if _, _, err := a.LiveQuote(ctx, "NOPE"); err == nil {
		t.Error("an asset Alpaca hasn't")
	}
	wrong, _ := New(env, "PKTEST", "nope")
	if _, err := wrong.Account(ctx); err == nil {
		t.Error("a key Alpaca refuses")
	}
}

func TestSpec(t *testing.T) {
	r := broker.NewRegistry()
	if err := r.Register(Spec()); err != nil {
		t.Fatal(err)
	}
	if _, err := Spec().New("live", broker.Credentials{"key_id": "AK", "secret_key": "s"}); err != nil {
		t.Fatal(err)
	}
}
