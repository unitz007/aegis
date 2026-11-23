package binance

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/unitz007/aegis/broker"
)

// pretendBinance is Binance's Spot API as its docs show it: signed requests checked, BTCUSDT (STOP_LOSS allowed),
// ETHUSDT (only STOP_LOSS_LIMIT), DOGEUSDT halted, no BTCUSD; 1,000 USDT and 0.01 BTC held.
type pretendBinance struct {
	mu        sync.Mutex
	skewOnce  bool
	balances  map[string]float64
	orders    map[int64]url.Values // open orders by id
	lists     map[int64][]int64
	listReq   map[int64]url.Values
	got       []url.Values
	paths     []string
	n         int64
	nextList  int64
	syncAsked bool
}

func (s *pretendBinance) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	reply := func(code int, v any) {
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}
	q := r.URL.Query()
	p := r.URL.Path
	s.paths = append(s.paths, r.Method+" "+p)
	if sig := q.Get("signature"); sig != "" {
		if r.Header.Get("X-MBX-APIKEY") != "key" {
			reply(401, map[string]any{"code": -2015, "msg": "Invalid API-key, IP, or permissions for action."})
			return
		}
		raw := r.URL.RawQuery
		i := strings.LastIndex(raw, "&signature=")
		m := hmac.New(sha256.New, []byte("sec"))
		m.Write([]byte(raw[:i]))
		if hex.EncodeToString(m.Sum(nil)) != sig || q.Get("timestamp") == "" {
			reply(400, map[string]any{"code": -1022, "msg": "Signature for this request is not valid."})
			return
		}
		if s.skewOnce {
			s.skewOnce = false
			reply(400, map[string]any{"code": -1021, "msg": "Timestamp for this request is outside of the recvWindow."})
			return
		}
	}
	order := func(v url.Values, list int64) int64 {
		s.n++
		v.Set("orderListId", strconv.FormatInt(list, 10))
		s.orders[s.n] = v
		return s.n
	}
	switch {
	case p == "/api/v3/time":
		s.syncAsked = true
		reply(200, map[string]any{"serverTime": 1764000000000})
	case p == "/api/v3/exchangeInfo":
		sym := q.Get("symbol")
		filters := []any{map[string]any{"filterType": "LOT_SIZE", "minQty": "0.00001000", "maxQty": "9000.00000000", "stepSize": "0.00001000"},
			map[string]any{"filterType": "PRICE_FILTER", "tickSize": "0.01000000"}, map[string]any{"filterType": "NOTIONAL", "minNotional": "5.00000000"}}
		var info map[string]any
		switch sym {
		case "BTCUSDT":
			info = map[string]any{"symbol": sym, "status": "TRADING", "baseAsset": "BTC", "quoteAsset": "USDT", "orderTypes": []string{"LIMIT", "LIMIT_MAKER", "MARKET", "STOP_LOSS", "STOP_LOSS_LIMIT"}, "filters": filters}
		case "ETHUSDT":
			info = map[string]any{"symbol": sym, "status": "TRADING", "baseAsset": "ETH", "quoteAsset": "USDT", "orderTypes": []string{"LIMIT", "LIMIT_MAKER", "MARKET", "STOP_LOSS_LIMIT"}, "filters": filters}
		case "DOGEUSDT":
			info = map[string]any{"symbol": sym, "status": "BREAK", "baseAsset": "DOGE", "quoteAsset": "USDT", "filters": filters}
		default:
			reply(400, map[string]any{"code": -1121, "msg": "Invalid symbol."})
			return
		}
		reply(200, map[string]any{"symbols": []any{info}})
	case p == "/api/v3/ticker/bookTicker":
		switch q.Get("symbol") {
		case "BTCUSDT":
			reply(200, map[string]any{"bidPrice": "90000.00", "askPrice": "90010.00"})
		default:
			reply(200, map[string]any{"bidPrice": "3000.00", "askPrice": "3001.00"})
		}
	case p == "/api/v3/ticker/price":
		reply(200, []any{map[string]any{"symbol": "BTCUSDT", "price": "90005.00"}, map[string]any{"symbol": "ETHUSDT", "price": "3000.50"}})
	case p == "/api/v3/account":
		var bs []any
		for a, v := range s.balances {
			locked := 0.0
			for _, o := range s.orders {
				if o.Get("side") == "SELL" && strings.HasPrefix(o.Get("symbol"), a) {
					locked = v
				}
			}
			bs = append(bs, map[string]any{"asset": a, "free": strconv.FormatFloat(v-locked, 'f', -1, 64), "locked": strconv.FormatFloat(locked, 'f', -1, 64)})
		}
		bs = append(bs, map[string]any{"asset": "XYZ", "free": "5", "locked": "0"}) // no USDT pair: left out
		reply(200, map[string]any{"uid": 354937868, "canTrade": true, "balances": bs})
	case p == "/api/v3/myTrades":
		reply(200, []any{map[string]any{"price": "80000", "qty": "0.005", "isBuyer": true, "time": 1763900000000},
			map[string]any{"price": "86000", "qty": "0.005", "isBuyer": true, "time": 1763950000000}})
	case p == "/api/v3/order" && r.Method == http.MethodPost:
		s.got = append(s.got, q)
		if q.Get("quantity") == "50.00000" {
			reply(400, map[string]any{"code": -2010, "msg": "Account has insufficient balance for requested action."})
			return
		}
		if q.Get("type") == "MARKET" {
			s.n++
			qty, _ := strconv.ParseFloat(q.Get("quantity"), 64)
			if q.Get("side") == "BUY" {
				s.balances["BTC"] += qty
				s.balances["USDT"] -= qty * 90010
			} else {
				s.balances["BTC"] -= qty
			}
			reply(200, map[string]any{"orderId": s.n, "status": "FILLED", "executedQty": q.Get("quantity"), "cummulativeQuoteQty": strconv.FormatFloat(qty*90010, 'f', -1, 64)})
			return
		}
		id := order(q, -1)
		reply(200, map[string]any{"orderId": id, "status": "NEW", "executedQty": "0"})
	case p == "/api/v3/orderList/otoco" || p == "/api/v3/orderList/oto" || p == "/api/v3/orderList/oco":
		s.got = append(s.got, q)
		s.nextList++
		list := s.nextList
		s.listReq[list] = q
		var ids []any
		if strings.HasPrefix(p, "/api/v3/orderList/oto") {
			w := url.Values{"symbol": {q.Get("symbol")}, "side": {"BUY"}, "type": {"LIMIT"}, "price": {q.Get("workingPrice")}, "quantity": {q.Get("workingQuantity")}}
			ids = append(ids, map[string]any{"orderId": order(w, list)})
			s.lists[list] = []int64{s.n}
		} else {
			above := url.Values{"symbol": {q.Get("symbol")}, "side": {"SELL"}, "type": {q.Get("aboveType")}, "price": {q.Get("abovePrice")}, "quantity": {q.Get("quantity")}}
			below := url.Values{"symbol": {q.Get("symbol")}, "side": {"SELL"}, "type": {q.Get("belowType")}, "stopPrice": {q.Get("belowStopPrice")}, "quantity": {q.Get("quantity")}}
			a, b := order(above, list), order(below, list)
			s.lists[list] = []int64{a, b}
			ids = append(ids, map[string]any{"orderId": a}, map[string]any{"orderId": b})
		}
		reply(200, map[string]any{"orderListId": list, "orders": ids})
	case p == "/api/v3/openOrders":
		var os []any
		for id, o := range s.orders {
			list, _ := strconv.ParseInt(o.Get("orderListId"), 10, 64)
			os = append(os, map[string]any{"symbol": o.Get("symbol"), "orderId": id, "orderListId": list, "price": o.Get("price"), "origQty": o.Get("quantity"),
				"executedQty": "0", "status": "NEW", "type": o.Get("type"), "side": o.Get("side"), "stopPrice": o.Get("stopPrice"), "time": 1764000000000})
		}
		reply(200, os)
	case p == "/api/v3/orderList" && r.Method == http.MethodGet:
		id, _ := strconv.ParseInt(q.Get("orderListId"), 10, 64)
		var os []any
		for _, o := range s.lists[id] {
			os = append(os, map[string]any{"symbol": "BTCUSDT", "orderId": o})
		}
		// the pending legs of an OTOCO, as Binance lists them before the buy fills
		if g := s.otoco(id); g != nil {
			os = append(os, map[string]any{"symbol": "BTCUSDT", "orderId": 900 + id}, map[string]any{"symbol": "BTCUSDT", "orderId": 950 + id})
		}
		reply(200, map[string]any{"orderListId": id, "orders": os})
	case p == "/api/v3/order" && r.Method == http.MethodGet:
		id, _ := strconv.ParseInt(q.Get("orderId"), 10, 64)
		if id >= 950 {
			g := s.otoco(id - 950)
			reply(200, map[string]any{"symbol": "BTCUSDT", "orderId": id, "side": "SELL", "type": g.Get("pendingBelowType"), "stopPrice": g.Get("pendingBelowStopPrice")})
			return
		}
		if id >= 900 {
			g := s.otoco(id - 900)
			reply(200, map[string]any{"symbol": "BTCUSDT", "orderId": id, "side": "SELL", "type": g.Get("pendingAboveType"), "price": g.Get("pendingAbovePrice")})
			return
		}
		o := s.orders[id]
		reply(200, map[string]any{"symbol": o.Get("symbol"), "orderId": id, "side": o.Get("side"), "type": o.Get("type"), "price": o.Get("price")})
	case p == "/api/v3/order" && r.Method == http.MethodDelete:
		id, _ := strconv.ParseInt(q.Get("orderId"), 10, 64)
		delete(s.orders, id)
		reply(200, map[string]any{"orderId": id, "status": "CANCELED"})
	case p == "/api/v3/orderList" && r.Method == http.MethodDelete:
		id, _ := strconv.ParseInt(q.Get("orderListId"), 10, 64)
		for _, o := range s.lists[id] {
			delete(s.orders, o)
		}
		delete(s.lists, id)
		reply(200, map[string]any{"orderListId": id})
	case p == "/api/v3/order/cancelReplace":
		s.got = append(s.got, q)
		id, _ := strconv.ParseInt(q.Get("cancelOrderId"), 10, 64)
		delete(s.orders, id)
		order(url.Values{"symbol": {q.Get("symbol")}, "side": {"BUY"}, "type": {"LIMIT"}, "price": {q.Get("price")}, "quantity": {q.Get("quantity")}}, -1)
		reply(200, map[string]any{"cancelResult": "SUCCESS", "newOrderResult": "SUCCESS"})
	default:
		reply(404, map[string]any{"code": -1, "msg": "no route " + p})
	}
}

// otoco is the OTOCO request that made list id, if one did.
func (s *pretendBinance) otoco(id int64) url.Values {
	if g := s.listReq[id]; g != nil && g.Get("pendingAboveType") != "" {
		return g
	}
	return nil
}

func pretend(t *testing.T) (*pretendBinance, string) {
	t.Helper()
	s := &pretendBinance{balances: map[string]float64{"USDT": 1000, "BTC": 0.01}, orders: map[int64]url.Values{}, lists: map[int64][]int64{}, listReq: map[int64]url.Values{}}
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	env := "test-" + t.Name()
	Bases[env] = srv.URL
	t.Cleanup(func() { delete(Bases, env) })
	return s, env
}

// A Binance spot account: requests signed, a dollar pair traded as USDT, a holding read as a trade with its average
// cost, a buy with its stop and target as one order list, the stop moved, the holding sold; spot's limits refused in
// words.
func TestAdapter_Spot(t *testing.T) {
	ctx := context.Background()
	s, env := pretend(t)
	a, err := New(env, "key", "sec")
	if err != nil {
		t.Fatal(err)
	}
	s.skewOnce = true
	acct, err := a.Account(ctx)
	if err != nil || !s.syncAsked || acct.Currency != "USDT" || acct.Balance != 1900.05 || acct.MarginAvailable != 1000 || acct.AccountID != "354937868" {
		t.Fatalf("account, valued in USDT, after setting the clock by Binance's: %+v %v sync=%v", acct, err, s.syncAsked)
	}
	bid, ask, err := a.LiveQuote(ctx, "BTC/USD")
	if err != nil || bid != 90000 || ask != 90010 {
		t.Fatalf("BTC/USD as BTCUSDT: %v %v %v", bid, ask, err)
	}
	trades, err := a.OpenTrades(ctx)
	if err != nil || len(trades) != 1 || trades[0].BrokerTradeID != "BTCUSDT" || trades[0].Quantity != 0.01 || trades[0].OpenPrice != 83000 ||
		trades[0].UnrealizedPnL != 70 || trades[0].CurrentPrice != 90000 || trades[0].OpenedAt.IsZero() {
		t.Fatalf("the BTC held, its average cost from its fills: %+v %v", trades, err)
	}
	// a market buy without levels; then one with both: an OTOCO with a limit at the ask, its legs 99.9% of it
	r, err := a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "BTCUSD", Units: 1, Quantity: 0.001})
	if err != nil || r.BrokerTradeID != "BTCUSDT" || r.FilledPrice != 90010 {
		t.Fatalf("market: %+v %v", r, err)
	}
	if g := s.got[0]; g.Get("type") != "MARKET" || g.Get("quantity") != "0.00100" || g.Get("side") != "BUY" || g.Get("symbol") != "BTCUSDT" {
		t.Errorf("as sent: %v", g)
	}
	r, err = a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "BTCUSD", Units: 1, Quantity: 0.001, StopLoss: 85000, TakeProfit: 99000})
	if err != nil || r.FilledPrice != 90010 {
		t.Fatalf("with a stop and target: %+v %v", r, err)
	}
	g := s.got[len(s.got)-1]
	if g.Get("workingType") != "LIMIT" || g.Get("workingPrice") != "90010.00" || g.Get("workingQuantity") != "0.00100" || g.Get("pendingQuantity") != "0.00099" ||
		g.Get("pendingAboveType") != "LIMIT_MAKER" || g.Get("pendingAbovePrice") != "99000.00" || g.Get("pendingBelowType") != "STOP_LOSS" || g.Get("pendingBelowStopPrice") != "85000.00" {
		t.Errorf("OTOCO as sent: %v", g)
	}
	orders, err := a.OpenOrders(ctx)
	if err != nil || len(orders) != 1 || orders[0].Level != 90010 || orders[0].StopLoss != 85000 || orders[0].TakeProfit != 99000 || orders[0].Quantity != 0.001 {
		t.Fatalf("the waiting buy with its legs: %+v %v", orders, err)
	}
	if err := a.AmendOrder(ctx, orders[0].BrokerOrderID, 89000, 0, 0); err == nil {
		t.Error("an order list can't be changed in place")
	}
	if err := a.CancelOrder(ctx, orders[0].BrokerOrderID); err != nil || len(s.orders) != 0 {
		t.Fatalf("cancelled with its list: %v %v", err, s.orders)
	}
	// a plain limit buy, moved with cancelReplace
	if _, err := a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "BTCUSDT", Units: 1, Quantity: 0.002, OrderType: "limit", EntryPrice: 88000}); err != nil {
		t.Fatal(err)
	}
	orders, _ = a.OpenOrders(ctx)
	if err := a.AmendOrder(ctx, orders[0].BrokerOrderID, 87500, 0, 0); err != nil {
		t.Fatal(err)
	}
	if g := s.got[len(s.got)-1]; g.Get("cancelReplaceMode") != "STOP_ON_FAILURE" || g.Get("price") != "87500.00" || g.Get("quantity") != "0.00200" {
		t.Errorf("replaced: %v", g)
	}
	orders, _ = a.OpenOrders(ctx)
	_ = a.CancelOrder(ctx, orders[0].BrokerOrderID)
	// the holding's stop and target set (an OCO), then the stop moved (cancelled, placed again with the target kept)
	if err := a.AmendTrade(ctx, "BTCUSDT", 84000, 98000); err != nil {
		t.Fatal(err)
	}
	if g := s.got[len(s.got)-1]; g.Get("aboveType") != "LIMIT_MAKER" || g.Get("belowStopPrice") != "84000.00" || g.Get("quantity") != "0.01100" {
		t.Errorf("OCO on the holding: %v", g)
	}
	trades, _ = a.OpenTrades(ctx)
	if len(trades) != 1 || trades[0].StopLoss != 84000 || trades[0].TakeProfit != 98000 {
		t.Fatalf("its stop and target read back: %+v", trades)
	}
	if err := a.AmendTrade(ctx, "BTCUSDT", 86000, 0); err != nil {
		t.Fatal(err)
	}
	if g := s.got[len(s.got)-1]; g.Get("belowStopPrice") != "86000.00" || g.Get("abovePrice") != "98000.00" {
		t.Errorf("stop moved, target kept: %v", g)
	}
	if err := a.CloseTrade(ctx, "BTCUSDT"); err != nil || len(s.orders) != 0 {
		t.Fatalf("sold, its stop and target off first: %v %v", err, s.orders)
	}
	if g := s.got[len(s.got)-1]; g.Get("side") != "SELL" || g.Get("type") != "MARKET" || g.Get("quantity") != "0.01100" {
		t.Errorf("the whole holding sold: %v", g)
	}
	// a pair without STOP_LOSS gets a stop-limit
	if _, err := a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "ETHUSDT", Units: 1, Quantity: 0.01, StopLoss: 2900}); err != nil {
		t.Fatal(err)
	}
	if g := s.got[len(s.got)-1]; g.Get("pendingType") != "STOP_LOSS_LIMIT" || g.Get("pendingStopPrice") != "2900.00" || g.Get("pendingPrice") != "2885.50" || g.Get("pendingTimeInForce") != "GTC" {
		t.Errorf("stop-limit as sent: %v", g)
	}
	// refusals
	_, err = a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "ETHUSDT", Units: -1, Quantity: 1})
	if rf, ok := broker.AsRefused(err); !ok || rf.BrokerSaid != "SPOT_NO_SHORT" {
		t.Errorf("selling what isn't held: %v", err)
	}
	_, err = a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "BTCUSDT", Units: 1, Quantity: 0.00001})
	if rf, ok := broker.AsRefused(err); !ok || rf.Kind != broker.RefusedSizeTooSmall {
		t.Errorf("below the smallest order (by its value), never raised: %v", err)
	}
	_, err = a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "DOGEUSDT", Units: 100})
	if rf, ok := broker.AsRefused(err); !ok || rf.Kind != broker.RefusedMarketClosed {
		t.Errorf("a halted pair: %v", err)
	}
	_, err = a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "BTCUSDT", Units: 50})
	if rf, ok := broker.AsRefused(err); !ok || rf.Kind != broker.RefusedNotEnoughMargin {
		t.Errorf("not enough USDT: %v", err)
	}
	_, err = a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "BTCUSDT", Units: 1, Quantity: 0.001, OrderType: "limit", EntryPrice: 95000, StopLoss: 90000})
	if rf, ok := broker.AsRefused(err); !ok || rf.Kind != broker.RefusedLevel {
		t.Errorf("a buy waiting above the market: %v", err)
	}
	if _, _, err := a.LiveQuote(ctx, "NOPEUSD"); err == nil {
		t.Error("a pair Binance hasn't")
	}
	if h, _ := a.MarketHours(ctx, "DOGEUSDT"); h.Open {
		t.Error("halted")
	}
	if rules, _ := a.MarketDealingRules(ctx, "BTCUSD"); rules.MinDealSize != 0.00001 || rules.MaxDealSize != 9000 || rules.MarginPercent != 100 {
		t.Errorf("rules: %+v", rules)
	}
	wrong, _ := New(env, "other", "sec")
	if _, err := wrong.Account(ctx); err == nil {
		t.Error("a key Binance refuses")
	} else if rf, ok := broker.AsRefused(err); !ok || rf.BrokerSaid != "-2015" {
		t.Errorf("a refused key is a refusal: %v", err)
	}
}

func TestSpec(t *testing.T) {
	r := broker.NewRegistry()
	if err := r.Register(Spec()); err != nil {
		t.Fatal(err)
	}
}
