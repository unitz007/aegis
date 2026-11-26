package ctrader

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/unitz007/aegis/broker"
)

// openAPI is a pretend cTrader Open API: one application, one account, EURUSD, BTCUSD and a closed USDJPY.
type openAPI struct {
	t         *testing.T
	mu        sync.Mutex
	positions map[int64]map[string]any
	orders    map[int64]map[string]any
	next      int64
	got       []map[string]any // new orders as sent
}

func (s *openAPI) serve(w http.ResponseWriter, r *http.Request) {
	ws, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer ws.Close()
	appOK := false
	for {
		var m message
		if ws.ReadJSON(&m) != nil {
			return
		}
		if m.PayloadType == tHeartbeat {
			continue
		}
		var p map[string]any
		d := json.NewDecoder(strings.NewReader(string(m.Payload)))
		d.UseNumber()
		_ = d.Decode(&p)
		reply := func(typ int, payload any) {
			raw, _ := json.Marshal(payload)
			_ = ws.WriteJSON(message{ClientMsgID: m.ClientMsgID, PayloadType: typ, Payload: raw})
		}
		event := func(id string, payload map[string]any) { // an execution, carrying its request's id or none
			payload["ctidTraderAccountId"] = 42
			raw, _ := json.Marshal(payload)
			_ = ws.WriteJSON(message{ClientMsgID: id, PayloadType: tExecutionEvent, Payload: raw})
		}
		fail := func(code string) { reply(tOAErrorRes, map[string]any{"errorCode": code, "description": "no"}) }
		if m.PayloadType != tAppAuthReq && !appOK {
			fail("CH_CLIENT_NOT_AUTHENTICATED")
			continue
		}
		s.mu.Lock()
		switch m.PayloadType {
		case tAppAuthReq:
			if p["clientId"] != "app" || p["clientSecret"] != "secret" {
				fail("CH_CLIENT_AUTH_FAILURE")
			} else {
				appOK = true
				reply(tAppAuthRes, map[string]any{})
			}
		case tAccountAuthReq:
			if i64(p["ctidTraderAccountId"]) != 42 || p["accessToken"] != "tok" {
				fail("CH_ACCESS_TOKEN_INVALID")
			} else {
				reply(tAccountAuthRes, map[string]any{"ctidTraderAccountId": 42})
			}
		case tSymbolsListReq:
			reply(tSymbolsListRes, map[string]any{"symbol": []any{
				map[string]any{"symbolId": 1, "symbolName": "EURUSD", "enabled": true},
				map[string]any{"symbolId": 2, "symbolName": "BTCUSD", "enabled": true},
				map[string]any{"symbolId": 3, "symbolName": "USDJPY", "enabled": true},
				map[string]any{"symbolId": 4, "symbolName": "GBPJPY", "enabled": false}}})
		case tSymbolByIDReq:
			id := ints(p["symbolId"])[0]
			sym := map[string]any{"symbolId": id, "digits": 5, "minVolume": 100000, "maxVolume": 1000000000, "stepVolume": 100000, "scheduleTimeZone": "UTC",
				"schedule": []any{map[string]any{"startSecond": 79200, "endSecond": 165600}}} // Sunday 22:00 to Monday 22:00
			if id == 2 {
				sym["digits"], sym["minVolume"], sym["stepVolume"] = 2, 1, 1
			}
			if id == 3 {
				sym["digits"] = 3
			}
			reply(tSymbolByIDRes, map[string]any{"symbol": []any{sym}})
		case tSubscribeSpotsReq:
			reply(tSubscribeSpotsRes, map[string]any{})
			for _, id := range ints(p["symbolId"]) {
				bid, ask := 109678, 109685
				if id == 2 {
					bid, ask = 6083220000, 6088220000
				}
				raw, _ := json.Marshal(map[string]any{"ctidTraderAccountId": 42, "symbolId": id, "bid": bid, "ask": ask})
				_ = ws.WriteJSON(message{PayloadType: tSpotEvent, Payload: raw})
			}
		case tAccountsByTokenReq:
			if p["accessToken"] != "tok" {
				fail("CH_ACCESS_TOKEN_INVALID")
				break
			}
			reply(tAccountsByTokenRes, map[string]any{"permissionScope": "SCOPE_TRADE", "ctidTraderAccount": []any{
				map[string]any{"ctidTraderAccountId": 42, "isLive": false, "traderLogin": 5012345, "brokerTitleShort": "Pepperstone"},
				map[string]any{"ctidTraderAccountId": 43, "isLive": true, "traderLogin": 7012345, "brokerTitleShort": "Pepperstone"}}})
		case tRefreshTokenReq:
			if p["refreshToken"] != "ref" {
				fail("CH_ACCESS_TOKEN_INVALID")
				break
			}
			reply(tRefreshTokenRes, map[string]any{"accessToken": "tok2", "tokenType": "bearer", "expiresIn": 2628000, "refreshToken": "ref2"})
		case tTraderReq:
			reply(tTraderRes, map[string]any{"trader": map[string]any{"balance": "1000000", "moneyDigits": 2, "depositAssetId": 7}})
		case tAssetListReq:
			reply(tAssetListRes, map[string]any{"asset": []any{map[string]any{"assetId": 7, "name": "EUR"}}})
		case tUnrealizedPnLReq:
			var list []any
			for id := range s.positions {
				list = append(list, map[string]any{"positionId": id, "grossUnrealizedPnL": 150, "netUnrealizedPnL": 123})
			}
			reply(tUnrealizedPnLRes, map[string]any{"moneyDigits": 2, "positionUnrealizedPnL": list})
		case tReconcileReq:
			var ps, os []any
			for _, x := range s.positions {
				ps = append(ps, x)
			}
			for _, x := range s.orders {
				os = append(os, x)
			}
			reply(tReconcileRes, map[string]any{"position": ps, "order": os})
		case tNewOrderReq:
			s.got = append(s.got, p)
			sym, typ := i64(p["symbolId"]), int(i64(p["orderType"]))
			if sym == 3 {
				raw, _ := json.Marshal(map[string]any{"ctidTraderAccountId": 42, "errorCode": "MARKET_CLOSED"})
				_ = ws.WriteJSON(message{ClientMsgID: m.ClientMsgID, PayloadType: tOrderErrorEvent, Payload: raw})
				break
			}
			s.next++
			td := map[string]any{"symbolId": sym, "volume": p["volume"], "tradeSide": p["tradeSide"], "openTimestamp": time.Now().UnixMilli()}
			order := map[string]any{"orderId": 500 + s.next, "orderType": typ, "tradeData": td, "clientOrderId": p["clientOrderId"],
				"limitPrice": p["limitPrice"], "stopPrice": p["stopPrice"], "stopLoss": p["stopLoss"], "takeProfit": p["takeProfit"]}
			event(m.ClientMsgID, map[string]any{"executionType": "ORDER_ACCEPTED", "order": order})
			if typ == orderMarket {
				fill := 1.09685
				if sym == 2 {
					fill = 60882.2
				}
				sl, tp := fill-f64(p["relativeStopLoss"])/1e5, fill+f64(p["relativeTakeProfit"])/1e5
				pos := map[string]any{"positionId": 900 + s.next, "tradeData": td, "price": fill, "stopLoss": sl, "takeProfit": tp, "usedMargin": 3650, "moneyDigits": 2}
				s.positions[900+s.next] = pos
				event("", map[string]any{"executionType": execFilled, "order": order, "position": pos, "deal": map[string]any{"executionPrice": fill}})
			} else {
				s.orders[500+s.next] = order
			}
		case tAmendPositionReq:
			pos := s.positions[i64(p["positionId"])]
			if pos == nil {
				fail("POSITION_NOT_FOUND")
				break
			}
			pos["stopLoss"], pos["takeProfit"] = p["stopLoss"], p["takeProfit"]
			event(m.ClientMsgID, map[string]any{"executionType": "ORDER_REPLACED", "position": pos})
		case tClosePositionReq:
			pos := s.positions[i64(p["positionId"])]
			if pos == nil || i64(p["volume"]) != i64(obj(pos["tradeData"])["volume"]) {
				fail("TRADING_BAD_VOLUME")
				break
			}
			delete(s.positions, i64(p["positionId"]))
			event(m.ClientMsgID, map[string]any{"executionType": execFilled, "position": pos})
		case tCancelOrderReq:
			delete(s.orders, i64(p["orderId"]))
			event(m.ClientMsgID, map[string]any{"executionType": execCancelled, "order": map[string]any{"orderId": p["orderId"]}})
		case tAmendOrderReq:
			o := s.orders[i64(p["orderId"])]
			o["limitPrice"], o["stopPrice"], o["stopLoss"], o["takeProfit"] = p["limitPrice"], p["stopPrice"], p["stopLoss"], p["takeProfit"]
			event(m.ClientMsgID, map[string]any{"executionType": execReplaced, "order": o})
		default:
			fail("UNSUPPORTED_MESSAGE")
		}
		s.mu.Unlock()
	}
}

func pretend(t *testing.T) (*openAPI, string) {
	t.Helper()
	s := &openAPI{t: t, positions: map[int64]map[string]any{}, orders: map[int64]map[string]any{}}
	srv := httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(srv.Close)
	env := "test-" + t.Name()
	Hosts[env] = "ws" + strings.TrimPrefix(srv.URL, "http")
	t.Cleanup(func() { delete(Hosts, env) })
	return s, env
}

// A trader's account on cTrader: placed, read, changed and closed as approved; sizes, levels and refusals as every
// broker says them.
func TestAdapter_TradesAsApproved(t *testing.T) {
	ctx := context.Background()
	s, env := pretend(t)
	a, err := New(env, "app", "secret", "42", "tok")
	if err != nil {
		t.Fatal(err)
	}
	bid, ask, err := a.LiveQuote(ctx, "EUR/USD")
	if err != nil || bid != 1.09678 || ask != 1.09685 {
		t.Fatalf("price: %v %v %v", bid, ask, err)
	}
	// a market buy: its stop and target relative to the price, the size in hundredths of a unit
	r, err := a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "EUR/USD", Units: 1000, OrderType: "market", StopLoss: 1.0918, TakeProfit: 1.1068})
	if err != nil || r.BrokerTradeID != "901" || r.FilledPrice != 1.09685 {
		t.Fatalf("market: %+v %v", r, err)
	}
	if got := s.got[0]; i64(got["volume"]) != 100000 || i64(got["relativeStopLoss"]) != 505 || i64(got["relativeTakeProfit"]) != 995 || got["stopLoss"] != nil {
		t.Errorf("as sent: %v", got)
	}
	// an entry below the market is a limit; above it, a stop: either waits for its level
	lim, err := a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "EURUSD", Units: 1000, OrderType: "limit", EntryPrice: 1.05, StopLoss: 1.045, TakeProfit: 1.07})
	if err != nil || lim.BrokerTradeID != "502" || i64(s.got[1]["orderType"]) != orderLimit || f64(s.got[1]["limitPrice"]) != 1.05 {
		t.Fatalf("limit: %+v %v %v", lim, err, s.got[1])
	}
	stop, err := a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "EURUSD", Units: -2000, OrderType: "limit", EntryPrice: 1.09})
	if err != nil || i64(s.got[2]["orderType"]) != orderStop || i64(s.got[2]["tradeSide"]) != sideSell {
		t.Fatalf("a sell below the market waits as a stop: %+v %v %v", stop, err, s.got[2])
	}
	// a crypto quantity
	if _, err := a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "BTCUSD", Units: 1, Quantity: 0.01, OrderType: "market"}); err != nil || i64(s.got[3]["volume"]) != 1 {
		t.Fatalf("crypto: %v %v", err, s.got[3])
	}
	trades, err := a.OpenTrades(ctx)
	if err != nil || len(trades) != 2 {
		t.Fatalf("open: %+v %v", trades, err)
	}
	var eu, btc broker.OpenTrade
	for _, tr := range trades {
		if tr.Symbol == "EURUSD" {
			eu = tr
		} else {
			btc = tr
		}
	}
	if eu.Units != 1000 || eu.UnrealizedPnL != 1.23 || eu.PnLCurrency != "EUR" || eu.CurrentPrice != 1.09678 || btc.Quantity != 0.01 || btc.Units != 1 {
		t.Errorf("read: %+v %+v", eu, btc)
	}
	orders, _ := a.OpenOrders(ctx)
	if len(orders) != 2 {
		t.Fatalf("orders: %+v", orders)
	}
	acct, err := a.Account(ctx)
	if err != nil || acct.Balance != 10000 || acct.Currency != "EUR" || acct.UnrealizedPnL != 2.46 || acct.MarginUsed != 73 {
		t.Errorf("account: %+v %v", acct, err)
	}
	// its stop moved, its target kept; closed whole
	if err := a.AmendTrade(ctx, "901", 1.0935, 0); err != nil {
		t.Fatal(err)
	}
	if p := s.positions[901]; f64(p["stopLoss"]) != 1.0935 || f64(p["takeProfit"]) == 0 {
		t.Errorf("amended: %v", p)
	}
	if err := a.CloseTrade(ctx, "901"); err != nil || s.positions[901] != nil {
		t.Fatalf("closed: %v", err)
	}
	if err := a.AmendOrder(ctx, "502", 1.051, 0, 0); err != nil || f64(s.orders[502]["limitPrice"]) != 1.051 || f64(s.orders[502]["stopLoss"]) != 1.045 {
		t.Fatalf("order amended: %v %v", err, s.orders[502])
	}
	if err := a.CancelOrder(ctx, "502"); err != nil || s.orders[502] != nil {
		t.Fatalf("cancelled: %v", err)
	}
	// refusals as every broker says them
	_, err = a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "USDJPY", Units: 1000})
	if rf, ok := broker.AsRefused(err); !ok || rf.Kind != broker.RefusedMarketClosed {
		t.Errorf("closed market: %v", err)
	}
	_, err = a.PlaceTrade(ctx, broker.TradeRequest{Symbol: "EURUSD", Units: 10})
	if rf, ok := broker.AsRefused(err); !ok || rf.Kind != broker.RefusedSizeTooSmall || rf.Limit != 1000 {
		t.Errorf("below the smallest deal, never raised: %v", err)
	}
	if err := a.CloseTrade(ctx, "999"); err == nil {
		t.Error("no such position")
	}
	// another trader's token is refused as access, for the platform to refresh or ask them to sign in again
	other, _ := New(env, "app", "secret", "42", "wrong")
	if _, err := other.Account(ctx); !errors.Is(err, ErrAccess) {
		t.Errorf("access: %v", err)
	}
}

// Its hours: open from Sunday 22:00 to Monday 22:00 (in the symbol's zone); closed, when it opens next.
func TestSymbol_Hours(t *testing.T) {
	s := &symbol{zone: "UTC", schedule: [][2]int64{{79200, 165600}}}
	sun := time.Date(2026, 11, 22, 17, 6, 0, 0, time.UTC)
	if h := s.hours(sun); h.Open || !h.NextOpen.Equal(time.Date(2026, 11, 22, 22, 0, 0, 0, time.UTC)) {
		t.Errorf("Sunday afternoon: %+v", h)
	}
	if h := s.hours(sun.Add(6 * time.Hour)); !h.Open {
		t.Errorf("Sunday night: %+v", h)
	}
	if h := s.hours(time.Date(2026, 11, 24, 9, 0, 0, 0, time.UTC)); h.Open || !h.NextOpen.Equal(time.Date(2026, 11, 29, 22, 0, 0, 0, time.UTC)) {
		t.Errorf("Tuesday, after the week's one range: %+v", h)
	}
	s.mode = 1
	if h := s.hours(sun.Add(6 * time.Hour)); h.Open {
		t.Error("trading switched off")
	}
}

// Signing in: the page with the application's id, the code turned into tokens, the trader's accounts, a renewed access.
func TestSignIn(t *testing.T) {
	ctx := context.Background()
	_, env := pretend(t)
	if u := AuthorizeURL("app", "https://x.test/cb", "trading", "st"); !strings.Contains(u, "client_id=app") || !strings.Contains(u, "scope=trading") || !strings.Contains(u, "redirect_uri=https%3A%2F%2Fx.test%2Fcb") {
		t.Errorf("page: %s", u)
	}
	tokens := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("grant_type") != "authorization_code" || q.Get("code") != "c0de" || q.Get("client_secret") != "secret" {
			_, _ = w.Write([]byte(`{"errorCode":"ACCESS_DENIED","description":"bad code"}`))
			return
		}
		_, _ = w.Write([]byte(`{"accessToken":"tok","tokenType":"bearer","expiresIn":2628000,"refreshToken":"ref","errorCode":null,"description":null}`))
	}))
	defer tokens.Close()
	was := TokenURL
	TokenURL = tokens.URL
	defer func() { TokenURL = was }()
	tok, err := ExchangeCode(ctx, "app", "secret", "c0de", "https://x.test/cb")
	if err != nil || tok.Access != "tok" || tok.Refresh != "ref" || time.Until(tok.Expires) < 29*24*time.Hour {
		t.Fatalf("tokens: %+v %v", tok, err)
	}
	if _, err := ExchangeCode(ctx, "app", "secret", "old", "https://x.test/cb"); err == nil || !strings.Contains(err.Error(), "bad code") {
		t.Errorf("a code refused: %v", err)
	}
	accts, trading, err := Accounts(ctx, env, "app", "secret", "tok")
	if err != nil || !trading || len(accts) != 2 || accts[0].ID != "42" || accts[0].Live || !accts[1].Live || accts[0].Broker != "Pepperstone" || accts[0].Login != "5012345" {
		t.Fatalf("accounts: %+v %v %v", accts, trading, err)
	}
	renewed, err := Refresh(ctx, env, "app", "secret", "ref")
	if err != nil || renewed.Access != "tok2" || renewed.Refresh != "ref2" {
		t.Fatalf("renewed: %+v %v", renewed, err)
	}
	if _, err := Refresh(ctx, env, "app", "secret", "revoked"); !errors.Is(err, ErrAccess) {
		t.Errorf("a refresh refused is access gone: %v", err)
	}
}

// A connection that only reads (Check Trades, the trade watcher) names each position and order by its symbol, never by
// the id cTrader gives it, even on a symbol the account may no longer trade. Found on a real demo account: "SELL 8000 3".
func TestAdapter_ReadsNameTheirSymbols(t *testing.T) {
	ctx := context.Background()
	s, env := pretend(t)
	td := func(sym int64, side int) map[string]any {
		return map[string]any{"symbolId": sym, "volume": 800000, "tradeSide": side, "openTimestamp": time.Now().UnixMilli()}
	}
	s.positions[71] = map[string]any{"positionId": 71, "price": 174.841, "stopLoss": 175.5, "tradeData": td(3, sideSell)}
	s.positions[72] = map[string]any{"positionId": 72, "price": 199.1, "tradeData": td(4, sideBuy)}
	s.orders[81] = map[string]any{"orderId": 81, "orderType": orderLimit, "limitPrice": 1.05, "tradeData": td(1, sideBuy)}
	a, err := New(env, "app", "secret", "42", "tok")
	if err != nil {
		t.Fatal(err)
	}
	trades, err := a.OpenTrades(ctx)
	if err != nil || len(trades) != 2 {
		t.Fatalf("open: %+v %v", trades, err)
	}
	got := map[string]string{}
	for _, tr := range trades {
		got[tr.BrokerTradeID] = tr.Symbol
	}
	if got["71"] != "USDJPY" || got["72"] != "GBPJPY" {
		t.Errorf("trades named %v", got)
	}
	orders, err := a.OpenOrders(ctx)
	if err != nil || len(orders) != 1 || orders[0].Symbol != "EURUSD" {
		t.Errorf("orders: %+v %v", orders, err)
	}
}

// A stream pushes the prices of the symbols asked for, as Open API spot events come, and a trade event of the account
// whenever an order fills or a position changes; it closes once its context is done.
func TestAdapter_Stream(t *testing.T) {
	s, env := pretend(t)
	a, err := New(env, "app", "secret", "42", "tok")
	if err != nil {
		t.Fatal(err)
	}
	var _ broker.Streamer = a
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := a.Stream(ctx, []string{"EURUSD"})
	if err != nil {
		t.Fatal(err)
	}
	next := func() broker.StreamEvent {
		t.Helper()
		select {
		case e, ok := <-ch:
			if !ok {
				t.Fatal("closed")
			}
			return e
		case <-time.After(3 * time.Second):
			t.Fatal("nothing pushed")
		}
		return broker.StreamEvent{}
	}
	if e := next(); e.Symbol != "EURUSD" || e.Bid != 1.09678 || e.Ask != 1.09685 || e.Trade {
		t.Fatalf("its price: %+v", e)
	}
	if _, err := a.PlaceTrade(context.Background(), broker.TradeRequest{Symbol: "EURUSD", Units: 1000, OrderType: "market", StopLoss: 1.0918, TakeProfit: 1.1068}); err != nil {
		t.Fatal(err)
	}
	if e := next(); !e.Trade {
		t.Fatalf("a trade event: %+v", e)
	}
	_ = s
	cancel()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("not closed once its context was done")
		}
	}
}
