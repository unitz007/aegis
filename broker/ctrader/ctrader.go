package ctrader

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/unitz007/aegis/broker"
)

// Adapter is one trader's cTrader account (ctidTraderAccountId) reached with their access token.
type Adapter struct {
	c       *conn
	account int64
	token   string

	mu       sync.Mutex
	symbols  map[string]int64 // normalised name → id
	names    map[int64]string
	details  map[int64]*symbol
	currency string
}

var (
	_ broker.Port                 = (*Adapter)(nil)
	_ broker.DealingRulesProvider = (*Adapter)(nil)
	_ broker.MarketHoursProvider  = (*Adapter)(nil)
)

// New is a trader's account on env ("demo" or "live") through the platform's application.
func New(env, clientID, clientSecret, accountID, accessToken string) (*Adapter, error) {
	url, ok := Hosts[env]
	if !ok {
		return nil, fmt.Errorf("ctrader: no %q environment", env)
	}
	acct, err := strconv.ParseInt(strings.TrimSpace(accountID), 10, 64)
	if err != nil || acct <= 0 {
		return nil, fmt.Errorf("ctrader: %q isn't an account", accountID)
	}
	if strings.TrimSpace(clientID) == "" || strings.TrimSpace(clientSecret) == "" || strings.TrimSpace(accessToken) == "" {
		return nil, errors.New("ctrader: the application and the trader's access are both needed")
	}
	return &Adapter{c: connFor(url, clientID, clientSecret), account: acct, token: accessToken}, nil
}

// Spec is cTrader for a registry: each trader signs in to cTrader and grants the application access to their account
// (broker.StrategyOAuth); the platform keeps the account's id and its tokens as the account's details.
func Spec(clientID, clientSecret string) broker.Spec {
	return broker.Spec{
		Name:     "ctrader",
		Label:    "cTrader",
		Strategy: broker.StrategyOAuth,
		Envs:     []string{"demo", "live"},
		Fields: []broker.Field{
			{Name: "account_id", Label: "Account"},
			{Name: "access_token", Label: "Access", Secret: true},
			{Name: "refresh_token", Label: "Refresh", Secret: true},
			{Name: "expires_at", Label: "Expires"},
		},
		Identity: []string{"account_id"},
		New: func(env string, c broker.Credentials) (broker.Port, error) {
			return New(env, clientID, clientSecret, c["account_id"], c["access_token"])
		},
	}
}

func (a *Adapter) ready(ctx context.Context) error { return a.c.authorise(ctx, a.account, a.token) }

func (a *Adapter) ask(ctx context.Context, typ int, payload map[string]any, want int) (fields, error) {
	if err := a.ready(ctx); err != nil {
		return nil, err
	}
	payload["ctidTraderAccountId"] = a.account
	return a.c.ask(ctx, typ, payload, want)
}

// execute sends a trading request and waits for the execution it ends in.
func (a *Adapter) execute(ctx context.Context, typ int, payload map[string]any, ends ...int) (fields, error) {
	if err := a.ready(ctx); err != nil {
		return nil, err
	}
	payload["ctidTraderAccountId"] = a.account
	_, ch, done, err := a.c.send(ctx, typ, payload)
	if err != nil {
		return nil, err
	}
	defer done()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		select {
		case m, ok := <-ch:
			if !ok {
				return nil, errors.New("ctrader: the connection closed before the order was confirmed")
			}
			switch m.PayloadType {
			case tErrorRes, tOAErrorRes, tOrderErrorEvent:
				return nil, refusal(errorOf(m))
			case tExecutionEvent:
				f := m.fields()
				et := enum(f["executionType"], execNames)
				for _, e := range ends {
					if et == e {
						return f, nil
					}
				}
				if et == execRejected || et == execCancelReject {
					code := str(f["errorCode"])
					if code == "" {
						code = "ORDER_REJECTED"
					}
					return nil, refusal(&APIError{Code: code})
				}
			}
		case <-ctx.Done():
			return nil, fmt.Errorf("ctrader: not confirmed in time: %w", ctx.Err())
		}
	}
}

// ── symbols ──

type symbol struct {
	id, min, max, step int64 // volumes in hundredths of a unit
	digits             int
	schedule           [][2]int64 // seconds from Sunday 00:00 in zone
	zone               string
	mode               int
}

// norm is a symbol's name compared: capitals, without separators ("EUR/USD" is "EURUSD").
func norm(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '/', '-', '_', ' ', '.':
			return -1
		}
		return r
	}, strings.ToUpper(strings.TrimSpace(s)))
}

func (a *Adapter) symbolID(ctx context.Context, name string) (int64, error) {
	a.mu.Lock()
	loaded := a.symbols != nil
	a.mu.Unlock()
	if !loaded {
		f, err := a.ask(ctx, tSymbolsListReq, map[string]any{}, tSymbolsListRes)
		if err != nil {
			return 0, err
		}
		ids, names := map[string]int64{}, map[int64]string{}
		for _, s := range objs(f["symbol"]) {
			if en, ok := s["enabled"].(bool); ok && !en {
				continue
			}
			id, n := i64(s["symbolId"]), str(s["symbolName"])
			if id == 0 || n == "" {
				continue
			}
			names[id] = n
			if _, taken := ids[norm(n)]; !taken {
				ids[norm(n)] = id
			}
		}
		a.mu.Lock()
		a.symbols, a.names = ids, names
		a.mu.Unlock()
	}
	want := norm(name)
	a.mu.Lock()
	defer a.mu.Unlock()
	if id, ok := a.symbols[want]; ok {
		return id, nil
	}
	// a broker's own suffix ("EURUSD.r", "EURUSDm"): the one name that starts with it
	var found int64
	for n, id := range a.symbols {
		if strings.HasPrefix(n, want) {
			if found != 0 {
				found = -1
				break
			}
			found = id
		}
	}
	if found > 0 {
		return found, nil
	}
	return 0, &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "SYMBOL_NOT_FOUND", Err: "ctrader: no symbol " + name + " on this account"}
}

func (a *Adapter) nameOf(id int64) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if n := a.names[id]; n != "" {
		return n
	}
	return strconv.FormatInt(id, 10)
}

func (a *Adapter) symbol(ctx context.Context, id int64) (*symbol, error) {
	a.mu.Lock()
	if s := a.details[id]; s != nil {
		a.mu.Unlock()
		return s, nil
	}
	a.mu.Unlock()
	f, err := a.ask(ctx, tSymbolByIDReq, map[string]any{"symbolId": []int64{id}}, tSymbolByIDRes)
	if err != nil {
		return nil, err
	}
	list := objs(f["symbol"])
	if len(list) == 0 {
		return nil, &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "SYMBOL_NOT_FOUND", Err: fmt.Sprintf("ctrader: symbol %d not found", id)}
	}
	r := list[0]
	s := &symbol{id: id, min: i64(r["minVolume"]), max: i64(r["maxVolume"]), step: i64(r["stepVolume"]), digits: int(i64(r["digits"])),
		zone: str(r["scheduleTimeZone"]), mode: enum(r["tradingMode"], tradingModeNames)}
	for _, iv := range objs(r["schedule"]) {
		s.schedule = append(s.schedule, [2]int64{i64(iv["startSecond"]), i64(iv["endSecond"])})
	}
	a.mu.Lock()
	if a.details == nil {
		a.details = map[int64]*symbol{}
	}
	a.details[id] = s
	a.mu.Unlock()
	return s, nil
}

// price is a price as the symbol quotes it (its digits).
func (s *symbol) price(p float64) float64 {
	if p <= 0 {
		return 0
	}
	m := math.Pow(10, float64(s.digits))
	return math.Round(p*m) / m
}

// relative is a distance in price as the Open API takes it: 1/100000 of a unit of price, on the symbol's own steps.
func (s *symbol) relative(d float64) int64 {
	r := math.Round(math.Abs(d) * 1e5)
	if step := math.Pow(10, float64(5-s.digits)); s.digits < 5 && step > 1 {
		r = math.Round(r/step) * step
	}
	return int64(r)
}

// volume is a size in hundredths of a unit, down to the symbol's step (never raised); below its smallest it is refused.
func (s *symbol) volume(units float64) (int64, error) {
	v := int64(math.Round(math.Abs(units) * 100))
	if s.step > 0 {
		v -= v % s.step
	}
	switch {
	case v <= 0 || (s.min > 0 && v < s.min):
		return 0, &broker.Refused{Kind: broker.RefusedSizeTooSmall, Limit: float64(s.min) / 100, BrokerSaid: "TRADING_BAD_VOLUME",
			Err: fmt.Sprintf("ctrader: %g is below the smallest deal (%g)", units, float64(s.min)/100)}
	case s.max > 0 && v > s.max:
		return 0, &broker.Refused{Kind: broker.RefusedSizeTooLarge, Limit: float64(s.max) / 100, BrokerSaid: "TRADING_BAD_VOLUME",
			Err: fmt.Sprintf("ctrader: %g is above the largest deal (%g)", units, float64(s.max)/100)}
	}
	return v, nil
}

// ── prices ──

// LiveQuote is a symbol's bid and ask (subscribed once, kept up to date by the Open API).
func (a *Adapter) LiveQuote(ctx context.Context, sym string) (float64, float64, error) {
	id, err := a.symbolID(ctx, sym)
	if err != nil {
		return 0, 0, err
	}
	return a.quote(ctx, id)
}

func (a *Adapter) quote(ctx context.Context, id int64) (float64, float64, error) {
	if err := a.ready(ctx); err != nil {
		return 0, 0, err
	}
	k := [2]int64{a.account, id}
	a.c.mu.Lock()
	q, subbed := a.c.quotes[k], a.c.subbed[k]
	a.c.mu.Unlock()
	if q.bid > 0 && q.ask > 0 {
		return q.bid, q.ask, nil
	}
	wait := make(chan struct{})
	a.c.mu.Lock()
	a.c.waiters[k] = append(a.c.waiters[k], wait)
	a.c.mu.Unlock()
	if !subbed {
		if _, err := a.ask(ctx, tSubscribeSpotsReq, map[string]any{"symbolId": []int64{id}}, tSubscribeSpotsRes); err != nil {
			var e *APIError
			if !(errors.As(err, &e) && e.Code == "ALREADY_SUBSCRIBED") {
				return 0, 0, err
			}
		}
		a.c.mu.Lock()
		a.c.subbed[k] = true
		a.c.mu.Unlock()
	}
	select {
	case <-wait:
	case <-time.After(5 * time.Second):
	case <-ctx.Done():
	}
	a.c.mu.Lock()
	q = a.c.quotes[k]
	a.c.mu.Unlock()
	if q.bid <= 0 || q.ask <= 0 {
		return 0, 0, fmt.Errorf("ctrader: no price for %s", a.nameOf(id))
	}
	return q.bid, q.ask, nil
}

// ── placing ──

// PlaceTrade places a market order (its stop and target sent relative to the price: the Open API takes no absolute
// ones on a market order) or an entry order at a level: a limit when the level is better than the market, a stop when
// it is beyond it, so it waits for the level either way.
func (a *Adapter) PlaceTrade(ctx context.Context, req broker.TradeRequest) (broker.TradeReceipt, error) {
	id, err := a.symbolID(ctx, req.Symbol)
	if err != nil {
		return broker.TradeReceipt{}, err
	}
	s, err := a.symbol(ctx, id)
	if err != nil {
		return broker.TradeReceipt{}, err
	}
	size := float64(req.Units)
	if req.Quantity != 0 {
		size = req.Quantity
	}
	vol, err := s.volume(size)
	if err != nil {
		return broker.TradeReceipt{}, err
	}
	buy := req.Units > 0 || (req.Units == 0 && req.Quantity > 0)
	side := sideSell
	if buy {
		side = sideBuy
	}
	o := map[string]any{"symbolId": id, "tradeSide": side, "volume": vol}
	if strings.EqualFold(strings.TrimSpace(req.OrderType), "limit") {
		if req.EntryPrice <= 0 {
			return broker.TradeReceipt{}, &broker.Refused{Kind: broker.RefusedLevel, BrokerSaid: "TRADING_BAD_PRICES", Err: "ctrader: a limit order needs its level"}
		}
		bid, ask, _ := a.quote(ctx, id)
		level := s.price(req.EntryPrice)
		beyond := (buy && ask > 0 && level > ask) || (!buy && bid > 0 && level < bid)
		if beyond {
			o["orderType"], o["stopPrice"] = orderStop, level
		} else {
			o["orderType"], o["limitPrice"] = orderLimit, level
		}
		if req.StopLoss > 0 {
			o["stopLoss"] = s.price(req.StopLoss)
		}
		if req.TakeProfit > 0 {
			o["takeProfit"] = s.price(req.TakeProfit)
		}
		f, err := a.execute(ctx, tNewOrderReq, o, execAccepted)
		if err != nil {
			return broker.TradeReceipt{}, err
		}
		ord := obj(f["order"])
		return broker.TradeReceipt{BrokerTradeID: strconv.FormatInt(i64(ord["orderId"]), 10), Symbol: req.Symbol, Units: req.Units,
			FilledPrice: level, StopLoss: req.StopLoss, TakeProfit: req.TakeProfit, OpenedAt: time.Now().UTC(), DecisionID: req.DecisionID, Broker: "ctrader"}, nil
	}
	o["orderType"] = orderMarket
	ref := req.EntryPrice
	if bid, ask, err := a.quote(ctx, id); err == nil {
		if ref = bid; buy {
			ref = ask
		}
	}
	if ref > 0 && req.StopLoss > 0 {
		o["relativeStopLoss"] = s.relative(ref - req.StopLoss)
	}
	if ref > 0 && req.TakeProfit > 0 {
		o["relativeTakeProfit"] = s.relative(req.TakeProfit - ref)
	}
	f, err := a.execute(ctx, tNewOrderReq, o, execFilled, execPartial)
	if err != nil {
		return broker.TradeReceipt{}, err
	}
	pos := obj(f["position"])
	filled := f64(pos["price"])
	if d := obj(f["deal"]); filled == 0 {
		filled = f64(d["executionPrice"])
	}
	return broker.TradeReceipt{BrokerTradeID: strconv.FormatInt(i64(pos["positionId"]), 10), Symbol: req.Symbol, Units: req.Units,
		FilledPrice: filled, StopLoss: f64(pos["stopLoss"]), TakeProfit: f64(pos["takeProfit"]), OpenedAt: time.Now().UTC(), DecisionID: req.DecisionID, Broker: "ctrader"}, nil
}

// ── reading ──

type book struct {
	positions, orders []map[string]any
}

func (a *Adapter) reconcile(ctx context.Context) (book, error) {
	f, err := a.ask(ctx, tReconcileReq, map[string]any{}, tReconcileRes)
	if err != nil {
		return book{}, err
	}
	return book{positions: objs(f["position"]), orders: objs(f["order"])}, nil
}

// sized is a volume in hundredths as units and, when it isn't whole, its quantity (units then only say the direction).
func sized(volume int64, sell bool) (int, float64) {
	units, qty := int(volume/100), 0.0
	if volume%100 != 0 {
		qty = float64(volume) / 100
		if units == 0 {
			units = 1
		}
	}
	if sell {
		units = -units
	}
	return units, qty
}

func ms(v any) time.Time {
	if n := i64(v); n > 0 {
		return time.UnixMilli(n).UTC()
	}
	return time.Time{}
}

// OpenTrades are the account's open positions, with what each would make if closed now (net of costs).
func (a *Adapter) OpenTrades(ctx context.Context) ([]broker.OpenTrade, error) {
	b, err := a.reconcile(ctx)
	if err != nil {
		return nil, err
	}
	pnl := a.pnl(ctx)
	cur, _ := a.depositCurrency(ctx)
	out := make([]broker.OpenTrade, 0, len(b.positions))
	for _, p := range b.positions {
		td := obj(p["tradeData"])
		sym := i64(td["symbolId"])
		sell := enum(td["tradeSide"], sideNames) == sideSell
		units, qty := sized(i64(td["volume"]), sell)
		t := broker.OpenTrade{BrokerTradeID: strconv.FormatInt(i64(p["positionId"]), 10), Symbol: a.nameOf(sym), Units: units, Quantity: qty,
			OpenPrice: f64(p["price"]), StopLoss: f64(p["stopLoss"]), TakeProfit: f64(p["takeProfit"]), OpenedAt: ms(td["openTimestamp"]), PnLCurrency: cur}
		if v, ok := pnl[i64(p["positionId"])]; ok {
			t.UnrealizedPnL = v
		}
		if bid, ask, err := a.quote(ctx, sym); err == nil {
			if t.CurrentPrice = bid; sell {
				t.CurrentPrice = ask
			}
		}
		out = append(out, t)
	}
	return out, nil
}

// pnl is each open position's profit or loss if closed now, net of costs, in the account's currency.
func (a *Adapter) pnl(ctx context.Context) map[int64]float64 {
	f, err := a.ask(ctx, tUnrealizedPnLReq, map[string]any{}, tUnrealizedPnLRes)
	if err != nil {
		return nil
	}
	div := math.Pow(10, float64(i64(f["moneyDigits"])))
	out := map[int64]float64{}
	for _, p := range objs(f["positionUnrealizedPnL"]) {
		out[i64(p["positionId"])] = float64(i64(p["netUnrealizedPnL"])) / div
	}
	return out
}

// OpenOrders are the account's entry orders waiting for their level (never the stops and targets of positions).
func (a *Adapter) OpenOrders(ctx context.Context) ([]broker.OpenOrder, error) {
	b, err := a.reconcile(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]broker.OpenOrder, 0, len(b.orders))
	for _, o := range b.orders {
		typ := enum(o["orderType"], orderTypeNames)
		if typ != orderLimit && typ != orderStop {
			continue
		}
		td := obj(o["tradeData"])
		sell := enum(td["tradeSide"], sideNames) == sideSell
		units, qty := sized(i64(td["volume"]), sell)
		dir, kind, level := "BUY", "LIMIT", f64(o["limitPrice"])
		if sell {
			dir = "SELL"
		}
		if typ == orderStop {
			kind, level = "STOP", f64(o["stopPrice"])
		}
		out = append(out, broker.OpenOrder{BrokerOrderID: strconv.FormatInt(i64(o["orderId"]), 10), Symbol: a.nameOf(i64(td["symbolId"])),
			Direction: dir, Units: units, Quantity: qty, OrderType: kind, Level: level, StopLoss: f64(o["stopLoss"]), TakeProfit: f64(o["takeProfit"]),
			CreatedAt: ms(td["openTimestamp"])})
	}
	return out, nil
}

func (a *Adapter) depositCurrency(ctx context.Context) (string, error) {
	a.mu.Lock()
	cur := a.currency
	a.mu.Unlock()
	if cur != "" {
		return cur, nil
	}
	f, err := a.ask(ctx, tTraderReq, map[string]any{}, tTraderRes)
	if err != nil {
		return "", err
	}
	asset := i64(obj(f["trader"])["depositAssetId"])
	g, err := a.ask(ctx, tAssetListReq, map[string]any{}, tAssetListRes)
	if err != nil {
		return "", err
	}
	for _, x := range objs(g["asset"]) {
		if i64(x["assetId"]) == asset {
			cur = strings.ToUpper(str(x["name"]))
		}
	}
	a.mu.Lock()
	a.currency = cur
	a.mu.Unlock()
	return cur, nil
}

// Account is the balance, what is open worth now, the margin used and what is free.
func (a *Adapter) Account(ctx context.Context) (broker.BrokerAccount, error) {
	f, err := a.ask(ctx, tTraderReq, map[string]any{}, tTraderRes)
	if err != nil {
		return broker.BrokerAccount{}, err
	}
	tr := obj(f["trader"])
	digits := i64(tr["moneyDigits"])
	balance := float64(i64(tr["balance"])) / math.Pow(10, float64(digits))
	cur, _ := a.depositCurrency(ctx)
	b, err := a.reconcile(ctx)
	if err != nil {
		return broker.BrokerAccount{}, err
	}
	used := 0.0
	for _, p := range b.positions {
		d := digits
		if pd := i64(p["moneyDigits"]); pd > 0 {
			d = pd
		}
		used += float64(i64(p["usedMargin"])) / math.Pow(10, float64(d))
	}
	open := 0.0
	for _, v := range a.pnl(ctx) {
		open += v
	}
	nav := balance + open
	return broker.BrokerAccount{AccountID: strconv.FormatInt(a.account, 10), Balance: balance, NAV: nav, UnrealizedPnL: open,
		MarginUsed: used, MarginAvailable: nav - used, Currency: cur}, nil
}

// ── changing ──

func (a *Adapter) position(ctx context.Context, id string) (map[string]any, error) {
	b, err := a.reconcile(ctx)
	if err != nil {
		return nil, err
	}
	for _, p := range b.positions {
		if strconv.FormatInt(i64(p["positionId"]), 10) == strings.TrimSpace(id) {
			return p, nil
		}
	}
	return nil, &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "POSITION_NOT_FOUND", Err: "ctrader: no open position " + id}
}

func (a *Adapter) order(ctx context.Context, id string) (map[string]any, error) {
	b, err := a.reconcile(ctx)
	if err != nil {
		return nil, err
	}
	for _, o := range b.orders {
		if strconv.FormatInt(i64(o["orderId"]), 10) == strings.TrimSpace(id) {
			return o, nil
		}
	}
	return nil, &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "ORDER_NOT_FOUND", Err: "ctrader: no waiting order " + id}
}

// CloseTrade closes an open position whole, at the market.
func (a *Adapter) CloseTrade(ctx context.Context, id string) error {
	p, err := a.position(ctx, id)
	if err != nil {
		return err
	}
	_, err = a.execute(ctx, tClosePositionReq, map[string]any{"positionId": i64(p["positionId"]), "volume": i64(obj(p["tradeData"])["volume"])},
		execFilled, execPartial)
	return err
}

// AmendTrade moves an open position's stop and/or target; a zero keeps the one it has.
func (a *Adapter) AmendTrade(ctx context.Context, id string, sl, tp float64) error {
	p, err := a.position(ctx, id)
	if err != nil {
		return err
	}
	s, err := a.symbol(ctx, i64(obj(p["tradeData"])["symbolId"]))
	if err != nil {
		return err
	}
	o := map[string]any{"positionId": i64(p["positionId"])}
	if sl = s.price(sl); sl <= 0 {
		sl = f64(p["stopLoss"])
	}
	if tp = s.price(tp); tp <= 0 {
		tp = f64(p["takeProfit"])
	}
	if sl > 0 {
		o["stopLoss"] = sl
	}
	if tp > 0 {
		o["takeProfit"] = tp
	}
	_, err = a.execute(ctx, tAmendPositionReq, o, execReplaced)
	return err
}

// CancelOrder takes a waiting order off the account.
func (a *Adapter) CancelOrder(ctx context.Context, id string) error {
	o, err := a.order(ctx, id)
	if err != nil {
		return err
	}
	_, err = a.execute(ctx, tCancelOrderReq, map[string]any{"orderId": i64(o["orderId"])}, execCancelled)
	return err
}

// AmendOrder moves a waiting order's level, stop and/or target; a zero keeps the one it has.
func (a *Adapter) AmendOrder(ctx context.Context, id string, level, sl, tp float64) error {
	o, err := a.order(ctx, id)
	if err != nil {
		return err
	}
	td := obj(o["tradeData"])
	s, err := a.symbol(ctx, i64(td["symbolId"]))
	if err != nil {
		return err
	}
	req := map[string]any{"orderId": i64(o["orderId"]), "volume": i64(td["volume"])}
	key := "limitPrice"
	if enum(o["orderType"], orderTypeNames) == orderStop {
		key = "stopPrice"
	}
	if level = s.price(level); level <= 0 {
		level = f64(o[key])
	}
	req[key] = level
	if sl = s.price(sl); sl <= 0 {
		sl = f64(o["stopLoss"])
	}
	if tp = s.price(tp); tp <= 0 {
		tp = f64(o["takeProfit"])
	}
	if sl > 0 {
		req["stopLoss"] = sl
	}
	if tp > 0 {
		req["takeProfit"] = tp
	}
	_, err = a.execute(ctx, tAmendOrderReq, req, execReplaced)
	return err
}

// ── what the broker says about a market ──

// MarketDealingRules are a symbol's smallest and largest deal, in units (or coins).
func (a *Adapter) MarketDealingRules(ctx context.Context, sym string) (broker.DealingRules, error) {
	id, err := a.symbolID(ctx, sym)
	if err != nil {
		return broker.DealingRules{}, err
	}
	s, err := a.symbol(ctx, id)
	if err != nil {
		return broker.DealingRules{}, err
	}
	return broker.DealingRules{MinDealSize: float64(s.min) / 100, MaxDealSize: float64(s.max) / 100}, nil
}

// MarketHours says whether a symbol trades now and when it next opens, from its weekly schedule (seconds from Sunday
// 00:00 in its own time zone) and its trading mode.
func (a *Adapter) MarketHours(ctx context.Context, sym string) (broker.MarketHours, error) {
	id, err := a.symbolID(ctx, sym)
	if err != nil {
		return broker.MarketHours{}, err
	}
	s, err := a.symbol(ctx, id)
	if err != nil {
		return broker.MarketHours{}, err
	}
	return s.hours(time.Now()), nil
}

func (s *symbol) hours(now time.Time) broker.MarketHours {
	loc := time.UTC
	if s.zone != "" {
		if l, err := time.LoadLocation(s.zone); err == nil {
			loc = l
		}
	}
	local := now.In(loc)
	sunday := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, loc).AddDate(0, 0, -int(local.Weekday()))
	at := int64(local.Sub(sunday) / time.Second)
	if len(s.schedule) == 0 {
		return broker.MarketHours{Open: s.mode == 0}
	}
	open := false
	for _, iv := range s.schedule {
		if at >= iv[0] && at < iv[1] {
			open = true
		}
	}
	if open && s.mode == 0 {
		return broker.MarketHours{Open: true}
	}
	var next time.Time
	for week := 0; week < 2 && next.IsZero(); week++ {
		for _, iv := range s.schedule {
			t := sunday.AddDate(0, 0, 7*week).Add(time.Duration(iv[0]) * time.Second)
			if t.After(local) && (next.IsZero() || t.Before(next)) {
				next = t
			}
		}
	}
	return broker.MarketHours{Open: false, NextOpen: next.UTC()}
}

// ── refusals ──

// refusal sorts the Open API's error codes into the kinds every broker shares; access that is gone stays ErrAccess.
func refusal(err error) error {
	if errors.Is(err, ErrAccess) {
		return err
	}
	var e *APIError
	if !errors.As(err, &e) {
		return err
	}
	r := &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: e.Code, Err: e.Error()}
	switch e.Code {
	case "MARKET_CLOSED", "SYMBOL_HAS_HOLIDAY":
		r.Kind = broker.RefusedMarketClosed
	case "NOT_ENOUGH_MONEY", "MAX_EXPOSURE_REACHED":
		r.Kind = broker.RefusedNotEnoughMargin
	case "TRADING_BAD_VOLUME":
		r.Kind = broker.RefusedSizeTooSmall
	case "TRADING_BAD_STOPS", "PROTECTION_IS_TOO_CLOSE_TO_MARKET":
		r.Kind = broker.RefusedStopTooClose
	case "TRADING_BAD_PRICES", "TRADING_BAD_EXPIRATION_DATE":
		r.Kind = broker.RefusedLevel
	}
	return r
}
