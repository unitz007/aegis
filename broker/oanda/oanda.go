// Package oanda trades on OANDA (forex and CFDs) through its v20 REST API, with each person's own account id and
// personal access token.
//
// Checked against OANDA's v20 reference (developer.oanda.com/rest-live-v20, 2026-11-23): orders (POST
// /v3/accounts/{id}/orders with {"order": …}, units as a signed string, timeInForce, positionFill, stopLossOnFill and
// takeProfitOnFill with a price; a 201 carries orderFillTransaction.tradeOpened.tradeID, or orderCancelTransaction with
// its reason; a 400 carries errorCode, errorMessage and orderRejectTransaction.rejectReason), pending orders, replacing
// an order (PUT /orders/{id}: the old one is cancelled and a new id made), cancelling (PUT /orders/{id}/cancel), open
// trades (the Trade object's stopLossOrder and takeProfitOrder), closing (PUT /trades/{id}/close {"units":"ALL"}), a
// trade's stop and target (PUT /trades/{id}/orders), the account summary, the account's instruments (minimumTradeSize,
// maximumOrderUnits, tradeUnitsPrecision, displayPrecision, marginRate) and pricing (bids, asks, tradeable), and the
// reject and cancel reasons sorted into the shared kinds. The base URLs (api-fxpractice.oanda.com,
// api-fxtrade.oanda.com) are OANDA's development guide's; the reference pages leave them as a placeholder.
//
// Orders are sent with positionFill OPEN_ONLY, so an order opposite an open trade is refused rather than closing it
// (OANDA's DEFAULT would net them on an account that doesn't hedge): a trade approved is placed as approved, or not at
// all. Replacing a waiting order gives it a new id. When a market is next open OANDA's API doesn't say, so it is left
// unknown. Untried against a real OANDA account: tested only against a pretend v20 API built from the reference.
package oanda

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/unitz007/aegis/broker"
)

// Bases are OANDA's v20 REST API by environment: "demo" is a practice (fxTrade Practice) account.
var Bases = map[string]string{
	"demo": "https://api-fxpractice.oanda.com",
	"live": "https://api-fxtrade.oanda.com",
}

// Adapter is one person's OANDA account.
type Adapter struct {
	base, account, token string
	client               *http.Client

	mu          sync.Mutex
	instruments map[string]*instrument // by normalised name ("EURUSD")
	currency    string
}

var (
	_ broker.Port                 = (*Adapter)(nil)
	_ broker.DealingRulesProvider = (*Adapter)(nil)
	_ broker.MarketHoursProvider  = (*Adapter)(nil)
)

// New is a person's OANDA account on env ("demo" or "live").
func New(env, accountID, token string) (*Adapter, error) {
	base, ok := Bases[env]
	if !ok {
		return nil, fmt.Errorf("oanda: no %q environment", env)
	}
	accountID, token = strings.TrimSpace(accountID), strings.TrimSpace(token)
	if accountID == "" || token == "" {
		return nil, errors.New("oanda: the account id and the access token are both needed")
	}
	return &Adapter{base: base, account: accountID, token: token, client: &http.Client{Timeout: 20 * time.Second}}, nil
}

// Spec is OANDA for a registry: a person types in their account id and a personal access token.
func Spec() broker.Spec {
	return broker.Spec{
		Name:  "oanda",
		Label: "OANDA",
		Envs:  []string{"demo", "live"},
		Fields: []broker.Field{
			{Name: "account_id", Label: "Account ID", Help: "In OANDA's hub, the account's number, like 101-004-1234567-001 (a practice account's for demo)."},
			{Name: "token", Label: "Personal access token", Secret: true, Help: "In OANDA's hub, open Tools → API → Manage API Access and generate a token."},
		},
		Identity: []string{"account_id"},
		New: func(env string, c broker.Credentials) (broker.Port, error) {
			return New(env, c["account_id"], c["token"])
		},
	}
}

// ── HTTP ──

type apiError struct {
	status       int
	code, reason string // errorCode, and a reject or cancel reason when there is one
	message      string
	path         string
}

func (e *apiError) Error() string {
	why := e.reason
	if why == "" {
		why = e.code
	}
	return fmt.Sprintf("oanda %s → HTTP %d: %s %s", e.path, e.status, why, e.message)
}

func (a *Adapter) call(ctx context.Context, method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.base+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Datetime-Format", "RFC3339")
	res, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("oanda http: %w", err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(res.Body, 4<<20))
	if res.StatusCode >= 400 {
		var e struct {
			ErrorCode    string `json:"errorCode"`
			ErrorMessage string `json:"errorMessage"`
			Reject       struct {
				RejectReason string `json:"rejectReason"`
			} `json:"orderRejectTransaction"`
		}
		_ = json.Unmarshal(raw, &e)
		if e.ErrorMessage == "" && e.ErrorCode == "" {
			e.ErrorMessage = strings.TrimSpace(string(raw))
		}
		return refusalOf(&apiError{status: res.StatusCode, code: e.ErrorCode, reason: e.Reject.RejectReason, message: e.ErrorMessage, path: path})
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("oanda decode %s: %w", path, err)
		}
	}
	return nil
}

func (a *Adapter) acct(path string) string { return "/v3/accounts/" + url.PathEscape(a.account) + path }

// num reads OANDA's decimal strings (and numbers, for safety).
func num(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case string:
		n, _ := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return n
	}
	return 0
}

// ── instruments ──

type instrument struct {
	name              string
	unitsPrecision    int
	pricePrecision    int
	minSize, maxUnits float64
	marginRate        float64
}

func norm(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '/', '-', '_', ' ', '.':
			return -1
		}
		return r
	}, strings.ToUpper(strings.TrimSpace(s)))
}

// instrument is the account's instrument a symbol names (EUR/USD, EURUSD and EUR_USD are EUR_USD), from the account's
// own list (read once).
func (a *Adapter) instrument(ctx context.Context, symbol string) (*instrument, error) {
	a.mu.Lock()
	loaded := a.instruments != nil
	in := a.instruments[norm(symbol)]
	a.mu.Unlock()
	if in != nil {
		return in, nil
	}
	if !loaded {
		var r struct {
			Instruments []struct {
				Name                string `json:"name"`
				DisplayPrecision    int    `json:"displayPrecision"`
				TradeUnitsPrecision int    `json:"tradeUnitsPrecision"`
				MinimumTradeSize    any    `json:"minimumTradeSize"`
				MaximumOrderUnits   any    `json:"maximumOrderUnits"`
				MarginRate          any    `json:"marginRate"`
			} `json:"instruments"`
		}
		if err := a.call(ctx, http.MethodGet, a.acct("/instruments"), nil, &r); err != nil {
			return nil, err
		}
		all := map[string]*instrument{}
		for _, i := range r.Instruments {
			all[norm(i.Name)] = &instrument{name: i.Name, unitsPrecision: i.TradeUnitsPrecision, pricePrecision: i.DisplayPrecision,
				minSize: num(i.MinimumTradeSize), maxUnits: num(i.MaximumOrderUnits), marginRate: num(i.MarginRate)}
		}
		a.mu.Lock()
		a.instruments = all
		in = all[norm(symbol)]
		a.mu.Unlock()
	}
	if in == nil {
		return nil, &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "INSTRUMENT_NOT_FOUND", Err: "oanda: this account has no instrument " + symbol}
	}
	return in, nil
}

// symbolOf is an instrument as people say it: EUR_USD is EURUSD.
func symbolOf(name string) string { return strings.ReplaceAll(name, "_", "") }

// price is a level as OANDA takes it: a string at the instrument's precision.
func (in *instrument) price(p float64) string {
	return strconv.FormatFloat(p, 'f', in.pricePrecision, 64)
}

// units is a size at the instrument's precision, rounded down (never raised); below its smallest it is refused.
func (in *instrument) units(size float64) (float64, error) {
	p := math.Pow(10, float64(in.unitsPrecision))
	u := math.Floor(math.Abs(size)*p+1e-9) / p
	if u <= 0 || (in.minSize > 0 && u < in.minSize) {
		return 0, &broker.Refused{Kind: broker.RefusedSizeTooSmall, Limit: in.minSize, BrokerSaid: "UNITS_MINIMUM_NOT_MET",
			Err: fmt.Sprintf("oanda: %g units is below the smallest trade for %s (%g)", math.Abs(size), in.name, in.minSize)}
	}
	if in.maxUnits > 0 && u > in.maxUnits {
		return 0, &broker.Refused{Kind: broker.RefusedSizeTooLarge, Limit: in.maxUnits, BrokerSaid: "UNITS_LIMIT_EXCEEDED",
			Err: fmt.Sprintf("oanda: %g units is above the largest order for %s (%g)", u, in.name, in.maxUnits)}
	}
	return u, nil
}

func (in *instrument) signed(u float64, sell bool) string {
	if sell {
		u = -u
	}
	return strconv.FormatFloat(u, 'f', in.unitsPrecision, 64)
}

// ── prices ──

type quote struct {
	bid, ask  float64
	tradeable bool
}

func (a *Adapter) quotes(ctx context.Context, names []string) (map[string]quote, error) {
	var r struct {
		Prices []struct {
			Instrument string `json:"instrument"`
			Tradeable  *bool  `json:"tradeable"`
			Status     string `json:"status"`
			Bids       []struct {
				Price any `json:"price"`
			} `json:"bids"`
			Asks []struct {
				Price any `json:"price"`
			} `json:"asks"`
		} `json:"prices"`
	}
	if err := a.call(ctx, http.MethodGet, a.acct("/pricing?instruments="+url.QueryEscape(strings.Join(names, ","))), nil, &r); err != nil {
		return nil, err
	}
	out := map[string]quote{}
	for _, p := range r.Prices {
		q := quote{}
		if len(p.Bids) > 0 {
			q.bid = num(p.Bids[0].Price)
		}
		if len(p.Asks) > 0 {
			q.ask = num(p.Asks[0].Price)
		}
		if p.Tradeable != nil {
			q.tradeable = *p.Tradeable
		} else {
			q.tradeable = strings.EqualFold(p.Status, "tradeable")
		}
		out[p.Instrument] = q
	}
	return out, nil
}

func (a *Adapter) quote(ctx context.Context, in *instrument) (quote, error) {
	qs, err := a.quotes(ctx, []string{in.name})
	if err != nil {
		return quote{}, err
	}
	q, ok := qs[in.name]
	if !ok {
		return quote{}, fmt.Errorf("oanda: no price for %s", in.name)
	}
	return q, nil
}

// LiveQuote is an instrument's bid and ask.
func (a *Adapter) LiveQuote(ctx context.Context, symbol string) (float64, float64, error) {
	in, err := a.instrument(ctx, symbol)
	if err != nil {
		return 0, 0, err
	}
	q, err := a.quote(ctx, in)
	if err != nil {
		return 0, 0, err
	}
	if q.bid <= 0 || q.ask <= 0 {
		return 0, 0, fmt.Errorf("oanda: no price for %s", symbol)
	}
	return q.bid, q.ask, nil
}

// ── placing ──

type orderReply struct {
	Create struct {
		ID string `json:"id"`
	} `json:"orderCreateTransaction"`
	Fill *struct {
		ID          string `json:"id"`
		Price       any    `json:"price"`
		TradeOpened *struct {
			TradeID string `json:"tradeID"`
			Price   any    `json:"price"`
		} `json:"tradeOpened"`
	} `json:"orderFillTransaction"`
	Cancel *struct {
		Reason string `json:"reason"`
	} `json:"orderCancelTransaction"`
}

// PlaceTrade places a market order (filled now or not at all), with its stop and target set as it fills, or an entry
// order at a level: a limit when the level is better than the market, a stop when it is beyond it. OPEN_ONLY: never
// closes an opposite trade instead.
func (a *Adapter) PlaceTrade(ctx context.Context, req broker.TradeRequest) (broker.TradeReceipt, error) {
	in, err := a.instrument(ctx, req.Symbol)
	if err != nil {
		return broker.TradeReceipt{}, err
	}
	size := float64(req.Units)
	if req.Quantity != 0 {
		size = req.Quantity
	}
	u, err := in.units(size)
	if err != nil {
		return broker.TradeReceipt{}, err
	}
	sell := req.Units < 0 || (req.Units == 0 && req.Quantity < 0)
	order := map[string]any{"instrument": in.name, "units": in.signed(u, sell), "positionFill": "OPEN_ONLY"}
	if req.StopLoss > 0 {
		order["stopLossOnFill"] = map[string]any{"price": in.price(req.StopLoss), "timeInForce": "GTC"}
	}
	if req.TakeProfit > 0 {
		order["takeProfitOnFill"] = map[string]any{"price": in.price(req.TakeProfit), "timeInForce": "GTC"}
	}
	limit := strings.EqualFold(strings.TrimSpace(req.OrderType), "limit")
	if limit {
		if req.EntryPrice <= 0 {
			return broker.TradeReceipt{}, &broker.Refused{Kind: broker.RefusedLevel, BrokerSaid: "PRICE_MISSING", Err: "oanda: an entry order needs its level"}
		}
		q, err := a.quote(ctx, in)
		if err != nil {
			return broker.TradeReceipt{}, err
		}
		typ := "LIMIT"
		if (!sell && q.ask > 0 && req.EntryPrice > q.ask) || (sell && q.bid > 0 && req.EntryPrice < q.bid) {
			typ = "STOP"
		}
		order["type"], order["price"], order["timeInForce"] = typ, in.price(req.EntryPrice), "GTC"
	} else {
		order["type"], order["timeInForce"] = "MARKET", "FOK"
	}
	var r orderReply
	if err := a.call(ctx, http.MethodPost, a.acct("/orders"), map[string]any{"order": order}, &r); err != nil {
		return broker.TradeReceipt{}, err
	}
	receipt := broker.TradeReceipt{Symbol: req.Symbol, Units: req.Units, StopLoss: req.StopLoss, TakeProfit: req.TakeProfit,
		OpenedAt: time.Now().UTC(), DecisionID: req.DecisionID, Broker: "oanda"}
	if r.Cancel != nil && r.Fill == nil {
		return broker.TradeReceipt{}, refusal(r.Cancel.Reason, "oanda order cancelled: "+r.Cancel.Reason)
	}
	if limit {
		receipt.BrokerTradeID, receipt.FilledPrice = r.Create.ID, req.EntryPrice
		if r.Fill != nil && r.Fill.TradeOpened != nil { // an entry already at its level fills at once
			receipt.BrokerTradeID, receipt.FilledPrice = r.Fill.TradeOpened.TradeID, num(r.Fill.TradeOpened.Price)
		}
		return receipt, nil
	}
	if r.Fill == nil || r.Fill.TradeOpened == nil {
		return broker.TradeReceipt{}, &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "NOT_FILLED", Err: "oanda: the market order opened no trade"}
	}
	receipt.BrokerTradeID, receipt.FilledPrice = r.Fill.TradeOpened.TradeID, num(r.Fill.TradeOpened.Price)
	return receipt, nil
}

// ── reading ──

type v20Trade struct {
	ID           string `json:"id"`
	Instrument   string `json:"instrument"`
	Price        any    `json:"price"`
	OpenTime     string `json:"openTime"`
	CurrentUnits any    `json:"currentUnits"`
	UnrealizedPL any    `json:"unrealizedPL"`
	StopLoss     *struct {
		Price any `json:"price"`
	} `json:"stopLossOrder"`
	TakeProfit *struct {
		Price any `json:"price"`
	} `json:"takeProfitOrder"`
}

func (a *Adapter) trades(ctx context.Context) ([]v20Trade, error) {
	var r struct {
		Trades []v20Trade `json:"trades"`
	}
	err := a.call(ctx, http.MethodGet, a.acct("/openTrades"), nil, &r)
	return r.Trades, err
}

// sized is a signed size as Units and, when it isn't whole, its quantity.
func sized(units float64) (int, float64) {
	u, q := int(units), 0.0
	if units != float64(u) {
		q = math.Abs(units)
		switch {
		case u == 0 && units > 0:
			u = 1
		case u == 0 && units < 0:
			u = -1
		}
	}
	return u, q
}

func at(s string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, s)
	return t
}

// OpenTrades are the account's open trades, each with its stop, target and what it would make if closed now (in the
// account's currency).
func (a *Adapter) OpenTrades(ctx context.Context) ([]broker.OpenTrade, error) {
	ts, err := a.trades(ctx)
	if err != nil {
		return nil, err
	}
	names := map[string]bool{}
	for _, t := range ts {
		names[t.Instrument] = true
	}
	qs := map[string]quote{}
	if len(names) > 0 {
		list := make([]string, 0, len(names))
		for n := range names {
			list = append(list, n)
		}
		if qs, err = a.quotes(ctx, list); err != nil {
			return nil, err
		}
	}
	cur, _ := a.accountCurrency(ctx)
	out := make([]broker.OpenTrade, 0, len(ts))
	for _, t := range ts {
		units, qty := sized(num(t.CurrentUnits))
		now := qs[t.Instrument].bid
		if units < 0 {
			now = qs[t.Instrument].ask
		}
		tr := broker.OpenTrade{BrokerTradeID: t.ID, Symbol: symbolOf(t.Instrument), Units: units, Quantity: qty, OpenPrice: num(t.Price),
			CurrentPrice: now, UnrealizedPnL: num(t.UnrealizedPL), PnLCurrency: cur, OpenedAt: at(t.OpenTime)}
		if t.StopLoss != nil {
			tr.StopLoss = num(t.StopLoss.Price)
		}
		if t.TakeProfit != nil {
			tr.TakeProfit = num(t.TakeProfit.Price)
		}
		out = append(out, tr)
	}
	return out, nil
}

type v20Order struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	Instrument  string `json:"instrument"`
	Units       any    `json:"units"`
	Price       any    `json:"price"`
	TimeInForce string `json:"timeInForce"`
	CreateTime  string `json:"createTime"`
	StopLoss    *struct {
		Price any `json:"price"`
	} `json:"stopLossOnFill"`
	TakeProfit *struct {
		Price any `json:"price"`
	} `json:"takeProfitOnFill"`
}

// entry is an order that opens a trade at a level (not a stop or target that belongs to one).
func (o v20Order) entry() bool {
	switch o.Type {
	case "LIMIT", "STOP", "MARKET_IF_TOUCHED":
		return true
	}
	return false
}

func (a *Adapter) pending(ctx context.Context) ([]v20Order, error) {
	var r struct {
		Orders []v20Order `json:"orders"`
	}
	err := a.call(ctx, http.MethodGet, a.acct("/pendingOrders"), nil, &r)
	return r.Orders, err
}

// OpenOrders are the account's entry orders waiting for their level (a trade's own stop and target are on the trade).
func (a *Adapter) OpenOrders(ctx context.Context) ([]broker.OpenOrder, error) {
	os, err := a.pending(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]broker.OpenOrder, 0, len(os))
	for _, o := range os {
		if !o.entry() {
			continue
		}
		units, qty := sized(num(o.Units))
		dir := "BUY"
		if units < 0 {
			dir = "SELL"
		}
		typ := "LIMIT"
		if o.Type != "LIMIT" {
			typ = "STOP"
		}
		oo := broker.OpenOrder{BrokerOrderID: o.ID, Symbol: symbolOf(o.Instrument), Direction: dir, Units: units, Quantity: qty, OrderType: typ,
			Level: num(o.Price), CreatedAt: at(o.CreateTime)}
		if o.StopLoss != nil {
			oo.StopLoss = num(o.StopLoss.Price)
		}
		if o.TakeProfit != nil {
			oo.TakeProfit = num(o.TakeProfit.Price)
		}
		out = append(out, oo)
	}
	return out, nil
}

type summary struct {
	ID              string `json:"id"`
	Currency        string `json:"currency"`
	Balance         any    `json:"balance"`
	NAV             any    `json:"NAV"`
	UnrealizedPL    any    `json:"unrealizedPL"`
	MarginUsed      any    `json:"marginUsed"`
	MarginAvailable any    `json:"marginAvailable"`
}

func (a *Adapter) summary(ctx context.Context) (summary, error) {
	var r struct {
		Account summary `json:"account"`
	}
	if err := a.call(ctx, http.MethodGet, a.acct("/summary"), nil, &r); err != nil {
		return summary{}, err
	}
	a.mu.Lock()
	a.currency = strings.ToUpper(r.Account.Currency)
	a.mu.Unlock()
	return r.Account, nil
}

func (a *Adapter) accountCurrency(ctx context.Context) (string, error) {
	a.mu.Lock()
	c := a.currency
	a.mu.Unlock()
	if c != "" {
		return c, nil
	}
	s, err := a.summary(ctx)
	return strings.ToUpper(s.Currency), err
}

// Account is the account's balance, its worth with what is open, the margin used and what is free.
func (a *Adapter) Account(ctx context.Context) (broker.BrokerAccount, error) {
	s, err := a.summary(ctx)
	if err != nil {
		return broker.BrokerAccount{}, err
	}
	id := s.ID
	if id == "" {
		id = a.account
	}
	return broker.BrokerAccount{AccountID: id, Balance: num(s.Balance), NAV: num(s.NAV), UnrealizedPnL: num(s.UnrealizedPL),
		MarginUsed: num(s.MarginUsed), MarginAvailable: num(s.MarginAvailable), Currency: strings.ToUpper(s.Currency)}, nil
}

// ── changing ──

// CloseTrade closes an open trade whole, at the market.
func (a *Adapter) CloseTrade(ctx context.Context, id string) error {
	var r orderReply
	if err := a.call(ctx, http.MethodPut, a.acct("/trades/"+url.PathEscape(strings.TrimSpace(id))+"/close"), map[string]any{"units": "ALL"}, &r); err != nil {
		return err
	}
	if r.Fill == nil && r.Cancel != nil {
		return refusal(r.Cancel.Reason, "oanda close cancelled: "+r.Cancel.Reason)
	}
	return nil
}

func (a *Adapter) instrumentOfTrade(ctx context.Context, id string) (*instrument, error) {
	ts, err := a.trades(ctx)
	if err != nil {
		return nil, err
	}
	for _, t := range ts {
		if t.ID == strings.TrimSpace(id) {
			return a.instrument(ctx, t.Instrument)
		}
	}
	return nil, &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "TRADE_DOESNT_EXIST", Err: "oanda: no open trade " + id}
}

// AmendTrade moves an open trade's stop and/or target; a zero keeps the one it has.
func (a *Adapter) AmendTrade(ctx context.Context, id string, sl, tp float64) error {
	if sl <= 0 && tp <= 0 {
		return nil
	}
	in, err := a.instrumentOfTrade(ctx, id)
	if err != nil {
		return err
	}
	body := map[string]any{}
	if sl > 0 {
		body["stopLoss"] = map[string]any{"price": in.price(sl), "timeInForce": "GTC"}
	}
	if tp > 0 {
		body["takeProfit"] = map[string]any{"price": in.price(tp), "timeInForce": "GTC"}
	}
	return a.call(ctx, http.MethodPut, a.acct("/trades/"+url.PathEscape(strings.TrimSpace(id))+"/orders"), body, nil)
}

func (a *Adapter) order(ctx context.Context, id string) (*v20Order, error) {
	os, err := a.pending(ctx)
	if err != nil {
		return nil, err
	}
	for i := range os {
		if os[i].ID == strings.TrimSpace(id) && os[i].entry() {
			return &os[i], nil
		}
	}
	return nil, &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "ORDER_DOESNT_EXIST", Err: "oanda: no waiting order " + id}
}

// CancelOrder takes a waiting order off the account.
func (a *Adapter) CancelOrder(ctx context.Context, id string) error {
	o, err := a.order(ctx, id)
	if err != nil {
		return err
	}
	return a.call(ctx, http.MethodPut, a.acct("/orders/"+url.PathEscape(o.ID)+"/cancel"), nil, nil)
}

// AmendOrder moves a waiting order's level, stop and/or target (a zero keeps the one it has), by replacing it: OANDA
// cancels the old order and makes a new one, with a new id.
func (a *Adapter) AmendOrder(ctx context.Context, id string, level, sl, tp float64) error {
	o, err := a.order(ctx, id)
	if err != nil {
		return err
	}
	in, err := a.instrument(ctx, o.Instrument)
	if err != nil {
		return err
	}
	lv := num(o.Price)
	if level > 0 {
		lv = level
	}
	order := map[string]any{"type": o.Type, "instrument": o.Instrument, "units": o.Units, "price": in.price(lv), "timeInForce": "GTC", "positionFill": "OPEN_ONLY"}
	stop, target := 0.0, 0.0
	if o.StopLoss != nil {
		stop = num(o.StopLoss.Price)
	}
	if o.TakeProfit != nil {
		target = num(o.TakeProfit.Price)
	}
	if sl > 0 {
		stop = sl
	}
	if tp > 0 {
		target = tp
	}
	if stop > 0 {
		order["stopLossOnFill"] = map[string]any{"price": in.price(stop), "timeInForce": "GTC"}
	}
	if target > 0 {
		order["takeProfitOnFill"] = map[string]any{"price": in.price(target), "timeInForce": "GTC"}
	}
	return a.call(ctx, http.MethodPut, a.acct("/orders/"+url.PathEscape(o.ID)), map[string]any{"order": order}, nil)
}

// ── what OANDA says about an instrument ──

// MarketDealingRules are an instrument's smallest trade and largest order in units, and its margin rate.
func (a *Adapter) MarketDealingRules(ctx context.Context, symbol string) (broker.DealingRules, error) {
	in, err := a.instrument(ctx, symbol)
	if err != nil {
		return broker.DealingRules{}, err
	}
	return broker.DealingRules{MinDealSize: in.minSize, MaxDealSize: in.maxUnits, MarginPercent: in.marginRate * 100}, nil
}

// MarketHours says whether an instrument trades now (its price's tradeable); when it next opens OANDA's API doesn't
// say, so it is left unknown.
func (a *Adapter) MarketHours(ctx context.Context, symbol string) (broker.MarketHours, error) {
	in, err := a.instrument(ctx, symbol)
	if err != nil {
		return broker.MarketHours{}, err
	}
	q, err := a.quote(ctx, in)
	if err != nil {
		return broker.MarketHours{}, err
	}
	return broker.MarketHours{Open: q.tradeable}, nil
}

// ── refusals ──

func refusalOf(e *apiError) error {
	if e.status == http.StatusUnauthorized || e.status == http.StatusForbidden {
		return &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "UNAUTHORIZED", Err: e.Error()}
	}
	if e.status >= 500 {
		return e
	}
	code := e.reason
	if code == "" {
		code = e.code
	}
	if code == "" {
		code = e.message
	}
	return refusal(code, e.Error())
}

// refusal sorts OANDA's own reasons (a reject reason, an order's cancel reason, an error code) into the kinds every
// broker shares.
func refusal(code, whole string) *broker.Refused {
	r := &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: code, Err: whole}
	c := strings.ToUpper(code)
	switch {
	case strings.Contains(c, "HALTED"), strings.Contains(c, "NOT_TRADEABLE"), strings.Contains(c, "MARKET_CLOSED"):
		r.Kind = broker.RefusedMarketClosed
	case strings.Contains(c, "INSUFFICIENT_MARGIN"), strings.Contains(c, "INSUFFICIENT_LIQUIDITY"), strings.Contains(c, "CLOSEOUT"):
		r.Kind = broker.RefusedNotEnoughMargin
	case strings.Contains(c, "UNITS_MINIMUM"):
		r.Kind = broker.RefusedSizeTooSmall
	case strings.Contains(c, "UNITS_LIMIT"), strings.Contains(c, "POSITION_SIZE_EXCEEDED"), strings.Contains(c, "UNITS_MAXIMUM"):
		r.Kind = broker.RefusedSizeTooLarge
	case strings.Contains(c, "DISTANCE_MINIMUM"):
		r.Kind = broker.RefusedStopTooClose
	case strings.Contains(c, "STOP_LOSS"), strings.Contains(c, "TAKE_PROFIT"), strings.Contains(c, "PRICE"), strings.Contains(c, "BOUNDS"), strings.Contains(c, "LOSING"):
		r.Kind = broker.RefusedLevel
	}
	return r
}
