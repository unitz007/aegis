// Package alpaca trades US stocks and crypto on Alpaca (alpaca.markets) through its Trading API v2, with each person's
// own API key id and secret.
//
// Checked against Alpaca's docs (docs.alpaca.markets, 2026-11-23): the base URLs (paper-api.alpaca.markets,
// api.alpaca.markets; data.alpaca.markets for prices), the APCA-API-KEY-ID and APCA-API-SECRET-KEY headers, GET
// /v2/account (cash, equity, buying_power, initial_margin, currency, account_number), GET /v2/orders (status, nested,
// limit up to 500; the order object and its legs), bracket, OTO and OCO orders (take_profit.limit_price,
// stop_loss.stop_price, day or gtc; OCO only as limit exits of a position), crypto's order types (market and limit,
// gtc or ioc; no stop orders), fractional orders (day only), replacing with PATCH (bracket and OCO legs' prices; not
// OTO), DELETE /v2/positions/{symbol} (answers with the closing order), the latest stock quote
// (/v2/stocks/{symbol}/quotes/latest: quote.bp, quote.ap) and crypto quotes (/v1beta3/crypto/us/latest/quotes:
// quotes[symbol].bp, .ap). Read from Alpaca's SDK docs, not its own reference page (which didn't load): the asset's
// fields (class, tradable, fractionable, min_order_size, min_trade_increment) and the clock (is_open, next_open).
//
// What it does and doesn't:
//   - Stocks: market or entry orders (a limit when the level is better than the market, a stop when beyond it), with
//     a stop and a target attached as a bracket (both) or OTO (one), good till cancelled. A stop and target need whole
//     shares (fractional orders are day orders without legs, so a fractional size with a stop or target is refused).
//     A market order while the market is shut is refused (Alpaca would queue it for the open at whatever price then).
//     A position's id is its symbol: Alpaca keeps one position a symbol, closed whole by DELETE /v2/positions.
//   - Crypto: bought and sold at the market or a limit; Alpaca attaches no stop or target to crypto (it has no stop
//     orders for it), so one asked for is refused rather than placed without it; a sale only of what is held (no going
//     short on crypto).
//   - A stop or target moved on a position replaces its leg (PATCH, a new order id); one it hasn't is placed as an OCO
//     (both), or a stop or limit order (one). Margin is said as 100% with buying power as what is free, so a size is
//     held to what the account can buy.
//
// Untried against a real Alpaca account: tested only against a pretend Alpaca built from the docs. In particular, how a
// filled bracket's legs come back from GET /v2/orders?nested=true is read both ways (under the parent, or on their own).
package alpaca

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

// Bases are Alpaca's Trading API by environment: "demo" is a paper account.
var Bases = map[string]string{
	"demo": "https://paper-api.alpaca.markets",
	"live": "https://api.alpaca.markets",
}

// DataBases are Alpaca's market data API by environment (the same for paper and live).
var DataBases = map[string]string{
	"demo": "https://data.alpaca.markets",
	"live": "https://data.alpaca.markets",
}

// Adapter is one person's Alpaca account.
type Adapter struct {
	base, data, key, secret string
	client                  *http.Client

	mu     sync.Mutex
	assets map[string]*asset
}

var (
	_ broker.Port                 = (*Adapter)(nil)
	_ broker.DealingRulesProvider = (*Adapter)(nil)
	_ broker.MarketHoursProvider  = (*Adapter)(nil)
)

// New is a person's Alpaca account on env ("demo" or "live").
func New(env, keyID, secret string) (*Adapter, error) {
	base, ok := Bases[env]
	if !ok {
		return nil, fmt.Errorf("alpaca: no %q environment", env)
	}
	data := DataBases[env]
	if data == "" {
		data = DataBases["live"]
	}
	keyID, secret = strings.TrimSpace(keyID), strings.TrimSpace(secret)
	if keyID == "" || secret == "" {
		return nil, errors.New("alpaca: the API key id and secret are both needed")
	}
	return &Adapter{base: base, data: data, key: keyID, secret: secret, client: &http.Client{Timeout: 20 * time.Second}, assets: map[string]*asset{}}, nil
}

// Spec is Alpaca for a registry: a person types in their API key id and secret.
func Spec() broker.Spec {
	return broker.Spec{
		Name:  "alpaca",
		Label: "Alpaca",
		Envs:  []string{"demo", "live"},
		Fields: []broker.Field{
			{Name: "key_id", Label: "API key ID", Help: "In Alpaca's dashboard, open API Keys and generate one (from the paper account for demo)."},
			{Name: "secret_key", Label: "Secret key", Secret: true, Help: "Shown once, beside the key ID, when you generate it."},
		},
		Identity: []string{"key_id"},
		New: func(env string, c broker.Credentials) (broker.Port, error) {
			return New(env, c["key_id"], c["secret_key"])
		},
	}
}

// ── HTTP ──

type apiError struct {
	status  int
	code    int
	message string
	path    string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("alpaca %s → HTTP %d: %s", e.path, e.status, e.message)
}

func (a *Adapter) do(ctx context.Context, base, method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, base+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("APCA-API-KEY-ID", a.key)
	req.Header.Set("APCA-API-SECRET-KEY", a.secret)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("alpaca http: %w", err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(res.Body, 4<<20))
	if res.StatusCode >= 400 {
		var e struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(raw, &e)
		if e.Message == "" {
			e.Message = strings.TrimSpace(string(raw))
		}
		return refusalOf(&apiError{status: res.StatusCode, code: e.Code, message: e.Message, path: path})
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("alpaca decode %s: %w", path, err)
		}
	}
	return nil
}

func (a *Adapter) call(ctx context.Context, method, path string, body, out any) error {
	return a.do(ctx, a.base, method, path, body, out)
}

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

func str(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }

// ── assets ──

type asset struct {
	symbol       string // as Alpaca names it: AAPL, BTC/USD
	crypto       bool
	tradable     bool
	fractionable bool
	minSize      float64
	increment    float64
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

// asset is what a symbol is on Alpaca: a stock (AAPL) or a coin (BTC/USD, BTCUSD). It is asked for as written without
// separators, then as a coin pair when that fails.
func (a *Adapter) asset(ctx context.Context, symbol string) (*asset, error) {
	key := norm(symbol)
	a.mu.Lock()
	if as := a.assets[key]; as != nil {
		a.mu.Unlock()
		return as, nil
	}
	a.mu.Unlock()
	var r struct {
		Symbol            string `json:"symbol"`
		Class             string `json:"class"`
		Tradable          bool   `json:"tradable"`
		Fractionable      bool   `json:"fractionable"`
		MinOrderSize      any    `json:"min_order_size"`
		MinTradeIncrement any    `json:"min_trade_increment"`
	}
	tries := []string{key}
	if s := strings.ToUpper(strings.TrimSpace(symbol)); strings.Contains(s, "/") {
		tries = append(tries, s)
	}
	var err error
	for _, try := range tries {
		if err = a.call(ctx, http.MethodGet, "/v2/assets/"+url.PathEscape(try), nil, &r); err == nil {
			break
		}
	}
	if err != nil {
		if rf, ok := broker.AsRefused(err); ok && rf.BrokerSaid == "404" {
			return nil, &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "ASSET_NOT_FOUND", Err: "alpaca: no asset " + symbol}
		}
		return nil, err
	}
	as := &asset{symbol: r.Symbol, crypto: strings.EqualFold(r.Class, "crypto"), tradable: r.Tradable, fractionable: r.Fractionable,
		minSize: num(r.MinOrderSize), increment: num(r.MinTradeIncrement)}
	if as.symbol == "" {
		as.symbol = key
	}
	a.mu.Lock()
	a.assets[key] = as
	a.assets[norm(as.symbol)] = as
	a.mu.Unlock()
	return as, nil
}

// qty is a size as Alpaca takes it, rounded down (never raised): a coin to its increment, a share whole unless it can
// be fractional (to nine places); below the smallest it is refused.
func (as *asset) qty(size float64, whole bool) (float64, error) {
	size = math.Abs(size)
	var q float64
	switch {
	case as.crypto && as.increment > 0:
		q = math.Floor(size/as.increment+1e-9) * as.increment
		q = math.Round(q*1e9) / 1e9
	case as.crypto, as.fractionable && !whole:
		q = math.Floor(size*1e9) / 1e9
	default:
		q = math.Floor(size + 1e-9)
	}
	min := as.minSize
	if !as.crypto && (!as.fractionable || whole) {
		min = 1
	}
	if q <= 0 || (min > 0 && q < min) {
		return 0, &broker.Refused{Kind: broker.RefusedSizeTooSmall, Limit: min, BrokerSaid: "MIN_ORDER_SIZE",
			Err: fmt.Sprintf("alpaca: %g is below the smallest order for %s (%g)", size, as.symbol, min)}
	}
	return q, nil
}

// ── prices and hours ──

func (a *Adapter) quote(ctx context.Context, as *asset) (float64, float64, error) {
	if as.crypto {
		var r struct {
			Quotes map[string]struct {
				BP float64 `json:"bp"`
				AP float64 `json:"ap"`
			} `json:"quotes"`
		}
		if err := a.do(ctx, a.data, http.MethodGet, "/v1beta3/crypto/us/latest/quotes?symbols="+url.QueryEscape(as.symbol), nil, &r); err != nil {
			return 0, 0, err
		}
		q, ok := r.Quotes[as.symbol]
		if !ok || q.BP <= 0 || q.AP <= 0 {
			return 0, 0, fmt.Errorf("alpaca: no price for %s", as.symbol)
		}
		return q.BP, q.AP, nil
	}
	var r struct {
		Quote struct {
			BP float64 `json:"bp"`
			AP float64 `json:"ap"`
		} `json:"quote"`
	}
	if err := a.do(ctx, a.data, http.MethodGet, "/v2/stocks/"+url.PathEscape(as.symbol)+"/quotes/latest", nil, &r); err != nil {
		return 0, 0, err
	}
	if r.Quote.BP <= 0 || r.Quote.AP <= 0 {
		return 0, 0, fmt.Errorf("alpaca: no price for %s", as.symbol)
	}
	return r.Quote.BP, r.Quote.AP, nil
}

// LiveQuote is a stock's or coin's bid and ask.
func (a *Adapter) LiveQuote(ctx context.Context, symbol string) (float64, float64, error) {
	as, err := a.asset(ctx, symbol)
	if err != nil {
		return 0, 0, err
	}
	return a.quote(ctx, as)
}

// MarketHours is the US stock market's clock for a stock (open now, and when it next opens); crypto always trades.
func (a *Adapter) MarketHours(ctx context.Context, symbol string) (broker.MarketHours, error) {
	as, err := a.asset(ctx, symbol)
	if err != nil {
		return broker.MarketHours{}, err
	}
	if as.crypto {
		return broker.MarketHours{Open: true}, nil
	}
	var c struct {
		IsOpen   bool   `json:"is_open"`
		NextOpen string `json:"next_open"`
	}
	if err := a.call(ctx, http.MethodGet, "/v2/clock", nil, &c); err != nil {
		return broker.MarketHours{}, err
	}
	h := broker.MarketHours{Open: c.IsOpen}
	if !c.IsOpen {
		h.NextOpen, _ = time.Parse(time.RFC3339, c.NextOpen)
	}
	return h, nil
}

// MarketDealingRules are a stock's or coin's smallest order and margin as 100% (buying power is what is free).
func (a *Adapter) MarketDealingRules(ctx context.Context, symbol string) (broker.DealingRules, error) {
	as, err := a.asset(ctx, symbol)
	if err != nil {
		return broker.DealingRules{}, err
	}
	min := as.minSize
	if !as.crypto && !as.fractionable {
		min = 1
	}
	return broker.DealingRules{MinDealSize: min, MarginPercent: 100}, nil
}

// ── orders ──

type alpacaOrder struct {
	ID             string        `json:"id"`
	Symbol         string        `json:"symbol"`
	AssetClass     string        `json:"asset_class"`
	Qty            any           `json:"qty"`
	FilledQty      any           `json:"filled_qty"`
	FilledAvgPrice any           `json:"filled_avg_price"`
	Side           string        `json:"side"`
	Type           string        `json:"type"`
	OrderClass     string        `json:"order_class"`
	TimeInForce    string        `json:"time_in_force"`
	LimitPrice     any           `json:"limit_price"`
	StopPrice      any           `json:"stop_price"`
	Status         string        `json:"status"`
	CreatedAt      string        `json:"created_at"`
	Legs           []alpacaOrder `json:"legs"`
}

func (o alpacaOrder) open() bool {
	switch o.Status {
	case "new", "accepted", "pending_new", "partially_filled", "held", "accepted_for_bidding", "pending_replace", "calculated":
		return true
	}
	return false
}

// isStop is an order that waits for a stop price (a stop loss); a limit is a target or an entry.
func (o alpacaOrder) isStop() bool {
	return strings.HasPrefix(o.Type, "stop") || o.Type == "trailing_stop"
}

func (a *Adapter) openOrders(ctx context.Context) ([]alpacaOrder, error) {
	var os []alpacaOrder
	err := a.call(ctx, http.MethodGet, "/v2/orders?status=open&nested=true&limit=500", nil, &os)
	return os, err
}

func side(sell bool) string {
	if sell {
		return "sell"
	}
	return "buy"
}

// PlaceTrade places a market order or an entry order at a level, with the stop and target attached where Alpaca can.
func (a *Adapter) PlaceTrade(ctx context.Context, req broker.TradeRequest) (broker.TradeReceipt, error) {
	as, err := a.asset(ctx, req.Symbol)
	if err != nil {
		return broker.TradeReceipt{}, err
	}
	if !as.tradable {
		return broker.TradeReceipt{}, &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "NOT_TRADABLE", Err: "alpaca: " + as.symbol + " can't be traded on Alpaca"}
	}
	size := float64(req.Units)
	if req.Quantity != 0 {
		size = req.Quantity
	}
	sell := req.Units < 0 || (req.Units == 0 && req.Quantity < 0)
	levels := req.StopLoss > 0 || req.TakeProfit > 0
	if as.crypto && levels {
		return broker.TradeReceipt{}, &broker.Refused{Kind: broker.RefusedLevel, BrokerSaid: "CRYPTO_NO_STOP",
			Err: "alpaca: Alpaca can't attach a stop or a target to a crypto order"}
	}
	q, err := as.qty(size, levels)
	if err != nil {
		if levels && as.fractionable && math.Abs(size) < 1 {
			if rf, ok := broker.AsRefused(err); ok {
				rf.Err = fmt.Sprintf("alpaca: a stop and target need whole shares, and %g is less than one", math.Abs(size))
			}
		}
		return broker.TradeReceipt{}, err
	}
	if as.crypto && sell {
		held, err := a.held(ctx, as)
		if err != nil {
			return broker.TradeReceipt{}, err
		}
		if held+1e-12 < q {
			return broker.TradeReceipt{}, &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "CRYPTO_NO_SHORT",
				Err: fmt.Sprintf("alpaca: Alpaca only sells crypto that is held (%g %s held), it can't go short", held, as.symbol)}
		}
	}
	tif := "gtc"
	if !as.crypto && q != math.Trunc(q) {
		tif = "day" // fractional shares are day orders
	}
	body := map[string]any{"symbol": as.symbol, "qty": str(q), "side": side(sell), "time_in_force": tif}
	limit := strings.EqualFold(strings.TrimSpace(req.OrderType), "limit")
	if limit {
		if req.EntryPrice <= 0 {
			return broker.TradeReceipt{}, &broker.Refused{Kind: broker.RefusedLevel, BrokerSaid: "LEVEL", Err: "alpaca: an entry order needs its level"}
		}
		bid, ask, err := a.quote(ctx, as)
		if err != nil {
			return broker.TradeReceipt{}, err
		}
		beyond := (!sell && req.EntryPrice > ask) || (sell && req.EntryPrice < bid)
		switch {
		case beyond && as.crypto:
			return broker.TradeReceipt{}, &broker.Refused{Kind: broker.RefusedLevel, BrokerSaid: "CRYPTO_NO_STOP",
				Err: "alpaca: Alpaca can't wait for crypto to pass a price (it has no stop orders for crypto)"}
		case beyond:
			body["type"], body["stop_price"] = "stop", str(req.EntryPrice)
		default:
			body["type"], body["limit_price"] = "limit", str(req.EntryPrice)
		}
	} else {
		if !as.crypto {
			h, err := a.MarketHours(ctx, req.Symbol)
			if err != nil {
				return broker.TradeReceipt{}, err
			}
			if !h.Open {
				return broker.TradeReceipt{}, &broker.Refused{Kind: broker.RefusedMarketClosed, BrokerSaid: "MARKET_CLOSED",
					Err: "alpaca: the US stock market is closed, and a market order would wait for the open"}
			}
		}
		body["type"] = "market"
	}
	switch {
	case req.StopLoss > 0 && req.TakeProfit > 0:
		body["order_class"] = "bracket"
		body["take_profit"] = map[string]any{"limit_price": str(req.TakeProfit)}
		body["stop_loss"] = map[string]any{"stop_price": str(req.StopLoss)}
	case req.StopLoss > 0:
		body["order_class"] = "oto"
		body["stop_loss"] = map[string]any{"stop_price": str(req.StopLoss)}
	case req.TakeProfit > 0:
		body["order_class"] = "oto"
		body["take_profit"] = map[string]any{"limit_price": str(req.TakeProfit)}
	}
	var o alpacaOrder
	if err := a.call(ctx, http.MethodPost, "/v2/orders", body, &o); err != nil {
		return broker.TradeReceipt{}, err
	}
	if o.Status == "rejected" || o.Status == "canceled" {
		return broker.TradeReceipt{}, &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: o.Status, Err: "alpaca: the order was " + o.Status}
	}
	r := broker.TradeReceipt{BrokerTradeID: o.ID, Symbol: req.Symbol, Units: req.Units, FilledPrice: num(o.FilledAvgPrice),
		StopLoss: req.StopLoss, TakeProfit: req.TakeProfit, OpenedAt: time.Now().UTC(), DecisionID: req.DecisionID, Broker: "alpaca"}
	if limit && r.FilledPrice == 0 {
		r.FilledPrice = req.EntryPrice
	}
	if !limit {
		r.BrokerTradeID = norm(as.symbol) // a position is known by its symbol
	}
	return r, nil
}

// ── positions ──

type alpacaPosition struct {
	Symbol        string `json:"symbol"`
	AssetClass    string `json:"asset_class"`
	Qty           any    `json:"qty"`
	Side          string `json:"side"`
	AvgEntryPrice any    `json:"avg_entry_price"`
	CurrentPrice  any    `json:"current_price"`
	UnrealizedPL  any    `json:"unrealized_pl"`
}

func (a *Adapter) positions(ctx context.Context) ([]alpacaPosition, error) {
	var ps []alpacaPosition
	err := a.call(ctx, http.MethodGet, "/v2/positions", nil, &ps)
	return ps, err
}

func (a *Adapter) held(ctx context.Context, as *asset) (float64, error) {
	ps, err := a.positions(ctx)
	if err != nil {
		return 0, err
	}
	for _, p := range ps {
		if norm(p.Symbol) == norm(as.symbol) && !strings.EqualFold(p.Side, "short") {
			return math.Abs(num(p.Qty)), nil
		}
	}
	return 0, nil
}

// exits are the open orders that close part or all of a position (the side opposite it), with their legs; an entry's
// own legs (a bracket waiting to fill) are not.
func exits(os []alpacaOrder, ps []alpacaPosition) map[string][]alpacaOrder {
	long := map[string]bool{}
	for _, p := range ps {
		long[norm(p.Symbol)] = !strings.EqualFold(p.Side, "short")
	}
	out := map[string][]alpacaOrder{}
	for _, o := range os {
		sym := norm(o.Symbol)
		isLong, held := long[sym]
		if !held || !o.open() {
			continue
		}
		if (isLong && o.Side == "sell") || (!isLong && o.Side == "buy") {
			out[sym] = append(out[sym], o)
			for _, l := range o.Legs {
				if l.open() {
					out[sym] = append(out[sym], l)
				}
			}
		}
	}
	return out
}

func levelsOf(os []alpacaOrder) (stop, target float64) {
	for _, o := range os {
		if o.isStop() {
			stop = num(o.StopPrice)
		} else if o.Type == "limit" {
			target = num(o.LimitPrice)
		}
	}
	return
}

func sized(q float64, short bool) (int, float64) {
	q = math.Abs(q)
	u, qty := int(q), 0.0
	if q != float64(u) {
		qty = q
		if u == 0 {
			u = 1
		}
	}
	if short {
		u = -u
	}
	return u, qty
}

// OpenTrades are the account's positions (one a symbol, known by it), with the stop and target its exit orders hold.
func (a *Adapter) OpenTrades(ctx context.Context) ([]broker.OpenTrade, error) {
	ps, err := a.positions(ctx)
	if err != nil {
		return nil, err
	}
	os, err := a.openOrders(ctx)
	if err != nil {
		return nil, err
	}
	ex := exits(os, ps)
	out := make([]broker.OpenTrade, 0, len(ps))
	for _, p := range ps {
		units, qty := sized(num(p.Qty), strings.EqualFold(p.Side, "short"))
		stop, target := levelsOf(ex[norm(p.Symbol)])
		out = append(out, broker.OpenTrade{BrokerTradeID: norm(p.Symbol), Symbol: norm(p.Symbol), Units: units, Quantity: qty, OpenPrice: num(p.AvgEntryPrice),
			CurrentPrice: num(p.CurrentPrice), UnrealizedPnL: num(p.UnrealizedPL), PnLCurrency: "USD", StopLoss: stop, TakeProfit: target})
	}
	return out, nil
}

// OpenOrders are the account's entry orders waiting for their level, each with its bracket's stop and target.
func (a *Adapter) OpenOrders(ctx context.Context) ([]broker.OpenOrder, error) {
	os, err := a.openOrders(ctx)
	if err != nil {
		return nil, err
	}
	ps, err := a.positions(ctx)
	if err != nil {
		return nil, err
	}
	isExit := map[string]bool{}
	for _, list := range exits(os, ps) {
		for _, o := range list {
			isExit[o.ID] = true
		}
	}
	out := make([]broker.OpenOrder, 0, len(os))
	for _, o := range os {
		if isExit[o.ID] || !o.open() {
			continue
		}
		units, qty := sized(num(o.Qty), o.Side == "sell")
		level, typ := num(o.LimitPrice), "LIMIT"
		if o.isStop() {
			level, typ = num(o.StopPrice), "STOP"
		}
		stop, target := levelsOf(o.Legs)
		created, _ := time.Parse(time.RFC3339Nano, o.CreatedAt)
		out = append(out, broker.OpenOrder{BrokerOrderID: o.ID, Symbol: norm(o.Symbol), Direction: strings.ToUpper(o.Side), Units: units, Quantity: qty,
			OrderType: typ, Level: level, StopLoss: stop, TakeProfit: target, CreatedAt: created})
	}
	return out, nil
}

// Account is the account's worth without what is open, with it, the margin held and the buying power free.
func (a *Adapter) Account(ctx context.Context) (broker.BrokerAccount, error) {
	var r struct {
		ID            string `json:"id"`
		AccountNumber string `json:"account_number"`
		Currency      string `json:"currency"`
		Equity        any    `json:"equity"`
		BuyingPower   any    `json:"buying_power"`
		InitialMargin any    `json:"initial_margin"`
	}
	if err := a.call(ctx, http.MethodGet, "/v2/account", nil, &r); err != nil {
		return broker.BrokerAccount{}, err
	}
	ps, err := a.positions(ctx)
	if err != nil {
		return broker.BrokerAccount{}, err
	}
	open := 0.0
	for _, p := range ps {
		open += num(p.UnrealizedPL)
	}
	open = math.Round(open*100) / 100
	id := r.AccountNumber
	if id == "" {
		id = r.ID
	}
	cur := strings.ToUpper(r.Currency)
	if cur == "" {
		cur = "USD"
	}
	eq := num(r.Equity)
	return broker.BrokerAccount{AccountID: id, Balance: eq - open, NAV: eq, UnrealizedPnL: open, MarginUsed: num(r.InitialMargin),
		MarginAvailable: num(r.BuyingPower), Currency: cur}, nil
}

// ── changing ──

func (a *Adapter) position(ctx context.Context, id string) (*alpacaPosition, []alpacaOrder, error) {
	ps, err := a.positions(ctx)
	if err != nil {
		return nil, nil, err
	}
	for i := range ps {
		if norm(ps[i].Symbol) == norm(id) {
			os, err := a.openOrders(ctx)
			if err != nil {
				return nil, nil, err
			}
			return &ps[i], exits(os, ps)[norm(id)], nil
		}
	}
	return nil, nil, &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "POSITION_NOT_FOUND", Err: "alpaca: no open position " + id}
}

// CloseTrade closes a position whole at the market, taking its stop and target off first (they hold its shares).
func (a *Adapter) CloseTrade(ctx context.Context, id string) error {
	p, ex, err := a.position(ctx, id)
	if err != nil {
		return err
	}
	for _, o := range ex {
		if err := a.call(ctx, http.MethodDelete, "/v2/orders/"+url.PathEscape(o.ID), nil, nil); err != nil {
			return err
		}
	}
	return a.call(ctx, http.MethodDelete, "/v2/positions/"+url.PathEscape(norm(p.Symbol)), nil, nil)
}

// AmendTrade moves a position's stop and/or target (a zero keeps the one it has): a leg it has is replaced; one it
// hasn't is placed as an exit for the whole position.
func (a *Adapter) AmendTrade(ctx context.Context, id string, sl, tp float64) error {
	if sl <= 0 && tp <= 0 {
		return nil
	}
	p, ex, err := a.position(ctx, id)
	if err != nil {
		return err
	}
	crypto := strings.EqualFold(p.AssetClass, "crypto")
	if crypto && sl > 0 {
		return &broker.Refused{Kind: broker.RefusedLevel, BrokerSaid: "CRYPTO_NO_STOP", Err: "alpaca: Alpaca can't hold a stop on crypto"}
	}
	var stopOrder, targetOrder *alpacaOrder
	for i := range ex {
		if ex[i].isStop() {
			stopOrder = &ex[i]
		} else if ex[i].Type == "limit" {
			targetOrder = &ex[i]
		}
	}
	short := strings.EqualFold(p.Side, "short")
	exitSide := side(!short)
	qty := str(math.Abs(num(p.Qty)))
	switch {
	case stopOrder == nil && targetOrder == nil && sl > 0 && tp > 0:
		return a.call(ctx, http.MethodPost, "/v2/orders", map[string]any{"symbol": p.Symbol, "qty": qty, "side": exitSide, "type": "limit",
			"time_in_force": "gtc", "order_class": "oco", "take_profit": map[string]any{"limit_price": str(tp)}, "stop_loss": map[string]any{"stop_price": str(sl)}}, nil)
	case (sl > 0 && stopOrder == nil && targetOrder != nil) || (tp > 0 && targetOrder == nil && stopOrder != nil):
		return &broker.Refused{Kind: broker.RefusedLevel, BrokerSaid: "ONE_LEG",
			Err: "alpaca: Alpaca can't add a stop or a target beside one the position already has on its own: set both at once instead"}
	}
	if sl > 0 {
		if stopOrder != nil {
			if err := a.call(ctx, http.MethodPatch, "/v2/orders/"+url.PathEscape(stopOrder.ID), map[string]any{"stop_price": str(sl)}, nil); err != nil {
				return err
			}
		} else if err := a.call(ctx, http.MethodPost, "/v2/orders", map[string]any{"symbol": p.Symbol, "qty": qty, "side": exitSide, "type": "stop",
			"stop_price": str(sl), "time_in_force": "gtc"}, nil); err != nil {
			return err
		}
	}
	if tp > 0 {
		if targetOrder != nil {
			return a.call(ctx, http.MethodPatch, "/v2/orders/"+url.PathEscape(targetOrder.ID), map[string]any{"limit_price": str(tp)}, nil)
		}
		return a.call(ctx, http.MethodPost, "/v2/orders", map[string]any{"symbol": p.Symbol, "qty": qty, "side": exitSide, "type": "limit",
			"limit_price": str(tp), "time_in_force": "gtc"}, nil)
	}
	return nil
}

func (a *Adapter) entry(ctx context.Context, id string) (*alpacaOrder, error) {
	os, err := a.OpenOrders(ctx)
	if err != nil {
		return nil, err
	}
	for _, o := range os {
		if o.BrokerOrderID == strings.TrimSpace(id) {
			all, err := a.openOrders(ctx)
			if err != nil {
				return nil, err
			}
			for i := range all {
				if all[i].ID == o.BrokerOrderID {
					return &all[i], nil
				}
			}
		}
	}
	return nil, &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "ORDER_NOT_FOUND", Err: "alpaca: no waiting order " + id}
}

// CancelOrder takes a waiting order (and its bracket) off the account.
func (a *Adapter) CancelOrder(ctx context.Context, id string) error {
	o, err := a.entry(ctx, id)
	if err != nil {
		return err
	}
	return a.call(ctx, http.MethodDelete, "/v2/orders/"+url.PathEscape(o.ID), nil, nil)
}

// AmendOrder moves a waiting order's level, stop and/or target (a zero keeps the one it has), by replacing each part
// (Alpaca gives a replaced order a new id; an OTO's legs can't be replaced, which Alpaca refuses).
func (a *Adapter) AmendOrder(ctx context.Context, id string, level, sl, tp float64) error {
	o, err := a.entry(ctx, id)
	if err != nil {
		return err
	}
	for _, l := range o.Legs {
		switch {
		case sl > 0 && l.isStop():
			if err := a.call(ctx, http.MethodPatch, "/v2/orders/"+url.PathEscape(l.ID), map[string]any{"stop_price": str(sl)}, nil); err != nil {
				return err
			}
			sl = 0
		case tp > 0 && l.Type == "limit":
			if err := a.call(ctx, http.MethodPatch, "/v2/orders/"+url.PathEscape(l.ID), map[string]any{"limit_price": str(tp)}, nil); err != nil {
				return err
			}
			tp = 0
		}
	}
	if sl > 0 || tp > 0 {
		return &broker.Refused{Kind: broker.RefusedLevel, BrokerSaid: "NO_LEG", Err: "alpaca: that order has no stop or target to move; cancel it and place it again with one"}
	}
	if level > 0 {
		field := "limit_price"
		if o.isStop() {
			field = "stop_price"
		}
		return a.call(ctx, http.MethodPatch, "/v2/orders/"+url.PathEscape(o.ID), map[string]any{field: str(level)}, nil)
	}
	return nil
}

// ── refusals ──

func refusalOf(e *apiError) error {
	switch {
	case e.status == http.StatusUnauthorized:
		return &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "UNAUTHORIZED", Err: e.Error()}
	case e.status == http.StatusNotFound:
		return &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "404", Err: e.Error()}
	case e.status >= 500 || e.status == http.StatusTooManyRequests:
		return e
	}
	return refusal(e.message, e.Error())
}

// refusal sorts Alpaca's own messages into the kinds every broker shares.
func refusal(msg, whole string) *broker.Refused {
	r := &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: msg, Err: whole}
	m := strings.ToLower(msg)
	switch {
	case strings.Contains(m, "market is closed"), strings.Contains(m, "market hours"), strings.Contains(m, "not open"):
		r.Kind = broker.RefusedMarketClosed
	case strings.Contains(m, "buying power"), strings.Contains(m, "insufficient"):
		r.Kind = broker.RefusedNotEnoughMargin
	case strings.Contains(m, "minimum"), strings.Contains(m, "min order"), strings.Contains(m, "qty must be"), strings.Contains(m, "notional"):
		r.Kind = broker.RefusedSizeTooSmall
	case strings.Contains(m, "stop_price"), strings.Contains(m, "limit_price"), strings.Contains(m, "take_profit"), strings.Contains(m, "stop_loss"),
		strings.Contains(m, "sub-penny"), strings.Contains(m, "base price"):
		r.Kind = broker.RefusedLevel
	}
	return r
}
