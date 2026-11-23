// Package binance trades spot crypto on Binance through its Spot REST API, with each person's own API key and secret
// (requests signed with HMAC-SHA256). Spot only: no futures, no margin.
//
// Checked against Binance's Spot API docs (developers.binance.com/docs/binance-spot-api-docs, 2026-11-23): signed
// requests (timestamp and recvWindow in the query, signature the hex HMAC-SHA256 of the query with the secret, the key
// in X-MBX-APIKEY), POST /api/v3/order (MARKET with quantity; LIMIT with timeInForce and price; STOP_LOSS and
// STOP_LOSS_LIMIT with stopPrice; LIMIT_MAKER), POST /api/v3/orderList/oco (aboveType LIMIT_MAKER above, belowType
// STOP_LOSS or STOP_LOSS_LIMIT below), DELETE /api/v3/order and DELETE /api/v3/orderList, POST
// /api/v3/order/cancelReplace, the OPO list's rule that a working order must be LIMIT or LIMIT_MAKER. From Binance's
// docs as known but whose page section didn't load here: POST /api/v3/orderList/otoco and /oto (working LIMIT,
// pending legs placed once it fills; pendingAbove*/pendingBelow* and pending* fields), GET /api/v3/exchangeInfo
// filters (LOT_SIZE, PRICE_FILTER, NOTIONAL/MIN_NOTIONAL), /api/v3/account, /api/v3/openOrders, /api/v3/myTrades,
// /api/v3/ticker/bookTicker and /api/v3/ticker/price, /api/v3/time, and the error codes (-1013 filter failures, -1021
// timestamp, -1121 invalid symbol, -2010 rejected, -2014/-2015 keys). Testnet is testnet.binance.vision.
//
// What it does and doesn't, spot being what it is:
//   - Buys open a holding; a sell only sells coins that are held (no going short). A pair named in dollars (BTCUSD,
//     BTC/USD) is traded as Binance's USDT pair (BTCUSDT), else USDC, as Binance.com has no USD pairs.
//   - A buy with a stop and/or target is placed as one order list, so the stop and target go with it: OTOCO (both) or
//     OTO (one), with a limit buy as the working order (at the level asked, or at the ask now for a market buy: spot
//     can't attach a stop to a market order, so a "market" buy with a stop rests as a limit at today's ask until it
//     fills). Its stop and target sell 99.9% of what was bought, leaving room for Binance's fee when it is taken in the
//     coin. A buy waiting above the market (a stop entry) isn't offered.
//   - A holding is a trade (its id the pair, BTCUSDT); its open price is the average of the buys since it was last
//     empty (from myTrades: 0 when it came by deposit); its stop and target are the sell orders on it.
//   - Moving a holding's stop or target cancels its sell orders and places new ones (Binance can't change an order in
//     place; if the new ones are refused, the old ones are put back). A waiting buy without a stop or target is moved
//     with cancelReplace; one with them can't be (cancel it and place it again).
//   - The account is valued in USDT: its coins at their bid; what is free is its free USDT; margin is 100%.
//
// Untried against a real Binance account or its testnet: tested only against a pretend Binance built from the docs.
package binance

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/unitz007/aegis/broker"
)

// Bases are Binance's Spot API by environment: "demo" is the Spot testnet.
var Bases = map[string]string{
	"demo": "https://testnet.binance.vision",
	"live": "https://api.binance.com",
}

// Quote is the coin an account is valued in, and cash: the stablecoins kept as cash beside it.
const Quote = "USDT"

var cash = map[string]bool{"USDT": true, "USDC": true, "FDUSD": true, "BUSD": true, "TUSD": true, "DAI": true}

// feeRoom is what a stop and target leave of a buy, for Binance's 0.1% fee when it is taken in the coin.
const feeRoom = 0.999

// Adapter is one person's Binance spot account.
type Adapter struct {
	base, key, secret string
	client            *http.Client

	mu      sync.Mutex
	offset  time.Duration // Binance's clock less ours
	symbols map[string]*market
	names   map[string]string // what a symbol was asked as → Binance's pair
}

var (
	_ broker.Port                 = (*Adapter)(nil)
	_ broker.DealingRulesProvider = (*Adapter)(nil)
	_ broker.MarketHoursProvider  = (*Adapter)(nil)
)

// New is a person's Binance spot account on env ("demo" for the testnet, or "live").
func New(env, apiKey, secret string) (*Adapter, error) {
	base, ok := Bases[env]
	if !ok {
		return nil, fmt.Errorf("binance: no %q environment", env)
	}
	apiKey, secret = strings.TrimSpace(apiKey), strings.TrimSpace(secret)
	if apiKey == "" || secret == "" {
		return nil, errors.New("binance: the API key and secret are both needed")
	}
	return &Adapter{base: base, key: apiKey, secret: secret, client: &http.Client{Timeout: 20 * time.Second},
		symbols: map[string]*market{}, names: map[string]string{}}, nil
}

// Spec is Binance for a registry: a person types in their API key and secret.
func Spec() broker.Spec {
	return broker.Spec{
		Name:  "binance",
		Label: "Binance",
		Envs:  []string{"demo", "live"},
		Fields: []broker.Field{
			{Name: "api_key", Label: "API key", Help: "In Binance, open Account → API Management and create a key with spot trading allowed (for demo, on testnet.binance.vision)."},
			{Name: "api_secret", Label: "Secret key", Secret: true, Help: "Shown once, beside the API key, when you create it."},
		},
		Identity: []string{"api_key"},
		New: func(env string, c broker.Credentials) (broker.Port, error) {
			return New(env, c["api_key"], c["api_secret"])
		},
	}
}

// ── HTTP ──

type apiError struct {
	status int
	code   int
	msg    string
	path   string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("binance %s → HTTP %d: %d %s", e.path, e.status, e.code, e.msg)
}

// call sends a request; a signed one carries the time and its signature, and is sent once more with Binance's own time
// when Binance says ours is off.
func (a *Adapter) call(ctx context.Context, method, path string, q url.Values, signed bool, out any) error {
	err := a.do(ctx, method, path, q, signed, out)
	var e *apiError
	if signed && errors.As(err, &e) && e.code == -1021 {
		if a.syncTime(ctx) == nil {
			err = a.do(ctx, method, path, q, signed, out)
		}
	}
	if errors.As(err, &e) {
		return refusalOf(e)
	}
	return err
}

func (a *Adapter) do(ctx context.Context, method, path string, q url.Values, signed bool, out any) error {
	v := url.Values{}
	for k, vs := range q {
		v[k] = append([]string(nil), vs...)
	}
	if signed {
		a.mu.Lock()
		now := time.Now().Add(a.offset)
		a.mu.Unlock()
		v.Set("timestamp", strconv.FormatInt(now.UnixMilli(), 10))
		v.Set("recvWindow", "5000")
	}
	query := v.Encode()
	if signed {
		m := hmac.New(sha256.New, []byte(a.secret))
		m.Write([]byte(query))
		query += "&signature=" + hex.EncodeToString(m.Sum(nil))
	}
	full := a.base + path
	if query != "" {
		full += "?" + query
	}
	req, err := http.NewRequestWithContext(ctx, method, full, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-MBX-APIKEY", a.key)
	res, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("binance http: %w", err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(res.Body, 8<<20))
	if res.StatusCode >= 400 {
		var e struct {
			Code int    `json:"code"`
			Msg  string `json:"msg"`
		}
		_ = json.Unmarshal(raw, &e)
		if e.Msg == "" {
			e.Msg = strings.TrimSpace(string(raw))
		}
		return &apiError{status: res.StatusCode, code: e.Code, msg: e.Msg, path: path}
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("binance decode %s: %w", path, err)
		}
	}
	return nil
}

func (a *Adapter) syncTime(ctx context.Context) error {
	var t struct {
		ServerTime int64 `json:"serverTime"`
	}
	if err := a.do(ctx, http.MethodGet, "/api/v3/time", nil, false, &t); err != nil {
		return err
	}
	a.mu.Lock()
	a.offset = time.Until(time.UnixMilli(t.ServerTime))
	a.mu.Unlock()
	return nil
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

// ── markets ──

type market struct {
	symbol, base, quote string
	status              string
	minQty, maxQty      float64
	step, tick          float64
	minNotional         float64
	stopLoss            bool // STOP_LOSS (a stop that sells at the market) is allowed
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

// market is the pair a symbol trades as: as written, then a dollar pair as USDT, then USDC.
func (a *Adapter) market(ctx context.Context, symbol string) (*market, error) {
	key := norm(symbol)
	a.mu.Lock()
	if n := a.names[key]; n != "" {
		m := a.symbols[n]
		a.mu.Unlock()
		return m, nil
	}
	a.mu.Unlock()
	tries := []string{key}
	if strings.HasSuffix(key, "USD") {
		tries = append(tries, key+"T", key+"C")
	}
	for _, try := range tries {
		m, err := a.exchangeInfo(ctx, try)
		if err != nil {
			if rf, ok := broker.AsRefused(err); ok && rf.BrokerSaid == "-1121" {
				continue
			}
			return nil, err
		}
		a.mu.Lock()
		a.names[key], a.symbols[m.symbol] = m.symbol, m
		a.mu.Unlock()
		return m, nil
	}
	return nil, &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "-1121", Err: "binance: no spot pair for " + symbol}
}

func (a *Adapter) exchangeInfo(ctx context.Context, symbol string) (*market, error) {
	var r struct {
		Symbols []struct {
			Symbol     string   `json:"symbol"`
			Status     string   `json:"status"`
			BaseAsset  string   `json:"baseAsset"`
			QuoteAsset string   `json:"quoteAsset"`
			OrderTypes []string `json:"orderTypes"`
			Filters    []struct {
				FilterType  string `json:"filterType"`
				MinQty      string `json:"minQty"`
				MaxQty      string `json:"maxQty"`
				StepSize    string `json:"stepSize"`
				TickSize    string `json:"tickSize"`
				MinNotional string `json:"minNotional"`
			} `json:"filters"`
		} `json:"symbols"`
	}
	if err := a.call(ctx, http.MethodGet, "/api/v3/exchangeInfo", url.Values{"symbol": {symbol}}, false, &r); err != nil {
		return nil, err
	}
	for _, s := range r.Symbols {
		if s.Symbol != symbol {
			continue
		}
		m := &market{symbol: s.Symbol, base: s.BaseAsset, quote: s.QuoteAsset, status: s.Status}
		for _, t := range s.OrderTypes {
			m.stopLoss = m.stopLoss || t == "STOP_LOSS"
		}
		for _, f := range s.Filters {
			switch f.FilterType {
			case "LOT_SIZE":
				m.minQty, m.maxQty, m.step = num(f.MinQty), num(f.MaxQty), num(f.StepSize)
			case "PRICE_FILTER":
				m.tick = num(f.TickSize)
			case "NOTIONAL", "MIN_NOTIONAL":
				m.minNotional = num(f.MinNotional)
			}
		}
		return m, nil
	}
	return nil, &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "-1121", Err: "binance: no spot pair " + symbol}
}

// places is how many decimals a step has (0.001 → 3).
func places(step float64) int {
	if step <= 0 {
		return 8
	}
	p := 0
	for step < 1-1e-12 && p < 12 {
		step *= 10
		p++
	}
	return p
}

// qty is a quantity rounded down to the pair's step (never raised).
func (m *market) qty(q float64) float64 {
	q = math.Abs(q)
	if m.step > 0 {
		q = math.Floor(q/m.step+1e-9) * m.step
	}
	p := math.Pow(10, float64(places(m.step)))
	return math.Round(q*p) / p
}

func (m *market) qtyStr(q float64) string { return strconv.FormatFloat(q, 'f', places(m.step), 64) }

// price is a level at the pair's tick, as Binance takes it.
func (m *market) price(p float64) string {
	if m.tick > 0 {
		p = math.Round(p/m.tick) * m.tick
	}
	return strconv.FormatFloat(p, 'f', places(m.tick), 64)
}

// sized holds a quantity to the pair's rules at a price; below the smallest it is refused, never raised.
func (m *market) sized(size, price float64) (float64, error) {
	q := m.qty(size)
	min := m.minQty
	if m.minNotional > 0 && price > 0 && m.minNotional/price > min {
		min = m.minNotional / price
	}
	if q <= 0 || q+1e-12 < min {
		return 0, &broker.Refused{Kind: broker.RefusedSizeTooSmall, Limit: min, BrokerSaid: "LOT_SIZE",
			Err: fmt.Sprintf("binance: %g %s is below the smallest order (%g)", math.Abs(size), m.base, min)}
	}
	if m.maxQty > 0 && q > m.maxQty {
		return 0, &broker.Refused{Kind: broker.RefusedSizeTooLarge, Limit: m.maxQty, BrokerSaid: "LOT_SIZE",
			Err: fmt.Sprintf("binance: %g %s is above the largest order (%g)", q, m.base, m.maxQty)}
	}
	return q, nil
}

// ── prices and hours ──

func (a *Adapter) book(ctx context.Context, symbol string) (float64, float64, error) {
	var r struct {
		BidPrice string `json:"bidPrice"`
		AskPrice string `json:"askPrice"`
	}
	if err := a.call(ctx, http.MethodGet, "/api/v3/ticker/bookTicker", url.Values{"symbol": {symbol}}, false, &r); err != nil {
		return 0, 0, err
	}
	bid, ask := num(r.BidPrice), num(r.AskPrice)
	if bid <= 0 || ask <= 0 {
		return 0, 0, fmt.Errorf("binance: no price for %s", symbol)
	}
	return bid, ask, nil
}

// LiveQuote is a pair's best bid and ask.
func (a *Adapter) LiveQuote(ctx context.Context, symbol string) (float64, float64, error) {
	m, err := a.market(ctx, symbol)
	if err != nil {
		return 0, 0, err
	}
	return a.book(ctx, m.symbol)
}

// MarketHours says whether a pair trades now (its status TRADING); crypto has no opening hours, so when a halted pair
// trades again is unknown.
func (a *Adapter) MarketHours(ctx context.Context, symbol string) (broker.MarketHours, error) {
	m, err := a.market(ctx, symbol)
	if err != nil {
		return broker.MarketHours{}, err
	}
	return broker.MarketHours{Open: m.status == "TRADING"}, nil
}

// MarketDealingRules are a pair's smallest and largest order in the coin; margin is 100% (spot).
func (a *Adapter) MarketDealingRules(ctx context.Context, symbol string) (broker.DealingRules, error) {
	m, err := a.market(ctx, symbol)
	if err != nil {
		return broker.DealingRules{}, err
	}
	return broker.DealingRules{MinDealSize: m.minQty, MaxDealSize: m.maxQty, MarginPercent: 100}, nil
}

// ── the account ──

type balance struct {
	Asset  string `json:"asset"`
	Free   string `json:"free"`
	Locked string `json:"locked"`
}

func (a *Adapter) balances(ctx context.Context) (string, []balance, error) {
	var r struct {
		UID      any       `json:"uid"`
		CanTrade bool      `json:"canTrade"`
		Balances []balance `json:"balances"`
	}
	if err := a.call(ctx, http.MethodGet, "/api/v3/account", nil, true, &r); err != nil {
		return "", nil, err
	}
	uid := ""
	switch v := r.UID.(type) {
	case float64:
		uid = strconv.FormatInt(int64(v), 10)
	case string:
		uid = v
	}
	return uid, r.Balances, nil
}

func (a *Adapter) free(ctx context.Context, asset string) (float64, error) {
	_, bs, err := a.balances(ctx)
	if err != nil {
		return 0, err
	}
	for _, b := range bs {
		if b.Asset == asset {
			return num(b.Free), nil
		}
	}
	return 0, nil
}

// Account is what the account holds valued in USDT (its coins at their bid), and its free USDT.
func (a *Adapter) Account(ctx context.Context) (broker.BrokerAccount, error) {
	uid, bs, err := a.balances(ctx)
	if err != nil {
		return broker.BrokerAccount{}, err
	}
	var prices []struct {
		Symbol string `json:"symbol"`
		Price  string `json:"price"`
	}
	if err := a.call(ctx, http.MethodGet, "/api/v3/ticker/price", nil, false, &prices); err != nil {
		return broker.BrokerAccount{}, err
	}
	price := map[string]float64{}
	for _, p := range prices {
		price[p.Symbol] = num(p.Price)
	}
	total, freeCash := 0.0, 0.0
	for _, b := range bs {
		amt := num(b.Free) + num(b.Locked)
		if amt <= 0 {
			continue
		}
		switch {
		case b.Asset == Quote:
			total += amt
			freeCash += num(b.Free)
		case cash[b.Asset]:
			total += amt
		default:
			total += amt * price[b.Asset+Quote]
		}
	}
	total = math.Round(total*100) / 100
	return broker.BrokerAccount{AccountID: uid, Balance: total, NAV: total, MarginAvailable: math.Round(freeCash*100) / 100, Currency: Quote}, nil
}

// ── orders ──

type binOrder struct {
	Symbol      string `json:"symbol"`
	OrderID     int64  `json:"orderId"`
	OrderListID int64  `json:"orderListId"`
	Price       string `json:"price"`
	OrigQty     string `json:"origQty"`
	ExecutedQty string `json:"executedQty"`
	Status      string `json:"status"`
	Type        string `json:"type"`
	Side        string `json:"side"`
	StopPrice   string `json:"stopPrice"`
	Time        int64  `json:"time"`
}

func (o binOrder) isStop() bool { return strings.HasPrefix(o.Type, "STOP_LOSS") }

func (a *Adapter) openOrders(ctx context.Context) ([]binOrder, error) {
	var os []binOrder
	err := a.call(ctx, http.MethodGet, "/api/v3/openOrders", nil, true, &os)
	return os, err
}

func (a *Adapter) orderList(ctx context.Context, id int64) ([]binOrder, error) {
	var l struct {
		Orders []struct {
			Symbol  string `json:"symbol"`
			OrderID int64  `json:"orderId"`
		} `json:"orders"`
	}
	if err := a.call(ctx, http.MethodGet, "/api/v3/orderList", url.Values{"orderListId": {strconv.FormatInt(id, 10)}}, true, &l); err != nil {
		return nil, err
	}
	var out []binOrder
	for _, o := range l.Orders {
		var full binOrder
		if err := a.call(ctx, http.MethodGet, "/api/v3/order", url.Values{"symbol": {o.Symbol}, "orderId": {strconv.FormatInt(o.OrderID, 10)}}, true, &full); err != nil {
			return nil, err
		}
		out = append(out, full)
	}
	return out, nil
}

func ms(t int64) time.Time {
	if t == 0 {
		return time.Time{}
	}
	return time.UnixMilli(t).UTC()
}

// PlaceTrade buys (or sells what is held) at the market or at a limit; a buy with a stop and/or target is one order
// list, so they go with it.
func (a *Adapter) PlaceTrade(ctx context.Context, req broker.TradeRequest) (broker.TradeReceipt, error) {
	m, err := a.market(ctx, req.Symbol)
	if err != nil {
		return broker.TradeReceipt{}, err
	}
	if m.status != "TRADING" {
		return broker.TradeReceipt{}, &broker.Refused{Kind: broker.RefusedMarketClosed, BrokerSaid: m.status, Err: "binance: " + m.symbol + " isn't trading now (" + m.status + ")"}
	}
	size := float64(req.Units)
	if req.Quantity != 0 {
		size = req.Quantity
	}
	sell := req.Units < 0 || (req.Units == 0 && req.Quantity < 0)
	limit := strings.EqualFold(strings.TrimSpace(req.OrderType), "limit")
	levels := req.StopLoss > 0 || req.TakeProfit > 0
	bid, ask, err := a.book(ctx, m.symbol)
	if err != nil {
		return broker.TradeReceipt{}, err
	}
	at := ask
	if sell {
		at = bid
	}
	if limit {
		if req.EntryPrice <= 0 {
			return broker.TradeReceipt{}, &broker.Refused{Kind: broker.RefusedLevel, BrokerSaid: "LEVEL", Err: "binance: an entry order needs its level"}
		}
		if (!sell && req.EntryPrice > ask) || (sell && req.EntryPrice < bid) {
			return broker.TradeReceipt{}, &broker.Refused{Kind: broker.RefusedLevel, BrokerSaid: "STOP_ENTRY",
				Err: "binance: a spot order can't wait for the price to pass a level beyond the market; place it as a limit at or better than the price now"}
		}
		at = req.EntryPrice
	}
	q, err := m.sized(size, at)
	if err != nil {
		return broker.TradeReceipt{}, err
	}
	if sell {
		if levels {
			return broker.TradeReceipt{}, &broker.Refused{Kind: broker.RefusedLevel, BrokerSaid: "SPOT_SELL",
				Err: "binance: a spot sale closes coins held, so it takes no stop or target"}
		}
		held, err := a.free(ctx, m.base)
		if err != nil {
			return broker.TradeReceipt{}, err
		}
		if held+1e-12 < q {
			return broker.TradeReceipt{}, &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "SPOT_NO_SHORT",
				Err: fmt.Sprintf("binance: spot only sells coins held (%g %s free), it can't go short", held, m.base)}
		}
	}
	r := broker.TradeReceipt{Symbol: req.Symbol, Units: req.Units, StopLoss: req.StopLoss, TakeProfit: req.TakeProfit,
		OpenedAt: time.Now().UTC(), DecisionID: req.DecisionID, Broker: "binance"}
	if !levels {
		v := url.Values{"symbol": {m.symbol}, "side": {side(sell)}, "quantity": {m.qtyStr(q)}, "newOrderRespType": {"FULL"}}
		if limit {
			v.Set("type", "LIMIT")
			v.Set("timeInForce", "GTC")
			v.Set("price", m.price(req.EntryPrice))
		} else {
			v.Set("type", "MARKET")
		}
		var o struct {
			OrderID             int64  `json:"orderId"`
			Status              string `json:"status"`
			ExecutedQty         string `json:"executedQty"`
			CummulativeQuoteQty string `json:"cummulativeQuoteQty"`
		}
		if err := a.call(ctx, http.MethodPost, "/api/v3/order", v, true, &o); err != nil {
			return broker.TradeReceipt{}, err
		}
		if o.Status == "EXPIRED" || o.Status == "REJECTED" {
			return broker.TradeReceipt{}, &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: o.Status, Err: "binance: the order " + strings.ToLower(o.Status)}
		}
		r.BrokerTradeID = strconv.FormatInt(o.OrderID, 10)
		r.FilledPrice = req.EntryPrice
		if exec := num(o.ExecutedQty); exec > 0 {
			r.FilledPrice = num(o.CummulativeQuoteQty) / exec
		}
		if !limit {
			r.BrokerTradeID = m.symbol // a holding is known by its pair
		}
		return r, nil
	}
	// a buy with a stop and/or target: a limit buy that, once filled, places them
	v := url.Values{"symbol": {m.symbol}, "workingType": {"LIMIT"}, "workingSide": {"BUY"}, "workingPrice": {m.price(at)},
		"workingQuantity": {m.qtyStr(q)}, "workingTimeInForce": {"GTC"}, "pendingSide": {"SELL"}, "pendingQuantity": {m.qtyStr(m.qty(q * feeRoom))}}
	path := "/api/v3/orderList/oto"
	switch {
	case req.StopLoss > 0 && req.TakeProfit > 0:
		path = "/api/v3/orderList/otoco"
		v.Set("pendingAboveType", "LIMIT_MAKER")
		v.Set("pendingAbovePrice", m.price(req.TakeProfit))
		m.stopLeg(v, "pendingBelow", req.StopLoss)
	case req.StopLoss > 0:
		m.stopLeg(v, "pending", req.StopLoss)
	default:
		v.Set("pendingType", "LIMIT_MAKER")
		v.Set("pendingPrice", m.price(req.TakeProfit))
	}
	var l struct {
		OrderListID int64 `json:"orderListId"`
		Orders      []struct {
			OrderID int64 `json:"orderId"`
		} `json:"orders"`
	}
	if err := a.call(ctx, http.MethodPost, path, v, true, &l); err != nil {
		return broker.TradeReceipt{}, err
	}
	if len(l.Orders) > 0 {
		r.BrokerTradeID = strconv.FormatInt(l.Orders[0].OrderID, 10) // the working buy
	}
	r.FilledPrice = at
	return r, nil
}

// stopLeg sets a stop that sells: at the market once hit where the pair allows it (STOP_LOSS), else a stop-limit half
// a percent under it. prefix names the leg ("pendingBelow", "below", "pending"), or "" for an order of its own.
func (m *market) stopLeg(v url.Values, prefix string, stop float64) {
	key := func(name string) string {
		if prefix == "" {
			return strings.ToLower(name[:1]) + name[1:]
		}
		return prefix + name
	}
	v.Set(key("StopPrice"), m.price(stop))
	if m.stopLoss {
		v.Set(key("Type"), "STOP_LOSS")
		return
	}
	v.Set(key("Type"), "STOP_LOSS_LIMIT")
	v.Set(key("Price"), m.price(stop*0.995))
	v.Set(key("TimeInForce"), "GTC")
}

func side(sell bool) string {
	if sell {
		return "SELL"
	}
	return "BUY"
}

// ── holdings ──

// openPrice is what a holding cost on average: its buys since it was last empty, from the account's own fills.
func (a *Adapter) openPrice(ctx context.Context, symbol string) (float64, time.Time, error) {
	var fills []struct {
		Price   string `json:"price"`
		Qty     string `json:"qty"`
		IsBuyer bool   `json:"isBuyer"`
		Time    int64  `json:"time"`
	}
	if err := a.call(ctx, http.MethodGet, "/api/v3/myTrades", url.Values{"symbol": {symbol}, "limit": {"1000"}}, true, &fills); err != nil {
		return 0, time.Time{}, err
	}
	sort.Slice(fills, func(i, j int) bool { return fills[i].Time < fills[j].Time })
	qty, cost := 0.0, 0.0
	var since int64
	for _, f := range fills {
		q, p := num(f.Qty), num(f.Price)
		if f.IsBuyer {
			if qty <= 1e-12 {
				since = f.Time
			}
			qty += q
			cost += q * p
			continue
		}
		if qty > 0 {
			cost -= cost * math.Min(q/qty, 1)
			qty = math.Max(qty-q, 0)
		}
	}
	if qty <= 1e-12 {
		return 0, time.Time{}, nil
	}
	return cost / qty, ms(since), nil
}

// OpenTrades are the coins held (each a long trade, known by its pair): its average cost, its bid, and the stop and
// target its sell orders hold. Dust below a pair's smallest order is left out.
func (a *Adapter) OpenTrades(ctx context.Context) ([]broker.OpenTrade, error) {
	_, bs, err := a.balances(ctx)
	if err != nil {
		return nil, err
	}
	os, err := a.openOrders(ctx)
	if err != nil {
		return nil, err
	}
	var out []broker.OpenTrade
	for _, b := range bs {
		amt := num(b.Free) + num(b.Locked)
		if amt <= 0 || cash[b.Asset] {
			continue
		}
		m, err := a.market(ctx, b.Asset+Quote)
		if err != nil {
			continue // a coin with no USDT pair isn't traded here
		}
		bid, _, err := a.book(ctx, m.symbol)
		if err != nil {
			return nil, err
		}
		if amt < m.minQty || (m.minNotional > 0 && amt*bid < m.minNotional) {
			continue
		}
		open, since, err := a.openPrice(ctx, m.symbol)
		if err != nil {
			return nil, err
		}
		t := broker.OpenTrade{BrokerTradeID: m.symbol, Symbol: m.symbol, Units: 1, Quantity: amt, OpenPrice: open, CurrentPrice: bid,
			PnLCurrency: m.quote, OpenedAt: since}
		if open > 0 {
			t.UnrealizedPnL = math.Round((bid-open)*amt*100) / 100
		}
		if amt == math.Trunc(amt) {
			t.Units, t.Quantity = int(amt), 0
		}
		for _, o := range os {
			if o.Symbol != m.symbol || o.Side != "SELL" {
				continue
			}
			if o.isStop() {
				t.StopLoss = num(o.StopPrice)
			} else {
				t.TakeProfit = num(o.Price)
			}
		}
		out = append(out, t)
	}
	return out, nil
}

// OpenOrders are the account's buys waiting for their level, each with the stop and target its order list will place.
func (a *Adapter) OpenOrders(ctx context.Context) ([]broker.OpenOrder, error) {
	os, err := a.openOrders(ctx)
	if err != nil {
		return nil, err
	}
	var out []broker.OpenOrder
	for _, o := range os {
		if o.Side != "BUY" {
			continue // a sell is a holding's stop or target
		}
		q := num(o.OrigQty) - num(o.ExecutedQty)
		oo := broker.OpenOrder{BrokerOrderID: strconv.FormatInt(o.OrderID, 10), Symbol: o.Symbol, Direction: "BUY", Units: 1, Quantity: q,
			OrderType: "LIMIT", Level: num(o.Price), CreatedAt: ms(o.Time)}
		if q == math.Trunc(q) {
			oo.Units, oo.Quantity = int(q), 0
		}
		if o.OrderListID >= 0 {
			legs, err := a.orderList(ctx, o.OrderListID)
			if err != nil {
				return nil, err
			}
			for _, l := range legs {
				switch {
				case l.OrderID == o.OrderID || l.Side != "SELL":
				case l.isStop():
					oo.StopLoss = num(l.StopPrice)
				default:
					oo.TakeProfit = num(l.Price)
				}
			}
		}
		out = append(out, oo)
	}
	return out, nil
}

// ── changing ──

func (a *Adapter) cancel(ctx context.Context, o binOrder) error {
	if o.OrderListID >= 0 {
		return a.call(ctx, http.MethodDelete, "/api/v3/orderList", url.Values{"symbol": {o.Symbol}, "orderListId": {strconv.FormatInt(o.OrderListID, 10)}}, true, nil)
	}
	return a.call(ctx, http.MethodDelete, "/api/v3/order", url.Values{"symbol": {o.Symbol}, "orderId": {strconv.FormatInt(o.OrderID, 10)}}, true, nil)
}

// exits are a holding's sell orders, each order list once.
func (a *Adapter) exits(ctx context.Context, symbol string) ([]binOrder, float64, float64, error) {
	os, err := a.openOrders(ctx)
	if err != nil {
		return nil, 0, 0, err
	}
	var out []binOrder
	seen := map[int64]bool{}
	stop, target := 0.0, 0.0
	for _, o := range os {
		if o.Symbol != symbol || o.Side != "SELL" {
			continue
		}
		if o.isStop() {
			stop = num(o.StopPrice)
		} else {
			target = num(o.Price)
		}
		if o.OrderListID >= 0 {
			if seen[o.OrderListID] {
				continue
			}
			seen[o.OrderListID] = true
		}
		out = append(out, o)
	}
	return out, stop, target, nil
}

// CloseTrade sells a holding whole at the market, taking its stop and target off first (they hold its coins).
func (a *Adapter) CloseTrade(ctx context.Context, id string) error {
	m, err := a.market(ctx, id)
	if err != nil {
		return err
	}
	ex, _, _, err := a.exits(ctx, m.symbol)
	if err != nil {
		return err
	}
	for _, o := range ex {
		if err := a.cancel(ctx, o); err != nil {
			return err
		}
	}
	held, err := a.free(ctx, m.base)
	if err != nil {
		return err
	}
	q := m.qty(held)
	if q <= 0 || q < m.minQty {
		return &broker.Refused{Kind: broker.RefusedSizeTooSmall, Limit: m.minQty, BrokerSaid: "LOT_SIZE", Err: "binance: no " + m.base + " held to sell"}
	}
	return a.call(ctx, http.MethodPost, "/api/v3/order", url.Values{"symbol": {m.symbol}, "side": {"SELL"}, "type": {"MARKET"}, "quantity": {m.qtyStr(q)}}, true, nil)
}

// protect places a holding's stop and/or target as sell orders: an OCO for both, else one order.
func (a *Adapter) protect(ctx context.Context, m *market, q, stop, target float64) error {
	v := url.Values{"symbol": {m.symbol}, "side": {"SELL"}, "quantity": {m.qtyStr(q)}}
	switch {
	case stop > 0 && target > 0:
		v.Set("aboveType", "LIMIT_MAKER")
		v.Set("abovePrice", m.price(target))
		m.stopLeg(v, "below", stop)
		return a.call(ctx, http.MethodPost, "/api/v3/orderList/oco", v, true, nil)
	case stop > 0:
		m.stopLeg(v, "", stop)
		return a.call(ctx, http.MethodPost, "/api/v3/order", v, true, nil)
	case target > 0:
		v.Set("type", "LIMIT_MAKER")
		v.Set("price", m.price(target))
		return a.call(ctx, http.MethodPost, "/api/v3/order", v, true, nil)
	}
	return nil
}

// AmendTrade moves a holding's stop and/or target (a zero keeps the one it has): its sell orders are cancelled and new
// ones placed for what it holds; if Binance refuses the new ones, the old ones are put back.
func (a *Adapter) AmendTrade(ctx context.Context, id string, sl, tp float64) error {
	if sl <= 0 && tp <= 0 {
		return nil
	}
	m, err := a.market(ctx, id)
	if err != nil {
		return err
	}
	ex, stop, target, err := a.exits(ctx, m.symbol)
	if err != nil {
		return err
	}
	for _, o := range ex {
		if err := a.cancel(ctx, o); err != nil {
			return err
		}
	}
	held, err := a.free(ctx, m.base)
	if err != nil {
		return err
	}
	q := m.qty(held)
	newStop, newTarget := stop, target
	if sl > 0 {
		newStop = sl
	}
	if tp > 0 {
		newTarget = tp
	}
	if err := a.protect(ctx, m, q, newStop, newTarget); err != nil {
		if (stop > 0 || target > 0) && a.protect(ctx, m, q, stop, target) != nil {
			return fmt.Errorf("%w (and the stop and target it had couldn't be put back: it has none now)", err)
		}
		return err
	}
	return nil
}

func (a *Adapter) waiting(ctx context.Context, id string) (*binOrder, error) {
	os, err := a.openOrders(ctx)
	if err != nil {
		return nil, err
	}
	for i := range os {
		if strconv.FormatInt(os[i].OrderID, 10) == strings.TrimSpace(id) && os[i].Side == "BUY" {
			return &os[i], nil
		}
	}
	return nil, &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "-2013", Err: "binance: no waiting order " + id}
}

// CancelOrder takes a waiting buy off the account, with its order list (the stop and target it would have placed).
func (a *Adapter) CancelOrder(ctx context.Context, id string) error {
	o, err := a.waiting(ctx, id)
	if err != nil {
		return err
	}
	return a.cancel(ctx, *o)
}

// AmendOrder moves a waiting buy's level, by cancelling and replacing it in one request; one with a stop or target
// can't be changed (Binance can't replace an order list).
func (a *Adapter) AmendOrder(ctx context.Context, id string, level, sl, tp float64) error {
	o, err := a.waiting(ctx, id)
	if err != nil {
		return err
	}
	if o.OrderListID >= 0 || sl > 0 || tp > 0 {
		return &broker.Refused{Kind: broker.RefusedLevel, BrokerSaid: "ORDER_LIST",
			Err: "binance: Binance can't change an order with a stop or target attached; cancel it and place it again"}
	}
	if level <= 0 {
		return nil
	}
	m, err := a.market(ctx, o.Symbol)
	if err != nil {
		return err
	}
	q := num(o.OrigQty) - num(o.ExecutedQty)
	return a.call(ctx, http.MethodPost, "/api/v3/order/cancelReplace", url.Values{"symbol": {o.Symbol}, "side": {"BUY"}, "type": {"LIMIT"},
		"cancelReplaceMode": {"STOP_ON_FAILURE"}, "cancelOrderId": {strconv.FormatInt(o.OrderID, 10)}, "timeInForce": {"GTC"},
		"quantity": {m.qtyStr(m.qty(q))}, "price": {m.price(level)}}, true, nil)
}

// ── refusals ──

func refusalOf(e *apiError) error {
	switch {
	case e.code == -2014 || e.code == -2015 || e.code == -1022 || e.status == http.StatusUnauthorized:
		return &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: strconv.Itoa(e.code), Err: e.Error()}
	case e.status >= 500 || e.status == http.StatusTooManyRequests || e.status == 418:
		return e
	}
	r := refusal(e.msg, e.Error())
	if r.Kind == broker.RefusedOther {
		r.BrokerSaid = strconv.Itoa(e.code)
	}
	return r
}

// refusal sorts Binance's own messages (its filter names and rejection words) into the kinds every broker shares.
func refusal(msg, whole string) *broker.Refused {
	r := &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: msg, Err: whole}
	m := strings.ToUpper(msg)
	switch {
	case strings.Contains(m, "INSUFFICIENT BALANCE"):
		r.Kind = broker.RefusedNotEnoughMargin
	case strings.Contains(m, "MAX_NUM"), strings.Contains(m, "MAX_POSITION"):
		r.Kind = broker.RefusedSizeTooLarge
	case strings.Contains(m, "LOT_SIZE"), strings.Contains(m, "NOTIONAL"):
		r.Kind = broker.RefusedSizeTooSmall
	case strings.Contains(m, "PRICE_FILTER"), strings.Contains(m, "PERCENT_PRICE"), strings.Contains(m, "TRIGGER IMMEDIATELY"),
		strings.Contains(m, "PRICE"):
		r.Kind = broker.RefusedLevel
	case strings.Contains(m, "MARKET IS CLOSED"), strings.Contains(m, "TRADING IS DISABLED"):
		r.Kind = broker.RefusedMarketClosed
	}
	return r
}
