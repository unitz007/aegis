// Package ig trades on IG (ig.com) through its REST trading API, with each person's own API key, username and password.
//
// What it was written from: IG's labs reference was unreachable while this was built (2026-11-22), so the endpoints,
// versions and fields follow the widely used trading-ig client and the IG responses its tests record. Not yet tried on
// a real IG account: prices are taken as IG's own scale divided by each market's scalingFactor, sizes as contracts of
// contractSize units, and only a CFD account is traded (a spread bet account's size is money per point, which Minions
// doesn't size in).
package ig

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

// Bases are IG's API by environment.
var Bases = map[string]string{
	"demo": "https://demo-api.ig.com/gateway/deal",
	"live": "https://api.ig.com/gateway/deal",
}

// Adapter is one person's IG login, trading on its CFD account.
type Adapter struct {
	base, key, user, pass string
	client                *http.Client

	mu       sync.Mutex
	cst, xst string
	account  string // the CFD account traded on
	currency string
	epics    map[string]string // symbol → epic
	markets  map[string]*market
}

var (
	_ broker.Port                 = (*Adapter)(nil)
	_ broker.DealingRulesProvider = (*Adapter)(nil)
	_ broker.MarketHoursProvider  = (*Adapter)(nil)
)

// New is a person's IG login on env ("demo" or "live").
func New(env, apiKey, username, password string) (*Adapter, error) {
	base, ok := Bases[env]
	if !ok {
		return nil, fmt.Errorf("ig: no %q environment", env)
	}
	if strings.TrimSpace(apiKey) == "" || strings.TrimSpace(username) == "" || password == "" {
		return nil, errors.New("ig: the API key, username and password are all needed")
	}
	return &Adapter{base: base, key: strings.TrimSpace(apiKey), user: strings.TrimSpace(username), pass: password,
		client: &http.Client{Timeout: 20 * time.Second}, epics: map[string]string{}, markets: map[string]*market{}}, nil
}

// Spec is IG for a registry: a person types in their API key, username and password.
func Spec() broker.Spec {
	return broker.Spec{
		Name:  "ig",
		Label: "IG",
		Envs:  []string{"demo", "live"},
		Fields: []broker.Field{
			{Name: "api_key", Label: "API key", Secret: true, Help: "In IG, open My IG → Settings → API keys and generate one (for a demo account, from the demo account's settings)."},
			{Name: "username", Label: "IG username", Help: "The username you log in to IG with."},
			{Name: "password", Label: "Password", Secret: true, Help: "Your IG password."},
		},
		Identity: []string{"api_key", "username"},
		New: func(env string, c broker.Credentials) (broker.Port, error) {
			return New(env, c["api_key"], c["username"], c["password"])
		},
	}
}

// ── HTTP ──

type apiError struct {
	status int
	code   string
	path   string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("ig %s → HTTP %d: %s", e.path, e.status, e.code)
}

// call sends a request with the session's tokens (signing in when there are none, and once more when they ran out).
func (a *Adapter) call(ctx context.Context, method, path, version string, body, out any, deleting bool) error {
	if err := a.ensure(ctx); err != nil {
		return err
	}
	err := a.do(ctx, method, path, version, body, out, deleting, true)
	var e *apiError
	if errors.As(err, &e) && e.status == http.StatusUnauthorized && !strings.Contains(e.code, "invalid-details") {
		a.mu.Lock()
		a.cst, a.xst = "", ""
		a.mu.Unlock()
		if err := a.ensure(ctx); err != nil {
			return err
		}
		err = a.do(ctx, method, path, version, body, out, deleting, true)
	}
	return refusalOf(err)
}

func (a *Adapter) do(ctx context.Context, method, path, version string, body, out any, deleting, signed bool) error {
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
	req.Header.Set("X-IG-API-KEY", a.key)
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	req.Header.Set("Accept", "application/json; charset=UTF-8")
	req.Header.Set("Version", version)
	if deleting {
		req.Header.Set("_method", "DELETE") // IG takes a DELETE with a body as a POST marked so
	}
	if signed {
		a.mu.Lock()
		req.Header.Set("CST", a.cst)
		req.Header.Set("X-SECURITY-TOKEN", a.xst)
		a.mu.Unlock()
	}
	res, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("ig http: %w", err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if res.StatusCode >= 400 {
		var e struct {
			ErrorCode string `json:"errorCode"`
		}
		_ = json.Unmarshal(raw, &e)
		if e.ErrorCode == "" {
			e.ErrorCode = strings.TrimSpace(string(raw))
		}
		return &apiError{status: res.StatusCode, code: e.ErrorCode, path: path}
	}
	if !signed {
		a.mu.Lock()
		a.cst, a.xst = res.Header.Get("CST"), res.Header.Get("X-SECURITY-TOKEN")
		a.mu.Unlock()
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("ig decode %s: %w", path, err)
		}
	}
	return nil
}

// ensure signs in (POST /session, version 2) and keeps to the login's CFD account, switching to it if another is current.
func (a *Adapter) ensure(ctx context.Context) error {
	a.mu.Lock()
	ready := a.cst != "" && a.account != ""
	a.mu.Unlock()
	if ready {
		return nil
	}
	var s struct {
		CurrentAccountID string `json:"currentAccountId"`
		CurrencyIsoCode  string `json:"currencyIsoCode"`
		Accounts         []struct {
			AccountID   string `json:"accountId"`
			AccountType string `json:"accountType"`
		} `json:"accounts"`
	}
	if err := a.do(ctx, http.MethodPost, "/session", "2", map[string]any{"identifier": a.user, "password": a.pass}, &s, false, false); err != nil {
		var e *apiError
		if errors.As(err, &e) && (e.status == http.StatusUnauthorized || e.status == http.StatusForbidden || strings.Contains(e.code, "invalid")) {
			return &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: e.code, Err: e.Error()}
		}
		return err
	}
	cfd := ""
	for _, acc := range s.Accounts {
		if strings.EqualFold(acc.AccountType, "CFD") {
			if cfd == "" || acc.AccountID == s.CurrentAccountID {
				cfd = acc.AccountID
			}
		}
	}
	if cfd == "" {
		return &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "NO_CFD_ACCOUNT",
			Err: "ig: this login has no CFD account (only CFD accounts are traded: a spread bet's size is money per point)"}
	}
	if cfd != s.CurrentAccountID {
		if err := a.do(ctx, http.MethodPut, "/session", "1", map[string]any{"accountId": cfd, "defaultAccount": false}, nil, false, true); err != nil {
			return refusalOf(err)
		}
	}
	a.mu.Lock()
	a.account, a.currency = cfd, strings.ToUpper(s.CurrencyIsoCode)
	a.mu.Unlock()
	return nil
}

// ── markets ──

type market struct {
	epic          string
	scale         float64 // IG's prices are the real ones times this
	contract      float64 // units in one contract
	minSize       float64 // contracts
	marginPercent float64
	currency      string
	status        string
	bid, offer    float64
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

// epic is the market a symbol trades as on a CFD account: found by searching, a CFD market first, then one that
// doesn't expire.
func (a *Adapter) epic(ctx context.Context, symbol string) (string, error) {
	key := norm(symbol)
	a.mu.Lock()
	if e := a.epics[key]; e != "" {
		a.mu.Unlock()
		return e, nil
	}
	a.mu.Unlock()
	var r struct {
		Markets []struct {
			Epic           string `json:"epic"`
			InstrumentName string `json:"instrumentName"`
			Expiry         string `json:"expiry"`
		} `json:"markets"`
	}
	if err := a.call(ctx, http.MethodGet, "/markets?searchTerm="+url.QueryEscape(key), "1", nil, &r, false); err != nil {
		return "", err
	}
	best, score := "", -1
	for _, m := range r.Markets {
		if !strings.Contains(norm(m.Epic), key) && !strings.Contains(norm(m.InstrumentName), key) {
			continue
		}
		s := 0
		if strings.Contains(strings.ToUpper(m.Epic), ".CFD.") {
			s += 2
		}
		if m.Expiry == "-" || m.Expiry == "DFB" {
			s++
		}
		if s > score {
			best, score = m.Epic, s
		}
	}
	if best == "" {
		return "", &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "MARKET_NOT_FOUND", Err: "ig: no market for " + symbol}
	}
	a.mu.Lock()
	a.epics[key] = best
	a.mu.Unlock()
	return best, nil
}

func f(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case string:
		n, _ := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return n
	case json.Number:
		n, _ := x.Float64()
		return n
	}
	return 0
}

// market reads a market's rules and price (GET /markets/{epic}, version 3).
func (a *Adapter) market(ctx context.Context, epic string) (*market, error) {
	var r struct {
		Instrument struct {
			LotSize          any    `json:"lotSize"`
			ContractSize     any    `json:"contractSize"`
			MarginFactor     any    `json:"marginFactor"`
			MarginFactorUnit string `json:"marginFactorUnit"`
			Currencies       []struct {
				Code      string `json:"code"`
				IsDefault bool   `json:"isDefault"`
			} `json:"currencies"`
		} `json:"instrument"`
		DealingRules struct {
			MinDealSize struct {
				Value any `json:"value"`
			} `json:"minDealSize"`
		} `json:"dealingRules"`
		Snapshot struct {
			MarketStatus  string `json:"marketStatus"`
			Bid           any    `json:"bid"`
			Offer         any    `json:"offer"`
			ScalingFactor any    `json:"scalingFactor"`
		} `json:"snapshot"`
	}
	if err := a.call(ctx, http.MethodGet, "/markets/"+url.PathEscape(epic), "3", nil, &r, false); err != nil {
		return nil, err
	}
	m := &market{epic: epic, scale: f(r.Snapshot.ScalingFactor), contract: f(r.Instrument.ContractSize), minSize: f(r.DealingRules.MinDealSize.Value),
		status: strings.ToUpper(r.Snapshot.MarketStatus)}
	if m.scale <= 0 {
		m.scale = 1
	}
	if m.contract <= 0 {
		m.contract = f(r.Instrument.LotSize)
	}
	if m.contract <= 0 {
		m.contract = 1
	}
	if strings.EqualFold(r.Instrument.MarginFactorUnit, "PERCENTAGE") {
		m.marginPercent = f(r.Instrument.MarginFactor)
	}
	for _, c := range r.Instrument.Currencies {
		if c.IsDefault || m.currency == "" {
			m.currency = c.Code
		}
	}
	m.bid, m.offer = real(f(r.Snapshot.Bid), m.scale), real(f(r.Snapshot.Offer), m.scale)
	a.mu.Lock()
	a.markets[epic] = m
	a.mu.Unlock()
	return m, nil
}

func (a *Adapter) marketOf(ctx context.Context, symbol string) (*market, error) {
	epic, err := a.epic(ctx, symbol)
	if err != nil {
		return nil, err
	}
	return a.market(ctx, epic)
}

// real is a price in IG's scale as it really is, without the noise dividing leaves (10846.2 / 10000 is 1.08462).
func real(p, scale float64) float64 {
	if p == 0 || scale <= 0 {
		return p
	}
	return math.Round(p/scale*1e8) / 1e8
}

// ig is a price as IG takes it for a market (times its scaling factor); 0 stays 0.
func (m *market) ig(p float64) any {
	if p <= 0 {
		return nil
	}
	return math.Round(p*m.scale*1e6) / 1e6
}

// size is a size in units as IG's contracts, down to a hundredth (never raised); below the market's smallest it is refused.
func (m *market) size(units float64) (float64, error) {
	s := math.Floor(math.Abs(units)/m.contract*100) / 100
	if s <= 0 || (m.minSize > 0 && s < m.minSize) {
		return 0, &broker.Refused{Kind: broker.RefusedSizeTooSmall, Limit: m.minSize * m.contract, BrokerSaid: "MINIMUM_ORDER_SIZE_ERROR",
			Err: fmt.Sprintf("ig: %g units is below the smallest deal (%g)", math.Abs(units), m.minSize*m.contract)}
	}
	return s, nil
}

// ── deals ──

// confirm reads a deal's confirmation (GET /confirms/{ref}): the deal id it made, or the refusal in IG's words.
func (a *Adapter) confirm(ctx context.Context, ref string) (string, float64, error) {
	var c struct {
		DealStatus    string  `json:"dealStatus"`
		Reason        string  `json:"reason"`
		DealID        string  `json:"dealId"`
		Level         float64 `json:"level"`
		AffectedDeals []struct {
			DealID string `json:"dealId"`
			Status string `json:"status"`
		} `json:"affectedDeals"`
	}
	var err error
	for i := 0; i < 4; i++ { // a confirmation can take a moment to be there
		if err = a.call(ctx, http.MethodGet, "/confirms/"+url.PathEscape(ref), "1", nil, &c, false); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return "", 0, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	if err != nil {
		return "", 0, err
	}
	if !strings.EqualFold(c.DealStatus, "ACCEPTED") {
		why := c.Reason
		if why == "" {
			why = "REJECTED"
		}
		return "", 0, refusal(why, "ig deal "+ref+" rejected: "+why)
	}
	id := c.DealID
	for _, d := range c.AffectedDeals {
		if strings.EqualFold(d.Status, "OPENED") && d.DealID != "" {
			id = d.DealID
		}
	}
	return id, c.Level, nil
}

func side(buy bool) string {
	if buy {
		return "BUY"
	}
	return "SELL"
}

// PlaceTrade places a market deal, with its stop and target as levels, or an entry order at a level: a limit when the
// level is better than the market, a stop when it is beyond it, so it waits for the level either way.
func (a *Adapter) PlaceTrade(ctx context.Context, req broker.TradeRequest) (broker.TradeReceipt, error) {
	m, err := a.marketOf(ctx, req.Symbol)
	if err != nil {
		return broker.TradeReceipt{}, err
	}
	units := float64(req.Units)
	if req.Quantity != 0 {
		units = req.Quantity
	}
	size, err := m.size(units)
	if err != nil {
		return broker.TradeReceipt{}, err
	}
	buy := req.Units > 0 || (req.Units == 0 && req.Quantity > 0)
	body := map[string]any{"epic": m.epic, "expiry": "-", "direction": side(buy), "size": size, "currencyCode": m.currency,
		"guaranteedStop": false, "forceOpen": true, "stopLevel": m.ig(req.StopLoss), "limitLevel": m.ig(req.TakeProfit)}
	var r struct {
		DealReference string `json:"dealReference"`
	}
	if strings.EqualFold(strings.TrimSpace(req.OrderType), "limit") {
		if req.EntryPrice <= 0 {
			return broker.TradeReceipt{}, &broker.Refused{Kind: broker.RefusedLevel, BrokerSaid: "LEVEL", Err: "ig: an entry order needs its level"}
		}
		typ := "LIMIT"
		if (buy && m.offer > 0 && req.EntryPrice > m.offer) || (!buy && m.bid > 0 && req.EntryPrice < m.bid) {
			typ = "STOP"
		}
		body["level"], body["type"], body["timeInForce"] = m.ig(req.EntryPrice), typ, "GOOD_TILL_CANCELLED"
		if err := a.call(ctx, http.MethodPost, "/workingorders/otc", "2", body, &r, false); err != nil {
			return broker.TradeReceipt{}, err
		}
	} else {
		body["orderType"] = "MARKET"
		if err := a.call(ctx, http.MethodPost, "/positions/otc", "2", body, &r, false); err != nil {
			return broker.TradeReceipt{}, err
		}
	}
	id, level, err := a.confirm(ctx, r.DealReference)
	if err != nil {
		return broker.TradeReceipt{}, err
	}
	return broker.TradeReceipt{BrokerTradeID: id, Symbol: req.Symbol, Units: req.Units, FilledPrice: real(level, m.scale),
		StopLoss: req.StopLoss, TakeProfit: req.TakeProfit, OpenedAt: time.Now().UTC(), DecisionID: req.DecisionID, Broker: "ig"}, nil
}

// ── reading ──

type igPosition struct {
	Position struct {
		ContractSize float64  `json:"contractSize"`
		DealID       string   `json:"dealId"`
		Size         float64  `json:"size"`
		Direction    string   `json:"direction"`
		Level        float64  `json:"level"`
		StopLevel    *float64 `json:"stopLevel"`
		LimitLevel   *float64 `json:"limitLevel"`
		Currency     string   `json:"currency"`
		CreatedUTC   string   `json:"createdDateUTC"`
	} `json:"position"`
	Market struct {
		Epic          string  `json:"epic"`
		Name          string  `json:"instrumentName"`
		Bid           float64 `json:"bid"`
		Offer         float64 `json:"offer"`
		ScalingFactor float64 `json:"scalingFactor"`
	} `json:"market"`
}

func (a *Adapter) positions(ctx context.Context) ([]igPosition, error) {
	var r struct {
		Positions []igPosition `json:"positions"`
	}
	err := a.call(ctx, http.MethodGet, "/positions", "2", nil, &r, false)
	return r.Positions, err
}

func deref(p *float64) float64 {
	if p == nil {
		return 0
	}
	return *p
}

func utc(s string) time.Time {
	for _, layout := range []string{"2006-01-02T15:04:05", "2006-01-02T15:04:05.000"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

// sized is a size in units as Units and, when it isn't whole (a coin), its quantity.
func sized(units float64, sell bool) (int, float64) {
	u, q := int(units), 0.0
	if units != float64(u) {
		q = units
		if u == 0 && units > 0 {
			u = 1
		}
	}
	if sell {
		u = -u
	}
	return u, q
}

// symbolOf is a market as people say it: its epic's middle ("CS.D.EURUSD.CFD.IP" is EURUSD), else its name.
func symbolOf(epic, name string) string {
	if parts := strings.Split(epic, "."); len(parts) >= 3 && parts[2] != "" {
		return parts[2]
	}
	return name
}

// OpenTrades are the CFD account's open positions, with what each would make if closed now.
func (a *Adapter) OpenTrades(ctx context.Context) ([]broker.OpenTrade, error) {
	ps, err := a.positions(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]broker.OpenTrade, 0, len(ps))
	for _, p := range ps {
		scale := p.Market.ScalingFactor
		if scale <= 0 {
			scale = 1
		}
		contract := p.Position.ContractSize
		if contract <= 0 {
			contract = 1
		}
		sell := strings.EqualFold(p.Position.Direction, "SELL")
		units, qty := sized(p.Position.Size*contract, sell)
		cur := p.Market.Bid
		if sell {
			cur = p.Market.Offer
		}
		open := real(p.Position.Level, scale)
		now := real(cur, scale)
		pnl := (now - open) * p.Position.Size * contract
		if sell {
			pnl = -pnl
		}
		out = append(out, broker.OpenTrade{BrokerTradeID: p.Position.DealID, Symbol: symbolOf(p.Market.Epic, p.Market.Name), Units: units, Quantity: qty,
			OpenPrice: open, CurrentPrice: now, UnrealizedPnL: math.Round(pnl*100) / 100, PnLCurrency: p.Position.Currency,
			StopLoss: real(deref(p.Position.StopLevel), scale), TakeProfit: real(deref(p.Position.LimitLevel), scale), OpenedAt: utc(p.Position.CreatedUTC)})
	}
	return out, nil
}

type igOrder struct {
	Data struct {
		DealID        string  `json:"dealId"`
		Direction     string  `json:"direction"`
		Epic          string  `json:"epic"`
		OrderSize     float64 `json:"orderSize"`
		OrderLevel    float64 `json:"orderLevel"`
		OrderType     string  `json:"orderType"`
		StopDistance  float64 `json:"stopDistance"`
		LimitDistance float64 `json:"limitDistance"`
		CreatedUTC    string  `json:"createdDateUTC"`
	} `json:"workingOrderData"`
	Market struct {
		Name          string  `json:"instrumentName"`
		LotSize       float64 `json:"lotSize"`
		ScalingFactor float64 `json:"scalingFactor"`
	} `json:"marketData"`
}

func (a *Adapter) orders(ctx context.Context) ([]igOrder, error) {
	var r struct {
		WorkingOrders []igOrder `json:"workingOrders"`
	}
	err := a.call(ctx, http.MethodGet, "/workingorders", "2", nil, &r, false)
	return r.WorkingOrders, err
}

// OpenOrders are the CFD account's entry orders waiting for their level; their stop and target are given by IG as
// distances from the level, and said here as levels.
func (a *Adapter) OpenOrders(ctx context.Context) ([]broker.OpenOrder, error) {
	os, err := a.orders(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]broker.OpenOrder, 0, len(os))
	for _, o := range os {
		scale := o.Market.ScalingFactor
		if scale <= 0 {
			scale = 1
		}
		contract := 1.0
		if m := a.cached(o.Data.Epic); m != nil {
			contract = m.contract
		} else if o.Market.LotSize > 0 {
			contract = o.Market.LotSize
		}
		sell := strings.EqualFold(o.Data.Direction, "SELL")
		units, qty := sized(o.Data.OrderSize*contract, sell)
		level := real(o.Data.OrderLevel, scale)
		stop, target := 0.0, 0.0
		if o.Data.StopDistance > 0 {
			if stop = real(o.Data.OrderLevel-o.Data.StopDistance, scale); sell {
				stop = real(o.Data.OrderLevel+o.Data.StopDistance, scale)
			}
		}
		if o.Data.LimitDistance > 0 {
			if target = real(o.Data.OrderLevel+o.Data.LimitDistance, scale); sell {
				target = real(o.Data.OrderLevel-o.Data.LimitDistance, scale)
			}
		}
		out = append(out, broker.OpenOrder{BrokerOrderID: o.Data.DealID, Symbol: symbolOf(o.Data.Epic, o.Market.Name), Direction: strings.ToUpper(o.Data.Direction),
			Units: units, Quantity: qty, OrderType: strings.ToUpper(o.Data.OrderType), Level: level, StopLoss: stop, TakeProfit: target, CreatedAt: utc(o.Data.CreatedUTC)})
	}
	return out, nil
}

func (a *Adapter) cached(epic string) *market {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.markets[epic]
}

// Account is the CFD account's balance, what is open worth now, the margin used and what is free.
func (a *Adapter) Account(ctx context.Context) (broker.BrokerAccount, error) {
	var r struct {
		Accounts []struct {
			AccountID string `json:"accountId"`
			Currency  string `json:"currency"`
			Balance   struct {
				Balance    float64 `json:"balance"`
				Deposit    float64 `json:"deposit"`
				ProfitLoss float64 `json:"profitLoss"`
				Available  float64 `json:"available"`
			} `json:"balance"`
		} `json:"accounts"`
	}
	if err := a.call(ctx, http.MethodGet, "/accounts", "1", nil, &r, false); err != nil {
		return broker.BrokerAccount{}, err
	}
	a.mu.Lock()
	want := a.account
	a.mu.Unlock()
	for _, acc := range r.Accounts {
		if acc.AccountID != want {
			continue
		}
		b := acc.Balance
		return broker.BrokerAccount{AccountID: acc.AccountID, Balance: b.Balance, NAV: b.Balance + b.ProfitLoss, UnrealizedPnL: b.ProfitLoss,
			MarginUsed: b.Deposit, MarginAvailable: b.Available, Currency: strings.ToUpper(acc.Currency)}, nil
	}
	return broker.BrokerAccount{}, fmt.Errorf("ig: account %s not found", want)
}

// LiveQuote is a market's bid and ask.
func (a *Adapter) LiveQuote(ctx context.Context, symbol string) (float64, float64, error) {
	m, err := a.marketOf(ctx, symbol)
	if err != nil {
		return 0, 0, err
	}
	if m.bid <= 0 || m.offer <= 0 {
		return 0, 0, fmt.Errorf("ig: no price for %s", symbol)
	}
	return m.bid, m.offer, nil
}

// ── changing ──

func (a *Adapter) position(ctx context.Context, id string) (*igPosition, error) {
	ps, err := a.positions(ctx)
	if err != nil {
		return nil, err
	}
	for i := range ps {
		if ps[i].Position.DealID == strings.TrimSpace(id) {
			return &ps[i], nil
		}
	}
	return nil, &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "POSITION_NOT_FOUND", Err: "ig: no open position " + id}
}

// CloseTrade closes an open position whole, at the market.
func (a *Adapter) CloseTrade(ctx context.Context, id string) error {
	p, err := a.position(ctx, id)
	if err != nil {
		return err
	}
	var r struct {
		DealReference string `json:"dealReference"`
	}
	opposite := side(strings.EqualFold(p.Position.Direction, "SELL"))
	if err := a.call(ctx, http.MethodPost, "/positions/otc", "1", map[string]any{"dealId": p.Position.DealID, "direction": opposite,
		"size": p.Position.Size, "orderType": "MARKET"}, &r, true); err != nil {
		return err
	}
	_, _, err = a.confirm(ctx, r.DealReference)
	return err
}

// AmendTrade moves an open position's stop and/or target; a zero keeps the one it has.
func (a *Adapter) AmendTrade(ctx context.Context, id string, sl, tp float64) error {
	p, err := a.position(ctx, id)
	if err != nil {
		return err
	}
	scale := p.Market.ScalingFactor
	if scale <= 0 {
		scale = 1
	}
	stop, limit := deref(p.Position.StopLevel), deref(p.Position.LimitLevel)
	if sl > 0 {
		stop = sl * scale
	}
	if tp > 0 {
		limit = tp * scale
	}
	body := map[string]any{"trailingStop": false}
	if stop > 0 {
		body["stopLevel"] = stop
	}
	if limit > 0 {
		body["limitLevel"] = limit
	}
	var r struct {
		DealReference string `json:"dealReference"`
	}
	if err := a.call(ctx, http.MethodPut, "/positions/otc/"+url.PathEscape(p.Position.DealID), "2", body, &r, false); err != nil {
		return err
	}
	_, _, err = a.confirm(ctx, r.DealReference)
	return err
}

func (a *Adapter) order(ctx context.Context, id string) (*igOrder, error) {
	os, err := a.orders(ctx)
	if err != nil {
		return nil, err
	}
	for i := range os {
		if os[i].Data.DealID == strings.TrimSpace(id) {
			return &os[i], nil
		}
	}
	return nil, &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: "ORDER_NOT_FOUND", Err: "ig: no waiting order " + id}
}

// CancelOrder takes a waiting order off the account.
func (a *Adapter) CancelOrder(ctx context.Context, id string) error {
	o, err := a.order(ctx, id)
	if err != nil {
		return err
	}
	var r struct {
		DealReference string `json:"dealReference"`
	}
	if err := a.call(ctx, http.MethodPost, "/workingorders/otc/"+url.PathEscape(o.Data.DealID), "2", map[string]any{}, &r, true); err != nil {
		return err
	}
	_, _, err = a.confirm(ctx, r.DealReference)
	return err
}

// AmendOrder moves a waiting order's level, stop and/or target; a zero keeps the one it has.
func (a *Adapter) AmendOrder(ctx context.Context, id string, level, sl, tp float64) error {
	o, err := a.order(ctx, id)
	if err != nil {
		return err
	}
	scale := o.Market.ScalingFactor
	if scale <= 0 {
		scale = 1
	}
	sell := strings.EqualFold(o.Data.Direction, "SELL")
	lv := o.Data.OrderLevel
	if level > 0 {
		lv = level * scale
	}
	body := map[string]any{"level": lv, "type": strings.ToUpper(o.Data.OrderType), "timeInForce": "GOOD_TILL_CANCELLED"}
	stop, limit := 0.0, 0.0
	if o.Data.StopDistance > 0 {
		if stop = o.Data.OrderLevel - o.Data.StopDistance; sell {
			stop = o.Data.OrderLevel + o.Data.StopDistance
		}
	}
	if o.Data.LimitDistance > 0 {
		if limit = o.Data.OrderLevel + o.Data.LimitDistance; sell {
			limit = o.Data.OrderLevel - o.Data.LimitDistance
		}
	}
	if sl > 0 {
		stop = sl * scale
	}
	if tp > 0 {
		limit = tp * scale
	}
	if stop > 0 {
		body["stopLevel"] = stop
	}
	if limit > 0 {
		body["limitLevel"] = limit
	}
	var r struct {
		DealReference string `json:"dealReference"`
	}
	if err := a.call(ctx, http.MethodPut, "/workingorders/otc/"+url.PathEscape(o.Data.DealID), "2", body, &r, false); err != nil {
		return err
	}
	_, _, err = a.confirm(ctx, r.DealReference)
	return err
}

// ── what IG says about a market ──

// MarketDealingRules are a market's smallest deal in units and its margin rate.
func (a *Adapter) MarketDealingRules(ctx context.Context, symbol string) (broker.DealingRules, error) {
	m, err := a.marketOf(ctx, symbol)
	if err != nil {
		return broker.DealingRules{}, err
	}
	return broker.DealingRules{MinDealSize: m.minSize * m.contract, MarginPercent: m.marginPercent}, nil
}

// MarketHours says whether a market trades now (its status); when it next opens IG's API doesn't say plainly, so it is
// left unknown.
func (a *Adapter) MarketHours(ctx context.Context, symbol string) (broker.MarketHours, error) {
	m, err := a.marketOf(ctx, symbol)
	if err != nil {
		return broker.MarketHours{}, err
	}
	return broker.MarketHours{Open: m.status == "TRADEABLE"}, nil
}

// ── refusals ──

func refusalOf(err error) error {
	var e *apiError
	if errors.As(err, &e) && e.status < 500 {
		if e.status == http.StatusUnauthorized && strings.Contains(e.code, "invalid-details") {
			return &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: e.code, Err: e.Error()}
		}
		return refusal(e.code, e.Error())
	}
	return err
}

// refusal sorts IG's own reasons (a confirmation's reason, an error's code) into the kinds every broker shares.
func refusal(code, whole string) *broker.Refused {
	r := &broker.Refused{Kind: broker.RefusedOther, BrokerSaid: code, Err: whole}
	c := strings.ToUpper(code)
	switch {
	case strings.Contains(c, "MARKET_CLOSED"), strings.Contains(c, "MARKET_OFFLINE"), strings.Contains(c, "MARKET-CLOSED"):
		r.Kind = broker.RefusedMarketClosed
	case strings.Contains(c, "INSUFFICIENT_FUNDS"), strings.Contains(c, "MARGIN"):
		r.Kind = broker.RefusedNotEnoughMargin
	case strings.Contains(c, "MINIMUM_ORDER_SIZE"):
		r.Kind = broker.RefusedSizeTooSmall
	case strings.Contains(c, "MAX_AUTO_SIZE"), strings.Contains(c, "SIZE_INCREMENT"):
		r.Kind = broker.RefusedSizeTooLarge
	case strings.Contains(c, "STOP") && strings.Contains(c, "DISTANCE"), strings.Contains(c, "MINIMUM_DISTANCE"):
		r.Kind = broker.RefusedStopTooClose
	case strings.Contains(c, "LEVEL"), strings.Contains(c, "ATTACHED_ORDER"):
		r.Kind = broker.RefusedLevel
	}
	return r
}
