// Package capital provides a Capital.com REST API adapter that implements
// broker.Port. Ported from StockAI internal/adapters/broker/capital_adapter.go
// (commit with direction-aware capitalClampStop + retry-once clamp fix).
//
// Auth flow: POST /api/v1/session returns CST + X-SECURITY-TOKEN headers.
// Both tokens are required on every subsequent request. Sessions expire after
// 10 min of inactivity; the adapter re-authenticates transparently on the first
// 401 response.
//
// Safety-critical logic carried verbatim from finOS:
//   - capitalClampStop: direction-aware stop-distance clamp (BUY/SELL, maxvalue/minvalue).
//   - Retry-once clamp blocks in placeMarketOrder and placeLimitOrder.
//   - rateGate: FIFO rate limiting at 8 req/s under Capital's 10 req/s cap.
//   - do(): 401/429 retry loop with exponential backoff.
package capital

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/unitz007/aegis/broker"
)

// Compile-time assertions: Adapter must satisfy broker.Port and
// broker.DealingRulesProvider.
var _ broker.Port = (*Adapter)(nil)
var _ broker.DealingRulesProvider = (*Adapter)(nil)

const (
	capitalDemoURL = "https://demo-api-capital.backend-capital.com"
	capitalLiveURL = "https://api-capital.backend-capital.com"
)

// Adapter implements broker.Port against the Capital.com REST API.
// Safe for concurrent use.
type Adapter struct {
	baseURL  string
	apiKey   string // CAPITAL_API_KEY  — sent as X-CAP-API-KEY header
	email    string // CAPITAL_EMAIL    — used as session identifier
	password string // CAPITAL_PASSWORD — account password
	client   *http.Client

	mu         sync.Mutex
	cst        string    // session token 1
	secToken   string    // session token 2 (X-SECURITY-TOKEN)
	sessionExp time.Time // when to force re-auth

	// gate paces all outbound requests to stay under Capital.com's 10 req/s
	// account limit. Without it, a cold-start scan (4 timeframes × ~13 symbols
	// fired concurrently) bursts well past the cap and Capital returns HTTP 429.
	gate *rateGate

	orderSourceMu   sync.RWMutex
	orderSources    map[string]string // dealId → source ("auto"|"human")
	orderTradeTypes map[string]string // dealId → trade type ("positional"|"swing"|...)
}

// New creates a Capital.com broker adapter.
// env should be "demo" or "live".
// email is the Capital.com account email (used as session identifier).
// apiKey is the API key generated in Capital.com settings (X-CAP-API-KEY header).
func New(env, apiKey, email, password string) *Adapter {
	base := capitalDemoURL
	if strings.ToLower(env) == "live" {
		base = capitalLiveURL
	}
	return &Adapter{
		baseURL:  base,
		apiKey:   apiKey,
		email:    email,
		password: password,
		client:   &http.Client{Timeout: 30 * time.Second},
		// 8 req/s (125ms spacing) leaves headroom under Capital's 10 req/s cap.
		gate:            newRateGate(capitalReqInterval),
		orderSources:    make(map[string]string),
		orderTradeTypes: make(map[string]string),
	}
}

// capitalReqInterval spaces requests at ~8/s, under Capital.com's 10 req/s cap.
const capitalReqInterval = 125 * time.Millisecond

// rateGate serializes outbound requests so they are spaced at least `interval`
// apart, while preserving fair FIFO ordering. Safe for concurrent use.
type rateGate struct {
	mu       sync.Mutex
	next     time.Time
	interval time.Duration
}

func newRateGate(interval time.Duration) *rateGate {
	return &rateGate{interval: interval}
}

// wait blocks until this caller's reserved slot arrives or ctx is cancelled.
// A nil gate is a no-op (adapters built via struct literal in tests skip pacing).
func (g *rateGate) wait(ctx context.Context) error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	now := time.Now()
	if g.next.Before(now) {
		g.next = now
	}
	slot := g.next
	g.next = g.next.Add(g.interval)
	g.mu.Unlock()

	d := time.Until(slot)
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// ── Auth ─────────────────────────────────────────────────────────────────────

// ensureSession authenticates if the current session is missing or expired.
func (a *Adapter) ensureSession(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cst != "" && time.Now().Before(a.sessionExp) {
		return nil
	}
	return a.createSession(ctx)
}

// createSession (caller must hold a.mu).
func (a *Adapter) createSession(ctx context.Context) error {
	payload := map[string]any{
		"identifier":        a.email,
		"password":          a.password,
		"encryptedPassword": false,
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		a.baseURL+"/api/v1/session", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("capital session request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-CAP-API-KEY", a.apiKey)

	resp, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("capital session: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("capital session HTTP %d: %s", resp.StatusCode, truncate(string(raw), 200))
	}

	cst := resp.Header.Get("CST")
	sec := resp.Header.Get("X-SECURITY-TOKEN")
	if cst == "" || sec == "" {
		return fmt.Errorf("capital session: missing CST or X-SECURITY-TOKEN headers")
	}
	a.cst = cst
	a.secToken = sec
	// Capital.com sessions last 10 min idle; refresh every 8 min to be safe.
	a.sessionExp = time.Now().Add(8 * time.Minute)
	return nil
}

// ── broker.Port implementation ────────────────────────────────────────────────

// PlaceTrade opens a market or limit order on Capital.com.
// req.OrderType == "limit" routes to the working-orders endpoint;
// everything else places an immediate market order.
func (a *Adapter) PlaceTrade(ctx context.Context, req broker.TradeRequest) (broker.TradeReceipt, error) {
	if strings.EqualFold(req.OrderType, "limit") {
		return a.placeLimitOrder(ctx, req)
	}
	return a.placeMarketOrder(ctx, req)
}

// placeMarketOrder submits an immediate market order via POST /api/v1/positions.
func (a *Adapter) placeMarketOrder(ctx context.Context, req broker.TradeRequest) (broker.TradeReceipt, error) {
	if err := a.ensureSession(ctx); err != nil {
		return broker.TradeReceipt{}, err
	}

	direction := "BUY"
	if req.Units < 0 || req.Quantity < 0 {
		direction = "SELL"
	}
	size := capitalDealSize(req)

	type orderPayload struct {
		Epic           string   `json:"epic"`
		Direction      string   `json:"direction"`
		Size           float64  `json:"size"`
		GuaranteedStop bool     `json:"guaranteedStop"`
		StopLevel      *float64 `json:"stopLevel,omitempty"`
		ProfitLevel    *float64 `json:"profitLevel,omitempty"`
	}
	pl := orderPayload{
		Epic:           capitalEpic(req.Symbol),
		Direction:      direction,
		Size:           size,
		GuaranteedStop: false,
	}
	if req.StopLoss > 0 {
		pl.StopLevel = &req.StopLoss
	}
	if req.TakeProfit > 0 {
		pl.ProfitLevel = &req.TakeProfit
	}

	var result struct {
		DealReference string `json:"dealReference"`
		Status        string `json:"status"`
		Reason        string `json:"reason"`
	}
	if err := a.post(ctx, "/api/v1/positions", pl, &result); err != nil {
		// Capital enforces a per-instrument minimum stop distance. A too-tight stop is
		// rejected with stoploss.maxvalue (BUY) or stoploss.minvalue (SELL), carrying the
		// exact permitted stop level. Clamp the stop to that level and retry once.
		if lvl, ok := capitalClampStop(err.Error()); ok && pl.StopLevel != nil {
			pl.StopLevel = &lvl
			if err2 := a.post(ctx, "/api/v1/positions", pl, &result); err2 != nil {
				return broker.TradeReceipt{}, fmt.Errorf("capital place trade (after stop clamp): %w", err2)
			}
		} else {
			return broker.TradeReceipt{}, fmt.Errorf("capital place trade: %w", err)
		}
	}
	if result.Status == "REJECTED" {
		return broker.TradeReceipt{}, fmt.Errorf("capital order rejected: %s", result.Reason)
	}

	// Confirm the deal to get the filled price and dealId.
	var confirm struct {
		Status      string  `json:"status"`
		Reason      string  `json:"reason"`
		DealID      string  `json:"dealId"`
		Epic        string  `json:"epic"`
		Direction   string  `json:"direction"`
		Size        float64 `json:"size"`
		Level       float64 `json:"level"` // fill price
		StopLevel   float64 `json:"stopLevel"`
		ProfitLevel float64 `json:"profitLevel"`
		Date        string  `json:"date"`
	}
	if err := a.get(ctx, "/api/v1/confirms/"+result.DealReference, &confirm); err != nil {
		return broker.TradeReceipt{
			BrokerTradeID: "pending:" + result.DealReference,
			Symbol:        req.Symbol,
			Units:         req.Units,
			TakeProfit:    req.TakeProfit,
			StopLoss:      req.StopLoss,
			OpenedAt:      time.Now().UTC(),
			DecisionID:    req.DecisionID,
			Broker:        "capital",
		}, nil
	}
	if confirm.Status == "REJECTED" {
		return broker.TradeReceipt{}, fmt.Errorf("capital confirm rejected: %s", confirm.Reason)
	}
	return broker.TradeReceipt{
		BrokerTradeID: confirm.DealID,
		Symbol:        req.Symbol,
		Units:         req.Units,
		FilledPrice:   confirm.Level,
		TakeProfit:    confirm.ProfitLevel,
		StopLoss:      confirm.StopLevel,
		OpenedAt:      parseCapitalTime(confirm.Date),
		DecisionID:    req.DecisionID,
		Broker:        "capital",
	}, nil
}

// placeLimitOrder submits a LIMIT working order via POST /api/v1/workingorders.
// The order sits in the broker's queue until price reaches EntryPrice.
func (a *Adapter) placeLimitOrder(ctx context.Context, req broker.TradeRequest) (broker.TradeReceipt, error) {
	if err := a.ensureSession(ctx); err != nil {
		return broker.TradeReceipt{}, err
	}
	if req.EntryPrice <= 0 {
		return broker.TradeReceipt{}, fmt.Errorf("capital limit order: entry_price is required")
	}

	direction := "BUY"
	if req.Units < 0 || req.Quantity < 0 {
		direction = "SELL"
	}
	size := capitalDealSize(req)

	type limitPayload struct {
		Epic           string   `json:"epic"`
		Direction      string   `json:"direction"`
		Size           float64  `json:"size"`
		Level          float64  `json:"level"`        // limit trigger price
		Type           string   `json:"type"`         // "LIMIT"
		GoodTillDate   *string  `json:"goodTillDate"` // nil = GTC
		GuaranteedStop bool     `json:"guaranteedStop"`
		StopLevel      *float64 `json:"stopLevel,omitempty"`
		ProfitLevel    *float64 `json:"profitLevel,omitempty"`
	}
	pl := limitPayload{
		Epic:           capitalEpic(req.Symbol),
		Direction:      direction,
		Size:           size,
		Level:          req.EntryPrice,
		Type:           "LIMIT",
		GoodTillDate:   nil, // good till cancelled
		GuaranteedStop: false,
	}
	if req.StopLoss > 0 {
		pl.StopLevel = &req.StopLoss
	}
	if req.TakeProfit > 0 {
		pl.ProfitLevel = &req.TakeProfit
	}

	var result struct {
		DealReference string `json:"dealReference"`
		Status        string `json:"status"`
		Reason        string `json:"reason"`
	}
	if err := a.post(ctx, "/api/v1/workingorders", pl, &result); err != nil {
		// Capital enforces a per-instrument minimum stop distance. A too-tight stop is
		// rejected with stoploss.maxvalue (BUY) or stoploss.minvalue (SELL), carrying the
		// exact permitted stop level. Clamp the stop to that level and retry once.
		if lvl, ok := capitalClampStop(err.Error()); ok && pl.StopLevel != nil {
			pl.StopLevel = &lvl
			if err2 := a.post(ctx, "/api/v1/workingorders", pl, &result); err2 != nil {
				return broker.TradeReceipt{}, fmt.Errorf("capital limit order (after stop clamp): %w", err2)
			}
		} else {
			return broker.TradeReceipt{}, fmt.Errorf("capital limit order: %w", err)
		}
	}
	if result.Status == "REJECTED" {
		return broker.TradeReceipt{}, fmt.Errorf("capital limit order rejected: %s", result.Reason)
	}

	// Confirm the working order to obtain the permanent dealId.
	// This mirrors what placeMarketOrder does; without it we only have the
	// temporary dealReference which does not match the dealId returned by
	// OpenOrders(), making source-lookup impossible.
	var confirm struct {
		Status      string  `json:"status"`
		Reason      string  `json:"reason"`
		DealID      string  `json:"dealId"`
		Level       float64 `json:"level"`
		StopLevel   float64 `json:"stopLevel"`
		ProfitLevel float64 `json:"profitLevel"`
		Date        string  `json:"date"`
	}
	dealID := result.DealReference // fallback if confirm call fails
	if err := a.get(ctx, "/api/v1/confirms/"+result.DealReference, &confirm); err == nil {
		if confirm.Status == "REJECTED" {
			return broker.TradeReceipt{}, fmt.Errorf("capital limit order confirm rejected: %s", confirm.Reason)
		}
		if confirm.DealID != "" {
			dealID = confirm.DealID
		}
	}

	// Store the permanent dealId → source/tradeType so OpenOrders() can annotate.
	if dealID != "" {
		a.orderSourceMu.Lock()
		if req.Source != "" {
			a.orderSources[dealID] = req.Source
		}
		if req.TradeType != "" {
			a.orderTradeTypes[dealID] = req.TradeType
		}
		a.orderSourceMu.Unlock()
	}

	// Working orders don't fill immediately — return a pending receipt.
	return broker.TradeReceipt{
		BrokerTradeID: "limit:" + dealID,
		Symbol:        req.Symbol,
		Units:         req.Units,
		FilledPrice:   req.EntryPrice, // indicative — actual fill when triggered
		TakeProfit:    req.TakeProfit,
		StopLoss:      req.StopLoss,
		OpenedAt:      time.Now().UTC(),
		DecisionID:    req.DecisionID,
		Broker:        "capital",
	}, nil
}

// OpenTrades returns all open positions from Capital.com.
func (a *Adapter) OpenTrades(ctx context.Context) ([]broker.OpenTrade, error) {
	if err := a.ensureSession(ctx); err != nil {
		return nil, err
	}

	var resp struct {
		Positions []struct {
			Position struct {
				DealID      string  `json:"dealId"`
				Direction   string  `json:"direction"`
				Size        float64 `json:"size"`
				Level       float64 `json:"level"`
				StopLevel   float64 `json:"stopLevel"`
				ProfitLevel float64 `json:"profitLevel"`
			} `json:"position"`
			Market struct {
				Epic  string  `json:"epic"`
				Bid   float64 `json:"bid"`
				Offer float64 `json:"offer"` // Capital uses "offer" for ask
			} `json:"market"`
		} `json:"positions"`
	}
	if err := a.get(ctx, "/api/v1/positions", &resp); err != nil {
		return nil, fmt.Errorf("capital open trades: %w", err)
	}

	// quoteToUSD converts a price delta expressed in a pair's QUOTE currency
	// into the account currency (USD). Capital does not return a pre-computed
	// profit field, so we hand-compute (priceDelta × size) — which lands in the
	// quote currency. For JPY-quoted and other cross pairs that is ~150× the USD
	// figure unless converted. quoteToUSD handles quote==USD (factor 1),
	// USD-base (1/price), and crosses (static fallback).
	// FX lookups are memoized for the duration of this call.
	fxCache := map[string]float64{}
	lookup := func(pair string) (float64, bool) {
		if v, ok := fxCache[pair]; ok {
			return v, v > 0
		}
		bid, ask, qErr := a.LiveQuote(ctx, pair)
		mid := 0.0
		if qErr == nil && bid > 0 && ask > 0 {
			mid = (bid + ask) / 2
		}
		fxCache[pair] = mid
		return mid, mid > 0
	}

	out := make([]broker.OpenTrade, 0, len(resp.Positions))
	for _, p := range resp.Positions {
		// Capital.com returns size in raw units (lotSize=1), not decimal lots.
		units := int(p.Position.Size)
		isLong := p.Position.Direction != "SELL"
		if !isLong {
			units = -units
		}

		// Compute unrealized P&L using Capital's bid/ask from the market snapshot.
		// LONG closes at bid, SHORT closes at offer (ask). Capital does not return
		// a pre-computed profit field in the positions list.
		var pnl float64
		if isLong && p.Market.Bid > 0 {
			pnl = (p.Market.Bid - p.Position.Level) * p.Position.Size
		} else if !isLong && p.Market.Offer > 0 {
			pnl = (p.Position.Level - p.Market.Offer) * p.Position.Size
		}

		// Current price: bid for LONG (what you'd close at), offer for SHORT.
		curPrice := p.Market.Bid
		if !isLong && p.Market.Offer > 0 {
			curPrice = p.Market.Offer
		}

		sym := fromCapitalEpic(p.Market.Epic)

		// Convert the quote-currency P&L into USD so it matches Capital's own
		// account-level profit figure. factor is always > 0; sign is preserved.
		if factor, _ := quoteToUSD(sym, curPrice, lookup); factor > 0 {
			pnl *= factor
		}

		out = append(out, broker.OpenTrade{
			BrokerTradeID: p.Position.DealID,
			Symbol:        sym,
			Units:         units,
			OpenPrice:     p.Position.Level,
			CurrentPrice:  curPrice,
			UnrealizedPnL: pnl,
			StopLoss:      p.Position.StopLevel,
			TakeProfit:    p.Position.ProfitLevel,
			OpenedAt:      time.Time{},
		})
	}
	return out, nil
}

// OpenOrders returns all working orders (limit / stop entries pending fill)
// from Capital.com via GET /api/v1/workingorders.
func (a *Adapter) OpenOrders(ctx context.Context) ([]broker.OpenOrder, error) {
	if err := a.ensureSession(ctx); err != nil {
		return nil, err
	}

	var resp struct {
		WorkingOrders []struct {
			WorkingOrderData struct {
				DealID         string  `json:"dealId"`
				Direction      string  `json:"direction"`
				Epic           string  `json:"epic"`
				OrderSize      float64 `json:"orderSize"`
				OrderLevel     float64 `json:"orderLevel"`
				OrderType      string  `json:"orderType"`
				CreatedDateUTC string  `json:"createdDateUTC"`
				StopLevel      float64 `json:"stopLevel"`
				ProfitLevel    float64 `json:"profitLevel"`
			} `json:"workingOrderData"`
			MarketData struct {
				Epic string `json:"epic"`
			} `json:"marketData"`
		} `json:"workingOrders"`
	}
	if err := a.get(ctx, "/api/v1/workingorders", &resp); err != nil {
		return nil, fmt.Errorf("capital open orders: %w", err)
	}

	out := make([]broker.OpenOrder, 0, len(resp.WorkingOrders))
	for _, w := range resp.WorkingOrders {
		d := w.WorkingOrderData
		epic := d.Epic
		if epic == "" {
			epic = w.MarketData.Epic
		}
		units := int(d.OrderSize)
		if d.Direction == "SELL" {
			units = -units
		}
		created, _ := time.Parse("2006-01-02T15:04:05.000", d.CreatedDateUTC)
		a.orderSourceMu.RLock()
		src := a.orderSources[d.DealID]
		tt := a.orderTradeTypes[d.DealID]
		a.orderSourceMu.RUnlock()
		out = append(out, broker.OpenOrder{
			BrokerOrderID: d.DealID,
			Symbol:        fromCapitalEpic(epic),
			Direction:     d.Direction,
			Units:         units,
			OrderType:     d.OrderType,
			Level:         d.OrderLevel,
			TakeProfit:    d.ProfitLevel,
			StopLoss:      d.StopLevel,
			CreatedAt:     created,
			Source:        src,
			TradeType:     tt,
		})
	}
	return out, nil
}

// CancelOrder deletes a pending working order by its deal ID.
func (a *Adapter) CancelOrder(ctx context.Context, brokerOrderID string) error {
	if err := a.ensureSession(ctx); err != nil {
		return err
	}
	return a.delete(ctx, "/api/v1/workingorders/"+brokerOrderID)
}

// AmendOrder updates the level, SL, or TP of a pending working order.
// Capital uses PUT /api/v1/workingorders/{dealId}.
func (a *Adapter) AmendOrder(ctx context.Context, brokerOrderID string, newLevel, newSL, newTP float64) error {
	if err := a.ensureSession(ctx); err != nil {
		return err
	}
	type payload struct {
		Level       *float64 `json:"level,omitempty"`
		StopLevel   *float64 `json:"stopLevel,omitempty"`
		ProfitLevel *float64 `json:"profitLevel,omitempty"`
	}
	pl := payload{}
	if newLevel > 0 {
		pl.Level = &newLevel
	}
	if newSL > 0 {
		pl.StopLevel = &newSL
	}
	if newTP > 0 {
		pl.ProfitLevel = &newTP
	}
	return a.put(ctx, "/api/v1/workingorders/"+brokerOrderID, pl, nil)
}

// Account returns the Capital.com account snapshot.
func (a *Adapter) Account(ctx context.Context) (broker.BrokerAccount, error) {
	if err := a.ensureSession(ctx); err != nil {
		return broker.BrokerAccount{}, err
	}

	var resp struct {
		Accounts []struct {
			AccountID   string `json:"accountId"`
			AccountName string `json:"accountName"`
			Preferred   bool   `json:"preferred"`
			Balance     struct {
				Balance    float64 `json:"balance"`
				ProfitLoss float64 `json:"profitLoss"`
				Available  float64 `json:"available"`
				Deposit    float64 `json:"deposit"`
			} `json:"balance"`
			Currency string `json:"currency"`
			Status   string `json:"status"`
		} `json:"accounts"`
	}
	if err := a.get(ctx, "/api/v1/accounts", &resp); err != nil {
		return broker.BrokerAccount{}, fmt.Errorf("capital account: %w", err)
	}

	// Use preferred account; fall back to first.
	for _, acc := range resp.Accounts {
		if acc.Preferred || len(resp.Accounts) == 1 {
			return broker.BrokerAccount{
				AccountID:       acc.AccountID,
				Balance:         acc.Balance.Balance,
				NAV:             acc.Balance.Balance + acc.Balance.ProfitLoss,
				UnrealizedPnL:   acc.Balance.ProfitLoss,
				MarginUsed:      acc.Balance.Deposit,
				MarginAvailable: acc.Balance.Available,
				Currency:        cleanCurrencyCode(acc.Currency),
			}, nil
		}
	}
	return broker.BrokerAccount{}, fmt.Errorf("capital: no accounts returned")
}

// CloseTrade closes a position by dealId.
func (a *Adapter) CloseTrade(ctx context.Context, brokerTradeID string) error {
	if err := a.ensureSession(ctx); err != nil {
		return err
	}
	if err := a.delete(ctx, "/api/v1/positions/"+brokerTradeID); err != nil {
		return fmt.Errorf("capital close trade %s: %w", brokerTradeID, err)
	}
	return nil
}

// AmendTrade updates the stop-loss and/or take-profit on an open position.
// Capital.com uses PUT /api/v1/positions/{dealId} with stopLevel / profitLevel.
// Pass 0 to leave a level unchanged.
func (a *Adapter) AmendTrade(ctx context.Context, brokerTradeID string, newSL, newTP float64) error {
	if err := a.ensureSession(ctx); err != nil {
		return err
	}
	body := map[string]any{}
	if newSL > 0 {
		body["stopLevel"] = newSL
	}
	if newTP > 0 {
		body["profitLevel"] = newTP
	}
	if len(body) == 0 {
		return nil
	}
	var resp struct {
		DealReference string `json:"dealReference"`
	}
	if err := a.put(ctx, "/api/v1/positions/"+brokerTradeID, body, &resp); err != nil {
		return fmt.Errorf("capital amend trade %s: %w", brokerTradeID, err)
	}
	return nil
}

// Ping verifies connectivity and auth by fetching the account list.
// Non-interface method — used by /readyz health check.
func (a *Adapter) Ping(ctx context.Context) error {
	if err := a.ensureSession(ctx); err != nil {
		return err
	}
	var resp struct {
		Accounts []any `json:"accounts"`
	}
	return a.get(ctx, "/api/v1/accounts", &resp)
}

// LiveQuote returns the current bid and ask for a symbol using the Capital.com
// markets snapshot endpoint.
func (a *Adapter) LiveQuote(ctx context.Context, symbol string) (bid, ask float64, err error) {
	if err := a.ensureSession(ctx); err != nil {
		return 0, 0, err
	}
	var resp struct {
		Snapshot struct {
			Bid   float64 `json:"bid"`
			Offer float64 `json:"offer"`
		} `json:"snapshot"`
	}
	if gErr := a.get(ctx, "/api/v1/markets/"+capitalEpic(symbol), &resp); gErr != nil {
		return 0, 0, gErr
	}
	return resp.Snapshot.Bid, resp.Snapshot.Offer, nil
}

// MarketDealingRules returns the per-instrument deal-size constraints from the
// Capital.com markets endpoint (instrument.dealingRules). Implements
// broker.DealingRulesProvider. Used by the executor to clamp crypto orders.
func (a *Adapter) MarketDealingRules(ctx context.Context, symbol string) (broker.DealingRules, error) {
	if err := a.ensureSession(ctx); err != nil {
		return broker.DealingRules{}, err
	}
	var resp struct {
		DealingRules struct {
			MinDealSize struct {
				Value float64 `json:"value"`
			} `json:"minDealSize"`
			MaxDealSize struct {
				Value float64 `json:"value"`
			} `json:"maxDealSize"`
		} `json:"dealingRules"`
	}
	if err := a.get(ctx, "/api/v1/markets/"+capitalEpic(symbol), &resp); err != nil {
		return broker.DealingRules{}, fmt.Errorf("capital dealing rules %s: %w", symbol, err)
	}
	return broker.DealingRules{
		MinDealSize: resp.DealingRules.MinDealSize.Value,
		MaxDealSize: resp.DealingRules.MaxDealSize.Value,
	}, nil
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────

func (a *Adapter) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.baseURL+path, nil)
	if err != nil {
		return err
	}
	a.setHeaders(req)
	return a.do(req, out)
}

func (a *Adapter) post(ctx context.Context, path string, body, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	a.setHeaders(req)
	req.Header.Set("Content-Type", "application/json")
	return a.do(req, out)
}

func (a *Adapter) put(ctx context.Context, path string, body, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, a.baseURL+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	a.setHeaders(req)
	req.Header.Set("Content-Type", "application/json")
	return a.do(req, out)
}

func (a *Adapter) delete(ctx context.Context, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, a.baseURL+path, nil)
	if err != nil {
		return err
	}
	a.setHeaders(req)
	return a.do(req, nil)
}

func (a *Adapter) setHeaders(req *http.Request) {
	a.mu.Lock()
	cst := a.cst
	sec := a.secToken
	a.mu.Unlock()
	req.Header.Set("CST", cst)
	req.Header.Set("X-SECURITY-TOKEN", sec)
	req.Header.Set("X-CAP-API-KEY", a.apiKey)
}

// capitalMax429Retries bounds the retry loop on HTTP 429 so a sustained
// rate-limit episode fails loudly rather than blocking a scan indefinitely.
const capitalMax429Retries = 4

func (a *Adapter) do(req *http.Request, out any) error {
	ctx := req.Context()
	var lastErr error
	for attempt := 0; attempt <= capitalMax429Retries; attempt++ {
		// Pace every attempt through the shared gate (incl. retries) so we
		// never exceed Capital's req/s cap even while backing off.
		if err := a.gate.wait(ctx); err != nil {
			return err
		}

		// Re-arm the request body for each attempt; bodies built via
		// bytes.NewReader expose GetBody, so POST/PUT retries replay cleanly.
		if req.GetBody != nil {
			body, err := req.GetBody()
			if err != nil {
				return fmt.Errorf("capital retry body: %w", err)
			}
			req.Body = body
		}

		resp, err := a.client.Do(req)
		if err != nil {
			return fmt.Errorf("capital http: %w", err)
		}

		// On 401, session expired — force re-auth on next call.
		if resp.StatusCode == 401 {
			resp.Body.Close()
			a.mu.Lock()
			a.cst = ""
			a.mu.Unlock()
			return fmt.Errorf("capital: session expired (re-auth will happen on next call)")
		}

		// On 429, back off and retry — Capital rate-limited this request.
		if resp.StatusCode == 429 {
			raw, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("capital %s %s → HTTP 429: %s",
				req.Method, req.URL.Path, truncate(string(raw), 200))
			if attempt == capitalMax429Retries {
				break
			}
			if err := sleepCtx(ctx, capital429Backoff(resp.Header.Get("Retry-After"), attempt)); err != nil {
				return err
			}
			continue
		}

		raw, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return fmt.Errorf("capital read body: %w", err)
		}
		if resp.StatusCode >= 400 {
			return fmt.Errorf("capital %s %s → HTTP %d: %s",
				req.Method, req.URL.Path, resp.StatusCode, truncate(string(raw), 200))
		}
		if out != nil && len(raw) > 0 {
			if err := json.Unmarshal(raw, out); err != nil {
				return fmt.Errorf("capital decode: %w (body=%s)", err, truncate(string(raw), 100))
			}
		}
		return nil
	}
	return lastErr
}

// capital429Backoff computes the wait before retrying a 429. It honours a
// numeric Retry-After header when present, otherwise uses exponential backoff
// (250ms, 500ms, 1s, 2s …) capped at 5s.
func capital429Backoff(retryAfter string, attempt int) time.Duration {
	if retryAfter != "" {
		if secs, err := strconv.Atoi(strings.TrimSpace(retryAfter)); err == nil && secs > 0 {
			d := time.Duration(secs) * time.Second
			if d > 5*time.Second {
				d = 5 * time.Second
			}
			return d
		}
	}
	d := 250 * time.Millisecond * (1 << attempt)
	if d > 5*time.Second {
		d = 5 * time.Second
	}
	return d
}

// sleepCtx sleeps for d unless ctx is cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// capitalEpic converts a symbol to Capital.com epic format.
// Strips slashes so "USD/CHF" → "USDCHF" as required by the Capital.com API.
func capitalEpic(symbol string) string {
	return strings.ToUpper(strings.ReplaceAll(symbol, "/", ""))
}

// fromCapitalEpic converts a Capital.com epic back to our canonical symbol.
// Forex 6-char pairs (USDCHF) are reconstructed as "USD/CHF".
func fromCapitalEpic(epic string) string {
	s := strings.ToUpper(epic)
	if len(s) == 6 {
		base := s[:3]
		quote := s[3:]
		_, baseIsFiat := knownFiatCurrencies[base]
		_, quoteIsFiat := knownFiatCurrencies[quote]
		if baseIsFiat && quoteIsFiat {
			return base + "/" + quote
		}
	}
	return s
}

// knownFiatCurrencies mirrors StockAI's domain.knownCurrencies for the forex
// epic split guard. Inlined to avoid importing the aegis root package.
var knownFiatCurrencies = map[string]struct{}{
	"USD": {}, "EUR": {}, "GBP": {}, "JPY": {}, "CHF": {},
	"AUD": {}, "CAD": {}, "NZD": {}, "SEK": {}, "NOK": {},
	"DKK": {}, "SGD": {}, "HKD": {}, "CNY": {}, "MXN": {},
	"ZAR": {}, "TRY": {}, "BRL": {}, "INR": {}, "KRW": {},
}

// unitsToCapitalSize converts internal units (integers, e.g. 1000) to the
// Capital.com deal size field.
//
// Capital.com's instrument endpoint returns lotSize=1 for forex CFDs, which
// means their "size" field is the raw number of units (not decimal lots).
// Sending size=0.01 produces error.invalid.size.minvalue because the API
// expects integer-scale units like 1000 (1000 units of base currency).
//
// Minimum enforced at 1000 units — a micro-position (~10 USD margin at 1% req).
func unitsToCapitalSize(units int) float64 {
	if units < 0 {
		units = -units
	}
	if units < 1000 {
		units = 1000 // Capital.com minimum deal size for forex CFDs
	}
	return float64(units)
}

// capitalDealSize selects the order's "size" field. When req.Quantity is set
// (crypto: a fractional base-asset amount already clamped to the instrument's
// dealing rules by the executor) it is used verbatim — the 1000-unit forex-CFD
// minimum floor must NOT apply, and the value stays fractional. Otherwise the
// integer Units path runs through unitsToCapitalSize (forex/stocks). Returns an
// absolute (unsigned) size; direction is conveyed separately.
func capitalDealSize(req broker.TradeRequest) float64 {
	if req.Quantity != 0 {
		return math.Abs(req.Quantity)
	}
	return unitsToCapitalSize(req.Units)
}

// cleanCurrencyCode strips non-alpha characters from Capital.com currency codes.
// Capital.com sometimes returns "EURd" instead of "EUR" — this normalises it.
func cleanCurrencyCode(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') {
			b.WriteRune(r)
		}
	}
	code := strings.ToUpper(b.String())
	if len(code) > 3 {
		return code[:3]
	}
	return code
}

// parseCapitalTime parses any of the date formats Capital.com returns across
// its endpoints. Returns the zero time on parse failure so callers can detect
// bad data.
//
// Known formats:
//
//	"2023/01/15 10:30:00:000"   — confirmation deal date
//	"2024-05-12T14:00:00"       — prices endpoint snapshotTimeUTC (no zone)
//	"2024-05-12T14:00:00.000"   — same with milliseconds
//	"2024-05-12T14:00:00Z"      — some endpoints add the explicit Z
func parseCapitalTime(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	formats := []string{
		"2006/01/02 15:04:05:000",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05.000",
		"2006-01-02T15:04:05Z",
		time.RFC3339,
	}
	for _, f := range formats {
		if t, err := time.ParseInLocation(f, s, time.UTC); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

// parseCapitalMinValue extracts the numeric minimum from a Capital.com error string.
// e.g. "error.invalid.stoploss.minvalue: 214.0275" → 214.0275
func parseCapitalMinValue(errStr, key string) (float64, error) {
	idx := strings.Index(errStr, key+": ")
	if idx == -1 {
		return 0, fmt.Errorf("key %q not in error", key)
	}
	rest := errStr[idx+len(key)+2:]
	end := strings.IndexAny(rest, " \t\n\r\"}")
	if end == -1 {
		end = len(rest)
	}
	return strconv.ParseFloat(strings.TrimSpace(rest[:end]), 64)
}

// capitalClampStop inspects a Capital order-rejection error for a stop-distance
// violation and returns the broker's permitted stop LEVEL to clamp to. Capital
// returns the exact boundary price: stoploss.maxvalue for BUY (stop must be ≤ it),
// stoploss.minvalue for SELL (stop must be ≥ it). Setting the stop to that level is
// the closest valid stop to entry. ok=false if the error is not a stop-distance violation.
func capitalClampStop(errStr string) (level float64, ok bool) {
	if v, err := parseCapitalMinValue(errStr, "stoploss.maxvalue"); err == nil {
		return v, true
	}
	if v, err := parseCapitalMinValue(errStr, "stoploss.minvalue"); err == nil {
		return v, true
	}
	return 0, false
}

// truncate returns up to n runes of s.
func truncate(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n]) + "…"
}

// ── Local quoteToUSD ──────────────────────────────────────────────────────────
//
// Inlined from StockAI domain/pairs.go (~159) to avoid importing the aegis root
// package (which would create a circular import: broker/capital → aegis → broker).
// This copy must stay consistent with aegis executor.go's staticQuoteUSD table.

// quoteToUSD returns how many USD equal one unit of the QUOTE currency for the
// given pair at the given price. factor is always > 0. exact=false signals a
// static fallback was used.
//
// Rules:
//   - QUOTE == "USD" (EURUSD, GBPUSD) → 1.0, exact=true
//   - BASE  == "USD" (USDJPY, USDCHF) → 1/entryPrice, exact=true
//   - Cross pair (no USD leg)          → live lookup, then static table, exact=false
//   - Non-fiat / unknown               → 1.0, exact=false (safe default)
func quoteToUSD(symbol string, entryPrice float64, liveLookup func(pair string) (float64, bool)) (float64, bool) {
	clean := strings.NewReplacer("/", "", "-", "", "_", "").Replace(strings.ToUpper(strings.TrimSpace(symbol)))
	if len(clean) != 6 {
		return 1.0, false
	}
	base := clean[:3]
	quote := clean[3:]

	switch {
	case quote == "USD":
		return 1.0, true
	case base == "USD":
		if entryPrice > 0 {
			return 1.0 / entryPrice, true
		}
	}

	// Cross pair: try live lookup first (e.g. GBPJPY → look up JPYUSD).
	if liveLookup != nil {
		if mid, ok := liveLookup(quote + "USD"); ok && mid > 0 {
			return mid, true
		}
	}

	// Static fallback table — same values as aegis executor.go staticQuoteUSD.
	if r, ok := staticQuoteUSDLocal[quote]; ok && r > 0 {
		return r, false
	}
	return 1.0, false
}

// staticQuoteUSDLocal mirrors aegis executor.go staticQuoteUSD and StockAI
// domain/pairs.go staticQuoteUSD. Values are intentionally conservative; a live
// lookup always takes precedence when available.
var staticQuoteUSDLocal = map[string]float64{
	"USD": 1.00,
	"EUR": 1.08,
	"GBP": 1.27,
	"JPY": 1.0 / 150.0,
	"CHF": 1.13,
	"CAD": 0.73,
	"AUD": 0.66,
	"NZD": 0.60,
	"SEK": 0.095,
	"NOK": 0.093,
	"DKK": 0.145,
	"SGD": 0.74,
	"HKD": 0.128,
	"CNY": 0.139,
}
