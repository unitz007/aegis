// Package paper provides an in-memory paper broker that implements broker.Port.
//
// Behaviour:
//   - Market orders fill immediately at EntryPrice (or at the live quote if a
//     QuoteFetcher is wired and EntryPrice is zero).
//   - Limit/stop working orders queue until the live quote reaches their trigger
//     level, then fill into a position. Triggers are evaluated lazily on every
//     read path — no background goroutine is required. Requires a QuoteFetcher
//     to be wired; otherwise working orders never auto-fill.
//   - Open positions auto-close at their stop-loss/take-profit when the live
//     quote reaches the level (filled at the level, no slippage modelling).
//   - Unrealized P&L is recalculated on every OpenTrades call using the latest
//     live quote (falls back to open price when no QuoteFetcher is wired).
//   - Balance starts at the initialBalance argument. Realized P&L is
//     credited/debited when a trade is closed.
//   - All methods are safe for concurrent use.
//   - In-memory only: no persistence across process restarts (v1 design choice).
package paper

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/unitz007/aegis/broker"
)

// QuoteFetcher is the signature the paper broker uses to fetch live prices.
// Matches the common (ctx, symbol) → (price, error) pattern without importing
// any external domain package.
type QuoteFetcher func(ctx context.Context, symbol string) (float64, error)

// Broker is an in-memory paper broker for simulation and testing.
// Implements broker.Port and broker.DealingRulesProvider.
// Zero value is not valid — use New.
type Broker struct {
	mu       sync.RWMutex
	balance  float64
	currency string

	trades map[string]*paperTrade // tradeID → filled position
	orders map[string]*paperOrder // orderID → resting working order

	quoteFetcher   QuoteFetcher
	initialBalance float64

	// DealingRules holds per-symbol dealing rules returned by MarketDealingRules.
	// If a symbol is not present, default rules (MinDealSize=0, MaxDealSize=0) apply.
	DealingRules map[string]broker.DealingRules
}

type paperTrade struct {
	id         string
	symbol     string
	units      int     // positive = long, negative = short
	openPrice  float64
	takeProfit float64
	stopLoss   float64
	openedAt   time.Time
	decisionID string
}

type paperOrder struct {
	id         string
	symbol     string
	units      int    // positive = long (BUY), negative = short (SELL)
	orderType  string // "LIMIT" | "STOP"
	level      float64
	takeProfit float64
	stopLoss   float64
	createdAt  time.Time
	decisionID string
	source     string
	tradeType  string
}

// New creates a paper broker with the given starting balance.
// currency is the account denomination (e.g. "USD"). An empty currency
// defaults to "USD".
// quoteFetcher may be nil; when nil, live-price-dependent features (trigger
// fills, auto-exit, live P&L) are disabled.
func New(initialBalance float64, quoteFetcher QuoteFetcher) *Broker {
	if initialBalance <= 0 {
		initialBalance = 100_000
	}
	return &Broker{
		balance:        initialBalance,
		currency:       "USD",
		trades:         make(map[string]*paperTrade),
		orders:         make(map[string]*paperOrder),
		quoteFetcher:   quoteFetcher,
		initialBalance: initialBalance,
		DealingRules:   make(map[string]broker.DealingRules),
	}
}

// Reset clears all open trades and working orders, restoring the balance to
// the initial value. Useful between test runs.
func (b *Broker) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.trades = make(map[string]*paperTrade)
	b.orders = make(map[string]*paperOrder)
	b.balance = b.initialBalance
}

// Balance returns the current realized cash balance.
func (b *Broker) Balance() float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.balance
}

// TradeCount returns the number of currently open positions.
func (b *Broker) TradeCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.trades)
}

// OrderCount returns the number of resting working orders.
func (b *Broker) OrderCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.orders)
}

// ── broker.Port implementation ────────────────────────────────────────────────

// PlaceTrade fills a market order instantly at EntryPrice, or — for a
// limit/stop order — queues a resting working order.
func (b *Broker) PlaceTrade(ctx context.Context, req broker.TradeRequest) (broker.TradeReceipt, error) {
	if ot := strings.ToLower(req.OrderType); ot == "limit" || ot == "stop" {
		return b.placeWorkingOrder(ctx, req, strings.ToUpper(ot))
	}

	// Market order: fill immediately.
	fillPrice := req.EntryPrice
	if fillPrice == 0 && b.quoteFetcher != nil {
		price, err := b.quoteFetcher(ctx, req.Symbol)
		if err != nil {
			return broker.TradeReceipt{}, fmt.Errorf("paper broker: fetch live price for %s: %w", req.Symbol, err)
		}
		fillPrice = price
	}
	if fillPrice == 0 {
		return broker.TradeReceipt{}, fmt.Errorf("paper broker: cannot determine fill price for %s (set EntryPrice or wire a QuoteFetcher)", req.Symbol)
	}

	tradeID := newTradeID()
	now := time.Now().UTC()

	b.mu.Lock()
	b.trades[tradeID] = &paperTrade{
		id:         tradeID,
		symbol:     req.Symbol,
		units:      req.Units,
		openPrice:  fillPrice,
		takeProfit: req.TakeProfit,
		stopLoss:   req.StopLoss,
		openedAt:   now,
		decisionID: req.DecisionID,
	}
	b.mu.Unlock()

	log.Printf("paper broker: market order filled %s %s units=%d @ %.5f id=%s",
		dirStr(req.Units), req.Symbol, req.Units, fillPrice, tradeID)

	return broker.TradeReceipt{
		BrokerTradeID: tradeID,
		Symbol:        req.Symbol,
		Units:         req.Units,
		FilledPrice:   fillPrice,
		TakeProfit:    req.TakeProfit,
		StopLoss:      req.StopLoss,
		OpenedAt:      now,
		DecisionID:    req.DecisionID,
		Broker:        "paper",
	}, nil
}

// placeWorkingOrder queues a resting limit or stop order.
func (b *Broker) placeWorkingOrder(_ context.Context, req broker.TradeRequest, ot string) (broker.TradeReceipt, error) {
	if req.EntryPrice <= 0 {
		return broker.TradeReceipt{}, fmt.Errorf("paper broker: %s order for %s requires a trigger price (EntryPrice > 0)", ot, req.Symbol)
	}
	orderID := newOrderID()
	now := time.Now().UTC()

	b.mu.Lock()
	b.orders[orderID] = &paperOrder{
		id:         orderID,
		symbol:     req.Symbol,
		units:      req.Units,
		orderType:  ot,
		level:      req.EntryPrice,
		takeProfit: req.TakeProfit,
		stopLoss:   req.StopLoss,
		createdAt:  now,
		decisionID: req.DecisionID,
		source:     req.Source,
		tradeType:  req.TradeType,
	}
	b.mu.Unlock()

	log.Printf("paper broker: %s %s %s working order queued id=%s level=%.5f units=%d",
		dirStr(req.Units), ot, req.Symbol, orderID, req.EntryPrice, req.Units)

	return broker.TradeReceipt{
		BrokerTradeID: orderID,
		Symbol:        req.Symbol,
		Units:         req.Units,
		FilledPrice:   req.EntryPrice, // trigger level — order is pending, not yet filled
		TakeProfit:    req.TakeProfit,
		StopLoss:      req.StopLoss,
		OpenedAt:      now,
		DecisionID:    req.DecisionID,
		Broker:        "paper",
	}, nil
}

// OpenTrades returns all currently open positions with live unrealized P&L.
func (b *Broker) OpenTrades(ctx context.Context) ([]broker.OpenTrade, error) {
	b.fillTriggeredOrders(ctx)
	b.processPositionExits(ctx)

	b.mu.RLock()
	snapshot := make([]*paperTrade, 0, len(b.trades))
	for _, t := range b.trades {
		snapshot = append(snapshot, t)
	}
	b.mu.RUnlock()

	out := make([]broker.OpenTrade, len(snapshot))
	var wg sync.WaitGroup
	for i, t := range snapshot {
		wg.Add(1)
		go func(idx int, trade *paperTrade) {
			defer wg.Done()
			currentPrice := trade.openPrice
			if b.quoteFetcher != nil {
				if price, err := b.quoteFetcher(ctx, trade.symbol); err == nil && price > 0 {
					currentPrice = price
				}
			}
			pnl := (currentPrice - trade.openPrice) * float64(trade.units)
			out[idx] = broker.OpenTrade{
				BrokerTradeID: trade.id,
				Symbol:        trade.symbol,
				Units:         trade.units,
				OpenPrice:     trade.openPrice,
				CurrentPrice:  currentPrice,
				UnrealizedPnL: math.Round(pnl*100) / 100,
				TakeProfit:    trade.takeProfit,
				StopLoss:      trade.stopLoss,
				OpenedAt:      trade.openedAt,
			}
		}(i, t)
	}
	wg.Wait()
	return out, nil
}

// OpenOrders returns the resting working orders that haven't triggered yet.
func (b *Broker) OpenOrders(ctx context.Context) ([]broker.OpenOrder, error) {
	b.fillTriggeredOrders(ctx)

	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]broker.OpenOrder, 0, len(b.orders))
	for _, o := range b.orders {
		out = append(out, broker.OpenOrder{
			BrokerOrderID: o.id,
			Symbol:        o.symbol,
			Direction:     dirStr(o.units),
			Units:         o.units,
			OrderType:     o.orderType,
			Level:         o.level,
			TakeProfit:    o.takeProfit,
			StopLoss:      o.stopLoss,
			CreatedAt:     o.createdAt,
			Source:        o.source,
			TradeType:     o.tradeType,
		})
	}
	return out, nil
}

// Account returns the paper account snapshot.
func (b *Broker) Account(ctx context.Context) (broker.BrokerAccount, error) {
	trades, err := b.OpenTrades(ctx)
	if err != nil {
		return broker.BrokerAccount{}, err
	}
	var totalUPnL float64
	for _, t := range trades {
		totalUPnL += t.UnrealizedPnL
	}

	b.mu.RLock()
	bal := b.balance
	ccy := b.currency
	b.mu.RUnlock()

	nav := math.Round((bal+totalUPnL)*100) / 100
	return broker.BrokerAccount{
		AccountID:       "paper-account",
		Balance:         bal,
		NAV:             nav,
		UnrealizedPnL:   math.Round(totalUPnL*100) / 100,
		MarginUsed:      0,
		MarginAvailable: nav,
		Currency:        ccy,
	}, nil
}

// CloseTrade closes an open paper position and credits realized P&L.
func (b *Broker) CloseTrade(ctx context.Context, brokerTradeID string) error {
	// Read snapshot under read lock.
	b.mu.RLock()
	t, ok := b.trades[brokerTradeID]
	b.mu.RUnlock()
	if !ok {
		return fmt.Errorf("paper broker: trade %s not found", brokerTradeID)
	}

	// Fetch close price outside any lock.
	closePrice := t.openPrice
	if b.quoteFetcher != nil {
		if price, err := b.quoteFetcher(ctx, t.symbol); err == nil && price > 0 {
			closePrice = price
		}
	}

	// Atomically re-verify, delete, and credit.
	b.mu.Lock()
	if _, stillOpen := b.trades[brokerTradeID]; !stillOpen {
		b.mu.Unlock()
		return fmt.Errorf("paper broker: trade %s not found (already closed)", brokerTradeID)
	}
	delete(b.trades, brokerTradeID)
	realizedPnL := (closePrice - t.openPrice) * float64(t.units)
	b.balance += realizedPnL
	b.balance = math.Round(b.balance*100) / 100
	b.mu.Unlock()

	log.Printf("paper broker: closed %s units=%d @ %.5f realizedPnL=%.2f", brokerTradeID, t.units, closePrice, realizedPnL)
	return nil
}

// LiveQuote is a no-op stub — no real spread exists in the paper broker.
// Returns (0, 0, nil); wired callers that need live prices use the QuoteFetcher.
func (b *Broker) LiveQuote(_ context.Context, _ string) (float64, float64, error) {
	return 0, 0, nil
}

// AmendTrade updates the SL/TP on an in-memory paper position.
func (b *Broker) AmendTrade(_ context.Context, brokerTradeID string, newSL, newTP float64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	t, ok := b.trades[brokerTradeID]
	if !ok {
		return fmt.Errorf("paper broker: trade %s not found", brokerTradeID)
	}
	if newSL > 0 {
		t.stopLoss = newSL
	}
	if newTP > 0 {
		t.takeProfit = newTP
	}
	return nil
}

// CancelOrder removes a resting working order.
func (b *Broker) CancelOrder(_ context.Context, brokerOrderID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.orders[brokerOrderID]; !ok {
		return fmt.Errorf("paper broker: order %s not found", brokerOrderID)
	}
	delete(b.orders, brokerOrderID)
	log.Printf("paper broker: cancelled working order %s", brokerOrderID)
	return nil
}

// AmendOrder updates the trigger level, SL, or TP of a resting working order.
func (b *Broker) AmendOrder(_ context.Context, brokerOrderID string, newLevel, newSL, newTP float64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	o, ok := b.orders[brokerOrderID]
	if !ok {
		return fmt.Errorf("paper broker: order %s not found", brokerOrderID)
	}
	if newLevel > 0 {
		o.level = newLevel
	}
	if newSL > 0 {
		o.stopLoss = newSL
	}
	if newTP > 0 {
		o.takeProfit = newTP
	}
	return nil
}

// MarketDealingRules implements broker.DealingRulesProvider.
// Returns the configured rules for the symbol, or zero-value rules if not configured.
func (b *Broker) MarketDealingRules(_ context.Context, symbol string) (broker.DealingRules, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if rules, ok := b.DealingRules[strings.ToUpper(symbol)]; ok {
		return rules, nil
	}
	return broker.DealingRules{}, nil
}

// ── Internal helpers ──────────────────────────────────────────────────────────

// fillTriggeredOrders converts any resting working order whose trigger has been
// reached into an open position. Evaluated lazily on every read path.
func (b *Broker) fillTriggeredOrders(ctx context.Context) {
	if b.quoteFetcher == nil {
		return
	}

	b.mu.RLock()
	if len(b.orders) == 0 {
		b.mu.RUnlock()
		return
	}
	snapshot := make([]paperOrder, 0, len(b.orders))
	for _, o := range b.orders {
		snapshot = append(snapshot, *o)
	}
	b.mu.RUnlock()

	type fill struct {
		order     paperOrder
		fillPrice float64
	}
	var fills []fill
	for _, o := range snapshot {
		price, err := b.quoteFetcher(ctx, o.symbol)
		if err != nil || price <= 0 {
			continue
		}
		if orderTriggered(o, price) {
			fills = append(fills, fill{order: o, fillPrice: o.level})
		}
	}
	if len(fills) == 0 {
		return
	}

	now := time.Now().UTC()
	b.mu.Lock()
	for _, f := range fills {
		if _, stillPending := b.orders[f.order.id]; !stillPending {
			continue
		}
		delete(b.orders, f.order.id)
		t := &paperTrade{
			id:         newTradeID(),
			symbol:     f.order.symbol,
			units:      f.order.units,
			openPrice:  f.fillPrice,
			takeProfit: f.order.takeProfit,
			stopLoss:   f.order.stopLoss,
			openedAt:   now,
			decisionID: f.order.decisionID,
		}
		b.trades[t.id] = t
		log.Printf("paper broker: working order filled → position %s %s units=%d @ %.5f", t.id, t.symbol, t.units, t.openPrice)
	}
	b.mu.Unlock()
}

// orderTriggered reports whether a resting order's trigger is reached at price.
//
//	BUY  LIMIT → fills at/below level   SELL LIMIT → fills at/above level
//	BUY  STOP  → fills at/above level   SELL STOP  → fills at/below level
func orderTriggered(o paperOrder, price float64) bool {
	isBuy := o.units > 0
	switch o.orderType {
	case "LIMIT":
		if isBuy {
			return price <= o.level
		}
		return price >= o.level
	case "STOP":
		if isBuy {
			return price >= o.level
		}
		return price <= o.level
	}
	return false
}

// processPositionExits closes any open position whose live quote has reached
// its stop-loss or take-profit.
func (b *Broker) processPositionExits(ctx context.Context) {
	if b.quoteFetcher == nil {
		return
	}

	b.mu.RLock()
	if len(b.trades) == 0 {
		b.mu.RUnlock()
		return
	}
	snapshot := make([]paperTrade, 0, len(b.trades))
	for _, t := range b.trades {
		if t.takeProfit > 0 || t.stopLoss > 0 {
			snapshot = append(snapshot, *t)
		}
	}
	b.mu.RUnlock()

	type exitInfo struct {
		id         string
		reason     string
		closePrice float64
		pnl        float64
	}
	var exits []exitInfo
	for _, t := range snapshot {
		price, err := b.quoteFetcher(ctx, t.symbol)
		if err != nil || price <= 0 {
			continue
		}
		if hit, cp, reason := positionExit(t, price); hit {
			exits = append(exits, exitInfo{
				id: t.id, reason: reason, closePrice: cp,
				pnl: (cp - t.openPrice) * float64(t.units),
			})
		}
	}
	if len(exits) == 0 {
		return
	}

	b.mu.Lock()
	for _, e := range exits {
		if _, ok := b.trades[e.id]; !ok {
			continue
		}
		delete(b.trades, e.id)
		b.balance += e.pnl
		b.balance = math.Round(b.balance*100) / 100
		log.Printf("paper broker: position %s auto-closed at %s price=%.5f realized P&L=%.2f", e.id, e.reason, e.closePrice, e.pnl)
	}
	b.mu.Unlock()
}

// positionExit reports whether a position's SL or TP has been reached.
func positionExit(t paperTrade, price float64) (hit bool, closePrice float64, reason string) {
	isLong := t.units > 0
	if isLong {
		if t.takeProfit > 0 && price >= t.takeProfit {
			return true, t.takeProfit, "TP"
		}
		if t.stopLoss > 0 && price <= t.stopLoss {
			return true, t.stopLoss, "SL"
		}
		return false, 0, ""
	}
	if t.takeProfit > 0 && price <= t.takeProfit {
		return true, t.takeProfit, "TP"
	}
	if t.stopLoss > 0 && price >= t.stopLoss {
		return true, t.stopLoss, "SL"
	}
	return false, 0, ""
}

// ── ID generators ─────────────────────────────────────────────────────────────

func newTradeID() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 8)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return "paper-" + string(b)
}

func newOrderID() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 8)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return "paper-ord-" + string(b)
}

func dirStr(units int) string {
	if units >= 0 {
		return "BUY"
	}
	return "SELL"
}
