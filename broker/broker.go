// Package broker defines the Port interface all broker adapters must satisfy,
// plus the shared value types used across the execution pipeline.
//
// All Port implementations must be safe for concurrent use.
package broker

import (
	"context"
	"time"
)

// Port is the interface all broker adapters must satisfy.
// Implementations must be safe for concurrent use.
type Port interface {
	// PlaceTrade opens a new market or limit order.
	PlaceTrade(ctx context.Context, req TradeRequest) (TradeReceipt, error)

	// OpenTrades returns all currently open positions.
	OpenTrades(ctx context.Context) ([]OpenTrade, error)

	// OpenOrders returns all pending (not yet filled) working orders.
	OpenOrders(ctx context.Context) ([]OpenOrder, error)

	// Account returns a snapshot of account balance and margin.
	Account(ctx context.Context) (BrokerAccount, error)

	// CloseTrade closes an open position at the current market price.
	CloseTrade(ctx context.Context, brokerTradeID string) error

	// LiveQuote fetches the current bid and ask for a symbol.
	LiveQuote(ctx context.Context, symbol string) (bid, ask float64, err error)

	// AmendTrade updates the stop-loss and/or take-profit of an open position.
	// A zero value for either level leaves that field unchanged.
	AmendTrade(ctx context.Context, brokerTradeID string, newSL, newTP float64) error

	// CancelOrder removes a resting working order before it triggers.
	CancelOrder(ctx context.Context, brokerOrderID string) error

	// AmendOrder updates the trigger level, SL, or TP of a resting working order.
	// A zero value for any field leaves that field unchanged.
	AmendOrder(ctx context.Context, brokerOrderID string, newLevel, newSL, newTP float64) error
}

// DealingRules describes per-instrument size constraints.
// MinDealSize and MaxDealSize are in broker-native units (base-asset units for
// crypto CFDs on Capital.com, contract units for stocks/futures).
type DealingRules struct {
	MinDealSize float64
	MaxDealSize float64
}

// DealingRulesProvider is an optional capability a Port may expose.
// Callers type-assert before crypto sizing to clamp/skip within broker limits.
//
// Safety invariant: if the broker does not implement DealingRulesProvider, or
// if MarketDealingRules returns an error, the executor MUST skip the trade.
// Never fall back to placing an unclamped crypto order.
type DealingRulesProvider interface {
	MarketDealingRules(ctx context.Context, symbol string) (DealingRules, error)
}

// ── Value types ───────────────────────────────────────────────────────────────

// TradeRequest is an instruction to open a market or limit order via a broker.
//
// For forex and stocks: Units encodes both direction and size.
//   - Units > 0 → long (BUY)
//   - Units < 0 → short (SELL)
//
// For crypto CFDs (e.g. Capital.com): the broker's "size" field is in
// base-asset units, not USD notional. Use Quantity for the fractional deal size
// and set Units to ±1 to indicate direction. When Quantity is non-zero it takes
// precedence over Units for the broker deal size.
type TradeRequest struct {
	Symbol string `json:"symbol"`
	// Units is the signed integer deal size. Positive = long, negative = short.
	// For crypto, set to ±1 and use Quantity for the fractional size.
	Units int `json:"units"`
	// Quantity is the fractional base-asset size for crypto CFDs.
	// Zero = use Units (forex/stocks).
	Quantity   float64 `json:"quantity,omitempty"`
	OrderType  string  `json:"order_type,omitempty"` // "market" | "limit"; default = "market"
	EntryPrice float64 `json:"entry_price"`          // indicative for market; trigger for limit
	TakeProfit float64 `json:"take_profit"`
	StopLoss   float64 `json:"stop_loss"`
	DecisionID string  `json:"decision_id,omitempty"`
	// Source is stamped by the executor: "algo" for deterministic strategies,
	// "ai" for LLM-sourced decisions.
	Source    string `json:"source,omitempty"`
	TradeType string `json:"trade_type,omitempty"` // caller-supplied tag
}

// TradeReceipt is the broker's acknowledgement of a submitted order.
type TradeReceipt struct {
	BrokerTradeID string    `json:"broker_trade_id"`
	Symbol        string    `json:"symbol"`
	Units         int       `json:"units"`
	FilledPrice   float64   `json:"filled_price"`
	TakeProfit    float64   `json:"take_profit"`
	StopLoss      float64   `json:"stop_loss"`
	OpenedAt      time.Time `json:"opened_at"`
	DecisionID    string    `json:"decision_id,omitempty"`
	Broker        string    `json:"broker,omitempty"` // e.g. "capital" | "oanda" | "paper"
}

// OpenTrade is a position currently held at the broker.
type OpenTrade struct {
	BrokerTradeID string    `json:"broker_trade_id"`
	Symbol        string    `json:"symbol"`
	Units         int       `json:"units"` // positive = long
	OpenPrice     float64   `json:"open_price"`
	CurrentPrice  float64   `json:"current_price"`
	UnrealizedPnL float64   `json:"unrealized_pnl"`
	TakeProfit    float64   `json:"take_profit,omitempty"`
	StopLoss      float64   `json:"stop_loss,omitempty"`
	OpenedAt      time.Time `json:"opened_at"`
}

// OpenOrder is a working order pending fill at the broker (limit/stop entry
// that hasn't triggered yet). Distinct from OpenTrade — a filled position.
//
// Both open trades and open orders must be checked before placing a new entry
// to prevent stacking duplicates.
type OpenOrder struct {
	BrokerOrderID string    `json:"broker_order_id"`
	Symbol        string    `json:"symbol"`
	Direction     string    `json:"direction"`  // "BUY" | "SELL"
	Units         int       `json:"units"`      // positive = long, negative = short
	OrderType     string    `json:"order_type"` // "LIMIT" | "STOP"
	Level         float64   `json:"level"`      // trigger price
	TakeProfit    float64   `json:"take_profit,omitempty"`
	StopLoss      float64   `json:"stop_loss,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	// Source distinguishes human-placed orders from algo-placed orders.
	// Human-placed orders (Source == "human") are NEVER flip-cancelled by the
	// executor — this is a safety invariant (#4).
	Source    string `json:"source,omitempty"`    // "auto" | "human"
	TradeType string `json:"trade_type,omitempty"`
}

// BrokerAccount holds a snapshot of account balance and margin.
type BrokerAccount struct {
	AccountID       string  `json:"account_id"`
	Balance         float64 `json:"balance"`
	NAV             float64 `json:"nav"`
	UnrealizedPnL   float64 `json:"unrealized_pnl"`
	MarginUsed      float64 `json:"margin_used"`
	MarginAvailable float64 `json:"margin_available"`
	Currency        string  `json:"currency"`
}

// ClosedTrade records the complete lifecycle of a completed trade.
//
//	RealizedPnL = (ClosePrice − OpenPrice) × Units
//	Positive Units = long; negative Units = short.
//	RealizedPnL is positive for a profitable trade in both directions.
type ClosedTrade struct {
	BrokerName  string    `json:"broker_name"`
	Symbol      string    `json:"symbol"`
	Units       int       `json:"units"` // positive = long, negative = short
	OpenPrice   float64   `json:"open_price"`
	ClosePrice  float64   `json:"close_price"`
	RealizedPnL float64   `json:"realized_pnl"`
	OpenedAt    time.Time `json:"opened_at"`
	ClosedAt    time.Time `json:"closed_at"`
	DecisionID  string    `json:"decision_id,omitempty"`
	TPHit       bool      `json:"tp_hit"`
	// ExitReason: "tp" | "sl" | "manual" | "algo_flip"
	// "algo_flip" is set when the executor closes an opposite-direction position
	// before opening a new one — preserving the flip-close safety invariant (#5).
	ExitReason string `json:"exit_reason,omitempty"`
}
