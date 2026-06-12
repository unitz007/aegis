package main

import (
	"strings"
	"time"
)

// StockAISessionGate is an aegis.SessionGateFunc that replicates the Asian-session
// execution guard from StockAI trade_executor.go (lines 155–165) and
// internal/adapters/harness/hooks.go (~80–110).
//
// Rules (UTC):
//   - Forex pairs: deferred during Asian hours (00:00–07:00 and 21:00–24:00)
//     unless the pair has a JPY / AUD / NZD leg (natively Asian flow).
//   - Crypto: always allowed (24/7 market).
//   - Stocks / other: always allowed.
//
// The gate intentionally allows the setup to be VISIBLE; only broker order
// placement is deferred. This matches the live executor behaviour.
func StockAISessionGate(symbol, assetClass string, now time.Time) bool {
	if assetClass != "forex" {
		return true // crypto and stocks trade 24/7
	}
	hour := now.UTC().Hour()*60 + now.UTC().Minute()
	isAsian := hour < 7*60 || hour >= 21*60
	if !isAsian {
		return true // London / NY / overlap — always execute
	}
	// Asian hours: only allow natively Asian pairs.
	return pairAllowedInAsian(symbol)
}

// pairAllowedInAsian returns true when the symbol's base or quote currency is
// natively traded during the Asian session. Ported from StockAI
// internal/app/ta/entry.go (~151).
//
//	JPY pairs (USDJPY, EURJPY, GBPJPY, AUDJPY, ...) → allowed
//	AUD pairs (AUDUSD, AUDJPY, AUDNZD, ...)         → allowed
//	NZD pairs (NZDUSD, NZDJPY, ...)                 → allowed
//	EUR/USD/GBP cross with no JPY/AUD/NZD leg       → not natively Asian
func pairAllowedInAsian(symbol string) bool {
	clean := strings.ToUpper(
		strings.NewReplacer("/", "", "-", "", "_", "").Replace(strings.TrimSpace(symbol)),
	)
	if len(clean) < 6 {
		return false // malformed — fall back to vetoing
	}
	base := clean[:3]
	quote := clean[3:6]
	return asianSessionCurrencies[base] || asianSessionCurrencies[quote]
}

// asianSessionCurrencies mirrors StockAI internal/app/ta/entry.go asianSessionPairs.
// Currency codes that trade with real volume during the Asian session
// (00:00–08:00 UTC). When ANY of these is on either leg of a pair, the
// Asian-session veto does NOT apply.
var asianSessionCurrencies = map[string]bool{
	"JPY": true, // Tokyo
	"AUD": true, // Sydney
	"NZD": true, // Wellington
}
