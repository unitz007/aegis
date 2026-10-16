package aegis

import (
	"fmt"
	"math"
	"strings"

	"github.com/unitz007/aegis/broker"
)

// ── Currency-exposure helpers ─────────────────────────────────────────────────

// BuildCurrencyExposure returns a map of currency → net signed units across all
// open trades (positive = net long, negative = net short).
//
// For a symbol like "EURUSD":
//   - EUR is the base currency → exposure = +units (long) or -units (short)
//   - USD is the quote currency → exposure = -units (long) or +units (short)
//
// Single-currency symbols (stocks, crypto base-asset) are keyed directly.
func BuildCurrencyExposure(trades []broker.OpenTrade) map[string]float64 {
	exposure := make(map[string]float64)
	for _, t := range trades {
		sym := strings.ToUpper(strings.ReplaceAll(t.Symbol, "/", ""))
		if len(sym) == 6 {
			base := sym[:3]
			quote := sym[3:]
			exposure[base] += float64(t.Units)
			exposure[quote] -= float64(t.Units)
		} else {
			// Non-pair: treat the whole symbol as the currency.
			exposure[sym] += float64(t.Units)
		}
	}
	return exposure
}

// CheckCurrencyExposure returns an error if placing the proposed trade would
// push any single currency's absolute exposure above maxUnits.
//
// units is the signed units of the proposed trade (positive = long, negative =
// short). For forex, base exposure increases by units and quote decreases.
func CheckCurrencyExposure(existing map[string]float64, symbol string, units int, maxUnits float64) error {
	if maxUnits <= 0 {
		return nil
	}
	sym := strings.ToUpper(strings.ReplaceAll(symbol, "/", ""))
	proposed := make(map[string]float64)
	if len(sym) == 6 {
		base := sym[:3]
		quote := sym[3:]
		proposed[base] = float64(units)
		proposed[quote] = -float64(units)
	} else {
		proposed[sym] = float64(units)
	}

	for ccy, delta := range proposed {
		net := existing[ccy] + delta
		if math.Abs(net) > maxUnits {
			return fmt.Errorf("exposure cap: %s net exposure %.0f would exceed limit %.0f", ccy, net, maxUnits)
		}
	}
	return nil
}
