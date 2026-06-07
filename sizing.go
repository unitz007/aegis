package aegis

import (
	"fmt"
	"math"
	"strings"

	"github.com/unitz007/aegis/broker"
)

// ── Sizing helpers ────────────────────────────────────────────────────────────

// SizeForex computes the number of integer base-currency units to trade for a
// forex pair.
//
// Formula:
//
//	riskAmount = balance * riskPct
//	slDistance = |entry - sl|   (in quote currency)
//	units = round(riskAmount / (slDistance * quoteToAccountFactor))
//
// quoteToAccountFactor converts the quote-currency SL distance to
// account-currency risk.  For USD-quoted pairs (EURUSD) this is 1.0.  For
// USD-base pairs (USDJPY) this is 1/entry (~0.0067 at 150).  Without this
// correction USDJPY sizing is ~150× too small.
//
// Returns 0 units (and nil error) when rounding eliminates the position; the
// caller should skip in that case.  Returns an error only for invalid inputs.
//
// NOTE: uses math.Round (not math.Floor) to match the in-tree StockAI path
// (trade_executor.go:512: int(math.Round(riskAmount / (slDistance * quoteToUSD)))).
// Floor was the original implementation; Round is the faithful port correction.
func SizeForex(balance, riskPct, entry, sl, quoteToAccountFactor float64) (units int, err error) {
	if balance <= 0 {
		return 0, fmt.Errorf("sizing: balance must be > 0")
	}
	if riskPct <= 0 || riskPct > 1 {
		return 0, fmt.Errorf("sizing: riskPct must be in (0,1], got %.4f", riskPct)
	}
	if entry <= 0 {
		return 0, fmt.Errorf("sizing: entry must be > 0")
	}
	if sl <= 0 {
		return 0, fmt.Errorf("sizing: sl must be > 0")
	}
	if quoteToAccountFactor <= 0 {
		return 0, fmt.Errorf("sizing: quoteToAccountFactor must be > 0")
	}

	slDist := math.Abs(entry - sl)
	if slDist == 0 {
		return 0, fmt.Errorf("sizing: entry and sl are equal — cannot size position")
	}

	riskAmount := balance * riskPct
	rawUnits := riskAmount / (slDist * quoteToAccountFactor)
	return int(math.Round(rawUnits)), nil
}

// SizeCrypto computes the fractional base-asset quantity to trade for a crypto
// CFD where the broker's "size" field is in base-asset units (e.g. BTC).
//
// CRITICAL: Capital.com's crypto CFD "size" field is in BASE-ASSET units, not
// USD notional. Sending USD notional directly caused 84 BTC orders (~$5.6M).
// This function preserves the correct formula:
//
//	riskAmount = balance * riskPct
//	slPct      = |entry - sl| / entry
//	quantity   = riskAmount / slPct / entry
//
// quantity is intentionally fractional — do not round before passing to the
// broker. Apply ClampCryptoQuantity after this call.
func SizeCrypto(balance, riskPct, entry, sl float64) (quantity float64, err error) {
	if balance <= 0 {
		return 0, fmt.Errorf("sizing: balance must be > 0")
	}
	if riskPct <= 0 || riskPct > 1 {
		return 0, fmt.Errorf("sizing: riskPct must be in (0,1], got %.4f", riskPct)
	}
	if entry <= 0 {
		return 0, fmt.Errorf("sizing: entry must be > 0")
	}
	if sl <= 0 {
		return 0, fmt.Errorf("sizing: sl must be > 0")
	}

	slPct := math.Abs(entry-sl) / entry
	if slPct == 0 {
		return 0, fmt.Errorf("sizing: entry and sl are equal — cannot size position")
	}

	riskAmount := balance * riskPct
	return riskAmount / slPct / entry, nil
}

// ClampCryptoQuantity applies broker dealing rules to a risk-derived crypto
// quantity.
//
//   - If qty > maxSize (and maxSize > 0), clamp to maxSize.
//   - If qty < minSize, return (qty, false) — the caller must skip. Never upsize
//     (safety invariant #7: upsizing would risk more than configured RiskPct).
func ClampCryptoQuantity(qty, minSize, maxSize float64) (clamped float64, ok bool) {
	if minSize > 0 && qty < minSize {
		return qty, false
	}
	if maxSize > 0 && qty > maxSize {
		qty = maxSize
	}
	return qty, true
}

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
