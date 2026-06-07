package aegis

import (
	"strings"
	"sync"
)

// ── Currency tables ───────────────────────────────────────────────────────────

// knownFiatCodes is the set of ISO 4217 currency codes recognized as fiat for
// the purposes of forex pair classification. A 6-letter symbol whose first 3
// and last 3 letters are both in this set is classified as "forex".
var knownFiatCodes = map[string]struct{}{
	"USD": {}, "EUR": {}, "GBP": {}, "JPY": {}, "AUD": {},
	"CAD": {}, "CHF": {}, "NZD": {}, "SEK": {}, "NOK": {},
	"DKK": {}, "SGD": {}, "HKD": {}, "MXN": {}, "ZAR": {},
	"TRY": {}, "HUF": {}, "CZK": {}, "PLN": {}, "ILS": {},
	"CNY": {}, "CNH": {}, "INR": {}, "KRW": {}, "BRL": {},
	"RUB": {}, "SAR": {}, "AED": {}, "THB": {}, "MYR": {},
	"IDR": {}, "PHP": {},
}

// knownCryptoBasesMu guards knownCryptoBases for concurrent RegisterCryptoBases
// and ClassifySymbolDefault calls.
var knownCryptoBasesMu sync.RWMutex

// knownCryptoBases is the static seed of recognized crypto base-asset tickers.
// Extended at runtime via RegisterCryptoBases.
var knownCryptoBases = map[string]struct{}{
	"BTC": {}, "ETH": {}, "BNB": {}, "XRP": {}, "SOL": {},
	"DOGE": {}, "ADA": {}, "AVAX": {}, "DOT": {}, "MATIC": {},
	"LINK": {}, "LTC": {}, "UNI": {}, "ATOM": {}, "XLM": {},
	"ALGO": {}, "VET": {}, "FIL": {}, "ETC": {}, "AAVE": {},
	"TRX": {}, "XMR": {}, "NEAR": {}, "FTM": {}, "SAND": {},
	"MANA": {}, "AXS": {}, "THETA": {}, "ICP": {}, "HBAR": {},
	"EOS": {}, "MKR": {}, "EGLD": {}, "CAKE": {}, "NEO": {},
	"RUNE": {}, "GRT": {}, "CRV": {}, "ENJ": {}, "LRC": {},
	"ZEC": {}, "STX": {}, "1INCH": {}, "BAT": {}, "CHZ": {},
	"COMP": {}, "SXP": {}, "YFI": {}, "OMG": {}, "SNX": {},
	"SUI": {}, "APT": {}, "ARB": {}, "OP": {}, "INJ": {},
	"PEPE": {}, "SHIB": {}, "TON": {}, "WLD": {}, "SEI": {},
}

// knownQuoteCurrencies is the set of fiat/stablecoin tickers accepted as the
// quote side of a crypto pair (e.g. "BTC/USD" → base="BTC", quote="USD").
var knownQuoteCurrencies = map[string]struct{}{
	"USD": {}, "USDT": {}, "USDC": {}, "BUSD": {},
	"EUR": {}, "GBP": {}, "JPY": {}, "AUD": {}, "CAD": {},
	"BTC": {}, // e.g. ETH/BTC
}

// RegisterCryptoBases extends the crypto-base table used by ClassifySymbolDefault.
// Call this from a live-data refresh hook (e.g. a CoinGecko top-100 poller) to
// classify newer coins correctly without a module update.
// Thread-safe; safe to call concurrently with ClassifySymbolDefault.
func RegisterCryptoBases(bases map[string]struct{}) {
	knownCryptoBasesMu.Lock()
	defer knownCryptoBasesMu.Unlock()
	for k, v := range bases {
		knownCryptoBases[strings.ToUpper(k)] = v
	}
}

// ClassifySymbolDefault classifies a trading symbol into one of three asset
// classes: "forex", "crypto", or "stock".
//
// Classification rules (applied in order):
//  1. Strip a "/" separator if present ("EUR/USD" → "EURUSD").
//  2. If the result is exactly 6 letters AND the first 3 and last 3 are both
//     in knownFiatCodes → "forex".
//  3. If the base ticker (before "/" or a known quote suffix) is in
//     knownCryptoBases → "crypto".
//  4. Otherwise → "stock".
//
// This is a pure, network-free function. Inject a custom SymbolClassifier via
// GatewayConfig.SymbolClassifier when dynamic classification is required.
func ClassifySymbolDefault(symbol string) string {
	sym := strings.ToUpper(strings.TrimSpace(symbol))

	// Normalise separator variants: "EUR/USD" → "EURUSD", "BTC-USD" → "BTCUSD"
	clean := strings.NewReplacer("/", "", "-", "", "_", "").Replace(sym)

	// ── Forex: exactly 6 chars, both halves are known fiat codes ─────────────
	if len(clean) == 6 {
		base3 := clean[:3]
		quote3 := clean[3:]
		_, baseIsFiat := knownFiatCodes[base3]
		_, quoteIsFiat := knownFiatCodes[quote3]
		if baseIsFiat && quoteIsFiat {
			return "forex"
		}
	}

	// ── Crypto: extract base ticker, check against knownCryptoBases ──────────
	base := extractCryptoBase(sym, clean)
	knownCryptoBasesMu.RLock()
	_, isCrypto := knownCryptoBases[base]
	knownCryptoBasesMu.RUnlock()
	if isCrypto {
		return "crypto"
	}

	return "stock"
}

// extractCryptoBase returns the upper-case base-asset ticker for a crypto
// symbol. It handles formats: "BTC/USD", "BTCUSD", "BTC-USDT", "BTCUSDT".
func extractCryptoBase(original, clean string) string {
	// If there's an explicit separator, the base is everything before it.
	if idx := strings.IndexAny(original, "/-_"); idx > 0 {
		return strings.ToUpper(original[:idx])
	}

	// No separator: try to strip known quote suffixes from the clean form.
	// Longest suffix first to avoid partial matches (USDT before USD).
	for _, suffix := range []string{"USDT", "USDC", "BUSD", "USD", "EUR", "GBP", "JPY", "BTC", "ETH"} {
		if strings.HasSuffix(clean, suffix) && len(clean) > len(suffix) {
			return clean[:len(clean)-len(suffix)]
		}
	}
	return clean
}
