package main

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"unicode"

	"github.com/unitz007/aegis"
)

// Defaults for the hard limits. They are deliberately modest: a limit that is too tight
// is a setting away from being loosened; a loss that was allowed cannot be undone.
const (
	defaultMaxUnits         = 100_000
	defaultMaxCryptoValue   = 10_000
	defaultMaxOpenPositions = 5
	defaultMaxTradesPerDay  = 10
	defaultMaxDailyLoss     = 1_000
)

// loadLimits reads the hard limits from the environment. A value of 0 switches that
// limit off on purpose; a negative or non-numeric value stops the server starting, so a
// typo can never silently remove a safety limit.
func loadLimits(getenv func(string) string) (aegis.Limits, error) {
	num := func(key string, def float64) (float64, error) {
		raw := strings.TrimSpace(getenv(key))
		if raw == "" {
			return def, nil
		}
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil || v < 0 {
			return 0, fmt.Errorf("%s=%q must be a number, 0 or more (0 switches the limit off)", key, raw)
		}
		return v, nil
	}
	var l aegis.Limits
	units, err := num("MAX_UNITS", defaultMaxUnits)
	if err != nil {
		return l, err
	}
	crypto, err := num("MAX_CRYPTO_VALUE", defaultMaxCryptoValue)
	if err != nil {
		return l, err
	}
	open, err := num("MAX_OPEN_POSITIONS", defaultMaxOpenPositions)
	if err != nil {
		return l, err
	}
	perDay, err := num("MAX_TRADES_PER_DAY", defaultMaxTradesPerDay)
	if err != nil {
		return l, err
	}
	loss, err := num("MAX_DAILY_LOSS", defaultMaxDailyLoss)
	if err != nil {
		return l, err
	}
	l.MaxUnits, l.MaxCryptoValue, l.MaxOpenPositions, l.MaxTradesPerDay, l.MaxDailyLoss =
		int(units), crypto, int(open), int(perDay), loss
	return l, nil
}

// describeLimits is the startup line: what is on, and what was switched off.
func describeLimits(l aegis.Limits) string {
	part := func(name string, v float64) string {
		if v <= 0 {
			return name + "=OFF"
		}
		return fmt.Sprintf("%s=%g", name, v)
	}
	return strings.Join([]string{
		part("max_units", float64(l.MaxUnits)),
		part("max_crypto_value", l.MaxCryptoValue),
		part("max_open_positions", float64(l.MaxOpenPositions)),
		part("max_trades_per_day", float64(l.MaxTradesPerDay)),
		part("max_daily_loss", l.MaxDailyLoss),
	}, " ")
}

// adminHandler is the owner's control panel: see whether trading is paused, pause it,
// resume it. Every call needs the admin token. It never places or closes a trade.
//
//	GET  /admin/status
//	POST /admin/pause   {"reason": "optional short note"}
//	POST /admin/resume
func adminHandler(token string, g *aegis.Guard, limits aegis.Limits) http.Handler {
	status := func() map[string]any {
		paused, reason, since := g.Status()
		out := map[string]any{
			"paused": paused,
			"limits": map[string]any{
				"max_units":          limits.MaxUnits,
				"max_crypto_value":   limits.MaxCryptoValue,
				"max_open_positions": limits.MaxOpenPositions,
				"max_trades_per_day": limits.MaxTradesPerDay,
				"max_daily_loss":     limits.MaxDailyLoss,
			},
		}
		if paused {
			out["reason"], out["since"] = reason, since.Format("2006-01-02T15:04:05Z")
		}
		return out
	}
	write := func(w http.ResponseWriter, code int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if token == "" || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			write(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		switch {
		case r.URL.Path == "/admin/status" && r.Method == http.MethodGet:
			write(w, http.StatusOK, status())
		case r.URL.Path == "/admin/pause" && r.Method == http.MethodPost:
			var body struct {
				Reason string `json:"reason"`
			}
			_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&body)
			g.Pause(cleanReason(body.Reason))
			write(w, http.StatusOK, status())
		case r.URL.Path == "/admin/resume" && r.Method == http.MethodPost:
			g.Resume()
			write(w, http.StatusOK, status())
		default:
			write(w, http.StatusNotFound, map[string]string{"error": "not found"})
		}
	})
}

// cleanReason keeps a pause note short and printable: it is shown to people whose
// trades are refused.
func cleanReason(s string) string {
	s = strings.TrimSpace(s)
	var b strings.Builder
	for _, r := range s {
		if unicode.IsControl(r) {
			continue
		}
		b.WriteRune(r)
		if b.Len() >= 120 {
			break
		}
	}
	return strings.TrimSpace(b.String())
}

// startPaused reports whether the server should start with trading paused.
func startPaused() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("TRADING_PAUSED")), "true")
}
