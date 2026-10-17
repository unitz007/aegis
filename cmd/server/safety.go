package main

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"unicode"

	"github.com/unitz007/aegis"
)

// adminHandler is the owner's pause switch: see whether trading is paused, pause it,
// resume it. (It sets no limits: those are each person's own, sent with their trades.) Every call needs the admin token. It never places or closes a trade.
//
//	GET  /admin/status
//	POST /admin/pause   {"reason": "optional short note"}
//	POST /admin/resume
func adminHandler(token string, g *aegis.Guard) http.Handler {
	status := func() map[string]any {
		paused, reason, since := g.Status()
		out := map[string]any{"paused": paused}
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
