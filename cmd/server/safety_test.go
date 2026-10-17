package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/unitz007/aegis"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestLoadLimits_DefaultsAreOnAndZeroSwitchesOneOff(t *testing.T) {
	l, err := loadLimits(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if l.MaxUnits != 100_000 || l.MaxCryptoValue != 10_000 || l.MaxOpenPositions != 5 || l.MaxTradesPerDay != 10 || l.MaxDailyLoss != 1_000 {
		t.Errorf("defaults: %+v", l)
	}
	l, err = loadLimits(env(map[string]string{"MAX_UNITS": "250000", "MAX_DAILY_LOSS": "0", "MAX_TRADES_PER_DAY": " 3 "}))
	if err != nil || l.MaxUnits != 250_000 || l.MaxDailyLoss != 0 || l.MaxTradesPerDay != 3 {
		t.Errorf("overrides: %+v %v", l, err)
	}
	if !strings.Contains(describeLimits(l), "max_daily_loss=OFF") || !strings.Contains(describeLimits(l), "max_units=250000") {
		t.Errorf("the startup line says what is on and what is off: %s", describeLimits(l))
	}
}

// A typo must never silently remove a safety limit.
func TestLoadLimits_AMistypedValueStopsStartup(t *testing.T) {
	for _, bad := range []string{"-5", "ten", "1e", "NaN%"} {
		if _, err := loadLimits(env(map[string]string{"MAX_UNITS": bad})); err == nil || !strings.Contains(err.Error(), "MAX_UNITS") {
			t.Errorf("MAX_UNITS=%q should be refused, got %v", bad, err)
		}
	}
}

func adminCall(h http.Handler, method, path, token, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestAdmin_PauseResumeAndStatusNeedTheToken(t *testing.T) {
	g := aegis.NewGuard(nil)
	h := adminHandler("s3cret", g, aegis.Limits{MaxUnits: 100_000})

	for _, tok := range []string{"", "wrong", "s3cre"} {
		if rec := adminCall(h, "POST", "/admin/pause", tok, ""); rec.Code != 401 {
			t.Errorf("token %q: %d", tok, rec.Code)
		}
	}
	if paused, _, _ := g.Status(); paused {
		t.Fatal("an unauthorised call must not pause anything")
	}

	rec := adminCall(h, "POST", "/admin/pause", "s3cret", `{"reason":"  odd fill on EURUSD\n"}`)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"paused":true`) || !strings.Contains(rec.Body.String(), "odd fill on EURUSD") {
		t.Fatalf("pause: %d %s", rec.Code, rec.Body)
	}
	if paused, reason, _ := g.Status(); !paused || reason != "odd fill on EURUSD" {
		t.Fatalf("paused with a clean reason: %v %q", paused, reason)
	}
	if rec := adminCall(h, "GET", "/admin/status", "s3cret", ""); !strings.Contains(rec.Body.String(), `"max_units":100000`) || !strings.Contains(rec.Body.String(), `"paused":true`) {
		t.Errorf("status shows state and limits: %s", rec.Body)
	}
	if rec := adminCall(h, "POST", "/admin/resume", "s3cret", ""); rec.Code != 200 || !strings.Contains(rec.Body.String(), `"paused":false`) {
		t.Fatalf("resume: %d %s", rec.Code, rec.Body)
	}
	if paused, _, _ := g.Status(); paused {
		t.Fatal("resumed")
	}
	if rec := adminCall(h, "GET", "/admin/pause", "s3cret", ""); rec.Code != 404 {
		t.Errorf("wrong method: %d", rec.Code)
	}
}

func TestAdmin_NoTokenConfiguredMeansNobodyGetsIn(t *testing.T) {
	h := adminHandler("", aegis.NewGuard(nil), aegis.Limits{})
	if rec := adminCall(h, "POST", "/admin/pause", "", ""); rec.Code != 401 {
		t.Errorf("an empty token must never match an empty header: %d", rec.Code)
	}
}

func TestCleanReason_IsShortAndPrintable(t *testing.T) {
	if got := cleanReason("  a\x00b\nc  "); got != "abc" {
		t.Errorf("control characters dropped: %q", got)
	}
	if got := cleanReason(strings.Repeat("x", 500)); len(got) > 125 {
		t.Errorf("kept short: %d", len(got))
	}
}
