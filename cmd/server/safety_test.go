package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/unitz007/aegis"
)

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
	h := adminHandler("s3cret", g)

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
	if rec := adminCall(h, "GET", "/admin/status", "s3cret", ""); !strings.Contains(rec.Body.String(), `"paused":true`) || strings.Contains(rec.Body.String(), "max_") {
		t.Errorf("status shows the state and sets no limits of its own: %s", rec.Body)
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
	h := adminHandler("", aegis.NewGuard(nil))
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
