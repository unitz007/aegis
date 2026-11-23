package capital

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// pretendCapital answers /session with fresh tokens ("cst-N"), and each other path with what fail says for the
// tokens it was sent.
func pretendCapital(t *testing.T, fail func(r *http.Request) int) (*httptest.Server, *int32, *int32) {
	t.Helper()
	var sessions, calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/session" {
			n := atomic.AddInt32(&sessions, 1)
			w.Header().Set("CST", "cst-"+string(rune('0'+n)))
			w.Header().Set("X-SECURITY-TOKEN", "sec")
			w.WriteHeader(http.StatusOK)
			return
		}
		atomic.AddInt32(&calls, 1)
		if code := fail(r); code != 0 {
			w.WriteHeader(code)
			_, _ = w.Write([]byte(`{"errorCode":"error.internal"}`))
			return
		}
		_, _ = w.Write([]byte(`{"accounts":[]}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &sessions, &calls
}

func TestDo_AReadCapitalFailsOnIsAskedOnceMoreWithANewSession(t *testing.T) {
	srv, sessions, calls := pretendCapital(t, func(r *http.Request) int {
		if r.Header.Get("CST") == "test-cst" {
			return http.StatusInternalServerError // the old session
		}
		return 0
	})
	a := newTestAdapter(srv.URL)
	if err := a.get(context.Background(), "/api/v1/accounts", nil); err != nil {
		t.Fatalf("a read that failed on Capital.com's side once should go through with a new session: %v", err)
	}
	if *sessions != 1 || *calls != 2 {
		t.Fatalf("sessions=%d calls=%d, want 1 and 2", *sessions, *calls)
	}
}

func TestDo_AReadThatKeepsFailingIsAskedOnlyTwice(t *testing.T) {
	srv, _, calls := pretendCapital(t, func(*http.Request) int { return http.StatusInternalServerError })
	a := newTestAdapter(srv.URL)
	if err := a.get(context.Background(), "/api/v1/accounts", nil); err == nil {
		t.Fatal("want the failure")
	}
	if *calls != 2 {
		t.Fatalf("calls=%d, want 2", *calls)
	}
}

func TestDo_AChangeCapitalFailsOnIsNeverSentAgain(t *testing.T) {
	srv, sessions, calls := pretendCapital(t, func(*http.Request) int { return http.StatusInternalServerError })
	a := newTestAdapter(srv.URL)
	if err := a.post(context.Background(), "/api/v1/positions", map[string]any{"epic": "EURUSD"}, nil); err == nil {
		t.Fatal("want the failure")
	}
	if *calls != 1 || *sessions != 0 {
		t.Fatalf("a change that may have gone through was sent again: calls=%d sessions=%d", *calls, *sessions)
	}
}

func TestDo_AnExpiredSessionSignsInAgainAndGoesThrough(t *testing.T) {
	srv, sessions, calls := pretendCapital(t, func(r *http.Request) int {
		if r.Header.Get("CST") == "test-cst" {
			return http.StatusUnauthorized
		}
		return 0
	})
	a := newTestAdapter(srv.URL)
	if err := a.post(context.Background(), "/api/v1/positions", map[string]any{"epic": "EURUSD"}, nil); err != nil {
		t.Fatalf("a refused session should sign in again and go through: %v", err)
	}
	if *sessions != 1 || *calls != 2 {
		t.Fatalf("sessions=%d calls=%d, want 1 and 2", *sessions, *calls)
	}
}
