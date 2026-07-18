package federation

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestJoinStore_ReadLimiterOutlastsStrict pins both recovery budgets: ordinary
// mutations cannot consume the final-confirm reserve, aggregate mutations stay
// capped, and read polling remains independent.
func TestJoinStore_ReadLimiterOutlastsStrict(t *testing.T) {
	s := NewJoinStore()
	base := time.Unix(1_700_000_000, 0)
	const ip = "10.0.0.7"

	ordinaryCap := connMaxAttempts - connConfirmReserve
	for i := 0; i < ordinaryCap; i++ {
		if !s.AllowConn(ip, base) {
			t.Fatalf("ordinary allow #%d must pass (cap %d)", i+1, ordinaryCap)
		}
	}
	if s.AllowConn(ip, base) {
		t.Fatalf("ordinary mutations consumed the %d-slot confirm reserve", connConfirmReserve)
	}
	for i := 0; i < connConfirmReserve; i++ {
		if !s.AllowConnConfirm(ip, base) {
			t.Fatalf("reserved confirm #%d was throttled", i+1)
		}
	}
	if s.AllowConnConfirm(ip, base) {
		t.Fatalf("aggregate mutation cap exceeded %d attempts", connMaxAttempts)
	}
	if !s.AllowConn(ip, base.Add(connWindow+time.Nanosecond)) {
		t.Fatal("mutation budget did not reset after its window")
	}

	// The read limiter is a SEPARATE budget: it is unaffected by the strict
	// exhaustion above and sustains a 2s poll (30/min) for the whole ceremony.
	// Simulate ~2 minutes of polling at 2s cadence (60 polls) — must never block.
	for i := 0; i < 60; i++ {
		at := base.Add(time.Duration(i) * 2 * time.Second)
		if !s.AllowConnRead(ip, at) {
			t.Fatalf("read poll #%d at %s was throttled — the stuck-guest bug", i+1, at.Sub(base))
		}
	}

	// And the read limiter still bounds an unbounded spray: past its own cap
	// within a single window it blocks.
	spray := base.Add(5 * time.Minute)
	for i := 0; i < connMaxReadAttempts; i++ {
		s.AllowConnRead(ip, spray)
	}
	if s.AllowConnRead(ip, spray) {
		t.Fatalf("read limiter must still block a spray past %d in-window", connMaxReadAttempts)
	}
}

// TestJoinAuthChargesExactlyOneLimiter pins the middleware dispatch rather
// than only the JoinStore methods. A previous implementation called the
// ordinary limiter before selecting confirm/read, so confirms counted twice
// and status polling silently exhausted the mutation budget.
func TestJoinAuthChargesExactlyOneLimiter(t *testing.T) {
	joins := NewJoinStore()
	m := &Manager{joins: joins}
	cert := &x509.Certificate{RawSubjectPublicKeyInfo: []byte("join-rate-test-spki")}
	const connIP = "10.0.0.7"

	serve := func(class joinRateClass, forwardedFor string) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/fed/v1/join/status", nil)
		req.RemoteAddr = connIP + ":4242"
		req.Header.Set("X-Forwarded-For", forwardedFor)
		req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
		rr := httptest.NewRecorder()
		m.joinAuth(class)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})).ServeHTTP(rr, req)
		if rr.Code != http.StatusNoContent {
			t.Fatalf("rate class %d returned %d", class, rr.Code)
		}
	}
	count := func(limiter *connRateLimiter) int {
		limiter.mu.Lock()
		defer limiter.mu.Unlock()
		return len(limiter.attempts[connIP])
	}

	for i := 0; i < 60; i++ {
		serve(joinRateRead, fmt.Sprintf("198.51.100.%d", i+1))
	}
	if got := count(joins.rl); got != 0 {
		t.Fatalf("read polling charged mutation limiter %d times", got)
	}
	if got := count(joins.rlRead); got != 60 {
		t.Fatalf("read polling charged read limiter %d times, want 60", got)
	}

	serve(joinRateConfirm, "203.0.113.1")
	if got := count(joins.rl); got != 1 {
		t.Fatalf("one confirmation charged mutation limiter %d times", got)
	}
	if got := count(joins.rlRead); got != 60 {
		t.Fatalf("confirmation changed read limiter count to %d", got)
	}

	serve(joinRateMutation, "203.0.113.2")
	if got := count(joins.rl); got != 2 {
		t.Fatalf("one confirmation plus one mutation charged %d times", got)
	}
}
