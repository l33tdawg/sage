package federation

import (
	"testing"
	"time"
)

// TestJoinStore_ReadLimiterOutlastsStrict pins the stuck-guest fix: the read-only
// polling budget (AllowConnRead, /join/status) must comfortably exceed the
// strict code-submitting budget (AllowConn, /join/request+confirm), so a bound
// guest polling status every ~2s for a multi-minute ceremony is never throttled
// while the code-submission abuse vector stays tightly capped.
func TestJoinStore_ReadLimiterOutlastsStrict(t *testing.T) {
	s := NewJoinStore()
	base := time.Unix(1_700_000_000, 0)
	const ip = "10.0.0.7"

	// Strict limiter blocks after exactly connMaxAttempts within the window.
	for i := 0; i < connMaxAttempts; i++ {
		if !s.AllowConn(ip, base) {
			t.Fatalf("strict allow #%d must pass (cap %d)", i+1, connMaxAttempts)
		}
	}
	if s.AllowConn(ip, base) {
		t.Fatalf("strict allow must block after %d attempts in-window", connMaxAttempts)
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
