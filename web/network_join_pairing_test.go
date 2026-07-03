package web

import (
	"sync"
	"testing"
)

// TestRedeemRateLimiter_ConcurrentAllowSweep exercises allow() and sweep()
// concurrently. With the limiter's internal mutex this must be race-free under
// `go test -race` (the reaper calls sweep() while guest handlers call allow()).
func TestRedeemRateLimiter_ConcurrentAllowSweep(t *testing.T) {
	rl := &redeemRateLimiter{}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			ips := []string{"10.0.0.1", "10.0.0.2", "192.168.1.5"}
			for i := 0; i < 500; i++ {
				rl.allow(ips[i%len(ips)])
				if i%50 == 0 {
					rl.sweep()
				}
			}
		}(g)
	}
	wg.Wait()
}

// TestRedeemRateLimiter_Caps confirms the 10/window cap still holds.
func TestRedeemRateLimiter_Caps(t *testing.T) {
	rl := &redeemRateLimiter{}
	allowed := 0
	for i := 0; i < 20; i++ {
		if rl.allow("1.2.3.4") {
			allowed++
		}
	}
	if allowed != redeemMaxAttempts {
		t.Fatalf("allowed %d, want %d", allowed, redeemMaxAttempts)
	}
	// A different IP has its own budget.
	if !rl.allow("5.6.7.8") {
		t.Fatal("second IP should be allowed independently")
	}
}
