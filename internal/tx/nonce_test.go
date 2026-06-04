package tx

import (
	"crypto/ed25519"
	"sync"
	"testing"
)

// TestMonotonicNonce_StrictlyIncreasingAndNonZero is the core invariant the
// app-v9 consensus nonce gate relies on: rapid successive calls for one key must
// strictly increase (so two txs from the same producer never collide) and never
// be 0 (which the consensus path rejects as the replay sentinel). 10k rapid calls
// exercise the max(UnixNano, last+1) fallback when the wall clock doesn't advance.
func TestMonotonicNonce_StrictlyIncreasingAndNonZero(t *testing.T) {
	_, sk, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	var prev uint64
	for i := 0; i < 10000; i++ {
		n := MonotonicNonce(sk)
		if n == 0 {
			t.Fatalf("MonotonicNonce returned 0 at iter %d (would trip the app-v9 nonce-0 sentinel)", i)
		}
		if n <= prev {
			t.Fatalf("not strictly increasing: got %d <= prev %d at iter %d", n, prev, i)
		}
		prev = n
	}
}

// TestMonotonicNonce_PerKeyIndependent confirms each signing key gets its own
// monotonic sequence (the map is keyed by pubkey), so interleaved producers
// signing with different keys don't perturb each other.
func TestMonotonicNonce_PerKeyIndependent(t *testing.T) {
	_, sk1, _ := ed25519.GenerateKey(nil)
	_, sk2, _ := ed25519.GenerateKey(nil)
	var last1, last2 uint64
	for i := 0; i < 1000; i++ {
		n1 := MonotonicNonce(sk1)
		n2 := MonotonicNonce(sk2)
		if n1 <= last1 {
			t.Fatalf("key1 not strictly increasing: %d <= %d", n1, last1)
		}
		if n2 <= last2 {
			t.Fatalf("key2 not strictly increasing: %d <= %d", n2, last2)
		}
		last1, last2 = n1, n2
	}
}

// TestMonotonicNonce_ConcurrentNoDuplicates is the reason the allocator is
// mutex-guarded: many producers sharing ONE key (e.g. every REST/web handler on
// the shared node priv-validator key) call concurrently and must each get a
// distinct value. Run with -race to verify the lock. Any duplicate would mean two
// txs carry the same nonce and the consensus gate drops the second.
func TestMonotonicNonce_ConcurrentNoDuplicates(t *testing.T) {
	_, sk, _ := ed25519.GenerateKey(nil)
	const goroutines = 16
	const perG = 1000

	var wg sync.WaitGroup
	results := make([][]uint64, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			out := make([]uint64, perG)
			for i := 0; i < perG; i++ {
				out[i] = MonotonicNonce(sk)
			}
			results[g] = out
		}(g)
	}
	wg.Wait()

	seen := make(map[uint64]bool, goroutines*perG)
	for _, out := range results {
		for _, n := range out {
			if n == 0 {
				t.Fatal("MonotonicNonce returned 0 under concurrency")
			}
			if seen[n] {
				t.Fatalf("duplicate nonce %d under concurrency (lock failure)", n)
			}
			seen[n] = true
		}
	}
	if len(seen) != goroutines*perG {
		t.Fatalf("expected %d distinct nonces, got %d", goroutines*perG, len(seen))
	}
}

// TestMonotonicNonce_SeedRaisesFloor verifies the on-chain seed lifts the starting
// point above the wall clock: with a floor far above any UnixNano, the first
// allocation resumes at floor+1 (not the clock), and the sequence stays strictly
// increasing from there. This is the cross-restart / cross-process fix — a fresh
// process resumes above the chain's committed nonce instead of trusting time.
func TestMonotonicNonce_SeedRaisesFloor(t *testing.T) {
	pub, sk, _ := ed25519.GenerateKey(nil)
	const floor = uint64(9_000_000_000_000_000_000) // ~9e18, well above 2026 UnixNano (~1.75e18), below MaxUint64
	SetNonceFloorFunc(func(p ed25519.PublicKey) (uint64, bool) {
		if string(p) == string(pub) {
			return floor, true
		}
		return 0, false
	})
	defer SetNonceFloorFunc(nil)

	n1 := MonotonicNonce(sk)
	if n1 != floor+1 {
		t.Fatalf("first nonce should resume at floor+1 (%d), got %d", floor+1, n1)
	}
	n2 := MonotonicNonce(sk)
	if n2 <= n1 {
		t.Fatalf("not strictly increasing after seed: %d <= %d", n2, n1)
	}
}

// TestMonotonicNonce_SeedIgnoredWhenBelowClock confirms the seed is a max(), not a
// set: a tiny committed nonce must NOT cap the sequence below the wall clock. The
// first allocation is still the UnixNano-scale value, never floor+1.
func TestMonotonicNonce_SeedIgnoredWhenBelowClock(t *testing.T) {
	pub, sk, _ := ed25519.GenerateKey(nil)
	SetNonceFloorFunc(func(p ed25519.PublicKey) (uint64, bool) {
		if string(p) == string(pub) {
			return 5, true
		}
		return 0, false
	})
	defer SetNonceFloorFunc(nil)

	n := MonotonicNonce(sk)
	if n == 6 || n < 1_000_000_000_000_000_000 {
		t.Fatalf("tiny floor should be dominated by the wall clock, got %d", n)
	}
}

// TestMonotonicNonce_SeedConsultedOncePerKey verifies the floor hook is read at
// most once per key (the seeded guard). After the first allocation the in-process
// sequence is authoritative — re-querying the chain on every sign would be wasteful
// and could regress under a lagging read.
func TestMonotonicNonce_SeedConsultedOncePerKey(t *testing.T) {
	pub, sk, _ := ed25519.GenerateKey(nil)
	var calls int
	SetNonceFloorFunc(func(p ed25519.PublicKey) (uint64, bool) {
		if string(p) == string(pub) {
			calls++
		}
		return 0, false
	})
	defer SetNonceFloorFunc(nil)

	for i := 0; i < 5; i++ {
		MonotonicNonce(sk)
	}
	if calls != 1 {
		t.Fatalf("floor hook should be consulted exactly once per key, got %d calls", calls)
	}
}

// TestMonotonicNonce_NilFloorUnchanged pins that with no hook wired (the default),
// the allocator behaves exactly as the pure-clock version: strictly increasing and
// never 0.
func TestMonotonicNonce_NilFloorUnchanged(t *testing.T) {
	SetNonceFloorFunc(nil)
	_, sk, _ := ed25519.GenerateKey(nil)
	var prev uint64
	for i := 0; i < 1000; i++ {
		n := MonotonicNonce(sk)
		if n == 0 || n <= prev {
			t.Fatalf("nil-floor behavior broke at iter %d: n=%d prev=%d", i, n, prev)
		}
		prev = n
	}
}
