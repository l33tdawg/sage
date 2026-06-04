package tx

import (
	"crypto/ed25519"
	"sync"
	"time"
)

// nonceMu guards lastNonce and seeded. nonce allocation is process-global because
// a single signing key (notably the node priv-validator key) is shared by every
// REST and web handler in the process; they must draw from one strictly-increasing
// sequence per key.
var (
	nonceMu   sync.Mutex
	lastNonce = make(map[string]uint64)
	seeded    = make(map[string]bool) // keys whose on-chain floor we've already consulted
)

// nonceFloorMu guards nonceFloor, installed once at boot and read on first use of
// each key.
var (
	nonceFloorMu sync.RWMutex
	nonceFloor   func(pub ed25519.PublicKey) (uint64, bool)
)

// SetNonceFloorFunc installs the on-chain committed-nonce lookup used to SEED the
// allocator the first time each signing key is used in this process. f reports the
// highest nonce already committed on-chain for pub (ok=false when unknown / not
// yet committed for that key).
//
// Wire it ONCE at boot, before any signing, with a CHEAP, non-blocking source — a
// local store read (e.g. badgerStore.GetNonce(hex(pub))). It is consulted at most
// once per key and the allocator mutex is held across the call, so a slow or
// remote source would stall concurrent allocations for that instant; wrap any such
// source in a short timeout. Passing nil clears the hook (restores pure-clock
// behavior). See MonotonicNonce for why this exists.
func SetNonceFloorFunc(f func(pub ed25519.PublicKey) (uint64, bool)) {
	nonceFloorMu.Lock()
	nonceFloor = f
	nonceFloorMu.Unlock()
}

// nonceFloorFor returns the wired floor for pub, or (0,false) when no hook is
// installed or the hook reports no committed nonce.
func nonceFloorFor(pub ed25519.PublicKey) (uint64, bool) {
	nonceFloorMu.RLock()
	f := nonceFloor
	nonceFloorMu.RUnlock()
	if f == nil {
		return 0, false
	}
	return f(pub)
}

// MonotonicNonce returns a strictly-increasing replay nonce for transactions
// signed by sk.
//
// Why this exists: app-v9 enforces nonce/replay protection in the CONSENSUS path
// (a tx is rejected when its nonce <= the signer's highest committed nonce). So
// every producer signing with a given key MUST emit strictly-increasing nonces,
// or a colliding/out-of-order tx is silently dropped (Code 4). Raw
// time.Now().UnixNano() is NOT safe for this: it can repeat on coarse-resolution
// clocks (notably darwin) and it races across the many concurrent producers that
// share the node signing key, so two txs in one block can carry equal or
// descending nonces and the second is rejected.
//
// This allocator is keyed by the signer's public key and returns
// max(UnixNano, lastForKey+1): it guarantees strict per-key monotonicity within
// the process regardless of clock resolution or producer concurrency, and never
// returns 0 (so it never trips app-v9's nonce-0 rejection).
//
// SEEDING (lifts both limits below when a floor hook is wired via
// SetNonceFloorFunc): the FIRST allocation for a key seeds the in-process sequence
// from the highest nonce already committed on-chain for that key, via max() — it
// can only RAISE the starting floor, never lower a value already set. So a fresh
// process, or a node that just restarted, resumes ABOVE the chain's last committed
// nonce instead of trusting the wall clock to exceed it. With no hook wired the
// seed is a no-op and behavior is exactly as before.
//
// SCOPE / KNOWN LIMITS (both are liveness-only — the consensus verdict is always
// deterministic, never a fork — and both are LIFTED once a floor hook is wired):
//   - One process per signing key. Without a seed hook the map is process-global
//     and NOT initialized from the committed on-chain nonce, so two processes
//     signing with the SAME key against the SAME chain can allocate
//     colliding/descending nonces and app-v9 drops the loser (Code 4). A wired
//     floor hook makes a newly-started process resume above the chain. (Truly
//     simultaneous same-key signing across processes is independently a CometBFT
//     equivocation/double-sign hazard — don't do it.)
//   - Cross-restart relies on a forward wall clock when no hook is wired: on
//     restart the map is empty and the first allocation is raw UnixNano, which
//     under NORMAL forward time exceeds the prior process's last committed nonce. A
//     BACKWARD clock step (NTP correction, manual set, VM snapshot restore) past
//     the committed nonce temporarily stalls that key (Code 4) until the clock
//     catches up. A wired floor hook removes this dependence on the clock entirely
//     by seeding from the committed nonce on first use.
func MonotonicNonce(sk ed25519.PrivateKey) uint64 {
	pub, ok := sk.Public().(ed25519.PublicKey)
	if !ok {
		// Unreachable for an ed25519 private key; fail safe to a positive value.
		return uint64(time.Now().UnixNano()) // #nosec G115 -- UnixNano is positive
	}
	key := string(pub)

	nonceMu.Lock()
	defer nonceMu.Unlock()

	// First use of this key in this process: seed the sequence from the highest
	// nonce already committed on-chain so a fresh / post-restart producer resumes
	// above the chain. Consulted at most once per key (guarded by seeded), and
	// applied with max() so it can only raise the floor — never regress a value an
	// allocation in this process already established.
	if !seeded[key] {
		seeded[key] = true
		if floor, ok := nonceFloorFor(pub); ok && floor > lastNonce[key] {
			lastNonce[key] = floor
		}
	}

	n := uint64(time.Now().UnixNano()) // #nosec G115 -- UnixNano is positive
	if n <= lastNonce[key] {
		n = lastNonce[key] + 1
	}
	lastNonce[key] = n
	return n
}
