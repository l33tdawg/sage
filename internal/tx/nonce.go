package tx

import (
	"context"
	"crypto/ed25519"
	"errors"
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
// ALLOCATION ORDER IS NOT ARRIVAL ORDER — use WithNonceLease when several
// goroutines can submit for one key at once. This function releases nonceMu the
// instant it returns, so concurrent callers can allocate N and N+1 and then
// reach CometBFT in either order; the late-arriving N is rejected Code 4.
// Calling MonotonicNonce directly is only safe where a key has a single
// submitter, or where submissions for that key are already serialized.
//
// SEEDING (lifts both limits below when a floor hook is wired via
// SetNonceFloorFunc): the FIRST allocation for a key seeds the in-process sequence
// from the highest nonce already committed on-chain for that key, via max() — it
// can only RAISE the starting floor, never lower a value already set. So a fresh
// process, or a node that just restarted, resumes ABOVE the chain's last committed
// nonce instead of trusting the wall clock to exceed it. With no hook wired the
// seed is a no-op and behavior is exactly as before.
//
// WHAT SEEDING DOES NOT DO, AND MUST NOT BE CLAIMED TO DO: it does not protect a
// transaction that is still IN FLIGHT. The hook reports the highest COMMITTED
// nonce; a submission whose outcome was never observed is unresolved precisely
// because it sits ABOVE that floor. So seeding a restarted process gets it past
// what the chain has accepted, and straight over the top of anything the
// previous process abandoned — which then lands afterwards and is rejected
// Code 4. That is the cross-restart residual documented in the header of
// nonce_fence.go, and it is the reason a restart is NOT a way to clear a signer
// fence. See docs/reference/concepts/signer-nonce-fence.md.
//
// SCOPE / KNOWN LIMITS (both are liveness-only — the consensus verdict is always
// deterministic, never a fork — and both are lifted, AS STATED AND NO FURTHER,
// once a floor hook is wired; neither of them is the in-flight case above):
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

// nonceLease is the per-signing-key submission slot handed out by
// WithNonceLease. ch is a capacity-1 semaphore rather than a sync.Mutex because
// a waiter must be able to abandon the queue when its request context dies, and
// a mutex cannot be acquired selectably.
//
// holders counts the current holder plus everyone queued behind it, so the map
// entry can be dropped the moment a key goes idle. See leases for why that
// matters.
type nonceLease struct {
	ch      chan struct{}
	holders int
}

// leaseMu guards leases. leases is deliberately SPARSE — an entry exists only
// while a key has an in-flight or queued submission, and the last participant
// deletes it on the way out. A permanently-growing map[pubkey]*mutex would be a
// slow leak on a long-lived node that signs for many agents (every registered
// agent key, every federation peer key), and nothing would ever reclaim it. The
// cost of this choice is one small allocation per idle->busy transition, which
// is noise next to the commit-confirmed broadcast the lease wraps.
var (
	leaseMu sync.Mutex
	leases  = make(map[string]*nonceLease)
)

// WithNonceLease allocates a nonce for sk and runs submit while holding a
// per-public-key lock, so no other holder of the SAME key can interleave.
//
// Why this is not just MonotonicNonce: MonotonicNonce guarantees allocation
// ORDER, not ARRIVAL order. It holds nonceMu only across the increment, so two
// goroutines signing with one key can allocate N and N+1 and then race to
// CometBFT in either order. app-v9's replay gate is strictly ">" against the
// highest COMMITTED nonce (internal/abci/app.go, both the CheckTx and the
// consensus path), so gaps are harmless but a DESCENDING arrival is fatal: the
// N tx that lands after N+1 is rejected Code 4 "nonce too low". That is exactly
// what a dashboard fan-out (clearing a board column, bulk-forgetting memories)
// produced — a subset of the batch failing for no operator-visible reason.
// Holding the lock across allocation AND submission makes a nonce valid only
// inside the lease, which is the invariant the consensus gate actually wants.
//
// Distinct keys never contend: unrelated signers stay fully concurrent, so this
// serializes only what correctness requires.
//
// ctx bounds the WAIT, not submit: a request whose context is already dead never
// allocates a nonce and never takes a slot. Note that skipping an allocation is
// harmless on its own — the consensus gate tests `>`, not `== last+1`, so gaps
// in the sequence cost nothing. The reason to check early is simply that a dead
// request must not hold the slot other callers are queued on.
//
// SUBMIT MUST EITHER REACH A DEFINITIVE OUTCOME OR SAY THAT IT DID NOT. Nothing
// forces submit to wait for consensus, and an adopter that returns while its
// transaction is still in flight would otherwise lose it: the lease releases,
// the next caller allocates a HIGHER nonce and commits it, and the abandoned
// LOWER nonce is rejected Code 4 when it finally arrives — exactly the failure
// this primitive exists to prevent, reintroduced through its own error path.
//
// So the release is CONDITIONAL on both submit's error and the registration:
//   - A nil error releases the slot normally.
//   - An ordinary error releases only when no live RegisterSubmittedTx record
//     remains. That covers pre-send failures and hash-bound consensus refusals,
//     whose broadcaster calls ClearSubmittedTx before returning. If an ordinary
//     error follows registration without a definitive bound verdict, the live
//     record fences the exact bytes even when the adopter forgot to wrap it.
//   - An error wrapped with Indeterminate also FENCES the signing key. Later callers
//     block on the fence instead of allocating past the abandoned nonce, and
//     ONLY A PROVEN FATE for that exact transaction lifts it — committed, or
//     definitively refused by consensus. No timer, no budget and no failed probe
//     can reopen the key, so a fence can be held indefinitely; that is the
//     deliberate trade, because a held fence fails loudly (ErrSignerFenced, plus
//     tx.FencedSigners) while a wrongly lifted one loses a transaction silently.
//     See nonce_fence.go.
//
// Getting that split backwards would fence the key on every ordinary validation
// failure, so the signal is a TYPE the adopter opts into at the point the
// ambiguity arises, never a guess made here from an error string.
// web/rbac_signing.go classifies at broadcastTxCommitWebContext, where the
// transport/decode/RPC ambiguity actually originates.
//
// Once the lease is held, submit owns cancellation. Errors from submit are
// returned undecorated so callers can keep matching on them — including an
// Indeterminate wrapper, whose Error() is its wrapped error's message verbatim.
//
// A caller that arrives while the key is FENCED blocks on the same per-key gate,
// bounded by its own ctx, and gets ErrSignerFenced if that ctx expires first.
// That error means nothing was signed or sent, and must never be treated as a
// consensus rejection. Neither error is cleared by restarting the node — a
// restart DISCARDS the fence without resolving anything, which is the silent
// loss the fence exists to prevent. See the header of nonce_fence.go.
//
// A caller that arrives once signing has been QUIESCED for a coordinated restart
// is refused immediately with ErrSigningQuiesced, before any lease or nonce.
// Same meaning: nothing was signed or sent.
//
// A PANIC out of submit is split by ONE fact: whether submit had already told
// the lease which bytes were about to go out (RegisterSubmittedTx).
//   - Registered: the panic is an UNOBSERVED OUTCOME, exactly like a broken
//     connection, and the key is FENCED on the registered bytes before the
//     panic continues unwinding. The registration is what makes the fence
//     provable — reconciliation identifies and re-submits those exact bytes —
//     so this is not a permanent hold bought with no evidence.
//   - Not registered: nothing is in flight — either nothing had been handed to
//     the network yet (the adopter's contract is to register immediately before
//     the send), or the fate was already decided and the adopter retired the
//     record on that proof (ClearSubmittedTx after a hash-bound consensus
//     rejection). The slot is released; fencing would have reconciliation
//     broadcast a transaction the caller was told never went out, or hold the
//     key forever over bytes consensus already refused.
//
// Both paths emit a structured submit_panic event naming the key before the
// panic continues unwinding to the caller. An adopter that broadcasts WITHOUT
// registering keeps the old residual: a post-send panic then releases the slot,
// the next caller allocates past bytes that may be in flight, and the eventual
// Code 4 is untraceable — which is why every broadcast helper in
// web/rbac_signing.go registers at the exact point the bytes reach the
// transport. (Contrast the RESOLVER panic in nonce_fence.go, which always has
// the bytes and therefore keeps the fence and retries.)
//
// DEADLOCK: the lock is taken and released entirely inside this function and
// never spans a return, so the only way to deadlock is for submit to reach back
// into WithNonceLease with the same key. Callers must not do that. The web
// adopters today (signAndBroadcastCommitPrepared and signAndBroadcastSyncContext,
// plus the app-v23 control path layered on them) all submit via CometBFT's HTTP
// RPC, and internal/abci does not import web, so the FinalizeBlock work that
// /broadcast_tx_commit waits on cannot call back into a lease. Do NOT "fix" a future re-entrancy report with a reentrant lock — a
// reentrant lease would silently re-permit the interleaving this exists to stop.
// nonceLeaseMaxWait bounds a lease acquisition or a fence wait when the caller's
// context carries NO deadline of its own. This primitive documents that a fenced
// key is "held indefinitely, bounded by the caller's ctx" (see the header comment
// and nonce_fence.go). A caller that passes a non-cancellable context removes
// that bound — the REST submit path does exactly this
// (memory_handler.go: submitConsensusTx(context.Background(), ...), deliberately
// NOT r.Context(), so an authorized durable write is not turned into a 503 on a
// client disconnect) — and a single held fence then parks the whole write path
// AND the auto-voter that would lift it, forever (observed 2026-09-22: 19+
// handlers parked 122-768 min in acquireNonceLease/awaitFenceLifted). Deriving a
// deadline restores the documented invariant without re-introducing
// client-driven cancellation. A var, not a const, so a test can shrink it.
var nonceLeaseMaxWait = 90 * time.Second

func WithNonceLease(ctx context.Context, sk ed25519.PrivateKey, submit func(nonce uint64) error) error {
	if submit == nil {
		// A nil submit cannot be "success": nothing was allocated and nothing was
		// sent. Returning nil here would let a caller that lost its closure to a
		// refactor report a committed transaction that never existed.
		return errors.New("nonce lease: submit must not be nil")
	}
	// Length MUST be checked before Public(). ed25519.PrivateKey.Public() slices
	// sk[32:64] with no bounds guard, so a nil or short key panics
	// ("slice bounds out of range [32:0]") rather than returning a bad value —
	// a type-assertion fallback below would be dead code guarding a crash that
	// already happened.
	if len(sk) != ed25519.PrivateKeySize {
		return errors.New("nonce lease: invalid ed25519 private key length")
	}
	pub, ok := sk.Public().(ed25519.PublicKey)
	if !ok {
		// Genuinely unreachable now that the length is validated, but a wrong
		// public-key type leaves nothing to serialize on, and silently dropping
		// to unserialized allocation would reintroduce the exact race this
		// primitive exists to prevent. Fail closed instead.
		return errors.New("nonce lease: ed25519 private key yielded a non-ed25519 public key")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// Restore the "bounded by its own ctx" invariant this primitive documents: a
	// caller with no deadline (the REST submit path passes context.Background())
	// would otherwise let a held fence or an un-released slot park this goroutine
	// — and every later writer for the key, plus the auto-voter — indefinitely.
	// Deriving a deadline makes the existing ctx.Done() paths in acquireNonceLease
	// and awaitFenceLifted return a retryable ErrSignerFenced/DeadlineExceeded
	// instead of parking. A caller that already set a deadline keeps it.
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, nonceLeaseMaxWait)
		defer cancel()
	}
	// Checked BEFORE the lease, so a quiesced node neither queues callers behind
	// a slot nor burns a nonce. A transaction signed into a teardown that is
	// already running is the likeliest transaction in this process's life to end
	// with an unobserved fate, and an unobserved fate at that exact moment is
	// the one case the in-process fence cannot carry across the restart.
	if signingQuiesced.Load() {
		return ErrSigningQuiesced
	}
	key := string(pub)

	lease, err := acquireNonceLease(ctx, key)
	if err != nil {
		return err
	}
	defer releaseNonceLease(key, lease)

	// Re-checked AFTER the slot is acquired, not just at entry. The entry check
	// alone had a hole a cross-review caught: a caller can pass it, park in the
	// queue for minutes behind a slow commit, and only acquire the slot AFTER a
	// coordinated restart quiesced signing — at which point it would allocate,
	// sign and broadcast straight into the drain. That submission is the
	// likeliest in the process's life to end with an unobserved fate, and a
	// fence raised for it at that moment is raised after the restart's veto was
	// evaluated, so the exec would discard it. Refusing here, with the slot
	// released by the deferred release and no nonce allocated, is what makes
	// "stops new nonce allocations" actually true for queued callers.
	if signingQuiesced.Load() {
		return ErrSigningQuiesced
	}

	// The fence is waited on AFTER the slot is held and BEFORE any allocation.
	// Holding the slot is what makes "block on the existing per-key gate" true
	// rather than a second queue with its own ordering; allocating only after
	// the fence lifts is what stops a later nonce from overtaking an abandoned
	// one. Reconciliation never takes a lease, so waiting here cannot deadlock
	// against the thing that lifts the fence.
	if fenceErr := awaitFenceLifted(ctx, key); fenceErr != nil {
		return fenceErr
	}
	// Same re-check after the fence wait, for the same reason: a caller can be
	// parked here when quiesce flips, and the fence lifting must not launch it
	// into a teardown.
	if signingQuiesced.Load() {
		return ErrSigningQuiesced
	}

	// Discard any registration a previous holder leaked. The deferred take below
	// clears every exit of THIS call, so a live entry here can only come from
	// RegisterSubmittedTx being misused outside a lease — and stale bytes left in
	// place would make a pre-send panic in THIS submission fence on a transaction
	// it never touched.
	takeRegisteredSubmission(key)

	// Guard the adopter's own code, and decide a panic's meaning from EVIDENCE
	// rather than assuming either way. If submit registered the encoded bytes
	// before handing them to the transport (RegisterSubmittedTx), a panic after
	// that point is an unobserved outcome — the node may have accepted the
	// transaction — so the key is fenced on those exact bytes BEFORE the deferred
	// lease release runs, exactly as an Indeterminate return would have. With no
	// registration the panic happened before anything was sent, so the slot is
	// released: fencing there would have reconciliation broadcast a transaction
	// the caller was told never went out. The take also clears the registration
	// on NORMAL returns, so a registration can never leak into the key's next
	// submission and mislabel an unrelated panic. The panic value itself is never
	// logged on either path — it can be an error built from a broadcast URL,
	// which carries the whole signed transaction.
	defer func() {
		reg := takeRegisteredSubmission(key)
		r := recover()
		if r == nil {
			return
		}
		if reg != nil {
			// Fence BEFORE the deferred release runs, for the same reason the
			// indeterminate return path does: reversed, the next caller could
			// take the freed slot and allocate past these bytes in the window
			// before the fence appeared.
			fenceSubmission(key, &indeterminateSubmit{
				err:     errSubmitPanickedOnWire,
				cause:   fenceCauseSubmitPanic,
				encoded: reg.encoded,
				resolve: reg.resolve,
			})
			emitFenceEvent("submit_panic",
				fenceKV("signer", signerPrefix(key)),
				fenceKV("note", "submit panicked AFTER registering its transaction as handed to the network; "+
					"the key is FENCED on those exact bytes and reconciliation will prove their fate"))
			panic(r)
		}
		emitFenceEvent("submit_panic",
			fenceKV("signer", signerPrefix(key)),
			fenceKV("note", "submit panicked with no live registration (never registered via RegisterSubmittedTx, "+
				"or retired on definitive proof via ClearSubmittedTx), so nothing is in flight and the slot is "+
				"released without a fence"))
		panic(r)
	}()

	subErr := submit(MonotonicNonce(sk))
	var indeterminate *indeterminateSubmit
	if errors.As(subErr, &indeterminate) {
		// Fence BEFORE the deferred release runs. Reversed, the next caller
		// could take the freed slot and allocate past the abandoned nonce in
		// the window before the fence appeared.
		fenceSubmission(key, indeterminate)
	} else if subErr != nil {
		// Registration is an enforceable lifecycle, not merely a panic hint.
		// Once bytes reached transport, every error is ambiguous unless the
		// adopter first retired the record on a hash-bound definitive verdict.
		if reg := takeRegisteredSubmission(key); reg != nil {
			fenceSubmission(key, &indeterminateSubmit{
				err:     errSubmitReturnedOnWire,
				cause:   fenceCauseSubmitError,
				encoded: reg.encoded,
				resolve: reg.resolve,
			})
		}
	}
	if subErr == nil {
		// The submit reported success, so the transaction is committed and its
		// nonce is consumed: the durable shadow has served its purpose and must
		// not survive to fence a key whose fate is known.
		discardFenceIntent(key)
	}
	return subErr
}

// registeredSubmission is the record RegisterSubmittedTx leaves for the panic
// guard: the exact encoded transaction submit declared it was handing to the
// network, plus the resolver that can prove its fate.
type registeredSubmission struct {
	encoded []byte
	resolve TxResolveFunc
}

// registeredMu guards registeredSubmissions. Like leases and fences the map is
// SPARSE: an entry exists only between RegisterSubmittedTx inside a running
// submit and the deferred take in WithNonceLease, so at most one entry per key
// with an in-flight submission — the lease serializes writers per key.
var (
	registeredMu          sync.Mutex
	registeredSubmissions = make(map[string]*registeredSubmission)
)

// errSubmitPanickedOnWire raises the fence when submit panics after
// registration. It is a fixed message on purpose: the panic value routinely
// wraps a broadcast error whose URL carries the entire signed transaction, so
// nothing derived from it may enter the fence record.
var errSubmitPanickedOnWire = errors.New(
	"submit panicked after handing its transaction to the network; the outcome was never observed")

// Fixed text prevents a returned error containing a signed-transaction request
// URL from entering the durable fence record.
var errSubmitReturnedOnWire = errors.New(
	"submit returned an error after handing its transaction to the network without definitive proof of its fate")

// RegisterSubmittedTx declares, from INSIDE a WithNonceLease submit callback and
// immediately BEFORE the encoded bytes are handed to the transport, that these
// exact bytes are about to go out for sk.
//
// This is what closes the lease's one remaining fail-open: a panic out of
// submit. Without a registration the lease cannot tell a panic before the send
// (nothing in flight — releasing is correct) from a panic after it (the node
// may have accepted the transaction — releasing lets the next caller allocate
// past it, the silent Code 4 loss the fence exists to prevent). With one, a
// panic is fenced on these bytes exactly as an Indeterminate return would be,
// and the fence is provable because reconciliation has the bytes to identify
// and re-submit. WithNonceLease clears the registration after success and after
// consuming an explicit Indeterminate result. An ordinary error with a live
// registration instead fences: the adopter must call ClearSubmittedTx once a
// hash-bound definitive verdict proves nothing is in flight. The record stays
// live for the REMAINDER of submit after such a verdict, so retiring it at that
// exact point also prevents a later panic from fencing decided bytes.
// See ClearSubmittedTx for what such a false fence costs: an indefinite hold at
// best, a late silent commit of "rejected" bytes at worst.
//
// encoded is copied for the same reason Indeterminate copies it: the caller's
// buffer usually comes from an encoder and may be reused, and a fence must
// re-submit the bytes that were SENT, not whatever the buffer holds later.
// resolve reconciles the transaction on the panic path; nil falls back to the
// process-wide resolver installed by SetTxResolverFunc.
//
// Calling it twice in one submit replaces the record: the latest registration
// names the bytes currently at risk. A key WithNonceLease would refuse (wrong
// length) cannot hold a lease and so has no submission to protect; empty bytes
// could raise only an unprovable fence (nothing to identify or re-submit),
// which is a permanent hold bought with no evidence. Both are ignored rather
// than tracked.
func RegisterSubmittedTx(sk ed25519.PrivateKey, encoded []byte, resolve TxResolveFunc) {
	if len(sk) != ed25519.PrivateKeySize || len(encoded) == 0 {
		return
	}
	pub, ok := sk.Public().(ed25519.PublicKey)
	if !ok {
		return
	}
	txBytes := make([]byte, len(encoded))
	copy(txBytes, encoded)
	registeredMu.Lock()
	registeredSubmissions[string(pub)] = &registeredSubmission{encoded: txBytes, resolve: resolve}
	registeredMu.Unlock()
	// Durable shadow, written at the same boundary and for the same reason the
	// in-memory registration exists: from here on the bytes may be in flight,
	// and a process that dies before their fate is proven must not let the next
	// start re-seed the allocator past them. See nonce_fence_intent.go.
	recordFenceIntent(sk, encoded)
}

// ClearSubmittedTx retires sk's registration from INSIDE a WithNonceLease
// submit callback, at the moment a DEFINITIVE outcome for the registered bytes
// has just been decoded — a hash-bound consensus rejection, which proves
// nothing is left in flight.
//
// It exists because the registration otherwise stays live until submit
// returns. A panic in that leftover window (recording metrics, classifying the
// error) would fence a transaction consensus already refused — and THAT fence
// goes wrong in one of two ways, depending on whether the refusal's cause
// persists:
//
//   - While it persists (and forever, only for a cause that cannot un-happen),
//     re-submission keeps drawing the same non-permanent-class refusal
//     (checkTxRefusalIsPermanent accepts only the nonce-gate code), the index
//     never finds a never-included transaction, and the signer's committed
//     nonce cannot advance to make the supersession proof reachable. The key
//     is refused, and the coordinated restart vetoed, indefinitely — a hold
//     bought on bytes that were provably never in flight.
//   - But most CheckTx refusals are MUTABLE state — authorization, clearance,
//     org membership, domain grants can all be granted back a block later
//     (checkTxRefusalIsPermanent's own doc). If that happens, reconciliation's
//     re-submit of the refused bytes is ADMITTED — the fenced key never
//     advanced its nonce past them — and can COMMIT, lifting the fence
//     fate=committed on a transaction whose caller was already told,
//     truthfully at the time, that it was definitively rejected. It executes
//     late and silently, and an operator who redid the change by hand has now
//     applied it twice. That is strictly worse than the loud stall above.
//
// Call it ONLY on proof that nothing is in flight. Clearing after an
// indeterminate result would re-open the exact fail-open RegisterSubmittedTx
// closes: the next panic would release the slot over bytes that may be in a
// mempool.
func ClearSubmittedTx(sk ed25519.PrivateKey) {
	if len(sk) != ed25519.PrivateKeySize {
		return
	}
	pub, ok := sk.Public().(ed25519.PublicKey)
	if !ok {
		return
	}
	takeRegisteredSubmission(string(pub))
	// A definitive verdict on these exact bytes: nothing is in flight, so the
	// durable shadow must not survive to fence the next start.
	discardFenceIntent(string(pub))
}

// takeRegisteredSubmission removes and returns key's registration, or nil.
// Removal and read are one atomic step so the record can never be consumed
// twice — once by a panic fence and again by a later submission's guard.
func takeRegisteredSubmission(key string) *registeredSubmission {
	registeredMu.Lock()
	reg := registeredSubmissions[key]
	delete(registeredSubmissions, key)
	registeredMu.Unlock()
	return reg
}

// acquireNonceLease blocks until this goroutine owns key's submission slot, or
// ctx dies first. On error nothing is held and the caller must not release.
func acquireNonceLease(ctx context.Context, key string) (*nonceLease, error) {
	// Check before queueing: an already-cancelled request must not take a slot
	// ahead of live ones only to drop it again.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	leaseMu.Lock()
	lease := leases[key]
	if lease == nil {
		lease = &nonceLease{ch: make(chan struct{}, 1)}
		leases[key] = lease
	}
	lease.holders++
	leaseMu.Unlock()

	select {
	case lease.ch <- struct{}{}:
	case <-ctx.Done():
		// Abandon the queue. dropNonceLeaseRef, not releaseNonceLease: the
		// semaphore was never taken, and draining it here would hand a phantom
		// release to whichever goroutine actually holds it.
		dropNonceLeaseRef(key, lease)
		return nil, ctx.Err()
	}

	// The wait may have outlived the request. Give the slot straight back rather
	// than consuming a nonce nobody will broadcast.
	if err := ctx.Err(); err != nil {
		releaseNonceLease(key, lease)
		return nil, err
	}
	return lease, nil
}

// releaseNonceLease frees key's submission slot and then drops this goroutine's
// reference to the lease.
func releaseNonceLease(key string, lease *nonceLease) {
	<-lease.ch
	dropNonceLeaseRef(key, lease)
}

// dropNonceLeaseRef removes this goroutine from lease's participant count and
// evicts the map entry once nobody holds or wants it.
//
// The eviction is safe against a concurrent arrival because both sides take
// leaseMu: an arrival that read this lease out of the map has ALREADY
// incremented holders under the same lock, so holders cannot be zero while any
// goroutine still believes it is queueing on this object. An arrival that
// misses the entry allocates a fresh one, and the evicted object is by then
// unreferenced and unheld.
func dropNonceLeaseRef(key string, lease *nonceLease) {
	leaseMu.Lock()
	lease.holders--
	if lease.holders <= 0 && leases[key] == lease {
		delete(leases, key)
	}
	leaseMu.Unlock()
}
