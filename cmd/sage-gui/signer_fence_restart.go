package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/l33tdawg/sage/internal/tx"
)

// This file is the node's side of the signer fence's restart guard.
//
// WHY A COORDINATED RESTART CAN BE THE DANGEROUS ONE. The fence in internal/tx
// remembers that some transaction carrying nonce N went out and was never
// accounted for, and it refuses to let that key allocate anything higher until
// N's fate is proven. Since durable intent landed, the RECORD is written to disk
// before the bytes reach the transport (RegisterSubmittedTx → the
// signer_fence_intent table) and re-raised at startup, so a restart no longer
// loses it. What a restart still loses is the signed BYTES: they live only in
// this process, and with them goes the cheapest proof, because reconciliation
// can no longer re-submit them. So the veto asks the narrow question — is this
// fence's record on disk? — and refuses only when it cannot confirm that:
//
//	the fence is discarded  ->  the allocator re-seeds from the highest
//	COMMITTED on-chain nonce, which is still BELOW N (that is exactly what
//	"unresolved" means)  ->  it issues some M in the gap  ->  M commits  ->  the
//	late N finally arrives and app-v9 rejects it Code 4.
//
// That sequence needs the RECORD to be gone: with the intent row intact the
// next start re-raises the fence and nothing is allocated past N. That is the
// case the veto is for, and it fails closed — no store wired, a read that fails
// or a row that is missing all mean "unprotected". Refusing blanket-wide was
// itself a bug: a node holding a fence could not take the restart that installs
// the release carrying the fence's own proof reader and recovery routes, so it
// could never be fixed. A restart over a CONFIRMED durable fence is allowed, and
// says so (fence_restart_allowed_durable); the key still refuses to sign until a
// fate is proven, and the restored fence re-reads the proof on its own.
//
// NOTHING HERE MAY SUGGEST RESTARTING ANYWAY. There is no flag, no override and
// no operator advice to "restart to clear it", because restarting is the action
// that loses the transaction. The way out is reconciliation proving the
// transaction's fate, which internal/tx is actively driving by re-submitting the
// identical bytes.
//
// WHEN THE VETO RUNS MATTERS AS MUCH AS WHAT IT CHECKS. A check made only when
// the restart is REQUESTED is a time-of-check race: the drain that follows
// severs in-flight HTTP handlers after the shutdown budget, and a severed
// broadcast is precisely how an indeterminate outcome — a new fence — is
// manufactured, AFTER the only veto that ever ran. So the restart path in
// node.go checks three times:
//  1. at request time (prepareAndQueueRestart / RequestRestartPrepared), so a
//     doomed restart is refused before anything reversible is prepared;
//  2. when the restart is taken off the queue: signing is QUIESCED first, the
//     drain WAITS until no submission is in flight or queued
//     (tx.WaitForSigningIdle) — at which point every fence that was going to
//     exist already exists — and only then is the veto re-checked, while the
//     restart can still be abandoned and the node can keep serving;
//  3. after the full drain, as a last-resort tripwire for the adoption path
//     that never ran step 2: a fence held there fails the shutdown gate and
//     aborts the version transition instead of being exec'd over.
// Step 2 is the guarantee; steps 1 and 3 keep the cheap refusal cheap and the
// impossible case loud.

// signerFenceVeto reports why a coordinated restart must not proceed, or "" when
// there is nothing outstanding.
//
// It is a variable so tests can drive both answers and the fail-closed path
// without fencing a real signing key.
var signerFenceVeto = tx.RestartVetoReason

// errRestartVetoUnavailable is what a veto that cannot be evaluated returns.
// Deliberately its own error: "we could not check" and "we checked and a key is
// fenced" are different operator stories, and only the first one is a bug in
// this node rather than a transaction waiting on the chain.
var errRestartVetoUnavailable = errors.New(
	"the signer-fence restart guard could not be evaluated, so the restart was refused; " +
		"restarting without it risks losing a transaction whose outcome was never confirmed")

// checkSignerFenceRestartVeto returns a non-nil error when a coordinated restart
// must be refused.
//
// IT FAILS CLOSED, AND THAT IS THE ENTIRE POINT. A guard that quietly degrades
// to "proceed" when it malfunctions is worse than no guard at all, because the
// release notes will say the case is handled and nobody will look again. So an
// unwired hook is a veto, and a hook that panics is a veto — the node survives
// (an update refusing to install is recoverable; a lost transaction is not) but
// it does not restart.
func checkSignerFenceRestartVeto(veto func() string) (err error) {
	if veto == nil {
		return errRestartVetoUnavailable
	}
	defer func() {
		if r := recover(); r != nil {
			// Not %v of the recovered value: a panic value from the fence path
			// can be an error built from a broadcast URL, which carries the
			// whole signed transaction. The category is all a restart decision
			// needs anyway.
			err = fmt.Errorf("%w (the guard panicked)", errRestartVetoUnavailable)
		}
	}()
	if reason := veto(); reason != "" {
		return errors.New("restart refused: " + reason)
	}
	return nil
}

// commitRestartAfterSigningDrain is step 2 of the veto ordering above — the
// guarantee itself — extracted from the shutdown select so it can be tested:
// quiesce signing, wait for the in-flight population to reach zero (at which
// point every fence that was going to exist already exists), and only then
// re-evaluate the veto, while the restart can still be abandoned and the node
// can keep serving.
//
// A nil return COMMITS the restart: the drain preparation's commit has run and
// signing is deliberately left quiesced — a transaction signed into the
// teardown that follows is the likeliest in the process's life to end with an
// unobserved fate, which is exactly what the in-process fence cannot carry
// across the exec. prepared keeps its release func for the caller's version
// transition bookkeeping.
//
// A non-nil return ABANDONS it, fail closed, and the ordering of the unwind is
// part of the contract:
//  1. abort() — undo the reversible drain preparation (snapshot scheduler
//     quiesce, pinned recovery binary) before anything else, so the node is
//     back to serving shape;
//  2. release() — let go of the preflight fence the updater handed over;
//  3. reset *prepared to zero — the shutdown path later ADOPTS whatever request
//     is left populated (adoptQueuedRestartRequests), so a stale abort/release
//     here would be double-run on the next signal;
//  4. resume signing — LAST, and unconditionally, or an abandoned restart
//     leaves the node permanently refusing every signing request with
//     ErrSigningQuiesced until a real restart, which is an outage with no
//     fence behind it.
func commitRestartAfterSigningDrain(prepared *preparedRestartRequest, veto func() string, idleBudget time.Duration) error {
	resumeSigning := tx.QuiesceSigningForRestart()
	idleCtx, cancelIdle := context.WithTimeout(context.Background(), idleBudget)
	idleErr := tx.WaitForSigningIdle(idleCtx)
	cancelIdle()
	// The SPECIFIC veto is consulted first, with the drain error as the fallback.
	//
	// This changes no decision — both answers abandon, and the unwind below is
	// identical either way. It changes what the operator is told, and the order
	// has to be this way round because of where the fence wait lives: it is
	// INSIDE the nonce lease (internal/tx/nonce.go acquires the lease, then waits
	// on awaitFenceLifted). A caller parked on a fence therefore keeps
	// WaitForSigningIdle's population above zero, so in exactly the situation the
	// veto exists to describe, the drain ALWAYS burns its full budget and fails.
	// Reporting that first replaced "signing key <k> is fenced on tx <h> (nonce
	// N), held for <d>" with a bare deadline-exceeded — the generic message, in
	// the one case where the specific one is what an operator needs to act on.
	vetoErr := checkSignerFenceRestartVeto(veto)
	if vetoErr == nil {
		vetoErr = idleErr
	}
	if vetoErr != nil {
		if prepared.abort != nil {
			prepared.abort()
		}
		if prepared.release != nil {
			prepared.release()
		}
		*prepared = preparedRestartRequest{}
		resumeSigning()
		return vetoErr
	}
	if prepared.commit != nil {
		prepared.commit()
	}
	return nil
}

// ordinaryShutdownSigningIdleBudget bounds how long an ordinary exit — a
// signal, or a serve error that is not a scheduled restart — waits for the
// in-flight signing population to reach zero before the listeners are
// force-closed under it.
//
// Deliberately well below signingIdleDrainBudget (node.go): that budget exists
// so a restart can still be ABANDONED when the wait fails, while this one is
// only a courtesy to work already in flight, and the operator who pressed
// Ctrl-C or quit from the tray is waiting on the exit. Whatever does not make
// it is covered by the durable intent and the restored fence.
const ordinaryShutdownSigningIdleBudget = 5 * time.Second

// drainSigningForOrdinaryShutdown applies step 2's guarantee to the exit that
// has no veto and no ordered re-check: stop new nonce allocations, then give
// the in-flight and queued submissions a small bounded window to finish before
// the HTTP force-close can sever them.
//
// WHY THIS EXISTS AT ALL. Every coordinated restart drains signing before it
// commits, so its teardown cannot manufacture a fence. A plain signal or serve
// error had no such drain: it drained HTTP for its budget and then
// force-closed, and a broadcast caught in that window raised an indeterminate
// outcome, wrote a durable intent, and came back at the next start as a fence
// that costs its payload. The fix is not to teach the exit to resolve fences;
// it is to stop making them.
//
// SIGNING IS DELIBERATELY LEFT QUIESCED, exactly as the committed-restart path
// leaves it, and this function never resumes. The process is going away, and a
// transaction signed into a teardown is the likeliest one in its life to end
// with an unobserved fate — the one thing the in-process fence cannot carry
// across an exec. A caller refused with ErrSigningQuiesced here is being told
// the truth about the node it is talking to.
//
// The returned error is not a veto. An exit the operator ordered has to win,
// so the caller logs it and proceeds.
func drainSigningForOrdinaryShutdown(budget time.Duration) error {
	tx.QuiesceSigningForRestart()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	return tx.WaitForSigningIdle(ctx)
}
