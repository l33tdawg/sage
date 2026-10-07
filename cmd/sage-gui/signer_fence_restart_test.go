package main

import (
	"context"
	"crypto/ed25519"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/tx"
)

// withSignerFenceVeto swaps the guard for one test. Real fences are process
// global and hold a real signing key closed, so driving the guard directly is
// both faster and the only way to exercise the paths that cannot happen on
// purpose (an unwired hook, a panicking one).
func withSignerFenceVeto(t *testing.T, veto func() string) {
	t.Helper()
	previous := signerFenceVeto
	signerFenceVeto = veto
	t.Cleanup(func() { signerFenceVeto = previous })
}

// TestCoordinatedRestartIsVetoedWhileASigningKeyIsFenced is the whole point of
// condition 4. A fence lives in memory only; restarting throws it away, the
// allocator re-seeds from the highest COMMITTED nonce (which is below the
// abandoned one), and the next transaction overtakes something that may still be
// in flight — rejected Code 4, untraceably, on some later unrelated request.
//
// The reversible drain must not even be PREPARED: a refused restart should leave
// the snapshot scheduler and the pinned recovery binary exactly as they were.
func TestCoordinatedRestartIsVetoedWhileASigningKeyIsFenced(t *testing.T) {
	withSignerFenceVeto(t, func() string {
		return "1 signing key(s) are awaiting proof of an earlier submission's fate"
	})

	var prepared atomic.Int32
	preparer := fakeDrainPreparer{
		commit: func() { prepared.Add(1) },
		abort:  func() { prepared.Add(1) },
	}
	requests := make(chan preparedRestartRequest, 1)

	err := prepareAndQueueRestart(context.Background(), preparer, requests, nil)
	if err == nil {
		t.Fatal("a restart was allowed while a signing key was fenced; the fence would be lost and a transaction with it")
	}
	if !strings.Contains(err.Error(), "awaiting proof") {
		t.Fatalf("the refusal does not tell the operator why: %v", err)
	}
	if len(requests) != 0 {
		t.Fatal("a vetoed restart was still queued")
	}
	if prepared.Load() != 0 {
		t.Fatal("a vetoed restart still prepared (and then unwound) the reversible drain")
	}
}

// TestCoordinatedRestartVetoFailsClosed pins the property that makes the guard
// worth having. A guard that degrades to "proceed" when it malfunctions is worse
// than no guard, because the release notes will claim the case is handled and
// nobody will look again. So: an unwired hook refuses, and a hook that panics
// refuses.
func TestCoordinatedRestartVetoFailsClosed(t *testing.T) {
	t.Run("unwired guard", func(t *testing.T) {
		withSignerFenceVeto(t, nil)
		if err := checkSignerFenceRestartVeto(signerFenceVeto); err == nil {
			t.Fatal("an unreadable fence state allowed a restart")
		}
	})

	t.Run("panicking guard", func(t *testing.T) {
		withSignerFenceVeto(t, func() string { panic("fence state unreadable") })
		err := checkSignerFenceRestartVeto(signerFenceVeto)
		if err == nil {
			t.Fatal("a panicking guard allowed a restart")
		}
		if !strings.Contains(err.Error(), "could not be evaluated") {
			t.Fatalf("the refusal does not distinguish 'could not check' from 'a key is fenced': %v", err)
		}
	})

	t.Run("queue path refuses too", func(t *testing.T) {
		withSignerFenceVeto(t, nil)
		requests := make(chan preparedRestartRequest, 1)
		preparer := fakeDrainPreparer{commit: func() {}, abort: func() {}}
		if err := prepareAndQueueRestart(context.Background(), preparer, requests, nil); err == nil {
			t.Fatal("prepareAndQueueRestart proceeded with an unreadable fence state")
		}
	})
}

// TestCoordinatedRestartProceedsWhenNoKeyIsFenced pins the other direction. The
// guard must be invisible in normal operation, or operators will learn to work
// around it and it will be useless the one time it matters.
func TestCoordinatedRestartProceedsWhenNoKeyIsFenced(t *testing.T) {
	withSignerFenceVeto(t, func() string { return "" })

	requests := make(chan preparedRestartRequest, 1)
	preparer := fakeDrainPreparer{commit: func() {}, abort: func() {}}
	if err := prepareAndQueueRestart(context.Background(), preparer, requests, nil); err != nil {
		t.Fatalf("an ordinary restart was refused: %v", err)
	}
	if len(requests) != 1 {
		t.Fatal("an allowed restart was not queued")
	}
}

// restartTestSigningKey generates a throwaway key for asserting whether signing
// is currently possible. tx state is process-global, so each test uses a fresh
// key and never leaves a fence or a lease behind.
func restartTestSigningKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, sk, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return sk
}

// TestDrainTimeRecheckAbandonsRestartAndResumesSigning covers the abandon side
// of the drain-time guarantee (step 2 of the veto ordering). A veto at the
// re-check must unwind in the load-bearing order — abort the reversible drain,
// release the preflight fence, reset the request so a later signal-path
// adoption cannot double-run the callbacks — and then RESUME signing. Dropping
// that resume (the easy mistake in a future edit of the shutdown select) leaves
// the node permanently answering every signing request with ErrSigningQuiesced:
// an outage with no fence behind it and no restart coming.
func TestDrainTimeRecheckAbandonsRestartAndResumesSigning(t *testing.T) {
	var order []string
	prepared := preparedRestartRequest{
		release:      func() { order = append(order, "release") },
		commit:       func() { order = append(order, "commit") },
		abort:        func() { order = append(order, "abort") },
		fallbackExec: "/tmp/pinned-pre-drain-binary",
	}

	err := commitRestartAfterSigningDrain(&prepared,
		func() string { return "1 signing key(s) are awaiting proof of an earlier submission's fate" },
		2*time.Second)
	if err == nil {
		t.Fatal("a fence at the drain-time re-check did not abandon the restart")
	}
	if !strings.Contains(err.Error(), "awaiting proof") {
		t.Fatalf("the abandonment does not carry the veto reason: %v", err)
	}
	if got := strings.Join(order, ","); got != "abort,release" {
		t.Fatalf("abandon ran %q, want abort then release (and never commit): abort must restore serving "+
			"shape before the preflight fence is let go, and a commit here would drain a node that is "+
			"staying up", got)
	}
	if prepared.abort != nil || prepared.release != nil || prepared.commit != nil || prepared.fallbackExec != "" {
		t.Fatal("an abandoned restart left the prepared request populated; the signal path adopts whatever " +
			"is left here and would double-run its callbacks")
	}

	// Signing must be usable again — the node keeps serving after an abandon.
	if leaseErr := tx.WithNonceLease(context.Background(), restartTestSigningKey(t),
		func(uint64) error { return nil }); leaseErr != nil {
		t.Fatalf("signing was not resumed after the abandoned restart: %v", leaseErr)
	}
}

// TestDrainTimeRecheckFailsClosedWhileSigningIsBusy pins the idle-wait half:
// a submission still in flight when the budget expires must ABANDON the
// restart, not proceed. Proceeding is exactly the TOCTOU the ordering exists to
// close — the drain would sever that submission and raise its fence after the
// only veto that was going to run.
func TestDrainTimeRecheckFailsClosedWhileSigningIsBusy(t *testing.T) {
	sk := restartTestSigningKey(t)
	entered := make(chan struct{})
	releaseHolder := make(chan struct{})
	holderDone := make(chan struct{})
	go func() {
		defer close(holderDone)
		_ = tx.WithNonceLease(context.Background(), sk, func(uint64) error {
			close(entered)
			<-releaseHolder
			return nil
		})
	}()
	<-entered
	defer func() {
		close(releaseHolder)
		<-holderDone
	}()

	var unwound atomic.Int32
	prepared := preparedRestartRequest{
		release: func() { unwound.Add(1) },
		abort:   func() { unwound.Add(1) },
		commit:  func() { t.Error("the restart committed while a submission was still in flight") },
	}
	err := commitRestartAfterSigningDrain(&prepared, func() string { return "" }, 50*time.Millisecond)
	if err == nil {
		t.Fatal("busy signing did not abandon the restart; the drain would sever the in-flight submission " +
			"and raise a fence after the veto ran")
	}
	if unwound.Load() != 2 {
		t.Fatalf("the abandon did not unwind the prepared drain (abort+release), got %d call(s)", unwound.Load())
	}
}

// TestOrdinaryShutdownDrainQuiescesAndNeverResumes pins both halves of the
// ordinary-exit drain: it stops new allocations, and it does NOT resume them,
// because the process is leaving.
func TestOrdinaryShutdownDrainQuiescesAndNeverResumes(t *testing.T) {
	t.Cleanup(func() { tx.QuiesceSigningForRestart()() })

	if err := drainSigningForOrdinaryShutdown(2 * time.Second); err != nil {
		t.Fatalf("an idle node failed its own shutdown drain: %v", err)
	}
	err := tx.WithNonceLease(context.Background(), restartTestSigningKey(t), func(uint64) error {
		t.Error("a transaction was signed into an ordinary shutdown")
		return nil
	})
	if !errors.Is(err, tx.ErrSigningQuiesced) {
		t.Fatalf("got %v, want ErrSigningQuiesced after the ordinary-shutdown drain", err)
	}
}

// TestOrdinaryShutdownDrainWaitsForInFlightSigning covers the case the drain
// exists for: a submission already inside a nonce lease is given its window
// instead of being severed by the listener force-close.
func TestOrdinaryShutdownDrainWaitsForInFlightSigning(t *testing.T) {
	t.Cleanup(func() { tx.QuiesceSigningForRestart()() })

	entered := make(chan struct{})
	release := make(chan struct{})
	holderDone := make(chan struct{})
	go func() {
		defer close(holderDone)
		_ = tx.WithNonceLease(context.Background(), restartTestSigningKey(t), func(uint64) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	go func() {
		time.Sleep(150 * time.Millisecond)
		close(release)
	}()

	start := time.Now()
	err := drainSigningForOrdinaryShutdown(10 * time.Second)
	elapsed := time.Since(start)
	<-holderDone

	if err != nil {
		t.Fatalf("the drain did not wait for an in-flight submission to finish: %v", err)
	}
	if elapsed < 100*time.Millisecond {
		t.Fatalf("the drain reported idle after %s while a submission was still inside its lease", elapsed)
	}
}

// TestOrdinaryShutdownDrainFailsClosedOnAStuckSubmission is the other half of
// the contract: a submission that outlasts the budget must not hold the exit
// open. The caller gets an error to log and proceeds — fail closed, with the
// durable intent carrying the fence across the restart.
func TestOrdinaryShutdownDrainFailsClosedOnAStuckSubmission(t *testing.T) {
	t.Cleanup(func() { tx.QuiesceSigningForRestart()() })

	entered := make(chan struct{})
	release := make(chan struct{})
	holderDone := make(chan struct{})
	go func() {
		defer close(holderDone)
		_ = tx.WithNonceLease(context.Background(), restartTestSigningKey(t), func(uint64) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	start := time.Now()
	err := drainSigningForOrdinaryShutdown(200 * time.Millisecond)
	elapsed := time.Since(start)
	close(release)
	<-holderDone

	if err == nil {
		t.Fatal("a submission that outlasted the budget was reported as idle; the exit would claim a drain it never had")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("the drain burned %s past its 200ms budget; an operator-ordered exit must not hang", elapsed)
	}
}

// TestDrainTimeRecheckCommitsWithSigningStillQuiesced covers the commit side:
// a clean re-check runs the drain preparation's commit and leaves signing
// QUIESCED — the process is now going away, and a transaction signed into the
// teardown is the likeliest one to end with the unobserved fate the in-process
// fence cannot carry across the exec.
func TestDrainTimeRecheckCommitsWithSigningStillQuiesced(t *testing.T) {
	// Whatever happens, do not leave the package's global signing state
	// quiesced for later tests: quiesce (idempotent) and resume.
	t.Cleanup(func() { tx.QuiesceSigningForRestart()() })

	var committed, unwound atomic.Int32
	released := func() { unwound.Add(1) }
	prepared := preparedRestartRequest{
		release: released,
		abort:   func() { unwound.Add(1) },
		commit:  func() { committed.Add(1) },
	}

	if err := commitRestartAfterSigningDrain(&prepared, func() string { return "" }, 2*time.Second); err != nil {
		t.Fatalf("a clean re-check refused the restart: %v", err)
	}
	if committed.Load() != 1 {
		t.Fatalf("the drain preparation was committed %d time(s), want exactly once", committed.Load())
	}
	if unwound.Load() != 0 {
		t.Fatal("a committed restart still ran its abandon callbacks")
	}
	if prepared.release == nil {
		t.Fatal("the committed request lost its release func; the version-transition bookkeeping needs it")
	}

	// Signing stays off for the teardown that follows.
	err := tx.WithNonceLease(context.Background(), restartTestSigningKey(t), func(uint64) error {
		t.Error("a transaction was signed into the committed teardown")
		return nil
	})
	if !errors.Is(err, tx.ErrSigningQuiesced) {
		t.Fatalf("got %v, want ErrSigningQuiesced while the committed restart drains", err)
	}
}

// TestRestartVetoNeverAdvisesARestart guards the false claim that produced this
// whole workstream. No operator-facing string on this path may suggest that
// restarting clears a fence: restarting is precisely the action that discards the
// fence without resolving anything and loses the transaction.
func TestRestartVetoNeverAdvisesARestart(t *testing.T) {
	withSignerFenceVeto(t, nil)
	messages := []string{errRestartVetoUnavailable.Error()}
	if err := checkSignerFenceRestartVeto(signerFenceVeto); err != nil {
		messages = append(messages, err.Error())
	}
	withSignerFenceVeto(t, func() string { return "a key is fenced" })
	if err := checkSignerFenceRestartVeto(signerFenceVeto); err != nil {
		messages = append(messages, err.Error())
	}
	for _, msg := range messages {
		lower := strings.ToLower(msg)
		for _, forbidden := range []string{"restart anyway", "restart to clear", "force a restart", "try restarting"} {
			if strings.Contains(lower, forbidden) {
				t.Fatalf("restart-guard text advises %q, the action that loses the transaction: %q", forbidden, msg)
			}
		}
	}
}

// TestDrainTimeoutStillReportsTheFenceReason pins the reporting order of the two
// ways the drain-time re-check can refuse.
//
// These two are not independent: the fence wait lives INSIDE the nonce lease
// (internal/tx/nonce.go takes the lease, then waits on awaitFenceLifted), so a
// caller parked on a fence is, by construction, also a caller keeping
// WaitForSigningIdle busy. The drain therefore ALWAYS times out in exactly the
// scenario RestartVetoReason exists to describe, and if the drain error is
// reported first the operator is told "context deadline exceeded" instead of
// which key is fenced, on which transaction, for how long.
//
// The decision is identical either way — both abandon — so this is purely about
// which story the operator gets. Reverting to `vetoErr := idleErr` makes this
// fail with the generic deadline message.
func TestDrainTimeoutStillReportsTheFenceReason(t *testing.T) {
	sk := restartTestSigningKey(t)
	entered := make(chan struct{})
	releaseHolder := make(chan struct{})
	holderDone := make(chan struct{})
	go func() {
		defer close(holderDone)
		_ = tx.WithNonceLease(context.Background(), sk, func(uint64) error {
			close(entered)
			<-releaseHolder
			return nil
		})
	}()
	<-entered
	defer func() {
		close(releaseHolder)
		<-holderDone
	}()

	const fenceReason = "1 signing key(s) are awaiting proof of an earlier submission's fate — " +
		"signing key ab12cd34 is fenced on tx DEADBEEF (nonce 7), held for 1m0s"

	var unwound atomic.Int32
	prepared := preparedRestartRequest{
		release: func() { unwound.Add(1) },
		abort:   func() { unwound.Add(1) },
		commit:  func() { t.Error("the restart committed while a signing key was fenced") },
	}

	// A short budget: the drain cannot succeed while the holder is parked, so
	// this is the timing in which the two reasons compete.
	err := commitRestartAfterSigningDrain(&prepared, func() string { return fenceReason }, 50*time.Millisecond)
	if err == nil {
		t.Fatal("a fence plus a busy drain did not abandon the restart")
	}
	if !strings.Contains(err.Error(), "awaiting proof") {
		t.Fatalf("the abandonment reported the generic drain timeout instead of the fence that caused it, "+
			"which is the one message an operator can act on: %v", err)
	}
	if strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("the drain timeout masked the fence reason: %v", err)
	}
	if unwound.Load() != 2 {
		t.Fatalf("the abandon did not unwind the prepared drain (abort+release), got %d call(s)", unwound.Load())
	}
}
