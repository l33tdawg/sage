package tx

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestWithNonceLease_NoDeadlineCallerDoesNotParkOnAHeldFence reproduces the
// 2026-09-22 abci0 write-path wedge and proves the fix.
//
// The wedge: an indeterminate submit fences the signing key (correct — later
// callers must not allocate past an in-flight transaction). A subsequent caller
// acquires the per-key slot and then waits in awaitFenceLifted for the fence to
// lift, HOLDING the slot. That wait is bounded only by the caller's context —
// and the REST submit path passes context.Background() (memory_handler.go, to
// avoid turning an authorized write into a 503 on client disconnect). With no
// deadline, awaitFenceLifted's <-ctx.Done() can never fire, so the holder parks
// forever, every later writer for the key piles up behind it, and the auto-voter
// (which shares WithNonceLease on the same key) is starved too — so the fence it
// would lift never lifts. Observed: 19+ goroutines parked 122-768 minutes.
//
// The fix (nonceLeaseMaxWait): WithNonceLease derives a bounded context when the
// caller supplied none, so a no-deadline caller returns a retryable
// ErrSignerFenced/DeadlineExceeded instead of parking. Before the fix this test
// hangs until the 5s guard and fails; after it, the caller returns in ~the
// shrunk nonceLeaseMaxWait.
func TestWithNonceLease_NoDeadlineCallerDoesNotParkOnAHeldFence(t *testing.T) {
	setFenceTimingsForTest(t, fastFenceTimings())
	restore := nonceLeaseMaxWait
	nonceLeaseMaxWait = 60 * time.Millisecond
	t.Cleanup(func() { nonceLeaseMaxWait = restore })

	sk := newLeaseTestKey(t)
	key := leaseKeyFor(t, sk)

	// A resolver that never returns an answer (only ctx expiry) keeps the fence
	// up: a timed-out probe is the absence of proof, not a verdict.
	resolver := func(ctx context.Context, encoded []byte) (TxOutcome, error) {
		<-ctx.Done()
		return TxOutcome{}, ctx.Err()
	}
	boom := errors.New("broadcast tx commit: connection refused")
	if err := WithNonceLease(context.Background(), sk, func(uint64) error {
		return Indeterminate(boom, []byte("encoded-transaction-bytes"), resolver)
	}); !errors.Is(err, boom) {
		t.Fatalf("raising the fence: got %v, want the submit error to survive", err)
	}
	if !keyIsFenced(key) {
		t.Fatal("an indeterminate submit did not fence the key")
	}

	// THE REGRESSION: a caller with a non-cancellable context must NOT park.
	done := make(chan error, 1)
	var allocated bool
	go func() {
		done <- WithNonceLease(context.Background(), sk, func(uint64) error {
			allocated = true
			return nil
		})
	}()
	select {
	case err := <-done:
		if allocated {
			t.Fatal("a no-deadline caller allocated a nonce past the fence: that nonce kills the abandoned tx")
		}
		if !errors.Is(err, ErrSignerFenced) {
			t.Fatalf("no-deadline caller got %v, want ErrSignerFenced (retryable, never a consensus rejection)", err)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("no-deadline caller lost the derived deadline cause: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a no-deadline caller PARKED on a held fence: the nonceLeaseMaxWait bound did not fire — the wedge")
	}

	if !keyIsFenced(key) {
		t.Fatal("a bounded waiter giving up lifted the fence: the fence must outlive the callers waiting on it")
	}
}

// TestWithNonceLease_CallerDeadlineIsPreservedNotOverridden guards the fix's
// other half: a caller that DID set a (shorter) deadline keeps it, so the
// derivation only ever adds a bound, never loosens one.
func TestWithNonceLease_CallerDeadlineIsPreservedNotOverridden(t *testing.T) {
	setFenceTimingsForTest(t, fastFenceTimings())
	restore := nonceLeaseMaxWait
	nonceLeaseMaxWait = 10 * time.Second // deliberately far larger than the caller's deadline
	t.Cleanup(func() { nonceLeaseMaxWait = restore })

	sk := newLeaseTestKey(t)
	key := leaseKeyFor(t, sk)
	resolver := func(ctx context.Context, encoded []byte) (TxOutcome, error) {
		<-ctx.Done()
		return TxOutcome{}, ctx.Err()
	}
	boom := errors.New("broadcast tx commit: connection refused")
	if err := WithNonceLease(context.Background(), sk, func(uint64) error {
		return Indeterminate(boom, []byte("encoded"), resolver)
	}); !errors.Is(err, boom) {
		t.Fatalf("raising the fence: got %v", err)
	}
	if !keyIsFenced(key) {
		t.Fatal("indeterminate submit did not fence the key")
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	err := WithNonceLease(ctx, sk, func(uint64) error { return nil })
	if !errors.Is(err, ErrSignerFenced) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want ErrSignerFenced+DeadlineExceeded", err)
	}
	if waited := time.Since(start); waited > 2*time.Second {
		t.Fatalf("caller waited %s: its own 40ms deadline was overridden by nonceLeaseMaxWait", waited)
	}
}
