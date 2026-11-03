package abci

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestAppV30MemoryTaintMirrorRefreshesAfterRevisionChange(t *testing.T) {
	app := setupTestApp(t)
	mirror, ok := app.offchainStore.(store.MemoryTaintMirrorStore)
	require.True(t, ok)

	decisionID := strings.Repeat("a", taint.DecisionIDBytes)
	created, err := app.badgerStore.ApplyTaintDecision(
		"sage",
		decisionID,
		tx.ParentRef{SourceChainID: "sage", MemoryID: "mirror-lifecycle-target"},
		[32]byte{1},
	)
	require.NoError(t, err)
	require.True(t, created)
	app.requestMemoryTaintMirrorRefresh()
	require.Eventually(t, func() bool {
		revision, err := mirror.MemoryTaintMirrorRevision(context.Background())
		return err == nil && revision == 1
	}, 3*time.Second, 5*time.Millisecond)

	changed, err := app.badgerStore.ClearTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, changed)
	app.requestMemoryTaintMirrorRefresh()
	require.Eventually(t, func() bool {
		revision, err := mirror.MemoryTaintMirrorRevision(context.Background())
		return err == nil && revision == 2
	}, 3*time.Second, 5*time.Millisecond)
}

func awaitMirrorSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("mirror worker signal timed out")
	}
}

func TestMemoryTaintMirrorWorkerCoalescesAndDoesNotBlockTrigger(t *testing.T) {
	started, release, second := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls, inFlight, maxInFlight atomic.Int32
	var firstExited atomic.Bool
	w := newMemoryTaintMirrorWorker(func(context.Context) {
		active := inFlight.Add(1)
		for previous := maxInFlight.Load(); active > previous; previous = maxInFlight.Load() {
			if maxInFlight.CompareAndSwap(previous, active) {
				break
			}
		}
		defer inFlight.Add(-1)
		switch calls.Add(1) {
		case 1:
			close(started)
			<-release
			firstExited.Store(true)
		case 2:
			if !firstExited.Load() {
				t.Error("second pass overlapped blocked first pass")
			}
			close(second)
		}
	})
	w.interval = 10 * time.Millisecond
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		require.NoError(t, w.stop(time.Second))
	}()
	// A broken trigger that performs work inline must not hang the test itself.
	requested := make(chan struct{})
	go func() { w.request(); close(requested) }()
	awaitMirrorSignal(t, requested)
	awaitMirrorSignal(t, started)
	for i := 0; i < 100; i++ {
		w.request()
	}
	close(release)
	awaitMirrorSignal(t, second)
	require.NoError(t, w.stop(time.Second))
	require.Equal(t, int32(2), calls.Load())
	require.Equal(t, int32(1), maxInFlight.Load())
	require.Zero(t, inFlight.Load())
	w.request()
	require.Equal(t, int32(2), calls.Load())
}

func TestMemoryTaintMirrorWorkerRateLimitsPasses(t *testing.T) {
	starts := make(chan time.Time, 2)
	w := newMemoryTaintMirrorWorker(func(context.Context) { starts <- time.Now() })
	defer func() { require.NoError(t, w.stop(time.Second)) }()
	w.request()
	var first time.Time
	select {
	case first = <-starts:
	case <-time.After(time.Second):
		t.Fatal("first pass missing")
	}
	w.request()
	select {
	case next := <-starts:
		require.GreaterOrEqual(t, next.Sub(first), memoryTaintMirrorInterval-10*time.Millisecond)
	case <-time.After(2 * time.Second):
		t.Fatal("second pass missing")
	}
}

func TestMemoryTaintMirrorWorkerCancellationAndIdleExit(t *testing.T) {
	started, canceled := make(chan struct{}), make(chan struct{})
	w := newMemoryTaintMirrorWorker(func(ctx context.Context) { close(started); <-ctx.Done(); close(canceled) })
	w.request()
	awaitMirrorSignal(t, started)
	require.NoError(t, w.stop(time.Second))
	awaitMirrorSignal(t, canceled)
	w.request()
	w.mu.Lock()
	require.False(t, w.running)
	require.True(t, w.stopped)
	w.mu.Unlock()
	idle := newMemoryTaintMirrorWorker(func(context.Context) { t.Error("idle worker unexpectedly ran") })
	require.NoError(t, idle.stop(time.Second))
}

func TestMemoryTaintMirrorWorkerCooperativeDeadline(t *testing.T) {
	deadline := make(chan struct{})
	w := newMemoryTaintMirrorWorker(func(ctx context.Context) {
		<-ctx.Done()
		if ctx.Err() == context.DeadlineExceeded {
			close(deadline)
		}
	})
	w.passTimeout = 10 * time.Millisecond
	w.request()
	awaitMirrorSignal(t, deadline)
	require.NoError(t, w.stop(time.Second))
}

func TestMemoryTaintMirrorWorkerRepeatedStopJoinsAfterTimeout(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	w := newMemoryTaintMirrorWorker(func(context.Context) { close(started); <-release })
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		require.NoError(t, w.stop(time.Second))
	}()
	w.request()
	awaitMirrorSignal(t, started)
	require.Error(t, w.stop(time.Millisecond))
	require.Error(t, w.stop(time.Millisecond))
	w.request()
	close(release)
	require.NoError(t, w.stop(time.Second))
}

func TestMemoryTaintMirrorCloseRefusalKeepsStoresOpen(t *testing.T) {
	bs, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	sqlite, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "mirror.db"))
	require.NoError(t, err)
	started, release := make(chan struct{}), make(chan struct{})
	w := newMemoryTaintMirrorWorker(func(context.Context) { close(started); <-release })
	app := &SageApp{badgerStore: bs, offchainStore: sqlite, memoryTaintMirrorWorker: w}
	closed := false
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		if err := w.stop(time.Second); err != nil {
			t.Errorf("keep stores open while worker remains active: %v", err)
			return
		}
		if !closed {
			_ = sqlite.Close()
			_ = bs.CloseBadger()
		}
	})
	w.request()
	awaitMirrorSignal(t, started)
	err = app.Close()
	// Release even if a regression makes Close return an unexpected result.
	close(release)
	require.Error(t, err)
	_, err = bs.TaintRevision()
	require.NoError(t, err)
	_, err = sqlite.MemoryTaintMirrorRevision(context.Background())
	require.NoError(t, err)
	require.NoError(t, app.Close())
	closed = true
}

func TestMemoryTaintMirrorWorkerRejectsScopedCanonicalHandle(t *testing.T) {
	app := setupTestApp(t)
	require.NotNil(t, app.memoryTaintMirrorWorker)
	scoped := app.badgerStore.BeginConsensusTransaction(nil)
	defer scoped.DiscardConsensusTransaction()
	working := &SageApp{badgerStore: scoped, offchainStore: app.offchainStore}
	require.Nil(t, working.memoryTaintMirrorWorker)
	working.startMemoryTaintMirrorWorker()
	require.Nil(t, working.memoryTaintMirrorWorker)
	working.requestMemoryTaintMirrorRefresh()
	require.NoError(t, working.stopMemoryTaintMirrorWorker())
}

func TestMemoryTaintMirrorWorkerExitsWhenIdle(t *testing.T) {
	finished := make(chan struct{})
	w := newMemoryTaintMirrorWorker(func(context.Context) { close(finished) })
	w.request()
	awaitMirrorSignal(t, finished)
	w.mu.Lock()
	done := w.done
	w.mu.Unlock()
	awaitMirrorSignal(t, done)
	w.mu.Lock()
	running, dirty := w.running, w.dirty
	w.mu.Unlock()
	require.False(t, running)
	require.False(t, dirty)
	require.NoError(t, w.stop(time.Second))
}

func TestMemoryTaintMirrorWorkerConcurrentStopFencesRequests(t *testing.T) {
	var calls atomic.Int32
	w := newMemoryTaintMirrorWorker(func(ctx context.Context) {
		calls.Add(1)
		<-ctx.Done()
	})
	requested := make(chan struct{}, 4)
	for i := 0; i < 4; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				w.request()
			}
			requested <- struct{}{}
		}()
	}
	require.NoError(t, w.stop(time.Second))
	for i := 0; i < 4; i++ {
		awaitMirrorSignal(t, requested)
	}
	before := calls.Load()
	for i := 0; i < 100; i++ {
		w.request()
	}
	require.NoError(t, w.stop(time.Second))
	require.Equal(t, before, calls.Load())
}

func TestAppV30MirrorLossFallsBackToBadgerAndRebuilds(t *testing.T) {
	app, _, target, chainID := setupAppV30TaintGovernance(t)
	ctx := context.Background()
	_, ok := app.offchainStore.(store.MemoryTaintMirrorStore)
	require.True(t, ok)

	// Join the constructor's worker before arranging stale SQL state. An
	// in-flight constructor pass must not race the revision-zero assertion.
	constructorWorker := app.memoryTaintMirrorWorker
	require.NotNil(t, constructorWorker)
	require.NoError(t, app.stopMemoryTaintMirrorWorker())

	// Give this test a known SQL path without exposing production internals or
	// changing the shared fixture. Its original SQL store remains fixture-owned.
	mirrorDir, err := os.MkdirTemp("", "sage-mirror-recovery-")
	require.NoError(t, err)
	var recoveryMirror *store.SQLiteStore
	t.Cleanup(func() {
		if stopErr := app.stopMemoryTaintMirrorWorker(); stopErr != nil {
			t.Errorf("leave recovery mirror open after worker stop failure: %v", stopErr)
			return
		}
		if recoveryMirror != nil {
			if closeErr := recoveryMirror.Close(); closeErr != nil {
				t.Errorf("close recovery mirror: %v", closeErr)
				return
			}
		}
		require.NoError(t, os.RemoveAll(mirrorDir))
	})
	mirrorPath := filepath.Join(mirrorDir, "mirror-recovery.db")
	recoveryMirror, err = store.NewSQLiteStore(ctx, mirrorPath)
	require.NoError(t, err)
	app.offchainStore = recoveryMirror
	mirror := recoveryMirror

	decisionID := strings.Repeat("d", taint.DecisionIDBytes)
	created, err := app.badgerStore.ApplyTaintDecision(
		chainID,
		decisionID,
		target,
		[32]byte{1},
	)
	require.NoError(t, err)
	require.True(t, created)
	canonicalRevision, err := app.badgerStore.TaintRevision()
	require.NoError(t, err)
	require.NotZero(t, canonicalRevision)

	// Simulate rolled-back mirror rows and revision, not a missing SQL schema.
	// No worker can repair this stale state before the canonical check below.
	require.NoError(t, mirror.ReplaceMemoryTaintMirror(ctx, 0, nil))
	fresh, _, err := store.MemoryTaintMirrorFresh(ctx, app.badgerStore, mirror)
	require.NoError(t, err)
	require.False(t, fresh)

	state, err := app.badgerStore.GetMemoryDisclosureState(target.MemoryID)
	require.NoError(t, err)
	record := &memory.MemoryRecord{
		MemoryID:        target.MemoryID,
		SubmittingAgent: state.Author,
		Content:         "governance taint target",
		ContentHash:     append([]byte(nil), state.ContentHash...),
		DomainTag:       state.Domain,
		Status:          memory.StatusCommitted,
	}
	_, disposition, err := app.badgerStore.ClassifyMemoryProjection(record)
	require.ErrorIs(t, err, store.ErrMemoryProjectionTainted)
	require.Equal(t, store.MemoryProjectionTainted, disposition)
	mirroredRevision, err := mirror.MemoryTaintMirrorRevision(ctx)
	require.NoError(t, err)
	require.Zero(t, mirroredRevision, "canonical disclosure must not repair SQL as a side effect")

	// Replacing the joined test worker lets the stock bounded asynchronous
	// start/enqueue path perform recovery. No synchronous refresh is restored.
	app.memoryTaintMirrorWorker = nil
	app.startMemoryTaintMirrorWorker()
	require.NotNil(t, app.memoryTaintMirrorWorker)
	require.NotSame(t, constructorWorker, app.memoryTaintMirrorWorker)
	require.Eventually(t, func() bool {
		revision, readErr := mirror.MemoryTaintMirrorRevision(ctx)
		return readErr == nil && revision == canonicalRevision
	}, 3*time.Second, 5*time.Millisecond)
	require.NoError(t, app.stopMemoryTaintMirrorWorker())

	// A current revision alone could hide an empty or incorrect publication.
	// Inspect the joined worker's exact SQL ID set through a read-only handle.
	observedSQL, err := sql.Open("sqlite", "file:"+mirrorPath+"?mode=ro")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, observedSQL.Close()) })
	rows, err := observedSQL.QueryContext(ctx, `SELECT memory_id FROM memory_taint_mirror ORDER BY memory_id`)
	require.NoError(t, err)
	defer rows.Close()
	var mirroredIDs []string
	for rows.Next() {
		var memoryID string
		require.NoError(t, rows.Scan(&memoryID))
		mirroredIDs = append(mirroredIDs, memoryID)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Equal(t, []string{target.MemoryID}, mirroredIDs, "rebuild must restore the exact target set, without missing or extra IDs")

	fresh, revision, err := store.MemoryTaintMirrorFresh(ctx, app.badgerStore, mirror)
	require.NoError(t, err)
	require.True(t, fresh)
	require.Equal(t, canonicalRevision, revision)
	_, disposition, err = app.badgerStore.ClassifyMemoryProjection(record)
	require.ErrorIs(t, err, store.ErrMemoryProjectionTainted)
	require.Equal(t, store.MemoryProjectionTainted, disposition)
}
