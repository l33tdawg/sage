package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	sqlite "modernc.org/sqlite"

	"github.com/l33tdawg/sage/internal/taint"
)

func boundedMirrorFixture(t *testing.T) (*BadgerStore, *SQLiteStore) {
	t.Helper()
	canonical := openTaintStore(t)
	mirror, err := NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "mirror.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mirror.Close()) })
	return canonical, mirror
}

func assertBoundedMirrorRows(t *testing.T, mirror *SQLiteStore, revision uint64, want []string) {
	t.Helper()
	actualRevision, err := mirror.MemoryTaintMirrorRevision(context.Background())
	require.NoError(t, err)
	require.Equal(t, revision, actualRevision)
	rows, err := mirror.conn.QueryContext(context.Background(), `SELECT memory_id FROM memory_taint_mirror ORDER BY memory_id`)
	require.NoError(t, err)
	var ids []string
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Equal(t, want, ids)
}

func TestBoundedMemoryTaintMirrorPublishesCompleteSnapshotAndClear(t *testing.T) {
	canonical, mirror := boundedMirrorFixture(t)
	target := testTaintParent("bounded-target")
	seedTaintChild(t, canonical, target, "bounded-child")
	decision := testTaintDecisionID("bounded")
	_, err := canonical.ApplyTaintDecision("sage", decision, target, testTaintPayloadHash("bounded"))
	require.NoError(t, err)
	_, err = canonical.ExpandTaintDecision("sage", decision, taint.PerExecutionUnitBudget)
	require.NoError(t, err)
	revision, count, published, err := RefreshMemoryTaintMirrorBounded(context.Background(), canonical, mirror, DefaultMemoryTaintMirrorLimits())
	require.NoError(t, err)
	require.True(t, published)
	require.Equal(t, 2, count)
	assertBoundedMirrorRows(t, mirror, revision, []string{"bounded-child", "bounded-target"})
	_, err = canonical.ClearTaintDecision(decision)
	require.NoError(t, err)
	revision, count, published, err = RefreshMemoryTaintMirrorBounded(context.Background(), canonical, mirror, DefaultMemoryTaintMirrorLimits())
	require.NoError(t, err)
	require.True(t, published)
	require.Zero(t, count)
	assertBoundedMirrorRows(t, mirror, revision, nil)
	_, _, published, err = RefreshMemoryTaintMirrorBounded(context.Background(), canonical, mirror, DefaultMemoryTaintMirrorLimits())
	require.NoError(t, err)
	require.False(t, published, "fresh mirrors need no publication")
}

func TestBoundedMemoryTaintMirrorScanCapsPreserveOldMirror(t *testing.T) {
	canonical, mirror := boundedMirrorFixture(t)
	require.NoError(t, mirror.ReplaceMemoryTaintMirror(context.Background(), 0, []string{"old"}))
	target := testTaintParent("cap-target")
	seedTaintChild(t, canonical, target, "cap-child")
	decision := testTaintDecisionID("cap")
	_, err := canonical.ApplyTaintDecision("sage", decision, target, testTaintPayloadHash("cap"))
	require.NoError(t, err)
	_, err = canonical.ExpandTaintDecision("sage", decision, taint.PerExecutionUnitBudget)
	require.NoError(t, err)
	for _, test := range []struct {
		name   string
		limits MemoryTaintMirrorLimits
	}{
		{"visits", MemoryTaintMirrorLimits{MaxIDs: 10, MaxRows: 10, MaxVisits: 1}},
		{"ids", MemoryTaintMirrorLimits{MaxIDs: 1, MaxRows: 10, MaxVisits: 100}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, published, err := RefreshMemoryTaintMirrorBounded(context.Background(), canonical, mirror, test.limits)
			require.ErrorIs(t, err, ErrMemoryTaintMirrorWorkLimit)
			require.False(t, published)
			assertBoundedMirrorRows(t, mirror, 0, []string{"old"})
		})
	}
}

func TestBoundedMemoryTaintMirrorSQLiteContentionSkips(t *testing.T) {
	_, mirror := boundedMirrorFixture(t)
	require.NoError(t, mirror.ReplaceMemoryTaintMirror(context.Background(), 4, []string{"old"}))
	// A regular writer owns this exact mutex. The advisory path must skip,
	// without changing the synchronization used by normal RunInTx writers.
	mirror.writeMu.Lock()
	type result struct {
		published bool
		err       error
	}
	done := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		published, err := mirror.TryReplaceMemoryTaintMirror(ctx, 5, []string{"new"}, 10)
		done <- result{published, err}
	}()
	var got result
	blocked := false
	select {
	case got = <-done:
	case <-time.After(500 * time.Millisecond):
		blocked = true
	}
	mirror.writeMu.Unlock()
	if blocked {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("writer did not return after release")
		}
		t.Fatal("advisory refresh waited for the normal writer")
	}
	published, err := got.published, got.err
	require.NoError(t, err)
	require.False(t, published)
	assertBoundedMirrorRows(t, mirror, 4, []string{"old"})
	published, err = mirror.TryReplaceMemoryTaintMirror(context.Background(), 5, []string{"new"}, 10)
	require.NoError(t, err)
	require.True(t, published)
	assertBoundedMirrorRows(t, mirror, 5, []string{"new"})
}

func TestBoundedMemoryTaintMirrorOldRowCapAndRollback(t *testing.T) {
	_, mirror := boundedMirrorFixture(t)
	require.NoError(t, mirror.ReplaceMemoryTaintMirror(context.Background(), 4, []string{"old-a", "old-b"}))
	published, err := mirror.TryReplaceMemoryTaintMirror(context.Background(), 5, []string{"new"}, 1)
	require.ErrorIs(t, err, ErrMemoryTaintMirrorWorkLimit)
	require.False(t, published)
	assertBoundedMirrorRows(t, mirror, 4, []string{"old-a", "old-b"})
	_, err = mirror.writeExecContext(context.Background(), `CREATE TRIGGER fail_mirror_insert BEFORE INSERT ON memory_taint_mirror BEGIN SELECT RAISE(ABORT, 'bounded-test'); END`)
	require.NoError(t, err)
	published, err = mirror.TryReplaceMemoryTaintMirror(context.Background(), 5, []string{"new"}, 10)
	require.Error(t, err)
	require.False(t, published)
	assertBoundedMirrorRows(t, mirror, 4, []string{"old-a", "old-b"})
}

func TestBoundedMemoryTaintMirrorCancellationAndMalformedState(t *testing.T) {
	canonical, mirror := boundedMirrorFixture(t)
	require.NoError(t, mirror.ReplaceMemoryTaintMirror(context.Background(), 0, []string{"old"}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, published, err := RefreshMemoryTaintMirrorBounded(ctx, canonical, mirror, DefaultMemoryTaintMirrorLimits())
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, published)
	published, err = mirror.TryReplaceMemoryTaintMirror(ctx, 1, []string{"new"}, 10)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, published)
	decision := testTaintDecisionID("malformed")
	_, err = canonical.ApplyTaintDecision("sage", decision, testTaintParent("malformed-target"), testTaintPayloadHash("malformed"))
	require.NoError(t, err)
	require.NoError(t, canonical.update(func(txn *badger.Txn) error { return txn.Set(taint.DecisionKey(decision), []byte{255}) }))
	_, _, published, err = RefreshMemoryTaintMirrorBounded(context.Background(), canonical, mirror, DefaultMemoryTaintMirrorLimits())
	require.ErrorIs(t, err, ErrTaintStateMalformed)
	require.False(t, published)
	assertBoundedMirrorRows(t, mirror, 0, []string{"old"})
}

func TestBoundedMemoryTaintMirrorRejectsScopedAndUnknownBackends(t *testing.T) {
	canonical, mirror := boundedMirrorFixture(t)
	require.True(t, MemoryTaintMirrorWorkerSupported(canonical, mirror))
	scoped := canonical.BeginConsensusTransaction(nil)
	defer scoped.DiscardConsensusTransaction()
	require.False(t, MemoryTaintMirrorWorkerSupported(scoped, mirror))
	require.NoError(t, mirror.RunInTx(context.Background(), func(tx OffchainStore) error {
		sqlScoped := tx.(*SQLiteStore)
		require.False(t, MemoryTaintMirrorWorkerSupported(canonical, sqlScoped))
		_, err := sqlScoped.TryReplaceMemoryTaintMirror(context.Background(), 1, nil, 10)
		require.Error(t, err)
		return nil
	}))
	require.False(t, MemoryTaintMirrorWorkerSupported(canonical, &PostgresStore{}))
	_, err := (&PostgresStore{}).TryReplaceMemoryTaintMirror(context.Background(), 1, nil, 10)
	require.Error(t, err)
	// An injected implementation of the manual API is not automatic capability.
	require.False(t, MemoryTaintMirrorWorkerSupported(canonical, &manualOnlyMirror{}))
	_, _, published, err := RefreshMemoryTaintMirrorBounded(context.Background(), canonical, &manualOnlyMirror{}, DefaultMemoryTaintMirrorLimits())
	require.Error(t, err)
	require.False(t, published)
}

type manualOnlyMirror struct{}

func (*manualOnlyMirror) ReplaceMemoryTaintMirror(context.Context, uint64, []string) error {
	panic("manual rebuild must not run")
}
func (*manualOnlyMirror) MemoryTaintMirrorRevision(context.Context) (uint64, error) {
	panic("manual read must not run")
}

func TestBoundedMemoryTaintMirrorInactiveHistoryConsumesVisitBudget(t *testing.T) {
	canonical, mirror := boundedMirrorFixture(t)
	for _, label := range []string{"old-one", "old-two"} {
		decision := testTaintDecisionID(label)
		_, err := canonical.ApplyTaintDecision("sage", decision, testTaintParent(label), testTaintPayloadHash(label))
		require.NoError(t, err)
		_, err = canonical.ClearTaintDecision(decision)
		require.NoError(t, err)
	}
	// These headers are retained but inactive. They still cost iterator/header
	// reads and cannot be skipped by the resource accounting.
	_, _, published, err := RefreshMemoryTaintMirrorBounded(context.Background(), canonical, mirror,
		MemoryTaintMirrorLimits{MaxIDs: 10, MaxRows: 10, MaxVisits: 2})
	require.ErrorIs(t, err, ErrMemoryTaintMirrorWorkLimit)
	require.False(t, published)
	assertBoundedMirrorRows(t, mirror, 0, nil)
}

func TestBoundedMemoryTaintMirrorMissingRevisionRollsBack(t *testing.T) {
	_, mirror := boundedMirrorFixture(t)
	require.NoError(t, mirror.ReplaceMemoryTaintMirror(context.Background(), 4, []string{"old"}))
	_, err := mirror.writeExecContext(context.Background(), `DELETE FROM memory_taint_mirror_revision`)
	require.NoError(t, err)
	published, err := mirror.TryReplaceMemoryTaintMirror(context.Background(), 5, []string{"new"}, 10)
	require.Error(t, err)
	require.False(t, published)
	var id string
	require.NoError(t, mirror.conn.QueryRowContext(context.Background(), `SELECT memory_id FROM memory_taint_mirror`).Scan(&id))
	require.Equal(t, "old", id)
}

func TestBoundedMemoryTaintMirrorInFlightSQLiteCancellation(t *testing.T) {
	// Register only on this test-owned driver instance. Neither the default
	// driver nor database/sql's process-global driver registry is changed.
	var entered atomic.Bool
	privateDriver := &sqlite.Driver{}
	require.NoError(t, privateDriver.RegisterScalarFunction("mirror_inflight_marker", 0,
		func(_ *sqlite.FunctionContext, _ []driver.Value) (driver.Value, error) {
			entered.Store(true)
			return int64(1), nil
		}))
	db := sql.OpenDB(&mirrorCancellationConnector{
		sqlite: privateDriver,
		dsn:    "file:" + filepath.ToSlash(filepath.Join(t.TempDir(), "cancel.db")) + "?_pragma=journal_mode(WAL)",
	})
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	mirror := &SQLiteStore{conn: db, db: db}
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer setupCancel()
	require.NoError(t, ensureSQLiteMemoryTaintMirrorSchema(setupCtx, mirror))
	require.NoError(t, mirror.ReplaceMemoryTaintMirror(setupCtx, 4, []string{"old-a", "old-b"}))
	_, err := mirror.writeExecContext(setupCtx, `
		CREATE TABLE mirror_foreground_probe(value INTEGER NOT NULL);
		CREATE TRIGGER slow_mirror_insert BEFORE INSERT ON memory_taint_mirror BEGIN
			SELECT mirror_inflight_marker();
			SELECT sum(x) FROM (
				WITH RECURSIVE work(x) AS (
					SELECT 1 UNION ALL SELECT x + 1 FROM work WHERE x < 1000000000
				) SELECT x FROM work
			);
		END;
	`)
	require.NoError(t, err)
	// Finish connection setup before the short pass deadline. The marker is
	// nondeterministic and nonblocking; it runs inside the actual INSERT,
	// after the bounded publisher's DELETE and before its expensive SELECT.
	require.NoError(t, db.PingContext(setupCtx))
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	published, replaceErr := mirror.TryReplaceMemoryTaintMirror(ctx, 5, []string{"new"}, 10)
	deadlineErr := ctx.Err()
	// Retain actual facts even when an assertion fails. A pre-BeginTx timeout
	// cannot pass the marker assertion. No second connection/query or timeout
	// goroutine wrapper is used to infer statement entry.
	t.Logf("marker=%t replaceErr=%v ctxErr=%v published=%t", entered.Load(), replaceErr, deadlineErr, published)
	require.True(t, entered.Load(), "must execute the private scalar inside the trigger")
	require.Error(t, replaceErr)
	require.ErrorIs(t, deadlineErr, context.DeadlineExceeded)
	require.False(t, published)
	available := mirror.writeMu.TryLock()
	if available {
		mirror.writeMu.Unlock()
	}
	require.True(t, available, "cancelled publication releases the normal writer mutex")
	foregroundCtx, foregroundCancel := context.WithTimeout(context.Background(), time.Second)
	defer foregroundCancel()
	revision, err := mirror.MemoryTaintMirrorRevision(foregroundCtx)
	require.NoError(t, err)
	require.Equal(t, uint64(4), revision)
	rows, err := mirror.conn.QueryContext(foregroundCtx, `SELECT memory_id FROM memory_taint_mirror ORDER BY memory_id`)
	require.NoError(t, err)
	var ids []string
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Equal(t, []string{"old-a", "old-b"}, ids)
	require.NoError(t, mirror.RunInTx(foregroundCtx, func(tx OffchainStore) error {
		_, err := tx.(*SQLiteStore).writeExecContext(foregroundCtx, `INSERT INTO mirror_foreground_probe(value) VALUES (2)`)
		return err
	}))
	var value int
	require.NoError(t, mirror.conn.QueryRowContext(foregroundCtx, `SELECT value FROM mirror_foreground_probe`).Scan(&value))
	require.Equal(t, 2, value, "ordinary foreground writer works after cancellation")
}

// mirrorCancellationConnector is a test-owned connector for an isolated
// sqlite.Driver. sql.OpenDB registers nothing globally. Like modernc's own
// connector, Connect honors ctx before the non-cancellable physical open;
// connection setup is completed before this fixture starts its pass deadline.
type mirrorCancellationConnector struct {
	sqlite *sqlite.Driver
	dsn    string
}

func (c *mirrorCancellationConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.sqlite.Open(c.dsn)
}

func (c *mirrorCancellationConnector) Driver() driver.Driver { return c.sqlite }
