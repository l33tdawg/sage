package store

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrMemoryTaintMirrorWorkLimit = errors.New("memory taint mirror local work limit reached")

// MemoryTaintMirrorLimits are process-local advisory limits, not consensus
// limits. A legal larger state may remain stale; canonical disclosure still
// uses Badger. A later trigger tries again, without an eventual-freshness claim.
type MemoryTaintMirrorLimits struct {
	MaxIDs    int
	MaxRows   int
	MaxVisits int
}

func DefaultMemoryTaintMirrorLimits() MemoryTaintMirrorLimits {
	return MemoryTaintMirrorLimits{MaxIDs: 32768, MaxRows: 32768, MaxVisits: 131072}
}

type boundedMemoryTaintMirror interface {
	MemoryTaintMirrorStore
	TryReplaceMemoryTaintMirror(context.Context, uint64, []string, int) (bool, error)
}

// MemoryTaintMirrorWorkerSupported admits only stock, owning SQL handles.
// Transaction clones must not escape into asynchronous work. There is no
// fallback to the unbounded manual rebuild API for injected mirror stores.
func MemoryTaintMirrorWorkerSupported(canonical *BadgerStore, mirror MemoryTaintMirrorStore) bool {
	if canonical == nil || canonical.db == nil || canonical.txn != nil {
		return false
	}
	switch s := mirror.(type) {
	case *SQLiteStore:
		return s != nil && s.db != nil
	case *PostgresStore:
		return s != nil && s.pool != nil
	default:
		return false
	}
}

// RefreshMemoryTaintMirrorBounded makes one cooperative, bounded attempt. SQL
// publishes a complete ID set and its revision atomically. This is not a
// cross-database freshness fence: a concurrent canonical change can leave a
// complete older revision stale. An uncertain commit can return an error after
// SQL became durable, so errors do not universally imply an unchanged mirror.
func RefreshMemoryTaintMirrorBounded(
	ctx context.Context, canonical *BadgerStore, mirror MemoryTaintMirrorStore,
	limits MemoryTaintMirrorLimits,
) (revision uint64, count int, published bool, err error) {
	if !MemoryTaintMirrorWorkerSupported(canonical, mirror) {
		return 0, 0, false, errors.New("automatic taint mirror requires owning stock stores")
	}
	if limits.MaxIDs <= 0 || limits.MaxRows <= 0 || limits.MaxVisits <= 0 ||
		limits.MaxIDs > 32768 || limits.MaxRows > 32768 || limits.MaxVisits > 131072 {
		return 0, 0, false, errors.New("automatic taint mirror requires positive work limits within automatic ceilings")
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return 0, 0, false, ctxErr
	}
	fresh, revision, err := MemoryTaintMirrorFresh(ctx, canonical, mirror)
	if err != nil || fresh {
		return revision, 0, false, err
	}
	revision, ids, err := canonical.snapshotEffectiveTaintMemberships(ctx, limits.MaxVisits, limits.MaxIDs)
	if err != nil {
		return 0, 0, false, err
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return 0, 0, false, ctxErr
	}
	current, err := canonical.TaintRevision()
	if err != nil {
		return 0, 0, false, err
	}
	if current != revision {
		return 0, 0, false, ErrMemoryTaintMirrorNotConverged
	}
	published, err = mirror.(boundedMemoryTaintMirror).TryReplaceMemoryTaintMirror(ctx, revision, ids, limits.MaxRows)
	if err != nil || !published {
		return revision, len(ids), published, err
	}
	// SQL may already be durable. Preserve that fact if the cooperative pass
	// expired, and do not open another canonical view after cancellation.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return revision, len(ids), true, ctxErr
	}
	current, err = canonical.TaintRevision()
	if err != nil {
		return revision, len(ids), published, err
	}
	if current != revision {
		return revision, len(ids), published, ErrMemoryTaintMirrorNotConverged
	}
	return revision, len(ids), true, nil
}

// TryReplaceMemoryTaintMirror skips SQLite writer contention instead of waiting
// for writeMu. Only this advisory path uses TryLock; canonical writers retain
// their existing synchronization. Precommit failures roll back IDs and revision.
func (s *SQLiteStore) TryReplaceMemoryTaintMirror(
	ctx context.Context, revision uint64, ids []string, maxRows int,
) (bool, error) {
	if s == nil || s.db == nil {
		return false, errors.New("automatic mirror cannot use a SQLite transaction handle")
	}
	if maxRows <= 0 || maxRows > 32768 || len(ids) > maxRows {
		return false, ErrMemoryTaintMirrorWorkLimit
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	ids, err := canonicalTaintMirrorIDs(ids)
	if err != nil {
		return false, err
	}
	if !s.writeMu.TryLock() {
		return false, nil
	}
	defer s.writeMu.Unlock()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return false, ctxErr
	}
	txn, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = txn.Rollback() }()
	rows, err := txn.QueryContext(ctx, `SELECT 1 FROM memory_taint_mirror LIMIT ?`, maxRows+1)
	if err != nil {
		return false, err
	}
	oldRows := 0
	for rows.Next() {
		oldRows++
		if ctxErr := ctx.Err(); ctxErr != nil {
			_ = rows.Close()
			return false, ctxErr
		}
		if oldRows > maxRows {
			_ = rows.Close()
			return false, ErrMemoryTaintMirrorWorkLimit
		}
	}
	rowErr := rows.Err()
	closeErr := rows.Close()
	if rowErr != nil {
		return false, rowErr
	}
	if closeErr != nil {
		return false, closeErr
	}
	if _, deleteErr := txn.ExecContext(ctx, `DELETE FROM memory_taint_mirror`); deleteErr != nil {
		return false, deleteErr
	}
	for _, id := range ids {
		if _, insertErr := txn.ExecContext(ctx, `INSERT INTO memory_taint_mirror(memory_id) VALUES (?)`, id); insertErr != nil {
			return false, insertErr
		}
	}
	result, err := txn.ExecContext(ctx, `UPDATE memory_taint_mirror_revision SET revision = ? WHERE singleton = 1`, revision)
	if err != nil {
		return false, err
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return false, fmt.Errorf("update taint mirror revision: affected %d rows: %v", n, err)
	}
	if err := txn.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// TryReplaceMemoryTaintMirror takes a deadline-bound table lock conflicting with
// all SQL writers (including the manual API) before probing old rows. Thus the
// bounded probe and DELETE see one serialized set. NOWAIT skips lock waits; the
// caller's deadline also bounds pool acquisition and subsequent SQL work.
func (s *PostgresStore) TryReplaceMemoryTaintMirror(
	ctx context.Context, revision uint64, ids []string, maxRows int,
) (bool, error) {
	if s == nil || s.pool == nil {
		return false, errors.New("automatic mirror cannot use a PostgreSQL transaction handle")
	}
	if maxRows <= 0 || maxRows > 32768 || len(ids) > maxRows {
		return false, ErrMemoryTaintMirrorWorkLimit
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	ids, err := canonicalTaintMirrorIDs(ids)
	if err != nil {
		return false, err
	}
	txn, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() {
		// Cleanup has its own short cooperative deadline if the pass expired.
		cleanup, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_ = txn.Rollback(cleanup)
	}()
	if _, lockErr := txn.Exec(ctx, `LOCK TABLE memory_taint_mirror, memory_taint_mirror_revision IN SHARE ROW EXCLUSIVE MODE NOWAIT`); lockErr != nil {
		return false, lockErr
	}
	rows, err := txn.Query(ctx, `SELECT 1 FROM memory_taint_mirror LIMIT $1`, maxRows+1)
	if err != nil {
		return false, err
	}
	oldRows := 0
	for rows.Next() {
		oldRows++
		if ctxErr := ctx.Err(); ctxErr != nil {
			rows.Close()
			return false, ctxErr
		}
		if oldRows > maxRows {
			rows.Close()
			return false, ErrMemoryTaintMirrorWorkLimit
		}
	}
	rowErr := rows.Err()
	rows.Close()
	if rowErr != nil {
		return false, rowErr
	}
	if _, deleteErr := txn.Exec(ctx, `DELETE FROM memory_taint_mirror`); deleteErr != nil {
		return false, deleteErr
	}
	for _, id := range ids {
		if _, insertErr := txn.Exec(ctx, `INSERT INTO memory_taint_mirror(memory_id) VALUES ($1)`, id); insertErr != nil {
			return false, insertErr
		}
	}
	result, err := txn.Exec(ctx, `UPDATE memory_taint_mirror_revision SET revision = $1 WHERE singleton = 1`, int64(revision))
	if err != nil {
		return false, err
	}
	if result.RowsAffected() != 1 {
		return false, errors.New("update taint mirror revision: missing singleton")
	}
	if err := txn.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}
