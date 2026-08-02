package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/l33tdawg/sage/internal/memory"
)

// LegacyMemoryProjectionRecord is the complete local evidence needed to plan
// one app-v25 adoption. It is deliberately an off-consensus type: only its
// hash-bound disclosure envelope is ever proposed to consensus.
type LegacyMemoryProjectionRecord struct {
	MemoryID        string
	SubmittingAgent string
	Content         string
	ContentHash     []byte
	Status          memory.MemoryStatus
	Domain          string
	Classification  uint8
	CreatedAt       time.Time
	CreatedAtCursor string
	EvidenceError   string
}

// LegacyMemoryRecoveryItem is one preserved row that cannot currently be
// adopted automatically. The reason is machine-readable; plaintext is never
// duplicated into this queue.
type LegacyMemoryRecoveryItem struct {
	MemoryID string
	Reason   string
}

// LegacyMemoryRecoveryRecord is the durable operator-facing recovery state.
type LegacyMemoryRecoveryRecord struct {
	MemoryID           string
	Reason             string
	ProjectionRevision uint64
	Resolved           bool
}

// LegacyMemoryRecoveryDisposition is a durable local CEREBRUM decision about
// one projection row that could not be adopted canonically. It does not delete
// or rewrite the memory row and is not a fabricated canonical lifecycle event.
type LegacyMemoryRecoveryDisposition struct {
	MemoryID           string
	Reason             string
	ProjectionRevision uint64
	AuthorizedBy       string
}

var ErrLegacyMemoryRecoverySnapshotChanged = errors.New(
	"legacy memory recovery inventory changed",
)

// LegacyMemoryAdoptionProgress is the aggregate, content-free migration view
// exposed to local CEREBRUM.
type LegacyMemoryAdoptionProgress struct {
	State      string `json:"state"`
	Discovered int    `json:"discovered"`
	Converted  int    `json:"converted"`
	Remaining  int    `json:"remaining"`
	Recovery   int    `json:"recovery"`
	Revision   uint64 `json:"projection_revision"`
	Message    string `json:"message"`
}

// LegacyMemoryProjectionSnapshot is a stable, read-only SQL inventory view
// used by app-v25 planning. Its SQLite implementation is backed by one WAL
// read transaction and one captured vault generation, so ordinary memory
// writes continue while every page observes the same evidence generation.
type LegacyMemoryProjectionSnapshot interface {
	MemoryProjectionRevision(context.Context) (uint64, error)
	ListLegacyMemoryRecoveryDispositionIDs(context.Context) (map[string]struct{}, error)
	ListLegacyMemoryRecoveryAssignments(context.Context) (map[string]LegacyMemoryRecoveryAssignment, error)
	ListLegacyMemoryProjectionPage(
		context.Context,
		string,
		string,
		int,
	) ([]LegacyMemoryProjectionRecord, error)
	VaultLocked() bool
	VaultGeneration() uint64
}

// ReadLegacyMemoryProjectionSnapshot runs fn against one stable SQLite WAL
// read transaction without taking writeMu. The snapshot captures the current
// vault pointer and generation so a concurrent unlock/key change cannot mix
// decryption generations within one adoption plan.
func (s *SQLiteStore) ReadLegacyMemoryProjectionSnapshot(
	ctx context.Context,
	fn func(LegacyMemoryProjectionSnapshot) error,
) error {
	if s == nil || s.conn == nil {
		return fmt.Errorf("legacy memory projection is unavailable")
	}
	if fn == nil {
		return fmt.Errorf("legacy memory projection snapshot callback is required")
	}
	if s.db == nil {
		return fn(s)
	}
	readTx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("begin legacy memory projection snapshot: %w", err)
	}
	defer func() { _ = readTx.Rollback() }()

	var fixedVaultGeneration atomic.Uint64
	fixedVaultGeneration.Store(s.VaultGeneration())
	snapshot := &SQLiteStore{
		conn:            readTx,
		dbPath:          s.dbPath,
		vaultGeneration: &fixedVaultGeneration,
	}
	snapshot.vault.Store(s.vault.Load())
	snapshot.vaultExpected.Store(s.vaultExpected.Load())
	if err := fn(snapshot); err != nil {
		return err
	}
	if err := readTx.Commit(); err != nil {
		return fmt.Errorf("commit legacy memory projection snapshot: %w", err)
	}
	return nil
}

// GetLegacyMemoryProjectionRecord returns the exact SQL evidence for one
// validator attestation. Missing rows return (nil, nil).
func (s *SQLiteStore) GetLegacyMemoryProjectionRecord(
	ctx context.Context,
	memoryID string,
) (*LegacyMemoryProjectionRecord, error) {
	if s == nil || s.conn == nil {
		return nil, fmt.Errorf("legacy memory projection is unavailable")
	}
	var (
		record    LegacyMemoryProjectionRecord
		status    string
		createdAt string
	)
	err := s.conn.QueryRowContext(
		ctx,
		`SELECT memory_id, submitting_agent, content, content_hash,
			status, domain_tag, classification, created_at
			FROM memories WHERE memory_id = ?`,
		memoryID,
	).Scan(
		&record.MemoryID,
		&record.SubmittingAgent,
		&record.Content,
		&record.ContentHash,
		&status,
		&record.Domain,
		&record.Classification,
		&createdAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read legacy memory projection %s: %w", memoryID, err)
	}
	record.Status = memory.MemoryStatus(status)
	record.CreatedAtCursor = createdAt
	record.CreatedAt = parseTime(createdAt)
	plaintext, err := s.decryptContent(record.Content)
	if err != nil {
		record.EvidenceError = "content_decryption_failed"
		return &record, nil
	}
	record.Content = plaintext
	return &record, nil
}

// GetLegacyMemoryProjectionRecords returns only the requested IDs from one
// read transaction. The returned slice matches memoryIDs exactly; a missing
// row is nil. App-v25 validators use this bounded snapshot for exact-batch
// attestation, so unrelated SQL writes cannot force a full inventory rescan.
func (s *SQLiteStore) GetLegacyMemoryProjectionRecords(
	ctx context.Context,
	memoryIDs []string,
) ([]*LegacyMemoryProjectionRecord, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("legacy memory projection batch requires the standalone projection store")
	}
	if len(memoryIDs) == 0 || len(memoryIDs) > 256 {
		return nil, fmt.Errorf("legacy memory projection batch size %d is outside 1..256", len(memoryIDs))
	}
	seen := make(map[string]struct{}, len(memoryIDs))
	placeholders := make([]string, len(memoryIDs))
	args := make([]any, len(memoryIDs))
	for i, memoryID := range memoryIDs {
		if memoryID == "" {
			return nil, fmt.Errorf("legacy memory projection batch contains an empty memory id")
		}
		if _, duplicate := seen[memoryID]; duplicate {
			return nil, fmt.Errorf("legacy memory projection batch contains duplicate memory id %q", memoryID)
		}
		seen[memoryID] = struct{}{}
		placeholders[i] = "?"
		args[i] = memoryID
	}
	readTx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin legacy memory projection batch: %w", err)
	}
	defer func() { _ = readTx.Rollback() }()
	// #nosec G202 -- placeholders contains only generated "?" tokens; every
	// memory ID remains a separately bound query argument.
	rows, err := readTx.QueryContext(
		ctx,
		`SELECT memory_id, submitting_agent, content, content_hash,
			status, domain_tag, classification, created_at
			FROM memories WHERE memory_id IN (`+strings.Join(placeholders, ",")+`)`,
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("read legacy memory projection batch: %w", err)
	}
	defer func() { _ = rows.Close() }()
	recordsByID := make(map[string]*LegacyMemoryProjectionRecord, len(memoryIDs))
	for rows.Next() {
		var (
			record    LegacyMemoryProjectionRecord
			status    string
			createdAt string
		)
		if err := rows.Scan(
			&record.MemoryID,
			&record.SubmittingAgent,
			&record.Content,
			&record.ContentHash,
			&status,
			&record.Domain,
			&record.Classification,
			&createdAt,
		); err != nil {
			return nil, fmt.Errorf("scan legacy memory projection batch: %w", err)
		}
		record.Status = memory.MemoryStatus(status)
		record.CreatedAtCursor = createdAt
		record.CreatedAt = parseTime(createdAt)
		plaintext, decryptErr := s.decryptContent(record.Content)
		if decryptErr != nil {
			record.EvidenceError = "content_decryption_failed"
		} else {
			record.Content = plaintext
		}
		recordsByID[record.MemoryID] = &record
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("walk legacy memory projection batch: %w", err)
	}
	if err := readTx.Commit(); err != nil {
		return nil, fmt.Errorf("commit legacy memory projection batch read: %w", err)
	}
	result := make([]*LegacyMemoryProjectionRecord, len(memoryIDs))
	for i, memoryID := range memoryIDs {
		result[i] = recordsByID[memoryID]
	}
	return result, nil
}

// ListLegacyMemoryProjectionPage walks the SQL inventory in a stable keyset
// order. Keyset paging avoids offset drift while the planner later verifies the
// transactionally maintained projection revision before it publishes a plan.
func (s *SQLiteStore) ListLegacyMemoryProjectionPage(
	ctx context.Context,
	afterCreatedAt string,
	afterMemoryID string,
	limit int,
) ([]LegacyMemoryProjectionRecord, error) {
	if s == nil || s.conn == nil {
		return nil, fmt.Errorf("legacy memory projection is unavailable")
	}
	if limit <= 0 || limit > 1024 {
		limit = 512
	}
	query := `SELECT memory_id, submitting_agent, content, content_hash,
		status, domain_tag, classification, created_at
		FROM memories
		WHERE (? = 0 OR created_at > ? OR (created_at = ? AND memory_id > ?))
		ORDER BY created_at ASC, memory_id ASC
		LIMIT ?`
	started := 0
	if afterMemoryID != "" || afterCreatedAt != "" {
		started = 1
	}
	rows, err := s.conn.QueryContext(
		ctx,
		query,
		started,
		afterCreatedAt,
		afterCreatedAt,
		afterMemoryID,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list legacy memory projection page: %w", err)
	}
	defer func() { _ = rows.Close() }()

	records := make([]LegacyMemoryProjectionRecord, 0, limit)
	for rows.Next() {
		var (
			record    LegacyMemoryProjectionRecord
			status    string
			createdAt string
		)
		if err := rows.Scan(
			&record.MemoryID,
			&record.SubmittingAgent,
			&record.Content,
			&record.ContentHash,
			&status,
			&record.Domain,
			&record.Classification,
			&createdAt,
		); err != nil {
			return nil, fmt.Errorf("scan legacy memory projection: %w", err)
		}
		record.Status = memory.MemoryStatus(status)
		record.CreatedAtCursor = createdAt
		record.CreatedAt = parseTime(createdAt)
		plaintext, decryptErr := s.decryptContent(record.Content)
		if decryptErr != nil {
			record.EvidenceError = "content_decryption_failed"
		} else {
			record.Content = plaintext
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("walk legacy memory projection: %w", err)
	}
	return records, nil
}

// SyncLegacyMemoryRecoveryQueue persists the current unresolved inventory so a
// restart cannot lose the operator's retry/export/explicit-deprecation choices.
// Rows absent from the latest stable scan are marked resolved, not deleted.
func (s *SQLiteStore) SyncLegacyMemoryRecoveryQueue(
	ctx context.Context,
	revision uint64,
	items []LegacyMemoryRecoveryItem,
) error {
	if s == nil || s.conn == nil {
		return fmt.Errorf("legacy memory projection is unavailable")
	}
	if s.db == nil {
		return fmt.Errorf("legacy memory recovery queue requires the standalone projection store")
	}
	txn, unlock, err := s.beginTxLocked(ctx)
	if err != nil {
		return fmt.Errorf("begin legacy memory recovery queue sync: %w", err)
	}
	defer unlock()
	defer func() { _ = txn.Rollback() }()
	if _, err = txn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS legacy_memory_recovery (
		memory_id TEXT PRIMARY KEY,
		machine_reason TEXT NOT NULL,
		projection_revision INTEGER NOT NULL,
		first_seen_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		last_seen_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		resolved_at TEXT
	)`); err != nil {
		return fmt.Errorf("create legacy memory recovery queue: %w", err)
	}
	if _, err = txn.ExecContext(ctx,
		`UPDATE legacy_memory_recovery
		 SET resolved_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		 WHERE resolved_at IS NULL`,
	); err != nil {
		return fmt.Errorf("mark prior legacy recovery rows resolved: %w", err)
	}
	for _, item := range items {
		if item.MemoryID == "" || item.Reason == "" {
			return fmt.Errorf("legacy recovery queue item requires memory id and reason")
		}
		if _, err = txn.ExecContext(ctx, `INSERT INTO legacy_memory_recovery (
				memory_id, machine_reason, projection_revision, resolved_at
			) SELECT ?, ?, ?, NULL
			WHERE NOT EXISTS (
				SELECT 1 FROM legacy_memory_recovery_disposition
				WHERE memory_id = ? AND disposition = 'deprecated'
			)
			ON CONFLICT(memory_id) DO UPDATE SET
				machine_reason = excluded.machine_reason,
				projection_revision = excluded.projection_revision,
				last_seen_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
				resolved_at = NULL`,
			item.MemoryID,
			item.Reason,
			revision,
			item.MemoryID,
		); err != nil {
			return fmt.Errorf("upsert legacy recovery row %s: %w", item.MemoryID, err)
		}
	}
	if err = txn.Commit(); err != nil {
		return fmt.Errorf("commit legacy memory recovery queue sync: %w", err)
	}
	return nil
}

// ListLegacyMemoryRecoveryQueue returns the durable machine-readable recovery
// inventory. It intentionally contains no plaintext.
func (s *SQLiteStore) ListLegacyMemoryRecoveryQueue(
	ctx context.Context,
	includeResolved bool,
) ([]LegacyMemoryRecoveryRecord, error) {
	query := `SELECT memory_id, machine_reason, projection_revision,
		CASE WHEN resolved_at IS NULL THEN 0 ELSE 1 END
		FROM legacy_memory_recovery`
	if !includeResolved {
		query += ` WHERE resolved_at IS NULL`
	}
	query += ` ORDER BY memory_id ASC`
	rows, err := s.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list legacy memory recovery queue: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var records []LegacyMemoryRecoveryRecord
	for rows.Next() {
		var (
			record   LegacyMemoryRecoveryRecord
			revision int64
			resolved int
		)
		if err := rows.Scan(
			&record.MemoryID,
			&record.Reason,
			&revision,
			&resolved,
		); err != nil {
			return nil, fmt.Errorf("scan legacy memory recovery queue: %w", err)
		}
		if revision < 0 {
			return nil, errors.New("legacy recovery projection revision is negative")
		}
		record.ProjectionRevision = uint64(revision)
		record.Resolved = resolved != 0
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("walk legacy memory recovery queue: %w", err)
	}
	return records, nil
}

// ListLegacyMemoryRecoveryDispositionIDs returns the content-free set of rows
// that the local CEREBRUM Root explicitly retired from automatic adoption.
// The original memories and recovery audit rows remain stored unchanged.
func (s *SQLiteStore) ListLegacyMemoryRecoveryDispositionIDs(
	ctx context.Context,
) (map[string]struct{}, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT memory_id FROM legacy_memory_recovery_disposition
		 WHERE disposition = 'deprecated' ORDER BY memory_id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list legacy memory recovery dispositions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	result := make(map[string]struct{})
	for rows.Next() {
		var memoryID string
		if err := rows.Scan(&memoryID); err != nil {
			return nil, fmt.Errorf("scan legacy memory recovery disposition: %w", err)
		}
		result[memoryID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("walk legacy memory recovery dispositions: %w", err)
	}
	return result, nil
}

// ValidateLegacyMemoryRecoverySnapshot gives a non-mutating preflight that an
// operator action still targets the exact unresolved inventory shown by
// CEREBRUM. The recovery queue is its own durable snapshot: ordinary current
// memory writes must not invalidate it just because they advance the global
// projection revision. DeprecateLegacyMemoryRecoverySnapshot repeats these
// exact-queue checks inside its write transaction before recording any
// disposition.
func (s *SQLiteStore) ValidateLegacyMemoryRecoverySnapshot(
	ctx context.Context,
	expectedRevision uint64,
	expectedCount int,
) error {
	if expectedRevision == 0 || expectedCount <= 0 {
		return ErrLegacyMemoryRecoverySnapshotChanged
	}
	var count int
	if err := s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM legacy_memory_recovery
		 WHERE resolved_at IS NULL AND projection_revision = ?`,
		expectedRevision,
	).Scan(&count); err != nil {
		return fmt.Errorf("count legacy memory recovery inventory: %w", err)
	}
	if count != expectedCount {
		return ErrLegacyMemoryRecoverySnapshotChanged
	}
	var other int
	if err := s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM legacy_memory_recovery
		 WHERE resolved_at IS NULL AND projection_revision != ?`,
		expectedRevision,
	).Scan(&other); err != nil {
		return fmt.Errorf("check legacy memory recovery revision: %w", err)
	}
	if other != 0 {
		return ErrLegacyMemoryRecoverySnapshotChanged
	}
	return nil
}

// DeprecateLegacyMemoryRecoverySnapshot records an explicit, content-free Root
// disposition for every row in one exact unresolved inventory. This is an
// atomic projection-control decision: memory rows, plaintext, hashes, status,
// and chain history are never deleted or rewritten.
func (s *SQLiteStore) DeprecateLegacyMemoryRecoverySnapshot(
	ctx context.Context,
	expectedRevision uint64,
	expectedCount int,
	authorizedBy string,
) (int, error) {
	if expectedRevision == 0 || expectedCount <= 0 || strings.TrimSpace(authorizedBy) == "" {
		return 0, ErrLegacyMemoryRecoverySnapshotChanged
	}
	txn, unlock, beginErr := s.beginTxLocked(ctx)
	if beginErr != nil {
		return 0, fmt.Errorf("begin legacy memory recovery disposition: %w", beginErr)
	}
	defer unlock()
	defer func() { _ = txn.Rollback() }()

	rows, queryErr := txn.QueryContext(ctx,
		`SELECT memory_id, machine_reason FROM legacy_memory_recovery
		 WHERE resolved_at IS NULL AND projection_revision = ?
		 ORDER BY memory_id ASC`, expectedRevision)
	if queryErr != nil {
		return 0, fmt.Errorf("read legacy memory recovery inventory: %w", queryErr)
	}
	type disposition struct{ memoryID, reason string }
	// expectedCount is supplied by the Root's signed recovery snapshot. It is
	// verified against this transaction's inventory below, but must not control
	// an up-front allocation before that verification has happened.
	var items []disposition
	for rows.Next() {
		var item disposition
		if scanErr := rows.Scan(&item.memoryID, &item.reason); scanErr != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("scan legacy memory recovery inventory: %w", scanErr)
		}
		items = append(items, item)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("walk legacy memory recovery inventory: %w", rowsErr)
	}
	if closeErr := rows.Close(); closeErr != nil {
		return 0, fmt.Errorf("close legacy memory recovery inventory: %w", closeErr)
	}
	var other int
	if err := txn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM legacy_memory_recovery
		 WHERE resolved_at IS NULL AND projection_revision != ?`, expectedRevision,
	).Scan(&other); err != nil {
		return 0, fmt.Errorf("check legacy memory recovery revision: %w", err)
	}
	if len(items) != expectedCount || other != 0 {
		return 0, ErrLegacyMemoryRecoverySnapshotChanged
	}
	// An operator assignment is a newer decision than the inventory view used
	// by a concurrent "deprecate all" request. Never retire that assignment out
	// from under the canonical adoption worker; force CEREBRUM to refresh so the
	// operator can explicitly choose the remaining unassigned records.
	var assigned int
	if err := txn.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM legacy_memory_recovery_assignment a
		JOIN legacy_memory_recovery r ON r.memory_id = a.memory_id
		WHERE r.resolved_at IS NULL AND r.projection_revision = ?
		  AND a.projection_revision = r.projection_revision
		  AND a.machine_reason = r.machine_reason`, expectedRevision).Scan(&assigned); err != nil {
		return 0, fmt.Errorf("check legacy recovery assignments before deprecation: %w", err)
	}
	if assigned != 0 {
		return 0, ErrLegacyMemoryRecoverySnapshotChanged
	}
	progressResult, err := txn.ExecContext(ctx,
		`UPDATE legacy_memory_adoption_progress
		 SET state = 'complete', remaining = 0, recovery = 0,
		     message = 'Memory upgrade complete. Preserved historical records were explicitly retired by CEREBRUM Root.',
		     updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		 WHERE singleton = 1 AND state = 'recovery' AND remaining = 0
		   AND recovery = ? AND projection_revision = ?`,
		expectedCount, expectedRevision,
	)
	if err != nil {
		return 0, fmt.Errorf("complete legacy memory recovery progress: %w", err)
	}
	progressRows, err := progressResult.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("verify legacy memory recovery progress update: %w", err)
	}
	if progressRows != 1 {
		return 0, ErrLegacyMemoryRecoverySnapshotChanged
	}
	for _, item := range items {
		if _, err := txn.ExecContext(ctx,
			`INSERT INTO legacy_memory_recovery_disposition (
				memory_id, disposition, machine_reason, projection_revision, authorized_by
			) VALUES (?, 'deprecated', ?, ?, ?)
			ON CONFLICT(memory_id) DO NOTHING`,
			item.memoryID, item.reason, expectedRevision, authorizedBy,
		); err != nil {
			return 0, fmt.Errorf("record legacy memory recovery disposition %s: %w", item.memoryID, err)
		}
	}
	if _, err := txn.ExecContext(ctx,
		`UPDATE legacy_memory_recovery
		 SET resolved_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		 WHERE resolved_at IS NULL AND projection_revision = ?`, expectedRevision,
	); err != nil {
		return 0, fmt.Errorf("resolve explicitly deprecated legacy recovery rows: %w", err)
	}
	if err := txn.Commit(); err != nil {
		return 0, fmt.Errorf("commit legacy memory recovery disposition: %w", err)
	}
	return len(items), nil
}

func (s *SQLiteStore) ListLegacyMemoryRecoveryDispositions(
	ctx context.Context,
) ([]LegacyMemoryRecoveryDisposition, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT memory_id, machine_reason, projection_revision, authorized_by
		 FROM legacy_memory_recovery_disposition ORDER BY memory_id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list legacy memory recovery disposition audit: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var result []LegacyMemoryRecoveryDisposition
	for rows.Next() {
		var record LegacyMemoryRecoveryDisposition
		var revision int64
		if err := rows.Scan(&record.MemoryID, &record.Reason, &revision, &record.AuthorizedBy); err != nil {
			return nil, fmt.Errorf("scan legacy memory recovery disposition audit: %w", err)
		}
		if revision < 0 {
			return nil, errors.New("legacy recovery disposition revision is negative")
		}
		record.ProjectionRevision = uint64(revision)
		result = append(result, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("walk legacy memory recovery disposition audit: %w", err)
	}
	return result, nil
}

// PublishLegacyMemoryAdoptionProgress atomically replaces the aggregate
// content-free progress snapshot after a stable inventory scan.
func (s *SQLiteStore) PublishLegacyMemoryAdoptionProgress(
	ctx context.Context,
	progress LegacyMemoryAdoptionProgress,
) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("legacy memory adoption progress requires the standalone projection store")
	}
	if progress.State == "" || progress.Discovered < 0 || progress.Converted < 0 ||
		progress.Remaining < 0 || progress.Recovery < 0 {
		return fmt.Errorf("legacy memory adoption progress is invalid")
	}
	txn, unlock, err := s.beginTxLocked(ctx)
	if err != nil {
		return fmt.Errorf("begin legacy adoption progress publication: %w", err)
	}
	defer unlock()
	defer func() { _ = txn.Rollback() }()
	if _, err = txn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS legacy_memory_adoption_progress (
		singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
		state TEXT NOT NULL,
		discovered INTEGER NOT NULL,
		converted INTEGER NOT NULL,
		remaining INTEGER NOT NULL,
		recovery INTEGER NOT NULL,
		projection_revision INTEGER NOT NULL,
		message TEXT NOT NULL,
		updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	)`); err != nil {
		return fmt.Errorf("create legacy adoption progress: %w", err)
	}
	if _, err = txn.ExecContext(ctx, `INSERT INTO legacy_memory_adoption_progress (
			singleton, state, discovered, converted, remaining, recovery,
			projection_revision, message
		) VALUES (1, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(singleton) DO UPDATE SET
			state = excluded.state,
			discovered = excluded.discovered,
			converted = excluded.converted,
			remaining = excluded.remaining,
			recovery = excluded.recovery,
			projection_revision = excluded.projection_revision,
			message = excluded.message,
			updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')`,
		progress.State,
		progress.Discovered,
		progress.Converted,
		progress.Remaining,
		progress.Recovery,
		progress.Revision,
		progress.Message,
	); err != nil {
		return fmt.Errorf("publish legacy adoption progress: %w", err)
	}
	if err = txn.Commit(); err != nil {
		return fmt.Errorf("commit legacy adoption progress: %w", err)
	}
	return nil
}

// GetLegacyMemoryAdoptionProgress returns nil until the first stable scan.
func (s *SQLiteStore) GetLegacyMemoryAdoptionProgress(
	ctx context.Context,
) (*LegacyMemoryAdoptionProgress, error) {
	if s == nil || s.conn == nil {
		return nil, fmt.Errorf("legacy memory projection is unavailable")
	}
	var (
		progress LegacyMemoryAdoptionProgress
		revision int64
	)
	err := s.conn.QueryRowContext(ctx, `SELECT state, discovered, converted,
		remaining, recovery, projection_revision, message
		FROM legacy_memory_adoption_progress WHERE singleton = 1`,
	).Scan(
		&progress.State,
		&progress.Discovered,
		&progress.Converted,
		&progress.Remaining,
		&progress.Recovery,
		&revision,
		&progress.Message,
	)
	if err == sql.ErrNoRows ||
		err != nil && strings.Contains(err.Error(), "no such table") {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read legacy memory adoption progress: %w", err)
	}
	if revision < 0 {
		return nil, errors.New("legacy adoption progress revision is negative")
	}
	progress.Revision = uint64(revision)
	return &progress, nil
}
