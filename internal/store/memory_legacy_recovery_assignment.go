package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/tx"
)

const MaxLegacyMemoryRecoverySelection = 256

// Disposition IDs are intentionally wider than adoption wire IDs. A malformed
// historical ID that cannot be represented in the consensus adoption payload
// must still be explicitly deprecable instead of becoming a permanent queue
// poison pill. The HTTP request cap remains the outer bound.
const maxLegacyMemoryRecoveryDispositionIDBytes = 128 << 10

// LegacyMemoryRecoveryAssignment is content-free operator intent. It is not
// canonical memory state: the existing Root-bound legacy-adoption governance
// proposal must still be accepted before AuthorPrincipal changes on-chain.
type LegacyMemoryRecoveryAssignment struct {
	MemoryID           string `json:"memory_id"`
	TargetAgentID      string `json:"target_agent_id"`
	Reason             string `json:"reason"`
	ProjectionRevision uint64 `json:"projection_revision"`
	AuthorizedBy       string `json:"authorized_by"`
}

type LegacyMemoryRecoveryInventoryItem struct {
	MemoryID           string
	Reason             string
	ProjectionRevision uint64
	SubmittingAgent    string
	Content            string
	ContentHash        []byte
	Status             memory.MemoryStatus
	Domain             string
	Classification     uint8
	EvidenceError      string
	AssignedTarget     string
}

// ListLegacyMemoryRecoveryInventoryPage joins the content-free queue to the
// original memory table at read time. Already-deprecated rows are deliberately
// excluded. No plaintext is copied into either recovery-control table.
func (s *SQLiteStore) ListLegacyMemoryRecoveryInventoryPage(
	ctx context.Context, afterMemoryID string, limit int,
) ([]LegacyMemoryRecoveryInventoryItem, string, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	if err := s.ensureLegacyMemoryRecoveryAssignmentTable(ctx); err != nil {
		return nil, "", err
	}
	rows, err := s.conn.QueryContext(ctx, `SELECT r.memory_id, r.machine_reason,
		r.projection_revision, m.submitting_agent, m.content, m.content_hash,
		m.status, m.domain_tag, m.classification,
		COALESCE(a.target_agent_id, '')
		FROM legacy_memory_recovery r
		JOIN memories m ON m.memory_id = r.memory_id
		LEFT JOIN legacy_memory_recovery_assignment a
			ON a.memory_id = r.memory_id
			AND a.projection_revision = r.projection_revision
			AND a.machine_reason = r.machine_reason
		WHERE r.resolved_at IS NULL AND m.status != 'deprecated' AND r.memory_id > ?
		ORDER BY r.memory_id ASC LIMIT ?`, afterMemoryID, limit+1)
	if err != nil {
		return nil, "", fmt.Errorf("list legacy recovery inventory page: %w", err)
	}
	defer func() { _ = rows.Close() }()
	// Do not derive allocation capacity from an HTTP-controlled page size. The
	// query is bounded above, and append growth remains independent of tainted
	// capacity input for static analysis and defensive resource accounting.
	items := make([]LegacyMemoryRecoveryInventoryItem, 0)
	for rows.Next() {
		var item LegacyMemoryRecoveryInventoryItem
		var revision int64
		var status string
		if err := rows.Scan(&item.MemoryID, &item.Reason, &revision,
			&item.SubmittingAgent, &item.Content, &item.ContentHash, &status,
			&item.Domain, &item.Classification, &item.AssignedTarget); err != nil {
			return nil, "", fmt.Errorf("scan legacy recovery inventory page: %w", err)
		}
		if revision < 0 {
			return nil, "", errors.New("legacy recovery inventory revision is negative")
		}
		item.ProjectionRevision = uint64(revision)
		item.Status = memory.MemoryStatus(status)
		plain, decryptErr := s.decryptContent(item.Content)
		if decryptErr != nil {
			item.Content = ""
			item.EvidenceError = "content_decryption_failed"
		} else {
			item.Content = plain
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("walk legacy recovery inventory page: %w", err)
	}
	next := ""
	if len(items) > limit {
		next = items[limit-1].MemoryID
		items = items[:limit]
	}
	return items, next, nil
}

func (s *SQLiteStore) ensureLegacyMemoryRecoveryAssignmentTable(ctx context.Context) error {
	_, err := s.writeExecContext(ctx, `CREATE TABLE IF NOT EXISTS legacy_memory_recovery_assignment (
		memory_id TEXT PRIMARY KEY,
		target_agent_id TEXT NOT NULL,
		machine_reason TEXT NOT NULL,
		projection_revision INTEGER NOT NULL,
		authorized_by TEXT NOT NULL,
		created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	)`)
	if err != nil {
		return fmt.Errorf("create legacy memory recovery assignment table: %w", err)
	}
	return nil
}

// ListLegacyMemoryRecoveryAssignments returns operator assignment intents.
// Plaintext remains in memories and is joined only by the Root inventory read.
func (s *SQLiteStore) ListLegacyMemoryRecoveryAssignments(
	ctx context.Context,
) (map[string]LegacyMemoryRecoveryAssignment, error) {
	rows, err := s.conn.QueryContext(ctx, `SELECT memory_id, target_agent_id,
		machine_reason, projection_revision, authorized_by
		FROM legacy_memory_recovery_assignment ORDER BY memory_id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list legacy memory recovery assignments: %w", err)
	}
	defer func() { _ = rows.Close() }()
	result := make(map[string]LegacyMemoryRecoveryAssignment)
	for rows.Next() {
		var record LegacyMemoryRecoveryAssignment
		var revision int64
		if err := rows.Scan(&record.MemoryID, &record.TargetAgentID, &record.Reason,
			&revision, &record.AuthorizedBy); err != nil {
			return nil, fmt.Errorf("scan legacy memory recovery assignment: %w", err)
		}
		if revision < 0 {
			return nil, errors.New("legacy recovery assignment revision is negative")
		}
		record.ProjectionRevision = uint64(revision)
		result[record.MemoryID] = record
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("walk legacy memory recovery assignments: %w", err)
	}
	return result, nil
}

func canonicalRecoverySelection(ids []string, maxIDBytes int) ([]string, error) {
	if len(ids) == 0 || len(ids) > MaxLegacyMemoryRecoverySelection {
		return nil, fmt.Errorf("legacy recovery selection size %d is outside 1..%d",
			len(ids), MaxLegacyMemoryRecoverySelection)
	}
	result := append([]string(nil), ids...)
	sort.Strings(result)
	for i, id := range result {
		if strings.TrimSpace(id) == "" {
			return nil, errors.New("legacy recovery selection contains an empty memory id")
		}
		if len(id) > maxIDBytes {
			return nil, fmt.Errorf("legacy recovery memory id length %d exceeds %d bytes", len(id), maxIDBytes)
		}
		if i > 0 && id == result[i-1] {
			return nil, errors.New("legacy recovery selection contains a duplicate memory id")
		}
	}
	return result, nil
}

// AssignLegacyMemoryRecoverySelection records a Root decision for an exact
// queue snapshot. Only author_identity_unresolved rows are eligible: corrupt,
// conflicting, hash-invalid, missing-domain and already-deprecated evidence is
// never made assignable by this local table. Exact replays are idempotent.
func (s *SQLiteStore) AssignLegacyMemoryRecoverySelection(
	ctx context.Context,
	expectedRevision uint64,
	expectedCount int,
	memoryIDs []string,
	targetAgentID string,
	authorizedBy string,
) (int, error) {
	ids, err := canonicalRecoverySelection(memoryIDs, tx.MaxMemoryLegacyAdoptionIDBytes)
	if err != nil || expectedRevision == 0 || expectedCount <= 0 ||
		strings.TrimSpace(targetAgentID) == "" || strings.TrimSpace(authorizedBy) == "" {
		return 0, ErrLegacyMemoryRecoverySnapshotChanged
	}
	if ensureErr := s.ensureLegacyMemoryRecoveryAssignmentTable(ctx); ensureErr != nil {
		return 0, ensureErr
	}
	txn, unlock, err := s.beginTxLocked(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin legacy recovery assignment: %w", err)
	}
	defer unlock()
	defer func() { _ = txn.Rollback() }()

	// An exact replay remains successful after the queue progresses.
	existingExact := 0
	for _, id := range ids {
		var target, reason string
		var revision int64
		err := txn.QueryRowContext(ctx, `SELECT target_agent_id, machine_reason,
			projection_revision FROM legacy_memory_recovery_assignment WHERE memory_id = ?`, id).
			Scan(&target, &reason, &revision)
		switch {
		case err == nil && reason == "author_identity_unresolved" &&
			revision == int64(expectedRevision):
			if target != targetAgentID {
				return 0, ErrLegacyMemoryRecoverySnapshotChanged
			}
			existingExact++
		case err == nil:
			// A later exact projection snapshot may legitimately present the
			// same memory again. Its old intent is retained for audit/replay but
			// must not pin or mislabel the new recovery decision.
		case errors.Is(err, sql.ErrNoRows):
		default:
			return 0, fmt.Errorf("read legacy recovery assignment %s: %w", id, err)
		}
	}
	if existingExact == len(ids) {
		return len(ids), nil
	}
	if existingExact != 0 {
		return 0, ErrLegacyMemoryRecoverySnapshotChanged
	}
	if err := validateLegacyRecoveryQueueTxn(ctx, txn, expectedRevision, expectedCount); err != nil {
		return 0, err
	}
	for _, id := range ids {
		reason, evidenceOK, evidenceErr := s.legacyMemoryRecoveryAssignmentEvidenceTxn(
			ctx, txn, id, expectedRevision,
		)
		if evidenceErr != nil {
			return 0, evidenceErr
		}
		if !evidenceOK {
			return 0, ErrLegacyMemoryRecoverySnapshotChanged
		}
		if _, err := txn.ExecContext(ctx, `INSERT INTO legacy_memory_recovery_assignment (
			memory_id, target_agent_id, machine_reason, projection_revision, authorized_by
		) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(memory_id) DO UPDATE SET
			target_agent_id = excluded.target_agent_id,
			machine_reason = excluded.machine_reason,
			projection_revision = excluded.projection_revision,
			authorized_by = excluded.authorized_by,
			created_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')`,
			id, targetAgentID, reason, expectedRevision, authorizedBy); err != nil {
			return 0, fmt.Errorf("record legacy recovery assignment %s: %w", id, err)
		}
	}
	if err := txn.Commit(); err != nil {
		return 0, fmt.Errorf("commit legacy recovery assignment: %w", err)
	}
	return len(ids), nil
}

// legacyMemoryRecoveryAssignmentEvidenceTxn re-verifies the same immutable
// evidence used by the app-v26 adoption worker while the exact recovery queue
// snapshot is locked. Root authority may choose a current policy principal;
// it cannot turn corrupt, incomplete, deprecated, or otherwise conflicting
// historical evidence into an assignable record by crafting the request.
func (s *SQLiteStore) legacyMemoryRecoveryAssignmentEvidenceTxn(
	ctx context.Context,
	txn *sql.Tx,
	memoryID string,
	expectedRevision uint64,
) (string, bool, error) {
	var (
		reason, storedContent, status, domain, author string
		contentHash                                   []byte
		classification                                int64
	)
	err := txn.QueryRowContext(ctx, `SELECT r.machine_reason, m.content,
		m.content_hash, m.status, m.domain_tag, m.submitting_agent,
		m.classification
		FROM legacy_memory_recovery r
		JOIN memories m ON m.memory_id = r.memory_id
		WHERE r.memory_id = ? AND r.resolved_at IS NULL
			AND r.projection_revision = ?`, memoryID, expectedRevision).Scan(
		&reason, &storedContent, &contentHash, &status, &domain, &author,
		&classification,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read legacy recovery assignment evidence %s: %w", memoryID, err)
	}
	if reason != "author_identity_unresolved" ||
		(status != string(memory.StatusProposed) && status != string(memory.StatusCommitted)) ||
		strings.TrimSpace(domain) == "" || strings.TrimSpace(author) == "" ||
		classification < int64(ClearancePublic) ||
		classification > int64(ClearanceTopSecret) || len(contentHash) != sha256.Size {
		return reason, false, nil
	}
	plainContent, decryptErr := s.decryptContent(storedContent)
	if decryptErr != nil ||
		(strings.HasPrefix(storedContent, encPrefix) && plainContent == VaultLockedPlaceholder) {
		return reason, false, nil
	}
	digest := sha256.Sum256([]byte(plainContent))
	return reason, bytes.Equal(digest[:], contentHash), nil
}

func validateLegacyRecoveryQueueTxn(
	ctx context.Context, txn *sql.Tx, expectedRevision uint64, expectedCount int,
) error {
	var count, other int
	if err := txn.QueryRowContext(ctx, `SELECT COUNT(*) FROM legacy_memory_recovery
		WHERE resolved_at IS NULL AND projection_revision = ?`, expectedRevision).Scan(&count); err != nil {
		return fmt.Errorf("count legacy recovery inventory: %w", err)
	}
	if err := txn.QueryRowContext(ctx, `SELECT COUNT(*) FROM legacy_memory_recovery
		WHERE resolved_at IS NULL AND projection_revision != ?`, expectedRevision).Scan(&other); err != nil {
		return fmt.Errorf("check legacy recovery inventory revision: %w", err)
	}
	if count != expectedCount || other != 0 {
		return ErrLegacyMemoryRecoverySnapshotChanged
	}
	return nil
}

// DeprecateLegacyMemoryRecoverySelection records dispositions for an exact
// selected subset. Already-assigned records cannot be deprecated by a stale
// view. Memory rows and canonical history remain unchanged.
func (s *SQLiteStore) DeprecateLegacyMemoryRecoverySelection(
	ctx context.Context,
	expectedRevision uint64,
	expectedCount int,
	memoryIDs []string,
	authorizedBy string,
) (int, error) {
	ids, err := canonicalRecoverySelection(memoryIDs, maxLegacyMemoryRecoveryDispositionIDBytes)
	if err != nil || expectedRevision == 0 || expectedCount <= 0 || strings.TrimSpace(authorizedBy) == "" {
		return 0, ErrLegacyMemoryRecoverySnapshotChanged
	}
	if ensureErr := s.ensureLegacyMemoryRecoveryAssignmentTable(ctx); ensureErr != nil {
		return 0, ensureErr
	}
	txn, unlock, err := s.beginTxLocked(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin selected legacy recovery deprecation: %w", err)
	}
	defer unlock()
	defer func() { _ = txn.Rollback() }()

	existingExact := 0
	for _, id := range ids {
		var disposition string
		var revision int64
		err := txn.QueryRowContext(ctx, `SELECT disposition, projection_revision
			FROM legacy_memory_recovery_disposition WHERE memory_id = ?`, id).
			Scan(&disposition, &revision)
		switch {
		case err == nil && disposition == "deprecated" && revision == int64(expectedRevision):
			existingExact++
		case err == nil:
			return 0, ErrLegacyMemoryRecoverySnapshotChanged
		case errors.Is(err, sql.ErrNoRows):
		default:
			return 0, fmt.Errorf("read legacy recovery disposition %s: %w", id, err)
		}
	}
	if existingExact == len(ids) {
		return len(ids), nil
	}
	if existingExact != 0 {
		return 0, ErrLegacyMemoryRecoverySnapshotChanged
	}
	if err := validateLegacyRecoveryQueueTxn(ctx, txn, expectedRevision, expectedCount); err != nil {
		return 0, err
	}
	for _, id := range ids {
		var reason string
		if err := txn.QueryRowContext(ctx, `SELECT machine_reason FROM legacy_memory_recovery
			WHERE memory_id = ? AND resolved_at IS NULL AND projection_revision = ?`,
			id, expectedRevision).Scan(&reason); err != nil {
			return 0, ErrLegacyMemoryRecoverySnapshotChanged
		}
		var assigned int
		if err := txn.QueryRowContext(ctx, `SELECT COUNT(*) FROM legacy_memory_recovery_assignment
			WHERE memory_id = ? AND projection_revision = ?
				AND machine_reason = ?`, id, expectedRevision, reason).Scan(&assigned); err != nil || assigned != 0 {
			return 0, ErrLegacyMemoryRecoverySnapshotChanged
		}
		if _, err := txn.ExecContext(ctx, `INSERT INTO legacy_memory_recovery_disposition (
			memory_id, disposition, machine_reason, projection_revision, authorized_by
		) VALUES (?, 'deprecated', ?, ?, ?)`, id, reason, expectedRevision, authorizedBy); err != nil {
			return 0, fmt.Errorf("record selected legacy recovery disposition %s: %w", id, err)
		}
		result, resolveErr := txn.ExecContext(ctx, `UPDATE legacy_memory_recovery
			SET resolved_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
			WHERE projection_revision = ? AND memory_id = ?`, expectedRevision, id)
		if resolveErr != nil {
			return 0, fmt.Errorf("resolve selected legacy recovery row %s: %w", id, resolveErr)
		}
		resolved, rowsErr := result.RowsAffected()
		if rowsErr != nil || resolved != 1 {
			return 0, ErrLegacyMemoryRecoverySnapshotChanged
		}
	}
	if err := updateLegacyRecoveryProgressTxn(ctx, txn, expectedRevision, expectedCount, len(ids)); err != nil {
		return 0, err
	}
	if err := txn.Commit(); err != nil {
		return 0, fmt.Errorf("commit selected legacy recovery deprecation: %w", err)
	}
	return len(ids), nil
}

func updateLegacyRecoveryProgressTxn(
	ctx context.Context, txn *sql.Tx, revision uint64, expectedCount, resolved int,
) error {
	remaining := expectedCount - resolved
	if remaining < 0 {
		return ErrLegacyMemoryRecoverySnapshotChanged
	}
	state := "recovery"
	message := "Preserved historical records still require CEREBRUM review."
	if remaining == 0 {
		state = "complete"
		message = "Memory upgrade complete. Preserved historical records were explicitly retired by CEREBRUM Root."
	}
	result, err := txn.ExecContext(ctx, `UPDATE legacy_memory_adoption_progress
		SET state = ?, remaining = 0, recovery = ?, message = ?,
		    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		WHERE singleton = 1 AND state = 'recovery' AND remaining = 0
		  AND recovery = ? AND projection_revision = ?`,
		state, remaining, message, expectedCount, revision)
	if err != nil {
		return fmt.Errorf("update selected legacy recovery progress: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("verify selected legacy recovery progress: %w", err)
	}
	if rows != 1 {
		return ErrLegacyMemoryRecoverySnapshotChanged
	}
	return nil
}
