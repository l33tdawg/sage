package store

// Keyset paging for the memory auto-voter's pending scan.
//
// The voter polls every 2 s. With OFFSET paging and no index that matches its
// ORDER BY, each page re-sorted the whole proposed backlog in a temp B-tree,
// and each later page re-read every row before it: a backlog of a few hundred
// proposed rows cost tens of MB of page reads per poll on an idle node. The
// (status, created_at, memory_id) index serves the ORDER BY, and the
// (created_at, memory_id) cursor makes each page read only its own rows.

import (
	"context"
	"fmt"

	"github.com/l33tdawg/sage/internal/memory"
)

func (s *SQLiteStore) migrateVoterPendingIndex(ctx context.Context) error {
	if _, err := s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_memories_status_created_id
		ON memories(status, created_at, memory_id)`); err != nil {
		return fmt.Errorf("create voter pending index: %w", err)
	}
	return nil
}

// GetVotablePendingByDomainAfter is GetVotablePendingByDomainPage with a
// keyset cursor instead of an offset: it returns up to limit votable proposed
// rows ordered by (created_at, memory_id) strictly after (afterCreatedAt,
// afterID). Empty strings start from the beginning. cursors[i] is the stored
// created_at of rows[i], to pass back as afterCreatedAt; it is the raw column
// value, so the comparison never depends on re-formatting a parsed time.
func (s *SQLiteStore) GetVotablePendingByDomainAfter(
	ctx context.Context,
	domainTag string,
	limit int,
	afterCreatedAt, afterID string,
) (rows []*memory.MemoryRecord, cursors []string, err error) {
	if limit <= 0 {
		limit = 20
	}
	q, err := s.conn.QueryContext(ctx,
		`SELECT m.memory_id, m.submitting_agent, m.content, m.content_hash,
			m.memory_type, m.domain_tag, m.confidence_score, m.status, m.created_at
		FROM memories AS m
		WHERE m.status = 'proposed' AND m.domain_tag LIKE ?
		  AND (m.created_at, m.memory_id) > (?, ?)
		  AND NOT EXISTS (
			SELECT 1 FROM legacy_memory_recovery AS r
			WHERE r.memory_id = m.memory_id AND r.resolved_at IS NULL
		  )
		ORDER BY m.created_at ASC, m.memory_id ASC LIMIT ?`,
		domainTag, afterCreatedAt, afterID, limit,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("get votable pending after cursor: %w", err)
	}
	defer func() { _ = q.Close() }()

	rows = make([]*memory.MemoryRecord, 0, limit)
	cursors = make([]string, 0, limit)
	for q.Next() {
		var r memory.MemoryRecord
		var mt, st, createdAt string
		if scanErr := q.Scan(
			&r.MemoryID,
			&r.SubmittingAgent,
			&r.Content,
			&r.ContentHash,
			&mt,
			&r.DomainTag,
			&r.ConfidenceScore,
			&st,
			&createdAt,
		); scanErr != nil {
			return nil, nil, fmt.Errorf("scan votable pending after cursor: %w", scanErr)
		}
		r.MemoryType = memory.MemoryType(mt)
		r.Status = memory.MemoryStatus(st)
		r.CreatedAt = parseTime(createdAt)
		rows = append(rows, &r)
		cursors = append(cursors, createdAt)
	}
	if rowsErr := q.Err(); rowsErr != nil {
		return nil, nil, fmt.Errorf("get votable pending after cursor rows: %w", rowsErr)
	}
	return rows, cursors, nil
}
