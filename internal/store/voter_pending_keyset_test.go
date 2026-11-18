package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
)

// The keyset scan walks the same total order as the offset scan, skips
// unresolved recovery rows, and resumes strictly after the cursor.
func TestGetVotablePendingByDomainAfter_KeysetMatchesOffsetOrder(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	at := time.Unix(1_700_000_000, 0).UTC()
	// 25 proposed rows over 3 timestamps, inserted out of order, so both the
	// created_at order and the memory_id tiebreak matter.
	for i := 24; i >= 0; i-- {
		rec := testMemory(fmt.Sprintf("m%02d", i), "agent1", fmt.Sprintf("content %d", i), "general")
		rec.CreatedAt = at.Add(time.Duration(i/10) * time.Second)
		require.NoError(t, s.InsertMemory(ctx, rec))
	}
	_, err := s.conn.ExecContext(ctx, `INSERT INTO legacy_memory_recovery (memory_id, machine_reason, projection_revision)
		VALUES ('m07', 'test', 1), ('m08', 'test', 1)`)
	require.NoError(t, err)
	_, err = s.conn.ExecContext(ctx, `UPDATE legacy_memory_recovery SET resolved_at = 'x' WHERE memory_id = 'm08'`)
	require.NoError(t, err)

	var want []string
	for off := 0; ; off += 4 {
		page, pErr := s.GetVotablePendingByDomainPage(ctx, "%", 4, off)
		require.NoError(t, pErr)
		for _, r := range page {
			want = append(want, r.MemoryID)
		}
		if len(page) < 4 {
			break
		}
	}
	require.Len(t, want, 24, "m07 is held by an unresolved recovery row; m08's row is resolved")

	var got []string
	ca, id := "", ""
	for {
		page, cursors, pErr := s.GetVotablePendingByDomainAfter(ctx, "%", 4, ca, id)
		require.NoError(t, pErr)
		require.Len(t, cursors, len(page))
		if len(page) == 0 {
			break
		}
		for _, r := range page {
			got = append(got, r.MemoryID)
		}
		ca, id = cursors[len(cursors)-1], page[len(page)-1].MemoryID
	}
	require.Equal(t, want, got, "keyset and offset scans must visit the same rows in the same order")

	// Resume from the middle of a same-timestamp run.
	page, _, err := s.GetVotablePendingByDomainAfter(ctx, "%", 3, cursorOf(t, s, "m12"), "m12")
	require.NoError(t, err)
	require.Equal(t, []string{"m13", "m14", "m15"}, ids(page))
	// Domain filter still applies.
	page, _, err = s.GetVotablePendingByDomainAfter(ctx, "other", 10, "", "")
	require.NoError(t, err)
	require.Empty(t, page)
}

func cursorOf(t *testing.T, s *SQLiteStore, id string) string {
	t.Helper()
	var ca string
	require.NoError(t, s.conn.QueryRowContext(context.Background(), `SELECT created_at FROM memories WHERE memory_id = ?`, id).Scan(&ca))
	return ca
}

func ids(rs []*memory.MemoryRecord) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.MemoryID
	}
	return out
}

// The keyset query is served by the (status, created_at, memory_id) index, so
// SQLite neither sorts the backlog in a temp B-tree nor reads rows before the
// cursor; and connections get the larger page cache.
func TestVoterPendingScan_IndexedAndCached(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	rows, err := s.conn.QueryContext(ctx, `EXPLAIN QUERY PLAN
		SELECT m.memory_id FROM memories AS m
		WHERE m.status = 'proposed' AND m.domain_tag LIKE ?
		  AND (m.created_at, m.memory_id) > (?, ?)
		  AND NOT EXISTS (SELECT 1 FROM legacy_memory_recovery AS r
			WHERE r.memory_id = m.memory_id AND r.resolved_at IS NULL)
		ORDER BY m.created_at ASC, m.memory_id ASC LIMIT ?`, "%", "", "", 128)
	require.NoError(t, err)
	var plan []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &notused, &detail))
		plan = append(plan, detail)
	}
	require.NoError(t, rows.Err())
	_ = rows.Close()
	joined := strings.Join(plan, " | ")
	require.Contains(t, joined, "idx_memories_status_created_id", joined)
	require.NotContains(t, joined, "TEMP B-TREE", joined)

	var cache int
	require.NoError(t, s.conn.QueryRowContext(ctx, `PRAGMA cache_size`).Scan(&cache))
	require.Equal(t, -16384, cache)
}
