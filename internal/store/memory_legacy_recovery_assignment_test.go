package store

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestCanonicalRecoveryAssignmentSelectionUsesAdoptionWireIDLimit(t *testing.T) {
	accepted := strings.Repeat("a", tx.MaxMemoryLegacyAdoptionIDBytes)
	selection, err := canonicalRecoverySelection(
		[]string{accepted}, tx.MaxMemoryLegacyAdoptionIDBytes,
	)
	require.NoError(t, err)
	require.Equal(t, []string{accepted}, selection)

	rejected := strings.Repeat("b", tx.MaxMemoryLegacyAdoptionIDBytes+1)
	_, err = canonicalRecoverySelection(
		[]string{rejected}, tx.MaxMemoryLegacyAdoptionIDBytes,
	)
	require.EqualError(t, err, "legacy recovery memory id length 513 exceeds 512 bytes")
}

func TestLegacyRecoveryOversizedAdoptionIDCanStillBeDeprecated(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "memories.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	memoryID := strings.Repeat("x", tx.MaxMemoryLegacyAdoptionIDBytes+1)
	content := "unadoptable historical record"
	digest := sha256.Sum256([]byte(content))
	require.NoError(t, s.InsertMemory(ctx, &memory.MemoryRecord{
		MemoryID: memoryID, SubmittingAgent: "historical-label",
		Content: content, ContentHash: digest[:], MemoryType: memory.TypeFact,
		DomainTag: "historical/domain", Status: memory.StatusCommitted,
	}))
	revision, err := s.MemoryProjectionRevision(ctx)
	require.NoError(t, err)
	require.NoError(t, s.SyncLegacyMemoryRecoveryQueue(ctx, revision, []LegacyMemoryRecoveryItem{{
		MemoryID: memoryID, Reason: "author_identity_unresolved",
	}}))
	require.NoError(t, s.PublishLegacyMemoryAdoptionProgress(ctx, LegacyMemoryAdoptionProgress{
		State: "recovery", Discovered: 1, Recovery: 1, Revision: revision,
	}))

	_, err = s.AssignLegacyMemoryRecoverySelection(
		ctx, revision, 1, []string{memoryID}, "active-agent", "root",
	)
	require.ErrorIs(t, err, ErrLegacyMemoryRecoverySnapshotChanged)
	deprecated, err := s.DeprecateLegacyMemoryRecoverySelection(
		ctx, revision, 1, []string{memoryID}, "root",
	)
	require.NoError(t, err)
	require.Equal(t, 1, deprecated)
	progress, err := s.GetLegacyMemoryAdoptionProgress(ctx)
	require.NoError(t, err)
	require.Zero(t, progress.Recovery)
}

func TestLegacyRecoveryAssignmentIsContentFreeCASProtectedAndIdempotent(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "memories.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	content := "verified historical content"
	digest := sha256.Sum256([]byte(content))
	require.NoError(t, s.InsertMemory(ctx, &memory.MemoryRecord{
		MemoryID: "assignable", SubmittingAgent: "historical-label",
		Content: content, ContentHash: digest[:], MemoryType: memory.TypeFact,
		DomainTag: "historical/domain", Status: memory.StatusCommitted,
	}))
	require.NoError(t, s.InsertMemory(ctx, &memory.MemoryRecord{
		MemoryID: "corrupt", SubmittingAgent: "historical-label",
		Content: "bad", ContentHash: make([]byte, sha256.Size), MemoryType: memory.TypeFact,
		DomainTag: "historical/domain", Status: memory.StatusCommitted,
	}))
	revision, err := s.MemoryProjectionRevision(ctx)
	require.NoError(t, err)
	require.NoError(t, s.SyncLegacyMemoryRecoveryQueue(ctx, revision, []LegacyMemoryRecoveryItem{
		{MemoryID: "assignable", Reason: "author_identity_unresolved"},
		// Simulate a crafted Root request whose queue label claims the row is
		// assignable even though the immutable content evidence is corrupt.
		{MemoryID: "corrupt", Reason: "author_identity_unresolved"},
	}))
	require.NoError(t, s.PublishLegacyMemoryAdoptionProgress(ctx, LegacyMemoryAdoptionProgress{
		State: "recovery", Discovered: 2, Recovery: 2, Revision: revision,
	}))

	assigned, err := s.AssignLegacyMemoryRecoverySelection(
		ctx, revision, 2, []string{"assignable"}, "active-agent", "root",
	)
	require.NoError(t, err)
	require.Equal(t, 1, assigned)
	// Exact transport replay is a no-op with the same successful result.
	assigned, err = s.AssignLegacyMemoryRecoverySelection(
		ctx, revision, 2, []string{"assignable"}, "active-agent", "root",
	)
	require.NoError(t, err)
	require.Equal(t, 1, assigned)
	_, err = s.AssignLegacyMemoryRecoverySelection(
		ctx, revision, 2, []string{"assignable"}, "different-agent", "root",
	)
	require.ErrorIs(t, err, ErrLegacyMemoryRecoverySnapshotChanged)
	_, err = s.AssignLegacyMemoryRecoverySelection(
		ctx, revision, 2, []string{"corrupt"}, "active-agent", "root",
	)
	require.ErrorIs(t, err, ErrLegacyMemoryRecoverySnapshotChanged)

	assignments, err := s.ListLegacyMemoryRecoveryAssignments(ctx)
	require.NoError(t, err)
	require.Equal(t, "active-agent", assignments["assignable"].TargetAgentID)
	_, err = s.DeprecateLegacyMemoryRecoverySnapshot(ctx, revision, 2, "root")
	require.ErrorIs(t, err, ErrLegacyMemoryRecoverySnapshotChanged,
		"deprecate-all from a stale view must not retire a queued assignment")
	page, next, err := s.ListLegacyMemoryRecoveryInventoryPage(ctx, "", 100)
	require.NoError(t, err)
	require.Empty(t, next)
	require.Len(t, page, 2)
	require.Equal(t, content, page[0].Content)
	require.Equal(t, "active-agent", page[0].AssignedTarget)

	// A new exact projection snapshot must not show or enforce a stale intent.
	bump := sha256.Sum256([]byte("projection bump"))
	require.NoError(t, s.InsertMemory(ctx, &memory.MemoryRecord{
		MemoryID: "projection-bump", SubmittingAgent: "active-agent",
		Content: "projection bump", ContentHash: bump[:], MemoryType: memory.TypeFact,
		DomainTag: "active/domain", Status: memory.StatusCommitted,
	}))
	newRevision, err := s.MemoryProjectionRevision(ctx)
	require.NoError(t, err)
	require.Greater(t, newRevision, revision)
	require.NoError(t, s.SyncLegacyMemoryRecoveryQueue(ctx, newRevision, []LegacyMemoryRecoveryItem{
		{MemoryID: "assignable", Reason: "author_identity_unresolved"},
		{MemoryID: "corrupt", Reason: "author_identity_unresolved"},
	}))
	require.NoError(t, s.PublishLegacyMemoryAdoptionProgress(ctx, LegacyMemoryAdoptionProgress{
		State: "recovery", Discovered: 2, Recovery: 2, Revision: newRevision,
	}))
	page, _, err = s.ListLegacyMemoryRecoveryInventoryPage(ctx, "", 100)
	require.NoError(t, err)
	require.Empty(t, page[0].AssignedTarget,
		"an assignment from an older projection must not label the new inventory")
	assigned, err = s.AssignLegacyMemoryRecoverySelection(
		ctx, newRevision, 2, []string{"assignable"}, "replacement-agent", "root",
	)
	require.NoError(t, err)
	require.Equal(t, 1, assigned)
	assignments, err = s.ListLegacyMemoryRecoveryAssignments(ctx)
	require.NoError(t, err)
	require.Equal(t, "replacement-agent", assignments["assignable"].TargetAgentID)
	require.Equal(t, newRevision, assignments["assignable"].ProjectionRevision)

	deprecated, err := s.DeprecateLegacyMemoryRecoverySelection(
		ctx, newRevision, 2, []string{"corrupt"}, "root",
	)
	require.NoError(t, err)
	require.Equal(t, 1, deprecated)
	progress, err := s.GetLegacyMemoryAdoptionProgress(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, progress.Recovery)
	require.Equal(t, "recovery", progress.State)
	record, err := s.GetMemory(ctx, "corrupt")
	require.NoError(t, err)
	require.Equal(t, memory.StatusCommitted, record.Status,
		"projection deprecation must not rewrite memory history")
}

func TestLegacyRecoveryInventoryExcludesAlreadyDeprecatedMemories(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "memories.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	content := "already forgotten"
	digest := sha256.Sum256([]byte(content))
	require.NoError(t, s.InsertMemory(ctx, &memory.MemoryRecord{
		MemoryID: "already-deprecated", SubmittingAgent: "retired",
		Content: content, ContentHash: digest[:], MemoryType: memory.TypeFact,
		DomainTag: "historical/domain", Status: memory.StatusDeprecated,
	}))
	revision, err := s.MemoryProjectionRevision(ctx)
	require.NoError(t, err)
	require.NoError(t, s.SyncLegacyMemoryRecoveryQueue(ctx, revision, []LegacyMemoryRecoveryItem{{
		MemoryID: "already-deprecated", Reason: "author_identity_unresolved",
	}}))
	page, _, err := s.ListLegacyMemoryRecoveryInventoryPage(ctx, "", 50)
	require.NoError(t, err)
	require.Empty(t, page)
}

func TestLegacyRecoveryInventoryPagesTenThousandRowsWhileOrdinaryWritesContinue(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "memories.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	const total = 10_001
	err = func() error {
		txn, unlock, beginErr := s.beginTxLocked(ctx)
		if beginErr != nil {
			return beginErr
		}
		defer unlock()
		defer func() { _ = txn.Rollback() }()
		stmt, prepareErr := txn.PrepareContext(ctx, `INSERT INTO memories (
			memory_id, submitting_agent, content, content_hash, memory_type,
			domain_tag, confidence_score, status
		) VALUES (?, ?, ?, ?, 'fact', 'historical/domain', 1, 'committed')`)
		if prepareErr != nil {
			return prepareErr
		}
		defer func() { _ = stmt.Close() }()
		for i := 0; i < total; i++ {
			memoryID := fmt.Sprintf("historical-%05d", i)
			content := "stable historical content " + memoryID
			digest := sha256.Sum256([]byte(content))
			if _, execErr := stmt.ExecContext(
				ctx, memoryID, "retired-agent", content, digest[:],
			); execErr != nil {
				return execErr
			}
		}
		return txn.Commit()
	}()
	require.NoError(t, err)
	revision, err := s.MemoryProjectionRevision(ctx)
	require.NoError(t, err)
	queue := make([]LegacyMemoryRecoveryItem, total)
	for i := range queue {
		queue[i] = LegacyMemoryRecoveryItem{
			MemoryID: fmt.Sprintf("historical-%05d", i),
			Reason:   "author_identity_unresolved",
		}
	}
	require.NoError(t, s.SyncLegacyMemoryRecoveryQueue(ctx, revision, queue))
	require.NoError(t, s.PublishLegacyMemoryAdoptionProgress(ctx, LegacyMemoryAdoptionProgress{
		State: "recovery", Discovered: total, Recovery: total, Revision: revision,
	}))

	seen := 0
	after := ""
	for {
		page, next, pageErr := s.ListLegacyMemoryRecoveryInventoryPage(ctx, after, 100)
		require.NoError(t, pageErr)
		seen += len(page)
		if next == "" {
			break
		}
		require.NotEqual(t, after, next, "inventory pagination must always advance")
		after = next
	}
	require.Equal(t, total, seen)

	selected := make([]string, 256)
	for i := range selected {
		selected[i] = fmt.Sprintf("historical-%05d", i)
	}
	result := make(chan error, 2)
	go func() {
		_, assignErr := s.AssignLegacyMemoryRecoverySelection(
			ctx, revision, total, selected, "replacement-agent", "root",
		)
		result <- assignErr
	}()
	go func() {
		content := "ordinary write during recovery decisions"
		digest := sha256.Sum256([]byte(content))
		result <- s.InsertMemory(ctx, &memory.MemoryRecord{
			MemoryID: "ordinary-current-write", SubmittingAgent: "active-agent",
			Content: content, ContentHash: digest[:], MemoryType: memory.TypeFact,
			DomainTag: "current/domain", Status: memory.StatusCommitted,
		})
	}()
	for i := 0; i < cap(result); i++ {
		require.NoError(t, <-result)
	}
	require.NoError(t, s.ValidateLegacyMemoryRecoverySnapshot(ctx, revision, total),
		"ordinary current writes must not invalidate a stable historical inventory")
	assignments, err := s.ListLegacyMemoryRecoveryAssignments(ctx)
	require.NoError(t, err)
	require.Len(t, assignments, len(selected))
}
