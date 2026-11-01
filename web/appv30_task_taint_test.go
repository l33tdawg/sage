package web

import (
	"context"
	"crypto/sha256"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestAgentTaskRecordTaintedIsDefinitiveDenial(t *testing.T) {
	badger, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, badger.CloseBadger()) })

	agentID := strings.Repeat("a", 64)
	domain := "general"
	content := "tainted assigned task"
	contentHash := sha256.Sum256([]byte(content))
	require.NoError(t, badger.SetMemoryHash(
		"tainted-task",
		contentHash[:],
		string(memory.StatusCommitted),
	))
	require.NoError(t, badger.SetMemoryDomain("tainted-task", domain))
	require.NoError(t, badger.SetMemoryAuthor("tainted-task", agentID))
	require.NoError(t, badger.SetMemoryAuthorPrincipal("tainted-task", agentID))
	require.NoError(t, badger.SetMemoryClassification(
		"tainted-task",
		uint8(store.ClearancePublic),
	))
	created, err := badger.ApplyTaintDecision(
		"sage",
		strings.Repeat("b", taint.DecisionIDBytes),
		tx.ParentRef{
			SourceChainID: "sage",
			MemoryID:      "tainted-task",
			ContentHash:   contentHash,
		},
		sha256.Sum256([]byte("taint task payload")),
	)
	require.NoError(t, err)
	require.True(t, created)

	h := &DashboardHandler{
		BadgerStore:    badger,
		AppV23ActiveFn: func() bool { return true },
	}
	task := &memory.MemoryRecord{
		MemoryID:        "tainted-task",
		SubmittingAgent: agentID,
		Content:         content,
		ContentHash:     contentHash[:],
		DomainTag:       domain,
		Status:          memory.StatusCommitted,
	}
	allowed, definitive := h.agentTaskRecordReadDecision(
		context.Background(),
		agentID,
		task,
	)
	require.False(t, allowed)
	require.True(t, definitive)
}

func TestAppV30TaskTaintFilterRequiresFreshMirrorRevision(t *testing.T) {
	ctx := context.Background()
	badger, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, badger.CloseBadger()) })
	sqlite, err := store.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "task-taint.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlite.Close()) })

	decisionID := strings.Repeat("c", taint.DecisionIDBytes)
	created, err := badger.ApplyTaintDecision(
		"sage",
		decisionID,
		tx.ParentRef{SourceChainID: "sage", MemoryID: "freshness-task"},
		sha256.Sum256([]byte("freshness payload")),
	)
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, sqlite.ReplaceMemoryTaintMirror(ctx, 1, []string{"freshness-task"}))

	h := &DashboardHandler{BadgerStore: badger, store: sqlite}
	require.True(t, h.appV30TaskTaintFilterEnabled(ctx))

	changed, err := badger.ClearTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, changed)
	require.False(t, h.appV30TaskTaintFilterEnabled(ctx))
}
