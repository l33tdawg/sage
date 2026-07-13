package store

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
)

func TestPostgresInsertMemoryPersistsTaskStatusWithoutReplayOverwrite(t *testing.T) {
	require.Contains(t, postgresInsertMemorySQL, "parent_hash, task_status, assignee, created_at")
	require.NotContains(t, postgresInsertMemorySQL, "task_status =")

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	store := &PostgresStore{db: mock}
	createdAt := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	record := &memory.MemoryRecord{
		MemoryID:        "task-1",
		SubmittingAgent: "agent-1",
		Content:         "[TASK] Verify the board",
		ContentHash:     []byte("content-hash"),
		MemoryType:      memory.TypeTask,
		DomainTag:       "work",
		Provider:        "codex",
		ConfidenceScore: 0.9,
		Status:          memory.StatusCommitted,
		ParentHash:      "parent-hash",
		TaskStatus:      memory.TaskStatusDone,
		Assignee:        "agent-1",
		CreatedAt:       createdAt,
	}

	mock.ExpectExec(regexp.QuoteMeta(postgresInsertMemorySQL)).
		WithArgs(
			record.MemoryID, record.SubmittingAgent, record.Content, record.ContentHash,
			pgxmock.AnyArg(), record.EmbeddingHash, string(record.MemoryType), record.DomainTag,
			record.Provider, record.EmbeddingProvider, record.ConfidenceScore, string(record.Status), record.ParentHash,
			string(record.TaskStatus), record.Assignee, record.CreatedAt,
		).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	require.NoError(t, store.InsertMemory(context.Background(), record))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresGetAllTasksClampsLimitAndReturnsUnclassifiedHistory(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	store := &PostgresStore{db: mock}
	createdAt := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	columns := []string{
		"memory_id", "submitting_agent", "content", "content_hash", "memory_type",
		"domain_tag", "provider", "confidence_score", "status", "parent_hash",
		"task_status", "assignee", "task_picked_up_by", "task_picked_up_at",
		"task_status_updated_at",
		"created_at", "committed_at", "deprecated_at",
	}
	rows := pgxmock.NewRows(columns).AddRow(
		"historical-task", "agent-1", "[TASK] Historical work", []byte("content-hash"),
		string(memory.TypeTask), "work", "codex", 0.9, string(memory.StatusCommitted),
		nil, "", "agent-2", "agent-2", nil, nil, createdAt, nil, nil,
	)
	mock.ExpectQuery(`(?s)SELECT .*FROM memories.*domain_tag = \$1.*ELSE 0 END,.*task_board_position ASC, created_at DESC.*LIMIT \$2`).
		WithArgs("work", 500).
		WillReturnRows(rows).
		RowsWillBeClosed()

	tasks, err := store.GetAllTasks(context.Background(), "work", 10_000)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, "historical-task", tasks[0].MemoryID)
	require.Equal(t, memory.TaskStatus(""), tasks[0].TaskStatus)
	require.Equal(t, "work", tasks[0].DomainTag)
	require.Equal(t, "agent-2", tasks[0].Assignee)
	require.Equal(t, "agent-2", tasks[0].TaskPickedUpBy)
	require.Nil(t, tasks[0].TaskPickedUpAt)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresReorderTasksPersistsRequestedThenRemainingCards(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	store := &PostgresStore{db: mock}
	mock.ExpectQuery(`(?s)SELECT memory_id::text FROM memories.*task_status = \$1.*task_board_position ASC`).
		WithArgs(string(memory.TaskStatusDone)).
		WillReturnRows(pgxmock.NewRows([]string{"memory_id"}).AddRow("done-a").AddRow("done-b").AddRow("done-c"))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE memories SET task_board_position = $2 WHERE memory_id = $1`)).
		WithArgs("done-c", 1).WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE memories SET task_board_position = $2 WHERE memory_id = $1`)).
		WithArgs("done-a", 2).WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE memories SET task_board_position = $2 WHERE memory_id = $1`)).
		WithArgs("done-b", 3).WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	require.NoError(t, store.ReorderTasks(context.Background(), memory.TaskStatusDone, []string{"done-c", "done-a"}))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresTaskBoardQueryDoesNotGuessHistoricalStatus(t *testing.T) {
	require.Contains(t, postgresInsertMemorySQL, "task_status")
	require.False(t, strings.Contains(strings.ToLower(postgresInsertMemorySQL), "task_status = excluded.task_status"))
}

func TestPostgresGetMemoryClassificationLocal(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	store := &PostgresStore{db: mock}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT classification FROM memories WHERE memory_id = $1`)).
		WithArgs("task-1").
		WillReturnRows(pgxmock.NewRows([]string{"classification"}).AddRow(2))

	classification, err := store.GetMemoryClassificationLocal(context.Background(), "task-1")
	require.NoError(t, err)
	require.Equal(t, 2, classification)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresEnsureMemoriesSchemaRepairsTaskStatusAndClassification(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	store := &PostgresStore{db: mock}
	mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock($1)`)).
		WithArgs(memoriesSchemaLockKey).
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
	mock.ExpectExec(regexp.QuoteMeta(`ALTER TABLE memories ADD COLUMN IF NOT EXISTS task_status TEXT DEFAULT '' CHECK (task_status IN ('', 'planned', 'in_progress', 'done', 'dropped'))`)).
		WillReturnResult(pgxmock.NewResult("ALTER TABLE", 0))
	mock.ExpectExec(regexp.QuoteMeta(`ALTER TABLE memories ADD COLUMN IF NOT EXISTS classification SMALLINT NOT NULL DEFAULT 1`)).
		WillReturnResult(pgxmock.NewResult("ALTER TABLE", 0))
	mock.ExpectExec(regexp.QuoteMeta(`ALTER TABLE memories ADD COLUMN IF NOT EXISTS embedding_provider TEXT NOT NULL DEFAULT ''`)).
		WillReturnResult(pgxmock.NewResult("ALTER TABLE", 0))
	mock.ExpectExec(regexp.QuoteMeta(`CREATE INDEX IF NOT EXISTS idx_memories_embedding_provider ON memories (embedding_provider)`)).
		WillReturnResult(pgxmock.NewResult("CREATE INDEX", 0))
	for _, stmt := range postgresTaskAssignmentSchema {
		mock.ExpectExec(regexp.QuoteMeta(stmt)).WillReturnResult(pgxmock.NewResult("DDL", 0))
	}
	mock.ExpectExec(`UPDATE memories SET task_assignment_version = 1`).
		WillReturnResult(pgxmock.NewResult("UPDATE", 0))
	mock.ExpectExec(`UPDATE memories SET task_requires_handoff = TRUE`).
		WillReturnResult(pgxmock.NewResult("UPDATE", 0))
	mock.ExpectExec(`UPDATE memories SET assignee = CASE`).
		WillReturnResult(pgxmock.NewResult("UPDATE", 0))
	mock.ExpectExec(`UPDATE memories SET task_status_updated_at = NOW\(\)`).
		WillReturnResult(pgxmock.NewResult("UPDATE", 0))
	mock.ExpectExec(`UPDATE memories SET task_status = 'planned'`).
		WillReturnResult(pgxmock.NewResult("UPDATE", 0))
	mock.ExpectExec(`INSERT INTO agent_notifications`).
		WillReturnResult(pgxmock.NewResult("INSERT", 0))

	require.NoError(t, store.ensureMemoriesSchema(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresEmbeddingRepairOperations(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	store := &PostgresStore{db: mock}

	mock.ExpectQuery(`SELECT COALESCE\(embedding_provider, ''\), COUNT\(\*\) FROM memories`).
		WillReturnRows(pgxmock.NewRows([]string{"embedding_provider", "count"}).
			AddRow("ollama", 7).AddRow("", 2))
	counts, err := store.CountMemoriesByProvider(context.Background())
	require.NoError(t, err)
	require.Equal(t, map[string]int{"ollama": 7, "": 2}, counts)

	mock.ExpectQuery(`(?s)SELECT memory_id, content FROM memories.*embedding_provider.*LIMIT \$2`).
		WithArgs("ollama", 50).
		WillReturnRows(pgxmock.NewRows([]string{"memory_id", "content"}).AddRow("mem-1", "legacy content"))
	items, err := store.ListMemoriesForReembed(context.Background(), "ollama", 50)
	require.NoError(t, err)
	require.Equal(t, []ReembedItem{{MemoryID: "mem-1", Content: "legacy content", Decryptable: true}}, items)

	mock.ExpectExec(`UPDATE memories SET embedding = \$2, embedding_provider = \$3 WHERE memory_id = \$1`).
		WithArgs("mem-1", pgxmock.AnyArg(), "ollama").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	require.NoError(t, store.UpdateMemoryEmbedding(context.Background(), "mem-1", []float32{0.1, 0.2}, "ollama"))

	mock.ExpectExec(`UPDATE memories SET embedding_provider = 'error' WHERE memory_id = \$1`).
		WithArgs("mem-2").WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	require.NoError(t, store.MarkMemoryEmbeddingError(context.Background(), "mem-2"))

	mock.ExpectExec(`UPDATE memories SET embedding_provider = '' WHERE embedding_provider = 'error'`).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	reset, err := store.ResetErroredEmbeddings(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, reset)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresClaimTaskBindsActiveAgentAndOwnership(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	store := &PostgresStore{db: mock}
	mock.ExpectExec(`(?s)UPDATE memories m.*SET assignee = \$2.*FROM agents a.*m\.task_requires_handoff = FALSE.*m\.assignee = \$2.*a\.agent_id = \$2`).
		WithArgs("task-1", "agent-1").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	claimed, err := store.ClaimTask(context.Background(), "task-1", "agent-1")
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresGetOpenTasksUsesExactAssigneeNotProvider(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	store := &PostgresStore{db: mock}
	createdAt := time.Date(2026, 7, 12, 0, 0, 0, 0, time.UTC)
	columns := []string{
		"memory_id", "submitting_agent", "content", "content_hash", "memory_type",
		"domain_tag", "provider", "confidence_score", "status", "parent_hash",
		"task_status", "assignee", "task_picked_up_by", "task_picked_up_at",
		"task_status_updated_at",
		"created_at", "committed_at", "deprecated_at",
	}
	rows := pgxmock.NewRows(columns).AddRow(
		"task-a", "author", "[TASK] assigned", []byte("hash"), string(memory.TypeTask),
		"tii-sage", "other-provider", 0.9, string(memory.StatusCommitted), nil,
		string(memory.TaskStatusPlanned), "agent-a", "", nil, nil, createdAt, nil, nil,
	)
	mock.ExpectQuery(`(?s)FROM memories.*domain_tag = \$1.*AND assignee = \$2.*ORDER BY`).
		WithArgs("tii-sage", "agent-a").WillReturnRows(rows)

	tasks, err := store.GetOpenTasks(context.Background(), "tii-sage", "spoofed-provider", "agent-a")
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, "agent-a", tasks[0].Assignee)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresAcknowledgeLocksTaskBeforeNotice(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	store := &PostgresStore{db: mock}
	mock.ExpectQuery(`(?s)SELECT n\.notification_id.*FOR UPDATE OF m`).
		WithArgs("notice-1", "agent-1").
		WillReturnRows(pgxmock.NewRows([]string{"notification_id"}).AddRow("notice-1"))
	mock.ExpectExec(`UPDATE agent_notifications SET state = 'read'`).
		WithArgs("notice-1", "agent-1").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	acknowledged, err := store.AcknowledgeAgentNotifications(context.Background(), "agent-1", []string{"notice-1"})
	require.NoError(t, err)
	require.Equal(t, []string{"notice-1"}, acknowledged)
	require.NoError(t, mock.ExpectationsWereMet())
}
