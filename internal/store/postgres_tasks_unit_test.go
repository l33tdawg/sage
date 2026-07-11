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
	require.Contains(t, postgresInsertMemorySQL, "parent_hash, task_status, created_at")
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
		CreatedAt:       createdAt,
	}

	mock.ExpectExec(regexp.QuoteMeta(postgresInsertMemorySQL)).
		WithArgs(
			record.MemoryID, record.SubmittingAgent, record.Content, record.ContentHash,
			pgxmock.AnyArg(), record.EmbeddingHash, string(record.MemoryType), record.DomainTag,
			record.Provider, record.ConfidenceScore, string(record.Status), record.ParentHash,
			string(record.TaskStatus), record.CreatedAt,
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
		"created_at", "committed_at", "deprecated_at",
	}
	rows := pgxmock.NewRows(columns).AddRow(
		"historical-task", "agent-1", "[TASK] Historical work", []byte("content-hash"),
		string(memory.TypeTask), "work", "codex", 0.9, string(memory.StatusCommitted),
		nil, "", "agent-2", "agent-2", nil, createdAt, nil, nil,
	)
	mock.ExpectQuery(`(?s)SELECT .*FROM memories.*domain_tag = \$1.*ELSE 0 END, created_at DESC.*LIMIT \$2`).
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
	for _, stmt := range postgresTaskAssignmentSchema {
		mock.ExpectExec(regexp.QuoteMeta(stmt)).WillReturnResult(pgxmock.NewResult("DDL", 0))
	}
	mock.ExpectExec(`UPDATE memories SET task_assignment_version = 1`).
		WillReturnResult(pgxmock.NewResult("UPDATE", 0))
	mock.ExpectExec(`UPDATE memories SET task_requires_handoff = TRUE`).
		WillReturnResult(pgxmock.NewResult("UPDATE", 0))
	mock.ExpectExec(`INSERT INTO agent_notifications`).
		WillReturnResult(pgxmock.NewResult("INSERT", 0))

	require.NoError(t, store.ensureMemoriesSchema(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresClaimTaskBindsActiveAgentAndOwnership(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	store := &PostgresStore{db: mock}
	mock.ExpectExec(`(?s)UPDATE memories m.*SET assignee = \$2.*FROM agents a.*m\.task_requires_handoff = FALSE.*a\.agent_id = \$2`).
		WithArgs("task-1", "agent-1").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	claimed, err := store.ClaimTask(context.Background(), "task-1", "agent-1")
	require.NoError(t, err)
	require.True(t, claimed)
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
