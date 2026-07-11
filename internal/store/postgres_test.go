//go:build integration

package store

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
)

// These tests require a running PostgreSQL instance with pgvector.
// Run with: go test -tags=integration ./internal/store/...

func setupTestStore(t *testing.T) *PostgresStore {
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, agentTestDSN()) // honors SAGE_TEST_POSTGRES_DSN
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	return store
}

func TestInsertGetMemory(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	record := &memory.MemoryRecord{
		MemoryID:        uuid.NewString(), // memory_id is a UUID column
		SubmittingAgent: "agent-test",
		Content:         "test memory content",
		ContentHash:     memory.ComputeContentHash("test memory content"),
		MemoryType:      memory.TypeFact,
		DomainTag:       "crypto",
		ConfidenceScore: 0.85,
		Status:          memory.StatusProposed,
		CreatedAt:       time.Now(),
	}

	err := store.InsertMemory(ctx, record)
	require.NoError(t, err)

	got, err := store.GetMemory(ctx, record.MemoryID)
	require.NoError(t, err)
	assert.Equal(t, record.MemoryID, got.MemoryID)
	assert.Equal(t, record.Content, got.Content)
	assert.Equal(t, record.MemoryType, got.MemoryType)
}

func TestPostgresUpdateStatus(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	id := uuid.NewString()
	record := &memory.MemoryRecord{
		MemoryID:        id,
		SubmittingAgent: "agent-test",
		Content:         "status test",
		ContentHash:     memory.ComputeContentHash("status test"),
		MemoryType:      memory.TypeObservation,
		DomainTag:       "challenge_generation",
		ConfidenceScore: 0.7,
		Status:          memory.StatusProposed,
		CreatedAt:       time.Now(),
	}

	require.NoError(t, store.InsertMemory(ctx, record))
	require.NoError(t, store.UpdateStatus(ctx, id, memory.StatusCommitted, time.Now()))

	got, err := store.GetMemory(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, memory.StatusCommitted, got.Status)
	assert.NotNil(t, got.CommittedAt)
}

func TestPostgresTaskBoardAllStatuses(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()
	domain := "task-board-" + uuid.NewString()
	statuses := []memory.TaskStatus{
		memory.TaskStatusPlanned,
		memory.TaskStatusInProgress,
		memory.TaskStatusDone,
		memory.TaskStatusDropped,
	}
	ids := make(map[string]memory.TaskStatus, len(statuses))
	plannedID := ""
	for _, status := range statuses {
		id := uuid.NewString()
		record := &memory.MemoryRecord{
			MemoryID: id, SubmittingAgent: "agent-test", Content: "[TASK] " + string(status),
			ContentHash: memory.ComputeContentHash("[TASK] " + string(status)),
			MemoryType:  memory.TypeTask, DomainTag: domain, ConfidenceScore: 0.9,
			Status: memory.StatusCommitted, TaskStatus: status, Provider: "codex", CreatedAt: time.Now(),
		}
		require.NoError(t, store.InsertMemory(ctx, record))
		ids[id] = status
		if status == memory.TaskStatusPlanned {
			plannedID = id
		}
	}
	tasks, err := store.GetAllTasks(ctx, domain, 10)
	require.NoError(t, err)
	require.Len(t, tasks, len(statuses))
	for _, task := range tasks {
		assert.Equal(t, ids[task.MemoryID], task.TaskStatus)
	}

	// Replaying the original planned insert must never resurrect terminal work.
	doneID := uuid.NewString()
	replay := &memory.MemoryRecord{
		MemoryID: doneID, SubmittingAgent: "agent-test", Content: "[TASK] replay",
		ContentHash: memory.ComputeContentHash("[TASK] replay"), MemoryType: memory.TypeTask,
		DomainTag: domain, ConfidenceScore: 0.9, Status: memory.StatusCommitted,
		TaskStatus: memory.TaskStatusPlanned, CreatedAt: time.Now(),
	}
	require.NoError(t, store.InsertMemory(ctx, replay))
	require.NoError(t, store.UpdateTaskStatus(ctx, doneID, memory.TaskStatusDone))
	require.NoError(t, store.InsertMemory(ctx, replay))
	replay.TaskStatus = ""
	require.NoError(t, store.InsertMemory(ctx, replay))
	replayed, err := store.GetMemory(ctx, doneID)
	require.NoError(t, err)
	assert.Equal(t, memory.TaskStatusDone, replayed.TaskStatus)

	unknownID := uuid.NewString()
	unknown := &memory.MemoryRecord{
		MemoryID: unknownID, SubmittingAgent: "agent-test", Content: "[TASK] unknown",
		ContentHash: memory.ComputeContentHash("[TASK] unknown"), MemoryType: memory.TypeTask,
		DomainTag: domain, ConfidenceScore: 0.9, Status: memory.StatusCommitted, CreatedAt: time.Now(),
	}
	require.NoError(t, store.InsertMemory(ctx, unknown))
	other := *unknown
	other.MemoryID = uuid.NewString()
	other.DomainTag = domain + "-other"
	require.NoError(t, store.InsertMemory(ctx, &other))
	deprecated := *unknown
	deprecated.MemoryID = uuid.NewString()
	deprecated.Status = memory.StatusDeprecated
	require.NoError(t, store.InsertMemory(ctx, &deprecated))

	limited, err := store.GetAllTasks(ctx, domain, 2)
	require.NoError(t, err)
	require.Len(t, limited, 2)
	assert.Equal(t, unknownID, limited[0].MemoryID, "unclassified history is prioritized for recovery")
	assert.Equal(t, memory.TaskStatusInProgress, limited[1].TaskStatus)
	all, err := store.GetAllTasks(ctx, domain, 500)
	require.NoError(t, err)
	for _, task := range all {
		assert.Equal(t, domain, task.DomainTag)
		assert.NotEqual(t, memory.StatusDeprecated, task.Status)
	}

	agentID := "agent-" + uuid.NewString()
	require.NoError(t, store.CreateAgent(ctx, &AgentEntry{
		AgentID: agentID, Name: "assigned-agent", RegisteredName: "assigned-agent",
		Provider: "claude-code", Status: "active", CreatedAt: time.Now(),
	}))
	assigned, err := store.AssignTaskAndNotify(ctx, plannedID, agentID)
	require.NoError(t, err)
	require.True(t, assigned.Changed)
	require.True(t, assigned.NotificationCreated)
	require.Equal(t, string(memory.TaskStatusInProgress), assigned.TaskStatus)
	backlog, err := store.GetOpenTasks(ctx, domain, "claude-code", agentID)
	require.NoError(t, err)
	require.Len(t, backlog, 1, "explicit assignment must outrank the author's provider")
	require.Equal(t, plannedID, backlog[0].MemoryID)
	require.Equal(t, agentID, backlog[0].Assignee)
	notices, err := store.PeekAgentNotifications(ctx, agentID, 5)
	require.NoError(t, err)
	require.Len(t, notices, 1)
	claimed, err := store.ClaimTask(ctx, plannedID, agentID)
	require.NoError(t, err)
	require.True(t, claimed)
	acknowledged, err := store.AcknowledgeAgentNotifications(ctx, agentID, []string{notices[0].NotificationID})
	require.NoError(t, err)
	require.Equal(t, []string{notices[0].NotificationID}, acknowledged)
	completed, err := store.CompleteTaskAsAgent(ctx, plannedID, agentID, memory.TaskStatusDone)
	require.NoError(t, err)
	require.True(t, completed)
	require.NoError(t, store.UpdateTaskStatus(ctx, plannedID, memory.TaskStatusPlanned))
	claimed, err = store.ClaimTask(ctx, plannedID, agentID)
	require.NoError(t, err)
	require.False(t, claimed, "terminal reopen requires a fresh operator handoff")
	reassigned, err := store.AssignTaskAndNotify(ctx, plannedID, agentID)
	require.NoError(t, err)
	require.True(t, reassigned.Changed)
	claimed, err = store.ClaimTask(ctx, plannedID, agentID)
	require.NoError(t, err)
	require.True(t, claimed)
}

func TestPostgresQuerySimilarIncludesDisputed(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()
	emb := []float32{0.1, 0.2, 0.3}

	insert := func(status memory.MemoryStatus) string {
		t.Helper()
		id := uuid.NewString()
		content := "disputed recall " + id
		record := &memory.MemoryRecord{
			MemoryID:        id,
			SubmittingAgent: "agent-test",
			Content:         content,
			ContentHash:     memory.ComputeContentHash(content),
			Embedding:       emb,
			MemoryType:      memory.TypeFact,
			DomainTag:       "crypto",
			ConfidenceScore: 0.9,
			Status:          status,
			CreatedAt:       time.Now(),
		}
		require.NoError(t, store.InsertMemory(ctx, record))
		return id
	}

	committedID := insert(memory.StatusCommitted)
	disputedID := insert(memory.StatusChallenged)

	base, err := store.QuerySimilar(ctx, emb, QueryOptions{
		DomainTag: "crypto", StatusFilter: "committed", TopK: 100,
	})
	require.NoError(t, err)
	assert.Contains(t, recordIDs(base), committedID)
	assert.NotContains(t, recordIDs(base), disputedID)

	withDisputed, err := store.QuerySimilar(ctx, emb, QueryOptions{
		DomainTag: "crypto", StatusFilter: "committed", IncludeDisputed: true, TopK: 100,
	})
	require.NoError(t, err)
	assert.Contains(t, recordIDs(withDisputed), committedID)
	assert.Contains(t, recordIDs(withDisputed), disputedID)
}

func TestVotes(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	memID := uuid.NewString()
	record := &memory.MemoryRecord{
		MemoryID:        memID,
		SubmittingAgent: "agent-test",
		Content:         "vote test",
		ContentHash:     memory.ComputeContentHash("vote test"),
		MemoryType:      memory.TypeFact,
		DomainTag:       "crypto",
		ConfidenceScore: 0.9,
		Status:          memory.StatusProposed,
		CreatedAt:       time.Now(),
	}
	require.NoError(t, store.InsertMemory(ctx, record))

	vote := &ValidationVote{
		MemoryID:     memID,
		ValidatorID:  "validator-1",
		Decision:     "accept",
		Rationale:    "looks good",
		WeightAtVote: 0.25,
		BlockHeight:  100,
		CreatedAt:    time.Now(),
	}
	require.NoError(t, store.InsertVote(ctx, vote))

	votes, err := store.GetVotes(ctx, memID)
	require.NoError(t, err)
	assert.Len(t, votes, 1)
	assert.Equal(t, "accept", votes[0].Decision)
}
