package store

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/stretchr/testify/require"
)

func TestGetAllTasksClampsBoardLimit(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	insertOpenTask(t, s, "historical-priority", "codex")
	_, err := s.writeExecContext(ctx, `UPDATE memories SET task_status = '' WHERE memory_id = 'historical-priority'`)
	require.NoError(t, err)
	for i := 0; i < 510; i++ {
		insertOpenTask(t, s, fmt.Sprintf("limit-task-%03d", i), "codex")
	}
	tasks, err := s.GetAllTasks(ctx, "", 10_000)
	require.NoError(t, err)
	require.Len(t, tasks, 500)
	require.Equal(t, "historical-priority", tasks[0].MemoryID, "unknown statuses must not be hidden behind the board limit")
	tasks, err = s.GetAllTasks(ctx, "", -1)
	require.NoError(t, err)
	require.Len(t, tasks, 100)
	open, err := s.GetOpenTasks(ctx, "", "", "")
	require.NoError(t, err)
	require.Len(t, open, 500, "agent backlog reads must remain bounded")
}

func TestGetAllTasksIncludesHistoricalEmptyStatusForRecovery(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	insertOpenTask(t, s, "historical-empty-status", "codex")
	_, err := s.writeExecContext(ctx, `UPDATE memories SET task_status = '' WHERE memory_id = 'historical-empty-status'`)
	require.NoError(t, err)
	tasks, err := s.GetAllTasks(ctx, "", 10)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Empty(t, tasks[0].TaskStatus, "unknown history must remain explicit until the user classifies it")
}

func TestReorderTasksPersistsWithinColumnAndKeepsOmittedCards(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	insertOpenTask(t, s, "order-a", "codex")
	insertOpenTask(t, s, "order-b", "codex")
	insertOpenTask(t, s, "order-c", "codex")

	require.NoError(t, s.ReorderTasks(ctx, memory.TaskStatusPlanned, []string{"order-a", "order-c"}))
	tasks, err := s.GetAllTasks(ctx, "", 10)
	require.NoError(t, err)
	require.Len(t, tasks, 3)
	require.Equal(t, []string{"order-a", "order-c", "order-b"}, []string{
		tasks[0].MemoryID, tasks[1].MemoryID, tasks[2].MemoryID,
	})

	require.NoError(t, s.UpdateTaskStatus(ctx, "order-a", memory.TaskStatusDone))
	var position int
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT task_board_position FROM memories WHERE memory_id = 'order-a'`).Scan(&position))
	require.Zero(t, position, "moving columns must put the card at the top of its new column")
}

func insertOpenTask(t *testing.T, s *SQLiteStore, id, provider string) {
	t.Helper()
	rec := testMemory(id, "author", "[TASK] "+id, "work")
	rec.MemoryType = memory.TypeTask
	rec.Status = memory.StatusCommitted
	rec.TaskStatus = memory.TaskStatusPlanned
	rec.Provider = provider
	require.NoError(t, s.InsertMemory(context.Background(), rec))
}

func insertActiveTestAgent(t *testing.T, s *SQLiteStore, id, provider string) {
	t.Helper()
	require.NoError(t, s.CreateAgent(context.Background(), &AgentEntry{
		AgentID: id, Name: id, RegisteredName: id, Provider: provider, Status: "active",
	}))
}

func TestGetOpenTasks_AssignmentOutranksAuthorProvider(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	insertOpenTask(t, s, "cross-provider", "codex")
	insertOpenTask(t, s, "unassigned-mine", "claude-code")
	insertOpenTask(t, s, "unassigned-foreign", "gemini")
	insertActiveTestAgent(t, s, "agent-tii", "claude-code")

	_, err := s.AssignTaskAndNotify(ctx, "cross-provider", "agent-tii")
	require.NoError(t, err)

	tasks, err := s.GetOpenTasks(ctx, "", "claude-code", "agent-tii")
	require.NoError(t, err)
	ids := make(map[string]bool)
	for _, task := range tasks {
		ids[task.MemoryID] = true
	}
	require.True(t, ids["cross-provider"], "explicit cross-provider assignment must be visible")
	require.False(t, ids["unassigned-mine"], "unassigned work is human triage, not an agent backlog")
	require.False(t, ids["unassigned-foreign"], "foreign-provider unassigned work stays hidden")

	other, err := s.GetOpenTasks(ctx, "", "codex", "agent-other")
	require.NoError(t, err)
	for _, task := range other {
		require.NotEqual(t, "cross-provider", task.MemoryID, "another agent must not see assigned work")
	}
	require.Empty(t, other, "provider must not widen another agent's backlog")
}

func TestClaimTaskRejectsUnassignedAndOtherAgentWork(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	insertOpenTask(t, s, "unassigned", "claude-code")
	insertOpenTask(t, s, "owned-by-b", "claude-code")
	insertActiveTestAgent(t, s, "agent-a", "claude-code")
	insertActiveTestAgent(t, s, "agent-b", "claude-code")
	_, err := s.AssignTaskAndNotify(ctx, "owned-by-b", "agent-b")
	require.NoError(t, err)

	claimed, err := s.ClaimTask(ctx, "unassigned", "agent-a")
	require.NoError(t, err)
	require.False(t, claimed)
	claimed, err = s.ClaimTask(ctx, "owned-by-b", "agent-a")
	require.NoError(t, err)
	require.False(t, claimed)
}

func TestInsertAgentCreatedTaskPersistsOwner(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	rec := testMemory("self-created", "agent-a", "[TASK] self-created", "work")
	rec.MemoryType = memory.TypeTask
	rec.Status = memory.StatusCommitted
	rec.TaskStatus = memory.TaskStatusInProgress
	rec.Provider = "claude-code"
	rec.Assignee = "agent-a"
	require.NoError(t, s.InsertMemory(ctx, rec))

	tasks, err := s.GetOpenTasks(ctx, "work", "spoofed-provider", "agent-a")
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, "agent-a", tasks[0].Assignee)
}

func TestMigrationReturnsOwnerlessInProgressTaskToPlanned(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	insertOpenTask(t, s, "ownerless-running", "claude-code")
	_, err := s.writeExecContext(ctx, `UPDATE memories SET task_status = 'in_progress', assignee = '' WHERE memory_id = 'ownerless-running'`)
	require.NoError(t, err)
	require.NoError(t, s.migrateTaskAssignmentNotifications(ctx))
	var status string
	require.NoError(t, s.conn.QueryRowContext(ctx, `SELECT task_status FROM memories WHERE memory_id = 'ownerless-running'`).Scan(&status))
	require.Equal(t, string(memory.TaskStatusPlanned), status)
}

func TestUpdateTaskStatusRejectsOwnerlessInProgress(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	insertOpenTask(t, s, "needs-owner", "claude-code")
	require.ErrorContains(t, s.UpdateTaskStatus(ctx, "needs-owner", memory.TaskStatusInProgress), "requires an assignee")
	var status string
	require.NoError(t, s.conn.QueryRowContext(ctx, `SELECT task_status FROM memories WHERE memory_id = 'needs-owner'`).Scan(&status))
	require.Equal(t, string(memory.TaskStatusPlanned), status)
}

func TestAssignTaskAndNotify_IdempotentAndVersioned(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	insertOpenTask(t, s, "task-1", "codex")
	insertActiveTestAgent(t, s, "agent-a", "codex")
	insertActiveTestAgent(t, s, "agent-b", "claude-code")

	first, err := s.AssignTaskAndNotify(ctx, "task-1", "agent-a")
	require.NoError(t, err)
	require.True(t, first.Changed)
	require.EqualValues(t, 1, first.AssignmentVersion)
	require.True(t, first.NotificationCreated)

	notices, err := s.TakeAgentNotifications(ctx, "agent-a", 5)
	require.NoError(t, err)
	require.Len(t, notices, 1)
	require.Equal(t, "task-1", notices[0].TaskID)

	claimed, err := s.ClaimTask(ctx, "task-1", "agent-a")
	require.NoError(t, err)
	require.True(t, claimed)
	var firstPickup string
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT COALESCE(task_picked_up_at, '') FROM memories WHERE memory_id = 'task-1'`).Scan(&firstPickup))
	claimed, err = s.ClaimTask(ctx, "task-1", "agent-a")
	require.NoError(t, err)
	require.True(t, claimed)
	var retryPickup string
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT COALESCE(task_picked_up_at, '') FROM memories WHERE memory_id = 'task-1'`).Scan(&retryPickup))
	require.Equal(t, firstPickup, retryPickup, "same-owner pickup retry preserves the first timestamp")

	repeat, err := s.AssignTaskAndNotify(ctx, "task-1", "agent-a")
	require.NoError(t, err)
	require.False(t, repeat.Changed)
	require.EqualValues(t, 1, repeat.AssignmentVersion)
	require.False(t, repeat.NotificationCreated)
	var pickedBy string
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT COALESCE(task_picked_up_by, '') FROM memories WHERE memory_id = 'task-1'`).Scan(&pickedBy))
	require.Equal(t, "agent-a", pickedBy, "idempotent retry must preserve pickup evidence")
	notices, err = s.TakeAgentNotifications(ctx, "agent-a", 5)
	require.NoError(t, err)
	require.Empty(t, notices, "same assignment must not duplicate a notice")

	reassigned, err := s.AssignTaskAndNotify(ctx, "task-1", "agent-b")
	require.NoError(t, err)
	require.True(t, reassigned.Changed)
	require.EqualValues(t, 2, reassigned.AssignmentVersion)
	notices, err = s.TakeAgentNotifications(ctx, "agent-b", 5)
	require.NoError(t, err)
	require.Len(t, notices, 1)
	require.EqualValues(t, 2, notices[0].AssignmentVersion)

	backToA, err := s.AssignTaskAndNotify(ctx, "task-1", "agent-a")
	require.NoError(t, err)
	require.EqualValues(t, 3, backToA.AssignmentVersion)
	require.True(t, backToA.NotificationCreated)
	notices, err = s.TakeAgentNotifications(ctx, "agent-a", 5)
	require.NoError(t, err)
	require.Len(t, notices, 1)
	require.EqualValues(t, 3, notices[0].AssignmentVersion)

	unassigned, err := s.AssignTaskAndNotify(ctx, "task-1", "")
	require.NoError(t, err)
	require.True(t, unassigned.Changed)
	require.False(t, unassigned.NotificationCreated)
	notices, err = s.TakeAgentNotifications(ctx, "agent-a", 5)
	require.NoError(t, err)
	require.Empty(t, notices)
}

func TestConcurrentTaskReassignmentLeavesOneCurrentNotice(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	insertOpenTask(t, s, "raced-task", "codex")
	insertActiveTestAgent(t, s, "agent-a", "codex")
	insertActiveTestAgent(t, s, "agent-b", "claude-code")

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, agentID := range []string{"agent-a", "agent-b"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			<-start
			_, err := s.AssignTaskAndNotify(ctx, "raced-task", id)
			errs <- err
		}(agentID)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	var current string
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT assignee FROM memories WHERE memory_id = 'raced-task'`).Scan(&current))
	total := 0
	for _, agentID := range []string{"agent-a", "agent-b"} {
		notices, err := s.TakeAgentNotifications(ctx, agentID, 5)
		require.NoError(t, err)
		if agentID == current {
			require.Len(t, notices, 1)
		} else {
			require.Empty(t, notices)
		}
		total += len(notices)
	}
	require.Equal(t, 1, total)
}

func TestConcurrentTaskStartAndTerminalLeavesTerminalUnassigned(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	insertOpenTask(t, s, "start-terminal-race", "codex")
	insertActiveTestAgent(t, s, "agent-a", "codex")

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, err := s.ClaimTask(ctx, "start-terminal-race", "agent-a")
		errs <- err
	}()
	go func() {
		defer wg.Done()
		<-start
		errs <- s.UpdateTaskStatus(ctx, "start-terminal-race", memory.TaskStatusDone)
	}()
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	var status, assignee string
	require.NoError(t, s.conn.QueryRowContext(ctx, `
		SELECT task_status, COALESCE(assignee, '') FROM memories
		WHERE memory_id = 'start-terminal-race'`).Scan(&status, &assignee))
	require.Equal(t, string(memory.TaskStatusDone), status)
	require.Empty(t, assignee)
}

func TestFreshLifecycleClaimReplacesPriorPickupEvidence(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	insertOpenTask(t, s, "fresh-lifecycle", "codex")
	insertActiveTestAgent(t, s, "agent-a", "codex")
	insertActiveTestAgent(t, s, "agent-b", "claude-code")
	_, err := s.AssignTaskAndNotify(ctx, "fresh-lifecycle", "agent-a")
	require.NoError(t, err)
	claimed, err := s.ClaimTask(ctx, "fresh-lifecycle", "agent-a")
	require.NoError(t, err)
	require.True(t, claimed)
	_, err = s.conn.ExecContext(ctx,
		`UPDATE memories SET task_picked_up_at = '2020-01-01T00:00:00Z' WHERE memory_id = 'fresh-lifecycle'`)
	require.NoError(t, err)
	require.NoError(t, s.UpdateTaskStatus(ctx, "fresh-lifecycle", memory.TaskStatusDone))
	require.NoError(t, s.UpdateTaskStatus(ctx, "fresh-lifecycle", memory.TaskStatusPlanned))
	claimed, err = s.ClaimTask(ctx, "fresh-lifecycle", "agent-b")
	require.NoError(t, err)
	require.False(t, claimed)
	_, err = s.AssignTaskAndNotify(ctx, "fresh-lifecycle", "agent-b")
	require.NoError(t, err)
	claimed, err = s.ClaimTask(ctx, "fresh-lifecycle", "agent-b")
	require.NoError(t, err)
	require.True(t, claimed)

	var pickedBy, pickedAt string
	require.NoError(t, s.conn.QueryRowContext(ctx, `
		SELECT task_picked_up_by, task_picked_up_at FROM memories
		WHERE memory_id = 'fresh-lifecycle'`).Scan(&pickedBy, &pickedAt))
	require.Equal(t, "agent-b", pickedBy)
	require.NotEqual(t, "2020-01-01T00:00:00Z", pickedAt)
}

func TestAcknowledgeNotificationReturnsOnlyCurrentAssignmentWinners(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	insertOpenTask(t, s, "stale-notice", "codex")
	insertActiveTestAgent(t, s, "agent-a", "codex")
	insertActiveTestAgent(t, s, "agent-b", "claude-code")
	_, err := s.AssignTaskAndNotify(ctx, "stale-notice", "agent-a")
	require.NoError(t, err)
	peeked, err := s.PeekAgentNotifications(ctx, "agent-a", 5)
	require.NoError(t, err)
	require.Len(t, peeked, 1)
	_, err = s.AssignTaskAndNotify(ctx, "stale-notice", "agent-b")
	require.NoError(t, err)

	acknowledged, err := s.AcknowledgeAgentNotifications(ctx, "agent-a", []string{peeked[0].NotificationID})
	require.NoError(t, err)
	require.Empty(t, acknowledged, "a stale pre-reassignment notice must lose the write-boundary CAS")
}

func TestDirectlyInsertedTerminalTaskStillRequiresHandoffAfterReopen(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	insertActiveTestAgent(t, s, "agent-a", "codex")
	for _, terminal := range []memory.TaskStatus{memory.TaskStatusDone, memory.TaskStatusDropped} {
		id := "direct-" + string(terminal)
		rec := testMemory(id, "author", "[TASK] "+id, "work")
		rec.MemoryType = memory.TypeTask
		rec.Status = memory.StatusCommitted
		rec.TaskStatus = terminal
		rec.Provider = "codex"
		require.NoError(t, s.InsertMemory(ctx, rec))
		require.NoError(t, s.UpdateTaskStatus(ctx, id, memory.TaskStatusPlanned))
		claimed, err := s.ClaimTask(ctx, id, "agent-a")
		require.NoError(t, err)
		require.False(t, claimed)
		_, err = s.AssignTaskAndNotify(ctx, id, "agent-a")
		require.NoError(t, err)
		claimed, err = s.ClaimTask(ctx, id, "agent-a")
		require.NoError(t, err)
		require.True(t, claimed)
	}
}

func TestConcurrentAgentCompleteAndReassignHasValidLinearizedOutcome(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	insertOpenTask(t, s, "complete-reassign-race", "codex")
	insertActiveTestAgent(t, s, "agent-a", "codex")
	insertActiveTestAgent(t, s, "agent-b", "claude-code")
	_, err := s.AssignTaskAndNotify(ctx, "complete-reassign-race", "agent-a")
	require.NoError(t, err)

	start := make(chan struct{})
	type completionOutcome struct {
		completed bool
		err       error
	}
	completedCh := make(chan completionOutcome, 1)
	reassignedCh := make(chan error, 1)
	go func() {
		<-start
		completed, completeErr := s.CompleteTaskAsAgent(ctx, "complete-reassign-race", "agent-a", memory.TaskStatusDone)
		completedCh <- completionOutcome{completed: completed, err: completeErr}
	}()
	go func() {
		<-start
		_, assignErr := s.AssignTaskAndNotify(ctx, "complete-reassign-race", "agent-b")
		reassignedCh <- assignErr
	}()
	close(start)
	completion := <-completedCh
	assignErr := <-reassignedCh
	require.NoError(t, completion.err)

	tasks, err := s.GetAllTasks(ctx, "work", 10)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	if completion.completed {
		require.Error(t, assignErr)
		require.Equal(t, memory.TaskStatusDone, tasks[0].TaskStatus)
		require.Empty(t, tasks[0].Assignee)
	} else {
		require.NoError(t, assignErr)
		require.Equal(t, memory.TaskStatusInProgress, tasks[0].TaskStatus)
		require.Equal(t, "agent-b", tasks[0].Assignee)
	}
}

func TestConcurrentAgentStartAndCompleteEndsTerminalUnassigned(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	insertOpenTask(t, s, "complete-start-race", "codex")
	insertActiveTestAgent(t, s, "agent-a", "codex")
	_, err := s.AssignTaskAndNotify(ctx, "complete-start-race", "agent-a")
	require.NoError(t, err)

	start := make(chan struct{})
	errs := make(chan error, 2)
	go func() {
		<-start
		_, claimErr := s.ClaimTask(ctx, "complete-start-race", "agent-a")
		errs <- claimErr
	}()
	go func() {
		<-start
		_, completeErr := s.CompleteTaskAsAgent(ctx, "complete-start-race", "agent-a", memory.TaskStatusDone)
		errs <- completeErr
	}()
	close(start)
	for i := 0; i < 2; i++ {
		require.NoError(t, <-errs)
	}
	tasks, err := s.GetAllTasks(ctx, "work", 10)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, memory.TaskStatusDone, tasks[0].TaskStatus)
	require.Empty(t, tasks[0].Assignee)
}

func TestTerminalTaskSupersedesUnreadAssignmentNotice(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	insertOpenTask(t, s, "task-terminal", "codex")
	insertActiveTestAgent(t, s, "agent-a", "codex")
	_, err := s.AssignTaskAndNotify(ctx, "task-terminal", "agent-a")
	require.NoError(t, err)
	claimed, err := s.ClaimTask(ctx, "task-terminal", "agent-a")
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, s.UpdateTaskStatus(ctx, "task-terminal", memory.TaskStatusDone))
	notices, err := s.TakeAgentNotifications(ctx, "agent-a", 5)
	require.NoError(t, err)
	require.Empty(t, notices)
	var assignee, pickedBy string
	require.NoError(t, s.conn.QueryRowContext(ctx, `
		SELECT COALESCE(assignee, ''), COALESCE(task_picked_up_by, '')
		FROM memories WHERE memory_id = 'task-terminal'`).Scan(&assignee, &pickedBy))
	require.Empty(t, assignee, "terminal tasks must not retain a current owner")
	require.Equal(t, "agent-a", pickedBy, "terminal transition preserves pickup evidence")
	claimed, err = s.ClaimTask(ctx, "task-terminal", "agent-a")
	require.NoError(t, err)
	require.False(t, claimed, "agent pickup must not reopen terminal work")
	var terminalStatus string
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT task_status FROM memories WHERE memory_id = 'task-terminal'`).Scan(&terminalStatus))
	require.Equal(t, string(memory.TaskStatusDone), terminalStatus)

	require.NoError(t, s.UpdateTaskStatus(ctx, "task-terminal", memory.TaskStatusPlanned))
	notices, err = s.TakeAgentNotifications(ctx, "agent-a", 5)
	require.NoError(t, err)
	require.Empty(t, notices, "reopening is unassigned until the operator hands it off again")
	claimed, err = s.ClaimTask(ctx, "task-terminal", "agent-a")
	require.NoError(t, err)
	require.False(t, claimed, "reopened terminal work requires a fresh operator handoff")
	reassigned, err := s.AssignTaskAndNotify(ctx, "task-terminal", "agent-a")
	require.NoError(t, err)
	require.True(t, reassigned.NotificationCreated)
	notices, err = s.TakeAgentNotifications(ctx, "agent-a", 5)
	require.NoError(t, err)
	require.Len(t, notices, 1)
}

func TestTaskStatusUpdatedAtTracksTransitionsNotTaskAge(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	insertOpenTask(t, s, "status-clock", "codex")
	insertActiveTestAgent(t, s, "agent-a", "codex")
	_, err := s.conn.ExecContext(ctx, `UPDATE memories
		SET created_at = '2020-01-01T00:00:00Z', task_status_updated_at = '2020-01-01T00:00:00Z'
		WHERE memory_id = 'status-clock'`)
	require.NoError(t, err)
	_, err = s.AssignTaskAndNotify(ctx, "status-clock", "agent-a")
	require.NoError(t, err)

	var inProgressAt string
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT task_status_updated_at FROM memories WHERE memory_id = 'status-clock'`).Scan(&inProgressAt))
	require.NotEqual(t, "2020-01-01T00:00:00Z", inProgressAt)

	_, err = s.conn.ExecContext(ctx, `UPDATE memories SET task_status_updated_at = '2020-01-02T00:00:00Z'
		WHERE memory_id = 'status-clock'`)
	require.NoError(t, err)
	require.NoError(t, s.UpdateTaskStatus(ctx, "status-clock", memory.TaskStatusDone))

	var doneAt string
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT task_status_updated_at FROM memories WHERE memory_id = 'status-clock'`).Scan(&doneAt))
	require.NotEqual(t, "2020-01-02T00:00:00Z", doneAt,
		"completion retention must start when the task enters Done")

	require.NoError(t, s.UpdateTaskStatus(ctx, "status-clock", memory.TaskStatusDone))
	var repeatedDoneAt string
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT task_status_updated_at FROM memories WHERE memory_id = 'status-clock'`).Scan(&repeatedDoneAt))
	require.Equal(t, doneAt, repeatedDoneAt, "a no-op Done update must not extend retention forever")

	tasks, err := s.GetAllTasks(ctx, "", 10)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.NotNil(t, tasks[0].TaskStatusUpdatedAt)
}

func TestTaskAssignmentMigrationBackfillsWithoutLosingPickup(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	insertOpenTask(t, s, "legacy-task", "codex")
	insertActiveTestAgent(t, s, "agent-a", "codex")
	_, err := s.conn.ExecContext(ctx, `
		UPDATE memories
		SET assignee = 'agent-a', task_assignment_version = 0,
		    task_picked_up_by = 'agent-a', task_picked_up_at = '2026-07-11T00:00:00Z'
		WHERE memory_id = 'legacy-task'`)
	require.NoError(t, err)
	_, err = s.conn.ExecContext(ctx, `DELETE FROM agent_notifications WHERE task_id = 'legacy-task'`)
	require.NoError(t, err)

	require.NoError(t, s.migrateTaskAssignmentNotifications(ctx))
	require.NoError(t, s.migrateTaskAssignmentNotifications(ctx), "migration must be restart-safe")

	var version, noticeCount int
	var pickedBy, pickedAt string
	require.NoError(t, s.conn.QueryRowContext(ctx, `
		SELECT task_assignment_version, COALESCE(task_picked_up_by, ''), COALESCE(task_picked_up_at, '')
		FROM memories WHERE memory_id = 'legacy-task'`).Scan(&version, &pickedBy, &pickedAt))
	require.Equal(t, 1, version)
	require.Equal(t, "agent-a", pickedBy)
	require.Equal(t, "2026-07-11T00:00:00Z", pickedAt)
	require.NoError(t, s.conn.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM agent_notifications WHERE task_id = 'legacy-task' AND state = 'unread'`).Scan(&noticeCount))
	require.Equal(t, 1, noticeCount)
}

func TestTaskAssignmentMigrationSkipsUnknownAndInactiveAssignees(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	insertOpenTask(t, s, "unknown-task", "codex")
	insertOpenTask(t, s, "inactive-task", "codex")
	insertActiveTestAgent(t, s, "inactive-agent", "codex")
	require.NoError(t, s.UpdateAgentStatus(ctx, "inactive-agent", "inactive"))
	_, err := s.conn.ExecContext(ctx, `
		UPDATE memories SET assignee = CASE memory_id
		  WHEN 'unknown-task' THEN 'unknown-agent' ELSE 'inactive-agent' END,
		  task_assignment_version = 0
		WHERE memory_id IN ('unknown-task','inactive-task')`)
	require.NoError(t, err)

	require.NoError(t, s.migrateTaskAssignmentNotifications(ctx))
	var count int
	require.NoError(t, s.conn.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM agent_notifications
		WHERE task_id IN ('unknown-task','inactive-task')`).Scan(&count))
	require.Zero(t, count)
}

func TestAssignTaskAndNotifyRechecksActiveAgentInsideTransaction(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	insertOpenTask(t, s, "guarded-task", "codex")
	insertActiveTestAgent(t, s, "inactive-agent", "codex")
	require.NoError(t, s.UpdateAgentStatus(ctx, "inactive-agent", "inactive"))

	result, err := s.AssignTaskAndNotify(ctx, "guarded-task", "inactive-agent")
	require.ErrorContains(t, err, "active registered agent")
	require.Nil(t, result)
	var assignee string
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT COALESCE(assignee, '') FROM memories WHERE memory_id = 'guarded-task'`).Scan(&assignee))
	require.Empty(t, assignee)
}
