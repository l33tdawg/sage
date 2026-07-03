package orchestrator

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockNodeController implements NodeController for testing.
type mockNodeController struct {
	mu          sync.Mutex
	stopErr     error
	startErr    error
	regenErr    error
	wipeErr     error
	stopCalled  int
	startCalled int
	regenCalled int
	wipeCalled  int
	dataDir     string
}

func (m *mockNodeController) StopChain() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopCalled++
	return m.stopErr
}

func (m *mockNodeController) StartChain() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startCalled++
	return m.startErr
}

func (m *mockNodeController) RegenerateGenesis(validators []ValidatorInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.regenCalled++
	return m.regenErr
}

func (m *mockNodeController) WipeChainState() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.wipeCalled++
	return m.wipeErr
}

func (m *mockNodeController) GetDataDir() string {
	return m.dataDir
}

func newTestRedeployer(t *testing.T) (*Redeployer, *store.SQLiteStore, *mockNodeController) {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	s, err := store.NewSQLiteStore(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	mock := &mockNodeController{dataDir: tmpDir}
	logger := zerolog.Nop()
	r := NewRedeployer(s, mock, logger)
	// Use a shorter lock TTL for tests
	r.lockTTL = 5 * time.Minute

	return r, s, mock
}

func TestDeployHappyPath(t *testing.T) {
	r, s, mock := newTestRedeployer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create the agent first so the orchestrator can find it
	agent := &store.AgentEntry{
		AgentID: "agent-1",
		Name:    "Test Agent",
		Role:    "validator",
		Status:  "pending",
	}
	require.NoError(t, s.CreateAgent(ctx, agent))

	err := r.Deploy(ctx, OpAddAgent, "agent-1")
	require.NoError(t, err)

	// Verify all node controller methods were called
	assert.Equal(t, 1, mock.stopCalled)
	assert.Equal(t, 1, mock.startCalled)
	assert.Equal(t, 1, mock.regenCalled)
	assert.Equal(t, 1, mock.wipeCalled)

	// Verify agent status was updated to active (by the RBAC phase)
	got, err := s.GetAgent(ctx, "agent-1")
	require.NoError(t, err)
	assert.Equal(t, "active", got.Status)

	// Verify lock was released
	status, err := r.GetStatus(ctx)
	require.NoError(t, err)
	assert.False(t, status.Active)
}

func TestDeployStopChainFails(t *testing.T) {
	r, s, mock := newTestRedeployer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mock.stopErr = fmt.Errorf("chain stop failed")

	agent := &store.AgentEntry{
		AgentID: "agent-1",
		Name:    "Test Agent",
		Role:    "validator",
		Status:  "pending",
	}
	require.NoError(t, s.CreateAgent(ctx, agent))

	err := r.Deploy(ctx, OpAddAgent, "agent-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CHAIN_STOPPED")
	assert.Contains(t, err.Error(), "chain stop failed")

	// Rollback should try to restart chain (startCalled incremented during rollback)
	assert.Equal(t, 1, mock.stopCalled)
	assert.GreaterOrEqual(t, mock.startCalled, 1, "rollback should attempt to restart chain")

	// Lock should be released after rollback
	status, err := r.GetStatus(ctx)
	require.NoError(t, err)
	assert.False(t, status.Active)
}

func TestDeployWipeStateFails(t *testing.T) {
	r, s, mock := newTestRedeployer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mock.wipeErr = fmt.Errorf("wipe state failed")

	agent := &store.AgentEntry{
		AgentID: "agent-1",
		Name:    "Test Agent",
		Role:    "validator",
		Status:  "pending",
	}
	require.NoError(t, s.CreateAgent(ctx, agent))

	err := r.Deploy(ctx, OpAddAgent, "agent-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CHAIN_STATE_WIPED")
	assert.Contains(t, err.Error(), "wipe state failed")

	// StopChain should have been called before wipe
	assert.Equal(t, 1, mock.stopCalled)
	assert.Equal(t, 1, mock.wipeCalled)

	// Rollback should try to restart chain
	assert.GreaterOrEqual(t, mock.startCalled, 1, "rollback should attempt to restart chain")
}

func TestConcurrentDeploy(t *testing.T) {
	r, s, _ := newTestRedeployer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	agent1 := &store.AgentEntry{
		AgentID: "agent-1",
		Name:    "Agent One",
		Role:    "validator",
		Status:  "pending",
	}
	agent2 := &store.AgentEntry{
		AgentID: "agent-2",
		Name:    "Agent Two",
		Role:    "validator",
		Status:  "pending",
	}
	require.NoError(t, s.CreateAgent(ctx, agent1))
	require.NoError(t, s.CreateAgent(ctx, agent2))

	// Deploy holds a mutex, so the second deploy will block until the first finishes.
	// We verify the mutex works by running two deploys concurrently — both should
	// succeed sequentially (the mutex serializes them, and each releases its lock).
	var wg sync.WaitGroup
	errs := make([]error, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		errs[0] = r.Deploy(ctx, OpAddAgent, "agent-1")
	}()
	go func() {
		defer wg.Done()
		errs[1] = r.Deploy(ctx, OpAddAgent, "agent-2")
	}()
	wg.Wait()

	// Both should succeed because the mutex serializes them, and each deploy
	// releases the lock at the end. (The second acquires the lock after the first releases.)
	successCount := 0
	for _, e := range errs {
		if e == nil {
			successCount++
		}
	}
	assert.Equal(t, 2, successCount, "both deploys should succeed sequentially")
}

func TestGetStatus(t *testing.T) {
	r, s, _ := newTestRedeployer(t)
	ctx := context.Background()

	// No active deployment
	status, err := r.GetStatus(ctx)
	require.NoError(t, err)
	assert.False(t, status.Active)

	// Manually acquire lock to simulate an active deployment
	require.NoError(t, s.AcquireRedeployLock(ctx, "agent-1", "add_agent", 10*time.Minute))

	status, err = r.GetStatus(ctx)
	require.NoError(t, err)
	assert.True(t, status.Active)
	assert.Equal(t, OpAddAgent, status.Operation)
	assert.Equal(t, "agent-1", status.AgentID)

	// Release lock
	require.NoError(t, s.ReleaseRedeployLock(ctx))
	status, err = r.GetStatus(ctx)
	require.NoError(t, err)
	assert.False(t, status.Active)
}

// TestGetLiveStatus_StaleInProgressAutoHeals verifies the root-cause fix: an
// in_progress log row left by a crashed/restarted run (no live in-process
// Deploy) must read as "failed", not wedge the status as "running" forever.
func TestGetLiveStatus_StaleInProgressAutoHeals(t *testing.T) {
	r, s, _ := newTestRedeployer(t)
	ctx := context.Background()
	require.NoError(t, s.InsertRedeployLog(ctx, &store.RedeploymentLogEntry{
		Operation: "remove_agent", AgentID: "abc", Phase: string(PhaseChainRestarted), Status: string(StatusInProgress),
	}))
	// r.running is false (no live Deploy in this process).
	status, phase, _, _, _, err := r.GetLiveStatus(ctx)
	require.NoError(t, err)
	assert.Equal(t, "failed", status, "a stuck in_progress row with no live run must auto-heal to failed")
	assert.Equal(t, string(PhaseChainRestarted), phase)
}

// TestQuickAgentOp_NoChainRedeploy verifies the single-node path applies an
// agent op WITHOUT any destructive chain operation and logs COMPLETED.
func TestQuickAgentOp_NoChainRedeploy(t *testing.T) {
	r, s, mock := newTestRedeployer(t)
	ctx := context.Background()
	require.NoError(t, r.QuickAgentOp(ctx, "remove_agent", "abc"))
	assert.Equal(t, 0, mock.stopCalled, "must not stop the chain")
	assert.Equal(t, 0, mock.wipeCalled, "must not wipe chain state")
	assert.Equal(t, 0, mock.regenCalled, "must not regenerate genesis")
	latest, err := s.GetLatestRedeployLog(ctx)
	require.NoError(t, err)
	assert.Equal(t, string(PhaseCompleted), latest.Phase)
	status, _, _, _, _, _ := r.GetLiveStatus(ctx)
	assert.Equal(t, "completed", status)
}

// TestClearStale marks lingering in_progress rows terminal when nothing is live.
func TestClearStale(t *testing.T) {
	r, s, _ := newTestRedeployer(t)
	ctx := context.Background()
	require.NoError(t, s.InsertRedeployLog(ctx, &store.RedeploymentLogEntry{
		Operation: "remove_agent", AgentID: "abc", Phase: string(PhaseChainRestarted), Status: string(StatusInProgress),
	}))
	n, err := r.ClearStale(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "one stale row cleared")
	latest, err := s.GetLatestRedeployLog(ctx)
	require.NoError(t, err)
	assert.NotEqual(t, string(StatusInProgress), latest.Status, "no in_progress rows should remain")
}
