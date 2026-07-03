package orchestrator

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/rs/zerolog"
)

// Phase represents a redeployment state machine phase.
type Phase string

const (
	PhaseLockAcquired      Phase = "LOCK_ACQUIRED"
	PhaseBackupCreated     Phase = "BACKUP_CREATED"
	PhaseChainStopped      Phase = "CHAIN_STOPPED"
	PhaseGenesisGenerated  Phase = "GENESIS_GENERATED"
	PhaseChainStateWiped   Phase = "CHAIN_STATE_WIPED"
	PhaseChainRestarted    Phase = "CHAIN_RESTARTED"
	PhaseConsensusVerified Phase = "CONSENSUS_VERIFIED"
	PhaseRBACConfigured    Phase = "RBAC_CONFIGURED"
	PhaseCompleted         Phase = "COMPLETED"
)

// PhaseStatus represents the status of a phase.
type PhaseStatus string

const (
	StatusPending    PhaseStatus = "pending"
	StatusInProgress PhaseStatus = "in_progress"
	StatusCompleted  PhaseStatus = "completed"
	StatusFailed     PhaseStatus = "failed"
	StatusRolledBack PhaseStatus = "rolled_back"
)

// Operation represents the type of redeployment operation.
type Operation string

const (
	OpAddAgent    Operation = "add_agent"
	OpRemoveAgent Operation = "remove_agent"
	OpRotateKey   Operation = "rotate_key"
)

// PhaseInfo tracks progress of a single phase.
type PhaseInfo struct {
	Phase  Phase       `json:"phase"`
	Status PhaseStatus `json:"status"`
	LogID  int64       `json:"log_id,omitempty"`
	Error  string      `json:"error,omitempty"`
}

// RedeployStatus is the public view of a redeployment operation.
type RedeployStatus struct {
	Active    bool        `json:"active"`
	Operation Operation   `json:"operation,omitempty"`
	AgentID   string      `json:"agent_id,omitempty"`
	Phases    []PhaseInfo `json:"phases,omitempty"`
	Error     string      `json:"error,omitempty"`
}

// NodeController provides methods to stop and restart the CometBFT node.
// The cmd/sage-gui package implements this interface.
type NodeController interface {
	StopChain() error
	StartChain() error
	RegenerateGenesis(validators []ValidatorInfo) error
	WipeChainState() error
	GetDataDir() string
}

// ValidatorInfo holds the info needed for genesis generation.
type ValidatorInfo struct {
	Name    string
	PubKey  []byte // Ed25519 public key (32 bytes)
	Power   int64
	Address []byte
}

// Redeployer manages chain redeployment operations.
type Redeployer struct {
	store      store.AgentStore
	logger     zerolog.Logger
	node       NodeController
	mu         sync.Mutex
	lockTTL    time.Duration
	statusChan chan RedeployStatus
	// running is true only while a Deploy goroutine is actively executing IN
	// THIS process. It's the authoritative "is a redeploy live" signal for the
	// status poll: a persisted lock/log row from a crashed-or-restarted run does
	// NOT set it, so a stuck run is reported as failed immediately on restart
	// (not after the 30-min lock TTL), and a genuinely-long live run keeps
	// reporting running even if its lock TTL lapses.
	running atomic.Bool
}

// NewRedeployer creates a new redeployer.
func NewRedeployer(agentStore store.AgentStore, nodeCtrl NodeController, logger zerolog.Logger) *Redeployer {
	return &Redeployer{
		store:      agentStore,
		logger:     logger,
		node:       nodeCtrl,
		lockTTL:    30 * time.Minute,
		statusChan: make(chan RedeployStatus, 10),
	}
}

// StatusChan returns a channel that receives status updates during redeployment.
func (r *Redeployer) StatusChan() <-chan RedeployStatus {
	return r.statusChan
}

// Deploy executes the full redeployment state machine for adding or removing an agent.
func (r *Redeployer) Deploy(ctx context.Context, op Operation, agentID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.running.Store(true)
	defer r.running.Store(false)

	phases := []Phase{
		PhaseLockAcquired,
		PhaseBackupCreated,
		PhaseChainStopped,
		PhaseGenesisGenerated,
		PhaseChainStateWiped,
		PhaseChainRestarted,
		PhaseConsensusVerified,
		PhaseRBACConfigured,
		PhaseCompleted,
	}

	r.logger.Info().
		Str("operation", string(op)).
		Str("agent_id", agentID).
		Msg("starting chain redeployment")

	var lastLogID int64

	for _, phase := range phases {
		r.broadcastStatus(op, agentID, phase, StatusInProgress)

		entry := &store.RedeploymentLogEntry{
			Operation: string(op),
			AgentID:   agentID,
			Phase:     string(phase),
			Status:    string(StatusInProgress),
		}
		if err := r.store.InsertRedeployLog(ctx, entry); err != nil {
			r.logger.Error().Err(err).Str("phase", string(phase)).Msg("failed to insert redeploy log")
		}
		lastLogID = entry.ID

		err := r.executePhase(ctx, phase, op, agentID)
		if err != nil {
			r.logger.Error().Err(err).Str("phase", string(phase)).Msg("redeployment phase failed")
			_ = r.store.UpdateRedeployLog(ctx, lastLogID, string(StatusFailed), err.Error())
			r.broadcastStatus(op, agentID, phase, StatusFailed)

			// Attempt rollback
			if rollbackErr := r.rollback(ctx, phase, op, agentID); rollbackErr != nil {
				r.logger.Error().Err(rollbackErr).Msg("rollback failed")
			}
			return fmt.Errorf("redeployment failed at %s: %w", phase, err)
		}

		_ = r.store.UpdateRedeployLog(ctx, lastLogID, string(StatusCompleted), "")
		r.broadcastStatus(op, agentID, phase, StatusCompleted)
	}

	r.logger.Info().
		Str("operation", string(op)).
		Str("agent_id", agentID).
		Msg("chain redeployment completed successfully")

	return nil
}

func (r *Redeployer) executePhase(ctx context.Context, phase Phase, op Operation, agentID string) error {
	switch phase {
	case PhaseLockAcquired:
		return r.store.AcquireRedeployLock(ctx, agentID, string(op), r.lockTTL)

	case PhaseBackupCreated:
		return BackupSQLite(r.node.GetDataDir())

	case PhaseChainStopped:
		return r.node.StopChain()

	case PhaseGenesisGenerated:
		// Get all active agents to build validator set
		agents, err := r.store.ListAgents(ctx)
		if err != nil {
			return fmt.Errorf("list agents for genesis: %w", err)
		}
		var validators []ValidatorInfo
		for _, a := range agents {
			if a.Status == "active" || (a.Status == "pending" && a.AgentID == agentID && op == OpAddAgent) {
				// Only include agents that should be validators
				if a.Role == "observer" {
					continue
				}
				validators = append(validators, ValidatorInfo{
					Name:  a.Name,
					Power: 10,
				})
			}
		}
		return r.node.RegenerateGenesis(validators)

	case PhaseChainStateWiped:
		return r.node.WipeChainState()

	case PhaseChainRestarted:
		return r.node.StartChain()

	case PhaseConsensusVerified:
		// Wait for blocks to be produced (timeout 60s)
		return r.waitForConsensus(ctx, 60*time.Second)

	case PhaseRBACConfigured:
		// Update agent status to active
		if op == OpAddAgent {
			return r.store.UpdateAgentStatus(ctx, agentID, "active")
		}
		return nil

	case PhaseCompleted:
		return r.store.ReleaseRedeployLock(ctx)

	default:
		return fmt.Errorf("unknown phase: %s", phase)
	}
}

func (r *Redeployer) rollback(ctx context.Context, failedPhase Phase, op Operation, agentID string) error {
	r.logger.Warn().Str("from_phase", string(failedPhase)).Msg("rolling back redeployment")

	// Always try to release the lock
	defer func() {
		if err := r.store.ReleaseRedeployLock(ctx); err != nil {
			r.logger.Error().Err(err).Msg("failed to release lock during rollback")
		}
	}()

	switch failedPhase {
	case PhaseChainRestarted, PhaseConsensusVerified, PhaseRBACConfigured:
		// Try to restore and restart
		if restoreErr := RestoreSQLiteBackup(r.node.GetDataDir()); restoreErr != nil {
			r.logger.Error().Err(restoreErr).Msg("failed to restore SQLite backup")
		}
		if startErr := r.node.StartChain(); startErr != nil {
			r.logger.Error().Err(startErr).Msg("failed to restart chain after rollback")
			return startErr
		}
	case PhaseChainStopped, PhaseGenesisGenerated, PhaseChainStateWiped:
		// Try to restart chain from existing state
		if startErr := r.node.StartChain(); startErr != nil {
			r.logger.Error().Err(startErr).Msg("failed to restart chain after rollback")
			return startErr
		}
	}

	return nil
}

func (r *Redeployer) waitForConsensus(ctx context.Context, timeout time.Duration) error {
	// For v3.0 MVP, wait briefly and assume success.
	// TODO: Check actual block height via CometBFT RPC (height > 2).
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(3 * time.Second):
		r.logger.Info().Msg("consensus check — assuming success (MVP)")
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("consensus timeout after %s", timeout)
	}
}

func (r *Redeployer) broadcastStatus(op Operation, agentID string, phase Phase, status PhaseStatus) {
	select {
	case r.statusChan <- RedeployStatus{
		Active:    status == StatusInProgress,
		Operation: op,
		AgentID:   agentID,
		Phases:    []PhaseInfo{{Phase: phase, Status: status}},
	}:
	default:
		// Don't block if no one is listening
	}
}

// IsRedeploying returns true when a redeployment operation is actively in
// progress.  It satisfies the web.RedeployChecker interface so the redeploy
// guard middleware can gate write endpoints.
func (r *Redeployer) IsRedeploying() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	lock, err := r.store.GetRedeployLock(ctx)
	if err != nil {
		return false // no lock or error — not redeploying
	}
	return time.Now().Before(lock.ExpiresAt)
}

// RecoverStaleLock checks for and releases any expired redeployment locks
// that may have survived a crash.  Call this on startup before accepting
// traffic.  An unexpired lock is left alone — it may belong to a legitimate
// in-progress operation on another goroutine.
func (r *Redeployer) RecoverStaleLock(ctx context.Context) error {
	lock, err := r.store.GetRedeployLock(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil // no lock — nothing to recover
		}
		return fmt.Errorf("recover stale lock: get lock: %w", err)
	}

	if time.Now().After(lock.ExpiresAt) {
		r.logger.Warn().
			Str("locked_by", lock.LockedBy).
			Str("operation", lock.Operation).
			Time("locked_at", lock.LockedAt).
			Time("expires_at", lock.ExpiresAt).
			Msg("releasing stale redeployment lock from previous run")
		if err := r.store.ReleaseRedeployLock(ctx); err != nil {
			return fmt.Errorf("recover stale lock: release: %w", err)
		}
		return nil
	}

	r.logger.Info().
		Str("locked_by", lock.LockedBy).
		Str("operation", lock.Operation).
		Time("expires_at", lock.ExpiresAt).
		Msg("active redeployment lock found — not releasing (may be in-progress)")
	return nil
}

// DeployOp is a string-based wrapper around Deploy for use by the web layer
// (avoids the web package needing to import the Operation type).
func (r *Redeployer) DeployOp(ctx context.Context, op, agentID string) error {
	return r.Deploy(ctx, Operation(op), agentID)
}

// GetRedeployStatus returns the current redeployment status as simple values
// suitable for the web layer (no orchestrator types in the return).
func (r *Redeployer) GetRedeployStatus(ctx context.Context) (active bool, operation, agentID string, err error) {
	status, err := r.GetStatus(ctx)
	if err != nil {
		return false, "", "", err
	}
	return status.Active, string(status.Operation), status.AgentID, nil
}

// GetLiveStatus returns a status view suitable for the dashboard status poll:
// a coarse status string (running|completed|failed|idle), the current-or-last
// phase, and an error message when the run failed. Because the redeploy lock is
// released on BOTH success and failure, active==false alone cannot tell the two
// apart - so the terminal outcome is derived from the most-recent redeploy log
// entry. current_phase lets the frontend advance its phase checklist.
func (r *Redeployer) GetLiveStatus(ctx context.Context) (status, currentPhase, operation, agentID, errMsg string, err error) {
	lock, lockErr := r.GetStatus(ctx)
	if lockErr != nil {
		return "", "", "", "", "", lockErr
	}
	operation = string(lock.Operation)
	agentID = lock.AgentID

	latest, logErr := r.store.GetLatestRedeployLog(ctx)
	if logErr != nil {
		return "", "", "", "", "", logErr
	}
	if latest == nil {
		// No redeployment has ever run.
		return "idle", "", operation, agentID, "", nil
	}

	// When the lock is already released, carry the identity from the last run.
	if operation == "" {
		operation = latest.Operation
	}
	if agentID == "" {
		agentID = latest.AgentID
	}
	currentPhase = latest.Phase

	switch {
	case latest.Status == string(StatusFailed) || latest.Status == string(StatusRolledBack):
		status = "failed"
		errMsg = latest.Error
	case latest.Phase == string(PhaseCompleted):
		// COMPLETED only ever appears on the success path, so treat it as done
		// even in the brief window before its row flips in_progress->completed.
		status = "completed"
	case r.running.Load():
		// A Deploy goroutine is actively executing in THIS process — mid-flight,
		// whether the last row is in_progress or we're between phases. This is the
		// authoritative signal (not the lock TTL): a long real run keeps reporting
		// running even past 30 min, and a persisted lock from a dead/prior-process
		// run does NOT fake it.
		status = "running"
	default:
		// No live in-process run, yet the last recorded phase is a non-terminal
		// one that never reached COMPLETED — the run was abandoned (crash/kill/
		// restart). Report failed, NEVER running: without this a frozen in_progress
		// log row (e.g. a redeploy that bricked and was recovered out-of-band) would
		// wedge the "reconfiguration in progress" banner forever, since the derived
		// status has no TTL of its own. The genuine success window (COMPLETED row
		// still flipping from in_progress) is caught by the PhaseCompleted case
		// above, so this cannot misfire on a real completion.
		status = "failed"
		errMsg = "redeployment did not complete (stalled at " + latest.Phase + ")"
	}
	return status, currentPhase, operation, agentID, errMsg, nil
}

// QuickAgentOp applies an agent operation WITHOUT a chain redeployment — for a
// single-validator (personal) node, where every agent maps to the node's one
// validator key so the validator set never actually changes. A full redeploy
// (stop → regenerate genesis → WIPE state → restart) is therefore a destructive
// no-op that has bricked personal nodes; the only meaningful step is the
// app-level RBAC update (mark an added agent active). remove/rotate are already
// handled by their own handlers. Writes a COMPLETED log so the status poll reads
// clean and no "reconfiguration in progress" banner is left behind.
func (r *Redeployer) QuickAgentOp(ctx context.Context, op, agentID string) error {
	if Operation(op) == OpAddAgent {
		if err := r.store.UpdateAgentStatus(ctx, agentID, "active"); err != nil {
			return fmt.Errorf("activate agent: %w", err)
		}
	}
	entry := &store.RedeploymentLogEntry{
		Operation: op,
		AgentID:   agentID,
		Phase:     string(PhaseCompleted),
		Status:    string(StatusCompleted),
	}
	if err := r.store.InsertRedeployLog(ctx, entry); err != nil {
		r.logger.Warn().Err(err).Msg("quick agent op: failed to log completion (harmless)")
	}
	r.logger.Info().Str("operation", op).Str("agent_id", agentID).
		Msg("agent operation applied without chain redeployment (single-node network)")
	return nil
}

// ClearStale clears a stuck/abandoned redeployment. It refuses when a run is
// genuinely live (lock held and not expired); otherwise it releases any stale
// lock and marks lingering in_progress log rows terminal so the status poll
// stops reporting "running". Returns the number of log rows cleared (0 = nothing
// was stuck). Idempotent and safe to call anytime.
func (r *Redeployer) ClearStale(ctx context.Context) (int, error) {
	status, err := r.GetStatus(ctx) // TTL-checked; auto-releases an expired lock
	if err != nil {
		return 0, err
	}
	if status.Active {
		return 0, fmt.Errorf("a redeployment is currently in progress — cannot clear")
	}
	_ = r.store.ReleaseRedeployLock(ctx) // usually already gone; belt-and-suspenders
	return r.store.ClearStaleRedeployLogs(ctx)
}

// GetStatus returns the current redeployment status.
func (r *Redeployer) GetStatus(ctx context.Context) (*RedeployStatus, error) {
	lock, err := r.store.GetRedeployLock(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return &RedeployStatus{Active: false}, nil
		}
		return nil, err
	}

	// Check if expired
	if time.Now().After(lock.ExpiresAt) {
		_ = r.store.ReleaseRedeployLock(ctx)
		return &RedeployStatus{Active: false}, nil
	}

	return &RedeployStatus{
		Active:    true,
		Operation: Operation(lock.Operation),
		AgentID:   lock.LockedBy,
	}, nil
}
