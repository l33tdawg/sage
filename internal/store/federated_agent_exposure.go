package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
)

const (
	// FederatedAgentExposureModeAll advertises every eligible ordinary local
	// agent to this connection. It is the documented default for a connection
	// that has never been configured.
	FederatedAgentExposureModeAll = "all"
	// FederatedAgentExposureModeSelected advertises exactly the listed agents.
	FederatedAgentExposureModeSelected = "selected"
	// FederatedAgentExposureModeNone advertises no agent at all: the peer's
	// directory listing and name search return nothing for this connection.
	FederatedAgentExposureModeNone = "none"

	// MaxFederatedAgentExposureAgents bounds one explicit allow list. It matches
	// the agent-export cap so a connection cannot describe a two-tier roster the
	// other surfaces would refuse.
	MaxFederatedAgentExposureAgents = 256
)

var (
	ErrFederatedAgentExposureRevisionConflict = errors.New("federated agent exposure revision conflict")
	ErrFederatedAgentExposureBindingMismatch  = errors.New("federated agent exposure agreement binding mismatch")
)

// FederatedAgentExposureBinding is the exact live trust generation supplied by
// the federation Manager. Browser/API callers must not choose these fields.
// It is the same four-part agreement binding every other connection-scoped
// snapshot uses, so a re-pair retires the previous policy by construction and
// the federation Manager can hand the binding it already resolved straight to
// each sibling reset.
type FederatedAgentExposureBinding = FederatedReaderBinding

// FederatedAgentExposure is the node-local, connection-scoped policy that
// decides which of this node's ordinary agents the peer may discover for
// messaging on this exact connection. It governs discovery only: the peer's
// directory listing, its paging, and its exact-name/id search. It never grants
// or removes memory Read (that is FederatedAgentExport plus manual peer RBAC)
// and it never authorizes delivery (that is the per-agent acceptance switch
// plus the agent's own deny_federated_pipe capability).
//
// An absent row is the documented default: mode=all with Configured=false.
type FederatedAgentExposure struct {
	RemoteChainID string   `json:"remote_chain_id"`
	PeerAgentID   string   `json:"peer_agent_id"`
	PolicyEpoch   string   `json:"policy_epoch"`
	RemoteCAPin   string   `json:"remote_ca_pin"`
	Mode          string   `json:"mode"`
	AgentIDs      []string `json:"agent_ids"`
	Revision      int64    `json:"revision"`
	// Configured is false only for the synthesized default. A configured row
	// always carries a positive revision so compare-and-swap stays monotonic.
	Configured bool `json:"configured"`
}

func (s *SQLiteStore) migrateFederatedAgentExposure(ctx context.Context) error {
	if _, err := s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS federated_agent_exposure (
		remote_chain_id TEXT NOT NULL,
		peer_agent_id   TEXT NOT NULL,
		policy_epoch    TEXT NOT NULL,
		remote_ca_pin   TEXT NOT NULL,
		mode            TEXT NOT NULL CHECK (mode IN ('all','selected','none')),
		revision        INTEGER NOT NULL CHECK (revision > 0),
		updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		PRIMARY KEY (remote_chain_id)
	)`); err != nil {
		return fmt.Errorf("create federated agent exposure table: %w", err)
	}
	if _, err := s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS federated_agent_exposure_agent (
		remote_chain_id TEXT NOT NULL,
		agent_id        TEXT NOT NULL,
		PRIMARY KEY (remote_chain_id, agent_id),
		FOREIGN KEY (remote_chain_id)
			REFERENCES federated_agent_exposure(remote_chain_id)
			ON DELETE CASCADE
	)`); err != nil {
		return fmt.Errorf("create federated agent exposure agent table: %w", err)
	}
	return nil
}

func canonicalFederatedAgentExposureBinding(binding FederatedAgentExposureBinding) error {
	if binding.RemoteChainID == "" || binding.RemoteChainID != strings.TrimSpace(binding.RemoteChainID) ||
		len(binding.RemoteChainID) > 256 || strings.ContainsAny(binding.RemoteChainID, "\x00\r\n") {
		return errors.New("federated agent exposure remote chain id is invalid")
	}
	if !isCanonicalAgentID(binding.PeerAgentID) {
		return errors.New("federated agent exposure peer agent id must be canonical")
	}
	if binding.PolicyEpoch == "" || binding.PolicyEpoch != strings.TrimSpace(binding.PolicyEpoch) ||
		len(binding.PolicyEpoch) > 256 || strings.ContainsAny(binding.PolicyEpoch, "\x00\r\n") {
		return errors.New("federated agent exposure policy epoch is invalid")
	}
	pin, err := hex.DecodeString(binding.RemoteCAPin)
	if err != nil || len(pin) == 0 || binding.RemoteCAPin != strings.ToLower(binding.RemoteCAPin) {
		return errors.New("federated agent exposure remote CA pin must be canonical non-empty hex")
	}
	return nil
}

// canonicalFederatedAgentExposure validates one complete policy snapshot. The
// allow list is a complete, sorted, de-duplicated set: modes all and none carry
// no agents, and selected carries at least one, so there is exactly one way to
// express each intent.
func canonicalFederatedAgentExposure(in FederatedAgentExposure) (FederatedAgentExposure, error) {
	binding := FederatedAgentExposureBinding{
		RemoteChainID: in.RemoteChainID, PeerAgentID: in.PeerAgentID,
		PolicyEpoch: in.PolicyEpoch, RemoteCAPin: in.RemoteCAPin,
	}
	if err := canonicalFederatedAgentExposureBinding(binding); err != nil {
		return FederatedAgentExposure{}, err
	}
	switch in.Mode {
	case FederatedAgentExposureModeAll, FederatedAgentExposureModeNone:
		if len(in.AgentIDs) != 0 {
			return FederatedAgentExposure{}, errors.New("federated agent exposure mode must not carry an agent list")
		}
		in.AgentIDs = []string{}
	case FederatedAgentExposureModeSelected:
		if len(in.AgentIDs) == 0 {
			return FederatedAgentExposure{}, errors.New("federated agent exposure selected mode requires at least one agent")
		}
	default:
		return FederatedAgentExposure{}, errors.New("federated agent exposure mode is invalid")
	}
	if len(in.AgentIDs) > MaxFederatedAgentExposureAgents {
		return FederatedAgentExposure{}, fmt.Errorf(
			"federated agent exposure is capped at %d agents", MaxFederatedAgentExposureAgents)
	}
	seen := make(map[string]struct{}, len(in.AgentIDs))
	agents := make([]string, 0, len(in.AgentIDs))
	for _, agentID := range in.AgentIDs {
		if !isCanonicalAgentID(agentID) || agentID != strings.ToLower(agentID) {
			return FederatedAgentExposure{}, errors.New("federated agent exposure agent id must be canonical")
		}
		if _, duplicate := seen[agentID]; duplicate {
			continue
		}
		seen[agentID] = struct{}{}
		agents = append(agents, agentID)
	}
	sort.Strings(agents)
	in.AgentIDs = agents
	if in.Revision <= 0 {
		return FederatedAgentExposure{}, errors.New("federated agent exposure revision must be positive")
	}
	if len(in.AgentIDs) == 0 && in.Mode == FederatedAgentExposureModeSelected {
		return FederatedAgentExposure{}, errors.New("federated agent exposure selected mode requires at least one agent")
	}
	return in, nil
}

func (s *SQLiteStore) requireFederatedAgentExposureSyncControl(
	ctx context.Context, binding FederatedAgentExposureBinding,
) error {
	var peer, epoch, pin, state string
	err := s.conn.QueryRowContext(ctx, `
		SELECT peer_agent_id, policy_epoch, remote_ca_pin, binding_state
		FROM sync_control WHERE remote_chain_id=?`, binding.RemoteChainID).
		Scan(&peer, &epoch, &pin, &state)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("%w: active sync control is required", ErrFederatedAgentExposureBindingMismatch)
	case err != nil:
		return fmt.Errorf("read federated agent exposure sync control: %w", err)
	case state != "active" || peer != binding.PeerAgentID || epoch != binding.PolicyEpoch || pin != binding.RemoteCAPin:
		return ErrFederatedAgentExposureBindingMismatch
	default:
		return nil
	}
}

// GetFederatedAgentExposure returns the configured policy, or the documented
// default (mode=all, Configured=false) when this connection has never been
// configured. A stored row whose frozen generation no longer matches the live
// agreement fails closed rather than silently re-applying an operator's old
// allow list to a new trust generation; the re-pair reset removes such rows.
func (s *SQLiteStore) GetFederatedAgentExposure(
	ctx context.Context, binding FederatedAgentExposureBinding,
) (*FederatedAgentExposure, error) {
	if err := canonicalFederatedAgentExposureBinding(binding); err != nil {
		return nil, err
	}
	if err := s.requireFederatedAgentExposureSyncControl(ctx, binding); err != nil {
		return nil, err
	}
	var out FederatedAgentExposure
	err := s.conn.QueryRowContext(ctx, `
		SELECT remote_chain_id, peer_agent_id, policy_epoch, remote_ca_pin, mode, revision
		FROM federated_agent_exposure WHERE remote_chain_id=?`, binding.RemoteChainID).
		Scan(&out.RemoteChainID, &out.PeerAgentID, &out.PolicyEpoch, &out.RemoteCAPin,
			&out.Mode, &out.Revision)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return &FederatedAgentExposure{
			RemoteChainID: binding.RemoteChainID,
			PeerAgentID:   binding.PeerAgentID,
			PolicyEpoch:   binding.PolicyEpoch,
			RemoteCAPin:   binding.RemoteCAPin,
			Mode:          FederatedAgentExposureModeAll,
			AgentIDs:      []string{},
			Configured:    false,
		}, nil
	case err != nil:
		return nil, fmt.Errorf("read federated agent exposure: %w", err)
	}
	if out.PeerAgentID != binding.PeerAgentID || out.PolicyEpoch != binding.PolicyEpoch ||
		out.RemoteCAPin != binding.RemoteCAPin {
		return nil, ErrFederatedAgentExposureBindingMismatch
	}
	out.Configured = true
	out.AgentIDs = []string{}
	if out.Mode == FederatedAgentExposureModeSelected {
		rows, queryErr := s.conn.QueryContext(ctx, `
			SELECT agent_id FROM federated_agent_exposure_agent
			WHERE remote_chain_id=? ORDER BY agent_id`, binding.RemoteChainID)
		if queryErr != nil {
			return nil, fmt.Errorf("read federated agent exposure agents: %w", queryErr)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var agentID string
			if scanErr := rows.Scan(&agentID); scanErr != nil {
				return nil, fmt.Errorf("scan federated agent exposure agent: %w", scanErr)
			}
			out.AgentIDs = append(out.AgentIDs, agentID)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			return nil, fmt.Errorf("iterate federated agent exposure agents: %w", rowsErr)
		}
	}
	canonical, err := canonicalFederatedAgentExposure(out)
	if err != nil {
		return nil, err
	}
	return &canonical, nil
}

// SetBoundFederatedAgentExposure commits a complete exposure snapshot when the
// stored revision still equals expectedRevision and the exact active JOIN
// generation is present in the same transaction. The allow list is replaced
// wholesale; there is no per-agent partial update, so a stale browser cannot
// merge a decision made against a different roster.
func (s *SQLiteStore) SetBoundFederatedAgentExposure(
	ctx context.Context, binding FederatedAgentExposureBinding, mode string,
	agentIDs []string, expectedRevision int64,
) (*FederatedAgentExposure, error) {
	if s != nil && s.db == nil {
		return nil, errors.New("federated agent exposure mutation is not permitted inside SQLite transaction")
	}
	canonical, err := canonicalFederatedAgentExposure(FederatedAgentExposure{
		RemoteChainID: binding.RemoteChainID,
		PeerAgentID:   binding.PeerAgentID,
		PolicyEpoch:   binding.PolicyEpoch,
		RemoteCAPin:   binding.RemoteCAPin,
		Mode:          mode,
		AgentIDs:      agentIDs,
		Revision:      expectedRevision + 1,
	})
	if err != nil {
		return nil, err
	}
	if expectedRevision < 0 {
		return nil, fmt.Errorf("%w: expected revision must not be negative", ErrFederatedAgentExposureRevisionConflict)
	}

	releaseAuthorization := s.beginFederationAuthorizationMutation(canonical.RemoteChainID)
	defer releaseAuthorization()
	unlock := s.LockSyncPolicyWrite()
	defer unlock()

	err = s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		var peer, epoch, pin, state string
		controlErr := tx.conn.QueryRowContext(ctx, `
			SELECT peer_agent_id, policy_epoch, remote_ca_pin, binding_state
			FROM sync_control WHERE remote_chain_id=?`, canonical.RemoteChainID).
			Scan(&peer, &epoch, &pin, &state)
		switch {
		case errors.Is(controlErr, sql.ErrNoRows):
			return fmt.Errorf("%w: active sync control is required", ErrFederatedAgentExposureBindingMismatch)
		case controlErr != nil:
			return fmt.Errorf("read federated agent exposure sync control: %w", controlErr)
		case state != "active" || peer != canonical.PeerAgentID || epoch != canonical.PolicyEpoch ||
			pin != canonical.RemoteCAPin:
			return ErrFederatedAgentExposureBindingMismatch
		}

		var currentRevision int64
		var currentPeer, currentEpoch, currentCAPin string
		getErr := tx.conn.QueryRowContext(ctx, `
			SELECT revision, peer_agent_id, policy_epoch, remote_ca_pin
			FROM federated_agent_exposure WHERE remote_chain_id=?`, canonical.RemoteChainID).
			Scan(&currentRevision, &currentPeer, &currentEpoch, &currentCAPin)
		switch {
		case errors.Is(getErr, sql.ErrNoRows) && expectedRevision != 0:
			return ErrFederatedAgentExposureRevisionConflict
		case getErr == nil && currentRevision != expectedRevision:
			return ErrFederatedAgentExposureRevisionConflict
		case getErr == nil && (currentPeer != canonical.PeerAgentID || currentEpoch != canonical.PolicyEpoch ||
			currentCAPin != canonical.RemoteCAPin):
			return ErrFederatedAgentExposureBindingMismatch
		case getErr != nil && !errors.Is(getErr, sql.ErrNoRows):
			return fmt.Errorf("read federated agent exposure revision: %w", getErr)
		}

		if _, execErr := tx.conn.ExecContext(ctx, `
			INSERT INTO federated_agent_exposure (
				remote_chain_id, peer_agent_id, policy_epoch, remote_ca_pin,
				mode, revision, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
			ON CONFLICT(remote_chain_id) DO UPDATE SET
				peer_agent_id=excluded.peer_agent_id,
				policy_epoch=excluded.policy_epoch,
				remote_ca_pin=excluded.remote_ca_pin,
				mode=excluded.mode,
				revision=excluded.revision,
				updated_at=excluded.updated_at`,
			canonical.RemoteChainID, canonical.PeerAgentID, canonical.PolicyEpoch,
			canonical.RemoteCAPin, canonical.Mode, canonical.Revision); execErr != nil {
			return fmt.Errorf("upsert federated agent exposure: %w", execErr)
		}
		if _, execErr := tx.conn.ExecContext(ctx, `
			DELETE FROM federated_agent_exposure_agent WHERE remote_chain_id=?`,
			canonical.RemoteChainID); execErr != nil {
			return fmt.Errorf("replace federated agent exposure agents: %w", execErr)
		}
		for _, agentID := range canonical.AgentIDs {
			if _, execErr := tx.conn.ExecContext(ctx, `
				INSERT INTO federated_agent_exposure_agent (remote_chain_id, agent_id)
				VALUES (?, ?)`, canonical.RemoteChainID, agentID); execErr != nil {
				return fmt.Errorf("insert federated agent exposure agent %q: %w", agentID, execErr)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.GetFederatedAgentExposure(ctx, binding)
}

// ResetFederatedAgentExposureForBinding is the controlled re-pair reset. It
// removes the retired generation's row, so a fresh connection starts at the
// documented default instead of inheriting an allow list from an agreement
// whose epoch, pin, or peer identity has been replaced.
func (s *SQLiteStore) ResetFederatedAgentExposureForBinding(
	ctx context.Context, binding FederatedAgentExposureBinding,
) (int64, error) {
	if err := canonicalFederatedAgentExposureBinding(binding); err != nil {
		return 0, err
	}
	if err := s.requireFederatedAgentExposureSyncControl(ctx, binding); err != nil {
		return 0, err
	}
	result, err := s.writeExecContext(ctx, `
		DELETE FROM federated_agent_exposure WHERE remote_chain_id=?`, binding.RemoteChainID)
	if err != nil {
		return 0, fmt.Errorf("reset federated agent exposure: %w", err)
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count reset federated agent exposures: %w", err)
	}
	return removed, nil
}
