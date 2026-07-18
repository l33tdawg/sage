package store

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

const maxFederatedPipeRemoteContactSnapshotBytes = 4 << 20

// migrateFederatedPipeContacts creates the local operator-consent ledger for
// inbound work requests. A row is deliberately bound to the exact JOIN
// generation and peer-RBAC revision that exposed the contact. It is not a
// transport queue and does not authorize delivery by itself.
func (s *SQLiteStore) migrateFederatedPipeContacts(ctx context.Context) {
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS fed_pipe_contact_acceptance (
		remote_chain_id TEXT NOT NULL,
		peer_agent_id   TEXT NOT NULL,
		policy_epoch    TEXT NOT NULL,
		remote_ca_pin   TEXT NOT NULL,
		policy_revision INTEGER NOT NULL CHECK (policy_revision > 0),
		local_agent_id  TEXT NOT NULL,
		contact_id      TEXT NOT NULL,
		updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		PRIMARY KEY (remote_chain_id, local_agent_id)
	)`)
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS fed_pipe_remote_contact_snapshot (
		remote_chain_id       TEXT PRIMARY KEY,
		peer_agent_id         TEXT NOT NULL,
		policy_epoch          TEXT NOT NULL,
		remote_ca_pin         TEXT NOT NULL,
		remote_policy_version INTEGER NOT NULL,
		remote_policy_revision INTEGER NOT NULL,
		remote_policy_hash    TEXT NOT NULL,
		local_agreement_id    TEXT NOT NULL,
		remote_agreement_id   TEXT NOT NULL,
		contact_revision      TEXT NOT NULL,
		snapshot              TEXT NOT NULL,
		updated_at            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	)`)
}

// FederatedPipeRemoteContactSnapshot is an encrypted, authenticated routing
// hint learned from a peer's status response. It is never delivery authority:
// the federation layer must still require a fresh live snapshot before sending
// payload bytes. Every read is bound to the exact local agreement + JOIN/sync
// generation so stale rows are invisible after revoke, re-pair, or replacement.
type FederatedPipeRemoteContactSnapshot struct {
	RemoteChainID        string
	PeerAgentID          string
	PolicyEpoch          string
	RemoteCAPin          string
	RemotePolicyVersion  int
	RemotePolicyRevision int64
	RemotePolicyHash     string
	LocalAgreementID     string
	RemoteAgreementID    string
	ContactRevision      string
	Snapshot             []byte
}

func validateFederatedPipeRemoteContactSnapshot(snapshot FederatedPipeRemoteContactSnapshot) error {
	if snapshot.RemoteChainID == "" || strings.TrimSpace(snapshot.RemoteChainID) != snapshot.RemoteChainID ||
		snapshot.PolicyEpoch == "" || snapshot.LocalAgreementID == "" {
		return fmt.Errorf("remote pipe contact snapshot binding is incomplete")
	}
	if err := validateFederatedPipeAgentID(snapshot.PeerAgentID); err != nil {
		return fmt.Errorf("remote pipe contact peer id: %w", err)
	}
	if err := validateFederatedPipeContactID(snapshot.RemoteCAPin); err != nil {
		return fmt.Errorf("remote pipe contact CA pin: %w", err)
	}
	for label, digest := range map[string]string{
		"local agreement":  snapshot.LocalAgreementID,
		"remote agreement": snapshot.RemoteAgreementID,
		"contact revision": snapshot.ContactRevision,
	} {
		if err := validateFederatedPipeContactID(digest); err != nil {
			return fmt.Errorf("remote pipe contact %s: %w", label, err)
		}
	}
	if len(snapshot.Snapshot) == 0 || len(snapshot.Snapshot) > maxFederatedPipeRemoteContactSnapshotBytes {
		return fmt.Errorf("remote pipe contact snapshot size is invalid")
	}
	return nil
}

// PutFederatedPipeRemoteContactSnapshot atomically upserts only if the exact
// active sync_control binding still exists. A concurrent revoke/re-pair either
// wins before this statement (zero rows) or leaves a cache row whose binding no
// longer joins current control and therefore cannot be read.
func (s *SQLiteStore) PutFederatedPipeRemoteContactSnapshot(ctx context.Context, snapshot FederatedPipeRemoteContactSnapshot) error {
	if err := validateFederatedPipeRemoteContactSnapshot(snapshot); err != nil {
		return err
	}
	encrypted, err := s.encryptContent(string(snapshot.Snapshot))
	if err != nil {
		return fmt.Errorf("encrypt remote pipe contact snapshot: %w", err)
	}
	result, err := s.writeExecContext(ctx, `
		INSERT INTO fed_pipe_remote_contact_snapshot
			(remote_chain_id, peer_agent_id, policy_epoch, remote_ca_pin,
			 remote_policy_version, remote_policy_revision, remote_policy_hash,
			 local_agreement_id, remote_agreement_id, contact_revision, snapshot)
		SELECT c.remote_chain_id, c.peer_agent_id, c.policy_epoch, c.remote_ca_pin,
			c.remote_policy_version, c.remote_revision, c.remote_policy_hash,
			?,?,?,?
		FROM sync_control c
		WHERE c.remote_chain_id=? AND c.peer_agent_id=? AND c.policy_epoch=?
			AND c.remote_ca_pin=? AND c.binding_state='active'
			AND c.remote_policy_version=? AND c.remote_revision=? AND c.remote_policy_hash=?
		ON CONFLICT(remote_chain_id) DO UPDATE SET
			peer_agent_id=excluded.peer_agent_id,
			policy_epoch=excluded.policy_epoch,
			remote_ca_pin=excluded.remote_ca_pin,
			remote_policy_version=excluded.remote_policy_version,
			remote_policy_revision=excluded.remote_policy_revision,
			remote_policy_hash=excluded.remote_policy_hash,
			local_agreement_id=excluded.local_agreement_id,
			remote_agreement_id=excluded.remote_agreement_id,
			contact_revision=excluded.contact_revision,
			snapshot=excluded.snapshot,
			updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')`,
		snapshot.LocalAgreementID, snapshot.RemoteAgreementID, snapshot.ContactRevision, encrypted,
		snapshot.RemoteChainID, snapshot.PeerAgentID, snapshot.PolicyEpoch,
		snapshot.RemoteCAPin, snapshot.RemotePolicyVersion, snapshot.RemotePolicyRevision, snapshot.RemotePolicyHash)
	if err != nil {
		return fmt.Errorf("store remote pipe contact snapshot: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check remote pipe contact snapshot binding: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("%w: active sync binding changed while caching remote contacts", ErrPeerRBACBindingMismatch)
	}
	return nil
}

// GetFederatedPipeRemoteContactSnapshot returns a cache row only when every
// current sync_control field and the caller-provided local agreement digest
// still match. Explicit nil means no usable authenticated cache.
func (s *SQLiteStore) GetFederatedPipeRemoteContactSnapshot(ctx context.Context, control SyncControl, localAgreementID string) (*FederatedPipeRemoteContactSnapshot, error) {
	if control.BindingState != "active" || localAgreementID == "" {
		return nil, nil
	}
	var out FederatedPipeRemoteContactSnapshot
	var encrypted string
	err := s.conn.QueryRowContext(ctx, `
		SELECT s.remote_chain_id, s.peer_agent_id, s.policy_epoch, s.remote_ca_pin,
			s.remote_policy_version, s.remote_policy_revision, s.remote_policy_hash,
			s.local_agreement_id, s.remote_agreement_id, s.contact_revision, s.snapshot
		FROM fed_pipe_remote_contact_snapshot s
		JOIN sync_control c ON c.remote_chain_id=s.remote_chain_id
			AND c.peer_agent_id=s.peer_agent_id AND c.policy_epoch=s.policy_epoch
			AND c.remote_ca_pin=s.remote_ca_pin AND c.binding_state='active'
			AND c.remote_policy_version=s.remote_policy_version
			AND c.remote_revision=s.remote_policy_revision
			AND c.remote_policy_hash=s.remote_policy_hash
		WHERE s.remote_chain_id=? AND s.peer_agent_id=? AND s.policy_epoch=?
			AND s.remote_ca_pin=? AND s.remote_policy_version=?
			AND s.remote_policy_revision=? AND s.remote_policy_hash=?
			AND s.local_agreement_id=?`, control.RemoteChainID, control.PeerAgentID,
		control.PolicyEpoch, control.RemoteCAPin, control.RemotePolicyVersion,
		control.RemoteRevision, control.RemotePolicyHash, localAgreementID).
		Scan(&out.RemoteChainID, &out.PeerAgentID, &out.PolicyEpoch, &out.RemoteCAPin,
			&out.RemotePolicyVersion, &out.RemotePolicyRevision, &out.RemotePolicyHash,
			&out.LocalAgreementID, &out.RemoteAgreementID, &out.ContactRevision, &encrypted)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read remote pipe contact snapshot: %w", err)
	}
	plaintext, err := s.decryptContent(encrypted)
	if err != nil || plaintext == VaultLockedPlaceholder {
		return nil, fmt.Errorf("%w: remote pipe contact snapshot is encrypted", ErrPipeContentUnavailable)
	}
	out.Snapshot = []byte(plaintext)
	if err := validateFederatedPipeRemoteContactSnapshot(out); err != nil {
		return nil, fmt.Errorf("stored remote pipe contact snapshot is invalid: %w", err)
	}
	return &out, nil
}

func (s *SQLiteStore) DeleteFederatedPipeRemoteContactSnapshot(ctx context.Context, remoteChainID string) error {
	_, err := s.writeExecContext(ctx, `DELETE FROM fed_pipe_remote_contact_snapshot WHERE remote_chain_id=?`, remoteChainID)
	return err
}

// DeleteFederatedPipeRemoteContactSnapshotBound invalidates only the cache row
// learned under the caller's immutable request-time binding. A delayed response
// from an old policy/JOIN generation can therefore never delete a newer row for
// the same chain.
func (s *SQLiteStore) DeleteFederatedPipeRemoteContactSnapshotBound(ctx context.Context, control SyncControl, localAgreementID string) error {
	if control.RemoteChainID == "" || control.PeerAgentID == "" || control.PolicyEpoch == "" ||
		control.RemoteCAPin == "" || localAgreementID == "" {
		return fmt.Errorf("remote pipe contact snapshot delete binding is incomplete")
	}
	_, err := s.writeExecContext(ctx, `
		DELETE FROM fed_pipe_remote_contact_snapshot
		WHERE remote_chain_id=? AND peer_agent_id=? AND policy_epoch=? AND remote_ca_pin=?
			AND remote_policy_version=? AND remote_policy_revision=?
			AND remote_policy_hash=? AND local_agreement_id=?`,
		control.RemoteChainID, control.PeerAgentID, control.PolicyEpoch, control.RemoteCAPin,
		control.RemotePolicyVersion, control.RemoteRevision, control.RemotePolicyHash, localAgreementID)
	return err
}

func validateFederatedPipeContactBinding(policy PeerRBACPolicy) error {
	if err := validatePeerRBACPolicyHeader(policy); err != nil {
		return err
	}
	if policy.Revision <= 0 {
		return fmt.Errorf("peer RBAC revision must be positive")
	}
	return nil
}

func validateFederatedPipeAgentID(agentID string) error {
	decoded, err := hex.DecodeString(agentID)
	if err != nil || len(decoded) != ed25519.PublicKeySize {
		return fmt.Errorf("local agent id must be a 64-hex Ed25519 public key")
	}
	return nil
}

func validateFederatedPipeContactID(contactID string) error {
	decoded, err := hex.DecodeString(contactID)
	if err != nil || len(decoded) != 32 {
		return fmt.Errorf("pipe contact id must be a 64-hex digest")
	}
	return nil
}

// GetFederatedPipeContactAcceptances returns only consent rows matching the
// caller's exact current policy binding. Stale rows are invisible and therefore
// fail closed even before asynchronous cleanup runs.
func (s *SQLiteStore) GetFederatedPipeContactAcceptances(ctx context.Context, policy PeerRBACPolicy) (map[string]string, error) {
	if err := validateFederatedPipeContactBinding(policy); err != nil {
		return nil, err
	}
	rows, err := s.conn.QueryContext(ctx, `
		SELECT local_agent_id, contact_id
		FROM fed_pipe_contact_acceptance
		WHERE remote_chain_id=? AND peer_agent_id=? AND policy_epoch=?
			AND remote_ca_pin=? AND policy_revision=?
		ORDER BY local_agent_id`, policy.RemoteChainID, policy.PeerAgentID,
		policy.PolicyEpoch, policy.RemoteCAPin, policy.Revision)
	if err != nil {
		return nil, fmt.Errorf("list federated pipe contact acceptances: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[string]string)
	for rows.Next() {
		var agentID, contactID string
		if err := rows.Scan(&agentID, &contactID); err != nil {
			return nil, fmt.Errorf("scan federated pipe contact acceptance: %w", err)
		}
		if err := validateFederatedPipeAgentID(agentID); err != nil {
			return nil, fmt.Errorf("stored federated pipe contact acceptance is invalid: %w", err)
		}
		if err := validateFederatedPipeContactID(contactID); err != nil {
			return nil, fmt.Errorf("stored federated pipe contact acceptance is invalid: %w", err)
		}
		out[agentID] = contactID
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate federated pipe contact acceptances: %w", err)
	}
	return out, nil
}

// SetBoundFederatedPipeContactAcceptance updates one local contact's inbound
// work-request consent only while the exact policy and JOIN binding remain
// active. The manager separately proves that contactID is the current
// chain-derived owner projection; the post-update re-derivation closes an owner
// transfer race without trusting this off-consensus table as identity.
func (s *SQLiteStore) SetBoundFederatedPipeContactAcceptance(ctx context.Context, policy PeerRBACPolicy, localAgentID, contactID string, accepting bool) error {
	if err := validateFederatedPipeContactBinding(policy); err != nil {
		return err
	}
	if err := validateFederatedPipeAgentID(localAgentID); err != nil {
		return err
	}
	if err := validateFederatedPipeContactID(contactID); err != nil {
		return err
	}

	unlock := s.LockSyncPolicyWrite()
	defer unlock()
	return s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		var currentPeer, currentEpoch, currentCAPin, controlState string
		var currentVersion int
		var currentRevision int64
		err := tx.conn.QueryRowContext(ctx, `
			SELECT p.peer_agent_id, p.policy_epoch, p.remote_ca_pin,
				p.policy_version, p.revision, c.binding_state
			FROM peer_rbac_policy p
			JOIN sync_control c ON c.remote_chain_id=p.remote_chain_id
				AND c.peer_agent_id=p.peer_agent_id
				AND c.policy_epoch=p.policy_epoch
				AND c.remote_ca_pin=p.remote_ca_pin
			WHERE p.remote_chain_id=?`, policy.RemoteChainID).
			Scan(&currentPeer, &currentEpoch, &currentCAPin, &currentVersion, &currentRevision, &controlState)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: active peer RBAC policy is required", ErrPeerRBACBindingMismatch)
		}
		if err != nil {
			return fmt.Errorf("read federated pipe contact binding: %w", err)
		}
		if controlState != "active" || currentPeer != policy.PeerAgentID ||
			currentEpoch != policy.PolicyEpoch || currentCAPin != policy.RemoteCAPin ||
			currentVersion != policy.PolicyVersion || currentRevision != policy.Revision {
			return fmt.Errorf("%w: peer RBAC policy changed during pipe contact update", ErrPeerRBACBindingMismatch)
		}

		if !accepting {
			if _, err := tx.writeExecContext(ctx, `DELETE FROM fed_pipe_contact_acceptance
				WHERE remote_chain_id=? AND local_agent_id=?`, policy.RemoteChainID, localAgentID); err != nil {
				return fmt.Errorf("disable federated pipe contact acceptance: %w", err)
			}
			return nil
		}
		if _, err := tx.writeExecContext(ctx, `
			INSERT INTO fed_pipe_contact_acceptance
				(remote_chain_id, peer_agent_id, policy_epoch, remote_ca_pin,
				 policy_revision, local_agent_id, contact_id)
			VALUES(?,?,?,?,?,?,?)
			ON CONFLICT(remote_chain_id, local_agent_id) DO UPDATE SET
				peer_agent_id=excluded.peer_agent_id,
				policy_epoch=excluded.policy_epoch,
				remote_ca_pin=excluded.remote_ca_pin,
				policy_revision=excluded.policy_revision,
				contact_id=excluded.contact_id,
				updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')`,
			policy.RemoteChainID, policy.PeerAgentID, policy.PolicyEpoch, policy.RemoteCAPin,
			policy.Revision, localAgentID, contactID); err != nil {
			return fmt.Errorf("enable federated pipe contact acceptance: %w", err)
		}
		return nil
	})
}
