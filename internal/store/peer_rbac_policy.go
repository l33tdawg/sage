package store

// Directional peer-RBAC is an off-consensus, full-snapshot capability
// projection. The header is deliberately separate from the domain rows: a
// present header with zero rows is an explicit deny-all policy, while no header
// means a legacy/unconfigured connection whose tx-33 scope remains authoritative.

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode"
)

const (
	CurrentPeerRBACPolicyVersion = 1
	MaxPeerRBACPolicyDomains     = 1024
	maxPeerRBACDomainBytes       = 512
	// peerRBACSyncPolicyVersion is the sync_control cutover marker. It lives in
	// store to avoid an import cycle with internal/federation, whose public v3
	// constant has the same wire value.
	peerRBACSyncPolicyVersion = 3
)

var (
	ErrPeerRBACBindingMismatch = errors.New("peer RBAC policy binding mismatch")
	// ErrPeerRBACWriteCapabilityUnavailable prevents an off-consensus peer
	// policy from becoming reusable public submit authority. Read and Copy remain
	// available; Write needs a future consensus-bound ingress capability.
	ErrPeerRBACWriteCapabilityUnavailable = errors.New("peer Write requires a consensus-bound federation ingress capability")
)

// PeerRBACDomainPermission is one concrete domain's directional capability
// grant. Write is retained for mixed-version wire compatibility but must be
// false in v11.9 persisted policy. Copy implies Read.
type PeerRBACDomainPermission struct {
	Domain string `json:"domain"`
	Read   bool   `json:"read"`
	Write  bool   `json:"write"`
	Copy   bool   `json:"copy"`
}

// PeerRBACPolicy is the complete local grant served to one frozen peer
// operator. Domains is always non-nil for a configured policy, including an
// explicit deny-all snapshot.
type PeerRBACPolicy struct {
	RemoteChainID string `json:"remote_chain_id"`
	PeerAgentID   string `json:"peer_agent_id"`
	PolicyEpoch   string `json:"policy_epoch"`
	RemoteCAPin   string `json:"remote_ca_pin"`
	PolicyVersion int    `json:"policy_version"`
	// Paused is a local operator kill-switch for this directional grant. The
	// domain snapshot remains intact so sharing can resume without repeating
	// JOIN, but every Read/Copy authorization path treats a paused policy as
	// deny-all. It is bound to the same frozen peer/epoch/CA header as Domains.
	Paused  bool                       `json:"paused"`
	Domains []PeerRBACDomainPermission `json:"domains"`
}

func (s *SQLiteStore) migratePeerRBACPolicies(ctx context.Context) {
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS peer_rbac_policy (
		remote_chain_id TEXT PRIMARY KEY,
		peer_agent_id   TEXT NOT NULL,
		policy_epoch    TEXT NOT NULL DEFAULT '',
		remote_ca_pin   TEXT NOT NULL DEFAULT '',
		policy_version  INTEGER NOT NULL,
		paused          INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0,1)),
		updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	)`)
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS peer_rbac_domain (
		remote_chain_id TEXT NOT NULL,
		domain_tag      TEXT NOT NULL,
		can_read        INTEGER NOT NULL CHECK (can_read IN (0,1)),
		can_write       INTEGER NOT NULL CHECK (can_write IN (0,1)),
		can_copy        INTEGER NOT NULL CHECK (can_copy IN (0,1)),
		PRIMARY KEY (remote_chain_id, domain_tag)
	)`)
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS peer_rbac_managed_grant (
		remote_chain_id TEXT NOT NULL,
		peer_agent_id   TEXT NOT NULL,
		domain_tag      TEXT NOT NULL,
		grant_level     INTEGER NOT NULL CHECK (grant_level = 2),
		granter_id      TEXT NOT NULL DEFAULT '',
		state           TEXT NOT NULL CHECK (state IN ('pending_apply','active','pending_revoke')),
		updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		PRIMARY KEY (remote_chain_id, domain_tag)
	)`)
	var hasGranterID int
	if err := s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info('peer_rbac_managed_grant') WHERE name='granter_id'`).Scan(&hasGranterID); err == nil && hasGranterID == 0 {
		_, _ = s.writeExecContext(ctx, `ALTER TABLE peer_rbac_managed_grant ADD COLUMN granter_id TEXT NOT NULL DEFAULT ''`)
	}
	for _, column := range []string{"policy_epoch", "remote_ca_pin"} {
		var present int
		if err := s.conn.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM pragma_table_info('peer_rbac_policy') WHERE name=?`, column).Scan(&present); err == nil && present == 0 {
			_, _ = s.writeExecContext(ctx, `ALTER TABLE peer_rbac_policy ADD COLUMN `+column+` TEXT NOT NULL DEFAULT ''`)
		}
	}
	var hasPaused int
	if err := s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info('peer_rbac_policy') WHERE name='paused'`).Scan(&hasPaused); err == nil && hasPaused == 0 {
		_, _ = s.writeExecContext(ctx, `ALTER TABLE peer_rbac_policy ADD COLUMN paused INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0,1))`)
	}
	// Development builds briefly wrote headers without ceremony-generation
	// fields. Backfill only from the same chain's frozen sync_control row; if no
	// such row exists the header remains invalid and therefore fails closed.
	_, _ = s.writeExecContext(ctx, `UPDATE peer_rbac_policy SET
		policy_epoch=COALESCE(NULLIF(policy_epoch,''),
			(SELECT policy_epoch FROM sync_control WHERE sync_control.remote_chain_id=peer_rbac_policy.remote_chain_id),''),
		remote_ca_pin=COALESCE(NULLIF(remote_ca_pin,''),
			(SELECT remote_ca_pin FROM sync_control WHERE sync_control.remote_chain_id=peer_rbac_policy.remote_chain_id),'')`)
	// Preview builds briefly persisted can_write and paired it with an ordinary
	// AccessGrant. Clear the advertised bit before policy can be served. The
	// managed-grant ledger is deliberately retained so startup can revoke the
	// exact consensus grant before opening listeners.
	_, _ = s.writeExecContext(ctx, `UPDATE peer_rbac_domain SET can_write=0 WHERE can_write<>0`)
	// A preview Write-only row becomes no authorization once Write is retired;
	// remove it instead of leaving a corrupt zero-permission policy that blocks
	// the operator from viewing the remaining safe snapshot. Repair Copy=>Read
	// defensively for rows written by older/non-canonical preview builds.
	_, _ = s.writeExecContext(ctx, `UPDATE peer_rbac_domain SET can_read=1 WHERE can_copy=1 AND can_read=0`)
	_, _ = s.writeExecContext(ctx, `DELETE FROM peer_rbac_domain WHERE can_read=0 AND can_copy=0`)
	// Repair the pre-atomic-cutover crash state: older builds could commit a
	// valid peer-RBAC header and stop before marking the local sync filter v3.
	// Exact active ceremony bindings are safe to advance; mismatched, inactive,
	// or malformed headers stay untouched and therefore fail closed.
	_ = s.reconcilePeerRBACSyncVersions(ctx)
}

func (s *SQLiteStore) reconcilePeerRBACSyncVersions(ctx context.Context) error {
	unlock := s.LockSyncPolicyWrite()
	defer unlock()
	return s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		rows, err := tx.conn.QueryContext(ctx, `
			SELECT p.remote_chain_id, p.peer_agent_id, p.policy_epoch, p.remote_ca_pin, p.policy_version
			FROM peer_rbac_policy p
			JOIN sync_control c ON c.remote_chain_id=p.remote_chain_id
				AND c.peer_agent_id=p.peer_agent_id
				AND c.policy_epoch=p.policy_epoch
				AND c.remote_ca_pin=p.remote_ca_pin
			WHERE c.binding_state='active' AND c.policy_version < ?`, peerRBACSyncPolicyVersion)
		if err != nil {
			return fmt.Errorf("list peer RBAC sync cutover candidates: %w", err)
		}
		var candidates []PeerRBACPolicy
		for rows.Next() {
			var policy PeerRBACPolicy
			if err := rows.Scan(&policy.RemoteChainID, &policy.PeerAgentID, &policy.PolicyEpoch,
				&policy.RemoteCAPin, &policy.PolicyVersion); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scan peer RBAC sync cutover candidate: %w", err)
			}
			if validatePeerRBACPolicyHeader(policy) == nil {
				candidates = append(candidates, policy)
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("iterate peer RBAC sync cutover candidates: %w", err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close peer RBAC sync cutover candidates: %w", err)
		}

		for _, policy := range candidates {
			result, err := tx.writeExecContext(ctx, `UPDATE sync_control SET
				policy_version=?, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
				WHERE remote_chain_id=? AND peer_agent_id=? AND policy_epoch=? AND remote_ca_pin=?
					AND binding_state='active' AND policy_version < ?`,
				peerRBACSyncPolicyVersion, policy.RemoteChainID, policy.PeerAgentID,
				policy.PolicyEpoch, policy.RemoteCAPin, peerRBACSyncPolicyVersion)
			if err != nil {
				return fmt.Errorf("reconcile peer RBAC sync cutover: %w", err)
			}
			if affected, err := result.RowsAffected(); err != nil {
				return fmt.Errorf("inspect reconciled peer RBAC sync cutover: %w", err)
			} else if affected != 1 {
				return fmt.Errorf("%w: sync control changed during peer RBAC reconciliation", ErrPeerRBACBindingMismatch)
			}
		}
		return nil
	})
}

func canonicalPeerRBACDomains(raw []PeerRBACDomainPermission) ([]PeerRBACDomainPermission, error) {
	if len(raw) > MaxPeerRBACPolicyDomains {
		return nil, fmt.Errorf("peer RBAC policy is capped at %d domains", MaxPeerRBACPolicyDomains)
	}
	out := append([]PeerRBACDomainPermission{}, raw...)
	for i := range out {
		domain := out[i].Domain
		if domain == "" || domain == "*" || len(domain) > maxPeerRBACDomainBytes || strings.TrimSpace(domain) != domain {
			return nil, fmt.Errorf("peer RBAC domains must be concrete, unpadded tags of at most %d bytes", maxPeerRBACDomainBytes)
		}
		for _, r := range domain {
			if unicode.IsControl(r) {
				return nil, fmt.Errorf("peer RBAC domain %q contains control characters", domain)
			}
		}
		if out[i].Write {
			return nil, fmt.Errorf("%w: domain %q", ErrPeerRBACWriteCapabilityUnavailable, domain)
		}
		if !out[i].Read && !out[i].Write && !out[i].Copy {
			return nil, fmt.Errorf("peer RBAC domain %q grants no permissions", domain)
		}
		out[i].Read = out[i].Read || out[i].Copy
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Domain < out[j].Domain })
	for i := 1; i < len(out); i++ {
		if out[i-1].Domain == out[i].Domain {
			return nil, fmt.Errorf("peer RBAC policy contains duplicate domain %q", out[i].Domain)
		}
	}
	return out, nil
}

func validatePeerRBACIdentity(remoteChainID, peerAgentID string, policyVersion int) error {
	if strings.TrimSpace(remoteChainID) == "" || strings.TrimSpace(remoteChainID) != remoteChainID {
		return fmt.Errorf("remote chain id is required")
	}
	peerKey, err := hex.DecodeString(peerAgentID)
	if err != nil || len(peerKey) != ed25519.PublicKeySize {
		return fmt.Errorf("peer agent id must be a hex-encoded Ed25519 public key")
	}
	if policyVersion != CurrentPeerRBACPolicyVersion {
		return fmt.Errorf("unsupported peer RBAC policy version %d", policyVersion)
	}
	return nil
}

func validatePeerRBACPolicyHeader(policy PeerRBACPolicy) error {
	if err := validatePeerRBACIdentity(policy.RemoteChainID, policy.PeerAgentID, policy.PolicyVersion); err != nil {
		return err
	}
	if strings.TrimSpace(policy.PolicyEpoch) == "" || strings.TrimSpace(policy.PolicyEpoch) != policy.PolicyEpoch {
		return fmt.Errorf("peer RBAC policy epoch is required")
	}
	caPin, err := hex.DecodeString(policy.RemoteCAPin)
	if err != nil || len(caPin) == 0 {
		return fmt.Errorf("peer RBAC remote CA pin must be a non-empty hex digest")
	}
	return nil
}

// GetPeerRBACPolicy returns nil,nil only when the connection has no configured
// peer-RBAC header. A non-nil policy with an empty Domains slice is the explicit
// deny-all snapshot and must never be collapsed into the legacy case.
func (s *SQLiteStore) GetPeerRBACPolicy(ctx context.Context, remoteChainID string) (*PeerRBACPolicy, error) {
	policy := &PeerRBACPolicy{Domains: make([]PeerRBACDomainPermission, 0)}
	var paused int
	err := s.conn.QueryRowContext(ctx, `
		SELECT remote_chain_id, peer_agent_id, policy_epoch, remote_ca_pin, policy_version, paused
		FROM peer_rbac_policy WHERE remote_chain_id=?`, remoteChainID).
		Scan(&policy.RemoteChainID, &policy.PeerAgentID, &policy.PolicyEpoch, &policy.RemoteCAPin, &policy.PolicyVersion, &paused)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get peer RBAC policy header: %w", err)
	}
	if paused != 0 && paused != 1 {
		return nil, fmt.Errorf("stored peer RBAC pause state is invalid")
	}
	policy.Paused = paused == 1
	if validationErr := validatePeerRBACPolicyHeader(*policy); validationErr != nil {
		return nil, fmt.Errorf("stored peer RBAC policy header is invalid: %w", validationErr)
	}

	rows, err := s.conn.QueryContext(ctx, `
		SELECT domain_tag, can_read, can_write, can_copy
		FROM peer_rbac_domain WHERE remote_chain_id=? ORDER BY domain_tag`, remoteChainID)
	if err != nil {
		return nil, fmt.Errorf("list peer RBAC domains: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var permission PeerRBACDomainPermission
		var read, write, copy int
		if scanErr := rows.Scan(&permission.Domain, &read, &write, &copy); scanErr != nil {
			return nil, fmt.Errorf("scan peer RBAC domain: %w", scanErr)
		}
		if (read != 0 && read != 1) || (write != 0 && write != 1) || (copy != 0 && copy != 1) {
			return nil, fmt.Errorf("stored peer RBAC domain %q violates permission invariants", permission.Domain)
		}
		permission.Read, permission.Write, permission.Copy = read == 1, write == 1, copy == 1
		policy.Domains = append(policy.Domains, permission)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("list peer RBAC domains: %w", rowsErr)
	}
	if len(policy.Domains) > MaxPeerRBACPolicyDomains {
		return nil, fmt.Errorf("stored peer RBAC policy exceeds the %d-domain limit", MaxPeerRBACPolicyDomains)
	}
	canonical, err := canonicalPeerRBACDomains(policy.Domains)
	if err != nil {
		return nil, fmt.Errorf("stored peer RBAC domain snapshot is invalid: %w", err)
	}
	policy.Domains = canonical
	return policy, nil
}

// ReplacePeerRBACPolicy is the standalone-compatible replacement path. It
// validates and cuts over an existing sync_control row, but deliberately permits
// no row for store-only users and tests.
func (s *SQLiteStore) ReplacePeerRBACPolicy(ctx context.Context, policy PeerRBACPolicy) (*PeerRBACPolicy, error) {
	return s.replacePeerRBACPolicy(ctx, policy, false)
}

// ReplaceBoundPeerRBACPolicy is the federation control-plane replacement path.
// The exact active sync_control ceremony binding must still exist while the
// shared sync-policy lock is held; otherwise the transaction fails instead of
// committing an orphan policy after concurrent revocation.
func (s *SQLiteStore) ReplaceBoundPeerRBACPolicy(ctx context.Context, policy PeerRBACPolicy) (*PeerRBACPolicy, error) {
	return s.replacePeerRBACPolicy(ctx, policy, true)
}

// replacePeerRBACPolicy atomically replaces every domain row while freezing the
// original (remote_chain_id, peer_agent_id, policy_version) binding. When a
// sync_control row exists, it must be the exact active ceremony binding and its
// local policy version is raised to v3 in the same transaction. That marker
// prevents a committed peer-RBAC snapshot from coexisting with an active legacy
// tx-33 sync lane. Rebinding requires revocation/purge plus fresh enrollment.
func (s *SQLiteStore) replacePeerRBACPolicy(ctx context.Context, policy PeerRBACPolicy, requireControl bool) (*PeerRBACPolicy, error) {
	if err := validatePeerRBACPolicyHeader(policy); err != nil {
		return nil, err
	}
	canonical, err := canonicalPeerRBACDomains(policy.Domains)
	if err != nil {
		return nil, err
	}
	policy.Domains = canonical

	unlock := s.LockSyncPolicyWrite()
	defer unlock()
	err = s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		var controlPeer, controlEpoch, controlCAPin, controlState string
		controlErr := tx.conn.QueryRowContext(ctx, `
			SELECT peer_agent_id, policy_epoch, remote_ca_pin, binding_state
			FROM sync_control WHERE remote_chain_id=?`, policy.RemoteChainID).
			Scan(&controlPeer, &controlEpoch, &controlCAPin, &controlState)
		hasControl := controlErr == nil
		switch {
		case errors.Is(controlErr, sql.ErrNoRows) && requireControl:
			return fmt.Errorf("%w: active sync control is required", ErrPeerRBACBindingMismatch)
		case errors.Is(controlErr, sql.ErrNoRows):
			// Standalone policy storage deliberately does not require federation
			// control metadata.
		case controlErr != nil:
			return fmt.Errorf("read peer RBAC sync binding: %w", controlErr)
		case controlState != "active" || controlPeer != policy.PeerAgentID ||
			controlEpoch != policy.PolicyEpoch || controlCAPin != policy.RemoteCAPin:
			return fmt.Errorf("%w: sync control does not match the active peer ceremony", ErrPeerRBACBindingMismatch)
		}

		var existingAgent string
		var existingVersion int
		var existingEpoch, existingCAPin string
		getErr := tx.conn.QueryRowContext(ctx, `
			SELECT peer_agent_id, policy_epoch, remote_ca_pin, policy_version
			FROM peer_rbac_policy WHERE remote_chain_id=?`, policy.RemoteChainID).
			Scan(&existingAgent, &existingEpoch, &existingCAPin, &existingVersion)
		switch {
		case getErr == nil && (existingAgent != policy.PeerAgentID || existingEpoch != policy.PolicyEpoch ||
			existingCAPin != policy.RemoteCAPin || existingVersion != policy.PolicyVersion):
			return fmt.Errorf("%w: existing peer=%s epoch=%s version=%d", ErrPeerRBACBindingMismatch, existingAgent, existingEpoch, existingVersion)
		case getErr != nil && !errors.Is(getErr, sql.ErrNoRows):
			return fmt.Errorf("read existing peer RBAC binding: %w", getErr)
		}
		if _, execErr := tx.writeExecContext(ctx, `
			INSERT INTO peer_rbac_policy(remote_chain_id, peer_agent_id, policy_epoch, remote_ca_pin, policy_version)
			VALUES(?,?,?,?,?)
			ON CONFLICT(remote_chain_id) DO UPDATE SET
				updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')`,
			policy.RemoteChainID, policy.PeerAgentID, policy.PolicyEpoch, policy.RemoteCAPin, policy.PolicyVersion); execErr != nil {
			return fmt.Errorf("upsert peer RBAC policy header: %w", execErr)
		}
		if _, execErr := tx.writeExecContext(ctx, `DELETE FROM peer_rbac_domain WHERE remote_chain_id=?`, policy.RemoteChainID); execErr != nil {
			return fmt.Errorf("replace peer RBAC domains: %w", execErr)
		}
		for _, permission := range canonical {
			if _, execErr := tx.writeExecContext(ctx, `
				INSERT INTO peer_rbac_domain(remote_chain_id, domain_tag, can_read, can_write, can_copy)
				VALUES(?,?,?,?,?)`, policy.RemoteChainID, permission.Domain,
				permission.Read, permission.Write, permission.Copy); execErr != nil {
				return fmt.Errorf("insert peer RBAC domain %q: %w", permission.Domain, execErr)
			}
		}
		if hasControl {
			result, execErr := tx.writeExecContext(ctx, `UPDATE sync_control SET
				policy_version=CASE WHEN policy_version < ? THEN ? ELSE policy_version END,
				updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
				WHERE remote_chain_id=? AND peer_agent_id=? AND policy_epoch=? AND remote_ca_pin=? AND binding_state='active'`,
				peerRBACSyncPolicyVersion, peerRBACSyncPolicyVersion, policy.RemoteChainID,
				policy.PeerAgentID, policy.PolicyEpoch, policy.RemoteCAPin)
			if execErr != nil {
				return fmt.Errorf("cut over peer RBAC sync policy: %w", execErr)
			}
			rows, rowsErr := result.RowsAffected()
			if rowsErr != nil {
				return fmt.Errorf("inspect peer RBAC sync cutover: %w", rowsErr)
			}
			if rows != 1 {
				return fmt.Errorf("%w: active sync control changed during peer RBAC cutover", ErrPeerRBACBindingMismatch)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.GetPeerRBACPolicy(ctx, policy.RemoteChainID)
}

// SetBoundPeerRBACPaused flips the local directional kill-switch without
// modifying its saved domain snapshot. The update is accepted only while the
// exact JOIN-frozen peer, epoch and CA binding remains active. Holding the
// sync-policy write lease makes a completed pause linearize after every
// response that was already authorized under the old state.
func (s *SQLiteStore) SetBoundPeerRBACPaused(ctx context.Context, binding PeerRBACPolicy, paused bool) (*PeerRBACPolicy, error) {
	if err := validatePeerRBACPolicyHeader(binding); err != nil {
		return nil, err
	}
	unlock := s.LockSyncPolicyWrite()
	defer unlock()
	err := s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		result, execErr := tx.writeExecContext(ctx, `UPDATE peer_rbac_policy SET
			paused=?, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
			WHERE remote_chain_id=? AND peer_agent_id=? AND policy_epoch=? AND remote_ca_pin=? AND policy_version=?
			AND EXISTS (SELECT 1 FROM sync_control c WHERE c.remote_chain_id=peer_rbac_policy.remote_chain_id
				AND c.peer_agent_id=peer_rbac_policy.peer_agent_id
				AND c.policy_epoch=peer_rbac_policy.policy_epoch
				AND c.remote_ca_pin=peer_rbac_policy.remote_ca_pin
				AND c.binding_state='active')`,
			paused, binding.RemoteChainID, binding.PeerAgentID, binding.PolicyEpoch,
			binding.RemoteCAPin, binding.PolicyVersion)
		if execErr != nil {
			return fmt.Errorf("set peer RBAC pause state: %w", execErr)
		}
		rows, rowsErr := result.RowsAffected()
		if rowsErr != nil {
			return fmt.Errorf("inspect peer RBAC pause update: %w", rowsErr)
		}
		if rows != 1 {
			return fmt.Errorf("%w: active peer RBAC binding changed during pause update", ErrPeerRBACBindingMismatch)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.GetPeerRBACPolicy(ctx, binding.RemoteChainID)
}

// DeletePeerRBACPolicy removes both the explicit header and its rows. It is a
// revocation/re-enrollment primitive, not a way to express deny-all (replace
// with an empty snapshot for that).
func (s *SQLiteStore) DeletePeerRBACPolicy(ctx context.Context, remoteChainID string) error {
	return s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		if _, err := tx.writeExecContext(ctx, `DELETE FROM peer_rbac_domain WHERE remote_chain_id=?`, remoteChainID); err != nil {
			return fmt.Errorf("delete peer RBAC domains: %w", err)
		}
		if _, err := tx.writeExecContext(ctx, `DELETE FROM peer_rbac_policy WHERE remote_chain_id=?`, remoteChainID); err != nil {
			return fmt.Errorf("delete peer RBAC policy: %w", err)
		}
		return nil
	})
}
