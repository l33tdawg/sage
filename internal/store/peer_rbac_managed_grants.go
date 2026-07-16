package store

// ManagedPeerAccessGrant is the crash-safe cleanup ledger for ordinary on-chain
// AccessGrant rows created by pre-release federation-Write previews. v11.9
// never creates a row or treats one as active peer policy. It retains legacy
// provenance until tx-7 removal is confirmed so an old reusable grant cannot
// survive behind the new fail-closed connection policy.

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
)

const (
	ManagedPeerGrantPendingApply  = "pending_apply"
	ManagedPeerGrantActive        = "active"
	ManagedPeerGrantPendingRevoke = "pending_revoke"
)

type ManagedPeerAccessGrant struct {
	RemoteChainID string `json:"remote_chain_id"`
	PeerAgentID   string `json:"peer_agent_id"`
	Domain        string `json:"domain"`
	Level         uint8  `json:"level"`
	// GranterID fingerprints the exact level-2/never-expiring row created by
	// federation. Empty is a migrated legacy ledger row with unknown provenance;
	// cleanup must retain it for manual review, never revoke by guesswork.
	GranterID string `json:"granter_id"`
	State     string `json:"state"`
}

func validManagedPeerGrantState(state string) bool {
	return state == ManagedPeerGrantPendingApply || state == ManagedPeerGrantActive || state == ManagedPeerGrantPendingRevoke
}

func validateManagedPeerAccessGrant(grant ManagedPeerAccessGrant) error {
	if err := validatePeerRBACIdentity(grant.RemoteChainID, grant.PeerAgentID, CurrentPeerRBACPolicyVersion); err != nil {
		return err
	}
	// Managed rows are cleanup provenance for preview-era ordinary grants; they
	// are not active peer policy and must remain loadable after Write is rejected.
	if _, err := canonicalPeerRBACDomains([]PeerRBACDomainPermission{{Domain: grant.Domain, Read: true}}); err != nil {
		return err
	}
	if grant.Level != 2 {
		return fmt.Errorf("managed federation grants must be level 2")
	}
	if grant.GranterID != "" {
		granterKey, err := hex.DecodeString(grant.GranterID)
		if err != nil || len(granterKey) != ed25519.PublicKeySize {
			return fmt.Errorf("managed federation grant granter must be a hex-encoded Ed25519 public key")
		}
	}
	if !validManagedPeerGrantState(grant.State) {
		return fmt.Errorf("invalid managed federation grant state %q", grant.State)
	}
	return nil
}

// UpsertManagedPeerAccessGrant updates cleanup state around a revoke. An
// existing chain/domain row is permanently bound to the original peer key
// until removal is confirmed and the ledger row is deleted.
func (s *SQLiteStore) UpsertManagedPeerAccessGrant(ctx context.Context, grant ManagedPeerAccessGrant) error {
	if err := validateManagedPeerAccessGrant(grant); err != nil {
		return err
	}
	result, err := s.writeExecContext(ctx, `
		INSERT INTO peer_rbac_managed_grant(remote_chain_id, peer_agent_id, domain_tag, grant_level, granter_id, state)
		VALUES(?,?,?,?,?,?)
		ON CONFLICT(remote_chain_id, domain_tag) DO UPDATE SET
			grant_level=excluded.grant_level, granter_id=excluded.granter_id, state=excluded.state,
			updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
		WHERE peer_rbac_managed_grant.peer_agent_id=excluded.peer_agent_id
		AND peer_rbac_managed_grant.granter_id=excluded.granter_id`,
		grant.RemoteChainID, grant.PeerAgentID, grant.Domain, grant.Level, grant.GranterID, grant.State)
	if err != nil {
		return fmt.Errorf("upsert managed federation grant: %w", err)
	}
	if n, rowsErr := result.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("inspect managed federation grant upsert: %w", rowsErr)
	} else if n != 1 {
		return fmt.Errorf("%w: managed grant peer key or granter provenance changed", ErrPeerRBACBindingMismatch)
	}
	return nil
}

func (s *SQLiteStore) DeleteManagedPeerAccessGrant(ctx context.Context, remoteChainID, peerAgentID, domain string) error {
	result, err := s.writeExecContext(ctx, `DELETE FROM peer_rbac_managed_grant
		WHERE remote_chain_id=? AND peer_agent_id=? AND domain_tag=?`, remoteChainID, peerAgentID, domain)
	if err != nil {
		return fmt.Errorf("delete managed federation grant: %w", err)
	}
	if _, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("inspect managed federation grant delete: %w", err)
	}
	return nil
}

// ListManagedPeerAccessGrants returns all rows when remoteChainID is empty and
// one connection's rows otherwise. Callers use the all-rows form for restart
// reconciliation of revokes that outlived their agreement.
func (s *SQLiteStore) ListManagedPeerAccessGrants(ctx context.Context, remoteChainID string) ([]ManagedPeerAccessGrant, error) {
	query := `SELECT remote_chain_id, peer_agent_id, domain_tag, grant_level, granter_id, state
		FROM peer_rbac_managed_grant`
	args := []any{}
	if remoteChainID != "" {
		query += ` WHERE remote_chain_id=?`
		args = append(args, remoteChainID)
	}
	query += ` ORDER BY remote_chain_id, domain_tag`
	rows, err := s.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list managed federation grants: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := make([]ManagedPeerAccessGrant, 0)
	for rows.Next() {
		var grant ManagedPeerAccessGrant
		if err := rows.Scan(&grant.RemoteChainID, &grant.PeerAgentID, &grant.Domain, &grant.Level, &grant.GranterID, &grant.State); err != nil {
			return nil, fmt.Errorf("scan managed federation grant: %w", err)
		}
		if err := validateManagedPeerAccessGrant(grant); err != nil {
			return nil, fmt.Errorf("stored managed federation grant is invalid: %w", err)
		}
		out = append(out, grant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list managed federation grants: %w", err)
	}
	return out, nil
}
