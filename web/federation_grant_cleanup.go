package web

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// revokeFederationManagedGrant removes one exact direct AccessGrant owned by
// the federation managed-grant ledger. Prefer the current domain owner's key.
// If ownership moved to a key this node no longer holds, app-v18's global-admin
// path is the crash-safe escape hatch: bind the tx to the *current* owner and
// owning ancestor so a concurrent transfer makes consensus reject it instead
// of revoking against stale authority. Unlike the interactive access matrix,
// cleanup deliberately does not require the remote grantee's private key to be
// local; trust revocation must work for a foreign peer.
func (h *DashboardHandler) revokeFederationManagedGrant(domain, granteeID string) grantResult {
	owner, ownedDomain, err := h.BadgerStore.ResolveOwningAncestor(domain)
	if err != nil || owner == "" {
		return grantResult{Domain: domain, Action: "skip", OK: false,
			Code: "owner_missing", Error: "domain has no current on-chain owner; managed grant cleanup will retry"}
	}

	ownerLocal := false
	var signer ed25519.PrivateKey
	if h.ResolveAgentKeyFn != nil {
		signer, ownerLocal = h.ResolveAgentKeyFn(owner)
	}
	var expectedOwner, expectedOwnedDomain string
	if len(signer) != ed25519.PrivateKeySize {
		if h.AppV18ActiveFn == nil || !h.AppV18ActiveFn() {
			return grantResult{Domain: domain, Action: "skip", OK: false,
				Code: "override_not_active", Error: "current owner key is not local and administrator cleanup is waiting for app-v18",
				OwnerID: owner, OwnedDomain: ownedDomain, OwnerLocal: ownerLocal,
				OverrideAvailable: len(h.AdminSigningKey) == ed25519.PrivateKeySize}
		}
		signer = h.AdminSigningKey
		if len(signer) != ed25519.PrivateKeySize {
			return grantResult{Domain: domain, Action: "skip", OK: false,
				Code: "admin_key_unavailable", Error: "genesis admin signing key is unavailable for managed grant cleanup",
				OwnerID: owner, OwnedDomain: ownedDomain, OwnerLocal: ownerLocal}
		}
		expectedOwner, expectedOwnedDomain = owner, ownedDomain
	}

	revokeTx := &tx.ParsedTx{
		Type: tx.TxTypeAccessRevoke,
		AccessRevoke: &tx.AccessRevoke{
			RevokerID:           agentIDForKey(signer),
			GranteeID:           granteeID,
			Domain:              domain,
			Reason:              "federation managed write permission removed",
			ExpectedOwnerID:     expectedOwner,
			ExpectedOwnedDomain: expectedOwnedDomain,
		},
	}
	if _, _, _, revokeErr := h.signAndBroadcastCommit(revokeTx, signer); revokeErr != nil {
		return grantResult{Domain: domain, Action: "revoke", OK: false,
			Code: "revoke_rejected", Error: revokeErr.Error(), OwnerID: owner,
			OwnedDomain: ownedDomain, OwnerLocal: ownerLocal}
	}
	return grantResult{Domain: domain, Action: "revoke", OK: true,
		OwnerID: owner, OwnedDomain: ownedDomain, OwnerLocal: ownerLocal}
}

// ReconcileFederationManagedGrants retires every preview-era direct level-2
// grant created by peer Write permissions. v11.9 has no safe peer Write path:
// an ordinary AccessGrant is reusable through public REST or raw Comet and is
// therefore cleanup-only regardless of current trust or peer policy.
func (h *DashboardHandler) ReconcileFederationManagedGrants(ctx context.Context) error {
	ss := h.syncStore()
	if ss == nil {
		return errors.New("federation managed-grant cleanup store unavailable")
	}
	if h.BadgerStore == nil {
		return errors.New("federation managed-grant consensus store unavailable")
	}
	h.federationPolicyMu.Lock()
	defer h.federationPolicyMu.Unlock()

	managed, err := ss.ListManagedPeerAccessGrants(ctx, "")
	if err != nil {
		return err
	}
	var errs []error
	touchedChains := make(map[string]struct{})
	for _, grant := range managed {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(errs, err)...)
		}
		touchedChains[grant.RemoteChainID] = struct{}{}

		level, expiresAt, granterID, grantErr := h.BadgerStore.GetAccessGrant(grant.Domain, grant.PeerAgentID)
		exactManaged := grantErr == nil && grant.GranterID != "" && level == grant.Level &&
			expiresAt == 0 && granterID == grant.GranterID

		grant.State = store.ManagedPeerGrantPendingRevoke
		if markErr := ss.UpsertManagedPeerAccessGrant(ctx, grant); markErr != nil {
			errs = append(errs, fmt.Errorf("%s/%s: mark pending revoke: %w", grant.RemoteChainID, grant.Domain, markErr))
			continue
		}
		switch {
		case errors.Is(grantErr, store.ErrAccessGrantNotFound):
			// A staged apply may have crashed before broadcasting. Absence is a
			// complete cleanup result.
		case grantErr != nil:
			errs = append(errs, fmt.Errorf("%s/%s: read grant for cleanup: %w", grant.RemoteChainID, grant.Domain, grantErr))
			continue
		case grant.GranterID == "":
			errs = append(errs, fmt.Errorf("%s/%s: managed grant provenance is unknown; refusing automatic revoke", grant.RemoteChainID, grant.Domain))
			continue
		case !exactManaged:
			// Somebody independently replaced/upgraded the exact row. The peer
			// policy already denies it, but federation no longer owns that grant.
			if deleteErr := ss.DeleteManagedPeerAccessGrant(ctx, grant.RemoteChainID, grant.PeerAgentID, grant.Domain); deleteErr != nil {
				errs = append(errs, fmt.Errorf("%s/%s: relinquish externally changed grant: %w", grant.RemoteChainID, grant.Domain, deleteErr))
			}
			continue
		default:
			result := h.revokeFederationManagedGrant(grant.Domain, grant.PeerAgentID)
			if !result.OK {
				errs = append(errs, fmt.Errorf("%s/%s: %s", grant.RemoteChainID, grant.Domain, result.Error))
				continue
			}
		}
		if _, _, _, remainingErr := h.BadgerStore.GetAccessGrant(grant.Domain, grant.PeerAgentID); remainingErr == nil {
			errs = append(errs, fmt.Errorf("%s/%s: access grant still present after revoke", grant.RemoteChainID, grant.Domain))
			continue
		} else if !errors.Is(remainingErr, store.ErrAccessGrantNotFound) {
			errs = append(errs, fmt.Errorf("%s/%s: verify access grant removal: %w", grant.RemoteChainID, grant.Domain, remainingErr))
			continue
		}
		if deleteErr := ss.DeleteManagedPeerAccessGrant(ctx, grant.RemoteChainID, grant.PeerAgentID, grant.Domain); deleteErr != nil {
			errs = append(errs, fmt.Errorf("%s/%s: delete cleanup row: %w", grant.RemoteChainID, grant.Domain, deleteErr))
		}
	}

	// A revoked connection's policy remains only as a cleanup identity ledger.
	// Once no managed grants remain, erase it so a later fresh ceremony can bind
	// a different peer key without inheriting any authorization.
	for chain := range touchedChains {
		if h.findAgreement(chain) != nil {
			continue
		}
		remaining, listErr := ss.ListManagedPeerAccessGrants(ctx, chain)
		if listErr != nil {
			errs = append(errs, fmt.Errorf("%s: verify cleanup ledger: %w", chain, listErr))
			continue
		}
		if len(remaining) == 0 {
			if deleteErr := ss.DeletePeerRBACPolicy(ctx, chain); deleteErr != nil {
				errs = append(errs, fmt.Errorf("%s: delete retired peer policy: %w", chain, deleteErr))
			}
		}
	}
	return errors.Join(errs...)
}

// StartFederationManagedGrantReconciler runs once at boot and periodically so
// a Comet outage during revoke cannot turn a retained cleanup row into a
// permanent grant. The node lifecycle owns cancellation/joining.
func (h *DashboardHandler) StartFederationManagedGrantReconciler() {
	if h.RunBackground == nil {
		return
	}
	h.RunBackground(func(ctx context.Context) {
		_ = h.ReconcileFederationManagedGrants(ctx)
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = h.ReconcileFederationManagedGrants(ctx)
			}
		}
	})
}
