package federation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

// TestDomainRemovalRejectsQueuedRowAtDrain proves D2 (§10 row 1): a domain removal
// terminally rejects an already-enqueued outbox row toward that domain at drain
// time — the drain-gate re-check (DomainAllowed(effectiveConsent, ...)) is the
// enforcement backstop once GroupSharedDomains drops the removed domain.
func TestDomainRemovalRejectsQueuedRowAtDrain(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t) // localChainID == "chain-local"
	seedDrainAgreement(t, bs, "chain-b", 2, "studio")

	seedGroupMember(t, ms, "g1", "chain-local", store.GroupRoleFullSync, store.GroupMemberActive, "")
	seedGroupMember(t, ms, "g1", "chain-b", store.GroupRoleFullSync, store.GroupMemberActive, "")
	seedGroupDomain(t, ms, "g1", "studio", "chain-local", 0)
	seedCommitted(t, ms, "m-studio", "studio", "group memory")

	// A fan-out row is already queued toward chain-b.
	_, err := ms.EnqueueSyncOutbox(ctx, "chain-b", "m-studio")
	require.NoError(t, err)

	// domain_remove apply: stamp the domain removed. Effective consent now drops it.
	require.NoError(t, ms.SetSyncGroupDomainRemoved(ctx, "g1", "studio", 1))
	consented, err := m.effectiveConsent(ctx, ms, "chain-b")
	require.NoError(t, err)
	assert.Empty(t, consented, "removed group domain drops out of effective consent")

	// Drain must terminally REJECT the straggler row (never deliver it).
	m.syncDrain(ctx, ms, mustAgreement(t, m, "chain-b"), consented)

	counts, err := ms.CountSyncOutboxByState(ctx, "chain-b")
	require.NoError(t, err)
	assert.Equal(t, 1, counts[store.SyncStateRejected], "queued row toward a removed domain is rejected at drain")
	assert.Zero(t, counts[store.SyncStateDelivered], "a removed-domain row must never deliver")
}

// TestReconcilePairwiseContinuesWhileResyncing proves D5 (SF2): while THIS node is
// resyncing in a shared group, the GROUP backfill digest is skipped but an
// independent PAIRWISE relationship keeps reconciling — the pairwise digest is
// issued and pairwise candidates still enqueue. (Previously the whole peer paused.)
func TestReconcilePairwiseContinuesWhileResyncing(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t) // localChainID == "chain-local"
	seedDrainAgreement(t, bs, "chain-b", 2, "hr", "studio")

	// Local is REBUILDING in group g1 (studio owned by the peer) AND holds an
	// independent pairwise consent for "hr" with the same peer.
	seedGroupMember(t, ms, "g1", "chain-local", store.GroupRoleFullSync, store.GroupMemberResyncing, "")
	seedGroupMember(t, ms, "g1", "chain-b", store.GroupRoleFullSync, store.GroupMemberActive, "")
	seedGroupDomain(t, ms, "g1", "studio", "chain-b", 0)
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))
	seedCommitted(t, ms, "m-hr", "hr", "independent pairwise fact")

	pairwiseDigest, groupDigest := false, false
	m.syncDigestFn = func(_ context.Context, _ string, req *SyncDigestRequest) (*SyncDigestResponse, error) {
		if req.GroupID == "" {
			pairwiseDigest = true
		} else {
			groupDigest = true
		}
		return &SyncDigestResponse{Consented: true, ConsentedDomains: []string{"hr"}}, nil
	}

	consented, err := m.effectiveConsent(ctx, ms, "chain-b")
	require.NoError(t, err)
	assert.Equal(t, []string{"hr"}, consented, "resyncing local's effective consent is pairwise only")
	m.reconcilePeer(ctx, ms, mustAgreement(t, m, "chain-b"), consented)

	assert.True(t, pairwiseDigest, "pairwise digest continues while resyncing")
	assert.False(t, groupDigest, "group backfill digest is paused while resyncing")

	// The independent pairwise candidate is enqueued (pairwise reconcile is live).
	counts, err := ms.CountSyncOutboxByState(ctx, "chain-b")
	require.NoError(t, err)
	assert.Equal(t, 1, counts[store.SyncStatePending], "pairwise candidate reconciles while resyncing")
}
