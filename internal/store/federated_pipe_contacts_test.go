package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFederatedPipeContactAcceptanceBindsPolicyAndSurvivesPauseOnly(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	epoch, caPin := testPeerRBACBinding()
	policy := PeerRBACPolicy{
		RemoteChainID: "chain-peer", PeerAgentID: testPeerAgentID(t),
		PolicyEpoch: epoch, RemoteCAPin: caPin, PolicyVersion: CurrentPeerRBACPolicyVersion,
		Domains: []PeerRBACDomainPermission{{Domain: "research", Read: true}},
	}
	if err := s.PrepareSyncControl(ctx, testLegacyPeerRBACControl(policy)); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateSyncControl(ctx, policy.RemoteChainID, policy.PolicyEpoch); err != nil {
		t.Fatal(err)
	}
	stored, err := s.ReplaceBoundPeerRBACPolicy(ctx, policy)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Revision != 1 {
		t.Fatalf("initial policy revision=%d, want 1", stored.Revision)
	}

	localAgentID := testPeerAgentID(t)
	contactID := strings.Repeat("cd", 32)
	acceptances, err := s.GetFederatedPipeContactAcceptances(ctx, *stored)
	if err != nil || len(acceptances) != 0 {
		t.Fatalf("default acceptances=%v err=%v, want empty", acceptances, err)
	}
	if acceptanceErr := s.SetBoundFederatedPipeContactAcceptance(ctx, *stored, localAgentID, contactID, true); acceptanceErr != nil {
		t.Fatal(acceptanceErr)
	}
	acceptances, err = s.GetFederatedPipeContactAcceptances(ctx, *stored)
	if err != nil || acceptances[localAgentID] != contactID {
		t.Fatalf("stored acceptance=%v err=%v", acceptances, err)
	}

	paused, err := s.SetBoundPeerRBACPaused(ctx, *stored, true)
	if err != nil {
		t.Fatal(err)
	}
	if paused.Revision != stored.Revision {
		t.Fatalf("pause advanced policy revision: before=%d after=%d", stored.Revision, paused.Revision)
	}
	acceptances, err = s.GetFederatedPipeContactAcceptances(ctx, *paused)
	if err != nil || acceptances[localAgentID] != contactID {
		t.Fatalf("pause discarded acceptance=%v err=%v", acceptances, err)
	}

	replaced, err := s.ReplaceBoundPeerRBACPolicy(ctx, policy)
	if err != nil {
		t.Fatal(err)
	}
	if replaced.Revision != stored.Revision+1 {
		t.Fatalf("replacement revision=%d, want %d", replaced.Revision, stored.Revision+1)
	}
	acceptances, err = s.GetFederatedPipeContactAcceptances(ctx, *replaced)
	if err != nil || len(acceptances) != 0 {
		t.Fatalf("replacement retained acceptance=%v err=%v", acceptances, err)
	}
	if err := s.SetBoundFederatedPipeContactAcceptance(ctx, *stored, localAgentID, contactID, true); !errors.Is(err, ErrPeerRBACBindingMismatch) {
		t.Fatalf("stale policy setter error=%v, want binding mismatch", err)
	}
}

func TestFederatedPipeContactSelectedAcceptancesChunkBeyondSQLiteVariableLimit(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	epoch, caPin := testPeerRBACBinding()
	policy := PeerRBACPolicy{
		RemoteChainID: "chain-peer", PeerAgentID: testPeerAgentID(t),
		PolicyEpoch: epoch, RemoteCAPin: caPin, PolicyVersion: CurrentPeerRBACPolicyVersion,
		Domains: []PeerRBACDomainPermission{{Domain: "research", Read: true}},
	}
	require.NoError(t, s.PrepareSyncControl(ctx, testLegacyPeerRBACControl(policy)))
	require.NoError(t, s.ActivateSyncControl(ctx, policy.RemoteChainID, policy.PolicyEpoch))
	stored, err := s.ReplaceBoundPeerRBACPolicy(ctx, policy)
	require.NoError(t, err)

	selected := make([]string, 0, MaxPeerRBACPolicyDomains+1)
	for i := 1; i <= MaxPeerRBACPolicyDomains+1; i++ {
		agentID := fmt.Sprintf("%064x", i)
		selected = append(selected, agentID)
		_, err := s.conn.ExecContext(ctx, `
			INSERT INTO fed_pipe_contact_acceptance
				(remote_chain_id, peer_agent_id, policy_epoch, remote_ca_pin,
				 policy_revision, local_agent_id, contact_id)
			VALUES(?,?,?,?,?,?,?)`, stored.RemoteChainID, stored.PeerAgentID, stored.PolicyEpoch,
			stored.RemoteCAPin, stored.Revision, agentID, strings.Repeat("ab", 32))
		require.NoError(t, err)
	}

	acceptances, err := s.GetFederatedPipeContactAcceptancesForAgents(ctx, *stored, selected)
	require.NoError(t, err)
	require.Len(t, acceptances, len(selected))
}

func TestFederatedPipeContactAcceptancePurgedWithPeerState(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	epoch, caPin := testPeerRBACBinding()
	policy := PeerRBACPolicy{
		RemoteChainID: "chain-peer", PeerAgentID: testPeerAgentID(t),
		PolicyEpoch: epoch, RemoteCAPin: caPin, PolicyVersion: CurrentPeerRBACPolicyVersion,
		Domains: []PeerRBACDomainPermission{{Domain: "research", Read: true}},
	}
	if err := s.PrepareSyncControl(ctx, testLegacyPeerRBACControl(policy)); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateSyncControl(ctx, policy.RemoteChainID, policy.PolicyEpoch); err != nil {
		t.Fatal(err)
	}
	stored, err := s.ReplaceBoundPeerRBACPolicy(ctx, policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetBoundFederatedPipeContactAcceptance(ctx, *stored, testPeerAgentID(t), strings.Repeat("ef", 32), true); err != nil {
		t.Fatal(err)
	}
	if err := s.PurgeSyncPeerState(ctx, policy.RemoteChainID); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM fed_pipe_contact_acceptance WHERE remote_chain_id=?`, policy.RemoteChainID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("purged peer retained %d acceptance rows", count)
	}
}

func TestFederatedPipeRemoteContactSnapshotRequiresExactLiveBinding(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	epoch, caPin := testPeerRBACBinding()
	peerAgentID := testPeerAgentID(t)
	control := testLegacyPeerRBACControl(PeerRBACPolicy{
		RemoteChainID: "chain-peer", PeerAgentID: peerAgentID,
		PolicyEpoch: epoch, RemoteCAPin: caPin,
	})
	if err := s.PrepareSyncControl(ctx, control); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateSyncControl(ctx, control.RemoteChainID, control.PolicyEpoch); err != nil {
		t.Fatal(err)
	}
	active, err := s.GetSyncControl(ctx, control.RemoteChainID)
	if err != nil || active == nil {
		t.Fatalf("active control=%+v err=%v", active, err)
	}
	snapshot := FederatedPipeRemoteContactSnapshot{
		RemoteChainID: active.RemoteChainID, PeerAgentID: active.PeerAgentID,
		PolicyEpoch: active.PolicyEpoch, RemoteCAPin: active.RemoteCAPin,
		RemotePolicyVersion: active.RemotePolicyVersion, RemotePolicyRevision: active.RemoteRevision,
		RemotePolicyHash: active.RemotePolicyHash, LocalAgreementID: strings.Repeat("11", 32),
		RemoteAgreementID: strings.Repeat("22", 32), ContactRevision: strings.Repeat("33", 32),
		Snapshot: []byte(`{"version":1,"contacts":[]}`),
	}
	if putErr := s.PutFederatedPipeRemoteContactSnapshot(ctx, snapshot); putErr != nil {
		t.Fatal(putErr)
	}
	loaded, err := s.GetFederatedPipeRemoteContactSnapshot(ctx, *active, snapshot.LocalAgreementID)
	if err != nil || loaded == nil || string(loaded.Snapshot) != string(snapshot.Snapshot) {
		t.Fatalf("loaded snapshot=%+v err=%v", loaded, err)
	}

	if _, updateErr := s.writeExecContext(ctx, `UPDATE sync_control SET remote_revision=remote_revision+1 WHERE remote_chain_id=?`, active.RemoteChainID); updateErr != nil {
		t.Fatal(updateErr)
	}
	changed, err := s.GetSyncControl(ctx, active.RemoteChainID)
	if err != nil {
		t.Fatal(err)
	}
	stale, err := s.GetFederatedPipeRemoteContactSnapshot(ctx, *changed, snapshot.LocalAgreementID)
	if err != nil || stale != nil {
		t.Fatalf("changed binding reused stale snapshot=%+v err=%v", stale, err)
	}
	if err := s.PutFederatedPipeRemoteContactSnapshot(ctx, snapshot); !errors.Is(err, ErrPeerRBACBindingMismatch) {
		t.Fatalf("stale snapshot put err=%v, want binding mismatch", err)
	}
	if err := s.DeleteSyncControl(ctx, active.RemoteChainID); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM fed_pipe_remote_contact_snapshot WHERE remote_chain_id=?`, active.RemoteChainID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("deleted sync control retained %d remote contact snapshots", count)
	}
}

func TestFederatedPipeRemoteContactSnapshotBoundDeletePreservesNewerPolicyBinding(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	epoch, caPin := testPeerRBACBinding()
	control := testLegacyPeerRBACControl(PeerRBACPolicy{
		RemoteChainID: "chain-peer", PeerAgentID: testPeerAgentID(t),
		PolicyEpoch: epoch, RemoteCAPin: caPin,
	})
	if err := s.PrepareSyncControl(ctx, control); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateSyncControl(ctx, control.RemoteChainID, control.PolicyEpoch); err != nil {
		t.Fatal(err)
	}
	oldControl, err := s.GetSyncControl(ctx, control.RemoteChainID)
	if err != nil || oldControl == nil {
		t.Fatalf("old control=%+v err=%v", oldControl, err)
	}
	localAgreementID := strings.Repeat("11", 32)
	oldSnapshot := FederatedPipeRemoteContactSnapshot{
		RemoteChainID: oldControl.RemoteChainID, PeerAgentID: oldControl.PeerAgentID,
		PolicyEpoch: oldControl.PolicyEpoch, RemoteCAPin: oldControl.RemoteCAPin,
		RemotePolicyVersion: oldControl.RemotePolicyVersion, RemotePolicyRevision: oldControl.RemoteRevision,
		RemotePolicyHash: oldControl.RemotePolicyHash, LocalAgreementID: localAgreementID,
		RemoteAgreementID: strings.Repeat("22", 32), ContactRevision: strings.Repeat("33", 32),
		Snapshot: []byte(`{"version":1,"revision":"old"}`),
	}
	if putErr := s.PutFederatedPipeRemoteContactSnapshot(ctx, oldSnapshot); putErr != nil {
		t.Fatal(putErr)
	}

	if _, applyErr := s.ApplyRemoteDirectionalSyncPolicy(ctx, control.RemoteChainID, control.PolicyEpoch,
		3, 1, strings.Repeat("44", 32), nil, nil); applyErr != nil {
		t.Fatal(applyErr)
	}
	newControl, err := s.GetSyncControl(ctx, control.RemoteChainID)
	if err != nil || newControl == nil {
		t.Fatalf("new control=%+v err=%v", newControl, err)
	}
	newSnapshot := FederatedPipeRemoteContactSnapshot{
		RemoteChainID: newControl.RemoteChainID, PeerAgentID: newControl.PeerAgentID,
		PolicyEpoch: newControl.PolicyEpoch, RemoteCAPin: newControl.RemoteCAPin,
		RemotePolicyVersion: newControl.RemotePolicyVersion, RemotePolicyRevision: newControl.RemoteRevision,
		RemotePolicyHash: newControl.RemotePolicyHash, LocalAgreementID: localAgreementID,
		RemoteAgreementID: strings.Repeat("55", 32), ContactRevision: strings.Repeat("66", 32),
		Snapshot: []byte(`{"version":1,"revision":"new"}`),
	}
	if putErr := s.PutFederatedPipeRemoteContactSnapshot(ctx, newSnapshot); putErr != nil {
		t.Fatal(putErr)
	}

	if deleteErr := s.DeleteFederatedPipeRemoteContactSnapshotBound(ctx, *oldControl, localAgreementID); deleteErr != nil {
		t.Fatal(deleteErr)
	}
	loaded, err := s.GetFederatedPipeRemoteContactSnapshot(ctx, *newControl, localAgreementID)
	if err != nil || loaded == nil {
		t.Fatalf("new snapshot after old bound delete=%+v err=%v", loaded, err)
	}
	if loaded.RemoteAgreementID != newSnapshot.RemoteAgreementID ||
		loaded.ContactRevision != newSnapshot.ContactRevision || string(loaded.Snapshot) != string(newSnapshot.Snapshot) {
		t.Fatalf("old bound delete changed newer snapshot: got=%+v want=%+v", loaded, newSnapshot)
	}
}
