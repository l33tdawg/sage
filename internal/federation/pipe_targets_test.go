package federation

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

func newRemotePipeCacheTestBinding(t *testing.T) (*Manager, *store.SQLiteStore, *store.CrossFedRecord, *store.SyncControl) {
	t.Helper()
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	const chainID = "chain-cache-peer"
	peerAgentID := strings.Repeat("ab", 32)
	pin := bytes.Repeat([]byte{0x41}, 32)
	require.NoError(t, bs.SetCrossFed(chainID, "https://peer.example:8444", pin, 4, 0, nil, nil, "active"))
	control := store.SyncControl{
		RemoteChainID: chainID, Role: "guest", ControllerChainID: chainID,
		ControllerAgentID: peerAgentID, PeerAgentID: peerAgentID,
		PolicyEpoch: "pipe-cache-epoch", RemoteCAPin: hex.EncodeToString(pin),
	}
	require.NoError(t, ss.PrepareSyncControl(ctx, control))
	require.NoError(t, ss.ActivateSyncControl(ctx, chainID, control.PolicyEpoch))
	agreement, err := m.ActiveAgreement(chainID)
	require.NoError(t, err)
	active, err := ss.GetSyncControl(ctx, chainID)
	require.NoError(t, err)
	require.NotNil(t, active)
	return m, ss, agreement, active
}

func remotePipeCacheTestGrant(agreementOctet, revisionOctet string) *PipeContactGrant {
	return &PipeContactGrant{
		Version: PipeContactVersion, AgreementID: strings.Repeat(agreementOctet, 32),
		Revision: strings.Repeat(revisionOctet, 32), Contacts: []PipeContact{},
	}
}

func putRemotePipeCacheTestGrant(t *testing.T, ss *store.SQLiteStore, agreement *store.CrossFedRecord, control *store.SyncControl, grant *PipeContactGrant) []byte {
	t.Helper()
	encoded, err := json.Marshal(grant)
	require.NoError(t, err)
	require.NoError(t, ss.PutFederatedPipeRemoteContactSnapshot(context.Background(), store.FederatedPipeRemoteContactSnapshot{
		RemoteChainID: control.RemoteChainID, PeerAgentID: control.PeerAgentID,
		PolicyEpoch: control.PolicyEpoch, RemoteCAPin: control.RemoteCAPin,
		RemotePolicyVersion: control.RemotePolicyVersion, RemotePolicyRevision: control.RemoteRevision,
		RemotePolicyHash: control.RemotePolicyHash, LocalAgreementID: pipeRoutingAgreementID(agreement),
		RemoteAgreementID: grant.AgreementID, ContactRevision: grant.Revision, Snapshot: encoded,
	}))
	return encoded
}

func TestMatchRemotePipeCandidatesRequiresExactQualifiedIdentity(t *testing.T) {
	agentA := strings.Repeat("a", 64)
	agentB := strings.Repeat("b", 64)
	candidates := []remotePipeCandidate{
		{chainID: "chain-amy", contact: PipeContact{AgentID: agentA, Address: agentA + "@chain-amy", Handle: "#amy-12345678/aaaaaaaa", DisplayName: "researcher"}},
		{chainID: "chain-bob", contact: PipeContact{AgentID: agentB, Address: agentB + "@chain-bob", Handle: "#bob-87654321/bbbbbbbb", DisplayName: "researcher"}},
	}

	matches := matchRemotePipeCandidates(agentA+"@chain-amy", candidates)
	require.Len(t, matches, 1)
	require.Equal(t, "chain-amy", matches[0].chainID)
	require.Empty(t, matchRemotePipeCandidates(agentA+"@chain-bob", candidates),
		"an agent ID cannot be transplanted onto another peer chain")
	require.Len(t, matchRemotePipeCandidates("#amy-12345678/aaaaaaaa", candidates), 1)
	require.Len(t, matchRemotePipeCandidates("researcher", candidates), 2,
		"bare display names remain ambiguous instead of selecting the first peer")
	require.Empty(t, matchRemotePipeCandidates("aaaaaaaa", candidates),
		"short agent prefixes are accepted only inside a peer-qualified handle")
}

func TestSplitPipeAddressRejectsMalformedQualifiedTargets(t *testing.T) {
	agentID := strings.Repeat("a", 64)
	agent, chain := splitPipeAddress(agentID + "@chain-amy")
	require.Equal(t, agentID, agent)
	require.Equal(t, "chain-amy", chain)
	for _, target := range []string{
		"short@chain-amy",
		agentID + "@",
		agentID + "@bad chain",
		"#amy-12345678/aaaaaaaa",
	} {
		agent, chain := splitPipeAddress(target)
		require.Empty(t, agent, target)
		require.Empty(t, chain, target)
	}
}

func TestDelayedOldStatusRefreshCannotClobberNewerPipeContactPolicyBinding(t *testing.T) {
	for _, test := range []struct {
		name    string
		status  *StatusResponse
		wantErr bool
	}{
		{name: "negative status", status: &StatusResponse{}},
		{name: "positive status", status: &StatusResponse{
			Capabilities: []string{CapabilityFederatedPipeline},
			PipeContacts: remotePipeCacheTestGrant("21", "31"),
		}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			m, ss, agreement, oldControl := newRemotePipeCacheTestBinding(t)
			putRemotePipeCacheTestGrant(t, ss, agreement, oldControl, remotePipeCacheTestGrant("22", "32"))

			_, err := ss.ApplyRemoteDirectionalSyncPolicy(ctx, oldControl.RemoteChainID, oldControl.PolicyEpoch,
				SyncPolicyVersionPeerRBAC, 1, strings.Repeat("41", 32), nil, nil)
			require.NoError(t, err)
			newControl, err := ss.GetSyncControl(ctx, oldControl.RemoteChainID)
			require.NoError(t, err)
			require.NotNil(t, newControl)
			newGrant := remotePipeCacheTestGrant("23", "33")
			newSnapshot := putRemotePipeCacheTestGrant(t, ss, agreement, newControl, newGrant)

			err = m.refreshRemotePipeContactCache(ctx, agreement, oldControl, test.status)
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			loaded, err := ss.GetFederatedPipeRemoteContactSnapshot(ctx, *newControl, pipeRoutingAgreementID(agreement))
			require.NoError(t, err)
			require.NotNil(t, loaded, "a delayed old refresh must not delete the newer cache row")
			require.Equal(t, newGrant.AgreementID, loaded.RemoteAgreementID)
			require.Equal(t, newGrant.Revision, loaded.ContactRevision)
			require.Equal(t, newSnapshot, loaded.Snapshot)
		})
	}
}

func TestVaultLockedPipeContactRefreshInvalidatesExactOldCache(t *testing.T) {
	ctx := context.Background()
	m, ss, agreement, control := newRemotePipeCacheTestBinding(t)
	putRemotePipeCacheTestGrant(t, ss, agreement, control, remotePipeCacheTestGrant("51", "61"))

	ss.SetVaultExpected(true)
	err := m.refreshRemotePipeContactCache(ctx, agreement, control, &StatusResponse{
		Capabilities: []string{CapabilityFederatedPipeline},
		PipeContacts: remotePipeCacheTestGrant("52", "62"),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "vault is locked")
	ss.SetVaultExpected(false)

	loaded, err := ss.GetFederatedPipeRemoteContactSnapshot(ctx, *control, pipeRoutingAgreementID(agreement))
	require.NoError(t, err)
	require.Nil(t, loaded, "a failed encrypted refresh must invalidate the exact old cache")
}
