package federation

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
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

type nonSQLitePipeMemoryStore struct{ store.MemoryStore }

func TestResolveRemotePipeTargetMissingLocalStoreIsNotPeerUnsupported(t *testing.T) {
	for _, backend := range []string{"missing", "non-SQLite"} {
		t.Run(backend, func(t *testing.T) {
			m, ss, _, _ := newRemotePipeCacheTestBinding(t)
			m.memStore = nil
			if backend == "non-SQLite" {
				m.memStore = &nonSQLitePipeMemoryStore{ss}
			}
			_, err := m.ResolveRemotePipeTarget(context.Background(), strings.Repeat("ab", 32)+"@chain-cache-peer")
			require.ErrorIs(t, err, ErrRemotePipeLocalStoreUnavailable)
			require.NotErrorIs(t, err, ErrRemotePipePeerUnsupported,
				"missing local storage says nothing about the remote peer's capabilities")
		})
	}
}

func TestFindRemotePipeContactsUnsupportedPeerRemainsDistinct(t *testing.T) {
	m, _, agreement, _ := newRemotePipeCacheTestBinding(t)
	_, err := m.findRemotePipeContactsWithStatus(context.Background(), agreement, &StatusResponse{}, "Mynah", 20)
	require.ErrorIs(t, err, ErrRemotePipePeerUnsupported)
	require.NotErrorIs(t, err, ErrRemotePipeLocalStoreUnavailable)
}

func TestMatchRemotePipeCandidatesAcceptsRegisteredNameButReturnsCanonicalRoute(t *testing.T) {
	agentID := strings.Repeat("c", 64)
	candidate := remotePipeCandidate{
		chainID: "chain-mini",
		contact: PipeContact{
			AgentID: agentID, Address: agentID + "@chain-mini",
			DisplayName: "Mynah on the mini", RegisteredName: "mynah/voice-bridge",
		},
	}

	matches := matchRemotePipeCandidates("mynah/voice-bridge", []remotePipeCandidate{candidate})
	require.Len(t, matches, 1)
	require.Equal(t, agentID+"@chain-mini", matches[0].contact.Address)
	require.True(t, pipeContactMatchesTarget(candidate.contact, "mynah/voice-bridge"),
		"the serving peer's targeted lookup must expose the same exact registered-name match")
	require.Empty(t, matchRemotePipeCandidates("mynah", []remotePipeCandidate{candidate}),
		"friendly send resolution must never use substring matching")
}

func TestMatchRemotePipeCandidatesRenameKeepsRegisteredAndCanonicalIdentityStable(t *testing.T) {
	agentID := strings.Repeat("d", 64)
	before := remotePipeCandidate{chainID: "chain-mini", contact: PipeContact{
		AgentID: agentID, Address: agentID + "@chain-mini",
		DisplayName: "Old Mynah", RegisteredName: "mynah/voice-bridge",
	}}
	after := before
	after.contact.DisplayName = "Mac Mini Mynah"

	require.Len(t, matchRemotePipeCandidates("Old Mynah", []remotePipeCandidate{before}), 1)
	require.Empty(t, matchRemotePipeCandidates("Old Mynah", []remotePipeCandidate{after}))
	for _, label := range []string{"Mac Mini Mynah", "mynah/voice-bridge"} {
		matches := matchRemotePipeCandidates(label, []remotePipeCandidate{after})
		require.Len(t, matches, 1)
		require.Equal(t, agentID+"@chain-mini", matches[0].contact.Address)
	}
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

func TestRemotePipeContactGrantRejectsOversizedLegacyStatusSnapshot(t *testing.T) {
	grant := remotePipeCacheTestGrant("ab", "cd")
	grant.Contacts = make([]PipeContact, 0, 1025)
	for i := 0; i < 1025; i++ {
		agentID := fmt.Sprintf("%064x", i+1)
		grant.Contacts = append(grant.Contacts, PipeContact{
			AgentID: agentID, ContactID: strings.Repeat("ef", 32),
			Address: agentID + "@chain-cache-peer", Handle: "#cache/" + agentID[:8],
			Available: true, Accepting: true,
			Domains: []PipeContactDomain{{Domain: "research"}},
		})
	}
	require.Error(t, validateRemotePipeContactGrant("chain-cache-peer", grant),
		"the v1 status route must remain compatible with v11.13.0 peers")
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

func TestLookupCapableLegacyStatusRefreshesExactCache(t *testing.T) {
	ctx := context.Background()
	m, ss, agreement, control := newRemotePipeCacheTestBinding(t)
	exact := remotePipeCacheTestGrant("71", "81")
	putRemotePipeCacheTestGrant(t, ss, agreement, control, exact)
	updated := remotePipeCacheTestGrant("91", "81")
	status := &StatusResponse{
		Capabilities: []string{CapabilityFederatedPipeline, CapabilityFederatedPipelineContactLookup},
		PipeContacts: updated,
	}
	require.NoError(t, m.refreshRemotePipeContactCache(ctx, agreement, control, status))
	loaded, err := ss.GetFederatedPipeRemoteContactSnapshot(ctx, *control, pipeRoutingAgreementID(agreement))
	require.NoError(t, err)
	require.NotNil(t, loaded, "a full legacy status snapshot remains a valid exact offline route hint")
	var cached PipeContactGrant
	require.NoError(t, json.Unmarshal(loaded.Snapshot, &cached))
	require.Equal(t, updated.Revision, cached.Revision)
}

func TestCompactLookupStatusPreservesLegacyExactCache(t *testing.T) {
	ctx := context.Background()
	m, ss, agreement, control := newRemotePipeCacheTestBinding(t)
	exact := remotePipeCacheTestGrant("71", "81")
	putRemotePipeCacheTestGrant(t, ss, agreement, control, exact)
	status := &StatusResponse{Capabilities: []string{CapabilityFederatedPipeline, CapabilityFederatedPipelineContactLookup}}
	require.NoError(t, m.refreshRemotePipeContactCache(ctx, agreement, control, status))
	loaded, err := ss.GetFederatedPipeRemoteContactSnapshot(ctx, *control, pipeRoutingAgreementID(agreement))
	require.NoError(t, err)
	require.NotNil(t, loaded, "a compact lookup preflight must not erase a separately authenticated legacy route hint")
}

func TestKnownMessageTargetAdmissionIsCallerBoundAndBindingBound(t *testing.T) {
	ctx := context.Background()
	m, ss, agreement, control := newRemotePipeCacheTestBinding(t)
	caller := strings.Repeat("cd", 32)
	targetAgent := strings.Repeat("ef", 32)
	target := &RemotePipeTarget{
		ChainID: agreement.RemoteChainID, AgentID: targetAgent,
		ContactID: strings.Repeat("12", 32), ContactRevision: strings.Repeat("13", 32),
		PolicyEpoch: control.PolicyEpoch, AgreementID: strings.Repeat("14", 32),
		Address: targetAgent + "@" + agreement.RemoteChainID,
		Domains: []PipeContactDomain{{Domain: "research", OwningDomain: "research"}},
	}
	require.NoError(t, m.rememberRemoteMessageTarget(ctx, caller, target))
	known, err := m.knownRemoteMessageTarget(ctx, caller, target.Address)
	require.NoError(t, err)
	require.Equal(t, target.Address, known.Address)
	require.Equal(t, target.Domains, known.Domains)
	other, err := m.knownRemoteMessageTarget(ctx, strings.Repeat("aa", 32), target.Address)
	require.NoError(t, err)
	require.Nil(t, other, "another caller must not inherit the known-recipient ticket")

	_, err = ss.ApplyRemoteDirectionalSyncPolicy(ctx, control.RemoteChainID, control.PolicyEpoch,
		SyncPolicyVersionPeerRBAC, 1, strings.Repeat("44", 32), nil, nil)
	require.NoError(t, err)
	stale, err := m.knownRemoteMessageTarget(ctx, caller, target.Address)
	require.NoError(t, err)
	require.Nil(t, stale, "a policy-generation change must make the ticket invisible")
}
