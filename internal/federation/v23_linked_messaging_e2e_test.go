package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/require"
)

type linkedMessagingPair struct {
	host, peer          *testChain
	memberID, guestID   string
	memberKey, guestKey ed25519.PrivateKey
	guest               store.FederatedGroupGuest
}

func callLinkedMessageHandler(
	t *testing.T,
	handler http.HandlerFunc,
	path string,
	peer *peerIdentity,
	payload any,
) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req = req.WithContext(context.WithValue(
		req.Context(), peerCtxKey{}, peer,
	))
	recorder := httptest.NewRecorder()
	handler(recorder, req)
	return recorder
}

func newLinkedMessagingPair(t *testing.T) *linkedMessagingPair {
	t.Helper()
	host := newTestChain(t, "linked-host")
	peer := newTestChain(t, "linked-peer")
	hostListener := startListener(t, host)
	peerListener := startListener(t, peer)
	federate(t, host, peer, peerListener.URL, []string{"*"}, 4, 0)
	federate(t, peer, host, hostListener.URL, []string{"*"}, 4, 0)
	enableV23Pair(t, host, peer, []string{"shared"})

	_, memberKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, guestKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	memberID := enrollV23OrdinaryAgent(t, host, "host-member", memberKey, 4)
	guestID := enrollV23OrdinaryAgent(t, peer, "peer-guest", guestKey, 4)
	require.NoError(t, pipeSQLite(t, host).CreateAgent(
		context.Background(),
		&store.AgentEntry{
			AgentID: memberID, Name: "Host Member", Status: "active",
			RegisteredName: "claude/host-member", Provider: "claude-code",
		},
	))
	require.NoError(t, pipeSQLite(t, peer).CreateAgent(
		context.Background(),
		&store.AgentEntry{
			AgentID: guestID, Name: "Peer Guest", Status: "active",
			RegisteredName: "mynah/peer-guest", Provider: "mynah",
		},
	))

	hostRoot := hex.EncodeToString(host.agentPub)
	require.NoError(t, host.badger.MutateAppV23AccessGroup(
		hostRoot, "linked-team", "Linked team", []string{memberID},
		0, false, 10,
	))
	agreement, err := host.mgr.ActiveAgreement(peer.chainID)
	require.NoError(t, err)
	digest, err := host.mgr.agreementBindingDigestV23(
		context.Background(), agreement, hex.EncodeToString(peer.agentPub),
	)
	require.NoError(t, err)
	guest := store.FederatedGroupGuest{
		GroupID: "linked-team", RemoteChainID: peer.chainID,
		RemoteAgentID: guestID, AgreementBindingDigest: digest,
		MaxClassification: 2, Revision: 1,
		State: store.FederatedGuestStateActive,
	}
	require.NoError(t, store.SignFederatedGroupGuest(&guest, host.agentKey))
	require.NoError(t, pipeSQLite(t, host).PutFederatedGroupGuest(
		context.Background(), guest,
	))
	return &linkedMessagingPair{
		host: host, peer: peer, memberID: memberID, guestID: guestID,
		memberKey: memberKey, guestKey: guestKey, guest: guest,
	}
}

func TestLinkedMessageDirectoryBidirectionalDiscoveryAndExactSend(t *testing.T) {
	pair := newLinkedMessagingPair(t)
	pair.enableBothDirections(t)
	ctx := context.Background()

	hosted, err := pair.host.mgr.FindRemoteLinkedMessageContacts(
		ctx, pair.peer.chainID, pair.memberID, "peer guest", 20,
	)
	require.NoError(t, err)
	require.Len(t, hosted.Contacts, 1)
	require.Equal(t, pair.guestID, hosted.Contacts[0].AgentID)
	require.Equal(t, "Peer Guest", hosted.Contacts[0].DisplayName)
	require.Equal(t, "mynah/peer-guest", hosted.Contacts[0].RegisteredName)
	require.Equal(t, "mynah", hosted.Contacts[0].Provider)
	require.Equal(t, pair.guestID+"@"+pair.peer.chainID, hosted.Contacts[0].Address)
	require.Empty(t, hosted.Contacts[0].Domains)
	require.Empty(t, hosted.Contacts[0].ContactID)
	require.Equal(t, LinkedMessageAuthorizationMode,
		hosted.Contacts[0].AuthorizationMode)
	require.False(t, hosted.Contacts[0].Available,
		"linked discovery must not claim online presence")
	require.False(t, hosted.Contacts[0].Accepting,
		"exact tuple consent must not be presented as live acceptance status")
	hostedDirectory, err := pair.host.mgr.ListRemoteLinkedMessageContacts(
		ctx, pair.peer.chainID, pair.memberID,
	)
	require.NoError(t, err)
	require.Len(t, hostedDirectory.Contacts, 1)
	require.Equal(t, hosted.Contacts[0], hostedDirectory.Contacts[0])

	remoteHosted, err := pair.peer.mgr.FindRemoteLinkedMessageContacts(
		ctx, pair.host.chainID, pair.guestID, "claude/host", 20,
	)
	require.NoError(t, err)
	require.Len(t, remoteHosted.Contacts, 1)
	require.Equal(t, pair.memberID, remoteHosted.Contacts[0].AgentID)
	require.Equal(t, "Host Member", remoteHosted.Contacts[0].DisplayName)
	require.Equal(t, "claude/host-member", remoteHosted.Contacts[0].RegisteredName)
	require.Equal(t, "claude-code", remoteHosted.Contacts[0].Provider)
	require.Equal(t, pair.memberID+"@"+pair.host.chainID, remoteHosted.Contacts[0].Address)
	remoteDirectory, err := pair.peer.mgr.ListRemoteLinkedMessageContacts(
		ctx, pair.host.chainID, pair.guestID,
	)
	require.NoError(t, err)
	require.Len(t, remoteDirectory.Contacts, 1)
	require.Equal(t, remoteHosted.Contacts[0], remoteDirectory.Contacts[0])

	target, err := pair.host.mgr.ResolveRemoteLinkedPipeTarget(
		ctx, pair.memberID, hosted.Contacts[0].Address,
	)
	require.NoError(t, err)
	_, outbox := enqueueLinkedSend(
		t, pair.host, pair.peer, pair.memberID, pair.memberKey,
		target, "directory-exact-send",
	)
	deliverLinkedSend(t, pair.host, pair.peer, outbox)
}

func TestLinkedMessageDirectoryEnumerationNeverSilentlyTruncates(t *testing.T) {
	for _, direction := range []string{
		LinkedMessageGuestToMember,
		LinkedMessageMemberToGuest,
	} {
		t.Run(direction, func(t *testing.T) {
			req := LinkedMessageDirectoryRequest{
				Direction: direction, Enumerate: true,
				Limit: maxLinkedMessageDirectoryInventory,
			}
			entries := make([]LinkedMessageDirectoryEntry, 0, req.Limit)
			for i := 0; i < req.Limit; i++ {
				require.True(t, appendLinkedDirectoryEntry(
					&entries, req, LinkedMessageDirectoryEntry{AgentID: strings.Repeat("a", 64)},
				))
			}
			require.False(t, appendLinkedDirectoryEntry(
				&entries, req, LinkedMessageDirectoryEntry{AgentID: strings.Repeat("b", 64)},
			))
			require.Len(t, entries, req.Limit)
		})
	}

	contacts := make([]PipeContact, maxLinkedMessageDirectoryInventory+1)
	_, err := boundLinkedDirectoryContacts(
		contacts, maxLinkedMessageDirectoryInventory, true,
	)
	require.ErrorIs(t, err, ErrFederatedPipeInvalid,
		"a two-direction union over the inventory limit must fail, not slice")
	bounded, err := boundLinkedDirectoryContacts(contacts, 20, false)
	require.NoError(t, err)
	require.Len(t, bounded, 20, "bounded human-name lookup keeps top-N semantics")
}

func TestLinkedMessageDirectoryAmbiguityUnicodeAndUnrelatedHidden(t *testing.T) {
	pair := newLinkedMessagingPair(t)
	pair.enableBothDirections(t)
	ctx := context.Background()

	_, secondKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	secondID := enrollV23OrdinaryAgent(t, pair.host, "resume-member", secondKey, 4)
	require.NoError(t, pipeSQLite(t, pair.host).CreateAgent(ctx, &store.AgentEntry{
		AgentID: secondID, Name: "Résumé Member", Status: "active",
		RegisteredName: "claude/resume-member", Provider: "claude-code",
	}))
	group, err := pair.host.badger.GetAppV23AccessGroup("linked-team")
	require.NoError(t, err)
	members := []string{pair.memberID, secondID}
	sort.Strings(members)
	require.NoError(t, pair.host.badger.MutateAppV23AccessGroup(
		hex.EncodeToString(pair.host.agentPub), group.GroupID, group.Name,
		members, group.Revision, false, 20,
	))
	_, err = pair.host.mgr.SetLinkedMessageConsentCAS(
		ctx, pair.peer.chainID, pair.guestID, secondID, 0, true,
	)
	require.NoError(t, err)

	_, unrelatedKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	unrelatedID := enrollV23OrdinaryAgent(
		t, pair.host, "host-unrelated", unrelatedKey, 4,
	)
	require.NoError(t, pipeSQLite(t, pair.host).CreateAgent(ctx, &store.AgentEntry{
		AgentID: unrelatedID, Name: "Host Unrelated", Status: "active",
		RegisteredName: "claude/host-unrelated", Provider: "claude-code",
	}))

	ambiguous, err := pair.peer.mgr.FindRemoteLinkedMessageContacts(
		ctx, pair.host.chainID, pair.guestID, "member", 20,
	)
	require.NoError(t, err)
	require.Len(t, ambiguous.Contacts, 2)
	ids := []string{ambiguous.Contacts[0].AgentID, ambiguous.Contacts[1].AgentID}
	require.ElementsMatch(t, []string{pair.memberID, secondID}, ids)
	require.NotContains(t, ids, unrelatedID)

	unicode, err := pair.peer.mgr.FindRemoteLinkedMessageContacts(
		ctx, pair.host.chainID, pair.guestID, "Résumé", 20,
	)
	require.NoError(t, err)
	require.Len(t, unicode.Contacts, 1)
	require.Equal(t, secondID, unicode.Contacts[0].AgentID)

	hidden, err := pair.peer.mgr.FindRemoteLinkedMessageContacts(
		ctx, pair.host.chainID, pair.guestID, "unrelated", 20,
	)
	require.NoError(t, err)
	require.Empty(t, hidden.Contacts)
}

func TestLinkedMessageDirectoryRevocationImmediatelyHidesLookup(t *testing.T) {
	pair := newLinkedMessagingPair(t)
	pair.enableBothDirections(t)
	ctx := context.Background()

	visible, err := pair.host.mgr.FindRemoteLinkedMessageContacts(
		ctx, pair.peer.chainID, pair.memberID, "peer guest", 20,
	)
	require.NoError(t, err)
	require.Len(t, visible.Contacts, 1)
	target, err := pair.host.mgr.ResolveRemoteLinkedPipeTarget(
		ctx, pair.memberID, visible.Contacts[0].Address,
	)
	require.NoError(t, err)
	_, queued := enqueueLinkedSend(
		t, pair.host, pair.peer, pair.memberID, pair.memberKey,
		target, "directory-revocation",
	)

	paused := pair.guest
	paused.Revision++
	paused.State = store.FederatedGuestStatePaused
	require.NoError(t, store.SignFederatedGroupGuest(&paused, pair.host.agentKey))
	require.NoError(t, pipeSQLite(t, pair.host).PutFederatedGroupGuest(ctx, paused))

	hidden, err := pair.host.mgr.FindRemoteLinkedMessageContacts(
		ctx, pair.peer.chainID, pair.memberID, "peer guest", 20,
	)
	require.NoError(t, err)
	require.Empty(t, hidden.Contacts)
	_, terminal, err := pair.host.mgr.buildPipelineEvent(
		ctx, pipeSQLite(t, pair.host), queued,
	)
	require.Error(t, err)
	require.True(t, terminal)
}

func TestLinkedMessageDirectoryRevocationMatrixHidesImmediately(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *linkedMessagingPair)
	}{
		{
			name: "group membership removed",
			mutate: func(t *testing.T, pair *linkedMessagingPair) {
				group, err := pair.host.badger.GetAppV23AccessGroup("linked-team")
				require.NoError(t, err)
				require.NoError(t, pair.host.badger.MutateAppV23AccessGroup(
					hex.EncodeToString(pair.host.agentPub), group.GroupID, group.Name,
					nil, group.Revision, false, 40,
				))
			},
		},
		{
			name: "receiver consent disabled",
			mutate: func(t *testing.T, pair *linkedMessagingPair) {
				ctx := context.Background()
				consent, err := pair.peer.mgr.GetLinkedMessageConsent(
					ctx, pair.host.chainID, pair.memberID, pair.guestID,
				)
				require.NoError(t, err)
				_, err = pair.peer.mgr.SetLinkedMessageConsentCAS(
					ctx, pair.host.chainID, pair.memberID, pair.guestID,
					consent.Revision, false,
				)
				require.NoError(t, err)
			},
		},
		{
			name: "host policy paused",
			mutate: func(t *testing.T, pair *linkedMessagingPair) {
				_, err := pair.host.mgr.SetPeerRBACPaused(
					context.Background(), pair.peer.chainID, true,
				)
				require.NoError(t, err)
			},
		},
		{
			name: "agreement revoked",
			mutate: func(t *testing.T, pair *linkedMessagingPair) {
				require.NoError(t, pair.host.badger.UpdateCrossFedStatus(
					pair.peer.chainID, "revoked",
				))
			},
		},
		{
			name: "local source becomes read-only",
			mutate: func(t *testing.T, pair *linkedMessagingPair) {
				setLinkedTestAgentReadOnly(t, pair.host, pair.memberID, 50)
			},
		},
		{
			name: "remote receiver becomes read-only",
			mutate: func(t *testing.T, pair *linkedMessagingPair) {
				setLinkedTestAgentReadOnly(t, pair.peer, pair.guestID, 50)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pair := newLinkedMessagingPair(t)
			pair.enableBothDirections(t)
			ctx := context.Background()
			before, err := pair.host.mgr.FindRemoteLinkedMessageContacts(
				ctx, pair.peer.chainID, pair.memberID, "peer guest", 20,
			)
			require.NoError(t, err)
			require.Len(t, before.Contacts, 1)
			tc.mutate(t, pair)
			after, err := pair.host.mgr.FindRemoteLinkedMessageContacts(
				ctx, pair.peer.chainID, pair.memberID, "peer guest", 20,
			)
			require.NoError(t, err)
			require.Empty(t, after.Contacts)
		})
	}
}

func setLinkedTestAgentReadOnly(
	t *testing.T,
	node *testChain,
	agentID string,
	height int64,
) {
	t.Helper()
	enrollment, err := node.badger.GetAppV23Enrollment(agentID)
	require.NoError(t, err)
	require.NotNil(t, enrollment)
	role, err := node.badger.GetAppV23Role(agentID)
	require.NoError(t, err)
	require.NotNil(t, role)
	require.NoError(t, node.badger.SetAppV23Policy(
		hex.EncodeToString(node.agentPub), agentID, store.AppV23RoleMember,
		enrollment.Profile, store.AppV23ProfileReadOnly, enrollment.Clearance,
		store.AgentCapabilityReadAllDomains, role.Revision, enrollment.Revision,
		height,
	))
}

func TestLinkedMessageDirectoryUsesAuthenticatedRelayRoute(t *testing.T) {
	pair := newLinkedMessagingPair(t)
	pair.enableBothDirections(t)
	ctx := context.Background()

	var relayRequests atomic.Int32
	relay := startCountedFederationServer(t, pair.host, &relayRequests)
	agreement, err := pair.peer.mgr.ActiveAgreement(pair.host.chainID)
	require.NoError(t, err)
	require.NoError(t, pair.peer.badger.SetCrossFed(
		pair.host.chainID, "https://192.0.2.1:44444", agreement.PeerPubKey,
		agreement.MaxClearance, agreement.ExpiresAt, agreement.AllowedDomains,
		agreement.AllowedDepts, agreement.Status,
	))
	var relayDials atomic.Int32
	pair.peer.mgr.SetPeerRouteDialFunc(func(
		dialCtx context.Context,
		chain string,
		authenticate PeerRouteAuthenticator,
	) (PeerRouteDialResult, bool, error) {
		relayDials.Add(1)
		conn, dialErr := dialTestServer(dialCtx, relay.URL)
		result, authErr := authenticate(dialCtx, PeerRouteDialResult{
			Conn: conn, Kind: RouteKindRelay, Target: "linked-directory-relay",
		}, dialErr)
		return result, true, authErr
	})

	result, err := pair.peer.mgr.FindRemoteLinkedMessageContacts(
		ctx, pair.host.chainID, pair.guestID, "host member", 20,
	)
	require.NoError(t, err)
	require.Len(t, result.Contacts, 1)
	require.Equal(t, pair.memberID, result.Contacts[0].AgentID)
	require.Positive(t, relayDials.Load())
	require.Positive(t, relayRequests.Load())
	require.Equal(t, RouteKindRelay,
		pair.peer.mgr.RouteDiagnostics(pair.host.chainID).State)
}

func TestLinkedMessageDirectoryRejectsMalformedRemoteMetadata(t *testing.T) {
	pair := newLinkedMessagingPair(t)
	pair.enableBothDirections(t)
	ctx := context.Background()

	hostAgreement, err := pair.host.mgr.ActiveAgreement(pair.peer.chainID)
	require.NoError(t, err)
	relation, _, err := pair.host.mgr.buildHostedLinkedRelation(
		ctx,
		&peerIdentity{
			ChainID:   pair.peer.chainID,
			AgentID:   hex.EncodeToString(pair.peer.agentPub),
			Agreement: hostAgreement,
		},
		hostAgreement, LinkedMessageGuestToMember,
		pair.guestID, pair.memberID,
	)
	require.NoError(t, err)
	malformed := LinkedMessageDirectoryResponse{
		Version: linkedMessageDirectoryVersion, ChainID: pair.host.chainID,
		Direction: LinkedMessageGuestToMember, SourceAgentID: pair.guestID,
		Entries: []LinkedMessageDirectoryEntry{{
			AgentID: pair.memberID, DisplayName: "Host Member",
			RegisteredName: "claude/host-member", Provider: "claude-code",
			Address: "wrong-address", ConsentRevision: relation.ReceiverConsentRevision,
			Relation: relation,
		}},
	}
	tlsConfig, err := pair.host.mgr.ServerTLSConfig()
	require.NoError(t, err)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(
		w http.ResponseWriter, _ *http.Request,
	) {
		writeJSON(w, http.StatusOK, malformed)
	}))
	server.TLS = tlsConfig
	server.StartTLS()
	t.Cleanup(server.Close)

	peerAgreement, err := pair.peer.mgr.ActiveAgreement(pair.host.chainID)
	require.NoError(t, err)
	require.NoError(t, pair.peer.badger.SetCrossFed(
		pair.host.chainID, server.URL, peerAgreement.PeerPubKey,
		peerAgreement.MaxClearance, peerAgreement.ExpiresAt,
		peerAgreement.AllowedDomains, peerAgreement.AllowedDepts,
		peerAgreement.Status,
	))

	result, err := pair.peer.mgr.FindRemoteLinkedMessageContacts(
		ctx, pair.host.chainID, pair.guestID, "host member", 20,
	)
	require.ErrorIs(t, err, ErrFederatedPipeInvalid)
	require.Nil(t, result)
}

func (p *linkedMessagingPair) enableBothDirections(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	hostCandidates, err := p.host.mgr.ListLinkedMessageConsentCandidates(
		ctx, hex.EncodeToString(p.host.agentPub), p.peer.chainID, p.guestID,
	)
	require.NoError(t, err)
	require.Len(t, hostCandidates, 1)
	require.Equal(t, p.memberID, hostCandidates[0].LocalAgentID)
	_, err = p.host.mgr.SetLinkedMessageConsentCAS(
		ctx, p.peer.chainID, p.guestID, p.memberID, 0, true,
	)
	require.NoError(t, err)

	peerCandidates, err := p.peer.mgr.ListRemoteHostedLinkedMessageConsentCandidates(
		ctx, hex.EncodeToString(p.peer.agentPub), p.host.chainID, p.guestID,
	)
	require.NoError(t, err)
	require.Len(t, peerCandidates, 1)
	require.Equal(t, p.memberID, peerCandidates[0].RemoteAgentID)
	require.Equal(t, p.guestID, peerCandidates[0].LocalAgentID)
	_, err = p.peer.mgr.SetLinkedMessageConsentCAS(
		ctx, p.host.chainID, p.memberID, p.guestID, 0, true,
	)
	require.NoError(t, err)
}

func enqueueLinkedSend(
	t *testing.T,
	source, destination *testChain,
	sourceID string,
	sourceKey ed25519.PrivateKey,
	target *RemotePipeTarget,
	suffix string,
) (*store.PipelineMessage, *store.PipelineTransportOutbox) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	request := signedPipeSendRequest{
		ToAgent: target.AgentID, SourceChainID: source.chainID,
		DestinationChainID: destination.chainID,
		Intent:             "linked-message", Payload: "untrusted linked payload " + suffix,
		TTLMinutes: 60,
	}
	body, err := json.Marshal(request)
	require.NoError(t, err)
	proof := signedPipeProof(
		t, sourceKey, sourceID, http.MethodPost, "/v1/pipe/send",
		body, now.Unix(),
	)
	relation, err := json.Marshal(target.LinkedRelation)
	require.NoError(t, err)
	msg := &store.PipelineMessage{
		PipeID: "pipe-linked-" + suffix, FromAgent: sourceID,
		ToAgent: target.AgentID, Intent: request.Intent, Payload: request.Payload,
		Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
		DestinationChainID:          destination.chainID,
		FederationPolicyEpoch:       target.PolicyEpoch,
		FederationAgreementID:       target.AgreementID,
		FederationContactID:         target.ContactID,
		FederationContactRevision:   target.ContactRevision,
		FederationAuthorizationMode: target.AuthorizationMode,
		FederationLinkedRelation:    relation,
	}
	outbox := &store.PipelineTransportOutbox{
		EventID: PipelineProofEventID(source.chainID, "send", proof),
		PipeID:  msg.PipeID, RemoteChainID: destination.chainID,
		EventKind: "send", PolicyEpoch: msg.FederationPolicyEpoch,
		AgreementID:       msg.FederationAgreementID,
		ContactID:         msg.FederationContactID,
		ContactRevision:   msg.FederationContactRevision,
		AuthorizationMode: msg.FederationAuthorizationMode,
		LinkedRelation:    relation, SourceAgentID: sourceID,
		TargetAgentID: target.AgentID, Proof: proof,
		CreatedAt: now, ExpiresAt: msg.ExpiresAt,
	}
	require.NoError(t, pipeSQLite(t, source).InsertPipelineWithTransport(
		context.Background(), msg, outbox,
	))
	return msg, outbox
}

func deliverLinkedSend(
	t *testing.T,
	source, destination *testChain,
	outbox *store.PipelineTransportOutbox,
) *store.PipelineMessage {
	t.Helper()
	ctx := context.Background()
	event, terminal, err := source.mgr.buildPipelineEvent(
		ctx, pipeSQLite(t, source), outbox,
	)
	require.NoError(t, err)
	require.False(t, terminal)
	response, err := source.mgr.PushPipeEvent(ctx, destination.chainID, event)
	require.NoError(t, err)
	require.Equal(t, "accepted", response.Status)
	inbox, err := pipeSQLite(t, destination).GetInbox(
		ctx, outbox.TargetAgentID, "", 5,
	)
	require.NoError(t, err)
	require.Len(t, inbox, 1)
	require.Equal(t, LinkedMessageAuthorizationMode,
		inbox[0].FederationAuthorizationMode)
	require.NoError(t, destination.mgr.AuthorizeImportedPipe(ctx, inbox[0]))
	return inbox[0]
}

func enqueueLinkedResult(
	t *testing.T,
	source, destination *testChain,
	imported *store.PipelineMessage,
	resultAgentID string,
	resultKey ed25519.PrivateKey,
	result string,
) *store.PipelineTransportOutbox {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, pipeSQLite(t, destination).ClaimPipeline(
		ctx, imported.PipeID, resultAgentID,
	))
	body, err := json.Marshal(signedPipeResultRequest{
		Result: result, SourcePipeID: imported.SourcePipeID,
		SourceChainID: destination.chainID,
	})
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Second)
	proof := signedPipeProof(
		t, resultKey, resultAgentID, http.MethodPut,
		"/v1/pipe/"+imported.PipeID+"/result", body, now.Unix(),
	)
	resultOutbox := &store.PipelineTransportOutbox{
		EventID: PipelineProofEventID(destination.chainID, "result", proof),
		PipeID:  imported.PipeID, RemoteChainID: source.chainID,
		EventKind: "result", PolicyEpoch: imported.FederationPolicyEpoch,
		AgreementID:       imported.FederationAgreementID,
		ContactID:         imported.FederationContactID,
		ContactRevision:   imported.FederationContactRevision,
		AuthorizationMode: imported.FederationAuthorizationMode,
		LinkedRelation:    append([]byte(nil), imported.FederationLinkedRelation...),
		SourceAgentID:     resultAgentID, TargetAgentID: imported.FromAgent,
		Proof: proof, CreatedAt: now, ExpiresAt: now.Add(24 * time.Hour),
	}
	require.NoError(t, pipeSQLite(t, destination).
		CompleteFederatedPipelineWithTransport(
			ctx, imported.PipeID, resultAgentID, result, resultOutbox,
		))
	return resultOutbox
}

func deliverLinkedResult(
	t *testing.T,
	source, destination *testChain,
	imported *store.PipelineMessage,
	resultAgentID string,
	resultKey ed25519.PrivateKey,
	result string,
) {
	t.Helper()
	ctx := context.Background()
	resultOutbox := enqueueLinkedResult(
		t, source, destination, imported, resultAgentID, resultKey, result,
	)
	event, terminal, err := destination.mgr.buildPipelineEvent(
		ctx, pipeSQLite(t, destination), resultOutbox,
	)
	require.NoError(t, err)
	require.False(t, terminal)
	event.Result = result
	response, err := destination.mgr.PushPipeEvent(
		ctx, source.chainID, event,
	)
	require.NoError(t, err)
	require.Equal(t, "accepted", response.Status)
}

func TestLinkedReaderMessagingBidirectionalExactIDsOverTwoNodes(t *testing.T) {
	pair := newLinkedMessagingPair(t)
	pair.enableBothDirections(t)
	ctx := context.Background()

	guestToMember, err := pair.peer.mgr.ResolveRemoteLinkedPipeTarget(
		ctx, pair.guestID, pair.memberID+"@"+pair.host.chainID,
	)
	require.NoError(t, err)
	require.Equal(t, LinkedMessageGuestToMember,
		guestToMember.LinkedRelation.Direction)
	guestSource, guestOutbox := enqueueLinkedSend(
		t, pair.peer, pair.host, pair.guestID, pair.guestKey,
		guestToMember, "guest-to-member",
	)
	guestImported := deliverLinkedSend(t, pair.peer, pair.host, guestOutbox)
	deliverLinkedResult(
		t, pair.peer, pair.host, guestImported,
		pair.memberID, pair.memberKey, "member reply",
	)
	guestSource, err = pipeSQLite(t, pair.peer).GetPipeline(
		ctx, guestSource.PipeID,
	)
	require.NoError(t, err)
	require.Equal(t, "completed", guestSource.Status)
	require.Equal(t, "member reply", guestSource.Result)

	memberToGuest, err := pair.host.mgr.ResolveRemoteLinkedPipeTarget(
		ctx, pair.memberID, pair.guestID+"@"+pair.peer.chainID,
	)
	require.NoError(t, err)
	require.Equal(t, LinkedMessageMemberToGuest,
		memberToGuest.LinkedRelation.Direction)
	memberSource, memberOutbox := enqueueLinkedSend(
		t, pair.host, pair.peer, pair.memberID, pair.memberKey,
		memberToGuest, "member-to-guest",
	)
	memberImported := deliverLinkedSend(t, pair.host, pair.peer, memberOutbox)
	deliverLinkedResult(
		t, pair.host, pair.peer, memberImported,
		pair.guestID, pair.guestKey, "guest reply",
	)
	memberSource, err = pipeSQLite(t, pair.host).GetPipeline(
		ctx, memberSource.PipeID,
	)
	require.NoError(t, err)
	require.Equal(t, "completed", memberSource.Status)
	require.Equal(t, "guest reply", memberSource.Result)

	_, nonMemberKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	nonMemberID := enrollV23OrdinaryAgent(
		t, pair.host, "non-member", nonMemberKey, 20,
	)
	_, err = pair.host.mgr.ResolveRemoteLinkedPipeTarget(
		ctx, nonMemberID, pair.guestID+"@"+pair.peer.chainID,
	)
	require.Error(t, err)
}

func TestLinkedReaderMessagingRevocationCutsAdmittedWorkAndQueuedSend(t *testing.T) {
	pair := newLinkedMessagingPair(t)
	pair.enableBothDirections(t)
	ctx := context.Background()
	target, err := pair.host.mgr.ResolveRemoteLinkedPipeTarget(
		ctx, pair.memberID, pair.guestID+"@"+pair.peer.chainID,
	)
	require.NoError(t, err)
	_, outbox := enqueueLinkedSend(
		t, pair.host, pair.peer, pair.memberID, pair.memberKey,
		target, "revoked-queued",
	)
	admitted := deliverLinkedSend(t, pair.host, pair.peer, outbox)

	paused := pair.guest
	paused.Revision++
	paused.State = store.FederatedGuestStatePaused
	require.NoError(t, store.SignFederatedGroupGuest(&paused, pair.host.agentKey))
	require.NoError(t, pipeSQLite(t, pair.host).PutFederatedGroupGuest(ctx, paused))

	require.Error(t, pair.peer.mgr.AuthorizeImportedPipe(ctx, admitted))
	_, terminal, err := pair.host.mgr.buildPipelineEvent(
		ctx, pipeSQLite(t, pair.host), outbox,
	)
	require.Error(t, err)
	require.True(t, terminal)
}

func TestLinkedReaderMessagingRevocationCutsCompletedResultBeforePush(t *testing.T) {
	tests := []struct {
		name      string
		resolveOn func(*linkedMessagingPair) *testChain
		sourceOn  func(*linkedMessagingPair) *testChain
		destOn    func(*linkedMessagingPair) *testChain
		sourceID  func(*linkedMessagingPair) string
		targetID  func(*linkedMessagingPair) string
		sourceKey func(*linkedMessagingPair) ed25519.PrivateKey
		resultKey func(*linkedMessagingPair) ed25519.PrivateKey
	}{
		{
			name:      "guest-to-member",
			resolveOn: func(p *linkedMessagingPair) *testChain { return p.peer },
			sourceOn:  func(p *linkedMessagingPair) *testChain { return p.peer },
			destOn:    func(p *linkedMessagingPair) *testChain { return p.host },
			sourceID:  func(p *linkedMessagingPair) string { return p.guestID },
			targetID:  func(p *linkedMessagingPair) string { return p.memberID },
			sourceKey: func(p *linkedMessagingPair) ed25519.PrivateKey { return p.guestKey },
			resultKey: func(p *linkedMessagingPair) ed25519.PrivateKey { return p.memberKey },
		},
		{
			name:      "member-to-guest",
			resolveOn: func(p *linkedMessagingPair) *testChain { return p.host },
			sourceOn:  func(p *linkedMessagingPair) *testChain { return p.host },
			destOn:    func(p *linkedMessagingPair) *testChain { return p.peer },
			sourceID:  func(p *linkedMessagingPair) string { return p.memberID },
			targetID:  func(p *linkedMessagingPair) string { return p.guestID },
			sourceKey: func(p *linkedMessagingPair) ed25519.PrivateKey { return p.memberKey },
			resultKey: func(p *linkedMessagingPair) ed25519.PrivateKey { return p.guestKey },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pair := newLinkedMessagingPair(t)
			pair.enableBothDirections(t)
			ctx := context.Background()
			source := tc.sourceOn(pair)
			destination := tc.destOn(pair)
			target, err := tc.resolveOn(pair).mgr.ResolveRemoteLinkedPipeTarget(
				ctx, tc.sourceID(pair),
				tc.targetID(pair)+"@"+destination.chainID,
			)
			require.NoError(t, err)
			_, sendOutbox := enqueueLinkedSend(
				t, source, destination, tc.sourceID(pair),
				tc.sourceKey(pair), target, "result-revoked-"+tc.name,
			)
			imported := deliverLinkedSend(t, source, destination, sendOutbox)
			resultOutbox := enqueueLinkedResult(
				t, source, destination, imported, tc.targetID(pair),
				tc.resultKey(pair), "completed before link revoke",
			)

			paused := pair.guest
			paused.Revision++
			paused.State = store.FederatedGuestStatePaused
			require.NoError(t, store.SignFederatedGroupGuest(
				&paused, pair.host.agentKey,
			))
			require.NoError(t, pipeSQLite(t, pair.host).
				PutFederatedGroupGuest(ctx, paused))

			event, terminal, err := destination.mgr.buildPipelineEvent(
				ctx, pipeSQLite(t, destination), resultOutbox,
			)
			require.Nil(t, event,
				"revoked result bytes must never enter an outbound envelope")
			require.Error(t, err)
			require.True(t, terminal)
			completed, err := pipeSQLite(t, destination).GetPipeline(
				ctx, imported.PipeID,
			)
			require.NoError(t, err)
			require.Equal(t, "completed", completed.Status,
				"local work history remains truthful after delivery revocation")
			transport, err := pipeSQLite(t, destination).GetPipelineTransport(
				ctx, resultOutbox.EventID,
			)
			require.NoError(t, err)
			require.Equal(t, "pending", transport.State)
		})
	}
}

func TestLinkedReaderMessagingResultMutationCancelsBeforePayloadPush(t *testing.T) {
	tests := []struct {
		name         string
		mutate       func(*linkedMessagingPair) error
		mutationLive func(*linkedMessagingPair) bool
	}{
		{
			name: "receiver-consent-off",
			mutate: func(pair *linkedMessagingPair) error {
				ctx := context.Background()
				consent, err := pair.peer.mgr.GetLinkedMessageConsent(
					ctx, pair.host.chainID, pair.memberID, pair.guestID,
				)
				if err != nil {
					return err
				}
				_, err = pair.peer.mgr.SetLinkedMessageConsentCAS(
					ctx, pair.host.chainID, pair.memberID, pair.guestID,
					consent.Revision, false,
				)
				return err
			},
			mutationLive: func(pair *linkedMessagingPair) bool {
				_, blocked := pair.peer.mgr.
					syncPolicyDeliveryLease(pair.host.chainID).
					deliveryBlocked()
				return blocked
			},
		},
		{
			name: "receiver-bit16",
			mutate: func(pair *linkedMessagingPair) error {
				enrollment, err := pair.peer.badger.GetAppV23Enrollment(
					pair.guestID,
				)
				if err != nil {
					return err
				}
				role, err := pair.peer.badger.GetAppV23Role(pair.guestID)
				if err != nil {
					return err
				}
				return pair.peer.badger.SetAppV23Policy(
					hex.EncodeToString(pair.peer.agentPub), pair.guestID,
					role.Role, enrollment.Profile, enrollment.Profile,
					enrollment.Clearance,
					store.AgentCapabilityDenyFederatedPipe,
					role.Revision, enrollment.Revision, 70,
				)
			},
			mutationLive: func(pair *linkedMessagingPair) bool {
				_, blocked := pair.peer.mgr.
					syncPolicyDeliveryLease(pair.host.chainID).
					deliveryBlocked()
				return blocked
			},
		},
		{
			name: "direct-consensus-agreement-revoke",
			mutate: func(pair *linkedMessagingPair) error {
				scoped := pair.peer.badger.BeginConsensusTransaction(nil)
				if err := scoped.UpdateCrossFedStatus(
					pair.host.chainID, "revoked",
				); err != nil {
					scoped.DiscardConsensusTransaction()
					return err
				}
				return scoped.CommitConsensusTransaction()
			},
			mutationLive: func(pair *linkedMessagingPair) bool {
				_, blocked := pair.peer.mgr.
					syncPolicyDeliveryLease(pair.host.chainID).
					deliveryBlocked()
				return blocked
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pair := newLinkedMessagingPair(t)
			pair.enableBothDirections(t)
			ctx := context.Background()
			target, err := pair.host.mgr.ResolveRemoteLinkedPipeTarget(
				ctx, pair.memberID, pair.guestID+"@"+pair.peer.chainID,
			)
			require.NoError(t, err)
			source, sendOutbox := enqueueLinkedSend(
				t, pair.host, pair.peer, pair.memberID, pair.memberKey,
				target, "result-race-"+tc.name,
			)
			imported := deliverLinkedSend(
				t, pair.host, pair.peer, sendOutbox,
			)
			resultOutbox := enqueueLinkedResult(
				t, pair.host, pair.peer, imported, pair.guestID,
				pair.guestKey, "must remain local after revoke",
			)

			entered := make(chan struct{})
			release := make(chan struct{})
			pair.peer.mgr.linkedResultBeforePushHook = func(string) {
				close(entered)
				<-release
			}
			var pushCalls atomic.Int32
			pair.peer.mgr.pipeEventPushFn = func(
				context.Context, string, *PipeEvent,
			) (*PipeEventResponse, error) {
				pushCalls.Add(1)
				return &PipeEventResponse{Status: "accepted"}, nil
			}
			deliveryDone := make(chan struct{})
			go func() {
				defer close(deliveryDone)
				pair.peer.mgr.deliverPipelineEvent(
					ctx, pipeSQLite(t, pair.peer), resultOutbox,
				)
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("result delivery did not reach final no-payload gate")
			}

			mutationDone := make(chan error, 1)
			go func() { mutationDone <- tc.mutate(pair) }()
			deadline := time.Now().Add(5 * time.Second)
			for !tc.mutationLive(pair) && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			require.True(t, tc.mutationLive(pair),
				"authorization mutation did not publish its delivery block")
			close(release)
			select {
			case <-deliveryDone:
			case <-time.After(5 * time.Second):
				t.Fatal("canceled result delivery did not drain")
			}
			select {
			case mutationErr := <-mutationDone:
				require.NoError(t, mutationErr)
			case <-time.After(5 * time.Second):
				t.Fatal("authorization mutation did not finish after drain")
			}
			require.Zero(t, pushCalls.Load(),
				"result payload push ran after revocation was published")
			storedSource, err := pipeSQLite(t, pair.host).GetPipeline(
				ctx, source.PipeID,
			)
			require.NoError(t, err)
			require.Equal(t, "pending", storedSource.Status)
			transport, err := pipeSQLite(t, pair.peer).
				GetPipelineTransport(ctx, resultOutbox.EventID)
			require.NoError(t, err)
			require.Equal(t, "pending", transport.State,
				"operator-controlled kill remains retryable if re-enabled")
		})
	}
}

func TestLinkedReaderMessagingOverlappingGenerationAndConsensusMutationDrain(t *testing.T) {
	pair := newLinkedMessagingPair(t)
	pair.enableBothDirections(t)
	ctx := context.Background()
	target, err := pair.host.mgr.ResolveRemoteLinkedPipeTarget(
		ctx, pair.memberID, pair.guestID+"@"+pair.peer.chainID,
	)
	require.NoError(t, err)
	_, sendOutbox := enqueueLinkedSend(
		t, pair.host, pair.peer, pair.memberID, pair.memberKey,
		target, "overlapping-authorization-mutations",
	)
	imported := deliverLinkedSend(t, pair.host, pair.peer, sendOutbox)
	resultOutbox := enqueueLinkedResult(
		t, pair.host, pair.peer, imported, pair.guestID,
		pair.guestKey, "must not cross the revoked edge",
	)

	entered := make(chan struct{})
	release := make(chan struct{})
	pair.peer.mgr.linkedResultBeforePushHook = func(string) {
		close(entered)
		<-release
	}
	var pushCalls atomic.Int32
	pair.peer.mgr.pipeEventPushFn = func(
		context.Context, string, *PipeEvent,
	) (*PipeEventResponse, error) {
		pushCalls.Add(1)
		return &PipeEventResponse{Status: "accepted"}, nil
	}
	deliveryDone := make(chan struct{})
	go func() {
		defer close(deliveryDone)
		pair.peer.mgr.deliverPipelineEvent(
			ctx, pipeSQLite(t, pair.peer), resultOutbox,
		)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("result delivery did not reach final no-payload gate")
	}

	generationReady := make(chan *SyncPolicyGenerationMutation, 1)
	go func() {
		generationReady <- pair.peer.mgr.
			BeginSyncPolicyGenerationMutation(pair.host.chainID)
	}()
	lease := pair.peer.mgr.syncPolicyDeliveryLease(pair.host.chainID)
	require.Eventually(t, func() bool {
		lease.stateMu.Lock()
		defer lease.stateMu.Unlock()
		return lease.generationBlocks == 1
	}, 5*time.Second, time.Millisecond,
		"peer mutation did not publish its block before waiting for delivery")

	consensusDone := make(chan error, 1)
	go func() {
		scoped := pair.peer.badger.BeginConsensusTransaction(nil)
		if updateErr := scoped.UpdateCrossFedStatus(
			pair.host.chainID, "revoked",
		); updateErr != nil {
			scoped.DiscardConsensusTransaction()
			consensusDone <- updateErr
			return
		}
		consensusDone <- scoped.CommitConsensusTransaction()
	}()
	require.Eventually(t, func() bool {
		lease.stateMu.Lock()
		defer lease.stateMu.Unlock()
		return lease.generationBlocks == 2
	}, 5*time.Second, time.Millisecond,
		"scoped consensus mutation did not publish its independent authorization block")
	select {
	case consensusErr := <-consensusDone:
		require.NoError(t, consensusErr)
		t.Fatal("overlapping consensus mutation returned before stale delivery drained")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	select {
	case <-deliveryDone:
	case <-time.After(5 * time.Second):
		t.Fatal("canceled result delivery did not drain")
	}
	var generation *SyncPolicyGenerationMutation
	select {
	case generation = <-generationReady:
	case <-time.After(5 * time.Second):
		t.Fatal("peer generation mutation did not finish draining")
	}
	generation.Restore()
	select {
	case consensusErr := <-consensusDone:
		require.NoError(t, consensusErr)
	case <-time.After(5 * time.Second):
		t.Fatal("global consensus mutation did not finish after delivery drained")
	}
	require.Zero(t, pushCalls.Load(),
		"result payload push ran after overlapping authorization blocks")
}

func TestLinkedReaderMessagingAgreementMutationIsPeerScoped(t *testing.T) {
	pair := newLinkedMessagingPair(t)
	ctx, finishUnrelated, err := pair.peer.mgr.beginLinkedDelivery(
		context.Background(), "unrelated-chain",
	)
	require.NoError(t, err)
	defer finishUnrelated()

	mutationDone := make(chan error, 1)
	go func() {
		mutationDone <- pair.peer.badger.UpdateCrossFedStatus(
			pair.host.chainID, "revoked",
		)
	}()
	select {
	case mutationErr := <-mutationDone:
		require.NoError(t, mutationErr)
	case <-time.After(5 * time.Second):
		t.Fatal("peer A agreement mutation waited on unrelated peer B delivery")
	}
	select {
	case <-ctx.Done():
		t.Fatal("peer A agreement mutation canceled unrelated peer B delivery")
	default:
	}
}

func TestLinkedReaderMessagingConsentAndMembershipFailClosed(t *testing.T) {
	pair := newLinkedMessagingPair(t)
	pair.enableBothDirections(t)
	ctx := context.Background()

	consent, err := pair.host.mgr.GetLinkedMessageConsent(
		ctx, pair.peer.chainID, pair.guestID, pair.memberID,
	)
	require.NoError(t, err)
	require.NotNil(t, consent)
	_, err = pair.host.mgr.SetLinkedMessageConsentCAS(
		ctx, pair.peer.chainID, pair.guestID, pair.memberID,
		consent.Revision, false,
	)
	require.NoError(t, err)
	_, err = pair.peer.mgr.ResolveRemoteLinkedPipeTarget(
		ctx, pair.guestID, pair.memberID+"@"+pair.host.chainID,
	)
	require.Error(t, err)

	rootID := hex.EncodeToString(pair.host.agentPub)
	group, err := pair.host.badger.GetAppV23AccessGroup("linked-team")
	require.NoError(t, err)
	require.NoError(t, pair.host.badger.MutateAppV23AccessGroup(
		rootID, group.GroupID, group.Name, nil, group.Revision, false, 30,
	))
	_, err = pair.host.mgr.ResolveRemoteLinkedPipeTarget(
		ctx, pair.memberID, pair.guestID+"@"+pair.peer.chainID,
	)
	require.Error(t, err)
}

func TestLinkedRelationRemoteFreshnessDoesNotHoldLocalRevocationLocks(t *testing.T) {
	pair := newLinkedMessagingPair(t)
	pair.enableBothDirections(t)
	ctx := context.Background()
	target, err := pair.host.mgr.ResolveRemoteLinkedPipeTarget(
		ctx, pair.memberID, pair.guestID+"@"+pair.peer.chainID,
	)
	require.NoError(t, err)
	_, outbox := enqueueLinkedSend(
		t, pair.host, pair.peer, pair.memberID, pair.memberKey,
		target, "no-lock-callback",
	)
	admitted := deliverLinkedSend(t, pair.host, pair.peer, outbox)

	entered := make(chan struct{})
	release := make(chan struct{})
	pair.peer.mgr.linkedRelationRevalidateFn = func(
		callCtx context.Context,
		_ *store.CrossFedRecord,
		_ *LinkedMessageRelation,
	) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-callCtx.Done():
			return callCtx.Err()
		}
	}
	authDone := make(chan error, 1)
	go func() {
		authDone <- pair.peer.mgr.AuthorizeImportedPipe(ctx, admitted)
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("remote relation callback was not reached")
	}

	pauseDone := make(chan error, 1)
	go func() {
		_, pauseErr := pair.peer.mgr.SetPeerRBACPaused(
			ctx, pair.host.chainID, true,
		)
		pauseDone <- pauseErr
	}()
	select {
	case pauseErr := <-pauseDone:
		require.NoError(t, pauseErr)
	case <-time.After(750 * time.Millisecond):
		t.Fatal("local policy pause waited on a remote freshness callback")
	}
	close(release)
	select {
	case authErr := <-authDone:
		require.Error(t, authErr)
	case <-time.After(2 * time.Second):
		t.Fatal("authorization did not finish after callback release")
	}
}

func TestLinkedReaderMessagingConsentRevisionAndPolicyRepairRejectReplay(t *testing.T) {
	pair := newLinkedMessagingPair(t)
	pair.enableBothDirections(t)
	ctx := context.Background()
	target, err := pair.peer.mgr.ResolveRemoteLinkedPipeTarget(
		ctx, pair.guestID, pair.memberID+"@"+pair.host.chainID,
	)
	require.NoError(t, err)
	_, outbox := enqueueLinkedSend(
		t, pair.peer, pair.host, pair.guestID, pair.guestKey,
		target, "consent-replay",
	)
	staleEvent, terminal, err := pair.peer.mgr.buildPipelineEvent(
		ctx, pipeSQLite(t, pair.peer), outbox,
	)
	require.NoError(t, err)
	require.False(t, terminal)

	consent, err := pair.host.mgr.GetLinkedMessageConsent(
		ctx, pair.peer.chainID, pair.guestID, pair.memberID,
	)
	require.NoError(t, err)
	disabledRevision, err := pair.host.mgr.SetLinkedMessageConsentCAS(
		ctx, pair.peer.chainID, pair.guestID, pair.memberID,
		consent.Revision, false,
	)
	require.NoError(t, err)
	_, err = pair.host.mgr.SetLinkedMessageConsentCAS(
		ctx, pair.peer.chainID, pair.guestID, pair.memberID,
		disabledRevision, true,
	)
	require.NoError(t, err)
	_, err = pair.peer.mgr.PushPipeEvent(
		ctx, pair.host.chainID, staleEvent,
	)
	require.Error(t, err, "acceptance off/on must not revive an old relation")
	fresh, err := pair.peer.mgr.ResolveRemoteLinkedPipeTarget(
		ctx, pair.guestID, pair.memberID+"@"+pair.host.chainID,
	)
	require.NoError(t, err)
	require.NotEqual(t, target.ContactRevision, fresh.ContactRevision)

	// Replacing the exact peer-policy generation invalidates both the local
	// consent binding and the guest agreement digest. Neither stale relation
	// may be replayed until the operator explicitly rebinds both.
	_, err = pair.host.mgr.ReplacePeerRBACPolicy(
		ctx, pair.peer.chainID,
		[]store.PeerRBACDomainPermission{{Domain: "shared", Read: true}},
	)
	require.NoError(t, err)
	_, err = pair.peer.mgr.PushPipeEvent(
		ctx, pair.host.chainID, staleEvent,
	)
	require.Error(t, err)
	_, err = pair.peer.mgr.ResolveRemoteLinkedPipeTarget(
		ctx, pair.guestID, pair.memberID+"@"+pair.host.chainID,
	)
	require.Error(t, err)
}

func TestLinkedReaderMessagingProofCannotBeReboundToFreshRelation(t *testing.T) {
	pair := newLinkedMessagingPair(t)
	pair.enableBothDirections(t)
	ctx := context.Background()
	target, err := pair.peer.mgr.ResolveRemoteLinkedPipeTarget(
		ctx, pair.guestID, pair.memberID+"@"+pair.host.chainID,
	)
	require.NoError(t, err)
	_, outbox := enqueueLinkedSend(
		t, pair.peer, pair.host, pair.guestID, pair.guestKey,
		target, "proof-relation-rebind",
	)
	accepted, terminal, err := pair.peer.mgr.buildPipelineEvent(
		ctx, pipeSQLite(t, pair.peer), outbox,
	)
	require.NoError(t, err)
	require.False(t, terminal)
	response, err := pair.peer.mgr.PushPipeEvent(
		ctx, pair.host.chainID, accepted,
	)
	require.NoError(t, err)
	require.Equal(t, "accepted", response.Status)

	consent, err := pair.host.mgr.GetLinkedMessageConsent(
		ctx, pair.peer.chainID, pair.guestID, pair.memberID,
	)
	require.NoError(t, err)
	disabledRevision, err := pair.host.mgr.SetLinkedMessageConsentCAS(
		ctx, pair.peer.chainID, pair.guestID, pair.memberID,
		consent.Revision, false,
	)
	require.NoError(t, err)
	_, err = pair.host.mgr.SetLinkedMessageConsentCAS(
		ctx, pair.peer.chainID, pair.guestID, pair.memberID,
		disabledRevision, true,
	)
	require.NoError(t, err)
	fresh, err := pair.peer.mgr.ResolveRemoteLinkedPipeTarget(
		ctx, pair.guestID, pair.memberID+"@"+pair.host.chainID,
	)
	require.NoError(t, err)
	require.NotEqual(t, accepted.ContactRevision, fresh.ContactRevision)

	// The proof still signs the same exact /pipe/send request. A malicious
	// sender must not be able to wrap it in a newly valid relation to evade the
	// receiver's durable replay key.
	rebound := *accepted
	rebound.PolicyEpoch = fresh.PolicyEpoch
	rebound.AgreementID = fresh.AgreementID
	rebound.ContactID = fresh.ContactID
	rebound.ContactRevision = fresh.ContactRevision
	rebound.LinkedRelation = fresh.LinkedRelation
	_, err = pair.peer.mgr.PushPipeEvent(ctx, pair.host.chainID, &rebound)
	require.Error(t, err)
	require.Contains(t, err.Error(), "federated pipeline replay conflict")

	inbox, err := pipeSQLite(t, pair.host).GetInbox(
		ctx, pair.memberID, "", 5,
	)
	require.NoError(t, err)
	require.Len(t, inbox, 1,
		"proof rebinding must not create a second imported request")
}

func TestLinkedMessageRejectsAgentProofBeforeRemoteFreshnessCallback(t *testing.T) {
	pair := newLinkedMessagingPair(t)
	pair.enableBothDirections(t)
	ctx := context.Background()
	target, err := pair.host.mgr.ResolveRemoteLinkedPipeTarget(
		ctx, pair.memberID, pair.guestID+"@"+pair.peer.chainID,
	)
	require.NoError(t, err)
	_, outbox := enqueueLinkedSend(
		t, pair.host, pair.peer, pair.memberID, pair.memberKey,
		target, "proof-before-callback",
	)
	event, terminal, err := pair.host.mgr.buildPipelineEvent(
		ctx, pipeSQLite(t, pair.host), outbox,
	)
	require.NoError(t, err)
	require.False(t, terminal)
	agreement, err := pair.peer.mgr.ActiveAgreement(pair.host.chainID)
	require.NoError(t, err)
	hostOperator := hex.EncodeToString(pair.host.agentPub)
	var callbacks atomic.Int32
	pair.peer.mgr.linkedRelationRevalidateFn = func(
		context.Context, *store.CrossFedRecord, *LinkedMessageRelation,
	) error {
		callbacks.Add(1)
		return nil
	}

	badSignature := *event
	badSignature.Proof = event.Proof
	badSignature.Proof.Signature =
		append([]byte(nil), event.Proof.Signature...)
	badSignature.Proof.Signature[0] ^= 0xff
	badSignature.EventID = PipelineProofEventID(
		badSignature.SourceChainID, badSignature.Kind, badSignature.Proof,
	)
	recorder := callPipeEvent(
		t, pair.peer.mgr, agreement, hostOperator, &badSignature,
	)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Zero(t, callbacks.Load())

	wrongBody, err := json.Marshal(signedPipeSendRequest{
		ToAgent:            event.TargetAgentID,
		SourceChainID:      event.SourceChainID,
		DestinationChainID: event.DestinationChainID,
		Intent:             event.Intent, Payload: "different signed payload",
		TTLMinutes: 60,
	})
	require.NoError(t, err)
	wrongCanonical := *event
	wrongCanonical.Proof = signedPipeProof(
		t, pair.memberKey, pair.memberID, http.MethodPost,
		"/v1/pipe/send", wrongBody, event.Proof.Timestamp,
	)
	wrongCanonical.EventID = PipelineProofEventID(
		wrongCanonical.SourceChainID, wrongCanonical.Kind,
		wrongCanonical.Proof,
	)
	recorder = callPipeEvent(
		t, pair.peer.mgr, agreement, hostOperator, &wrongCanonical,
	)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Zero(t, callbacks.Load())

	recorder = callPipeEvent(
		t, pair.peer.mgr, agreement, hostOperator, event,
	)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.EqualValues(t, 1, callbacks.Load(),
		"valid remote-hosted work must still perform one freshness callback")
}

func TestLinkedMessagePrivateRoutesAreBoundedAndDoNotEnumerateFailures(t *testing.T) {
	pair := newLinkedMessagingPair(t)
	agreement, err := pair.host.mgr.ActiveAgreement(pair.peer.chainID)
	require.NoError(t, err)
	authenticatedPeer := &peerIdentity{
		ChainID:   pair.peer.chainID,
		AgentID:   hex.EncodeToString(pair.peer.agentPub),
		Agreement: agreement,
	}
	unknownIDs := make([]string, 2)
	for i := range unknownIDs {
		pub, _, keyErr := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, keyErr)
		unknownIDs[i] = hex.EncodeToString(pub)
	}

	assertSameUnavailable := func(
		t *testing.T,
		handler http.HandlerFunc,
		path string,
		payloads ...any,
	) {
		t.Helper()
		var baseline string
		for i, payload := range payloads {
			recorder := callLinkedMessageHandler(
				t, handler, path, authenticatedPeer, payload,
			)
			require.Equal(t, http.StatusNotFound, recorder.Code)
			if i == 0 {
				baseline = recorder.Body.String()
			} else {
				require.Equal(t, baseline, recorder.Body.String())
			}
		}
	}

	assertSameUnavailable(
		t, pair.host.mgr.handleLinkedMessageResolve,
		"/fed/v1/pipe/linked/resolve",
		LinkedMessageResolveRequest{
			Version:       LinkedMessageRelationVersion,
			Direction:     LinkedMessageGuestToMember,
			SourceAgentID: pair.guestID, TargetAgentID: pair.memberID,
		},
		LinkedMessageResolveRequest{
			Version:       LinkedMessageRelationVersion,
			Direction:     LinkedMessageGuestToMember,
			SourceAgentID: unknownIDs[0], TargetAgentID: pair.memberID,
		},
		LinkedMessageResolveRequest{
			Version:       LinkedMessageRelationVersion,
			Direction:     LinkedMessageGuestToMember,
			SourceAgentID: pair.guestID, TargetAgentID: unknownIDs[1],
		},
	)
	assertSameUnavailable(
		t, pair.host.mgr.handleLinkedMessageConsentOffer,
		"/fed/v1/pipe/linked/consent-offer",
		LinkedMessageConsentOfferRequest{
			Version:       LinkedMessageRelationVersion,
			SourceAgentID: unknownIDs[0], TargetAgentID: pair.guestID,
		},
		LinkedMessageConsentOfferRequest{
			Version:       LinkedMessageRelationVersion,
			SourceAgentID: pair.memberID, TargetAgentID: unknownIDs[1],
		},
	)
	assertSameUnavailable(
		t, pair.host.mgr.handleLinkedMessageRevalidate,
		"/fed/v1/pipe/linked/revalidate",
		LinkedMessageRevalidateRequest{
			Version: LinkedMessageRelationVersion,
		},
		LinkedMessageRevalidateRequest{
			Version: LinkedMessageRelationVersion,
			Relation: &LinkedMessageRelation{
				Version: LinkedMessageRelationVersion,
			},
		},
	)

	// Candidate discovery is deliberately exact-guest scoped. Two canonical
	// unknown identities receive the same bounded empty projection.
	var emptyBody string
	for i, unknownID := range unknownIDs {
		recorder := callLinkedMessageHandler(
			t, pair.host.mgr.handleLinkedMessageConsentCandidates,
			"/fed/v1/pipe/linked/consent-candidates", authenticatedPeer,
			LinkedMessageConsentCandidateRequest{
				Version:      LinkedMessageRelationVersion,
				GuestAgentID: unknownID,
			},
		)
		require.Equal(t, http.StatusOK, recorder.Code)
		if i == 0 {
			emptyBody = recorder.Body.String()
		} else {
			require.Equal(t, emptyBody, recorder.Body.String())
		}
		require.LessOrEqual(t, recorder.Body.Len(),
			maxLinkedMessageCandidateResponseBytes)
	}

	// Handler-local limits remain fail-closed even if a caller bypasses the
	// outer signed-route middleware used by the real mTLS server.
	oversized := bytes.Repeat([]byte{'x'}, maxLinkedMessageResolveBytes+1)
	request := httptest.NewRequest(
		http.MethodPost, "/fed/v1/pipe/linked/resolve",
		bytes.NewReader(oversized),
	)
	request = request.WithContext(context.WithValue(
		request.Context(), peerCtxKey{}, authenticatedPeer,
	))
	recorder := httptest.NewRecorder()
	pair.host.mgr.handleLinkedMessageResolve(recorder, request)
	require.Equal(t, http.StatusNotFound, recorder.Code)

	require.EqualValues(t, maxLinkedMessageResolveBytes,
		peerResponseLimit("/fed/v1/pipe/linked/resolve", nil))
	require.EqualValues(t, maxLinkedMessageResolveBytes,
		peerResponseLimit("/fed/v1/pipe/linked/revalidate", nil))
	require.EqualValues(t, maxLinkedMessageResolveBytes,
		peerResponseLimit("/fed/v1/pipe/linked/consent-offer", nil))
	require.EqualValues(t, maxLinkedMessageCandidateResponseBytes,
		peerResponseLimit("/fed/v1/pipe/linked/consent-candidates", nil))
}

func TestLinkedMessageFieldsAreOmittedFromOrdinaryPipeWireEnvelope(t *testing.T) {
	event := PipeEvent{
		Version: PipeEventVersion, EventID: "ordinary-event", Kind: "send",
		SourceChainID: "chain-a", DestinationChainID: "chain-b",
		SourceAgentID: strings.Repeat("a", 64),
		TargetAgentID: strings.Repeat("b", 64),
		Intent:        "ordinary", Payload: "ordinary payload",
		CreatedAt:   time.Unix(100, 0).UTC(),
		ExpiresAt:   time.Unix(200, 0).UTC(),
		PolicyEpoch: "epoch", AgreementID: strings.Repeat("c", 64),
		ContactID:       strings.Repeat("d", 64),
		ContactRevision: strings.Repeat("e", 64),
		Proof: store.PipelineAgentProof{
			AgentID:   strings.Repeat("a", 64),
			Signature: []byte("signature"), Timestamp: 100,
			CanonicalRequest: []byte("legacy ordinary request"),
		},
	}
	body, err := json.Marshal(event)
	require.NoError(t, err)
	require.NotContains(t, string(body), `"authorization_mode"`)
	require.NotContains(t, string(body), `"linked_relation"`)

	var decoded PipeEvent
	require.NoError(t, json.Unmarshal(body, &decoded))
	require.Empty(t, decoded.AuthorizationMode)
	require.Nil(t, decoded.LinkedRelation)
}

func TestLinkedMessageProtocolMaximumShapeFitsResponseBounds(t *testing.T) {
	relation := &LinkedMessageRelation{
		Version:                 LinkedMessageRelationVersion,
		Direction:               LinkedMessageMemberToGuest,
		HostChainID:             strings.Repeat("h", 128),
		PeerChainID:             strings.Repeat("p", 128),
		SourceAgentID:           strings.Repeat("a", 64),
		TargetAgentID:           strings.Repeat("b", 64),
		GroupRevision:           ^uint64(0),
		HostAgreementDigest:     strings.Repeat("c", 64),
		ReceiverConsentRevision: int64(^uint64(0) >> 1),
		Guest: store.FederatedGroupGuest{
			GroupID:                strings.Repeat("g", 64),
			RemoteChainID:          strings.Repeat("p", 128),
			RemoteAgentID:          strings.Repeat("b", 64),
			AgreementBindingDigest: strings.Repeat("c", 64),
			MaxClassification:      4, Revision: int64(^uint64(0) >> 1),
			State:                       store.FederatedGuestStateActive,
			AuthorizedBy:                strings.Repeat("d", 64),
			AuthorityKind:               store.FederatedGuestAuthorityAdmin,
			AuthorityRootGeneration:     ^uint64(0),
			AuthorityRoleRevision:       ^uint64(0),
			AuthorityEnrollmentRevision: ^uint64(0),
			Signature:                   bytes.Repeat([]byte{0xff}, ed25519.SignatureSize),
		},
		SignerAgentID: strings.Repeat("d", 64),
		Signature:     bytes.Repeat([]byte{0xff}, ed25519.SignatureSize),
	}
	relationBody, err := json.Marshal(relation)
	require.NoError(t, err)
	require.LessOrEqual(t, len(relationBody), maxLinkedMessageResolveBytes)

	relations := make([]*LinkedMessageRelation,
		MaxLinkedMessageConsentCandidates)
	for i := range relations {
		relations[i] = relation
	}
	responseBody, err := json.Marshal(
		LinkedMessageConsentCandidateResponse{
			Version: LinkedMessageRelationVersion, Relations: relations,
		},
	)
	require.NoError(t, err)
	require.LessOrEqual(t, len(responseBody),
		maxLinkedMessageCandidateResponseBytes)

	manager := &Manager{}
	releases := make([]func(), 0, maxConcurrentLinkedRevalidationsPerPeer)
	for i := 0; i < maxConcurrentLinkedRevalidationsPerPeer; i++ {
		release, acquired := manager.acquireLinkedRevalidation("chain-peer")
		require.True(t, acquired)
		releases = append(releases, release)
	}
	_, acquired := manager.acquireLinkedRevalidation("chain-peer")
	require.False(t, acquired,
		"one peer must not create unbounded concurrent freshness callbacks")
	releases[0]()
	replacement, acquired := manager.acquireLinkedRevalidation("chain-peer")
	require.True(t, acquired)
	replacement()
	for _, release := range releases[1:] {
		release()
	}
}

func TestLinkedMessageRelationRejectsSameAgentIDInBothDirections(t *testing.T) {
	pair := newLinkedMessagingPair(t)
	pair.enableBothDirections(t)
	ctx := context.Background()
	hostSigner := hex.EncodeToString(pair.host.agentPub)

	guestToMember, err := pair.peer.mgr.ResolveRemoteLinkedPipeTarget(
		ctx, pair.guestID, pair.memberID+"@"+pair.host.chainID,
	)
	require.NoError(t, err)
	sameGuest := *guestToMember.LinkedRelation
	sameGuest.TargetAgentID = sameGuest.SourceAgentID
	require.NoError(t, sameGuest.sign(pair.host.agentKey))
	require.Error(t, validateLinkedMessageRelation(&sameGuest, hostSigner))

	memberToGuest, err := pair.host.mgr.ResolveRemoteLinkedPipeTarget(
		ctx, pair.memberID, pair.guestID+"@"+pair.peer.chainID,
	)
	require.NoError(t, err)
	sameMember := *memberToGuest.LinkedRelation
	sameMember.SourceAgentID = sameMember.TargetAgentID
	require.NoError(t, sameMember.sign(pair.host.agentKey))
	require.Error(t, validateLinkedMessageRelation(&sameMember, hostSigner))
}

func TestLinkedReaderMessagingDenyPipeCapabilityCutsExactRoute(t *testing.T) {
	deny := func(t *testing.T, node *testChain, agentID string, height int64) {
		t.Helper()
		enrollment, err := node.badger.GetAppV23Enrollment(agentID)
		require.NoError(t, err)
		role, err := node.badger.GetAppV23Role(agentID)
		require.NoError(t, err)
		require.NoError(t, node.badger.SetAppV23Policy(
			hex.EncodeToString(node.agentPub), agentID, role.Role,
			enrollment.Profile, enrollment.Profile, enrollment.Clearance,
			store.AgentCapabilityDenyFederatedPipe,
			role.Revision, enrollment.Revision, height,
		))
	}
	t.Run("guest source", func(t *testing.T) {
		pair := newLinkedMessagingPair(t)
		pair.enableBothDirections(t)
		deny(t, pair.peer, pair.guestID, 50)
		_, err := pair.peer.mgr.ResolveRemoteLinkedPipeTarget(
			context.Background(), pair.guestID,
			pair.memberID+"@"+pair.host.chainID,
		)
		require.Error(t, err)
	})
	t.Run("host member", func(t *testing.T) {
		pair := newLinkedMessagingPair(t)
		pair.enableBothDirections(t)
		deny(t, pair.host, pair.memberID, 50)
		_, err := pair.host.mgr.ResolveRemoteLinkedPipeTarget(
			context.Background(), pair.memberID,
			pair.guestID+"@"+pair.peer.chainID,
		)
		require.Error(t, err)
	})
}
