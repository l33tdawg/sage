package federation

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/require"
)

func enableReceiptV2Runtime(chain *testChain) {
	chain.mgr.postV26ForNextTx = func() bool { return true }
}

func restoreReceiptV2RuntimeAfterManagerRestart(t *testing.T, local, remote *testChain) {
	t.Helper()
	sqlite := pipeSQLite(t, local)
	local.mgr.federatedGuestStore = sqlite
	local.mgr.queryChallengeStore = sqlite
	local.mgr.localGroupResolver = v23GroupResolverFake{"v23-test-readers": {}}
	local.mgr.postV23ForNextTx = func() bool { return true }
	local.mgr.postV26ForNextTx = func() bool { return true }
	pair := []string{local.chainID, remote.chainID}
	sort.Strings(pair)
	seed := sha256.Sum256([]byte(pair[0] + "\x00" + pair[1]))
	commit, rollback, err := local.mgr.stageSeed(
		remote.chainID, seed[:], seed[:], time.Now().Unix(),
	)
	require.NoError(t, err)
	t.Cleanup(rollback)
	require.NoError(t, commit())
}

func configureReceiptRuntimeFixture(t *testing.T, source, destination *testChain) pipeFaultFixture {
	t.Helper()
	ctx := context.Background()
	_, sourceKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	_, targetKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	sourceID := enrollV23OrdinaryAgent(t, source, "receipt-source", sourceKey, 4)
	targetID := enrollV23OrdinaryAgent(t, destination, "receipt-target", targetKey, 4)
	require.NoError(t, pipeSQLite(t, source).CreateAgent(ctx, &store.AgentEntry{
		AgentID: sourceID, Name: "receipt-source", Status: "active",
	}))
	require.NoError(t, pipeSQLite(t, destination).CreateAgent(ctx, &store.AgentEntry{
		AgentID: targetID, Name: "receipt-target", Status: "active",
	}))
	require.NoError(t, destination.badger.RegisterDomain("fault-gate", targetID, "", 10))
	_, err = destination.mgr.ReplacePeerRBACPolicy(ctx, source.chainID,
		[]store.PeerRBACDomainPermission{{Domain: "fault-gate.work", Read: true}})
	require.NoError(t, err)
	grant, err := destination.mgr.LocalPipeContacts(ctx, source.chainID)
	require.NoError(t, err)
	require.Len(t, grant.Contacts, 1)
	contact := grant.Contacts[0]
	require.Equal(t, targetID, contact.AgentID)
	_, err = destination.mgr.SetPipeContactAcceptance(
		ctx, source.chainID, targetID, contact.ContactID, true,
	)
	require.NoError(t, err)
	target, err := source.mgr.ResolveRemotePipeTarget(ctx, targetID+"@"+destination.chainID)
	require.NoError(t, err)
	return pipeFaultFixture{
		sourceAgent: sourceID, sourceKey: sourceKey,
		targetAgent: targetID, targetKey: targetKey, target: target,
	}
}

func enqueueNegotiatedFaultSend(
	t *testing.T, source, destination *testChain, fixture pipeFaultFixture,
) (*store.PipelineMessage, *store.PipelineTransportOutbox) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	request := signedPipeSendRequest{
		ToAgent: fixture.targetAgent, SourceChainID: source.chainID,
		DestinationChainID: destination.chainID, Intent: "receipt-runtime",
		Payload: "durable receipt survives a lost response", TTLMinutes: 60,
	}
	body, err := json.Marshal(request)
	require.NoError(t, err)
	proof := signedPipeProof(t, fixture.sourceKey, fixture.sourceAgent,
		http.MethodPost, "/v1/pipe/send", body, now.Unix())
	msg := &store.PipelineMessage{
		PipeID:    "pipe-receipt-runtime-" + source.chainID,
		FromAgent: fixture.sourceAgent, ToAgent: fixture.targetAgent,
		DestinationChainID:          destination.chainID,
		FederationPolicyEpoch:       fixture.target.PolicyEpoch,
		FederationAgreementID:       fixture.target.AgreementID,
		FederationContactID:         fixture.target.ContactID,
		FederationContactRevision:   fixture.target.ContactRevision,
		FederationAuthorizationMode: fixture.target.AuthorizationMode,
		Intent:                      request.Intent, Payload: request.Payload, Status: "pending",
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	if fixture.target.LinkedRelation != nil {
		msg.FederationLinkedRelation, err = json.Marshal(fixture.target.LinkedRelation)
		require.NoError(t, err)
	}
	outbox := &store.PipelineTransportOutbox{
		EventID: PipelineProofEventID(source.chainID, "send", proof), PipeID: msg.PipeID,
		RemoteChainID: destination.chainID, EventKind: "send",
		PolicyEpoch: msg.FederationPolicyEpoch, AgreementID: msg.FederationAgreementID,
		ContactID: msg.FederationContactID, ContactRevision: msg.FederationContactRevision,
		AuthorizationMode: msg.FederationAuthorizationMode,
		LinkedRelation:    append([]byte(nil), msg.FederationLinkedRelation...),
		SourceAgentID:     fixture.sourceAgent, TargetAgentID: fixture.targetAgent,
		ReceiptProtocolVersion: fixture.target.ReceiptProtocolVersion,
		Proof:                  proof, CreatedAt: now, ExpiresAt: msg.ExpiresAt,
	}
	require.NoError(t, pipeSQLite(t, source).InsertPipelineWithTransport(
		context.Background(), msg, outbox,
	))
	return msg, outbox
}

func receiptWireEvent(outbox *store.FederatedReceiptOutbox) *PipeReceiptEvent {
	return &PipeReceiptEvent{
		Version: PipeReceiptVersion, EventID: outbox.EventID, MessageID: outbox.MessageID,
		RecipientPipeID: outbox.RecipientPipeID, SenderChainID: outbox.SenderChainID,
		RecipientChainID: outbox.RecipientChainID, SenderAgentID: outbox.SenderAgentID,
		RecipientAgentID: outbox.RecipientAgentID, ContentDigest: outbox.ContentDigest,
		Kind: outbox.Kind, EventAt: outbox.EventAt, PolicyEpoch: outbox.PolicyEpoch,
		AgreementID: outbox.AgreementID, ContactID: outbox.ContactID,
		ContactRevision: outbox.ContactRevision, AuthorizationMode: outbox.AuthorizationMode,
		RelationDigest: outbox.RelationDigest, Proof: outbox.Proof,
	}
}

func TestReceiptV2DirectMTLSLostResponseRetryAndRestart(t *testing.T) {
	ctx := context.Background()
	source := newTestChain(t, "receipt-runtime-source")
	destination := newTestChain(t, "receipt-runtime-destination")
	enableReceiptV2Runtime(source)
	enableReceiptV2Runtime(destination)
	sourceServer := startRestartablePipeServer(t, source, "")
	destinationServer := startRestartablePipeServer(t, destination, "")
	defer func() { sourceServer.stop(t); destinationServer.stop(t) }()
	federate(t, source, destination, "https://"+destinationServer.address, nil, 4, 0)
	federate(t, destination, source, "https://"+sourceServer.address, nil, 4, 0)
	enableV23Pair(t, source, destination, []string{"fault-gate", "fault-unrelated"})
	status, err := source.mgr.PeerStatus(ctx, destination.chainID)
	require.NoError(t, err)
	require.Contains(t, status.Capabilities, CapabilityFederatedPipelineReceiptsV2,
		"an app-v26 peer must advertise receipt-v2 before the send is negotiated")

	fixture := configureReceiptRuntimeFixture(t, source, destination)
	require.Equal(t, PipeReceiptVersion, fixture.target.ReceiptProtocolVersion)
	sourceMessage, sendOutbox := enqueueNegotiatedFaultSend(t, source, destination, fixture)
	source.mgr.pipelineDrain(ctx, pipeSQLite(t, source))
	storedSend, err := pipeSQLite(t, source).GetPipelineTransport(ctx, sendOutbox.EventID)
	require.NoError(t, err)
	require.Equal(t, "delivered", storedSend.State)
	projection, err := pipeSQLite(t, source).GetFederatedReceiptForSender(
		ctx, fixture.sourceAgent, sourceMessage.PipeID,
	)
	require.NoError(t, err)
	require.NotNil(t, projection.DeliveredAt)
	require.Nil(t, projection.ClaimedAt)

	imports, err := pipeSQLite(t, destination).ListPipelines(ctx, "", 10)
	require.NoError(t, err)
	require.Len(t, imports, 1)
	imported := imports[0]
	challenge, err := destination.mgr.ImportedPipeReceiptChallenge(
		ctx, imported.PipeID, fixture.targetAgent, "claimed",
	)
	require.NoError(t, err)
	claimAt := time.Now().UTC().Truncate(time.Second)
	claimProof := signedPipeProof(t, fixture.targetKey, fixture.targetAgent, http.MethodPut,
		"/v1/pipe/"+imported.PipeID+"/receipt/claimed", challenge, claimAt.Unix())
	replayed, err := destination.mgr.RecordImportedPipeReceipt(
		ctx, imported.PipeID, fixture.targetAgent, "claimed", claimProof,
	)
	require.NoError(t, err)
	require.False(t, replayed)
	due, err := pipeSQLite(t, destination).ListPendingFederatedReceiptOutbox(
		ctx, time.Now().UTC().Add(time.Second), 10,
	)
	require.NoError(t, err)
	require.Len(t, due, 1)

	// Simulate the precise lost-response window: the sender durably accepts the
	// receipt, but the recipient crashes before recording the HTTP success.
	accepted, err := destination.mgr.PushPipeReceiptV2(
		ctx, source.chainID, receiptWireEvent(due[0]),
	)
	require.NoError(t, err)
	require.Equal(t, "accepted", accepted.Status)
	pendingAfterLostResponse, err := pipeSQLite(t, destination).ListPendingFederatedReceiptOutbox(
		ctx, time.Now().UTC().Add(time.Second), 10,
	)
	require.NoError(t, err)
	require.Len(t, pendingAfterLostResponse, 1)
	require.Equal(t, due[0].EventID, pendingAfterLostResponse[0].EventID)

	// Restart both managers and listeners around the still-pending durable row.
	sourceServer.stop(t)
	destinationServer.stop(t)
	restartPipeManager(source)
	restartPipeManager(destination)
	restoreReceiptV2RuntimeAfterManagerRestart(t, source, destination)
	restoreReceiptV2RuntimeAfterManagerRestart(t, destination, source)
	sourceServer = startRestartablePipeServer(t, source, sourceServer.address)
	destinationServer = startRestartablePipeServer(t, destination, destinationServer.address)
	require.NoError(t, pipeSQLite(t, destination).RecordFederatedReceiptOutboxFailure(
		ctx, due[0].EventID, "lost response", time.Now().UTC().Add(-time.Second), "",
	))
	destination.mgr.receiptDrain(ctx, pipeSQLite(t, destination))
	pendingAfterRetry, err := pipeSQLite(t, destination).ListPendingFederatedReceiptOutbox(
		ctx, time.Now().UTC().Add(time.Second), 10,
	)
	require.NoError(t, err)
	require.Empty(t, pendingAfterRetry,
		"the retry after restart must accept the sender's durable duplicate and leave no pending work")
	projection, err = pipeSQLite(t, source).GetFederatedReceiptForSender(
		ctx, fixture.sourceAgent, sourceMessage.PipeID,
	)
	require.NoError(t, err)
	require.NotNil(t, projection.ClaimedAt)
	require.True(t, projection.ClaimedAt.Equal(claimAt))

	duplicate, err := destination.mgr.PushPipeReceiptV2(
		ctx, source.chainID, receiptWireEvent(due[0]),
	)
	require.NoError(t, err)
	require.Equal(t, "duplicate", duplicate.Status,
		"an exact retry after both restarts must resolve to the original durable evidence")
	unchanged, err := pipeSQLite(t, source).GetFederatedReceiptForSender(
		ctx, fixture.sourceAgent, sourceMessage.PipeID,
	)
	require.NoError(t, err)
	require.True(t, unchanged.ClaimedAt.Equal(*projection.ClaimedAt),
		"duplicate retries must not rewrite the first evidence timestamp")
}

func TestReceiptV2NegotiationFallsBackToLegacyV1(t *testing.T) {
	ctx := context.Background()
	source := newTestChain(t, "receipt-v1-source")
	destination := newTestChain(t, "receipt-v1-destination")
	// Only the sender is app-v26 capable. The destination must not advertise v2.
	enableReceiptV2Runtime(source)
	sourceServer := startRestartablePipeServer(t, source, "")
	destinationServer := startRestartablePipeServer(t, destination, "")
	defer func() { sourceServer.stop(t); destinationServer.stop(t) }()
	federate(t, source, destination, "https://"+destinationServer.address, nil, 4, 0)
	federate(t, destination, source, "https://"+sourceServer.address, nil, 4, 0)
	enableV23Pair(t, source, destination, []string{"fault-gate", "fault-unrelated"})
	// Destination deliberately remains pre-v26 after the shared v23 setup.
	destination.mgr.postV26ForNextTx = func() bool { return false }

	fixture := configureReceiptRuntimeFixture(t, source, destination)
	require.Zero(t, fixture.target.ReceiptProtocolVersion)
	sourceMessage, outbox := enqueueNegotiatedFaultSend(t, source, destination, fixture)
	require.Zero(t, outbox.ReceiptProtocolVersion)
	source.mgr.pipelineDrain(ctx, pipeSQLite(t, source))
	stored, err := pipeSQLite(t, source).GetPipelineTransport(ctx, outbox.EventID)
	require.NoError(t, err)
	require.Equal(t, "delivered", stored.State)
	_, err = pipeSQLite(t, source).GetFederatedReceiptForSender(
		ctx, fixture.sourceAgent, sourceMessage.PipeID,
	)
	require.ErrorIs(t, err, store.ErrFederatedReceiptNotFound)

	imports, err := pipeSQLite(t, destination).ListPipelines(ctx, "", 10)
	require.NoError(t, err)
	require.Len(t, imports, 1)
	imported := imports[0]
	_, err = destination.mgr.ImportedPipeReceiptChallenge(
		ctx, imported.PipeID, fixture.targetAgent, "claimed",
	)
	require.ErrorIs(t, err, store.ErrFederatedReceiptNotFound)
	require.NoError(t, destination.mgr.WithAuthorizedImportedPipe(ctx, imported, func() error {
		return pipeSQLite(t, destination).ClaimPipeline(ctx, imported.PipeID, fixture.targetAgent)
	}))
	claimed, err := pipeSQLite(t, destination).GetPipeline(ctx, imported.PipeID)
	require.NoError(t, err)
	require.Equal(t, "claimed", claimed.Status,
		"v1 peers must retain legacy workflow even though they cannot emit canonical receipts")
}
