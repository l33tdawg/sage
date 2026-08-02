package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/require"
)

func signedReceiptV2Fixture(t *testing.T) (*PipeReceiptEvent, ed25519.PrivateKey) {
	t.Helper()
	_, recipientKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	recipientID := auth.PublicKeyToAgentID(recipientKey.Public().(ed25519.PublicKey))
	event := &PipeReceiptEvent{
		Version: PipeReceiptVersion, MessageID: "pipe-event-origin",
		RecipientPipeID: "recipient-local-pipe", SenderChainID: "chain-sender",
		RecipientChainID: "chain-recipient", SenderAgentID: strings.Repeat("a", 64),
		RecipientAgentID: recipientID, ContentDigest: strings.Repeat("1", 64), Kind: "read",
		PolicyEpoch: "epoch-1", AgreementID: strings.Repeat("2", 64),
		ContactID: strings.Repeat("3", 64), ContactRevision: strings.Repeat("4", 64),
		AuthorizationMode: "linked-v23", RelationDigest: strings.Repeat("5", 64),
	}
	resignReceiptV2(t, event, recipientKey, nil)
	return event, recipientKey
}

func resignReceiptV2(t *testing.T, event *PipeReceiptEvent, key ed25519.PrivateKey, bodyOverride []byte) {
	resignReceiptV2At(t, event, key, bodyOverride, time.Now().UTC().Truncate(time.Second))
}

func resignReceiptV2At(
	t *testing.T, event *PipeReceiptEvent, key ed25519.PrivateKey, bodyOverride []byte, ts time.Time,
) {
	t.Helper()
	if bodyOverride == nil {
		var err error
		bodyOverride, err = json.Marshal(signedPipeReceiptRequest{
			Version: event.Version, MessageID: event.MessageID,
			SenderChainID: event.SenderChainID, RecipientChainID: event.RecipientChainID,
			SenderAgentID: event.SenderAgentID, RecipientAgentID: event.RecipientAgentID,
			ContentDigest: event.ContentDigest, EventKind: event.Kind, PolicyEpoch: event.PolicyEpoch,
			AgreementID: event.AgreementID, ContactID: event.ContactID,
			ContactRevision: event.ContactRevision, AuthorizationMode: event.AuthorizationMode,
			RelationDigest: event.RelationDigest,
		})
		require.NoError(t, err)
	}
	path := "/v1/pipe/" + event.RecipientPipeID + "/receipt/" + event.Kind
	nonce := []byte("receipt-v2-nonce")
	event.EventAt = ts
	event.Proof = store.PipelineAgentProof{
		AgentID:   event.RecipientAgentID,
		Signature: auth.SignRequestWithNonce(key, http.MethodPut, path, bodyOverride, ts.Unix(), nonce),
		Timestamp: ts.Unix(), Nonce: nonce,
		CanonicalRequest: append([]byte(http.MethodPut+" "+path+"\n"), bodyOverride...),
	}
	event.EventID = pipeReceiptEventID(event)
}

func TestReceiptV2HandlerAcceptsDelayedDurableEvidenceButRejectsBeyondMessageLifetime(t *testing.T) {
	ctx := context.Background()
	manager, sqlite, badger := newDrainTestManager(t)
	manager.postV26ForNextTx = func() bool { return true }
	peerOperator := newPeerOperatorID(t)
	agreement := configurePeerRBACConnection(t, manager, sqlite, badger,
		"chain-peer", peerOperator, "host", nil, 4)

	_, senderKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	senderID := auth.PublicKeyToAgentID(senderKey.Public().(ed25519.PublicKey))
	_, recipientKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	recipientID := auth.PublicKeyToAgentID(recipientKey.Public().(ed25519.PublicKey))
	now := time.Now().UTC().Truncate(time.Second)
	createdAt, expiresAt := now.Add(-48*time.Hour), now.Add(-25*time.Hour)
	message := &store.PipelineMessage{
		PipeID: "delayed-receipt-local-pipe", FromAgent: senderID, ToAgent: recipientID,
		DestinationChainID: "chain-peer", Intent: "request", Payload: "payload",
		FederationPolicyEpoch: "epoch-chain-peer", FederationAgreementID: strings.Repeat("2", 64),
		FederationContactID: strings.Repeat("3", 64), FederationContactRevision: strings.Repeat("4", 64),
		Status: "pending", CreatedAt: createdAt, ExpiresAt: expiresAt,
	}
	sourceProof := signedPipeProof(t, senderKey, senderID, http.MethodPost,
		"/v1/pipe", []byte(`{"to":"delayed"}`), createdAt.Unix())
	outbox := &store.PipelineTransportOutbox{
		EventID: "delayed-receipt-send-event", PipeID: message.PipeID, RemoteChainID: "chain-peer",
		EventKind: "send", PolicyEpoch: message.FederationPolicyEpoch,
		AgreementID: message.FederationAgreementID, ContactID: message.FederationContactID,
		ContactRevision: message.FederationContactRevision, SourceAgentID: senderID,
		TargetAgentID: recipientID, ReceiptProtocolVersion: PipeReceiptVersion,
		Proof: sourceProof, CreatedAt: createdAt, ExpiresAt: expiresAt,
	}
	require.NoError(t, sqlite.InsertPipelineWithTransport(ctx, message, outbox))
	manager.pipeTargetResolveFn = func(context.Context, string) (*RemotePipeTarget, error) {
		return &RemotePipeTarget{
			ChainID: outbox.RemoteChainID, AgentID: outbox.TargetAgentID,
			PolicyEpoch: outbox.PolicyEpoch, AgreementID: outbox.AgreementID,
			ContactID: outbox.ContactID, ContactRevision: outbox.ContactRevision,
			ReceiptProtocolVersion: PipeReceiptVersion,
		}, nil
	}
	receipt := &PipeReceiptEvent{
		Version: PipeReceiptVersion, MessageID: outbox.EventID, RecipientPipeID: "peer-local-pipe",
		SenderChainID: manager.localChainID, RecipientChainID: outbox.RemoteChainID,
		SenderAgentID: senderID, RecipientAgentID: recipientID,
		ContentDigest: pipeReceiptContentDigest(outbox.EventID, manager.localChainID, outbox.RemoteChainID, message),
		Kind:          "claimed", PolicyEpoch: outbox.PolicyEpoch, AgreementID: outbox.AgreementID,
		ContactID: outbox.ContactID, ContactRevision: outbox.ContactRevision,
	}
	// Two hours old is well outside request-auth skew but still inside the
	// original message's bounded result/receipt lifetime.
	resignReceiptV2At(t, receipt, recipientKey, nil, now.Add(-2*time.Hour))
	peer := &peerIdentity{ChainID: "chain-peer", AgentID: peerOperator, Agreement: agreement}
	accepted := callReceiptV2Handler(t, manager, peer, receipt)
	require.Equal(t, http.StatusOK, accepted.Code, accepted.Body.String())

	beyond := *receipt
	beyond.Kind = "read"
	// This remains in the past (so it is not a future-skew rejection) but falls
	// after expires_at + the bounded receipt/result lifetime.
	resignReceiptV2At(t, &beyond, recipientKey, nil, now.Add(-30*time.Minute))
	rejected := callReceiptV2Handler(t, manager, peer, &beyond)
	require.Equal(t, http.StatusConflict, rejected.Code, rejected.Body.String())
}

func TestReceiptV2ProofBindsEveryParticipantAndGenerationField(t *testing.T) {
	base, _ := signedReceiptV2Fixture(t)
	require.NoError(t, validatePipeReceiptProof(base))
	tests := []struct {
		name   string
		mutate func(*PipeReceiptEvent)
	}{
		{"message", func(e *PipeReceiptEvent) { e.MessageID += "-other" }},
		{"recipient pipe path", func(e *PipeReceiptEvent) { e.RecipientPipeID += "-other" }},
		{"sender chain", func(e *PipeReceiptEvent) { e.SenderChainID += "-other" }},
		{"recipient chain", func(e *PipeReceiptEvent) { e.RecipientChainID += "-other" }},
		{"sender", func(e *PipeReceiptEvent) { e.SenderAgentID = strings.Repeat("b", 64) }},
		{"recipient", func(e *PipeReceiptEvent) { e.RecipientAgentID = strings.Repeat("c", 64) }},
		{"content", func(e *PipeReceiptEvent) { e.ContentDigest = strings.Repeat("6", 64) }},
		{"kind", func(e *PipeReceiptEvent) { e.Kind = "claimed" }},
		{"policy epoch", func(e *PipeReceiptEvent) { e.PolicyEpoch += "-other" }},
		{"agreement", func(e *PipeReceiptEvent) { e.AgreementID = strings.Repeat("6", 64) }},
		{"contact", func(e *PipeReceiptEvent) { e.ContactID = strings.Repeat("6", 64) }},
		{"contact revision", func(e *PipeReceiptEvent) { e.ContactRevision = strings.Repeat("6", 64) }},
		{"authorization mode", func(e *PipeReceiptEvent) { e.AuthorizationMode = "" }},
		{"relation", func(e *PipeReceiptEvent) { e.RelationDigest = strings.Repeat("6", 64) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tampered := *base
			test.mutate(&tampered)
			// Remove event-ID mismatch as the reason for rejection so this checks
			// the exact signed body/path binding itself.
			tampered.EventID = pipeReceiptEventID(&tampered)
			require.ErrorIs(t, validatePipeReceiptProof(&tampered), store.ErrFederatedReceiptInvalid)
		})
	}
}

func TestReceiptV2ProofRejectsUnknownSignedFieldsAndFutureTimeButAllowsDurableRetry(t *testing.T) {
	event, key := signedReceiptV2Fixture(t)
	var body map[string]any
	require.NoError(t, json.Unmarshal(event.Proof.CanonicalRequest[strings.IndexByte(string(event.Proof.CanonicalRequest), '\n')+1:], &body))
	body["execute_tool"] = "sage_forget"
	unknown, err := json.Marshal(body)
	require.NoError(t, err)
	resignReceiptV2(t, event, key, unknown)
	require.ErrorIs(t, validatePipeReceiptProof(event), store.ErrFederatedReceiptInvalid)

	old, key := signedReceiptV2Fixture(t)
	oldTime := time.Now().UTC().Add(-2 * maxTimestampSkew).Truncate(time.Second)
	bodyBytes := old.Proof.CanonicalRequest[strings.IndexByte(string(old.Proof.CanonicalRequest), '\n')+1:]
	path := "/v1/pipe/" + old.RecipientPipeID + "/receipt/" + old.Kind
	nonce := []byte("receipt-v2-old-nonce")
	old.EventAt = oldTime
	old.Proof = store.PipelineAgentProof{
		AgentID:   old.RecipientAgentID,
		Signature: auth.SignRequestWithNonce(key, http.MethodPut, path, bodyBytes, oldTime.Unix(), nonce),
		Timestamp: oldTime.Unix(), Nonce: nonce,
		CanonicalRequest: append([]byte(http.MethodPut+" "+path+"\n"), bodyBytes...),
	}
	old.EventID = pipeReceiptEventID(old)
	require.NoError(t, validatePipeReceiptProof(old),
		"a durable exact receipt must remain deliverable after an outage longer than the request-auth skew")

	future, key := signedReceiptV2Fixture(t)
	futureTime := time.Now().UTC().Add(2 * maxTimestampSkew).Truncate(time.Second)
	bodyBytes = future.Proof.CanonicalRequest[strings.IndexByte(string(future.Proof.CanonicalRequest), '\n')+1:]
	path = "/v1/pipe/" + future.RecipientPipeID + "/receipt/" + future.Kind
	nonce = []byte("receipt-v2-future-nonce")
	future.EventAt = futureTime
	future.Proof = store.PipelineAgentProof{
		AgentID:   future.RecipientAgentID,
		Signature: auth.SignRequestWithNonce(key, http.MethodPut, path, bodyBytes, futureTime.Unix(), nonce),
		Timestamp: futureTime.Unix(), Nonce: nonce,
		CanonicalRequest: append([]byte(http.MethodPut+" "+path+"\n"), bodyBytes...),
	}
	future.EventID = pipeReceiptEventID(future)
	require.ErrorIs(t, validatePipeReceiptProof(future), store.ErrFederatedReceiptInvalid)
}

func callReceiptV2Handler(t *testing.T, manager *Manager, peer *peerIdentity, event *PipeReceiptEvent) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(event)
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPost, "/fed/v2/pipe/receipt", bytes.NewReader(body))
	request = request.WithContext(context.WithValue(request.Context(), peerCtxKey{}, peer))
	recorder := httptest.NewRecorder()
	manager.handlePipeReceiptV2(recorder, request)
	return recorder
}

// A valid v2 proof is still not admissible for a send created before the two
// peers negotiated receipt-v2. Otherwise a peer can rewrite historical
// "unsupported" rows into confirmed receipts after a unilateral upgrade.
func TestReceiptV2HandlerRejectsUnnegotiatedLegacySend(t *testing.T) {
	ctx := context.Background()
	manager, sqlite, badger := newDrainTestManager(t)
	manager.postV26ForNextTx = func() bool { return true }
	peerOperator := newPeerOperatorID(t)
	agreement := configurePeerRBACConnection(t, manager, sqlite, badger,
		"chain-peer", peerOperator, "host", nil, 4)

	_, senderKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	senderID := auth.PublicKeyToAgentID(senderKey.Public().(ed25519.PublicKey))
	_, recipientKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	recipientID := auth.PublicKeyToAgentID(recipientKey.Public().(ed25519.PublicKey))
	now := time.Now().UTC().Truncate(time.Second)
	message := &store.PipelineMessage{
		PipeID: "legacy-local-pipe", FromAgent: senderID, ToAgent: recipientID,
		DestinationChainID: "chain-peer", Intent: "request", Payload: "payload",
		FederationPolicyEpoch: "epoch-chain-peer", FederationAgreementID: strings.Repeat("2", 64),
		FederationContactID: strings.Repeat("3", 64), FederationContactRevision: strings.Repeat("4", 64),
		Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	sourceProof := signedPipeProof(t, senderKey, senderID, http.MethodPost,
		"/v1/pipe", []byte(`{"to":"legacy"}`), now.Unix())
	outbox := &store.PipelineTransportOutbox{
		EventID: "legacy-send-event", PipeID: message.PipeID, RemoteChainID: "chain-peer",
		EventKind: "send", PolicyEpoch: "epoch-chain-peer",
		AgreementID: strings.Repeat("2", 64), ContactID: strings.Repeat("3", 64),
		ContactRevision: strings.Repeat("4", 64), SourceAgentID: senderID,
		TargetAgentID: recipientID, Proof: sourceProof, CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	require.NoError(t, sqlite.InsertPipelineWithTransport(ctx, message, outbox))
	manager.pipeTargetResolveFn = func(context.Context, string) (*RemotePipeTarget, error) {
		return &RemotePipeTarget{
			ChainID: outbox.RemoteChainID, AgentID: outbox.TargetAgentID,
			PolicyEpoch: outbox.PolicyEpoch, AgreementID: outbox.AgreementID,
			ContactID: outbox.ContactID, ContactRevision: outbox.ContactRevision,
		}, nil
	}
	receipt := &PipeReceiptEvent{
		Version: PipeReceiptVersion, MessageID: outbox.EventID, RecipientPipeID: "peer-local-pipe",
		SenderChainID: manager.localChainID, RecipientChainID: outbox.RemoteChainID,
		SenderAgentID: senderID, RecipientAgentID: recipientID,
		ContentDigest: pipeReceiptContentDigest(outbox.EventID, manager.localChainID, outbox.RemoteChainID, message),
		Kind:          "read", PolicyEpoch: outbox.PolicyEpoch, AgreementID: outbox.AgreementID,
		ContactID: outbox.ContactID, ContactRevision: outbox.ContactRevision,
	}
	resignReceiptV2(t, receipt, recipientKey, nil)
	peer := &peerIdentity{ChainID: "chain-peer", AgentID: peerOperator, Agreement: agreement}
	recorder := callReceiptV2Handler(t, manager, peer, receipt)
	require.Equal(t, http.StatusConflict, recorder.Code)
	require.NotContains(t, recorder.Body.String(), recipientID,
		"negotiation refusal must not become a recipient-existence oracle")
}

func TestReceiptV2HandlerRejectsOversizeUnknownAndWrongPeerBindings(t *testing.T) {
	manager, _, _ := newDrainTestManager(t)
	manager.postV26ForNextTx = func() bool { return true }
	peer := &peerIdentity{
		ChainID: "chain-peer", AgentID: strings.Repeat("f", 64),
		Agreement: &store.CrossFedRecord{RemoteChainID: "chain-peer", Status: "active"},
	}

	oversize := httptest.NewRequest(http.MethodPost, "/fed/v2/pipe/receipt",
		bytes.NewReader(bytes.Repeat([]byte{'x'}, maxPipeReceiptV2BodyBytes+1)))
	oversize = oversize.WithContext(context.WithValue(oversize.Context(), peerCtxKey{}, peer))
	oversizeRecorder := httptest.NewRecorder()
	manager.handlePipeReceiptV2(oversizeRecorder, oversize)
	require.Equal(t, http.StatusBadRequest, oversizeRecorder.Code)

	event, key := signedReceiptV2Fixture(t)
	event.SenderChainID = manager.localChainID
	event.RecipientChainID = peer.ChainID
	var signed map[string]any
	bodyOffset := bytes.IndexByte(event.Proof.CanonicalRequest, '\n') + 1
	require.NoError(t, json.Unmarshal(event.Proof.CanonicalRequest[bodyOffset:], &signed))
	signed["sender_chain_id"] = event.SenderChainID
	signed["recipient_chain_id"] = event.RecipientChainID
	signed["notification_action"] = "execute"
	unknown, err := json.Marshal(signed)
	require.NoError(t, err)
	resignReceiptV2(t, event, key, unknown)
	unknownRecorder := callReceiptV2Handler(t, manager, peer, event)
	require.Equal(t, http.StatusBadRequest, unknownRecorder.Code)

	wrongPeer, key := signedReceiptV2Fixture(t)
	wrongPeer.SenderChainID = manager.localChainID
	wrongPeer.RecipientChainID = "chain-other"
	resignReceiptV2(t, wrongPeer, key, nil)
	wrongPeerRecorder := callReceiptV2Handler(t, manager, peer, wrongPeer)
	require.Equal(t, http.StatusBadRequest, wrongPeerRecorder.Code)
}

func TestReceiptV2AcceptedReplaySurvivesRevocationButNewTransitionDoesNot(t *testing.T) {
	ctx := context.Background()
	manager, sqlite, badger := newDrainTestManager(t)
	manager.postV26ForNextTx = func() bool { return true }
	peerOperator := newPeerOperatorID(t)
	agreement := configurePeerRBACConnection(t, manager, sqlite, badger,
		"chain-peer", peerOperator, "host", nil, 4)

	_, senderKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	senderID := auth.PublicKeyToAgentID(senderKey.Public().(ed25519.PublicKey))
	_, recipientKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	recipientID := auth.PublicKeyToAgentID(recipientKey.Public().(ed25519.PublicKey))
	now := time.Now().UTC().Truncate(time.Second)
	message := &store.PipelineMessage{
		PipeID: "negotiated-local-pipe", FromAgent: senderID, ToAgent: recipientID,
		DestinationChainID: "chain-peer", Intent: "request", Payload: "payload",
		FederationPolicyEpoch: "epoch-chain-peer", FederationAgreementID: strings.Repeat("2", 64),
		FederationContactID: strings.Repeat("3", 64), FederationContactRevision: strings.Repeat("4", 64),
		Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	sourceProof := signedPipeProof(t, senderKey, senderID, http.MethodPost,
		"/v1/pipe", []byte(`{"to":"negotiated"}`), now.Unix())
	outbox := &store.PipelineTransportOutbox{
		EventID: "negotiated-send-event", PipeID: message.PipeID, RemoteChainID: "chain-peer",
		EventKind: "send", PolicyEpoch: message.FederationPolicyEpoch,
		AgreementID: message.FederationAgreementID, ContactID: message.FederationContactID,
		ContactRevision: message.FederationContactRevision, SourceAgentID: senderID,
		TargetAgentID: recipientID, ReceiptProtocolVersion: PipeReceiptVersion,
		Proof: sourceProof, CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	require.NoError(t, sqlite.InsertPipelineWithTransport(ctx, message, outbox))
	manager.pipeTargetResolveFn = func(context.Context, string) (*RemotePipeTarget, error) {
		return &RemotePipeTarget{
			ChainID: outbox.RemoteChainID, AgentID: outbox.TargetAgentID,
			PolicyEpoch: outbox.PolicyEpoch, AgreementID: outbox.AgreementID,
			ContactID: outbox.ContactID, ContactRevision: outbox.ContactRevision,
			ReceiptProtocolVersion: PipeReceiptVersion,
		}, nil
	}
	receipt := &PipeReceiptEvent{
		Version: PipeReceiptVersion, MessageID: outbox.EventID, RecipientPipeID: "peer-local-pipe",
		SenderChainID: manager.localChainID, RecipientChainID: outbox.RemoteChainID,
		SenderAgentID: senderID, RecipientAgentID: recipientID,
		ContentDigest: pipeReceiptContentDigest(outbox.EventID, manager.localChainID, outbox.RemoteChainID, message),
		Kind:          "claimed", PolicyEpoch: outbox.PolicyEpoch, AgreementID: outbox.AgreementID,
		ContactID: outbox.ContactID, ContactRevision: outbox.ContactRevision,
	}
	resignReceiptV2(t, receipt, recipientKey, nil)
	peer := &peerIdentity{ChainID: "chain-peer", AgentID: peerOperator, Agreement: agreement}
	accepted := callReceiptV2Handler(t, manager, peer, receipt)
	require.Equal(t, http.StatusOK, accepted.Code)
	require.Contains(t, accepted.Body.String(), `"accepted"`)

	manager.pipeTargetResolveFn = func(context.Context, string) (*RemotePipeTarget, error) {
		return nil, errors.New("relation revoked")
	}
	duplicate := callReceiptV2Handler(t, manager, peer, receipt)
	require.Equal(t, http.StatusOK, duplicate.Code,
		"an already durable historical fact remains idempotently replayable")
	require.Contains(t, duplicate.Body.String(), `"duplicate"`)

	read := *receipt
	read.Kind = "read"
	resignReceiptV2(t, &read, recipientKey, nil)
	rejected := callReceiptV2Handler(t, manager, peer, &read)
	require.Equal(t, http.StatusConflict, rejected.Code)
	require.Contains(t, rejected.Body.String(), "no longer active")
	projection, err := sqlite.GetFederatedReceiptForSender(ctx, senderID, message.PipeID)
	require.NoError(t, err)
	require.NotNil(t, projection.ClaimedAt)
	require.Nil(t, projection.ReadAt, "revocation must prevent a new old-generation transition")
}
