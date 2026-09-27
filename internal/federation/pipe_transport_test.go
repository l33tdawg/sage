package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
)

func TestImportedMessageIDsUseCanonicalVocabulary(t *testing.T) {
	id, err := newImportedPipeID()
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(id, "msg-fed-"), id)
}

func signedPipeProof(t *testing.T, priv ed25519.PrivateKey, agentID, method, path string, body []byte, ts int64) store.PipelineAgentProof {
	t.Helper()
	nonce := []byte("pipe-nonce-12345")
	canonical := append([]byte(method+" "+path+"\n"), body...)
	return store.PipelineAgentProof{
		AgentID: agentID, Signature: auth.SignRequestWithNonce(priv, method, path, body, ts, nonce),
		Timestamp: ts, Nonce: nonce, CanonicalRequest: canonical,
	}
}

func callPipeEvent(t *testing.T, m *Manager, agreement *store.CrossFedRecord, peerAgentID string, event *PipeEvent) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(event)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/fed/v1/pipe/event", bytes.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{
		ChainID: event.SourceChainID, AgentID: peerAgentID, Agreement: agreement,
	}))
	rr := httptest.NewRecorder()
	m.handlePipeEvent(rr, req)
	return rr
}

func TestHandlePipeEventSendVerifiesProofLifetimeContactAndDeduplicates(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	ensurePipeContactAppV23(t, m, bs)
	type wake struct {
		target       string
		notification AgentMessageNotification
	}
	var wakes []wake
	m.messageNotifier = func(target string, notification AgentMessageNotification) {
		wakes = append(wakes, wake{target: target, notification: notification})
	}
	var durableWakes []uint64
	m.SetMessageWakeNotifier(func(target string, seq uint64) {
		state, err := ss.GetMessageWakeState(ctx, target)
		require.NoError(t, err)
		require.Equal(t, store.MessageWakeState{Seq: seq, Pending: true}, state, "callback runs after durable commit")
		durableWakes = append(durableWakes, seq)
	})
	peerOperator := newPeerOperatorID(t)
	agreement := configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerOperator, "host", nil, 4)
	owner := newPeerOperatorID(t)
	unrelatedOwner := newPeerOperatorID(t)
	seedPipeContactOrdinaryAgent(t, m, ss, bs, owner, "sentinel", "active", 0, 10)
	seedPipeContactOrdinaryAgent(t, m, ss, bs, unrelatedOwner, "unrelated", "active", 0, 20)
	require.NoError(t, bs.RegisterDomain("security", owner, "", 10))
	require.NoError(t, bs.RegisterDomain("unrelated", unrelatedOwner, "", 11))
	_, err := m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{
		{Domain: "security.alerts", Read: true}, {Domain: "unrelated.work", Read: true},
	})
	require.NoError(t, err)
	exportPipeContactAgent(t, m, "chain-peer", owner)
	exportPipeContactAgent(t, m, "chain-peer", unrelatedOwner)
	grant, err := m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	require.Len(t, grant.Contacts, 2)
	contactFor := func(agentID string) PipeContact {
		for _, candidate := range grant.Contacts {
			if candidate.AgentID == agentID {
				return candidate
			}
		}
		t.Fatalf("contact %s not found", agentID)
		return PipeContact{}
	}
	contact := contactFor(owner)
	require.True(t, contact.Accepting)

	sourcePub, sourcePriv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	sourceAgent := hex.EncodeToString(sourcePub)
	ts := time.Now().UTC().Truncate(time.Second)
	signedBody, err := json.Marshal(map[string]any{
		"to_agent": contact.AgentID, "source_chain_id": "chain-peer", "destination_chain_id": "chain-local",
		"intent": "triage", "payload": "review finding", "ttl_minutes": 90,
		"idempotency_key": "canonical-message-retry-token",
	})
	require.NoError(t, err)
	proof := signedPipeProof(t, sourcePriv, sourceAgent, http.MethodPost, "/v1/pipe/send", signedBody, ts.Unix())
	event := &PipeEvent{
		Version: PipeEventVersion, Kind: "send", SourceChainID: "chain-peer", DestinationChainID: "chain-local",
		SourceAgentID: sourceAgent, TargetAgentID: owner, Intent: "triage", Payload: "review finding",
		CreatedAt: ts, ExpiresAt: ts.Add(90 * time.Minute), PolicyEpoch: "epoch-chain-peer",
		AgreementID: grant.AgreementID, ContactID: contact.ContactID,
		ContactRevision: pipeContactAuthorizationRevision(grant, &contact), Proof: proof,
	}
	event.EventID = PipelineProofEventID(event.SourceChainID, event.Kind, proof)

	rr := callPipeEvent(t, m, agreement, peerOperator, event)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var accepted PipeEventResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&accepted))
	require.Equal(t, "accepted", accepted.Status)
	pipes, err := ss.ListPipelines(ctx, "", 10)
	require.NoError(t, err)
	require.Len(t, pipes, 1)
	msg, err := ss.GetPipeline(ctx, pipes[0].PipeID)
	require.NoError(t, err)
	require.Equal(t, event.EventID, msg.SourcePipeID)
	require.Equal(t, sourceAgent, msg.FromAgent)
	require.Equal(t, owner, msg.ToAgent)
	require.Equal(t, event.Payload, msg.Payload)
	require.Len(t, wakes, 1)
	require.Equal(t, owner, wakes[0].target)
	require.Equal(t, msg.PipeID, wakes[0].notification.MessageID)
	require.Equal(t, sourceAgent, wakes[0].notification.FromAgent)
	require.Equal(t, event.CreatedAt, wakes[0].notification.CreatedAt)

	rr = callPipeEvent(t, m, agreement, peerOperator, event)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var duplicate PipeEventResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&duplicate))
	require.Equal(t, "duplicate", duplicate.Status)
	require.Len(t, wakes, 1, "a duplicate federated admission must not wake the recipient again")
	require.Equal(t, []uint64{1}, durableWakes)
	require.NoError(t, ss.UpdateAgentStatus(ctx, unrelatedOwner, "inactive"))
	rr = callPipeEvent(t, m, agreement, peerOperator, event)
	require.Equal(t, http.StatusOK, rr.Code, "unrelated availability must not invalidate exact-target work: %s", rr.Body.String())
	require.NoError(t, ss.UpdateAgentStatus(ctx, unrelatedOwner, "active"))
	_, err = m.SetPeerRBACPaused(ctx, "chain-peer", true)
	require.NoError(t, err)
	rr = callPipeEvent(t, m, agreement, peerOperator, event)
	require.Equal(t, http.StatusLocked, rr.Code, "temporary pause must remain retryable: %s", rr.Body.String())
	_, err = m.SetPeerRBACPaused(ctx, "chain-peer", false)
	require.NoError(t, err)
	rr = callPipeEvent(t, m, agreement, peerOperator, event)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&duplicate))
	require.Equal(t, "duplicate", duplicate.Status, "resume must accept the unchanged retry exactly once")
	forged := *event
	forged.Proof = event.Proof
	forged.Proof.Signature = append([]byte(nil), event.Proof.Signature...)
	forged.Proof.Signature[0] ^= 0xff
	forged.EventID = PipelineProofEventID(forged.SourceChainID, forged.Kind, forged.Proof)
	rr = callPipeEvent(t, m, agreement, peerOperator, &forged)
	require.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())

	caseReplay := *event
	caseReplay.SourceAgentID = strings.ToUpper(sourceAgent)
	caseReplay.Proof = event.Proof
	caseReplay.Proof.AgentID = caseReplay.SourceAgentID
	caseReplay.EventID = PipelineProofEventID(caseReplay.SourceChainID, caseReplay.Kind, caseReplay.Proof)
	rr = callPipeEvent(t, m, agreement, peerOperator, &caseReplay)
	require.Equal(t, http.StatusBadRequest, rr.Code, "agent-id case must not create a second replay identity: %s", rr.Body.String())

	renewed := *event
	renewed.ExpiresAt = renewed.ExpiresAt.Add(time.Hour)
	rr = callPipeEvent(t, m, agreement, peerOperator, &renewed)
	require.Equal(t, http.StatusBadRequest, rr.Code, "a node must not renew an agent-signed TTL: %s", rr.Body.String())

	aliasBody, err := json.Marshal(map[string]any{
		"to_provider": contact.Handle, "intent": "triage", "payload": "review finding", "ttl_minutes": 90,
	})
	require.NoError(t, err)
	aliasProof := signedPipeProof(t, sourcePriv, sourceAgent, http.MethodPost, "/v1/pipe/send", aliasBody, ts.Unix())
	aliasEvent := *event
	aliasEvent.Proof = aliasProof
	aliasEvent.EventID = PipelineProofEventID(aliasEvent.SourceChainID, aliasEvent.Kind, aliasProof)
	rr = callPipeEvent(t, m, agreement, peerOperator, &aliasEvent)
	require.Equal(t, http.StatusBadRequest, rr.Code, "a signed friendly label must not be reroutable by its source node: %s", rr.Body.String())

	otherOperator := newPeerOperatorID(t)
	otherAgreement := configurePeerRBACConnection(t, m, ss, bs, "chain-other", otherOperator, "host", nil, 4)
	_, err = m.ReplacePeerRBACPolicy(ctx, "chain-other", []store.PeerRBACDomainPermission{{Domain: "security.alerts", Read: true}})
	require.NoError(t, err)
	exportPipeContactAgent(t, m, "chain-other", owner)
	otherGrant, err := m.LocalPipeContacts(ctx, "chain-other")
	require.NoError(t, err)
	relabeled := *event
	relabeled.SourceChainID = "chain-other"
	relabeled.PolicyEpoch = "epoch-chain-other"
	relabeled.AgreementID = otherGrant.AgreementID
	relabeled.ContactID = otherGrant.Contacts[0].ContactID
	relabeled.ContactRevision = pipeContactAuthorizationRevision(otherGrant, &otherGrant.Contacts[0])
	relabeled.EventID = PipelineProofEventID(relabeled.SourceChainID, relabeled.Kind, relabeled.Proof)
	rr = callPipeEvent(t, m, otherAgreement, otherOperator, &relabeled)
	require.Equal(t, http.StatusBadRequest, rr.Code, "an agent proof signed on one source chain must not be relabeled by another trusted node: %s", rr.Body.String())
}

func TestNotifyAdmittedMessageIsBestEffortAndPanicSafe(t *testing.T) {
	m := &Manager{messageNotifier: func(string, AgentMessageNotification) {
		panic("closed wake transport")
	}}
	require.NotPanics(t, func() {
		m.notifyAdmittedMessage("exact-recipient", AgentMessageNotification{
			MessageID: "local-message", FromAgent: "remote-source", CreatedAt: time.Now().UTC(),
		})
	})
}

func TestHandlePipeEventSendRejectsStaleOwnerRevision(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	ensurePipeContactAppV23(t, m, bs)
	peerOperator := newPeerOperatorID(t)
	agreement := configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerOperator, "host", nil, 4)
	ownerA, ownerB := newPeerOperatorID(t), newPeerOperatorID(t)
	seedPipeContactOrdinaryAgent(t, m, ss, bs, ownerA, "owner-a", "active", 0, 10)
	seedPipeContactOrdinaryAgent(t, m, ss, bs, ownerB, "owner-b", "active", 0, 20)
	require.NoError(t, bs.RegisterDomain("research", ownerA, "", 10))
	_, err := m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{{Domain: "research.work", Read: true}})
	require.NoError(t, err)
	exportPipeContactAgent(t, m, "chain-peer", ownerA)
	grant, err := m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	oldContact := grant.Contacts[0]

	sourcePub, sourcePriv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	sourceAgent := hex.EncodeToString(sourcePub)
	ts := time.Now().UTC().Truncate(time.Second)
	body, _ := json.Marshal(map[string]any{
		"to_agent": ownerA, "source_chain_id": "chain-peer", "destination_chain_id": "chain-local", "payload": "work", "ttl_minutes": 60,
	})
	proof := signedPipeProof(t, sourcePriv, sourceAgent, http.MethodPost, "/v1/pipe/send", body, ts.Unix())
	event := &PipeEvent{
		Version: PipeEventVersion, Kind: "send", SourceChainID: "chain-peer", DestinationChainID: "chain-local",
		SourceAgentID: sourceAgent, TargetAgentID: ownerA, Payload: "work", CreatedAt: ts, ExpiresAt: ts.Add(time.Hour),
		PolicyEpoch: "epoch-chain-peer", AgreementID: grant.AgreementID, ContactID: oldContact.ContactID,
		ContactRevision: pipeContactAuthorizationRevision(grant, &oldContact), Proof: proof,
	}
	event.EventID = PipelineProofEventID(event.SourceChainID, event.Kind, proof)
	require.NoError(t, bs.TransferDomain("research", ownerB, "", 11))
	rr := callPipeEvent(t, m, agreement, peerOperator, event)
	require.Equal(t, http.StatusConflict, rr.Code, rr.Body.String())
	pipes, err := ss.ListPipelines(ctx, "", 10)
	require.NoError(t, err)
	require.Empty(t, pipes)
}

func TestHandlePipeEventResultAppliesOnlyToBoundOriginAndDeduplicates(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	peerOperator := newPeerOperatorID(t)
	agreement := configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerOperator, "host", nil, 4)

	localPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	localSender := hex.EncodeToString(localPub)
	remotePub, remotePriv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	remoteAgent := hex.EncodeToString(remotePub)
	now := time.Now().UTC().Truncate(time.Second)
	dummyProof := store.PipelineAgentProof{
		AgentID: localSender, Signature: make([]byte, ed25519.SignatureSize), Timestamp: now.Unix(),
		Nonce: []byte("12345678"), CanonicalRequest: []byte("POST /v1/pipe/send\n{}"),
	}
	originEventID := PipelineProofEventID("chain-local", "send", dummyProof)
	msg := &store.PipelineMessage{
		PipeID: "pipe-origin-local", FromAgent: localSender, ToAgent: remoteAgent,
		DestinationChainID: "chain-peer", FederationPolicyEpoch: "epoch-chain-peer",
		FederationAgreementID: strings.Repeat("a", 64), FederationContactID: strings.Repeat("b", 64),
		FederationContactRevision: strings.Repeat("c", 64), Intent: "review", Payload: "work",
		Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	outbox := &store.PipelineTransportOutbox{
		EventID: originEventID, PipeID: msg.PipeID, RemoteChainID: "chain-peer", EventKind: "send",
		PolicyEpoch: msg.FederationPolicyEpoch, AgreementID: msg.FederationAgreementID,
		ContactID: msg.FederationContactID, ContactRevision: msg.FederationContactRevision,
		SourceAgentID: localSender, TargetAgentID: remoteAgent, Proof: dummyProof,
		CreatedAt: now, ExpiresAt: msg.ExpiresAt,
	}
	require.NoError(t, ss.InsertPipelineWithTransport(ctx, msg, outbox))

	resultBody, _ := json.Marshal(map[string]any{"result": "done safely", "source_pipe_id": originEventID, "source_chain_id": "chain-peer", "claimant_session_id": "mcp-replying-session"})
	resultProof := signedPipeProof(t, remotePriv, remoteAgent, http.MethodPut, "/v1/pipe/pipe-remote-import/result", resultBody, now.Add(time.Minute).Unix())
	event := &PipeEvent{
		Version: PipeEventVersion, Kind: "result", OriginEventID: originEventID, SourcePipeID: "pipe-remote-import",
		SourceChainID: "chain-peer", DestinationChainID: "chain-local", SourceAgentID: remoteAgent,
		TargetAgentID: localSender, Result: "done safely", CreatedAt: now.Add(time.Minute),
		ExpiresAt: now.Add(time.Minute).Add(PipeEventResultLifetime), PolicyEpoch: msg.FederationPolicyEpoch,
		AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
		ContactRevision: msg.FederationContactRevision, Proof: resultProof,
	}
	event.EventID = PipelineProofEventID(event.SourceChainID, event.Kind, resultProof)
	rr := callPipeEvent(t, m, agreement, peerOperator, event)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	completed, err := ss.GetPipeline(ctx, msg.PipeID)
	require.NoError(t, err)
	require.Equal(t, "completed", completed.Status)
	require.Equal(t, "done safely", completed.Result)
	require.Empty(t, completed.JournalID)

	rr = callPipeEvent(t, m, agreement, peerOperator, event)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var replay PipeEventResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&replay))
	require.Equal(t, "duplicate", replay.Status)

	wrongSourceBody, _ := json.Marshal(map[string]any{"result": "done safely", "source_pipe_id": originEventID, "source_chain_id": "chain-other"})
	wrongSourceProof := signedPipeProof(t, remotePriv, remoteAgent, http.MethodPut, "/v1/pipe/pipe-remote-import/result", wrongSourceBody, now.Add(time.Minute).Unix())
	wrongSource := *event
	wrongSource.Proof = wrongSourceProof
	wrongSource.EventID = PipelineProofEventID(wrongSource.SourceChainID, wrongSource.Kind, wrongSourceProof)
	rr = callPipeEvent(t, m, agreement, peerOperator, &wrongSource)
	require.Equal(t, http.StatusBadRequest, rr.Code, "a result proof must bind its exact source chain: %s", rr.Body.String())

	// The lifetime is part of the signed binding, not a local retention choice.
	// Re-deriving it as created+PipeEventResultLifetime is the ONLY form the
	// destination admits, so a result row whose expires_at was re-stamped by a
	// transport-retention migration (this is how a receiver-local msg-fed-… row
	// became 'msg-%'-matched) is rejected before admission exactly as the peer's
	// "400 invalid pipeline agent proof" reported.
	retentionExtended := *event
	retentionExtended.ExpiresAt = event.CreatedAt.Add(store.CanonicalMessageLifetime)
	rr = callPipeEvent(t, m, agreement, peerOperator, &retentionExtended)
	require.Equal(t, http.StatusBadRequest, rr.Code,
		"only the proof-derived lifetime may be admitted: %s", rr.Body.String())
	require.Contains(t, rr.Body.String(), "invalid pipeline agent proof")

	// The legacy 24-hour window of v11.19.x stays admissible so a peer that has
	// not adopted the current one can still return the result of work this node
	// sent. Checked at the admission gate itself: re-applying the same proof
	// through the handler would be a replay conflict for reasons that have nothing
	// to do with the window.
	legacyWindow := *event
	legacyWindow.ExpiresAt = event.CreatedAt.Add(24 * time.Hour)
	require.NoError(t, prevalidatePipeEventAgentProof(&legacyWindow),
		"a legacy reply window must still pass destination prevalidation")
	require.Error(t, prevalidatePipeEventAgentProof(&retentionExtended),
		"a retention-extended reply window must not pass destination prevalidation")

	wrongOrigin := *event
	wrongOrigin.OriginEventID = "pipe-event-" + hex.EncodeToString(sha256.New().Sum(nil))
	rr = callPipeEvent(t, m, agreement, peerOperator, &wrongOrigin)
	require.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
}

func TestPipelineOutboxRevalidatesAndDeliversExactContact(t *testing.T) {
	ctx := context.Background()
	m, ss, _ := newDrainTestManager(t)
	sourcePub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	sourceAgent := hex.EncodeToString(sourcePub)
	targetAgent := newPeerOperatorID(t)
	require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{AgentID: sourceAgent, Name: "sender", Status: "active"}))
	now := time.Now().UTC().Truncate(time.Second)
	proof := store.PipelineAgentProof{
		AgentID: sourceAgent, Signature: make([]byte, ed25519.SignatureSize), Timestamp: now.Unix(),
		Nonce: []byte("12345678"), CanonicalRequest: []byte("POST /v1/pipe/send\n{}"),
	}
	msg := &store.PipelineMessage{
		PipeID: "pipe-outbox", FromAgent: sourceAgent, ToAgent: targetAgent, DestinationChainID: "chain-peer",
		FederationPolicyEpoch: "epoch-1", FederationAgreementID: strings.Repeat("a", 64),
		FederationContactID: strings.Repeat("b", 64), FederationContactRevision: strings.Repeat("c", 64),
		Intent: "review", Payload: "work", Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	outbox := &store.PipelineTransportOutbox{
		EventID: PipelineProofEventID("chain-local", "send", proof), PipeID: msg.PipeID,
		RemoteChainID: msg.DestinationChainID, EventKind: "send", PolicyEpoch: msg.FederationPolicyEpoch,
		AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
		ContactRevision: msg.FederationContactRevision, SourceAgentID: sourceAgent,
		TargetAgentID: targetAgent, Proof: proof, CreatedAt: now, ExpiresAt: msg.ExpiresAt,
	}
	require.NoError(t, ss.InsertPipelineWithTransport(ctx, msg, outbox))
	m.pipeTargetResolveFn = func(context.Context, string) (*RemotePipeTarget, error) {
		return &RemotePipeTarget{
			ChainID: "chain-peer", AgentID: targetAgent, PolicyEpoch: msg.FederationPolicyEpoch,
			AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
			ContactRevision: msg.FederationContactRevision,
		}, nil
	}
	var delivered *PipeEvent
	m.pipeEventPushFn = func(_ context.Context, chainID string, event *PipeEvent) (*PipeEventResponse, error) {
		require.Equal(t, "chain-peer", chainID)
		delivered = event
		return &PipeEventResponse{Status: "accepted"}, nil
	}
	m.pipelineDrain(ctx, ss)
	stored, err := ss.GetPipelineTransport(ctx, outbox.EventID)
	require.NoError(t, err)
	require.NotNil(t, delivered, "state=%s error=%s", stored.State, stored.LastError)
	require.Equal(t, msg.Payload, delivered.Payload)
	require.Equal(t, msg.Intent, delivered.Intent)
	require.Equal(t, outbox.EventID, delivered.EventID)
	require.Equal(t, "delivered", stored.State)
}

func TestPipelineOutboxRechecksSourceDenyBeforePayloadLeavesNode(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	m.postV22ForNextTx = func() bool { return true }
	sourceAgent := newPeerOperatorID(t)
	targetAgent := newPeerOperatorID(t)
	require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{
		AgentID: sourceAgent, Name: "sender", Status: "active",
	}))
	require.NoError(t, bs.RegisterAgent(sourceAgent, "sender", "member", "", "test", "", 1))

	now := time.Now().UTC().Truncate(time.Second)
	proof := store.PipelineAgentProof{
		AgentID: sourceAgent, Signature: make([]byte, ed25519.SignatureSize),
		Timestamp: now.Unix(), Nonce: []byte("12345678"),
		CanonicalRequest: []byte("POST /v1/pipe/send\n{}"),
	}
	msg := &store.PipelineMessage{
		PipeID: "pipe-outbox-denied-after-enqueue", FromAgent: sourceAgent, ToAgent: targetAgent,
		DestinationChainID: "chain-peer", FederationPolicyEpoch: "epoch-1",
		FederationAgreementID:     strings.Repeat("a", 64),
		FederationContactID:       strings.Repeat("b", 64),
		FederationContactRevision: strings.Repeat("c", 64),
		Intent:                    "review", Payload: "must stay local", Status: "pending",
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	outbox := &store.PipelineTransportOutbox{
		EventID: PipelineProofEventID("chain-local", "send", proof), PipeID: msg.PipeID,
		RemoteChainID: msg.DestinationChainID, EventKind: "send",
		PolicyEpoch: msg.FederationPolicyEpoch, AgreementID: msg.FederationAgreementID,
		ContactID: msg.FederationContactID, ContactRevision: msg.FederationContactRevision,
		SourceAgentID: sourceAgent, TargetAgentID: targetAgent, Proof: proof,
		CreatedAt: now, ExpiresAt: msg.ExpiresAt,
	}
	require.NoError(t, ss.InsertPipelineWithTransport(ctx, msg, outbox))
	require.NoError(t, bs.SetAgentPermissionWithCapabilities(
		sourceAgent, 1, "", "", "", "", store.AgentCapabilityDenyFederatedPipe,
	))

	resolveCalls := 0
	pushCalls := 0
	m.pipeTargetResolveFn = func(context.Context, string) (*RemotePipeTarget, error) {
		resolveCalls++
		return nil, errors.New("must not resolve")
	}
	m.pipeEventPushFn = func(context.Context, string, *PipeEvent) (*PipeEventResponse, error) {
		pushCalls++
		return nil, errors.New("must not push")
	}

	m.pipelineDrain(ctx, ss)

	require.Zero(t, resolveCalls)
	require.Zero(t, pushCalls)
	stored, err := ss.GetPipelineTransport(ctx, outbox.EventID)
	require.NoError(t, err)
	require.Equal(t, "pending", stored.State)
	require.Contains(t, stored.LastError, errFederatedPipeSourceDenied.Error())

	// Re-enable, begin one delivery, then revoke concurrently. The permission
	// transaction must not publish until the already-authorized network side
	// effect completes; after the setter returns, no old payload is in flight.
	require.NoError(t, bs.SetAgentPermissionWithCapabilities(sourceAgent, 1, "", "", "", "", 0))
	m.pipeTargetResolveFn = func(context.Context, string) (*RemotePipeTarget, error) {
		return &RemotePipeTarget{
			ChainID: "chain-peer", AgentID: targetAgent, PolicyEpoch: msg.FederationPolicyEpoch,
			AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
			ContactRevision: msg.FederationContactRevision,
		}, nil
	}
	pushStarted := make(chan struct{})
	releasePush := make(chan struct{})
	m.pipeEventPushFn = func(context.Context, string, *PipeEvent) (*PipeEventResponse, error) {
		close(pushStarted)
		<-releasePush
		return &PipeEventResponse{Status: "accepted"}, nil
	}
	delivered := make(chan struct{})
	go func() {
		m.deliverPipelineEvent(ctx, ss, outbox)
		close(delivered)
	}()
	<-pushStarted
	denyPublished := make(chan error, 1)
	go func() {
		denyPublished <- bs.SetAgentPermissionWithCapabilities(
			sourceAgent, 1, "", "", "", "", store.AgentCapabilityDenyFederatedPipe,
		)
	}()
	select {
	case err := <-denyPublished:
		t.Fatalf("deny published while an older payload was still in flight: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releasePush)
	<-delivered
	require.NoError(t, <-denyPublished)
}

func TestPipelineOutboxMalformedCapabilityMaskFailsClosed(t *testing.T) {
	m, _, bs := newDrainTestManager(t)
	m.postV22ForNextTx = func() bool { return true }

	sourceAgent := newPeerOperatorID(t)
	require.NoError(t, bs.RegisterAgent(sourceAgent, "sender", "member", "", "test", "", 1))
	agent, err := bs.GetRegisteredAgent(sourceAgent)
	require.NoError(t, err)
	agent.Capabilities = store.AgentCapabilities(1 << 31)
	rawAgent, err := json.Marshal(agent)
	require.NoError(t, err)
	require.NoError(t, bs.SetRawForTest([]byte("agent:"+sourceAgent), rawAgent))

	require.False(t, m.sourceMayUseFederatedPipe(sourceAgent),
		"unknown stored bits must never turn into an implicit federated-pipe allow")
}

func TestPipelineOutboxRetriesPeerSuspensionInsteadOfTerminalizing(t *testing.T) {
	ctx := context.Background()
	m, ss, _ := newDrainTestManager(t)
	sourceAgent := newPeerOperatorID(t)
	targetAgent := newPeerOperatorID(t)
	require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{AgentID: sourceAgent, Name: "sender", Status: "active"}))
	now := time.Now().UTC().Truncate(time.Second)
	proof := store.PipelineAgentProof{
		AgentID: sourceAgent, Signature: make([]byte, ed25519.SignatureSize), Timestamp: now.Unix(),
		Nonce: []byte("12345678"), CanonicalRequest: []byte("POST /v1/pipe/send\n{}"),
	}
	msg := &store.PipelineMessage{
		PipeID: "pipe-temporarily-suspended", FromAgent: sourceAgent, ToAgent: targetAgent, DestinationChainID: "chain-peer",
		FederationPolicyEpoch: "epoch-1", FederationAgreementID: strings.Repeat("a", 64),
		FederationContactID: strings.Repeat("b", 64), FederationContactRevision: strings.Repeat("c", 64),
		Payload: "retry after resume", Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	outbox := &store.PipelineTransportOutbox{
		EventID: PipelineProofEventID("chain-local", "send", proof), PipeID: msg.PipeID,
		RemoteChainID: msg.DestinationChainID, EventKind: "send", PolicyEpoch: msg.FederationPolicyEpoch,
		AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
		ContactRevision: msg.FederationContactRevision, SourceAgentID: sourceAgent,
		TargetAgentID: targetAgent, Proof: proof, CreatedAt: now, ExpiresAt: msg.ExpiresAt,
	}
	require.NoError(t, ss.InsertPipelineWithTransport(ctx, msg, outbox))
	m.pipeTargetResolveFn = func(context.Context, string) (*RemotePipeTarget, error) {
		return &RemotePipeTarget{
			ChainID: "chain-peer", AgentID: targetAgent, PolicyEpoch: msg.FederationPolicyEpoch,
			AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
			ContactRevision: msg.FederationContactRevision,
		}, nil
	}
	m.pipeEventPushFn = func(context.Context, string, *PipeEvent) (*PipeEventResponse, error) {
		return nil, &pipeEventHTTPError{Status: http.StatusLocked, Body: `{"error":"temporarily suspended"}`}
	}

	m.pipelineDrain(ctx, ss)
	stored, err := ss.GetPipelineTransport(ctx, outbox.EventID)
	require.NoError(t, err)
	require.Equal(t, "pending", stored.State)
	require.Equal(t, 1, stored.Attempts)
	require.Contains(t, stored.LastError, "423")
	queued, err := ss.GetPipeline(ctx, msg.PipeID)
	require.NoError(t, err)
	require.Equal(t, "pending", queued.Status)
	updates, err := ss.ListPipelineDeliveryUpdates(ctx, sourceAgent, 10)
	require.NoError(t, err)
	require.Empty(t, updates, "temporary suspension must not emit terminal feedback")
}

func TestPipelineHTTPFailureVerdictKeepsPeerBodyLimitRetryable(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		terminal   bool
		retryFloor time.Duration
	}{
		{name: "bad request stays permanent", status: http.StatusBadRequest, terminal: true},
		{name: "forbidden stays permanent", status: http.StatusForbidden, terminal: true},
		{name: "unprocessable stays permanent", status: http.StatusUnprocessableEntity, terminal: true},
		{name: "body limit retries on a floor", status: http.StatusRequestEntityTooLarge,
			terminal: false, retryFloor: time.Hour},
		{name: "not implemented retries on a floor", status: http.StatusNotImplemented,
			terminal: false, retryFloor: time.Hour},
		{name: "server error retries on backoff", status: http.StatusInternalServerError},
		{name: "gateway timeout retries on backoff", status: http.StatusGatewayTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			terminal, retryFloor := pipelineHTTPFailureVerdict(tc.status)
			require.Equal(t, tc.terminal, terminal)
			require.Equal(t, tc.retryFloor, retryFloor)
		})
	}
}

// The peer's per-route body cap moves when the peer upgrades, and a peer can emit
// 413 from a failed body read rather than a real overflow. Terminalizing it
// stranded msg-cc112a6a on 2026-09-15 while the same peer accepted a larger
// message unchanged; the event has to stay queued for the retry to happen.
func TestPipelineOutboxRetriesPeerBodyLimitInsteadOfTerminalizing(t *testing.T) {
	ctx := context.Background()
	m, ss, _ := newDrainTestManager(t)
	sourceAgent := newPeerOperatorID(t)
	targetAgent := newPeerOperatorID(t)
	require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{AgentID: sourceAgent, Name: "sender", Status: "active"}))
	now := time.Now().UTC().Truncate(time.Second)
	proof := store.PipelineAgentProof{
		AgentID: sourceAgent, Signature: make([]byte, ed25519.SignatureSize), Timestamp: now.Unix(),
		Nonce: []byte("12345678"), CanonicalRequest: []byte("POST /v1/pipe/send\n{}"),
	}
	msg := &store.PipelineMessage{
		PipeID: "pipe-body-limit", FromAgent: sourceAgent, ToAgent: targetAgent, DestinationChainID: "chain-peer",
		FederationPolicyEpoch: "epoch-1", FederationAgreementID: strings.Repeat("a", 64),
		FederationContactID: strings.Repeat("b", 64), FederationContactRevision: strings.Repeat("c", 64),
		Payload: "message the peer refused as too large", Status: "pending", CreatedAt: now,
		ExpiresAt: now.Add(7 * 24 * time.Hour),
	}
	outbox := &store.PipelineTransportOutbox{
		EventID: PipelineProofEventID("chain-local", "send", proof), PipeID: msg.PipeID,
		RemoteChainID: msg.DestinationChainID, EventKind: "send", PolicyEpoch: msg.FederationPolicyEpoch,
		AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
		ContactRevision: msg.FederationContactRevision, SourceAgentID: sourceAgent,
		TargetAgentID: targetAgent, Proof: proof, CreatedAt: now, ExpiresAt: msg.ExpiresAt,
	}
	require.NoError(t, ss.InsertPipelineWithTransport(ctx, msg, outbox))
	m.pipeTargetResolveFn = func(context.Context, string) (*RemotePipeTarget, error) {
		return &RemotePipeTarget{
			ChainID: "chain-peer", AgentID: targetAgent, PolicyEpoch: msg.FederationPolicyEpoch,
			AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
			ContactRevision: msg.FederationContactRevision,
		}, nil
	}
	m.pipeEventPushFn = func(context.Context, string, *PipeEvent) (*PipeEventResponse, error) {
		return nil, &pipeEventHTTPError{
			Status: http.StatusRequestEntityTooLarge,
			Body:   `{"error":"request body too large"}`,
		}
	}

	m.pipelineDrain(ctx, ss)
	stored, err := ss.GetPipelineTransport(ctx, outbox.EventID)
	require.NoError(t, err)
	require.Equal(t, "pending", stored.State,
		"a peer body-limit refusal must not permanently fail the event")
	require.Equal(t, 1, stored.Attempts)
	require.Contains(t, stored.LastError, "413")
	queued, err := ss.GetPipeline(ctx, msg.PipeID)
	require.NoError(t, err)
	require.Equal(t, "pending", queued.Status,
		"the message itself must stay deliverable")
	updates, err := ss.ListPipelineDeliveryUpdates(ctx, sourceAgent, 10)
	require.NoError(t, err)
	require.Empty(t, updates, "a retryable refusal must not emit terminal feedback")
}

func TestOutboundPipeSendHoldsSourceAvailabilityLeaseThroughPeerAck(t *testing.T) {
	ctx := context.Background()
	m, ss, _ := newDrainTestManager(t)
	sourceAgent := newPeerOperatorID(t)
	targetAgent := newPeerOperatorID(t)
	require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{AgentID: sourceAgent, Name: "sender", Status: "active"}))
	now := time.Now().UTC().Truncate(time.Second)
	proof := store.PipelineAgentProof{
		AgentID: sourceAgent, Signature: make([]byte, ed25519.SignatureSize), Timestamp: now.Unix(),
		Nonce: []byte("12345678"), CanonicalRequest: []byte("POST /v1/pipe/send\n{}"),
	}
	msg := &store.PipelineMessage{
		PipeID: "pipe-source-lease", FromAgent: sourceAgent, ToAgent: targetAgent, DestinationChainID: "chain-peer",
		FederationPolicyEpoch: "epoch-1", FederationAgreementID: strings.Repeat("a", 64),
		FederationContactID: strings.Repeat("b", 64), FederationContactRevision: strings.Repeat("c", 64),
		Payload: "work", Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	outbox := &store.PipelineTransportOutbox{
		EventID: PipelineProofEventID("chain-local", "send", proof), PipeID: msg.PipeID,
		RemoteChainID: msg.DestinationChainID, EventKind: "send", PolicyEpoch: msg.FederationPolicyEpoch,
		AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
		ContactRevision: msg.FederationContactRevision, SourceAgentID: sourceAgent,
		TargetAgentID: targetAgent, Proof: proof, CreatedAt: now, ExpiresAt: msg.ExpiresAt,
	}
	require.NoError(t, ss.InsertPipelineWithTransport(ctx, msg, outbox))
	m.pipeTargetResolveFn = func(context.Context, string) (*RemotePipeTarget, error) {
		return &RemotePipeTarget{
			ChainID: "chain-peer", AgentID: targetAgent, PolicyEpoch: msg.FederationPolicyEpoch,
			AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
			ContactRevision: msg.FederationContactRevision,
		}, nil
	}
	pushStarted := make(chan struct{})
	releasePush := make(chan struct{})
	m.pipeEventPushFn = func(context.Context, string, *PipeEvent) (*PipeEventResponse, error) {
		close(pushStarted)
		<-releasePush
		return &PipeEventResponse{Status: "accepted"}, nil
	}
	drained := make(chan struct{})
	go func() {
		m.pipelineDrain(ctx, ss)
		close(drained)
	}()
	<-pushStarted
	suspended := make(chan error, 1)
	go func() { suspended <- ss.UpdateAgentStatus(ctx, sourceAgent, "inactive") }()
	select {
	case err := <-suspended:
		t.Fatalf("source suspension completed while its payload was awaiting peer acknowledgement: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releasePush)
	<-drained
	require.NoError(t, <-suspended)
	stored, err := ss.GetPipelineTransport(ctx, outbox.EventID)
	require.NoError(t, err)
	require.Equal(t, "delivered", stored.State)
}

func TestPipelineOutboxSendLinearizesWithPeerPurge(t *testing.T) {
	ctx := context.Background()
	m, ss, _ := newDrainTestManager(t)
	sourcePub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	sourceAgent := hex.EncodeToString(sourcePub)
	targetAgent := newPeerOperatorID(t)
	require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{AgentID: sourceAgent, Name: "sender", Status: "active"}))
	now := time.Now().UTC().Truncate(time.Second)
	proof := store.PipelineAgentProof{
		AgentID: sourceAgent, Signature: make([]byte, ed25519.SignatureSize), Timestamp: now.Unix(),
		Nonce: []byte("12345678"), CanonicalRequest: []byte("POST /v1/pipe/send\n{}"),
	}
	msg := &store.PipelineMessage{
		PipeID: "pipe-revoke-race", FromAgent: sourceAgent, ToAgent: targetAgent, DestinationChainID: "chain-peer",
		FederationPolicyEpoch: "epoch-1", FederationAgreementID: strings.Repeat("a", 64),
		FederationContactID: strings.Repeat("b", 64), FederationContactRevision: strings.Repeat("c", 64),
		Payload: "bounded work", Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	outbox := &store.PipelineTransportOutbox{
		EventID: PipelineProofEventID("chain-local", "send", proof), PipeID: msg.PipeID,
		RemoteChainID: msg.DestinationChainID, EventKind: "send", PolicyEpoch: msg.FederationPolicyEpoch,
		AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
		ContactRevision: msg.FederationContactRevision, SourceAgentID: sourceAgent,
		TargetAgentID: targetAgent, Proof: proof, CreatedAt: now, ExpiresAt: msg.ExpiresAt,
	}
	require.NoError(t, ss.InsertPipelineWithTransport(ctx, msg, outbox))
	m.pipeTargetResolveFn = func(context.Context, string) (*RemotePipeTarget, error) {
		return &RemotePipeTarget{
			ChainID: "chain-peer", AgentID: targetAgent, PolicyEpoch: msg.FederationPolicyEpoch,
			AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
			ContactRevision: msg.FederationContactRevision,
		}, nil
	}
	pushStarted := make(chan struct{})
	releasePush := make(chan struct{})
	m.pipeEventPushFn = func(context.Context, string, *PipeEvent) (*PipeEventResponse, error) {
		close(pushStarted)
		<-releasePush
		return &PipeEventResponse{Status: "accepted"}, nil
	}
	drained := make(chan struct{})
	go func() {
		m.pipelineDrain(ctx, ss)
		close(drained)
	}()
	<-pushStarted
	purged := make(chan error, 1)
	go func() { purged <- ss.PurgeSyncPeerState(ctx, "chain-peer") }()
	select {
	case purgeErr := <-purged:
		t.Fatalf("peer purge returned while its old-generation payload was in flight: %v", purgeErr)
	case <-time.After(100 * time.Millisecond):
	}
	close(releasePush)
	<-drained
	require.NoError(t, <-purged)
	_, err = ss.GetPipelineTransport(ctx, outbox.EventID)
	require.Error(t, err, "purge must remove the retired generation's delivery row")
	stored, err := ss.GetPipeline(ctx, msg.PipeID)
	require.NoError(t, err)
	require.Equal(t, "failed", stored.Status, "purge must terminalize the user-visible request")
}

func TestPipelineOutboxResultLinearizesPeerAckWithDeliveredMarkBeforePurge(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	ensurePipeContactAppV23(t, m, bs)
	peerOperator := newPeerOperatorID(t)
	configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerOperator, "host", nil, 4)
	completerPub, completerPriv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	completer := hex.EncodeToString(completerPub)
	remoteAgent := newPeerOperatorID(t)
	seedPipeContactOrdinaryAgent(t, m, ss, bs, completer, "completer", "active", 0, 10)
	require.NoError(t, bs.RegisterDomain("research", completer, "", 10))
	_, err = m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{{Domain: "research.work", Read: true}})
	require.NoError(t, err)
	exportPipeContactAgent(t, m, "chain-peer", completer)
	grant, err := m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	require.Len(t, grant.Contacts, 1)
	contact := grant.Contacts[0]
	now := time.Now().UTC().Truncate(time.Second)
	msg := &store.PipelineMessage{
		PipeID: "pipe-result-revoke-race", FromAgent: remoteAgent, ToAgent: completer,
		SourceChainID: "chain-peer", SourcePipeID: "remote-send-event",
		FederationPolicyEpoch: "epoch-chain-peer", FederationAgreementID: grant.AgreementID,
		FederationContactID:       contact.ContactID,
		FederationContactRevision: pipeContactAuthorizationRevision(grant, &contact),
		Payload:                   "work", Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	require.NoError(t, ss.InsertPipeline(ctx, msg))
	require.NoError(t, ss.ClaimPipeline(ctx, msg.PipeID, completer))
	resultBody, err := json.Marshal(map[string]any{
		"result": "done", "source_pipe_id": msg.SourcePipeID, "source_chain_id": "chain-local",
	})
	require.NoError(t, err)
	proof := signedPipeProof(t, completerPriv, completer, http.MethodPut, "/v1/pipe/"+msg.PipeID+"/result", resultBody, now.Unix())
	outbox := &store.PipelineTransportOutbox{
		EventID: PipelineProofEventID("chain-local", "result", proof), PipeID: msg.PipeID,
		RemoteChainID: msg.SourceChainID, EventKind: "result", PolicyEpoch: msg.FederationPolicyEpoch,
		AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
		ContactRevision: msg.FederationContactRevision, SourceAgentID: completer,
		TargetAgentID: remoteAgent, Proof: proof, CreatedAt: now, ExpiresAt: msg.ExpiresAt,
	}
	require.NoError(t, ss.CompleteFederatedPipelineWithTransport(ctx, msg.PipeID, completer, "done", outbox))

	preflightPassed := false
	preflightPipeID := ""
	preflightEventID := ""
	m.pipeResultPreflightFn = func(_ context.Context, gotMsg *store.PipelineMessage, gotOutbox *store.PipelineTransportOutbox) error {
		preflightPipeID = gotMsg.PipeID
		preflightEventID = gotOutbox.EventID
		preflightPassed = true
		return nil
	}
	pushStarted := make(chan struct{})
	releasePush := make(chan struct{})
	pushSawPreflight := false
	pushedResult := ""
	m.pipeEventPushFn = func(_ context.Context, _ string, event *PipeEvent) (*PipeEventResponse, error) {
		pushSawPreflight = preflightPassed
		pushedResult = event.Result
		close(pushStarted)
		<-releasePush
		return &PipeEventResponse{Status: "accepted"}, nil
	}
	drained := make(chan struct{})
	go func() {
		m.pipelineDrain(ctx, ss)
		close(drained)
	}()
	<-pushStarted
	require.Equal(t, msg.PipeID, preflightPipeID)
	require.Equal(t, outbox.EventID, preflightEventID)
	require.True(t, pushSawPreflight, "result push started before the fresh peer preflight completed")
	require.Equal(t, "done", pushedResult)
	purged := make(chan error, 1)
	go func() { purged <- ss.PurgeSyncPeerState(ctx, "chain-peer") }()
	select {
	case purgeErr := <-purged:
		t.Fatalf("peer purge returned between result acceptance and durable delivery: %v", purgeErr)
	case <-time.After(100 * time.Millisecond):
	}
	close(releasePush)
	<-drained
	require.NoError(t, <-purged)
	_, err = ss.GetPipelineTransport(ctx, outbox.EventID)
	require.Error(t, err, "purge must remove an already delivered result event")
	updates, err := ss.ListPipelineDeliveryUpdates(ctx, completer, 10)
	require.NoError(t, err)
	require.Empty(t, updates, "an acknowledged result must never emit a false terminal failure")
	stored, err := ss.GetPipeline(ctx, msg.PipeID)
	require.NoError(t, err)
	require.Equal(t, "completed", stored.Status)
}

func TestPipelineOutboxResultPreflightFailureNeverPushesOrBuildsResultEnvelope(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	ensurePipeContactAppV23(t, m, bs)
	peerOperator := newPeerOperatorID(t)
	configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerOperator, "host", nil, 4)
	completerPub, completerPriv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	completer := hex.EncodeToString(completerPub)
	remoteAgent := newPeerOperatorID(t)
	seedPipeContactOrdinaryAgent(t, m, ss, bs, completer, "completer", "active", 0, 10)
	require.NoError(t, bs.RegisterDomain("preflight", completer, "", 10))
	_, err = m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{{Domain: "preflight.work", Read: true}})
	require.NoError(t, err)
	exportPipeContactAgent(t, m, "chain-peer", completer)
	grant, err := m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	require.Len(t, grant.Contacts, 1)
	contact := grant.Contacts[0]
	now := time.Now().UTC().Truncate(time.Second)
	msg := &store.PipelineMessage{
		PipeID: "pipe-result-preflight-denied", FromAgent: remoteAgent, ToAgent: completer,
		SourceChainID: "chain-peer", SourcePipeID: "remote-send-preflight-denied",
		FederationPolicyEpoch: "epoch-chain-peer", FederationAgreementID: grant.AgreementID,
		FederationContactID:       contact.ContactID,
		FederationContactRevision: pipeContactAuthorizationRevision(grant, &contact),
		Payload:                   "work", Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	require.NoError(t, ss.InsertPipeline(ctx, msg))
	require.NoError(t, ss.ClaimPipeline(ctx, msg.PipeID, completer))
	resultBody, err := json.Marshal(map[string]any{
		"result": "sensitive result", "source_pipe_id": msg.SourcePipeID, "source_chain_id": "chain-local",
	})
	require.NoError(t, err)
	proof := signedPipeProof(t, completerPriv, completer, http.MethodPut, "/v1/pipe/"+msg.PipeID+"/result", resultBody, now.Unix())
	outbox := &store.PipelineTransportOutbox{
		EventID: PipelineProofEventID("chain-local", "result", proof), PipeID: msg.PipeID,
		RemoteChainID: msg.SourceChainID, EventKind: "result", PolicyEpoch: msg.FederationPolicyEpoch,
		AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
		ContactRevision: msg.FederationContactRevision, SourceAgentID: completer,
		TargetAgentID: remoteAgent, Proof: proof, CreatedAt: now, ExpiresAt: msg.ExpiresAt,
	}
	require.NoError(t, ss.CompleteFederatedPipelineWithTransport(ctx, msg.PipeID, completer, "sensitive result", outbox))

	event, terminal, err := m.buildPipelineEvent(ctx, ss, outbox)
	require.NoError(t, err)
	require.False(t, terminal)
	require.Empty(t, event.Result, "result bytes entered the outbound envelope before the fresh peer preflight")
	// The outbox row above carries msg.ExpiresAt (one hour) as its expiry, which
	// is what a retention re-stamp looks like to a reply. The envelope must be
	// derived from the signed proof instead: the destination admits ONLY
	// created+PipeEventResultLifetime and calls anything else an invalid proof.
	require.Equal(t, now, event.CreatedAt)
	require.Equal(t, now.Add(PipeEventResultLifetime), event.ExpiresAt,
		"the wire lifetime must come from the signed proof, never from the retained row")

	preflightCalls := 0
	preflightPipeID := ""
	preflightEventID := ""
	m.pipeResultPreflightFn = func(_ context.Context, gotMsg *store.PipelineMessage, gotOutbox *store.PipelineTransportOutbox) error {
		preflightCalls++
		preflightPipeID = gotMsg.PipeID
		preflightEventID = gotOutbox.EventID
		return errors.New("fresh authenticated peer status unavailable")
	}
	pushCalls := 0
	pushedResult := ""
	m.pipeEventPushFn = func(_ context.Context, _ string, event *PipeEvent) (*PipeEventResponse, error) {
		pushCalls++
		pushedResult = event.Result
		return &PipeEventResponse{Status: "accepted"}, nil
	}

	m.pipelineDrain(ctx, ss)
	require.Equal(t, 1, preflightCalls)
	require.Equal(t, msg.PipeID, preflightPipeID)
	require.Equal(t, outbox.EventID, preflightEventID)
	require.Zero(t, pushCalls, "result delivery reached the network push after a failed fresh preflight")
	require.Empty(t, pushedResult, "a failed preflight exposed result bytes to the push boundary")
	stored, err := ss.GetPipelineTransport(ctx, outbox.EventID)
	require.NoError(t, err)
	require.Equal(t, "pending", stored.State)
	require.Contains(t, stored.LastError, "fresh authenticated peer status unavailable")
}

// A destination older than the current reply window refuses an otherwise valid
// reply with one opaque 400. The delivery loop must narrow the row to the legacy
// window and retry instead of terminalizing a reply that is still deliverable,
// and it must not loop: once the row carries the legacy value, a second refusal
// is the ordinary terminal failure.
func TestPipelineOutboxResultDowngradesReplyWindowOnceForAnOlderDestination(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	ensurePipeContactAppV23(t, m, bs)
	peerOperator := newPeerOperatorID(t)
	configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerOperator, "host", nil, 4)
	completerPub, completerPriv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	completer := hex.EncodeToString(completerPub)
	remoteAgent := newPeerOperatorID(t)
	seedPipeContactOrdinaryAgent(t, m, ss, bs, completer, "completer", "active", 0, 10)
	require.NoError(t, bs.RegisterDomain("window", completer, "", 10))
	_, err = m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{{Domain: "window.work", Read: true}})
	require.NoError(t, err)
	exportPipeContactAgent(t, m, "chain-peer", completer)
	grant, err := m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	require.Len(t, grant.Contacts, 1)
	contact := grant.Contacts[0]
	now := time.Now().UTC().Truncate(time.Second)
	msg := &store.PipelineMessage{
		PipeID: "pipe-result-window", FromAgent: remoteAgent, ToAgent: completer,
		SourceChainID: "chain-peer", SourcePipeID: "remote-send-window",
		FederationPolicyEpoch: "epoch-chain-peer", FederationAgreementID: grant.AgreementID,
		FederationContactID:       contact.ContactID,
		FederationContactRevision: pipeContactAuthorizationRevision(grant, &contact),
		Payload:                   "work", Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	require.NoError(t, ss.InsertPipeline(ctx, msg))
	require.NoError(t, ss.ClaimPipeline(ctx, msg.PipeID, completer))
	resultBody, err := json.Marshal(map[string]any{
		"result": "done", "source_pipe_id": msg.SourcePipeID, "source_chain_id": "chain-local",
	})
	require.NoError(t, err)
	proof := signedPipeProof(t, completerPriv, completer, http.MethodPut, "/v1/pipe/"+msg.PipeID+"/result", resultBody, now.Unix())
	outbox := &store.PipelineTransportOutbox{
		EventID: PipelineProofEventID("chain-local", "result", proof), PipeID: msg.PipeID,
		RemoteChainID: msg.SourceChainID, EventKind: "result", PolicyEpoch: msg.FederationPolicyEpoch,
		AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
		ContactRevision: msg.FederationContactRevision, SourceAgentID: completer,
		TargetAgentID: remoteAgent, Proof: proof, CreatedAt: now, ExpiresAt: now.Add(PipeEventResultLifetime),
	}
	require.NoError(t, ss.CompleteFederatedPipelineWithTransport(ctx, msg.PipeID, completer, "done", outbox))

	m.pipeResultPreflightFn = func(context.Context, *store.PipelineMessage, *store.PipelineTransportOutbox) error {
		return nil
	}
	var windows []time.Duration
	m.pipeEventPushFn = func(_ context.Context, _ string, event *PipeEvent) (*PipeEventResponse, error) {
		windows = append(windows, event.ExpiresAt.Sub(event.CreatedAt))
		return nil, &pipeEventHTTPError{Status: http.StatusBadRequest, Body: `{"error":"invalid pipeline agent proof"}`}
	}

	m.deliverPipelineEvent(ctx, ss, outbox)
	require.Equal(t, []time.Duration{PipeEventResultLifetime}, windows,
		"the first refusal of a valid reply must not terminalize it")
	downgraded, err := ss.GetPipelineTransport(ctx, outbox.EventID)
	require.NoError(t, err)
	require.Equal(t, "pending", downgraded.State)
	require.Equal(t, now.Add(24*time.Hour).Unix(), downgraded.ExpiresAt.Unix(),
		"the retry must carry the legacy window a destination that old can accept")

	m.deliverPipelineEvent(ctx, ss, downgraded)
	require.Equal(t, []time.Duration{PipeEventResultLifetime, 24 * time.Hour}, windows)
	terminal, err := ss.GetPipelineTransport(ctx, outbox.EventID)
	require.NoError(t, err)
	require.Equal(t, "failed", terminal.State, "a second refusal has nothing left to downgrade")
	require.Contains(t, terminal.LastError, "invalid pipeline agent proof")
}

func TestImportedPipeActionHoldsOwnerLeaseThroughSideEffect(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	ensurePipeContactAppV23(t, m, bs)
	peerOperator := newPeerOperatorID(t)
	configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerOperator, "host", nil, 4)
	ownerA, ownerB := newPeerOperatorID(t), newPeerOperatorID(t)
	seedPipeContactOrdinaryAgent(t, m, ss, bs, ownerA, "owner-a", "active", 0, 10)
	seedPipeContactOrdinaryAgent(t, m, ss, bs, ownerB, "owner-b", "active", 0, 20)
	require.NoError(t, bs.RegisterDomain("research", ownerA, "", 10))
	_, err := m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{{Domain: "research.work", Read: true}})
	require.NoError(t, err)
	exportPipeContactAgent(t, m, "chain-peer", ownerA)
	grant, err := m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	contact := grant.Contacts[0]
	msg := &store.PipelineMessage{
		PipeID: "pipe-owner-lease", FromAgent: newPeerOperatorID(t), ToAgent: ownerA,
		SourceChainID: "chain-peer", SourcePipeID: "pipe-event-owner-lease",
		FederationPolicyEpoch: "epoch-chain-peer", FederationAgreementID: grant.AgreementID,
		FederationContactID:       contact.ContactID,
		FederationContactRevision: pipeContactAuthorizationRevision(grant, &contact),
		Payload:                   "work", Status: "completed", CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour),
	}
	actionStarted := make(chan struct{})
	releaseAction := make(chan struct{})
	authorized := make(chan error, 1)
	go func() {
		authorized <- m.WithAuthorizedImportedPipe(ctx, msg, func() error {
			close(actionStarted)
			<-releaseAction
			return nil
		})
	}()
	<-actionStarted
	transferred := make(chan error, 1)
	go func() { transferred <- bs.TransferDomain("research", ownerB, "", 11) }()
	select {
	case err := <-transferred:
		t.Fatalf("owner transfer completed during an authorized external side effect: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseAction)
	require.NoError(t, <-authorized)
	require.NoError(t, <-transferred)
	require.Error(t, m.AuthorizeImportedPipe(ctx, msg), "the retired owner must fail the next authorization")
}

func TestImportedPipeActionHoldsExplicitExportLeaseThroughSideEffect(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	ensurePipeContactAppV23(t, m, bs)
	peerOperator := newPeerOperatorID(t)
	configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerOperator, "host", nil, 4)
	owner := newPeerOperatorID(t)
	seedPipeContactOrdinaryAgent(t, m, ss, bs, owner, "owner", "active", 0, 10)
	require.NoError(t, bs.RegisterDomain("research", owner, "", 10))
	_, err := m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{{Domain: "research", Read: true}})
	require.NoError(t, err)
	exported := exportPipeContactAgent(t, m, "chain-peer", owner)
	grant, err := m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	require.Len(t, grant.Contacts, 1)
	contact := grant.Contacts[0]
	msg := &store.PipelineMessage{
		PipeID: "pipe-export-lease", FromAgent: newPeerOperatorID(t), ToAgent: owner,
		SourceChainID: "chain-peer", SourcePipeID: "pipe-event-export-lease",
		FederationPolicyEpoch: "epoch-chain-peer", FederationAgreementID: grant.AgreementID,
		FederationContactID:       contact.ContactID,
		FederationContactRevision: pipeContactAuthorizationRevision(grant, &contact),
		Payload:                   "work", Status: "completed", CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour),
	}
	actionStarted := make(chan struct{})
	releaseAction := make(chan struct{})
	authorized := make(chan error, 1)
	go func() {
		authorized <- m.WithAuthorizedImportedPipe(ctx, msg, func() error {
			close(actionStarted)
			<-releaseAction
			return nil
		})
	}()
	<-actionStarted
	paused := make(chan error, 1)
	go func() {
		_, pauseErr := m.SetFederatedAgentExport(ctx, "chain-peer", owner,
			store.FederatedAgentExportStatePaused, 4, nil, exported.Revision)
		paused <- pauseErr
	}()
	select {
	case pauseErr := <-paused:
		t.Fatalf("export pause completed during an authorized federated side effect: %v", pauseErr)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseAction)
	require.NoError(t, <-authorized)
	require.NoError(t, <-paused)
	require.Error(t, m.AuthorizeImportedPipe(ctx, msg), "a paused export must fail the next authorization")
}

func TestImportedPipeOrgMembershipDoesNotSynthesizeFederatedTarget(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	ensurePipeContactAppV23(t, m, bs)
	peerOperator := newPeerOperatorID(t)
	configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerOperator, "host", nil, 4)
	owner, reader := newPeerOperatorID(t), newPeerOperatorID(t)
	seedPipeContactOrdinaryAgent(t, m, ss, bs, owner, "owner", "active", 0, 10)
	seedPipeContactOrdinaryAgent(t, m, ss, bs, reader, "reader", "active", 0, 20)
	require.NoError(t, bs.RegisterOrg("research-org", "Research", "", owner, 1))
	require.NoError(t, bs.AddOrgMember("research-org", owner, 4, "admin", 1))
	require.NoError(t, bs.AddOrgMember("research-org", reader, 1, "member", 1))
	require.NoError(t, bs.RegisterDomain("research", owner, "", 10))
	_, err := m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{{Domain: "research", Read: true}})
	require.NoError(t, err)
	exportPipeContactAgent(t, m, "chain-peer", owner)
	grant, err := m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	require.Len(t, grant.Contacts, 1)
	require.Equal(t, owner, grant.Contacts[0].AgentID)
	for _, contact := range grant.Contacts {
		require.NotEqual(t, reader, contact.AgentID,
			"same-org membership is not explicit federation membership")
	}
}

func TestImportedPipeActionHoldsAgentAvailabilityLeaseThroughSideEffect(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	ensurePipeContactAppV23(t, m, bs)
	peerOperator := newPeerOperatorID(t)
	configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerOperator, "host", nil, 4)
	owner := newPeerOperatorID(t)
	seedPipeContactOrdinaryAgent(t, m, ss, bs, owner, "owner", "active", 0, 10)
	require.NoError(t, bs.RegisterDomain("research", owner, "", 10))
	_, err := m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{{Domain: "research.work", Read: true}})
	require.NoError(t, err)
	exportPipeContactAgent(t, m, "chain-peer", owner)
	grant, err := m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	contact := grant.Contacts[0]
	msg := &store.PipelineMessage{
		PipeID: "pipe-agent-lease", FromAgent: newPeerOperatorID(t), ToAgent: owner,
		SourceChainID: "chain-peer", SourcePipeID: "pipe-event-agent-lease",
		FederationPolicyEpoch: "epoch-chain-peer", FederationAgreementID: grant.AgreementID,
		FederationContactID:       contact.ContactID,
		FederationContactRevision: pipeContactAuthorizationRevision(grant, &contact),
		Payload:                   "work", Status: "completed", CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour),
	}
	actionStarted := make(chan struct{})
	releaseAction := make(chan struct{})
	authorized := make(chan error, 1)
	go func() {
		authorized <- m.WithAuthorizedImportedPipe(ctx, msg, func() error {
			close(actionStarted)
			<-releaseAction
			return nil
		})
	}()
	<-actionStarted
	suspended := make(chan error, 1)
	go func() { suspended <- ss.UpdateAgentStatus(ctx, owner, "inactive") }()
	select {
	case err := <-suspended:
		t.Fatalf("agent suspension completed during an authorized external side effect: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseAction)
	require.NoError(t, <-authorized)
	require.NoError(t, <-suspended)
	require.Error(t, m.AuthorizeImportedPipe(ctx, msg), "an unavailable target must fail the next authorization")
}

func TestSessionBoundReplyProofRemainsStrictAndSigned(t *testing.T) {
	pub, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	agent := hex.EncodeToString(pub)
	now := time.Now().UTC().Truncate(time.Second)
	for _, tc := range []struct {
		name           string
		session, extra bool
		wantError      bool
	}{
		{"legacy", false, false, false},
		{"session", true, false, false},
		{"unknown-field", true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := map[string]any{"result": "done", "source_pipe_id": "origin", "source_chain_id": "replying-chain"}
			if tc.session {
				body["claimant_session_id"] = "mcp-owner"
			}
			if tc.extra {
				body["unexpected_authority"] = "root"
			}
			encoded, err := json.Marshal(body)
			require.NoError(t, err)
			proof := signedPipeProof(t, key, agent, http.MethodPut, "/v1/pipe/imported/result", encoded, now.Unix())
			event := &PipeEvent{Kind: "result", SourceChainID: "replying-chain", SourcePipeID: "imported", OriginEventID: "origin", Result: "done", CreatedAt: now, ExpiresAt: now.Add(PipeEventResultLifetime), Proof: proof}
			if tc.wantError {
				require.Error(t, prevalidatePipeEventAgentProof(event))
				return
			}
			require.NoError(t, prevalidatePipeEventAgentProof(event))
			if tc.session {
				event.Proof.CanonicalRequest = bytes.Replace(event.Proof.CanonicalRequest, []byte("mcp-owner"), []byte("mcp-other"), 1)
				require.Error(t, prevalidatePipeEventAgentProof(event), "the session field remains covered by the exact agent signature")
			}
		})
	}
}

func TestFederatedMessageWakeNotifierIsPanicSafe(t *testing.T) {
	m := &Manager{}
	m.SetMessageWakeNotifier(func(string, uint64) { panic("broken bridge") })
	require.NotPanics(t, func() { m.notifyMessageWake("recipient", 1) })
}
