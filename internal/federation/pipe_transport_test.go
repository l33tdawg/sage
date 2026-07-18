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
	peerOperator := newPeerOperatorID(t)
	agreement := configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerOperator, "host", nil, 4)
	owner := newPeerOperatorID(t)
	unrelatedOwner := newPeerOperatorID(t)
	require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{AgentID: owner, Name: "sentinel", Status: "active"}))
	require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{AgentID: unrelatedOwner, Name: "unrelated", Status: "active"}))
	require.NoError(t, bs.RegisterDomain("security", owner, "", 10))
	require.NoError(t, bs.RegisterDomain("unrelated", unrelatedOwner, "", 11))
	_, err := m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{
		{Domain: "security.alerts", Read: true}, {Domain: "unrelated.work", Read: true},
	})
	require.NoError(t, err)
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
	_, err = m.SetPipeContactAcceptance(ctx, "chain-peer", owner, contact.ContactID, true)
	require.NoError(t, err)
	grant, err = m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	unrelated := contactFor(unrelatedOwner)
	_, err = m.SetPipeContactAcceptance(ctx, "chain-peer", unrelatedOwner, unrelated.ContactID, true)
	require.NoError(t, err)
	grant, err = m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	contact = contactFor(owner)
	unrelated = contactFor(unrelatedOwner)
	require.True(t, contact.Accepting)

	sourcePub, sourcePriv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	sourceAgent := hex.EncodeToString(sourcePub)
	ts := time.Now().UTC().Truncate(time.Second)
	signedBody, err := json.Marshal(map[string]any{
		"to_agent": contact.AgentID, "source_chain_id": "chain-peer", "destination_chain_id": "chain-local",
		"intent": "triage", "payload": "review finding", "ttl_minutes": 90,
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

	rr = callPipeEvent(t, m, agreement, peerOperator, event)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var duplicate PipeEventResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&duplicate))
	require.Equal(t, "duplicate", duplicate.Status)
	_, err = m.SetPipeContactAcceptance(ctx, "chain-peer", unrelatedOwner, unrelated.ContactID, false)
	require.NoError(t, err)
	rr = callPipeEvent(t, m, agreement, peerOperator, event)
	require.Equal(t, http.StatusOK, rr.Code, "unrelated acceptance must not invalidate exact-target work: %s", rr.Body.String())
	_, err = m.SetPipeContactAcceptance(ctx, "chain-peer", unrelatedOwner, unrelated.ContactID, true)
	require.NoError(t, err)
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
	_, err = m.SetPipeContactAcceptance(ctx, "chain-peer", owner, contact.ContactID, false)
	require.NoError(t, err)
	rr = callPipeEvent(t, m, agreement, peerOperator, event)
	require.Equal(t, http.StatusLocked, rr.Code, "temporary acceptance-off must remain retryable: %s", rr.Body.String())
	_, err = m.SetPipeContactAcceptance(ctx, "chain-peer", owner, contact.ContactID, true)
	require.NoError(t, err)
	rr = callPipeEvent(t, m, agreement, peerOperator, event)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&duplicate))
	require.Equal(t, "duplicate", duplicate.Status, "acceptance-on must accept the unchanged retry exactly once")

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
	otherGrant, err := m.LocalPipeContacts(ctx, "chain-other")
	require.NoError(t, err)
	_, err = m.SetPipeContactAcceptance(ctx, "chain-other", owner, otherGrant.Contacts[0].ContactID, true)
	require.NoError(t, err)
	otherGrant, err = m.LocalPipeContacts(ctx, "chain-other")
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

func TestHandlePipeEventSendRejectsStaleOwnerRevision(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	peerOperator := newPeerOperatorID(t)
	agreement := configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerOperator, "host", nil, 4)
	ownerA, ownerB := newPeerOperatorID(t), newPeerOperatorID(t)
	for _, owner := range []string{ownerA, ownerB} {
		require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{AgentID: owner, Name: "owner", Status: "active"}))
	}
	require.NoError(t, bs.RegisterDomain("research", ownerA, "", 10))
	_, err := m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{{Domain: "research.work", Read: true}})
	require.NoError(t, err)
	grant, err := m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	_, err = m.SetPipeContactAcceptance(ctx, "chain-peer", ownerA, grant.Contacts[0].ContactID, true)
	require.NoError(t, err)
	grant, err = m.LocalPipeContacts(ctx, "chain-peer")
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

	resultBody, _ := json.Marshal(map[string]any{"result": "done safely", "source_pipe_id": originEventID, "source_chain_id": "chain-peer"})
	resultProof := signedPipeProof(t, remotePriv, remoteAgent, http.MethodPut, "/v1/pipe/pipe-remote-import/result", resultBody, now.Add(time.Minute).Unix())
	event := &PipeEvent{
		Version: PipeEventVersion, Kind: "result", OriginEventID: originEventID, SourcePipeID: "pipe-remote-import",
		SourceChainID: "chain-peer", DestinationChainID: "chain-local", SourceAgentID: remoteAgent,
		TargetAgentID: localSender, Result: "done safely", CreatedAt: now.Add(time.Minute),
		ExpiresAt: now.Add(time.Minute).Add(pipeEventResultLifetime), PolicyEpoch: msg.FederationPolicyEpoch,
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
	case err := <-purged:
		t.Fatalf("peer purge returned while its old-generation payload was in flight: %v", err)
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
	peerOperator := newPeerOperatorID(t)
	configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerOperator, "host", nil, 4)
	completerPub, completerPriv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	completer := hex.EncodeToString(completerPub)
	remoteAgent := newPeerOperatorID(t)
	require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{AgentID: completer, Name: "completer", Status: "active"}))
	require.NoError(t, bs.RegisterDomain("research", completer, "", 10))
	_, err = m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{{Domain: "research.work", Read: true}})
	require.NoError(t, err)
	grant, err := m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	require.Len(t, grant.Contacts, 1)
	_, err = m.SetPipeContactAcceptance(ctx, "chain-peer", completer, grant.Contacts[0].ContactID, true)
	require.NoError(t, err)
	grant, err = m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
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
	case err := <-purged:
		t.Fatalf("peer purge returned between result acceptance and durable delivery: %v", err)
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
	peerOperator := newPeerOperatorID(t)
	configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerOperator, "host", nil, 4)
	completerPub, completerPriv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	completer := hex.EncodeToString(completerPub)
	remoteAgent := newPeerOperatorID(t)
	require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{AgentID: completer, Name: "completer", Status: "active"}))
	require.NoError(t, bs.RegisterDomain("preflight", completer, "", 10))
	_, err = m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{{Domain: "preflight.work", Read: true}})
	require.NoError(t, err)
	grant, err := m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	require.Len(t, grant.Contacts, 1)
	_, err = m.SetPipeContactAcceptance(ctx, "chain-peer", completer, grant.Contacts[0].ContactID, true)
	require.NoError(t, err)
	grant, err = m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
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

func TestImportedPipeActionHoldsOwnerLeaseThroughSideEffect(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	peerOperator := newPeerOperatorID(t)
	configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerOperator, "host", nil, 4)
	ownerA, ownerB := newPeerOperatorID(t), newPeerOperatorID(t)
	for _, owner := range []string{ownerA, ownerB} {
		require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{AgentID: owner, Name: "owner", Status: "active"}))
	}
	require.NoError(t, bs.RegisterDomain("research", ownerA, "", 10))
	_, err := m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{{Domain: "research.work", Read: true}})
	require.NoError(t, err)
	grant, err := m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	_, err = m.SetPipeContactAcceptance(ctx, "chain-peer", ownerA, grant.Contacts[0].ContactID, true)
	require.NoError(t, err)
	grant, err = m.LocalPipeContacts(ctx, "chain-peer")
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

func TestImportedPipeActionHoldsAgentAvailabilityLeaseThroughSideEffect(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	peerOperator := newPeerOperatorID(t)
	configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerOperator, "host", nil, 4)
	owner := newPeerOperatorID(t)
	require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{AgentID: owner, Name: "owner", Status: "active"}))
	require.NoError(t, bs.RegisterDomain("research", owner, "", 10))
	_, err := m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{{Domain: "research.work", Read: true}})
	require.NoError(t, err)
	grant, err := m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	_, err = m.SetPipeContactAcceptance(ctx, "chain-peer", owner, grant.Contacts[0].ContactID, true)
	require.NoError(t, err)
	grant, err = m.LocalPipeContacts(ctx, "chain-peer")
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
