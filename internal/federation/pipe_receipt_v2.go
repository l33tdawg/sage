package federation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/store"
)

const (
	PipeReceiptVersion        = 2
	maxPipeReceiptV2BodyBytes = 64 << 10
)

// PipeReceiptEvent is a separate v2 protocol. It never overloads the legacy
// PipeEvent or the co-commit /fed/v1/receipt route.
type PipeReceiptEvent struct {
	Version           int                      `json:"version"`
	EventID           string                   `json:"event_id"`
	MessageID         string                   `json:"message_id"`
	RecipientPipeID   string                   `json:"recipient_pipe_id"`
	SenderChainID     string                   `json:"sender_chain_id"`
	RecipientChainID  string                   `json:"recipient_chain_id"`
	SenderAgentID     string                   `json:"sender_agent_id"`
	RecipientAgentID  string                   `json:"recipient_agent_id"`
	ContentDigest     string                   `json:"content_digest"`
	Kind              string                   `json:"event_kind"`
	EventAt           time.Time                `json:"event_at"`
	PolicyEpoch       string                   `json:"policy_epoch"`
	AgreementID       string                   `json:"agreement_id"`
	ContactID         string                   `json:"contact_id"`
	ContactRevision   string                   `json:"contact_revision"`
	AuthorizationMode string                   `json:"authorization_mode,omitempty"`
	RelationDigest    string                   `json:"relation_digest,omitempty"`
	Proof             store.PipelineAgentProof `json:"proof"`
}

type PipeReceiptResponse struct {
	Status string `json:"status"`
}

type signedPipeReceiptRequest struct {
	Version           int    `json:"version"`
	MessageID         string `json:"message_id"`
	SenderChainID     string `json:"sender_chain_id"`
	RecipientChainID  string `json:"recipient_chain_id"`
	SenderAgentID     string `json:"sender_agent_id"`
	RecipientAgentID  string `json:"recipient_agent_id"`
	ContentDigest     string `json:"content_digest"`
	EventKind         string `json:"event_kind"`
	PolicyEpoch       string `json:"policy_epoch"`
	AgreementID       string `json:"agreement_id"`
	ContactID         string `json:"contact_id"`
	ContactRevision   string `json:"contact_revision"`
	AuthorizationMode string `json:"authorization_mode,omitempty"`
	RelationDigest    string `json:"relation_digest,omitempty"`
}

func signedReceiptRequest(binding *store.FederatedReceiptBinding, kind string) signedPipeReceiptRequest {
	return signedPipeReceiptRequest{
		Version: PipeReceiptVersion, MessageID: binding.MessageID,
		SenderChainID: binding.SenderChainID, RecipientChainID: binding.RecipientChainID,
		SenderAgentID: binding.SenderAgentID, RecipientAgentID: binding.RecipientAgentID,
		ContentDigest: binding.ContentDigest, EventKind: kind, PolicyEpoch: binding.PolicyEpoch,
		AgreementID: binding.AgreementID, ContactID: binding.ContactID,
		ContactRevision: binding.ContactRevision, AuthorizationMode: binding.AuthorizationMode,
		RelationDigest: binding.RelationDigest,
	}
}

// ImportedPipeReceiptChallenge returns the exact immutable body an imported
// message recipient must sign for one claim/read acknowledgement. It contains
// no payload and is unavailable for legacy/unnegotiated imports.
func (m *Manager) ImportedPipeReceiptChallenge(
	ctx context.Context, localPipeID, recipientID, kind string,
) (json.RawMessage, error) {
	if kind != "claimed" && kind != "read" {
		return nil, store.ErrFederatedReceiptInvalid
	}
	ss := m.syncStore()
	if ss == nil {
		return nil, store.ErrFederatedReceiptNotFound
	}
	msg, err := ss.GetPipeline(ctx, localPipeID)
	if err != nil || msg.SourceChainID == "" || msg.DestinationChainID != "" || msg.ToAgent != recipientID {
		return nil, store.ErrFederatedReceiptNotFound
	}
	if kind == "claimed" && msg.Status == "pending" && !msg.ExpiresAt.After(time.Now().UTC()) {
		return nil, store.ErrFederatedReceiptNotFound
	}
	binding, err := ss.GetFederatedReceiptInbound(ctx, localPipeID)
	if err != nil || binding.RecipientAgentID != recipientID {
		return nil, store.ErrFederatedReceiptNotFound
	}
	if err := m.WithAuthorizedImportedPipe(ctx, msg, nil); err != nil {
		return nil, err
	}
	body, err := json.Marshal(signedReceiptRequest(binding, kind))
	return json.RawMessage(body), err
}

// RecordImportedPipeReceipt verifies the exact recipient's nonce-bound proof,
// then keeps authorization leases through the atomic claim/outbox write.
func (m *Manager) RecordImportedPipeReceipt(
	ctx context.Context, localPipeID, recipientID, kind string, proof store.PipelineAgentProof,
) (bool, error) {
	ss := m.syncStore()
	if ss == nil || (kind != "claimed" && kind != "read") {
		return false, store.ErrFederatedReceiptInvalid
	}
	msg, err := ss.GetPipeline(ctx, localPipeID)
	if err != nil || msg.SourceChainID == "" || msg.DestinationChainID != "" || msg.ToAgent != recipientID {
		return false, store.ErrFederatedReceiptNotFound
	}
	if kind == "claimed" && msg.Status == "pending" && !msg.ExpiresAt.After(time.Now().UTC()) {
		return false, store.ErrFederatedReceiptNotFound
	}
	binding, err := ss.GetFederatedReceiptInbound(ctx, localPipeID)
	if err != nil || binding.RecipientAgentID != recipientID {
		return false, store.ErrFederatedReceiptNotFound
	}
	method, path, body, err := verifyPipelineAgentProof(proof)
	expectedPath := "/v1/pipe/" + localPipeID + "/receipt/" + kind
	var signed signedPipeReceiptRequest
	if err != nil || proof.AgentID != recipientID || method != http.MethodPut || path != expectedPath ||
		decodeStrictPipeJSON(body, &signed) != nil ||
		!reflect.DeepEqual(signed, signedReceiptRequest(binding, kind)) {
		return false, store.ErrFederatedReceiptInvalid
	}
	eventAt := time.Unix(proof.Timestamp, 0).UTC()
	if time.Since(eventAt) > maxTimestampSkew || eventAt.After(time.Now().UTC().Add(maxTimestampSkew)) {
		return false, store.ErrFederatedReceiptInvalid
	}
	transport := &PipeReceiptEvent{
		Version: PipeReceiptVersion, MessageID: binding.MessageID, RecipientPipeID: localPipeID,
		SenderChainID: binding.SenderChainID, RecipientChainID: binding.RecipientChainID,
		SenderAgentID: binding.SenderAgentID, RecipientAgentID: binding.RecipientAgentID,
		ContentDigest: binding.ContentDigest, Kind: kind, EventAt: eventAt,
		PolicyEpoch: binding.PolicyEpoch, AgreementID: binding.AgreementID,
		ContactID: binding.ContactID, ContactRevision: binding.ContactRevision,
		AuthorizationMode: binding.AuthorizationMode, RelationDigest: binding.RelationDigest,
		Proof: proof,
	}
	transport.EventID = pipeReceiptEventID(transport)
	outbox := &store.FederatedReceiptOutbox{
		FederatedReceiptBinding: *binding, EventID: transport.EventID,
		RecipientPipeID: localPipeID, Kind: kind, EventAt: eventAt, Proof: proof,
		CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	}
	var replayed bool
	err = m.WithAuthorizedImportedPipe(ctx, msg, func() error {
		var recordErr error
		replayed, recordErr = ss.RecordImportedFederatedReceipt(ctx, localPipeID, recipientID, outbox)
		return recordErr
	})
	if err == nil {
		m.NudgePipelineTransport()
	}
	return replayed, err
}

func pipeReceiptContentDigest(messageID, senderChainID, recipientChainID string, msg *store.PipelineMessage) string {
	if msg == nil {
		return ""
	}
	body, _ := json.Marshal(struct {
		Purpose          string `json:"purpose"`
		Version          int    `json:"version"`
		MessageID        string `json:"message_id"`
		SenderChainID    string `json:"sender_chain_id"`
		RecipientChainID string `json:"recipient_chain_id"`
		SenderAgentID    string `json:"sender_agent_id"`
		RecipientAgentID string `json:"recipient_agent_id"`
		Intent           string `json:"intent"`
		Payload          string `json:"payload"`
		CreatedAt        string `json:"created_at"`
		ExpiresAt        string `json:"expires_at"`
	}{
		"sage-federated-pipeline-receipt-content-v2", PipeReceiptVersion,
		messageID, senderChainID, recipientChainID, msg.FromAgent, msg.ToAgent,
		msg.Intent, msg.Payload, msg.CreatedAt.UTC().Format(time.RFC3339Nano),
		msg.ExpiresAt.UTC().Format(time.RFC3339Nano),
	})
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func pipeReceiptProofHash(event *PipeReceiptEvent) [32]byte {
	body, _ := json.Marshal(struct {
		Purpose          string                   `json:"purpose"`
		Version          int                      `json:"version"`
		SenderChainID    string                   `json:"sender_chain_id"`
		RecipientChainID string                   `json:"recipient_chain_id"`
		EventKind        string                   `json:"event_kind"`
		Proof            store.PipelineAgentProof `json:"proof"`
	}{"sage-federated-pipeline-receipt-proof-v2", PipeReceiptVersion,
		event.SenderChainID, event.RecipientChainID, event.Kind, event.Proof})
	return sha256.Sum256(body)
}

func pipeReceiptEventID(event *PipeReceiptEvent) string {
	sum := pipeReceiptProofHash(event)
	return "pipe-receipt-v2-" + hex.EncodeToString(sum[:])
}

func receiptStoreEvent(event *PipeReceiptEvent, localPipeID string) *store.FederatedReceiptEvent {
	proofHash := pipeReceiptProofHash(event)
	return &store.FederatedReceiptEvent{
		FederatedReceiptBinding: store.FederatedReceiptBinding{
			MessageID: event.MessageID, LocalPipeID: localPipeID,
			SenderChainID: event.SenderChainID, RecipientChainID: event.RecipientChainID,
			SenderAgentID: event.SenderAgentID, RecipientAgentID: event.RecipientAgentID,
			ContentDigest: event.ContentDigest, PolicyEpoch: event.PolicyEpoch,
			AgreementID: event.AgreementID, ContactID: event.ContactID,
			ContactRevision: event.ContactRevision, AuthorizationMode: event.AuthorizationMode,
			RelationDigest: event.RelationDigest,
		},
		EventID: event.EventID, Kind: event.Kind, EventAt: event.EventAt, ProofHash: proofHash[:],
	}
}

func validatePipeReceiptProof(event *PipeReceiptEvent) error {
	if event == nil || event.Version != PipeReceiptVersion || event.EventID != pipeReceiptEventID(event) ||
		event.Proof.AgentID != event.RecipientAgentID || event.RecipientPipeID == "" ||
		len(event.RecipientPipeID) > 200 || strings.ContainsAny(event.RecipientPipeID, "/?#") ||
		(event.Kind != "claimed" && event.Kind != "read") {
		return store.ErrFederatedReceiptInvalid
	}
	method, path, body, err := verifyPipelineAgentProof(event.Proof)
	expectedPath := "/v1/pipe/" + event.RecipientPipeID + "/receipt/" + event.Kind
	if err != nil || method != http.MethodPut || path != expectedPath {
		return store.ErrFederatedReceiptInvalid
	}
	var signed signedPipeReceiptRequest
	if decodeStrictPipeJSON(body, &signed) != nil || signed.Version != PipeReceiptVersion ||
		signed.MessageID != event.MessageID || signed.SenderChainID != event.SenderChainID ||
		signed.RecipientChainID != event.RecipientChainID || signed.SenderAgentID != event.SenderAgentID ||
		signed.RecipientAgentID != event.RecipientAgentID || signed.ContentDigest != event.ContentDigest ||
		signed.EventKind != event.Kind || signed.PolicyEpoch != event.PolicyEpoch ||
		signed.AgreementID != event.AgreementID || signed.ContactID != event.ContactID ||
		signed.ContactRevision != event.ContactRevision || signed.AuthorizationMode != event.AuthorizationMode ||
		signed.RelationDigest != event.RelationDigest {
		return store.ErrFederatedReceiptInvalid
	}
	eventTime := time.Unix(event.Proof.Timestamp, 0).UTC()
	// Creation freshness is enforced by the recipient's authenticated REST
	// middleware. Federation delivery may be delayed by restart/offline peers;
	// exact proof uniqueness and the original message lifetime bound replay.
	if !event.EventAt.Equal(eventTime) || eventTime.After(time.Now().UTC().Add(maxTimestampSkew)) {
		return store.ErrFederatedReceiptInvalid
	}
	return nil
}

func (m *Manager) revalidateOutboundReceiptTarget(ctx context.Context, outbox *store.PipelineTransportOutbox) error {
	var target *RemotePipeTarget
	var err error
	address := outbox.TargetAgentID + "@" + outbox.RemoteChainID
	if outbox.AuthorizationMode == LinkedMessageAuthorizationMode {
		target, err = m.ResolveRemoteLinkedPipeTarget(ctx, outbox.SourceAgentID, address)
	} else {
		resolve := m.pipeTargetResolveFn
		if resolve == nil {
			resolve = m.resolveRemotePipeTargetLive
		}
		target, err = resolve(ctx, address)
	}
	if err != nil || target == nil || target.PolicyEpoch != outbox.PolicyEpoch ||
		target.AgreementID != outbox.AgreementID || target.ContactID != outbox.ContactID ||
		target.ContactRevision != outbox.ContactRevision || target.AgentID != outbox.TargetAgentID ||
		target.ChainID != outbox.RemoteChainID || target.AuthorizationMode != outbox.AuthorizationMode ||
		linkedMessageRelationDigest(target.LinkedRelation) != pipelineStoredRelationDigest(outbox.LinkedRelation) {
		return ErrFederatedPipeInvalid
	}
	return nil
}

// withAuthorizedOutboundReceipt holds the per-peer generation lease through
// the local receipt commit. Remote route freshness is checked before taking
// SQLite/Badger read locks; then local agreement/policy/contact mutations are
// excluded until action completes. This closes the revoke/re-pair gap without
// holding database locks across peer I/O.
func (m *Manager) withAuthorizedOutboundReceipt(
	ctx context.Context,
	peer *peerIdentity,
	outbox *store.PipelineTransportOutbox,
	action func() error,
) error {
	if peer == nil || outbox == nil || outbox.RemoteChainID != peer.ChainID ||
		outbox.ReceiptProtocolVersion != PipeReceiptVersion {
		return ErrFederatedPipeInvalid
	}
	deliveryCtx, releaseDelivery, err := m.beginLinkedDelivery(ctx, peer.ChainID)
	if err != nil {
		return err
	}
	defer releaseDelivery()
	if err := m.revalidateOutboundReceiptTarget(deliveryCtx, outbox); err != nil {
		return err
	}
	ss := m.syncStore()
	if ss == nil || m.badger == nil {
		return ErrFederatedPipeInvalid
	}
	policyUnlock := ss.LockSyncPolicyRead()
	defer policyUnlock()
	ownerUnlock := m.badger.LockDomainOwnershipRead()
	defer ownerUnlock()
	contactUnlock := ss.LockAgentContactRead()
	defer contactUnlock()
	if _, err := m.currentRequestAgreementBound(ctx, peer); err != nil {
		return ErrFederatedPipeInvalid
	}
	if action == nil {
		return nil
	}
	return action()
}

func pipelineStoredRelationDigest(encoded []byte) string {
	if len(encoded) == 0 {
		return ""
	}
	var relation LinkedMessageRelation
	if json.Unmarshal(encoded, &relation) != nil {
		return ""
	}
	return linkedMessageRelationDigest(&relation)
}

func (m *Manager) handlePipeReceiptV2(w http.ResponseWriter, r *http.Request) {
	peer := peerFromCtx(r.Context())
	ss := m.syncStore()
	if peer == nil || peer.Agreement == nil || ss == nil || m.postV26ForNextTx == nil || !m.postV26ForNextTx() {
		httpError(w, http.StatusNotFound, "federated pipeline receipts are unavailable")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxPipeReceiptV2BodyBytes+1))
	var event PipeReceiptEvent
	if err != nil || len(body) > maxPipeReceiptV2BodyBytes || decodeStrictPipeJSON(body, &event) != nil ||
		event.SenderChainID != m.localChainID || event.RecipientChainID != peer.ChainID ||
		validatePipeReceiptProof(&event) != nil {
		httpError(w, http.StatusBadRequest, "invalid federated pipeline receipt")
		return
	}
	outbox, err := ss.GetPipelineTransport(r.Context(), event.MessageID)
	if err != nil || outbox.EventKind != "send" {
		httpError(w, http.StatusConflict, "federated pipeline receipt binding changed")
		return
	}
	if outbox.ReceiptProtocolVersion != PipeReceiptVersion {
		httpError(w, http.StatusConflict, "federated pipeline receipt-v2 was not negotiated for this send")
		return
	}
	if outbox.RemoteChainID != peer.ChainID ||
		outbox.SourceAgentID != event.SenderAgentID || outbox.TargetAgentID != event.RecipientAgentID ||
		outbox.PolicyEpoch != event.PolicyEpoch || outbox.AgreementID != event.AgreementID ||
		outbox.ContactID != event.ContactID || outbox.ContactRevision != event.ContactRevision ||
		outbox.AuthorizationMode != event.AuthorizationMode ||
		pipelineStoredRelationDigest(outbox.LinkedRelation) != event.RelationDigest {
		httpError(w, http.StatusConflict, "federated pipeline receipt binding changed")
		return
	}
	msg, err := ss.GetPipeline(r.Context(), outbox.PipeID)
	if err != nil || event.EventAt.Before(msg.CreatedAt.Add(-maxTimestampSkew)) ||
		event.EventAt.After(msg.ExpiresAt.Add(pipeEventResultLifetime)) ||
		pipeReceiptContentDigest(event.MessageID, m.localChainID, peer.ChainID, msg) != event.ContentDigest {
		httpError(w, http.StatusConflict, "federated pipeline receipt content changed")
		return
	}
	stored := receiptStoreEvent(&event, outbox.PipeID)
	replay, err := ss.CheckFederatedReceiptEventReplay(r.Context(), stored)
	if err != nil {
		httpError(w, http.StatusConflict, "federated pipeline receipt conflicts with accepted evidence")
		return
	}
	if replay {
		writeJSON(w, http.StatusOK, &PipeReceiptResponse{Status: "duplicate"})
		return
	}
	var replayed bool
	err = m.withAuthorizedOutboundReceipt(r.Context(), peer, outbox, func() error {
		var applyErr error
		replayed, applyErr = ss.ApplyFederatedReceiptEvent(r.Context(), stored)
		return applyErr
	})
	if err != nil {
		if errors.Is(err, store.ErrFederatedReceiptConflict) {
			httpError(w, http.StatusConflict, "federated pipeline receipt conflicts with accepted evidence")
		} else if errors.Is(err, ErrFederatedPipeInvalid) || errors.Is(err, ErrFederatedPipeSuspended) {
			httpError(w, http.StatusConflict, "federated pipeline receipt generation is no longer active")
		} else {
			httpError(w, http.StatusBadRequest, "invalid federated pipeline receipt")
		}
		return
	}
	status := "accepted"
	if replayed {
		status = "duplicate"
	}
	writeJSON(w, http.StatusOK, &PipeReceiptResponse{Status: status})
}

func (m *Manager) PushPipeReceiptV2(ctx context.Context, remoteChainID string, event *PipeReceiptEvent) (*PipeReceiptResponse, error) {
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, err
	}
	body, status, err := m.doPeerRequest(ctx, agreement, http.MethodPost, "/fed/v2/pipe/receipt", event)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, &pipeEventHTTPError{Status: status, Body: truncate(body, 200)}
	}
	var out PipeReceiptResponse
	if json.Unmarshal(body, &out) != nil || (out.Status != "accepted" && out.Status != "duplicate") {
		return nil, fmt.Errorf("peer returned invalid receipt-v2 response")
	}
	return &out, nil
}
