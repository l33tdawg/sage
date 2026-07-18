package federation

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
)

const (
	PipeEventVersion        = 1
	pipeEventResultLifetime = 24 * time.Hour
	maxPipeProofBytes       = 1 << 20
)

var (
	ErrFederatedPipeSuspended = errors.New("federated pipeline delivery is temporarily suspended")
	ErrFederatedPipeInvalid   = errors.New("federated pipeline authorization is no longer current")
)

// PipeEvent is the private, authenticated peer envelope for the existing
// agent pipeline. It is transport metadata, not a second inbox protocol.
// EventID is deterministically derived from the real agent's signed request;
// OriginEventID links a result to the original send proof without exposing or
// trusting either node's private local pipe id.
type PipeEvent struct {
	Version            int                      `json:"version"`
	EventID            string                   `json:"event_id"`
	Kind               string                   `json:"kind"`
	OriginEventID      string                   `json:"origin_event_id,omitempty"`
	SourcePipeID       string                   `json:"source_pipe_id,omitempty"`
	SourceChainID      string                   `json:"source_chain_id"`
	DestinationChainID string                   `json:"destination_chain_id"`
	SourceAgentID      string                   `json:"source_agent_id"`
	TargetAgentID      string                   `json:"target_agent_id"`
	Intent             string                   `json:"intent,omitempty"`
	Payload            string                   `json:"payload,omitempty"`
	Result             string                   `json:"result,omitempty"`
	CreatedAt          time.Time                `json:"created_at"`
	ExpiresAt          time.Time                `json:"expires_at"`
	PolicyEpoch        string                   `json:"policy_epoch"`
	AgreementID        string                   `json:"agreement_id"`
	ContactID          string                   `json:"contact_id"`
	ContactRevision    string                   `json:"contact_revision"`
	Proof              store.PipelineAgentProof `json:"proof"`
}

type PipeEventResponse struct {
	Status string `json:"status"`
}

type pipeEventHTTPError struct {
	Status int
	Body   string
}

func (e *pipeEventHTTPError) Error() string {
	return fmt.Sprintf("peer pipeline endpoint returned %d: %s", e.Status, e.Body)
}

type signedPipeSendRequest struct {
	ToAgent            string `json:"to_agent"`
	ToProvider         string `json:"to_provider"`
	SourceChainID      string `json:"source_chain_id"`
	DestinationChainID string `json:"destination_chain_id"`
	Intent             string `json:"intent"`
	Payload            string `json:"payload"`
	TTLMinutes         int    `json:"ttl_minutes"`
}

type signedPipeResultRequest struct {
	Result        string `json:"result"`
	SourcePipeID  string `json:"source_pipe_id"`
	SourceChainID string `json:"source_chain_id"`
}

// PipelineProofHash is the stable replay identity for an already-verified
// local agent request. The source chain and event kind domain-separate the same
// agent key used on different SAGE nodes and different pipeline transitions.
func PipelineProofHash(sourceChainID, eventKind string, proof store.PipelineAgentProof) [32]byte {
	input := struct {
		Version          int    `json:"version"`
		SourceChainID    string `json:"source_chain_id"`
		EventKind        string `json:"event_kind"`
		AgentID          string `json:"agent_id"`
		Signature        []byte `json:"signature"`
		Timestamp        int64  `json:"timestamp"`
		Nonce            []byte `json:"nonce"`
		CanonicalRequest []byte `json:"canonical_request"`
	}{PipeEventVersion, sourceChainID, eventKind, proof.AgentID, proof.Signature,
		proof.Timestamp, proof.Nonce, proof.CanonicalRequest}
	encoded, _ := json.Marshal(input)
	return sha256.Sum256(encoded)
}

func PipelineProofEventID(sourceChainID, eventKind string, proof store.PipelineAgentProof) string {
	sum := PipelineProofHash(sourceChainID, eventKind, proof)
	return "pipe-event-" + hex.EncodeToString(sum[:])
}

func isPipeDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func verifyPipelineAgentProof(proof store.PipelineAgentProof) (method, path string, body []byte, err error) {
	if len(proof.CanonicalRequest) == 0 || len(proof.CanonicalRequest) > maxPipeProofBytes ||
		len(proof.Nonce) < 8 || len(proof.Nonce) > 64 || proof.Timestamp <= 0 {
		return "", "", nil, fmt.Errorf("federated pipeline agent proof is malformed")
	}
	pub, err := auth.AgentIDToPublicKey(proof.AgentID)
	if err != nil {
		return "", "", nil, fmt.Errorf("federated pipeline agent id: %w", err)
	}
	if auth.PublicKeyToAgentID(pub) != proof.AgentID {
		return "", "", nil, fmt.Errorf("federated pipeline agent id is not canonical lowercase hex")
	}
	lineEnd := bytes.IndexByte(proof.CanonicalRequest, '\n')
	if lineEnd <= 0 {
		return "", "", nil, fmt.Errorf("federated pipeline canonical request is malformed")
	}
	requestLine := string(proof.CanonicalRequest[:lineEnd])
	method, path, ok := strings.Cut(requestLine, " ")
	if !ok || method == "" || path == "" || strings.Contains(path, " ") || strings.Contains(path, "?") {
		return "", "", nil, fmt.Errorf("federated pipeline canonical request line is invalid")
	}
	body = append([]byte(nil), proof.CanonicalRequest[lineEnd+1:]...)
	if !auth.VerifyRequestWithNonce(pub, method, path, body, proof.Timestamp, proof.Nonce, proof.Signature) {
		return "", "", nil, fmt.Errorf("federated pipeline agent signature verification failed")
	}
	return method, path, body, nil
}

func decodeStrictPipeJSON(body []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}

func normalizedPipeTTL(minutes int) time.Duration {
	if minutes <= 0 {
		minutes = 60
	}
	if minutes > 1440 {
		minutes = 1440
	}
	return time.Duration(minutes) * time.Minute
}

func signedTargetMatchesContact(req signedPipeSendRequest, event *PipeEvent, contact PipeContact) bool {
	return req.ToProvider == "" && req.ToAgent == event.TargetAgentID &&
		req.SourceChainID == event.SourceChainID &&
		req.DestinationChainID == event.DestinationChainID && contact.AgentID == event.TargetAgentID
}

func (m *Manager) authorizeInboundPipeContact(ctx context.Context, peer *peerIdentity, event *PipeEvent) (*PipeContact, error) {
	agreement, err := m.currentRequestAgreementBound(ctx, peer)
	if err != nil {
		return nil, err
	}
	policy, err := m.getPeerRBACPolicyForAgreement(ctx, agreement)
	if err != nil {
		return nil, err
	}
	if policy == nil || policy.PeerAgentID != peer.AgentID || policy.PolicyEpoch != event.PolicyEpoch {
		return nil, ErrFederatedPipeInvalid
	}
	boundPeer := &peerIdentity{ChainID: peer.ChainID, AgentID: peer.AgentID, Agreement: agreement}
	grant, err := m.buildPipeContactGrant(ctx, boundPeer, policy)
	if err != nil {
		return nil, err
	}
	if grant == nil || grant.AgreementID != event.AgreementID {
		return nil, ErrFederatedPipeInvalid
	}
	for i := range grant.Contacts {
		contact := &grant.Contacts[i]
		if contact.AgentID != event.TargetAgentID || contact.ContactID != event.ContactID {
			continue
		}
		if grant.Paused || !contact.Available || !contact.Accepting {
			return nil, ErrFederatedPipeSuspended
		}
		// Pause, target-agent availability and this target's acceptance are
		// reversible states. Check them first so an event caught in that race can
		// retry unchanged; then compare only this exact contact's authorization
		// revision. Unrelated contacts must not invalidate exact-address work.
		if pipeContactAuthorizationRevision(grant, contact) != event.ContactRevision {
			return nil, ErrFederatedPipeInvalid
		}
		return contact, nil
	}
	return nil, ErrFederatedPipeInvalid
}

// AuthorizeImportedPipe revalidates an already admitted foreign work item at
// disclosure, claim and completion. Pause/offline/acceptance-off suspends it;
// an ownership, domain, agreement or contact-revision change invalidates it.
func (m *Manager) AuthorizeImportedPipe(ctx context.Context, msg *store.PipelineMessage) error {
	return m.WithAuthorizedImportedPipe(ctx, msg, nil)
}

// WithAuthorizedImportedPipe holds both the peer-policy and consensus owner
// read leases through one local state transition or bounded result delivery.
// A domain replacement, pause, revoke, or owner transfer therefore either wins
// before the check or waits until the authorized side effect is complete.
func (m *Manager) WithAuthorizedImportedPipe(ctx context.Context, msg *store.PipelineMessage, action func() error) error {
	if msg == nil || msg.SourceChainID == "" || msg.SourcePipeID == "" || msg.DestinationChainID != "" {
		return ErrFederatedPipeInvalid
	}
	ss := m.syncStore()
	if ss == nil {
		return ErrFederatedPipeInvalid
	}
	unlock := ss.LockSyncPolicyRead()
	defer unlock()
	if m.badger == nil {
		return ErrFederatedPipeInvalid
	}
	ownerUnlock := m.badger.LockDomainOwnershipRead()
	defer ownerUnlock()
	contactUnlock := ss.LockAgentContactRead()
	defer contactUnlock()
	agreement, err := m.ActiveAgreement(msg.SourceChainID)
	if err != nil {
		return ErrFederatedPipeInvalid
	}
	policy, err := m.getPeerRBACPolicyForAgreement(ctx, agreement)
	if err != nil || policy == nil {
		return ErrFederatedPipeInvalid
	}
	peer := &peerIdentity{ChainID: msg.SourceChainID, AgentID: policy.PeerAgentID, Agreement: agreement}
	event := &PipeEvent{PolicyEpoch: msg.FederationPolicyEpoch, AgreementID: msg.FederationAgreementID,
		ContactID: msg.FederationContactID, ContactRevision: msg.FederationContactRevision,
		TargetAgentID: msg.ToAgent}
	if _, err = m.authorizeInboundPipeContact(ctx, peer, event); err != nil {
		return err
	}
	if action != nil {
		if actionErr := action(); actionErr != nil {
			return actionErr
		}
		// Re-derive while both leases are still held. This catches non-owner
		// contact inputs (agent availability, agreement and acceptance) changing
		// through any path that participates in the policy gate.
		_, err = m.authorizeInboundPipeContact(ctx, peer, event)
		return err
	}
	return nil
}

func newImportedPipeID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "pipe-fed-" + hex.EncodeToString(b), nil
}

func pipeEventContentHash(event *PipeEvent) [32]byte {
	encoded, _ := json.Marshal(event)
	return sha256.Sum256(encoded)
}

func (m *Manager) handlePipeEvent(w http.ResponseWriter, r *http.Request) {
	peer := peerFromCtx(r.Context())
	if peer == nil || peer.Agreement == nil {
		httpError(w, http.StatusForbidden, "unauthenticated")
		return
	}
	ss := m.syncStore()
	if ss == nil {
		httpError(w, http.StatusNotImplemented, "federated pipeline storage is unavailable")
		return
	}
	var event PipeEvent
	body, readErr := io.ReadAll(io.LimitReader(r.Body, maxFedBodyBytes+1))
	if readErr != nil || len(body) > maxFedBodyBytes || decodeStrictPipeJSON(body, &event) != nil {
		httpError(w, http.StatusBadRequest, "invalid pipeline event")
		return
	}
	if event.Version != PipeEventVersion || event.SourceChainID != peer.ChainID ||
		event.DestinationChainID != m.localChainID || event.SourceAgentID != event.Proof.AgentID ||
		event.EventID == "" || event.PolicyEpoch == "" || len(event.PolicyEpoch) > 256 ||
		!isPipeDigest(event.AgreementID) || !isPipeDigest(event.ContactID) ||
		!isPipeDigest(event.ContactRevision) || event.CreatedAt.IsZero() || event.ExpiresAt.IsZero() ||
		ValidateChainID(event.SourceChainID) != nil || ValidateChainID(event.DestinationChainID) != nil {
		httpError(w, http.StatusBadRequest, "pipeline event binding is incomplete")
		return
	}
	if event.EventID != PipelineProofEventID(event.SourceChainID, event.Kind, event.Proof) {
		httpError(w, http.StatusConflict, "pipeline event id does not match the agent proof")
		return
	}
	if len(event.Intent) > store.MaxPipeIntentBytes || len(event.Payload) > store.MaxPipeContentBytes ||
		len(event.Result) > store.MaxPipeContentBytes {
		httpError(w, http.StatusRequestEntityTooLarge, "pipeline content is too large")
		return
	}

	unlock := ss.LockSyncPolicyRead()
	defer unlock()
	if m.badger == nil {
		httpError(w, http.StatusNotImplemented, "consensus domain state is unavailable")
		return
	}
	ownerUnlock := m.badger.LockDomainOwnershipRead()
	defer ownerUnlock()
	contactUnlock := ss.LockAgentContactRead()
	defer contactUnlock()
	if _, err := m.currentRequestAgreementBound(r.Context(), peer); err != nil {
		httpError(w, http.StatusForbidden, "federation agreement is no longer active for this operator")
		return
	}

	var localPipeID string
	var duplicate bool
	var err error
	switch event.Kind {
	case "send":
		localPipeID, duplicate, err = m.admitPipeSend(r.Context(), ss, peer, &event)
	case "result":
		localPipeID, duplicate, err = m.applyPipeResult(r.Context(), ss, peer, &event)
	default:
		err = fmt.Errorf("unsupported pipeline event kind %q", event.Kind)
	}
	if err != nil {
		switch {
		case errors.Is(err, ErrFederatedPipeSuspended):
			// 423 is deliberately retryable at the sender. A pause, unavailable
			// agent, or acceptance-off race after live preflight must not turn the
			// durable request into a permanent failure; Resume/acceptance-on may
			// make the exact unchanged event admissible again.
			httpError(w, http.StatusLocked, "federated agent work requests are temporarily suspended")
		case errors.Is(err, ErrFederatedPipeInvalid):
			httpError(w, http.StatusConflict, "federated pipeline authorization changed")
		case errors.Is(err, store.ErrPipelineTransportEquivocation), errors.Is(err, store.ErrPipelineTransportReplay):
			httpError(w, http.StatusConflict, "federated pipeline replay conflict")
		case errors.Is(err, store.ErrPipeQuotaGlobal), errors.Is(err, store.ErrPipeQuotaPerAgent), errors.Is(err, store.ErrPipeQuotaPerPeer):
			httpError(w, http.StatusTooManyRequests, "federated pipeline quota reached")
		case errors.Is(err, store.ErrPipeContentUnavailable):
			httpError(w, http.StatusServiceUnavailable, "pipeline vault is locked")
		case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
			httpError(w, http.StatusServiceUnavailable, "pipeline admission interrupted")
		default:
			m.logger.Warn().Err(err).Str("peer", peer.ChainID).Str("kind", event.Kind).Msg("federated pipeline event rejected")
			httpError(w, http.StatusBadRequest, "invalid federated pipeline event")
		}
		return
	}
	status := "accepted"
	if duplicate {
		status = "duplicate"
	}
	_ = localPipeID // node-local id is deliberately never disclosed to the peer
	writeJSON(w, http.StatusOK, &PipeEventResponse{Status: status})
}

func (m *Manager) admitPipeSend(ctx context.Context, ss *store.SQLiteStore, peer *peerIdentity, event *PipeEvent) (string, bool, error) {
	if event.OriginEventID != "" || event.SourcePipeID != "" || event.Payload == "" || event.Result != "" ||
		event.TargetAgentID == "" {
		return "", false, fmt.Errorf("send event shape is invalid")
	}
	contact, err := m.authorizeInboundPipeContact(ctx, peer, event)
	if err != nil {
		return "", false, err
	}
	method, path, body, err := verifyPipelineAgentProof(event.Proof)
	if err != nil || method != http.MethodPost || path != "/v1/pipe/send" {
		return "", false, fmt.Errorf("send proof does not authorize the pipe endpoint: %w", err)
	}
	var signed signedPipeSendRequest
	if decodeErr := decodeStrictPipeJSON(body, &signed); decodeErr != nil {
		return "", false, fmt.Errorf("decode signed pipe send: %w", decodeErr)
	}
	if signed.Payload != event.Payload || signed.Intent != event.Intent || !signedTargetMatchesContact(signed, event, *contact) {
		return "", false, fmt.Errorf("signed send request does not match the pipeline event")
	}
	created := time.Unix(event.Proof.Timestamp, 0).UTC()
	expires := created.Add(normalizedPipeTTL(signed.TTLMinutes))
	now := time.Now().UTC()
	if !event.CreatedAt.Equal(created) || !event.ExpiresAt.Equal(expires) || now.After(expires) || created.After(now.Add(maxTimestampSkew)) {
		return "", false, fmt.Errorf("signed pipeline lifetime is invalid or expired")
	}
	localID, err := newImportedPipeID()
	if err != nil {
		return "", false, err
	}
	msg := &store.PipelineMessage{
		PipeID: localID, FromAgent: event.SourceAgentID, ToAgent: event.TargetAgentID,
		Intent: event.Intent, Payload: event.Payload, Status: "pending", CreatedAt: created, ExpiresAt: expires,
		SourceChainID: event.SourceChainID, SourcePipeID: event.EventID,
		FederationPolicyEpoch: event.PolicyEpoch, FederationAgreementID: event.AgreementID,
		FederationContactID: event.ContactID, FederationContactRevision: event.ContactRevision,
	}
	proofHash := PipelineProofHash(event.SourceChainID, event.Kind, event.Proof)
	contentHash := pipeEventContentHash(event)
	dedup := &store.PipelineTransportDedup{
		RemoteChainID: event.SourceChainID, PolicyEpoch: event.PolicyEpoch, AgreementID: event.AgreementID,
		ContactID: event.ContactID, ContactRevision: event.ContactRevision,
		SourceAgentID: event.SourceAgentID, TargetAgentID: event.TargetAgentID,
		EventKind: event.Kind, RemotePipeID: event.EventID, ContentHash: contentHash[:], ProofHash: proofHash[:],
		LocalPipeID: localID, Outcome: "accepted", ExpiresAt: expires.Add(maxTimestampSkew),
	}
	return ss.AdmitFederatedPipeline(ctx, msg, dedup)
}

func (m *Manager) applyPipeResult(ctx context.Context, ss *store.SQLiteStore, peer *peerIdentity, event *PipeEvent) (string, bool, error) {
	if event.OriginEventID == "" || event.SourcePipeID == "" || event.Result == "" || event.Payload != "" || event.Intent != "" {
		return "", false, fmt.Errorf("result event shape is invalid")
	}
	if len(event.SourcePipeID) > 200 || strings.ContainsAny(event.SourcePipeID, "/?#") {
		return "", false, fmt.Errorf("result source pipe id is invalid")
	}
	method, path, body, err := verifyPipelineAgentProof(event.Proof)
	if err != nil || method != http.MethodPut || path != "/v1/pipe/"+event.SourcePipeID+"/result" {
		return "", false, fmt.Errorf("result proof does not authorize the pipe endpoint: %w", err)
	}
	var signed signedPipeResultRequest
	if decodeErr := decodeStrictPipeJSON(body, &signed); decodeErr != nil {
		return "", false, fmt.Errorf("decode signed pipe result: %w", decodeErr)
	}
	if signed.Result != event.Result || signed.SourcePipeID != event.OriginEventID ||
		signed.SourceChainID != event.SourceChainID {
		return "", false, fmt.Errorf("signed result request does not match the pipeline event")
	}
	created := time.Unix(event.Proof.Timestamp, 0).UTC()
	expires := created.Add(pipeEventResultLifetime)
	now := time.Now().UTC()
	if !event.CreatedAt.Equal(created) || !event.ExpiresAt.Equal(expires) || now.After(expires) || created.After(now.Add(maxTimestampSkew)) {
		return "", false, fmt.Errorf("signed pipeline result lifetime is invalid or expired")
	}
	sendEvent, err := ss.GetPipelineTransport(ctx, event.OriginEventID)
	if err != nil {
		return "", false, ErrFederatedPipeInvalid
	}
	if sendEvent.EventKind != "send" || sendEvent.RemoteChainID != peer.ChainID ||
		sendEvent.TargetAgentID != event.SourceAgentID || sendEvent.SourceAgentID != event.TargetAgentID ||
		sendEvent.PolicyEpoch != event.PolicyEpoch || sendEvent.AgreementID != event.AgreementID ||
		sendEvent.ContactID != event.ContactID || sendEvent.ContactRevision != event.ContactRevision {
		return "", false, ErrFederatedPipeInvalid
	}
	msg, err := ss.GetPipeline(ctx, sendEvent.PipeID)
	if err != nil || msg.DestinationChainID != peer.ChainID || !created.Before(msg.ExpiresAt) {
		return "", false, ErrFederatedPipeInvalid
	}
	proofHash := PipelineProofHash(event.SourceChainID, event.Kind, event.Proof)
	contentHash := pipeEventContentHash(event)
	dedup := &store.PipelineTransportDedup{
		RemoteChainID: event.SourceChainID, PolicyEpoch: event.PolicyEpoch, AgreementID: event.AgreementID,
		ContactID: event.ContactID, ContactRevision: event.ContactRevision,
		SourceAgentID: event.SourceAgentID, TargetAgentID: event.TargetAgentID,
		EventKind: event.Kind, RemotePipeID: event.EventID, ContentHash: contentHash[:], ProofHash: proofHash[:],
		LocalPipeID: sendEvent.PipeID, Outcome: "completed", ExpiresAt: expires.Add(maxTimestampSkew),
	}
	duplicate, err := ss.ApplyFederatedPipelineResult(ctx, sendEvent.PipeID, event.Result, dedup)
	return sendEvent.PipeID, duplicate, err
}

func (m *Manager) PushPipeEvent(ctx context.Context, remoteChainID string, event *PipeEvent) (*PipeEventResponse, error) {
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, err
	}
	body, status, err := m.doPeerRequest(ctx, agreement, http.MethodPost, "/fed/v1/pipe/event", event)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, &pipeEventHTTPError{Status: status, Body: truncate(body, 200)}
	}
	var out PipeEventResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode peer pipeline response: %w", err)
	}
	if out.Status != "accepted" && out.Status != "duplicate" {
		return nil, fmt.Errorf("peer %s returned invalid pipeline status %q", remoteChainID, out.Status)
	}
	return &out, nil
}
