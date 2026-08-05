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
	Version                int                      `json:"version"`
	EventID                string                   `json:"event_id"`
	Kind                   string                   `json:"kind"`
	OriginEventID          string                   `json:"origin_event_id,omitempty"`
	SourcePipeID           string                   `json:"source_pipe_id,omitempty"`
	SourceChainID          string                   `json:"source_chain_id"`
	DestinationChainID     string                   `json:"destination_chain_id"`
	SourceAgentID          string                   `json:"source_agent_id"`
	TargetAgentID          string                   `json:"target_agent_id"`
	Intent                 string                   `json:"intent,omitempty"`
	Payload                string                   `json:"payload,omitempty"`
	Result                 string                   `json:"result,omitempty"`
	CreatedAt              time.Time                `json:"created_at"`
	ExpiresAt              time.Time                `json:"expires_at"`
	PolicyEpoch            string                   `json:"policy_epoch"`
	AgreementID            string                   `json:"agreement_id"`
	ContactID              string                   `json:"contact_id"`
	ContactRevision        string                   `json:"contact_revision"`
	AuthorizationMode      string                   `json:"authorization_mode,omitempty"`
	LinkedRelation         *LinkedMessageRelation   `json:"linked_relation,omitempty"`
	ReceiptProtocolVersion int                      `json:"receipt_protocol_version,omitempty"`
	ReceiptContentDigest   string                   `json:"receipt_content_digest,omitempty"`
	Proof                  store.PipelineAgentProof `json:"proof"`
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
	// IdempotencyKey is sender-local replay control. It is intentionally not
	// copied into the federated event, but current message clients include it
	// in the exact signed /v1/pipe/send request that the peer verifies.
	IdempotencyKey string `json:"idempotency_key,omitempty"`
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
	if minutes == 0 {
		return store.CanonicalMessageLifetime
	}
	if minutes < 0 {
		minutes = 1440
	}
	if minutes > 1440 {
		minutes = 1440
	}
	return time.Duration(minutes) * time.Minute
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
	ss := m.syncStore()
	if ss == nil {
		return nil, ErrFederatedPipeInvalid
	}
	// Inbound delivery authorizes one signed target, never an administrative
	// roster. Re-derive that exact target only so an authenticated sender cannot
	// turn a delivery attempt into a full roster × policy-domain scan while the
	// policy/ownership/contact leases are held.
	agents, err := ss.GetPipeContactAgents(ctx, []string{event.TargetAgentID})
	if err != nil {
		return nil, err
	}
	grant, err := m.buildPipeContactGrantForCandidates(ctx, boundPeer, policy, agents, []string{event.TargetAgentID}, false, nil, true)
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
// a domain-access, ownership, agreement or contact-revision change invalidates it.
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
		SourceAgentID: msg.FromAgent, TargetAgentID: msg.ToAgent,
		AuthorizationMode: msg.FederationAuthorizationMode}
	if event.AuthorizationMode == LinkedMessageAuthorizationMode {
		event.LinkedRelation, err = decodeLinkedMessageRelation(msg.FederationLinkedRelation)
		if err != nil {
			return ErrFederatedPipeInvalid
		}
		// The host freshness callback is deliberately outside all local leases.
		// Revoke/pause must never wait on a remote node.
		if err = m.preflightRemoteHostedLinkedRelation(
			ctx, peer, event, false,
		); err != nil {
			return err
		}
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
	currentAgreement, err := m.ActiveAgreement(msg.SourceChainID)
	if err != nil || !sameAgreementGeneration(agreement, currentAgreement) {
		return ErrFederatedPipeInvalid
	}
	currentPolicy, err := m.getPeerRBACPolicyForAgreement(ctx, currentAgreement)
	if err != nil || currentPolicy == nil ||
		currentPolicy.PeerAgentID != policy.PeerAgentID ||
		currentPolicy.PolicyEpoch != policy.PolicyEpoch ||
		currentPolicy.Revision != policy.Revision {
		return ErrFederatedPipeInvalid
	}
	peer.Agreement = currentAgreement
	if event.AuthorizationMode == LinkedMessageAuthorizationMode {
		if err = m.authorizeInboundLinkedPipe(ctx, peer, event); err != nil {
			return err
		}
	} else if _, err = m.authorizeInboundPipeContact(ctx, peer, event); err != nil {
		return err
	}
	if action != nil {
		if actionErr := action(); actionErr != nil {
			return actionErr
		}
		// Re-derive while both leases are still held. This catches non-owner
		// contact inputs (agent availability, agreement and acceptance) changing
		// through any path that participates in the policy gate.
		if event.AuthorizationMode == LinkedMessageAuthorizationMode {
			err = m.authorizeInboundLinkedPipe(ctx, peer, event)
		} else {
			_, err = m.authorizeInboundPipeContact(ctx, peer, event)
		}
		return err
	}
	return nil
}

func newImportedPipeID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	// This receiver-local ID is exposed by the canonical Inbox facade. Keep
	// accepting historical pipe-fed-* values, but do not leak pipeline naming in
	// newly delivered messages.
	return "msg-fed-" + hex.EncodeToString(b), nil
}

func pipeEventContentHash(event *PipeEvent) [32]byte {
	encoded, _ := json.Marshal(event)
	return sha256.Sum256(encoded)
}

// prevalidatePipeEventAgentProof rejects unauthenticated or shape-mismatched
// work before any remote linked-relation callback. The durable admission path
// repeats these checks after authorization; this early pass exists to prevent
// an authenticated peer operator from using invalid agent proofs as a callback
// amplifier against the relation host.
func prevalidatePipeEventAgentProof(event *PipeEvent) error {
	if event == nil {
		return errors.New("pipeline event is nil")
	}
	switch event.Kind {
	case "send":
		if event.OriginEventID != "" || event.SourcePipeID != "" ||
			event.Payload == "" || event.Result != "" ||
			event.TargetAgentID == "" {
			return errors.New("send event shape is invalid")
		}
		method, path, body, err := verifyPipelineAgentProof(event.Proof)
		if err != nil || method != http.MethodPost || path != "/v1/pipe/send" {
			return fmt.Errorf(
				"send proof does not authorize the pipe endpoint: %w", err,
			)
		}
		var signed signedPipeSendRequest
		if err := decodeStrictPipeJSON(body, &signed); err != nil {
			return fmt.Errorf("decode signed pipe send: %w", err)
		}
		if signed.ToProvider != "" ||
			signed.ToAgent != event.TargetAgentID ||
			signed.SourceChainID != event.SourceChainID ||
			signed.DestinationChainID != event.DestinationChainID ||
			signed.Payload != event.Payload || signed.Intent != event.Intent {
			return errors.New("signed send request does not match the pipeline event")
		}
		created := time.Unix(event.Proof.Timestamp, 0).UTC()
		expires := created.Add(normalizedPipeTTL(signed.TTLMinutes))
		now := time.Now().UTC()
		if !event.CreatedAt.Equal(created) || !event.ExpiresAt.Equal(expires) ||
			now.After(expires) || created.After(now.Add(maxTimestampSkew)) {
			return errors.New("signed pipeline lifetime is invalid or expired")
		}
	case "result":
		if event.OriginEventID == "" || event.SourcePipeID == "" ||
			event.Result == "" || event.Payload != "" || event.Intent != "" ||
			len(event.SourcePipeID) > 200 ||
			strings.ContainsAny(event.SourcePipeID, "/?#") {
			return errors.New("result event shape is invalid")
		}
		method, path, body, err := verifyPipelineAgentProof(event.Proof)
		if err != nil || method != http.MethodPut ||
			path != "/v1/pipe/"+event.SourcePipeID+"/result" {
			return fmt.Errorf(
				"result proof does not authorize the pipe endpoint: %w", err,
			)
		}
		var signed signedPipeResultRequest
		if err := decodeStrictPipeJSON(body, &signed); err != nil {
			return fmt.Errorf("decode signed pipe result: %w", err)
		}
		if signed.Result != event.Result ||
			signed.SourcePipeID != event.OriginEventID ||
			signed.SourceChainID != event.SourceChainID {
			return errors.New("signed result request does not match the pipeline event")
		}
		created := time.Unix(event.Proof.Timestamp, 0).UTC()
		expires := created.Add(pipeEventResultLifetime)
		now := time.Now().UTC()
		if !event.CreatedAt.Equal(created) || !event.ExpiresAt.Equal(expires) ||
			now.After(expires) || created.After(now.Add(maxTimestampSkew)) {
			return errors.New("signed pipeline result lifetime is invalid or expired")
		}
	default:
		return fmt.Errorf("unsupported pipeline event kind %q", event.Kind)
	}
	return nil
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
	switch event.AuthorizationMode {
	case "":
		if event.LinkedRelation != nil {
			httpError(w, http.StatusBadRequest, "pipeline authorization binding is invalid")
			return
		}
	case LinkedMessageAuthorizationMode:
		if event.LinkedRelation == nil ||
			linkedMessageRelationDigest(event.LinkedRelation) == "" {
			httpError(w, http.StatusBadRequest, "pipeline authorization binding is invalid")
			return
		}
	default:
		httpError(w, http.StatusBadRequest, "pipeline authorization binding is invalid")
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
	if err := prevalidatePipeEventAgentProof(&event); err != nil {
		httpError(w, http.StatusBadRequest, "invalid pipeline agent proof")
		return
	}
	if event.AuthorizationMode == LinkedMessageAuthorizationMode {
		switch event.Kind {
		case "send", "result":
			releaseRevalidation, acquired :=
				m.acquireLinkedRevalidation(peer.ChainID)
			if !acquired {
				httpError(w, http.StatusTooManyRequests,
					"linked relation revalidation is busy")
				return
			}
			err := m.preflightRemoteHostedLinkedRelation(
				r.Context(), peer, &event, event.Kind == "result",
			)
			releaseRevalidation()
			if err != nil {
				httpError(w, http.StatusConflict, "federated pipeline authorization changed")
				return
			}
		default:
			httpError(w, http.StatusBadRequest, "invalid federated pipeline event")
			return
		}
	}

	unlock := ss.LockSyncPolicyRead()
	locksHeld := true
	var ownerUnlock, contactUnlock func()
	defer func() {
		if locksHeld {
			if contactUnlock != nil {
				contactUnlock()
			}
			if ownerUnlock != nil {
				ownerUnlock()
			}
			unlock()
		}
	}()
	if m.badger == nil {
		httpError(w, http.StatusNotImplemented, "consensus domain state is unavailable")
		return
	}
	ownerUnlock = m.badger.LockDomainOwnershipRead()
	contactUnlock = ss.LockAgentContactRead()
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
	// The durable admission is complete. Release policy/ownership/contact read
	// leases before calling an optional embedding hook; wake-up transport must
	// never participate in federation authorization lock ordering.
	contactUnlock()
	ownerUnlock()
	unlock()
	locksHeld = false
	if event.Kind == "send" && !duplicate {
		m.notifyAdmittedMessage(event.TargetAgentID, AgentMessageNotification{
			MessageID: localPipeID, FromAgent: event.SourceAgentID, CreatedAt: event.CreatedAt,
		})
	}
	status := "accepted"
	if duplicate {
		status = "duplicate"
	}
	writeJSON(w, http.StatusOK, &PipeEventResponse{Status: status})
}

func (m *Manager) notifyAdmittedMessage(targetAgentID string, notification AgentMessageNotification) {
	if m == nil || m.messageNotifier == nil || targetAgentID == "" {
		return
	}
	// Best effort and panic-safe: SSE backpressure or an embedding bug cannot
	// roll back or reinterpret the already durable federated admission.
	func() {
		defer func() { _ = recover() }()
		m.messageNotifier(targetAgentID, notification)
	}()
}

func (m *Manager) admitPipeSend(ctx context.Context, ss *store.SQLiteStore, peer *peerIdentity, event *PipeEvent) (string, bool, error) {
	if event.OriginEventID != "" || event.SourcePipeID != "" || event.Payload == "" || event.Result != "" ||
		event.TargetAgentID == "" {
		return "", false, fmt.Errorf("send event shape is invalid")
	}
	var contact *PipeContact
	var err error
	if event.AuthorizationMode == LinkedMessageAuthorizationMode {
		err = m.authorizeInboundLinkedPipe(ctx, peer, event)
	} else {
		contact, err = m.authorizeInboundPipeContact(ctx, peer, event)
	}
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
	targetMatches := signed.ToProvider == "" &&
		signed.ToAgent == event.TargetAgentID &&
		signed.SourceChainID == event.SourceChainID &&
		signed.DestinationChainID == event.DestinationChainID
	if contact != nil {
		targetMatches = targetMatches && contact.AgentID == event.TargetAgentID
	}
	if signed.Payload != event.Payload || signed.Intent != event.Intent || !targetMatches {
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
		FederationAuthorizationMode:       event.AuthorizationMode,
		FederationReceiptProtocolVersion:  event.ReceiptProtocolVersion,
		FederationReceiptContentDigest:    event.ReceiptContentDigest,
		FederationReceiptRecipientChainID: event.DestinationChainID,
	}
	if event.LinkedRelation != nil {
		msg.FederationLinkedRelation, err = json.Marshal(event.LinkedRelation)
		if err != nil {
			return "", false, ErrFederatedPipeInvalid
		}
	}
	switch event.ReceiptProtocolVersion {
	case 0:
		if event.ReceiptContentDigest != "" {
			return "", false, fmt.Errorf("legacy send cannot carry receipt-v2 evidence")
		}
	case PipeReceiptVersion:
		if m.postV26ForNextTx == nil || !m.postV26ForNextTx() ||
			event.ReceiptContentDigest != pipeReceiptContentDigest(
				event.EventID, event.SourceChainID, event.DestinationChainID, msg,
			) {
			return "", false, fmt.Errorf("receipt-v2 negotiation or content binding is invalid")
		}
	default:
		return "", false, fmt.Errorf("unsupported receipt protocol version")
	}
	proofHash := PipelineProofHash(event.SourceChainID, event.Kind, event.Proof)
	contentHash := pipeEventContentHash(event)
	dedup := &store.PipelineTransportDedup{
		RemoteChainID: event.SourceChainID, PolicyEpoch: event.PolicyEpoch, AgreementID: event.AgreementID,
		ContactID: event.ContactID, ContactRevision: event.ContactRevision,
		AuthorizationMode:    event.AuthorizationMode,
		LinkedRelationDigest: linkedMessageRelationDigest(event.LinkedRelation),
		SourceAgentID:        event.SourceAgentID, TargetAgentID: event.TargetAgentID,
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
	if event.AuthorizationMode == LinkedMessageAuthorizationMode {
		if err := m.authorizeInboundLinkedResult(ctx, peer, event); err != nil {
			return "", false, err
		}
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
		sendEvent.ContactID != event.ContactID || sendEvent.ContactRevision != event.ContactRevision ||
		sendEvent.AuthorizationMode != event.AuthorizationMode ||
		!bytes.Equal(sendEvent.LinkedRelation, mustMarshalLinkedMessageRelation(event.LinkedRelation)) {
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
		AuthorizationMode:    event.AuthorizationMode,
		LinkedRelationDigest: linkedMessageRelationDigest(event.LinkedRelation),
		SourceAgentID:        event.SourceAgentID, TargetAgentID: event.TargetAgentID,
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
