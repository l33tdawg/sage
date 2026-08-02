package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
)

const (
	LinkedMessageAuthorizationMode = "linked-v23"
	LinkedMessageRelationVersion   = 1
	LinkedMessageGuestToMember     = "guest_to_member"
	LinkedMessageMemberToGuest     = "member_to_guest"

	maxLinkedMessageResolveBytes            = 16 << 10
	maxLinkedMessageCandidateResponseBytes  = 2 << 20
	maxConcurrentLinkedRevalidationsPerPeer = 4
	linkedMessageResolveTimeout             = 5 * time.Second
)

// LinkedMessageRelation is a transport-only assertion made by the node that
// owns a Linked-reader row. It authorizes one direction between one exact guest
// and one exact current member of one exact local group. It contains no domain
// selector and grants no read, write, copy, or memory authority.
type LinkedMessageRelation struct {
	Version                 int                       `json:"version"`
	Direction               string                    `json:"direction"`
	HostChainID             string                    `json:"host_chain_id"`
	PeerChainID             string                    `json:"peer_chain_id"`
	SourceAgentID           string                    `json:"source_agent_id"`
	TargetAgentID           string                    `json:"target_agent_id"`
	GroupRevision           uint64                    `json:"group_revision"`
	HostAgreementDigest     string                    `json:"host_agreement_digest"`
	ReceiverConsentRevision int64                     `json:"receiver_consent_revision"`
	Guest                   store.FederatedGroupGuest `json:"guest"`
	SignerAgentID           string                    `json:"signer_agent_id"`
	Signature               []byte                    `json:"signature"`
}

type linkedMessageRelationSignedFields struct {
	Purpose                 string                    `json:"purpose"`
	Version                 int                       `json:"version"`
	Direction               string                    `json:"direction"`
	HostChainID             string                    `json:"host_chain_id"`
	PeerChainID             string                    `json:"peer_chain_id"`
	SourceAgentID           string                    `json:"source_agent_id"`
	TargetAgentID           string                    `json:"target_agent_id"`
	GroupRevision           uint64                    `json:"group_revision"`
	HostAgreementDigest     string                    `json:"host_agreement_digest"`
	ReceiverConsentRevision int64                     `json:"receiver_consent_revision"`
	Guest                   store.FederatedGroupGuest `json:"guest"`
	SignerAgentID           string                    `json:"signer_agent_id"`
}

func (r LinkedMessageRelation) signingBytes() ([]byte, error) {
	return json.Marshal(linkedMessageRelationSignedFields{
		Purpose: "sage-linked-message-relation-v23", Version: r.Version,
		Direction: r.Direction, HostChainID: r.HostChainID,
		PeerChainID: r.PeerChainID, SourceAgentID: r.SourceAgentID,
		TargetAgentID: r.TargetAgentID, GroupRevision: r.GroupRevision,
		HostAgreementDigest:     r.HostAgreementDigest,
		ReceiverConsentRevision: r.ReceiverConsentRevision,
		Guest:                   r.Guest, SignerAgentID: r.SignerAgentID,
	})
}

func (r *LinkedMessageRelation) sign(key ed25519.PrivateKey) error {
	if r == nil || len(key) != ed25519.PrivateKeySize {
		return errors.New("linked-message relation signing key is unavailable")
	}
	pub := key.Public().(ed25519.PublicKey)
	r.SignerAgentID = hex.EncodeToString(pub)
	body, err := r.signingBytes()
	if err != nil {
		return err
	}
	r.Signature = ed25519.Sign(key, body)
	return nil
}

func validateLinkedMessageRelation(
	r *LinkedMessageRelation,
	expectedSigner string,
) error {
	if r == nil || r.Version != LinkedMessageRelationVersion ||
		(r.Direction != LinkedMessageGuestToMember &&
			r.Direction != LinkedMessageMemberToGuest) ||
		r.HostChainID == "" || r.PeerChainID == "" ||
		r.HostChainID == r.PeerChainID || r.GroupRevision == 0 ||
		r.ReceiverConsentRevision <= 0 ||
		r.SourceAgentID == r.TargetAgentID ||
		r.SignerAgentID != expectedSigner ||
		r.Guest.GroupID == "" || r.Guest.GroupID != strings.TrimSpace(r.Guest.GroupID) ||
		r.Guest.RemoteChainID != r.PeerChainID ||
		r.Guest.AgreementBindingDigest != r.HostAgreementDigest ||
		!isPipeDigest(r.HostAgreementDigest) {
		return ErrFederatedPipeInvalid
	}
	for _, agentID := range []string{
		r.SourceAgentID, r.TargetAgentID, r.Guest.RemoteAgentID, r.SignerAgentID,
	} {
		if _, err := auth.AgentIDToPublicKey(agentID); err != nil ||
			agentID != strings.ToLower(agentID) {
			return ErrFederatedPipeInvalid
		}
	}
	switch r.Direction {
	case LinkedMessageGuestToMember:
		if r.SourceAgentID != r.Guest.RemoteAgentID {
			return ErrFederatedPipeInvalid
		}
	case LinkedMessageMemberToGuest:
		if r.TargetAgentID != r.Guest.RemoteAgentID {
			return ErrFederatedPipeInvalid
		}
	}
	if store.VerifyFederatedGroupGuest(r.Guest) != nil ||
		len(r.Signature) != ed25519.SignatureSize {
		return ErrFederatedPipeInvalid
	}
	pub, err := auth.AgentIDToPublicKey(r.SignerAgentID)
	if err != nil {
		return ErrFederatedPipeInvalid
	}
	body, err := r.signingBytes()
	if err != nil || !ed25519.Verify(pub, body, r.Signature) {
		return ErrFederatedPipeInvalid
	}
	return nil
}

func linkedMessageContactID(r *LinkedMessageRelation) string {
	input := struct {
		Purpose       string `json:"purpose"`
		HostChainID   string `json:"host_chain_id"`
		PeerChainID   string `json:"peer_chain_id"`
		Direction     string `json:"direction"`
		GroupID       string `json:"group_id"`
		SourceAgentID string `json:"source_agent_id"`
		TargetAgentID string `json:"target_agent_id"`
	}{
		"sage-linked-message-contact-v23", r.HostChainID, r.PeerChainID,
		r.Direction, r.Guest.GroupID, r.SourceAgentID, r.TargetAgentID,
	}
	body, _ := json.Marshal(input)
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func linkedMessageContactRevision(r *LinkedMessageRelation) string {
	body, _ := json.Marshal(r)
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func mustMarshalLinkedMessageRelation(r *LinkedMessageRelation) []byte {
	if r == nil {
		return nil
	}
	body, _ := json.Marshal(r)
	return body
}

func linkedMessageRelationDigest(r *LinkedMessageRelation) string {
	body := mustMarshalLinkedMessageRelation(r)
	if len(body) == 0 {
		return ""
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func linkedMessageRelationEqual(a, b *LinkedMessageRelation) bool {
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return bytes.Equal(ab, bb)
}

type LinkedMessageResolveRequest struct {
	Version       int                    `json:"version"`
	Direction     string                 `json:"direction"`
	SourceAgentID string                 `json:"source_agent_id"`
	TargetAgentID string                 `json:"target_agent_id"`
	Relation      *LinkedMessageRelation `json:"relation,omitempty"`
}

type LinkedMessageResolveResponse struct {
	Version                 int               `json:"version"`
	ChainID                 string            `json:"chain_id"`
	SourceAgentID           string            `json:"source_agent_id"`
	TargetAgentID           string            `json:"target_agent_id"`
	ConsentRevision         int64             `json:"consent_revision,omitempty"`
	ReceiverAgreementDigest string            `json:"receiver_agreement_digest,omitempty"`
	PolicyEpoch             string            `json:"policy_epoch,omitempty"`
	Target                  *RemotePipeTarget `json:"target,omitempty"`
}

type LinkedMessageRevalidateRequest struct {
	Version  int                    `json:"version"`
	Relation *LinkedMessageRelation `json:"relation"`
}

type LinkedMessageRevalidateResponse struct {
	Version          int    `json:"version"`
	HostChainID      string `json:"host_chain_id"`
	PeerChainID      string `json:"peer_chain_id"`
	RelationRevision string `json:"relation_revision"`
}

type LinkedMessageConsentOfferRequest struct {
	Version       int    `json:"version"`
	SourceAgentID string `json:"source_agent_id"`
	TargetAgentID string `json:"target_agent_id"`
}

type LinkedMessageConsentOfferResponse struct {
	Version  int                    `json:"version"`
	Relation *LinkedMessageRelation `json:"relation"`
}

type LinkedMessageConsentCandidateRequest struct {
	Version      int    `json:"version"`
	GuestAgentID string `json:"guest_agent_id"`
}

type LinkedMessageConsentCandidateResponse struct {
	Version   int                      `json:"version"`
	Relations []*LinkedMessageRelation `json:"relations"`
}

func decodeLinkedMessageRelation(body []byte) (*LinkedMessageRelation, error) {
	if len(body) == 0 || len(body) > maxLinkedMessageResolveBytes {
		return nil, ErrFederatedPipeInvalid
	}
	var relation LinkedMessageRelation
	if err := json.Unmarshal(body, &relation); err != nil {
		return nil, ErrFederatedPipeInvalid
	}
	return &relation, nil
}

func (m *Manager) currentLinkedMessagePolicy(
	ctx context.Context,
	agreement *store.CrossFedRecord,
	peerAgentID string,
) (*store.PeerRBACPolicy, string, error) {
	policy, err := m.v23BindingReady(ctx, agreement, peerAgentID)
	if err != nil || policy == nil || policy.Paused {
		return nil, "", ErrFederatedPipeSuspended
	}
	digest, err := m.agreementBindingDigestV23(ctx, agreement, peerAgentID)
	if err != nil {
		return nil, "", ErrFederatedPipeInvalid
	}
	return policy, digest, nil
}

func (m *Manager) linkedMessageLocalAgentEligible(agentID string) bool {
	eligible, err := m.localFederatedGuestAgentEligible(agentID)
	if err != nil || !eligible || m.badger == nil {
		return false
	}
	enrollment, err := m.badger.GetAppV23Enrollment(agentID)
	if err != nil || enrollment == nil || !enrollment.Active ||
		enrollment.Profile == store.AppV23ProfileReadOnly {
		return false
	}
	// Linked messaging exists only in app-v23, so the deny bit is always live;
	// do not rely on an optional legacy app-v22 activation callback.
	capabilities, registered, err :=
		m.badger.GetRegisteredAgentCapabilities(agentID)
	return err == nil && registered &&
		!capabilities.Has(store.AgentCapabilityDenyFederatedPipe)
}

func appV23GroupHasMember(group *store.AppV23AccessGroup, agentID string) bool {
	if group == nil {
		return false
	}
	i := sort.SearchStrings(group.Members, agentID)
	return i < len(group.Members) && group.Members[i] == agentID
}

func sameFederatedGuest(a, b store.FederatedGroupGuest) bool {
	ab, aerr := json.Marshal(a)
	bb, berr := json.Marshal(b)
	return aerr == nil && berr == nil && bytes.Equal(ab, bb)
}

// revalidateHostedLinkedRelation proves the exact signed guest bytes still
// occupy the current host row and that the exact member remains active in the
// exact group revision. No directory/contact projection participates.
func (m *Manager) revalidateHostedLinkedRelation(
	ctx context.Context,
	peer *peerIdentity,
	agreement *store.CrossFedRecord,
	relation *LinkedMessageRelation,
) error {
	if relation == nil || relation.HostChainID != m.localChainID ||
		relation.PeerChainID != peer.ChainID ||
		validateLinkedMessageRelation(relation, hex.EncodeToString(m.agentPub)) != nil {
		return ErrFederatedPipeInvalid
	}
	_, digest, err := m.currentLinkedMessagePolicy(ctx, agreement, peer.AgentID)
	if err != nil || digest != relation.HostAgreementDigest {
		return ErrFederatedPipeInvalid
	}
	guests, err := m.federatedGuestStore.ListFederatedGroupGuests(
		ctx, peer.ChainID, relation.Guest.RemoteAgentID,
	)
	if err != nil {
		return ErrFederatedPipeInvalid
	}
	found := false
	for i := range guests {
		if sameFederatedGuest(guests[i], relation.Guest) {
			found = true
			break
		}
	}
	if !found || relation.Guest.State != store.FederatedGuestStateActive ||
		!m.federatedGuestAuthorityActive(relation.Guest) {
		return ErrFederatedPipeInvalid
	}
	group, err := m.badger.GetAppV23AccessGroup(relation.Guest.GroupID)
	if err != nil || group == nil || group.Revision != relation.GroupRevision {
		return ErrFederatedPipeInvalid
	}
	memberID := relation.TargetAgentID
	if relation.Direction == LinkedMessageMemberToGuest {
		memberID = relation.SourceAgentID
	}
	if !appV23GroupHasMember(group, memberID) ||
		!m.linkedMessageLocalAgentEligible(memberID) {
		return ErrFederatedPipeInvalid
	}
	return nil
}

func (m *Manager) receiverLinkedMessageConsent(
	ctx context.Context,
	policy *store.PeerRBACPolicy,
	remoteSourceAgentID, localTargetAgentID string,
) (*store.FederatedLinkedMessageConsent, error) {
	ss := m.syncStore()
	if ss == nil || policy == nil || policy.Paused {
		return nil, ErrFederatedPipeSuspended
	}
	consent, err := ss.GetBoundFederatedLinkedMessageConsent(
		ctx, *policy, remoteSourceAgentID, localTargetAgentID,
	)
	if err != nil {
		return nil, ErrFederatedPipeInvalid
	}
	if consent == nil || !consent.Accepting {
		return nil, ErrFederatedPipeSuspended
	}
	return consent, nil
}

func (m *Manager) buildHostedLinkedRelation(
	ctx context.Context,
	peer *peerIdentity,
	agreement *store.CrossFedRecord,
	direction, sourceAgentID, targetAgentID string,
) (*LinkedMessageRelation, *store.PeerRBACPolicy, error) {
	policy, digest, err := m.currentLinkedMessagePolicy(ctx, agreement, peer.AgentID)
	if err != nil {
		return nil, nil, err
	}
	remoteGuestID, memberID := sourceAgentID, targetAgentID
	if direction == LinkedMessageMemberToGuest {
		remoteGuestID, memberID = targetAgentID, sourceAgentID
	}
	guests, err := m.federatedGuestStore.ListFederatedGroupGuests(
		ctx, peer.ChainID, remoteGuestID,
	)
	if err != nil {
		return nil, nil, ErrFederatedPipeInvalid
	}
	sort.Slice(guests, func(i, j int) bool { return guests[i].GroupID < guests[j].GroupID })
	for _, guest := range guests {
		if guest.State != store.FederatedGuestStateActive ||
			guest.AgreementBindingDigest != digest ||
			!m.federatedGuestAuthorityActive(guest) {
			continue
		}
		group, groupErr := m.badger.GetAppV23AccessGroup(guest.GroupID)
		if groupErr != nil || group == nil ||
			!appV23GroupHasMember(group, memberID) ||
			!m.linkedMessageLocalAgentEligible(memberID) {
			continue
		}
		consentRevision := int64(1) // remote receiver replaces this below.
		if direction == LinkedMessageGuestToMember {
			consent, consentErr := m.receiverLinkedMessageConsent(
				ctx, policy, sourceAgentID, targetAgentID,
			)
			if consentErr != nil {
				continue
			}
			consentRevision = consent.Revision
		}
		relation := &LinkedMessageRelation{
			Version: LinkedMessageRelationVersion, Direction: direction,
			HostChainID: m.localChainID, PeerChainID: peer.ChainID,
			SourceAgentID: sourceAgentID, TargetAgentID: targetAgentID,
			GroupRevision: group.Revision, HostAgreementDigest: digest,
			ReceiverConsentRevision: consentRevision, Guest: guest,
		}
		if err := relation.sign(m.agentKey); err != nil {
			return nil, nil, ErrFederatedPipeInvalid
		}
		return relation, policy, nil
	}
	return nil, nil, ErrRemotePipeTargetNotFound
}

func (m *Manager) linkedTargetFromRelation(
	policy *store.PeerRBACPolicy,
	receiverAgreementDigest string,
	relation *LinkedMessageRelation,
) *RemotePipeTarget {
	return &RemotePipeTarget{
		ChainID: relation.PeerChainID, AgentID: relation.TargetAgentID,
		ContactID:       linkedMessageContactID(relation),
		ContactRevision: linkedMessageContactRevision(relation),
		PolicyEpoch:     policy.PolicyEpoch, AgreementID: receiverAgreementDigest,
		Address:           relation.TargetAgentID + "@" + relation.PeerChainID,
		AuthorizationMode: LinkedMessageAuthorizationMode,
		LinkedRelation:    relation,
	}
}

// authorizeInboundLinkedPipe is used at admission, claim, completion and
// result delivery. It repeats receiver consent and whichever half of the
// relation is consensus-local on this node.
func (m *Manager) authorizeInboundLinkedPipe(
	ctx context.Context,
	peer *peerIdentity,
	event *PipeEvent,
) error {
	if event == nil || event.AuthorizationMode != LinkedMessageAuthorizationMode ||
		event.LinkedRelation == nil {
		return ErrFederatedPipeInvalid
	}
	agreement, err := m.currentRequestAgreementBound(ctx, peer)
	if err != nil {
		return ErrFederatedPipeInvalid
	}
	policy, receiverDigest, err := m.currentLinkedMessagePolicy(ctx, agreement, peer.AgentID)
	if err != nil || policy.PolicyEpoch != event.PolicyEpoch ||
		receiverDigest != event.AgreementID {
		return ErrFederatedPipeInvalid
	}
	relation := event.LinkedRelation
	if relation.SourceAgentID != event.SourceAgentID ||
		relation.TargetAgentID != event.TargetAgentID ||
		linkedMessageContactID(relation) != event.ContactID ||
		linkedMessageContactRevision(relation) != event.ContactRevision {
		return ErrFederatedPipeInvalid
	}
	if relation.HostChainID == m.localChainID {
		if relation.Direction != LinkedMessageGuestToMember ||
			relation.PeerChainID != peer.ChainID ||
			relation.SourceAgentID == relation.TargetAgentID ||
			m.revalidateHostedLinkedRelation(ctx, peer, agreement, relation) != nil {
			return ErrFederatedPipeInvalid
		}
	} else {
		if relation.Direction != LinkedMessageMemberToGuest ||
			relation.HostChainID != peer.ChainID ||
			relation.PeerChainID != m.localChainID ||
			validateLinkedMessageRelation(relation, peer.AgentID) != nil ||
			relation.TargetAgentID != relation.Guest.RemoteAgentID ||
			!m.linkedMessageLocalAgentEligible(relation.TargetAgentID) {
			return ErrFederatedPipeInvalid
		}
	}
	consent, err := m.receiverLinkedMessageConsent(
		ctx, policy, event.SourceAgentID, event.TargetAgentID,
	)
	if err != nil {
		return err
	}
	if consent.Revision != relation.ReceiverConsentRevision {
		return ErrFederatedPipeInvalid
	}
	return nil
}

// authorizeInboundLinkedResult revalidates the original send relation without
// inventing a reverse contact. The event's agents are reversed, while the
// embedded assertion remains byte-identical to the original send.
func (m *Manager) authorizeInboundLinkedResult(
	ctx context.Context,
	peer *peerIdentity,
	event *PipeEvent,
) error {
	if event == nil || event.AuthorizationMode != LinkedMessageAuthorizationMode ||
		event.LinkedRelation == nil {
		return ErrFederatedPipeInvalid
	}
	agreement, err := m.currentRequestAgreementBound(ctx, peer)
	if err != nil {
		return ErrFederatedPipeInvalid
	}
	policy, _, err := m.currentLinkedMessagePolicy(ctx, agreement, peer.AgentID)
	if err != nil || policy.PolicyEpoch == "" {
		return ErrFederatedPipeInvalid
	}
	relation := event.LinkedRelation
	if event.SourceAgentID != relation.TargetAgentID ||
		event.TargetAgentID != relation.SourceAgentID ||
		linkedMessageContactID(relation) != event.ContactID ||
		linkedMessageContactRevision(relation) != event.ContactRevision {
		return ErrFederatedPipeInvalid
	}
	if relation.HostChainID == m.localChainID {
		if relation.Direction != LinkedMessageMemberToGuest ||
			relation.PeerChainID != peer.ChainID ||
			m.revalidateHostedLinkedRelation(
				ctx, peer, agreement, relation,
			) != nil ||
			!m.linkedMessageLocalAgentEligible(relation.SourceAgentID) {
			return ErrFederatedPipeInvalid
		}
		return nil
	}
	if relation.HostChainID != peer.ChainID ||
		relation.PeerChainID != m.localChainID ||
		relation.Direction != LinkedMessageGuestToMember ||
		validateLinkedMessageRelation(relation, peer.AgentID) != nil ||
		!m.linkedMessageLocalAgentEligible(relation.SourceAgentID) {
		return ErrFederatedPipeInvalid
	}
	return nil
}

// preflightRemoteHostedLinkedRelation performs the only network freshness
// callback. Callers must invoke it before taking local policy/ownership/contact
// leases, then repeat all local checks under those leases.
func (m *Manager) preflightRemoteHostedLinkedRelation(
	ctx context.Context,
	peer *peerIdentity,
	event *PipeEvent,
	result bool,
) error {
	if event == nil || event.AuthorizationMode != LinkedMessageAuthorizationMode ||
		event.LinkedRelation == nil ||
		event.LinkedRelation.HostChainID != peer.ChainID {
		return nil
	}
	relation := event.LinkedRelation
	if relation.PeerChainID != m.localChainID ||
		validateLinkedMessageRelation(relation, peer.AgentID) != nil {
		return ErrFederatedPipeInvalid
	}
	if result {
		if event.SourceAgentID != relation.TargetAgentID ||
			event.TargetAgentID != relation.SourceAgentID {
			return ErrFederatedPipeInvalid
		}
	} else if event.SourceAgentID != relation.SourceAgentID ||
		event.TargetAgentID != relation.TargetAgentID {
		return ErrFederatedPipeInvalid
	}
	agreement, err := m.currentRequestAgreementBound(ctx, peer)
	if err != nil {
		return ErrFederatedPipeInvalid
	}
	return m.revalidateRemoteHostedLinkedRelation(ctx, agreement, relation)
}

func linkedMessageUnavailable(w http.ResponseWriter) {
	httpError(w, http.StatusNotFound, "linked message target unavailable")
}

func (m *Manager) acquireLinkedRevalidation(
	remoteChainID string,
) (func(), bool) {
	m.linkedRevalidationMu.Lock()
	if m.linkedRevalidationSem == nil {
		m.linkedRevalidationSem = make(map[string]chan struct{})
	}
	sem := m.linkedRevalidationSem[remoteChainID]
	if sem == nil {
		sem = make(chan struct{}, maxConcurrentLinkedRevalidationsPerPeer)
		m.linkedRevalidationSem[remoteChainID] = sem
	}
	m.linkedRevalidationMu.Unlock()
	select {
	case sem <- struct{}{}:
		return func() { <-sem }, true
	default:
		return func() {}, false
	}
}

// handleLinkedMessageRevalidate is the live link-host oracle used for queued
// member->guest work. Every failure is indistinguishable to prevent exact-ID,
// group, or membership enumeration.
func (m *Manager) handleLinkedMessageRevalidate(w http.ResponseWriter, r *http.Request) {
	peer := peerFromCtx(r.Context())
	if peer == nil || peer.Agreement == nil {
		httpError(w, http.StatusForbidden, "unauthenticated")
		return
	}
	var req LinkedMessageRevalidateRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxLinkedMessageResolveBytes)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		linkedMessageUnavailable(w)
		return
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) ||
		req.Version != LinkedMessageRelationVersion || req.Relation == nil {
		linkedMessageUnavailable(w)
		return
	}
	ss := m.syncStore()
	if ss == nil || m.badger == nil {
		linkedMessageUnavailable(w)
		return
	}
	policyUnlock := ss.LockSyncPolicyRead()
	ownerUnlock := m.badger.LockDomainOwnershipRead()
	defer policyUnlock()
	defer ownerUnlock()
	agreement, err := m.currentRequestAgreementBound(r.Context(), peer)
	if err != nil ||
		m.revalidateHostedLinkedRelation(
			r.Context(), peer, agreement, req.Relation,
		) != nil {
		linkedMessageUnavailable(w)
		return
	}
	writeJSON(w, http.StatusOK, &LinkedMessageRevalidateResponse{
		Version: LinkedMessageRelationVersion, HostChainID: m.localChainID,
		PeerChainID:      peer.ChainID,
		RelationRevision: linkedMessageContactRevision(req.Relation),
	})
}

// handleLinkedMessageConsentOffer proves one exact host-side
// member->guest pair before the receiver creates default-off consent. It
// exposes no inventory and does not itself enable delivery.
func (m *Manager) handleLinkedMessageConsentOffer(w http.ResponseWriter, r *http.Request) {
	peer := peerFromCtx(r.Context())
	if peer == nil || peer.Agreement == nil {
		httpError(w, http.StatusForbidden, "unauthenticated")
		return
	}
	var req LinkedMessageConsentOfferRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxLinkedMessageResolveBytes)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		linkedMessageUnavailable(w)
		return
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) ||
		req.Version != LinkedMessageRelationVersion {
		linkedMessageUnavailable(w)
		return
	}
	ss := m.syncStore()
	if ss == nil || m.badger == nil {
		linkedMessageUnavailable(w)
		return
	}
	policyUnlock := ss.LockSyncPolicyRead()
	ownerUnlock := m.badger.LockDomainOwnershipRead()
	defer policyUnlock()
	defer ownerUnlock()
	agreement, err := m.currentRequestAgreementBound(r.Context(), peer)
	if err != nil {
		linkedMessageUnavailable(w)
		return
	}
	relation, _, err := m.buildHostedLinkedRelation(
		r.Context(), peer, agreement, LinkedMessageMemberToGuest,
		req.SourceAgentID, req.TargetAgentID,
	)
	if err != nil {
		linkedMessageUnavailable(w)
		return
	}
	writeJSON(w, http.StatusOK, &LinkedMessageConsentOfferResponse{
		Version: LinkedMessageRelationVersion, Relation: relation,
	})
}

// handleLinkedMessageConsentCandidates returns a bounded exact-ID-only offer
// projection for one guest identity on the authenticated peer. It contains no
// names, directory entries, domains, or unrelated members.
func (m *Manager) handleLinkedMessageConsentCandidates(w http.ResponseWriter, r *http.Request) {
	peer := peerFromCtx(r.Context())
	if peer == nil || peer.Agreement == nil {
		httpError(w, http.StatusForbidden, "unauthenticated")
		return
	}
	var req LinkedMessageConsentCandidateRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxLinkedMessageResolveBytes)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		linkedMessageUnavailable(w)
		return
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) ||
		req.Version != LinkedMessageRelationVersion {
		linkedMessageUnavailable(w)
		return
	}
	if _, err := auth.AgentIDToPublicKey(req.GuestAgentID); err != nil ||
		req.GuestAgentID != strings.ToLower(req.GuestAgentID) {
		linkedMessageUnavailable(w)
		return
	}
	ss := m.syncStore()
	if ss == nil || m.badger == nil {
		linkedMessageUnavailable(w)
		return
	}
	policyUnlock := ss.LockSyncPolicyRead()
	ownerUnlock := m.badger.LockDomainOwnershipRead()
	defer policyUnlock()
	defer ownerUnlock()
	agreement, err := m.currentRequestAgreementBound(r.Context(), peer)
	if err != nil {
		linkedMessageUnavailable(w)
		return
	}
	_, digest, err := m.currentLinkedMessagePolicy(
		r.Context(), agreement, peer.AgentID,
	)
	if err != nil {
		linkedMessageUnavailable(w)
		return
	}
	guests, err := m.federatedGuestStore.ListFederatedGroupGuests(
		r.Context(), peer.ChainID, req.GuestAgentID,
	)
	if err != nil {
		linkedMessageUnavailable(w)
		return
	}
	memberSet := make(map[string]struct{})
	for _, guest := range guests {
		if guest.State != store.FederatedGuestStateActive ||
			guest.AgreementBindingDigest != digest ||
			!m.federatedGuestAuthorityActive(guest) {
			continue
		}
		group, groupErr := m.badger.GetAppV23AccessGroup(guest.GroupID)
		if groupErr != nil || group == nil ||
			len(group.Members) > store.AppV23MaxGroupMembers {
			continue
		}
		for _, memberID := range group.Members {
			if m.linkedMessageLocalAgentEligible(memberID) {
				memberSet[memberID] = struct{}{}
			}
		}
	}
	memberIDs := make([]string, 0, len(memberSet))
	for memberID := range memberSet {
		memberIDs = append(memberIDs, memberID)
	}
	sort.Strings(memberIDs)
	if len(memberIDs) > MaxLinkedMessageConsentCandidates {
		linkedMessageUnavailable(w)
		return
	}
	relations := make([]*LinkedMessageRelation, 0, len(memberIDs))
	for _, memberID := range memberIDs {
		relation, _, buildErr := m.buildHostedLinkedRelation(
			r.Context(), peer, agreement, LinkedMessageMemberToGuest,
			memberID, req.GuestAgentID,
		)
		if buildErr == nil {
			relations = append(relations, relation)
		}
	}
	writeJSON(w, http.StatusOK, &LinkedMessageConsentCandidateResponse{
		Version: LinkedMessageRelationVersion, Relations: relations,
	})
}

func (m *Manager) checkRemoteHostedLinkedMessagePair(
	ctx context.Context,
	agreement *store.CrossFedRecord,
	peerAgentID, remoteSourceAgentID, localTargetAgentID string,
) bool {
	requestCtx, cancel := context.WithTimeout(ctx, linkedMessageResolveTimeout)
	defer cancel()
	body, status, err := m.doPeerRequest(
		requestCtx, agreement, http.MethodPost,
		"/fed/v1/pipe/linked/consent-offer",
		LinkedMessageConsentOfferRequest{
			Version:       LinkedMessageRelationVersion,
			SourceAgentID: remoteSourceAgentID,
			TargetAgentID: localTargetAgentID,
		},
	)
	if err != nil || status != http.StatusOK {
		return false
	}
	var response LinkedMessageConsentOfferResponse
	if json.Unmarshal(body, &response) != nil {
		return false
	}
	relation := response.Relation
	return response.Version == LinkedMessageRelationVersion &&
		relation != nil &&
		relation.Direction == LinkedMessageMemberToGuest &&
		relation.HostChainID == agreement.RemoteChainID &&
		relation.PeerChainID == m.localChainID &&
		relation.SourceAgentID == remoteSourceAgentID &&
		relation.TargetAgentID == localTargetAgentID &&
		validateLinkedMessageRelation(relation, peerAgentID) == nil &&
		m.linkedMessageLocalAgentEligible(localTargetAgentID)
}

func (m *Manager) revalidateRemoteHostedLinkedRelation(
	ctx context.Context,
	agreement *store.CrossFedRecord,
	relation *LinkedMessageRelation,
) error {
	if agreement == nil || relation == nil ||
		relation.HostChainID != agreement.RemoteChainID ||
		relation.PeerChainID != m.localChainID {
		return ErrFederatedPipeInvalid
	}
	if m.linkedRelationRevalidateFn != nil {
		return m.linkedRelationRevalidateFn(ctx, agreement, relation)
	}
	requestCtx, cancel := context.WithTimeout(ctx, linkedMessageResolveTimeout)
	defer cancel()
	body, status, err := m.doPeerRequest(
		requestCtx, agreement, http.MethodPost,
		"/fed/v1/pipe/linked/revalidate",
		LinkedMessageRevalidateRequest{
			Version: LinkedMessageRelationVersion, Relation: relation,
		},
	)
	if err != nil || status != http.StatusOK {
		return ErrFederatedPipeInvalid
	}
	var response LinkedMessageRevalidateResponse
	if json.Unmarshal(body, &response) != nil ||
		response.Version != LinkedMessageRelationVersion ||
		response.HostChainID != relation.HostChainID ||
		response.PeerChainID != relation.PeerChainID ||
		response.RelationRevision != linkedMessageContactRevision(relation) {
		return ErrFederatedPipeInvalid
	}
	return nil
}

func (m *Manager) handleLinkedMessageResolve(w http.ResponseWriter, r *http.Request) {
	peer := peerFromCtx(r.Context())
	if peer == nil || peer.Agreement == nil {
		httpError(w, http.StatusForbidden, "unauthenticated")
		return
	}
	var req LinkedMessageResolveRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxLinkedMessageResolveBytes)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		linkedMessageUnavailable(w)
		return
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		linkedMessageUnavailable(w)
		return
	}
	if req.Version != LinkedMessageRelationVersion ||
		(req.Direction != LinkedMessageGuestToMember &&
			req.Direction != LinkedMessageMemberToGuest) {
		linkedMessageUnavailable(w)
		return
	}
	for _, id := range []string{req.SourceAgentID, req.TargetAgentID} {
		if _, err := auth.AgentIDToPublicKey(id); err != nil ||
			id != strings.ToLower(id) {
			linkedMessageUnavailable(w)
			return
		}
	}

	ss := m.syncStore()
	if ss == nil || m.badger == nil {
		linkedMessageUnavailable(w)
		return
	}
	policyUnlock := ss.LockSyncPolicyRead()
	ownerUnlock := m.badger.LockDomainOwnershipRead()
	defer policyUnlock()
	defer ownerUnlock()
	agreement, err := m.currentRequestAgreementBound(r.Context(), peer)
	if err != nil {
		linkedMessageUnavailable(w)
		return
	}

	var relation *LinkedMessageRelation
	var policy *store.PeerRBACPolicy
	var receiverDigest string
	switch req.Direction {
	case LinkedMessageGuestToMember:
		if req.Relation != nil {
			linkedMessageUnavailable(w)
			return
		}
		relation, policy, err = m.buildHostedLinkedRelation(
			r.Context(), peer, agreement, req.Direction,
			req.SourceAgentID, req.TargetAgentID,
		)
		if err == nil {
			receiverDigest = relation.HostAgreementDigest
		}
	case LinkedMessageMemberToGuest:
		relation = req.Relation
		policy, receiverDigest, err =
			m.currentLinkedMessagePolicy(r.Context(), agreement, peer.AgentID)
		if err == nil && !m.linkedMessageLocalAgentEligible(req.TargetAgentID) {
			err = ErrFederatedPipeInvalid
		}
		var consent *store.FederatedLinkedMessageConsent
		if err == nil {
			consent, err = m.receiverLinkedMessageConsent(
				r.Context(), policy, req.SourceAgentID, req.TargetAgentID,
			)
		}
		if err == nil && relation == nil {
			writeJSON(w, http.StatusOK, &LinkedMessageResolveResponse{
				Version: LinkedMessageRelationVersion, ChainID: m.localChainID,
				SourceAgentID: req.SourceAgentID, TargetAgentID: req.TargetAgentID,
				ConsentRevision:         consent.Revision,
				ReceiverAgreementDigest: receiverDigest,
				PolicyEpoch:             policy.PolicyEpoch,
			})
			return
		}
		if err == nil &&
			(relation.SourceAgentID != req.SourceAgentID ||
				relation.TargetAgentID != req.TargetAgentID ||
				relation.Direction != req.Direction ||
				relation.HostChainID != peer.ChainID ||
				relation.PeerChainID != m.localChainID ||
				validateLinkedMessageRelation(relation, peer.AgentID) != nil) {
			err = ErrFederatedPipeInvalid
		}
		if err == nil {
			// The assertion belongs to the host. The receiver cannot alter it;
			// the source freezes the revision learned in the preceding
			// no-payload preflight and a toggle race fails generically.
			if relation.ReceiverConsentRevision != consent.Revision {
				err = ErrFederatedPipeInvalid
			}
		}
	}
	if err != nil || relation == nil || policy == nil {
		linkedMessageUnavailable(w)
		return
	}
	target := m.linkedTargetFromRelation(policy, receiverDigest, relation)
	target.ChainID = m.localChainID
	target.Address = target.AgentID + "@" + m.localChainID
	writeJSON(w, http.StatusOK, &LinkedMessageResolveResponse{
		Version: LinkedMessageRelationVersion, ChainID: m.localChainID,
		SourceAgentID: req.SourceAgentID, TargetAgentID: req.TargetAgentID,
		ConsentRevision:         relation.ReceiverConsentRevision,
		ReceiverAgreementDigest: receiverDigest, PolicyEpoch: policy.PolicyEpoch,
		Target: target,
	})
}

func (m *Manager) callLinkedMessageResolve(
	ctx context.Context,
	agreement *store.CrossFedRecord,
	req LinkedMessageResolveRequest,
) (*LinkedMessageResolveResponse, error) {
	requestCtx, cancel := context.WithTimeout(ctx, linkedMessageResolveTimeout)
	defer cancel()
	body, status, err := m.doPeerRequest(
		requestCtx, agreement, http.MethodPost,
		"/fed/v1/pipe/linked/resolve", req,
	)
	if err != nil {
		return nil, errors.Join(ErrRemotePipeResolutionIncomplete, err)
	}
	if status != http.StatusOK {
		return nil, ErrRemotePipeTargetNotFound
	}
	var response LinkedMessageResolveResponse
	if err := json.Unmarshal(body, &response); err != nil ||
		response.Version != LinkedMessageRelationVersion ||
		response.ChainID != agreement.RemoteChainID ||
		response.SourceAgentID != req.SourceAgentID ||
		response.TargetAgentID != req.TargetAgentID {
		return nil, ErrRemotePipeTargetNotFound
	}
	return &response, nil
}

// ResolveRemoteLinkedPipeTarget resolves only an exact agent@chain address and
// binds the authenticated local caller into the relation. Friendly names,
// peer operator IDs and directory fallbacks are intentionally unsupported.
func (m *Manager) ResolveRemoteLinkedPipeTarget(
	ctx context.Context,
	sourceAgentID, target string,
) (*RemotePipeTarget, error) {
	target = strings.TrimSpace(target)
	at := strings.LastIndex(target, "@")
	if at != 64 || at == len(target)-1 {
		return nil, ErrRemotePipeTargetNotFound
	}
	targetAgentID, remoteChainID := target[:at], target[at+1:]
	for _, id := range []string{sourceAgentID, targetAgentID} {
		if _, err := auth.AgentIDToPublicKey(id); err != nil ||
			id != strings.ToLower(id) {
			return nil, ErrRemotePipeTargetNotFound
		}
	}
	if !m.linkedMessageLocalAgentEligible(sourceAgentID) {
		return nil, ErrRemotePipeTargetNotFound
	}
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, ErrRemotePipeTargetNotFound
	}
	peerAgentID, err := m.ResolvePeerOperatorAgentID(ctx, remoteChainID)
	if err != nil {
		return nil, ErrRemotePipeTargetNotFound
	}
	peer := &peerIdentity{
		ChainID: remoteChainID, AgentID: peerAgentID, Agreement: agreement,
	}

	// First try a locally hosted link (member -> guest). The relation is built
	// and signed locally, then the remote receiver validates exact consent.
	relation, _, localErr := m.buildHostedLinkedRelation(
		ctx, peer, agreement, LinkedMessageMemberToGuest,
		sourceAgentID, targetAgentID,
	)
	if localErr == nil {
		preflight, callErr := m.callLinkedMessageResolve(
			ctx, agreement, LinkedMessageResolveRequest{
				Version:       LinkedMessageRelationVersion,
				Direction:     LinkedMessageMemberToGuest,
				SourceAgentID: sourceAgentID, TargetAgentID: targetAgentID,
			},
		)
		if callErr != nil {
			return nil, callErr
		}
		if preflight.Target != nil || preflight.ConsentRevision <= 0 ||
			!isPipeDigest(preflight.ReceiverAgreementDigest) ||
			preflight.PolicyEpoch == "" {
			return nil, ErrRemotePipeTargetNotFound
		}
		relation.ReceiverConsentRevision = preflight.ConsentRevision
		if err := relation.sign(m.agentKey); err != nil {
			return nil, ErrRemotePipeTargetNotFound
		}
		response, callErr := m.callLinkedMessageResolve(
			ctx, agreement, LinkedMessageResolveRequest{
				Version:       LinkedMessageRelationVersion,
				Direction:     LinkedMessageMemberToGuest,
				SourceAgentID: sourceAgentID, TargetAgentID: targetAgentID,
				Relation: relation,
			},
		)
		if callErr != nil || response.Target == nil {
			return nil, ErrRemotePipeTargetNotFound
		}
		targetResult := response.Target
		if targetResult.ChainID != remoteChainID ||
			targetResult.AgentID != targetAgentID ||
			targetResult.AuthorizationMode != LinkedMessageAuthorizationMode ||
			!linkedMessageRelationEqual(targetResult.LinkedRelation, relation) {
			return nil, ErrRemotePipeTargetNotFound
		}
		return targetResult, nil
	}

	// Otherwise ask whether the remote node hosts guest(source)->member(target).
	response, callErr := m.callLinkedMessageResolve(
		ctx, agreement, LinkedMessageResolveRequest{
			Version:       LinkedMessageRelationVersion,
			Direction:     LinkedMessageGuestToMember,
			SourceAgentID: sourceAgentID, TargetAgentID: targetAgentID,
		},
	)
	if callErr != nil {
		return nil, callErr
	}
	if response.Target == nil {
		return nil, ErrRemotePipeTargetNotFound
	}
	targetResult := response.Target
	relation = targetResult.LinkedRelation
	if targetResult.ChainID != remoteChainID ||
		targetResult.AgentID != targetAgentID ||
		targetResult.AuthorizationMode != LinkedMessageAuthorizationMode ||
		relation == nil || relation.HostChainID != remoteChainID ||
		relation.PeerChainID != m.localChainID ||
		relation.Direction != LinkedMessageGuestToMember ||
		validateLinkedMessageRelation(relation, peerAgentID) != nil {
		return nil, ErrRemotePipeTargetNotFound
	}
	return targetResult, nil
}

// SetLinkedMessageConsentCAS is the operator/control-plane primitive. HTTP
// adapters must apply their normal localhost and CEREBRUM authorization gates.
func (m *Manager) SetLinkedMessageConsentCAS(
	ctx context.Context,
	remoteChainID, remoteAgentID, localAgentID string,
	expectedRevision int64,
	accepting bool,
) (int64, error) {
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return 0, err
	}
	peerAgentID, err := m.ResolvePeerOperatorAgentID(ctx, remoteChainID)
	if err != nil {
		return 0, err
	}
	policy, err := m.v23BindingReady(ctx, agreement, peerAgentID)
	if err != nil {
		return 0, err
	}
	if policy.Paused && accepting {
		return 0, ErrFederatedPipeSuspended
	}
	ss := m.syncStore()
	if ss == nil {
		return 0, ErrFederatedPipeInvalid
	}
	releaseDelivery := m.beginPeerLinkedAuthorizationMutation(remoteChainID)
	defer releaseDelivery()
	currentAgreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil || !sameAgreementGeneration(agreement, currentAgreement) {
		return 0, ErrFederatedPipeInvalid
	}
	currentPolicy, err := m.v23BindingReady(
		ctx, currentAgreement, peerAgentID,
	)
	if err != nil || currentPolicy.PolicyEpoch != policy.PolicyEpoch ||
		currentPolicy.RemoteCAPin != policy.RemoteCAPin ||
		currentPolicy.Revision != policy.Revision ||
		currentPolicy.Paused != policy.Paused {
		return 0, ErrFederatedPipeInvalid
	}
	agreement, policy = currentAgreement, currentPolicy
	currentPair, err := m.currentHostedLinkedMessagePair(
		ctx, agreement, peerAgentID, remoteAgentID, localAgentID,
	)
	if err != nil {
		return 0, ErrRemotePipeTargetNotFound
	}
	if !currentPair && accepting {
		// The complementary direction is hosted by the peer. Obtain one
		// exact, signed, no-consent offer before creating local consent; no
		// roster or friendly lookup is permitted.
		currentPair = m.checkRemoteHostedLinkedMessagePair(
			ctx, agreement, peerAgentID, remoteAgentID, localAgentID,
		)
	}
	existing, err := ss.GetBoundFederatedLinkedMessageConsent(
		ctx, *policy, remoteAgentID, localAgentID,
	)
	if err != nil || (accepting && !currentPair) ||
		(!accepting && !currentPair && existing == nil) {
		// Do not reveal whether the remote ID, group or local member was the
		// missing component, and never permit dormant pre-consent.
		return 0, ErrRemotePipeTargetNotFound
	}
	return ss.SetBoundFederatedLinkedMessageConsentCAS(
		ctx, *policy, remoteAgentID, localAgentID, expectedRevision, accepting,
	)
}

func (m *Manager) currentHostedLinkedMessagePair(
	ctx context.Context,
	agreement *store.CrossFedRecord,
	peerAgentID, remoteAgentID, localAgentID string,
) (bool, error) {
	if m.federatedGuestStore == nil || m.badger == nil ||
		!m.linkedMessageLocalAgentEligible(localAgentID) {
		return false, nil
	}
	_, digest, err := func() (*store.PeerRBACPolicy, string, error) {
		policy, policyErr := m.v23BindingReady(ctx, agreement, peerAgentID)
		if policyErr != nil {
			return nil, "", policyErr
		}
		digest, digestErr := m.agreementBindingDigestV23(
			ctx, agreement, peerAgentID,
		)
		return policy, digest, digestErr
	}()
	if err != nil {
		return false, err
	}
	guests, err := m.federatedGuestStore.ListFederatedGroupGuests(
		ctx, agreement.RemoteChainID, remoteAgentID,
	)
	if err != nil {
		return false, err
	}
	for _, guest := range guests {
		if guest.State != store.FederatedGuestStateActive ||
			guest.AgreementBindingDigest != digest ||
			!m.federatedGuestAuthorityActive(guest) {
			continue
		}
		group, groupErr := m.badger.GetAppV23AccessGroup(guest.GroupID)
		if groupErr != nil {
			return false, groupErr
		}
		if appV23GroupHasMember(group, localAgentID) {
			return true, nil
		}
	}
	return false, nil
}

// GetLinkedMessageConsent returns the exact current-generation receiver-local
// consent row. Nil means default-off or stale/absent binding; an explicit
// disabled current row is returned with its monotonic revision.
func (m *Manager) GetLinkedMessageConsent(
	ctx context.Context,
	remoteChainID, remoteAgentID, localAgentID string,
) (*store.FederatedLinkedMessageConsent, error) {
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, err
	}
	peerAgentID, err := m.ResolvePeerOperatorAgentID(ctx, remoteChainID)
	if err != nil {
		return nil, err
	}
	policy, err := m.v23BindingReady(ctx, agreement, peerAgentID)
	if err != nil {
		return nil, err
	}
	ss := m.syncStore()
	if ss == nil {
		return nil, ErrFederatedPipeInvalid
	}
	return ss.GetBoundFederatedLinkedMessageConsent(
		ctx, *policy, remoteAgentID, localAgentID,
	)
}

const MaxLinkedMessageConsentCandidates = store.AppV23MaxGroupsPerAgent * store.AppV23MaxGroupMembers

type LinkedMessageConsentCandidate struct {
	RemoteChainID string   `json:"remote_chain_id"`
	RemoteAgentID string   `json:"remote_agent_id"`
	LocalAgentID  string   `json:"local_agent_id"`
	GroupIDs      []string `json:"group_ids"`
	Revision      int64    `json:"revision"`
	Accepting     bool     `json:"accepting"`
}

// ListLinkedMessageConsentCandidates is intentionally exact-principal scoped:
// callers first choose one existing bounded Linked-reader identity. The method
// returns only current active group-member pairs for that identity, never a
// local or remote roster.
func (m *Manager) ListLinkedMessageConsentCandidates(
	ctx context.Context,
	callerID, remoteChainID, remoteAgentID string,
) ([]LinkedMessageConsentCandidate, error) {
	if !m.localAdminSignerActive(callerID) {
		return nil, errors.New("current local Admin or root credential is required")
	}
	// Remote activity is checked before local leases; a slow peer must not
	// block local revocation or policy mutation.
	if err := m.CheckRemoteFederatedGuestAgentEligibility(
		ctx, remoteChainID, remoteAgentID,
	); err != nil {
		return []LinkedMessageConsentCandidate{}, nil
	}
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, err
	}
	peerAgentID, err := m.ResolvePeerOperatorAgentID(ctx, remoteChainID)
	if err != nil {
		return nil, err
	}
	policy, err := m.v23BindingReady(ctx, agreement, peerAgentID)
	if err != nil {
		return nil, err
	}
	digest, err := m.agreementBindingDigestV23(ctx, agreement, peerAgentID)
	if err != nil {
		return nil, err
	}
	guests, err := m.federatedGuestStore.ListFederatedGroupGuests(
		ctx, remoteChainID, remoteAgentID,
	)
	if err != nil {
		return nil, err
	}
	groupIDsByMember := make(map[string][]string)
	for _, guest := range guests {
		if guest.State != store.FederatedGuestStateActive ||
			guest.AgreementBindingDigest != digest ||
			!m.federatedGuestAuthorityActive(guest) {
			continue
		}
		group, groupErr := m.badger.GetAppV23AccessGroup(guest.GroupID)
		if groupErr != nil {
			return nil, groupErr
		}
		if group == nil || group.Revision == 0 ||
			len(group.Members) > store.AppV23MaxGroupMembers {
			continue
		}
		for _, memberID := range group.Members {
			if !m.linkedMessageLocalAgentEligible(memberID) {
				continue
			}
			groupIDsByMember[memberID] = append(
				groupIDsByMember[memberID], group.GroupID,
			)
		}
	}
	memberIDs := make([]string, 0, len(groupIDsByMember))
	for memberID := range groupIDsByMember {
		memberIDs = append(memberIDs, memberID)
	}
	sort.Strings(memberIDs)
	if len(memberIDs) > MaxLinkedMessageConsentCandidates {
		return nil, errors.New("linked-message consent candidate bound exceeded")
	}
	ss := m.syncStore()
	if ss == nil {
		return nil, ErrFederatedPipeInvalid
	}
	out := make([]LinkedMessageConsentCandidate, 0, len(memberIDs))
	for _, memberID := range memberIDs {
		groupIDs := groupIDsByMember[memberID]
		sort.Strings(groupIDs)
		candidate := LinkedMessageConsentCandidate{
			RemoteChainID: remoteChainID, RemoteAgentID: remoteAgentID,
			LocalAgentID: memberID, GroupIDs: groupIDs,
		}
		consent, consentErr := ss.GetBoundFederatedLinkedMessageConsent(
			ctx, *policy, remoteAgentID, memberID,
		)
		if consentErr != nil {
			return nil, consentErr
		}
		if consent != nil {
			candidate.Revision = consent.Revision
			candidate.Accepting = consent.Accepting
		}
		out = append(out, candidate)
	}
	return out, nil
}

// ListRemoteHostedLinkedMessageConsentCandidates lets the peer that owns the
// guest agent discover which exact remote members may be explicitly consented
// to message it. The authenticated host supplies bounded signed offers; this
// node verifies every offer and projects only IDs plus local consent state.
func (m *Manager) ListRemoteHostedLinkedMessageConsentCandidates(
	ctx context.Context,
	callerID, hostChainID, localGuestAgentID string,
) ([]LinkedMessageConsentCandidate, error) {
	if !m.localAdminSignerActive(callerID) {
		return nil, errors.New("current local Admin or root credential is required")
	}
	if !m.linkedMessageLocalAgentEligible(localGuestAgentID) {
		return []LinkedMessageConsentCandidate{}, nil
	}
	agreement, err := m.ActiveAgreement(hostChainID)
	if err != nil {
		return nil, err
	}
	peerAgentID, err := m.ResolvePeerOperatorAgentID(ctx, hostChainID)
	if err != nil {
		return nil, err
	}
	policy, err := m.v23BindingReady(ctx, agreement, peerAgentID)
	if err != nil {
		return nil, err
	}
	requestCtx, cancel := context.WithTimeout(ctx, linkedMessageResolveTimeout)
	defer cancel()
	body, status, err := m.doPeerRequest(
		requestCtx, agreement, http.MethodPost,
		"/fed/v1/pipe/linked/consent-candidates",
		LinkedMessageConsentCandidateRequest{
			Version:      LinkedMessageRelationVersion,
			GuestAgentID: localGuestAgentID,
		},
	)
	if err != nil || status != http.StatusOK {
		return []LinkedMessageConsentCandidate{}, nil
	}
	var response LinkedMessageConsentCandidateResponse
	if json.Unmarshal(body, &response) != nil ||
		response.Version != LinkedMessageRelationVersion ||
		len(response.Relations) > MaxLinkedMessageConsentCandidates {
		return nil, ErrFederatedPipeInvalid
	}
	byMember := make(map[string]*LinkedMessageConsentCandidate)
	for _, relation := range response.Relations {
		if relation == nil ||
			relation.Direction != LinkedMessageMemberToGuest ||
			relation.HostChainID != hostChainID ||
			relation.PeerChainID != m.localChainID ||
			relation.TargetAgentID != localGuestAgentID ||
			validateLinkedMessageRelation(relation, peerAgentID) != nil {
			return nil, ErrFederatedPipeInvalid
		}
		candidate := byMember[relation.SourceAgentID]
		if candidate == nil {
			candidate = &LinkedMessageConsentCandidate{
				RemoteChainID: hostChainID,
				RemoteAgentID: relation.SourceAgentID,
				LocalAgentID:  localGuestAgentID,
			}
			byMember[relation.SourceAgentID] = candidate
		}
		candidate.GroupIDs = append(candidate.GroupIDs, relation.Guest.GroupID)
	}
	memberIDs := make([]string, 0, len(byMember))
	for memberID := range byMember {
		memberIDs = append(memberIDs, memberID)
	}
	sort.Strings(memberIDs)
	ss := m.syncStore()
	if ss == nil {
		return nil, ErrFederatedPipeInvalid
	}
	out := make([]LinkedMessageConsentCandidate, 0, len(memberIDs))
	for _, memberID := range memberIDs {
		candidate := byMember[memberID]
		sort.Strings(candidate.GroupIDs)
		consent, consentErr := ss.GetBoundFederatedLinkedMessageConsent(
			ctx, *policy, memberID, localGuestAgentID,
		)
		if consentErr != nil {
			return nil, consentErr
		}
		if consent != nil {
			candidate.Revision = consent.Revision
			candidate.Accepting = consent.Accepting
		}
		out = append(out, *candidate)
	}
	return out, nil
}
