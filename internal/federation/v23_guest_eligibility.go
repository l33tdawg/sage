package federation

import (
	"context"
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

const federatedGuestEligibilityTimeout = 5 * time.Second

var ErrRemoteFederatedGuestAgentIneligible = errors.New(
	"remote identity is not an eligible active ordinary agent",
)

// localFederatedGuestAgentEligible evaluates only consensus identity state. It
// never consults SQLite contact/directory rows, domain advertisements, display
// names, or pipeline acceptance, because those are narrower routing views and
// their absence is not proof that an exact ordinary agent does not exist.
func (m *Manager) localFederatedGuestAgentEligible(agentID string) (bool, error) {
	if m.badger == nil || m.postV23ForNextTx == nil || !m.postV23ForNextTx() {
		return false, errors.New("app-v23 agent eligibility state is unavailable")
	}
	if _, err := auth.AgentIDToPublicKey(agentID); err != nil ||
		agentID != strings.ToLower(agentID) {
		return false, errors.New("agent id is not canonical")
	}
	root, err := m.badger.GetAppV23Root()
	if err != nil || root == nil {
		return false, errors.New("app-v23 Root state is unavailable")
	}
	wasRoot, err := m.badger.IsAppV23RootCredential(agentID)
	if err != nil {
		return false, err
	}
	if wasRoot || agentID == root.PrincipalID || agentID == root.CredentialID {
		return false, nil
	}
	enrollment, err := m.badger.GetAppV23Enrollment(agentID)
	if err != nil {
		return false, err
	}
	role, err := m.badger.GetAppV23Role(agentID)
	if err != nil {
		return false, err
	}
	if enrollment == nil || role == nil || !enrollment.Active {
		return false, nil
	}
	if enrollment.AgentID != agentID || role.AgentID != agentID {
		return false, errors.New("app-v23 agent identity projection is inconsistent")
	}
	if enrollment.Profile == store.AppV23ProfileRoot ||
		m.badger.ValidateCurrentAgentPolicy(
			role.Role, enrollment.Profile, enrollment.Capabilities, enrollment.Clearance,
		) != nil {
		return false, nil
	}
	// Delegated Admin authority belongs to one Root generation. A stale Admin
	// must be reviewed by the current Root before it can be newly linked,
	// although ordinary Member/Manager enrollment survives Root handover.
	if role.Role == store.AppV23RoleAdmin &&
		enrollment.RootGeneration != root.Generation {
		return false, nil
	}
	agent, err := m.badger.GetRegisteredAgent(agentID)
	if err != nil {
		return false, fmt.Errorf("read enrolled app-v23 agent projection: %w", err)
	}
	if agent == nil || agent.AgentID != agentID ||
		agent.Role != role.Role ||
		agent.Clearance != enrollment.Clearance ||
		agent.Capabilities != enrollment.Capabilities {
		return false, errors.New("app-v23 agent policy projection is inconsistent")
	}
	return true, nil
}

// handleFederatedGuestAgentEligibility is a bounded, peer-authenticated exact
// ID oracle. peerAuth already binds mTLS, chain, operator, request signature,
// nonce, and active agreement generation. This handler repeats the live v23
// ceremony/policy check under the same read leases as the identity projection.
func (m *Manager) handleFederatedGuestAgentEligibility(w http.ResponseWriter, r *http.Request) {
	peer := peerFromCtx(r.Context())
	if peer == nil || peer.Agreement == nil {
		httpError(w, http.StatusForbidden, "unauthenticated")
		return
	}
	var req FederatedGuestAgentEligibilityRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxFederatedGuestEligibilityRequestBytes)
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		httpError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	req.AgentID = strings.TrimSpace(req.AgentID)
	if _, err := auth.AgentIDToPublicKey(req.AgentID); err != nil ||
		req.AgentID != strings.ToLower(req.AgentID) {
		httpError(w, http.StatusBadRequest, "invalid canonical agent id")
		return
	}
	ss := m.syncStore()
	if ss == nil || m.badger == nil {
		httpError(w, http.StatusServiceUnavailable, "agent eligibility is unavailable")
		return
	}
	policyUnlock := ss.LockSyncPolicyRead()
	ownerUnlock := m.badger.LockDomainOwnershipRead()
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		ownerUnlock()
		policyUnlock()
	}
	defer release()
	agreement, err := m.currentRequestAgreementBound(r.Context(), peer)
	if err != nil {
		release()
		httpError(w, http.StatusForbidden, "federation agreement generation changed")
		return
	}
	if _, bindingErr := m.v23BindingReady(r.Context(), agreement, peer.AgentID); bindingErr != nil {
		release()
		httpError(w, http.StatusConflict, "federation v23 binding is not ready")
		return
	}
	eligible, err := m.localFederatedGuestAgentEligible(req.AgentID)
	if err != nil {
		m.logger.Error().Err(err).Str("peer", peer.ChainID).
			Msg("federated guest exact-agent eligibility failed")
		release()
		httpError(w, http.StatusInternalServerError, "agent eligibility check failed")
		return
	}
	response := FederatedGuestAgentEligibilityResponse{
		ProtocolVersion: FederationProtocolV23,
		ChainID:         m.localChainID,
		AgentID:         req.AgentID,
		Eligible:        eligible,
	}
	// Never let a slow authenticated peer retain the consensus/policy leases
	// while draining the small response.
	release()
	writeJSON(w, http.StatusOK, response)
}

func (m *Manager) checkRemoteFederatedGuestAgentEligibility(
	ctx context.Context,
	agreement *store.CrossFedRecord,
	remoteAgentID string,
) error {
	if agreement == nil {
		return errors.New("active federation agreement is required")
	}
	if m.federatedGuestEligibilityFn != nil {
		return m.federatedGuestEligibilityFn(
			ctx, agreement.RemoteChainID, remoteAgentID,
		)
	}
	requestCtx, cancel := context.WithTimeout(ctx, federatedGuestEligibilityTimeout)
	defer cancel()
	body, status, err := m.doPeerRequest(
		requestCtx,
		agreement,
		http.MethodPost,
		"/fed/v1/guest/agent/eligibility",
		FederatedGuestAgentEligibilityRequest{AgentID: remoteAgentID},
	)
	if err != nil {
		return fmt.Errorf("remote agent eligibility is unavailable: %w", err)
	}
	if status != http.StatusOK {
		return fmt.Errorf(
			"remote agent eligibility was denied by peer %s: status %d: %s",
			agreement.RemoteChainID, status, truncate(body, 200),
		)
	}
	var response FederatedGuestAgentEligibilityResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("decode remote agent eligibility: %w", err)
	}
	if response.ProtocolVersion != FederationProtocolV23 ||
		response.ChainID != agreement.RemoteChainID ||
		response.AgentID != remoteAgentID {
		return errors.New("remote agent eligibility response binding is invalid")
	}
	if !response.Eligible {
		return ErrRemoteFederatedGuestAgentIneligible
	}
	return nil
}

// CheckRemoteFederatedGuestAgentEligibility exposes the same authoritative
// exact-ID validation used by attach/rebind to local UI/API adapters. A UI may
// offer an explicit agent_id@chain_id fallback only after this live check
// succeeds; it must not infer eligibility from a directory miss.
func (m *Manager) CheckRemoteFederatedGuestAgentEligibility(
	ctx context.Context,
	remoteChainID, remoteAgentID string,
) error {
	if _, err := auth.AgentIDToPublicKey(remoteAgentID); err != nil ||
		remoteAgentID != strings.ToLower(remoteAgentID) {
		return errors.New("remote agent id is invalid")
	}
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return err
	}
	peerAgentID, err := m.ResolvePeerOperatorAgentID(ctx, remoteChainID)
	if err != nil {
		return err
	}
	if _, err := m.v23BindingReady(ctx, agreement, peerAgentID); err != nil {
		return fmt.Errorf("federation v23 binding is not ready: %w", err)
	}
	return m.checkRemoteFederatedGuestAgentEligibility(
		ctx, agreement, remoteAgentID,
	)
}
