package web

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/scope"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// GovernanceStoreProvider is implemented by stores that support governance queries.
type GovernanceStoreProvider interface {
	store.GovernanceStore
}

// RegisterGovernanceRoutes registers all /v1/dashboard/governance/ routes.
func (h *DashboardHandler) RegisterGovernanceRoutes(r chi.Router) {
	govStore, ok := h.store.(GovernanceStoreProvider)
	if !ok {
		return // Store doesn't support governance
	}

	// Read-only query endpoints
	r.Get("/v1/dashboard/governance/proposals", handleListProposals(govStore))
	r.Get("/v1/dashboard/governance/proposals/{id}", h.handleGetProposal(govStore))

	// Write endpoints — broadcast governance transactions through CometBFT
	r.Post("/v1/dashboard/governance/propose", h.handleDashboardGovPropose)
	r.Post("/v1/dashboard/governance/vote", h.handleDashboardGovVote)
}

// --- Read Endpoints ----------------------------------------------------------

func handleListProposals(govStore store.GovernanceStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		status := r.URL.Query().Get("status")
		proposals, err := govStore.ListGovProposals(r.Context(), status)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to list proposals: "+err.Error())
			return
		}
		if proposals == nil {
			proposals = []*store.GovProposal{}
		}
		writeJSONResp(w, http.StatusOK, map[string]any{"proposals": proposals})
	}
}

func (h *DashboardHandler) handleGetProposal(govStore store.GovernanceStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if id == "" {
			writeError(w, http.StatusBadRequest, "proposal id is required")
			return
		}

		proposal, err := govStore.GetGovProposal(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusNotFound, "proposal not found")
			return
		}

		votes, err := govStore.GetGovVotes(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to get votes: "+err.Error())
			return
		}
		if votes == nil {
			votes = []*store.GovVote{}
		}

		// Build quorum progress from on-chain state if BadgerStore is available.
		quorum := map[string]any{
			"accept_power": int64(0),
			"reject_power": int64(0),
			"total_power":  int64(0),
			"threshold":    "2/3",
		}
		if h.BadgerStore != nil {
			// Convert SQLite votes to map[validatorID]decision for quorum check.
			voteMap := make(map[string]string, len(votes))
			for _, v := range votes {
				voteMap[v.ValidatorID] = v.Decision
			}

			// Get validator powers from on-chain state.
			powers := make(map[string]int64)
			// Use the BadgerStore's governance vote data for accurate power lookup.
			onChainVotes, voteErr := h.BadgerStore.GetGovVotes(id)
			if voteErr == nil && onChainVotes != nil {
				// Merge on-chain votes (more authoritative) over SQLite votes.
				for vid, dec := range onChainVotes {
					voteMap[vid] = dec
				}
			}

			// Attempt to read validator powers from the active validator set.
			allVals, loadErr := h.BadgerStore.LoadValidators()
			if loadErr == nil && allVals != nil {
				for vid, power := range allVals {
					powers[vid] = power
				}
			}

			if len(powers) > 0 {
				_, _, acceptPower, rejectPower, totalPower := governance.CheckGovQuorum(voteMap, powers)
				quorum["accept_power"] = acceptPower
				quorum["reject_power"] = rejectPower
				quorum["total_power"] = totalPower
			}
		}

		writeJSONResp(w, http.StatusOK, map[string]any{
			"proposal":        proposal,
			"votes":           votes,
			"quorum_progress": quorum,
		})
	}
}

// --- Write Endpoints ---------------------------------------------------------

// handleDashboardGovPropose handles POST /v1/dashboard/governance/propose.
func (h *DashboardHandler) handleDashboardGovPropose(w http.ResponseWriter, r *http.Request) {
	if !h.requireDashboardGovernanceOperator(w, r) {
		return
	}
	if h.CometBFTRPC == "" || h.SigningKey == nil {
		writeError(w, http.StatusServiceUnavailable, "CometBFT consensus not configured")
		return
	}

	var req struct {
		ValidatorID      string                  `json:"validator_id,omitempty"`
		GovernanceDomain string                  `json:"governance_domain,omitempty"`
		Operation        string                  `json:"operation"`
		TargetID         string                  `json:"target_id"`
		TargetPubkey     string                  `json:"target_pubkey,omitempty"`
		TargetPower      int64                   `json:"target_power,omitempty"`
		ExpiryBlocks     int64                   `json:"expiry_blocks,omitempty"`
		Reason           string                  `json:"reason"`
		Payload          string                  `json:"payload,omitempty"`
		Scope            *scope.ProposalTemplate `json:"scope,omitempty"`
	}
	rawBody, err := readDashboardGovernanceBody(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if err = json.Unmarshal(rawBody, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.Operation == "" {
		writeError(w, http.StatusBadRequest, "operation is required (add_validator, remove_validator, update_power, sync_group_action, scope_action)")
		return
	}
	if req.Reason == "" {
		writeError(w, http.StatusBadRequest, "reason is required")
		return
	}

	op, err := parseDashboardGovOp(req.Operation)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	postAppV20 := h.AppV20ActiveFn != nil && h.AppV20ActiveFn()
	if op == tx.GovOpScopeAction && !postAppV20 {
		writeError(w, http.StatusConflict, "scope_action requires app-v20 activation")
		return
	}

	var pubKeyBytes []byte
	if req.TargetPubkey != "" {
		pubKeyBytes, err = hex.DecodeString(req.TargetPubkey)
		if err != nil {
			writeError(w, http.StatusBadRequest, "target_pubkey must be valid hex")
			return
		}
	}
	var payloadBytes []byte
	req.TargetID, payloadBytes, err = resolveDashboardGovProposalPayload(op, req.TargetID, req.Payload, req.Scope)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	outerKey := h.AdminSigningKey
	proofBody := rawBody
	if postAppV20 {
		outerKey = h.SigningKey
		if len(h.AdminSigningKey) != ed25519.PrivateKeySize {
			writeError(w, http.StatusServiceUnavailable, "governance operator key not configured")
			return
		}
		req.ValidatorID, req.GovernanceDomain, err = h.dashboardGovernanceAuthorizationContext()
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		proofBody, err = json.Marshal(req)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to encode governance authorization")
			return
		}
	} else if len(outerKey) != ed25519.PrivateKeySize {
		writeError(w, http.StatusServiceUnavailable, "governance admin signing key not configured")
		return
	}

	proposeTx := &tx.ParsedTx{
		Type:      tx.TxTypeGovPropose,
		Nonce:     tx.MonotonicNonce(outerKey),
		Timestamp: time.Now(),
		GovPropose: &tx.GovPropose{
			Operation:    op,
			TargetID:     req.TargetID,
			TargetPubKey: pubKeyBytes,
			TargetPower:  req.TargetPower,
			ExpiryBlocks: req.ExpiryBlocks,
			Reason:       req.Reason,
			Payload:      payloadBytes,
		},
	}

	if postAppV20 {
		if err = embedDashboardGovernanceProof(proposeTx, h.AdminSigningKey, r.Method, r.URL.RequestURI(), proofBody); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to authorize transaction")
			return
		}
	} else {
		embedDashboardAgentProof(proposeTx, outerKey)
	}
	if err = tx.SignTx(proposeTx, outerKey); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to sign transaction")
		return
	}
	encoded, err := tx.EncodeTx(proposeTx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to encode transaction")
		return
	}

	txHash, committedHeight, _, err := broadcastTxCommitWeb(h.CometBFTRPC, encoded)
	if err != nil {
		writeError(w, http.StatusBadGateway, "governance proposal rejected: "+err.Error())
		return
	}
	validatorID := auth.PublicKeyToAgentID(ed25519.PublicKey(proposeTx.PublicKey))
	proposalID := governance.ComputeProposalID(validatorID, committedHeight, governance.ProposalOp(op), req.TargetID)
	proposalStatus, err := h.dashboardCommittedGovernanceStatus(proposalID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "proposal committed but authoritative state verification failed: "+err.Error())
		return
	}

	// Emit SSE event for real-time dashboard updates.
	if h.SSE != nil {
		h.SSE.Broadcast(SSEEvent{
			Type:    EventGovernance,
			Content: fmt.Sprintf("Proposal submitted: %s %s", req.Operation, req.TargetID),
			Data: map[string]any{
				"action":      "propose",
				"proposal_id": proposalID,
				"tx_hash":     txHash,
				"operation":   req.Operation,
				"target_id":   req.TargetID,
			},
		})
	}

	writeJSONResp(w, http.StatusOK, map[string]any{
		"proposal_id": proposalID,
		"tx_hash":     txHash,
		"status":      proposalStatus,
	})
}

func resolveDashboardGovProposalPayload(op tx.GovProposalOp, targetID, rawPayload string, template *scope.ProposalTemplate) (string, []byte, error) {
	if template != nil {
		if op != tx.GovOpScopeAction {
			return "", nil, fmt.Errorf("scope is only valid for scope_action")
		}
		if rawPayload != "" {
			return "", nil, fmt.Errorf("payload and scope are mutually exclusive")
		}
		encoded, err := scope.EncodeProposalTemplate(*template)
		if err != nil {
			return "", nil, fmt.Errorf("scope: %w", err)
		}
		if targetID == "" {
			targetID = template.ScopeID
		} else if targetID != template.ScopeID {
			return "", nil, fmt.Errorf("target_id %q does not match scope_id %q", targetID, template.ScopeID)
		}
		return targetID, encoded, nil
	}

	var payload []byte
	if rawPayload != "" {
		decoded, err := base64.StdEncoding.DecodeString(rawPayload)
		if err != nil {
			return "", nil, fmt.Errorf("payload must be valid base64")
		}
		payload = decoded
	}
	if targetID == "" {
		return "", nil, fmt.Errorf("target_id is required")
	}
	if op == tx.GovOpScopeAction && len(payload) == 0 {
		return "", nil, fmt.Errorf("scope_action requires either payload or scope")
	}
	return targetID, payload, nil
}

// handleDashboardGovVote handles POST /v1/dashboard/governance/vote.
func (h *DashboardHandler) handleDashboardGovVote(w http.ResponseWriter, r *http.Request) {
	if !h.requireDashboardGovernanceOperator(w, r) {
		return
	}
	if h.CometBFTRPC == "" || h.SigningKey == nil {
		writeError(w, http.StatusServiceUnavailable, "CometBFT consensus not configured")
		return
	}

	var req struct {
		ValidatorID      string `json:"validator_id,omitempty"`
		GovernanceDomain string `json:"governance_domain,omitempty"`
		ProposalID       string `json:"proposal_id"`
		Decision         string `json:"decision"`
	}
	rawBody, err := readDashboardGovernanceBody(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if err = json.Unmarshal(rawBody, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.ProposalID == "" {
		writeError(w, http.StatusBadRequest, "proposal_id is required")
		return
	}

	decision, err := parseDashboardVoteDecision(req.Decision)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	voteTx := &tx.ParsedTx{
		Type:      tx.TxTypeGovVote,
		Nonce:     tx.MonotonicNonce(h.SigningKey),
		Timestamp: time.Now(),
		GovVote: &tx.GovVote{
			ProposalID: req.ProposalID,
			Decision:   decision,
		},
	}

	if h.AppV20ActiveFn != nil && h.AppV20ActiveFn() {
		if len(h.AdminSigningKey) != ed25519.PrivateKeySize {
			writeError(w, http.StatusServiceUnavailable, "governance operator key not configured")
			return
		}
		req.ValidatorID, req.GovernanceDomain, err = h.dashboardGovernanceAuthorizationContext()
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		proofBody, marshalErr := json.Marshal(req)
		if marshalErr != nil {
			writeError(w, http.StatusInternalServerError, "failed to encode governance authorization")
			return
		}
		if err = embedDashboardGovernanceProof(voteTx, h.AdminSigningKey, r.Method, r.URL.RequestURI(), proofBody); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to authorize transaction")
			return
		}
	} else {
		embedDashboardAgentProof(voteTx, h.SigningKey)
	}
	if err = tx.SignTx(voteTx, h.SigningKey); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to sign transaction")
		return
	}
	encoded, err := tx.EncodeTx(voteTx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to encode transaction")
		return
	}

	txHash, _, _, err := broadcastTxCommitWeb(h.CometBFTRPC, encoded)
	if err != nil {
		writeError(w, http.StatusBadGateway, "governance vote rejected: "+err.Error())
		return
	}

	// Emit SSE event for real-time dashboard updates.
	if h.SSE != nil {
		h.SSE.Broadcast(SSEEvent{
			Type:    EventGovernance,
			Content: fmt.Sprintf("Vote cast: %s on %s", req.Decision, req.ProposalID),
			Data: map[string]any{
				"action":      "vote",
				"proposal_id": req.ProposalID,
				"decision":    req.Decision,
				"tx_hash":     txHash,
			},
		})
	}

	writeJSONResp(w, http.StatusOK, map[string]any{
		"tx_hash": txHash,
		"status":  "recorded",
	})
}

// --- Helpers -----------------------------------------------------------------

func (h *DashboardHandler) requireDashboardGovernanceOperator(w http.ResponseWriter, r *http.Request) bool {
	if !h.isCEREBRUMOperatorRequest(r) {
		writeError(w, http.StatusForbidden, "only the authenticated local node operator may mutate governance")
		return false
	}
	return true
}

func (h *DashboardHandler) dashboardGovernanceAuthorizationContext() (string, string, error) {
	if len(h.SigningKey) != ed25519.PrivateKeySize {
		return "", "", fmt.Errorf("live validator signing key not configured")
	}
	pub, ok := h.SigningKey.Public().(ed25519.PublicKey)
	if !ok || len(pub) != ed25519.PublicKeySize {
		return "", "", fmt.Errorf("live validator identity is invalid")
	}
	if h.GovernanceDomainFn == nil {
		return "", "", fmt.Errorf("app-v20 governance chain domain not configured")
	}
	domain := h.GovernanceDomainFn()
	decoded, err := hex.DecodeString(domain)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != domain {
		return "", "", fmt.Errorf("app-v20 governance chain domain is unavailable")
	}
	return auth.PublicKeyToAgentID(pub), domain, nil
}

func (h *DashboardHandler) dashboardCommittedGovernanceStatus(proposalID string) (string, error) {
	if h.BadgerStore == nil {
		return "unknown", nil
	}
	data, err := h.BadgerStore.GetGovProposal(proposalID)
	if err != nil {
		return "", fmt.Errorf("load proposal: %w", err)
	}
	if len(data) == 0 {
		return "", fmt.Errorf("proposal is absent after successful commit")
	}
	var proposal governance.ProposalState
	if err = json.Unmarshal(data, &proposal); err != nil {
		return "", fmt.Errorf("decode proposal: %w", err)
	}
	if proposal.ProposalID != proposalID {
		return "", fmt.Errorf("proposal id mismatch: got %q", proposal.ProposalID)
	}
	return string(proposal.Status), nil
}

func readDashboardGovernanceBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	return io.ReadAll(r.Body)
}

// embedDashboardGovernanceProof records the operator/admin as an action-bound
// authorizer while leaving the outer ParsedTx signature to the validator. This
// mirrors the signed REST gateway envelope and gives consensus exact
// method/path/body, freshness, and single-use nonce evidence.
func embedDashboardGovernanceProof(ptx *tx.ParsedTx, operatorKey ed25519.PrivateKey, method, path string, body []byte) error {
	if len(operatorKey) != ed25519.PrivateKeySize {
		return fmt.Errorf("operator key has length %d", len(operatorKey))
	}
	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		return fmt.Errorf("generate governance proof nonce: %w", err)
	}
	timestamp := time.Now().Unix()
	canonical := append([]byte(method+" "+path+"\n"), body...)
	bodyHash := sha256.Sum256(canonical)
	pub, ok := operatorKey.Public().(ed25519.PublicKey)
	if !ok {
		return fmt.Errorf("operator key has no Ed25519 public key")
	}
	ptx.AgentPubKey = append([]byte(nil), pub...)
	ptx.AgentSig = auth.SignRequestWithNonce(operatorKey, method, path, body, timestamp, nonce)
	ptx.AgentTimestamp = timestamp
	ptx.AgentBodyHash = append([]byte(nil), bodyHash[:]...)
	ptx.AgentNonce = append([]byte(nil), nonce...)
	ptx.AgentRequest = canonical
	return nil
}

// decodeJSONBody decodes JSON from the request body into the target.
func decodeJSONBody(r *http.Request, v any) error {
	return json.NewDecoder(r.Body).Decode(v)
}

func parseDashboardGovOp(s string) (tx.GovProposalOp, error) {
	switch s {
	case "add_validator":
		return tx.GovOpAddValidator, nil
	case "remove_validator":
		return tx.GovOpRemoveValidator, nil
	case "update_power":
		return tx.GovOpUpdatePower, nil
	case "sync_group_action":
		return tx.GovOpSyncGroupAction, nil
	case "scope_action":
		return tx.GovOpScopeAction, nil
	default:
		return 0, fmt.Errorf("operation must be one of: add_validator, remove_validator, update_power, sync_group_action, scope_action")
	}
}

func parseDashboardVoteDecision(s string) (tx.VoteDecision, error) {
	switch s {
	case "accept":
		return tx.VoteDecisionAccept, nil
	case "reject":
		return tx.VoteDecisionReject, nil
	case "abstain":
		return tx.VoteDecisionAbstain, nil
	default:
		return 0, fmt.Errorf("decision must be one of: accept, reject, abstain")
	}
}
