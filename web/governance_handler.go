package web

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/governance"
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
	if h.CometBFTRPC == "" || h.SigningKey == nil {
		writeError(w, http.StatusServiceUnavailable, "CometBFT consensus not configured")
		return
	}

	var req struct {
		Operation    string `json:"operation"`
		TargetID     string `json:"target_id"`
		TargetPubkey string `json:"target_pubkey,omitempty"`
		TargetPower  int64  `json:"target_power,omitempty"`
		ExpiryBlocks int64  `json:"expiry_blocks,omitempty"`
		Reason       string `json:"reason"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := decodeJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.Operation == "" {
		writeError(w, http.StatusBadRequest, "operation is required (add_validator, remove_validator, update_power)")
		return
	}
	if req.TargetID == "" {
		writeError(w, http.StatusBadRequest, "target_id is required")
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

	var pubKeyBytes []byte
	if req.TargetPubkey != "" {
		pubKeyBytes, err = hex.DecodeString(req.TargetPubkey)
		if err != nil {
			writeError(w, http.StatusBadRequest, "target_pubkey must be valid hex")
			return
		}
	}

	proposeTx := &tx.ParsedTx{
		Type:      tx.TxTypeGovPropose,
		Nonce:     uint64(time.Now().UnixNano()), //nolint:gosec // G115: UnixNano is always positive // #nosec G115 -- nonce from timestamp
		Timestamp: time.Now(),
		GovPropose: &tx.GovPropose{
			Operation:    op,
			TargetID:     req.TargetID,
			TargetPubKey: pubKeyBytes,
			TargetPower:  req.TargetPower,
			ExpiryBlocks: req.ExpiryBlocks,
			Reason:       req.Reason,
		},
	}

	embedDashboardAgentProof(proposeTx, h.SigningKey)
	if err = tx.SignTx(proposeTx, h.SigningKey); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to sign transaction")
		return
	}
	encoded, err := tx.EncodeTx(proposeTx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to encode transaction")
		return
	}

	if err = broadcastTxSync(h.CometBFTRPC, encoded); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to broadcast: "+err.Error())
		return
	}

	// Emit SSE event for real-time dashboard updates.
	if h.SSE != nil {
		h.SSE.Broadcast(SSEEvent{
			Type:    EventGovernance,
			Content: fmt.Sprintf("Proposal submitted: %s %s", req.Operation, req.TargetID),
			Data: map[string]any{
				"action":    "propose",
				"operation": req.Operation,
				"target_id": req.TargetID,
			},
		})
	}

	writeJSONResp(w, http.StatusOK, map[string]any{
		"status":  "submitted",
		"message": "Governance proposal submitted for consensus.",
	})
}

// handleDashboardGovVote handles POST /v1/dashboard/governance/vote.
func (h *DashboardHandler) handleDashboardGovVote(w http.ResponseWriter, r *http.Request) {
	if h.CometBFTRPC == "" || h.SigningKey == nil {
		writeError(w, http.StatusServiceUnavailable, "CometBFT consensus not configured")
		return
	}

	var req struct {
		ProposalID string `json:"proposal_id"`
		Decision   string `json:"decision"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := decodeJSONBody(r, &req); err != nil {
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
		Nonce:     uint64(time.Now().UnixNano()), //nolint:gosec // G115: UnixNano is always positive // #nosec G115 -- nonce from timestamp
		Timestamp: time.Now(),
		GovVote: &tx.GovVote{
			ProposalID: req.ProposalID,
			Decision:   decision,
		},
	}

	embedDashboardAgentProof(voteTx, h.SigningKey)
	if err = tx.SignTx(voteTx, h.SigningKey); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to sign transaction")
		return
	}
	encoded, err := tx.EncodeTx(voteTx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to encode transaction")
		return
	}

	if err = broadcastTxSync(h.CometBFTRPC, encoded); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to broadcast: "+err.Error())
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
			},
		})
	}

	writeJSONResp(w, http.StatusOK, map[string]any{
		"status":  "submitted",
		"message": "Governance vote submitted for consensus.",
	})
}

// --- Helpers -----------------------------------------------------------------

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
	default:
		return 0, fmt.Errorf("operation must be one of: add_validator, remove_validator, update_power")
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
