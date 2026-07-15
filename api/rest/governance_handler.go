package rest

import (
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/l33tdawg/sage/internal/scope"
	"github.com/l33tdawg/sage/internal/tx"
)

// --- Request / Response types ------------------------------------------------

// GovProposeRequest is the JSON body for POST /v1/governance/propose.
//
// Payload (added v8.0) is an optional base64-encoded blob carrying operation-
// specific data. For OpDomainReassign it's the JSON-encoded DomainReassign
// body {domain, new_owner_id, parent_domain, open_to_shared} that the
// executing TxTypeDomainReassign tx must reproduce byte-for-byte. App-v20
// scope_action accepts either a canonical binary scope record in Payload or a
// guided Scope template that the node canonicalizes. Legacy validator-set ops
// (add/remove/update_power) leave both empty.
type GovProposeRequest struct {
	Operation    string                  `json:"operation"`
	TargetID     string                  `json:"target_id"`
	TargetPubkey string                  `json:"target_pubkey,omitempty"`
	TargetPower  int64                   `json:"target_power,omitempty"`
	ExpiryBlocks int64                   `json:"expiry_blocks,omitempty"`
	Reason       string                  `json:"reason"`
	Payload      string                  `json:"payload,omitempty"`
	Scope        *scope.ProposalTemplate `json:"scope,omitempty"`
}

// GovProposeResponse is the JSON body for a successful proposal.
type GovProposeResponse struct {
	ProposalID string `json:"proposal_id"`
	TxHash     string `json:"tx_hash"`
	Status     string `json:"status"`
}

// GovVoteRequest is the JSON body for POST /v1/governance/vote.
type GovVoteRequest struct {
	ProposalID string `json:"proposal_id"`
	Decision   string `json:"decision"`
}

// GovVoteResponse is the JSON body for a successful governance vote.
type GovVoteResponse struct {
	TxHash string `json:"tx_hash"`
	Status string `json:"status"`
}

// GovCancelRequest is the JSON body for POST /v1/governance/cancel.
type GovCancelRequest struct {
	ProposalID string `json:"proposal_id"`
}

// GovCancelResponse is the JSON body for a successful governance cancel.
type GovCancelResponse struct {
	TxHash string `json:"tx_hash"`
	Status string `json:"status"`
}

// --- Handlers ----------------------------------------------------------------

// handleGovPropose handles POST /v1/governance/propose.
func (s *Server) handleGovPropose(w http.ResponseWriter, r *http.Request) {
	var req GovProposeRequest
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	if req.Operation == "" {
		writeProblem(w, http.StatusBadRequest, "Missing operation", "operation is required (add_validator, remove_validator, update_power, domain_reassign, memory_domain_repair, sync_group_action, scope_action).")
		return
	}
	if req.Reason == "" {
		writeProblem(w, http.StatusBadRequest, "Missing reason", "reason is required.")
		return
	}

	op, err := parseGovOp(req.Operation)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid operation", err.Error())
		return
	}
	if op == tx.GovOpScopeAction && !s.isPostV20ForNextTx() {
		writeProblem(w, http.StatusConflict, "Scope governance is not active", "scope_action requires app-v20 activation.")
		return
	}

	var pubKeyBytes []byte
	if req.TargetPubkey != "" {
		pubKeyBytes, err = hex.DecodeString(req.TargetPubkey)
		if err != nil {
			writeProblem(w, http.StatusBadRequest, "Invalid target_pubkey", "target_pubkey must be valid hex.")
			return
		}
	}

	// v8.0 payloads remain byte-for-byte base64. For app-v20 scope_action,
	// structured input is converted into the same canonical binary record and
	// the target ID can be derived from scope_id.
	var payloadBytes []byte
	req.TargetID, payloadBytes, err = resolveGovProposalPayload(op, req.TargetID, req.Payload, req.Scope)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid governance payload", err.Error())
		return
	}

	proposeTx := &tx.ParsedTx{
		Type:      tx.TxTypeGovPropose,
		Nonce:     tx.MonotonicNonce(s.signingKey),
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

	// Embed agent's cryptographic proof for on-chain identity verification.
	s.embedAgentAuth(r.Context(), proposeTx)

	// Sign the transaction with the node's signing key.
	if err = tx.SignTx(proposeTx, s.signingKey); err != nil {
		s.logger.Error().Err(err).Msg("failed to sign gov propose tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}

	encoded, err := tx.EncodeTx(proposeTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode gov propose tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}

	txHash, err := s.broadcastTxCommit(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast gov propose tx")
		status, publicMsg := broadcastErrorPublic(err)
		writeProblem(w, status, "Broadcast error", publicMsg)
		return
	}

	if s.OnEvent != nil {
		s.OnEvent("governance", "", "", "Proposal submitted: "+req.Operation+" "+req.TargetID, map[string]any{
			"tx_hash":   txHash,
			"operation": req.Operation,
			"target_id": req.TargetID,
		})
	}

	writeJSON(w, http.StatusOK, GovProposeResponse{
		ProposalID: txHash, // ABCI computes the deterministic proposal ID; tx_hash is the on-chain reference
		TxHash:     txHash,
		Status:     "voting",
	})
}

func resolveGovProposalPayload(op tx.GovProposalOp, targetID, rawPayload string, template *scope.ProposalTemplate) (string, []byte, error) {
	if template != nil {
		if op != tx.GovOpScopeAction {
			return "", nil, errors.New("scope is only valid for scope_action")
		}
		if rawPayload != "" {
			return "", nil, errors.New("payload and scope are mutually exclusive")
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
			return "", nil, errors.New("payload must be valid base64")
		}
		payload = decoded
	}
	if targetID == "" {
		return "", nil, errors.New("target_id is required")
	}
	if op == tx.GovOpScopeAction && len(payload) == 0 {
		return "", nil, errors.New("scope_action requires either payload or scope")
	}
	return targetID, payload, nil
}

// handleGovVote handles POST /v1/governance/vote.
func (s *Server) handleGovVote(w http.ResponseWriter, r *http.Request) {
	var req GovVoteRequest
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	if req.ProposalID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing proposal_id", "proposal_id is required.")
		return
	}

	decision, err := parseVoteDecision(req.Decision)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid decision", err.Error())
		return
	}

	voteTx := &tx.ParsedTx{
		Type:      tx.TxTypeGovVote,
		Nonce:     tx.MonotonicNonce(s.signingKey),
		Timestamp: time.Now(),
		GovVote: &tx.GovVote{
			ProposalID: req.ProposalID,
			Decision:   decision,
		},
	}

	// Embed agent's cryptographic proof for on-chain identity verification.
	s.embedAgentAuth(r.Context(), voteTx)

	// Sign the transaction with the node's signing key.
	if err = tx.SignTx(voteTx, s.signingKey); err != nil {
		s.logger.Error().Err(err).Msg("failed to sign gov vote tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}

	encoded, err := tx.EncodeTx(voteTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode gov vote tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}

	txHash, err := s.broadcastTxCommit(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast gov vote tx")
		status, publicMsg := broadcastErrorPublic(err)
		writeProblem(w, status, "Broadcast error", publicMsg)
		return
	}

	if s.OnEvent != nil {
		s.OnEvent("governance", "", "", "Vote cast: "+req.Decision+" on "+req.ProposalID, map[string]any{
			"tx_hash":     txHash,
			"proposal_id": req.ProposalID,
			"decision":    req.Decision,
		})
	}

	writeJSON(w, http.StatusOK, GovVoteResponse{
		TxHash: txHash,
		Status: "recorded",
	})
}

// handleGovCancel handles POST /v1/governance/cancel.
func (s *Server) handleGovCancel(w http.ResponseWriter, r *http.Request) {
	var req GovCancelRequest
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	if req.ProposalID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing proposal_id", "proposal_id is required.")
		return
	}

	cancelTx := &tx.ParsedTx{
		Type:      tx.TxTypeGovCancel,
		Nonce:     tx.MonotonicNonce(s.signingKey),
		Timestamp: time.Now(),
		GovCancel: &tx.GovCancel{
			ProposalID: req.ProposalID,
		},
	}

	// Embed agent's cryptographic proof for on-chain identity verification.
	s.embedAgentAuth(r.Context(), cancelTx)

	// Sign the transaction with the node's signing key.
	if err := tx.SignTx(cancelTx, s.signingKey); err != nil {
		s.logger.Error().Err(err).Msg("failed to sign gov cancel tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}

	encoded, err := tx.EncodeTx(cancelTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode gov cancel tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}

	txHash, err := s.broadcastTxCommit(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast gov cancel tx")
		status, publicMsg := broadcastErrorPublic(err)
		writeProblem(w, status, "Broadcast error", publicMsg)
		return
	}

	if s.OnEvent != nil {
		s.OnEvent("governance", "", "", "Proposal cancelled: "+req.ProposalID, map[string]any{
			"tx_hash":     txHash,
			"proposal_id": req.ProposalID,
		})
	}

	writeJSON(w, http.StatusOK, GovCancelResponse{
		TxHash: txHash,
		Status: "cancelled",
	})
}

// --- Helpers -----------------------------------------------------------------

func parseGovOp(s string) (tx.GovProposalOp, error) {
	switch s {
	case "add_validator":
		return tx.GovOpAddValidator, nil
	case "remove_validator":
		return tx.GovOpRemoveValidator, nil
	case "update_power":
		return tx.GovOpUpdatePower, nil
	case "domain_reassign":
		return tx.GovOpDomainReassign, nil
	case "memory_domain_repair":
		// app-v16: backfill legacy memories' on-chain domain. Payload is a base64
		// JSON [{"memory_id","domain"}] array (like domain_reassign, this op is
		// REST-only — the MCP tool exposes only the payload-less validator ops).
		return tx.GovOpMemoryDomainRepair, nil
	case "sync_group_action":
		return tx.GovOpSyncGroupAction, nil
	case "scope_action":
		return tx.GovOpScopeAction, nil
	default:
		return 0, fmt.Errorf("operation must be one of: add_validator, remove_validator, update_power, domain_reassign, memory_domain_repair, sync_group_action, scope_action")
	}
}
