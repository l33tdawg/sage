package rest

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/tx"
)

// DomainReassignRequest is the JSON body for POST /v1/domain/reassign.
//
// The execution tx for SAGE v8.0's access-control recovery primitive. The
// caller submits this *after* a successful gov_propose(domain_reassign) +
// 3/4 supermajority vote sequence — the ProposalID links the tx to the
// on-chain decision record. ABCI verifies the body matches the proposal
// payload byte-for-byte and re-checks the admin gate as defence-in-depth.
type DomainReassignRequest struct {
	Domain       string `json:"domain"`
	NewOwnerID   string `json:"new_owner_id"`
	ParentDomain string `json:"parent_domain,omitempty"`
	ProposalID   string `json:"proposal_id"`
	OpenToShared bool   `json:"open_to_shared,omitempty"`
}

// DomainReassignResponse is returned on HTTP 200. PurgedGrants comes from
// the ABCI success log — the previous owner's chain-of-trust is wiped on
// transfer, and the count surfaces so operators can audit the blast radius.
type DomainReassignResponse struct {
	TxHash       string `json:"tx_hash"`
	PurgedGrants int    `json:"purged_grants"`
}

// domainReassignPurgeRe extracts the grant count from processDomainReassign's
// success Log. The handler emits: "domain reassigned: <d> -> <o> (purged N grants, open_to_shared=<bool>)".
// We log-parse rather than re-plumb the ExecTxResult Data field — Data is a
// hex string in CometBFT's wire format, the log line is already part of the
// existing observability contract, and a single regex is cheaper than another
// hop through the ABCI types. If the format ever changes the test pins it.
var domainReassignPurgeRe = regexp.MustCompile(`purged\s+(\d+)\s+grants`)

// handleDomainReassign handles POST /v1/domain/reassign.
//
// Wire surface for TxTypeDomainReassign. Mirrors handleAccessGrant for the
// sign-encode-broadcast trio, then surfaces the FinalizeBlock log to the
// caller on rejection — domain reassign failures are operational signals
// (proposal not found, body mismatch, TTL expired, parent mismatch) and the
// abstracted "request rejected" sanitization from broadcastErrorPublic
// would strip the diagnostic an admin needs to fix their submission.
func (s *Server) handleDomainReassign(w http.ResponseWriter, r *http.Request) {
	var req DomainReassignRequest
	err := decodeJSON(r, &req)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	if req.Domain == "" {
		writeProblem(w, http.StatusBadRequest, "Missing domain", "domain is required")
		return
	}
	if req.NewOwnerID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing new_owner_id", "new_owner_id is required")
		return
	}
	if decoded, hexErr := hex.DecodeString(req.NewOwnerID); hexErr != nil || len(decoded) != 32 {
		writeProblem(w, http.StatusBadRequest, "Invalid new_owner_id", "new_owner_id must be hex(64) — 32-byte Ed25519 agent ID")
		return
	}
	if req.ProposalID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing proposal_id", "proposal_id is required")
		return
	}
	if decoded, hexErr := hex.DecodeString(req.ProposalID); hexErr != nil || len(decoded) == 0 {
		writeProblem(w, http.StatusBadRequest, "Invalid proposal_id", "proposal_id must be valid hex")
		return
	}

	// Submitter context is preserved on the ParsedTx via embedAgentAuth — the
	// ABCI handler will re-derive the senderID from the Ed25519 proof and
	// enforce the admin-role gate independently. We don't trust the REST
	// layer to attest the submitter's role.

	reassignTx := &tx.ParsedTx{
		Type:      tx.TxTypeDomainReassign,
		Nonce:     tx.MonotonicNonce(s.signingKey),
		Timestamp: time.Now(),
		DomainReassign: &tx.DomainReassign{
			Domain:       req.Domain,
			NewOwnerID:   req.NewOwnerID,
			ParentDomain: req.ParentDomain,
			ProposalID:   req.ProposalID,
			OpenToShared: req.OpenToShared,
		},
	}

	embedAgentAuth(r.Context(), reassignTx)

	err = tx.SignTx(reassignTx, s.signingKey)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to sign domain reassign tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}

	encoded, err := tx.EncodeTx(reassignTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode domain reassign tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}

	txHash, txLog, err := s.broadcastTxCommitWithLog(encoded)
	if err != nil {
		s.logger.Error().Err(err).Str("domain", req.Domain).Msg("failed to broadcast domain reassign tx")
		status, publicMsg := domainReassignErrorPublic(err)
		writeProblem(w, status, "Broadcast error", publicMsg)
		return
	}

	purged := parsePurgedGrants(txLog)

	if s.OnEvent != nil {
		s.OnEvent("governance", "", "", "Domain reassigned: "+req.Domain+" -> "+req.NewOwnerID, map[string]any{
			"tx_hash":       txHash,
			"domain":        req.Domain,
			"new_owner_id":  req.NewOwnerID,
			"proposal_id":   req.ProposalID,
			"purged_grants": purged,
		})
	}

	writeJSON(w, http.StatusOK, DomainReassignResponse{
		TxHash:       txHash,
		PurgedGrants: purged,
	})
}

// parsePurgedGrants extracts the grant count from processDomainReassign's
// success Log. Returns 0 if the log doesn't match (defensive — we never
// want a missing count to fail an otherwise-successful response).
func parsePurgedGrants(log string) int {
	m := domainReassignPurgeRe.FindStringSubmatch(log)
	if len(m) < 2 {
		return 0
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return 0
	}
	return n
}

// domainReassignErrorPublic maps broadcast errors to HTTP statuses for the
// /v1/domain/reassign endpoint. Unlike the generic broadcastErrorPublic which
// sanitizes all FinalizeBlock rejections to "request rejected", this path
// surfaces the upstream Log as the problem detail — operators need to see
// "proposal not found", "proposal already consumed", "body mismatch", etc.
// to fix their submission. The Log is already vetted at the ABCI layer and
// doesn't leak agent-id oracles the way memory-tx errors do.
func domainReassignErrorPublic(err error) (int, string) {
	if err == nil {
		return http.StatusInternalServerError, "internal error"
	}
	msg := err.Error()

	switch {
	case strings.Contains(msg, "agent identity verification failed"):
		return http.StatusUnauthorized, msg
	case strings.Contains(msg, "unknown tx type"):
		// Pre-fork rejection (CheckTx Code 10 or FinalizeBlock Code 10) —
		// the chain hasn't activated v8 so the tx type isn't recognised.
		// 403 because the caller is asking for a capability the chain
		// hasn't authorised yet; not 400 (the body is well-formed).
		return http.StatusForbidden, msg
	case strings.Contains(msg, "tx rejected in CheckTx"):
		// Other CheckTx rejections (invalid signature, nonce, decode) mean
		// the request itself was malformed — 400.
		return http.StatusBadRequest, msg
	case strings.Contains(msg, "tx rejected in FinalizeBlock"):
		// Every documented FinalizeBlock failure mode (proposal not found,
		// proposal not executed, body mismatch, already consumed, TTL
		// expired, domain doesn't exist, parent mismatch, not admin, wrong
		// operation type) is a 403 — the admin submitted an unauthorized
		// or stale request.
		return http.StatusForbidden, msg
	}
	return http.StatusInternalServerError, msg
}

// broadcastTxCommitWithLog is broadcastTxCommit plus the FinalizeBlock log
// surfaced on success. Domain reassign needs the success log to parse the
// purged-grants count; reusing broadcastTxCommit would discard it.
func (s *Server) broadcastTxCommitWithLog(txBytes []byte) (string, string, error) {
	txHex := hex.EncodeToString(txBytes)
	url := fmt.Sprintf("%s/broadcast_tx_commit?tx=0x%s", s.cometbftRPC, txHex)

	ctx, cancel := context.WithTimeout(context.Background(), broadcastTxCommitTimeout())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil) // #nosec G107 -- internal CometBFT RPC
	if err != nil {
		return "", "", fmt.Errorf("create broadcast request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("broadcast tx commit: %w", err)
	}
	defer resp.Body.Close()

	var result cometCommitResponse
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", "", fmt.Errorf("decode broadcast commit response: %w", err)
	}

	if result.Error != nil {
		return "", "", fmt.Errorf("broadcast error: %s", result.Error.Message)
	}

	if result.Result.CheckTx.Code != 0 {
		return "", "", fmt.Errorf("tx rejected in CheckTx (code %d): %s", result.Result.CheckTx.Code, result.Result.CheckTx.Log)
	}

	if result.Result.TxResult.Code != 0 {
		return "", "", fmt.Errorf("tx rejected in FinalizeBlock (code %d): %s", result.Result.TxResult.Code, result.Result.TxResult.Log)
	}

	return result.Result.Hash, result.Result.TxResult.Log, nil
}
