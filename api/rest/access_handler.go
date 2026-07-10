package rest

import (
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// --- Request types -----------------------------------------------------------

// AccessRequestReq is the JSON body for POST /v1/access/request.
type AccessRequestReq struct {
	TargetDomain   string `json:"target_domain"`
	Justification  string `json:"justification,omitempty"`
	RequestedLevel int    `json:"requested_level,omitempty"`
}

// AccessGrantReq is the JSON body for POST /v1/access/grant.
type AccessGrantReq struct {
	GranteeID string `json:"grantee_id"`
	Domain    string `json:"domain"`
	Level     int    `json:"level,omitempty"`
	ExpiresAt int64  `json:"expires_at,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

// AccessRevokeReq is the JSON body for POST /v1/access/revoke.
type AccessRevokeReq struct {
	GranteeID string `json:"grantee_id"`
	Domain    string `json:"domain"`
	Reason    string `json:"reason,omitempty"`
}

// DomainRegisterReq is the JSON body for POST /v1/domain/register.
type DomainRegisterReq struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Parent      string `json:"parent,omitempty"`
}

// --- Handlers ----------------------------------------------------------------

// handleAccessRequest handles POST /v1/access/request.
func (s *Server) handleAccessRequest(w http.ResponseWriter, r *http.Request) {
	var req AccessRequestReq
	err := decodeJSON(r, &req)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	if req.TargetDomain == "" {
		writeProblem(w, http.StatusBadRequest, "Missing target domain", "target_domain is required")
		return
	}
	if req.RequestedLevel == 0 {
		req.RequestedLevel = 1
	}

	agentID := middleware.ContextAgentID(r.Context())

	accessTx := &tx.ParsedTx{
		Type:      tx.TxTypeAccessRequest,
		Nonce:     tx.MonotonicNonce(s.signingKey),
		Timestamp: time.Now(),
		AccessRequest: &tx.AccessRequest{
			RequesterID:    agentID,
			TargetDomain:   req.TargetDomain,
			Justification:  req.Justification,
			RequestedLevel: uint8(req.RequestedLevel), // #nosec G115 -- validated small int
		},
	}

	s.embedAgentAuth(r.Context(), accessTx)

	err = tx.SignTx(accessTx, s.signingKey)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to sign access request tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}

	encoded, err := tx.EncodeTx(accessTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode access request tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}

	txHash, err := s.broadcastTxCommit(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast access request tx")
		status, publicMsg := broadcastErrorPublic(err)
		writeProblem(w, status, "Broadcast error", publicMsg)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"status":  "pending",
		"tx_hash": txHash,
	})
}

// handleAccessGrant handles POST /v1/access/grant.
func (s *Server) handleAccessGrant(w http.ResponseWriter, r *http.Request) {
	var req AccessGrantReq
	err := decodeJSON(r, &req)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	if req.GranteeID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing grantee ID", "grantee_id is required")
		return
	}
	if req.Domain == "" {
		writeProblem(w, http.StatusBadRequest, "Missing domain", "domain is required")
		return
	}
	if req.Level == 0 {
		req.Level = 1
	}

	agentID := middleware.ContextAgentID(r.Context())

	grantTx := &tx.ParsedTx{
		Type:      tx.TxTypeAccessGrant,
		Nonce:     tx.MonotonicNonce(s.signingKey),
		Timestamp: time.Now(),
		AccessGrant: &tx.AccessGrant{
			GranterID: agentID,
			GranteeID: req.GranteeID,
			Domain:    req.Domain,
			Level:     uint8(req.Level), // #nosec G115 -- validated small int
			ExpiresAt: req.ExpiresAt,
			RequestID: req.RequestID,
		},
	}

	s.embedAgentAuth(r.Context(), grantTx)

	err = tx.SignTx(grantTx, s.signingKey)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to sign access grant tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}

	encoded, err := tx.EncodeTx(grantTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode access grant tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}

	txHash, err := s.broadcastTxCommit(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast access grant tx")
		status, publicMsg := broadcastErrorPublic(err)
		writeProblem(w, status, "Broadcast error", publicMsg)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"status":  "granted",
		"tx_hash": txHash,
	})
}

// handleAccessRevoke handles POST /v1/access/revoke.
func (s *Server) handleAccessRevoke(w http.ResponseWriter, r *http.Request) {
	var req AccessRevokeReq
	err := decodeJSON(r, &req)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	if req.GranteeID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing grantee ID", "grantee_id is required")
		return
	}
	if req.Domain == "" {
		writeProblem(w, http.StatusBadRequest, "Missing domain", "domain is required")
		return
	}

	agentID := middleware.ContextAgentID(r.Context())

	revokeTx := &tx.ParsedTx{
		Type:      tx.TxTypeAccessRevoke,
		Nonce:     tx.MonotonicNonce(s.signingKey),
		Timestamp: time.Now(),
		AccessRevoke: &tx.AccessRevoke{
			RevokerID: agentID,
			GranteeID: req.GranteeID,
			Domain:    req.Domain,
			Reason:    req.Reason,
		},
	}

	s.embedAgentAuth(r.Context(), revokeTx)

	err = tx.SignTx(revokeTx, s.signingKey)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to sign access revoke tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}

	encoded, err := tx.EncodeTx(revokeTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode access revoke tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}

	txHash, err := s.broadcastTxCommit(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast access revoke tx")
		status, publicMsg := broadcastErrorPublic(err)
		writeProblem(w, status, "Broadcast error", publicMsg)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "revoked",
		"tx_hash": txHash,
	})
}

// handleListGrants handles GET /v1/access/grants/{agent_id}.
//
// Reconciles the off-chain accessStore mirror against Badger (chain truth)
// — a row that survives in the mirror after a chain reset / Badger wipe
// can't actually be exercised, and surfacing it would mislead callers into
// believing they hold access the chain doesn't honour.
func (s *Server) handleListGrants(w http.ResponseWriter, r *http.Request) {
	agentID := chi.URLParam(r, "agent_id")
	if agentID == "" {
		agentID = middleware.ContextAgentID(r.Context())
	}

	if s.accessStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Access store unavailable",
			"Access control storage is not configured.")
		return
	}

	grants, err := s.accessStore.GetActiveGrants(r.Context(), agentID)
	if err != nil {
		s.logger.Error().Err(err).Str("agent_id", agentID).Msg("failed to get grants")
		writeProblem(w, http.StatusInternalServerError, "Query error", "Failed to query access grants.")
		return
	}

	if s.badgerStore != nil {
		live := grants[:0]
		for _, g := range grants {
			if _, _, _, gErr := s.badgerStore.GetAccessGrant(g.Domain, g.GranteeID); gErr == nil {
				live = append(live, g)
			} else {
				s.logger.Warn().Str("domain", g.Domain).Str("grantee", g.GranteeID).Msg("dropping stale mirror grant absent from chain")
			}
		}
		grants = live
	}

	writeJSON(w, http.StatusOK, grants)
}

// handleDomainRegister handles POST /v1/domain/register.
func (s *Server) handleDomainRegister(w http.ResponseWriter, r *http.Request) {
	var req DomainRegisterReq
	err := decodeJSON(r, &req)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	if req.Name == "" {
		writeProblem(w, http.StatusBadRequest, "Missing domain name", "name is required")
		return
	}

	agentID := middleware.ContextAgentID(r.Context())

	domainTx := &tx.ParsedTx{
		Type:      tx.TxTypeDomainRegister,
		Nonce:     tx.MonotonicNonce(s.signingKey),
		Timestamp: time.Now(),
		DomainRegister: &tx.DomainRegister{
			DomainName:   req.Name,
			OwnerAgentID: agentID,
			Description:  req.Description,
			ParentDomain: req.Parent,
		},
	}

	s.embedAgentAuth(r.Context(), domainTx)

	err = tx.SignTx(domainTx, s.signingKey)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to sign domain register tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}

	encoded, err := tx.EncodeTx(domainTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode domain register tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}

	txHash, err := s.broadcastTxCommit(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast domain register tx")
		status, publicMsg := broadcastErrorPublic(err)
		writeProblem(w, status, "Broadcast error", publicMsg)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"status":  "registered",
		"tx_hash": txHash,
	})
}

// handleGetDomain handles GET /v1/domain/{name}.
//
// Ownership comes from BadgerDB (chain-authoritative). The off-chain
// accessStore mirror is only consulted to enrich description and created_at
// — fields not stored on chain. Returning the mirror's owner_agent_id
// directly would let stale mirrors (e.g. chain reset that didn't drop the
// accessStore tables) tell callers they own a domain the chain rejects on,
// producing the classic "submit_observation passes, grant_access fails with
// code 34" footgun.
func (s *Server) handleGetDomain(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if name == "" {
		writeProblem(w, http.StatusBadRequest, "Missing domain name", "name path parameter is required")
		return
	}

	if s.badgerStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Chain store unavailable",
			"On-chain state is not configured.")
		return
	}

	ownerID, parent, height, err := s.badgerStore.GetDomainOwnerAndMeta(name)
	if err != nil {
		writeProblem(w, http.StatusNotFound, "Domain not found",
			fmt.Sprintf("No domain found with name %s", name))
		return
	}

	resp := &store.DomainEntry{
		DomainName:    name,
		OwnerAgentID:  ownerID,
		ParentDomain:  parent,
		CreatedHeight: height,
	}

	if s.accessStore != nil {
		if mirror, mirrorErr := s.accessStore.GetDomain(r.Context(), name); mirrorErr == nil && mirror != nil {
			if mirror.OwnerAgentID != "" && mirror.OwnerAgentID != ownerID {
				s.logger.Warn().Str("domain", name).Str("chain_owner", ownerID).Str("mirror_owner", mirror.OwnerAgentID).Msg("accessStore mirror disagrees with chain on domain owner — serving chain value")
			}
			resp.Description = mirror.Description
			resp.CreatedAt = mirror.CreatedAt
		}
	}

	writeJSON(w, http.StatusOK, resp)
}
