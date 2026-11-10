package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/auth"
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
		Type: tx.TxTypeAccessRequest,
		AccessRequest: &tx.AccessRequest{
			RequesterID:    agentID,
			TargetDomain:   req.TargetDomain,
			Justification:  req.Justification,
			RequestedLevel: uint8(req.RequestedLevel), // #nosec G115 -- validated small int
		},
	}

	s.embedAgentAuth(r.Context(), accessTx)

	var txHash string
	stage, err := s.submitConsensusTx(r.Context(), accessTx, func(encoded []byte) error {
		var submitErr error
		txHash, submitErr = s.broadcastTxCommit(encoded)
		return submitErr
	})
	if err != nil {
		s.writeConsensusTxError(w, stage, "access request", err)
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
		Type: tx.TxTypeAccessGrant,
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

	var txHash string
	stage, err := s.submitConsensusTx(r.Context(), grantTx, func(encoded []byte) error {
		var submitErr error
		txHash, submitErr = s.broadcastTxCommit(encoded)
		return submitErr
	})
	if err != nil {
		s.writeConsensusTxError(w, stage, "access grant", err)
		return
	}
	// The dashboard stream has no subscriber identity, so this is deliberately
	// a payload-free cache-invalidation tick. Each client must re-fetch its
	// caller-filtered connectome projection after the grant is committed.
	if s.OnEvent != nil {
		s.OnEvent("access", "", "", "", nil)
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
		Type: tx.TxTypeAccessRevoke,
		AccessRevoke: &tx.AccessRevoke{
			RevokerID: agentID,
			GranteeID: req.GranteeID,
			Domain:    req.Domain,
			Reason:    req.Reason,
		},
	}

	s.embedAgentAuth(r.Context(), revokeTx)

	var txHash string
	stage, err := s.submitConsensusTx(r.Context(), revokeTx, func(encoded []byte) error {
		var submitErr error
		txHash, submitErr = s.broadcastTxCommit(encoded)
		return submitErr
	})
	if err != nil {
		s.writeConsensusTxError(w, stage, "access revoke", err)
		return
	}
	// See handleAccessGrant: authorization details must never enter the global
	// dashboard stream. The empty tick only invalidates caller-scoped caches.
	if s.OnEvent != nil {
		s.OnEvent("access", "", "", "", nil)
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "revoked",
		"tx_hash": txHash,
	})
}

// handleListGrants handles GET /v1/access/grants/{agent_id}.
//
// Post-app-v23, Badger is the sole source of authorization values. The SQL
// accessStore is consulted only for legacy descriptive metadata after the
// canonical rows have been read; it can never add, omit, or override a grant.
func (s *Server) handleListGrants(w http.ResponseWriter, r *http.Request) {
	agentID := chi.URLParam(r, "agent_id")
	if agentID == "" {
		agentID = middleware.ContextAgentID(r.Context())
	}
	postV23 := s.isPostV23ForNextTx()
	if postV23 {
		callerID := middleware.ContextAgentID(r.Context())
		if callerID == "" {
			writeProblem(w, http.StatusForbidden, "Agent identity required",
				"An authenticated local identity is required to list access grants.")
			return
		}
		isRoot, rootErr := s.appV23IsRootIdentity(callerID)
		if rootErr != nil {
			writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
				"Current CEREBRUM Root credential history could not be verified.")
			return
		}
		if isRoot {
			writeProblem(w, http.StatusForbidden, "Root is not an agent",
				"CEREBRUM Root credentials cannot use the ordinary-agent grant-list surface.")
			return
		}
		targetIsRoot, targetRootErr := s.appV23IsRootIdentity(agentID)
		if targetRootErr != nil {
			writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
				"Target CEREBRUM Root credential history could not be verified.")
			return
		}
		if targetIsRoot {
			writeProblem(w, http.StatusForbidden, "Root is not an agent",
				"CEREBRUM Root credentials cannot be targeted through the ordinary-agent grant-list surface.")
			return
		}
		if agentID != callerID &&
			!s.callerIsOperatorOrAdmin(r.Context(), callerID) {
			writeProblem(w, http.StatusForbidden, "Access denied",
				"Members and Managers may list only their own access grants; a current local Admin may inspect another local agent.")
			return
		}
	}

	if postV23 {
		if s.badgerStore == nil {
			writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
				"Committed grant state is unavailable.")
			return
		}
		rows, err := s.badgerStore.ListAccessGrantsForAgent(agentID)
		if err != nil {
			s.logger.Error().Err(err).Str("agent_id", agentID).
				Msg("failed to read canonical direct grants")
			writeProblem(w, http.StatusServiceUnavailable, "Query unavailable",
				"Committed access grants could not be read.")
			return
		}
		now := time.Now().Unix()
		grants := make([]store.DirectGrantObservation, 0, len(rows))
		for _, row := range rows {
			if row.ExpiresAt != 0 && row.ExpiresAt <= now {
				continue
			}
			grants = append(grants, store.DirectGrantObservation{
				Domain:      row.Domain,
				GranteeID:   row.AgentID,
				GranterID:   row.GranterID,
				AccessLevel: row.Level,
				ExpiresAt:   row.ExpiresAt,
			})
		}
		writeJSON(w, http.StatusOK, s.directGrantEntries(r.Context(), agentID, grants))
		return
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
		for _, grant := range grants {
			if grant == nil {
				continue
			}
			level, expiresAt, granterID, grantErr := s.badgerStore.GetAccessGrant(grant.Domain, grant.GranteeID)
			if grantErr != nil {
				s.logger.Warn().Str("domain", grant.Domain).Str("grantee", grant.GranteeID).
					Msg("dropping stale mirror grant absent from chain")
				continue
			}
			grant.Level = level
			grant.GranterID = granterID
			grant.ExpiresAt = nil
			if expiresAt > 0 {
				expires := time.Unix(expiresAt, 0).UTC()
				grant.ExpiresAt = &expires
			}
			live = append(live, grant)
		}
		grants = live
	}
	writeJSON(w, http.StatusOK, grants)
}

// handleDirectGrantsSnapshot returns one bounded, complete active direct-grant
// observation for one target. Every returned authorization-relevant value comes
// from the same committed Badger read transaction.
func (s *Server) handleDirectGrantsSnapshot(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "private, no-store")
	if r.URL.RawQuery != "" {
		writeProblem(w, http.StatusBadRequest, "Invalid snapshot request",
			"Direct-grant snapshots do not accept query parameters.")
		return
	}
	if !s.isPostV23ForNextTx() {
		writeProblem(w, http.StatusConflict, "App-v23 required",
			"Direct-grant snapshots require the app-v23 authorization state.")
		return
	}
	callerID := middleware.ContextAgentID(r.Context())
	if callerID == "" {
		writeProblem(w, http.StatusForbidden, "Agent identity required",
			"An authenticated local identity is required to inspect direct grants.")
		return
	}
	target := chi.URLParam(r, "agent_id")
	pub, err := auth.AgentIDToPublicKey(target)
	if err != nil || auth.PublicKeyToAgentID(pub) != target {
		writeProblem(w, http.StatusBadRequest, "Invalid target",
			"A canonical local agent ID is required.")
		return
	}
	if s.badgerStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Inspection unavailable",
			"Committed grant state is unavailable.")
		return
	}
	snapshot, err := s.badgerStore.InspectDirectGrants(r.Context(), callerID, target, time.Now().Unix())
	if err != nil {
		s.writeDirectGrantsSnapshotError(w, err)
		return
	}
	raw, err := json.Marshal(snapshot)
	if err != nil || len(raw) > 1<<20 {
		s.logger.Error().Err(err).Int("response_bytes", len(raw)).
			Msg("committed direct-grant snapshot exceeded its response budget")
		writeProblem(w, http.StatusServiceUnavailable, "Inspection unavailable",
			"The complete committed grant snapshot exceeds the response budget.")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

func (s *Server) directGrantEntries(
	ctx context.Context, target string, grants []store.DirectGrantObservation,
) []store.AccessGrantEntry {
	metadata := make(map[string]*store.AccessGrantEntry)
	if s.accessStore != nil {
		if mirrored, err := s.accessStore.GetActiveGrants(ctx, target); err == nil {
			for _, grant := range mirrored {
				if grant != nil {
					metadata[grant.Domain] = grant
				}
			}
		} else {
			s.logger.Warn().Err(err).Str("agent_id", target).
				Msg("grant metadata mirror unavailable; returning canonical rows only")
		}
	}

	out := make([]store.AccessGrantEntry, 0, len(grants))
	for _, grant := range grants {
		entry := store.AccessGrantEntry{
			Domain:    grant.Domain,
			GranteeID: grant.GranteeID,
			GranterID: grant.GranterID,
			Level:     grant.AccessLevel,
		}
		if grant.ExpiresAt > 0 {
			expiresAt := time.Unix(grant.ExpiresAt, 0).UTC()
			entry.ExpiresAt = &expiresAt
		}
		if mirrored := metadata[grant.Domain]; mirrored != nil {
			entry.CreatedHeight = mirrored.CreatedHeight
			entry.CreatedAt = mirrored.CreatedAt
		}
		out = append(out, entry)
	}
	return out
}

func (s *Server) writeDirectGrantsSnapshotError(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrDirectGrantsInspectionDenied) {
		writeProblem(w, http.StatusForbidden, "Inspection denied",
			"Inspection requires an active ordinary caller and an active ordinary target, with a current local Admin for cross-agent reads.")
		return
	}
	s.logger.Error().Err(err).Msg("failed to read committed direct-grant snapshot")
	writeProblem(w, http.StatusServiceUnavailable, "Inspection unavailable",
		"A complete committed grant snapshot could not be read.")
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
		Type: tx.TxTypeDomainRegister,
		DomainRegister: &tx.DomainRegister{
			DomainName:   req.Name,
			OwnerAgentID: agentID,
			Description:  req.Description,
			ParentDomain: req.Parent,
		},
	}

	s.embedAgentAuth(r.Context(), domainTx)

	var txHash string
	stage, err := s.submitConsensusTx(r.Context(), domainTx, func(encoded []byte) error {
		var submitErr error
		txHash, submitErr = s.broadcastTxCommit(encoded)
		return submitErr
	})
	if err != nil {
		s.writeConsensusTxError(w, stage, "domain register", err)
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
