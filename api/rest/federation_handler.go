package rest

import (
	"encoding/hex"
	"net/http"
	"net/url"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/tx"
)

// maxRemoteChainIDLenREST mirrors the consensus maxRemoteChainIDLen so the JOIN
// builder fails fast instead of after a wasted stage+broadcast.
const maxRemoteChainIDLenREST = 50

// requireNodeOperator gates an off-consensus federation action that is performed
// with the node OPERATOR's key (outbound signed calls) and has no other authz
// (unlike the cross_fed set/revoke txs, which are authorized on-chain). Fails
// closed when no operator id is configured. Mirrors the federated-recall gate.
func (s *Server) requireNodeOperator(w http.ResponseWriter, r *http.Request) bool {
	callerID := middleware.ContextAgentID(r.Context())
	if s.nodeOperatorID == "" || callerID != s.nodeOperatorID {
		writeProblem(w, http.StatusForbidden, "Operator only", "This federation action is restricted to the node operator.")
		return false
	}
	return true
}

// v11 Mode-1 exchange agreement setup — the REST tx-builder for
// TxTypeCrossFedSet/Revoke (33/34). This is the operator half of the
// federation-JOIN ceremony: the peers exchange CA certificates + endpoints
// out-of-band, then each side calls POST /v1/federation/cross, which persists
// the remote CA on disk, computes its SPKI pin, and submits the terms tx with
// that pin as PeerPubKey. AUTHORIZATION is enforced on-chain
// (crossFedAuthorized: chain-admin or owner of every scoped domain) — the
// REST layer only builds the tx. The self-federation guard lives HERE (and in
// federation.ActiveAgreement), deliberately not in consensus.

// CrossFedSetRequest is the JSON body for POST /v1/federation/cross.
type CrossFedSetRequest struct {
	RemoteChainID string `json:"remote_chain_id"`
	// Endpoint is the remote federation listener base URL (https://host:8444).
	Endpoint string `json:"endpoint"`
	// RemoteCAPEM is the remote chain's CA certificate (PEM), exchanged
	// out-of-band. Its SPKI fingerprint becomes the on-chain pinned key.
	RemoteCAPEM    string   `json:"remote_ca_pem"`
	MaxClearance   int      `json:"max_clearance"`
	AllowedDomains []string `json:"allowed_domains"`
	AllowedDepts   []string `json:"allowed_depts,omitempty"`
	ExpiresAt      int64    `json:"expires_at,omitempty"`
}

// CrossFedSetResponse reports the persisted trust anchor and the committed tx.
type CrossFedSetResponse struct {
	RemoteChainID string `json:"remote_chain_id"`
	SPKIPin       string `json:"spki_pin"`
	TxHash        string `json:"tx_hash"`
	Status        string `json:"status"`
}

// handleCrossFedSet handles POST /v1/federation/cross.
func (s *Server) handleCrossFedSet(w http.ResponseWriter, r *http.Request) {
	if s.federation == nil {
		writeProblem(w, http.StatusNotImplemented, "Federation disabled", "The federation transport is not wired on this node.")
		return
	}
	var req CrossFedSetRequest
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid JSON", err.Error())
		return
	}
	if err := federation.ValidateChainID(req.RemoteChainID); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid remote chain id", err.Error())
		return
	}
	// Match the consensus cap (maxRemoteChainIDLen=50) so an over-length id fails
	// fast at validation instead of after a wasted stage+broadcast round-trip
	// that the on-chain handler rejects with Code 102.
	if len(req.RemoteChainID) > maxRemoteChainIDLenREST {
		writeProblem(w, http.StatusBadRequest, "Invalid remote chain id", "remote_chain_id exceeds the maximum length (50).")
		return
	}
	// Self-federation guard — enforced off-consensus by design (the ABCI app
	// has no deterministic source for its own chain id; see processCrossFedSet).
	if req.RemoteChainID == s.federation.LocalChainID() {
		writeProblem(w, http.StatusBadRequest, "Self federation", "remote_chain_id equals this chain's id.")
		return
	}
	// Require scheme+host only: the federation client appends a fixed /fed/v1/*
	// path (and the V2 signature covers it), so a stored path/query/fragment
	// would make every call to this peer 404 and mis-sign. Fail fast at JOIN.
	endpoint, err := url.Parse(req.Endpoint)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" ||
		(endpoint.Path != "" && endpoint.Path != "/") || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		writeProblem(w, http.StatusBadRequest, "Invalid endpoint", "endpoint must be an https://host[:port] URL with no path, query, or fragment.")
		return
	}
	if req.MaxClearance < 0 || req.MaxClearance > 4 {
		writeProblem(w, http.StatusBadRequest, "Invalid clearance", "max_clearance must be 0..4.")
		return
	}
	if len(req.AllowedDomains) == 0 {
		writeProblem(w, http.StatusBadRequest, "Missing scope", "allowed_domains is required (\"*\" for a chain-admin treaty).")
		return
	}
	// Department scoping is NOT yet enforced on the query path (cross-chain dept
	// semantics are an open design question — plan §9.2). Reject a restrictive
	// allowed_depts rather than silently store a scope that does nothing (a
	// false sense of narrowing). "*" (or empty) is the only accepted value.
	for _, d := range req.AllowedDepts {
		if d != "*" {
			writeProblem(w, http.StatusBadRequest, "Dept scoping unsupported",
				"allowed_depts is not enforced in v11.0; omit it or use [\"*\"]. Scope with allowed_domains instead.")
			return
		}
	}
	if req.ExpiresAt != 0 && req.ExpiresAt <= time.Now().Unix() {
		writeProblem(w, http.StatusBadRequest, "Invalid expiry", "expires_at is in the past.")
		return
	}

	// STAGE the remote CA to a pending sidecar and derive the pin, but do NOT
	// commit it to the live path until the terms tx is authorized on-chain.
	// Authorization (crossFedAuthorized) lives in the consensus handler, so an
	// unauthorized caller's tx is rejected — and because we only commit the CA
	// after that succeeds, an unauthorized set can never overwrite an existing
	// agreement's live pinned CA on disk (a silent availability kill).
	pin, commitCA, rollbackCA, err := s.federation.StageRemoteCA(req.RemoteChainID, []byte(req.RemoteCAPEM))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid remote CA", err.Error())
		return
	}

	setTx := &tx.ParsedTx{
		Type:      tx.TxTypeCrossFedSet,
		Nonce:     tx.MonotonicNonce(s.signingKey),
		Timestamp: time.Now(),
		CrossFedTerms: &tx.CrossFedTerms{
			RemoteChainID:  req.RemoteChainID,
			Endpoint:       req.Endpoint,
			PeerPubKey:     pin,
			MaxClearance:   tx.ClearanceLevel(req.MaxClearance), // #nosec G115 -- validated 0..4
			AllowedDomains: req.AllowedDomains,
			AllowedDepts:   req.AllowedDepts,
			ExpiresAt:      req.ExpiresAt,
			Status:         "active",
		},
	}
	embedAgentAuth(r.Context(), setTx)
	if err := tx.SignTx(setTx, s.signingKey); err != nil {
		rollbackCA()
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}
	encoded, err := tx.EncodeTx(setTx)
	if err != nil {
		rollbackCA()
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}
	hash, err := s.broadcastTxCommit(encoded)
	if err != nil {
		rollbackCA() // authz/consensus rejected — leave any existing live CA untouched
		s.logger.Error().Err(err).Str("remote", req.RemoteChainID).Msg("cross_fed set rejected")
		status, msg := broadcastErrorPublic(err)
		writeProblem(w, status, "Agreement rejected", msg)
		return
	}
	if err := commitCA(); err != nil {
		// The terms are on-chain but the CA didn't land — the agreement is
		// inert (pin mismatch fails closed) until re-provisioned. Surface it.
		s.logger.Error().Err(err).Str("remote", req.RemoteChainID).Msg("cross_fed CA commit failed post-broadcast")
		writeProblem(w, http.StatusInternalServerError, "CA commit failed", "Agreement recorded on-chain but the CA could not be persisted; re-run the join to provision it.")
		return
	}
	writeJSON(w, http.StatusCreated, &CrossFedSetResponse{
		RemoteChainID: req.RemoteChainID,
		SPKIPin:       hex.EncodeToString(pin),
		TxHash:        hash,
		Status:        "active",
	})
}

// handleCrossFedRevoke handles POST /v1/federation/cross/{chain_id}/revoke.
func (s *Server) handleCrossFedRevoke(w http.ResponseWriter, r *http.Request) {
	remoteChainID := chi.URLParam(r, "chain_id")
	if err := federation.ValidateChainID(remoteChainID); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid remote chain id", err.Error())
		return
	}
	var req struct {
		Reason string `json:"reason,omitempty"`
	}
	_ = decodeJSON(r, &req) // reason is optional; an empty body is fine

	revokeTx := &tx.ParsedTx{
		Type:      tx.TxTypeCrossFedRevoke,
		Nonce:     tx.MonotonicNonce(s.signingKey),
		Timestamp: time.Now(),
		CrossFedRevoke: &tx.CrossFedRevoke{
			RemoteChainID: remoteChainID,
			Reason:        req.Reason,
		},
	}
	embedAgentAuth(r.Context(), revokeTx)
	if err := tx.SignTx(revokeTx, s.signingKey); err != nil {
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}
	encoded, err := tx.EncodeTx(revokeTx)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}
	hash, err := s.broadcastTxCommit(encoded)
	if err != nil {
		s.logger.Error().Err(err).Str("remote", remoteChainID).Msg("cross_fed revoke rejected")
		status, msg := broadcastErrorPublic(err)
		writeProblem(w, status, "Revoke rejected", msg)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"remote_chain_id": remoteChainID,
		"tx_hash":         hash,
		"status":          "revoked",
	})
}

// CrossFedListEntry is one agreement in GET /v1/federation/cross.
type CrossFedListEntry struct {
	RemoteChainID  string   `json:"remote_chain_id"`
	Endpoint       string   `json:"endpoint"`
	SPKIPin        string   `json:"spki_pin"`
	MaxClearance   int      `json:"max_clearance"`
	AllowedDomains []string `json:"allowed_domains"`
	AllowedDepts   []string `json:"allowed_depts,omitempty"`
	ExpiresAt      int64    `json:"expires_at,omitempty"`
	Status         string   `json:"status"`
	Expired        bool     `json:"expired,omitempty"`
}

// handleCrossFedList handles GET /v1/federation/cross — reads on-chain state
// directly, so it works even when the transport isn't wired.
func (s *Server) handleCrossFedList(w http.ResponseWriter, r *http.Request) {
	_ = middleware.ContextAgentID(r.Context()) // authed route; listing terms is not sensitive (pins are public)
	if s.badgerStore == nil {
		writeProblem(w, http.StatusNotImplemented, "No chain state", "This node has no on-chain store wired.")
		return
	}
	records, err := s.badgerStore.ListCrossFed()
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Read error", "Failed to list agreements.")
		return
	}
	now := time.Now().Unix()
	entries := make([]CrossFedListEntry, 0, len(records))
	for _, rec := range records {
		entries = append(entries, CrossFedListEntry{
			RemoteChainID:  rec.RemoteChainID,
			Endpoint:       rec.Endpoint,
			SPKIPin:        hex.EncodeToString(rec.PeerPubKey),
			MaxClearance:   int(rec.MaxClearance),
			AllowedDomains: rec.AllowedDomains,
			AllowedDepts:   rec.AllowedDepts,
			ExpiresAt:      rec.ExpiresAt,
			Status:         rec.Status,
			Expired:        rec.ExpiresAt != 0 && now >= rec.ExpiresAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"agreements": entries, "total": len(entries)})
}

// handleCrossFedPeerStatus handles GET /v1/federation/cross/{chain_id}/status —
// the live reachability/identity preflight against a peer's federation
// listener (mTLS + signed request), used by the activation runbook to
// distinguish "peer unreachable" from "peer misconfigured".
func (s *Server) handleCrossFedPeerStatus(w http.ResponseWriter, r *http.Request) {
	if s.federation == nil {
		writeProblem(w, http.StatusNotImplemented, "Federation disabled", "The federation transport is not wired on this node.")
		return
	}
	if !s.requireNodeOperator(w, r) {
		return
	}
	remoteChainID := chi.URLParam(r, "chain_id")
	if err := federation.ValidateChainID(remoteChainID); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid remote chain id", err.Error())
		return
	}
	ctx, cancel := contextWithFedTimeout(r)
	defer cancel()
	status, err := s.federation.PeerStatus(ctx, remoteChainID)
	if err != nil {
		// reachable stays a bool across both branches so typed clients don't break
		// on the error path (the whole point of this endpoint is the
		// unreachable-vs-misconfigured distinction).
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"remote_chain_id": remoteChainID,
			"reachable":       false,
			"error":           err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"remote_chain_id": remoteChainID,
		"reachable":       true,
		"peer_time":       status.Time,
	})
}
