package rest

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// handleAgentRegister handles POST /v1/agent/register.
// Builds a TxTypeAgentRegister and broadcasts via CometBFT.
// Idempotent: returns existing record if already registered.
func (s *Server) handleAgentRegister(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name       string `json:"name"`
		Role       string `json:"role"`
		BootBio    string `json:"boot_bio"`
		Provider   string `json:"provider"`
		P2PAddress string `json:"p2p_address"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if req.Name == "" {
		writeProblem(w, http.StatusBadRequest, "Missing name", "name is required.")
		return
	}
	if req.Role == "" {
		req.Role = "member"
	}

	agentID := middleware.ContextAgentID(r.Context())

	// Idempotent: check if already registered on-chain
	if s.badgerStore != nil && s.badgerStore.IsAgentRegistered(agentID) {
		existing, err := s.badgerStore.GetRegisteredAgent(agentID)
		if err == nil {
			name := existing.Name

			// Self-healing: if the dashboard (SQLite) has a different name than on-chain
			// (e.g. admin renamed via GUI but the CometBFT broadcast failed), reconcile
			// by pushing the SQLite name to on-chain state.
			if s.agentStore != nil {
				if sqliteAgent, agErr := s.agentStore.GetAgent(r.Context(), agentID); agErr == nil && sqliteAgent.Name != existing.Name {
					name = sqliteAgent.Name
					s.reconcileAgentName(agentID, sqliteAgent.Name, existing.BootBio)
				}
			}

			writeJSON(w, http.StatusOK, map[string]any{
				"agent_id":        existing.AgentID,
				"name":            name,
				"registered_name": existing.RegisteredName,
				"role":            existing.Role,
				"provider":        existing.Provider,
				"status":          "already_registered",
				"on_chain_height": existing.RegisteredAt,
			})
			return
		}
	}

	registerTx := &tx.ParsedTx{
		Type:      tx.TxTypeAgentRegister,
		Nonce:     tx.MonotonicNonce(s.signingKey),
		Timestamp: time.Now(),
		AgentRegister: &tx.AgentRegister{
			AgentID:    agentID,
			Name:       req.Name,
			Role:       req.Role,
			BootBio:    req.BootBio,
			Provider:   req.Provider,
			P2PAddress: req.P2PAddress,
		},
	}

	s.embedAgentAuth(r.Context(), registerTx)

	if err := tx.SignTx(registerTx, s.signingKey); err != nil {
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}

	encoded, err := tx.EncodeTx(registerTx)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}

	// broadcast_tx_commit (not _sync) so the response includes the block
	// height. Clients use on_chain_height as a trivial "did this actually
	// land on-chain?" check — surfacing it on first registration means
	// the first register_agent call doesn't come back with height=None
	// (prior behaviour) and then height=<N> only on the idempotent
	// re-registration path. SDK callers were reading the height=None as
	// a version-drift signal; with the fix both code paths surface it.
	txHash, height, err := s.broadcastTxCommitWithHeight(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast agent register tx")
		status, publicMsg := broadcastErrorPublic(err)
		writeProblem(w, status, "Broadcast error", publicMsg)
		return
	}

	if s.OnEvent != nil {
		s.OnEvent("agent", agentID, "", fmt.Sprintf("Agent %q registered (%s)", req.Name, req.Role), nil)
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"agent_id":        agentID,
		"name":            req.Name,
		"registered_name": req.Name,
		"role":            req.Role,
		"provider":        req.Provider,
		"status":          "registered",
		"tx_hash":         txHash,
		"on_chain_height": height,
	})
}

// tokenIdentityRegisterWait bounds how long MCP token issuance blocks on the
// on-chain registration of a freshly-minted per-token identity. A slow block
// must not hang the issuing REST call, so past this the token is returned and a
// background retry finishes the registration (autoRegister precedent).
const tokenIdentityRegisterWait = 8 * time.Second

// registerMintedAgentIdentity registers a freshly-minted per-token ed25519
// identity on-chain by issuing a STANDARD TxTypeAgentRegister — identical in
// shape to the one /v1/agent/register builds — with the agent proof signed by
// the minted key itself, so processAgentRegister's verifyAgentIdentity binds the
// on-chain identity to hex(pub). No new tx field is introduced: the minted
// pubkey plus its self-signed proof are the only new bytes on the wire, so
// old-binary replay stays byte-identical. Key generation happens OFF-consensus
// in the caller; FinalizeBlock only re-verifies the embedded proof, never
// generates a key.
func (s *Server) registerMintedAgentIdentity(tokenPub ed25519.PublicKey, tokenPriv ed25519.PrivateKey, name, provider string) (string, int64, error) {
	agentID := hex.EncodeToString(tokenPub)
	if name == "" {
		name = "mcp-token-" + agentID[:8]
	}

	// The signed request body MUST reconstruct the exact AgentRegister payload
	// consensus rebuilds under app-v17's delegated-proof binding
	// (verifySignedAgentAction: name required, role defaults to "member"), or the
	// tx is rejected once app-v17 is active.
	regBody := struct {
		Name       string `json:"name"`
		Role       string `json:"role"`
		BootBio    string `json:"boot_bio"`
		Provider   string `json:"provider"`
		P2PAddress string `json:"p2p_address"`
	}{Name: name, Role: "member", Provider: provider}
	body, err := json.Marshal(regBody)
	if err != nil {
		return "", 0, fmt.Errorf("marshal register body: %w", err)
	}

	const method, path = http.MethodPost, "/v1/agent/register"
	timestamp := time.Now().Unix()
	nonce := make([]byte, 8)
	if _, rErr := rand.Read(nonce); rErr != nil {
		return "", 0, fmt.Errorf("generate proof nonce: %w", rErr)
	}
	// Canonical request + body hash exactly as the REST auth middleware computes
	// for a token-key-signed POST /v1/agent/register.
	canonical := []byte(method + " " + path + "\n")
	canonical = append(canonical, body...)
	bodyHash := sha256.Sum256(canonical)
	sig := auth.SignRequestWithNonce(tokenPriv, method, path, body, timestamp, nonce)

	registerTx := &tx.ParsedTx{
		Type:      tx.TxTypeAgentRegister,
		Nonce:     tx.MonotonicNonce(s.signingKey),
		Timestamp: time.Now(),
		AgentRegister: &tx.AgentRegister{
			AgentID:  agentID,
			Name:     name,
			Role:     "member",
			Provider: provider,
		},
		AgentPubKey:    append([]byte(nil), tokenPub...),
		AgentSig:       sig,
		AgentTimestamp: timestamp,
		AgentBodyHash:  bodyHash[:],
		AgentNonce:     nonce,
	}
	// The delegated-proof envelope is only accepted (and required) post-app-v17;
	// gate it exactly like embedAgentAuth so pre-fork bytes stay reproducible.
	if s.isPostV17ForNextTx() {
		registerTx.AgentRequest = append([]byte(nil), canonical...)
	}
	if signErr := tx.SignTx(registerTx, s.signingKey); signErr != nil {
		return "", 0, fmt.Errorf("sign register tx: %w", signErr)
	}
	encoded, err := tx.EncodeTx(registerTx)
	if err != nil {
		return "", 0, fmt.Errorf("encode register tx: %w", err)
	}
	return s.broadcastTxCommitWithHeight(encoded)
}

// retryRegisterMintedIdentity finishes a token identity registration in the
// background after the synchronous window elapsed or a transient failure. Each
// attempt rebuilds a FRESH proof (new timestamp/nonce → new single-use app-v17
// delegated-proof fingerprint) so a retry never collides with the proof-replay
// gate, and processAgentRegister is idempotent if the first attempt actually
// landed. Stops on success or a definitive rejection.
func (s *Server) retryRegisterMintedIdentity(tokenPub ed25519.PublicKey, tokenPriv ed25519.PrivateKey, name, provider string) {
	go func() {
		shortID := hex.EncodeToString(tokenPub)
		if len(shortID) > 16 {
			shortID = shortID[:16]
		}
		for _, delay := range []time.Duration{2 * time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second} {
			time.Sleep(delay)
			if _, _, err := s.registerMintedAgentIdentity(tokenPub, tokenPriv, name, provider); err == nil {
				s.logger.Info().Str("agent_id", shortID).Msg("mcp token identity registered on-chain (background retry)")
				return
			} else if isDefinitiveRegisterRejection(err) {
				s.logger.Error().Err(err).Str("agent_id", shortID).
					Msg("mcp token identity registration definitively rejected; keyed calls fail-closed until re-registered")
				return
			}
		}
		s.logger.Warn().Str("agent_id", shortID).Msg("mcp token identity registration still pending after background retries")
	}()
}

// handleAgentUpdate handles PUT /v1/agent/update.
// Self-update only — agent can only update its own metadata.
func (s *Server) handleAgentUpdate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name    string `json:"name"`
		BootBio string `json:"boot_bio"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	agentID := middleware.ContextAgentID(r.Context())

	updateTx := &tx.ParsedTx{
		Type:      tx.TxTypeAgentUpdate,
		Nonce:     tx.MonotonicNonce(s.signingKey),
		Timestamp: time.Now(),
		AgentUpdateTx: &tx.AgentUpdate{
			AgentID: agentID,
			Name:    req.Name,
			BootBio: req.BootBio,
		},
	}

	s.embedAgentAuth(r.Context(), updateTx)

	if err := tx.SignTx(updateTx, s.signingKey); err != nil {
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}

	encoded, err := tx.EncodeTx(updateTx)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}

	txHash, err := s.broadcastTxCommit(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast agent update tx")
		status, publicMsg := broadcastErrorPublic(err)
		writeProblem(w, status, "Broadcast error", publicMsg)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"agent_id": agentID,
		"name":     req.Name,
		"status":   "updated",
		"tx_hash":  txHash,
	})
}

// reconcileAgentName pushes the SQLite (display) name to on-chain state via an
// AgentUpdate transaction. This self-heals the split-brain where a GUI rename
// updated SQLite but the CometBFT broadcast silently failed.
func (s *Server) reconcileAgentName(agentID, name, bio string) {
	updateTx := &tx.ParsedTx{
		Type:      tx.TxTypeAgentUpdate,
		Nonce:     tx.MonotonicNonce(s.signingKey),
		Timestamp: time.Now(),
		AgentUpdateTx: &tx.AgentUpdate{
			AgentID: agentID,
			Name:    name,
			BootBio: bio,
		},
	}
	if err := tx.SignTx(updateTx, s.signingKey); err != nil {
		s.logger.Warn().Err(err).Str("agent_id", agentID).Msg("reconcile: failed to sign agent name update")
		return
	}
	encoded, err := tx.EncodeTx(updateTx)
	if err != nil {
		s.logger.Warn().Err(err).Str("agent_id", agentID).Msg("reconcile: failed to encode agent name update")
		return
	}
	if _, err := s.broadcastTxCommit(encoded); err != nil {
		s.logger.Warn().Err(err).Str("agent_id", agentID).Msg("reconcile: failed to broadcast agent name update")
		return
	}
	s.logger.Info().Str("agent_id", agentID).Str("name", name).Msg("reconciled agent name: on-chain updated to match display name")
}

// handleAgentSetPermission handles PUT /v1/agent/{id}/permission.
//
// Auth model (v6.6.9 — see also processAgentSetPermission in
// internal/abci/app.go which is the consensus-side source of truth):
//
//   - Self-set: the caller is the target agent.
//   - Global admin: caller's on-chain Role == "admin" (legacy
//     deployment-admin identity established at genesis bootstrap or
//     initial register).
//   - Org admin: caller is an org member with role="admin" in any org the
//     target also belongs to.
//
// Anything else is rejected with HTTP 403 BEFORE broadcasting the tx, and
// the ABCI handler re-checks the same auth model on-chain so the gate
// holds even when REST is bypassed (direct CometBFT broadcast, GUI, etc).
//
// Prior to v6.6.9 this endpoint:
//  1. used broadcast_tx_sync (only checks CheckTx code, not FinalizeBlock),
//  2. had no REST-side auth check, and
//  3. the ABCI handler hard-gated on global Role=="admin",
//
// so a non-admin caller would get 200 + a real tx_hash for a tx that
// FinalizeBlock then rejected with code=67 — the SQL row was never
// updated, but the API said success. That silent-failure is the bug fixed
// here (Bug B from the v6.6.8 Level Up follow-up).
func (s *Server) handleAgentSetPermission(w http.ResponseWriter, r *http.Request) {
	targetID := chi.URLParam(r, "id")
	if targetID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing agent ID", "id path parameter is required.")
		return
	}

	var req struct {
		Clearance     *int    `json:"clearance"`
		DomainAccess  *string `json:"domain_access"`
		VisibleAgents *string `json:"visible_agents"`
		OrgID         *string `json:"org_id"`
		DeptID        *string `json:"dept_id"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	// Pre-flight RBAC: bail out fast with 403 so callers don't get a
	// 200+tx_hash for a write the chain will refuse. The ABCI handler is
	// the trust boundary — re-verifies the same conditions in consensus —
	// so when the BadgerDB-backed pre-check is unavailable we let the
	// request through to the chain (which still rejects access-denied
	// writes). Pre-flight is a UX optimisation, not a security gate.
	if s.badgerStore != nil {
		callerID := middleware.ContextAgentID(r.Context())
		if callerID == "" {
			writeProblem(w, http.StatusUnauthorized, "Authentication required", "agent identity required to set permissions.")
			return
		}
		if !s.callerCanSetPermission(r.Context(), callerID, targetID) {
			writeProblem(w, http.StatusForbidden, "Access denied", "access denied")
			return
		}
	}

	// PATCH semantics (v6.8.4): when the caller omits a field (JSON null /
	// missing) we preserve the on-chain value rather than resetting it to a
	// default. The wire format stays full-replace — Clearance/DomainAccess/
	// VisibleAgents are non-pointer fields in tx.AgentSetPermission and ABCI
	// always overwrites them in BadgerDB — so we have to backfill HERE before
	// signing, otherwise a bridge calling
	// `set_agent_permission(visible_agents='*')` without specifying clearance
	// would silently demote the agent to clearance=1, which then propagates
	// through the network_agents SQL mirror via the agent_register flush. Bug
	// 2 in the v6.8.4 hotfix bundle (see also processAgentRegister idempotent
	// path in internal/abci/app.go).
	var existing *store.OnChainAgent
	needBackfill := req.Clearance == nil || req.DomainAccess == nil || req.VisibleAgents == nil ||
		req.OrgID == nil || req.DeptID == nil
	if needBackfill && s.badgerStore != nil {
		if e, err := s.badgerStore.GetRegisteredAgent(targetID); err == nil {
			existing = e
		}
	}

	clearance := uint8(1)
	if req.Clearance != nil {
		clearance = uint8(*req.Clearance) // #nosec G115 -- validated small int 0-4
	} else if existing != nil {
		clearance = existing.Clearance
	}
	domainAccess := ""
	if req.DomainAccess != nil {
		domainAccess = *req.DomainAccess
	} else if existing != nil {
		domainAccess = existing.DomainAccess
	}
	visibleAgents := ""
	if req.VisibleAgents != nil {
		visibleAgents = *req.VisibleAgents
	} else if existing != nil {
		visibleAgents = existing.VisibleAgents
	}
	orgID := ""
	if req.OrgID != nil {
		orgID = *req.OrgID
	} else if existing != nil {
		orgID = existing.OrgID
	}
	deptID := ""
	if req.DeptID != nil {
		deptID = *req.DeptID
	} else if existing != nil {
		deptID = existing.DeptID
	}

	permTx := &tx.ParsedTx{
		Type:      tx.TxTypeAgentSetPermission,
		Nonce:     tx.MonotonicNonce(s.signingKey),
		Timestamp: time.Now(),
		AgentSetPermission: &tx.AgentSetPermission{
			AgentID:       targetID,
			Clearance:     clearance,
			DomainAccess:  domainAccess,
			VisibleAgents: visibleAgents,
			OrgID:         orgID,
			DeptID:        deptID,
		},
	}

	s.embedAgentAuth(r.Context(), permTx)

	if err := tx.SignTx(permTx, s.signingKey); err != nil {
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}

	encoded, err := tx.EncodeTx(permTx)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}

	// Use broadcast_tx_commit (NOT _sync) so a FinalizeBlock rejection is
	// surfaced as an error to the REST caller. Sync only inspects CheckTx
	// (signature/nonce) and would happily return a tx_hash for a tx that
	// the consensus handler later rejects — the v6.6.8-and-prior silent
	// failure mode.
	txHash, err := s.broadcastTxCommit(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast agent set permission tx")
		status, publicMsg := broadcastErrorPublic(err)
		writeProblem(w, status, "Broadcast error", publicMsg)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"agent_id": targetID,
		"status":   "permissions_updated",
		"tx_hash":  txHash,
	})
}

// callerCanSetPermission mirrors the consensus-side auth check in
// processAgentSetPermission. Keep these two implementations in sync —
// the ABCI handler is the trust boundary, this is the fail-fast UX layer.
//
// v6.8.5: when BadgerDB has no record for the caller, fall through to
// the SQL agent store. If SQL says role='admin', allow the request to
// reach ABCI which will self-heal via bootstrapAdminFromSQL (see the
// security invariants on that helper). Without this fallback the REST
// pre-flight 403s before ABCI ever gets a chance to recover the chain.
func (s *Server) callerCanSetPermission(ctx context.Context, callerID, targetID string) bool {
	if callerID == "" {
		return false
	}
	// 1. Self-set.
	if callerID == targetID {
		return true
	}
	// 2. Global admin (legacy deployment-admin identity).
	caller, err := s.badgerStore.GetRegisteredAgent(callerID)
	switch {
	case err == nil && caller != nil && caller.Role == "admin":
		return true
	case err != nil && s.agentStore != nil:
		// BadgerDB miss — check SQL. SQL `role='admin'` is the same trust
		// source the GUI Create Agent flow already relies on; matching it
		// here just lets the ABCI bootstrap fix complete end-to-end.
		if sqlAgent, sqlErr := s.agentStore.GetAgent(ctx, callerID); sqlErr == nil && sqlAgent != nil && sqlAgent.Role == "admin" {
			return true
		}
	}
	// 3. Org admin in any org the target belongs to.
	orgs, listErr := s.badgerStore.ListAgentOrgs(targetID)
	if listErr != nil {
		return false
	}
	for _, orgID := range orgs {
		_, role, mErr := s.badgerStore.GetMemberClearance(orgID, callerID)
		if mErr == nil && role == "admin" {
			return true
		}
	}
	return false
}

// callerIsOperatorOrAdmin reports whether callerID is the node operator or an
// admin (on-chain or SQL) — the trust level allowed to see another agent's
// ACL-topology fields or manage MCP tokens for other agents. Empty caller is
// never privileged.
func (s *Server) callerIsOperatorOrAdmin(ctx context.Context, callerID string) bool {
	if callerID == "" {
		return false
	}
	if s.nodeOperatorID != "" && callerID == s.nodeOperatorID {
		return true
	}
	if s.badgerStore != nil {
		if a, err := s.badgerStore.GetRegisteredAgent(callerID); err == nil && a != nil && a.Role == "admin" {
			return true
		}
	}
	if s.agentStore != nil {
		if a, err := s.agentStore.GetAgent(ctx, callerID); err == nil && a != nil && a.Role == "admin" {
			return true
		}
	}
	return false
}

// sanitizeAgentForRead returns a copy of a safe to serialize on the agent read
// endpoints. claim_token / claim_expires_at are a one-time CLI-install
// credential that handleClaimAgent exchanges for the agent's private key seed
// (web/network_handler.go); they must NEVER be exposed via these read paths —
// the pairing flow delivers the token through the authenticated dashboard
// create-agent response instead. Unless the caller is the agent itself or an
// operator/admin, the per-agent ACL topology (domain_access, visible_agents) is
// stripped too.
func sanitizeAgentForRead(a *store.AgentEntry, privileged bool) *store.AgentEntry {
	if a == nil {
		return nil
	}
	out := *a
	out.ClaimToken = ""
	out.ClaimExpiresAt = nil
	// BundlePath is an absolute server-side path to the agent's key-bundle dir
	// (the directory that also holds the Ed25519 seed); it is never useful to an
	// API consumer and discloses the operator's home/dir layout, so strip it
	// unconditionally. The dashboard bundle download reads it from the stored
	// entry, not from this response.
	out.BundlePath = ""
	if !privileged {
		out.DomainAccess = ""
		out.VisibleAgents = ""
	}
	return &out
}

// handleGetRegisteredAgent handles GET /v1/agent/{id}.
// Reads from offchain store (no tx broadcast needed).
func (s *Server) handleGetRegisteredAgent(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		writeProblem(w, http.StatusBadRequest, "Missing agent ID", "id path parameter is required.")
		return
	}

	if s.agentStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Agent store unavailable", "Agent store not configured.")
		return
	}

	agent, err := s.agentStore.GetAgent(r.Context(), id)
	if err != nil {
		writeProblem(w, http.StatusNotFound, "Agent not found", fmt.Sprintf("No agent found with ID %s.", id))
		return
	}

	callerID := middleware.ContextAgentID(r.Context())
	privileged := callerID == id || s.callerIsOperatorOrAdmin(r.Context(), callerID)
	writeJSON(w, http.StatusOK, sanitizeAgentForRead(agent, privileged))
}

// handleListRegisteredAgents handles GET /v1/agents.
// Lists all registered agents from offchain store.
func (s *Server) handleListRegisteredAgents(w http.ResponseWriter, r *http.Request) {
	if s.agentStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Agent store unavailable", "Agent store not configured.")
		return
	}

	agents, err := s.agentStore.ListAgents(r.Context())
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "List error", err.Error())
		return
	}
	if agents == nil {
		agents = make([]*store.AgentEntry, 0)
	}

	// /v1/agents is unauthenticated — never expose claim_token (a one-time
	// credential exchangeable for the agent key seed) or per-agent ACL topology.
	sanitized := make([]*store.AgentEntry, 0, len(agents))
	for _, a := range agents {
		sanitized = append(sanitized, sanitizeAgentForRead(a, false))
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"agents": sanitized,
		"total":  len(sanitized),
	})
}
