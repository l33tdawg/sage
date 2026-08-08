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
	"strconv"
	"strings"
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

	credentialID := middleware.ContextAgentID(r.Context())
	isRoot, rootErr := s.appV23IsRootIdentity(credentialID)
	if rootErr != nil {
		writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
			"Current CEREBRUM Root state is unavailable.")
		return
	}
	if isRoot {
		writeProblem(w, http.StatusForbidden, "Root is not an agent",
			"CEREBRUM Root cannot register or update an ordinary agent identity.")
		return
	}
	agentID := credentialID
	if s.isPostV23ForNextTx() {
		var policyErr error
		agentID, policyErr = appV23PolicyPrincipal(s.badgerStore, credentialID)
		if policyErr != nil {
			writeProblem(w, http.StatusForbidden, "Current Root required",
				"The authenticated credential is not a current app-v23 policy principal.")
			return
		}
	}

	// Idempotent: check if already registered on-chain
	if s.badgerStore != nil && s.badgerStore.IsAgentRegistered(agentID) {
		existing, err := s.badgerStore.GetRegisteredAgent(agentID)
		if err == nil {
			name := existing.Name
			registeredName := existing.RegisteredName
			provider := existing.Provider
			status := "already_registered"
			approvalRequired := false
			if s.isPostV23ForNextTx() {
				enrollment, enrollmentErr := s.badgerStore.GetAppV23Enrollment(agentID)
				if enrollmentErr != nil {
					writeProblem(w, http.StatusServiceUnavailable, "Committed state unavailable",
						"The local enrollment state could not be read.")
					return
				}
				approvalRequired = enrollment == nil || !enrollment.Active
				if approvalRequired {
					status = "pending_review"
				}
			}
			retryRemovedPending := false
			if approvalRequired && s.agentStore != nil {
				// Root may reject a zero-authority pending registration from
				// CEREBRUM. The immutable identity record remains in chain
				// history, while its node-local projection is marked removed.
				// A later signed register call is the agent's explicit new
				// request: let the idempotent consensus transaction run again so
				// Commit can restore the pending projection for another review.
				local, localErr := s.agentStore.GetAgent(r.Context(), agentID)
				retryRemovedPending = localErr == nil && local != nil &&
					(local.Status == "removed" || local.RemovedAt != nil)
			}

			if s.agentStore != nil && s.isPostV23ForNextTx() && !approvalRequired {
				// Direct app-v23 genesis and state sync commit the ordinary-agent
				// roster before a node-local SQL projection exists. Repair that
				// projection from consensus here as an idempotent self-heal.
				// Request fields are authenticated display hints only; role,
				// lifecycle, clearance, and capabilities always come from Badger.
				projected, projectionErr := store.EnsureAppV23AgentProjection(
					r.Context(),
					s.agentStore,
					s.badgerStore,
					agentID,
					&store.AgentEntry{
						AgentID:        agentID,
						Name:           req.Name,
						RegisteredName: req.Name,
						BootBio:        req.BootBio,
						Provider:       req.Provider,
						P2PAddress:     req.P2PAddress,
					},
				)
				if projectionErr != nil {
					writeProblem(w, http.StatusServiceUnavailable, "Agent projection unavailable",
						"The agent is active in consensus, but its local serving projection could not be repaired.")
					return
				}
				if projected.Name != "" {
					name = projected.Name
				}
				if projected.RegisteredName != "" {
					registeredName = projected.RegisteredName
				}
				if projected.Provider != "" {
					provider = projected.Provider
				}
			} else if s.agentStore != nil && !s.isPostV23ForNextTx() {
				// Legacy self-healing: if the dashboard (SQLite) has a different
				// name than on-chain (e.g. an older GUI rename broadcast failed),
				// push the SQLite name to on-chain state.
				if sqliteAgent, agErr := s.agentStore.GetAgent(r.Context(), agentID); agErr == nil && sqliteAgent.Name != existing.Name {
					name = sqliteAgent.Name
					s.reconcileAgentName(agentID, sqliteAgent.Name, existing.BootBio)
				}
			}

			if !retryRemovedPending {
				writeJSON(w, http.StatusOK, map[string]any{
					"agent_id":          existing.AgentID,
					"name":              name,
					"registered_name":   registeredName,
					"role":              existing.Role,
					"provider":          provider,
					"clearance":         existing.Clearance,
					"capabilities":      existing.Capabilities,
					"status":            status,
					"approval_required": approvalRequired,
					"on_chain_height":   existing.RegisteredAt,
				})
				return
			}
		}
	}

	registerTx := &tx.ParsedTx{
		Type: tx.TxTypeAgentRegister,
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

	// broadcast_tx_commit (not _sync) so the response includes the block
	// height. Clients use on_chain_height as a trivial "did this actually
	// land on-chain?" check — surfacing it on first registration means
	// the first register_agent call doesn't come back with height=None
	// (prior behaviour) and then height=<N> only on the idempotent
	// re-registration path. SDK callers were reading the height=None as
	// a version-drift signal; with the fix both code paths surface it.
	var txHash string
	var height int64
	stage, err := s.submitConsensusTx(r.Context(), registerTx, func(encoded []byte) error {
		var submitErr error
		txHash, height, submitErr = s.broadcastTxCommitWithHeight(encoded)
		return submitErr
	})
	if err != nil {
		s.writeConsensusTxError(w, stage, "agent register", err)
		return
	}

	committed := &store.OnChainAgent{
		AgentID: agentID, Name: req.Name, RegisteredName: req.Name,
		Role: req.Role, Provider: req.Provider, RegisteredAt: height,
	}
	status := "registered"
	approvalRequired := false
	if s.badgerStore != nil {
		actual, readErr := s.badgerStore.GetRegisteredAgent(agentID)
		if readErr != nil || actual == nil {
			if s.isPostV23ForNextTx() {
				writeProblem(w, http.StatusServiceUnavailable, "Committed state unavailable",
					"Registration committed, but its consensus policy could not be read. Retry the idempotent registration request.")
				return
			}
		} else {
			committed = actual
		}
	}
	if s.isPostV23ForNextTx() {
		enrollment, readErr := s.badgerStore.GetAppV23Enrollment(agentID)
		if readErr != nil {
			writeProblem(w, http.StatusServiceUnavailable, "Committed state unavailable",
				"Registration committed, but its local enrollment state could not be read. Retry the idempotent registration request.")
			return
		}
		approvalRequired = enrollment == nil || !enrollment.Active
		if approvalRequired {
			status = "pending_review"
		}
	}
	if s.OnEvent != nil {
		s.OnEvent("agent", agentID, "",
			fmt.Sprintf("Agent %q registered (%s; %s)", committed.Name, committed.Role, status), nil)
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"agent_id":          committed.AgentID,
		"name":              committed.Name,
		"registered_name":   committed.RegisteredName,
		"role":              committed.Role,
		"provider":          committed.Provider,
		"clearance":         committed.Clearance,
		"capabilities":      committed.Capabilities,
		"status":            status,
		"approval_required": approvalRequired,
		"tx_hash":           txHash,
		"on_chain_height":   height,
	})
}

// registerMintedAgentIdentity registers a freshly-minted per-token ed25519
// identity on-chain by issuing a STANDARD TxTypeAgentRegister — identical in
// shape to the one /v1/agent/register builds — with the agent proof signed by
// the minted key itself, so processAgentRegister's verifyAgentIdentity binds the
// on-chain identity to hex(pub). No new tx field is introduced: the minted
// pubkey plus its self-signed proof are the only new bytes on the wire, so
// old-binary replay stays byte-identical. Key generation happens OFF-consensus
// in the caller; FinalizeBlock only re-verifies the embedded proof, never
// generates a key.
func (s *Server) registerMintedAgentIdentity(ctx context.Context, tokenPub ed25519.PublicKey, tokenPriv ed25519.PrivateKey, name, provider string) (string, int64, error) {
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
		Type: tx.TxTypeAgentRegister,
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
	var txHash string
	var height int64
	stage, err := s.submitConsensusTx(ctx, registerTx, func(encoded []byte) error {
		var submitErr error
		txHash, height, submitErr = s.broadcastTxCommitWithHeightContext(ctx, encoded)
		return submitErr
	})
	if err != nil {
		switch stage {
		case consensusTxSign:
			return "", 0, fmt.Errorf("sign register tx: %w", err)
		case consensusTxEncode:
			return "", 0, fmt.Errorf("encode register tx: %w", err)
		default:
			return "", 0, err
		}
	}
	return txHash, height, nil
}

// handleAgentUpdate handles PUT /v1/agent/update.
// Self-update only — agent can only update its own metadata.
func (s *Server) handleAgentUpdate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name    *string `json:"name"`
		BootBio *string `json:"boot_bio"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	agentID := middleware.ContextAgentID(r.Context())
	isRoot, rootErr := s.appV23IsRootIdentity(agentID)
	if rootErr != nil {
		writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
			"Current CEREBRUM Root state is unavailable.")
		return
	}
	if isRoot {
		writeProblem(w, http.StatusForbidden, "Root is not an agent",
			"CEREBRUM Root metadata cannot be edited through the agent API.")
		return
	}
	if s.badgerStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Agent state unavailable",
			"The current agent metadata could not be read.")
		return
	}
	if !s.badgerStore.IsAgentRegistered(agentID) {
		writeProblem(w, http.StatusNotFound, "Agent not found",
			"The authenticated agent is not registered.")
		return
	}
	current, currentErr := s.badgerStore.GetRegisteredAgent(agentID)
	if currentErr != nil || current == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Agent state unavailable",
			"The current agent metadata could not be read.")
		return
	}

	// AgentUpdate is a replacement transaction on-chain, while this REST
	// endpoint exposes optional fields. Resolve omitted fields from canonical
	// state before constructing the transaction so a name-only update cannot
	// erase the boot bio (and a bio-only update cannot erase the display name).
	name := current.Name
	bootBio := current.BootBio
	if req.Name != nil {
		name = *req.Name
	}
	if req.BootBio != nil {
		bootBio = *req.BootBio
	}

	updateTx := &tx.ParsedTx{
		Type: tx.TxTypeAgentUpdate,
		AgentUpdateTx: &tx.AgentUpdate{
			AgentID: agentID,
			Name:    name,
			BootBio: bootBio,
		},
	}

	s.embedAgentAuth(r.Context(), updateTx)

	var txHash string
	stage, err := s.submitConsensusTx(r.Context(), updateTx, func(encoded []byte) error {
		var submitErr error
		txHash, submitErr = s.broadcastTxCommit(encoded)
		return submitErr
	})
	if err != nil {
		s.writeConsensusTxError(w, stage, "agent update", err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"agent_id": agentID,
		"name":     name,
		"status":   "updated",
		"tx_hash":  txHash,
	})
}

// reconcileAgentName pushes the SQLite (display) name to on-chain state via an
// AgentUpdate transaction. This self-heals the split-brain where a GUI rename
// updated SQLite but the CometBFT broadcast silently failed.
func (s *Server) reconcileAgentName(agentID, name, bio string) {
	updateTx := &tx.ParsedTx{
		Type: tx.TxTypeAgentUpdate,
		AgentUpdateTx: &tx.AgentUpdate{
			AgentID: agentID,
			Name:    name,
			BootBio: bio,
		},
	}
	stage, err := s.submitConsensusTx(context.Background(), updateTx, func(encoded []byte) error {
		_, submitErr := s.broadcastTxCommit(encoded)
		return submitErr
	})
	if err != nil {
		s.logger.Warn().Err(err).Str("agent_id", agentID).
			Str("stage", string(stage)).Msg("reconcile: failed to submit agent name update")
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
	if s.isPostV23ForNextTx() {
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusGone)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"type":   "https://sage.dev/errors/app-v23-atomic-agent-policy-required",
			"title":  "Legacy agent permission endpoint retired",
			"status": http.StatusGone,
			"detail": "App-v23 requires role, profile, clearance, capabilities, and home-domain approval to be committed atomically through CEREBRUM.",
			"code":   "app_v23_atomic_policy_required",
			"replacement": map[string]string{
				"method":          http.MethodPut,
				"path":            "/v1/dashboard/network/access/agents/{id}/policy",
				"target_agent_id": targetID,
			},
		})
		return
	}

	var req struct {
		Clearance     *int    `json:"clearance"`
		DomainAccess  *string `json:"domain_access"`
		VisibleAgents *string `json:"visible_agents"`
		OrgID         *string `json:"org_id"`
		DeptID        *string `json:"dept_id"`
		Capabilities  *uint32 `json:"capabilities"`
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
	if s.badgerStore != nil {
		if e, err := s.badgerStore.GetRegisteredAgent(targetID); err == nil {
			existing = e
		}
	}
	if s.isPostV22ForNextTx() && existing == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Agent permissions unavailable", "current consensus agent state is unavailable.")
		return
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
	capabilities := store.AgentCapabilities(0)
	if req.Capabilities != nil {
		capabilities = store.AgentCapabilities(*req.Capabilities)
	} else if existing != nil {
		capabilities = existing.Capabilities
	}
	if !capabilities.Valid() {
		writeProblem(w, http.StatusBadRequest, "Invalid capabilities", fmt.Sprintf("unknown capability bits: 0x%x", uint32(capabilities&^store.KnownAgentCapabilities)))
		return
	}
	capabilitiesChanged := req.Capabilities != nil && (existing == nil || capabilities != existing.Capabilities)
	if capabilitiesChanged && !s.isPostV22ForNextTx() {
		writeProblem(w, http.StatusConflict, "Agent capabilities unavailable", "app-v22 must activate before agent capabilities can be assigned.")
		return
	}
	permissionsChanged := existing == nil ||
		clearance != existing.Clearance ||
		domainAccess != existing.DomainAccess ||
		visibleAgents != existing.VisibleAgents ||
		(orgID != "" && orgID != existing.OrgID) ||
		(deptID != "" && deptID != existing.DeptID) ||
		capabilities != existing.Capabilities
	if s.isPostV22ForNextTx() && permissionsChanged {
		if s.badgerStore == nil {
			writeProblem(w, http.StatusServiceUnavailable, "Agent permissions unavailable", "consensus agent state is unavailable.")
			return
		}
		callerID := middleware.ContextAgentID(r.Context())
		caller, callerErr := s.badgerStore.GetRegisteredAgent(callerID)
		if callerErr != nil || caller == nil || caller.Role != "admin" {
			writeProblem(w, http.StatusForbidden, "Access denied", "only a global administrator can change agent permissions after app-v22.")
			return
		}
	}
	var wireCapabilities uint32
	if req.Capabilities != nil {
		wireCapabilities = *req.Capabilities
	}

	permTx := &tx.ParsedTx{
		Type: tx.TxTypeAgentSetPermission,
		AgentSetPermission: &tx.AgentSetPermission{
			AgentID:             targetID,
			Clearance:           clearance,
			DomainAccess:        domainAccess,
			VisibleAgents:       visibleAgents,
			OrgID:               orgID,
			DeptID:              deptID,
			Capabilities:        wireCapabilities,
			CapabilitiesPresent: capabilitiesChanged,
		},
	}

	s.embedAgentAuth(r.Context(), permTx)

	// Use broadcast_tx_commit (NOT _sync) so a FinalizeBlock rejection is
	// surfaced as an error to the REST caller. Sync only inspects CheckTx
	// (signature/nonce) and would happily return a tx_hash for a tx that
	// the consensus handler later rejects — the v6.6.8-and-prior silent
	// failure mode.
	var txHash string
	stage, err := s.submitConsensusTx(r.Context(), permTx, func(encoded []byte) error {
		var submitErr error
		txHash, submitErr = s.broadcastTxCommit(encoded)
		return submitErr
	})
	if err != nil {
		s.writeConsensusTxError(w, stage, "agent set permission", err)
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
	if s.isPostV23ForNextTx() {
		if s.badgerStore == nil {
			return false
		}
		root, err := s.badgerStore.GetAppV23Root()
		if err != nil || root == nil {
			return false
		}
		if callerID == root.CredentialID {
			return true
		}
		if callerID == root.PrincipalID && root.CredentialID != root.PrincipalID {
			return false
		}
		enrollment, err := s.badgerStore.GetAppV23Enrollment(callerID)
		if err != nil || enrollment == nil || !enrollment.Active ||
			enrollment.RootGeneration != root.Generation {
			return false
		}
		role, err := s.badgerStore.GetAppV23Role(callerID)
		return err == nil && role != nil &&
			role.Role == store.AppV23RoleAdmin &&
			store.ValidateAppV23Policy(
				role.Role, enrollment.Profile, enrollment.Capabilities, enrollment.Clearance,
			) == nil
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

// overlayOnChainAgentPolicyForRead prevents the SQL projection from becoming a
// second authority for app-v22 policy fields. Projection persistence keeps the
// normal path current, while this bounded Badger overlay also makes reads
// correct across upgrades and crash-recovery windows where an older SQL row
// still carries the pre-v22 zero mask.
func (s *Server) overlayOnChainAgentPolicyForRead(agent *store.AgentEntry) {
	if agent == nil || s.badgerStore == nil {
		return
	}
	onChain, err := s.badgerStore.GetRegisteredAgent(agent.AgentID)
	if err != nil || onChain == nil {
		return
	}
	agent.Role = onChain.Role
	agent.Clearance = int(onChain.Clearance)
	agent.OrgID = onChain.OrgID
	agent.DeptID = onChain.DeptID
	agent.DomainAccess = onChain.DomainAccess
	agent.VisibleAgents = onChain.VisibleAgents
	agent.Capabilities = onChain.Capabilities
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
	isRoot, rootErr := s.appV23IsRootIdentity(id)
	if rootErr != nil {
		writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
			"Current CEREBRUM Root state is unavailable.")
		return
	}
	if isRoot {
		writeProblem(w, http.StatusNotFound, "Agent not found", "No agent found with that ID.")
		return
	}

	agent, err := s.agentStore.GetAgent(r.Context(), id)
	if err != nil {
		writeProblem(w, http.StatusNotFound, "Agent not found", fmt.Sprintf("No agent found with ID %s.", id))
		return
	}
	s.overlayOnChainAgentPolicyForRead(agent)

	callerID := middleware.ContextAgentID(r.Context())
	privileged := callerID == id || s.callerIsOperatorOrAdmin(r.Context(), callerID)
	writeJSON(w, http.StatusOK, sanitizeAgentForRead(agent, privileged))
}

// handleListRegisteredAgents handles signed GET /v1/agents. After app-v23 it
// returns only active ordinary canonical enrollments; the unsigned full-roster
// oracle was removed because it bypassed caller-scoped recipient discovery and
// exposed local RBAC/network topology.
func (s *Server) handleListRegisteredAgents(w http.ResponseWriter, r *http.Request) {
	if s.agentStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Agent store unavailable", "Agent store not configured.")
		return
	}

	agents, err := s.agentStore.ListAgents(r.Context())
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "List error", "The agent roster could not be read.")
		return
	}
	if agents == nil {
		agents = make([]*store.AgentEntry, 0)
	}

	callerID := middleware.ContextAgentID(r.Context())
	privileged := s.callerIsOperatorOrAdmin(r.Context(), callerID)
	sanitized := make([]*store.AgentEntry, 0, len(agents))
	for _, a := range agents {
		if a == nil {
			continue
		}
		if s.isPostV23ForNextTx() {
			active, activeErr := s.appV23ActiveOrdinaryAgent(a.AgentID)
			if activeErr != nil {
				writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
					"Current local enrollment state is unavailable.")
				return
			}
			if !active {
				continue
			}
		}
		isRoot, rootErr := s.appV23IsRootIdentity(a.AgentID)
		if rootErr != nil {
			writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
				"Current CEREBRUM Root state is unavailable.")
			return
		}
		if isRoot {
			continue
		}
		s.overlayOnChainAgentPolicyForRead(a)
		sanitized = append(sanitized, sanitizeAgentForRead(
			a,
			privileged || callerID == a.AgentID,
		))
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"agents": sanitized,
		"total":  len(sanitized),
	})
}

type agentNameFinder interface {
	FindAgentsByName(ctx context.Context, name string, limit int) ([]*store.AgentEntry, error)
}

type agentDirectoryLister interface {
	ListAgentDirectory(ctx context.Context, limit int) ([]*store.AgentEntry, error)
}

type agentDirectoryEntry struct {
	AgentID        string `json:"agent_id"`
	Name           string `json:"name"`
	RegisteredName string `json:"registered_name"`
	Provider       string `json:"provider,omitempty"`
	Status         string `json:"status"`
}

// handleListAgentDirectory returns only exact recipient identity metadata. It
// avoids ListAgents' derived memory-count query because sage_directory never
// exposes administrative records or per-agent memory totals.
func (s *Server) handleListAgentDirectory(w http.ResponseWriter, r *http.Request) {
	const maxLocalDirectoryEntries = 100
	if s.agentStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Agent store unavailable", "Agent store not configured.")
		return
	}
	lister, ok := s.agentStore.(agentDirectoryLister)
	if !ok {
		writeProblem(w, http.StatusNotImplemented, "Agent directory unavailable",
			"The configured agent store does not support metadata-only directory reads.")
		return
	}
	// Fetch one sentinel row beyond the public cap so callers learn that the
	// projection is incomplete without a second count query or an unbounded
	// roster scan. Named discovery remains available through /v1/agents/find.
	agents, err := lister.ListAgentDirectory(r.Context(), maxLocalDirectoryEntries+1)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Directory error", "The agent directory could not be read.")
		return
	}
	directory := make([]agentDirectoryEntry, 0, min(len(agents), maxLocalDirectoryEntries))
	// The sentinel is evaluated before canonical filtering. Some SQL-visible
	// rows may be pending/inactive on chain, so the bounded page can contain
	// fewer than 100 returned recipients and still be incomplete.
	truncated := len(agents) > maxLocalDirectoryEntries
	for _, agent := range agents {
		if agent == nil || agent.AgentID == "" {
			continue
		}
		active, activeErr := s.appV23ActiveOrdinaryAgent(agent.AgentID)
		if activeErr != nil {
			writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
				"Current local enrollment state is unavailable.")
			return
		}
		if !active {
			continue
		}
		if len(directory) == maxLocalDirectoryEntries {
			break
		}
		directory = append(directory, agentDirectoryEntry{
			AgentID: agent.AgentID, Name: agent.Name,
			RegisteredName: agent.RegisteredName, Provider: agent.Provider,
			Status: "active",
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"agents": directory, "total": len(directory), "truncated": truncated,
	})
}

type agentNamePageFinder interface {
	FindAgentsByNamePage(ctx context.Context, name string, limit, offset int) ([]*store.AgentEntry, error)
}

type agentLookupCandidateFinder interface {
	FindAgentLookupCandidates(ctx context.Context, name string, limit int) ([]*store.AgentEntry, error)
}

type agentLookupResult struct {
	*store.AgentEntry
	MatchKind string `json:"match_kind"`
}

const (
	agentLookupCandidatePageSize  = 20
	agentLookupCandidateBatchSize = 160
	// Candidate enrollment is rechecked against consensus one row at a time.
	// Bound a hostile pending-registration prefix so one friendly-name lookup
	// cannot amplify into thousands of SQL pages and Badger reads.
	maxAgentLookupCandidatePages = 8
)

// equalAgentLookupField applies the lookup endpoint's documented comparison:
// ASCII letters are case-insensitive while every non-ASCII byte retains its
// registered casing. strings.EqualFold is deliberately too broad here.
func equalAgentLookupField(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range len(left) {
		l, r := left[i], right[i]
		if l >= 'A' && l <= 'Z' {
			l += 'a' - 'A'
		}
		if r >= 'A' && r <= 'Z' {
			r += 'a' - 'A'
		}
		if l != r {
			return false
		}
	}
	return true
}

func agentLookupMatchKind(query string, agent *store.AgentEntry) string {
	if agent != nil &&
		(equalAgentLookupField(query, agent.AgentID) ||
			equalAgentLookupField(query, agent.Name) ||
			equalAgentLookupField(query, agent.RegisteredName) ||
			equalAgentLookupField(query, agent.Provider)) {
		return "exact"
	}
	return "substring"
}

// handleFindRegisteredAgents is the signed, bounded companion to the public
// roster endpoint. MCP recipient discovery must not fetch ListAgents merely to
// return at most 20 matches: that full endpoint computes every agent's derived
// memory count. Local substring matching uses a capped metadata-only query.
func (s *Server) handleFindRegisteredAgents(w http.ResponseWriter, r *http.Request) {
	if s.agentStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Agent store unavailable", "Agent store not configured.")
		return
	}
	finder, ok := s.agentStore.(agentNameFinder)
	if !ok {
		writeProblem(w, http.StatusNotImplemented, "Agent lookup unavailable", "The configured agent store does not support bounded name lookup.")
		return
	}
	name := strings.TrimSpace(r.URL.Query().Get("name"))
	if name == "" || len(name) > 512 {
		writeProblem(w, http.StatusBadRequest, "Invalid agent name", "name must be between 1 and 512 bytes.")
		return
	}
	limit := 20
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil || parsed < 1 || parsed > 20 {
			writeProblem(w, http.StatusBadRequest, "Invalid agent limit", "limit must be between 1 and 20.")
			return
		}
		limit = parsed
	}
	// SQL status is only a discovery projection after app-v23; canonical active
	// enrollment lives in Badger. Page the bounded SQL candidates until the
	// requested number of canonical recipients is found or the query is
	// exhausted. Applying the public limit before this filter lets 20 pending
	// self-registrations hide every later active match.
	pager, paged := s.agentStore.(agentNamePageFinder)
	candidateFinder, hasCandidateFinder := s.agentStore.(agentLookupCandidateFinder)
	sanitized := make([]agentLookupResult, 0, limit)
	seen := make(map[string]struct{}, limit)
	foundExact := false
	for offset := 0; len(sanitized) < limit; {
		if !hasCandidateFinder && paged && offset/agentLookupCandidatePageSize >= maxAgentLookupCandidatePages {
			writeProblem(w, http.StatusServiceUnavailable, "Lookup incomplete",
				"The bounded agent candidate scan was exhausted; narrow the name query.")
			return
		}
		var agents []*store.AgentEntry
		var err error
		if hasCandidateFinder {
			agents, err = candidateFinder.FindAgentLookupCandidates(
				r.Context(), name, agentLookupCandidateBatchSize,
			)
		} else if paged {
			agents, err = pager.FindAgentsByNamePage(
				r.Context(), name, agentLookupCandidatePageSize, offset,
			)
		} else {
			// Compatibility for tests and third-party stores that implement the
			// original bounded finder but not the paged extension.
			agents, err = finder.FindAgentsByName(r.Context(), name, limit)
		}
		if err != nil {
			writeProblem(w, http.StatusInternalServerError, "Lookup error",
				"The agent directory could not be searched.")
			return
		}
		for _, agent := range agents {
			if agent == nil {
				continue
			}
			if _, duplicate := seen[agent.AgentID]; duplicate {
				continue
			}
			seen[agent.AgentID] = struct{}{}
			active, activeErr := s.appV23ActiveOrdinaryAgent(agent.AgentID)
			if activeErr != nil {
				writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
					"Current local enrollment state is unavailable.")
				return
			}
			if !active {
				continue
			}
			matchKind := agentLookupMatchKind(name, agent)
			foundExact = foundExact || matchKind == "exact"
			sanitized = append(sanitized, agentLookupResult{
				AgentEntry: sanitizeAgentForRead(agent, false),
				MatchKind:  matchKind,
			})
			if len(sanitized) == limit {
				break
			}
		}
		if hasCandidateFinder && len(agents) == agentLookupCandidateBatchSize && len(sanitized) < limit && !foundExact {
			writeProblem(w, http.StatusServiceUnavailable, "Lookup incomplete",
				"The bounded agent candidate scan was exhausted; narrow the name query.")
			return
		}
		if hasCandidateFinder || !paged || len(agents) < agentLookupCandidatePageSize {
			break
		}
		offset += len(agents)
	}
	writeJSON(w, http.StatusOK, map[string]any{"agents": sanitized, "total": len(sanitized)})
}
