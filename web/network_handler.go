package web

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// AgentStoreProvider is implemented by stores that support agent management.
type AgentStoreProvider interface {
	store.AgentStore
}

// roleTemplate defines a preset role with defaults.
type roleTemplate struct {
	Name      string `json:"name"`
	Role      string `json:"role"`
	Bio       string `json:"bio"`
	Clearance int    `json:"clearance"`
	Avatar    string `json:"avatar"`
}

var defaultTemplates = []roleTemplate{
	{Name: "Coding Assistant", Role: "member", Bio: "AI coding assistant (Claude Code, Cursor, etc.) that builds institutional memory from development sessions", Clearance: 1, Avatar: "\U0001F4BB"},
	{Name: "Voice Assistant", Role: "member", Bio: "Voice-activated agent for hands-free memory capture and recall", Clearance: 1, Avatar: "\U0001F399\uFE0F"},
	{Name: "Research Agent", Role: "member", Bio: "Autonomous research agent that gathers and synthesizes knowledge", Clearance: 2, Avatar: "\U0001F52C"},
	{Name: "Family Member", Role: "member", Bio: "Personal agent for a family member sharing the knowledge network", Clearance: 1, Avatar: "\U0001F464"},
	{Name: "Security Monitor", Role: "observer", Bio: "Read-only agent monitoring security-relevant memories", Clearance: 3, Avatar: "\U0001F6E1\uFE0F"},
	{Name: "Custom", Role: "member", Bio: "", Clearance: 1, Avatar: "\U0001F916"},
}

// memoryReassignLogFingerprint keeps request-controlled agent IDs out of log
// records while preserving a stable value operators can use to correlate
// repeated failures. The fixed-size lowercase hex representation cannot carry
// CR/LF or other control characters into a log sink.
func memoryReassignLogFingerprint(agentID string) string {
	digest := sha256.Sum256([]byte(agentID))
	return hex.EncodeToString(digest[:12])
}

// RegisterNetworkRoutes registers all /v1/dashboard/network/ routes.
func (h *DashboardHandler) RegisterNetworkRoutes(r chi.Router) {
	agentStore, ok := h.store.(AgentStoreProvider)
	if !ok {
		return // Store doesn't support agent management
	}

	r.With(h.cerebrumOperatorGate).
		Get("/v1/dashboard/network/agents", h.handleListAgents(agentStore))
	r.With(h.cerebrumOperatorGate).
		Get("/v1/dashboard/network/agents/{id}", h.handleGetAgent(agentStore))
	r.Post("/v1/dashboard/network/agents", h.handleCreateAgent(agentStore))
	r.Patch("/v1/dashboard/network/agents/{id}", h.handleUpdateAgent(agentStore))
	r.Delete("/v1/dashboard/network/agents/{id}", h.handleRemoveAgent(agentStore))
	r.With(h.cerebrumOperatorGate).Get("/v1/dashboard/network/agents/{id}/bundle", h.handleDownloadBundle(agentStore))
	r.Post("/v1/dashboard/network/agents/{id}/rotate-key", h.handleRotateAgentKey(agentStore))
	r.Get("/v1/dashboard/network/templates", handleTemplates())
	r.Get("/v1/dashboard/network/redeploy/status", h.handleRedeployStatusLive)
	r.Post("/v1/dashboard/network/redeploy", h.handleTriggerRedeploy)
	r.Post("/v1/dashboard/network/redeploy/clear", h.handleClearRedeploy)

	r.With(h.cerebrumOperatorGate).
		Get("/v1/dashboard/network/unregistered", h.handleUnregisteredAgents(agentStore))
	r.Post("/v1/dashboard/network/merge", h.handleMergeAgent(agentStore))
	r.With(h.cerebrumOperatorGate).
		Get("/v1/dashboard/network/agents/{id}/tags", h.handleAgentTags(agentStore))
	// v11.3: RBAC domain-ownership transfer (honest replacement for the retired
	// authorship-rewrite transfer-tag/transfer-domain paths). handleAgentDomains
	// lists an agent's RBAC domains; handleReassignDomainOwnership moves a
	// domain's ownership + access on-chain without rewriting authorship.
	r.With(h.cerebrumOperatorGate).
		Get("/v1/dashboard/network/agents/{id}/domains", h.handleAgentDomains(agentStore))
	r.Post("/v1/dashboard/network/reassign-domain-ownership", h.handleReassignDomainOwnership(agentStore))
	r.With(h.cerebrumOperatorGate).Get("/v1/dashboard/network/access", h.handleAppV23AccessState(agentStore))
	r.With(h.cerebrumOperatorGate).Put("/v1/dashboard/network/access/agents/{id}/policy", h.handleAppV23AgentPolicy())
	r.With(h.cerebrumOperatorGate).Put("/v1/dashboard/network/access/agents/{id}/name", h.handleAppV26AgentDisplayName())
	r.With(h.cerebrumOperatorGate).Put("/v1/dashboard/network/access/groups/{groupID}", h.handleAppV23AccessGroupPut())
	r.With(h.cerebrumOperatorGate).Delete("/v1/dashboard/network/access/groups/{groupID}", h.handleAppV23AccessGroupDelete())
	r.With(h.cerebrumOperatorGate).Get("/v1/dashboard/network/access/linked-readers", h.handleAppV23LinkedReadersList())
	r.With(h.cerebrumOperatorGate).Post("/v1/dashboard/network/access/linked-readers/eligibility", h.handleAppV23LinkedReaderEligibility())
	r.With(h.cerebrumOperatorGate).Post("/v1/dashboard/network/access/linked-readers", h.handleAppV23LinkedReaderMutation())
	r.With(h.cerebrumOperatorGate).Get("/v1/dashboard/network/access/linked-messages/candidates", h.handleAppV23RemoteHostedLinkedMessageCandidates())
	r.With(h.cerebrumOperatorGate).Get("/v1/dashboard/network/access/linked-messages/consent", h.handleAppV23LinkedMessageConsentGet())
	r.With(h.cerebrumOperatorGate).Put("/v1/dashboard/network/access/linked-messages/consent", h.handleAppV23LinkedMessageConsentPut())
	r.Post("/v1/dashboard/network/access/root/handover", h.handleAppV23RootCredentialHandover())

	// Pairing code generation (authenticated — admin creates code for an agent)
	if h.Pairing != nil {
		registerPairingCreateRoute(r, agentStore, h.Pairing, h.appV23IsRootIdentity)
	}
}

func (h *DashboardHandler) handleListAgents(agentStore store.AgentStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		agents, err := agentStore.ListAgents(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if agents == nil {
			agents = []*store.AgentEntry{}
		}
		filtered := make([]*store.AgentEntry, 0, len(agents))
		for _, agent := range agents {
			if agent == nil || h.appV23IsRootIdentity(agent.AgentID) {
				continue
			}
			filtered = append(filtered, agent)
		}
		agents = filtered
		if h.BadgerStore != nil {
			for _, agent := range agents {
				if onChain, getErr := h.BadgerStore.GetRegisteredAgent(agent.AgentID); getErr == nil && onChain != nil {
					overlayOnChainAgentPolicy(agent, onChain)
				}
			}
		}
		writeJSONResp(w, http.StatusOK, map[string]any{"agents": agents})
	}
}

func (h *DashboardHandler) handleGetAgent(agentStore store.AgentStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if h.appV23IsRootIdentity(id) {
			writeError(w, http.StatusNotFound, "agent not found")
			return
		}
		agent, err := agentStore.GetAgent(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusNotFound, "agent not found")
			return
		}
		if h.BadgerStore != nil {
			if onChain, getErr := h.BadgerStore.GetRegisteredAgent(agent.AgentID); getErr == nil && onChain != nil {
				overlayOnChainAgentPolicy(agent, onChain)
			}
		}
		writeJSONResp(w, http.StatusOK, agent)
	}
}

// overlayOnChainAgentPolicy keeps CEREBRUM's access controls honest when an
// old SQLite projection is stale. Badger is the consensus authority for every
// policy-bearing field; SQLite remains the source for display metadata.
func overlayOnChainAgentPolicy(agent *store.AgentEntry, onChain *store.OnChainAgent) {
	if agent == nil || onChain == nil {
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

func (h *DashboardHandler) handleCreateAgent(agentStore store.AgentStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !h.isCEREBRUMOperatorRequest(r) {
			writeCEREBRUMOperatorForbidden(w, "Creating agents requires operator authority.")
			return
		}
		var req struct {
			Name         string `json:"name"`
			Role         string `json:"role"`
			Avatar       string `json:"avatar"`
			BootBio      string `json:"boot_bio"`
			Clearance    int    `json:"clearance"`
			OrgID        string `json:"org_id"`
			DeptID       string `json:"dept_id"`
			DomainAccess string `json:"domain_access"`
			P2PAddress   string `json:"p2p_address"`
			Provider     string `json:"provider"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		if req.Name == "" {
			writeError(w, http.StatusBadRequest, "name is required")
			return
		}
		if req.Role == "" {
			req.Role = "member"
		}
		requestedRole := strings.ToLower(strings.TrimSpace(req.Role))
		if requestedRole == "" {
			requestedRole = store.AppV23RoleMember
		}
		appV23PendingApproval := h.appV23IsActive()
		if appV23PendingApproval {
			if strings.TrimSpace(h.CometBFTRPC) == "" || h.BadgerStore == nil {
				writeAppV23AccessError(w, http.StatusServiceUnavailable, "registration_consensus_unavailable",
					"App-v23 agent creation requires commit-confirmed consensus state.")
				return
			}
			root, rootErr := h.BadgerStore.GetAppV23Root()
			if rootErr != nil || root == nil {
				writeAppV23AccessError(w, http.StatusServiceUnavailable, "root_state_unavailable",
					"Current CEREBRUM Root state is unavailable; no agent was generated.")
				return
			}
			// AgentRegister is discoverability, not elevation. Every new
			// app-v23 principal enters as a capability-restricted pending
			// Member; Manager/Admin are applied later by tx-36 approval with
			// target consent. Do not persist or stage any requested legacy
			// policy field before that atomic approval.
			req.Role = store.AppV23RoleMember
			req.Clearance = 0
			req.OrgID = ""
			req.DeptID = ""
			req.DomainAccess = ""
		} else {
			normalizedDomainAccess, normalizeErr := normalizeDomainAccessBlob(req.DomainAccess)
			if normalizeErr != nil {
				writeError(w, http.StatusBadRequest, "invalid domain access policy")
				return
			}
			if err := h.validateDomainAccessBlob(normalizedDomainAccess); err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			req.DomainAccess = normalizedDomainAccess
			if req.Clearance < 0 || req.Clearance > 4 {
				req.Clearance = 1
			}
		}

		// Generate Ed25519 keypair server-side
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "key generation failed")
			return
		}
		agentID := hex.EncodeToString(pub)
		seed := priv.Seed()

		// Generate CometBFT-compatible validator key
		validatorPubkey := base64.StdEncoding.EncodeToString(pub)

		agent := &store.AgentEntry{
			AgentID:         agentID,
			Name:            req.Name,
			Role:            req.Role,
			Avatar:          req.Avatar,
			BootBio:         req.BootBio,
			ValidatorPubkey: validatorPubkey,
			Status:          "pending",
			Clearance:       req.Clearance,
			OrgID:           req.OrgID,
			DeptID:          req.DeptID,
			DomainAccess:    req.DomainAccess,
			P2PAddress:      req.P2PAddress,
			Provider:        req.Provider,
		}

		if createErr := agentStore.CreateAgent(r.Context(), agent); createErr != nil {
			writeError(w, http.StatusInternalServerError, createErr.Error())
			return
		}

		// Broadcast on-chain registration through CometBFT (non-blocking).
		// The ABCI processor will set on_chain_height in BadgerDB.
		if !appV23PendingApproval && h.CometBFTRPC != "" && h.SigningKey != nil {
			h.runBackground(func(_ context.Context) {
				registrationKey := h.SigningKey
				registerTx := &tx.ParsedTx{
					Type:      tx.TxTypeAgentRegister,
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
				// Through the nonce lease, not a bare MonotonicNonce. This tx
				// shares h.SigningKey with every commit-confirmed dashboard
				// mutation, so allocating outside a lease let a concurrent
				// mutation's higher nonce overtake it and app-v9 rejected this
				// one Code 4 — invisibly, because nobody is waiting on a
				// fire-and-forget broadcast. The agent then simply never
				// appeared on-chain and the dashboard showed it un-synced
				// forever. The nonce and the timestamp are stamped inside the
				// lease; setting them here would be a nonce allocated outside
				// the lock that makes it valid.
				signCtx, cancelSign := context.WithTimeout(context.Background(), backgroundSigningBudget())
				bErr := h.signAndBroadcastSyncContext(signCtx, registerTx, registrationKey)
				cancelSign()
				// AgentRegister should land before AgentSetPermission (the agent
				// has to exist on-chain first). The lease does NOT provide that
				// ordering — the permission tx below is signed by a different
				// key (h.AdminSigningKey) and so never contends for this one's
				// slot. What provides it, same as before this change: this call
				// returns only once CheckTx has RUN on the registration.
				// ADMISSION IS NOT VERIFIED — broadcast_tx_sync's response body
				// is deliberately not decoded (see broadcastTxSyncContext), so a
				// 200 carrying a non-zero CheckTx code (refused, NOT admitted)
				// is indistinguishable from admission here. On such a refusal
				// the permission tx below is broadcast anyway and references an
				// agent that never entered the mempool; it is orphaned, the ABCI
				// processor leaves on_chain_height unset, and the dashboard
				// surfaces the agent as un-synced. This runs after the 201
				// response, so failures can only be logged.
				if bErr != nil {
					log.Printf("agent-create: on-chain AgentRegister broadcast failed for %s: %v", agentID, bErr)
					return
				}
				// Sync the wizard-chosen permissions on-chain immediately, so
				// BadgerDB matches SQLite at creation instead of only after a later
				// edit (the create-time on-chain/off-chain drift the audit flagged:
				// authoritative on-chain RBAC checks otherwise disagreed with the
				// dashboard right after an agent was created).
				permTx := &tx.ParsedTx{
					Type: tx.TxTypeAgentSetPermission,
					AgentSetPermission: &tx.AgentSetPermission{
						AgentID:       agentID,
						Clearance:     uint8(agent.Clearance), // #nosec G115 -- clearance is 0-4
						DomainAccess:  agent.DomainAccess,
						VisibleAgents: agent.VisibleAgents,
						OrgID:         agent.OrgID,
						DeptID:        agent.DeptID,
					},
				}
				if len(h.AdminSigningKey) != ed25519.PrivateKeySize {
					log.Printf("agent-create: on-chain AgentSetPermission skipped for %s: genesis admin key unavailable", agentID)
				} else if _, _, _, bErr := h.signAndBroadcastCommit(permTx, h.AdminSigningKey); bErr != nil {
					log.Printf("agent-create: on-chain AgentSetPermission broadcast failed for %s: %v", agentID, bErr)
				}
			})
		}

		// Generate and save agent bundle
		bundleDir := filepath.Join(sageHome(), "bundles", agentID)
		if mkErr := os.MkdirAll(bundleDir, 0700); mkErr != nil {
			writeError(w, http.StatusInternalServerError, "failed to create bundle dir")
			return
		}

		// Save agent key (seed)
		if wErr := os.WriteFile(filepath.Join(bundleDir, "agent.key"), seed, 0600); wErr != nil {
			writeError(w, http.StatusInternalServerError, "failed to save agent key")
			return
		}

		// Generate bundle ZIP
		bundlePath, err := generateBundle(bundleDir, agent, seed)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "bundle generation failed: "+err.Error())
			return
		}

		// Update agent with bundle path
		agent.BundlePath = bundlePath
		_ = agentStore.UpdateAgent(r.Context(), agent)

		// Generate one-time claim token for CLI install
		claimToken, tokenErr := generateClaimToken()
		if tokenErr != nil {
			writeError(w, http.StatusInternalServerError, "failed to generate claim token")
			return
		}
		claimExpiry := time.Now().Add(24 * time.Hour)
		agent.ClaimToken = claimToken
		agent.ClaimExpiresAt = &claimExpiry
		if updateErr := agentStore.UpdateAgent(r.Context(), agent); updateErr != nil {
			writeError(w, http.StatusInternalServerError, "failed to save claim token")
			return
		}

		var registrationHash string
		var registrationHeight int64
		if appV23PendingApproval {
			// The target's seed and recovery bundle are durable before its
			// self-registration can commit. This avoids an irrecoverable
			// pending principal whose consent key existed only in a goroutine.
			registerTx := &tx.ParsedTx{
				Type: tx.TxTypeAgentRegister,
				AgentRegister: &tx.AgentRegister{
					AgentID:    agentID,
					Name:       req.Name,
					Role:       store.AppV23RoleMember,
					BootBio:    req.BootBio,
					Provider:   req.Provider,
					P2PAddress: req.P2PAddress,
				},
			}
			registrationHash, registrationHeight, _, err = h.signAndBroadcastCommit(registerTx, priv)
			if err != nil {
				// Nothing signed or sent is not consensus declining the
				// registration; see writeSignerNotSentIfHeld.
				if writeSignerNotSentIfHeld(w, err) {
					return
				}
				writeAppV23AccessError(w, http.StatusBadGateway, "agent_registration_rejected",
					"The agent key is safely stored, but consensus did not register it. No approval or elevated policy was created.")
				return
			}
			registered, readErr := h.BadgerStore.GetRegisteredAgent(agentID)
			if readErr != nil || registered == nil ||
				registered.Role != store.AppV23RoleMember ||
				registered.Capabilities != store.DefaultSelfRegisteredAgentCapabilities {
				writeAppV23AccessError(w, http.StatusServiceUnavailable, "agent_registration_unconfirmed",
					"Consensus returned success but the restricted pending-agent state could not be confirmed.")
				return
			}
			agent.Capabilities = store.DefaultSelfRegisteredAgentCapabilities
			if updateErr := agentStore.UpdateAgent(r.Context(), agent); updateErr != nil {
				writeAppV23AccessError(w, http.StatusInternalServerError, "agent_projection_failed",
					"The agent registered with consensus, but its local dashboard projection could not be updated.")
				return
			}
		}

		response := map[string]any{
			"agent":             agent,
			"agent_id":          agentID,
			"claim_token":       claimToken,
			"install_command":   fmt.Sprintf("sage-gui mcp install --token %s", claimToken),
			"approval_required": appV23PendingApproval,
			"requested_role":    requestedRole,
		}
		if appV23PendingApproval {
			response["tx_hash"] = registrationHash
			response["height"] = registrationHeight
		}
		writeJSONResp(w, http.StatusCreated, response)
	}
}

func (h *DashboardHandler) handleUpdateAgent(agentStore store.AgentStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if h.appV23IsRootIdentity(id) {
			writeAppV23AccessError(w, http.StatusForbidden, "root_agent_surface_forbidden",
				"CEREBRUM Root is not an agent and cannot be edited through agent management.")
			return
		}
		if h.appV23IsActive() && !h.isCEREBRUMOperatorRequest(r) {
			writeCEREBRUMOperatorForbidden(w, "Changing local agent metadata requires current CEREBRUM Admin authority.")
			return
		}

		existing, err := agentStore.GetAgent(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusNotFound, "agent not found")
			return
		}
		// Policy is consensus-authoritative. Start edits from Badger rather
		// than a potentially stale SQLite projection, and retain a complete
		// snapshot so any rejected transaction can be rolled back atomically.
		if h.BadgerStore != nil {
			if onChain, getErr := h.BadgerStore.GetRegisteredAgent(id); getErr == nil && onChain != nil {
				overlayOnChainAgentPolicy(existing, onChain)
			}
		}
		policyBeforeUpdate := *existing
		oldDomainAccess := existing.DomainAccess

		var req struct {
			Name          *string                    `json:"name"`
			Role          *string                    `json:"role"`
			Avatar        *string                    `json:"avatar"`
			BootBio       *string                    `json:"boot_bio"`
			Clearance     *int                       `json:"clearance"`
			OrgID         *string                    `json:"org_id"`
			DeptID        *string                    `json:"dept_id"`
			DomainAccess  *string                    `json:"domain_access"`
			P2PAddress    *string                    `json:"p2p_address"`
			VisibleAgents *string                    `json:"visible_agents"`
			Capabilities  *uint32                    `json:"capabilities"`
			AdminOverride []adminOverrideExpectation `json:"admin_override"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		// Permission-bearing fields are node-operator policy, not agent
		// self-service. A valid agent signature proves only possession of that
		// agent's key; allowing it through here would make the dashboard's admin
		// key sign arbitrary self-elevation (clearance/org/domain/visibility)
		// or let one agent rewrite another agent's policy.
		sensitivePolicyChange := req.Role != nil ||
			req.Clearance != nil ||
			req.OrgID != nil ||
			req.DeptID != nil ||
			req.DomainAccess != nil ||
			req.VisibleAgents != nil ||
			req.Capabilities != nil ||
			len(req.AdminOverride) > 0
		if sensitivePolicyChange && h.appV23IsActive() {
			writeAppV23AccessError(w, http.StatusGone, "legacy_permission_route_retired",
				"This legacy agent route cannot change app-v23 access policy. Use the atomic Access Controls policy endpoint.")
			return
		}
		if sensitivePolicyChange && !h.isCEREBRUMOperatorRequest(r) {
			writeCEREBRUMOperatorForbidden(w, "Changing agent permissions requires operator authority.")
			return
		}
		// app-v26 makes an agent's operator-facing display label consensus
		// governed and treats its registered identity and boot purpose as
		// immutable provenance. Keeping this legacy SQLite-only path open would
		// let the Agents page display a name that consensus never committed, or
		// rewrite the purpose used to explain why a principal was enrolled. The
		// dedicated Access Controls endpoint copies every immutable field from
		// current consensus state and changes only Name.
		if h.appV26IsActive() && (req.Name != nil || req.BootBio != nil) {
			writeAppV23AccessError(w, http.StatusGone, "governed_agent_metadata_required",
				"App-v26 display names are changed through Access Controls. Registered identity and boot purpose are immutable.")
			return
		}
		overrides := make(map[string]adminOverrideExpectation, len(req.AdminOverride))
		if len(req.AdminOverride) > 0 {
			// The genesis key is a human/operator capability. A cryptographically
			// valid agent request must never be able to turn a JSON flag into an
			// admin-signed grant. The sensitive-policy gate above admits only a
			// valid encrypted local CEREBRUM session or the exact cryptographically
			// verified node-operator identity.
			for _, expected := range req.AdminOverride {
				expected.Domain = strings.TrimSpace(expected.Domain)
				expected.OwnerID = strings.TrimSpace(expected.OwnerID)
				expected.OwnedDomain = strings.TrimSpace(expected.OwnedDomain)
				if expected.Domain == "" || expected.OwnerID == "" || expected.OwnedDomain == "" || expected.Level < 0 || expected.Level > 3 {
					writeError(w, http.StatusBadRequest, "invalid administrator override confirmation")
					return
				}
				if _, duplicate := overrides[expected.Domain]; duplicate {
					writeError(w, http.StatusBadRequest, "duplicate administrator override domain")
					return
				}
				overrides[expected.Domain] = expected
			}
		}

		if req.Name != nil {
			existing.Name = *req.Name
		}
		// Role is authoritative ON-CHAIN (set at AgentRegister) and there is no
		// AgentSetPermission field to mutate it, so persisting a role change here
		// would diverge SQLite from BadgerDB while the GUI shows "Saved". Do NOT
		// write it; surface an honest warning if a change was attempted. (Full
		// on-chain role mutation is a deferred consensus change.)
		roleChangeRejected := req.Role != nil && *req.Role != existing.Role
		if req.Avatar != nil {
			existing.Avatar = *req.Avatar
		}
		if req.BootBio != nil {
			existing.BootBio = *req.BootBio
		}
		if req.Clearance != nil {
			// Clamp to the valid 0-4 clearance band (as create does) so the later
			// uint8() narrowing cannot truncate an out-of-range value and diverge
			// the on-chain clearance from what is stored/displayed.
			c := *req.Clearance
			if c < 0 {
				c = 0
			} else if c > 4 {
				c = 4
			}
			existing.Clearance = c
		}
		if req.OrgID != nil {
			existing.OrgID = *req.OrgID
		}
		if req.DeptID != nil {
			existing.DeptID = *req.DeptID
		}
		if req.DomainAccess != nil {
			normalizedDomainAccess, normalizeErr := normalizeDomainAccessBlob(*req.DomainAccess)
			if normalizeErr != nil {
				writeError(w, http.StatusBadRequest, "invalid domain access policy")
				return
			}
			if err := h.validateDomainAccessBlob(normalizedDomainAccess); err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			*req.DomainAccess = normalizedDomainAccess
			existing.DomainAccess = normalizedDomainAccess
		}
		if req.P2PAddress != nil {
			existing.P2PAddress = *req.P2PAddress
		}
		if req.VisibleAgents != nil {
			existing.VisibleAgents = *req.VisibleAgents
		}
		capabilitiesBeforeUpdate := existing.Capabilities
		capabilitiesChanged := false
		if req.Capabilities != nil {
			capabilities := store.AgentCapabilities(*req.Capabilities)
			if !capabilities.Valid() {
				writeError(w, http.StatusBadRequest, "invalid agent capabilities")
				return
			}
			capabilitiesChanged = capabilities != capabilitiesBeforeUpdate
			if capabilitiesChanged && (h.AppV22ActiveFn == nil || !h.AppV22ActiveFn()) {
				writeError(w, http.StatusConflict, "agent capabilities require app-v22 activation")
				return
			}
			if capabilitiesChanged &&
				(h.CometBFTRPC == "" || h.SigningKey == nil || len(h.AdminSigningKey) != ed25519.PrivateKeySize) {
				writeError(w, http.StatusServiceUnavailable, "agent capabilities require an available consensus signer")
				return
			}
			// Capabilities are consensus-authoritative. Do not put the requested
			// mask into the SQL metadata projection before Comet commit; ABCI's
			// committed projection (and the Badger read overlay) publishes it.
			// Keeping existing.Capabilities unchanged also means a successful
			// broadcast response cannot make an uncommitted mask visible during
			// the Commit/projection window.
		}
		if len(overrides) > 0 {
			desired := parseDomainAccessLevels(existing.DomainAccess)
			if desired == nil {
				writeError(w, http.StatusBadRequest, "invalid domain access policy")
				return
			}
			for domain, expected := range overrides {
				if desired[domain] != expected.Level {
					writeError(w, http.StatusConflict, "administrator override no longer matches the selected access; review and confirm again")
					return
				}
			}
		}

		policyUpdateRequested := req.Clearance != nil || req.DomainAccess != nil ||
			req.VisibleAgents != nil || req.OrgID != nil || req.DeptID != nil ||
			capabilitiesChanged
		if policyUpdateRequested && h.BadgerStore != nil &&
			(h.CometBFTRPC == "" || h.SigningKey == nil || len(h.AdminSigningKey) != ed25519.PrivateKeySize) {
			writeError(w, http.StatusServiceUnavailable, "agent permission update requires an available consensus signer")
			return
		}

		if err := agentStore.UpdateAgent(r.Context(), existing); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		// Broadcast the update through CometBFT synchronously so the GUI knows
		// whether the on-chain state actually updated. Without this, a silent
		// broadcast failure causes SQLite and BadgerDB to diverge (the
		// "split-brain rename" bug). The metadata (name/boot_bio) and the
		// SECURITY-critical permission (clearance/domain/visible) syncs are
		// INDEPENDENT: a failed metadata broadcast must NOT skip the permission
		// broadcast (which would leave SQLite showing new clearance/domain the
		// chain never got, with no warning).
		var warnings []string
		if roleChangeRejected {
			warnings = append(warnings, "role is set at registration and cannot be changed here (unchanged)")
		}
		if !h.appV23IsActive() && h.CometBFTRPC != "" && h.SigningKey != nil {
			// Metadata changes (name, boot_bio) go through AgentUpdate.
			if req.Name != nil || req.BootBio != nil {
				if err := h.broadcastAgentUpdate(id, existing.Name, existing.BootBio); err != nil {
					warnings = append(warnings, "on-chain sync failed: "+err.Error())
				}
			}
			// Permission changes go through AgentSetPermission - attempted
			// regardless of the metadata result above. org_id/dept_id are part of
			// the on-chain RBAC record (the tx carries them), so an org/dept-only
			// edit must broadcast too, else SQLite diverges from BadgerDB silently.
			if policyUpdateRequested {
				clearance := uint8(existing.Clearance) // #nosec G115 -- clamped to 0-4 above
				var wireCapabilities uint32
				if req.Capabilities != nil {
					wireCapabilities = *req.Capabilities
				}
				permTx := &tx.ParsedTx{
					Type: tx.TxTypeAgentSetPermission,
					AgentSetPermission: &tx.AgentSetPermission{
						AgentID:             id,
						Clearance:           clearance,
						DomainAccess:        existing.DomainAccess,
						VisibleAgents:       existing.VisibleAgents,
						OrgID:               existing.OrgID,
						DeptID:              existing.DeptID,
						Capabilities:        wireCapabilities,
						CapabilitiesPresent: capabilitiesChanged,
					},
				}
				if txHash, height, _, bErr := h.signAndBroadcastCommit(permTx, h.AdminSigningKey); bErr != nil {
					// Preserve unrelated metadata edits, but restore every
					// consensus-owned policy field. Returning 2xx with a warning
					// here made the dashboard claim grants that Badger denied.
					// The restore runs for a fenced signer too: whether refused or
					// never sent, the chain does not have this change, and the
					// projection must mirror the chain.
					existing.Role = policyBeforeUpdate.Role
					existing.Clearance = policyBeforeUpdate.Clearance
					existing.OrgID = policyBeforeUpdate.OrgID
					existing.DeptID = policyBeforeUpdate.DeptID
					existing.DomainAccess = policyBeforeUpdate.DomainAccess
					existing.VisibleAgents = policyBeforeUpdate.VisibleAgents
					existing.Capabilities = policyBeforeUpdate.Capabilities
					if restoreErr := agentStore.UpdateAgent(r.Context(), existing); restoreErr != nil {
						writeError(w, http.StatusInternalServerError, "permission transaction failed and the local projection could not be restored")
						return
					}
					// FIRST among the response choices: a fenced/quiesced signer
					// means the permission tx was never signed or sent, so
					// answering "was rejected" below would report a verdict that
					// never happened.
					if writeSignerNotSentIfHeld(w, bErr) {
						return
					}
					writeError(w, http.StatusBadGateway, "agent permission transaction was rejected: "+bErr.Error())
					return
				} else {
					h.emitAccessActivity("permissions_updated", fmt.Sprintf("Permissions updated for %s", existing.Name), "", map[string]any{
						"agent_id": id, "agent_name": existing.Name, "clearance": clearance, "tx_hash": txHash, "height": height,
					})
				}
			}
		}
		onChainWarning := strings.Join(warnings, "; ")

		// v11.3: the read/write matrix now issues REAL on-chain access grants
		// (the enforced grant: keys), not just the cosmetic DomainAccess blob
		// broadcast above. Each grant/revoke is signed as the domain owner; any
		// domain whose owner key isn't local is reported as skipped so the UI
		// can be honest instead of claiming an enforcement that didn't happen.
		var grantResults []grantResult
		if req.DomainAccess != nil {
			grantResults = h.reconcileDomainGrants(id, oldDomainAccess, existing.DomainAccess, overrides)
		}

		resp := map[string]any{
			"agent_id":  existing.AgentID,
			"name":      existing.Name,
			"role":      existing.Role,
			"avatar":    existing.Avatar,
			"boot_bio":  existing.BootBio,
			"clearance": existing.Clearance,
			"status":    existing.Status,
		}
		if onChainWarning != "" {
			resp["on_chain_warning"] = onChainWarning
		}
		if h.appV23IsActive() && (req.Name != nil || req.Avatar != nil || req.BootBio != nil || req.P2PAddress != nil) {
			resp["metadata_scope"] = "local"
		}
		if len(grantResults) > 0 {
			resp["grant_results"] = grantResults
		}
		writeJSONResp(w, http.StatusOK, resp)
	}
}

func (h *DashboardHandler) handleRemoveAgent(agentStore store.AgentStore) http.HandlerFunc {
	legacy := handleRemoveAgentLegacy(agentStore)
	return func(w http.ResponseWriter, r *http.Request) {
		if !h.appV23IsActive() {
			legacy.ServeHTTP(w, r)
			return
		}
		actor, ok := h.requireAppV23ControlActor(w, r, true)
		if !ok {
			return
		}
		id := strings.TrimSpace(chi.URLParam(r, "id"))
		if _, err := auth.AgentIDToPublicKey(id); err != nil {
			writeAppV23AccessError(w, http.StatusBadRequest, "invalid_agent_id",
				"Agent ID must be canonical lowercase Ed25519 hex.")
			return
		}
		if h.appV23IsRootIdentity(id) {
			writeAppV23AccessError(w, http.StatusForbidden, "root_removal_forbidden",
				"CEREBRUM Root cannot be removed through agent management.")
			return
		}
		agent, err := agentStore.GetAgent(r.Context(), id)
		if err != nil {
			writeAppV23AccessError(w, http.StatusNotFound, "agent_not_found", "Agent not found.")
			return
		}
		enrollment, err := h.BadgerStore.GetAppV23Enrollment(id)
		if err != nil {
			writeAppV23AccessError(w, http.StatusServiceUnavailable, "enrollment_state_unavailable",
				"Consensus enrollment state could not be verified; the local agent record was left unchanged.")
			return
		}
		// Deletion is deliberately idempotent. A prior request may already have
		// committed the on-chain deactivation and updated the local projection
		// before the browser lost its response; retrying must settle the UI, not
		// send the operator back into an endless "removing" state.  Consensus is
		// authoritative, though: a stale local "removed" row must never hide an
		// active enrollment or leave live authority invisible to CEREBRUM.
		if agent.Status == "removed" && (enrollment == nil || !enrollment.Active) {
			writeJSONResp(w, http.StatusOK, map[string]any{
				"ok": true, "status": "removed", "already_removed": true,
				"consensus_active": false, "redeploy_required": false,
			})
			return
		}
		if agent.MemoryCount > 0 && r.URL.Query().Get("force") != "true" {
			writeJSONResp(w, http.StatusConflict, map[string]any{
				"ok": false, "code": "agent_has_memories", "error": "Agent has memories.",
				"memory_count": agent.MemoryCount,
				"message":      "Use ?force=true to deactivate it while preserving original memory attribution.",
			})
			return
		}
		if enrollment == nil {
			// Directory-only / historical records have never held an active
			// app-v23 standing. There is no on-chain authority to revoke, so
			// finish the local projection removal instead of making an otherwise
			// empty agent impossible to remove forever.
			if removeErr := agentStore.RemoveAgent(r.Context(), id); removeErr != nil {
				writeAppV23AccessError(w, http.StatusInternalServerError, "projection_update_failed",
					"The agent has no consensus enrollment, but its local dashboard record could not be marked removed.")
				return
			}
			writeJSONResp(w, http.StatusOK, map[string]any{
				"ok": true, "status": "removed", "consensus_active": false,
				"local_only": true, "redeploy_required": false,
			})
			return
		}
		role, err := h.BadgerStore.GetAppV23Role(id)
		if err != nil || role == nil {
			if !enrollment.Active {
				if removeErr := agentStore.RemoveAgent(r.Context(), id); removeErr != nil {
					writeAppV23AccessError(w, http.StatusInternalServerError, "projection_update_failed",
						"The agent is already inactive on-chain, but its local dashboard record could not be marked removed.")
					return
				}
				writeJSONResp(w, http.StatusOK, map[string]any{
					"ok": true, "status": "removed", "consensus_active": false,
					"already_inactive": true, "redeploy_required": false,
				})
				return
			}
			writeAppV23AccessError(w, http.StatusConflict, "role_state_missing",
				"The active consensus enrollment has no role state to deactivate.")
			return
		}
		var hash string
		var height int64
		if enrollment.Active {
			deactivatedProfile := enrollment.Profile
			deactivatedCapabilities := uint32(enrollment.Capabilities)
			if deactivatedProfile == store.AppV23ProfileLegacyRestricted {
				// Legacy-restricted is migration-only and cannot be retained
				// by a post-migration mutation. Deactivation is an explicit
				// operator decision, so retire it into the inert canonical
				// Member policy instead of trying to mint the hidden profile.
				deactivatedProfile = store.AppV23ProfileStandard
				deactivatedCapabilities = 0
			}
			ptx := &tx.ParsedTx{
				Type: tx.TxTypeLocalAgentApprove,
				LocalAgentApprove: &tx.LocalAgentApprove{
					AgentID: id, Active: false, Role: store.AppV23RoleMember,
					Profile: deactivatedProfile, HomeDomain: enrollment.HomeDomain,
					Clearance: enrollment.Clearance, Capabilities: deactivatedCapabilities,
					Scope: actor.Root.Scope, ExpectedRevision: enrollment.Revision,
					ExpectedRoleRevision: role.Revision,
				},
			}
			hash, height, _, err = h.signAndBroadcastAppV23Control(ptx, actor)
			if err != nil {
				// A fenced or quiesced signing key means nothing was signed or
				// sent, so there is no consensus verdict to report. Reporting
				// one would tell the operator the removal was refused when it
				// was only deferred.
				if writeSignerNotSentIfHeld(w, err) {
					return
				}
				writeAppV23AccessError(w, http.StatusConflict, "consensus_deactivation_rejected",
					"The agent was not removed because consensus did not commit its deactivation.")
				return
			}
		}
		committed, err := h.BadgerStore.GetAppV23Enrollment(id)
		if err != nil || committed == nil || committed.Active {
			writeAppV23AccessError(w, http.StatusServiceUnavailable, "deactivation_state_unconfirmed",
				"Consensus deactivation could not be confirmed; the local agent record was left unchanged.")
			return
		}
		groups, err := h.BadgerStore.ListAppV23AgentGroups(id)
		if err != nil || len(groups) != 0 {
			writeAppV23AccessError(w, http.StatusServiceUnavailable, "group_cleanup_unconfirmed",
				"Consensus group cleanup could not be confirmed; the local agent record was left unchanged.")
			return
		}
		if err := agentStore.RemoveAgent(r.Context(), id); err != nil {
			writeAppV23AccessError(w, http.StatusInternalServerError, "projection_update_failed",
				"The agent is deactivated on-chain, but its local dashboard projection could not be marked removed.")
			return
		}
		writeJSONResp(w, http.StatusOK, map[string]any{
			"ok": true, "status": "removed", "consensus_active": false,
			"tx_hash": hash, "height": height, "redeploy_required": false,
		})
	}
}

func handleRemoveAgentLegacy(agentStore store.AgentStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")

		agent, err := agentStore.GetAgent(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusNotFound, "agent not found")
			return
		}

		// Prevent removing the last admin — that would brick the network
		if agent.Role == "admin" {
			allAgents, listErr := agentStore.ListAgents(r.Context())
			if listErr == nil {
				adminCount := 0
				for _, a := range allAgents {
					if a.Role == "admin" && a.Status != "removed" {
						adminCount++
					}
				}
				if adminCount <= 1 {
					writeError(w, http.StatusForbidden, "cannot remove the last admin — the network needs at least one admin node")
					return
				}
			}
		}

		// Check for pending memories
		if agent.MemoryCount > 0 {
			q := r.URL.Query()
			force := q.Get("force") == "true"
			if !force {
				writeJSONResp(w, http.StatusConflict, map[string]any{
					"error":        "agent has memories",
					"memory_count": agent.MemoryCount,
					"message":      "Use ?force=true to remove anyway. Memories will be preserved with original attribution.",
				})
				return
			}
		}

		if err := agentStore.RemoveAgent(r.Context(), id); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		writeJSONResp(w, http.StatusOK, map[string]string{"status": "removed"})
	}
}

func (h *DashboardHandler) handleDownloadBundle(agentStore store.AgentStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if h.appV23IsRootIdentity(id) {
			writeError(w, http.StatusNotFound, "agent not found")
			return
		}

		agent, err := agentStore.GetAgent(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusNotFound, "agent not found")
			return
		}

		if agent.BundlePath == "" {
			writeError(w, http.StatusNotFound, "no bundle available")
			return
		}
		writeAgentBundle(w, agent.BundlePath, agent.Name)
	}
}

func writeAgentBundle(w http.ResponseWriter, bundlePath, name string) {
	data, err := os.ReadFile(bundlePath) //nolint:gosec // server-controlled or trusted stored bundle path
	if err != nil {
		writeError(w, http.StatusInternalServerError, "bundle file not found")
		return
	}

	safeName := strings.Map(func(r rune) rune {
		if r == '"' || r == '\\' || r == '\r' || r == '\n' || r < 32 {
			return '_'
		}
		return r
	}, name)
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="sage-agent-%s.zip"`, safeName))
	w.Write(data) //nolint:errcheck,gosec // server-generated ZIP archive, not user input
}

func (h *DashboardHandler) handleRotateAgentKey(agentStore store.AgentStore) http.HandlerFunc {
	legacy := handleRotateAgentKeyLegacy(agentStore)
	return func(w http.ResponseWriter, r *http.Request) {
		if h.appV23IsActive() {
			writeAppV23AccessError(w, http.StatusConflict, "agent_key_rotation_requires_reenrollment",
				"App-v23 agent identities are replaced through re-enrollment. Root handover is available only on the separate CEREBRUM Root authority card.")
			return
		}
		legacy.ServeHTTP(w, r)
	}
}

const appV23RootHandoverPhrase = "ROTATE CEREBRUM ROOT"

func (h *DashboardHandler) handleAppV23RootCredentialHandover() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// A successful response contains the only network-delivered copy of a
		// live Root recovery credential. It must never be cached or retrievable
		// through a generic/current-Root download URL.
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Pragma", "no-cache")
		if !h.appV23IsActive() {
			writeAppV23AccessError(w, http.StatusConflict, "app_v23_inactive",
				"Root credential handover requires app-v23 activation.")
			return
		}
		actor, ok := h.requireAppV23ControlActor(w, r, true)
		if !ok {
			return
		}
		if !actor.IsRoot {
			writeAppV23AccessError(w, http.StatusForbidden, "root_rotation_requires_root",
				"Only the current CEREBRUM Root can rotate the Root credential.")
			return
		}
		var confirmation struct {
			ConfirmIrreversible bool   `json:"confirm_irreversible"`
			ConfirmationPhrase  string `json:"confirmation_phrase"`
			ExpectedGeneration  uint64 `json:"expected_generation"`
		}
		if err := decodeAppV23AccessJSON(w, r, &confirmation); err != nil {
			writeAppV23AccessError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		if !confirmation.ConfirmIrreversible ||
			strings.TrimSpace(confirmation.ConfirmationPhrase) != appV23RootHandoverPhrase {
			writeAppV23AccessError(w, http.StatusBadRequest, "root_handover_confirmation_required",
				"Root handover requires both the irreversible confirmation and the exact typed phrase.")
			return
		}
		if confirmation.ExpectedGeneration != actor.Root.Generation {
			writeAppV23AccessError(w, http.StatusConflict, "stale_root_generation",
				"Root changed after this handover ceremony began. Reload CEREBRUM and begin again.")
			return
		}
		public, newKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			writeAppV23AccessError(w, http.StatusInternalServerError, "credential_generation_failed",
				"Could not generate a new Root credential.")
			return
		}
		newID := hex.EncodeToString(public)
		seed := newKey.Seed()
		bundleDir := filepath.Join(sageHome(), "bundles", newID)
		if mkdirErr := os.MkdirAll(bundleDir, 0700); mkdirErr != nil {
			writeAppV23AccessError(w, http.StatusInternalServerError, "credential_storage_failed",
				"Could not create the new Root credential directory.")
			return
		}
		if writeErr := os.WriteFile(filepath.Join(bundleDir, "agent.key"), seed, 0600); writeErr != nil { //nolint:gosec // server-controlled credential path
			writeAppV23AccessError(w, http.StatusInternalServerError, "credential_storage_failed",
				"Could not durably store the new Root credential.")
			return
		}
		bundlePath, err := generateRootRecoveryBundle(bundleDir, seed)
		if err != nil {
			writeAppV23AccessError(w, http.StatusInternalServerError, "credential_bundle_failed",
				"Could not prepare the new Root credential recovery bundle.")
			return
		}
		if h.ResolveAgentKeyFn == nil {
			writeAppV23AccessError(w, http.StatusServiceUnavailable, "root_rotation_key_unresolvable",
				"This machine cannot resolve newly stored Root credentials; no rotation was submitted.")
			return
		}
		resolved, resolvedOK := h.ResolveAgentKeyFn(newID)
		if !resolvedOK || len(resolved) != ed25519.PrivateKeySize || agentIDForKey(resolved) != newID {
			writeAppV23AccessError(w, http.StatusServiceUnavailable, "root_rotation_key_unresolvable",
				"The new Root recovery key was stored but could not be resolved locally; no rotation was submitted.")
			return
		}
		bundle, err := os.ReadFile(bundlePath) //nolint:gosec // server-created Root recovery archive
		if err != nil {
			writeAppV23AccessError(w, http.StatusInternalServerError, "credential_bundle_failed",
				"Could not prepare the one-time Root credential recovery delivery.")
			return
		}
		rotation := &tx.RootCredentialRotate{
			ExpectedGeneration: actor.Root.Generation,
			NewCredentialID:    newID,
			Scope:              actor.Root.Scope,
		}
		rotation.NewCredentialSignature = ed25519.Sign(
			newKey,
			tx.RootCredentialRotationSignBytes(actor.Root.PrincipalID, rotation),
		)
		ptx := &tx.ParsedTx{Type: tx.TxTypeRootCredentialRotate, RootCredentialRotate: rotation}
		hash, height, _, err := h.signAndBroadcastAppV23Control(ptx, actor)
		if err != nil {
			// Nothing signed or sent is not a rotation refusal; see
			// writeSignerNotSentIfHeld.
			if writeSignerNotSentIfHeld(w, err) {
				return
			}
			writeAppV23AccessError(w, http.StatusConflict, "root_rotation_rejected",
				"The Root credential rotation was not committed. The unactivated recovery bundle remains local.")
			return
		}
		committed, err := h.BadgerStore.GetAppV23Root()
		if err != nil || committed == nil ||
			committed.CredentialID != newID ||
			committed.Generation != actor.Root.Generation+1 {
			writeAppV23AccessError(w, http.StatusServiceUnavailable, "root_rotation_unconfirmed",
				"The rotation response was ambiguous; inspect committed Root state before retrying.")
			return
		}
		resolved, resolvedOK = h.ResolveAgentKeyFn(newID)
		if !resolvedOK || len(resolved) != ed25519.PrivateKeySize || agentIDForKey(resolved) != newID {
			writeAppV23AccessError(w, http.StatusServiceUnavailable, "root_rotation_key_unresolvable",
				"The new Root committed, but its local recovery key could not be resolved. The old key material was retained.")
			return
		}
		writeJSONResp(w, http.StatusOK, map[string]any{
			"ok": true, "root_principal_id": committed.PrincipalID,
			"old_credential_id": actor.Root.CredentialID,
			"new_agent_id":      newID, "new_credential_id": newID,
			"generation": committed.Generation, "tx_hash": hash, "height": height,
			"recovery_bundle":   base64.StdEncoding.EncodeToString(bundle),
			"bundle_filename":   "sage-cerebrum-root-recovery.zip",
			"redeploy_required": false,
			"message":           "Root credential rotated by tx-39. Existing Root domains and memories were not moved or copied; historical authorship is unchanged, and the new credential now controls the stable Root principal. Secure the one-time recovery download now.",
		})
	}
}

func generateRootRecoveryBundle(bundleDir string, seed []byte) (string, error) {
	if len(seed) != ed25519.SeedSize {
		return "", errors.New("invalid Root credential seed")
	}
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	keyHeader := &zip.FileHeader{Name: "sage-cerebrum-root-recovery/agent.key", Method: zip.Store}
	keyHeader.SetMode(0600)
	keyWriter, err := zw.CreateHeader(keyHeader)
	if err != nil {
		return "", err
	}
	if _, writeErr := keyWriter.Write(seed); writeErr != nil {
		return "", writeErr
	}
	readmeHeader := &zip.FileHeader{Name: "sage-cerebrum-root-recovery/README.txt", Method: zip.Deflate}
	readmeHeader.SetMode(0600)
	readme, err := zw.CreateHeader(readmeHeader)
	if err != nil {
		return "", err
	}
	if _, err := io.WriteString(readme,
		"CEREBRUM Root recovery credential.\n\n"+
			"This is not an agent bundle. Keep it offline and private. "+
			"Anyone holding agent.key can exercise this node's sovereign Root authority.\n\n"+
			"A Root handover changes only the current operational credential. "+
			"It does not move or copy domains or memories, and it does not rewrite historical authorship. "+
			"The stable Root principal keeps its existing domains while this credential exercises current authority.\n"); err != nil {
		return "", err
	}
	if err := zw.Close(); err != nil {
		return "", err
	}
	zipPath := filepath.Join(bundleDir, "sage-cerebrum-root-recovery.zip")
	if err := os.WriteFile(zipPath, buf.Bytes(), 0600); err != nil { //nolint:gosec // server-controlled credential path
		return "", err
	}
	return zipPath, nil
}

func handleRotateAgentKeyLegacy(agentStore store.AgentStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")

		// Verify agent exists before rotation
		oldAgent, err := agentStore.GetAgent(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusNotFound, "agent not found")
			return
		}
		if oldAgent.Status == "removed" {
			writeError(w, http.StatusBadRequest, "cannot rotate key for removed agent")
			return
		}

		// Rotate the key (generates new keypair, updates agent + memories atomically)
		newAgentID, seed, err := agentStore.RotateAgentKey(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "key rotation failed: "+err.Error())
			return
		}

		// Generate and save new bundle
		bundleDir := filepath.Join(sageHome(), "bundles", newAgentID)
		if mkErr := os.MkdirAll(bundleDir, 0700); mkErr != nil { //nolint:gosec // bundleDir is server-controlled path
			writeError(w, http.StatusInternalServerError, "failed to create bundle dir")
			return
		}

		// Save new agent key (seed)
		if wErr := os.WriteFile(filepath.Join(bundleDir, "agent.key"), seed, 0600); wErr != nil { //nolint:gosec // server-controlled path
			writeError(w, http.StatusInternalServerError, "failed to save agent key")
			return
		}

		// Fetch the updated agent record
		newAgent, err := agentStore.GetAgent(r.Context(), newAgentID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to fetch rotated agent")
			return
		}

		// Generate bundle ZIP
		bundlePath, err := generateBundle(bundleDir, newAgent, seed)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "bundle generation failed: "+err.Error())
			return
		}

		// Update agent with bundle path
		newAgent.BundlePath = bundlePath
		_ = agentStore.UpdateAgent(r.Context(), newAgent)

		writeJSONResp(w, http.StatusOK, map[string]any{
			"agent":        newAgent,
			"new_agent_id": newAgentID,
			"old_agent_id": id,
			"message":      "Key rotated successfully. Download the new bundle and trigger chain redeployment.",
		})
	}
}

func handleTemplates() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSONResp(w, http.StatusOK, map[string]any{"templates": defaultTemplates})
	}
}

const agentClaimTokenBytes = 32

// generateClaimToken generates a 256-bit unguessable bearer credential. Claim
// redemption is intentionally reachable without dashboard authentication, so a
// short human-entered code is not an acceptable authority for this route.
func generateClaimToken() (string, error) {
	b := make([]byte, agentClaimTokenBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func validAgentClaimToken(token string) bool {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	return err == nil &&
		len(raw) == agentClaimTokenBytes &&
		base64.RawURLEncoding.EncodeToString(raw) == token
}

const agentClaimRequestMaxBytes = 4 << 10

// RegisterAgentClaimRoute wires the one route for which the one-time claim
// token is the complete authority. It must remain outside authMiddleware so an
// encrypted node can be claimed by the remote CLI it was created for.
func (h *DashboardHandler) RegisterAgentClaimRoute(r chi.Router) {
	agentStore, agentsOK := h.store.(AgentStoreProvider)
	claimStore, claimsOK := h.store.(store.AgentClaimStore)
	if !agentsOK || !claimsOK {
		return
	}
	r.Post("/v1/dashboard/network/claim", handleClaimAgent(
		agentStore, claimStore, &redeemRateLimiter{}, h.appV23IsRootIdentity,
	))
}

// handleClaimAgent exchanges a one-time claim token for the agent's key seed
// and a deliberately credential-free subset of its metadata.
func handleClaimAgent(
	agentStore store.AgentStore,
	claimStore store.AgentClaimStore,
	rl *redeemRateLimiter,
	isRootIdentity func(string) bool,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// A successful response contains an Ed25519 seed. Explicitly forbid
		// storage by browsers and intermediaries on every claim outcome.
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Pragma", "no-cache")

		if !rl.allow(clientIP(r)) {
			w.Header().Set("Retry-After", "60")
			writeError(w, http.StatusTooManyRequests, "too many redemption attempts, try again later")
			return
		}

		var req struct {
			Token string `json:"token"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, agentClaimRequestMaxBytes)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		req.Token = strings.TrimSpace(req.Token)
		if req.Token == "" {
			writeError(w, http.StatusBadRequest, "token is required")
			return
		}
		if !validAgentClaimToken(req.Token) {
			writeError(w, http.StatusNotFound, "invalid or expired claim token")
			return
		}

		// The store performs an indexed compare-and-clear. Consumption happens
		// before any bundle I/O, so a clear downstream failure cannot leave the
		// credential reusable.
		agentID, err := claimStore.RedeemAgentClaim(r.Context(), req.Token, time.Now())
		if errors.Is(err, store.ErrAgentClaimInvalid) {
			writeError(w, http.StatusNotFound, "invalid or expired claim token")
			return
		}
		if errors.Is(err, store.ErrAgentClaimExpired) {
			writeError(w, http.StatusGone, "claim token has expired")
			return
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to redeem claim token")
			return
		}
		// The token was deliberately burned above. A claim issued before
		// app-v23 activation must never become a post-activation Root-key
		// exfiltration path.
		if isRootIdentity != nil && isRootIdentity(agentID) {
			writeError(w, http.StatusNotFound, "invalid or expired claim token")
			return
		}

		matched, err := agentStore.GetAgent(r.Context(), agentID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "agent not found after claim")
			return
		}
		keyPath := filepath.Join(sageHome(), "bundles", agentID, "agent.key")
		seed, err := os.ReadFile(keyPath)
		if err != nil || len(seed) != ed25519.SeedSize {
			writeError(w, http.StatusInternalServerError, "agent key not found on server")
			return
		}

		writeJSONResp(w, http.StatusOK, map[string]any{
			"agent_id": matched.AgentID,
			"agent": map[string]any{
				"agent_id":         matched.AgentID,
				"name":             matched.Name,
				"role":             matched.Role,
				"clearance":        matched.Clearance,
				"avatar":           matched.Avatar,
				"boot_bio":         matched.BootBio,
				"domain_access":    matched.DomainAccess,
				"validator_pubkey": matched.ValidatorPubkey,
			},
			"key_seed": hex.EncodeToString(seed),
		})
	}
}

// handleRedeployStatusLive returns the current redeployment status using the orchestrator.
func (h *DashboardHandler) handleRedeployStatusLive(w http.ResponseWriter, r *http.Request) {
	if h.Redeployer == nil {
		writeJSONResp(w, http.StatusOK, map[string]any{"active": false, "status": "idle", "message": "redeployer not configured"})
		return
	}

	status, currentPhase, operation, agentID, errMsg, err := h.Redeployer.GetLiveStatus(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSONResp(w, http.StatusOK, map[string]any{
		"active":        status == "running",
		"status":        status,
		"current_phase": currentPhase,
		"operation":     operation,
		"agent_id":      agentID,
		"error":         errMsg,
	})
}

// handleClearRedeploy clears a stuck/abandoned redeployment (e.g. one that was
// interrupted mid-flight and left a frozen in_progress log row that would
// otherwise wedge the status banner forever). Refuses if a run is genuinely live.
func (h *DashboardHandler) handleClearRedeploy(w http.ResponseWriter, r *http.Request) {
	if h.Redeployer == nil {
		writeError(w, http.StatusServiceUnavailable, "redeployer not configured")
		return
	}
	cleared, err := h.Redeployer.ClearStale(r.Context())
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"cleared": cleared, "status": "idle"})
}

// handleTriggerRedeploy starts a chain redeployment operation.
// Returns 202 Accepted immediately — the operation runs in a background goroutine.
// Poll GET /v1/dashboard/network/redeploy/status for progress.
func (h *DashboardHandler) handleTriggerRedeploy(w http.ResponseWriter, r *http.Request) {
	if h.Redeployer == nil {
		writeError(w, http.StatusServiceUnavailable, "redeployer not configured")
		return
	}

	var req struct {
		Operation string `json:"operation"` // "add_agent" or "remove_agent"
		AgentID   string `json:"agent_id"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	// Validate operation
	switch req.Operation {
	case "add_agent", "remove_agent", "rotate_key":
		// valid
	default:
		writeError(w, http.StatusBadRequest, "operation must be add_agent, remove_agent, or rotate_key")
		return
	}

	if req.AgentID == "" {
		writeError(w, http.StatusBadRequest, "agent_id is required")
		return
	}
	if h.appV23IsRootIdentity(req.AgentID) {
		writeAppV23AccessError(w, http.StatusForbidden, "root_agent_surface_forbidden",
			"CEREBRUM Root is not an agent and cannot be targeted by an agent redeployment operation.")
		return
	}

	// Check if already redeploying
	if h.Redeployer.IsRedeploying() {
		writeError(w, http.StatusConflict, "redeployment already in progress")
		return
	}

	// Single-validator chain: an agent op never changes the validator set, so the
	// destructive stop/wipe/restart is unnecessary AND has bricked personal nodes
	// before. Apply the lightweight equivalent and return — no chain
	// redeployment, no "reconfiguration in progress" banner. Prefer the live
	// validator count (a network-mode host with only non-validator peers is still
	// single-validator); fall back to the quorum flag if the count isn't wired.
	singleValidator := !h.QuorumEnabled
	if h.ValidatorCountFn != nil {
		singleValidator = h.ValidatorCountFn() <= 1
	}
	if singleValidator {
		if err := h.Redeployer.QuickAgentOp(r.Context(), req.Operation, req.AgentID); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSONResp(w, http.StatusOK, map[string]any{
			"status":  "completed",
			"message": "applied without a chain redeployment (single-node network)",
		})
		return
	}

	// Launch in the node-owned background group so a CEREBRUM restart cancels
	// and joins redeployment before consensus/stores are closed.
	run := func(ctx context.Context) {
		if err := h.Redeployer.DeployOp(ctx, req.Operation, req.AgentID); err != nil {
			// Error is logged by the orchestrator and stored in the redeploy log.
			// The client discovers failures by polling /redeploy/status.
			_ = err
		}

		// Broadcast completion via SSE if available
		if h.SSE != nil {
			h.SSE.Broadcast(SSEEvent{
				Type: EventRedeploy,
				Data: map[string]any{
					"operation": req.Operation,
					"agent_id":  req.AgentID,
					"completed": true,
				},
			})
		}
	}
	if h.RunBackground != nil {
		h.RunBackground(run)
	} else {
		// Test/embedded handlers without a lifecycle owner retain async behavior.
		go run(context.Background()) //nolint:gosec
	}

	writeJSONResp(w, http.StatusAccepted, map[string]any{
		"status":    "started",
		"operation": req.Operation,
		"agent_id":  req.AgentID,
		"message":   "Redeployment started. Poll GET /v1/dashboard/network/redeploy/status for progress.",
	})
}

// handleUnregisteredAgents discovers agents that have memories but are not registered
// in the network dashboard. These are orphaned agent identities (e.g., from per-project
// keys that were never formally registered).
func (h *DashboardHandler) handleUnregisteredAgents(agentStore store.AgentStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Get all agent IDs from memory data
		var stats *store.StoreStats
		var projection *appV23ProjectionResponse
		if audited, ok := r.Context().Value(appV23ProjectionAuditContextKey{}).(appV23ProjectionAuditSnapshot); ok {
			stats = audited.stats
			projection = audited.projection
		} else {
			var err error
			stats, _, projection, err = h.cerebrumVisibleStatsAndActivity(r.Context())
			if err != nil {
				if writeAppV23DashboardProjectionFailure(w, err) {
					return
				}
				writeError(w, http.StatusInternalServerError, "failed to get stats: "+err.Error())
				return
			}
		}

		// Get registered agents
		registered, err := agentStore.ListAgents(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to list agents: "+err.Error())
			return
		}
		knownIDs := make(map[string]bool, len(registered))
		for _, a := range registered {
			knownIDs[a.AgentID] = true
		}

		// Find agents in memory data that are not registered
		type unregisteredAgent struct {
			AgentID     string `json:"agent_id"`
			MemoryCount int    `json:"memory_count"`
			ShortID     string `json:"short_id"`
		}
		var unregistered []unregisteredAgent
		if stats != nil {
			for agentID, count := range stats.ByAgent {
				if agentID == "" {
					continue
				}
				if h.appV23IsRootIdentity(agentID) {
					continue
				}
				if !knownIDs[agentID] {
					shortID := agentID
					if len(shortID) > 16 {
						shortID = shortID[:8] + "…" + shortID[len(shortID)-8:]
					}
					unregistered = append(unregistered, unregisteredAgent{
						AgentID:     agentID,
						MemoryCount: count,
						ShortID:     shortID,
					})
				}
			}
		}

		response := map[string]any{"unregistered": unregistered}
		if projection != nil {
			response["projection"] = h.projectionResponseForRequest(r, projection)
		}
		writeJSONResp(w, http.StatusOK, response)
	}
}

// handleMergeAgent merges all memories from an unregistered (source) agent into
// a registered (target) agent. This goes through CometBFT consensus via
// TxTypeMemoryReassign — no raw SQL backdoor.
func (h *DashboardHandler) handleMergeAgent(agentStore store.AgentStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var appV23Actor *appV23ControlActor
		if h.appV23IsActive() {
			var ok bool
			appV23Actor, ok = h.requireAppV23ControlActor(w, r, true)
			if !ok {
				return
			}
		}
		var req struct {
			SourceAgentID string `json:"source_agent_id"`
			TargetAgentID string `json:"target_agent_id"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		if req.SourceAgentID == "" || req.TargetAgentID == "" {
			writeError(w, http.StatusBadRequest, "source_agent_id and target_agent_id are required")
			return
		}
		if h.appV23IsRootIdentity(req.SourceAgentID) ||
			h.appV23IsRootIdentity(req.TargetAgentID) {
			writeAppV23AccessError(w, http.StatusForbidden, "root_agent_surface_forbidden",
				"CEREBRUM Root is not an agent and cannot participate in memory reassignment.")
			return
		}
		if appV23Actor != nil {
			writeAppV23AccessError(w, http.StatusConflict, "memory_reassign_retired",
				"Historical memory authorship is immutable. Use governed domain continuity to preserve access without rewriting history.")
			return
		}

		// Verify target agent is registered
		if _, err := agentStore.GetAgent(r.Context(), req.TargetAgentID); err != nil {
			writeError(w, http.StatusBadRequest, "target agent not found in registry")
			return
		}

		// Perform the actual memory reassignment in the offchain store (SQLite)
		count, reassignErr := agentStore.ReassignMemories(r.Context(), req.SourceAgentID, req.TargetAgentID)
		if reassignErr != nil {
			writeError(w, http.StatusInternalServerError, "memory reassignment failed: "+reassignErr.Error())
			return
		}

		// Also broadcast through CometBFT consensus for on-chain audit record
		if h.CometBFTRPC != "" && h.SigningKey != nil {
			h.runBackground(func(_ context.Context) {
				reassignTx := &tx.ParsedTx{
					Type:      tx.TxTypeMemoryReassign,
					Timestamp: time.Now(),
					MemoryReassign: &tx.MemoryReassign{
						SourceAgentID: req.SourceAgentID,
						TargetAgentID: req.TargetAgentID,
					},
				}
				// Leased: this shares h.SigningKey with the dashboard's
				// commit-confirmed mutations, so an unleased allocation here
				// could be overtaken and silently dropped Code 4 — losing the
				// on-chain audit record for a reassignment that already happened
				// in SQLite.
				signCtx, cancelSign := context.WithTimeout(context.Background(), backgroundSigningBudget())
				if bErr := h.signAndBroadcastSyncContext(signCtx, reassignTx, h.SigningKey); bErr != nil {
					log.Printf("memory-reassign: on-chain audit record broadcast failed for source_sha256=%s target_sha256=%s: %v",
						memoryReassignLogFingerprint(req.SourceAgentID), memoryReassignLogFingerprint(req.TargetAgentID), bErr)
				}
				cancelSign()
			})
		}

		writeJSONResp(w, http.StatusOK, map[string]any{
			"status":         "completed",
			"message":        fmt.Sprintf("%d memories reassigned from source to target.", count),
			"memories_moved": count,
			"source":         req.SourceAgentID,
			"target":         req.TargetAgentID,
		})
	}
}

// handleAgentTags returns all tags used by a specific agent's memories.
func (h *DashboardHandler) handleAgentTags(agentStore store.AgentStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if h.appV23IsRootIdentity(id) {
			writeError(w, http.StatusNotFound, "agent not found")
			return
		}
		var tags []store.TagCount
		partial := false
		var err error
		if h.appV23IsActive() {
			tags, partial, err = h.appV23SafeTagCounts(r.Context(), id)
		} else {
			tags, err = agentStore.ListAgentTags(r.Context(), id)
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to list agent tags: "+err.Error())
			return
		}
		if tags == nil {
			tags = []store.TagCount{}
		}
		writeJSONResp(w, http.StatusOK, map[string]any{
			"agent_id": id, "tags": tags, "partial": partial,
		})
	}
}

// NOTE: handleTransferTag / handleTransferDomain were RETIRED in v11.3. They
// rewrote memory authorship (submitting_agent) off-chain and broadcast a
// semantically-wrong all-memories MemoryReassign (tx-23) for audit. The honest
// replacement is RBAC domain-ownership transfer via handleReassignDomainOwnership
// (reassign_handler.go), which moves ownership + access on-chain and leaves
// authorship immutable. tx-23 itself is retained for consensus replay + the
// orphan-merge path (handleMergeAgent).

// embedDashboardAgentProof constructs and embeds an Ed25519 agent identity proof
// into a ParsedTx using the dashboard's signing key. This is required for ABCI
// to verify the sender's identity on-chain via verifyAgentIdentity().
func embedDashboardAgentProof(ptx *tx.ParsedTx, signingKey ed25519.PrivateKey) {
	body := []byte(fmt.Sprintf("%d:%s", ptx.Type, ptx.Timestamp.Format(time.RFC3339Nano)))
	h := sha256.Sum256(body)
	ts := time.Now().Unix()
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(ts)) // #nosec G115 -- timestamp conversion safe
	message := append(h[:], tsBytes...)

	pubKey, ok := signingKey.Public().(ed25519.PublicKey)
	if !ok {
		return
	}
	ptx.AgentPubKey = pubKey
	ptx.AgentSig = ed25519.Sign(signingKey, message)
	ptx.AgentBodyHash = h[:]
	ptx.AgentTimestamp = ts
}

// sageHome returns the SAGE home directory.
func sageHome() string {
	home := os.Getenv("SAGE_HOME")
	if home != "" {
		return home
	}
	userHome, _ := os.UserHomeDir()
	return filepath.Join(userHome, ".sage")
}

// sanitizeAgentNameForPath strips any character that could be used to
// escape the bundle directory (path separators, dotted-segments, NUL).
// The result is safe to use as a single filename component inside
// bundleDir/<bundleName>.zip.
func sanitizeAgentNameForPath(name string) string {
	// Replace anything that isn't [A-Za-z0-9_-] with '_'. This is more
	// restrictive than filepath.Base but eliminates the entire class of
	// path-injection bugs at the source.
	out := make([]byte, 0, len(name))
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z',
			c >= 'A' && c <= 'Z',
			c >= '0' && c <= '9',
			c == '_', c == '-':
			out = append(out, c)
		default:
			out = append(out, '_')
		}
	}
	if len(out) == 0 {
		return "agent"
	}
	return string(out)
}

// generateBundle creates a ZIP bundle for an agent.
func generateBundle(bundleDir string, agent *store.AgentEntry, seed []byte) (string, error) {
	// Sanitise agent name before it becomes part of any filesystem path.
	// req.Name is user-controlled JSON and could otherwise contain
	// "../" or "/" to escape bundleDir.
	safeName := sanitizeAgentNameForPath(agent.Name)
	zipPath := filepath.Join(bundleDir, fmt.Sprintf("sage-agent-%s.zip", safeName))
	// Belt-and-braces: confirm the resulting path is still inside
	// bundleDir after Clean. If a future change to sanitise lets a
	// traversal sequence through, we fail closed rather than write
	// outside the sandbox.
	cleanZip := filepath.Clean(zipPath)
	cleanDir := filepath.Clean(bundleDir)
	if !strings.HasPrefix(cleanZip, cleanDir+string(os.PathSeparator)) {
		return "", fmt.Errorf("agent name produces path outside bundle dir")
	}

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)

	// agent.key — Ed25519 seed (32 bytes)
	fw, err := zw.Create(fmt.Sprintf("sage-agent-%s/agent.key", safeName))
	if err != nil {
		return "", err
	}
	if _, wErr := fw.Write(seed); wErr != nil {
		return "", wErr
	}

	// config.yaml — minimal config pointing to this node
	configYAML := fmt.Sprintf(`# SAGE Agent Configuration — %s
data_dir: ~/.sage/data
rest_addr: ":8080"

embedding:
  provider: hash
  dimension: 768

quorum:
  enabled: true
  peers: []  # Will be configured during setup
`, agent.Name)
	fw, err = zw.Create(fmt.Sprintf("sage-agent-%s/config.yaml", safeName))
	if err != nil {
		return "", err
	}
	if _, wErr := fw.Write([]byte(configYAML)); wErr != nil {
		return "", wErr
	}

	// .mcp.json — MCP config for Claude
	mcpJSON := `{
  "mcpServers": {
    "sage": {
      "command": "sage-gui",
      "args": ["mcp"],
      "env": {
        "SAGE_API_URL": "http://localhost:8080"
      }
    }
  }
}`
	fw, err = zw.Create(fmt.Sprintf("sage-agent-%s/.mcp.json", safeName))
	if err != nil {
		return "", err
	}
	if _, wErr := fw.Write([]byte(mcpJSON)); wErr != nil {
		return "", wErr
	}

	// SETUP.txt — human-readable instructions
	setupTxt := fmt.Sprintf(`SAGE Agent Setup — %s
================================

1. Copy this entire folder to the target machine
2. Install sage-gui: download from github.com/l33tdawg/sage/releases
3. Move agent.key to ~/.sage/agent.key
4. Move config.yaml to ~/.sage/config.yaml
5. Move .mcp.json to your project root
6. Start the agent: sage-gui serve

Agent ID: %s
Role: %s
Clearance: %d

This agent will connect to the primary node's network.
`, agent.Name, agent.AgentID, agent.Role, agent.Clearance)
	fw, err = zw.Create(fmt.Sprintf("sage-agent-%s/SETUP.txt", safeName))
	if err != nil {
		return "", err
	}
	if _, wErr := fw.Write([]byte(setupTxt)); wErr != nil {
		return "", wErr
	}

	if err := zw.Close(); err != nil {
		return "", err
	}

	// zipPath = bundleDir/sage-agent-<safeName>.zip where safeName is
	// the [A-Za-z0-9_-] sanitised agent name (see sanitizeAgentNameForPath
	// above) and a HasPrefix(bundleDir) check has already passed.
	if err := os.WriteFile(zipPath, buf.Bytes(), 0600); err != nil { //nolint:gosec // zipPath is bundleDir/<sanitised>.zip
		return "", err
	}
	return zipPath, nil
}

// broadcastAgentUpdate signs and broadcasts a TxTypeAgentUpdate through CometBFT.
// Returns an error if any step fails so callers can surface it to the UI.
//
// It goes through the nonce lease (signAndBroadcastSyncContext), like every other
// producer that signs with h.SigningKey: a nonce allocated outside the lease that
// serializes submission for that key can be overtaken by a concurrent dashboard
// mutation and rejected Code 4 "nonce too low".
func (h *DashboardHandler) broadcastAgentUpdate(agentID, name, bio string) error {
	updateTx := &tx.ParsedTx{
		Type:      tx.TxTypeAgentUpdate,
		Timestamp: time.Now(),
		AgentUpdateTx: &tx.AgentUpdate{
			AgentID: agentID,
			Name:    name,
			BootBio: bio,
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), backgroundSigningBudget())
	defer cancel()
	return h.signAndBroadcastSyncContext(ctx, updateTx, h.SigningKey)
}
