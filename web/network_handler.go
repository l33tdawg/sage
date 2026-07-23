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
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

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

// RegisterNetworkRoutes registers all /v1/dashboard/network/ routes.
func (h *DashboardHandler) RegisterNetworkRoutes(r chi.Router) {
	agentStore, ok := h.store.(AgentStoreProvider)
	if !ok {
		return // Store doesn't support agent management
	}

	r.Get("/v1/dashboard/network/agents", handleListAgents(agentStore))
	r.Get("/v1/dashboard/network/agents/{id}", handleGetAgent(agentStore))
	r.Post("/v1/dashboard/network/agents", h.handleCreateAgent(agentStore))
	r.Patch("/v1/dashboard/network/agents/{id}", h.handleUpdateAgent(agentStore))
	r.Delete("/v1/dashboard/network/agents/{id}", handleRemoveAgent(agentStore, h.store))
	r.Get("/v1/dashboard/network/agents/{id}/bundle", handleDownloadBundle(agentStore))
	r.Post("/v1/dashboard/network/agents/{id}/rotate-key", handleRotateAgentKey(agentStore))
	r.Get("/v1/dashboard/network/templates", handleTemplates())
	r.Post("/v1/dashboard/network/claim", handleClaimAgent(agentStore))
	r.Get("/v1/dashboard/network/redeploy/status", h.handleRedeployStatusLive)
	r.Post("/v1/dashboard/network/redeploy", h.handleTriggerRedeploy)
	r.Post("/v1/dashboard/network/redeploy/clear", h.handleClearRedeploy)

	r.Get("/v1/dashboard/network/unregistered", h.handleUnregisteredAgents(agentStore))
	r.Post("/v1/dashboard/network/merge", h.handleMergeAgent(agentStore))
	r.Get("/v1/dashboard/network/agents/{id}/tags", h.handleAgentTags(agentStore))
	// v11.3: RBAC domain-ownership transfer (honest replacement for the retired
	// authorship-rewrite transfer-tag/transfer-domain paths). handleAgentDomains
	// lists an agent's RBAC domains; handleReassignDomainOwnership moves a
	// domain's ownership + access on-chain without rewriting authorship.
	r.Get("/v1/dashboard/network/agents/{id}/domains", h.handleAgentDomains(agentStore))
	r.Post("/v1/dashboard/network/reassign-domain-ownership", h.handleReassignDomainOwnership(agentStore))

	// Pairing code generation (authenticated — admin creates code for an agent)
	if h.Pairing != nil {
		registerPairingCreateRoute(r, agentStore, h.Pairing)
	}
}

func handleListAgents(agentStore store.AgentStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		agents, err := agentStore.ListAgents(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if agents == nil {
			agents = []*store.AgentEntry{}
		}
		writeJSONResp(w, http.StatusOK, map[string]any{"agents": agents})
	}
}

func handleGetAgent(agentStore store.AgentStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		agent, err := agentStore.GetAgent(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusNotFound, "agent not found")
			return
		}
		writeJSONResp(w, http.StatusOK, agent)
	}
}

func (h *DashboardHandler) handleCreateAgent(agentStore store.AgentStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
		if req.Clearance < 0 || req.Clearance > 4 {
			req.Clearance = 1
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
		if h.CometBFTRPC != "" && h.SigningKey != nil {
			h.runBackground(func(_ context.Context) {
				registerTx := &tx.ParsedTx{
					Type:      tx.TxTypeAgentRegister,
					Nonce:     tx.MonotonicNonce(h.SigningKey),
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
				embedDashboardAgentProof(registerTx, h.SigningKey)
				if signErr := tx.SignTx(registerTx, h.SigningKey); signErr != nil {
					log.Printf("agent-create: on-chain AgentRegister sign failed for %s: %v", agentID, signErr)
					return
				}
				encoded, encErr := tx.EncodeTx(registerTx)
				if encErr != nil {
					log.Printf("agent-create: on-chain AgentRegister encode failed for %s: %v", agentID, encErr)
					return
				}
				// AgentRegister must land before AgentSetPermission (the agent has
				// to exist on-chain first); broadcastTxSync waits for the commit.
				// This runs after the 201 response, so failures can only be logged;
				// the ABCI processor leaves on_chain_height unset on failure, which
				// the dashboard surfaces as an un-synced agent.
				if bErr := broadcastTxSync(h.CometBFTRPC, encoded); bErr != nil {
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
		if err := agentStore.UpdateAgent(r.Context(), agent); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to save claim token")
			return
		}

		writeJSONResp(w, http.StatusCreated, map[string]any{
			"agent":           agent,
			"agent_id":        agentID,
			"claim_token":     claimToken,
			"install_command": fmt.Sprintf("sage-gui mcp install --token %s", claimToken),
		})
	}
}

func (h *DashboardHandler) handleUpdateAgent(agentStore store.AgentStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")

		existing, err := agentStore.GetAgent(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusNotFound, "agent not found")
			return
		}
		// Snapshot the prior access matrix so a domain_access change can be
		// reconciled into real on-chain AccessGrant/AccessRevoke txs below.
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
			AdminOverride []adminOverrideExpectation `json:"admin_override"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		overrides := make(map[string]adminOverrideExpectation, len(req.AdminOverride))
		if len(req.AdminOverride) > 0 {
			// The genesis key is a human/operator capability. A cryptographically
			// valid agent request must never be able to turn a JSON flag into an
			// admin-signed grant. Only the local CEREBRUM/operator boundary may
			// invoke this explicit override.
			if verifiedDashboardAgentID(r.Context()) != "" ||
				(r.Header.Get("Sec-Fetch-Site") == "" && strings.TrimSpace(r.Header.Get("Origin")) == "") ||
				!isLocalRequest(r) {
				writeError(w, http.StatusForbidden, "administrator access override must be confirmed in local CEREBRUM")
				return
			}
			for _, expected := range req.AdminOverride {
				expected.Domain = strings.TrimSpace(expected.Domain)
				expected.OwnerID = strings.TrimSpace(expected.OwnerID)
				expected.OwnedDomain = strings.TrimSpace(expected.OwnedDomain)
				if expected.Domain == "" || expected.OwnerID == "" || expected.OwnedDomain == "" || expected.Level < 0 || expected.Level > 2 {
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
			existing.DomainAccess = *req.DomainAccess
		}
		if req.P2PAddress != nil {
			existing.P2PAddress = *req.P2PAddress
		}
		if req.VisibleAgents != nil {
			existing.VisibleAgents = *req.VisibleAgents
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
		if h.CometBFTRPC != "" && h.SigningKey != nil {
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
			if req.Clearance != nil || req.DomainAccess != nil || req.VisibleAgents != nil || req.OrgID != nil || req.DeptID != nil {
				clearance := uint8(existing.Clearance) // #nosec G115 -- clamped to 0-4 above
				permTx := &tx.ParsedTx{
					Type: tx.TxTypeAgentSetPermission,
					AgentSetPermission: &tx.AgentSetPermission{
						AgentID:       id,
						Clearance:     clearance,
						DomainAccess:  existing.DomainAccess,
						VisibleAgents: existing.VisibleAgents,
						OrgID:         existing.OrgID,
						DeptID:        existing.DeptID,
					},
				}
				if len(h.AdminSigningKey) != ed25519.PrivateKeySize {
					warnings = append(warnings, "on-chain permission sync failed: genesis admin key unavailable")
				} else if _, _, _, bErr := h.signAndBroadcastCommit(permTx, h.AdminSigningKey); bErr != nil {
					warnings = append(warnings, "on-chain permission sync failed: "+bErr.Error())
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
		if len(grantResults) > 0 {
			resp["grant_results"] = grantResults
		}
		writeJSONResp(w, http.StatusOK, resp)
	}
}

func handleRemoveAgent(agentStore store.AgentStore, _ store.MemoryStore) http.HandlerFunc {
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

func handleDownloadBundle(agentStore store.AgentStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")

		agent, err := agentStore.GetAgent(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusNotFound, "agent not found")
			return
		}

		if agent.BundlePath == "" {
			writeError(w, http.StatusNotFound, "no bundle available")
			return
		}

		data, err := os.ReadFile(agent.BundlePath) //nolint:gosec // BundlePath is from trusted agent store
		if err != nil {
			writeError(w, http.StatusInternalServerError, "bundle file not found")
			return
		}

		// Sanitize agent name for use in Content-Disposition header
		safeName := strings.Map(func(r rune) rune {
			if r == '"' || r == '\\' || r == '\r' || r == '\n' || r < 32 {
				return '_'
			}
			return r
		}, agent.Name)
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="sage-agent-%s.zip"`, safeName))
		w.Write(data) //nolint:errcheck,gosec // server-generated ZIP archive, not user input
	}
}

func handleRotateAgentKey(agentStore store.AgentStore) http.HandlerFunc {
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

// claimCharset is the set of unambiguous uppercase alphanumeric characters for claim tokens.
const claimCharset = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"

// generateClaimToken generates a 6-character uppercase alphanumeric claim token using crypto/rand.
func generateClaimToken() (string, error) {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	for i := range b {
		b[i] = claimCharset[int(b[i])%len(claimCharset)]
	}
	return string(b), nil
}

// handleClaimAgent exchanges a one-time claim token for the agent's key seed and info.
func handleClaimAgent(agentStore store.AgentStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Token string `json:"token"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		if req.Token == "" {
			writeError(w, http.StatusBadRequest, "token is required")
			return
		}

		// Find the agent with this claim token
		agents, err := agentStore.ListAgents(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to list agents")
			return
		}

		var matched *store.AgentEntry
		for _, a := range agents {
			if a.ClaimToken == req.Token {
				matched = a
				break
			}
		}
		if matched == nil {
			writeError(w, http.StatusNotFound, "invalid or expired claim token")
			return
		}

		// Check expiry
		if matched.ClaimExpiresAt != nil && time.Now().After(*matched.ClaimExpiresAt) {
			// Clear expired token
			matched.ClaimToken = ""
			matched.ClaimExpiresAt = nil
			_ = agentStore.UpdateAgent(r.Context(), matched)
			writeError(w, http.StatusGone, "claim token has expired")
			return
		}

		// Read the agent key seed from the bundle directory
		keyPath := filepath.Join(sageHome(), "bundles", matched.AgentID, "agent.key")
		seed, err := os.ReadFile(keyPath)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "agent key not found on server")
			return
		}

		// Clear the claim token (one-time use)
		matched.ClaimToken = ""
		matched.ClaimExpiresAt = nil
		_ = agentStore.UpdateAgent(r.Context(), matched)

		writeJSONResp(w, http.StatusOK, map[string]any{
			"agent":    matched,
			"agent_id": matched.AgentID,
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
				Type: "redeploy",
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
		stats, err := h.cerebrumVisibleStats(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to get stats: "+err.Error())
			return
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
		for agentID, count := range stats.ByAgent {
			if agentID == "" {
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

		writeJSONResp(w, http.StatusOK, map[string]any{"unregistered": unregistered})
	}
}

// handleMergeAgent merges all memories from an unregistered (source) agent into
// a registered (target) agent. This goes through CometBFT consensus via
// TxTypeMemoryReassign — no raw SQL backdoor.
func (h *DashboardHandler) handleMergeAgent(agentStore store.AgentStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
					Nonce:     tx.MonotonicNonce(h.SigningKey),
					Timestamp: time.Now(),
					MemoryReassign: &tx.MemoryReassign{
						SourceAgentID: req.SourceAgentID,
						TargetAgentID: req.TargetAgentID,
					},
				}
				embedDashboardAgentProof(reassignTx, h.SigningKey)
				if signErr := tx.SignTx(reassignTx, h.SigningKey); signErr != nil {
					return
				}
				encoded, encErr := tx.EncodeTx(reassignTx)
				if encErr != nil {
					return
				}
				_ = broadcastTxSync(h.CometBFTRPC, encoded)
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
		tags, err := agentStore.ListAgentTags(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to list agent tags: "+err.Error())
			return
		}
		if tags == nil {
			tags = []store.TagCount{}
		}
		writeJSONResp(w, http.StatusOK, map[string]any{"agent_id": id, "tags": tags})
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
func (h *DashboardHandler) broadcastAgentUpdate(agentID, name, bio string) error {
	updateTx := &tx.ParsedTx{
		Type:      tx.TxTypeAgentUpdate,
		Nonce:     tx.MonotonicNonce(h.SigningKey),
		Timestamp: time.Now(),
		AgentUpdateTx: &tx.AgentUpdate{
			AgentID: agentID,
			Name:    name,
			BootBio: bio,
		},
	}
	embedDashboardAgentProof(updateTx, h.SigningKey)
	if err := tx.SignTx(updateTx, h.SigningKey); err != nil {
		return fmt.Errorf("sign agent update: %w", err)
	}
	encoded, err := tx.EncodeTx(updateTx)
	if err != nil {
		return fmt.Errorf("encode agent update: %w", err)
	}
	return broadcastTxSync(h.CometBFTRPC, encoded)
}

// broadcastTxSync sends a transaction to CometBFT via broadcast_tx_sync RPC.
// Used by dashboard handlers to put agent operations on-chain.
func broadcastTxSync(cometRPC string, txBytes []byte) error {
	txHex := hex.EncodeToString(txBytes)
	u := fmt.Sprintf("%s/broadcast_tx_sync?tx=0x%s", cometRPC, txHex)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return fmt.Errorf("create broadcast request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("broadcast tx: %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("broadcast tx: CometBFT returned status %d", resp.StatusCode)
	}
	return nil
}
