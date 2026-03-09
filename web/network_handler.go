package web

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/store"
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
	r.Post("/v1/dashboard/network/agents", handleCreateAgent(agentStore))
	r.Patch("/v1/dashboard/network/agents/{id}", handleUpdateAgent(agentStore))
	r.Delete("/v1/dashboard/network/agents/{id}", handleRemoveAgent(agentStore, h.store))
	r.Get("/v1/dashboard/network/agents/{id}/bundle", handleDownloadBundle(agentStore))
	r.Post("/v1/dashboard/network/agents/{id}/rotate-key", handleRotateAgentKey(agentStore))
	r.Get("/v1/dashboard/network/templates", handleTemplates())
	r.Get("/v1/dashboard/network/redeploy/status", h.handleRedeployStatusLive)
	r.Post("/v1/dashboard/network/redeploy", h.handleTriggerRedeploy)

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

func handleCreateAgent(agentStore store.AgentStore) http.HandlerFunc {
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
		}

		if err := agentStore.CreateAgent(r.Context(), agent); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		// Generate and save agent bundle
		bundleDir := filepath.Join(sageHome(), "bundles", agentID)
		if mkErr := os.MkdirAll(bundleDir, 0700); mkErr != nil {
			writeError(w, http.StatusInternalServerError, "failed to create bundle dir")
			return
		}

		// Save agent key (seed)
		if err := os.WriteFile(filepath.Join(bundleDir, "agent.key"), seed, 0600); err != nil {
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

		writeJSONResp(w, http.StatusCreated, map[string]any{
			"agent":    agent,
			"agent_id": agentID,
		})
	}
}

func handleUpdateAgent(agentStore store.AgentStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")

		existing, err := agentStore.GetAgent(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusNotFound, "agent not found")
			return
		}

		var req struct {
			Name         *string `json:"name"`
			Role         *string `json:"role"`
			Avatar       *string `json:"avatar"`
			BootBio      *string `json:"boot_bio"`
			Clearance    *int    `json:"clearance"`
			OrgID        *string `json:"org_id"`
			DeptID       *string `json:"dept_id"`
			DomainAccess *string `json:"domain_access"`
			P2PAddress   *string `json:"p2p_address"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}

		if req.Name != nil {
			existing.Name = *req.Name
		}
		if req.Role != nil {
			existing.Role = *req.Role
		}
		if req.Avatar != nil {
			existing.Avatar = *req.Avatar
		}
		if req.BootBio != nil {
			existing.BootBio = *req.BootBio
		}
		if req.Clearance != nil {
			existing.Clearance = *req.Clearance
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

		if err := agentStore.UpdateAgent(r.Context(), existing); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		writeJSONResp(w, http.StatusOK, existing)
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

		data, err := os.ReadFile(agent.BundlePath)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "bundle file not found")
			return
		}

		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="sage-agent-%s.zip"`, agent.Name))
		w.Write(data) //nolint:errcheck
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
		if mkErr := os.MkdirAll(bundleDir, 0700); mkErr != nil {
			writeError(w, http.StatusInternalServerError, "failed to create bundle dir")
			return
		}

		// Save new agent key (seed)
		if wErr := os.WriteFile(filepath.Join(bundleDir, "agent.key"), seed, 0600); wErr != nil {
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

// handleRedeployStatusLive returns the current redeployment status using the orchestrator.
func (h *DashboardHandler) handleRedeployStatusLive(w http.ResponseWriter, r *http.Request) {
	if h.Redeployer == nil {
		writeJSONResp(w, http.StatusOK, map[string]any{"active": false, "error": "redeployer not configured"})
		return
	}

	active, operation, agentID, err := h.Redeployer.GetRedeployStatus(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSONResp(w, http.StatusOK, map[string]any{
		"active":    active,
		"operation": operation,
		"agent_id":  agentID,
	})
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

	// Launch in background goroutine — client polls for status
	go func() {
		ctx := context.Background()
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
	}()

	writeJSONResp(w, http.StatusAccepted, map[string]any{
		"status":    "started",
		"operation": req.Operation,
		"agent_id":  req.AgentID,
		"message":   "Redeployment started. Poll GET /v1/dashboard/network/redeploy/status for progress.",
	})
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

// generateBundle creates a ZIP bundle for an agent.
func generateBundle(bundleDir string, agent *store.AgentEntry, seed []byte) (string, error) {
	zipPath := filepath.Join(bundleDir, fmt.Sprintf("sage-agent-%s.zip", agent.Name))

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)

	// agent.key — Ed25519 seed (32 bytes)
	fw, err := zw.Create(fmt.Sprintf("sage-agent-%s/agent.key", agent.Name))
	if err != nil {
		return "", err
	}
	if _, err := fw.Write(seed); err != nil {
		return "", err
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
	fw, err = zw.Create(fmt.Sprintf("sage-agent-%s/config.yaml", agent.Name))
	if err != nil {
		return "", err
	}
	if _, err := fw.Write([]byte(configYAML)); err != nil {
		return "", err
	}

	// .mcp.json — MCP config for Claude
	mcpJSON := `{
  "mcpServers": {
    "sage": {
      "command": "sage-lite",
      "args": ["mcp"],
      "env": {
        "SAGE_API_URL": "http://localhost:8080"
      }
    }
  }
}`
	fw, err = zw.Create(fmt.Sprintf("sage-agent-%s/.mcp.json", agent.Name))
	if err != nil {
		return "", err
	}
	if _, err := fw.Write([]byte(mcpJSON)); err != nil {
		return "", err
	}

	// SETUP.txt — human-readable instructions
	setupTxt := fmt.Sprintf(`SAGE Agent Setup — %s
================================

1. Copy this entire folder to the target machine
2. Install sage-lite: download from github.com/l33tdawg/sage/releases
3. Move agent.key to ~/.sage/agent.key
4. Move config.yaml to ~/.sage/config.yaml
5. Move .mcp.json to your project root
6. Start the agent: sage-lite serve

Agent ID: %s
Role: %s
Clearance: %d

This agent will connect to the primary node's network.
`, agent.Name, agent.AgentID, agent.Role, agent.Clearance)
	fw, err = zw.Create(fmt.Sprintf("sage-agent-%s/SETUP.txt", agent.Name))
	if err != nil {
		return "", err
	}
	if _, err := fw.Write([]byte(setupTxt)); err != nil {
		return "", err
	}

	if err := zw.Close(); err != nil {
		return "", err
	}

	if err := os.WriteFile(zipPath, buf.Bytes(), 0600); err != nil {
		return "", err
	}
	return zipPath, nil
}
