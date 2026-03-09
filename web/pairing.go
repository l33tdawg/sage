package web

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/store"
)

const (
	pairingCodeTTL    = 15 * time.Minute
	pairingCodePrefix = "SAG"
	pairingCodeLen    = 3 // chars after prefix+dash
)

// pairingEntry holds a pairing code and its metadata.
type pairingEntry struct {
	Code      string    `json:"code"`
	AgentID   string    `json:"agent_id"`
	ExpiresAt time.Time `json:"expires_at"`
	Consumed  bool      `json:"consumed"`
}

// PairingStore manages ephemeral pairing codes for LAN agent setup.
type PairingStore struct {
	mu      sync.RWMutex
	entries map[string]*pairingEntry // key: code
}

// NewPairingStore creates a new in-memory pairing store.
func NewPairingStore() *PairingStore {
	return &PairingStore{
		entries: make(map[string]*pairingEntry),
	}
}

// Generate creates a new pairing code for the given agent ID.
func (ps *PairingStore) Generate(agentID string) (*pairingEntry, error) {
	code, err := generatePairingCode()
	if err != nil {
		return nil, fmt.Errorf("generate pairing code: %w", err)
	}

	entry := &pairingEntry{
		Code:      code,
		AgentID:   agentID,
		ExpiresAt: time.Now().Add(pairingCodeTTL),
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	// Invalidate any existing code for this agent
	for k, e := range ps.entries {
		if e.AgentID == agentID {
			delete(ps.entries, k)
		}
	}

	ps.entries[code] = entry
	return entry, nil
}

// Consume looks up a code, validates it, marks it consumed, and returns the agent ID.
// Returns empty string if invalid, expired, or already consumed.
func (ps *PairingStore) Consume(code string) (string, bool) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	entry, ok := ps.entries[code]
	if !ok {
		return "", false
	}
	if entry.Consumed || time.Now().After(entry.ExpiresAt) {
		delete(ps.entries, code)
		return "", false
	}

	entry.Consumed = true
	agentID := entry.AgentID
	// Remove it immediately — single use
	delete(ps.entries, code)
	return agentID, true
}

// Cleanup removes expired entries. Called lazily on Generate.
func (ps *PairingStore) Cleanup() {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	now := time.Now()
	for k, e := range ps.entries {
		if now.After(e.ExpiresAt) || e.Consumed {
			delete(ps.entries, k)
		}
	}
}

// generatePairingCode creates a code like "SAG-X7K" using crypto/rand.
func generatePairingCode() (string, error) {
	const charset = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789" // no 0/O/1/I for readability
	b := make([]byte, pairingCodeLen)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	var sb strings.Builder
	sb.WriteString(pairingCodePrefix)
	sb.WriteByte('-')
	for _, v := range b {
		sb.WriteByte(charset[int(v)%len(charset)])
	}
	return sb.String(), nil
}

// handleCreatePairingCode generates a pairing code for an existing agent.
// POST /v1/dashboard/network/agents/{id}/pair
func handleCreatePairingCode(agentStore store.AgentStore, ps *PairingStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")

		// Verify agent exists
		_, err := agentStore.GetAgent(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusNotFound, "agent not found")
			return
		}

		// Clean up expired codes lazily
		ps.Cleanup()

		entry, err := ps.Generate(id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to generate pairing code")
			return
		}

		writeJSONResp(w, http.StatusCreated, map[string]any{
			"code":       entry.Code,
			"expires_at": entry.ExpiresAt.Format(time.RFC3339),
			"ttl_seconds": int(time.Until(entry.ExpiresAt).Seconds()),
		})
	}
}

// handleRedeemPairingCode redeems a pairing code and returns the agent bundle.
// GET /v1/dashboard/network/pair/{code}
// UNAUTHENTICATED — the code IS the authentication.
func handleRedeemPairingCode(agentStore store.AgentStore, ps *PairingStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		code := chi.URLParam(r, "code")
		code = strings.ToUpper(strings.TrimSpace(code))

		agentID, ok := ps.Consume(code)
		if !ok {
			writeError(w, http.StatusNotFound, "invalid, expired, or already used pairing code")
			return
		}

		// Fetch agent
		agent, err := agentStore.GetAgent(r.Context(), agentID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "agent not found after pairing")
			return
		}

		// Build the bundle response as JSON (not ZIP — easier for CLI consumption)
		bundleDir := filepath.Join(sageHome(), "bundles", agentID)

		// Read the agent key (seed)
		seed, err := os.ReadFile(filepath.Join(bundleDir, "agent.key"))
		if err != nil {
			writeError(w, http.StatusInternalServerError, "agent key not found")
			return
		}

		writeJSONResp(w, http.StatusOK, map[string]any{
			"agent_id":         agent.AgentID,
			"name":             agent.Name,
			"role":             agent.Role,
			"clearance":        agent.Clearance,
			"avatar":           agent.Avatar,
			"boot_bio":         agent.BootBio,
			"domain_access":    agent.DomainAccess,
			"validator_pubkey": agent.ValidatorPubkey,
			"agent_key":        base64.StdEncoding.EncodeToString(seed),
			"paired_at":        time.Now().UTC().Format(time.RFC3339),
		})
	}
}

// RegisterPairingRoutes registers the unauthenticated pairing redemption route.
// This MUST be called OUTSIDE the auth middleware group.
func (h *DashboardHandler) RegisterPairingRoutes(r chi.Router) {
	if h.Pairing == nil {
		return
	}
	agentStore, ok := h.store.(AgentStoreProvider)
	if !ok {
		return
	}
	r.Get("/v1/dashboard/network/pair/{code}", handleRedeemPairingCode(agentStore, h.Pairing))
}

// registerPairingCreateRoute registers the authenticated pairing code creation route.
// Called from RegisterNetworkRoutes (inside the auth middleware group).
func registerPairingCreateRoute(r chi.Router, agentStore store.AgentStore, ps *PairingStore) {
	r.Post("/v1/dashboard/network/agents/{id}/pair", handleCreatePairingCode(agentStore, ps))
}

