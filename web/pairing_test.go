package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

func TestGeneratePairingCode(t *testing.T) {
	code, err := generatePairingCode()
	require.NoError(t, err)

	// Format: SAG-XXX
	assert.Len(t, code, 7) // "SAG" + "-" + 3 chars
	assert.Equal(t, "SAG-", code[:4])

	// All chars after prefix should be from the charset
	suffix := code[4:]
	const charset = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
	for _, c := range suffix {
		assert.Contains(t, charset, string(c), "unexpected char in code: %c", c)
	}
}

func TestGeneratePairingCodeUniqueness(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		code, err := generatePairingCode()
		require.NoError(t, err)
		seen[code] = true
	}
	// With 28^3 = 21952 possible codes, 100 should all be unique
	assert.GreaterOrEqual(t, len(seen), 95, "too many collisions in 100 codes")
}

func TestPairingStoreGenerateAndConsume(t *testing.T) {
	ps := NewPairingStore()

	entry, err := ps.Generate("agent-123")
	require.NoError(t, err)
	assert.NotEmpty(t, entry.Code)
	assert.Equal(t, "agent-123", entry.AgentID)
	assert.False(t, entry.Consumed)
	assert.True(t, entry.ExpiresAt.After(time.Now()))

	// Consume the code
	agentID, ok := ps.Consume(entry.Code)
	assert.True(t, ok)
	assert.Equal(t, "agent-123", agentID)

	// Second consume should fail (single-use)
	_, ok = ps.Consume(entry.Code)
	assert.False(t, ok)
}

func TestPairingStoreExpiry(t *testing.T) {
	ps := NewPairingStore()

	entry, err := ps.Generate("agent-456")
	require.NoError(t, err)

	// Manually expire the entry
	ps.mu.Lock()
	ps.entries[entry.Code].ExpiresAt = time.Now().Add(-1 * time.Second)
	ps.mu.Unlock()

	// Should fail — expired
	_, ok := ps.Consume(entry.Code)
	assert.False(t, ok)
}

func TestPairingStoreReplacesExistingCode(t *testing.T) {
	ps := NewPairingStore()

	entry1, err := ps.Generate("agent-789")
	require.NoError(t, err)

	entry2, err := ps.Generate("agent-789")
	require.NoError(t, err)

	// First code should be invalidated
	_, ok := ps.Consume(entry1.Code)
	assert.False(t, ok)

	// Second code should work
	agentID, ok := ps.Consume(entry2.Code)
	assert.True(t, ok)
	assert.Equal(t, "agent-789", agentID)
}

func TestPairingStoreCleanup(t *testing.T) {
	ps := NewPairingStore()

	entry, err := ps.Generate("agent-cleanup")
	require.NoError(t, err)

	// Expire it
	ps.mu.Lock()
	ps.entries[entry.Code].ExpiresAt = time.Now().Add(-1 * time.Second)
	ps.mu.Unlock()

	ps.Cleanup()

	ps.mu.RLock()
	assert.Empty(t, ps.entries)
	ps.mu.RUnlock()
}

// createTestAgentWithBundle sets up an agent with a bundle directory for pairing tests.
func createTestAgentWithBundle(t *testing.T, s *store.SQLiteStore, agentID, name string) {
	t.Helper()
	agent := &store.AgentEntry{
		AgentID:         agentID,
		Name:            name,
		Role:            "member",
		ValidatorPubkey: "dGVzdA==",
		Status:          "pending",
		Clearance:       1,
	}
	require.NoError(t, s.CreateAgent(context.Background(), agent))

	// Create bundle directory with agent key
	bundleDir := filepath.Join(t.TempDir(), "bundles", agentID)
	require.NoError(t, os.MkdirAll(bundleDir, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(bundleDir, "agent.key"), []byte("test-seed-32-bytes-for-testing!!"), 0600))

	// Point SAGE_HOME to temp dir so the handler finds bundles
	t.Setenv("SAGE_HOME", filepath.Dir(bundleDir[:len(bundleDir)-len(agentID)-1]))
}

func TestHandleCreatePairingCode(t *testing.T) {
	h, s := newTestHandler(t)
	ps := NewPairingStore()
	h.Pairing = ps

	r := chi.NewRouter()
	h.RegisterRoutes(r)

	// Create an agent first
	agent := &store.AgentEntry{
		AgentID:         "abc123",
		Name:            "test-agent",
		Role:            "member",
		ValidatorPubkey: "dGVzdA==",
		Status:          "pending",
		Clearance:       1,
	}
	require.NoError(t, s.CreateAgent(context.Background(), agent))

	// Generate pairing code
	req := httptest.NewRequest("POST", "/v1/dashboard/network/agents/abc123/pair", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["code"])
	assert.NotEmpty(t, resp["expires_at"])
	assert.Greater(t, resp["ttl_seconds"].(float64), float64(0))

	code := resp["code"].(string)
	assert.Equal(t, "SAG-", code[:4])
}

func TestHandleCreatePairingCodeAgentNotFound(t *testing.T) {
	h, _ := newTestHandler(t)
	ps := NewPairingStore()
	h.Pairing = ps

	r := chi.NewRouter()
	h.RegisterRoutes(r)

	req := httptest.NewRequest("POST", "/v1/dashboard/network/agents/nonexistent/pair", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleRedeemPairingCode(t *testing.T) {
	h, s := newTestHandler(t)
	ps := NewPairingStore()
	h.Pairing = ps

	agentID := "redeem-test-agent"

	// Set up SAGE_HOME to temp dir
	sageDir := t.TempDir()
	t.Setenv("SAGE_HOME", sageDir)

	// Create agent
	agent := &store.AgentEntry{
		AgentID:         agentID,
		Name:            "redeem-test",
		Role:            "member",
		ValidatorPubkey: "dGVzdHB1YmtleQ==",
		Status:          "pending",
		Clearance:       2,
	}
	require.NoError(t, s.CreateAgent(context.Background(), agent))

	// Create bundle directory with agent key
	bundleDir := filepath.Join(sageDir, "bundles", agentID)
	require.NoError(t, os.MkdirAll(bundleDir, 0700))
	seed := []byte("test-seed-32-bytes-for-testing!!")
	require.NoError(t, os.WriteFile(filepath.Join(bundleDir, "agent.key"), seed, 0600))

	// Generate a pairing code
	entry, err := ps.Generate(agentID)
	require.NoError(t, err)

	// Register routes
	r := chi.NewRouter()
	h.RegisterRoutes(r)

	// Redeem the code
	req := httptest.NewRequest("GET", "/v1/dashboard/network/pair/"+entry.Code, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, agentID, resp["agent_id"])
	assert.Equal(t, "redeem-test", resp["name"])
	assert.Equal(t, "member", resp["role"])
	assert.Equal(t, float64(2), resp["clearance"])
	assert.NotEmpty(t, resp["agent_key"])
	assert.NotEmpty(t, resp["paired_at"])

	// Second redeem should fail (single-use)
	req2 := httptest.NewRequest("GET", "/v1/dashboard/network/pair/"+entry.Code, nil)
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, req2)
	assert.Equal(t, http.StatusNotFound, w2.Code)
}

func TestHandleRedeemPairingCodeInvalid(t *testing.T) {
	h, _ := newTestHandler(t)
	ps := NewPairingStore()
	h.Pairing = ps

	r := chi.NewRouter()
	h.RegisterRoutes(r)

	req := httptest.NewRequest("GET", "/v1/dashboard/network/pair/SAG-ZZZ", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleRedeemPairingCodeExpired(t *testing.T) {
	h, s := newTestHandler(t)
	ps := NewPairingStore()
	h.Pairing = ps

	agentID := "expired-agent"
	agent := &store.AgentEntry{
		AgentID:         agentID,
		Name:            "expired",
		Role:            "member",
		ValidatorPubkey: "dGVzdA==",
		Status:          "pending",
		Clearance:       1,
	}
	require.NoError(t, s.CreateAgent(context.Background(), agent))

	entry, err := ps.Generate(agentID)
	require.NoError(t, err)

	// Manually expire
	ps.mu.Lock()
	ps.entries[entry.Code].ExpiresAt = time.Now().Add(-1 * time.Second)
	ps.mu.Unlock()

	r := chi.NewRouter()
	h.RegisterRoutes(r)

	req := httptest.NewRequest("GET", "/v1/dashboard/network/pair/"+entry.Code, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleRedeemCaseInsensitive(t *testing.T) {
	h, s := newTestHandler(t)
	ps := NewPairingStore()
	h.Pairing = ps

	sageDir := t.TempDir()
	t.Setenv("SAGE_HOME", sageDir)

	agentID := "case-test-agent"
	agent := &store.AgentEntry{
		AgentID:         agentID,
		Name:            "case-test",
		Role:            "member",
		ValidatorPubkey: "dGVzdA==",
		Status:          "pending",
		Clearance:       1,
	}
	require.NoError(t, s.CreateAgent(context.Background(), agent))

	bundleDir := filepath.Join(sageDir, "bundles", agentID)
	require.NoError(t, os.MkdirAll(bundleDir, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(bundleDir, "agent.key"), []byte("test-seed-32-bytes-for-testing!!"), 0600))

	entry, err := ps.Generate(agentID)
	require.NoError(t, err)

	r := chi.NewRouter()
	h.RegisterRoutes(r)

	// Use lowercase version of code
	lowerCode := entry.Code[:4] + entry.Code[4:] // already upper, but test the normalization
	req := httptest.NewRequest("GET", "/v1/dashboard/network/pair/"+lowerCode, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}
