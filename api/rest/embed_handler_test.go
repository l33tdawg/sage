package rest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/embedding"
	"github.com/l33tdawg/sage/internal/metrics"
)

// vaultActiveMockStore wraps mockMemoryStore and implements VaultActive() to
// simulate a node where the synaptic-ledger vault is unlocked and content is
// AES-256-GCM encrypted at rest. This is the only condition the v6.6.10
// /v1/embed/info change cares about — it forces semantic=true regardless of
// the underlying embedder so MCP clients don't take the broken FTS5 path.
type vaultActiveMockStore struct {
	*mockMemoryStore
	vaultActive bool
}

func (v *vaultActiveMockStore) VaultActive() bool { return v.vaultActive }

// TestHandleEmbedInfo_VaultActiveForcesSemantic confirms that /v1/embed/info
// reports semantic=true whenever the store is vault-active, even when the
// configured embedder is the non-semantic HashProvider. This is the primary
// fix in v6.6.10 — without it, MCP isSemanticMode would route to FTS5 and
// SQLiteStore.SearchByText would return the cryptic vault-encrypted error.
func TestHandleEmbedInfo_VaultActiveForcesSemantic(t *testing.T) {
	hashEmbedder := embedding.NewHashProvider(768)
	require.False(t, hashEmbedder.Semantic(), "HashProvider must be non-semantic for this test to be meaningful")

	memStore := &vaultActiveMockStore{mockMemoryStore: newMockMemoryStore(), vaultActive: true}
	scoreStore := newMockScoreStore()
	health := metrics.NewHealthChecker()
	health.SetPostgresHealth(true)
	health.SetCometBFTHealth(true)

	srv := NewServer("", memStore, scoreStore, nil, health, zerolog.Nop(), hashEmbedder)

	req, _ := signedRequest(t, http.MethodGet, "/v1/embed/info", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var resp EmbedInfoResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	assert.True(t, resp.Semantic, "vault-active store must force semantic=true even with HashProvider embedder")
	assert.Equal(t, "vault-encrypted", resp.Provider, "provider should reflect the vault-encrypted state")
}

// TestHandleEmbedInfo_VaultInactiveHonorsEmbedder confirms the existing
// behavior is preserved when the vault is NOT active — the embedder's
// Semantic() flag is the source of truth, and FTS5 fallback is allowed
// because content is plaintext-indexable.
func TestHandleEmbedInfo_VaultInactiveHonorsEmbedder(t *testing.T) {
	hashEmbedder := embedding.NewHashProvider(768)

	memStore := &vaultActiveMockStore{mockMemoryStore: newMockMemoryStore(), vaultActive: false}
	scoreStore := newMockScoreStore()
	health := metrics.NewHealthChecker()
	health.SetPostgresHealth(true)
	health.SetCometBFTHealth(true)

	srv := NewServer("", memStore, scoreStore, nil, health, zerolog.Nop(), hashEmbedder)

	req, _ := signedRequest(t, http.MethodGet, "/v1/embed/info", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var resp EmbedInfoResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	assert.False(t, resp.Semantic, "vault-inactive store must honor the embedder's actual Semantic() value")
	assert.Equal(t, "hash", resp.Provider)
}

// TestHandleEmbedInfo_NoVaultAPIPreservesLegacyBehavior confirms that stores
// which don't implement VaultActive() (e.g. PostgresStore in the current
// codebase) get the legacy behavior unchanged — semantic flag tracks the
// embedder verbatim. Important: we must not break PostgresStore's existing
// semantic reporting.
func TestHandleEmbedInfo_NoVaultAPIPreservesLegacyBehavior(t *testing.T) {
	hashEmbedder := embedding.NewHashProvider(768)

	// Plain mockMemoryStore does NOT implement VaultActive().
	memStore := newMockMemoryStore()
	scoreStore := newMockScoreStore()
	health := metrics.NewHealthChecker()
	health.SetPostgresHealth(true)
	health.SetCometBFTHealth(true)

	srv := NewServer("", memStore, scoreStore, nil, health, zerolog.Nop(), hashEmbedder)

	req, _ := signedRequest(t, http.MethodGet, "/v1/embed/info", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var resp EmbedInfoResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	// HashProvider is non-semantic AND the store doesn't expose vault status,
	// so the response should reflect the embedder unchanged: semantic=false.
	assert.False(t, resp.Semantic)
	assert.Equal(t, "hash", resp.Provider)
}
