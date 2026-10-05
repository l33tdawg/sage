package mcp

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An indeterminate submit must never be reported to an agent as a stored
// memory. storeMemory — the shared write path behind sage_turn and sage_reflect
// — decoded only embedding_queued before this, so a 202 carrying "sent, fate
// unknown" was indistinguishable from success: the same lie the REST layer used
// to tell, one layer further up, and the one an agent is least able to notice.
func TestStoreMemoryReportsIndeterminateInsteadOfSuccess(t *testing.T) {
	var submits atomic.Int32

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/memory/pre-validate", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted": true})
	})
	mux.HandleFunc("/v1/embed/info", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"submit_embedding_authoritative": true})
	})
	mux.HandleFunc("/v1/memory/submit", func(w http.ResponseWriter, _ *http.Request) {
		submits.Add(1)
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":    "indeterminate",
			"tx_hash":   "AABBCCDD",
			"committed": false,
			"retryable": false,
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer(ts.URL, priv)

	_, storeErr := s.storeMemory(
		context.Background(),
		"An observation long enough to clear the node's quality floor",
		"crypto",
		"observation",
		0.8,
	)
	require.Error(t, storeErr, "an indeterminate outcome must not be reported as a stored memory")
	assert.Contains(t, storeErr.Error(), "indeterminate")
	assert.Contains(t, storeErr.Error(), "AABBCCDD", "the error must name the transaction that may still commit")
	assert.EqualValues(t, 1, submits.Load(), "an ambiguous POST must not be retried")
}
