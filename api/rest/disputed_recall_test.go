package rest

import (
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// challengedFact seeds an app-v17 two-phase-CHALLENGED (disputed-but-live) memory.
func challengedFact(id, content string, conf float64, created time.Time) *memory.MemoryRecord {
	r := committedFact(id, content, conf, created)
	r.Status = memory.StatusChallenged
	return r
}

// TestQueryMemory_DisputedRowFlaggedAndHaircut is the off-chain recall-surface
// contract: a challenged-but-live memory stays recallable, is marked disputed, and
// its surfaced confidence is hair-cut — while a committed sibling with identical
// fields is returned byte-for-byte as before (no flag, full confidence). It also
// pins the handler wiring that opts the store into disputed admission.
func TestQueryMemory_DisputedRowFlaggedAndHaircut(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")

	created := time.Now().Add(-3 * 24 * time.Hour)
	memStore.memories["committed"] = committedFact("committed", "shared belief content", 0.90, created)
	memStore.memories["disputed"] = challengedFact("disputed", "shared belief content", 0.90, created)

	resp := postJSON(t, srv, "/v1/memory/query", QueryMemoryRequest{
		Embedding: []float32{0.1, 0.2, 0.3}, TopK: 10,
	})
	byID := resultsByID(resp)

	require.Contains(t, byID, "disputed", "a challenged-but-live memory must stay recallable")
	require.Contains(t, byID, "committed")

	disp := byID["disputed"]
	comm := byID["committed"]

	// Flag + status.
	assert.True(t, disp.Disputed, "challenged memory must be flagged disputed")
	assert.Equal(t, string(memory.StatusChallenged), disp.Status)
	assert.False(t, comm.Disputed, "committed memory is never disputed")
	assert.Equal(t, string(memory.StatusCommitted), comm.Status)

	// Confidence: committed is unchanged; disputed is the SAME decayed value hair-cut
	// by exactly the disputed multiplier (identical fields ⇒ identical decay).
	assert.InDelta(t, comm.ConfidenceScore*disputedConfidenceHaircut, disp.ConfidenceScore, 1e-9,
		"disputed confidence must be the committed-equivalent decayed value times the haircut")
	assert.Greater(t, comm.ConfidenceScore, disp.ConfidenceScore, "the committed row must not be hair-cut")

	// initial_confidence (the stored on-chain value) is NOT hair-cut — only the
	// surfaced decayed score is.
	require.NotNil(t, disp.InitialConfidence)
	assert.InDelta(t, 0.90, *disp.InitialConfidence, 1e-9, "stored confidence is untouched by the disputed haircut")

	// Handler wiring: the store was asked to admit disputed rows.
	assert.True(t, memStore.lastQueryOpts.IncludeDisputed, "recall handler must opt the store into disputed admission")
}

// TestSearchAndHybrid_AdmitAndFlagDisputed pins the same disputed flag + store-
// admission wiring for /v1/memory/search and /v1/memory/hybrid (each carries an
// independent copy of the serialize + opts logic).
func TestSearchAndHybrid_AdmitAndFlagDisputed(t *testing.T) {
	t.Run("search", func(t *testing.T) {
		srv, memStore, _ := newTestServer(t, "")
		memStore.memories["disputed"] = challengedFact("disputed", "widget dispute note", 0.90, time.Now())
		resp := postJSON(t, srv, "/v1/memory/search", SearchMemoryRequest{Query: "widget", TopK: 10})
		byID := resultsByID(resp)
		require.Contains(t, byID, "disputed")
		assert.True(t, byID["disputed"].Disputed)
		assert.True(t, memStore.lastQueryOpts.IncludeDisputed, "/search must opt into disputed admission")
	})

	t.Run("hybrid", func(t *testing.T) {
		srv, memStore, _ := newTestServer(t, "")
		memStore.memories["disputed"] = challengedFact("disputed", "widget hybrid note", 0.90, time.Now())
		resp := postJSON(t, srv, "/v1/memory/hybrid", HybridSearchMemoryRequest{
			Query: "widget", Embedding: []float32{0.1, 0.2, 0.3}, TopK: 10,
		})
		byID := resultsByID(resp)
		require.Contains(t, byID, "disputed")
		assert.True(t, byID["disputed"].Disputed)
		assert.True(t, memStore.lastQueryOpts.IncludeDisputed, "/hybrid must opt into disputed admission")
	})
}
