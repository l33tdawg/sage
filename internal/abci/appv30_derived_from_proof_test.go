package abci

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
)

func TestDelegatedMemorySubmitBindsAppV30DerivedFrom(t *testing.T) {
	app := setupTestApp(t)
	content := "authenticated lineage child"
	contentHash := sha256.Sum256([]byte(content))
	parentHash := sha256.Sum256([]byte("authenticated lineage parent"))
	derivedFrom := tx.ParentRef{
		SourceChainID: "sage",
		MemoryID:      "parent-a",
		ContentHash:   parentHash,
	}
	request := canonicalAgentRequest(t, "POST", "/v1/memory/submit", map[string]any{
		"content":          content,
		"memory_type":      "fact",
		"domain_tag":       "general",
		"confidence_score": 0.9,
		"derived_from": []any{map[string]any{
			"source_chain_id": derivedFrom.SourceChainID,
			"memory_id":       derivedFrom.MemoryID,
			"content_hash":    hex.EncodeToString(derivedFrom.ContentHash[:]),
		}},
	})
	signed, err := parseSignedAgentRequest(request)
	require.NoError(t, err)

	actual := &tx.ParsedTx{
		Type: tx.TxTypeMemorySubmit,
		MemorySubmit: &tx.MemorySubmit{
			MemoryID:        "00000000-0000-4000-8000-000000000000",
			ContentHash:     contentHash[:],
			MemoryType:      tx.MemoryTypeFact,
			DomainTag:       "general",
			ConfidenceScore: 0.9,
			Content:         content,
			DerivedFrom:     []tx.ParentRef{derivedFrom},
		},
	}
	require.NoError(t, app.verifySignedAgentAction(
		actual, "delegated-agent", signed, false, false, false, false,
	))

	actual.MemorySubmit.DerivedFrom[0].MemoryID = "different-parent"
	require.ErrorContains(t, app.verifySignedAgentAction(
		actual, "delegated-agent", signed, false, false, false, false,
	), "transaction payload differs")
}
