package abci

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
)

func signedLineageReplayTx(t *testing.T, agent, outer agentKey, request []byte, at time.Time) *tx.ParsedTx {
	t.Helper()
	requestHash := sha256.Sum256(request)
	var timestamp [8]byte
	binary.BigEndian.PutUint64(timestamp[:], uint64(at.Unix())) // #nosec G115 -- fixed positive test timestamp
	message := append(append([]byte(nil), requestHash[:]...), timestamp[:]...)
	contentHash := sha256.Sum256([]byte("historical lineage field"))
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeMemorySubmit, Nonce: 1, Timestamp: at,
		MemorySubmit: &tx.MemorySubmit{
			MemoryID:    "00000000-0000-4000-8000-000000000000",
			ContentHash: contentHash[:], MemoryType: tx.MemoryTypeFact,
			DomainTag: "general", ConfidenceScore: 0.9,
			Content: "historical lineage field",
		},
		AgentPubKey: agent.pub, AgentSig: ed25519.Sign(agent.priv, message),
		AgentTimestamp: at.Unix(), AgentBodyHash: requestHash[:],
		AgentRequest: append([]byte(nil), request...),
	}
	require.NoError(t, tx.SignTx(parsed, outer.priv))
	return parsed
}

func TestDelegatedDerivedFromHistoricalUnknownJSONRemainsIgnored(t *testing.T) {
	at := time.Unix(1700000000, 0).UTC()
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"object", map[string]any{"unexpected": true}},
		{"string", "previously ignored"},
		{"number", 7},
		{"null", nil},
		{"empty-array", []any{}},
		{"malformed-parent", []any{map[string]any{"content_hash": "not-hex"}}},
		{"valid-parent", []any{map[string]any{
			"source_chain_id": "chain", "memory_id": "parent",
			"content_hash": hex.EncodeToString(make([]byte, sha256.Size)),
		}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := setupTestApp(t)
			app.appV30AppliedHeight = 50
			agent, outer := newAgentKey(t), newAgentKey(t)
			request := canonicalAgentRequest(t, "POST", "/v1/memory/submit", map[string]any{
				"content": "historical lineage field", "memory_type": "fact",
				"domain_tag": "general", "confidence_score": 0.9,
				"derived_from": tc.value,
			})
			parsed := signedLineageReplayTx(t, agent, outer, request, at)
			require.False(t, app.postAppV30Fork(50))
			require.NoError(t, app.enforceDelegatedAgentProofForFork(
				parsed, at, false, false, false, false, false, app.postAppV30Fork(50),
			), "a previously unknown JSON value must not gain a type check below the fork")
			require.True(t, app.postAppV30Fork(51))
			require.Error(t, app.enforceDelegatedAgentProofForFork(
				parsed, at, false, false, false, false, false, app.postAppV30Fork(51),
			), "post-fork the field must either be well-formed and bound or rejected")
		})
	}
}

func TestDelegatedDerivedFromPostForkCanonicalBindingRejectsTampering(t *testing.T) {
	app := setupTestApp(t)
	at := time.Unix(1700000000, 0).UTC()
	agent, outer := newAgentKey(t), newAgentKey(t)
	refs := []tx.ParentRef{
		{SourceChainID: "chain", MemoryID: "parent-a", ContentHash: sha256.Sum256([]byte("a"))},
		{SourceChainID: "chain", MemoryID: "parent-b", ContentHash: sha256.Sum256([]byte("b"))},
	}
	request := canonicalAgentRequest(t, "POST", "/v1/memory/submit", map[string]any{
		"content": "historical lineage field", "memory_type": "fact",
		"domain_tag": "general", "confidence_score": 0.9,
		"derived_from": []any{
			map[string]any{"source_chain_id": refs[1].SourceChainID, "memory_id": refs[1].MemoryID, "content_hash": hex.EncodeToString(refs[1].ContentHash[:])},
			map[string]any{"source_chain_id": refs[0].SourceChainID, "memory_id": refs[0].MemoryID, "content_hash": hex.EncodeToString(refs[0].ContentHash[:])},
		},
	})
	parsed := signedLineageReplayTx(t, agent, outer, request, at)
	parsed.MemorySubmit.DerivedFrom = append([]tx.ParentRef(nil), refs...)
	require.NoError(t, tx.SignTx(parsed, outer.priv))
	require.NoError(t, app.enforceDelegatedAgentProofForFork(
		parsed, at, false, false, false, false, false, true,
	), "request order is canonicalized with the existing transaction codec")

	// Absence in the signed request cannot authorize a lineage-bearing wire
	// payload either; omission is checked in both directions.
	withoutLineageRequest := canonicalAgentRequest(t, "POST", "/v1/memory/submit", map[string]any{
		"content": "historical lineage field", "memory_type": "fact",
		"domain_tag": "general", "confidence_score": 0.9,
	})
	withoutLineage := signedLineageReplayTx(t, agent, outer, withoutLineageRequest, at)
	require.NoError(t, app.enforceDelegatedAgentProofForFork(
		withoutLineage, at, false, false, false, false, false, true,
	), "a post-fork submission without lineage remains valid")
	withoutLineage.MemorySubmit.DerivedFrom = append([]tx.ParentRef(nil), refs...)
	require.NoError(t, tx.SignTx(withoutLineage, outer.priv))
	require.ErrorContains(t, app.enforceDelegatedAgentProofForFork(
		withoutLineage, at, false, false, false, false, false, true,
	), "transaction payload differs")

	for _, tc := range []struct {
		name   string
		mutate func(*tx.MemorySubmit)
	}{
		{"chain", func(s *tx.MemorySubmit) { s.DerivedFrom[0].SourceChainID = "other" }},
		{"memory", func(s *tx.MemorySubmit) { s.DerivedFrom[0].MemoryID = "other" }},
		{"hash", func(s *tx.MemorySubmit) { s.DerivedFrom[0].ContentHash[0] ^= 1 }},
		{"omitted", func(s *tx.MemorySubmit) { s.DerivedFrom = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := *parsed
			submit := *parsed.MemorySubmit
			submit.DerivedFrom = append([]tx.ParentRef(nil), refs...)
			tc.mutate(&submit)
			tx.SortParentRefs(submit.DerivedFrom)
			changed.MemorySubmit = &submit
			require.NoError(t, tx.SignTx(&changed, outer.priv))
			require.ErrorContains(t, app.enforceDelegatedAgentProofForFork(
				&changed, at, false, false, false, false, false, true,
			), "transaction payload differs")
		})
	}
}
