package abci

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taskidempotency"
	"github.com/l33tdawg/sage/internal/tx"
)

// This is a receipt-producer component fixture. It does not claim lineage
// admission, a full signed block, or qualified app-v30 height-zero activation.
func TestTaskReceiptReplayBindsDerivedFrom(t *testing.T) {
	app, _, assignee := directAppV23GenesisTestApp(t)
	at := time.Unix(1700000000, 0).UTC()
	const key = "lineage-task"
	memoryID, err := taskidempotency.MemoryID(assignee.id, key)
	require.NoError(t, err)
	contentHash := sha256.Sum256([]byte("lineage task"))
	refs := []tx.ParentRef{{
		SourceChainID: "chain", MemoryID: "parent",
		ContentHash: sha256.Sum256([]byte("parent")),
	}}
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeMemorySubmit, Nonce: 1, Timestamp: at,
		MemorySubmit: &tx.MemorySubmit{
			MemoryID: memoryID, ContentHash: contentHash[:],
			MemoryType: tx.MemoryTypeTask, DomainTag: "voice-interface",
			Content: "lineage task", TaskStatus: "planned",
			Classification: tx.ClearanceInternal, DerivedFrom: refs,
		},
		AgentRequest: canonicalAgentRequest(t, "POST", "/v1/memory/submit", map[string]any{
			"content": "lineage task", "memory_type": "task", "domain_tag": "voice-interface",
			"task_status": "planned", "idempotency_key": key, "classification": 1,
			"derived_from": []any{map[string]any{
				"source_chain_id": refs[0].SourceChainID, "memory_id": refs[0].MemoryID,
				"content_hash": hex.EncodeToString(refs[0].ContentHash[:]),
			}},
		}),
	}
	require.NoError(t, tx.SignTx(parsed, assignee.priv))
	intent, err := app.appV23TaskIdempotencyForSubmit(parsed, assignee.id, assignee.id, 1)
	require.NoError(t, err)
	require.NotNil(t, intent)
	require.False(t, intent.isReplay)
	require.NoError(t, app.badgerStore.SetAppV23TaskIdempotencyBinding(assignee.id, key, intent.binding))
	require.NoError(t, app.badgerStore.SetMemoryHash(memoryID, contentHash[:], "committed"))
	require.NoError(t, app.badgerStore.SetMemoryDomain(memoryID, parsed.MemorySubmit.DomainTag))
	require.NoError(t, app.badgerStore.SetMemoryClassification(memoryID, uint8(parsed.MemorySubmit.Classification)))
	require.NoError(t, app.badgerStore.SetMemoryAuthorPrincipal(memoryID, assignee.id))

	replay, err := app.appV23TaskIdempotencyForSubmit(parsed, assignee.id, assignee.id, 2)
	require.NoError(t, err)
	require.NotNil(t, replay)
	require.True(t, replay.isReplay)
	require.Equal(t, intent.binding, replay.binding)

	for _, tc := range []struct {
		name   string
		mutate func(*tx.MemorySubmit)
	}{
		{"chain", func(s *tx.MemorySubmit) { s.DerivedFrom[0].SourceChainID = "other" }},
		{"memory", func(s *tx.MemorySubmit) { s.DerivedFrom[0].MemoryID = "other" }},
		{"hash", func(s *tx.MemorySubmit) { s.DerivedFrom[0].ContentHash[0] ^= 1 }},
		{"empty", func(s *tx.MemorySubmit) { s.DerivedFrom = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := *parsed
			submit := *parsed.MemorySubmit
			submit.DerivedFrom = append([]tx.ParentRef(nil), refs...)
			tc.mutate(&submit)
			changed.MemorySubmit = &submit
			require.NoError(t, tx.SignTx(&changed, assignee.priv))
			_, replayErr := app.appV23TaskIdempotencyForSubmit(&changed, assignee.id, assignee.id, 2)
			require.ErrorIs(t, replayErr, store.ErrAppV23TaskIdempotencyConflict)
			retained, readErr := app.badgerStore.GetAppV23TaskIdempotencyBinding(assignee.id, key)
			require.NoError(t, readErr)
			require.Equal(t, intent.binding, retained)
		})
	}
}
