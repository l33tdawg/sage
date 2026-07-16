//go:build integration

package store

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pgvector/pgvector-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
)

func TestPostgresProjectionReplayReceiptAndAppendOnlySinks(t *testing.T) {
	ctx := context.Background()
	s := setupTestStore(t)
	memoryID := uuid.NewString()
	marker := uuid.NewString()
	proposalID := "projection-" + marker
	height := time.Now().UnixNano()
	tipHeight := height + 100_000
	blockTime := time.Now().UTC().Truncate(time.Microsecond)
	record := &memory.MemoryRecord{
		MemoryID: memoryID, SubmittingAgent: "projection-" + marker,
		Content: "postgres replay projection", ContentHash: memory.ComputeContentHash("postgres replay projection"),
		MemoryType: memory.TypeFact, DomainTag: "research", ConfidenceScore: 0.95,
		Status: memory.StatusProposed, CreatedAt: blockTime,
	}
	require.NoError(t, s.InsertMemory(ctx, record))
	t.Cleanup(func() {
		_, _ = s.db.Exec(ctx, `DELETE FROM governance_votes WHERE proposal_id = $1`, proposalID)
		_, _ = s.db.Exec(ctx, `DELETE FROM governance_proposals WHERE proposal_id = $1`, proposalID)
		_, _ = s.db.Exec(ctx, `DELETE FROM access_logs WHERE agent_id = $1`, record.SubmittingAgent)
		_, _ = s.db.Exec(ctx, `DELETE FROM challenges WHERE memory_id = $1`, memoryID)
		_, _ = s.db.Exec(ctx, `DELETE FROM corroborations WHERE memory_id = $1`, memoryID)
		_, _ = s.db.Exec(ctx, `DELETE FROM knowledge_triples WHERE memory_id = $1`, memoryID)
		_, _ = s.db.Exec(ctx, `DELETE FROM abci_projection_batches WHERE block_height = $1`, height)
		_, _ = s.db.Exec(ctx, `DELETE FROM abci_projection_batches WHERE block_height = $1`, tipHeight)
		_, _ = s.db.Exec(ctx, `DELETE FROM memories WHERE memory_id = $1`, memoryID)
	})

	triples := []memory.KnowledgeTriple{{Subject: "SAGE", Predicate: "uses", Object: "PostgreSQL"}}
	challenge := &ChallengeEntry{
		MemoryID: memoryID, ChallengerID: "challenger-" + marker,
		Reason: "replay", Evidence: "postgres", BlockHeight: height, CreatedAt: blockTime,
	}
	corroboration := &Corroboration{
		MemoryID: memoryID, AgentID: "corroborator-" + marker,
		Evidence: "postgres", CreatedAt: blockTime,
	}
	proposal := &GovProposal{
		ProposalID: proposalID, Operation: "update_power", TargetAgentID: "validator-a",
		TargetPower: 20, ProposerID: "validator-b", Status: "voting",
		CreatedHeight: height, ExpiryHeight: height + 100, Reason: "postgres replay",
	}

	// The append-only methods are independently safe for an exact retry.
	for range 2 {
		require.NoError(t, s.InsertTriples(ctx, memoryID, triples))
		require.NoError(t, s.InsertChallenge(ctx, challenge))
		require.NoError(t, s.InsertCorroboration(ctx, corroboration))
		require.NoError(t, s.InsertGovProposal(ctx, proposal))
	}

	for query, want := range map[string]int{
		`SELECT COUNT(*) FROM knowledge_triples WHERE memory_id = $1`: 1,
		`SELECT COUNT(*) FROM challenges WHERE memory_id = $1`:        1,
		`SELECT COUNT(*) FROM corroborations WHERE memory_id = $1`:    1,
	} {
		var count int
		require.NoError(t, s.db.QueryRow(ctx, query, memoryID).Scan(&count))
		assert.Equal(t, want, count)
	}

	appHash := sha256.Sum256([]byte(marker))
	apply := func() bool {
		applied := false
		require.NoError(t, s.RunInTx(ctx, func(tx OffchainStore) error {
			var err error
			applied, err = tx.ClaimProjectionBatch(ctx, height, appHash[:])
			if err != nil || !applied {
				return err
			}
			return tx.InsertAccessLog(ctx, &AccessLogEntry{
				AgentID: record.SubmittingAgent, Domain: "research", Action: "query",
				MemoryIDs: []string{memoryID}, BlockHeight: height, CreatedAt: blockTime,
			})
		}))
		return applied
	}
	assert.True(t, apply())
	assert.False(t, apply())
	tipHash := sha256.Sum256([]byte("advanced-tip-" + marker))
	require.NoError(t, s.RunInTx(ctx, func(tx OffchainStore) error {
		fresh, err := tx.ClaimProjectionBatch(ctx, tipHeight, tipHash[:])
		assert.True(t, fresh)
		return err
	}))
	assert.False(t, apply(), "old receipt must survive a long shared-projection gap")
	var accessLogs int
	require.NoError(t, s.db.QueryRow(ctx,
		`SELECT COUNT(*) FROM access_logs WHERE agent_id = $1`, record.SubmittingAgent,
	).Scan(&accessLogs))
	assert.Equal(t, 1, accessLogs)
}

func TestPostgresSharedProjectionBareWinnerThenEnrichedReplay(t *testing.T) {
	ctx := context.Background()
	s := setupTestStore(t)
	memoryID := uuid.NewString()
	marker := uuid.NewString()
	height := time.Now().UnixNano()
	blockTime := time.Now().UTC().Truncate(time.Microsecond)
	commitTime := blockTime.Add(time.Second)
	content := "raw co-commit content supplied only by the receiving validator"
	contentHash := memory.ComputeContentHash(content)
	embeddingHash := sha256.Sum256([]byte("receiver-embedding"))
	appHash := sha256.Sum256([]byte("shared-projection-" + marker))
	agentID := "shared-projection-" + marker

	t.Cleanup(func() {
		_, _ = s.db.Exec(ctx, `DELETE FROM access_logs WHERE agent_id = $1`, agentID)
		_, _ = s.db.Exec(ctx, `DELETE FROM knowledge_triples WHERE memory_id = $1`, memoryID)
		_, _ = s.db.Exec(ctx, `DELETE FROM abci_projection_batches WHERE block_height = $1`, height)
		_, _ = s.db.Exec(ctx, `DELETE FROM memories WHERE memory_id = $1`, memoryID)
	})

	bare := &memory.MemoryRecord{
		MemoryID: memoryID, SubmittingAgent: agentID, ContentHash: contentHash,
		EmbeddingHash: embeddingHash[:],
		MemoryType:    memory.TypeTask, DomainTag: "research", ConfidenceScore: 0.9,
		Status: memory.StatusProposed, TaskStatus: memory.TaskStatusPlanned, CreatedAt: blockTime,
	}
	accessLog := &AccessLogEntry{
		AgentID: agentID, Domain: "research", Action: "query",
		MemoryIDs: []string{memoryID}, BlockHeight: height, CreatedAt: blockTime,
	}
	require.NoError(t, s.RunInTx(ctx, func(tx OffchainStore) error {
		fresh, err := tx.ClaimProjectionBatch(ctx, height, appHash[:])
		if err != nil {
			return err
		}
		if !fresh {
			return fmt.Errorf("bare validator did not win fresh projection receipt")
		}
		if err := tx.InsertMemory(ctx, bare); err != nil {
			return err
		}
		if err := tx.UpdateStatus(ctx, memoryID, memory.StatusCommitted, commitTime); err != nil {
			return err
		}
		return tx.InsertAccessLog(ctx, accessLog)
	}))

	embedding := make([]float32, 768)
	embedding[0] = 0.25
	enriched := *bare
	enriched.Content = content
	enriched.Embedding = embedding
	enriched.EmbeddingHash = embeddingHash[:]
	enriched.Provider = "receiver-llm"
	enriched.EmbeddingProvider = "receiver-embedder"
	enriched.Assignee = "receiver-owner"
	triples := []memory.KnowledgeTriple{{Subject: "SAGE", Predicate: "merges", Object: "supplementary data"}}

	mergeReplay := func(record *memory.MemoryRecord) {
		t.Helper()
		require.NoError(t, s.RunInTx(ctx, func(tx OffchainStore) error {
			fresh, err := tx.ClaimProjectionBatch(ctx, height, appHash[:])
			if err != nil {
				return err
			}
			if fresh {
				return fmt.Errorf("exact shared projection replay unexpectedly reclaimed receipt")
			}
			postgres := tx.(*PostgresStore)
			if err := postgres.MergeProjectionSupplementary(ctx, record); err != nil {
				return err
			}
			return postgres.InsertTriples(ctx, memoryID, triples)
		}))
	}
	mergeReplay(&enriched)

	// A third validator carries conflicting non-empty, off-consensus values.
	// First accepted enrichment is stable, while exact triples remain a set.
	conflicting := enriched
	conflicting.Embedding = append([]float32(nil), embedding...)
	conflicting.Embedding[0] = 0.75
	conflictingHash := sha256.Sum256([]byte("conflicting-embedding"))
	conflicting.EmbeddingHash = conflictingHash[:]
	conflicting.Provider = "later-validator"
	conflicting.EmbeddingProvider = "later-embedder"
	conflicting.Assignee = "later-owner"
	mergeReplay(&conflicting)

	var (
		gotContent, gotProvider, gotEmbeddingProvider, gotAssignee, gotStatus string
		gotEmbedding                                                          pgvector.Vector
		gotEmbeddingHash                                                      []byte
		gotCreatedAt, gotCommittedAt                                          time.Time
	)
	require.NoError(t, s.db.QueryRow(ctx, `SELECT content, embedding, embedding_hash,
		provider, embedding_provider, assignee, status, created_at, committed_at
		FROM memories WHERE memory_id = $1`, memoryID).Scan(
		&gotContent, &gotEmbedding, &gotEmbeddingHash, &gotProvider,
		&gotEmbeddingProvider, &gotAssignee, &gotStatus, &gotCreatedAt, &gotCommittedAt,
	))
	assert.Equal(t, content, gotContent)
	assert.Equal(t, embedding, gotEmbedding.Slice())
	assert.Equal(t, embeddingHash[:], gotEmbeddingHash)
	assert.Equal(t, "receiver-llm", gotProvider)
	assert.Equal(t, "receiver-embedder", gotEmbeddingProvider)
	assert.Equal(t, "receiver-owner", gotAssignee)
	assert.Equal(t, string(memory.StatusCommitted), gotStatus, "enrichment replay must not regress lifecycle state")
	assert.WithinDuration(t, blockTime, gotCreatedAt, time.Microsecond)
	assert.WithinDuration(t, commitTime, gotCommittedAt, time.Microsecond)

	var tripleCount, accessLogCount int
	require.NoError(t, s.db.QueryRow(ctx,
		`SELECT COUNT(*) FROM knowledge_triples WHERE memory_id = $1`, memoryID,
	).Scan(&tripleCount))
	require.NoError(t, s.db.QueryRow(ctx,
		`SELECT COUNT(*) FROM access_logs WHERE agent_id = $1`, agentID,
	).Scan(&accessLogCount))
	assert.Equal(t, 1, tripleCount)
	assert.Equal(t, 1, accessLogCount, "receipt replay must never duplicate history")
}
