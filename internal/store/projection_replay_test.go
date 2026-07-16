package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/vault"
)

func projectionRowCount(t *testing.T, s *SQLiteStore, table string) int {
	t.Helper()
	var query string
	switch table {
	case "knowledge_triples":
		query = `SELECT COUNT(*) FROM knowledge_triples`
	case "challenges":
		query = `SELECT COUNT(*) FROM challenges`
	case "corroborations":
		query = `SELECT COUNT(*) FROM corroborations`
	case "access_logs":
		query = `SELECT COUNT(*) FROM access_logs`
	case "governance_proposals":
		query = `SELECT COUNT(*) FROM governance_proposals`
	case "abci_projection_batches":
		query = `SELECT COUNT(*) FROM abci_projection_batches`
	default:
		t.Fatalf("test table %q must be explicitly allowlisted", table)
	}
	var count int
	require.NoError(t, s.conn.QueryRowContext(context.Background(), query).Scan(&count))
	return count
}

func TestAppendOnlyProjectionSinksExactReplayIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	record := testMemory("projection-memory", "agent-a", "projection content", "research")
	require.NoError(t, s.InsertMemory(ctx, record))

	blockTime := time.Unix(42_000, 123_000_000).UTC()
	triples := []memory.KnowledgeTriple{
		{Subject: "SAGE", Predicate: "supports", Object: "federation"},
		{Subject: "federation", Predicate: "requires", Object: "replay safety"},
	}
	challenge := &ChallengeEntry{
		MemoryID: record.MemoryID, ChallengerID: "challenger-a", Reason: "verify",
		Evidence: "evidence-a", BlockHeight: 42, CreatedAt: blockTime,
	}
	corroboration := &Corroboration{
		MemoryID: record.MemoryID, AgentID: "agent-b", Evidence: "confirmed", CreatedAt: blockTime,
	}
	proposal := &GovProposal{
		ProposalID: "proposal-replay", Operation: "update_power", TargetAgentID: "validator-a",
		TargetPower: 20, ProposerID: "validator-b", Status: "voting",
		CreatedHeight: 42, ExpiryHeight: 142, Reason: "exact replay",
	}

	for range 2 {
		require.NoError(t, s.RunInTx(ctx, func(tx OffchainStore) error {
			if err := tx.InsertTriples(ctx, record.MemoryID, triples); err != nil {
				return err
			}
			if err := tx.InsertChallenge(ctx, challenge); err != nil {
				return err
			}
			if err := tx.InsertCorroboration(ctx, corroboration); err != nil {
				return err
			}
			return tx.InsertGovProposal(ctx, proposal)
		}))
	}

	assert.Equal(t, 2, projectionRowCount(t, s, "knowledge_triples"))
	assert.Equal(t, 1, projectionRowCount(t, s, "challenges"))
	assert.Equal(t, 1, projectionRowCount(t, s, "corroborations"))
	assert.Equal(t, 1, projectionRowCount(t, s, "governance_proposals"))

	// Natural-looking repetitions in a later block remain distinct history.
	later := blockTime.Add(time.Second)
	challengeLater := *challenge
	challengeLater.BlockHeight = 43
	challengeLater.CreatedAt = later
	corroborationLater := *corroboration
	corroborationLater.Evidence = "confirmed again with new evidence"
	corroborationLater.CreatedAt = later
	require.NoError(t, s.InsertChallenge(ctx, &challengeLater))
	require.NoError(t, s.InsertCorroboration(ctx, &corroborationLater))
	assert.Equal(t, 2, projectionRowCount(t, s, "challenges"))
	assert.Equal(t, 2, projectionRowCount(t, s, "corroborations"))
}

func TestProjectionBatchReceiptSkipsWholeReplayAndFailsClosedOnFork(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	hashA := sha256.Sum256([]byte("block-a"))
	hashB := sha256.Sum256([]byte("block-b"))
	entry := &AccessLogEntry{
		AgentID: "agent-a", Domain: "research", Action: "query",
		MemoryIDs: []string{"m1", "m2"}, BlockHeight: 77,
		CreatedAt: time.Unix(77_000, 0).UTC(),
	}

	apply := func(appHash []byte) (bool, error) {
		applied := false
		err := s.RunInTx(ctx, func(tx OffchainStore) error {
			var claimErr error
			applied, claimErr = tx.ClaimProjectionBatch(ctx, 77, appHash)
			if claimErr != nil || !applied {
				return claimErr
			}
			return tx.InsertAccessLog(ctx, entry)
		})
		return applied, err
	}

	applied, err := apply(hashA[:])
	require.NoError(t, err)
	assert.True(t, applied)
	applied, err = apply(hashA[:])
	require.NoError(t, err)
	assert.False(t, applied)
	assert.Equal(t, 1, projectionRowCount(t, s, "access_logs"), "the receipt protects sinks whose payload may legitimately repeat")
	assert.Equal(t, 1, projectionRowCount(t, s, "abci_projection_batches"))

	_, err = apply(hashB[:])
	require.ErrorContains(t, err, "already belongs to AppHash")
	assert.Equal(t, 1, projectionRowCount(t, s, "access_logs"))

	_, err = s.ClaimProjectionBatch(ctx, 78, hashA[:])
	require.ErrorContains(t, err, "inside RunInTx")

	rollback := errors.New("force projection rollback")
	err = s.RunInTx(ctx, func(tx OffchainStore) error {
		fresh, claimErr := tx.ClaimProjectionBatch(ctx, 78, hashB[:])
		require.NoError(t, claimErr)
		require.True(t, fresh)
		return rollback
	})
	require.ErrorIs(t, err, rollback)
	err = s.RunInTx(ctx, func(tx OffchainStore) error {
		fresh, claimErr := tx.ClaimProjectionBatch(ctx, 78, hashB[:])
		require.NoError(t, claimErr)
		require.True(t, fresh, "a rolled-back receipt must not suppress the retry")
		return nil
	})
	require.NoError(t, err)
}

func TestProjectionBatchReceiptSurvivesLongPartition(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	oldHash := sha256.Sum256([]byte("partitioned-validator-old-block"))
	tipHash := sha256.Sum256([]byte("advanced-shared-projection-tip"))

	claim := func(height int64, appHash []byte) bool {
		t.Helper()
		fresh := false
		require.NoError(t, s.RunInTx(ctx, func(tx OffchainStore) error {
			var err error
			fresh, err = tx.ClaimProjectionBatch(ctx, height, appHash)
			return err
		}))
		return fresh
	}

	assert.True(t, claim(1, oldHash[:]))
	assert.True(t, claim(100_000, tipHash[:]), "another validator may advance the shared projection far ahead")
	assert.False(t, claim(1, oldHash[:]), "a validator returning from a long partition must still skip its old replay")
	assert.Equal(t, 2, projectionRowCount(t, s, "abci_projection_batches"))
}

func TestProjectionSchemaMigrationPreservesLegacyDuplicates(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "legacy-duplicates.db")
	s, err := NewSQLiteStore(ctx, dbPath)
	require.NoError(t, err)
	record := testMemory("legacy-duplicate-memory", "agent-a", "legacy projection", "research")
	require.NoError(t, s.InsertMemory(ctx, record))
	createdAt := formatTime(time.Unix(55_000, 0).UTC())

	// Model a pre-receipt database that already contains legitimate historical
	// duplicates. Startup must not add a destructive UNIQUE index or rewrite
	// those rows merely to make future exact retries idempotent.
	for range 2 {
		_, err = s.conn.ExecContext(ctx,
			`INSERT INTO knowledge_triples (memory_id, subject, predicate, object) VALUES (?, ?, ?, ?)`,
			record.MemoryID, "SAGE", "preserves", "history")
		require.NoError(t, err)
		_, err = s.conn.ExecContext(ctx,
			`INSERT INTO challenges (memory_id, challenger_id, reason, evidence, block_height, created_at)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			record.MemoryID, "challenger-a", "legacy", "same evidence", 55, createdAt)
		require.NoError(t, err)
		_, err = s.conn.ExecContext(ctx,
			`INSERT INTO corroborations (memory_id, agent_id, evidence, created_at) VALUES (?, ?, ?, ?)`,
			record.MemoryID, "agent-b", "same evidence", createdAt)
		require.NoError(t, err)
	}
	_, err = s.conn.ExecContext(ctx, `DROP TABLE abci_projection_batches`)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	s, err = NewSQLiteStore(ctx, dbPath)
	require.NoError(t, err, "receipt-table migration must tolerate existing duplicate sink rows")
	t.Cleanup(func() { _ = s.Close() })
	assert.Equal(t, 2, projectionRowCount(t, s, "knowledge_triples"))
	assert.Equal(t, 2, projectionRowCount(t, s, "challenges"))
	assert.Equal(t, 2, projectionRowCount(t, s, "corroborations"))
	assert.Equal(t, 0, projectionRowCount(t, s, "abci_projection_batches"))

	// Defense-in-depth retries do not amplify the old duplicates, but neither do
	// they collapse the preserved rows.
	require.NoError(t, s.InsertTriples(ctx, record.MemoryID, []memory.KnowledgeTriple{{Subject: "SAGE", Predicate: "preserves", Object: "history"}}))
	require.NoError(t, s.InsertChallenge(ctx, &ChallengeEntry{
		MemoryID: record.MemoryID, ChallengerID: "challenger-a", Reason: "legacy",
		Evidence: "same evidence", BlockHeight: 55, CreatedAt: time.Unix(55_000, 0).UTC(),
	}))
	require.NoError(t, s.InsertCorroboration(ctx, &Corroboration{
		MemoryID: record.MemoryID, AgentID: "agent-b", Evidence: "same evidence",
		CreatedAt: time.Unix(55_000, 0).UTC(),
	}))
	assert.Equal(t, 2, projectionRowCount(t, s, "knowledge_triples"))
	assert.Equal(t, 2, projectionRowCount(t, s, "challenges"))
	assert.Equal(t, 2, projectionRowCount(t, s, "corroborations"))
}

func TestSQLiteRunInTxPreservesVaultForProjectionWrites(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	keyPath := filepath.Join(t.TempDir(), "projection-vault.key")
	require.NoError(t, vault.Init(keyPath, "projection-passphrase"))
	v, err := vault.Open(keyPath, "projection-passphrase")
	require.NoError(t, err)
	s.SetVault(v)
	s.SetVaultExpected(true)

	record := testMemory("encrypted-projection", "agent-a", "never store me in plaintext", "research")
	require.NoError(t, s.RunInTx(ctx, func(tx OffchainStore) error {
		return tx.InsertMemory(ctx, record)
	}))

	var raw string
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT content FROM memories WHERE memory_id = ?`, record.MemoryID,
	).Scan(&raw))
	assert.NotEqual(t, record.Content, raw)
	assert.Contains(t, raw, encPrefix)
	got, err := s.GetMemory(ctx, record.MemoryID)
	require.NoError(t, err)
	assert.Equal(t, record.Content, got.Content)
}
