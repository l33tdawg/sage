//go:build integration

package store

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
)

// These tests exercise the PostgresStore AgentStore implementation against a
// live PostgreSQL (pgvector) instance with the deploy/init.sql schema applied.
// Run with: go test -tags=integration ./internal/store/...
//
// DSN is taken from SAGE_TEST_POSTGRES_DSN, defaulting to the local dev DB.

func agentTestDSN() string {
	if v := os.Getenv("SAGE_TEST_POSTGRES_DSN"); v != "" {
		return v
	}
	return "postgres://sage:sage_dev_password@localhost:5432/sage?sslmode=disable"
}

func agentTestStore(t *testing.T) *PostgresStore {
	t.Helper()
	ctx := context.Background()
	s, err := NewPostgresStore(ctx, agentTestDSN())
	require.NoError(t, err, "connect to test postgres (set SAGE_TEST_POSTGRES_DSN)")
	t.Cleanup(func() { s.Close() })
	return s
}

func newAgentID() string { return "agtest-" + uuid.NewString() }

// seedAgent inserts an agent with a unique ID and registers cleanup of the
// agent row plus any memories attributed to it.
func seedAgent(t *testing.T, s *PostgresStore, mutate func(*AgentEntry)) *AgentEntry {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	id := newAgentID()
	a := &AgentEntry{
		AgentID:        id,
		Name:           "name-" + id,
		RegisteredName: "reg-" + id,
		Role:           "member",
		Avatar:         "av",
		BootBio:        "bio",
		Status:         "active",
		Clearance:      3,
		OrgID:          "org-x",
		DeptID:         "dept-y",
		DomainAccess:   "crypto,vuln_intel",
		VisibleAgents:  "*",
		Provider:       "claude-code",
		OnChainHeight:  42,
		CreatedAt:      now,
		FirstSeen:      &now,
	}
	if mutate != nil {
		mutate(a)
	}
	require.NoError(t, s.CreateAgent(ctx, a))
	t.Cleanup(func() {
		_, _ = s.db.Exec(ctx, `DELETE FROM memories WHERE submitting_agent = $1`, a.AgentID)
		_, _ = s.db.Exec(ctx, `DELETE FROM agents WHERE agent_id = $1`, a.AgentID)
	})
	return a
}

func seedMemory(t *testing.T, s *PostgresStore, agentID, domain string) {
	t.Helper()
	content := "content-" + uuid.NewString()
	rec := &memory.MemoryRecord{
		MemoryID:        uuid.NewString(),
		SubmittingAgent: agentID,
		Content:         content,
		ContentHash:     memory.ComputeContentHash(content),
		MemoryType:      memory.TypeFact,
		DomainTag:       domain,
		ConfidenceScore: 0.9,
		Status:          memory.StatusCommitted,
		CreatedAt:       time.Now(),
	}
	require.NoError(t, s.InsertMemory(context.Background(), rec))
}

func TestPostgresAgentCreateGetRoundTrip(t *testing.T) {
	s := agentTestStore(t)
	ctx := context.Background()
	a := seedAgent(t, s, nil)

	got, err := s.GetAgent(ctx, a.AgentID)
	require.NoError(t, err)
	assert.Equal(t, a.AgentID, got.AgentID)
	assert.Equal(t, a.Name, got.Name)
	assert.Equal(t, a.RegisteredName, got.RegisteredName)
	assert.Equal(t, a.Role, got.Role)
	assert.Equal(t, a.Avatar, got.Avatar)
	assert.Equal(t, a.BootBio, got.BootBio)
	assert.Equal(t, "active", got.Status)
	assert.Equal(t, 3, got.Clearance)
	assert.Equal(t, "org-x", got.OrgID)
	assert.Equal(t, "dept-y", got.DeptID)
	assert.Equal(t, "crypto,vuln_intel", got.DomainAccess)
	assert.Equal(t, "*", got.VisibleAgents)
	assert.Equal(t, "claude-code", got.Provider)
	assert.Equal(t, int64(42), got.OnChainHeight)
	assert.Equal(t, 0, got.MemoryCount)
	require.NotNil(t, got.FirstSeen)
	assert.True(t, a.CreatedAt.Equal(got.CreatedAt), "created_at instant preserved")
}

func TestPostgresGetAgentByName(t *testing.T) {
	s := agentTestStore(t)
	ctx := context.Background()
	a := seedAgent(t, s, nil)

	got, err := s.GetAgentByName(ctx, a.Name)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, a.AgentID, got.AgentID)

	// Not found must return (nil, nil) per the interface contract.
	missing, err := s.GetAgentByName(ctx, "no-such-agent-"+uuid.NewString())
	require.NoError(t, err)
	assert.Nil(t, missing)
}

// TestPostgresAgentRegisterIdempotency mirrors flushPendingWrites' agent_register
// path: CreateAgent first, and on PK conflict fall back to UpdateAgent.
func TestPostgresAgentRegisterIdempotency(t *testing.T) {
	s := agentTestStore(t)
	ctx := context.Background()
	a := seedAgent(t, s, nil)

	// Re-creating the same agent must fail (PK conflict) so the flush path
	// knows to fall back to UpdateAgent.
	err := s.CreateAgent(ctx, a)
	require.Error(t, err, "duplicate CreateAgent must conflict, not silently succeed")

	a.Clearance = 4
	a.DomainAccess = "crypto,vuln_intel,infrastructure"
	require.NoError(t, s.UpdateAgent(ctx, a))

	got, err := s.GetAgent(ctx, a.AgentID)
	require.NoError(t, err)
	assert.Equal(t, 4, got.Clearance)
	assert.Equal(t, "crypto,vuln_intel,infrastructure", got.DomainAccess)
}

// TestPostgresAgentRegisterIdempotencyInTx is the regression test for the
// transaction-abort panic: the abci offchain flush runs all writes inside ONE
// pgx transaction, so a re-registration must NOT poison it. CreateAgent on an
// existing agent has to report the conflict (RowsAffected==0 → error) WITHOUT
// aborting the tx, so the UpdateAgent fallback in the same tx still commits.
// With a bare INSERT this fails with "current transaction is aborted".
func TestPostgresAgentRegisterIdempotencyInTx(t *testing.T) {
	s := agentTestStore(t)
	ctx := context.Background()
	a := seedAgent(t, s, nil)

	err := s.RunInTx(ctx, func(tx OffchainStore) error {
		ps := tx.(*PostgresStore)
		createErr := ps.CreateAgent(ctx, a) // agent already exists
		require.Error(t, createErr, "CreateAgent on existing agent must report conflict")
		// Same transaction must still be usable — this is the regression guard.
		a.Clearance = 4
		return ps.UpdateAgent(ctx, a)
	})
	require.NoError(t, err, "tx must not be poisoned by the create conflict")

	got, err := s.GetAgent(ctx, a.AgentID)
	require.NoError(t, err)
	assert.Equal(t, 4, got.Clearance)
}

// TestPostgresUpdateAgentPreservesImmutableFields confirms UpdateAgent only
// touches the SQLite-mirrored mutable column subset.
func TestPostgresUpdateAgentPreservesImmutableFields(t *testing.T) {
	s := agentTestStore(t)
	ctx := context.Background()
	a := seedAgent(t, s, nil)

	updated := *a
	updated.RegisteredName = "HACKED-registered-name"
	updated.Status = "removed"
	updated.Role = "admin"
	require.NoError(t, s.UpdateAgent(ctx, &updated))

	got, err := s.GetAgent(ctx, a.AgentID)
	require.NoError(t, err)
	assert.Equal(t, a.RegisteredName, got.RegisteredName, "registered_name is immutable via UpdateAgent")
	assert.Equal(t, "active", got.Status, "status is not changed by UpdateAgent")
	assert.Equal(t, "admin", got.Role, "role is mutable via UpdateAgent")
}

func TestPostgresListAgentsExcludesRemoved(t *testing.T) {
	s := agentTestStore(t)
	ctx := context.Background()
	keep := seedAgent(t, s, nil)
	gone := seedAgent(t, s, nil)
	require.NoError(t, s.RemoveAgent(ctx, gone.AgentID))

	agents, err := s.ListAgents(ctx)
	require.NoError(t, err)
	ids := map[string]bool{}
	for _, ag := range agents {
		ids[ag.AgentID] = true
	}
	assert.True(t, ids[keep.AgentID], "active agent should be listed")
	assert.False(t, ids[gone.AgentID], "removed agent must be excluded")
}

func TestPostgresAgentStatusAndLastSeen(t *testing.T) {
	s := agentTestStore(t)
	ctx := context.Background()
	a := seedAgent(t, s, nil)

	require.NoError(t, s.UpdateAgentStatus(ctx, a.AgentID, "suspended"))
	got, err := s.GetAgent(ctx, a.AgentID)
	require.NoError(t, err)
	assert.Equal(t, "suspended", got.Status)

	// UpdateAgentLastSeen flips status back to active and stamps last_seen.
	seen := time.Now().UTC()
	require.NoError(t, s.UpdateAgentLastSeen(ctx, a.AgentID, seen))
	got, err = s.GetAgent(ctx, a.AgentID)
	require.NoError(t, err)
	assert.Equal(t, "active", got.Status)
	require.NotNil(t, got.LastSeen)
}

// TestPostgresBackfillFirstSeen verifies the "only when NULL" semantics.
func TestPostgresBackfillFirstSeen(t *testing.T) {
	s := agentTestStore(t)
	ctx := context.Background()
	a := seedAgent(t, s, nil)

	// Force first_seen NULL to simulate a permission-sync agent created without it.
	_, err := s.db.Exec(ctx, `UPDATE agents SET first_seen = NULL WHERE agent_id = $1`, a.AgentID)
	require.NoError(t, err)

	first := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	require.NoError(t, s.BackfillFirstSeen(ctx, a.AgentID, first))
	got, err := s.GetAgent(ctx, a.AgentID)
	require.NoError(t, err)
	require.NotNil(t, got.FirstSeen)
	assert.True(t, first.Equal(*got.FirstSeen))

	// A second backfill with a different time must be a no-op (already set).
	require.NoError(t, s.BackfillFirstSeen(ctx, a.AgentID, first.Add(48*time.Hour)))
	got, err = s.GetAgent(ctx, a.AgentID)
	require.NoError(t, err)
	assert.True(t, first.Equal(*got.FirstSeen), "first_seen must not be overwritten once set")
}

func TestPostgresMemoryCount(t *testing.T) {
	s := agentTestStore(t)
	ctx := context.Background()
	a := seedAgent(t, s, nil)
	seedMemory(t, s, a.AgentID, "crypto")
	seedMemory(t, s, a.AgentID, "crypto")
	seedMemory(t, s, a.AgentID, "vuln_intel")

	got, err := s.GetAgent(ctx, a.AgentID)
	require.NoError(t, err)
	assert.Equal(t, 3, got.MemoryCount)
}

func TestPostgresRotateAgentKey(t *testing.T) {
	s := agentTestStore(t)
	ctx := context.Background()
	a := seedAgent(t, s, nil)
	seedMemory(t, s, a.AgentID, "crypto")
	seedMemory(t, s, a.AgentID, "vuln_intel")

	newID, seed, err := s.RotateAgentKey(ctx, a.AgentID)
	require.NoError(t, err)
	assert.Len(t, newID, 64, "new agent ID is hex-encoded 32-byte pubkey")
	assert.Len(t, seed, ed25519.SeedSize)
	assert.NotEqual(t, a.AgentID, newID)
	t.Cleanup(func() {
		_, _ = s.db.Exec(ctx, `DELETE FROM memories WHERE submitting_agent = $1`, newID)
		_, _ = s.db.Exec(ctx, `DELETE FROM agents WHERE agent_id = $1`, newID)
	})

	// Old agent retired.
	old, err := s.GetAgent(ctx, a.AgentID)
	require.NoError(t, err)
	assert.Equal(t, "removed", old.Status)
	assert.Equal(t, 0, old.MemoryCount, "memories re-attributed away from old ID")

	// New agent active and carrying the memories.
	fresh, err := s.GetAgent(ctx, newID)
	require.NoError(t, err)
	assert.NotEqual(t, "removed", fresh.Status)
	assert.Equal(t, 2, fresh.MemoryCount)

	// Rotating a removed agent is rejected.
	_, _, err = s.RotateAgentKey(ctx, a.AgentID)
	require.Error(t, err)
}

func TestPostgresReassignMemories(t *testing.T) {
	s := agentTestStore(t)
	ctx := context.Background()
	src := seedAgent(t, s, nil)
	dst := seedAgent(t, s, nil)
	seedMemory(t, s, src.AgentID, "crypto")
	seedMemory(t, s, src.AgentID, "crypto")
	seedMemory(t, s, src.AgentID, "vuln_intel")

	count, err := s.ReassignMemories(ctx, src.AgentID, dst.AgentID)
	require.NoError(t, err)
	assert.Equal(t, int64(3), count)

	srcGot, _ := s.GetAgent(ctx, src.AgentID)
	dstGot, _ := s.GetAgent(ctx, dst.AgentID)
	assert.Equal(t, 0, srcGot.MemoryCount)
	assert.Equal(t, 3, dstGot.MemoryCount)

	// Reassigning to a removed agent is rejected.
	require.NoError(t, s.RemoveAgent(ctx, dst.AgentID))
	_, err = s.ReassignMemories(ctx, src.AgentID, dst.AgentID)
	require.Error(t, err)
}

func TestPostgresListAgentDomains(t *testing.T) {
	s := agentTestStore(t)
	ctx := context.Background()
	a := seedAgent(t, s, nil)
	seedMemory(t, s, a.AgentID, "crypto")
	seedMemory(t, s, a.AgentID, "crypto")
	seedMemory(t, s, a.AgentID, "vuln_intel")

	domains, err := s.ListAgentDomains(ctx, a.AgentID)
	require.NoError(t, err)
	// Ordered by count desc: crypto (2) before vuln_intel (1).
	require.Equal(t, []string{"crypto", "vuln_intel"}, domains)
}

func TestPostgresReassignMemoriesByDomain(t *testing.T) {
	s := agentTestStore(t)
	ctx := context.Background()
	src := seedAgent(t, s, nil)
	dst := seedAgent(t, s, nil)
	seedMemory(t, s, src.AgentID, "crypto")
	seedMemory(t, s, src.AgentID, "crypto")
	seedMemory(t, s, src.AgentID, "web")

	count, err := s.ReassignMemoriesByDomain(ctx, src.AgentID, dst.AgentID, "crypto")
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)

	srcGot, _ := s.GetAgent(ctx, src.AgentID)
	dstGot, _ := s.GetAgent(ctx, dst.AgentID)
	assert.Equal(t, 1, srcGot.MemoryCount, "non-crypto memory stays with source")
	assert.Equal(t, 2, dstGot.MemoryCount)

	srcDomains, _ := s.ListAgentDomains(ctx, src.AgentID)
	assert.Equal(t, []string{"web"}, srcDomains)
}

// TestPostgresTagOpsAreNoOps documents that tags are a SQLite/personal-mode
// feature: the Postgres tag methods must not error.
func TestPostgresTagOpsAreNoOps(t *testing.T) {
	s := agentTestStore(t)
	ctx := context.Background()
	dst := seedAgent(t, s, nil)

	tags, err := s.ListAgentTags(ctx, dst.AgentID)
	require.NoError(t, err)
	assert.Empty(t, tags)

	count, err := s.ReassignMemoriesByTag(ctx, "some-source", dst.AgentID, "any-tag")
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	// Still validates the target like SQLite does.
	require.NoError(t, s.RemoveAgent(ctx, dst.AgentID))
	_, err = s.ReassignMemoriesByTag(ctx, "some-source", dst.AgentID, "any-tag")
	require.Error(t, err)
}

// TestPostgresEnsureAgentSchemaLegacyMigration creates the pre-v8 5-column
// skeleton in a throwaway database and verifies ensureAgentSchema upgrades it
// in place so CreateAgent works without wiping the data volume.
func TestPostgresEnsureAgentSchemaLegacyMigration(t *testing.T) {
	ctx := context.Background()
	admin := agentTestStore(t)

	dbName := fmt.Sprintf("sage_migtest_%d", time.Now().UnixNano())
	_, err := admin.db.Exec(ctx, "CREATE DATABASE "+dbName)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.db.Exec(ctx, "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)")
	})

	u, err := url.Parse(agentTestDSN())
	require.NoError(t, err)
	u.Path = "/" + dbName
	pool, err := pgxpool.New(ctx, u.String())
	require.NoError(t, err)
	defer pool.Close()

	// Legacy skeleton: agent_id + the four columns shipped before v8.
	_, err = pool.Exec(ctx, `CREATE TABLE agents (
		agent_id      TEXT        PRIMARY KEY,
		display_name  TEXT,
		organization  TEXT,
		domains       TEXT[],
		registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`)
	require.NoError(t, err)

	s := &PostgresStore{db: pool, pool: pool}
	require.NoError(t, s.ensureAgentSchema(ctx), "legacy skeleton must migrate in place")

	// Every v8 column must now exist.
	want := []string{
		"name", "registered_name", "role", "avatar", "boot_bio", "validator_pubkey",
		"node_id", "p2p_address", "status", "clearance", "org_id", "dept_id",
		"domain_access", "bundle_path", "on_chain_height", "visible_agents",
		"provider", "claim_token", "claim_expires_at", "first_seen", "last_seen",
		"created_at", "removed_at",
	}
	rows, err := pool.Query(ctx, `SELECT column_name FROM information_schema.columns WHERE table_name = 'agents'`)
	require.NoError(t, err)
	have := map[string]bool{}
	for rows.Next() {
		var c string
		require.NoError(t, rows.Scan(&c))
		have[c] = true
	}
	rows.Close()
	for _, c := range want {
		assert.True(t, have[c], "column %q must exist after migration", c)
	}

	// And a write through the real method must now succeed.
	now := time.Now().UTC()
	require.NoError(t, s.CreateAgent(ctx, &AgentEntry{
		AgentID:   "migrated-" + uuid.NewString(),
		Name:      "post-migration",
		Status:    "active",
		Clearance: 2,
		CreatedAt: now,
		FirstSeen: &now,
	}))
}

// TestPostgresEnsureAgentSchemaConcurrentBoot simulates N quorum nodes
// cold-booting against the same not-yet-migrated database. The transaction-
// scoped advisory lock must serialize the migration so none lose the CREATE/
// ALTER race and fail to boot.
func TestPostgresEnsureAgentSchemaConcurrentBoot(t *testing.T) {
	ctx := context.Background()
	admin := agentTestStore(t)

	dbName := fmt.Sprintf("sage_migrace_%d", time.Now().UnixNano())
	_, err := admin.db.Exec(ctx, "CREATE DATABASE "+dbName)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.db.Exec(ctx, "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)")
	})

	u, err := url.Parse(agentTestDSN())
	require.NoError(t, err)
	u.Path = "/" + dbName
	migDSN := u.String()

	// Seed the legacy skeleton so ensureAgentSchema must actually migrate.
	seedPool, err := pgxpool.New(ctx, migDSN)
	require.NoError(t, err)
	_, err = seedPool.Exec(ctx, `CREATE TABLE agents (
		agent_id TEXT PRIMARY KEY, display_name TEXT, organization TEXT,
		domains TEXT[], registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`)
	require.NoError(t, err)
	seedPool.Close()

	const n = 4 // quorum size
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			s, e := NewPostgresStore(ctx, migDSN) // runs ensureAgentSchema
			if e == nil {
				s.Close()
			}
			errs[idx] = e
		}(i)
	}
	wg.Wait()
	for i, e := range errs {
		assert.NoError(t, e, "node %d must boot without losing the migration race", i)
	}
}
