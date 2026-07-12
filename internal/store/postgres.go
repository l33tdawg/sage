package store

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgvector/pgvector-go"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/vault"
)

// pgxDB is satisfied by both *pgxpool.Pool and pgx.Tx, allowing
// PostgresStore methods to work inside or outside a transaction.
type pgxDB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// PostgresStore implements MemoryStore and ValidatorScoreStore using PostgreSQL.
type PostgresStore struct {
	db   pgxDB         // either pool or tx
	pool *pgxpool.Pool // nil for tx-scoped stores
}

// NewPostgresStore creates a new PostgreSQL store.
func NewPostgresStore(ctx context.Context, connString string) (*PostgresStore, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}

	config.MaxConns = 20
	config.MinConns = 2

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	s := &PostgresStore{db: pool, pool: pool}

	// Self-heal the agents schema. init.sql only runs on first DB init
	// (docker-entrypoint-initdb.d), so pre-v8 deployments carry the legacy
	// 5-column skeleton that lacks the access-control columns the offchain
	// flush writes. Mirror SQLite's ALTER-on-boot migration (sqlite.go) so an
	// agent_register tx can't panic Commit with a missing-column error.
	if err := s.ensureAgentSchema(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ensure agent schema: %w", err)
	}

	// Same rationale as the agents mirror: the governance_proposals /
	// governance_votes tables are written by the abci flush, so a deployment
	// whose init.sql predates them must self-heal on boot.
	if err := s.ensureGovSchema(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ensure governance schema: %w", err)
	}

	// Same rationale: the memories.task_status column postdates the earliest
	// Postgres-quorum deployments, and v11.2 recall (GetMemory / QuerySimilar)
	// now SELECTs COALESCE(task_status, '') — which is a plan-time undefined_column
	// error, not a maskable NULL, on a legacy schema. Self-heal so those
	// deployments don't 500 every read after upgrade (also heals the pre-existing
	// UpdateTaskStatus / GetOpenTasks breakage on the same DBs).
	if err := s.ensureMemoriesSchema(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ensure memories schema: %w", err)
	}

	return s, nil
}

// agentSchemaLockKey is an arbitrary stable 64-bit key for the advisory lock
// that serializes concurrent agents-table migrations across quorum nodes.
const agentSchemaLockKey int64 = 0x5341_4745_4147_4E54 // "SAGEAGNT"

// ensureAgentSchema brings the agents table up to the v8 access-control shape.
// Idempotent: a no-op against a schema already created by the current init.sql,
// and an in-place migration for the legacy (agent_id, display_name,
// organization, domains, registered_at) skeleton. Must stay in sync with the
// agents table in deploy/init.sql and the columns CreateAgent/UpdateAgent write.
//
// The whole migration runs inside one transaction holding a transaction-scoped
// advisory lock, so that when N quorum nodes cold-boot against a shared,
// not-yet-migrated database, only one performs the DDL and the rest block then
// observe no-ops — avoiding a lost CREATE/ALTER race that would fail a node's
// boot. The lock releases automatically on commit. On an already-current schema
// every statement is a no-op and the lock is uncontended. RunInTx runs inline
// when the store is already tx-scoped (pool == nil).
func (s *PostgresStore) ensureAgentSchema(ctx context.Context) error {
	return s.RunInTx(ctx, func(tx OffchainStore) error {
		ps := tx.(*PostgresStore)

		if _, err := ps.db.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, agentSchemaLockKey); err != nil {
			return fmt.Errorf("acquire agents schema lock: %w", err)
		}

		if _, err := ps.db.Exec(ctx, `
			CREATE TABLE IF NOT EXISTS agents (
				agent_id         TEXT        PRIMARY KEY,
				name             TEXT        NOT NULL DEFAULT '',
				registered_name  TEXT        NOT NULL DEFAULT '',
				role             TEXT        NOT NULL DEFAULT 'member',
				avatar           TEXT        NOT NULL DEFAULT '',
				boot_bio         TEXT        NOT NULL DEFAULT '',
				validator_pubkey TEXT        NOT NULL DEFAULT '',
				node_id          TEXT        NOT NULL DEFAULT '',
				p2p_address      TEXT        NOT NULL DEFAULT '',
				status           TEXT        NOT NULL DEFAULT 'pending',
				clearance        INTEGER     NOT NULL DEFAULT 1,
				org_id           TEXT        NOT NULL DEFAULT '',
				dept_id          TEXT        NOT NULL DEFAULT '',
				domain_access    TEXT        NOT NULL DEFAULT '',
				bundle_path      TEXT        NOT NULL DEFAULT '',
				on_chain_height  BIGINT      NOT NULL DEFAULT 0,
				visible_agents   TEXT        NOT NULL DEFAULT '',
				provider         TEXT        NOT NULL DEFAULT '',
				claim_token      TEXT        NOT NULL DEFAULT '',
				claim_expires_at TIMESTAMPTZ,
				first_seen       TIMESTAMPTZ,
				last_seen        TIMESTAMPTZ,
				created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				removed_at       TIMESTAMPTZ
			)`); err != nil {
			return fmt.Errorf("create agents table: %w", err)
		}

		// ADD COLUMN IF NOT EXISTS upgrades the legacy skeleton in place; each is
		// a no-op on a fresh table. The trailing CREATE INDEX statements mirror
		// deploy/init.sql so a migrated legacy DB gets the same indexes a fresh
		// install does. Listed in struct order for auditability.
		stmts := []string{
			`ALTER TABLE agents ADD COLUMN IF NOT EXISTS name             TEXT        NOT NULL DEFAULT ''`,
			`ALTER TABLE agents ADD COLUMN IF NOT EXISTS registered_name  TEXT        NOT NULL DEFAULT ''`,
			`ALTER TABLE agents ADD COLUMN IF NOT EXISTS role             TEXT        NOT NULL DEFAULT 'member'`,
			`ALTER TABLE agents ADD COLUMN IF NOT EXISTS avatar           TEXT        NOT NULL DEFAULT ''`,
			`ALTER TABLE agents ADD COLUMN IF NOT EXISTS boot_bio         TEXT        NOT NULL DEFAULT ''`,
			`ALTER TABLE agents ADD COLUMN IF NOT EXISTS validator_pubkey TEXT        NOT NULL DEFAULT ''`,
			`ALTER TABLE agents ADD COLUMN IF NOT EXISTS node_id          TEXT        NOT NULL DEFAULT ''`,
			`ALTER TABLE agents ADD COLUMN IF NOT EXISTS p2p_address      TEXT        NOT NULL DEFAULT ''`,
			`ALTER TABLE agents ADD COLUMN IF NOT EXISTS status           TEXT        NOT NULL DEFAULT 'pending'`,
			`ALTER TABLE agents ADD COLUMN IF NOT EXISTS clearance        INTEGER     NOT NULL DEFAULT 1`,
			`ALTER TABLE agents ADD COLUMN IF NOT EXISTS org_id           TEXT        NOT NULL DEFAULT ''`,
			`ALTER TABLE agents ADD COLUMN IF NOT EXISTS dept_id          TEXT        NOT NULL DEFAULT ''`,
			`ALTER TABLE agents ADD COLUMN IF NOT EXISTS domain_access    TEXT        NOT NULL DEFAULT ''`,
			`ALTER TABLE agents ADD COLUMN IF NOT EXISTS bundle_path      TEXT        NOT NULL DEFAULT ''`,
			`ALTER TABLE agents ADD COLUMN IF NOT EXISTS on_chain_height  BIGINT      NOT NULL DEFAULT 0`,
			`ALTER TABLE agents ADD COLUMN IF NOT EXISTS visible_agents   TEXT        NOT NULL DEFAULT ''`,
			`ALTER TABLE agents ADD COLUMN IF NOT EXISTS provider         TEXT        NOT NULL DEFAULT ''`,
			`ALTER TABLE agents ADD COLUMN IF NOT EXISTS claim_token      TEXT        NOT NULL DEFAULT ''`,
			`ALTER TABLE agents ADD COLUMN IF NOT EXISTS claim_expires_at TIMESTAMPTZ`,
			`ALTER TABLE agents ADD COLUMN IF NOT EXISTS first_seen       TIMESTAMPTZ`,
			`ALTER TABLE agents ADD COLUMN IF NOT EXISTS last_seen        TIMESTAMPTZ`,
			`ALTER TABLE agents ADD COLUMN IF NOT EXISTS created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
			`ALTER TABLE agents ADD COLUMN IF NOT EXISTS removed_at       TIMESTAMPTZ`,
			`CREATE INDEX IF NOT EXISTS idx_agents_name ON agents (name) WHERE status != 'removed'`,
			`CREATE INDEX IF NOT EXISTS idx_agents_org ON agents (org_id) WHERE org_id != ''`,
		}
		for _, stmt := range stmts {
			if _, err := ps.db.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("migrate agents schema: %w", err)
			}
		}
		return nil
	})
}

// govSchemaLockKey serializes concurrent governance-table migrations across
// quorum nodes (distinct from agentSchemaLockKey).
const govSchemaLockKey int64 = 0x5341_4745_474F_56FF // "SAGEGOV"

// ensureGovSchema creates the governance mirror tables if absent. Idempotent and
// serialized via a transaction-scoped advisory lock, mirroring ensureAgentSchema.
// Must stay in sync with the governance tables in deploy/init.sql.
func (s *PostgresStore) ensureGovSchema(ctx context.Context) error {
	return s.RunInTx(ctx, func(tx OffchainStore) error {
		ps := tx.(*PostgresStore)
		if _, err := ps.db.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, govSchemaLockKey); err != nil {
			return fmt.Errorf("acquire governance schema lock: %w", err)
		}
		stmts := []string{
			`CREATE TABLE IF NOT EXISTS governance_proposals (
				proposal_id     TEXT        PRIMARY KEY,
				operation       TEXT        NOT NULL,
				target_agent_id TEXT        NOT NULL,
				target_pubkey   TEXT        NOT NULL DEFAULT '',
				target_power    BIGINT      NOT NULL DEFAULT 0,
				proposer_id     TEXT        NOT NULL,
				status          TEXT        NOT NULL DEFAULT 'voting',
				created_height  BIGINT      NOT NULL,
				expiry_height   BIGINT      NOT NULL,
				executed_height BIGINT,
				reason          TEXT        NOT NULL DEFAULT '',
				created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
			)`,
			`CREATE INDEX IF NOT EXISTS idx_gov_proposals_status ON governance_proposals (status, created_height DESC)`,
			`CREATE TABLE IF NOT EXISTS governance_votes (
				proposal_id  TEXT   NOT NULL,
				validator_id TEXT   NOT NULL,
				decision     TEXT   NOT NULL,
				height       BIGINT NOT NULL,
				PRIMARY KEY (proposal_id, validator_id)
			)`,
		}
		for _, stmt := range stmts {
			if _, err := ps.db.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("create governance schema: %w", err)
			}
		}
		return nil
	})
}

// memoriesSchemaLockKey serializes concurrent memories-table migrations across
// quorum nodes (distinct from the agent/gov keys).
const memoriesSchemaLockKey int64 = 0x5341_4745_4D45_4D53 // "SAGEMEMS"

var postgresTaskAssignmentSchema = []string{
	`ALTER TABLE memories ADD COLUMN IF NOT EXISTS assignee TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE memories ADD COLUMN IF NOT EXISTS task_picked_up_at TIMESTAMPTZ`,
	`ALTER TABLE memories ADD COLUMN IF NOT EXISTS task_picked_up_by TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE memories ADD COLUMN IF NOT EXISTS task_assignment_version BIGINT NOT NULL DEFAULT 0`,
	`ALTER TABLE memories ADD COLUMN IF NOT EXISTS task_requires_handoff BOOLEAN NOT NULL DEFAULT FALSE`,
	`CREATE INDEX IF NOT EXISTS idx_memories_assignee ON memories (assignee) WHERE assignee != ''`,
	`CREATE INDEX IF NOT EXISTS idx_memories_task_picked_up_by ON memories (task_picked_up_by) WHERE task_picked_up_by != ''`,
	`CREATE TABLE IF NOT EXISTS agent_notifications (
		notification_id TEXT PRIMARY KEY,
		agent_id TEXT NOT NULL,
		kind TEXT NOT NULL CHECK (kind IN ('task_assignment')),
		task_id UUID NOT NULL REFERENCES memories(memory_id) ON DELETE CASCADE,
		assignment_version BIGINT NOT NULL,
		domain TEXT NOT NULL DEFAULT '',
		title TEXT NOT NULL DEFAULT '',
		state TEXT NOT NULL DEFAULT 'unread' CHECK (state IN ('unread','read','superseded')),
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		read_at TIMESTAMPTZ,
		UNIQUE(kind, task_id, assignment_version, agent_id)
	)`,
	`CREATE INDEX IF NOT EXISTS idx_agent_notifications_inbox ON agent_notifications(agent_id, state, created_at)`,
	`CREATE INDEX IF NOT EXISTS idx_agent_notifications_task ON agent_notifications(task_id, assignment_version, state)`,
}

// ensureMemoriesSchema backfills memory columns on legacy Postgres deployments
// whose one-shot init.sql predates task status, assignment, or RBAC classification. Missing
// columns fail at query planning (42703), so COALESCE/fail-closed authorization
// cannot compensate. The migration is idempotent and serialized under the same
// transaction-scoped advisory lock. Keep it in sync with deploy/init.sql.
func (s *PostgresStore) ensureMemoriesSchema(ctx context.Context) error {
	return s.RunInTx(ctx, func(tx OffchainStore) error {
		ps := tx.(*PostgresStore)
		if _, err := ps.db.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, memoriesSchemaLockKey); err != nil {
			return fmt.Errorf("acquire memories schema lock: %w", err)
		}
		// Match deploy/init.sql:24 and the SQLite migration exactly: DEFAULT '' + the
		// same CHECK, no NOT NULL (recall COALESCEs NULL→''). Existing rows take the
		// '' default, which satisfies the CHECK; IF NOT EXISTS is idempotent so a
		// current/fresh DB (already has the column + CHECK from init.sql) is untouched.
		if _, err := ps.db.Exec(ctx,
			`ALTER TABLE memories ADD COLUMN IF NOT EXISTS task_status TEXT DEFAULT '' CHECK (task_status IN ('', 'planned', 'in_progress', 'done', 'dropped'))`); err != nil {
			return fmt.Errorf("migrate memories.task_status: %w", err)
		}
		// deploy/init.sql adds this after CREATE TABLE, so it must also be replayed
		// here for existing volumes. Default Internal (1), matching SQLite and the
		// canonical Postgres schema; authorization still fails closed on read errors.
		if _, err := ps.db.Exec(ctx,
			`ALTER TABLE memories ADD COLUMN IF NOT EXISTS classification SMALLINT NOT NULL DEFAULT 1`); err != nil {
			return fmt.Errorf("migrate memories.classification: %w", err)
		}
		for _, stmt := range postgresTaskAssignmentSchema {
			if _, err := ps.db.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("migrate task assignment schema: %w", err)
			}
		}
		// Repair pre-notification assignments without disturbing authenticated
		// pickup evidence. ON CONFLICT keeps restart/parallel boot idempotent.
		if _, err := ps.db.Exec(ctx, `UPDATE memories SET task_assignment_version = 1
			WHERE memory_type = 'task' AND COALESCE(assignee, '') != '' AND task_assignment_version = 0`); err != nil {
			return fmt.Errorf("backfill assignment generations: %w", err)
		}
		if _, err := ps.db.Exec(ctx, `UPDATE memories SET task_requires_handoff = TRUE
			WHERE memory_type = 'task' AND task_status IN ('done','dropped')`); err != nil {
			return fmt.Errorf("backfill terminal task handoff gates: %w", err)
		}
		if _, err := ps.db.Exec(ctx, `UPDATE memories SET task_status = 'planned'
			WHERE memory_type = 'task' AND task_status = 'in_progress'
			  AND COALESCE(assignee, '') = ''`); err != nil {
			return fmt.Errorf("repair ownerless in-progress tasks: %w", err)
		}
		if _, err := ps.db.Exec(ctx, `INSERT INTO agent_notifications
			(notification_id, agent_id, kind, task_id, assignment_version, domain, title, state)
			SELECT 'task-assignment:' || m.memory_id || ':' || m.task_assignment_version,
			       m.assignee, 'task_assignment', m.memory_id, m.task_assignment_version,
			       m.domain_tag, 'A task was assigned to you', 'unread'
			FROM memories m JOIN agents a ON a.agent_id = m.assignee
			WHERE m.memory_type = 'task' AND m.status != 'deprecated'
			  AND m.task_status IN ('planned','in_progress')
			  AND a.status = 'active' AND a.removed_at IS NULL
			ON CONFLICT DO NOTHING`); err != nil {
			return fmt.Errorf("backfill assignment notifications: %w", err)
		}
		return nil
	})
}

const postgresInsertMemorySQL = `INSERT INTO memories (memory_id, submitting_agent, content, content_hash, embedding, embedding_hash,
	memory_type, domain_tag, provider, confidence_score, status, parent_hash, task_status, assignee, created_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
	ON CONFLICT (memory_id) DO UPDATE SET
		submitting_agent = EXCLUDED.submitting_agent,
		status = EXCLUDED.status,
		created_at = EXCLUDED.created_at,
		embedding = COALESCE(EXCLUDED.embedding, memories.embedding),
		embedding_hash = COALESCE(EXCLUDED.embedding_hash, memories.embedding_hash),
		provider = COALESCE(NULLIF(EXCLUDED.provider, ''), memories.provider),
		assignee = COALESCE(NULLIF(EXCLUDED.assignee, ''), memories.assignee),
		parent_hash = COALESCE(NULLIF(EXCLUDED.parent_hash, ''), memories.parent_hash)`

func (s *PostgresStore) InsertMemory(ctx context.Context, record *memory.MemoryRecord) error {
	var emb *pgvector.Vector
	if len(record.Embedding) > 0 {
		v := pgvector.NewVector(record.Embedding)
		emb = &v
	}

	_, err := s.db.Exec(ctx, postgresInsertMemorySQL,
		record.MemoryID, record.SubmittingAgent, record.Content, record.ContentHash,
		emb, record.EmbeddingHash,
		string(record.MemoryType), record.DomainTag, record.Provider, record.ConfidenceScore,
		string(record.Status), record.ParentHash, string(record.TaskStatus), record.Assignee, record.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert memory: %w", err)
	}
	return nil
}

func (s *PostgresStore) UpdateMemoryEmbedding(_ context.Context, _ string, _ []float32, _ string) error {
	return fmt.Errorf("UpdateMemoryEmbedding not implemented for PostgresStore")
}

func (s *PostgresStore) CountMemoriesByProvider(_ context.Context) (map[string]int, error) {
	return nil, fmt.Errorf("CountMemoriesByProvider not implemented for PostgresStore")
}

func (s *PostgresStore) ListMemoriesForReembed(_ context.Context, _ int) ([]ReembedItem, error) {
	return nil, fmt.Errorf("ListMemoriesForReembed not implemented for PostgresStore")
}

func (s *PostgresStore) MarkMemoryEmbeddingSkipped(_ context.Context, _ string) error {
	return fmt.Errorf("MarkMemoryEmbeddingSkipped not implemented for PostgresStore")
}

func (s *PostgresStore) MarkMemoryEmbeddingError(_ context.Context, _ string) error {
	return fmt.Errorf("MarkMemoryEmbeddingError not implemented for PostgresStore")
}

func (s *PostgresStore) DeprecateUnreadableMemories(_ context.Context) (int, error) {
	return 0, fmt.Errorf("DeprecateUnreadableMemories not implemented for PostgresStore")
}

func (s *PostgresStore) ResetErroredEmbeddings(_ context.Context) (int, error) {
	return 0, fmt.Errorf("ResetErroredEmbeddings not implemented for PostgresStore")
}

func (s *PostgresStore) RekeyUnreadableMemories(_ context.Context, _ *vault.Vault, _ bool) (int, error) {
	return 0, fmt.Errorf("RekeyUnreadableMemories not implemented for PostgresStore")
}

func (s *PostgresStore) GetMemory(ctx context.Context, memoryID string) (*memory.MemoryRecord, error) {
	row := s.db.QueryRow(ctx,
		`SELECT memory_id, submitting_agent, content, content_hash, embedding, embedding_hash,
			memory_type, domain_tag, provider, confidence_score, status, parent_hash, created_at, committed_at, deprecated_at,
			COALESCE(task_status, '')
		FROM memories WHERE memory_id = $1`, memoryID)

	var r memory.MemoryRecord
	var mt, st, taskStatus string
	var emb *pgvector.Vector
	var parentHash *string

	err := row.Scan(&r.MemoryID, &r.SubmittingAgent, &r.Content, &r.ContentHash,
		&emb, &r.EmbeddingHash, &mt, &r.DomainTag, &r.Provider, &r.ConfidenceScore,
		&st, &parentHash, &r.CreatedAt, &r.CommittedAt, &r.DeprecatedAt, &taskStatus)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("memory not found: %s", memoryID)
		}
		return nil, fmt.Errorf("get memory: %w", err)
	}

	r.MemoryType = memory.MemoryType(mt)
	r.Status = memory.MemoryStatus(st)
	r.TaskStatus = memory.TaskStatus(taskStatus)
	if emb != nil {
		r.Embedding = emb.Slice()
	}
	if parentHash != nil {
		r.ParentHash = *parentHash
	}

	return &r, nil
}

func (s *PostgresStore) UpdateStatus(ctx context.Context, memoryID string, status memory.MemoryStatus, now time.Time) error {
	var query string
	switch status {
	case memory.StatusCommitted:
		query = `UPDATE memories SET status = $2, committed_at = $3 WHERE memory_id = $1`
	case memory.StatusDeprecated:
		query = `UPDATE memories SET status = $2, deprecated_at = $3 WHERE memory_id = $1`
	default:
		query = `UPDATE memories SET status = $2 WHERE memory_id = $1`
	}

	var err error
	if status == memory.StatusCommitted || status == memory.StatusDeprecated {
		_, err = s.db.Exec(ctx, query, memoryID, string(status), now)
	} else {
		_, err = s.db.Exec(ctx, `UPDATE memories SET status = $2 WHERE memory_id = $1`, memoryID, string(status))
	}
	if err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	return nil
}

func (s *PostgresStore) QuerySimilar(ctx context.Context, embedding []float32, opts QueryOptions) ([]*memory.MemoryRecord, error) {
	if opts.TopK <= 0 {
		opts.TopK = 10
	}
	if opts.TopK > 100 {
		opts.TopK = 100
	}

	query := `SELECT memory_id, submitting_agent, content, content_hash, embedding,
		memory_type, domain_tag, provider, confidence_score, status, parent_hash, created_at,
		committed_at, deprecated_at, COALESCE(task_status, ''), embedding <=> $1 AS distance
		FROM memories WHERE embedding IS NOT NULL`
	args := []any{pgvector.NewVector(embedding)}
	argIdx := 2

	if opts.DomainTag != "" {
		query += fmt.Sprintf(" AND domain_tag = $%d", argIdx)
		args = append(args, opts.DomainTag)
		argIdx++
	}
	if opts.Provider != "" {
		query += fmt.Sprintf(" AND (provider = $%d OR provider = '' OR memory_type = 'fact')", argIdx)
		args = append(args, opts.Provider)
		argIdx++
	}
	if opts.MinConfidence > 0 {
		query += fmt.Sprintf(" AND confidence_score >= $%d", argIdx)
		args = append(args, opts.MinConfidence)
		argIdx++
	}
	if opts.StatusFilter == "committed" && opts.IncludeDisputed {
		// app-v17 parity with SQLite: a challenged memory is still live and
		// recallable while its two-phase dispute is open. REST marks it disputed
		// and applies the presentation haircut after this store admission step.
		query += " AND status IN ('committed', 'challenged')"
	} else if opts.StatusFilter != "" {
		query += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, opts.StatusFilter)
		argIdx++
	}
	if len(opts.SubmittingAgents) > 0 {
		placeholders := make([]string, len(opts.SubmittingAgents))
		for i, a := range opts.SubmittingAgents {
			placeholders[i] = fmt.Sprintf("$%d", argIdx)
			args = append(args, a)
			argIdx++
		}
		query += " AND submitting_agent IN (" + strings.Join(placeholders, ",") + ")"
	}
	// opts.Tags is ignored on PostgresStore — tags are a SQLite-only feature
	// (PostgresStore.SetTags is a no-op, so no tagged memories can exist).

	scanLimit := opts.TopK
	if opts.DecayFloor > 0 {
		// Over-fetch: the decayed floor isn't a SQL predicate, so pull a bounded
		// distance-ordered pool and filter it in Go below, before trimming to top_k.
		scanLimit = decayFilterScanCap
	}
	query += fmt.Sprintf(" ORDER BY embedding <=> $1 LIMIT $%d", argIdx)
	args = append(args, scanLimit)

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query similar: %w", err)
	}
	defer rows.Close()

	results := make([]*memory.MemoryRecord, 0)
	for rows.Next() {
		var r memory.MemoryRecord
		var mt, st, taskStatus string
		var emb *pgvector.Vector
		var parentHash *string
		var distance float64

		scanErr := rows.Scan(&r.MemoryID, &r.SubmittingAgent, &r.Content, &r.ContentHash,
			&emb, &mt, &r.DomainTag, &r.Provider, &r.ConfidenceScore,
			&st, &parentHash, &r.CreatedAt, &r.CommittedAt, &r.DeprecatedAt, &taskStatus, &distance)
		if scanErr != nil {
			return nil, fmt.Errorf("scan row: %w", scanErr)
		}

		r.MemoryType = memory.MemoryType(mt)
		r.Status = memory.MemoryStatus(st)
		r.TaskStatus = memory.TaskStatus(taskStatus)
		if emb != nil {
			r.Embedding = emb.Slice()
		}
		if parentHash != nil {
			r.ParentHash = *parentHash
		}
		results = append(results, &r)
	}

	// Decayed-confidence floor (if any) over the over-fetched pool, before trim.
	if opts.DecayFloor > 0 {
		counts, cErr := s.GetCorroborationCounts(ctx, recordIDs(results))
		if cErr != nil {
			return nil, fmt.Errorf("query similar decay floor: %w", cErr)
		}
		results = applyDecayFloor(results, opts.DecayFloor, opts.DecayNow, counts, opts.IncludeDisputed)
		if len(results) > opts.TopK {
			results = results[:opts.TopK]
		}
	}
	return results, nil
}

// SearchByText is not implemented for PostgresStore — Postgres deployments use Ollama for semantic search.
func (s *PostgresStore) SearchByText(_ context.Context, _ string, _ QueryOptions) ([]*memory.MemoryRecord, error) {
	return nil, fmt.Errorf("text search not available on PostgresStore — use semantic search with Ollama")
}

// SearchHybrid on Postgres degrades to vector-only since there's no FTS index
// in this backend. Kept for interface parity so callers don't branch on store
// type — the merge layer just sees a one-stream input.
func (s *PostgresStore) SearchHybrid(ctx context.Context, _ string, embedding []float32, opts QueryOptions) ([]*memory.MemoryRecord, error) {
	if len(embedding) == 0 {
		return nil, fmt.Errorf("hybrid search on Postgres requires an embedding")
	}
	return s.QuerySimilar(ctx, embedding, opts)
}

func (s *PostgresStore) InsertTriples(ctx context.Context, memoryID string, triples []memory.KnowledgeTriple) error {
	batch := &pgx.Batch{}
	for _, t := range triples {
		batch.Queue("INSERT INTO knowledge_triples (memory_id, subject, predicate, object) VALUES ($1, $2, $3, $4)",
			memoryID, t.Subject, t.Predicate, t.Object)
	}

	br := s.db.SendBatch(ctx, batch)
	defer func() { _ = br.Close() }()

	for range triples {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("insert triple: %w", err)
		}
	}
	return nil
}

func (s *PostgresStore) InsertVote(ctx context.Context, vote *ValidationVote) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO validation_votes (memory_id, validator_id, decision, rationale, weight_at_vote, block_height, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (memory_id, validator_id) DO UPDATE SET decision = $3, rationale = $4, weight_at_vote = $5`,
		vote.MemoryID, vote.ValidatorID, vote.Decision, vote.Rationale, vote.WeightAtVote, vote.BlockHeight, vote.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert vote: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetVotes(ctx context.Context, memoryID string) ([]*ValidationVote, error) {
	rows, err := s.db.Query(ctx,
		`SELECT id, memory_id, validator_id, decision, rationale, weight_at_vote, block_height, created_at
		FROM validation_votes WHERE memory_id = $1 ORDER BY created_at`, memoryID)
	if err != nil {
		return nil, fmt.Errorf("get votes: %w", err)
	}
	defer rows.Close()

	var votes []*ValidationVote
	for rows.Next() {
		v := &ValidationVote{}
		if scanErr := rows.Scan(&v.ID, &v.MemoryID, &v.ValidatorID, &v.Decision, &v.Rationale,
			&v.WeightAtVote, &v.BlockHeight, &v.CreatedAt); scanErr != nil {
			return nil, fmt.Errorf("scan vote: %w", scanErr)
		}
		votes = append(votes, v)
	}
	return votes, nil
}

func (s *PostgresStore) InsertChallenge(ctx context.Context, challenge *ChallengeEntry) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO challenges (memory_id, challenger_id, reason, evidence, block_height, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		challenge.MemoryID, challenge.ChallengerID, challenge.Reason, challenge.Evidence, challenge.BlockHeight, challenge.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert challenge: %w", err)
	}
	return nil
}

func (s *PostgresStore) InsertCorroboration(ctx context.Context, corr *Corroboration) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO corroborations (memory_id, agent_id, evidence, created_at)
		VALUES ($1, $2, $3, $4)`,
		corr.MemoryID, corr.AgentID, corr.Evidence, corr.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert corroboration: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetCorroborations(ctx context.Context, memoryID string) ([]*Corroboration, error) {
	rows, err := s.db.Query(ctx,
		`SELECT id, memory_id, agent_id, evidence, created_at
		FROM corroborations WHERE memory_id = $1 ORDER BY created_at`, memoryID)
	if err != nil {
		return nil, fmt.Errorf("get corroborations: %w", err)
	}
	defer rows.Close()

	var corrs []*Corroboration
	for rows.Next() {
		c := &Corroboration{}
		if scanErr := rows.Scan(&c.ID, &c.MemoryID, &c.AgentID, &c.Evidence, &c.CreatedAt); scanErr != nil {
			return nil, fmt.Errorf("scan corroboration: %w", scanErr)
		}
		corrs = append(corrs, c)
	}
	return corrs, nil
}

func (s *PostgresStore) GetPendingByDomain(ctx context.Context, domainTag string, limit int) ([]*memory.MemoryRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.Query(ctx,
		`SELECT memory_id, submitting_agent, content, content_hash,
			memory_type, domain_tag, confidence_score, status, created_at
		FROM memories WHERE status = 'proposed' AND domain_tag LIKE $1
		ORDER BY created_at LIMIT $2`, domainTag, limit)
	if err != nil {
		return nil, fmt.Errorf("get pending: %w", err)
	}
	defer rows.Close()

	results := make([]*memory.MemoryRecord, 0)
	for rows.Next() {
		var r memory.MemoryRecord
		var mt, st string
		if scanErr := rows.Scan(&r.MemoryID, &r.SubmittingAgent, &r.Content, &r.ContentHash,
			&mt, &r.DomainTag, &r.ConfidenceScore, &st, &r.CreatedAt); scanErr != nil {
			return nil, fmt.Errorf("scan pending: %w", scanErr)
		}
		r.MemoryType = memory.MemoryType(mt)
		r.Status = memory.MemoryStatus(st)
		results = append(results, &r)
	}
	return results, nil
}

// OldestProposedCreatedAt returns MIN(created_at) over status='proposed'
// memories — the voter-observability probe behind
// sage_proposed_oldest_age_seconds. ok=false (zero time) when nothing is
// pending. Keep in lockstep with the SQLiteStore implementation.
func (s *PostgresStore) OldestProposedCreatedAt(ctx context.Context) (time.Time, bool, error) {
	var createdAt *time.Time // MIN over zero rows is NULL
	err := s.db.QueryRow(ctx,
		`SELECT MIN(created_at) FROM memories WHERE status = 'proposed'`).Scan(&createdAt)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("oldest proposed: %w", err)
	}
	if createdAt == nil {
		return time.Time{}, false, nil
	}
	return *createdAt, true, nil
}

// ProposedPendingCount returns how many memories are still in status='proposed'
// (sage_proposed_pending_count). Keep in lockstep with the SQLiteStore
// implementation.
func (s *PostgresStore) ProposedPendingCount(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRow(ctx,
		`SELECT COUNT(*) FROM memories WHERE status = 'proposed'`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("proposed pending count: %w", err)
	}
	return n, nil
}

// GetScore retrieves a validator's score.
func (s *PostgresStore) GetScore(ctx context.Context, validatorID string) (*ValidatorScore, error) {
	row := s.db.QueryRow(ctx,
		`SELECT validator_id, weighted_sum, weight_denom, vote_count, expertise_vec,
			last_active_ts, current_weight, updated_at
		FROM validator_scores WHERE validator_id = $1`, validatorID)

	vs := &ValidatorScore{}
	err := row.Scan(&vs.ValidatorID, &vs.WeightedSum, &vs.WeightDenom, &vs.VoteCount,
		&vs.ExpertiseVec, &vs.LastActiveTS, &vs.CurrentWeight, &vs.UpdatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("validator score not found: %s", validatorID)
		}
		return nil, fmt.Errorf("get validator score: %w", err)
	}
	return vs, nil
}

// UpdateScore upserts a validator's score.
func (s *PostgresStore) UpdateScore(ctx context.Context, score *ValidatorScore) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO validator_scores (validator_id, weighted_sum, weight_denom, vote_count, expertise_vec,
			last_active_ts, current_weight, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (validator_id) DO UPDATE SET
			weighted_sum = $2, weight_denom = $3, vote_count = $4, expertise_vec = $5,
			last_active_ts = $6, current_weight = $7, updated_at = $8`,
		score.ValidatorID, score.WeightedSum, score.WeightDenom, score.VoteCount,
		score.ExpertiseVec, score.LastActiveTS, score.CurrentWeight, score.UpdatedAt)
	if err != nil {
		return fmt.Errorf("update validator score: %w", err)
	}
	return nil
}

// GetAllScores retrieves all validator scores.
func (s *PostgresStore) GetAllScores(ctx context.Context) ([]*ValidatorScore, error) {
	rows, err := s.db.Query(ctx,
		`SELECT validator_id, weighted_sum, weight_denom, vote_count, expertise_vec,
			last_active_ts, current_weight, updated_at
		FROM validator_scores ORDER BY validator_id`)
	if err != nil {
		return nil, fmt.Errorf("get all scores: %w", err)
	}
	defer rows.Close()

	var scores []*ValidatorScore
	for rows.Next() {
		vs := &ValidatorScore{}
		if scanErr := rows.Scan(&vs.ValidatorID, &vs.WeightedSum, &vs.WeightDenom, &vs.VoteCount,
			&vs.ExpertiseVec, &vs.LastActiveTS, &vs.CurrentWeight, &vs.UpdatedAt); scanErr != nil {
			return nil, fmt.Errorf("scan validator score: %w", scanErr)
		}
		scores = append(scores, vs)
	}
	return scores, nil
}

// InsertEpochScore records an epoch score snapshot.
func (s *PostgresStore) InsertEpochScore(ctx context.Context, epoch *EpochScore) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO epoch_scores (epoch_num, block_height, validator_id, accuracy, domain_score,
			recency_score, corr_score, raw_weight, capped_weight, normalized_weight)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (epoch_num, validator_id) DO NOTHING`,
		epoch.EpochNum, epoch.BlockHeight, epoch.ValidatorID, epoch.Accuracy, epoch.DomainScore,
		epoch.RecencyScore, epoch.CorrScore, epoch.RawWeight, epoch.CappedWeight, epoch.NormalizedWeight)
	if err != nil {
		return fmt.Errorf("insert epoch score: %w", err)
	}
	return nil
}

// --- Federation Access Control ---

// InsertAccessGrant inserts an access grant into PostgreSQL.
func (s *PostgresStore) InsertAccessGrant(ctx context.Context, grant *AccessGrantEntry) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO access_grants (domain, grantee_id, granter_id, access_level, expires_at, created_height, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (domain, grantee_id, created_height) DO NOTHING`,
		grant.Domain, grant.GranteeID, grant.GranterID, grant.Level, grant.ExpiresAt, grant.CreatedHeight, grant.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert access grant: %w", err)
	}
	return nil
}

// GetActiveGrants retrieves all non-revoked grants for an agent.
func (s *PostgresStore) GetActiveGrants(ctx context.Context, agentID string) ([]*AccessGrantEntry, error) {
	rows, err := s.db.Query(ctx,
		`SELECT domain, grantee_id, granter_id, access_level, expires_at, created_height, created_at
		FROM access_grants WHERE grantee_id = $1 AND revoked_at IS NULL
		ORDER BY created_at`, agentID)
	if err != nil {
		return nil, fmt.Errorf("get active grants: %w", err)
	}
	defer rows.Close()

	var grants []*AccessGrantEntry
	for rows.Next() {
		g := &AccessGrantEntry{}
		if scanErr := rows.Scan(&g.Domain, &g.GranteeID, &g.GranterID, &g.Level, &g.ExpiresAt, &g.CreatedHeight, &g.CreatedAt); scanErr != nil {
			return nil, fmt.Errorf("scan grant: %w", scanErr)
		}
		grants = append(grants, g)
	}
	return grants, nil
}

// RevokeGrant marks a grant as revoked.
func (s *PostgresStore) RevokeGrant(ctx context.Context, domain, granteeID string, height int64) error {
	_, err := s.db.Exec(ctx,
		`UPDATE access_grants SET revoked_at = NOW()
		WHERE domain = $1 AND grantee_id = $2 AND revoked_at IS NULL`,
		domain, granteeID)
	if err != nil {
		return fmt.Errorf("revoke grant: %w", err)
	}
	return nil
}

// InsertAccessRequest inserts an access request into PostgreSQL.
func (s *PostgresStore) InsertAccessRequest(ctx context.Context, req *AccessRequestEntry) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO access_requests (request_id, requester_id, target_domain, justification, status, created_height, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (request_id) DO NOTHING`,
		req.RequestID, req.RequesterID, req.TargetDomain, req.Justification, req.Status, req.CreatedHeight, req.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert access request: %w", err)
	}
	return nil
}

// UpdateAccessRequestStatus updates the status of an access request.
func (s *PostgresStore) UpdateAccessRequestStatus(ctx context.Context, requestID, status string, height int64) error {
	_, err := s.db.Exec(ctx,
		`UPDATE access_requests SET status = $2, resolved_height = $3 WHERE request_id = $1`,
		requestID, status, height)
	if err != nil {
		return fmt.Errorf("update access request status: %w", err)
	}
	return nil
}

// InsertAccessLog inserts an audit log entry into PostgreSQL.
func (s *PostgresStore) InsertAccessLog(ctx context.Context, log *AccessLogEntry) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO access_logs (agent_id, domain, action, memory_ids, block_height, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		log.AgentID, log.Domain, log.Action, log.MemoryIDs, log.BlockHeight, log.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert access log: %w", err)
	}
	return nil
}

// InsertDomain inserts a domain registry entry into PostgreSQL.
func (s *PostgresStore) InsertDomain(ctx context.Context, domain *DomainEntry) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO domain_registry (domain_name, owner_agent_id, parent_domain, description, created_height, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (domain_name) DO NOTHING`,
		domain.DomainName, domain.OwnerAgentID, domain.ParentDomain, domain.Description, domain.CreatedHeight, domain.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert domain: %w", err)
	}
	return nil
}

// GetDomain retrieves a domain registry entry from PostgreSQL.
func (s *PostgresStore) GetDomain(ctx context.Context, name string) (*DomainEntry, error) {
	row := s.db.QueryRow(ctx,
		`SELECT domain_name, owner_agent_id, parent_domain, description, created_height, created_at
		FROM domain_registry WHERE domain_name = $1`, name)

	d := &DomainEntry{}
	var parentDomain, description *string
	err := row.Scan(&d.DomainName, &d.OwnerAgentID, &parentDomain, &description, &d.CreatedHeight, &d.CreatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("domain not found: %s", name)
		}
		return nil, fmt.Errorf("get domain: %w", err)
	}
	if parentDomain != nil {
		d.ParentDomain = *parentDomain
	}
	if description != nil {
		d.Description = *description
	}
	return d, nil
}

// --- Organization / Federation / Classification ---

// InsertOrg inserts an organization into PostgreSQL.
func (s *PostgresStore) InsertOrg(ctx context.Context, org *OrgEntry) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO organizations (org_id, name, description, admin_agent_id, created_height, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (org_id) DO NOTHING`,
		org.OrgID, org.Name, org.Description, org.AdminAgentID, org.CreatedHeight, org.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert org: %w", err)
	}
	return nil
}

// GetOrg retrieves an organization from PostgreSQL.
func (s *PostgresStore) GetOrg(ctx context.Context, orgID string) (*OrgEntry, error) {
	row := s.db.QueryRow(ctx,
		`SELECT org_id, name, description, admin_agent_id, created_height, created_at
		FROM organizations WHERE org_id = $1`, orgID)

	o := &OrgEntry{}
	var description *string
	err := row.Scan(&o.OrgID, &o.Name, &description, &o.AdminAgentID, &o.CreatedHeight, &o.CreatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("org not found: %s", orgID)
		}
		return nil, fmt.Errorf("get org: %w", err)
	}
	if description != nil {
		o.Description = *description
	}
	return o, nil
}

// InsertOrgMember inserts an org member into PostgreSQL.
func (s *PostgresStore) InsertOrgMember(ctx context.Context, member *OrgMemberEntry) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO org_members (org_id, agent_id, clearance, role, created_height, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (org_id, agent_id) DO NOTHING`,
		member.OrgID, member.AgentID, int16(member.Clearance), member.Role, member.CreatedHeight, member.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert org member: %w", err)
	}
	return nil
}

// RemoveOrgMember marks a member as removed.
func (s *PostgresStore) RemoveOrgMember(ctx context.Context, orgID, agentID string, height int64) error {
	_, err := s.db.Exec(ctx,
		`UPDATE org_members SET removed_at = NOW()
		WHERE org_id = $1 AND agent_id = $2 AND removed_at IS NULL`,
		orgID, agentID)
	if err != nil {
		return fmt.Errorf("remove org member: %w", err)
	}
	return nil
}

// UpdateMemberClearance updates a member's clearance level.
func (s *PostgresStore) UpdateMemberClearance(ctx context.Context, orgID, agentID string, clearance ClearanceLevel) error {
	_, err := s.db.Exec(ctx,
		`UPDATE org_members SET clearance = $3
		WHERE org_id = $1 AND agent_id = $2 AND removed_at IS NULL`,
		orgID, agentID, int16(clearance))
	if err != nil {
		return fmt.Errorf("update member clearance: %w", err)
	}
	return nil
}

// GetOrgMembers retrieves all active members of an organization.
func (s *PostgresStore) GetOrgMembers(ctx context.Context, orgID string) ([]*OrgMemberEntry, error) {
	rows, err := s.db.Query(ctx,
		`SELECT org_id, agent_id, clearance, role, created_height, created_at
		FROM org_members WHERE org_id = $1 AND removed_at IS NULL
		ORDER BY created_at`, orgID)
	if err != nil {
		return nil, fmt.Errorf("get org members: %w", err)
	}
	defer rows.Close()

	var members []*OrgMemberEntry
	for rows.Next() {
		m := &OrgMemberEntry{}
		var clearance int16
		if scanErr := rows.Scan(&m.OrgID, &m.AgentID, &clearance, &m.Role, &m.CreatedHeight, &m.CreatedAt); scanErr != nil {
			return nil, fmt.Errorf("scan org member: %w", scanErr)
		}
		m.Clearance = ClearanceLevel(clearance) // #nosec G115 -- clearance is 0-4
		members = append(members, m)
	}
	return members, nil
}

// InsertFederation inserts a federation agreement into PostgreSQL.
func (s *PostgresStore) InsertFederation(ctx context.Context, fed *FederationEntry) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO federations (federation_id, proposer_org_id, target_org_id, allowed_domains, allowed_depts,
			max_clearance, expires_at, requires_approval, status, created_height, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (federation_id) DO NOTHING`,
		fed.FederationID, fed.ProposerOrgID, fed.TargetOrgID, fed.AllowedDomains, fed.AllowedDepts,
		int16(fed.MaxClearance), fed.ExpiresAt, fed.RequiresApproval, fed.Status,
		fed.CreatedHeight, fed.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert federation: %w", err)
	}
	return nil
}

// GetFederation retrieves a federation agreement from PostgreSQL.
func (s *PostgresStore) GetFederation(ctx context.Context, federationID string) (*FederationEntry, error) {
	row := s.db.QueryRow(ctx,
		`SELECT federation_id, proposer_org_id, target_org_id, allowed_domains, allowed_depts,
			max_clearance, expires_at, requires_approval, status, created_height,
			approved_height, created_at, revoked_at
		FROM federations WHERE federation_id = $1`, federationID)

	f := &FederationEntry{}
	var maxClearance int16
	err := row.Scan(&f.FederationID, &f.ProposerOrgID, &f.TargetOrgID, &f.AllowedDomains, &f.AllowedDepts,
		&maxClearance, &f.ExpiresAt, &f.RequiresApproval, &f.Status, &f.CreatedHeight,
		&f.ApprovedHeight, &f.CreatedAt, &f.RevokedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("federation not found: %s", federationID)
		}
		return nil, fmt.Errorf("get federation: %w", err)
	}
	f.MaxClearance = ClearanceLevel(maxClearance) // #nosec G115 -- clearance is 0-4
	return f, nil
}

// ApproveFederation marks a federation as active with an approved height.
func (s *PostgresStore) ApproveFederation(ctx context.Context, federationID string, height int64) error {
	_, err := s.db.Exec(ctx,
		`UPDATE federations SET status = 'active', approved_height = $2
		WHERE federation_id = $1 AND status = 'proposed'`,
		federationID, height)
	if err != nil {
		return fmt.Errorf("approve federation: %w", err)
	}
	return nil
}

// RevokeFederation marks a federation as revoked.
func (s *PostgresStore) RevokeFederation(ctx context.Context, federationID string, height int64) error {
	_, err := s.db.Exec(ctx,
		`UPDATE federations SET status = 'revoked', revoked_at = NOW()
		WHERE federation_id = $1 AND status = 'active'`,
		federationID)
	if err != nil {
		return fmt.Errorf("revoke federation: %w", err)
	}
	return nil
}

// GetActiveFederations retrieves all active federations for an org (as proposer or target).
func (s *PostgresStore) GetActiveFederations(ctx context.Context, orgID string) ([]*FederationEntry, error) {
	rows, err := s.db.Query(ctx,
		`SELECT federation_id, proposer_org_id, target_org_id, allowed_domains, allowed_depts,
			max_clearance, expires_at, requires_approval, status, created_height,
			approved_height, created_at, revoked_at
		FROM federations
		WHERE (proposer_org_id = $1 OR target_org_id = $1) AND status IN ('active', 'proposed')
		ORDER BY created_at`, orgID)
	if err != nil {
		return nil, fmt.Errorf("get active federations: %w", err)
	}
	defer rows.Close()

	var feds []*FederationEntry
	for rows.Next() {
		f := &FederationEntry{}
		var maxClearance int16
		if scanErr := rows.Scan(&f.FederationID, &f.ProposerOrgID, &f.TargetOrgID, &f.AllowedDomains, &f.AllowedDepts,
			&maxClearance, &f.ExpiresAt, &f.RequiresApproval, &f.Status, &f.CreatedHeight,
			&f.ApprovedHeight, &f.CreatedAt, &f.RevokedAt); scanErr != nil {
			return nil, fmt.Errorf("scan federation: %w", scanErr)
		}
		f.MaxClearance = ClearanceLevel(maxClearance) // #nosec G115 -- clearance is 0-4
		feds = append(feds, f)
	}
	return feds, nil
}

// --- Department methods ---

// InsertDept inserts a department into PostgreSQL.
func (s *PostgresStore) InsertDept(ctx context.Context, dept *DeptEntry) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO departments (dept_id, org_id, dept_name, description, parent_dept, created_height)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT DO NOTHING`,
		dept.DeptID, dept.OrgID, dept.DeptName, dept.Description, dept.ParentDept, dept.CreatedHeight)
	if err != nil {
		return fmt.Errorf("insert dept: %w", err)
	}
	return nil
}

// GetDept retrieves a department by org and dept ID.
func (s *PostgresStore) GetDept(ctx context.Context, orgID, deptID string) (*DeptEntry, error) {
	row := s.db.QueryRow(ctx,
		`SELECT dept_id, org_id, dept_name, description, parent_dept, created_height, created_at
		FROM departments WHERE org_id = $1 AND dept_id = $2`, orgID, deptID)

	d := &DeptEntry{}
	var description, parentDept *string
	err := row.Scan(&d.DeptID, &d.OrgID, &d.DeptName, &description, &parentDept, &d.CreatedHeight, &d.CreatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("dept not found: %s/%s", orgID, deptID)
		}
		return nil, fmt.Errorf("get dept: %w", err)
	}
	if description != nil {
		d.Description = *description
	}
	if parentDept != nil {
		d.ParentDept = *parentDept
	}
	return d, nil
}

// GetOrgDepts retrieves all departments for an organization.
func (s *PostgresStore) GetOrgDepts(ctx context.Context, orgID string) ([]*DeptEntry, error) {
	rows, err := s.db.Query(ctx,
		`SELECT dept_id, org_id, dept_name, description, parent_dept, created_height, created_at
		FROM departments WHERE org_id = $1 ORDER BY dept_name`, orgID)
	if err != nil {
		return nil, fmt.Errorf("get org depts: %w", err)
	}
	defer rows.Close()

	var depts []*DeptEntry
	for rows.Next() {
		d := &DeptEntry{}
		var description, parentDept *string
		if scanErr := rows.Scan(&d.DeptID, &d.OrgID, &d.DeptName, &description, &parentDept, &d.CreatedHeight, &d.CreatedAt); scanErr != nil {
			return nil, fmt.Errorf("scan dept: %w", scanErr)
		}
		if description != nil {
			d.Description = *description
		}
		if parentDept != nil {
			d.ParentDept = *parentDept
		}
		depts = append(depts, d)
	}
	return depts, nil
}

// InsertDeptMember inserts a department member into PostgreSQL.
func (s *PostgresStore) InsertDeptMember(ctx context.Context, member *DeptMemberEntry) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO dept_members (org_id, dept_id, agent_id, clearance, role, created_height, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT DO NOTHING`,
		member.OrgID, member.DeptID, member.AgentID, int16(member.Clearance), member.Role, member.CreatedHeight, member.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert dept member: %w", err)
	}
	return nil
}

// RemoveDeptMember soft-deletes a department member.
func (s *PostgresStore) RemoveDeptMember(ctx context.Context, orgID, deptID, agentID string, height int64) error {
	_, err := s.db.Exec(ctx,
		`UPDATE dept_members SET removed_at = NOW()
		WHERE org_id = $1 AND dept_id = $2 AND agent_id = $3 AND removed_at IS NULL`,
		orgID, deptID, agentID)
	if err != nil {
		return fmt.Errorf("remove dept member: %w", err)
	}
	return nil
}

// GetDeptMembers retrieves all active members of a department.
func (s *PostgresStore) GetDeptMembers(ctx context.Context, orgID, deptID string) ([]*DeptMemberEntry, error) {
	rows, err := s.db.Query(ctx,
		`SELECT org_id, dept_id, agent_id, clearance, role, created_height, created_at
		FROM dept_members WHERE org_id = $1 AND dept_id = $2 AND removed_at IS NULL
		ORDER BY created_at`, orgID, deptID)
	if err != nil {
		return nil, fmt.Errorf("get dept members: %w", err)
	}
	defer rows.Close()

	var members []*DeptMemberEntry
	for rows.Next() {
		m := &DeptMemberEntry{}
		var clearance int16
		if scanErr := rows.Scan(&m.OrgID, &m.DeptID, &m.AgentID, &clearance, &m.Role, &m.CreatedHeight, &m.CreatedAt); scanErr != nil {
			return nil, fmt.Errorf("scan dept member: %w", scanErr)
		}
		m.Clearance = ClearanceLevel(clearance) // #nosec G115 -- clearance is 0-4
		members = append(members, m)
	}
	return members, nil
}

// UpdateDeptMemberClearance updates a department member's clearance level.
func (s *PostgresStore) UpdateDeptMemberClearance(ctx context.Context, orgID, deptID, agentID string, clearance ClearanceLevel) error {
	_, err := s.db.Exec(ctx,
		`UPDATE dept_members SET clearance = $4
		WHERE org_id = $1 AND dept_id = $2 AND agent_id = $3 AND removed_at IS NULL`,
		orgID, deptID, agentID, int16(clearance))
	if err != nil {
		return fmt.Errorf("update dept member clearance: %w", err)
	}
	return nil
}

// UpdateMemoryClassification updates a memory's classification level.
func (s *PostgresStore) UpdateMemoryClassification(ctx context.Context, memoryID string, classification ClearanceLevel) error {
	_, err := s.db.Exec(ctx,
		`UPDATE memories SET classification = $2 WHERE memory_id = $1`,
		memoryID, int16(classification))
	if err != nil {
		return fmt.Errorf("update memory classification: %w", err)
	}
	return nil
}

// GetMemoryClassificationLocal reads the off-chain mirror used by task and
// memory authorization checks. It intentionally does not fall back to a
// default: a missing row or unavailable mirror must fail closed.
func (s *PostgresStore) GetMemoryClassificationLocal(ctx context.Context, memoryID string) (int, error) {
	var classification int
	if err := s.db.QueryRow(ctx,
		`SELECT classification FROM memories WHERE memory_id = $1`, memoryID,
	).Scan(&classification); err != nil {
		return 0, fmt.Errorf("get memory classification: %w", err)
	}
	return classification, nil
}

// ListMemories returns memories matching the given filters with pagination.
func (s *PostgresStore) ListMemories(ctx context.Context, opts ListOptions) ([]*memory.MemoryRecord, int, error) {
	if opts.Limit <= 0 {
		opts.Limit = 50
	}

	countQuery := `SELECT COUNT(*) FROM memories WHERE 1=1`
	query := `SELECT memory_id, submitting_agent, content, content_hash,
		memory_type, domain_tag, provider, confidence_score, status, parent_hash, created_at,
		committed_at, deprecated_at FROM memories WHERE 1=1`
	var args []any
	argIdx := 1

	if opts.DomainTag != "" {
		filter := fmt.Sprintf(" AND domain_tag = $%d", argIdx)
		query += filter
		countQuery += filter
		args = append(args, opts.DomainTag)
		argIdx++
	}
	if opts.Provider != "" {
		filter := fmt.Sprintf(" AND (provider = $%d OR provider = '' OR memory_type = 'fact')", argIdx)
		query += filter
		countQuery += filter
		args = append(args, opts.Provider)
		argIdx++
	}
	if opts.Status != "" {
		filter := fmt.Sprintf(" AND status = $%d", argIdx)
		query += filter
		countQuery += filter
		args = append(args, opts.Status)
		argIdx++
	}
	if opts.SubmittingAgent != "" {
		filter := fmt.Sprintf(" AND submitting_agent = $%d", argIdx)
		query += filter
		countQuery += filter
		args = append(args, opts.SubmittingAgent)
		argIdx++
	}
	if len(opts.SubmittingAgents) > 0 {
		placeholders := make([]string, len(opts.SubmittingAgents))
		for i, a := range opts.SubmittingAgents {
			placeholders[i] = fmt.Sprintf("$%d", argIdx)
			args = append(args, a)
			argIdx++
		}
		filter := " AND submitting_agent IN (" + strings.Join(placeholders, ",") + ")"
		query += filter
		countQuery += filter
	}

	switch opts.Sort {
	case "oldest":
		query += " ORDER BY created_at ASC"
	case "confidence":
		query += " ORDER BY confidence_score DESC"
	default:
		query += " ORDER BY created_at DESC"
	}

	query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", argIdx, argIdx+1)
	queryArgs := append(args, opts.Limit, opts.Offset)

	var total int
	if err := s.db.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count memories: %w", err)
	}

	rows, err := s.db.Query(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("list memories: %w", err)
	}
	defer rows.Close()

	results := make([]*memory.MemoryRecord, 0)
	for rows.Next() {
		var r memory.MemoryRecord
		var mt, st string
		var parentHash *string
		if scanErr := rows.Scan(&r.MemoryID, &r.SubmittingAgent, &r.Content, &r.ContentHash,
			&mt, &r.DomainTag, &r.Provider, &r.ConfidenceScore, &st, &parentHash,
			&r.CreatedAt, &r.CommittedAt, &r.DeprecatedAt); scanErr != nil {
			return nil, 0, fmt.Errorf("scan memory: %w", scanErr)
		}
		r.MemoryType = memory.MemoryType(mt)
		r.Status = memory.MemoryStatus(st)
		if parentHash != nil {
			r.ParentHash = *parentHash
		}
		results = append(results, &r)
	}
	return results, total, nil
}

// GetStats returns aggregate statistics about stored memories.
func (s *PostgresStore) GetStats(ctx context.Context) (*StoreStats, error) {
	stats := &StoreStats{
		ByDomain: make(map[string]int),
		ByStatus: make(map[string]int),
	}

	if err := s.db.QueryRow(ctx, `SELECT COUNT(*) FROM memories`).Scan(&stats.TotalMemories); err != nil {
		return nil, fmt.Errorf("count total: %w", err)
	}

	rows, err := s.db.Query(ctx, `SELECT domain_tag, COUNT(*) FROM memories GROUP BY domain_tag`)
	if err != nil {
		return nil, fmt.Errorf("count by domain: %w", err)
	}
	for rows.Next() {
		var domain string
		var count int
		if scanErr := rows.Scan(&domain, &count); scanErr != nil {
			rows.Close()
			return nil, fmt.Errorf("scan domain count: %w", scanErr)
		}
		stats.ByDomain[domain] = count
	}
	rows.Close()

	rows, err = s.db.Query(ctx, `SELECT status, COUNT(*) FROM memories GROUP BY status`)
	if err != nil {
		return nil, fmt.Errorf("count by status: %w", err)
	}
	for rows.Next() {
		var status string
		var count int
		if scanErr := rows.Scan(&status, &count); scanErr != nil {
			rows.Close()
			return nil, fmt.Errorf("scan status count: %w", scanErr)
		}
		stats.ByStatus[status] = count
	}
	rows.Close()

	row := s.db.QueryRow(ctx, `SELECT MAX(created_at) FROM memories`)
	var lastActivity *time.Time
	if scanErr := row.Scan(&lastActivity); scanErr == nil {
		stats.LastActivity = lastActivity
	}

	return stats, nil
}

// GetTimeline returns memory counts aggregated by time periods.
func (s *PostgresStore) GetTimeline(ctx context.Context, from, to time.Time, domain string, bucket string) ([]TimelineBucket, error) {
	trunc := "day"
	switch bucket {
	case "hour":
		trunc = "hour"
	case "week":
		trunc = "week"
	case "month":
		trunc = "month"
	}

	query := fmt.Sprintf(`SELECT date_trunc('%s', created_at) AS period, COUNT(*)
		FROM memories WHERE created_at >= $1 AND created_at <= $2`, trunc)
	args := []any{from, to}
	argIdx := 3

	if domain != "" {
		query += fmt.Sprintf(" AND domain_tag = $%d", argIdx)
		args = append(args, domain)
	}

	query += ` GROUP BY period ORDER BY period`

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get timeline: %w", err)
	}
	defer rows.Close()

	buckets := make([]TimelineBucket, 0)
	for rows.Next() {
		var period time.Time
		var count int
		if scanErr := rows.Scan(&period, &count); scanErr != nil {
			return nil, fmt.Errorf("scan timeline: %w", scanErr)
		}
		buckets = append(buckets, TimelineBucket{
			Period: period.Format(time.RFC3339),
			Count:  count,
			Domain: domain,
		})
	}
	return buckets, nil
}

// DeleteMemory soft-deletes a memory by setting status to deprecated.
func (s *PostgresStore) DeleteMemory(ctx context.Context, memoryID string) error {
	_, err := s.db.Exec(ctx,
		`UPDATE memories SET status = 'deprecated', deprecated_at = NOW() WHERE memory_id = $1`,
		memoryID)
	if err != nil {
		return fmt.Errorf("delete memory: %w", err)
	}
	return nil
}

// UpdateDomainTag updates the domain tag of a memory.
func (s *PostgresStore) UpdateDomainTag(ctx context.Context, memoryID string, domain string) error {
	_, err := s.db.Exec(ctx,
		`UPDATE memories SET domain_tag = $2 WHERE memory_id = $1`,
		memoryID, domain)
	if err != nil {
		return fmt.Errorf("update domain tag: %w", err)
	}
	return nil
}

// SetTaskAssignee updates only the basic assignment fields. Dashboard callers
// use AssignTaskAndNotify so the transition and its durable notice are atomic.
func (s *PostgresStore) SetTaskAssignee(ctx context.Context, memoryID, assignee string) error {
	result, err := s.db.Exec(ctx, `UPDATE memories
		SET assignee = $2, task_picked_up_by = '', task_picked_up_at = NULL
		WHERE memory_id = $1 AND memory_type = 'task'`, memoryID, assignee)
	if err != nil {
		return fmt.Errorf("set task assignee: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("task not found: %s", memoryID)
	}
	return nil
}

func (s *PostgresStore) ClaimTask(ctx context.Context, memoryID, agentID string) (bool, error) {
	result, err := s.db.Exec(ctx, `UPDATE memories m
		SET assignee = $2, task_status = 'in_progress',
		    task_picked_up_by = CASE
		      WHEN COALESCE(m.assignee, '') = '' THEN $2
		      WHEN COALESCE(m.task_picked_up_by, '') = '' THEN $2
		      ELSE m.task_picked_up_by END,
		    task_picked_up_at = CASE
		      WHEN COALESCE(m.assignee, '') = '' OR m.task_picked_up_at IS NULL THEN NOW()
		      ELSE m.task_picked_up_at END
		FROM agents a
		WHERE m.memory_id = $1 AND m.memory_type = 'task'
		  AND m.task_status IN ('planned','in_progress')
		  AND m.task_requires_handoff = FALSE
		  AND m.assignee = $2
		  AND a.agent_id = $2 AND a.status = 'active' AND a.removed_at IS NULL`, memoryID, agentID)
	if err != nil {
		return false, fmt.Errorf("claim task: %w", err)
	}
	return result.RowsAffected() > 0, nil
}

func (s *PostgresStore) UpdateTaskStatus(ctx context.Context, memoryID string, taskStatus memory.TaskStatus) error {
	terminal := taskStatus == memory.TaskStatusDone || taskStatus == memory.TaskStatusDropped
	if terminal && s.pool != nil {
		return s.RunInTx(ctx, func(tx OffchainStore) error {
			return tx.(*PostgresStore).UpdateTaskStatus(ctx, memoryID, taskStatus)
		})
	}
	query := `UPDATE memories SET
		task_requires_handoff = CASE WHEN task_status IN ('done','dropped') THEN TRUE ELSE task_requires_handoff END,
		task_status = $2 WHERE memory_id = $1 AND memory_type = 'task'
		AND ($2 != 'in_progress' OR COALESCE(assignee, '') != '')`
	if terminal {
		query = `UPDATE memories SET task_status = $2,
			task_assignment_version = task_assignment_version + CASE WHEN COALESCE(assignee, '') != '' THEN 1 ELSE 0 END,
			assignee = '', task_requires_handoff = TRUE
			WHERE memory_id = $1 AND memory_type = 'task'`
	}
	result, err := s.db.Exec(ctx, query, memoryID, string(taskStatus))
	if err != nil {
		return fmt.Errorf("update task status: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("task not found or in_progress requires an assignee: %s", memoryID)
	}
	if terminal {
		if _, err := s.db.Exec(ctx, `UPDATE agent_notifications SET state = 'superseded'
			WHERE task_id = $1 AND state = 'unread'`, memoryID); err != nil {
			return fmt.Errorf("supersede terminal task notifications: %w", err)
		}
	}
	return nil
}

// AssignTaskAndNotify atomically updates assignment state and creates the
// generation-bound one-way inbox notice. Row locking makes A→B→A transitions
// monotonic under concurrent dashboard requests.
func (s *PostgresStore) AssignTaskAndNotify(ctx context.Context, memoryID, assignee string) (*TaskAssignmentResult, error) {
	if s.pool != nil {
		var out *TaskAssignmentResult
		err := s.RunInTx(ctx, func(tx OffchainStore) error {
			var innerErr error
			out, innerErr = tx.(*PostgresStore).AssignTaskAndNotify(ctx, memoryID, assignee)
			return innerErr
		})
		return out, err
	}
	if assignee != "" {
		var active int
		if err := s.db.QueryRow(ctx, `SELECT COUNT(*) FROM agents
			WHERE agent_id = $1 AND status = 'active' AND removed_at IS NULL`, assignee).Scan(&active); err != nil {
			return nil, fmt.Errorf("validate task assignee: %w", err)
		}
		if active != 1 {
			return nil, fmt.Errorf("choose an active registered agent")
		}
	}
	var current, domain, taskStatus, memoryStatus string
	var version int64
	err := s.db.QueryRow(ctx, `SELECT COALESCE(assignee, ''), task_assignment_version,
		domain_tag, COALESCE(task_status, ''), status
		FROM memories WHERE memory_id = $1 AND memory_type = 'task' FOR UPDATE`, memoryID).
		Scan(&current, &version, &domain, &taskStatus, &memoryStatus)
	if err == pgx.ErrNoRows {
		return nil, fmt.Errorf("task not found: %s", memoryID)
	}
	if err != nil {
		return nil, fmt.Errorf("read task assignment: %w", err)
	}
	if memoryStatus == string(memory.StatusDeprecated) || taskStatus == string(memory.TaskStatusDone) || taskStatus == string(memory.TaskStatusDropped) {
		return nil, fmt.Errorf("task is no longer open: %s", memoryID)
	}

	changed := current != assignee
	if changed {
		version++
		newStatus := taskStatus
		if assignee != "" {
			newStatus = string(memory.TaskStatusInProgress)
		}
		if _, err := s.db.Exec(ctx, `UPDATE memories SET assignee = $2,
			task_assignment_version = $3, task_status = $4,
			task_picked_up_by = '', task_picked_up_at = NULL,
			task_requires_handoff = CASE WHEN $2 != '' THEN FALSE ELSE task_requires_handoff END
			WHERE memory_id = $1 AND memory_type = 'task'`, memoryID, assignee, version, newStatus); err != nil {
			return nil, fmt.Errorf("update task assignment: %w", err)
		}
		taskStatus = newStatus
		if _, err := s.db.Exec(ctx, `UPDATE agent_notifications SET state = 'superseded'
			WHERE task_id = $1 AND state = 'unread'`, memoryID); err != nil {
			return nil, fmt.Errorf("supersede task notifications: %w", err)
		}
	} else if assignee != "" && version == 0 {
		version = 1
		if _, err := s.db.Exec(ctx, `UPDATE memories SET task_assignment_version = $2 WHERE memory_id = $1`, memoryID, version); err != nil {
			return nil, fmt.Errorf("backfill task assignment generation: %w", err)
		}
	}

	notificationCreated := false
	if assignee != "" {
		notificationID := fmt.Sprintf("task-assignment:%s:%d", memoryID, version)
		result, err := s.db.Exec(ctx, `INSERT INTO agent_notifications
			(notification_id, agent_id, kind, task_id, assignment_version, domain, title, state)
			VALUES ($1, $2, 'task_assignment', $3, $4, $5, 'A task was assigned to you', 'unread')
			ON CONFLICT DO NOTHING`, notificationID, assignee, memoryID, version, domain)
		if err != nil {
			return nil, fmt.Errorf("create task notification: %w", err)
		}
		notificationCreated = result.RowsAffected() == 1
	}
	return &TaskAssignmentResult{
		Changed: changed, Assignee: assignee, AssignmentVersion: version,
		TaskStatus: taskStatus, NotificationCreated: notificationCreated,
	}, nil
}

func (s *PostgresStore) CompleteTaskAsAgent(ctx context.Context, memoryID, agentID string, status memory.TaskStatus) (bool, error) {
	if status != memory.TaskStatusDone && status != memory.TaskStatusDropped {
		return false, fmt.Errorf("agent terminal status must be done or dropped")
	}
	if s.pool != nil {
		var completed bool
		err := s.RunInTx(ctx, func(tx OffchainStore) error {
			var innerErr error
			completed, innerErr = tx.(*PostgresStore).CompleteTaskAsAgent(ctx, memoryID, agentID, status)
			return innerErr
		})
		return completed, err
	}
	result, err := s.db.Exec(ctx, `UPDATE memories m SET task_status = $3,
		task_assignment_version = task_assignment_version + 1,
		assignee = '', task_requires_handoff = TRUE
		FROM agents a
		WHERE m.memory_id = $1 AND m.memory_type = 'task'
		  AND m.task_status IN ('planned','in_progress') AND m.assignee = $2
		  AND a.agent_id = $2 AND a.status = 'active' AND a.removed_at IS NULL`,
		memoryID, agentID, string(status))
	if err != nil {
		return false, fmt.Errorf("complete task as agent: %w", err)
	}
	if result.RowsAffected() == 0 {
		return false, nil
	}
	if _, err := s.db.Exec(ctx, `UPDATE agent_notifications SET state = 'superseded'
		WHERE task_id = $1 AND state = 'unread'`, memoryID); err != nil {
		return false, fmt.Errorf("supersede completed task notifications: %w", err)
	}
	return true, nil
}

func (s *PostgresStore) PeekAgentNotifications(ctx context.Context, agentID string, limit int) ([]*AgentNotification, error) {
	if limit <= 0 || limit > 20 {
		limit = 5
	}
	rows, err := s.db.Query(ctx, `SELECT n.notification_id, n.agent_id, n.kind, n.task_id,
		n.assignment_version, n.domain, n.title, n.state, n.created_at
		FROM agent_notifications n JOIN memories m ON m.memory_id = n.task_id
		WHERE n.agent_id = $1 AND n.state = 'unread'
		  AND m.memory_type = 'task' AND m.status != 'deprecated'
		  AND m.task_status IN ('planned','in_progress')
		  AND m.assignee = n.agent_id AND m.task_assignment_version = n.assignment_version
		ORDER BY n.created_at ASC LIMIT $2`, agentID, limit)
	if err != nil {
		return nil, fmt.Errorf("get agent notifications: %w", err)
	}
	defer rows.Close()
	items := make([]*AgentNotification, 0)
	for rows.Next() {
		var n AgentNotification
		if err := rows.Scan(&n.NotificationID, &n.AgentID, &n.Kind, &n.TaskID,
			&n.AssignmentVersion, &n.Domain, &n.Title, &n.State, &n.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan agent notification: %w", err)
		}
		items = append(items, &n)
	}
	return items, rows.Err()
}

func (s *PostgresStore) AcknowledgeAgentNotifications(ctx context.Context, agentID string, notificationIDs []string) ([]string, error) {
	if len(notificationIDs) == 0 {
		return []string{}, nil
	}
	if s.pool != nil {
		var acknowledged []string
		err := s.RunInTx(ctx, func(tx OffchainStore) error {
			var innerErr error
			acknowledged, innerErr = tx.(*PostgresStore).AcknowledgeAgentNotifications(ctx, agentID, notificationIDs)
			return innerErr
		})
		return acknowledged, err
	}
	acknowledged := make([]string, 0, len(notificationIDs))
	for _, notificationID := range notificationIDs {
		// Lock the task before touching its notice, matching assignment/completion's
		// memory→notification lock order. This makes the assignment generation at
		// acknowledgement a write-boundary fact, not a stale MVCC snapshot.
		var currentID string
		err := s.db.QueryRow(ctx, `SELECT n.notification_id
			FROM agent_notifications n
			JOIN memories m ON m.memory_id = n.task_id
			JOIN agents a ON a.agent_id = n.agent_id
			WHERE n.notification_id = $1 AND n.agent_id = $2 AND n.state = 'unread'
			  AND m.memory_type = 'task' AND m.status != 'deprecated'
			  AND m.task_status IN ('planned','in_progress')
			  AND m.assignee = n.agent_id AND m.task_assignment_version = n.assignment_version
			  AND a.status = 'active' AND a.removed_at IS NULL
			FOR UPDATE OF m`, notificationID, agentID).Scan(&currentID)
		if err == pgx.ErrNoRows {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("revalidate agent notification: %w", err)
		}
		result, err := s.db.Exec(ctx, `UPDATE agent_notifications SET state = 'read', read_at = NOW()
			WHERE notification_id = $1 AND agent_id = $2 AND state = 'unread'`, currentID, agentID)
		if err != nil {
			return nil, fmt.Errorf("acknowledge agent notification: %w", err)
		}
		if result.RowsAffected() == 1 {
			acknowledged = append(acknowledged, notificationID)
		}
	}
	return acknowledged, nil
}

func (s *PostgresStore) SupersedeAgentNotifications(ctx context.Context, agentID string, notificationIDs []string) error {
	for _, notificationID := range notificationIDs {
		if _, err := s.db.Exec(ctx, `UPDATE agent_notifications SET state = 'superseded'
			WHERE notification_id = $1 AND agent_id = $2 AND state = 'unread'`, notificationID, agentID); err != nil {
			return fmt.Errorf("supersede agent notification: %w", err)
		}
	}
	return nil
}

func (s *PostgresStore) TakeAgentNotifications(ctx context.Context, agentID string, limit int) ([]*AgentNotification, error) {
	items, err := s.PeekAgentNotifications(ctx, agentID, limit)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.NotificationID)
	}
	acknowledged, err := s.AcknowledgeAgentNotifications(ctx, agentID, ids)
	if err != nil {
		return nil, err
	}
	winners := make(map[string]bool, len(acknowledged))
	for _, id := range acknowledged {
		winners[id] = true
	}
	returned := items[:0]
	for _, item := range items {
		if winners[item.NotificationID] {
			item.State = "read"
			returned = append(returned, item)
		}
	}
	return returned, nil
}

var _ TaskAssignmentStore = (*PostgresStore)(nil)

// LinkMemories creates a link between two memories.
func (s *PostgresStore) LinkMemories(ctx context.Context, sourceID, targetID, linkType string) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO memory_links (source_id, target_id, link_type) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
		sourceID, targetID, linkType)
	if err != nil {
		return fmt.Errorf("link memories: %w", err)
	}
	return nil
}

// GetLinkedMemories returns all memories linked to the given memory ID.
func (s *PostgresStore) GetLinkedMemories(ctx context.Context, memoryID string) ([]memory.MemoryLink, error) {
	rows, err := s.db.Query(ctx,
		`SELECT source_id, target_id, link_type, created_at FROM memory_links WHERE source_id = $1 OR target_id = $1`,
		memoryID)
	if err != nil {
		return nil, fmt.Errorf("get linked memories: %w", err)
	}
	defer rows.Close()

	links := make([]memory.MemoryLink, 0)
	for rows.Next() {
		var l memory.MemoryLink
		var createdAt time.Time
		if err := rows.Scan(&l.SourceID, &l.TargetID, &l.LinkType, &createdAt); err != nil {
			return nil, fmt.Errorf("scan link: %w", err)
		}
		l.CreatedAt = createdAt.Format(time.RFC3339)
		links = append(links, l)
	}
	return links, rows.Err()
}

// GetCorroborationCounts returns the corroboration count for each memory ID in a
// single batched query (avoids the N+1 of GetCorroborations per memory).
func (s *PostgresStore) GetCorroborationCounts(ctx context.Context, memoryIDs []string) (map[string]int, error) {
	counts := make(map[string]int, len(memoryIDs))
	if len(memoryIDs) == 0 {
		return counts, nil
	}
	rows, err := s.db.Query(ctx,
		`SELECT memory_id, COUNT(*) FROM corroborations WHERE memory_id = ANY($1) GROUP BY memory_id`, memoryIDs)
	if err != nil {
		return nil, fmt.Errorf("get corroboration counts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			return nil, fmt.Errorf("scan corroboration count: %w", err)
		}
		counts[id] = n
	}
	return counts, rows.Err()
}

// GetLinksAmong returns typed links where BOTH endpoints are in memoryIDs, in one
// query (vs. one GetLinkedMemories per memory).
func (s *PostgresStore) GetLinksAmong(ctx context.Context, memoryIDs []string) ([]memory.MemoryLink, error) {
	links := make([]memory.MemoryLink, 0)
	if len(memoryIDs) == 0 {
		return links, nil
	}
	rows, err := s.db.Query(ctx,
		`SELECT source_id, target_id, link_type, created_at FROM memory_links WHERE source_id = ANY($1) AND target_id = ANY($1)`,
		memoryIDs)
	if err != nil {
		return nil, fmt.Errorf("get links among: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var l memory.MemoryLink
		var createdAt time.Time
		if err := rows.Scan(&l.SourceID, &l.TargetID, &l.LinkType, &createdAt); err != nil {
			return nil, fmt.Errorf("scan link: %w", err)
		}
		l.CreatedAt = createdAt.Format(time.RFC3339)
		links = append(links, l)
	}
	return links, rows.Err()
}

// GetOpenTasks returns all task memories that are planned or in_progress.
func (s *PostgresStore) GetOpenTasks(ctx context.Context, domain string, provider string, assignee string) ([]*memory.MemoryRecord, error) {
	query := `SELECT memory_id, submitting_agent, content, content_hash,
		memory_type, domain_tag, COALESCE(provider, ''), confidence_score, status, parent_hash, COALESCE(task_status, ''),
		COALESCE(assignee, ''), COALESCE(task_picked_up_by, ''), task_picked_up_at,
		created_at, committed_at, deprecated_at
		FROM memories
		WHERE memory_type = 'task'
		AND task_status IN ('planned', 'in_progress')
		AND status != 'deprecated'`
	args := []any{}
	argN := 1

	if domain != "" {
		query += fmt.Sprintf(" AND domain_tag = $%d", argN)
		args = append(args, domain)
		argN++
	}
	if assignee != "" {
		query += fmt.Sprintf(" AND assignee = $%d", argN)
		args = append(args, assignee)
		argN++
	} else if provider != "" {
		query += fmt.Sprintf(" AND (provider = $%d OR provider = '')", argN)
		args = append(args, provider)
		argN++
	}
	_ = argN
	query += " ORDER BY created_at DESC LIMIT 500"

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get open tasks: %w", err)
	}
	defer rows.Close()

	records := make([]*memory.MemoryRecord, 0)
	for rows.Next() {
		var r memory.MemoryRecord
		var mt, st, ts string
		var parentHash *string
		var pickedUpAt *time.Time
		if err := rows.Scan(&r.MemoryID, &r.SubmittingAgent, &r.Content, &r.ContentHash,
			&mt, &r.DomainTag, &r.Provider, &r.ConfidenceScore, &st, &parentHash, &ts,
			&r.Assignee, &r.TaskPickedUpBy, &pickedUpAt,
			&r.CreatedAt, &r.CommittedAt, &r.DeprecatedAt); err != nil {
			return nil, fmt.Errorf("scan task: %w", err)
		}
		r.MemoryType = memory.MemoryType(mt)
		r.Status = memory.MemoryStatus(st)
		r.TaskStatus = memory.TaskStatus(ts)
		r.TaskPickedUpAt = pickedUpAt
		if parentHash != nil {
			r.ParentHash = *parentHash
		}
		records = append(records, &r)
	}
	return records, rows.Err()
}

// GetAllTasks returns all task memories across all statuses for the Kanban board.
func (s *PostgresStore) GetAllTasks(ctx context.Context, domain string, limit int) ([]*memory.MemoryRecord, error) {
	if limit <= 0 {
		limit = 100
	} else if limit > 500 {
		limit = 500
	}
	query := `SELECT memory_id, submitting_agent, content, content_hash,
		memory_type, domain_tag, COALESCE(provider, ''), confidence_score, status, parent_hash, COALESCE(task_status, ''),
		COALESCE(assignee, ''), COALESCE(task_picked_up_by, ''), task_picked_up_at,
		created_at, committed_at, deprecated_at
		FROM memories
		WHERE memory_type = 'task' AND status != 'deprecated'`
	args := []any{}
	argN := 1
	if domain != "" {
		query += fmt.Sprintf(" AND domain_tag = $%d", argN)
		args = append(args, domain)
		argN++
	}
	query += ` ORDER BY CASE task_status
		WHEN 'in_progress' THEN 1
		WHEN 'planned' THEN 2
		WHEN 'done' THEN 3
		WHEN 'dropped' THEN 4
		ELSE 0 END, created_at DESC`
	query += fmt.Sprintf(" LIMIT $%d", argN)
	args = append(args, limit)

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get all tasks: %w", err)
	}
	defer rows.Close()
	records := make([]*memory.MemoryRecord, 0)
	for rows.Next() {
		var r memory.MemoryRecord
		var mt, st, ts string
		var parentHash *string
		var pickedUpAt *time.Time
		if err := rows.Scan(&r.MemoryID, &r.SubmittingAgent, &r.Content, &r.ContentHash,
			&mt, &r.DomainTag, &r.Provider, &r.ConfidenceScore, &st, &parentHash, &ts,
			&r.Assignee, &r.TaskPickedUpBy, &pickedUpAt,
			&r.CreatedAt, &r.CommittedAt, &r.DeprecatedAt); err != nil {
			return nil, fmt.Errorf("scan task: %w", err)
		}
		r.MemoryType = memory.MemoryType(mt)
		r.Status = memory.MemoryStatus(st)
		r.TaskStatus = memory.TaskStatus(ts)
		r.TaskPickedUpAt = pickedUpAt
		if parentHash != nil {
			r.ParentHash = *parentHash
		}
		records = append(records, &r)
	}
	return records, rows.Err()
}

// ---- Tag operations (stubs — Postgres uses enterprise deployment, tags are SQLite/personal) ----

func (s *PostgresStore) SetTags(_ context.Context, _ string, _ []string) error {
	return nil
}

func (s *PostgresStore) GetTags(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}

func (s *PostgresStore) GetTagsBatch(_ context.Context, _ []string) (map[string][]string, error) {
	return map[string][]string{}, nil
}

func (s *PostgresStore) ListAllTags(_ context.Context) ([]TagCount, error) {
	return nil, nil
}

func (s *PostgresStore) ListMemoriesByTag(_ context.Context, _ string, _, _ int) ([]*memory.MemoryRecord, int, error) {
	return nil, 0, nil
}

func (s *PostgresStore) FindByContentHash(_ context.Context, _ string) (bool, error) {
	return false, nil // TODO: implement for postgres
}

// RepairSelfDupRejected is a no-op for postgres: the dedup self-match bug it
// repairs never fired here (FindByContentHash is an always-false stub on this
// backend), and the repair is gated single-node anyway — postgres backs
// multi-node production daemons.
func (s *PostgresStore) RepairSelfDupRejected(_ context.Context, _ string, _ func(memoryID string) error) (int, error) {
	return 0, nil
}

func (s *PostgresStore) Close() error {
	if s.pool != nil {
		s.pool.Close()
	}
	return nil
}

// Ping checks the database connection.
func (s *PostgresStore) Ping(ctx context.Context) error {
	if s.pool != nil {
		return s.pool.Ping(ctx)
	}
	return nil
}

// RunInTx executes fn within a PostgreSQL transaction. All writes through
// the tx-scoped OffchainStore are atomic — either all succeed or all roll back.
func (s *PostgresStore) RunInTx(ctx context.Context, fn func(tx OffchainStore) error) error {
	if s.pool == nil {
		// Already in a transaction — execute directly.
		return fn(s)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	txStore := &PostgresStore{db: tx}
	if err := fn(txStore); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// --- AgentStore (mirrors SQLiteStore against the agents table) ---

// agentColumns is the SELECT projection shared by ListAgents / GetAgent /
// GetAgentByName. memory_count is derived live from the memories table (cast to
// int4 to scan into AgentEntry.MemoryCount). Keep the order in lockstep with
// scanAgent.
const agentColumns = `
	a.agent_id, a.name, a.registered_name, a.role, a.avatar, a.boot_bio,
	a.validator_pubkey, a.node_id, a.p2p_address, a.status, a.clearance,
	a.org_id, a.dept_id, a.domain_access, a.bundle_path,
	a.first_seen, a.last_seen, a.created_at, a.removed_at,
	a.on_chain_height, a.visible_agents, a.provider,
	COALESCE((SELECT COUNT(*) FROM memories WHERE submitting_agent = a.agent_id), 0)::int,
	a.claim_token, a.claim_expires_at`

// scanAgent reads one agents row in agentColumns order. Satisfied by both
// pgx.Row (QueryRow) and pgx.Rows (after Next).
func scanAgent(row interface{ Scan(...any) error }) (*AgentEntry, error) {
	a := &AgentEntry{}
	if err := row.Scan(
		&a.AgentID, &a.Name, &a.RegisteredName, &a.Role, &a.Avatar, &a.BootBio,
		&a.ValidatorPubkey, &a.NodeID, &a.P2PAddress, &a.Status, &a.Clearance,
		&a.OrgID, &a.DeptID, &a.DomainAccess, &a.BundlePath,
		&a.FirstSeen, &a.LastSeen, &a.CreatedAt, &a.RemovedAt,
		&a.OnChainHeight, &a.VisibleAgents, &a.Provider, &a.MemoryCount,
		&a.ClaimToken, &a.ClaimExpiresAt,
	); err != nil {
		return nil, err
	}
	if a.RegisteredName == "" {
		a.RegisteredName = a.Name // backfill for pre-existing agents
	}
	return a, nil
}

func (s *PostgresStore) ListAgents(ctx context.Context) ([]*AgentEntry, error) {
	rows, err := s.db.Query(ctx, `SELECT `+agentColumns+`
		FROM agents a WHERE a.status != 'removed' ORDER BY a.created_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("list agents: %w", err)
	}
	defer rows.Close()

	var agents []*AgentEntry
	for rows.Next() {
		a, scanErr := scanAgent(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan agent: %w", scanErr)
		}
		agents = append(agents, a)
	}
	return agents, rows.Err()
}

func (s *PostgresStore) GetAgent(ctx context.Context, agentID string) (*AgentEntry, error) {
	a, err := scanAgent(s.db.QueryRow(ctx, `SELECT `+agentColumns+`
		FROM agents a WHERE a.agent_id = $1`, agentID))
	if err != nil {
		return nil, fmt.Errorf("get agent: %w", err)
	}
	return a, nil
}

func (s *PostgresStore) GetAgentByName(ctx context.Context, name string) (*AgentEntry, error) {
	a, err := scanAgent(s.db.QueryRow(ctx, `SELECT `+agentColumns+`
		FROM agents a WHERE a.name = $1 AND a.status != 'removed'`, name))
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil // not found — return nil, nil per interface contract
		}
		return nil, fmt.Errorf("get agent by name: %w", err)
	}
	return a, nil
}

func (s *PostgresStore) CreateAgent(ctx context.Context, agent *AgentEntry) error {
	now := time.Now().UTC()
	firstSeen := now
	if agent.FirstSeen != nil {
		firstSeen = *agent.FirstSeen
	}
	createdAt := now
	if !agent.CreatedAt.IsZero() {
		createdAt = agent.CreatedAt
	}
	// ON CONFLICT DO NOTHING — NOT a bare INSERT. The agent_register flush path
	// relies on a conflict signalling "already exists" so it can fall back to
	// UpdateAgent. A raw PK-conflict error would abort the surrounding pgx
	// transaction (the flush runs inside one), and the fallback UpdateAgent would
	// then fail with "current transaction is aborted" and panic the node on any
	// re-registration or block replay. DO NOTHING resolves the conflict without
	// poisoning the tx; we surface it via RowsAffected()==0 so the caller still
	// falls back to UpdateAgent.
	tag, err := s.db.Exec(ctx, `
		INSERT INTO agents (agent_id, name, registered_name, role, avatar, boot_bio, validator_pubkey,
			node_id, p2p_address, status, clearance, org_id, dept_id, domain_access, bundle_path,
			on_chain_height, visible_agents, provider, claim_token, claim_expires_at,
			first_seen, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
		ON CONFLICT (agent_id) DO NOTHING`,
		agent.AgentID, agent.Name, agent.RegisteredName, agent.Role, agent.Avatar, agent.BootBio, agent.ValidatorPubkey,
		agent.NodeID, agent.P2PAddress, agent.Status, agent.Clearance, agent.OrgID, agent.DeptID,
		agent.DomainAccess, agent.BundlePath, agent.OnChainHeight, agent.VisibleAgents, agent.Provider,
		agent.ClaimToken, agent.ClaimExpiresAt, firstSeen, createdAt)
	if err != nil {
		return fmt.Errorf("create agent: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("create agent: agent %s already exists", agent.AgentID)
	}
	return nil
}

func (s *PostgresStore) UpdateAgent(ctx context.Context, agent *AgentEntry) error {
	// Mirrors SQLite: registered_name, validator_pubkey, node_id, status,
	// first_seen and created_at are intentionally not overwritten here.
	_, err := s.db.Exec(ctx, `
		UPDATE agents SET name=$1, role=$2, avatar=$3, boot_bio=$4, clearance=$5,
			org_id=$6, dept_id=$7, domain_access=$8, p2p_address=$9,
			on_chain_height=$10, visible_agents=$11, provider=$12,
			claim_token=$13, claim_expires_at=$14
		WHERE agent_id=$15`,
		agent.Name, agent.Role, agent.Avatar, agent.BootBio, agent.Clearance,
		agent.OrgID, agent.DeptID, agent.DomainAccess, agent.P2PAddress,
		agent.OnChainHeight, agent.VisibleAgents, agent.Provider,
		agent.ClaimToken, agent.ClaimExpiresAt, agent.AgentID)
	if err != nil {
		return fmt.Errorf("update agent: %w", err)
	}
	return nil
}

func (s *PostgresStore) RemoveAgent(ctx context.Context, agentID string) error {
	_, err := s.db.Exec(ctx, `UPDATE agents SET status='removed', removed_at=NOW() WHERE agent_id=$1`, agentID)
	if err != nil {
		return fmt.Errorf("remove agent: %w", err)
	}
	return nil
}

func (s *PostgresStore) UpdateAgentStatus(ctx context.Context, agentID, status string) error {
	_, err := s.db.Exec(ctx, `UPDATE agents SET status=$1 WHERE agent_id=$2`, status, agentID)
	if err != nil {
		return fmt.Errorf("update agent status: %w", err)
	}
	return nil
}

func (s *PostgresStore) UpdateAgentLastSeen(ctx context.Context, agentID string, lastSeen time.Time) error {
	_, err := s.db.Exec(ctx, `UPDATE agents SET last_seen=$1, status='active' WHERE agent_id=$2`, lastSeen.UTC(), agentID)
	if err != nil {
		return fmt.Errorf("update agent last seen: %w", err)
	}
	return nil
}

func (s *PostgresStore) BackfillFirstSeen(ctx context.Context, agentID string, firstSeen time.Time) error {
	_, err := s.db.Exec(ctx, `UPDATE agents SET first_seen=$1 WHERE agent_id=$2 AND first_seen IS NULL`, firstSeen.UTC(), agentID)
	if err != nil {
		return fmt.Errorf("backfill first_seen: %w", err)
	}
	return nil
}

func (s *PostgresStore) RotateAgentKey(ctx context.Context, oldAgentID string) (string, []byte, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", nil, fmt.Errorf("generate key: %w", err)
	}
	newAgentID := hex.EncodeToString(pub)
	newValidatorPubkey := base64.StdEncoding.EncodeToString(pub)
	seed := priv.Seed()

	// Atomic: new agent row + memory re-attribution + retire old. No
	// redeployment_log write — that table is SQLite/personal-mode only.
	err = s.RunInTx(ctx, func(tx OffchainStore) error {
		ps := tx.(*PostgresStore)

		// 1. Verify old agent exists and is not removed.
		var status string
		if scanErr := ps.db.QueryRow(ctx, `SELECT status FROM agents WHERE agent_id=$1`, oldAgentID).Scan(&status); scanErr != nil {
			return fmt.Errorf("agent not found: %s", oldAgentID)
		}
		if status == "removed" {
			return fmt.Errorf("cannot rotate key for removed agent %s", oldAgentID)
		}

		// 2. Insert new agent row (copy of old, with new keys). Mirrors the
		// SQLite column subset — uncopied columns take table defaults.
		if _, err2 := ps.db.Exec(ctx, `
			INSERT INTO agents (agent_id, name, role, avatar, boot_bio, validator_pubkey,
				node_id, p2p_address, status, clearance, org_id, dept_id, domain_access, bundle_path, first_seen, created_at)
			SELECT $1, name, role, avatar, boot_bio, $2,
				node_id, p2p_address, status, clearance, org_id, dept_id, domain_access, '',
				first_seen, created_at
			FROM agents WHERE agent_id=$3`,
			newAgentID, newValidatorPubkey, oldAgentID); err2 != nil {
			return fmt.Errorf("insert rotated agent: %w", err2)
		}

		// 3. Re-attribute all memories to the new agent ID.
		if _, err2 := ps.db.Exec(ctx, `UPDATE memories SET submitting_agent=$1 WHERE submitting_agent=$2`, newAgentID, oldAgentID); err2 != nil {
			return fmt.Errorf("re-attribute memories: %w", err2)
		}

		// 4. Retire the old agent.
		if _, err2 := ps.db.Exec(ctx, `UPDATE agents SET status='removed', removed_at=NOW() WHERE agent_id=$1`, oldAgentID); err2 != nil {
			return fmt.Errorf("retire old agent: %w", err2)
		}
		return nil
	})
	if err != nil {
		return "", nil, err
	}
	return newAgentID, seed, nil
}

func (s *PostgresStore) ReassignMemories(ctx context.Context, sourceAgentID, targetAgentID string) (int64, error) {
	var count int64
	err := s.RunInTx(ctx, func(tx OffchainStore) error {
		ps := tx.(*PostgresStore)

		var status string
		if scanErr := ps.db.QueryRow(ctx, `SELECT status FROM agents WHERE agent_id=$1`, targetAgentID).Scan(&status); scanErr != nil {
			return fmt.Errorf("target agent not found: %s", targetAgentID)
		}
		if status == "removed" {
			return fmt.Errorf("cannot reassign to removed agent %s", targetAgentID)
		}
		if scanErr := ps.db.QueryRow(ctx, `SELECT COUNT(*) FROM memories WHERE submitting_agent=$1`, sourceAgentID).Scan(&count); scanErr != nil {
			return fmt.Errorf("count source memories: %w", scanErr)
		}
		if count == 0 {
			return nil
		}
		if _, execErr := ps.db.Exec(ctx, `UPDATE memories SET submitting_agent=$1 WHERE submitting_agent=$2`, targetAgentID, sourceAgentID); execErr != nil {
			return fmt.Errorf("reassign memories: %w", execErr)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *PostgresStore) ListAgentTags(_ context.Context, _ string) ([]TagCount, error) {
	// Tags are a SQLite/personal-mode feature; Postgres deployments carry none.
	return nil, nil
}

func (s *PostgresStore) ListAgentDomains(ctx context.Context, agentID string) ([]string, error) {
	rows, err := s.db.Query(ctx, `
		SELECT domain_tag FROM memories
		WHERE submitting_agent = $1 AND domain_tag != ''
		GROUP BY domain_tag
		ORDER BY COUNT(*) DESC, domain_tag ASC`, agentID)
	if err != nil {
		return nil, fmt.Errorf("list agent domains: %w", err)
	}
	defer rows.Close()

	var domains []string
	for rows.Next() {
		var domain string
		if scanErr := rows.Scan(&domain); scanErr != nil {
			return nil, fmt.Errorf("scan agent domain: %w", scanErr)
		}
		domains = append(domains, domain)
	}
	return domains, rows.Err()
}

func (s *PostgresStore) AcquireRedeployLock(_ context.Context, _, _ string, _ time.Duration) error {
	return fmt.Errorf("AcquireRedeployLock not implemented for PostgresStore")
}

func (s *PostgresStore) ReleaseRedeployLock(_ context.Context) error {
	return fmt.Errorf("ReleaseRedeployLock not implemented for PostgresStore")
}

func (s *PostgresStore) GetRedeployLock(_ context.Context) (*RedeploymentLock, error) {
	return nil, fmt.Errorf("GetRedeployLock not implemented for PostgresStore")
}

func (s *PostgresStore) InsertRedeployLog(_ context.Context, _ *RedeploymentLogEntry) error {
	return fmt.Errorf("InsertRedeployLog not implemented for PostgresStore")
}

func (s *PostgresStore) GetRedeployLog(_ context.Context, _ string) ([]*RedeploymentLogEntry, error) {
	return nil, fmt.Errorf("GetRedeployLog not implemented for PostgresStore")
}

func (s *PostgresStore) GetLatestRedeployLog(_ context.Context) (*RedeploymentLogEntry, error) {
	return nil, fmt.Errorf("GetLatestRedeployLog not implemented for PostgresStore")
}

func (s *PostgresStore) UpdateRedeployLog(_ context.Context, _ int64, _, _ string) error {
	return fmt.Errorf("UpdateRedeployLog not implemented for PostgresStore")
}

func (s *PostgresStore) ClearStaleRedeployLogs(_ context.Context) (int, error) {
	return 0, fmt.Errorf("ClearStaleRedeployLogs not implemented for PostgresStore")
}

// --- Pipeline Store stubs (SQLite-only feature for now) ---

func (s *PostgresStore) InsertPipeline(_ context.Context, _ *PipelineMessage) error {
	return fmt.Errorf("InsertPipeline not implemented for PostgresStore")
}

func (s *PostgresStore) GetPipeline(_ context.Context, _ string) (*PipelineMessage, error) {
	return nil, fmt.Errorf("GetPipeline not implemented for PostgresStore")
}

func (s *PostgresStore) GetInbox(_ context.Context, _, _ string, _ int) ([]*PipelineMessage, error) {
	return nil, fmt.Errorf("GetInbox not implemented for PostgresStore")
}

func (s *PostgresStore) ClaimPipeline(_ context.Context, _, _ string) error {
	return fmt.Errorf("ClaimPipeline not implemented for PostgresStore")
}

func (s *PostgresStore) CompletePipeline(_ context.Context, _, _, _, _ string) error {
	return fmt.Errorf("CompletePipeline not implemented for PostgresStore")
}

func (s *PostgresStore) GetCompletedForSender(_ context.Context, _ string, _ int) ([]*PipelineMessage, error) {
	return nil, fmt.Errorf("GetCompletedForSender not implemented for PostgresStore")
}

func (s *PostgresStore) ListPipelines(_ context.Context, _ string, _ int) ([]*PipelineMessage, error) {
	return nil, fmt.Errorf("ListPipelines not implemented for PostgresStore")
}

func (s *PostgresStore) PipelineStats(_ context.Context) (map[string]int, error) {
	return nil, fmt.Errorf("PipelineStats not implemented for PostgresStore")
}

func (s *PostgresStore) ExpirePipelines(_ context.Context) (int, error) {
	return 0, fmt.Errorf("ExpirePipelines not implemented for PostgresStore")
}

func (s *PostgresStore) ExpireStalePipelines(_ context.Context, _ time.Time) (int, error) {
	return 0, fmt.Errorf("ExpireStalePipelines not implemented for PostgresStore")
}

func (s *PostgresStore) PurgePipelines(_ context.Context, _ time.Time) (int, error) {
	return 0, fmt.Errorf("PurgePipelines not implemented for PostgresStore")
}

// --- GovernanceStore (mirrors SQLiteStore against the governance_* tables) ---

// govProposalColumns is the SELECT projection shared by GetGovProposal and
// ListGovProposals; keep in lockstep with scanGovProposal.
const govProposalColumns = `proposal_id, operation, target_agent_id, target_pubkey,
	target_power, proposer_id, status, created_height, expiry_height,
	executed_height, reason, created_at`

// scanGovProposal reads one governance_proposals row in govProposalColumns
// order. created_at is rendered as RFC3339 to match the SQLite store's string.
func scanGovProposal(row interface{ Scan(...any) error }) (*GovProposal, error) {
	var p GovProposal
	var createdAt time.Time
	if err := row.Scan(&p.ProposalID, &p.Operation, &p.TargetAgentID, &p.TargetPubkey,
		&p.TargetPower, &p.ProposerID, &p.Status, &p.CreatedHeight, &p.ExpiryHeight,
		&p.ExecutedHeight, &p.Reason, &createdAt); err != nil {
		return nil, err
	}
	p.CreatedAt = createdAt.UTC().Format(time.RFC3339Nano)
	return &p, nil
}

// InsertGovProposal mirrors a governance proposal. ON CONFLICT DO NOTHING + a nil
// return keeps the abci flush replay-safe: a duplicate proposal_id on block
// replay must not error, which would abort the flush transaction and panic the
// node. Status changes flow through UpdateGovProposalStatus, so the original row
// is never overwritten here.
func (s *PostgresStore) InsertGovProposal(ctx context.Context, p *GovProposal) error {
	_, err := s.db.Exec(ctx, `
		INSERT INTO governance_proposals (proposal_id, operation, target_agent_id, target_pubkey,
			target_power, proposer_id, status, created_height, expiry_height, executed_height, reason)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (proposal_id) DO NOTHING`,
		p.ProposalID, p.Operation, p.TargetAgentID, p.TargetPubkey,
		p.TargetPower, p.ProposerID, p.Status, p.CreatedHeight,
		p.ExpiryHeight, p.ExecutedHeight, p.Reason)
	if err != nil {
		return fmt.Errorf("insert gov proposal: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetGovProposal(ctx context.Context, proposalID string) (*GovProposal, error) {
	p, err := scanGovProposal(s.db.QueryRow(ctx, `SELECT `+govProposalColumns+`
		FROM governance_proposals WHERE proposal_id = $1`, proposalID))
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("gov proposal not found: %s", proposalID)
		}
		return nil, fmt.Errorf("get gov proposal: %w", err)
	}
	return p, nil
}

func (s *PostgresStore) UpdateGovProposalStatus(ctx context.Context, proposalID, status string, executedHeight *int64) error {
	_, err := s.db.Exec(ctx, `
		UPDATE governance_proposals SET status = $1, executed_height = $2
		WHERE proposal_id = $3`, status, executedHeight, proposalID)
	if err != nil {
		return fmt.Errorf("update gov proposal status: %w", err)
	}
	return nil
}

func (s *PostgresStore) ListGovProposals(ctx context.Context, status string) ([]*GovProposal, error) {
	query := `SELECT ` + govProposalColumns + ` FROM governance_proposals`
	var args []any
	if status != "" {
		query += ` WHERE status = $1`
		args = append(args, status)
	}
	query += ` ORDER BY created_height DESC`

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list gov proposals: %w", err)
	}
	defer rows.Close()

	proposals := make([]*GovProposal, 0)
	for rows.Next() {
		p, scanErr := scanGovProposal(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan gov proposal: %w", scanErr)
		}
		proposals = append(proposals, p)
	}
	return proposals, rows.Err()
}

// InsertGovVote mirrors SQLite's INSERT OR REPLACE: a validator may revise its
// vote and replays must be idempotent, so upsert on the (proposal_id,
// validator_id) PK rather than erroring on conflict.
func (s *PostgresStore) InsertGovVote(ctx context.Context, v *GovVote) error {
	_, err := s.db.Exec(ctx, `
		INSERT INTO governance_votes (proposal_id, validator_id, decision, height)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (proposal_id, validator_id) DO UPDATE SET
			decision = EXCLUDED.decision, height = EXCLUDED.height`,
		v.ProposalID, v.ValidatorID, v.Decision, v.Height)
	if err != nil {
		return fmt.Errorf("insert gov vote: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetGovVotes(ctx context.Context, proposalID string) ([]*GovVote, error) {
	rows, err := s.db.Query(ctx, `
		SELECT proposal_id, validator_id, decision, height
		FROM governance_votes WHERE proposal_id = $1 ORDER BY validator_id`, proposalID)
	if err != nil {
		return nil, fmt.Errorf("get gov votes: %w", err)
	}
	defer rows.Close()

	votes := make([]*GovVote, 0)
	for rows.Next() {
		var v GovVote
		if scanErr := rows.Scan(&v.ProposalID, &v.ValidatorID, &v.Decision, &v.Height); scanErr != nil {
			return nil, fmt.Errorf("scan gov vote: %w", scanErr)
		}
		votes = append(votes, &v)
	}
	return votes, rows.Err()
}
