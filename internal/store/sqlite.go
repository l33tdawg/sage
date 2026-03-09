package store

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite" // Pure Go SQLite driver

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/vault"
)

// sqlQuerier is satisfied by both *sql.DB and *sql.Tx, allowing
// SQLiteStore methods to work inside or outside a transaction.
type sqlQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// SQLiteStore implements MemoryStore, ValidatorScoreStore, AccessStore, and OrgStore using SQLite.
type SQLiteStore struct {
	conn   sqlQuerier // either *sql.DB or *sql.Tx
	db     *sql.DB    // nil for tx-scoped stores
	dbPath string
	vault  *vault.Vault // nil = no encryption
}

// encPrefix marks content as encrypted (prepended to base64 ciphertext).
const encPrefix = "enc::"

// SetVault attaches an encryption vault to the store.
// When set, memory content is encrypted on write and decrypted on read.
func (s *SQLiteStore) SetVault(v *vault.Vault) {
	s.vault = v
}

// encryptContent encrypts a string if the vault is set.
// Returns the original string if no vault.
func (s *SQLiteStore) encryptContent(plaintext string) (string, error) {
	if s.vault == nil {
		return plaintext, nil
	}
	encrypted, err := s.vault.EncryptString(plaintext)
	if err != nil {
		return "", fmt.Errorf("encrypt content: %w", err)
	}
	return encPrefix + base64.StdEncoding.EncodeToString(encrypted), nil
}

// decryptContent decrypts a string if it's encrypted.
// Returns the original string if not encrypted or no vault.
func (s *SQLiteStore) decryptContent(stored string) (string, error) {
	if !strings.HasPrefix(stored, encPrefix) {
		return stored, nil // not encrypted
	}
	if s.vault == nil {
		return "[encrypted — vault locked]", nil
	}
	data, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(stored, encPrefix))
	if err != nil {
		return "", fmt.Errorf("decode encrypted content: %w", err)
	}
	return s.vault.DecryptString(data)
}

// encryptEmbedding encrypts embedding bytes if the vault is set.
func (s *SQLiteStore) encryptEmbedding(data []byte) ([]byte, error) {
	if s.vault == nil || data == nil {
		return data, nil
	}
	return s.vault.Encrypt(data)
}

// decryptEmbedding decrypts embedding bytes if vault is set and data looks encrypted.
// Encrypted embeddings are longer than raw ones (nonce + tag overhead).
func (s *SQLiteStore) decryptEmbedding(data []byte) ([]byte, error) {
	if s.vault == nil || data == nil {
		return data, nil
	}
	// Try to decrypt — if it fails, it's likely unencrypted legacy data.
	decrypted, err := s.vault.Decrypt(data)
	if err != nil {
		return data, nil // return as-is for backward compatibility
	}
	return decrypted, nil
}

// NewSQLiteStore creates a new SQLite-backed store.
func NewSQLiteStore(ctx context.Context, dbPath string) (*SQLiteStore, error) {
	dsn := dbPath + "?_journal_mode=WAL&_busy_timeout=5000&_synchronous=NORMAL&_foreign_keys=ON"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	s := &SQLiteStore{conn: db, db: db, dbPath: dbPath}
	if err := s.initSchema(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}

	return s, nil
}

func (s *SQLiteStore) initSchema(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS memories (
		memory_id        TEXT PRIMARY KEY,
		submitting_agent TEXT NOT NULL,
		content          TEXT NOT NULL,
		content_hash     BLOB NOT NULL,
		embedding        BLOB,
		embedding_hash   BLOB,
		memory_type      TEXT NOT NULL CHECK (memory_type IN ('fact', 'observation', 'inference')),
		domain_tag       TEXT NOT NULL,
		confidence_score REAL NOT NULL CHECK (confidence_score BETWEEN 0 AND 1),
		status           TEXT NOT NULL DEFAULT 'proposed',
		parent_hash      TEXT,
		classification   INTEGER NOT NULL DEFAULT 1,
		created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		committed_at     TEXT,
		deprecated_at    TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_memories_domain ON memories(domain_tag);
	CREATE INDEX IF NOT EXISTS idx_memories_status ON memories(status);

	CREATE TABLE IF NOT EXISTS knowledge_triples (
		id        INTEGER PRIMARY KEY AUTOINCREMENT,
		memory_id TEXT REFERENCES memories(memory_id),
		subject   TEXT NOT NULL,
		predicate TEXT NOT NULL,
		object    TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS validation_votes (
		id             INTEGER PRIMARY KEY AUTOINCREMENT,
		memory_id      TEXT REFERENCES memories(memory_id),
		validator_id   TEXT NOT NULL,
		decision       TEXT NOT NULL CHECK (decision IN ('accept', 'reject', 'abstain')),
		rationale      TEXT,
		weight_at_vote REAL,
		block_height   INTEGER,
		created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_validation_votes_memory_validator
		ON validation_votes(memory_id, validator_id);

	CREATE TABLE IF NOT EXISTS corroborations (
		id         INTEGER PRIMARY KEY AUTOINCREMENT,
		memory_id  TEXT REFERENCES memories(memory_id),
		agent_id   TEXT NOT NULL,
		evidence   TEXT,
		created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	);

	CREATE TABLE IF NOT EXISTS challenges (
		id             INTEGER PRIMARY KEY AUTOINCREMENT,
		memory_id      TEXT NOT NULL REFERENCES memories(memory_id),
		challenger_id  TEXT NOT NULL,
		reason         TEXT NOT NULL,
		evidence       TEXT,
		block_height   INTEGER,
		created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	);
	CREATE INDEX IF NOT EXISTS idx_challenges_memory ON challenges(memory_id);

	CREATE TABLE IF NOT EXISTS validator_scores (
		validator_id   TEXT PRIMARY KEY,
		weighted_sum   REAL NOT NULL DEFAULT 0,
		weight_denom   REAL NOT NULL DEFAULT 0,
		vote_count     INTEGER NOT NULL DEFAULT 0,
		expertise_vec  TEXT NOT NULL DEFAULT '[]',
		last_active_ts TEXT,
		current_weight REAL NOT NULL DEFAULT 0,
		updated_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	);

	CREATE TABLE IF NOT EXISTS epoch_scores (
		epoch_num         INTEGER NOT NULL,
		block_height      INTEGER NOT NULL,
		validator_id      TEXT NOT NULL,
		accuracy          REAL NOT NULL,
		domain_score      REAL NOT NULL,
		recency_score     REAL NOT NULL,
		corr_score        REAL NOT NULL,
		raw_weight        REAL NOT NULL,
		capped_weight     REAL NOT NULL,
		normalized_weight REAL NOT NULL,
		PRIMARY KEY (epoch_num, validator_id)
	);

	CREATE TABLE IF NOT EXISTS domains (
		domain_tag  TEXT PRIMARY KEY,
		description TEXT,
		decay_rate  REAL NOT NULL DEFAULT 0.005,
		created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	);

	CREATE TABLE IF NOT EXISTS agents (
		agent_id      TEXT PRIMARY KEY,
		display_name  TEXT,
		organization  TEXT,
		domains       TEXT,
		registered_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	);

	CREATE TABLE IF NOT EXISTS domain_registry (
		domain_name    TEXT PRIMARY KEY,
		owner_agent_id TEXT NOT NULL,
		parent_domain  TEXT,
		description    TEXT,
		created_height INTEGER NOT NULL,
		created_at     TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	);

	CREATE TABLE IF NOT EXISTS access_grants (
		id             INTEGER PRIMARY KEY AUTOINCREMENT,
		domain         TEXT NOT NULL,
		grantee_id     TEXT NOT NULL,
		granter_id     TEXT NOT NULL,
		access_level   INTEGER NOT NULL DEFAULT 1,
		expires_at     TEXT,
		revoked_at     TEXT,
		created_height INTEGER NOT NULL,
		created_at     TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		UNIQUE(domain, grantee_id, created_height)
	);
	CREATE INDEX IF NOT EXISTS idx_access_grants_grantee ON access_grants(grantee_id) WHERE revoked_at IS NULL;
	CREATE INDEX IF NOT EXISTS idx_access_grants_domain ON access_grants(domain) WHERE revoked_at IS NULL;

	CREATE TABLE IF NOT EXISTS access_requests (
		request_id      TEXT PRIMARY KEY,
		requester_id    TEXT NOT NULL,
		target_domain   TEXT NOT NULL,
		justification   TEXT,
		status          TEXT NOT NULL DEFAULT 'pending',
		created_height  INTEGER NOT NULL,
		resolved_height INTEGER,
		created_at      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	);

	CREATE TABLE IF NOT EXISTS access_logs (
		id           INTEGER PRIMARY KEY AUTOINCREMENT,
		agent_id     TEXT NOT NULL,
		domain       TEXT NOT NULL,
		action       TEXT NOT NULL,
		memory_ids   TEXT,
		block_height INTEGER NOT NULL,
		created_at   TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	);
	CREATE INDEX IF NOT EXISTS idx_access_logs_agent ON access_logs(agent_id);
	CREATE INDEX IF NOT EXISTS idx_access_logs_domain ON access_logs(domain);

	CREATE TABLE IF NOT EXISTS organizations (
		org_id         TEXT PRIMARY KEY,
		name           TEXT NOT NULL,
		description    TEXT,
		admin_agent_id TEXT NOT NULL,
		created_height INTEGER NOT NULL,
		created_at     TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	);

	CREATE TABLE IF NOT EXISTS org_members (
		org_id         TEXT NOT NULL,
		agent_id       TEXT NOT NULL,
		clearance      INTEGER NOT NULL DEFAULT 1,
		role           TEXT NOT NULL DEFAULT 'member',
		created_height INTEGER NOT NULL,
		created_at     TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		removed_at     TEXT,
		PRIMARY KEY (org_id, agent_id)
	);
	CREATE INDEX IF NOT EXISTS idx_org_members_agent ON org_members(agent_id) WHERE removed_at IS NULL;

	CREATE TABLE IF NOT EXISTS federations (
		federation_id     TEXT PRIMARY KEY,
		proposer_org_id   TEXT NOT NULL,
		target_org_id     TEXT NOT NULL,
		allowed_domains   TEXT,
		allowed_depts     TEXT,
		max_clearance     INTEGER NOT NULL DEFAULT 2,
		expires_at        TEXT,
		requires_approval INTEGER NOT NULL DEFAULT 0,
		status            TEXT NOT NULL DEFAULT 'proposed',
		created_height    INTEGER NOT NULL,
		approved_height   INTEGER,
		created_at        TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		revoked_at        TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_federations_proposer ON federations(proposer_org_id) WHERE status = 'active';
	CREATE INDEX IF NOT EXISTS idx_federations_target ON federations(target_org_id) WHERE status = 'active';

	CREATE TABLE IF NOT EXISTS departments (
		dept_id        TEXT NOT NULL,
		org_id         TEXT NOT NULL,
		dept_name      TEXT NOT NULL,
		description    TEXT,
		parent_dept    TEXT,
		created_height INTEGER NOT NULL,
		created_at     TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		PRIMARY KEY (org_id, dept_id)
	);
	CREATE INDEX IF NOT EXISTS idx_departments_org ON departments(org_id);

	CREATE TABLE IF NOT EXISTS dept_members (
		org_id         TEXT NOT NULL,
		dept_id        TEXT NOT NULL,
		agent_id       TEXT NOT NULL,
		clearance      INTEGER NOT NULL DEFAULT 1,
		role           TEXT NOT NULL DEFAULT 'member',
		created_height INTEGER NOT NULL,
		created_at     TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		removed_at     TEXT,
		PRIMARY KEY (org_id, dept_id, agent_id)
	);
	CREATE INDEX IF NOT EXISTS idx_dept_members_agent ON dept_members(agent_id) WHERE removed_at IS NULL;
	CREATE INDEX IF NOT EXISTS idx_dept_members_dept ON dept_members(org_id, dept_id) WHERE removed_at IS NULL;

	CREATE TABLE IF NOT EXISTS preferences (
		key        TEXT PRIMARY KEY,
		value      TEXT NOT NULL,
		updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	);

	CREATE TABLE IF NOT EXISTS network_agents (
		agent_id         TEXT PRIMARY KEY,
		name             TEXT NOT NULL,
		role             TEXT DEFAULT 'member',
		avatar           TEXT,
		boot_bio         TEXT,
		validator_pubkey TEXT,
		node_id          TEXT,
		p2p_address      TEXT,
		status           TEXT DEFAULT 'pending',
		clearance        INTEGER DEFAULT 1,
		org_id           TEXT,
		dept_id          TEXT,
		domain_access    TEXT,
		bundle_path      TEXT,
		first_seen       TEXT,
		last_seen        TEXT,
		created_at       TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		removed_at       TEXT
	);

	CREATE TABLE IF NOT EXISTS redeployment_log (
		id              INTEGER PRIMARY KEY AUTOINCREMENT,
		operation       TEXT NOT NULL,
		agent_id        TEXT NOT NULL,
		phase           TEXT NOT NULL,
		status          TEXT NOT NULL,
		details         TEXT,
		sqlite_backup   TEXT,
		genesis_backup  TEXT,
		started_at      TEXT,
		completed_at    TEXT,
		error           TEXT
	);

	CREATE TABLE IF NOT EXISTS redeployment_lock (
		id          INTEGER PRIMARY KEY CHECK (id = 1),
		locked_by   TEXT,
		locked_at   TEXT,
		operation   TEXT,
		expires_at  TEXT
	);
	`

	if _, err := s.conn.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	// Migration: add provider column if missing.
	s.migrateProvider(ctx)

	// Seed default domains
	seeds := []struct {
		tag  string
		rate float64
	}{
		{"crypto", 0.001},
		{"vuln_intel", 0.01},
		{"challenge_generation", 0.005},
		{"solver_feedback", 0.005},
		{"calibration", 0.005},
		{"infrastructure", 0.005},
	}
	for _, seed := range seeds {
		_, err := s.conn.ExecContext(ctx,
			`INSERT INTO domains (domain_tag, decay_rate) VALUES (?, ?) ON CONFLICT DO NOTHING`,
			seed.tag, seed.rate)
		if err != nil {
			return fmt.Errorf("seed domain %s: %w", seed.tag, err)
		}
	}

	return nil
}

// migrateProvider adds the provider column to memories if it doesn't exist.
func (s *SQLiteStore) migrateProvider(ctx context.Context) {
	// Check if column exists by attempting a query.
	row := s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name='provider'`)
	var count int
	if err := row.Scan(&count); err != nil || count > 0 {
		return // already exists or error checking
	}
	_, _ = s.conn.ExecContext(ctx, `ALTER TABLE memories ADD COLUMN provider TEXT NOT NULL DEFAULT ''`)
	_, _ = s.conn.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_memories_provider ON memories(provider)`)
}

// --- Helper functions ---

func parseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t, _ = time.Parse("2006-01-02T15:04:05.999999999Z07:00", s)
	}
	return t
}

func parseTimePtr(s *string) *time.Time {
	if s == nil {
		return nil
	}
	t := parseTime(*s)
	return &t
}

func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func formatTimePtr(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := formatTime(*t)
	return &s
}

func encodeEmbedding(emb []float32) []byte {
	if len(emb) == 0 {
		return nil
	}
	data, _ := json.Marshal(emb)
	return data
}

func decodeEmbedding(data []byte) []float32 {
	if len(data) == 0 {
		return nil
	}
	var emb []float32
	if err := json.Unmarshal(data, &emb); err != nil {
		return nil
	}
	return emb
}

func encodeStringSlice(ss []string) string {
	if ss == nil {
		return "[]"
	}
	data, _ := json.Marshal(ss)
	return string(data)
}

func decodeStringSlice(s string) []string {
	if s == "" {
		return nil
	}
	var ss []string
	if err := json.Unmarshal([]byte(s), &ss); err != nil {
		return nil
	}
	return ss
}

func encodeFloat64Slice(fs []float64) string {
	if fs == nil {
		return "[]"
	}
	data, _ := json.Marshal(fs)
	return string(data)
}

func decodeFloat64Slice(s string) []float64 {
	if s == "" {
		return nil
	}
	var fs []float64
	if err := json.Unmarshal([]byte(s), &fs); err != nil {
		return nil
	}
	return fs
}

func cosineSimilarity(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, normA, normB float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}
	denom := math.Sqrt(normA) * math.Sqrt(normB)
	if denom == 0 {
		return 0
	}
	return dot / denom
}

// --- MemoryStore implementation ---

func (s *SQLiteStore) InsertMemory(ctx context.Context, record *memory.MemoryRecord) error {
	// Encrypt content and embedding if vault is set.
	content, err := s.encryptContent(record.Content)
	if err != nil {
		return err
	}
	embData := encodeEmbedding(record.Embedding)
	encEmb, err := s.encryptEmbedding(embData)
	if err != nil {
		return fmt.Errorf("encrypt embedding: %w", err)
	}

	_, err = s.conn.ExecContext(ctx,
		`INSERT INTO memories (memory_id, submitting_agent, content, content_hash, embedding, embedding_hash,
			memory_type, domain_tag, provider, confidence_score, status, parent_hash, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (memory_id) DO UPDATE SET
			submitting_agent = excluded.submitting_agent,
			status = excluded.status,
			created_at = excluded.created_at`,
		record.MemoryID, record.SubmittingAgent, content, record.ContentHash,
		encEmb, record.EmbeddingHash,
		string(record.MemoryType), record.DomainTag, record.Provider, record.ConfidenceScore,
		string(record.Status), record.ParentHash, formatTime(record.CreatedAt))
	if err != nil {
		return fmt.Errorf("insert memory: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetMemory(ctx context.Context, memoryID string) (*memory.MemoryRecord, error) {
	row := s.conn.QueryRowContext(ctx,
		`SELECT memory_id, submitting_agent, content, content_hash, embedding, embedding_hash,
			memory_type, domain_tag, provider, confidence_score, status, parent_hash, created_at, committed_at, deprecated_at
		FROM memories WHERE memory_id = ?`, memoryID)

	var r memory.MemoryRecord
	var mt, st, createdAt string
	var embData []byte
	var parentHash, committedAt, deprecatedAt *string

	err := row.Scan(&r.MemoryID, &r.SubmittingAgent, &r.Content, &r.ContentHash,
		&embData, &r.EmbeddingHash, &mt, &r.DomainTag, &r.Provider, &r.ConfidenceScore,
		&st, &parentHash, &createdAt, &committedAt, &deprecatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("memory not found: %s", memoryID)
		}
		return nil, fmt.Errorf("get memory: %w", err)
	}

	r.MemoryType = memory.MemoryType(mt)
	r.Status = memory.MemoryStatus(st)

	// Decrypt content and embedding if encrypted.
	if decContent, decErr := s.decryptContent(r.Content); decErr == nil {
		r.Content = decContent
	}
	decEmb, _ := s.decryptEmbedding(embData)
	r.Embedding = decodeEmbedding(decEmb)

	r.CreatedAt = parseTime(createdAt)
	r.CommittedAt = parseTimePtr(committedAt)
	r.DeprecatedAt = parseTimePtr(deprecatedAt)
	if parentHash != nil {
		r.ParentHash = *parentHash
	}

	return &r, nil
}

func (s *SQLiteStore) UpdateStatus(ctx context.Context, memoryID string, status memory.MemoryStatus, now time.Time) error {
	nowStr := formatTime(now)
	var err error
	switch status {
	case memory.StatusCommitted:
		_, err = s.conn.ExecContext(ctx,
			`UPDATE memories SET status = ?, committed_at = ? WHERE memory_id = ?`,
			string(status), nowStr, memoryID)
	case memory.StatusDeprecated:
		_, err = s.conn.ExecContext(ctx,
			`UPDATE memories SET status = ?, deprecated_at = ? WHERE memory_id = ?`,
			string(status), nowStr, memoryID)
	default:
		_, err = s.conn.ExecContext(ctx,
			`UPDATE memories SET status = ? WHERE memory_id = ?`,
			string(status), memoryID)
	}
	if err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	return nil
}

func (s *SQLiteStore) QuerySimilar(ctx context.Context, embedding []float32, opts QueryOptions) ([]*memory.MemoryRecord, error) {
	if opts.TopK <= 0 {
		opts.TopK = 10
	}
	if opts.TopK > 100 {
		opts.TopK = 100
	}

	query := `SELECT memory_id, submitting_agent, content, content_hash, embedding,
		memory_type, domain_tag, provider, confidence_score, status, parent_hash, created_at,
		committed_at, deprecated_at
		FROM memories WHERE embedding IS NOT NULL`
	var args []any

	if opts.DomainTag != "" {
		query += " AND domain_tag = ?"
		args = append(args, opts.DomainTag)
	}
	if opts.Provider != "" {
		// Provider scoping: show memories from this provider OR facts (shared cross-provider).
		query += " AND (provider = ? OR provider = '' OR memory_type = 'fact')"
		args = append(args, opts.Provider)
	}
	if opts.MinConfidence > 0 {
		query += " AND confidence_score >= ?"
		args = append(args, opts.MinConfidence)
	}
	if opts.StatusFilter != "" {
		query += " AND status = ?"
		args = append(args, opts.StatusFilter)
	}

	rows, err := s.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query similar: %w", err)
	}
	defer rows.Close()

	type scoredRecord struct {
		record     *memory.MemoryRecord
		similarity float64
	}
	var scored []scoredRecord

	for rows.Next() {
		var r memory.MemoryRecord
		var mt, st, createdAt string
		var embData []byte
		var parentHash, committedAt, deprecatedAt *string

		scanErr := rows.Scan(&r.MemoryID, &r.SubmittingAgent, &r.Content, &r.ContentHash,
			&embData, &mt, &r.DomainTag, &r.Provider, &r.ConfidenceScore,
			&st, &parentHash, &createdAt, &committedAt, &deprecatedAt)
		if scanErr != nil {
			return nil, fmt.Errorf("scan row: %w", scanErr)
		}

		r.MemoryType = memory.MemoryType(mt)
		r.Status = memory.MemoryStatus(st)

		// Decrypt content and embedding if encrypted.
		if decContent, decErr := s.decryptContent(r.Content); decErr == nil {
			r.Content = decContent
		}
		decEmb, _ := s.decryptEmbedding(embData)
		r.Embedding = decodeEmbedding(decEmb)

		r.CreatedAt = parseTime(createdAt)
		r.CommittedAt = parseTimePtr(committedAt)
		r.DeprecatedAt = parseTimePtr(deprecatedAt)
		if parentHash != nil {
			r.ParentHash = *parentHash
		}

		sim := cosineSimilarity(embedding, r.Embedding)
		scored = append(scored, scoredRecord{record: &r, similarity: sim})
	}

	sort.Slice(scored, func(i, j int) bool {
		return scored[i].similarity > scored[j].similarity
	})

	limit := opts.TopK
	if limit > len(scored) {
		limit = len(scored)
	}

	results := make([]*memory.MemoryRecord, limit)
	for i := 0; i < limit; i++ {
		results[i] = scored[i].record
	}
	return results, nil
}

func (s *SQLiteStore) InsertTriples(ctx context.Context, memoryID string, triples []memory.KnowledgeTriple) error {
	if len(triples) == 0 {
		return nil
	}

	insertAll := func(q sqlQuerier) error {
		for _, t := range triples {
			if _, err := q.ExecContext(ctx,
				`INSERT INTO knowledge_triples (memory_id, subject, predicate, object) VALUES (?, ?, ?, ?)`,
				memoryID, t.Subject, t.Predicate, t.Object); err != nil {
				return fmt.Errorf("insert triple: %w", err)
			}
		}
		return nil
	}

	// If already in a transaction (tx-scoped store), execute directly.
	if s.db == nil {
		return insertAll(s.conn)
	}

	// Otherwise, wrap in a local transaction for atomicity.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	if err := insertAll(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLiteStore) InsertVote(ctx context.Context, vote *ValidationVote) error {
	_, err := s.conn.ExecContext(ctx,
		`INSERT INTO validation_votes (memory_id, validator_id, decision, rationale, weight_at_vote, block_height, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (memory_id, validator_id) DO UPDATE SET decision = excluded.decision, rationale = excluded.rationale, weight_at_vote = excluded.weight_at_vote`,
		vote.MemoryID, vote.ValidatorID, vote.Decision, vote.Rationale, vote.WeightAtVote, vote.BlockHeight, formatTime(vote.CreatedAt))
	if err != nil {
		return fmt.Errorf("insert vote: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetVotes(ctx context.Context, memoryID string) ([]*ValidationVote, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT id, memory_id, validator_id, decision, rationale, weight_at_vote, block_height, created_at
		FROM validation_votes WHERE memory_id = ? ORDER BY created_at`, memoryID)
	if err != nil {
		return nil, fmt.Errorf("get votes: %w", err)
	}
	defer rows.Close()

	var votes []*ValidationVote
	for rows.Next() {
		v := &ValidationVote{}
		var createdAt string
		if scanErr := rows.Scan(&v.ID, &v.MemoryID, &v.ValidatorID, &v.Decision, &v.Rationale,
			&v.WeightAtVote, &v.BlockHeight, &createdAt); scanErr != nil {
			return nil, fmt.Errorf("scan vote: %w", scanErr)
		}
		v.CreatedAt = parseTime(createdAt)
		votes = append(votes, v)
	}
	return votes, nil
}

func (s *SQLiteStore) InsertChallenge(ctx context.Context, challenge *ChallengeEntry) error {
	_, err := s.conn.ExecContext(ctx,
		`INSERT INTO challenges (memory_id, challenger_id, reason, evidence, block_height, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		challenge.MemoryID, challenge.ChallengerID, challenge.Reason, challenge.Evidence, challenge.BlockHeight, formatTime(challenge.CreatedAt))
	if err != nil {
		return fmt.Errorf("insert challenge: %w", err)
	}
	return nil
}

func (s *SQLiteStore) InsertCorroboration(ctx context.Context, corr *Corroboration) error {
	_, err := s.conn.ExecContext(ctx,
		`INSERT INTO corroborations (memory_id, agent_id, evidence, created_at)
		VALUES (?, ?, ?, ?)`,
		corr.MemoryID, corr.AgentID, corr.Evidence, formatTime(corr.CreatedAt))
	if err != nil {
		return fmt.Errorf("insert corroboration: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetCorroborations(ctx context.Context, memoryID string) ([]*Corroboration, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT id, memory_id, agent_id, evidence, created_at
		FROM corroborations WHERE memory_id = ? ORDER BY created_at`, memoryID)
	if err != nil {
		return nil, fmt.Errorf("get corroborations: %w", err)
	}
	defer rows.Close()

	var corrs []*Corroboration
	for rows.Next() {
		c := &Corroboration{}
		var createdAt string
		if scanErr := rows.Scan(&c.ID, &c.MemoryID, &c.AgentID, &c.Evidence, &createdAt); scanErr != nil {
			return nil, fmt.Errorf("scan corroboration: %w", scanErr)
		}
		c.CreatedAt = parseTime(createdAt)
		corrs = append(corrs, c)
	}
	return corrs, nil
}

func (s *SQLiteStore) GetPendingByDomain(ctx context.Context, domainTag string, limit int) ([]*memory.MemoryRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.conn.QueryContext(ctx,
		`SELECT memory_id, submitting_agent, content, content_hash,
			memory_type, domain_tag, confidence_score, status, created_at
		FROM memories WHERE status = 'proposed' AND domain_tag LIKE ?
		ORDER BY created_at LIMIT ?`, domainTag, limit)
	if err != nil {
		return nil, fmt.Errorf("get pending: %w", err)
	}
	defer rows.Close()

	var results []*memory.MemoryRecord
	for rows.Next() {
		var r memory.MemoryRecord
		var mt, st, createdAt string
		if scanErr := rows.Scan(&r.MemoryID, &r.SubmittingAgent, &r.Content, &r.ContentHash,
			&mt, &r.DomainTag, &r.ConfidenceScore, &st, &createdAt); scanErr != nil {
			return nil, fmt.Errorf("scan pending: %w", scanErr)
		}
		r.MemoryType = memory.MemoryType(mt)
		r.Status = memory.MemoryStatus(st)
		r.CreatedAt = parseTime(createdAt)
		results = append(results, &r)
	}
	return results, nil
}

func (s *SQLiteStore) ListMemories(ctx context.Context, opts ListOptions) ([]*memory.MemoryRecord, int, error) {
	if opts.Limit <= 0 {
		opts.Limit = 50
	}

	countQuery := `SELECT COUNT(*) FROM memories WHERE 1=1`
	query := `SELECT memory_id, submitting_agent, content, content_hash,
		memory_type, domain_tag, provider, confidence_score, status, parent_hash, created_at,
		committed_at, deprecated_at FROM memories WHERE 1=1`
	var args []any

	if opts.DomainTag != "" {
		filter := " AND domain_tag = ?"
		query += filter
		countQuery += filter
		args = append(args, opts.DomainTag)
	}
	if opts.Provider != "" {
		filter := " AND (provider = ? OR provider = '' OR memory_type = 'fact')"
		query += filter
		countQuery += filter
		args = append(args, opts.Provider)
	}
	if opts.Status != "" {
		filter := " AND status = ?"
		query += filter
		countQuery += filter
		args = append(args, opts.Status)
	}
	if opts.SubmittingAgent != "" {
		filter := " AND submitting_agent = ?"
		query += filter
		countQuery += filter
		args = append(args, opts.SubmittingAgent)
	}

	switch opts.Sort {
	case "oldest":
		query += " ORDER BY created_at ASC"
	case "confidence":
		query += " ORDER BY confidence_score DESC"
	default:
		query += " ORDER BY created_at DESC"
	}

	query += " LIMIT ? OFFSET ?"
	queryArgs := make([]any, len(args))
	copy(queryArgs, args)
	queryArgs = append(queryArgs, opts.Limit, opts.Offset)

	var total int
	if err := s.conn.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count memories: %w", err)
	}

	rows, err := s.conn.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("list memories: %w", err)
	}
	defer rows.Close()

	var results []*memory.MemoryRecord
	for rows.Next() {
		var r memory.MemoryRecord
		var mt, st, createdAt string
		var parentHash, committedAt, deprecatedAt *string
		if scanErr := rows.Scan(&r.MemoryID, &r.SubmittingAgent, &r.Content, &r.ContentHash,
			&mt, &r.DomainTag, &r.Provider, &r.ConfidenceScore, &st, &parentHash,
			&createdAt, &committedAt, &deprecatedAt); scanErr != nil {
			return nil, 0, fmt.Errorf("scan memory: %w", scanErr)
		}
		r.MemoryType = memory.MemoryType(mt)
		r.Status = memory.MemoryStatus(st)
		r.CreatedAt = parseTime(createdAt)
		r.CommittedAt = parseTimePtr(committedAt)
		r.DeprecatedAt = parseTimePtr(deprecatedAt)
		if parentHash != nil {
			r.ParentHash = *parentHash
		}
		// Decrypt content if encrypted.
		if decContent, decErr := s.decryptContent(r.Content); decErr == nil {
			r.Content = decContent
		}
		results = append(results, &r)
	}
	return results, total, nil
}

func (s *SQLiteStore) GetStats(ctx context.Context) (*StoreStats, error) {
	stats := &StoreStats{
		ByDomain: make(map[string]int),
		ByStatus: make(map[string]int),
	}

	if err := s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM memories`).Scan(&stats.TotalMemories); err != nil {
		return nil, fmt.Errorf("count total: %w", err)
	}

	rows, err := s.conn.QueryContext(ctx, `SELECT domain_tag, COUNT(*) FROM memories GROUP BY domain_tag`)
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

	rows, err = s.conn.QueryContext(ctx, `SELECT status, COUNT(*) FROM memories GROUP BY status`)
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

	// Count by agent
	stats.ByAgent = make(map[string]int)
	rows, err = s.conn.QueryContext(ctx, `SELECT submitting_agent, COUNT(*) FROM memories GROUP BY submitting_agent`)
	if err != nil {
		return nil, fmt.Errorf("count by agent: %w", err)
	}
	for rows.Next() {
		var agent string
		var count int
		if scanErr := rows.Scan(&agent, &count); scanErr != nil {
			rows.Close()
			return nil, fmt.Errorf("scan agent count: %w", scanErr)
		}
		stats.ByAgent[agent] = count
	}
	rows.Close()

	var lastActivity *string
	if scanErr := s.conn.QueryRowContext(ctx, `SELECT MAX(created_at) FROM memories`).Scan(&lastActivity); scanErr == nil {
		stats.LastActivity = parseTimePtr(lastActivity)
	}

	// Get DB file size
	if info, err := os.Stat(s.dbPath); err == nil {
		stats.DBSizeBytes = info.Size()
	}

	return stats, nil
}

func (s *SQLiteStore) GetTimeline(ctx context.Context, from, to time.Time, domain string, bucket string) ([]TimelineBucket, error) {
	// SQLite uses strftime for date truncation
	var format string
	switch bucket {
	case "hour":
		format = "%Y-%m-%dT%H:00:00Z"
	case "week":
		format = "%Y-W%W"
	case "month":
		format = "%Y-%m"
	default:
		format = "%Y-%m-%d"
	}

	query := fmt.Sprintf(`SELECT strftime('%s', created_at) AS period, COUNT(*) `+ //nolint:gosec // format is from a fixed switch, not user input
		`FROM memories WHERE created_at >= ? AND created_at <= ?`, format)
	args := []any{formatTime(from), formatTime(to)}

	if domain != "" {
		query += " AND domain_tag = ?"
		args = append(args, domain)
	}

	query += " GROUP BY period ORDER BY period"

	rows, err := s.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get timeline: %w", err)
	}
	defer rows.Close()

	var buckets []TimelineBucket
	for rows.Next() {
		var period string
		var count int
		if scanErr := rows.Scan(&period, &count); scanErr != nil {
			return nil, fmt.Errorf("scan timeline: %w", scanErr)
		}
		buckets = append(buckets, TimelineBucket{
			Period: period,
			Count:  count,
			Domain: domain,
		})
	}
	return buckets, nil
}

func (s *SQLiteStore) DeleteMemory(ctx context.Context, memoryID string) error {
	_, err := s.conn.ExecContext(ctx,
		`UPDATE memories SET status = 'deprecated', deprecated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE memory_id = ?`,
		memoryID)
	if err != nil {
		return fmt.Errorf("delete memory: %w", err)
	}
	return nil
}

func (s *SQLiteStore) UpdateDomainTag(ctx context.Context, memoryID string, domain string) error {
	_, err := s.conn.ExecContext(ctx,
		`UPDATE memories SET domain_tag = ? WHERE memory_id = ?`,
		domain, memoryID)
	if err != nil {
		return fmt.Errorf("update domain tag: %w", err)
	}
	return nil
}

// --- ValidatorScoreStore implementation ---

func (s *SQLiteStore) GetScore(ctx context.Context, validatorID string) (*ValidatorScore, error) {
	row := s.conn.QueryRowContext(ctx,
		`SELECT validator_id, weighted_sum, weight_denom, vote_count, expertise_vec,
			last_active_ts, current_weight, updated_at
		FROM validator_scores WHERE validator_id = ?`, validatorID)

	vs := &ValidatorScore{}
	var expertiseVec, updatedAt string
	var lastActiveTS *string
	err := row.Scan(&vs.ValidatorID, &vs.WeightedSum, &vs.WeightDenom, &vs.VoteCount,
		&expertiseVec, &lastActiveTS, &vs.CurrentWeight, &updatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("validator score not found: %s", validatorID)
		}
		return nil, fmt.Errorf("get validator score: %w", err)
	}
	vs.ExpertiseVec = decodeFloat64Slice(expertiseVec)
	vs.LastActiveTS = parseTimePtr(lastActiveTS)
	vs.UpdatedAt = parseTime(updatedAt)
	return vs, nil
}

func (s *SQLiteStore) UpdateScore(ctx context.Context, score *ValidatorScore) error {
	_, err := s.conn.ExecContext(ctx,
		`INSERT INTO validator_scores (validator_id, weighted_sum, weight_denom, vote_count, expertise_vec,
			last_active_ts, current_weight, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (validator_id) DO UPDATE SET
			weighted_sum = excluded.weighted_sum, weight_denom = excluded.weight_denom,
			vote_count = excluded.vote_count, expertise_vec = excluded.expertise_vec,
			last_active_ts = excluded.last_active_ts, current_weight = excluded.current_weight,
			updated_at = excluded.updated_at`,
		score.ValidatorID, score.WeightedSum, score.WeightDenom, score.VoteCount,
		encodeFloat64Slice(score.ExpertiseVec), formatTimePtr(score.LastActiveTS),
		score.CurrentWeight, formatTime(score.UpdatedAt))
	if err != nil {
		return fmt.Errorf("update validator score: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetAllScores(ctx context.Context) ([]*ValidatorScore, error) {
	rows, err := s.conn.QueryContext(ctx,
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
		var expertiseVec, updatedAt string
		var lastActiveTS *string
		if scanErr := rows.Scan(&vs.ValidatorID, &vs.WeightedSum, &vs.WeightDenom, &vs.VoteCount,
			&expertiseVec, &lastActiveTS, &vs.CurrentWeight, &updatedAt); scanErr != nil {
			return nil, fmt.Errorf("scan validator score: %w", scanErr)
		}
		vs.ExpertiseVec = decodeFloat64Slice(expertiseVec)
		vs.LastActiveTS = parseTimePtr(lastActiveTS)
		vs.UpdatedAt = parseTime(updatedAt)
		scores = append(scores, vs)
	}
	return scores, nil
}

func (s *SQLiteStore) InsertEpochScore(ctx context.Context, epoch *EpochScore) error {
	_, err := s.conn.ExecContext(ctx,
		`INSERT INTO epoch_scores (epoch_num, block_height, validator_id, accuracy, domain_score,
			recency_score, corr_score, raw_weight, capped_weight, normalized_weight)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (epoch_num, validator_id) DO NOTHING`,
		epoch.EpochNum, epoch.BlockHeight, epoch.ValidatorID, epoch.Accuracy, epoch.DomainScore,
		epoch.RecencyScore, epoch.CorrScore, epoch.RawWeight, epoch.CappedWeight, epoch.NormalizedWeight)
	if err != nil {
		return fmt.Errorf("insert epoch score: %w", err)
	}
	return nil
}

// --- AccessStore implementation ---

func (s *SQLiteStore) InsertAccessGrant(ctx context.Context, grant *AccessGrantEntry) error {
	_, err := s.conn.ExecContext(ctx,
		`INSERT INTO access_grants (domain, grantee_id, granter_id, access_level, expires_at, created_height, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (domain, grantee_id, created_height) DO NOTHING`,
		grant.Domain, grant.GranteeID, grant.GranterID, grant.Level,
		formatTimePtr(grant.ExpiresAt), grant.CreatedHeight, formatTime(grant.CreatedAt))
	if err != nil {
		return fmt.Errorf("insert access grant: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetActiveGrants(ctx context.Context, agentID string) ([]*AccessGrantEntry, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT domain, grantee_id, granter_id, access_level, expires_at, created_height, created_at
		FROM access_grants WHERE grantee_id = ? AND revoked_at IS NULL
		ORDER BY created_at`, agentID)
	if err != nil {
		return nil, fmt.Errorf("get active grants: %w", err)
	}
	defer rows.Close()

	var grants []*AccessGrantEntry
	for rows.Next() {
		g := &AccessGrantEntry{}
		var expiresAt, createdAt *string
		if scanErr := rows.Scan(&g.Domain, &g.GranteeID, &g.GranterID, &g.Level, &expiresAt, &g.CreatedHeight, &createdAt); scanErr != nil {
			return nil, fmt.Errorf("scan grant: %w", scanErr)
		}
		g.ExpiresAt = parseTimePtr(expiresAt)
		if createdAt != nil {
			g.CreatedAt = parseTime(*createdAt)
		}
		grants = append(grants, g)
	}
	return grants, nil
}

func (s *SQLiteStore) RevokeGrant(ctx context.Context, domain, granteeID string, height int64) error {
	_, err := s.conn.ExecContext(ctx,
		`UPDATE access_grants SET revoked_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		WHERE domain = ? AND grantee_id = ? AND revoked_at IS NULL`,
		domain, granteeID)
	if err != nil {
		return fmt.Errorf("revoke grant: %w", err)
	}
	return nil
}

func (s *SQLiteStore) InsertAccessRequest(ctx context.Context, req *AccessRequestEntry) error {
	_, err := s.conn.ExecContext(ctx,
		`INSERT INTO access_requests (request_id, requester_id, target_domain, justification, status, created_height, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (request_id) DO NOTHING`,
		req.RequestID, req.RequesterID, req.TargetDomain, req.Justification, req.Status, req.CreatedHeight, formatTime(req.CreatedAt))
	if err != nil {
		return fmt.Errorf("insert access request: %w", err)
	}
	return nil
}

func (s *SQLiteStore) UpdateAccessRequestStatus(ctx context.Context, requestID, status string, height int64) error {
	_, err := s.conn.ExecContext(ctx,
		`UPDATE access_requests SET status = ?, resolved_height = ? WHERE request_id = ?`,
		status, height, requestID)
	if err != nil {
		return fmt.Errorf("update access request status: %w", err)
	}
	return nil
}

func (s *SQLiteStore) InsertAccessLog(ctx context.Context, log *AccessLogEntry) error {
	memoryIDsJSON := encodeStringSlice(log.MemoryIDs)
	_, err := s.conn.ExecContext(ctx,
		`INSERT INTO access_logs (agent_id, domain, action, memory_ids, block_height, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		log.AgentID, log.Domain, log.Action, memoryIDsJSON, log.BlockHeight, formatTime(log.CreatedAt))
	if err != nil {
		return fmt.Errorf("insert access log: %w", err)
	}
	return nil
}

func (s *SQLiteStore) InsertDomain(ctx context.Context, domain *DomainEntry) error {
	_, err := s.conn.ExecContext(ctx,
		`INSERT INTO domain_registry (domain_name, owner_agent_id, parent_domain, description, created_height, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (domain_name) DO NOTHING`,
		domain.DomainName, domain.OwnerAgentID, domain.ParentDomain, domain.Description, domain.CreatedHeight, formatTime(domain.CreatedAt))
	if err != nil {
		return fmt.Errorf("insert domain: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetDomain(ctx context.Context, name string) (*DomainEntry, error) {
	row := s.conn.QueryRowContext(ctx,
		`SELECT domain_name, owner_agent_id, parent_domain, description, created_height, created_at
		FROM domain_registry WHERE domain_name = ?`, name)

	d := &DomainEntry{}
	var parentDomain, description *string
	var createdAt string
	err := row.Scan(&d.DomainName, &d.OwnerAgentID, &parentDomain, &description, &d.CreatedHeight, &createdAt)
	if err != nil {
		if err == sql.ErrNoRows {
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
	d.CreatedAt = parseTime(createdAt)
	return d, nil
}

// --- OrgStore implementation ---

func (s *SQLiteStore) InsertOrg(ctx context.Context, org *OrgEntry) error {
	_, err := s.conn.ExecContext(ctx,
		`INSERT INTO organizations (org_id, name, description, admin_agent_id, created_height, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (org_id) DO NOTHING`,
		org.OrgID, org.Name, org.Description, org.AdminAgentID, org.CreatedHeight, formatTime(org.CreatedAt))
	if err != nil {
		return fmt.Errorf("insert org: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetOrg(ctx context.Context, orgID string) (*OrgEntry, error) {
	row := s.conn.QueryRowContext(ctx,
		`SELECT org_id, name, description, admin_agent_id, created_height, created_at
		FROM organizations WHERE org_id = ?`, orgID)

	o := &OrgEntry{}
	var description *string
	var createdAt string
	err := row.Scan(&o.OrgID, &o.Name, &description, &o.AdminAgentID, &o.CreatedHeight, &createdAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("org not found: %s", orgID)
		}
		return nil, fmt.Errorf("get org: %w", err)
	}
	if description != nil {
		o.Description = *description
	}
	o.CreatedAt = parseTime(createdAt)
	return o, nil
}

func (s *SQLiteStore) InsertOrgMember(ctx context.Context, member *OrgMemberEntry) error {
	_, err := s.conn.ExecContext(ctx,
		`INSERT INTO org_members (org_id, agent_id, clearance, role, created_height, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (org_id, agent_id) DO NOTHING`,
		member.OrgID, member.AgentID, int(member.Clearance), member.Role, member.CreatedHeight, formatTime(member.CreatedAt))
	if err != nil {
		return fmt.Errorf("insert org member: %w", err)
	}
	return nil
}

func (s *SQLiteStore) RemoveOrgMember(ctx context.Context, orgID, agentID string, height int64) error {
	_, err := s.conn.ExecContext(ctx,
		`UPDATE org_members SET removed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		WHERE org_id = ? AND agent_id = ? AND removed_at IS NULL`,
		orgID, agentID)
	if err != nil {
		return fmt.Errorf("remove org member: %w", err)
	}
	return nil
}

func (s *SQLiteStore) UpdateMemberClearance(ctx context.Context, orgID, agentID string, clearance ClearanceLevel) error {
	_, err := s.conn.ExecContext(ctx,
		`UPDATE org_members SET clearance = ?
		WHERE org_id = ? AND agent_id = ? AND removed_at IS NULL`,
		int(clearance), orgID, agentID)
	if err != nil {
		return fmt.Errorf("update member clearance: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetOrgMembers(ctx context.Context, orgID string) ([]*OrgMemberEntry, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT org_id, agent_id, clearance, role, created_height, created_at
		FROM org_members WHERE org_id = ? AND removed_at IS NULL
		ORDER BY created_at`, orgID)
	if err != nil {
		return nil, fmt.Errorf("get org members: %w", err)
	}
	defer rows.Close()

	var members []*OrgMemberEntry
	for rows.Next() {
		m := &OrgMemberEntry{}
		var clearance int
		var createdAt string
		if scanErr := rows.Scan(&m.OrgID, &m.AgentID, &clearance, &m.Role, &m.CreatedHeight, &createdAt); scanErr != nil {
			return nil, fmt.Errorf("scan org member: %w", scanErr)
		}
		m.Clearance = ClearanceLevel(clearance) // #nosec G115 -- clearance is 0-4
		m.CreatedAt = parseTime(createdAt)
		members = append(members, m)
	}
	return members, nil
}

func (s *SQLiteStore) InsertFederation(ctx context.Context, fed *FederationEntry) error {
	_, err := s.conn.ExecContext(ctx,
		`INSERT INTO federations (federation_id, proposer_org_id, target_org_id, allowed_domains, allowed_depts,
			max_clearance, expires_at, requires_approval, status, created_height, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (federation_id) DO NOTHING`,
		fed.FederationID, fed.ProposerOrgID, fed.TargetOrgID,
		encodeStringSlice(fed.AllowedDomains), encodeStringSlice(fed.AllowedDepts),
		int(fed.MaxClearance), formatTimePtr(fed.ExpiresAt),
		boolToInt(fed.RequiresApproval), fed.Status,
		fed.CreatedHeight, formatTime(fed.CreatedAt))
	if err != nil {
		return fmt.Errorf("insert federation: %w", err)
	}
	return nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (s *SQLiteStore) GetFederation(ctx context.Context, federationID string) (*FederationEntry, error) {
	row := s.conn.QueryRowContext(ctx,
		`SELECT federation_id, proposer_org_id, target_org_id, allowed_domains, allowed_depts,
			max_clearance, expires_at, requires_approval, status, created_height,
			approved_height, created_at, revoked_at
		FROM federations WHERE federation_id = ?`, federationID)

	f := &FederationEntry{}
	var maxClearance int
	var reqApproval int
	var allowedDomains, allowedDepts string
	var expiresAt, createdAt, revokedAt *string
	var approvedHeight *int64
	err := row.Scan(&f.FederationID, &f.ProposerOrgID, &f.TargetOrgID, &allowedDomains, &allowedDepts,
		&maxClearance, &expiresAt, &reqApproval, &f.Status, &f.CreatedHeight,
		&approvedHeight, &createdAt, &revokedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("federation not found: %s", federationID)
		}
		return nil, fmt.Errorf("get federation: %w", err)
	}
	f.MaxClearance = ClearanceLevel(maxClearance) // #nosec G115 -- clearance is 0-4
	f.RequiresApproval = reqApproval != 0
	f.AllowedDomains = decodeStringSlice(allowedDomains)
	f.AllowedDepts = decodeStringSlice(allowedDepts)
	f.ExpiresAt = parseTimePtr(expiresAt)
	f.ApprovedHeight = approvedHeight
	if createdAt != nil {
		f.CreatedAt = parseTime(*createdAt)
	}
	f.RevokedAt = parseTimePtr(revokedAt)
	return f, nil
}

func (s *SQLiteStore) ApproveFederation(ctx context.Context, federationID string, height int64) error {
	_, err := s.conn.ExecContext(ctx,
		`UPDATE federations SET status = 'active', approved_height = ?
		WHERE federation_id = ? AND status = 'proposed'`,
		height, federationID)
	if err != nil {
		return fmt.Errorf("approve federation: %w", err)
	}
	return nil
}

func (s *SQLiteStore) RevokeFederation(ctx context.Context, federationID string, height int64) error {
	_, err := s.conn.ExecContext(ctx,
		`UPDATE federations SET status = 'revoked', revoked_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		WHERE federation_id = ? AND status = 'active'`,
		federationID)
	if err != nil {
		return fmt.Errorf("revoke federation: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetActiveFederations(ctx context.Context, orgID string) ([]*FederationEntry, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT federation_id, proposer_org_id, target_org_id, allowed_domains, allowed_depts,
			max_clearance, expires_at, requires_approval, status, created_height,
			approved_height, created_at, revoked_at
		FROM federations
		WHERE (proposer_org_id = ? OR target_org_id = ?) AND status IN ('active', 'proposed')
		ORDER BY created_at`, orgID, orgID)
	if err != nil {
		return nil, fmt.Errorf("get active federations: %w", err)
	}
	defer rows.Close()

	var feds []*FederationEntry
	for rows.Next() {
		f := &FederationEntry{}
		var maxClearance, reqApproval int
		var allowedDomains, allowedDepts string
		var expiresAt, createdAt, revokedAt *string
		var approvedHeight *int64
		if scanErr := rows.Scan(&f.FederationID, &f.ProposerOrgID, &f.TargetOrgID, &allowedDomains, &allowedDepts,
			&maxClearance, &expiresAt, &reqApproval, &f.Status, &f.CreatedHeight,
			&approvedHeight, &createdAt, &revokedAt); scanErr != nil {
			return nil, fmt.Errorf("scan federation: %w", scanErr)
		}
		f.MaxClearance = ClearanceLevel(maxClearance) // #nosec G115 -- clearance is 0-4
		f.RequiresApproval = reqApproval != 0
		f.AllowedDomains = decodeStringSlice(allowedDomains)
		f.AllowedDepts = decodeStringSlice(allowedDepts)
		f.ExpiresAt = parseTimePtr(expiresAt)
		f.ApprovedHeight = approvedHeight
		if createdAt != nil {
			f.CreatedAt = parseTime(*createdAt)
		}
		f.RevokedAt = parseTimePtr(revokedAt)
		feds = append(feds, f)
	}
	return feds, nil
}

func (s *SQLiteStore) UpdateMemoryClassification(ctx context.Context, memoryID string, classification ClearanceLevel) error {
	_, err := s.conn.ExecContext(ctx,
		`UPDATE memories SET classification = ? WHERE memory_id = ?`,
		int(classification), memoryID)
	if err != nil {
		return fmt.Errorf("update memory classification: %w", err)
	}
	return nil
}

// --- Department methods ---

func (s *SQLiteStore) InsertDept(ctx context.Context, dept *DeptEntry) error {
	_, err := s.conn.ExecContext(ctx,
		`INSERT INTO departments (dept_id, org_id, dept_name, description, parent_dept, created_height)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT DO NOTHING`,
		dept.DeptID, dept.OrgID, dept.DeptName, dept.Description, dept.ParentDept, dept.CreatedHeight)
	if err != nil {
		return fmt.Errorf("insert dept: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetDept(ctx context.Context, orgID, deptID string) (*DeptEntry, error) {
	row := s.conn.QueryRowContext(ctx,
		`SELECT dept_id, org_id, dept_name, description, parent_dept, created_height, created_at
		FROM departments WHERE org_id = ? AND dept_id = ?`, orgID, deptID)

	d := &DeptEntry{}
	var description, parentDept *string
	var createdAt string
	err := row.Scan(&d.DeptID, &d.OrgID, &d.DeptName, &description, &parentDept, &d.CreatedHeight, &createdAt)
	if err != nil {
		if err == sql.ErrNoRows {
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
	d.CreatedAt = parseTime(createdAt)
	return d, nil
}

func (s *SQLiteStore) GetOrgDepts(ctx context.Context, orgID string) ([]*DeptEntry, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT dept_id, org_id, dept_name, description, parent_dept, created_height, created_at
		FROM departments WHERE org_id = ? ORDER BY dept_name`, orgID)
	if err != nil {
		return nil, fmt.Errorf("get org depts: %w", err)
	}
	defer rows.Close()

	var depts []*DeptEntry
	for rows.Next() {
		d := &DeptEntry{}
		var description, parentDept *string
		var createdAt string
		if scanErr := rows.Scan(&d.DeptID, &d.OrgID, &d.DeptName, &description, &parentDept, &d.CreatedHeight, &createdAt); scanErr != nil {
			return nil, fmt.Errorf("scan dept: %w", scanErr)
		}
		if description != nil {
			d.Description = *description
		}
		if parentDept != nil {
			d.ParentDept = *parentDept
		}
		d.CreatedAt = parseTime(createdAt)
		depts = append(depts, d)
	}
	return depts, nil
}

func (s *SQLiteStore) InsertDeptMember(ctx context.Context, member *DeptMemberEntry) error {
	_, err := s.conn.ExecContext(ctx,
		`INSERT INTO dept_members (org_id, dept_id, agent_id, clearance, role, created_height, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT DO NOTHING`,
		member.OrgID, member.DeptID, member.AgentID, int(member.Clearance), member.Role, member.CreatedHeight, formatTime(member.CreatedAt))
	if err != nil {
		return fmt.Errorf("insert dept member: %w", err)
	}
	return nil
}

func (s *SQLiteStore) RemoveDeptMember(ctx context.Context, orgID, deptID, agentID string, height int64) error {
	_, err := s.conn.ExecContext(ctx,
		`UPDATE dept_members SET removed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		WHERE org_id = ? AND dept_id = ? AND agent_id = ? AND removed_at IS NULL`,
		orgID, deptID, agentID)
	if err != nil {
		return fmt.Errorf("remove dept member: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetDeptMembers(ctx context.Context, orgID, deptID string) ([]*DeptMemberEntry, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT org_id, dept_id, agent_id, clearance, role, created_height, created_at
		FROM dept_members WHERE org_id = ? AND dept_id = ? AND removed_at IS NULL
		ORDER BY created_at`, orgID, deptID)
	if err != nil {
		return nil, fmt.Errorf("get dept members: %w", err)
	}
	defer rows.Close()

	var members []*DeptMemberEntry
	for rows.Next() {
		m := &DeptMemberEntry{}
		var clearance int
		var createdAt string
		if scanErr := rows.Scan(&m.OrgID, &m.DeptID, &m.AgentID, &clearance, &m.Role, &m.CreatedHeight, &createdAt); scanErr != nil {
			return nil, fmt.Errorf("scan dept member: %w", scanErr)
		}
		m.Clearance = ClearanceLevel(clearance) // #nosec G115 -- clearance is 0-4
		m.CreatedAt = parseTime(createdAt)
		members = append(members, m)
	}
	return members, nil
}

func (s *SQLiteStore) UpdateDeptMemberClearance(ctx context.Context, orgID, deptID, agentID string, clearance ClearanceLevel) error {
	_, err := s.conn.ExecContext(ctx,
		`UPDATE dept_members SET clearance = ?
		WHERE org_id = ? AND dept_id = ? AND agent_id = ? AND removed_at IS NULL`,
		int(clearance), orgID, deptID, agentID)
	if err != nil {
		return fmt.Errorf("update dept member clearance: %w", err)
	}
	return nil
}

// --- AgentStore implementation ---

func (s *SQLiteStore) ListAgents(ctx context.Context) ([]*AgentEntry, error) {
	rows, err := s.conn.QueryContext(ctx, `
		SELECT a.agent_id, a.name, a.role, COALESCE(a.avatar,''), COALESCE(a.boot_bio,''),
			COALESCE(a.validator_pubkey,''), COALESCE(a.node_id,''), COALESCE(a.p2p_address,''),
			a.status, a.clearance, COALESCE(a.org_id,''), COALESCE(a.dept_id,''),
			COALESCE(a.domain_access,''), COALESCE(a.bundle_path,''),
			a.first_seen, a.last_seen, a.created_at, a.removed_at,
			COALESCE((SELECT COUNT(*) FROM memories WHERE submitting_agent = a.agent_id), 0)
		FROM network_agents a
		WHERE a.status != 'removed'
		ORDER BY a.created_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("list agents: %w", err)
	}
	defer rows.Close()

	var agents []*AgentEntry
	for rows.Next() {
		a := &AgentEntry{}
		var firstSeen, lastSeen, createdAt, removedAt *string
		if scanErr := rows.Scan(&a.AgentID, &a.Name, &a.Role, &a.Avatar, &a.BootBio,
			&a.ValidatorPubkey, &a.NodeID, &a.P2PAddress, &a.Status, &a.Clearance,
			&a.OrgID, &a.DeptID, &a.DomainAccess, &a.BundlePath,
			&firstSeen, &lastSeen, &createdAt, &removedAt, &a.MemoryCount); scanErr != nil {
			return nil, fmt.Errorf("scan agent: %w", scanErr)
		}
		a.FirstSeen = parseTimePtr(firstSeen)
		a.LastSeen = parseTimePtr(lastSeen)
		if createdAt != nil {
			a.CreatedAt = parseTime(*createdAt)
		}
		a.RemovedAt = parseTimePtr(removedAt)
		agents = append(agents, a)
	}
	return agents, nil
}

func (s *SQLiteStore) GetAgent(ctx context.Context, agentID string) (*AgentEntry, error) {
	a := &AgentEntry{}
	var firstSeen, lastSeen, createdAt, removedAt *string
	err := s.conn.QueryRowContext(ctx, `
		SELECT a.agent_id, a.name, a.role, COALESCE(a.avatar,''), COALESCE(a.boot_bio,''),
			COALESCE(a.validator_pubkey,''), COALESCE(a.node_id,''), COALESCE(a.p2p_address,''),
			a.status, a.clearance, COALESCE(a.org_id,''), COALESCE(a.dept_id,''),
			COALESCE(a.domain_access,''), COALESCE(a.bundle_path,''),
			a.first_seen, a.last_seen, a.created_at, a.removed_at,
			COALESCE((SELECT COUNT(*) FROM memories WHERE submitting_agent = a.agent_id), 0)
		FROM network_agents a WHERE a.agent_id = ?`, agentID).Scan(
		&a.AgentID, &a.Name, &a.Role, &a.Avatar, &a.BootBio,
		&a.ValidatorPubkey, &a.NodeID, &a.P2PAddress, &a.Status, &a.Clearance,
		&a.OrgID, &a.DeptID, &a.DomainAccess, &a.BundlePath,
		&firstSeen, &lastSeen, &createdAt, &removedAt, &a.MemoryCount)
	if err != nil {
		return nil, fmt.Errorf("get agent: %w", err)
	}
	a.FirstSeen = parseTimePtr(firstSeen)
	a.LastSeen = parseTimePtr(lastSeen)
	if createdAt != nil {
		a.CreatedAt = parseTime(*createdAt)
	}
	a.RemovedAt = parseTimePtr(removedAt)
	return a, nil
}

func (s *SQLiteStore) CreateAgent(ctx context.Context, agent *AgentEntry) error {
	_, err := s.conn.ExecContext(ctx, `
		INSERT INTO network_agents (agent_id, name, role, avatar, boot_bio, validator_pubkey,
			node_id, p2p_address, status, clearance, org_id, dept_id, domain_access, bundle_path)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		agent.AgentID, agent.Name, agent.Role, agent.Avatar, agent.BootBio, agent.ValidatorPubkey,
		agent.NodeID, agent.P2PAddress, agent.Status, agent.Clearance, agent.OrgID, agent.DeptID,
		agent.DomainAccess, agent.BundlePath)
	if err != nil {
		return fmt.Errorf("create agent: %w", err)
	}
	return nil
}

func (s *SQLiteStore) UpdateAgent(ctx context.Context, agent *AgentEntry) error {
	_, err := s.conn.ExecContext(ctx, `
		UPDATE network_agents SET name=?, role=?, avatar=?, boot_bio=?, clearance=?,
			org_id=?, dept_id=?, domain_access=?, p2p_address=?
		WHERE agent_id=?`,
		agent.Name, agent.Role, agent.Avatar, agent.BootBio, agent.Clearance,
		agent.OrgID, agent.DeptID, agent.DomainAccess, agent.P2PAddress, agent.AgentID)
	if err != nil {
		return fmt.Errorf("update agent: %w", err)
	}
	return nil
}

func (s *SQLiteStore) RemoveAgent(ctx context.Context, agentID string) error {
	_, err := s.conn.ExecContext(ctx, `
		UPDATE network_agents SET status='removed', removed_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
		WHERE agent_id=?`, agentID)
	if err != nil {
		return fmt.Errorf("remove agent: %w", err)
	}
	return nil
}

func (s *SQLiteStore) UpdateAgentStatus(ctx context.Context, agentID, status string) error {
	_, err := s.conn.ExecContext(ctx, `UPDATE network_agents SET status=? WHERE agent_id=?`, status, agentID)
	if err != nil {
		return fmt.Errorf("update agent status: %w", err)
	}
	return nil
}

func (s *SQLiteStore) UpdateAgentLastSeen(ctx context.Context, agentID string, lastSeen time.Time) error {
	ts := lastSeen.UTC().Format("2006-01-02T15:04:05.000Z")
	_, err := s.conn.ExecContext(ctx, `UPDATE network_agents SET last_seen=?, status='active' WHERE agent_id=?`, ts, agentID)
	if err != nil {
		return fmt.Errorf("update agent last seen: %w", err)
	}
	return nil
}

func (s *SQLiteStore) RotateAgentKey(ctx context.Context, oldAgentID string) (string, []byte, error) {
	// Generate new Ed25519 keypair
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", nil, fmt.Errorf("generate key: %w", err)
	}
	newAgentID := hex.EncodeToString(pub)
	newValidatorPubkey := base64.StdEncoding.EncodeToString(pub)
	seed := priv.Seed()

	// Run the update atomically — agent record + memory re-attribution
	doRotate := func(q sqlQuerier) error {
		// 1. Verify old agent exists and is not removed
		var status string
		if scanErr := q.QueryRowContext(ctx, `SELECT status FROM network_agents WHERE agent_id=?`, oldAgentID).Scan(&status); scanErr != nil {
			return fmt.Errorf("agent not found: %s", oldAgentID)
		}
		if status == "removed" {
			return fmt.Errorf("cannot rotate key for removed agent %s", oldAgentID)
		}

		// 2. Insert new agent row (copy of old, with new keys)
		_, err2 := q.ExecContext(ctx, `
			INSERT INTO network_agents (agent_id, name, role, avatar, boot_bio, validator_pubkey,
				node_id, p2p_address, status, clearance, org_id, dept_id, domain_access, bundle_path, first_seen, created_at)
			SELECT ?, name, role, avatar, boot_bio, ?,
				node_id, p2p_address, status, clearance, org_id, dept_id, domain_access, '',
				first_seen, created_at
			FROM network_agents WHERE agent_id=?`,
			newAgentID, newValidatorPubkey, oldAgentID)
		if err2 != nil {
			return fmt.Errorf("insert rotated agent: %w", err2)
		}

		// 3. Re-attribute all memories to the new agent ID
		_, err2 = q.ExecContext(ctx, `UPDATE memories SET submitting_agent=? WHERE submitting_agent=?`, newAgentID, oldAgentID)
		if err2 != nil {
			return fmt.Errorf("re-attribute memories: %w", err2)
		}

		// 4. Mark old agent as removed with audit note
		_, err2 = q.ExecContext(ctx, `
			UPDATE network_agents SET status='removed',
				removed_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
			WHERE agent_id=?`, oldAgentID)
		if err2 != nil {
			return fmt.Errorf("retire old agent: %w", err2)
		}

		// 5. Log the rotation in redeployment_log for audit
		_, err2 = q.ExecContext(ctx, `
			INSERT INTO redeployment_log (operation, agent_id, phase, status, details, started_at)
			VALUES ('rotate_key', ?, 'KEY_ROTATED', 'completed', ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))`,
			newAgentID, fmt.Sprintf("rotated from %s to %s", oldAgentID, newAgentID))
		if err2 != nil {
			return fmt.Errorf("log rotation: %w", err2)
		}

		return nil
	}

	// If we have a *sql.DB (not already in a tx), wrap in transaction
	if s.db != nil {
		tx, txErr := s.db.BeginTx(ctx, nil)
		if txErr != nil {
			return "", nil, fmt.Errorf("begin tx: %w", txErr)
		}
		defer tx.Rollback() //nolint:errcheck

		if err2 := doRotate(tx); err2 != nil {
			return "", nil, err2
		}
		if err2 := tx.Commit(); err2 != nil {
			return "", nil, fmt.Errorf("commit: %w", err2)
		}
	} else {
		// Already in a transaction
		if err2 := doRotate(s.conn); err2 != nil {
			return "", nil, err2
		}
	}

	return newAgentID, seed, nil
}

func (s *SQLiteStore) AcquireRedeployLock(ctx context.Context, lockedBy, operation string, ttl time.Duration) error {
	now := time.Now().UTC()
	expires := now.Add(ttl)
	// Try to insert the singleton lock row. If it exists, check if expired.
	_, err := s.conn.ExecContext(ctx, `
		INSERT INTO redeployment_lock (id, locked_by, locked_at, operation, expires_at)
		VALUES (1, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			locked_by=excluded.locked_by, locked_at=excluded.locked_at,
			operation=excluded.operation, expires_at=excluded.expires_at
		WHERE redeployment_lock.expires_at < ?`,
		lockedBy, now.Format(time.RFC3339), operation, expires.Format(time.RFC3339),
		now.Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("acquire redeploy lock: %w", err)
	}
	// Verify we actually hold the lock
	var holder string
	if scanErr := s.conn.QueryRowContext(ctx, `SELECT locked_by FROM redeployment_lock WHERE id=1`).Scan(&holder); scanErr != nil {
		return fmt.Errorf("verify lock: %w", scanErr)
	}
	if holder != lockedBy {
		return fmt.Errorf("lock held by %s", holder)
	}
	return nil
}

func (s *SQLiteStore) ReleaseRedeployLock(ctx context.Context) error {
	_, err := s.conn.ExecContext(ctx, `DELETE FROM redeployment_lock WHERE id=1`)
	return err
}

func (s *SQLiteStore) GetRedeployLock(ctx context.Context) (*RedeploymentLock, error) {
	lock := &RedeploymentLock{}
	var lockedAt, expiresAt string
	err := s.conn.QueryRowContext(ctx, `SELECT locked_by, locked_at, operation, expires_at FROM redeployment_lock WHERE id=1`).
		Scan(&lock.LockedBy, &lockedAt, &lock.Operation, &expiresAt)
	if err != nil {
		return nil, err // sql.ErrNoRows if no lock
	}
	lock.LockedAt = parseTime(lockedAt)
	lock.ExpiresAt = parseTime(expiresAt)
	return lock, nil
}

func (s *SQLiteStore) InsertRedeployLog(ctx context.Context, entry *RedeploymentLogEntry) error {
	now := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	res, err := s.conn.ExecContext(ctx, `
		INSERT INTO redeployment_log (operation, agent_id, phase, status, details, sqlite_backup, genesis_backup, started_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		entry.Operation, entry.AgentID, entry.Phase, entry.Status, entry.Details,
		entry.SQLiteBackup, entry.GenesisBackup, now)
	if err != nil {
		return fmt.Errorf("insert redeploy log: %w", err)
	}
	id, _ := res.LastInsertId()
	entry.ID = id
	return nil
}

func (s *SQLiteStore) GetRedeployLog(ctx context.Context, operation string) ([]*RedeploymentLogEntry, error) {
	rows, err := s.conn.QueryContext(ctx, `
		SELECT id, operation, agent_id, phase, status, COALESCE(details,''),
			COALESCE(sqlite_backup,''), COALESCE(genesis_backup,''),
			started_at, completed_at, COALESCE(error,'')
		FROM redeployment_log WHERE operation=? ORDER BY id ASC`, operation)
	if err != nil {
		return nil, fmt.Errorf("get redeploy log: %w", err)
	}
	defer rows.Close()

	var entries []*RedeploymentLogEntry
	for rows.Next() {
		e := &RedeploymentLogEntry{}
		var startedAt, completedAt *string
		if scanErr := rows.Scan(&e.ID, &e.Operation, &e.AgentID, &e.Phase, &e.Status,
			&e.Details, &e.SQLiteBackup, &e.GenesisBackup,
			&startedAt, &completedAt, &e.Error); scanErr != nil {
			return nil, fmt.Errorf("scan redeploy log: %w", scanErr)
		}
		e.StartedAt = parseTimePtr(startedAt)
		e.CompletedAt = parseTimePtr(completedAt)
		entries = append(entries, e)
	}
	return entries, nil
}

func (s *SQLiteStore) UpdateRedeployLog(ctx context.Context, id int64, status, errorMsg string) error {
	now := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	_, err := s.conn.ExecContext(ctx, `
		UPDATE redeployment_log SET status=?, completed_at=?, error=? WHERE id=?`,
		status, now, errorMsg, id)
	return err
}

// --- Close & Ping ---

func (s *SQLiteStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *SQLiteStore) Ping(ctx context.Context) error {
	if s.db != nil {
		return s.db.PingContext(ctx)
	}
	return nil
}

// RunInTx executes fn within a SQLite transaction. All writes through
// the tx-scoped OffchainStore are atomic — either all succeed or all roll back.
func (s *SQLiteStore) RunInTx(ctx context.Context, fn func(tx OffchainStore) error) error {
	if s.db == nil {
		// Already in a transaction — execute directly.
		return fn(s)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	txStore := &SQLiteStore{conn: tx, dbPath: s.dbPath}
	if err := fn(txStore); err != nil {
		return err
	}
	return tx.Commit()
}

// --- Preferences ---

// GetPreference returns a single preference value by key.
func (s *SQLiteStore) GetPreference(ctx context.Context, key string) (string, error) {
	var value string
	err := s.conn.QueryRowContext(ctx, `SELECT value FROM preferences WHERE key = ?`, key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return value, err
}

// SetPreference sets a single preference key-value pair.
func (s *SQLiteStore) SetPreference(ctx context.Context, key, value string) error {
	_, err := s.conn.ExecContext(ctx,
		`INSERT INTO preferences (key, value, updated_at) VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
		ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`,
		key, value)
	return err
}

// GetAllPreferences returns all preferences as a map.
func (s *SQLiteStore) GetAllPreferences(ctx context.Context) (map[string]string, error) {
	rows, err := s.conn.QueryContext(ctx, `SELECT key, value FROM preferences`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	prefs := make(map[string]string)
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		prefs[k] = v
	}
	return prefs, rows.Err()
}

// GetCleanupCandidates returns memories eligible for auto-deprecation.
// It finds: (1) observations older than ttlDays, (2) memories with computed confidence below threshold.
func (s *SQLiteStore) GetCleanupCandidates(ctx context.Context, observationTTLDays int, sessionTTLDays int, staleThreshold float64) ([]*memory.MemoryRecord, error) {
	// Find non-deprecated observations and low-confidence memories
	rows, err := s.conn.QueryContext(ctx,
		`SELECT memory_id, submitting_agent, content, content_hash, embedding, embedding_hash,
			memory_type, domain_tag, provider, confidence_score, status, parent_hash, created_at, committed_at, deprecated_at
		FROM memories
		WHERE status NOT IN ('deprecated')
		AND (
			(memory_type = 'observation' AND created_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ? || ' days'))
			OR (memory_type = 'observation' AND domain_tag = 'session-context' AND created_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ? || ' days'))
		)
		ORDER BY created_at ASC
		LIMIT 500`,
		fmt.Sprintf("-%d", observationTTLDays),
		fmt.Sprintf("-%d", sessionTTLDays))
	if err != nil {
		return nil, fmt.Errorf("query cleanup candidates: %w", err)
	}
	defer rows.Close()

	var records []*memory.MemoryRecord
	for rows.Next() {
		rec, err := s.scanMemoryRow(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, rows.Err()
}

// DeprecateMemories batch-deprecates memories by IDs.
func (s *SQLiteStore) DeprecateMemories(ctx context.Context, memoryIDs []string) (int, error) {
	if len(memoryIDs) == 0 {
		return 0, nil
	}
	placeholders := make([]string, len(memoryIDs))
	args := make([]any, len(memoryIDs))
	for i, id := range memoryIDs {
		placeholders[i] = "?"
		args[i] = id
	}
	query := fmt.Sprintf(
		`UPDATE memories SET status = 'deprecated', deprecated_at = strftime('%%Y-%%m-%%dT%%H:%%M:%%fZ', 'now')
		WHERE memory_id IN (%s) AND status != 'deprecated'`,
		strings.Join(placeholders, ","))
	result, err := s.conn.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("deprecate memories: %w", err)
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}

// scanMemoryRow scans a single memory row from a *sql.Rows.
func (s *SQLiteStore) scanMemoryRow(rows *sql.Rows) (*memory.MemoryRecord, error) {
	var r memory.MemoryRecord
	var mt, st, createdAt string
	var embData []byte
	var parentHash, committedAt, deprecatedAt *string

	err := rows.Scan(&r.MemoryID, &r.SubmittingAgent, &r.Content, &r.ContentHash,
		&embData, &r.EmbeddingHash, &mt, &r.DomainTag, &r.Provider, &r.ConfidenceScore,
		&st, &parentHash, &createdAt, &committedAt, &deprecatedAt)
	if err != nil {
		return nil, fmt.Errorf("scan memory: %w", err)
	}

	r.MemoryType = memory.MemoryType(mt)
	r.Status = memory.MemoryStatus(st)

	if decContent, decErr := s.decryptContent(r.Content); decErr == nil {
		r.Content = decContent
	}
	decEmb, _ := s.decryptEmbedding(embData)
	r.Embedding = decodeEmbedding(decEmb)

	r.CreatedAt = parseTime(createdAt)
	r.CommittedAt = parseTimePtr(committedAt)
	r.DeprecatedAt = parseTimePtr(deprecatedAt)
	if parentHash != nil {
		r.ParentHash = *parentHash
	}

	return &r, nil
}
