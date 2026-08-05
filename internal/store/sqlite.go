package store

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite" // Pure Go SQLite driver

	"github.com/l33tdawg/sage/internal/embedding"
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
	conn                  sqlQuerier // either *sql.DB or *sql.Tx
	db                    *sql.DB    // nil for tx-scoped stores
	dbPath                string
	vault                 atomic.Pointer[vault.Vault] // nil = no encryption; hot-swapped when CEREBRUM unlocks
	vaultExpected         atomic.Bool                 // true = encryption should be active; reject writes if vault nil
	vaultGeneration       *atomic.Uint64              // shared by tx clones; defeats lock/unlock ABA in audited snapshot tokens
	decryptWarnOnce       sync.Once                   // gates the one-time decryption failure warning
	writeMu               sync.Mutex                  // serializes ALL writes to prevent SQLITE_BUSY
	syncPolicyGate        *sync.RWMutex               // shared with tx clones; linearizes consent vs egress
	syncOriginGate        *sync.RWMutex               // shared with tx clones; linearizes copy provenance vs re-forward scans
	agentContactGate      *sync.RWMutex               // shared with tx clones; linearizes advertised agent identity/availability
	agentContactWriteHeld bool                        // true only on a RunInAgentContactTx-scoped clone
	// federationAuthorizationMutationHook publishes/cancels the bounded
	// per-peer linked delivery lease before a local consent, guest-link, or
	// agent-availability mutation. Empty chain means the mutation can affect
	// every linked peer.
	federationAuthorizationMutationHooks *authorizationMutationHookState

	// Optional cross-encoder reranker; nil = skip the rerank pass and return
	// the RRF-sorted candidates directly. Wired at server startup via
	// SetReranker after ResolveRerankerConfig+BuildReranker, so the store
	// stays agnostic to whether an operator turned the reranker on.
	reranker           embedding.Reranker
	rerankerOversample int          // RRF candidate pool size factor when reranker is active
	rerankerMu         sync.RWMutex // guards reranker + rerankerOversample; they are hot-swappable at runtime via SetReranker (Settings > Recall toggle), so live recall readers must synchronize
}

// writeExecContext wraps ExecContext with writeMu for standalone (non-tx) writes.
// Inside a transaction (db == nil), the mutex was already acquired by RunInTx,
// so we skip it to avoid deadlock.
func (s *SQLiteStore) writeExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if s.db != nil {
		s.writeMu.Lock()
		defer s.writeMu.Unlock()
	}
	return s.conn.ExecContext(ctx, query, args...) //nolint:wrapcheck // intentional pass-through
}

// LockProjectionPublicationRead holds every typed SQLite mutation behind the
// store's existing write serializer while a caller reads a revision token and
// publishes bytes derived from that exact SQL snapshot generation. Acquire
// this before BadgerStore.LockProjectionPublicationRead to preserve the
// consensus SQL-then-Badger lock order.
func (s *SQLiteStore) LockProjectionPublicationRead() func() {
	if s == nil || s.db == nil {
		return func() {}
	}
	s.writeMu.Lock()
	return s.writeMu.Unlock
}

// beginTxLocked opens a write transaction while holding writeMu, and returns
// the tx plus an unlock func the caller must defer-run after tx.Rollback /
// tx.Commit. Use this instead of raw `s.db.BeginTx` for any method that
// writes; raw BeginTx bypasses writeMu and races both writeExecContext and
// other transactions, reintroducing SQLITE_BUSY on rollback-journal builds
// and excess WAL contention on WAL builds.
func (s *SQLiteStore) beginTxLocked(ctx context.Context) (*sql.Tx, func(), error) {
	s.writeMu.Lock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.writeMu.Unlock()
		return nil, func() {}, err //nolint:wrapcheck // callers wrap
	}
	return tx, s.writeMu.Unlock, nil
}

// encPrefix marks content as encrypted (prepended to base64 ciphertext).
const encPrefix = "enc::"

// ErrTextSearchVaultEncrypted is returned by SearchByText when the store has
// an attached vault. Content is AES-256-GCM encrypted at rest, which means
// FTS5 cannot text-index it. The fix is upstream: REST callers should pick
// semantic search via /v1/embed/info (which now reports semantic=true while
// the vault is active). This error remains for direct REST clients that
// hit /v1/memory/search anyway, and for the MCP belt-and-braces retry path
// in internal/mcp/tools.go which detects this marker substring.
const ErrTextSearchVaultEncryptedMsg = "text search unavailable: content is vault-encrypted; this node is in semantic-only mode"

// SetVault attaches an encryption vault to the store.
// When set, memory content is encrypted on write and decrypted on read.
func (s *SQLiteStore) SetVault(v *vault.Vault) {
	s.vault.Store(v)
	if s.vaultGeneration != nil {
		s.vaultGeneration.Add(1)
	}
}

// SetReranker attaches an optional cross-encoder reranker used by
// SearchHybrid to refine the top-K ordering. Pass nil to disable; the
// hybrid path falls back to plain RRF ordering when no reranker is set.
// `oversample` is the multiplier applied to TopK when sizing the candidate
// pool fed to the reranker; values <= 0 fall through to the default 2.
func (s *SQLiteStore) SetReranker(r embedding.Reranker, oversample int) {
	if oversample <= 0 {
		oversample = 2
	}
	s.rerankerMu.Lock()
	s.reranker = r
	s.rerankerOversample = oversample
	s.rerankerMu.Unlock()
}

// RerankerInfo reports the optional reranker configuration to
// /v1/dashboard/health. enabled is true whenever a reranker is attached; when
// it is the HTTP flavour we also surface the configured model + upstream URL.
// The reranker is hot-swappable at runtime (SetReranker), so snapshot it under
// the read lock before inspecting it.
func (s *SQLiteStore) RerankerInfo() (bool, string, string) {
	s.rerankerMu.RLock()
	r := s.reranker
	s.rerankerMu.RUnlock()
	if hr, ok := r.(*embedding.HTTPReranker); ok {
		return true, hr.Model(), hr.URL()
	}
	return r != nil, "", ""
}

// VaultActive reports whether content is encrypted at rest by an attached
// vault. When true, FTS5 text search is unavailable (encrypted content can't
// be text-indexed) and callers MUST use semantic similarity search instead.
// REST handlers like /v1/embed/info use this to force semantic mode on for
// vault-active nodes so MCP clients don't get routed to the broken FTS5 path.
func (s *SQLiteStore) VaultActive() bool {
	return s.vault.Load() != nil
}

// VaultExpected marks that encryption should be active. When true and the vault
// is nil (locked), writes are rejected rather than silently going plaintext.
func (s *SQLiteStore) SetVaultExpected(expected bool) {
	changed := s.vaultExpected.Swap(expected) != expected
	if changed && s.vaultGeneration != nil {
		s.vaultGeneration.Add(1)
	}
}

// VaultGeneration advances on every vault publication and expected-state
// transition, including lock->unlock ABA and same-state key rotation.
func (s *SQLiteStore) VaultGeneration() uint64 {
	if s == nil || s.vaultGeneration == nil {
		return 0
	}
	return s.vaultGeneration.Load()
}

// VaultLocked reports whether writes would currently be rejected because
// encryption is expected but the vault has not been unlocked. Callers that
// stage work (e.g. sync admission) use this to defer instead of failing.
func (s *SQLiteStore) VaultLocked() bool {
	return s.vaultExpected.Load() && s.vault.Load() == nil
}

// encryptContent encrypts a string if the vault is set.
// Returns the original string if no vault and encryption is not expected.
// Returns an error if encryption is expected but vault is locked.
func (s *SQLiteStore) encryptContent(plaintext string) (string, error) {
	activeVault := s.vault.Load()
	if activeVault == nil {
		if s.vaultExpected.Load() {
			return "", fmt.Errorf("vault is locked — unlock encryption before storing memories")
		}
		return plaintext, nil
	}
	encrypted, err := activeVault.EncryptString(plaintext)
	if err != nil {
		return "", fmt.Errorf("encrypt content: %w", err)
	}
	return encPrefix + base64.StdEncoding.EncodeToString(encrypted), nil
}

// decryptContent decrypts a string if it's encrypted.
// Returns the original string if not encrypted or no vault.
// VaultLockedPlaceholder is returned for encrypted content when no vault key is
// available. It is NOT real content — callers that need plaintext (e.g. re-embed)
// must treat it as undecryptable, never store or embed it.
const VaultLockedPlaceholder = "[encrypted — vault locked]"

func (s *SQLiteStore) decryptContent(stored string) (string, error) {
	if !strings.HasPrefix(stored, encPrefix) {
		return stored, nil // not encrypted
	}
	activeVault := s.vault.Load()
	if activeVault == nil {
		return VaultLockedPlaceholder, nil
	}
	data, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(stored, encPrefix))
	if err != nil {
		return "", fmt.Errorf("decode encrypted content: %w", err)
	}
	plaintext, decErr := activeVault.DecryptString(data)
	if decErr != nil {
		// Log once per process lifetime — this typically means the vault key
		// doesn't match the key used to encrypt these memories (e.g., the vault
		// was re-initialized). Logging every row would be too noisy.
		s.decryptWarnOnce.Do(func() {
			fmt.Fprintf(os.Stderr, "SAGE WARNING: failed to decrypt memory content — vault key may not match the key used to encrypt stored memories. Use the recovery key to restore the original vault, or deprecated affected memories.\n")
		})
		return stored, decErr // return raw enc:: content so caller sees it's encrypted
	}
	return plaintext, nil
}

// encryptEmbedding encrypts embedding bytes if the vault is set.
func (s *SQLiteStore) encryptEmbedding(data []byte) ([]byte, error) {
	activeVault := s.vault.Load()
	if activeVault == nil || data == nil {
		if s.vaultExpected.Load() && data != nil {
			return nil, fmt.Errorf("vault is locked — unlock encryption before storing embeddings")
		}
		return data, nil
	}
	return activeVault.Encrypt(data)
}

// decryptEmbedding decrypts embedding bytes if vault is set and data looks encrypted.
// Encrypted embeddings are longer than raw ones (nonce + tag overhead).
func (s *SQLiteStore) decryptEmbedding(data []byte) ([]byte, error) {
	activeVault := s.vault.Load()
	if activeVault == nil || data == nil {
		return data, nil
	}
	// Try to decrypt — if it fails, it's likely unencrypted legacy data.
	decrypted, err := activeVault.Decrypt(data)
	if err != nil {
		return data, nil // return as-is for backward compatibility
	}
	return decrypted, nil
}

// NewSQLiteStore creates a new SQLite-backed store.
func NewSQLiteStore(ctx context.Context, dbPath string) (*SQLiteStore, error) {
	// modernc.org/sqlite uses `_pragma=name(value)` syntax. The older
	// `_name=value` form (mattn/go-sqlite3) is silently ignored, which
	// means prior deployments ran in rollback-journal mode with a zero
	// busy timeout — every concurrent writer contention surfaced as
	// SQLITE_BUSY instead of waiting.
	dsn := dbPath +
		"?_pragma=journal_mode(WAL)" +
		"&_pragma=busy_timeout(15000)" +
		"&_pragma=synchronous(NORMAL)" +
		"&_pragma=foreign_keys(ON)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	// Belt-and-braces: re-apply the pragmas via explicit queries so a
	// DSN-parsing change in a future driver version can't silently
	// regress this. PRAGMA statements are no-ops when already applied.
	for _, p := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=15000",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA foreign_keys=ON",
	} {
		if _, pragErr := db.ExecContext(ctx, p); pragErr != nil {
			_ = db.Close()
			return nil, fmt.Errorf("apply %s: %w", p, pragErr)
		}
	}

	s := &SQLiteStore{
		conn: db, db: db, dbPath: dbPath,
		vaultGeneration: &atomic.Uint64{},
		syncPolicyGate:  &sync.RWMutex{}, syncOriginGate: &sync.RWMutex{},
		agentContactGate:                     &sync.RWMutex{},
		federationAuthorizationMutationHooks: &authorizationMutationHookState{},
	}
	if err := s.initSchema(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}

	return s, nil
}

var defaultDomainSeeds = []struct {
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

// RequirePristineStateSyncProjection rejects every application-owned row in a
// receiving node's off-chain database except the exact canonical domain seeds
// installed by initSchema. State sync rebuilds canonical scoped content by
// upsert; allowing any other pre-existing row would preserve stale local
// memories, identities, policy, credentials, or federation state that the
// trusted AppHash did not authorize. FTS5 shadow tables are internal storage;
// the virtual memories_fts table itself is checked.
func (s *SQLiteStore) RequirePristineStateSyncProjection(ctx context.Context) error {
	if s == nil || s.conn == nil {
		return errors.New("state sync projection store is required")
	}
	rows, err := s.conn.QueryContext(ctx, `
		SELECT name FROM sqlite_master
		WHERE type='table' AND name NOT LIKE 'sqlite_%'
		ORDER BY name`)
	if err != nil {
		return fmt.Errorf("list state sync projection tables: %w", err)
	}
	tables := make([]string, 0, 32)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan state sync projection table: %w", err)
		}
		if strings.HasPrefix(name, "memories_fts_") {
			continue
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("iterate state sync projection tables: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close state sync projection table list: %w", err)
	}
	for _, table := range tables {
		if table == "domains" {
			if err := s.requireDefaultStateSyncDomains(ctx); err != nil {
				return err
			}
			continue
		}
		if table == "memory_projection_revision" {
			var id, revision int64
			if err := s.conn.QueryRowContext(ctx,
				`SELECT singleton, revision FROM memory_projection_revision`,
			).Scan(&id, &revision); err != nil || id != 1 || revision != 0 {
				return errors.New("state sync receiving requires a pristine memory projection revision")
			}
			continue
		}
		if table == "graph_projection_revision" {
			var id, revision int64
			if err := s.conn.QueryRowContext(ctx,
				`SELECT singleton, revision FROM graph_projection_revision`,
			).Scan(&id, &revision); err != nil || id != 1 || revision != 0 {
				return errors.New("state sync receiving requires a pristine graph projection revision")
			}
			continue
		}
		quoted := `"` + strings.ReplaceAll(table, `"`, `""`) + `"`
		var populated int
		if err := s.conn.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM `+quoted+` LIMIT 1)`).Scan(&populated); err != nil {
			return fmt.Errorf("inspect state sync projection table %q: %w", table, err)
		}
		if populated != 0 {
			return fmt.Errorf("state sync receiving requires an empty off-chain projection; table %q is populated", table)
		}
	}
	return nil
}

func (s *SQLiteStore) requireDefaultStateSyncDomains(ctx context.Context) error {
	rows, err := s.conn.QueryContext(ctx, `SELECT domain_tag, decay_rate FROM domains ORDER BY domain_tag`)
	if err != nil {
		return fmt.Errorf("inspect state sync default domains: %w", err)
	}
	defer func() { _ = rows.Close() }()
	actual := make(map[string]float64, len(defaultDomainSeeds))
	for rows.Next() {
		var tag string
		var rate float64
		if err := rows.Scan(&tag, &rate); err != nil {
			return fmt.Errorf("scan state sync default domain: %w", err)
		}
		if _, duplicate := actual[tag]; duplicate {
			return errors.New("state sync receiving default domains contain a duplicate")
		}
		actual[tag] = rate
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate state sync default domains: %w", err)
	}
	if len(actual) != len(defaultDomainSeeds) {
		return errors.New("state sync receiving requires the exact canonical default domains")
	}
	for _, seed := range defaultDomainSeeds {
		if rate, ok := actual[seed.tag]; !ok || rate != seed.rate {
			return errors.New("state sync receiving requires the exact canonical default domains")
		}
	}
	return nil
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
		memory_type      TEXT NOT NULL CHECK (memory_type IN ('fact', 'observation', 'inference', 'task')),
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
	CREATE INDEX IF NOT EXISTS idx_memories_created_at ON memories(created_at);

	CREATE TABLE IF NOT EXISTS legacy_memory_recovery (
		memory_id TEXT PRIMARY KEY,
		machine_reason TEXT NOT NULL,
		projection_revision INTEGER NOT NULL,
		first_seen_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		last_seen_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		resolved_at TEXT
	);
	CREATE TABLE IF NOT EXISTS legacy_memory_recovery_disposition (
		memory_id TEXT PRIMARY KEY,
		disposition TEXT NOT NULL CHECK (disposition = 'deprecated'),
		machine_reason TEXT NOT NULL,
		projection_revision INTEGER NOT NULL,
		authorized_by TEXT NOT NULL,
		created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	);
	CREATE TABLE IF NOT EXISTS legacy_memory_recovery_assignment (
		memory_id TEXT PRIMARY KEY,
		target_agent_id TEXT NOT NULL,
		machine_reason TEXT NOT NULL,
		projection_revision INTEGER NOT NULL,
		authorized_by TEXT NOT NULL,
		created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	);

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

	-- Presence means the numeric evidence rows are only a reconstructed
	-- canonical lower bound, not a complete lifetime audit projection. Absence
	-- preserves backward compatibility for native/existing projections.
	CREATE TABLE IF NOT EXISTS memory_evidence_projection_incomplete (
		memory_id TEXT PRIMARY KEY REFERENCES memories(memory_id) ON DELETE CASCADE
	);

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

	CREATE TABLE IF NOT EXISTS memory_links (
		source_id  TEXT NOT NULL REFERENCES memories(memory_id),
		target_id  TEXT NOT NULL REFERENCES memories(memory_id),
		link_type  TEXT NOT NULL DEFAULT 'related',
		created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		PRIMARY KEY (source_id, target_id)
	);
	CREATE INDEX IF NOT EXISTS idx_memory_links_target ON memory_links(target_id);

	CREATE TABLE IF NOT EXISTS memory_tags (
		memory_id  TEXT NOT NULL REFERENCES memories(memory_id) ON DELETE CASCADE,
		tag        TEXT NOT NULL,
		created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		PRIMARY KEY (memory_id, tag)
	);
	CREATE INDEX IF NOT EXISTS idx_memory_tags_tag ON memory_tags(tag);

	CREATE TABLE IF NOT EXISTS governance_proposals (
		proposal_id     TEXT PRIMARY KEY,
		operation       TEXT NOT NULL,
		target_agent_id TEXT NOT NULL,
		target_pubkey   TEXT,
		target_power    INTEGER,
		proposer_id     TEXT NOT NULL,
		status          TEXT NOT NULL DEFAULT 'voting',
		created_height  INTEGER NOT NULL,
		expiry_height   INTEGER NOT NULL,
		executed_height INTEGER,
		reason          TEXT,
		created_at      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	);

	CREATE TABLE IF NOT EXISTS governance_votes (
		proposal_id  TEXT NOT NULL,
		validator_id TEXT NOT NULL,
		decision     TEXT NOT NULL,
		height       INTEGER NOT NULL,
		PRIMARY KEY (proposal_id, validator_id)
	);

	-- Exactly-once receipt for one consensus block's complete off-chain
	-- projection transaction. This is intentionally separate from individual
	-- table identities: two legitimate events may have identical payloads, while
	-- an exact CometBFT block replay must apply the complete batch zero times.
	CREATE TABLE IF NOT EXISTS abci_projection_batches (
		block_height INTEGER PRIMARY KEY,
		app_hash     BLOB NOT NULL CHECK (length(app_hash) = 32),
		created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	);
	`

	if _, err := s.writeExecContext(ctx, schema); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	// Migration: add provider column if missing.
	s.migrateProvider(ctx)

	// Migration: add task support (task_status column + update CHECK constraint).
	s.migrateTaskSupport(ctx)
	s.migrateTaskAssignee(ctx)
	s.migrateTaskPickup(ctx)
	s.migrateTaskStatusUpdatedAt(ctx)
	s.migrateTaskBoardPosition(ctx)
	if err := s.migrateTaskAssignmentNotifications(ctx); err != nil {
		return fmt.Errorf("migrate task assignment notifications: %w", err)
	}

	// Migration: add embedding_provider column. MUST run AFTER migrateTaskSupport,
	// which recreates the memories table from a fixed schema (and would otherwise
	// drop a column added before it).
	s.migrateEmbeddingProvider(ctx)

	// Migration: add app-v17 two-phase-challenge columns (disputed_height/quorum).
	// MUST also run AFTER migrateTaskSupport for the same reason.
	s.migrateDisputed(ctx)

	// Schema migrations — add columns to network_agents that didn't exist in earlier versions.
	agentMigrations := []string{
		"ALTER TABLE network_agents ADD COLUMN on_chain_height INTEGER DEFAULT 0",
		"ALTER TABLE network_agents ADD COLUMN visible_agents TEXT DEFAULT ''",
		"ALTER TABLE network_agents ADD COLUMN capabilities INTEGER DEFAULT 0",
		"ALTER TABLE network_agents ADD COLUMN provider TEXT DEFAULT ''",
		"ALTER TABLE network_agents ADD COLUMN claim_token TEXT DEFAULT ''",
		"ALTER TABLE network_agents ADD COLUMN claim_expires_at TEXT",
		"ALTER TABLE network_agents ADD COLUMN registered_name TEXT DEFAULT ''",
	}
	for _, m := range agentMigrations {
		_, _ = s.writeExecContext(ctx, m) // Ignore "duplicate column" errors for idempotency
	}
	// Exact federated contact lookup must stay index-backed while federation
	// authorization leases are held. These are deliberately separate expression
	// indexes: an OR across name, registered name, and provider is queried as
	// three small bounded lookups below, rather than as one broad roster scan.
	for _, index := range []string{
		"CREATE INDEX IF NOT EXISTS idx_network_agents_contact_name ON network_agents(name COLLATE NOCASE, agent_id) WHERE status != 'removed'",
		"CREATE INDEX IF NOT EXISTS idx_network_agents_contact_registered_name ON network_agents(registered_name COLLATE NOCASE, agent_id) WHERE status != 'removed'",
		"CREATE INDEX IF NOT EXISTS idx_network_agents_contact_provider ON network_agents(provider COLLATE NOCASE, agent_id) WHERE status != 'removed'",
		"CREATE INDEX IF NOT EXISTS idx_network_agents_claim_token ON network_agents(claim_token) WHERE claim_token != ''",
	} {
		if _, err := s.writeExecContext(ctx, index); err != nil {
			return fmt.Errorf("create federated contact lookup index: %w", err)
		}
	}

	// Migration: add pipeline_messages table.
	s.migratePipeline(ctx)
	if err := s.migratePipelineTerminalRetention(ctx); err != nil {
		return fmt.Errorf("migrate pipeline terminal retention: %w", err)
	}
	if err := s.migrateMessages(ctx); err != nil {
		return fmt.Errorf("migrate canonical messages: %w", err)
	}
	s.migratePipelineTransport(ctx)
	if _, err := s.writeExecContext(ctx, `UPDATE pipeline_transport_outbox
		SET expires_at=strftime('%Y-%m-%dT%H:%M:%fZ',created_at,'+100 years')
		WHERE pipe_id LIKE 'msg-%' AND state='pending'`); err != nil {
		return fmt.Errorf("extend canonical message transport retention: %w", err)
	}
	if err := s.migratePipelineV23SecurityColumns(ctx); err != nil {
		return fmt.Errorf("migrate pipeline v23 authorization columns: %w", err)
	}
	if err := s.migrateFederatedPipelineReceiptsV2(ctx); err != nil {
		return fmt.Errorf("migrate federated pipeline receipts v2: %w", err)
	}

	// Migration: add mcp_tokens table for HTTP MCP transport bearer auth.
	if err := s.migrateMCPTokens(ctx); err != nil {
		return fmt.Errorf("migrate MCP tokens: %w", err)
	}

	// Migration: add mcp_auth_codes table for the OAuth 2.0 + PKCE wrapper
	// in front of bearer auth (v6.7.2 — ChatGPT MCP connector compat).
	if err := s.migrateMCPAuthCodes(ctx); err != nil {
		return fmt.Errorf("migrate OAuth auth codes: %w", err)
	}

	// Migration: add oauth_clients table for persisted DCR registrations
	// (v6.8.0 — required so /oauth/authorize can validate redirect_uri).
	if err := s.migrateOAuthClients(ctx); err != nil {
		return fmt.Errorf("migrate OAuth clients: %w", err)
	}

	// Migration: add domain-sync tables (v11.5 — sync_domains consent,
	// sync_outbox store-and-forward queue, sync_origin admission ledger).
	s.migrateSyncTables(ctx)

	// Migration: add the v11.8 synchronization-GROUP overlay tables (off-consensus:
	// sync_group roster, sync_group_member, sync_group_domain, sync_group_log audit
	// journal, sync_tombstone) + the sync_control.group_id binding column.
	s.migrateSyncGroupTables(ctx)

	// Directional peer-RBAC capability snapshots. Header presence is meaningful:
	// a present header with zero domain rows is explicit deny-all, not legacy.
	s.migratePeerRBACPolicies(ctx)

	// Per-contact inbound work-request consent. This stays local/off-consensus,
	// but every row is bound to one exact peer-RBAC/JOIN generation.
	if err := s.migrateFederatedPipeContacts(ctx); err != nil {
		return fmt.Errorf("migrate federated pipe contacts: %w", err)
	}
	if err := s.migrateFederatedLinkedMessageConsent(ctx); err != nil {
		return fmt.Errorf("migrate federated linked-message consent: %w", err)
	}

	// app-v23 read-only guest links. These are signed node-local capability
	// snapshots; local group membership remains owned by the consensus layer.
	if err := s.migrateFederatedGroupGuests(ctx); err != nil {
		return fmt.Errorf("migrate federated group guests: %w", err)
	}
	if err := s.migrateFederatedQueryChallenges(ctx); err != nil {
		return fmt.Errorf("migrate federated query challenges: %w", err)
	}
	if err := s.FederationV23SchemaReady(ctx); err != nil {
		return fmt.Errorf("verify federation v23 schema: %w", err)
	}

	// FTS5 full-text search index on memory content.
	// Used as a fallback when semantic embeddings are unavailable (hash mode).
	_, _ = s.writeExecContext(ctx, `
		CREATE VIRTUAL TABLE IF NOT EXISTS memories_fts USING fts5(
			memory_id UNINDEXED,
			content,
			domain_tag UNINDEXED,
			tokenize='porter unicode61'
		)
	`)

	// Seed default domains
	for _, seed := range defaultDomainSeeds {
		_, err := s.writeExecContext(ctx,
			`INSERT INTO domains (domain_tag, decay_rate) VALUES (?, ?) ON CONFLICT DO NOTHING`,
			seed.tag, seed.rate)
		if err != nil {
			return fmt.Errorf("seed domain %s: %w", seed.tag, err)
		}
	}
	if err := s.ensureMemoryProjectionRevision(ctx); err != nil {
		return err
	}
	if err := s.ensureGraphProjectionRevision(ctx); err != nil {
		return err
	}

	return nil
}

// ensureMemoryProjectionRevision installs a same-transaction invalidation
// token for audited CEREBRUM snapshots. Triggers are created after every
// memories-table migration because SQLite drops table-bound triggers when an
// older schema is rebuilt via DROP/RENAME.
func (s *SQLiteStore) ensureMemoryProjectionRevision(ctx context.Context) error {
	const schema = `
		CREATE INDEX IF NOT EXISTS idx_memories_projection_page
			ON memories(created_at, memory_id);
		CREATE TABLE IF NOT EXISTS memory_projection_revision (
			singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
			revision  INTEGER NOT NULL CHECK (revision >= 0)
		);
		INSERT OR IGNORE INTO memory_projection_revision(singleton, revision)
			VALUES (1, 0);
		CREATE TRIGGER IF NOT EXISTS memories_projection_revision_insert
			AFTER INSERT ON memories
			BEGIN
				UPDATE memory_projection_revision
				SET revision = revision + 1 WHERE singleton = 1;
			END;
		DROP TRIGGER IF EXISTS memories_projection_revision_update;
		CREATE TRIGGER memories_projection_revision_update
			AFTER UPDATE OF
				submitting_agent, content, content_hash,
				domain_tag, status, created_at
			ON memories
			BEGIN
				UPDATE memory_projection_revision
				SET revision = revision + 1 WHERE singleton = 1;
			END;
		CREATE TRIGGER IF NOT EXISTS memories_projection_revision_delete
			AFTER DELETE ON memories
			BEGIN
				UPDATE memory_projection_revision
				SET revision = revision + 1 WHERE singleton = 1;
			END;
	`
	if _, err := s.writeExecContext(ctx, schema); err != nil {
		return fmt.Errorf("install memory projection revision: %w", err)
	}
	return nil
}

// MemoryProjectionRevision returns the transactionally maintained generation
// of every SQL memory row. Raw SQL repair/tamper and normal application writes
// both pass through the same triggers.
func (s *SQLiteStore) MemoryProjectionRevision(ctx context.Context) (uint64, error) {
	var revision int64
	if err := s.conn.QueryRowContext(ctx,
		`SELECT revision FROM memory_projection_revision WHERE singleton = 1`,
	).Scan(&revision); err != nil {
		return 0, fmt.Errorf("read memory projection revision: %w", err)
	}
	if revision < 0 {
		return 0, errors.New("memory projection revision is negative")
	}
	return uint64(revision), nil
}

// ensureGraphProjectionRevision tracks every SQL metadata table rendered into
// graph bytes. It is deliberately separate from the full memory-audit revision:
// changing a tag, corroboration, or typed link invalidates graph cache entries
// without forcing an inventory-wide canonical disclosure audit.
func (s *SQLiteStore) ensureGraphProjectionRevision(ctx context.Context) error {
	const schema = `
		CREATE TABLE IF NOT EXISTS graph_projection_revision (
			singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
			revision  INTEGER NOT NULL CHECK (revision >= 0)
		);
		INSERT OR IGNORE INTO graph_projection_revision(singleton, revision)
			VALUES (1, 0);
		DROP TRIGGER IF EXISTS memories_graph_revision_update;
		CREATE TRIGGER memories_graph_revision_update
			AFTER UPDATE OF memory_type, confidence_score, parent_hash ON memories
			BEGIN
				UPDATE graph_projection_revision
				SET revision = revision + 1 WHERE singleton = 1;
			END;
		CREATE TRIGGER IF NOT EXISTS memory_tags_graph_revision_insert
			AFTER INSERT ON memory_tags BEGIN
				UPDATE graph_projection_revision
				SET revision = revision + 1 WHERE singleton = 1;
			END;
		CREATE TRIGGER IF NOT EXISTS memory_tags_graph_revision_update
			AFTER UPDATE ON memory_tags BEGIN
				UPDATE graph_projection_revision
				SET revision = revision + 1 WHERE singleton = 1;
			END;
		CREATE TRIGGER IF NOT EXISTS memory_tags_graph_revision_delete
			AFTER DELETE ON memory_tags BEGIN
				UPDATE graph_projection_revision
				SET revision = revision + 1 WHERE singleton = 1;
			END;
		CREATE TRIGGER IF NOT EXISTS corroborations_graph_revision_insert
			AFTER INSERT ON corroborations BEGIN
				UPDATE graph_projection_revision
				SET revision = revision + 1 WHERE singleton = 1;
			END;
		CREATE TRIGGER IF NOT EXISTS corroborations_graph_revision_update
			AFTER UPDATE ON corroborations BEGIN
				UPDATE graph_projection_revision
				SET revision = revision + 1 WHERE singleton = 1;
			END;
		CREATE TRIGGER IF NOT EXISTS corroborations_graph_revision_delete
			AFTER DELETE ON corroborations BEGIN
				UPDATE graph_projection_revision
				SET revision = revision + 1 WHERE singleton = 1;
			END;
		CREATE TRIGGER IF NOT EXISTS memory_links_graph_revision_insert
			AFTER INSERT ON memory_links BEGIN
				UPDATE graph_projection_revision
				SET revision = revision + 1 WHERE singleton = 1;
			END;
		CREATE TRIGGER IF NOT EXISTS memory_links_graph_revision_update
			AFTER UPDATE ON memory_links BEGIN
				UPDATE graph_projection_revision
				SET revision = revision + 1 WHERE singleton = 1;
			END;
		CREATE TRIGGER IF NOT EXISTS memory_links_graph_revision_delete
			AFTER DELETE ON memory_links BEGIN
				UPDATE graph_projection_revision
				SET revision = revision + 1 WHERE singleton = 1;
			END;
	`
	if _, err := s.writeExecContext(ctx, schema); err != nil {
		return fmt.Errorf("install graph projection revision: %w", err)
	}
	return nil
}

// GraphProjectionRevision returns the exact metadata generation rendered into
// graph nodes and edges.
func (s *SQLiteStore) GraphProjectionRevision(ctx context.Context) (uint64, error) {
	var revision int64
	if err := s.conn.QueryRowContext(ctx,
		`SELECT revision FROM graph_projection_revision WHERE singleton = 1`,
	).Scan(&revision); err != nil {
		return 0, fmt.Errorf("read graph projection revision: %w", err)
	}
	if revision < 0 {
		return 0, errors.New("graph projection revision is negative")
	}
	return uint64(revision), nil
}

// migrateProvider adds the provider column to memories if it doesn't exist.
func (s *SQLiteStore) migrateProvider(ctx context.Context) {
	// Check if column exists by attempting a query.
	row := s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name='provider'`)
	var count int
	if err := row.Scan(&count); err != nil || count > 0 {
		return // already exists or error checking
	}
	_, _ = s.writeExecContext(ctx, `ALTER TABLE memories ADD COLUMN provider TEXT NOT NULL DEFAULT ''`)
	_, _ = s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_memories_provider ON memories(provider)`)
}

// migrateEmbeddingProvider adds a SEPARATE column tracking which embedder
// produced each memory's vector. This MUST be distinct from `provider` (which is
// the submitting AGENT's LLM identity, used for recall scoping) — re-embedding
// must never touch `provider`. Empty = not yet tagged (treated as needing
// re-embed when migrating to a semantic model).
func (s *SQLiteStore) migrateEmbeddingProvider(ctx context.Context) {
	row := s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name='embedding_provider'`)
	var count int
	if err := row.Scan(&count); err != nil || count > 0 {
		return // already exists or error checking
	}
	_, _ = s.writeExecContext(ctx, `ALTER TABLE memories ADD COLUMN embedding_provider TEXT NOT NULL DEFAULT ''`)
	_, _ = s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_memories_embedding_provider ON memories(embedding_provider)`)
}

// migrateDisputed adds the app-v17 two-phase-challenge columns to memories.
// disputed_height is the challenge-EXECUTION height (C-D3): it doubles as the
// discriminator that keeps ResolveChallengedMemories from sweeping LIVE v17
// disputes to deprecated on reboot — legacy pre-fork challenged rows predate the
// column and read the default 0, so the sweep (scoped to disputed_height=0) still
// cleans them while never touching a v17 dispute (disputed_height > 0).
// disputed_quorum records the deterministically-measured modify-verb-holder count
// for operator display. SQLite ADD COLUMN gives existing rows the default 0, so no
// backfill is needed. MUST run AFTER migrateTaskSupport (which recreates the table).
func (s *SQLiteStore) migrateDisputed(ctx context.Context) {
	// Check each column independently. SQLite applies ALTER TABLE statements one
	// at a time, so a crash between the two can leave a valid disputed_height but
	// no disputed_quorum. Treating the first column as an all-or-nothing migration
	// marker would then make every future MarkDisputed fail permanently.
	for _, col := range []struct {
		name string
		ddl  string
	}{
		{"disputed_height", `ALTER TABLE memories ADD COLUMN disputed_height INTEGER NOT NULL DEFAULT 0`},
		{"disputed_quorum", `ALTER TABLE memories ADD COLUMN disputed_quorum INTEGER NOT NULL DEFAULT 0`},
	} {
		row := s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name=?`, col.name)
		var count int
		if err := row.Scan(&count); err != nil || count > 0 {
			continue
		}
		_, _ = s.writeExecContext(ctx, col.ddl)
	}
}

// MarkDisputed reflects an app-v17 two-phase CHALLENGE (park) in the off-chain
// mirror: status → 'challenged', plus the challenge-EXECUTION height and the
// deterministically-measured modify-verb-holder quorum (C-D3, persisted and never
// re-measured). disputed_height is what ResolveChallengedMemories keys off to
// leave a live dispute alone. Never touches committed_at/deprecated_at (a
// reinstate/confirm later moves the row through UpdateStatus's normal arms).
func (s *SQLiteStore) MarkDisputed(ctx context.Context, memoryID string, disputedHeight int64, quorum int, _ time.Time) error {
	_, err := s.writeExecContext(ctx,
		`UPDATE memories SET status = 'challenged', disputed_height = ?, disputed_quorum = ? WHERE memory_id = ?`,
		disputedHeight, quorum, memoryID)
	if err != nil {
		return fmt.Errorf("mark disputed: %w", err)
	}
	return nil
}

// migrateTaskAssignee adds the assignee column to memories so tasks can be
// assigned to / claimed by a specific agent (empty = open to any agent).
func (s *SQLiteStore) migrateTaskAssignee(ctx context.Context) {
	row := s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name='assignee'`)
	var count int
	if err := row.Scan(&count); err != nil || count > 0 {
		return // already migrated or error
	}
	_, _ = s.writeExecContext(ctx, `ALTER TABLE memories ADD COLUMN assignee TEXT DEFAULT ''`)
	_, _ = s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_memories_assignee ON memories(assignee) WHERE assignee != ''`)
}

// migrateTaskPickup records when an agent actually claims/starts an assigned
// task. Dashboard assignment alone intentionally leaves these fields empty.
func (s *SQLiteStore) migrateTaskPickup(ctx context.Context) {
	row := s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name='task_picked_up_at'`)
	var count int
	if err := row.Scan(&count); err == nil && count == 0 {
		_, _ = s.writeExecContext(ctx, `ALTER TABLE memories ADD COLUMN task_picked_up_at TEXT`)
	}
	row = s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name='task_picked_up_by'`)
	if err := row.Scan(&count); err == nil && count == 0 {
		_, _ = s.writeExecContext(ctx, `ALTER TABLE memories ADD COLUMN task_picked_up_by TEXT DEFAULT ''`)
		_, _ = s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_memories_task_picked_up_by ON memories(task_picked_up_by) WHERE task_picked_up_by != ''`)
	}
}

// migrateTaskStatusUpdatedAt adds the lifecycle clock used by the task board.
// Legacy terminal cards get a fresh retention window on upgrade so an old
// created_at value cannot make them disappear before the user sees them.
func (s *SQLiteStore) migrateTaskStatusUpdatedAt(ctx context.Context) {
	var count int
	if err := s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name='task_status_updated_at'`).Scan(&count); err != nil || count > 0 {
		return
	}
	if _, err := s.writeExecContext(ctx, `ALTER TABLE memories ADD COLUMN task_status_updated_at TEXT`); err != nil {
		return
	}
	_, _ = s.writeExecContext(ctx, `UPDATE memories
		SET task_status_updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		WHERE memory_type = 'task' AND task_status IN ('done','dropped')`)
}

func (s *SQLiteStore) migrateTaskBoardPosition(ctx context.Context) {
	var count int
	if err := s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name='task_board_position'`).Scan(&count); err != nil || count > 0 {
		return
	}
	_, _ = s.writeExecContext(ctx, `ALTER TABLE memories ADD COLUMN task_board_position INTEGER NOT NULL DEFAULT 0`)
}

// migrateTaskAssignmentNotifications adds a monotonic assignment generation
// and a dedicated one-way notification inbox. These notices are deliberately
// separate from pipeline work items: reading one requires no result and does
// not consume pipeline quota.
func (s *SQLiteStore) migrateTaskAssignmentNotifications(ctx context.Context) error {
	var hasVersion int
	if err := s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name='task_assignment_version'`).Scan(&hasVersion); err != nil {
		return fmt.Errorf("inspect assignment generation: %w", err)
	}
	if hasVersion == 0 {
		if _, err := s.writeExecContext(ctx, `ALTER TABLE memories ADD COLUMN task_assignment_version INTEGER NOT NULL DEFAULT 0`); err != nil {
			return fmt.Errorf("add assignment generation: %w", err)
		}
	}
	var hasHandoff int
	if err := s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name='task_requires_handoff'`).Scan(&hasHandoff); err != nil {
		return fmt.Errorf("inspect task handoff gate: %w", err)
	}
	if hasHandoff == 0 {
		if _, err := s.writeExecContext(ctx, `ALTER TABLE memories ADD COLUMN task_requires_handoff INTEGER NOT NULL DEFAULT 0`); err != nil {
			return fmt.Errorf("add task handoff gate: %w", err)
		}
	}
	if _, err := s.writeExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS agent_notifications (
			notification_id TEXT PRIMARY KEY,
			agent_id TEXT NOT NULL,
			kind TEXT NOT NULL CHECK (kind IN ('task_assignment')),
			task_id TEXT NOT NULL,
			assignment_version INTEGER NOT NULL,
			domain TEXT NOT NULL DEFAULT '',
			title TEXT NOT NULL DEFAULT '',
			state TEXT NOT NULL DEFAULT 'unread' CHECK (state IN ('unread','read','superseded')),
			created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
			read_at TEXT,
			UNIQUE(kind, task_id, assignment_version, agent_id)
		)`); err != nil {
		return fmt.Errorf("create agent notifications: %w", err)
	}
	if _, err := s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_agent_notifications_inbox ON agent_notifications(agent_id, state, created_at)`); err != nil {
		return fmt.Errorf("index agent notification inbox: %w", err)
	}
	if _, err := s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_agent_notifications_task ON agent_notifications(task_id, assignment_version, state)`); err != nil {
		return fmt.Errorf("index task notifications: %w", err)
	}
	// Upgrade repair: existing assigned/open tasks predate notifications. Give
	// them generation 1 and create exactly one unread notice without disturbing
	// any authenticated pickup timestamp.
	if _, err := s.writeExecContext(ctx, `
		UPDATE memories SET task_assignment_version = 1
		WHERE memory_type = 'task' AND COALESCE(assignee, '') != '' AND task_assignment_version = 0`); err != nil {
		return fmt.Errorf("backfill assignment generations: %w", err)
	}
	if _, err := s.writeExecContext(ctx, `
		UPDATE memories SET task_requires_handoff = 1
		WHERE memory_type = 'task' AND task_status IN ('done','dropped')`); err != nil {
		return fmt.Errorf("backfill terminal task handoff gates: %w", err)
	}
	if _, err := s.writeExecContext(ctx, `
		UPDATE memories SET assignee = CASE
		  WHEN COALESCE(task_picked_up_by, '') != '' THEN task_picked_up_by
		  WHEN EXISTS (SELECT 1 FROM network_agents a WHERE a.agent_id = memories.submitting_agent) THEN submitting_agent
		  ELSE '' END
		WHERE memory_type = 'task' AND task_status IN ('done','dropped')
		  AND COALESCE(assignee, '') = ''
		  AND (COALESCE(task_picked_up_by, '') != ''
		       OR EXISTS (SELECT 1 FROM network_agents a WHERE a.agent_id = memories.submitting_agent))`); err != nil {
		return fmt.Errorf("backfill terminal task attribution: %w", err)
	}
	// Workflow repair: in-progress without an owner is not actionable and can be
	// mistaken for live assigned work. Return historical rows to human triage.
	if _, err := s.writeExecContext(ctx, `
		UPDATE memories SET task_status = 'planned'
		WHERE memory_type = 'task' AND task_status = 'in_progress'
		  AND COALESCE(assignee, '') = ''`); err != nil {
		return fmt.Errorf("repair ownerless in-progress tasks: %w", err)
	}
	if _, err := s.writeExecContext(ctx, `
		INSERT OR IGNORE INTO agent_notifications
		 (notification_id, agent_id, kind, task_id, assignment_version, domain, title, state)
		SELECT 'task-assignment:' || m.memory_id || ':' || m.task_assignment_version,
		       m.assignee, 'task_assignment', m.memory_id, m.task_assignment_version,
		       m.domain_tag, 'A task was assigned to you', 'unread'
		FROM memories m
		JOIN network_agents a ON a.agent_id = m.assignee
		WHERE m.memory_type = 'task' AND m.status != 'deprecated'
		  AND m.task_status IN ('planned','in_progress')
		  AND a.status = 'active' AND a.removed_at IS NULL`); err != nil {
		return fmt.Errorf("backfill assignment notifications: %w", err)
	}
	return nil
}

// migrateTaskSupport adds task_status column and updates the memory_type CHECK constraint
// to support 'task' type memories. For existing databases, we must recreate the table
// since SQLite doesn't support ALTER TABLE to modify CHECK constraints.
func (s *SQLiteStore) migrateTaskSupport(ctx context.Context) {
	// Check if task_status column already exists
	row := s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name='task_status'`)
	var count int
	if err := row.Scan(&count); err != nil || count > 0 {
		return // already migrated or error
	}

	// Add task_status column
	_, _ = s.writeExecContext(ctx, `ALTER TABLE memories ADD COLUMN task_status TEXT DEFAULT '' CHECK (task_status IN ('', 'planned', 'in_progress', 'done', 'dropped'))`)

	// Recreate the table to update the memory_type CHECK constraint.
	// SQLite doesn't allow altering CHECK constraints, so we use the rename-and-copy approach.
	_, err := s.writeExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS memories_new (
			memory_id        TEXT PRIMARY KEY,
			submitting_agent TEXT NOT NULL,
			content          TEXT NOT NULL,
			content_hash     BLOB NOT NULL,
			embedding        BLOB,
			embedding_hash   BLOB,
			memory_type      TEXT NOT NULL CHECK (memory_type IN ('fact', 'observation', 'inference', 'task')),
			domain_tag       TEXT NOT NULL,
			confidence_score REAL NOT NULL CHECK (confidence_score BETWEEN 0 AND 1),
			status           TEXT NOT NULL DEFAULT 'proposed',
			parent_hash      TEXT,
			classification   INTEGER NOT NULL DEFAULT 1,
			created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
			committed_at     TEXT,
			deprecated_at    TEXT,
			provider         TEXT NOT NULL DEFAULT '',
			task_status      TEXT DEFAULT '' CHECK (task_status IN ('', 'planned', 'in_progress', 'done', 'dropped'))
		)`)
	if err != nil {
		return
	}

	_, err = s.writeExecContext(ctx, `INSERT INTO memories_new SELECT memory_id, submitting_agent, content, content_hash, embedding, embedding_hash, memory_type, domain_tag, confidence_score, status, parent_hash, classification, created_at, committed_at, deprecated_at, provider, '' FROM memories`)
	if err != nil {
		_, _ = s.writeExecContext(ctx, `DROP TABLE IF EXISTS memories_new`)
		return
	}

	_, _ = s.writeExecContext(ctx, `DROP TABLE memories`)
	_, _ = s.writeExecContext(ctx, `ALTER TABLE memories_new RENAME TO memories`)
	_, _ = s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_memories_domain ON memories(domain_tag)`)
	_, _ = s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_memories_status ON memories(status)`)
	_, _ = s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_memories_provider ON memories(provider)`)
	_, _ = s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_memories_task_status ON memories(task_status) WHERE task_status != ''`)
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

	_, err = s.writeExecContext(ctx,
		`INSERT INTO memories (memory_id, submitting_agent, content, content_hash, embedding, embedding_hash,
			memory_type, domain_tag, provider, embedding_provider, confidence_score, status, parent_hash, task_status, assignee, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (memory_id) DO UPDATE SET
			submitting_agent = excluded.submitting_agent,
			status = excluded.status,
			created_at = excluded.created_at,
			-- content/hash/domain/type/confidence are overwritten so a co-commit
			-- squat-reclaim (which rewrites Badger's content hash to the co-commit's)
			-- also replaces the squatter's off-chain content, keeping the SQLite
			-- mirror consistent with the committed on-chain hash. Safe for the other
			-- conflict callers too: content-hash IDs collide only on identical
			-- content; UUID IDs (task/journal/import) never collide; and the on-chain
			-- submit path rewrites the Badger hash in lockstep - so overwriting the
			-- mirror keeps it consistent rather than losing data.
			content = excluded.content,
			content_hash = excluded.content_hash,
			domain_tag = excluded.domain_tag,
			memory_type = excluded.memory_type,
			confidence_score = excluded.confidence_score,
			embedding = COALESCE(excluded.embedding, memories.embedding),
			embedding_hash = COALESCE(excluded.embedding_hash, memories.embedding_hash),
			provider = COALESCE(NULLIF(excluded.provider, ''), memories.provider),
			embedding_provider = COALESCE(NULLIF(excluded.embedding_provider, ''), memories.embedding_provider),
			assignee = COALESCE(NULLIF(excluded.assignee, ''), memories.assignee),
			parent_hash = COALESCE(NULLIF(excluded.parent_hash, ''), memories.parent_hash)`,
		record.MemoryID, record.SubmittingAgent, content, record.ContentHash,
		encEmb, record.EmbeddingHash,
		string(record.MemoryType), record.DomainTag, record.Provider, record.EmbeddingProvider, record.ConfidenceScore,
		string(record.Status), record.ParentHash, string(record.TaskStatus), record.Assignee, formatTime(record.CreatedAt))
	if err != nil {
		return fmt.Errorf("insert memory: %w", err)
	}

	// Sync FTS5 index with plaintext content for full-text search.
	// Skip when vault is active to avoid storing plaintext in a secondary table.
	if s.vault.Load() == nil {
		_, _ = s.writeExecContext(ctx, `DELETE FROM memories_fts WHERE memory_id = ?`, record.MemoryID)
		_, _ = s.writeExecContext(ctx, `INSERT INTO memories_fts(memory_id, content, domain_tag) VALUES (?, ?, ?)`,
			record.MemoryID, record.Content, record.DomainTag)
	}

	return nil
}

// UpdateMemoryEmbedding replaces a memory's stored embedding vector and records
// which EMBEDDER produced it (the embedding_provider column — NOT the `provider`
// column, which is the submitting agent's LLM identity used for recall scoping).
// This is an OFF-CHAIN, per-node search-index operation — the embedding is not
// part of consensus/AppHash, so re-embedding (e.g. migrating hash pseudo-vectors
// to a real semantic model) is a local SQLite update with no transaction/
// redeploy. The vector is encoded + encrypted exactly like the insert path
// (requires the vault unlocked when encryption is on).
func (s *SQLiteStore) UpdateMemoryEmbedding(ctx context.Context, memoryID string, emb []float32, embeddingProvider string) error {
	encEmb, err := s.encryptEmbedding(encodeEmbedding(emb))
	if err != nil {
		return fmt.Errorf("encrypt embedding: %w", err)
	}
	res, err := s.writeExecContext(ctx,
		`UPDATE memories SET embedding = ?, embedding_provider = ? WHERE memory_id = ?`,
		encEmb, embeddingProvider, memoryID)
	if err != nil {
		return fmt.Errorf("update embedding: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("memory %s not found", memoryID)
	}
	return nil
}

// CountMemoriesByProvider returns how many memories carry each EMBEDDING provider
// tag (embedding_provider column; empty string = not yet tagged / needs
// re-embedding). Used by the embeddings-setup status.
func (s *SQLiteStore) CountMemoriesByProvider(ctx context.Context) (map[string]int, error) {
	// Exclude deprecated memories — they're hidden from all of CEREBRUM (audit-only
	// in the DB), so they must never inflate the embeddings accounting (needs
	// re-embed / unreadable / on-ollama).
	rows, err := s.conn.QueryContext(ctx, `SELECT COALESCE(embedding_provider, ''), COUNT(*) FROM memories WHERE status != 'deprecated' GROUP BY embedding_provider`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make(map[string]int)
	for rows.Next() {
		var provider string
		var n int
		if scanErr := rows.Scan(&provider, &n); scanErr != nil {
			return nil, scanErr
		}
		out[provider] = n
	}
	return out, rows.Err()
}

// GetDomainLastActivity returns, per domain, the created_at of its most
// recent non-deprecated memory. The MRI lobe list sorts by this so the most
// recently active domains surface first.
func (s *SQLiteStore) GetDomainLastActivity(ctx context.Context) (map[string]string, error) {
	rows, err := s.conn.QueryContext(ctx, `SELECT domain_tag, MAX(created_at) FROM memories WHERE status != 'deprecated' GROUP BY domain_tag`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make(map[string]string)
	for rows.Next() {
		var domain, last string
		if scanErr := rows.Scan(&domain, &last); scanErr != nil {
			return nil, scanErr
		}
		out[domain] = last
	}
	return out, rows.Err()
}

// ReembedItem is one memory to (re-)embed: its id and decrypted content.
// Decryptable is false when the stored content could not be decrypted (vault-key
// mismatch / corruption) — such a memory can't be embedded and should be tagged
// so it drops out of the work set rather than being retried forever.
type ReembedItem struct {
	MemoryID    string
	Content     string
	Decryptable bool
}

// ListMemoriesForReembed returns up to `limit` memories whose provider stamp
// differs from targetProvider, decrypting their content. It deliberately uses a
// WHERE filter + LIMIT (no OFFSET): the migration loop retags every returned row
// (target / skipped / error), so each subsequent call returns the next batch and
// converges without mixing vector spaces. A decrypt failure is tolerated
// (Decryptable=false), never fatal, matching every other list path in this store.
func (s *SQLiteStore) ListMemoriesForReembed(ctx context.Context, targetProvider string, limit int) ([]ReembedItem, error) {
	// Skip deprecated memories: they're hidden from every view and never
	// searched, so embedding them is wasted work (and would keep failing on
	// undecryptable-but-deprecated rows). They stay untagged; the status counts
	// below also exclude them so they never inflate migration counts.
	rows, err := s.conn.QueryContext(ctx,
		`SELECT memory_id, content FROM memories
		 WHERE COALESCE(embedding_provider, '') NOT IN (?, 'skipped', 'error') AND status != 'deprecated'
		 ORDER BY created_at ASC, memory_id ASC LIMIT ?`,
		targetProvider, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []ReembedItem
	for rows.Next() {
		var it ReembedItem
		var content string
		if scanErr := rows.Scan(&it.MemoryID, &content); scanErr != nil {
			return nil, scanErr
		}
		dec, decErr := s.decryptContent(content)
		// No vault key available (locked): decryption is impossible for EVERY
		// encrypted memory, not just this one. Abort the whole batch rather than
		// letting the caller embed the placeholder or mis-tag readable memories as
		// unreadable — a genuine key-mismatch (decErr != nil) is different and is
		// reported per-row via Decryptable=false.
		if dec == VaultLockedPlaceholder {
			return nil, fmt.Errorf("vault key unavailable — unlock before re-embedding")
		}
		it.Decryptable = decErr == nil
		if decErr == nil {
			it.Content = dec
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// ResetErroredEmbeddings clears the 'error' embedding tag (readable memories whose
// embed transiently failed) back to ” so the next re-embed run retries them.
// Returns how many were reset.
func (s *SQLiteStore) ResetErroredEmbeddings(ctx context.Context) (int, error) {
	res, err := s.writeExecContext(ctx,
		`UPDATE memories SET embedding_provider = '' WHERE embedding_provider = 'error'`)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// MarkMemoryEmbeddingSkipped tags a memory's embedding_provider as "skipped",
// meaning it is UNREADABLE and can't ever be embedded (undecryptable content /
// vault-key mismatch, or empty content). It drops out of the re-embed work set and
// is a candidate for deprecation. Off-chain, local; does not touch the embedding
// vector or the agent `provider` column.
func (s *SQLiteStore) MarkMemoryEmbeddingSkipped(ctx context.Context, memoryID string) error {
	_, err := s.writeExecContext(ctx,
		`UPDATE memories SET embedding_provider = 'skipped' WHERE memory_id = ?`, memoryID)
	return err
}

// MarkMemoryEmbeddingError tags a memory's embedding_provider as "error", meaning
// its content IS readable but the embedder failed on it (likely transient). It
// drops out of the CURRENT run's work set (so the job terminates) but is kept
// distinct from "skipped" so it is NEVER deprecated as unreadable and can be
// retried later.
func (s *SQLiteStore) MarkMemoryEmbeddingError(ctx context.Context, memoryID string) error {
	_, err := s.writeExecContext(ctx,
		`UPDATE memories SET embedding_provider = 'error' WHERE memory_id = ?`, memoryID)
	return err
}

// RekeyUnreadableMemories recovers memories orphaned by a past vault re-init. It
// walks the unreadable ('skipped') set, decrypts each memory's content with the
// supplied OLD vault (built in-memory from an old recovery key), and — unless
// dryRun — re-encrypts it with the LIVE vault in place and clears embedding_provider
// so it re-embeds. Only content changes; content_hash is plaintext-derived so it
// (and the on-chain hash) stays identical, and agent/domain/timestamps are
// untouched. Memories the old key can't decrypt are left as-is (try another key).
// Returns how many were (or would be) recovered. Requires the live vault unlocked.
func (s *SQLiteStore) RekeyUnreadableMemories(ctx context.Context, oldVault *vault.Vault, dryRun bool) (int, error) {
	if s.vault.Load() == nil {
		return 0, fmt.Errorf("live vault is locked — unlock before recovering")
	}
	// Snapshot the candidate rows first (don't UPDATE while a query is open on the
	// same connection).
	// Only recover VISIBLE unreadable memories — deprecated ones are audit-only and
	// staying hidden, so re-keying them would be pointless (they'd remain hidden).
	rows, err := s.conn.QueryContext(ctx, `SELECT memory_id, content FROM memories WHERE embedding_provider = 'skipped' AND status != 'deprecated'`)
	if err != nil {
		return 0, err
	}
	type cand struct{ id, content string }
	var cands []cand
	for rows.Next() {
		var c cand
		if scanErr := rows.Scan(&c.id, &c.content); scanErr != nil {
			_ = rows.Close()
			return 0, scanErr
		}
		cands = append(cands, c)
	}
	if closeErr := rows.Close(); closeErr != nil {
		return 0, closeErr
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	recovered := 0
	for _, c := range cands {
		if !strings.HasPrefix(c.content, encPrefix) {
			continue // not encrypted — nothing an old key can do
		}
		raw, decErr := base64.StdEncoding.DecodeString(strings.TrimPrefix(c.content, encPrefix))
		if decErr != nil {
			continue
		}
		plaintext, dErr := oldVault.DecryptString(raw)
		if dErr != nil {
			continue // this old key does not decrypt this memory — leave for another key
		}
		recovered++
		if dryRun {
			continue
		}
		reEnc, encErr := s.encryptContent(plaintext) // re-encrypt under the LIVE vault
		if encErr != nil {
			return recovered, encErr
		}
		if _, upErr := s.writeExecContext(ctx,
			`UPDATE memories SET content = ?, embedding_provider = '' WHERE memory_id = ?`,
			reEnc, c.id); upErr != nil {
			return recovered, upErr
		}
	}
	return recovered, nil
}

// DeprecateUnreadableMemories soft-deprecates every memory tagged unreadable
// (embedding_provider = 'skipped') — hidden from all views, row + on-chain hash
// retained (reversible, not a hard delete). Returns how many were deprecated.
func (s *SQLiteStore) DeprecateUnreadableMemories(ctx context.Context) (int, error) {
	res, err := s.writeExecContext(ctx,
		`UPDATE memories SET status = 'deprecated', deprecated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		 WHERE embedding_provider = 'skipped' AND status != 'deprecated'`)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// ListUnreadableMemoryIDs returns the stable local work queue for governed
// deprecation. It deliberately exposes identities only to the in-process
// CEREBRUM handler; app-v23+ must submit lifecycle changes through consensus
// instead of updating this SQL projection directly.
func (s *SQLiteStore) ListUnreadableMemoryIDs(
	ctx context.Context,
	afterMemoryID string,
	limit int,
) ([]string, int, error) {
	if limit <= 0 {
		return nil, 0, errors.New("unreadable memory page limit must be positive")
	}
	var total int
	if err := s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM memories
		 WHERE embedding_provider = 'skipped' AND status != 'deprecated'`,
	).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.conn.QueryContext(ctx,
		`SELECT memory_id FROM memories
		 WHERE embedding_provider = 'skipped' AND status != 'deprecated'
		   AND memory_id > ?
		 ORDER BY memory_id ASC
		 LIMIT ?`,
		afterMemoryID, limit)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, 0, err
		}
		ids = append(ids, id)
	}
	return ids, total, rows.Err()
}

func (s *SQLiteStore) GetMemory(ctx context.Context, memoryID string) (*memory.MemoryRecord, error) {
	row := s.conn.QueryRowContext(ctx,
		`SELECT memory_id, submitting_agent, content, content_hash, embedding, embedding_hash,
			memory_type, domain_tag, provider, confidence_score, status, parent_hash, created_at, committed_at, deprecated_at,
			COALESCE(task_status, ''), COALESCE(assignee, '')
		FROM memories WHERE memory_id = ?`, memoryID)

	var r memory.MemoryRecord
	var mt, st, createdAt, taskStatus string
	var embData []byte
	var parentHash, committedAt, deprecatedAt *string

	err := row.Scan(&r.MemoryID, &r.SubmittingAgent, &r.Content, &r.ContentHash,
		&embData, &r.EmbeddingHash, &mt, &r.DomainTag, &r.Provider, &r.ConfidenceScore,
		&st, &parentHash, &createdAt, &committedAt, &deprecatedAt, &taskStatus, &r.Assignee)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("memory not found: %s", memoryID)
		}
		return nil, fmt.Errorf("get memory: %w", err)
	}

	r.MemoryType = memory.MemoryType(mt)
	r.Status = memory.MemoryStatus(st)
	r.TaskStatus = memory.TaskStatus(taskStatus)

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
		_, err = s.writeExecContext(ctx,
			`UPDATE memories SET status = ?, committed_at = ?, disputed_height = 0, disputed_quorum = 0 WHERE memory_id = ?`,
			string(status), nowStr, memoryID)
	case memory.StatusDeprecated:
		_, err = s.writeExecContext(ctx,
			`UPDATE memories SET status = ?, deprecated_at = ?, disputed_height = 0, disputed_quorum = 0 WHERE memory_id = ?`,
			string(status), nowStr, memoryID)
	default:
		_, err = s.writeExecContext(ctx,
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
		committed_at, deprecated_at, COALESCE(task_status, '')
	FROM memories WHERE embedding IS NOT NULL`
	var args []any
	if opts.VectorProvider != "" {
		query += " AND embedding_provider = ?"
		args = append(args, opts.VectorProvider)
	}

	if opts.DomainTag != "" {
		query += " AND domain_tag = ?"
		args = append(args, opts.DomainTag)
	}
	if opts.Provider != "" && opts.DomainTag == "" {
		// Provider scoping: show memories from this provider OR facts (shared cross-provider).
		// Skip when domain is explicitly specified — the domain filter IS the relevance filter,
		// and cross-provider memories in the same domain should be visible.
		query += " AND (provider = ? OR provider = '' OR memory_type = 'fact')"
		args = append(args, opts.Provider)
	}
	if opts.MinConfidence > 0 {
		query += " AND confidence_score >= ?"
		args = append(args, opts.MinConfidence)
	}
	if opts.StatusFilter == "committed" && opts.IncludeDisputed {
		// app-v17: admit disputed-but-live rows alongside committed (flagged at
		// serialize). Committed rows are unaffected.
		query += " AND status IN ('committed', 'challenged')"
	} else if opts.StatusFilter != "" {
		query += " AND status = ?"
		args = append(args, opts.StatusFilter)
	}
	if len(opts.SubmittingAgents) > 0 {
		placeholders := make([]string, len(opts.SubmittingAgents))
		for i, a := range opts.SubmittingAgents {
			placeholders[i] = "?"
			args = append(args, a)
		}
		query += " AND submitting_agent IN (" + strings.Join(placeholders, ",") + ")"
	}
	if len(opts.Tags) > 0 {
		placeholders := make([]string, len(opts.Tags))
		for i, t := range opts.Tags {
			placeholders[i] = "?"
			args = append(args, t)
		}
		query += " AND memory_id IN (SELECT memory_id FROM memory_tags WHERE tag IN (" +
			strings.Join(placeholders, ",") + "))"
	}
	hasCandidateFilter := opts.CandidateFilter != nil ||
		opts.CandidateBatchFilter != nil
	if hasCandidateFilter {
		// SQLite ranks vectors in Go. Read at most one sentinel beyond the
		// authorization budget so an authenticated app-v23 recall cannot force a
		// full-corpus decrypt/cosine/policy scan. Refuse the broad query instead
		// of returning a misleading ranking over an arbitrary prefix.
		query += " LIMIT ?"
		args = append(args, CandidateFilterScanBudget+1)
	}

	rows, err := s.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query similar: %w", err)
	}
	defer func() { _ = rows.Close() }()

	type scoredRecord struct {
		record     *memory.MemoryRecord
		similarity float64
	}
	var scored []scoredRecord

	for rows.Next() {
		var r memory.MemoryRecord
		var mt, st, createdAt, taskStatus string
		var embData []byte
		var parentHash, committedAt, deprecatedAt *string

		scanErr := rows.Scan(&r.MemoryID, &r.SubmittingAgent, &r.Content, &r.ContentHash,
			&embData, &mt, &r.DomainTag, &r.Provider, &r.ConfidenceScore,
			&st, &parentHash, &createdAt, &committedAt, &deprecatedAt, &taskStatus)
		if scanErr != nil {
			return nil, fmt.Errorf("scan row: %w", scanErr)
		}

		r.MemoryType = memory.MemoryType(mt)
		r.Status = memory.MemoryStatus(st)
		r.TaskStatus = memory.TaskStatus(taskStatus)

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

		// Compute similarity for ranking only — no minimum threshold.
		// When a domain filter is active, the domain IS the relevance filter;
		// all matching records are returned regardless of similarity score.
		sim := cosineSimilarity(embedding, r.Embedding)
		scored = append(scored, scoredRecord{record: &r, similarity: sim})
		if hasCandidateFilter && len(scored) > CandidateFilterScanBudget {
			return nil, ErrCandidateFilterScanBudgetExceeded
		}
	}

	sort.Slice(scored, func(i, j int) bool {
		return scored[i].similarity > scored[j].similarity
	})

	// Materialize in similarity order, then apply the decayed-confidence floor (if
	// any) over the FULL candidate set BEFORE the top-K trim, so top_k is filled
	// with qualifying records instead of being truncated by a decay-blind
	// stored-column pre-filter (which both leaked aged sub-floor memories and
	// starved corroboration-boosted ones).
	ordered := make([]*memory.MemoryRecord, len(scored))
	for i := range scored {
		ordered[i] = scored[i].record
	}
	if opts.DecayFloor > 0 {
		counts, cErr := s.GetCorroborationCounts(ctx, recordIDs(ordered))
		if cErr != nil {
			return nil, fmt.Errorf("query similar decay floor: %w", cErr)
		}
		ordered = applyDecayFloor(ordered, opts.DecayFloor, opts.DecayNow, counts, opts.IncludeDisputed)
	}
	ordered, err = applyCandidateFilters(
		ordered, opts.CandidateBatchFilter, opts.CandidateFilter,
	)
	if err != nil {
		return nil, err
	}

	// opts.TopK was capped to [1, 100] at function entry, but make an explicit
	// local guard so CodeQL sees the bound at the allocation site — defence-in-depth
	// against future refactors that might bypass the entry-time clamp.
	limit := opts.TopK
	const maxLimit = 1000
	if limit < 0 || limit > maxLimit {
		limit = maxLimit
	}
	if limit > len(ordered) {
		limit = len(ordered)
	}
	return ordered[:limit], nil
}

// SearchByText performs full-text search using FTS5 with BM25 ranking.
// Falls back gracefully when vault is active (encrypted content can't be FTS-indexed).
func (s *SQLiteStore) SearchByText(ctx context.Context, query string, opts QueryOptions) ([]*memory.MemoryRecord, error) {
	if s.vault.Load() != nil {
		return nil, fmt.Errorf("%s", ErrTextSearchVaultEncryptedMsg)
	}
	if query == "" {
		return nil, fmt.Errorf("search query is required")
	}
	if opts.TopK <= 0 {
		opts.TopK = 10
	}
	if opts.TopK > 100 {
		opts.TopK = 100
	}

	// Escape FTS5 special characters by wrapping each term in double quotes.
	// This prevents query syntax injection while preserving multi-word search.
	escapedQuery := ftsEscapeQuery(query)

	sqlStr := `SELECT m.memory_id, m.submitting_agent, m.content, m.content_hash, m.embedding,
		m.memory_type, m.domain_tag, m.provider, m.confidence_score, m.status, m.parent_hash,
		m.created_at, m.committed_at, m.deprecated_at, COALESCE(m.task_status, '')
		FROM memories_fts f
		JOIN memories m ON m.memory_id = f.memory_id
		WHERE memories_fts MATCH ?`
	args := []any{escapedQuery}

	if opts.DomainTag != "" {
		sqlStr += " AND f.domain_tag = ?"
		args = append(args, opts.DomainTag)
	}
	for _, prefix := range opts.ExcludeDomainPrefixes {
		if prefix == "" {
			continue
		}
		sqlStr += " AND LOWER(m.domain_tag) NOT LIKE ?"
		args = append(args, strings.ToLower(prefix)+"%")
	}
	if opts.Provider != "" && opts.DomainTag == "" {
		sqlStr += " AND (m.provider = ? OR m.provider = '' OR m.memory_type = 'fact')"
		args = append(args, opts.Provider)
	}
	if opts.MinConfidence > 0 {
		sqlStr += " AND m.confidence_score >= ?"
		args = append(args, opts.MinConfidence)
	}
	if opts.StatusFilter == "active" {
		sqlStr += " AND m.status != 'deprecated'" // audit-only deprecated memories never surface in search
	} else if opts.StatusFilter == "committed" && opts.IncludeDisputed {
		// app-v17: admit disputed-but-live rows alongside committed (flagged at
		// serialize). Committed rows are unaffected.
		sqlStr += " AND m.status IN ('committed', 'challenged')"
	} else if opts.StatusFilter != "" {
		sqlStr += " AND m.status = ?"
		args = append(args, opts.StatusFilter)
	}
	// Date-range filter on the joined memories row; same ISO-8601 lexicographic
	// compare as ListMemories (see note there).
	if opts.CreatedFrom != "" {
		sqlStr += " AND m.created_at >= ?"
		args = append(args, opts.CreatedFrom)
	}
	if opts.CreatedTo != "" {
		sqlStr += " AND m.created_at <= ?"
		args = append(args, opts.CreatedTo)
	}
	if len(opts.SubmittingAgents) > 0 {
		placeholders := make([]string, len(opts.SubmittingAgents))
		for i, a := range opts.SubmittingAgents {
			placeholders[i] = "?"
			args = append(args, a)
		}
		sqlStr += " AND m.submitting_agent IN (" + strings.Join(placeholders, ",") + ")"
	}
	if len(opts.Tags) > 0 {
		placeholders := make([]string, len(opts.Tags))
		for i, t := range opts.Tags {
			placeholders[i] = "?"
			args = append(args, t)
		}
		sqlStr += " AND m.memory_id IN (SELECT memory_id FROM memory_tags WHERE tag IN (" +
			strings.Join(placeholders, ",") + "))"
	}

	orderBy := " ORDER BY rank"
	hasCandidateFilter := opts.CandidateFilter != nil ||
		opts.CandidateBatchFilter != nil
	if hasCandidateFilter {
		// Stable secondary order is required when the trusted app-v23 path walks
		// multiple ranked pages. The historical one-page query is unchanged.
		orderBy += ", m.memory_id ASC"
	}
	fetchPage := func(limit, offset int) ([]*memory.MemoryRecord, error) {
		pageArgs := make([]any, len(args), len(args)+2)
		copy(pageArgs, args)
		pageArgs = append(pageArgs, limit, offset)
		rows, queryErr := s.conn.QueryContext(
			ctx, sqlStr+orderBy+" LIMIT ? OFFSET ?", pageArgs...,
		)
		if queryErr != nil {
			return nil, fmt.Errorf("search by text: %w", queryErr)
		}
		defer func() { _ = rows.Close() }()

		// Do not use a caller-derived SQL limit as an allocation hint. The query
		// remains bounded by its LIMIT while append grows only for rows returned.
		page := make([]*memory.MemoryRecord, 0)
		for rows.Next() {
			var r memory.MemoryRecord
			var mt, st, createdAt, taskStatus string
			var embData []byte
			var parentHash, committedAt, deprecatedAt *string

			scanErr := rows.Scan(&r.MemoryID, &r.SubmittingAgent, &r.Content, &r.ContentHash,
				&embData, &mt, &r.DomainTag, &r.Provider, &r.ConfidenceScore,
				&st, &parentHash, &createdAt, &committedAt, &deprecatedAt, &taskStatus)
			if scanErr != nil {
				return nil, fmt.Errorf("scan row: %w", scanErr)
			}

			r.MemoryType = memory.MemoryType(mt)
			r.Status = memory.MemoryStatus(st)
			r.TaskStatus = memory.TaskStatus(taskStatus)

			// Decrypt content if encrypted (shouldn't be in FTS mode, but defensive).
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
			page = append(page, &r)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			return nil, fmt.Errorf("search by text rows: %w", rowsErr)
		}
		return page, nil
	}

	filterPage := func(page []*memory.MemoryRecord) ([]*memory.MemoryRecord, error) {
		if opts.DecayFloor > 0 {
			counts, cErr := s.GetCorroborationCounts(ctx, recordIDs(page))
			if cErr != nil {
				return nil, fmt.Errorf("search by text decay floor: %w", cErr)
			}
			page = applyDecayFloor(page, opts.DecayFloor, opts.DecayNow, counts, opts.IncludeDisputed)
		}
		return applyCandidateFilters(
			page, opts.CandidateBatchFilter, opts.CandidateFilter,
		)
	}

	if !hasCandidateFilter {
		scanLimit := opts.TopK
		if opts.DecayFloor > 0 {
			// Preserve the historical bounded decay-only over-fetch.
			scanLimit = decayFilterScanCap
		}
		results, fetchErr := fetchPage(scanLimit, 0)
		if fetchErr != nil {
			return nil, fetchErr
		}
		results, fetchErr = filterPage(results)
		if fetchErr != nil {
			return nil, fetchErr
		}
		if len(results) > opts.TopK {
			results = results[:opts.TopK]
		}
		return results, nil
	}

	// Authorization-aware app-v23 path: each SQL page and the whole candidate
	// walk are bounded. A sparse authorized tail must not let one authenticated
	// request force unbounded SQL + live-policy work.
	const candidatePageSize = 128
	results := make([]*memory.MemoryRecord, 0)
	for offset := 0; len(results) < opts.TopK; offset += candidatePageSize {
		if offset >= CandidateFilterScanBudget {
			return nil, ErrCandidateFilterScanBudgetExceeded
		}
		page, fetchErr := fetchPage(candidatePageSize, offset)
		if fetchErr != nil {
			return nil, fetchErr
		}
		rawCount := len(page)
		page, fetchErr = filterPage(page)
		if fetchErr != nil {
			return nil, fetchErr
		}
		remaining := opts.TopK - len(results)
		if len(page) > remaining {
			page = page[:remaining]
		}
		results = append(results, page...)
		if rawCount < candidatePageSize {
			break
		}
	}
	return results, nil
}

// SearchHybrid runs FTS5/BM25 and vector cosine in parallel, then fuses the
// two ranked lists via weighted Reciprocal Rank Fusion. When the vault is
// active the FTS path is unavailable; we fall back to QuerySimilar alone so
// recall still works. When the query is empty we degrade to vector-only.
// If a reranker is attached, RRF returns an oversampled candidate pool that
// the reranker rescores into the final top-K.
func (s *SQLiteStore) SearchHybrid(ctx context.Context, query string, embedding []float32, opts QueryOptions) ([]*memory.MemoryRecord, error) {
	if len(embedding) == 0 && query == "" {
		return nil, fmt.Errorf("hybrid search requires either a query or an embedding")
	}

	params := ResolveHybridParams()
	requestedTopK := opts.TopK
	if requestedTopK <= 0 {
		requestedTopK = 10
	}
	// RRFMerge and the REST expansion layer both cap fused output at 1,000.
	// Clamp before multiplying by operator-controlled oversample factors so a
	// huge pre-v23 top_k cannot wrap int and silently shrink the candidate pool.
	const maxHybridTopK = 1000
	if requestedTopK > maxHybridTopK {
		requestedTopK = maxHybridTopK
	}

	// When the reranker is active we ask RRF for more candidates than the
	// caller wants back, so the cross-encoder has a real pool to choose from.
	// Without a reranker, RRF trims directly to requestedTopK.
	// Snapshot the hot-swappable reranker under the read lock so a concurrent
	// SetReranker (Settings > Recall toggle) can't tear the interface value out
	// from under this recall.
	s.rerankerMu.RLock()
	reranker := s.reranker
	rerankerOversample := s.rerankerOversample
	s.rerankerMu.RUnlock()

	rerankPool := requestedTopK
	rerankActive := reranker != nil && query != ""
	if rerankActive {
		os := rerankerOversample
		if os <= 0 {
			os = 2
		}
		if os > maxHybridTopK/requestedTopK {
			rerankPool = maxHybridTopK
		} else {
			rerankPool = requestedTopK * os
			if rerankPool > maxHybridTopK {
				rerankPool = maxHybridTopK
			}
		}
	}

	// Each underlying index oversamples so the fusion has enough overlap to
	// rank fairly when the two lists diverge near the tail.
	subOpts := opts
	const maxHybridLeafTopK = 100
	if params.OversampleMul > maxHybridLeafTopK/rerankPool {
		subOpts.TopK = maxHybridLeafTopK
	} else {
		subOpts.TopK = rerankPool * params.OversampleMul
	}

	var bm25Results, vectorResults []*memory.MemoryRecord
	vaultActive := s.vault.Load() != nil
	if query != "" && !vaultActive {
		r, err := s.SearchByText(ctx, query, subOpts)
		if err != nil {
			// Under a decayed floor or live authorization hook a leaf error is
			// fail-closed. Swallowing it could turn an exhausted authorization
			// budget or unavailable policy state into a misleading partial 200.
			// The same applies when BM25 is the only requested leaf: there is no
			// successful vector result to degrade to.
			if subOpts.DecayFloor > 0 ||
				subOpts.CandidateFilter != nil ||
				subOpts.CandidateBatchFilter != nil ||
				len(embedding) == 0 {
				return nil, err
			}
		} else {
			bm25Results = r
		}
	} else if query != "" && len(embedding) == 0 {
		return nil, fmt.Errorf("%s", ErrTextSearchVaultEncryptedMsg)
	}

	if len(embedding) > 0 {
		r, err := s.QuerySimilar(ctx, embedding, subOpts)
		if err != nil && (subOpts.DecayFloor > 0 ||
			subOpts.CandidateFilter != nil ||
			subOpts.CandidateBatchFilter != nil ||
			len(bm25Results) == 0) {
			return nil, err
		}
		vectorResults = r
	}

	merged := RRFMerge(bm25Results, vectorResults, rerankPool, params)

	if !rerankActive || len(merged) == 0 {
		// No reranker configured, or no candidates to rescore. Trim to the
		// caller's requested top-K (RRF already did this when rerankPool ==
		// requestedTopK, but keep the safety in case oversample was applied
		// elsewhere) and return.
		if len(merged) > requestedTopK {
			merged = merged[:requestedTopK]
		}
		return merged, nil
	}

	return s.applyReranker(ctx, query, merged, requestedTopK, reranker)
}

// applyReranker rescores `candidates` against `query` using the attached
// reranker and returns the top-K. Failures fall back to the RRF-sorted
// candidates so a flaky reranker upstream never blocks recall.
func (s *SQLiteStore) applyReranker(ctx context.Context, query string, candidates []*memory.MemoryRecord, topK int, reranker embedding.Reranker) ([]*memory.MemoryRecord, error) {
	// Bound topK so an attacker-influenced value can't drive a giant
	// preallocation in `out := make(..., 0, topK)` below.
	const maxRerankK = 1000
	if topK < 0 {
		topK = 0
	}
	if topK > maxRerankK {
		topK = maxRerankK
	}
	if topK > len(candidates) {
		topK = len(candidates)
	}
	texts := make([]string, len(candidates))
	for i, c := range candidates {
		texts[i] = c.Content
	}

	scored, err := reranker.Rerank(ctx, query, texts)
	if err != nil || len(scored) == 0 {
		// Best-effort: if the reranker is unreachable or returns nothing
		// useful, surface the RRF ordering rather than fail the whole recall.
		if len(candidates) > topK {
			candidates = candidates[:topK]
		}
		return candidates, nil
	}

	// TEI returns results sorted by score descending; we still sort by score
	// defensively so callers don't depend on an undocumented contract from
	// whatever reranker the operator wired in.
	sort.SliceStable(scored, func(i, j int) bool { return scored[i].Score > scored[j].Score })

	out := make([]*memory.MemoryRecord, 0, topK)
	seen := make(map[int]struct{}, len(scored))
	for _, r := range scored {
		if r.Index < 0 || r.Index >= len(candidates) {
			continue
		}
		if _, dup := seen[r.Index]; dup {
			continue
		}
		seen[r.Index] = struct{}{}
		out = append(out, candidates[r.Index])
		if len(out) >= topK {
			break
		}
	}
	return out, nil
}

// ftsEscapeQuery wraps individual words in double quotes to escape FTS5 special characters.
func ftsEscapeQuery(query string) string {
	words := strings.Fields(query)
	if len(words) == 0 {
		return query
	}
	escaped := make([]string, len(words))
	for i, w := range words {
		// Remove any existing double quotes to prevent injection
		w = strings.ReplaceAll(w, `"`, ``)
		if w != "" {
			escaped[i] = `"` + w + `"`
		}
	}
	return strings.Join(escaped, " ")
}

func (s *SQLiteStore) InsertTriples(ctx context.Context, memoryID string, triples []memory.KnowledgeTriple) error {
	if len(triples) == 0 {
		return nil
	}

	insertAll := func(q sqlQuerier) error {
		for _, t := range triples {
			if _, err := q.ExecContext(ctx,
				`INSERT INTO knowledge_triples (memory_id, subject, predicate, object)
				 SELECT ?, ?, ?, ?
				 WHERE NOT EXISTS (
					SELECT 1 FROM knowledge_triples
					WHERE memory_id = ? AND subject = ? AND predicate = ? AND object = ?
				 )`,
				memoryID, t.Subject, t.Predicate, t.Object,
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
	tx, unlock, err := s.beginTxLocked(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer unlock()
	defer tx.Rollback() //nolint:errcheck

	if err := insertAll(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLiteStore) InsertVote(ctx context.Context, vote *ValidationVote) error {
	_, err := s.writeExecContext(ctx,
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
		FROM validation_votes WHERE memory_id = ? ORDER BY created_at, id`, memoryID)
	if err != nil {
		return nil, fmt.Errorf("get votes: %w", err)
	}
	defer func() { _ = rows.Close() }()

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
	createdAt := formatTime(challenge.CreatedAt)
	_, err := s.writeExecContext(ctx,
		`INSERT INTO challenges (memory_id, challenger_id, reason, evidence, block_height, created_at)
		 SELECT ?, ?, ?, ?, ?, ?
		 WHERE NOT EXISTS (
			SELECT 1 FROM challenges
			WHERE memory_id = ? AND challenger_id = ? AND reason = ?
			  AND COALESCE(evidence, '') = ? AND COALESCE(block_height, 0) = ?
			  AND created_at = ?
		 )`,
		challenge.MemoryID, challenge.ChallengerID, challenge.Reason, challenge.Evidence, challenge.BlockHeight, createdAt,
		challenge.MemoryID, challenge.ChallengerID, challenge.Reason, challenge.Evidence, challenge.BlockHeight, createdAt)
	if err != nil {
		return fmt.Errorf("insert challenge: %w", err)
	}
	return nil
}

// EnsureCanonicalEvidenceProjection restores count-bearing audit rows from
// AppHash-covered evidence markers without duplicating richer rows already
// projected during ordinary block execution. It is used by scoped recovery
// after state sync or projection loss and is safe to rerun.
func (s *SQLiteStore) EnsureCanonicalEvidenceProjection(
	ctx context.Context,
	memoryID string,
	corroboratorIDs, challengerIDs []string,
	at time.Time,
) (bool, error) {
	createdAt := formatTime(at)
	repaired := false
	for _, agentID := range corroboratorIDs {
		result, err := s.writeExecContext(ctx,
			`INSERT INTO corroborations (memory_id, agent_id, evidence, created_at)
			 SELECT ?, ?, ?, ?
			 WHERE NOT EXISTS (
				SELECT 1 FROM corroborations WHERE memory_id = ? AND agent_id = ?
			 )`,
			memoryID, agentID, "recovered from canonical corroboration marker", createdAt,
			memoryID, agentID)
		if err != nil {
			return repaired, fmt.Errorf("restore canonical corroboration projection: %w", err)
		}
		if rows, rowsErr := result.RowsAffected(); rowsErr != nil {
			return repaired, fmt.Errorf("inspect canonical corroboration repair: %w", rowsErr)
		} else if rows > 0 {
			repaired = true
		}
	}
	for _, agentID := range challengerIDs {
		result, err := s.writeExecContext(ctx,
			`INSERT INTO challenges (memory_id, challenger_id, reason, evidence, block_height, created_at)
			 SELECT ?, ?, ?, ?, ?, ?
			 WHERE NOT EXISTS (
				SELECT 1 FROM challenges WHERE memory_id = ? AND challenger_id = ?
			 )`,
			memoryID, agentID, "recovered from canonical app-v21 challenge marker", "", 0, createdAt,
			memoryID, agentID)
		if err != nil {
			return repaired, fmt.Errorf("restore canonical challenge projection: %w", err)
		}
		if rows, rowsErr := result.RowsAffected(); rowsErr != nil {
			return repaired, fmt.Errorf("inspect canonical challenge repair: %w", rowsErr)
		} else if rows > 0 {
			repaired = true
		}
	}
	return repaired, nil
}

// MarkEvidenceProjectionIncomplete durably records that recovery reconstructed
// only the canonical lower bound for this memory. ON CONFLICT is intentionally
// one-way: later native evidence writes cannot silently claim the lost legacy
// history became complete.
func (s *SQLiteStore) HasMemoryProjection(ctx context.Context, memoryID string) (bool, error) {
	var exists bool
	if err := s.conn.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM memories WHERE memory_id = ?)`,
		memoryID).Scan(&exists); err != nil {
		return false, fmt.Errorf("check memory projection: %w", err)
	}
	return exists, nil
}

func (s *SQLiteStore) MarkEvidenceProjectionIncomplete(ctx context.Context, memoryID string) error {
	if _, err := s.writeExecContext(ctx,
		`INSERT OR IGNORE INTO memory_evidence_projection_incomplete (memory_id) VALUES (?)`,
		memoryID); err != nil {
		return fmt.Errorf("mark evidence projection incomplete: %w", err)
	}
	return nil
}

// GetEvidenceProjectionCompleteness returns true when no durable incomplete
// marker exists. Queries are chunked below SQLite's bound-parameter limit.
func (s *SQLiteStore) GetEvidenceProjectionCompleteness(ctx context.Context, memoryIDs []string) (map[string]bool, error) {
	complete := make(map[string]bool, len(memoryIDs))
	for _, memoryID := range memoryIDs {
		complete[memoryID] = true
	}
	for start := 0; start < len(memoryIDs); start += 900 {
		end := start + 900
		if end > len(memoryIDs) {
			end = len(memoryIDs)
		}
		chunk := memoryIDs[start:end]
		placeholders := make([]string, len(chunk))
		args := make([]any, len(chunk))
		for i, memoryID := range chunk {
			placeholders[i] = "?"
			args[i] = memoryID
		}
		rows, err := s.conn.QueryContext(ctx,
			`SELECT memory_id FROM memory_evidence_projection_incomplete WHERE memory_id IN (`+
				strings.Join(placeholders, ",")+`)`,
			args...)
		if err != nil {
			return nil, fmt.Errorf("get evidence projection completeness: %w", err)
		}
		for rows.Next() {
			var memoryID string
			if scanErr := rows.Scan(&memoryID); scanErr != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan evidence projection completeness: %w", scanErr)
			}
			complete[memoryID] = false
		}
		rowsErr := rows.Err()
		_ = rows.Close()
		if rowsErr != nil {
			return nil, rowsErr
		}
	}
	return complete, nil
}

// GetChallengeCounts returns the number of distinct challengers recorded for
// each memory ID. The challenges table is an append-only off-chain audit
// projection, so this is a lifetime evidence count rather than an open-vote
// count. Queries are chunked to stay within SQLite's bound-parameter limit.
func (s *SQLiteStore) GetChallengeCounts(ctx context.Context, memoryIDs []string) (map[string]int, error) {
	counts := make(map[string]int, len(memoryIDs))
	for start := 0; start < len(memoryIDs); start += 900 {
		end := start + 900
		if end > len(memoryIDs) {
			end = len(memoryIDs)
		}
		chunk := memoryIDs[start:end]
		ph := make([]string, len(chunk))
		args := make([]any, len(chunk))
		for i, id := range chunk {
			ph[i] = "?"
			args[i] = id
		}
		rows, err := s.conn.QueryContext(ctx,
			`SELECT memory_id, COUNT(DISTINCT challenger_id) FROM challenges WHERE memory_id IN (`+strings.Join(ph, ",")+`) GROUP BY memory_id`, args...)
		if err != nil {
			return nil, fmt.Errorf("get challenge counts: %w", err)
		}
		for rows.Next() {
			var id string
			var n int
			if scanErr := rows.Scan(&id, &n); scanErr != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan challenge count: %w", scanErr)
			}
			counts[id] = n
		}
		rowsErr := rows.Err()
		_ = rows.Close()
		if rowsErr != nil {
			return nil, rowsErr
		}
	}
	return counts, nil
}

func (s *SQLiteStore) InsertCorroboration(ctx context.Context, corr *Corroboration) error {
	createdAt := formatTime(corr.CreatedAt)
	_, err := s.writeExecContext(ctx,
		`INSERT INTO corroborations (memory_id, agent_id, evidence, created_at)
		 SELECT ?, ?, ?, ?
		 WHERE NOT EXISTS (
			SELECT 1 FROM corroborations
			WHERE memory_id = ? AND agent_id = ?
			  AND COALESCE(evidence, '') = ? AND created_at = ?
		 )`,
		corr.MemoryID, corr.AgentID, corr.Evidence, createdAt,
		corr.MemoryID, corr.AgentID, corr.Evidence, createdAt)
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
	defer func() { _ = rows.Close() }()

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
	defer func() { _ = rows.Close() }()

	results := make([]*memory.MemoryRecord, 0)
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

// GetPendingByDomainPage is the stable, additive paging form used by app-v23
// disclosure-aware REST reads. GetPendingByDomain retains its historical SQL
// and ordering for pre-v23 callers.
func (s *SQLiteStore) GetPendingByDomainPage(
	ctx context.Context,
	domainTag string,
	limit, offset int,
) ([]*memory.MemoryRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	rows, err := s.conn.QueryContext(ctx,
		`SELECT memory_id, submitting_agent, content, content_hash,
			memory_type, domain_tag, confidence_score, status, created_at
		FROM memories WHERE status = 'proposed' AND domain_tag LIKE ?
		ORDER BY created_at ASC, memory_id ASC LIMIT ? OFFSET ?`,
		domainTag, limit, offset,
	)
	if err != nil {
		return nil, fmt.Errorf("get pending page: %w", err)
	}
	defer func() { _ = rows.Close() }()

	results := make([]*memory.MemoryRecord, 0, limit)
	for rows.Next() {
		var r memory.MemoryRecord
		var mt, st, createdAt string
		if scanErr := rows.Scan(&r.MemoryID, &r.SubmittingAgent, &r.Content, &r.ContentHash,
			&mt, &r.DomainTag, &r.ConfidenceScore, &st, &createdAt); scanErr != nil {
			return nil, fmt.Errorf("scan pending page: %w", scanErr)
		}
		r.MemoryType = memory.MemoryType(mt)
		r.Status = memory.MemoryStatus(st)
		r.CreatedAt = parseTime(createdAt)
		results = append(results, &r)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("get pending page rows: %w", rowsErr)
	}
	return results, nil
}

// GetVotablePendingByDomainPage is the auto-voter's recovery-aware projection
// scan. Durable unresolved app-v25 recovery rows remain visible to operators,
// but cannot consume the same bounded voter scan on every poll. Once a later
// stable migration scan resolves a row, it automatically becomes votable.
func (s *SQLiteStore) GetVotablePendingByDomainPage(
	ctx context.Context,
	domainTag string,
	limit, offset int,
) ([]*memory.MemoryRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	rows, err := s.conn.QueryContext(ctx,
		`SELECT m.memory_id, m.submitting_agent, m.content, m.content_hash,
			m.memory_type, m.domain_tag, m.confidence_score, m.status, m.created_at
		FROM memories AS m
		WHERE m.status = 'proposed' AND m.domain_tag LIKE ?
		  AND NOT EXISTS (
			SELECT 1 FROM legacy_memory_recovery AS r
			WHERE r.memory_id = m.memory_id AND r.resolved_at IS NULL
		  )
		ORDER BY m.created_at ASC, m.memory_id ASC LIMIT ? OFFSET ?`,
		domainTag, limit, offset,
	)
	if err != nil {
		return nil, fmt.Errorf("get votable pending page: %w", err)
	}
	defer func() { _ = rows.Close() }()

	results := make([]*memory.MemoryRecord, 0, limit)
	for rows.Next() {
		var r memory.MemoryRecord
		var mt, st, createdAt string
		if scanErr := rows.Scan(
			&r.MemoryID,
			&r.SubmittingAgent,
			&r.Content,
			&r.ContentHash,
			&mt,
			&r.DomainTag,
			&r.ConfidenceScore,
			&st,
			&createdAt,
		); scanErr != nil {
			return nil, fmt.Errorf("scan votable pending page: %w", scanErr)
		}
		r.MemoryType = memory.MemoryType(mt)
		r.Status = memory.MemoryStatus(st)
		r.CreatedAt = parseTime(createdAt)
		results = append(results, &r)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("get votable pending page rows: %w", rowsErr)
	}
	return results, nil
}

// OldestProposedCreatedAt returns MIN(created_at) over status='proposed'
// memories — the voter-observability probe behind
// sage_proposed_oldest_age_seconds. ok=false (zero time) when nothing is
// pending. Keep in lockstep with the PostgresStore implementation.
func (s *SQLiteStore) OldestProposedCreatedAt(ctx context.Context) (time.Time, bool, error) {
	var createdAt sql.NullString
	err := s.conn.QueryRowContext(ctx,
		`SELECT MIN(created_at) FROM memories WHERE status = 'proposed'`).Scan(&createdAt)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("oldest proposed: %w", err)
	}
	if !createdAt.Valid {
		return time.Time{}, false, nil
	}
	return parseTime(createdAt.String), true, nil
}

// ProposedPendingCount returns how many memories are still in status='proposed'
// (sage_proposed_pending_count). Keep in lockstep with the PostgresStore
// implementation.
func (s *SQLiteStore) ProposedPendingCount(ctx context.Context) (int, error) {
	var n int
	err := s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM memories WHERE status = 'proposed'`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("proposed pending count: %w", err)
	}
	return n, nil
}

func (s *SQLiteStore) ListMemories(ctx context.Context, opts ListOptions) ([]*memory.MemoryRecord, int, error) {
	if opts.Limit <= 0 {
		opts.Limit = 50
	}

	countQuery := `SELECT COUNT(*) FROM memories WHERE 1=1`
	query := `SELECT memory_id, submitting_agent, content, content_hash,
		memory_type, domain_tag, provider, confidence_score, status, parent_hash, created_at,
		committed_at, deprecated_at, COALESCE(task_status, '') FROM memories WHERE 1=1`
	var args []any

	if opts.DomainTag != "" {
		filter := " AND domain_tag = ?"
		query += filter
		countQuery += filter
		args = append(args, opts.DomainTag)
	}
	for _, prefix := range opts.ExcludeDomainPrefixes {
		if prefix == "" {
			continue
		}
		filter := " AND LOWER(domain_tag) NOT LIKE ?"
		query += filter
		countQuery += filter
		args = append(args, strings.ToLower(prefix)+"%")
	}
	if opts.Provider != "" {
		filter := " AND (provider = ? OR provider = '' OR memory_type = 'fact')"
		query += filter
		countQuery += filter
		args = append(args, opts.Provider)
	}
	switch {
	case opts.Status == "active":
		// "active" = everything EXCEPT deprecated. Deprecated memories are
		// audit-only and must not surface in normal CEREBRUM browse/search. This
		// is opt-IN (only the human browse UI passes it), so internal callers with
		// an empty Status — export/backup, counts, dedup — still see all statuses.
		query += " AND status != 'deprecated'"
		countQuery += " AND status != 'deprecated'"
	case opts.Status != "":
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
	if opts.Tag != "" {
		filter := " AND memory_id IN (SELECT memory_id FROM memory_tags WHERE tag = ?)"
		query += filter
		countQuery += filter
		args = append(args, opts.Tag)
	}
	// created_at is stored as ISO-8601 UTC text (RFC3339Nano, e.g.
	// 2026-07-07T03:49:13.7Z; trailing-zero fractions are elided). The fixed
	// date/time-to-seconds prefix makes a lexicographic >=/<= range compare a valid
	// chronological filter at day/second granularity. Callers pass precise ISO bounds;
	// the dashboard sends a fraction-less whole-second upper bound so a boundary-second
	// row (which sorts after any ".fffZ") is not wrongly excluded.
	if opts.CreatedFrom != "" {
		filter := " AND created_at >= ?"
		query += filter
		countQuery += filter
		args = append(args, opts.CreatedFrom)
	}
	if opts.CreatedTo != "" {
		filter := " AND created_at <= ?"
		query += filter
		countQuery += filter
		args = append(args, opts.CreatedTo)
	}
	if len(opts.SubmittingAgents) > 0 {
		placeholders := make([]string, len(opts.SubmittingAgents))
		for i, a := range opts.SubmittingAgents {
			placeholders[i] = "?"
			args = append(args, a)
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
	if opts.StablePaging {
		query += ", memory_id ASC"
	}

	query += " LIMIT ? OFFSET ?"
	queryArgs := make([]any, len(args), len(args)+2)
	copy(queryArgs, args)
	queryArgs = append(queryArgs, opts.Limit, opts.Offset)

	var total int
	if !opts.SkipTotal {
		if err := s.conn.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
			return nil, 0, fmt.Errorf("count memories: %w", err)
		}
	}

	rows, err := s.conn.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("list memories: %w", err)
	}
	defer func() { _ = rows.Close() }()

	results := make([]*memory.MemoryRecord, 0)
	for rows.Next() {
		var r memory.MemoryRecord
		var mt, st, createdAt, taskStatus string
		var parentHash, committedAt, deprecatedAt *string
		if scanErr := rows.Scan(&r.MemoryID, &r.SubmittingAgent, &r.Content, &r.ContentHash,
			&mt, &r.DomainTag, &r.Provider, &r.ConfidenceScore, &st, &parentHash,
			&createdAt, &committedAt, &deprecatedAt, &taskStatus); scanErr != nil {
			return nil, 0, fmt.Errorf("scan memory: %w", scanErr)
		}
		r.MemoryType = memory.MemoryType(mt)
		r.Status = memory.MemoryStatus(st)
		r.TaskStatus = memory.TaskStatus(taskStatus)
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
	return s.getStats(ctx, nil)
}

// GetStatsExcludingDomainPrefixes is the presentation-safe aggregate variant.
// The complete GetStats contract remains unchanged for audit/internal callers.
func (s *SQLiteStore) GetStatsExcludingDomainPrefixes(ctx context.Context, prefixes []string) (*StoreStats, error) {
	return s.getStats(ctx, prefixes)
}

func (s *SQLiteStore) getStats(ctx context.Context, excludedPrefixes []string) (*StoreStats, error) {
	stats := &StoreStats{
		ByDomain: make(map[string]int),
		ByStatus: make(map[string]int),
	}
	excludedSQL := ""
	excludedArgs := make([]any, 0, len(excludedPrefixes))
	for _, prefix := range excludedPrefixes {
		if prefix == "" {
			continue
		}
		excludedSQL += " AND LOWER(domain_tag) NOT LIKE ?"
		excludedArgs = append(excludedArgs, strings.ToLower(prefix)+"%")
	}

	if err := s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM memories WHERE status != 'deprecated'`+excludedSQL,
		excludedArgs...,
	).Scan(&stats.TotalMemories); err != nil {
		return nil, fmt.Errorf("count total: %w", err)
	}

	rows, err := s.conn.QueryContext(ctx,
		`SELECT domain_tag, COUNT(*) FROM memories WHERE status != 'deprecated'`+excludedSQL+` GROUP BY domain_tag`,
		excludedArgs...,
	)
	if err != nil {
		return nil, fmt.Errorf("count by domain: %w", err)
	}
	for rows.Next() {
		var domain string
		var count int
		if scanErr := rows.Scan(&domain, &count); scanErr != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan domain count: %w", scanErr)
		}
		stats.ByDomain[domain] = count
	}
	_ = rows.Close()

	rows, err = s.conn.QueryContext(ctx,
		`SELECT status, COUNT(*) FROM memories WHERE 1=1`+excludedSQL+` GROUP BY status`,
		excludedArgs...,
	)
	if err != nil {
		return nil, fmt.Errorf("count by status: %w", err)
	}
	for rows.Next() {
		var status string
		var count int
		if scanErr := rows.Scan(&status, &count); scanErr != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan status count: %w", scanErr)
		}
		stats.ByStatus[status] = count
	}
	_ = rows.Close()

	// Count by agent
	stats.ByAgent = make(map[string]int)
	rows, err = s.conn.QueryContext(ctx,
		`SELECT submitting_agent, COUNT(*) FROM memories WHERE 1=1`+excludedSQL+` GROUP BY submitting_agent`,
		excludedArgs...,
	)
	if err != nil {
		return nil, fmt.Errorf("count by agent: %w", err)
	}
	for rows.Next() {
		var agent string
		var count int
		if scanErr := rows.Scan(&agent, &count); scanErr != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan agent count: %w", scanErr)
		}
		stats.ByAgent[agent] = count
	}
	_ = rows.Close()

	var lastActivity *string
	if scanErr := s.conn.QueryRowContext(ctx,
		`SELECT MAX(created_at) FROM memories WHERE 1=1`+excludedSQL,
		excludedArgs...,
	).Scan(&lastActivity); scanErr == nil {
		stats.LastActivity = parseTimePtr(lastActivity)
	}

	// Get DB file size
	if info, err := os.Stat(s.dbPath); err == nil {
		stats.DBSizeBytes = info.Size()
	}

	return stats, nil
}

func (s *SQLiteStore) GetTimeline(ctx context.Context, from, to time.Time, domain string, bucket string) ([]TimelineBucket, error) {
	return s.getTimeline(ctx, from, to, domain, bucket, nil)
}

func (s *SQLiteStore) FormatTimelinePeriod(at time.Time, bucket string) string {
	at = at.UTC()
	switch bucket {
	case "hour":
		return at.Truncate(time.Hour).Format("2006-01-02T15:00:00Z")
	case "week":
		yearStart := time.Date(at.Year(), time.January, 1, 0, 0, 0, 0, time.UTC)
		firstMondayOffset := (8 - int(yearStart.Weekday())) % 7
		yearDay := at.YearDay() - 1
		week := 0
		if yearDay >= firstMondayOffset {
			week = ((yearDay - firstMondayOffset) / 7) + 1
		}
		return fmt.Sprintf("%04d-W%02d", at.Year(), week)
	case "month":
		return at.Format("2006-01")
	default:
		return at.Format("2006-01-02")
	}
}

func (s *SQLiteStore) GetTimelineExcludingDomainPrefixes(
	ctx context.Context,
	from, to time.Time,
	domain, bucket string,
	prefixes []string,
) ([]TimelineBucket, error) {
	return s.getTimeline(ctx, from, to, domain, bucket, prefixes)
}

func (s *SQLiteStore) getTimeline(
	ctx context.Context,
	from, to time.Time,
	domain, bucket string,
	excludedPrefixes []string,
) ([]TimelineBucket, error) {
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
	for _, prefix := range excludedPrefixes {
		if prefix == "" {
			continue
		}
		query += " AND LOWER(domain_tag) NOT LIKE ?"
		args = append(args, strings.ToLower(prefix)+"%")
	}

	query += " GROUP BY period ORDER BY period"

	rows, err := s.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get timeline: %w", err)
	}
	defer func() { _ = rows.Close() }()

	buckets := make([]TimelineBucket, 0)
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
	_, err := s.writeExecContext(ctx,
		`UPDATE memories SET status = 'deprecated', deprecated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE memory_id = ?`,
		memoryID)
	if err != nil {
		return fmt.Errorf("delete memory: %w", err)
	}
	// Clean up FTS5 index — deprecated memories shouldn't appear in text search.
	_, _ = s.writeExecContext(ctx, `DELETE FROM memories_fts WHERE memory_id = ?`, memoryID)
	return nil
}

// BackfillFTS populates the FTS5 index from existing memories that aren't yet
// indexed. Only works when vault is nil (plaintext available). Call after vault
// setup, and ASYNCHRONOUSLY (see the call site) — on a large chain the initial
// index build is a CPU-bound SQLite sort that must never block node startup.
//
// A cheap COUNT gate short-circuits when the index already covers every active
// memory, so a restart does NOT re-run the expensive work. This matters because
// memories_fts is an FTS5 virtual table whose columns are NOT B-tree indexed for
// equality: the `LEFT JOIN memories_fts ON memory_id = memory_id` anti-join below
// forces SQLite to materialize and sort the whole index (minutes of pegged CPU on
// an 830k-row chain). Pre-gate, that ran on every boot — even with nothing to
// insert — and wedged startup before the first block was produced. Every
// store/update/deprecate path keeps FTS in sync incrementally, so once the initial
// backfill completes the gate makes subsequent boots a sub-second no-op.
func (s *SQLiteStore) BackfillFTS(ctx context.Context) error {
	if s.vault.Load() != nil {
		return nil // Can't index encrypted content
	}

	// Cheap gate (reads only — no writeMu, so it never blocks the block-mirror
	// writer): skip when the index already covers the active set. count(*) on
	// memories_fts scans the FTS5 content shadow table (no tokenization/sort);
	// count on memories uses idx_memories_status. Deprecation removes the row from
	// FTS too, so ftsCount tracks active-indexed rows — ftsCount >= activeCount
	// means every active memory is already indexed.
	var ftsCount, activeCount int64
	if err := s.conn.QueryRowContext(ctx, `SELECT count(*) FROM memories_fts`).Scan(&ftsCount); err != nil {
		return fmt.Errorf("backfill FTS: count index: %w", err)
	}
	if err := s.conn.QueryRowContext(ctx, `SELECT count(*) FROM memories WHERE status != 'deprecated'`).Scan(&activeCount); err != nil {
		return fmt.Errorf("backfill FTS: count memories: %w", err)
	}
	if ftsCount >= activeCount {
		return nil // index already complete — skip the expensive anti-join
	}

	_, err := s.writeExecContext(ctx, `
		INSERT INTO memories_fts(memory_id, content, domain_tag)
		SELECT m.memory_id, m.content, m.domain_tag
		FROM memories m
		LEFT JOIN memories_fts f ON f.memory_id = m.memory_id
		WHERE f.memory_id IS NULL AND m.status != 'deprecated'
	`)
	if err != nil {
		return fmt.Errorf("backfill FTS: %w", err)
	}
	return nil
}

func (s *SQLiteStore) UpdateDomainTag(ctx context.Context, memoryID string, domain string) error {
	_, err := s.writeExecContext(ctx,
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
	_, err := s.writeExecContext(ctx,
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
	defer func() { _ = rows.Close() }()

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
	_, err := s.writeExecContext(ctx,
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
	_, err := s.writeExecContext(ctx,
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
	return s.getActiveGrants(ctx, agentID, 0, false)
}

func (s *SQLiteStore) GetActiveGrantsBounded(ctx context.Context, agentID string, limit int) ([]*AccessGrantEntry, error) {
	return s.getActiveGrants(ctx, agentID, limit, true)
}

func (s *SQLiteStore) getActiveGrants(ctx context.Context, agentID string, limit int, filterExpired bool) ([]*AccessGrantEntry, error) {
	query := `SELECT domain, grantee_id, granter_id, access_level, expires_at, created_height, created_at
		FROM access_grants
		WHERE grantee_id = ? AND revoked_at IS NULL`
	args := []any{agentID}
	if filterExpired {
		// RFC3339Nano strings are not safely orderable within the same second
		// when one value omits the fractional component ('.' sorts before 'Z').
		// Parse both sides as instants before applying the candidate LIMIT.
		query += ` AND (expires_at IS NULL OR julianday(expires_at) > julianday(?))`
		args = append(args, formatTime(time.Now().UTC()))
	}
	query += `
		ORDER BY created_at`
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.conn.QueryContext(ctx,
		query, args...)
	if err != nil {
		return nil, fmt.Errorf("get active grants: %w", err)
	}
	defer func() { _ = rows.Close() }()

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
	_, err := s.writeExecContext(ctx,
		`UPDATE access_grants SET revoked_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		WHERE domain = ? AND grantee_id = ? AND revoked_at IS NULL`,
		domain, granteeID)
	if err != nil {
		return fmt.Errorf("revoke grant: %w", err)
	}
	return nil
}

func (s *SQLiteStore) InsertAccessRequest(ctx context.Context, req *AccessRequestEntry) error {
	_, err := s.writeExecContext(ctx,
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
	_, err := s.writeExecContext(ctx,
		`UPDATE access_requests SET status = ?, resolved_height = ? WHERE request_id = ?`,
		status, height, requestID)
	if err != nil {
		return fmt.Errorf("update access request status: %w", err)
	}
	return nil
}

func (s *SQLiteStore) InsertAccessLog(ctx context.Context, log *AccessLogEntry) error {
	memoryIDsJSON := encodeStringSlice(log.MemoryIDs)
	_, err := s.writeExecContext(ctx,
		`INSERT INTO access_logs (agent_id, domain, action, memory_ids, block_height, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		log.AgentID, log.Domain, log.Action, memoryIDsJSON, log.BlockHeight, formatTime(log.CreatedAt))
	if err != nil {
		return fmt.Errorf("insert access log: %w", err)
	}
	return nil
}

func (s *SQLiteStore) InsertDomain(ctx context.Context, domain *DomainEntry) error {
	_, err := s.writeExecContext(ctx,
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
	_, err := s.writeExecContext(ctx,
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
	_, err := s.writeExecContext(ctx,
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
	_, err := s.writeExecContext(ctx,
		`UPDATE org_members SET removed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		WHERE org_id = ? AND agent_id = ? AND removed_at IS NULL`,
		orgID, agentID)
	if err != nil {
		return fmt.Errorf("remove org member: %w", err)
	}
	return nil
}

func (s *SQLiteStore) UpdateMemberClearance(ctx context.Context, orgID, agentID string, clearance ClearanceLevel) error {
	_, err := s.writeExecContext(ctx,
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
	defer func() { _ = rows.Close() }()

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
	_, err := s.writeExecContext(ctx,
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
	_, err := s.writeExecContext(ctx,
		`UPDATE federations SET status = 'active', approved_height = ?
		WHERE federation_id = ? AND status = 'proposed'`,
		height, federationID)
	if err != nil {
		return fmt.Errorf("approve federation: %w", err)
	}
	return nil
}

func (s *SQLiteStore) RevokeFederation(ctx context.Context, federationID string, height int64) error {
	_, err := s.writeExecContext(ctx,
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
	defer func() { _ = rows.Close() }()

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
	_, err := s.writeExecContext(ctx,
		`UPDATE memories SET classification = ? WHERE memory_id = ?`,
		int(classification), memoryID)
	if err != nil {
		return fmt.Errorf("update memory classification: %w", err)
	}
	return nil
}

// --- Department methods ---

func (s *SQLiteStore) InsertDept(ctx context.Context, dept *DeptEntry) error {
	_, err := s.writeExecContext(ctx,
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
	defer func() { _ = rows.Close() }()

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
	_, err := s.writeExecContext(ctx,
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
	_, err := s.writeExecContext(ctx,
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
	defer func() { _ = rows.Close() }()

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
	_, err := s.writeExecContext(ctx,
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
		SELECT a.agent_id, a.name, COALESCE(a.registered_name,''), a.role, COALESCE(a.avatar,''), COALESCE(a.boot_bio,''),
			COALESCE(a.validator_pubkey,''), COALESCE(a.node_id,''), COALESCE(a.p2p_address,''),
			a.status, a.clearance, COALESCE(a.org_id,''), COALESCE(a.dept_id,''),
			COALESCE(a.domain_access,''), COALESCE(a.bundle_path,''),
			a.first_seen, a.last_seen, a.created_at, a.removed_at,
			COALESCE(a.on_chain_height, 0), COALESCE(a.visible_agents, ''), COALESCE(a.capabilities, 0), COALESCE(a.provider, ''),
			COALESCE((SELECT COUNT(*) FROM memories WHERE submitting_agent = a.agent_id), 0),
			(SELECT MAX(COALESCE(committed_at, created_at)) FROM memories WHERE submitting_agent = a.agent_id AND status = 'committed'),
			COALESCE(a.claim_token, ''), a.claim_expires_at
		FROM network_agents a
		WHERE a.status != 'removed'
		ORDER BY a.created_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("list agents: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var agents []*AgentEntry
	for rows.Next() {
		a := &AgentEntry{}
		var firstSeen, lastSeen, createdAt, removedAt, lastCommittedAt, claimExpiry *string
		if scanErr := rows.Scan(&a.AgentID, &a.Name, &a.RegisteredName, &a.Role, &a.Avatar, &a.BootBio,
			&a.ValidatorPubkey, &a.NodeID, &a.P2PAddress, &a.Status, &a.Clearance,
			&a.OrgID, &a.DeptID, &a.DomainAccess, &a.BundlePath,
			&firstSeen, &lastSeen, &createdAt, &removedAt,
			&a.OnChainHeight, &a.VisibleAgents, &a.Capabilities, &a.Provider, &a.MemoryCount, &lastCommittedAt,
			&a.ClaimToken, &claimExpiry); scanErr != nil {
			return nil, fmt.Errorf("scan agent: %w", scanErr)
		}
		a.FirstSeen = parseTimePtr(firstSeen)
		a.LastSeen = parseTimePtr(lastSeen)
		if createdAt != nil {
			a.CreatedAt = parseTime(*createdAt)
		}
		a.RemovedAt = parseTimePtr(removedAt)
		a.LastCommittedMemoryAt = parseTimePtr(lastCommittedAt)
		a.ClaimExpiresAt = parseTimePtr(claimExpiry)
		if a.RegisteredName == "" {
			a.RegisteredName = a.Name // backfill for pre-existing agents
		}
		agents = append(agents, a)
	}
	return agents, nil
}

// ListAgentDirectory returns the local recipient identity projection without
// computing roster-wide derived memory counts. Canonical enrollment filtering
// remains the REST layer's responsibility because SQL status is only a cache
// after app-v23.
func (s *SQLiteStore) ListAgentDirectory(ctx context.Context, limit int) ([]*AgentEntry, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.conn.QueryContext(ctx, `
		SELECT agent_id, name, COALESCE(registered_name,''), COALESCE(provider,''), status, removed_at
		FROM network_agents
		WHERE status != 'removed'
		ORDER BY created_at ASC, agent_id
		LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("list agent directory: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanPipeContactLookupAgents(rows)
}

// FindPipeContactLookupCandidates returns a deliberately small agent metadata
// projection for federated human-name discovery. The SQL result is capped
// before live Badger RBAC checks and exact field matches sort ahead of substring
// matches, so a short name such as "mynah" can find the voice bridge without
// weakening exact target/address resolution. LIKE metacharacters are escaped;
// SQLite's LOWER/NOCASE behavior folds ASCII only.
func (s *SQLiteStore) FindPipeContactLookupCandidates(ctx context.Context, query string, limit int) ([]*AgentEntry, error) {
	return s.findPipeContactLookupCandidatePage(ctx, query, limit, 0)
}

func (s *SQLiteStore) findPipeContactLookupCandidatePage(
	ctx context.Context,
	query string,
	limit, offset int,
) ([]*AgentEntry, error) {
	if offset < 0 {
		return nil, nil
	}
	exact, pattern, limit, ok := normalizeAgentNameLookup(query, limit, 0)
	if !ok {
		return nil, nil
	}
	rows, err := s.conn.QueryContext(ctx, `
		SELECT agent_id, name, COALESCE(registered_name,''), COALESCE(provider,''), status, removed_at
		FROM network_agents
		WHERE status = 'active' AND removed_at IS NULL
		  AND (
		    LOWER(agent_id) LIKE ? ESCAPE '\'
		    OR LOWER(name) LIKE ? ESCAPE '\'
		    OR LOWER(COALESCE(registered_name,'')) LIKE ? ESCAPE '\'
		    OR LOWER(COALESCE(provider,'')) LIKE ? ESCAPE '\'
		  )
			ORDER BY CASE
			  WHEN LOWER(agent_id) LIKE ? ESCAPE '\'
			    OR LOWER(name) LIKE ? ESCAPE '\'
			    OR LOWER(COALESCE(registered_name,'')) LIKE ? ESCAPE '\'
			    OR LOWER(COALESCE(provider,'')) LIKE ? ESCAPE '\' THEN 0
			  ELSE 1
			END, LOWER(name), agent_id
			LIMIT ? OFFSET ?`,
		pattern, pattern, pattern, pattern,
		exact, exact, exact, exact,
		limit, offset,
	)
	if err != nil {
		return nil, fmt.Errorf("find pipe contact lookup candidates: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanPipeContactLookupAgents(rows)
}

// FindAgentsByName is the bounded local recipient lookup used by MCP. Unlike
// peer-facing contact discovery, a local operator may use a human substring
// such as "mynah" for "MYNAH (SAGE Voice Bridge Agent)". This projection stays
// metadata-only and capped; it never invokes ListAgents' roster-wide derived
// memory-count query.
func (s *SQLiteStore) FindAgentsByName(ctx context.Context, query string, limit int) ([]*AgentEntry, error) {
	if limit > maxAgentNameLookupResults {
		limit = maxAgentNameLookupResults
	}
	return s.FindAgentsByNamePage(ctx, query, limit, 0)
}

// FindAgentLookupCandidates returns one bounded metadata-only candidate batch
// for the signed REST recipient lookup. Canonical enrollment authorization is
// applied by the REST layer after this single SQL query.
func (s *SQLiteStore) FindAgentLookupCandidates(ctx context.Context, query string, limit int) ([]*AgentEntry, error) {
	if limit > maxAgentNameLookupCandidates {
		limit = maxAgentNameLookupCandidates
	}
	return s.findPipeContactLookupCandidatePage(ctx, query, limit, 0)
}

// FindAgentsByNamePage exposes one stable, bounded SQL candidate page. The
// signed REST lookup walks these pages while applying canonical Badger
// enrollment checks, so SQL-active but consensus-pending registrations cannot
// consume the public result limit and hide a later active recipient.
func (s *SQLiteStore) FindAgentsByNamePage(
	ctx context.Context,
	query string,
	limit, offset int,
) ([]*AgentEntry, error) {
	if limit > maxAgentNameLookupResults {
		limit = maxAgentNameLookupResults
	}
	return s.findPipeContactLookupCandidatePage(ctx, query, limit, offset)
}

// FindPipeContactAgentsByIDPrefix resolves a friendly handle suffix without
// loading the complete local agent roster. Callers must validate the hex prefix
// and enforce a small limit before calling it.
func (s *SQLiteStore) FindPipeContactAgentsByIDPrefix(ctx context.Context, prefix string, limit int) ([]*AgentEntry, error) {
	if limit <= 0 || prefix == "" {
		return nil, nil
	}
	rows, err := s.conn.QueryContext(ctx, `
		SELECT agent_id, name, COALESCE(registered_name,''), COALESCE(provider,''), status, removed_at
		FROM network_agents
		WHERE status != 'removed' AND agent_id LIKE ? ESCAPE '\'
		ORDER BY agent_id
		LIMIT ?`, strings.ToLower(prefix)+"%", limit)
	if err != nil {
		return nil, fmt.Errorf("find pipe contact agents by ID prefix: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanPipeContactLookupAgents(rows)
}

// ListPipeContactStatusCandidates returns a deterministic, small legacy-status
// candidate set. Federation adds effective owners separately, so this query
// only needs active local agents and must never become a roster-wide metadata
// load under the status authorization leases.
func (s *SQLiteStore) ListPipeContactStatusCandidates(ctx context.Context, limit int) ([]*AgentEntry, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.conn.QueryContext(ctx, `
		SELECT agent_id, name, COALESCE(registered_name,''), COALESCE(provider,''), status, removed_at
		FROM network_agents
		WHERE status='active' AND removed_at IS NULL
		ORDER BY agent_id
		LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("list pipe contact status candidates: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanPipeContactLookupAgents(rows)
}

// GetPipeContactAgents loads only the metadata needed for contact projection.
// It deliberately avoids GetAgent's derived memory count and chunks requests so
// a peer-RBAC policy with many distinct owners remains below SQLite's host
// parameter ceiling while federation status holds its authorization snapshot.
func (s *SQLiteStore) GetPipeContactAgents(ctx context.Context, agentIDs []string) ([]*AgentEntry, error) {
	const maxPipeContactAgentsPerQuery = 512
	if len(agentIDs) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(agentIDs))
	selected := make([]string, 0, len(agentIDs))
	for _, agentID := range agentIDs {
		if agentID == "" {
			continue
		}
		if _, duplicate := seen[agentID]; duplicate {
			continue
		}
		seen[agentID] = struct{}{}
		selected = append(selected, agentID)
	}
	out := make([]*AgentEntry, 0, len(selected))
	for start := 0; start < len(selected); start += maxPipeContactAgentsPerQuery {
		end := start + maxPipeContactAgentsPerQuery
		if end > len(selected) {
			end = len(selected)
		}
		batch := selected[start:end]
		placeholders := make([]string, len(batch))
		args := make([]any, len(batch))
		for i, agentID := range batch {
			placeholders[i] = "?"
			args[i] = agentID
		}
		rows, err := s.conn.QueryContext(ctx, `
			SELECT agent_id, name, COALESCE(registered_name,''), COALESCE(provider,''), status, removed_at
			FROM network_agents
			WHERE agent_id IN (`+strings.Join(placeholders, ",")+`)
			ORDER BY agent_id`, args...)
		if err != nil {
			return nil, fmt.Errorf("load pipe contact agents: %w", err)
		}
		agents, scanErr := scanPipeContactLookupAgents(rows)
		closeErr := rows.Close()
		if scanErr != nil {
			return nil, scanErr
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close pipe contact agents: %w", closeErr)
		}
		out = append(out, agents...)
	}
	return out, nil
}

func scanPipeContactLookupAgents(rows *sql.Rows) ([]*AgentEntry, error) {
	agents := make([]*AgentEntry, 0)
	for rows.Next() {
		agent := &AgentEntry{}
		var removedAt *string
		if err := rows.Scan(&agent.AgentID, &agent.Name, &agent.RegisteredName, &agent.Provider, &agent.Status, &removedAt); err != nil {
			return nil, fmt.Errorf("scan pipe contact lookup agent: %w", err)
		}
		agent.RemovedAt = parseTimePtr(removedAt)
		if agent.RegisteredName == "" {
			agent.RegisteredName = agent.Name
		}
		agents = append(agents, agent)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pipe contact lookup agents: %w", err)
	}
	return agents, nil
}

func (s *SQLiteStore) GetAgent(ctx context.Context, agentID string) (*AgentEntry, error) {
	a := &AgentEntry{}
	var firstSeen, lastSeen, createdAt, removedAt, lastCommittedAt, claimExpiry *string
	err := s.conn.QueryRowContext(ctx, `
		SELECT a.agent_id, a.name, COALESCE(a.registered_name,''), a.role, COALESCE(a.avatar,''), COALESCE(a.boot_bio,''),
			COALESCE(a.validator_pubkey,''), COALESCE(a.node_id,''), COALESCE(a.p2p_address,''),
			a.status, a.clearance, COALESCE(a.org_id,''), COALESCE(a.dept_id,''),
			COALESCE(a.domain_access,''), COALESCE(a.bundle_path,''),
			a.first_seen, a.last_seen, a.created_at, a.removed_at,
			COALESCE(a.on_chain_height, 0), COALESCE(a.visible_agents, ''), COALESCE(a.capabilities, 0), COALESCE(a.provider, ''),
			COALESCE((SELECT COUNT(*) FROM memories WHERE submitting_agent = a.agent_id), 0),
			(SELECT MAX(COALESCE(committed_at, created_at)) FROM memories WHERE submitting_agent = a.agent_id AND status = 'committed'),
			COALESCE(a.claim_token, ''), a.claim_expires_at
		FROM network_agents a WHERE a.agent_id = ?`, agentID).Scan(
		&a.AgentID, &a.Name, &a.RegisteredName, &a.Role, &a.Avatar, &a.BootBio,
		&a.ValidatorPubkey, &a.NodeID, &a.P2PAddress, &a.Status, &a.Clearance,
		&a.OrgID, &a.DeptID, &a.DomainAccess, &a.BundlePath,
		&firstSeen, &lastSeen, &createdAt, &removedAt,
		&a.OnChainHeight, &a.VisibleAgents, &a.Capabilities, &a.Provider, &a.MemoryCount, &lastCommittedAt,
		&a.ClaimToken, &claimExpiry)
	if err != nil {
		return nil, fmt.Errorf("get agent: %w", err)
	}
	a.FirstSeen = parseTimePtr(firstSeen)
	a.LastSeen = parseTimePtr(lastSeen)
	if createdAt != nil {
		a.CreatedAt = parseTime(*createdAt)
	}
	a.RemovedAt = parseTimePtr(removedAt)
	a.LastCommittedMemoryAt = parseTimePtr(lastCommittedAt)
	a.ClaimExpiresAt = parseTimePtr(claimExpiry)
	if a.RegisteredName == "" {
		a.RegisteredName = a.Name // backfill for pre-existing agents
	}
	return a, nil
}

func (s *SQLiteStore) GetAgentByName(ctx context.Context, name string) (*AgentEntry, error) {
	a := &AgentEntry{}
	var firstSeen, lastSeen, createdAt, removedAt, lastCommittedAt, claimExpiry *string
	err := s.conn.QueryRowContext(ctx, `
		SELECT a.agent_id, a.name, COALESCE(a.registered_name,''), a.role, COALESCE(a.avatar,''), COALESCE(a.boot_bio,''),
			COALESCE(a.validator_pubkey,''), COALESCE(a.node_id,''), COALESCE(a.p2p_address,''),
			a.status, a.clearance, COALESCE(a.org_id,''), COALESCE(a.dept_id,''),
			COALESCE(a.domain_access,''), COALESCE(a.bundle_path,''),
			a.first_seen, a.last_seen, a.created_at, a.removed_at,
			COALESCE(a.on_chain_height, 0), COALESCE(a.visible_agents, ''), COALESCE(a.capabilities, 0), COALESCE(a.provider, ''),
			COALESCE((SELECT COUNT(*) FROM memories WHERE submitting_agent = a.agent_id), 0),
			(SELECT MAX(COALESCE(committed_at, created_at)) FROM memories WHERE submitting_agent = a.agent_id AND status = 'committed'),
			COALESCE(a.claim_token, ''), a.claim_expires_at
		FROM network_agents a WHERE a.name = ? AND a.status != 'removed'`, name).Scan(
		&a.AgentID, &a.Name, &a.RegisteredName, &a.Role, &a.Avatar, &a.BootBio,
		&a.ValidatorPubkey, &a.NodeID, &a.P2PAddress, &a.Status, &a.Clearance,
		&a.OrgID, &a.DeptID, &a.DomainAccess, &a.BundlePath,
		&firstSeen, &lastSeen, &createdAt, &removedAt,
		&a.OnChainHeight, &a.VisibleAgents, &a.Capabilities, &a.Provider, &a.MemoryCount, &lastCommittedAt,
		&a.ClaimToken, &claimExpiry)
	if err != nil {
		return nil, nil // not found — return nil, nil per interface contract
	}
	a.FirstSeen = parseTimePtr(firstSeen)
	a.LastSeen = parseTimePtr(lastSeen)
	if createdAt != nil {
		a.CreatedAt = parseTime(*createdAt)
	}
	a.RemovedAt = parseTimePtr(removedAt)
	a.LastCommittedMemoryAt = parseTimePtr(lastCommittedAt)
	a.ClaimExpiresAt = parseTimePtr(claimExpiry)
	if a.RegisteredName == "" {
		a.RegisteredName = a.Name // backfill for pre-existing agents
	}
	return a, nil
}

func (s *SQLiteStore) CreateAgent(ctx context.Context, agent *AgentEntry) error {
	return s.withAgentContactMutation(func() error {
		return s.createAgent(ctx, agent)
	})
}

func (s *SQLiteStore) createAgent(ctx context.Context, agent *AgentEntry) error {
	var claimExpiry *string
	if agent.ClaimExpiresAt != nil {
		t := agent.ClaimExpiresAt.Format(time.RFC3339)
		claimExpiry = &t
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	firstSeen := now
	if agent.FirstSeen != nil {
		firstSeen = agent.FirstSeen.Format(time.RFC3339Nano)
	}
	createdAt := now
	if !agent.CreatedAt.IsZero() {
		createdAt = agent.CreatedAt.Format(time.RFC3339Nano)
	}
	_, err := s.writeExecContext(ctx, `
		INSERT INTO network_agents (agent_id, name, registered_name, role, avatar, boot_bio, validator_pubkey,
			node_id, p2p_address, status, clearance, org_id, dept_id, domain_access, bundle_path,
			on_chain_height, visible_agents, capabilities, provider, claim_token, claim_expires_at,
			first_seen, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		agent.AgentID, agent.Name, agent.RegisteredName, agent.Role, agent.Avatar, agent.BootBio, agent.ValidatorPubkey,
		agent.NodeID, agent.P2PAddress, agent.Status, agent.Clearance, agent.OrgID, agent.DeptID,
		agent.DomainAccess, agent.BundlePath, agent.OnChainHeight, agent.VisibleAgents, agent.Capabilities, agent.Provider,
		agent.ClaimToken, claimExpiry, firstSeen, createdAt)
	if err != nil {
		return fmt.Errorf("create agent: %w", err)
	}
	return nil
}

func (s *SQLiteStore) UpdateAgent(ctx context.Context, agent *AgentEntry) error {
	return s.withAgentContactMutation(func() error {
		return s.updateAgent(ctx, agent)
	})
}

func (s *SQLiteStore) updateAgent(ctx context.Context, agent *AgentEntry) error {
	var claimExpiry *string
	if agent.ClaimExpiresAt != nil {
		t := agent.ClaimExpiresAt.Format(time.RFC3339)
		claimExpiry = &t
	}
	_, err := s.writeExecContext(ctx, `
		UPDATE network_agents SET name=?, role=?, avatar=?, boot_bio=?, clearance=?,
			org_id=?, dept_id=?, domain_access=?, p2p_address=?,
			on_chain_height=?, visible_agents=?, capabilities=?, provider=?,
			claim_token=?, claim_expires_at=?
		WHERE agent_id=?`,
		agent.Name, agent.Role, agent.Avatar, agent.BootBio, agent.Clearance,
		agent.OrgID, agent.DeptID, agent.DomainAccess, agent.P2PAddress,
		agent.OnChainHeight, agent.VisibleAgents, agent.Capabilities, agent.Provider,
		agent.ClaimToken, claimExpiry, agent.AgentID)
	if err != nil {
		return fmt.Errorf("update agent: %w", err)
	}
	return nil
}

func (s *SQLiteStore) RemoveAgent(ctx context.Context, agentID string) error {
	return s.withLinkedAuthorizationInvalidation(func() error {
		return s.withAgentContactMutation(func() error {
			_, err := s.writeExecContext(ctx, `
				UPDATE network_agents SET status='removed', removed_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
				WHERE agent_id=?`, agentID)
			if err != nil {
				return fmt.Errorf("remove agent: %w", err)
			}
			return nil
		})
	})
}

func (s *SQLiteStore) UpdateAgentStatus(ctx context.Context, agentID, status string) error {
	mutate := func() error {
		return s.withAgentContactMutation(func() error {
			_, err := s.writeExecContext(ctx, `
				UPDATE network_agents
				SET status=?, removed_at=CASE WHEN ?='active' THEN NULL ELSE removed_at END
				WHERE agent_id=?`, status, status, agentID)
			if err != nil {
				return fmt.Errorf("update agent status: %w", err)
			}
			return nil
		})
	}
	if status == "active" {
		return mutate()
	}
	return s.withLinkedAuthorizationInvalidation(mutate)
}

func (s *SQLiteStore) UpdateAgentLastSeen(ctx context.Context, agentID string, lastSeen time.Time) error {
	ts := lastSeen.UTC().Format("2006-01-02T15:04:05.000Z")
	return s.withAgentContactMutation(func() error {
		_, err := s.writeExecContext(ctx, `UPDATE network_agents SET last_seen=?, status='active' WHERE agent_id=?`, ts, agentID)
		if err != nil {
			return fmt.Errorf("update agent last seen: %w", err)
		}
		return nil
	})
}

func (s *SQLiteStore) BackfillFirstSeen(ctx context.Context, agentID string, firstSeen time.Time) error {
	ts := firstSeen.UTC().Format("2006-01-02T15:04:05.000Z")
	_, err := s.writeExecContext(ctx, `UPDATE network_agents SET first_seen=? WHERE agent_id=? AND first_seen IS NULL`, ts, agentID)
	if err != nil {
		return fmt.Errorf("backfill first_seen: %w", err)
	}
	return nil
}

func (s *SQLiteStore) RotateAgentKey(ctx context.Context, oldAgentID string) (string, []byte, error) {
	var newAgentID string
	var seed []byte
	err := s.withLinkedAuthorizationInvalidation(func() error {
		return s.withAgentContactMutation(func() error {
			var rotateErr error
			newAgentID, seed, rotateErr = s.rotateAgentKey(ctx, oldAgentID)
			return rotateErr
		})
	})
	return newAgentID, seed, err
}

func (s *SQLiteStore) rotateAgentKey(ctx context.Context, oldAgentID string) (string, []byte, error) {
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
		tx, unlock, txErr := s.beginTxLocked(ctx)
		if txErr != nil {
			return "", nil, fmt.Errorf("begin tx: %w", txErr)
		}
		defer unlock()
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

func (s *SQLiteStore) ReassignMemories(ctx context.Context, sourceAgentID, targetAgentID string) (int64, error) {
	var count int64

	doReassign := func(q sqlQuerier) error {
		// 1. Validate target agent exists and is not removed
		var status string
		if err := q.QueryRowContext(ctx, `SELECT status FROM network_agents WHERE agent_id=?`, targetAgentID).Scan(&status); err != nil {
			return fmt.Errorf("target agent not found: %s", targetAgentID)
		}
		if status == "removed" {
			return fmt.Errorf("cannot reassign to removed agent %s", targetAgentID)
		}

		// 2. Count memories from source agent
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM memories WHERE submitting_agent=?`, sourceAgentID).Scan(&count); err != nil {
			return fmt.Errorf("count source memories: %w", err)
		}
		if count == 0 {
			return nil
		}

		// 3. Reassign memories
		_, err := q.ExecContext(ctx, `UPDATE memories SET submitting_agent=? WHERE submitting_agent=?`, targetAgentID, sourceAgentID)
		if err != nil {
			return fmt.Errorf("reassign memories: %w", err)
		}

		// 4. Log in redeployment_log
		details, _ := json.Marshal(map[string]interface{}{
			"source": sourceAgentID,
			"target": targetAgentID,
			"count":  count,
		})
		_, err = q.ExecContext(ctx, `
			INSERT INTO redeployment_log (operation, agent_id, phase, status, details, started_at)
			VALUES ('memory_reassign', ?, 'REASSIGNED', 'completed', ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))`,
			targetAgentID, string(details))
		if err != nil {
			return fmt.Errorf("log reassignment: %w", err)
		}

		return nil
	}

	// If we have a *sql.DB (not already in a tx), wrap in transaction
	if s.db != nil {
		tx, unlock, txErr := s.beginTxLocked(ctx)
		if txErr != nil {
			return 0, fmt.Errorf("begin tx: %w", txErr)
		}
		defer unlock()
		defer tx.Rollback() //nolint:errcheck

		if err := doReassign(tx); err != nil {
			return 0, err
		}
		if err := tx.Commit(); err != nil {
			return 0, fmt.Errorf("commit: %w", err)
		}
	} else {
		// Already in a transaction
		if err := doReassign(s.conn); err != nil {
			return 0, err
		}
	}

	return count, nil
}

func (s *SQLiteStore) ListAgentTags(ctx context.Context, agentID string) ([]TagCount, error) {
	rows, err := s.conn.QueryContext(ctx, `
		SELECT mt.tag, COUNT(*) as cnt
		FROM memory_tags mt
		INNER JOIN memories m ON mt.memory_id = m.memory_id
		WHERE m.submitting_agent = ?
		GROUP BY mt.tag
		ORDER BY cnt DESC`, agentID)
	if err != nil {
		return nil, fmt.Errorf("list agent tags: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tags []TagCount
	for rows.Next() {
		var tc TagCount
		if err := rows.Scan(&tc.Tag, &tc.Count); err != nil {
			return nil, fmt.Errorf("scan tag count: %w", err)
		}
		tags = append(tags, tc)
	}
	return tags, rows.Err()
}

func (s *SQLiteStore) ListAgentDomains(ctx context.Context, agentID string) ([]string, error) {
	return s.ListAgentDomainsBounded(ctx, agentID, 0)
}

func (s *SQLiteStore) ListAgentDomainsBounded(ctx context.Context, agentID string, limit int) ([]string, error) {
	query := `
		SELECT domain_tag, COUNT(*) as cnt
		FROM memories
		WHERE submitting_agent = ? AND domain_tag != ''
		GROUP BY domain_tag
		ORDER BY cnt DESC, domain_tag ASC`
	args := []any{agentID}
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list agent domains: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var domains []string
	for rows.Next() {
		var domain string
		var cnt int64
		if err := rows.Scan(&domain, &cnt); err != nil {
			return nil, fmt.Errorf("scan agent domain: %w", err)
		}
		domains = append(domains, domain)
	}
	return domains, rows.Err()
}

func (s *SQLiteStore) AcquireRedeployLock(ctx context.Context, lockedBy, operation string, ttl time.Duration) error {
	now := time.Now().UTC()
	expires := now.Add(ttl)
	// Try to insert the singleton lock row. If it exists, check if expired.
	_, err := s.writeExecContext(ctx, `
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
	_, err := s.writeExecContext(ctx, `DELETE FROM redeployment_lock WHERE id=1`)
	return err
}

// ClearStaleRedeployLogs marks lingering in_progress log rows as rolled_back so
// an abandoned/crashed run can't wedge the status poll forever.
func (s *SQLiteStore) ClearStaleRedeployLogs(ctx context.Context) (int, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := s.writeExecContext(ctx,
		`UPDATE redeployment_log SET status='rolled_back', completed_at=?, error='cleared: stale/abandoned redeployment run' WHERE status='in_progress'`, now)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
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
	res, err := s.writeExecContext(ctx, `
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
	defer func() { _ = rows.Close() }()

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

func (s *SQLiteStore) GetLatestRedeployLog(ctx context.Context) (*RedeploymentLogEntry, error) {
	row := s.conn.QueryRowContext(ctx, `
		SELECT id, operation, agent_id, phase, status, COALESCE(details,''),
			COALESCE(sqlite_backup,''), COALESCE(genesis_backup,''),
			started_at, completed_at, COALESCE(error,'')
		FROM redeployment_log ORDER BY id DESC LIMIT 1`)
	e := &RedeploymentLogEntry{}
	var startedAt, completedAt *string
	err := row.Scan(&e.ID, &e.Operation, &e.AgentID, &e.Phase, &e.Status,
		&e.Details, &e.SQLiteBackup, &e.GenesisBackup,
		&startedAt, &completedAt, &e.Error)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get latest redeploy log: %w", err)
	}
	e.StartedAt = parseTimePtr(startedAt)
	e.CompletedAt = parseTimePtr(completedAt)
	return e, nil
}

func (s *SQLiteStore) UpdateRedeployLog(ctx context.Context, id int64, status, errorMsg string) error {
	now := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	_, err := s.writeExecContext(ctx, `
		UPDATE redeployment_log SET status=?, completed_at=?, error=? WHERE id=?`,
		status, now, errorMsg, id)
	return err
}

// FindByContentHash checks if a committed memory with this content hash exists.
// The contentHash parameter is the hex-encoded SHA-256 hash of the content.
//
// The predicate MUST be committed-only. The voter's dedupCheck runs while the
// candidate memory is itself sitting in this table with status='proposed', so the
// previous predicate (status != 'deprecated') matched the candidate's OWN row —
// every per-node vote became a self-inflicted "duplicate content" reject. On a
// single-validator chain that reject was unanimous, so every memory was
// deprecated on arrival (and on legacy multi-validator sets it wedged memories at
// proposed). See RepairSelfDupRejected for the recovery path.
func (s *SQLiteStore) FindByContentHash(ctx context.Context, contentHash string) (bool, error) {
	hashBytes, err := hex.DecodeString(contentHash)
	if err != nil {
		return false, fmt.Errorf("decode content hash: %w", err)
	}
	var count int
	err = s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM memories WHERE content_hash = ? AND status = 'committed'`,
		hashBytes).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// RepairSelfDupRejected resurrects memories wrongly deprecated by the dedup
// self-match bug (see FindByContentHash): the per-node voter rejected every
// memory as a "duplicate" of its own proposed row, and on a single-validator
// chain that unanimous reject deprecated it on arrival.
//
// A memory qualifies ONLY when its recorded vote history is exactly one vote —
// selfID rejecting with the dedupCheck rationale — and it was never challenged.
// That fingerprint cannot match legitimately deprecated memories: quorum
// rejections on real multi-validator sets carry multiple votes, challenge
// deprecations carry a challenges row, and the legacy 4-archetype era always
// recorded 4 votes per memory.
//
// For each candidate, flipChain (the caller's chain-state flip, e.g. badger
// status + vote-key cleanup) runs FIRST; only on its success does the mirror row
// flip back to proposed and the bogus vote row drop. A failure between the two
// leaves the candidate matched again on the next startup, so the repair is
// re-entrant — flipChain must therefore be idempotent. Returns the number of
// repaired memories.
func (s *SQLiteStore) RepairSelfDupRejected(ctx context.Context, selfID string, flipChain func(memoryID string) error) (int, error) {
	rows, err := s.conn.QueryContext(ctx, `
		SELECT m.memory_id FROM memories m
		WHERE m.status = 'deprecated'
		  AND EXISTS (SELECT 1 FROM validation_votes v
		              WHERE v.memory_id = m.memory_id AND v.validator_id = ?
		                AND v.decision = 'reject' AND v.rationale LIKE 'duplicate content%')
		  AND NOT EXISTS (SELECT 1 FROM validation_votes v2
		              WHERE v2.memory_id = m.memory_id AND NOT (v2.validator_id = ?
		                AND v2.decision = 'reject' AND v2.rationale LIKE 'duplicate content%'))
		  AND NOT EXISTS (SELECT 1 FROM challenges c WHERE c.memory_id = m.memory_id)`,
		selfID, selfID)
	if err != nil {
		return 0, fmt.Errorf("repair self-dup-rejected: scan candidates: %w", err)
	}
	candidates := make([]string, 0)
	for rows.Next() {
		var id string
		if scanErr := rows.Scan(&id); scanErr != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("repair self-dup-rejected: scan id: %w", scanErr)
		}
		candidates = append(candidates, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("repair self-dup-rejected: iterate: %w", err)
	}
	_ = rows.Close()

	repaired := 0
	for _, id := range candidates {
		if flipChain != nil {
			if err := flipChain(id); err != nil {
				return repaired, fmt.Errorf("repair self-dup-rejected: chain flip %s: %w", id, err)
			}
		}
		if _, err := s.writeExecContext(ctx,
			`UPDATE memories SET status = 'proposed', deprecated_at = NULL WHERE memory_id = ?`, id); err != nil {
			return repaired, fmt.Errorf("repair self-dup-rejected: repropose %s: %w", id, err)
		}
		if _, err := s.writeExecContext(ctx,
			`DELETE FROM validation_votes WHERE memory_id = ? AND validator_id = ?`, id, selfID); err != nil {
			return repaired, fmt.Errorf("repair self-dup-rejected: drop vote %s: %w", id, err)
		}
		repaired++
	}
	return repaired, nil
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

// ClaimProjectionBatch implements the cross-store crash boundary used by
// SageApp.Commit. The receipt and every mirror write commit in one SQLite
// transaction. A SIGKILL after that transaction but before Badger Commit makes
// Comet replay the block; the existing receipt then turns the replay into a
// no-op instead of duplicating append-only audit rows. Receipts are retained:
// a validator returning from an arbitrarily long partition may share an
// already-advanced PostgreSQL projection and must still recognize old replays.
// The storage tradeoff is one indexed height/hash row per projected block.
// This is exact-replay protection, not a general cross-store rollback protocol:
// operator snapshots must still restore Badger, SQL, and CometBFT as a pair.
func (s *SQLiteStore) ClaimProjectionBatch(ctx context.Context, height int64, appHash []byte) (bool, error) {
	if s.db != nil {
		return false, errors.New("claim projection batch must run inside RunInTx")
	}
	if height <= 0 {
		return false, fmt.Errorf("claim projection batch: invalid height %d", height)
	}
	if len(appHash) != sha256.Size {
		return false, fmt.Errorf("claim projection batch at height %d: AppHash must be %d bytes, got %d", height, sha256.Size, len(appHash))
	}
	result, err := s.conn.ExecContext(ctx, `
		INSERT INTO abci_projection_batches (block_height, app_hash)
		VALUES (?, ?)
		ON CONFLICT (block_height) DO NOTHING`, height, appHash)
	if err != nil {
		return false, fmt.Errorf("claim projection batch at height %d: %w", height, err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("claim projection batch at height %d rows affected: %w", height, err)
	}
	if inserted == 0 {
		var existing []byte
		if err := s.conn.QueryRowContext(ctx,
			`SELECT app_hash FROM abci_projection_batches WHERE block_height = ?`, height,
		).Scan(&existing); err != nil {
			return false, fmt.Errorf("read projection batch at height %d: %w", height, err)
		}
		if !bytes.Equal(existing, appHash) {
			return false, fmt.Errorf("projection batch height %d already belongs to AppHash %x, refusing %x", height, existing, appHash)
		}
		return false, nil
	}

	return true, nil
}

// LockAgentContactRead holds the agent identity, display label, and availability
// inputs used by the peer-scoped contact projection stable through one bounded
// federation side effect. The write side wraps every agent mutation.
func (s *SQLiteStore) LockAgentContactRead() func() {
	if s == nil || s.agentContactGate == nil {
		return func() {}
	}
	s.agentContactGate.RLock()
	return s.agentContactGate.RUnlock
}

// SetFederationAuthorizationMutationHook configures the process-local linked
// delivery publication hook during startup.
func (s *SQLiteStore) SetFederationAuthorizationMutationHook(
	hook func(remoteChainID string) func(),
) {
	if s != nil {
		if s.federationAuthorizationMutationHooks == nil {
			s.federationAuthorizationMutationHooks = &authorizationMutationHookState{}
		}
		s.federationAuthorizationMutationHooks.mu.Lock()
		s.federationAuthorizationMutationHooks.hook = hook
		s.federationAuthorizationMutationHooks.mu.Unlock()
	}
}

func (s *SQLiteStore) beginFederationAuthorizationMutation(
	remoteChainID string,
) func() {
	if s == nil {
		return func() {}
	}
	hook, releaseHookState :=
		s.federationAuthorizationMutationHooks.acquire()
	if hook == nil {
		return releaseHookState
	}
	releaseAuthorization := hook(remoteChainID)
	return func() {
		releaseAuthorization()
		releaseHookState()
	}
}

func (s *SQLiteStore) withAgentContactMutation(fn func() error) error {
	// A tx-scoped clone is created only by runInTx. The public RunInTx has
	// no safe way to acquire another process lock after SQLite's writeMu;
	// projection batches that mutate agents therefore enter through
	// RunInAgentContactTx, which acquires the write side in the right order.
	if s.db == nil {
		if s.agentContactGate != nil && !s.agentContactWriteHeld {
			return errors.New("agent mutation inside transaction requires RunInAgentContactTx")
		}
		return fn()
	}
	if s.agentContactGate == nil {
		return fn()
	}
	s.agentContactGate.Lock()
	defer s.agentContactGate.Unlock()
	return fn()
}

func (s *SQLiteStore) withLinkedAuthorizationInvalidation(
	fn func() error,
) error {
	// Every tx clone already owns writeMu, and RunInAgentContactTx additionally
	// owns agentContactGate.Write. Waiting for a linked-delivery drain from
	// either callback would invert delivery's order (delivery lease -> writeMu
	// and contact Read) and can deadlock. These lifecycle invalidators cannot
	// succeed through an ordinary clone anyway; enter through the base store so
	// the delivery barrier is published before either SQLite/contact lock.
	if s != nil && s.db == nil {
		return errors.New(
			"linked authorization invalidation is not permitted inside SQLite transaction",
		)
	}
	releaseAuthorization := s.beginFederationAuthorizationMutation("")
	defer releaseAuthorization()
	return fn()
}

// RunInTx executes a contact-neutral callback within a SQLite transaction. All
// writes through the tx-scoped OffchainStore are atomic — either all succeed
// or all roll back.
func (s *SQLiteStore) RunInTx(ctx context.Context, fn func(tx OffchainStore) error) error {
	return s.runInTx(ctx, false, fn)
}

// RunInAgentContactTx is used by an ABCI projection batch that can create or
// update network_agents. Only that batch takes the contact write lease; an
// unrelated transaction therefore cannot be stalled by a slow authenticated
// peer that is reading a contact snapshot.
func (s *SQLiteStore) RunInAgentContactTx(ctx context.Context, fn func(tx OffchainStore) error) error {
	return s.runInTx(ctx, true, fn)
}

// runPipelineTx is reserved for transactions whose schema does not touch
// network_agents. It prevents a self-deadlock when an imported-pipe operation
// already holds the contact read lease through its durable SQLite transition.
func (s *SQLiteStore) runPipelineTx(ctx context.Context, fn func(tx OffchainStore) error) error {
	return s.runInTx(ctx, false, fn)
}

func (s *SQLiteStore) runInTx(ctx context.Context, contactMutation bool, fn func(tx OffchainStore) error) error {
	if s.db == nil {
		// Already in a transaction — execute directly.
		return fn(s)
	}
	if contactMutation && s.agentContactGate != nil {
		s.agentContactGate.Lock()
		defer s.agentContactGate.Unlock()
	}
	// Serialize write transactions at the Go level to prevent SQLITE_BUSY.
	// SQLite's busy_timeout handles statement-level contention, but concurrent
	// DEFERRED transactions that both escalate to write locks can still fail.
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	txStore := &SQLiteStore{
		conn: tx, dbPath: s.dbPath,
		vaultGeneration: s.vaultGeneration,
		syncPolicyGate:  s.syncPolicyGate, syncOriginGate: s.syncOriginGate,
		agentContactGate:                     s.agentContactGate,
		agentContactWriteHeld:                contactMutation,
		federationAuthorizationMutationHooks: s.federationAuthorizationMutationHooks,
	}
	txStore.vault.Store(s.vault.Load())
	txStore.vaultExpected.Store(s.vaultExpected.Load())
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
	_, err := s.writeExecContext(ctx,
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
	defer func() { _ = rows.Close() }()

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
			memory_type, domain_tag, provider, confidence_score, status, parent_hash, created_at, committed_at, deprecated_at, COALESCE(task_status, '')
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
	defer func() { _ = rows.Close() }()

	records := make([]*memory.MemoryRecord, 0)
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
	result, err := s.writeExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("deprecate memories: %w", err)
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}

// ResolveChallengedMemories sweeps LEGACY stale "challenged" rows to "deprecated"
// at boot. Before v4.5.0 a challenge could leave a memory limbo-'challenged'; this
// one-shot migration cleans those up.
//
// app-v17 (v11.5) reintroduced a LIVE, reinstatable 'challenged' state (the
// quorum-scaled two-phase challenge). A post-v17 dispute is written to the mirror
// with disputed_height = the challenge-execution height (> 0), so it MUST be
// scoped OUT of this sweep — otherwise a reboot would silently deprecate an
// in-flight dispute and diverge the mirror from badger. Legacy limbo rows predate
// the disputed_height column and read the default 0, so `disputed_height = 0` is
// the exact legacy-vs-v17 discriminator. COALESCE guards DBs where the column was
// somehow left NULL.
func (s *SQLiteStore) ResolveChallengedMemories(ctx context.Context) (int, error) {
	result, err := s.writeExecContext(ctx,
		`UPDATE memories SET status = 'deprecated', deprecated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		WHERE status = 'challenged' AND COALESCE(disputed_height, 0) = 0`)
	if err != nil {
		return 0, fmt.Errorf("resolve challenged: %w", err)
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}

// scanMemoryRow scans a single memory row from a *sql.Rows.
func (s *SQLiteStore) scanMemoryRow(rows *sql.Rows) (*memory.MemoryRecord, error) {
	var r memory.MemoryRecord
	var mt, st, createdAt, taskStatus string
	var embData []byte
	var parentHash, committedAt, deprecatedAt *string

	err := rows.Scan(&r.MemoryID, &r.SubmittingAgent, &r.Content, &r.ContentHash,
		&embData, &r.EmbeddingHash, &mt, &r.DomainTag, &r.Provider, &r.ConfidenceScore,
		&st, &parentHash, &createdAt, &committedAt, &deprecatedAt, &taskStatus)
	if err != nil {
		return nil, fmt.Errorf("scan memory: %w", err)
	}

	r.MemoryType = memory.MemoryType(mt)
	r.Status = memory.MemoryStatus(st)
	r.TaskStatus = memory.TaskStatus(taskStatus)

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

// UpdateTaskStatus updates the task_status of a task memory.
func (s *SQLiteStore) UpdateTaskStatus(ctx context.Context, memoryID string, taskStatus memory.TaskStatus) error {
	terminal := taskStatus == memory.TaskStatusDone || taskStatus == memory.TaskStatusDropped
	if terminal && s.db != nil {
		return s.RunInTx(ctx, func(tx OffchainStore) error {
			return tx.(*SQLiteStore).UpdateTaskStatus(ctx, memoryID, taskStatus)
		})
	}
	query := `UPDATE memories
		SET task_requires_handoff = CASE
		      WHEN task_status IN ('done','dropped') THEN 1 ELSE task_requires_handoff END,
		    assignee = CASE WHEN task_status IN ('done','dropped') AND ? IN ('planned','in_progress')
		      THEN '' ELSE assignee END,
		    task_board_position = CASE WHEN task_status != ? THEN 0 ELSE task_board_position END,
		    task_status_updated_at = CASE WHEN task_status != ?
		      THEN strftime('%Y-%m-%dT%H:%M:%fZ', 'now') ELSE task_status_updated_at END,
		    task_status = ?
		WHERE memory_id = ? AND memory_type = 'task'
		  AND (? != 'in_progress' OR (task_status NOT IN ('done','dropped') AND COALESCE(assignee, '') != ''))`
	args := []any{string(taskStatus), string(taskStatus), string(taskStatus), string(taskStatus), memoryID, string(taskStatus)}
	if terminal {
		// Keep the last assignee on terminal cards as durable attribution. A later
		// reopen clears it in the non-terminal arm and requires a fresh handoff.
		query = `UPDATE memories
			SET task_status = ?,
			    task_board_position = CASE WHEN task_status != ? THEN 0 ELSE task_board_position END,
			    task_status_updated_at = CASE WHEN task_status != ?
			      THEN strftime('%Y-%m-%dT%H:%M:%fZ', 'now') ELSE task_status_updated_at END,
			    task_assignment_version = task_assignment_version + CASE WHEN COALESCE(assignee, '') != '' THEN 1 ELSE 0 END,
			    assignee = CASE WHEN COALESCE(assignee, '') != '' THEN assignee
			      WHEN COALESCE(task_picked_up_by, '') != '' THEN task_picked_up_by
			      WHEN EXISTS (SELECT 1 FROM network_agents a WHERE a.agent_id = memories.submitting_agent) THEN submitting_agent
			      ELSE '' END,
			    task_requires_handoff = 1
			WHERE memory_id = ? AND memory_type = 'task'`
		args = []any{string(taskStatus), string(taskStatus), string(taskStatus), memoryID}
	}
	result, err := s.writeExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update task status: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("task not found or in_progress requires an assignee: %s", memoryID)
	}
	if terminal {
		if _, err := s.writeExecContext(ctx,
			`UPDATE agent_notifications SET state = 'superseded' WHERE task_id = ? AND state = 'unread'`, memoryID); err != nil {
			return fmt.Errorf("supersede terminal task notifications: %w", err)
		}
	}
	return nil
}

// LinkMemories creates a link between two memories.
func (s *SQLiteStore) LinkMemories(ctx context.Context, sourceID, targetID, linkType string) error {
	_, err := s.writeExecContext(ctx,
		`INSERT INTO memory_links (source_id, target_id, link_type) VALUES (?, ?, ?) ON CONFLICT DO NOTHING`,
		sourceID, targetID, linkType)
	if err != nil {
		return fmt.Errorf("link memories: %w", err)
	}
	return nil
}

// GetLinkedMemories returns all memories linked to the given memory ID.
func (s *SQLiteStore) GetLinkedMemories(ctx context.Context, memoryID string) ([]memory.MemoryLink, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT source_id, target_id, link_type, created_at FROM memory_links
		WHERE source_id = ? OR target_id = ?`, memoryID, memoryID)
	if err != nil {
		return nil, fmt.Errorf("get linked memories: %w", err)
	}
	defer func() { _ = rows.Close() }()

	links := make([]memory.MemoryLink, 0)
	for rows.Next() {
		var l memory.MemoryLink
		if err := rows.Scan(&l.SourceID, &l.TargetID, &l.LinkType, &l.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan link: %w", err)
		}
		links = append(links, l)
	}
	return links, rows.Err()
}

// GetCorroborationCounts returns the distinct corroborator count for each memory
// ID in a single batched query (chunked to stay within SQLite's bound-parameter
// limit).
func (s *SQLiteStore) GetCorroborationCounts(ctx context.Context, memoryIDs []string) (map[string]int, error) {
	counts := make(map[string]int, len(memoryIDs))
	for start := 0; start < len(memoryIDs); start += 900 {
		end := start + 900
		if end > len(memoryIDs) {
			end = len(memoryIDs)
		}
		chunk := memoryIDs[start:end]
		ph := make([]string, len(chunk))
		args := make([]any, len(chunk))
		for i, id := range chunk {
			ph[i] = "?"
			args[i] = id
		}
		rows, err := s.conn.QueryContext(ctx,
			`SELECT memory_id, COUNT(DISTINCT agent_id) FROM corroborations WHERE memory_id IN (`+strings.Join(ph, ",")+`) GROUP BY memory_id`, args...)
		if err != nil {
			return nil, fmt.Errorf("get corroboration counts: %w", err)
		}
		for rows.Next() {
			var id string
			var n int
			if scanErr := rows.Scan(&id, &n); scanErr != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan corroboration count: %w", scanErr)
			}
			counts[id] = n
		}
		rowsErr := rows.Err()
		_ = rows.Close()
		if rowsErr != nil {
			return nil, rowsErr
		}
	}
	return counts, nil
}

// GetLinksAmong returns typed links where BOTH endpoints are in memoryIDs, in one
// query per chunk (vs. one GetLinkedMemories per memory). It queries by source_id
// and filters target membership in Go, keeping each query within the parameter limit.
func (s *SQLiteStore) GetLinksAmong(ctx context.Context, memoryIDs []string) ([]memory.MemoryLink, error) {
	links := make([]memory.MemoryLink, 0)
	if len(memoryIDs) == 0 {
		return links, nil
	}
	inSet := make(map[string]bool, len(memoryIDs))
	for _, id := range memoryIDs {
		inSet[id] = true
	}
	for start := 0; start < len(memoryIDs); start += 900 {
		end := start + 900
		if end > len(memoryIDs) {
			end = len(memoryIDs)
		}
		chunk := memoryIDs[start:end]
		ph := make([]string, len(chunk))
		args := make([]any, len(chunk))
		for i, id := range chunk {
			ph[i] = "?"
			args[i] = id
		}
		rows, err := s.conn.QueryContext(ctx,
			`SELECT source_id, target_id, link_type, created_at FROM memory_links WHERE source_id IN (`+strings.Join(ph, ",")+`)`, args...)
		if err != nil {
			return nil, fmt.Errorf("get links among: %w", err)
		}
		for rows.Next() {
			var l memory.MemoryLink
			if scanErr := rows.Scan(&l.SourceID, &l.TargetID, &l.LinkType, &l.CreatedAt); scanErr != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan link: %w", scanErr)
			}
			if inSet[l.TargetID] {
				links = append(links, l)
			}
		}
		rowsErr := rows.Err()
		_ = rows.Close()
		if rowsErr != nil {
			return nil, rowsErr
		}
	}
	return links, nil
}

// GetOpenTasks returns all task memories that are planned or in_progress.
func (s *SQLiteStore) GetOpenTasks(ctx context.Context, domain string, provider string, assignee string) ([]*memory.MemoryRecord, error) {
	query := `SELECT memory_id, submitting_agent, content, content_hash, embedding, embedding_hash,
		memory_type, domain_tag, provider, confidence_score, status, parent_hash, created_at, committed_at, deprecated_at, COALESCE(task_status, '')
		FROM memories
		WHERE memory_type = 'task'
		AND task_status IN ('planned', 'in_progress')
		AND status NOT IN ('deprecated')`
	var args []any

	if domain != "" {
		query += ` AND domain_tag = ?`
		args = append(args, domain)
	}
	// A signed agent sees only work explicitly assigned to its immutable agent
	// ID. Provider identifies the authoring client, not the owner, and must never
	// widen an agent backlog to unassigned work.
	if assignee != "" {
		query += ` AND assignee = ?`
		args = append(args, assignee)
	} else if provider != "" {
		query += ` AND (provider = ? OR provider = '')`
		args = append(args, provider)
	}
	query += ` ORDER BY created_at DESC LIMIT 500`

	rows, err := s.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get open tasks: %w", err)
	}
	defer func() { _ = rows.Close() }()

	records := make([]*memory.MemoryRecord, 0)
	for rows.Next() {
		rec, err := s.scanMemoryRow(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	_ = rows.Close() // release the query before hydrating board-only fields
	if err := s.populateTaskAssignees(ctx, records); err != nil {
		return nil, err
	}
	return records, nil
}

// GetOpenTasksPage is the stable, additive paging form used by app-v23 REST.
// It preserves exact-assignee isolation while allowing disclosure filtering to
// continue beyond a revoked first 500 rows.
func (s *SQLiteStore) GetOpenTasksPage(
	ctx context.Context,
	domain, provider, assignee string,
	limit, offset int,
) ([]*memory.MemoryRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	query := `SELECT memory_id, submitting_agent, content, content_hash, embedding, embedding_hash,
		memory_type, domain_tag, provider, confidence_score, status, parent_hash, created_at, committed_at, deprecated_at, COALESCE(task_status, '')
		FROM memories
		WHERE memory_type = 'task'
		AND task_status IN ('planned', 'in_progress')
		AND status NOT IN ('deprecated')`
	var args []any
	if domain != "" {
		query += ` AND domain_tag = ?`
		args = append(args, domain)
	}
	if assignee != "" {
		query += ` AND assignee = ?`
		args = append(args, assignee)
	} else if provider != "" {
		query += ` AND (provider = ? OR provider = '')`
		args = append(args, provider)
	}
	query += ` ORDER BY created_at DESC, memory_id ASC LIMIT ? OFFSET ?`
	args = append(args, limit, offset)

	rows, err := s.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get open task page: %w", err)
	}
	defer func() { _ = rows.Close() }()

	records := make([]*memory.MemoryRecord, 0, limit)
	for rows.Next() {
		rec, scanErr := s.scanMemoryRow(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		records = append(records, rec)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, rowsErr
	}
	_ = rows.Close()
	if err := s.populateTaskAssignees(ctx, records); err != nil {
		return nil, err
	}
	return records, nil
}

// GetAllTasks returns all task memories across all statuses for the Kanban board.
func (s *SQLiteStore) GetAllTasks(ctx context.Context, domain string, limit int) ([]*memory.MemoryRecord, error) {
	if limit <= 0 {
		limit = 100
	} else if limit > 500 {
		limit = 500
	}
	query := `SELECT memory_id, submitting_agent, content, content_hash, embedding, embedding_hash,
		memory_type, domain_tag, provider, confidence_score, status, parent_hash, created_at, committed_at, deprecated_at, COALESCE(task_status, '')
		FROM memories
		WHERE memory_type = 'task'
		AND status NOT IN ('deprecated')`
	var args []any

	if domain != "" {
		query += ` AND domain_tag = ?`
		args = append(args, domain)
	}

	query += ` ORDER BY CASE task_status
		WHEN 'in_progress' THEN 1
		WHEN 'planned' THEN 2
		WHEN 'done' THEN 3
		WHEN 'dropped' THEN 4
		ELSE 0 END,
		CASE WHEN COALESCE(task_board_position, 0) = 0 THEN 0 ELSE 1 END,
		task_board_position ASC, created_at DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	records := make([]*memory.MemoryRecord, 0)
	for rows.Next() {
		rec, err := s.scanMemoryRow(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	_ = rows.Close() // done reading; the follow-up assignee query reuses the conn
	if err := s.populateTaskAssignees(ctx, records); err != nil {
		return nil, err
	}
	return records, nil
}

// ReorderTasks persists a complete visible ordering for one board column.
// Cards not present in orderedIDs retain their relative order after the supplied
// cards, which keeps a bounded UI request from losing older history.
func (s *SQLiteStore) ReorderTasks(ctx context.Context, taskStatus memory.TaskStatus, orderedIDs []string) error {
	if s.db != nil {
		return s.RunInTx(ctx, func(tx OffchainStore) error {
			return tx.(*SQLiteStore).ReorderTasks(ctx, taskStatus, orderedIDs)
		})
	}
	rows, err := s.conn.QueryContext(ctx, `SELECT memory_id FROM memories
		WHERE memory_type = 'task' AND status != 'deprecated' AND task_status = ?
		ORDER BY CASE WHEN COALESCE(task_board_position, 0) = 0 THEN 0 ELSE 1 END,
		         task_board_position ASC, created_at DESC`, string(taskStatus))
	if err != nil {
		return fmt.Errorf("read task board order: %w", err)
	}
	defer func() { _ = rows.Close() }()
	all := make([]string, 0)
	valid := make(map[string]struct{})
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("scan task board order: %w", err)
		}
		all = append(all, id)
		valid[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read task board order: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close task board order rows: %w", err)
	}
	seen := make(map[string]struct{}, len(orderedIDs))
	final := make([]string, 0, len(all))
	for _, id := range orderedIDs {
		if _, ok := valid[id]; !ok {
			return fmt.Errorf("task %s is not a board card in %s", id, taskStatus)
		}
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("duplicate task in board order: %s", id)
		}
		seen[id] = struct{}{}
		final = append(final, id)
	}
	for _, id := range all {
		if _, included := seen[id]; !included {
			final = append(final, id)
		}
	}
	for i, id := range final {
		if _, err := s.conn.ExecContext(ctx, `UPDATE memories SET task_board_position = ? WHERE memory_id = ?`, i+1, id); err != nil {
			return fmt.Errorf("persist task board order: %w", err)
		}
	}
	return nil
}

// populateTaskAssignees fills task board-only fields that are not part of
// scanMemoryRow's fixed column set.
func (s *SQLiteStore) populateTaskAssignees(ctx context.Context, records []*memory.MemoryRecord) error {
	if len(records) == 0 {
		return nil
	}
	ph := make([]string, len(records))
	args := make([]any, len(records))
	for i, rec := range records {
		ph[i] = "?"
		args[i] = rec.MemoryID
	}
	rows, err := s.conn.QueryContext(ctx,
		`SELECT memory_id, COALESCE(assignee, ''), COALESCE(task_picked_up_by, ''), task_picked_up_at, task_status_updated_at
		 FROM memories WHERE memory_id IN (`+strings.Join(ph, ",")+`)`, args...)
	if err != nil {
		return fmt.Errorf("query task assignment fields: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type taskBoardFields struct {
		assignee        string
		pickedBy        string
		pickedUpAt      *time.Time
		statusUpdatedAt *time.Time
	}
	byID := make(map[string]taskBoardFields, len(records))
	for rows.Next() {
		var id, a, pickedBy string
		var pickedAt, statusUpdatedAt *string
		if err := rows.Scan(&id, &a, &pickedBy, &pickedAt, &statusUpdatedAt); err != nil {
			return fmt.Errorf("scan task assignment fields: %w", err)
		}
		byID[id] = taskBoardFields{
			assignee:        a,
			pickedBy:        pickedBy,
			pickedUpAt:      parseTimePtr(pickedAt),
			statusUpdatedAt: parseTimePtr(statusUpdatedAt),
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read task assignment fields: %w", err)
	}
	for _, rec := range records {
		fields := byID[rec.MemoryID]
		rec.Assignee = fields.assignee
		rec.TaskPickedUpBy = fields.pickedBy
		rec.TaskPickedUpAt = fields.pickedUpAt
		rec.TaskStatusUpdatedAt = fields.statusUpdatedAt
	}
	return nil
}

// ClaimTask atomically starts an OPEN task only if it is already assigned to
// agentID. Unassigned work requires an operator handoff first. Terminal tasks
// are never reopened by agent pickup. First pickup evidence is preserved on
// retries.
func (s *SQLiteStore) ClaimTask(ctx context.Context, memoryID, agentID string) (bool, error) {
	res, err := s.writeExecContext(ctx,
		`UPDATE memories
		 SET assignee = ?, task_status = 'in_progress',
		     task_board_position = CASE WHEN task_status != 'in_progress' THEN 0 ELSE task_board_position END,
		     task_status_updated_at = CASE WHEN task_status != 'in_progress'
		       THEN strftime('%Y-%m-%dT%H:%M:%fZ', 'now') ELSE task_status_updated_at END,
		     task_picked_up_by = CASE
		       WHEN COALESCE(assignee, '') = '' THEN ?
		       WHEN COALESCE(task_picked_up_by, '') = '' THEN ?
		       ELSE task_picked_up_by END,
		     task_picked_up_at = CASE
		       WHEN COALESCE(assignee, '') = '' OR task_picked_up_at IS NULL
		       THEN strftime('%Y-%m-%dT%H:%M:%fZ', 'now') ELSE task_picked_up_at END
		 WHERE memory_id = ? AND memory_type = 'task'
		   AND task_status IN ('planned','in_progress')
		   AND task_requires_handoff = 0
		   AND assignee = ?
		   AND EXISTS (SELECT 1 FROM network_agents a
		               WHERE a.agent_id = ? AND a.status = 'active' AND a.removed_at IS NULL)`,
		agentID, agentID, agentID, memoryID, agentID, agentID)
	if err != nil {
		return false, fmt.Errorf("claim task: %w", err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// CompleteTaskAsAgent atomically completes/drops an open task only for its
// current active assignee. It preserves that assignee as terminal attribution,
// advances the assignment generation, and retires unread notices in
// the same transaction.
func (s *SQLiteStore) CompleteTaskAsAgent(ctx context.Context, memoryID, agentID string, status memory.TaskStatus) (bool, error) {
	if status != memory.TaskStatusDone && status != memory.TaskStatusDropped {
		return false, fmt.Errorf("agent terminal status must be done or dropped")
	}
	if s.db != nil {
		var completed bool
		err := s.RunInTx(ctx, func(tx OffchainStore) error {
			var innerErr error
			completed, innerErr = tx.(*SQLiteStore).CompleteTaskAsAgent(ctx, memoryID, agentID, status)
			return innerErr
		})
		return completed, err
	}
	result, err := s.conn.ExecContext(ctx,
		`UPDATE memories
		 SET task_status = ?, task_assignment_version = task_assignment_version + 1,
		     task_board_position = 0,
		     task_status_updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
		     task_requires_handoff = 1
		 WHERE memory_id = ? AND memory_type = 'task'
		   AND task_status IN ('planned','in_progress') AND assignee = ?
		   AND EXISTS (SELECT 1 FROM network_agents a
		               WHERE a.agent_id = ? AND a.status = 'active' AND a.removed_at IS NULL)`,
		string(status), memoryID, agentID, agentID)
	if err != nil {
		return false, fmt.Errorf("complete task as agent: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return false, nil
	}
	if _, err := s.conn.ExecContext(ctx,
		`UPDATE agent_notifications SET state = 'superseded'
		 WHERE task_id = ? AND state = 'unread'`, memoryID); err != nil {
		return false, fmt.Errorf("supersede completed task notifications: %w", err)
	}
	return true, nil
}

// SetTaskAssignee assigns a task to (or claims it for) an agent. Empty assignee
// clears the assignment. Only affects task-type memories.
func (s *SQLiteStore) SetTaskAssignee(ctx context.Context, memoryID, assignee string) error {
	res, err := s.writeExecContext(ctx,
		`UPDATE memories
		 SET assignee = ?, task_picked_up_by = '', task_picked_up_at = NULL
		 WHERE memory_id = ? AND memory_type = 'task'`, assignee, memoryID)
	if err != nil {
		return fmt.Errorf("set task assignee: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("task not found: %s", memoryID)
	}
	return nil
}

// AssignTaskAndNotify commits the assignment transition and its one-way inbox
// notice together. Repeating the same assignment is idempotent and preserves
// authenticated pickup evidence. A monotonic generation makes A->B->A a new,
// independently deliverable event while stale notices are superseded.
func (s *SQLiteStore) AssignTaskAndNotify(ctx context.Context, memoryID, assignee string) (*TaskAssignmentResult, error) {
	if s.db != nil {
		var out *TaskAssignmentResult
		err := s.RunInTx(ctx, func(tx OffchainStore) error {
			var innerErr error
			out, innerErr = tx.(*SQLiteStore).AssignTaskAndNotify(ctx, memoryID, assignee)
			return innerErr
		})
		return out, err
	}

	var current, domain, taskStatus, memoryStatus string
	var version int64
	if assignee != "" {
		var active int
		if err := s.conn.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM network_agents
			 WHERE agent_id = ? AND status = 'active' AND removed_at IS NULL`, assignee).Scan(&active); err != nil {
			return nil, fmt.Errorf("validate task assignee: %w", err)
		}
		if active != 1 {
			return nil, fmt.Errorf("choose an active registered agent")
		}
	}
	err := s.conn.QueryRowContext(ctx,
		`SELECT COALESCE(assignee, ''), COALESCE(task_assignment_version, 0), domain_tag,
		        COALESCE(task_status, ''), status
		 FROM memories WHERE memory_id = ? AND memory_type = 'task'`, memoryID).
		Scan(&current, &version, &domain, &taskStatus, &memoryStatus)
	if err == sql.ErrNoRows {
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
		if _, err = s.conn.ExecContext(ctx,
			`UPDATE memories SET assignee = ?, task_assignment_version = ?,
			 task_board_position = CASE WHEN task_status != ? THEN 0 ELSE task_board_position END,
			 task_status_updated_at = CASE WHEN task_status != ?
			   THEN strftime('%Y-%m-%dT%H:%M:%fZ', 'now') ELSE task_status_updated_at END,
			 task_status = ?,
			 task_picked_up_by = '', task_picked_up_at = NULL,
			 task_requires_handoff = CASE WHEN ? != '' THEN 0 ELSE task_requires_handoff END
			 WHERE memory_id = ? AND memory_type = 'task'`,
			assignee, version, newStatus, newStatus, newStatus, assignee, memoryID); err != nil {
			return nil, fmt.Errorf("update task assignment: %w", err)
		}
		taskStatus = newStatus
		if _, err = s.conn.ExecContext(ctx,
			`UPDATE agent_notifications SET state = 'superseded'
			 WHERE task_id = ? AND state = 'unread'`, memoryID); err != nil {
			return nil, fmt.Errorf("supersede task notifications: %w", err)
		}
	} else if assignee != "" && version == 0 {
		// Backfill a generation for legacy assigned rows without resetting pickup.
		version = 1
		if _, err = s.conn.ExecContext(ctx,
			`UPDATE memories SET task_assignment_version = ? WHERE memory_id = ?`, version, memoryID); err != nil {
			return nil, fmt.Errorf("backfill task assignment generation: %w", err)
		}
	}

	notificationCreated := false
	if assignee != "" {
		notificationID := fmt.Sprintf("task-assignment:%s:%d", memoryID, version)
		insertResult, insertErr := s.conn.ExecContext(ctx,
			`INSERT OR IGNORE INTO agent_notifications
			 (notification_id, agent_id, kind, task_id, assignment_version, domain, title, state)
			 VALUES (?, ?, 'task_assignment', ?, ?, ?, 'A task was assigned to you', 'unread')`,
			notificationID, assignee, memoryID, version, domain)
		if insertErr != nil {
			return nil, fmt.Errorf("create task notification: %w", insertErr)
		}
		inserted, _ := insertResult.RowsAffected()
		notificationCreated = inserted == 1
	}

	return &TaskAssignmentResult{
		Changed: changed, Assignee: assignee, AssignmentVersion: version, TaskStatus: taskStatus,
		NotificationCreated: notificationCreated,
	}, nil
}

// PeekAgentNotifications returns unread notices without acknowledging them.
// Joining the task row prevents a superseded assignment from authorizing or
// resurfacing stale work. The handler performs current RBAC checks before it
// acknowledges only the notices it will actually return.
func (s *SQLiteStore) PeekAgentNotifications(ctx context.Context, agentID string, limit int) ([]*AgentNotification, error) {
	if limit <= 0 || limit > 20 {
		limit = 5
	}
	rows, err := s.conn.QueryContext(ctx,
		`SELECT n.notification_id, n.agent_id, n.kind, n.task_id, n.assignment_version,
		        n.domain, n.title, n.state, n.created_at
		 FROM agent_notifications n
		 JOIN memories m ON m.memory_id = n.task_id
		 WHERE n.agent_id = ? AND n.state = 'unread'
		   AND m.memory_type = 'task' AND m.status != 'deprecated'
		   AND m.task_status IN ('planned','in_progress')
		   AND m.assignee = n.agent_id
		   AND m.task_assignment_version = n.assignment_version
		 ORDER BY n.created_at ASC LIMIT ?`, agentID, limit)
	if err != nil {
		return nil, fmt.Errorf("get agent notifications: %w", err)
	}
	items := make([]*AgentNotification, 0)
	for rows.Next() {
		var n AgentNotification
		var createdAt string
		if err := rows.Scan(&n.NotificationID, &n.AgentID, &n.Kind, &n.TaskID,
			&n.AssignmentVersion, &n.Domain, &n.Title, &n.State, &createdAt); err != nil {
			_ = rows.Close()
			return nil, err
		}
		n.CreatedAt = parseTime(createdAt)
		items = append(items, &n)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return items, nil
}

// AcknowledgeAgentNotifications marks only notices already authorized for and
// returned to this agent. The current task assignment/version is rechecked at
// the write boundary so a concurrent reassignment cannot acknowledge stale work.
func (s *SQLiteStore) AcknowledgeAgentNotifications(ctx context.Context, agentID string, notificationIDs []string) ([]string, error) {
	if len(notificationIDs) == 0 {
		return []string{}, nil
	}
	if s.db != nil {
		var acknowledged []string
		err := s.RunInTx(ctx, func(tx OffchainStore) error {
			var innerErr error
			acknowledged, innerErr = tx.(*SQLiteStore).AcknowledgeAgentNotifications(ctx, agentID, notificationIDs)
			return innerErr
		})
		return acknowledged, err
	}
	acknowledged := make([]string, 0, len(notificationIDs))
	for _, notificationID := range notificationIDs {
		result, err := s.conn.ExecContext(ctx,
			`UPDATE agent_notifications SET state = 'read', read_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
			 WHERE notification_id = ? AND agent_id = ? AND state = 'unread'
			   AND EXISTS (
			     SELECT 1 FROM memories m
			     WHERE m.memory_id = agent_notifications.task_id
			       AND m.memory_type = 'task' AND m.status != 'deprecated'
			       AND m.task_status IN ('planned','in_progress')
			       AND m.assignee = agent_notifications.agent_id
			       AND m.task_assignment_version = agent_notifications.assignment_version
			   )
			   AND EXISTS (
			     SELECT 1 FROM network_agents a
			     WHERE a.agent_id = agent_notifications.agent_id
			       AND a.status = 'active' AND a.removed_at IS NULL
			   )`, notificationID, agentID)
		if err != nil {
			return nil, fmt.Errorf("acknowledge agent notification: %w", err)
		}
		rows, _ := result.RowsAffected()
		if rows == 1 {
			acknowledged = append(acknowledged, notificationID)
		}
	}
	return acknowledged, nil
}

// SupersedeAgentNotifications retires notices for which current authorization
// was definitively denied (as opposed to a transient lookup failure).
func (s *SQLiteStore) SupersedeAgentNotifications(ctx context.Context, agentID string, notificationIDs []string) error {
	if len(notificationIDs) == 0 {
		return nil
	}
	if s.db != nil {
		return s.RunInTx(ctx, func(tx OffchainStore) error {
			return tx.(*SQLiteStore).SupersedeAgentNotifications(ctx, agentID, notificationIDs)
		})
	}
	for _, notificationID := range notificationIDs {
		if _, err := s.conn.ExecContext(ctx,
			`UPDATE agent_notifications SET state = 'superseded'
			 WHERE notification_id = ? AND agent_id = ? AND state = 'unread'`,
			notificationID, agentID); err != nil {
			return fmt.Errorf("supersede agent notification: %w", err)
		}
	}
	return nil
}

// TakeAgentNotifications remains as the atomic store-level convenience used by
// internal callers/tests that do not need an external RBAC decision.
func (s *SQLiteStore) TakeAgentNotifications(ctx context.Context, agentID string, limit int) ([]*AgentNotification, error) {
	if s.db != nil {
		var out []*AgentNotification
		err := s.RunInTx(ctx, func(tx OffchainStore) error {
			var innerErr error
			out, innerErr = tx.(*SQLiteStore).TakeAgentNotifications(ctx, agentID, limit)
			return innerErr
		})
		return out, err
	}
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

// ---- Tag operations ----

// SetTags replaces all tags on a memory with the given set.
func (s *SQLiteStore) SetTags(ctx context.Context, memoryID string, tags []string) error {
	// Commit and scoped recovery already run inside RunInTx. Reuse that
	// transaction directly; attempting beginTxLocked on a tx-scoped store would
	// dereference the intentionally-nil s.db and would also break atomicity.
	if s.db == nil {
		if _, err := s.conn.ExecContext(ctx, `DELETE FROM memory_tags WHERE memory_id = ?`, memoryID); err != nil {
			return fmt.Errorf("clear tags: %w", err)
		}
		for _, tag := range tags {
			if _, err := s.conn.ExecContext(ctx, `INSERT OR IGNORE INTO memory_tags (memory_id, tag) VALUES (?, ?)`, memoryID, tag); err != nil {
				return fmt.Errorf("insert tag %q: %w", tag, err)
			}
		}
		return nil
	}
	tx, unlock, err := s.beginTxLocked(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer unlock()
	defer tx.Rollback() //nolint:errcheck

	if _, err := tx.ExecContext(ctx, `DELETE FROM memory_tags WHERE memory_id = ?`, memoryID); err != nil {
		return fmt.Errorf("clear tags: %w", err)
	}

	if len(tags) > 0 {
		stmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO memory_tags (memory_id, tag) VALUES (?, ?)`)
		if err != nil {
			return fmt.Errorf("prepare insert: %w", err)
		}
		defer func() { _ = stmt.Close() }()
		for _, tag := range tags {
			tag = strings.TrimSpace(tag)
			if tag == "" {
				continue
			}
			if _, err := stmt.ExecContext(ctx, memoryID, tag); err != nil {
				return fmt.Errorf("insert tag %q: %w", tag, err)
			}
		}
	}

	return tx.Commit()
}

// GetTagsBatch returns tags for multiple memories in one query.
func (s *SQLiteStore) GetTagsBatch(ctx context.Context, memoryIDs []string) (map[string][]string, error) {
	if len(memoryIDs) == 0 {
		return map[string][]string{}, nil
	}
	placeholders := make([]string, len(memoryIDs))
	args := make([]any, len(memoryIDs))
	for i, id := range memoryIDs {
		placeholders[i] = "?"
		args[i] = id
	}
	query := `SELECT memory_id, tag FROM memory_tags WHERE memory_id IN (` + strings.Join(placeholders, ",") + `) ORDER BY memory_id, tag`
	rows, err := s.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query tags batch: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make(map[string][]string, len(memoryIDs))
	for rows.Next() {
		var memID, tag string
		if err := rows.Scan(&memID, &tag); err != nil {
			return nil, fmt.Errorf("scan tag: %w", err)
		}
		result[memID] = append(result[memID], tag)
	}
	return result, rows.Err()
}

// GetTags returns all tags for a memory.
func (s *SQLiteStore) GetTags(ctx context.Context, memoryID string) ([]string, error) {
	rows, err := s.conn.QueryContext(ctx, `SELECT tag FROM memory_tags WHERE memory_id = ? ORDER BY tag`, memoryID)
	if err != nil {
		return nil, fmt.Errorf("query tags: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tags []string
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, fmt.Errorf("scan tag: %w", err)
		}
		tags = append(tags, tag)
	}
	return tags, rows.Err()
}

// ListAllTags returns all unique tags with their memory counts.
func (s *SQLiteStore) ListAllTags(ctx context.Context) ([]TagCount, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT tag, COUNT(*) as cnt FROM memory_tags GROUP BY tag ORDER BY cnt DESC, tag ASC`)
	if err != nil {
		return nil, fmt.Errorf("list tags: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tags []TagCount
	for rows.Next() {
		var tc TagCount
		if err := rows.Scan(&tc.Tag, &tc.Count); err != nil {
			return nil, fmt.Errorf("scan tag count: %w", err)
		}
		tags = append(tags, tc)
	}
	return tags, rows.Err()
}

// ListMemoriesByTag returns memories that have a specific tag.
func (s *SQLiteStore) ListMemoriesByTag(ctx context.Context, tag string, limit, offset int) ([]*memory.MemoryRecord, int, error) {
	if limit <= 0 {
		limit = 50
	}

	var total int
	if err := s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM memory_tags WHERE tag = ?`, tag).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count by tag: %w", err)
	}

	rows, err := s.conn.QueryContext(ctx, `
		SELECT m.memory_id, m.submitting_agent, m.content, m.content_hash,
			m.memory_type, m.domain_tag, m.provider, m.confidence_score, m.status, m.parent_hash,
			m.created_at, m.committed_at, m.deprecated_at, COALESCE(m.task_status, '')
		FROM memories m
		INNER JOIN memory_tags mt ON m.memory_id = mt.memory_id
		WHERE mt.tag = ?
		ORDER BY m.created_at DESC
		LIMIT ? OFFSET ?`, tag, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("list by tag: %w", err)
	}
	defer func() { _ = rows.Close() }()

	results := make([]*memory.MemoryRecord, 0)
	for rows.Next() {
		var r memory.MemoryRecord
		var memType, st, createdAt, taskStatus string
		var parentHash, committedAt, deprecatedAt *string
		if scanErr := rows.Scan(&r.MemoryID, &r.SubmittingAgent, &r.Content, &r.ContentHash,
			&memType, &r.DomainTag, &r.Provider, &r.ConfidenceScore, &st, &parentHash,
			&createdAt, &committedAt, &deprecatedAt, &taskStatus); scanErr != nil {
			return nil, 0, fmt.Errorf("scan memory: %w", scanErr)
		}
		r.MemoryType = memory.MemoryType(memType)
		r.Status = memory.MemoryStatus(st)
		r.TaskStatus = memory.TaskStatus(taskStatus)
		r.CreatedAt = parseTime(createdAt)
		r.CommittedAt = parseTimePtr(committedAt)
		r.DeprecatedAt = parseTimePtr(deprecatedAt)
		if parentHash != nil {
			r.ParentHash = *parentHash
		}
		if decContent, decErr := s.decryptContent(r.Content); decErr == nil {
			r.Content = decContent
		}
		results = append(results, &r)
	}
	return results, total, nil
}

// --- Pipeline Store ---

// migratePipeline creates the pipeline_messages table if it doesn't exist.
func (s *SQLiteStore) migratePipeline(ctx context.Context) {
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS pipeline_messages (
		pipe_id       TEXT PRIMARY KEY,
		from_agent    TEXT NOT NULL,
		from_provider TEXT NOT NULL DEFAULT '',
		to_agent      TEXT NOT NULL DEFAULT '',
		to_provider   TEXT NOT NULL DEFAULT '',
		intent        TEXT NOT NULL DEFAULT '',
		payload       TEXT NOT NULL,
		result        TEXT,
		status        TEXT NOT NULL DEFAULT 'pending'
		              CHECK (status IN ('pending','claimed','completed','expired','failed')),
		created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		claimed_by    TEXT NOT NULL DEFAULT '',
		claimed_at    TEXT,
		completed_at  TEXT,
		terminal_at   TEXT,
		expires_at    TEXT NOT NULL,
		journal_id    TEXT,
		source_chain_id         TEXT NOT NULL DEFAULT '',
		source_pipe_id          TEXT NOT NULL DEFAULT '',
		destination_chain_id    TEXT NOT NULL DEFAULT '',
		federation_policy_epoch TEXT NOT NULL DEFAULT '',
		federation_agreement_id TEXT NOT NULL DEFAULT '',
		federation_contact_id   TEXT NOT NULL DEFAULT '',
		federation_contact_revision TEXT NOT NULL DEFAULT '',
		federation_authorization_mode TEXT NOT NULL DEFAULT '',
		federation_linked_relation BLOB NOT NULL DEFAULT x''
	)`)
	var hasClaimedBy int
	_ = s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('pipeline_messages') WHERE name='claimed_by'`).Scan(&hasClaimedBy)
	if hasClaimedBy == 0 {
		_, _ = s.writeExecContext(ctx, `ALTER TABLE pipeline_messages ADD COLUMN claimed_by TEXT NOT NULL DEFAULT ''`)
	}
	for _, migration := range []string{
		`ALTER TABLE pipeline_messages ADD COLUMN source_chain_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE pipeline_messages ADD COLUMN source_pipe_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE pipeline_messages ADD COLUMN destination_chain_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE pipeline_messages ADD COLUMN federation_policy_epoch TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE pipeline_messages ADD COLUMN federation_agreement_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE pipeline_messages ADD COLUMN federation_contact_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE pipeline_messages ADD COLUMN federation_contact_revision TEXT NOT NULL DEFAULT ''`,
	} {
		_, _ = s.writeExecContext(ctx, migration)
	}
	_, _ = s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_pipe_to_provider ON pipeline_messages(to_provider, status)`)
	_, _ = s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_pipe_to_agent ON pipeline_messages(to_agent, status)`)
	_, _ = s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_pipe_from_agent ON pipeline_messages(from_agent, status)`)
	_, _ = s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_pipe_claimed_by ON pipeline_messages(claimed_by, status)`)
	_, _ = s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_pipe_expires ON pipeline_messages(status, expires_at)`)
	_, _ = s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_pipe_destination ON pipeline_messages(destination_chain_id, status)`)
	_, _ = s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_pipe_source ON pipeline_messages(source_chain_id, source_pipe_id)`)
	_, _ = s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_pipe_source_status ON pipeline_messages(source_chain_id, status)`)
}

// migratePipelineTerminalRetention makes the terminal-state transition, rather
// than the original send time, authoritative for bounded message retention.
// The triggers keep every status transition on the same invariant, including
// federation failure/revocation paths that do not flow through CompletePipeline.
func (s *SQLiteStore) migratePipelineTerminalRetention(ctx context.Context) error {
	if err := s.addSQLiteColumnIfMissing(ctx, "pipeline_messages", "terminal_at",
		`ALTER TABLE pipeline_messages ADD COLUMN terminal_at TEXT`); err != nil {
		return err
	}
	if _, err := s.writeExecContext(ctx, `UPDATE pipeline_messages
		SET terminal_at=CASE
			WHEN status='completed' AND completed_at IS NOT NULL THEN completed_at
			WHEN status='expired' AND expires_at <= strftime('%Y-%m-%dT%H:%M:%fZ','now') THEN expires_at
			ELSE strftime('%Y-%m-%dT%H:%M:%fZ','now')
		END
		WHERE status IN ('completed','expired','failed') AND terminal_at IS NULL`); err != nil {
		return fmt.Errorf("backfill pipeline terminal timestamps: %w", err)
	}
	for _, statement := range []string{
		`CREATE TRIGGER IF NOT EXISTS stamp_pipeline_terminal_after_update
		AFTER UPDATE OF status ON pipeline_messages
		WHEN NEW.status IN ('completed','expired','failed')
		 AND OLD.status NOT IN ('completed','expired','failed')
		 AND NEW.terminal_at IS NULL
		BEGIN
			UPDATE pipeline_messages SET terminal_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
			WHERE pipe_id=NEW.pipe_id;
		END`,
		`CREATE TRIGGER IF NOT EXISTS stamp_pipeline_terminal_after_insert
		AFTER INSERT ON pipeline_messages
		WHEN NEW.status IN ('completed','expired','failed') AND NEW.terminal_at IS NULL
		BEGIN
			UPDATE pipeline_messages SET terminal_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
			WHERE pipe_id=NEW.pipe_id;
		END`,
		`CREATE INDEX IF NOT EXISTS idx_pipe_terminal_retention
			ON pipeline_messages(status, terminal_at)`,
	} {
		if _, err := s.writeExecContext(ctx, statement); err != nil {
			return fmt.Errorf("install pipeline terminal retention invariant: %w", err)
		}
	}
	return nil
}

func (s *SQLiteStore) decryptPipelineFields(m *PipelineMessage) error {
	for label, field := range map[string]*string{
		"intent": &m.Intent, "payload": &m.Payload, "result": &m.Result,
	} {
		if *field == "" {
			continue
		}
		plaintext, err := s.decryptContent(*field)
		if err != nil {
			return fmt.Errorf("decrypt pipeline %s: %w", label, err)
		}
		if plaintext == VaultLockedPlaceholder {
			return ErrPipeContentUnavailable
		}
		*field = plaintext
	}
	return nil
}

func (s *SQLiteStore) InsertPipeline(ctx context.Context, msg *PipelineMessage) error {
	// Size caps (E8c) — bound one pipe's payload/intent before it hits disk.
	if len(msg.Payload) > MaxPipeContentBytes {
		return ErrPipePayloadTooLarge
	}
	if len(msg.Intent) > MaxPipeIntentBytes {
		return ErrPipeIntentTooLarge
	}
	encryptedIntent, err := s.encryptContent(msg.Intent)
	if err != nil {
		return fmt.Errorf("encrypt pipeline intent: %w", err)
	}
	encryptedPayload, err := s.encryptContent(msg.Payload)
	if err != nil {
		return fmt.Errorf("encrypt pipeline payload: %w", err)
	}

	// Serialize the quota reads and the insert as one critical section. The old
	// shape counted outside writeMu and locked only for the final INSERT, so a
	// parallel burst could have every request observe the same below-cap count
	// and then enqueue far beyond both advertised quotas. A tx-scoped store is
	// already running under its parent's write lock and must not lock again.
	if s.db != nil {
		s.writeMu.Lock()
		defer s.writeMu.Unlock()
	}

	// Quotas (E8c) — cap non-terminal pipes per requester and globally so a
	// flood of never-claimed work items cannot exhaust disk. Both counts are
	// index-backed (idx_pipe_from_agent, idx_pipe_expires) and, with the critical
	// section above, the check + insert is atomic with respect to every writer in
	// this process.
	var perAgent int
	if quotaErr := s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pipeline_messages WHERE from_agent = ? AND source_chain_id = ? AND status IN ('pending','claimed')`,
		msg.FromAgent, msg.SourceChainID).Scan(&perAgent); quotaErr != nil {
		return quotaErr
	}
	if perAgent >= MaxOpenPipesPerAgent {
		return ErrPipeQuotaPerAgent
	}
	if msg.SourceChainID != "" {
		var perPeer int
		if quotaErr := s.conn.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM pipeline_messages WHERE source_chain_id = ? AND status IN ('pending','claimed')`,
			msg.SourceChainID).Scan(&perPeer); quotaErr != nil {
			return quotaErr
		}
		if perPeer >= MaxOpenPipesPerPeer {
			return ErrPipeQuotaPerPeer
		}
	}
	var global int
	if quotaErr := s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pipeline_messages WHERE status IN ('pending','claimed')`).Scan(&global); quotaErr != nil {
		return quotaErr
	}
	if global >= MaxOpenPipesGlobal {
		return ErrPipeQuotaGlobal
	}

	// Call the underlying connection directly: standalone stores already hold
	// writeMu above, while tx-scoped stores are already inside the parent lock.
	linkedRelation := msg.FederationLinkedRelation
	if linkedRelation == nil {
		linkedRelation = []byte{}
	}
	_, err = s.conn.ExecContext(ctx,
		`INSERT INTO pipeline_messages (pipe_id, from_agent, from_provider, to_agent, to_provider, intent, payload, status, created_at, expires_at,
		 source_chain_id, source_pipe_id, destination_chain_id, federation_policy_epoch, federation_agreement_id, federation_contact_id, federation_contact_revision,
		 federation_authorization_mode, federation_linked_relation)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msg.PipeID, msg.FromAgent, msg.FromProvider, msg.ToAgent, msg.ToProvider,
		encryptedIntent, encryptedPayload, msg.Status, formatTime(msg.CreatedAt), formatTime(msg.ExpiresAt),
		msg.SourceChainID, msg.SourcePipeID, msg.DestinationChainID, msg.FederationPolicyEpoch,
		msg.FederationAgreementID, msg.FederationContactID, msg.FederationContactRevision,
		msg.FederationAuthorizationMode, linkedRelation)
	return err
}

func (s *SQLiteStore) GetPipeline(ctx context.Context, pipeID string) (*PipelineMessage, error) {
	row := s.conn.QueryRowContext(ctx,
		`SELECT pipe_id, from_agent, from_provider, to_agent, to_provider, intent, payload,
		        COALESCE(result, ''), status, created_at, COALESCE(claimed_by, ''), claimed_at, completed_at, expires_at, COALESCE(journal_id, ''),
		        source_chain_id, source_pipe_id, destination_chain_id, federation_policy_epoch, federation_agreement_id, federation_contact_id, federation_contact_revision,
		        federation_authorization_mode, federation_linked_relation
		 FROM pipeline_messages WHERE pipe_id = ?`, pipeID)

	var m PipelineMessage
	var createdAt, expiresAt string
	var claimedAt, completedAt *string
	if err := row.Scan(&m.PipeID, &m.FromAgent, &m.FromProvider, &m.ToAgent, &m.ToProvider,
		&m.Intent, &m.Payload, &m.Result, &m.Status, &createdAt, &m.ClaimedBy, &claimedAt, &completedAt,
		&expiresAt, &m.JournalID, &m.SourceChainID, &m.SourcePipeID, &m.DestinationChainID,
		&m.FederationPolicyEpoch, &m.FederationAgreementID, &m.FederationContactID, &m.FederationContactRevision,
		&m.FederationAuthorizationMode, &m.FederationLinkedRelation); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("pipeline message not found: %s", pipeID)
		}
		return nil, err
	}
	m.CreatedAt = parseTime(createdAt)
	m.ExpiresAt = parseTime(expiresAt)
	m.ClaimedAt = parseTimePtr(claimedAt)
	m.CompletedAt = parseTimePtr(completedAt)
	if err := s.decryptPipelineFields(&m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (s *SQLiteStore) GetInbox(ctx context.Context, agentID, provider string, limit int) ([]*PipelineMessage, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT pipe_id, from_agent, from_provider, to_agent, to_provider, intent, payload, status, created_at, expires_at,
		        source_chain_id, source_pipe_id, destination_chain_id, federation_policy_epoch, federation_agreement_id, federation_contact_id, federation_contact_revision,
		        federation_authorization_mode, federation_linked_relation
		 FROM pipeline_messages
		 WHERE status = 'pending'
		   AND destination_chain_id = ''
		   AND (to_agent = ? OR (to_agent = '' AND to_provider = ?))
		   AND expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		 ORDER BY created_at ASC LIMIT ?`,
		agentID, provider, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	items := make([]*PipelineMessage, 0)
	for rows.Next() {
		var m PipelineMessage
		var createdAt, expiresAt string
		if err := rows.Scan(&m.PipeID, &m.FromAgent, &m.FromProvider, &m.ToAgent, &m.ToProvider,
			&m.Intent, &m.Payload, &m.Status, &createdAt, &expiresAt, &m.SourceChainID,
			&m.SourcePipeID, &m.DestinationChainID, &m.FederationPolicyEpoch,
			&m.FederationAgreementID, &m.FederationContactID, &m.FederationContactRevision,
			&m.FederationAuthorizationMode, &m.FederationLinkedRelation); err != nil {
			return nil, err
		}
		m.CreatedAt = parseTime(createdAt)
		m.ExpiresAt = parseTime(expiresAt)
		if err := s.decryptPipelineFields(&m); err != nil {
			return nil, err
		}
		items = append(items, &m)
	}
	return items, rows.Err()
}

// GetInboxHistory returns the recipient's retained pipeline history without
// mutating claim state. GetInbox intentionally remains a pending-only work
// queue; this separate projection lets a recipient revisit a claimed or
// completed message until the ordinary pipeline retention sweep purges it.
func (s *SQLiteStore) GetInboxHistory(ctx context.Context, agentID, provider string, limit int) ([]*PipelineMessage, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT pipe_id, from_agent, from_provider, to_agent, to_provider, intent, payload,
		        COALESCE(result, ''), status, created_at, COALESCE(claimed_by, ''), claimed_at, completed_at, expires_at, COALESCE(journal_id, ''),
		        source_chain_id, source_pipe_id, destination_chain_id, federation_policy_epoch, federation_agreement_id, federation_contact_id, federation_contact_revision,
		        federation_authorization_mode, federation_linked_relation
		 FROM pipeline_messages
		 WHERE destination_chain_id = ''
		   AND (
			to_agent = ?
			OR (to_agent = '' AND to_provider = ? AND (status = 'pending' OR claimed_by = '' OR claimed_by = ?))
		   )
		 ORDER BY created_at DESC LIMIT ?`,
		agentID, provider, agentID, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	items := make([]*PipelineMessage, 0)
	for rows.Next() {
		var m PipelineMessage
		var createdAt, expiresAt string
		var claimedAt, completedAt *string
		if err := rows.Scan(&m.PipeID, &m.FromAgent, &m.FromProvider, &m.ToAgent, &m.ToProvider,
			&m.Intent, &m.Payload, &m.Result, &m.Status, &createdAt, &m.ClaimedBy, &claimedAt, &completedAt,
			&expiresAt, &m.JournalID, &m.SourceChainID, &m.SourcePipeID, &m.DestinationChainID,
			&m.FederationPolicyEpoch, &m.FederationAgreementID, &m.FederationContactID, &m.FederationContactRevision,
			&m.FederationAuthorizationMode, &m.FederationLinkedRelation); err != nil {
			return nil, err
		}
		m.CreatedAt = parseTime(createdAt)
		m.ExpiresAt = parseTime(expiresAt)
		m.ClaimedAt = parseTimePtr(claimedAt)
		m.CompletedAt = parseTimePtr(completedAt)
		if err := s.decryptPipelineFields(&m); err != nil {
			return nil, err
		}
		items = append(items, &m)
	}
	return items, rows.Err()
}

// CountPendingInbox returns only the number of currently actionable rows for
// one exact local recipient/provider. It intentionally performs no claim or
// content decryption, making it safe for the lightweight sage_turn flag.
func (s *SQLiteStore) CountPendingInbox(ctx context.Context, agentID, provider string) (int, error) {
	var count int
	err := s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM pipeline_messages
		WHERE destination_chain_id = '' AND status = 'pending'
		  AND expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		  AND (to_agent = ? OR (to_agent = '' AND to_provider = ?))`,
		agentID, provider).Scan(&count)
	return count, err
}

// GetOutbox returns retained messages sent by one local agent. The source-chain
// guard prevents an imported row from appearing in a coincidentally-named local
// sender's history.
func (s *SQLiteStore) GetOutbox(ctx context.Context, agentID string, limit int) ([]*PipelineMessage, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT pipe_id, from_agent, from_provider, to_agent, to_provider, intent, payload,
		        COALESCE(result, ''), status, created_at, COALESCE(claimed_by, ''), claimed_at, completed_at, expires_at, COALESCE(journal_id, ''),
		        source_chain_id, source_pipe_id, destination_chain_id, federation_policy_epoch, federation_agreement_id, federation_contact_id, federation_contact_revision,
		        federation_authorization_mode, federation_linked_relation
		 FROM pipeline_messages
		 WHERE from_agent = ? AND source_chain_id = ''
		 ORDER BY created_at DESC LIMIT ?`, agentID, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	items := make([]*PipelineMessage, 0)
	for rows.Next() {
		var m PipelineMessage
		var createdAt, expiresAt string
		var claimedAt, completedAt *string
		if err := rows.Scan(&m.PipeID, &m.FromAgent, &m.FromProvider, &m.ToAgent, &m.ToProvider,
			&m.Intent, &m.Payload, &m.Result, &m.Status, &createdAt, &m.ClaimedBy, &claimedAt, &completedAt,
			&expiresAt, &m.JournalID, &m.SourceChainID, &m.SourcePipeID, &m.DestinationChainID,
			&m.FederationPolicyEpoch, &m.FederationAgreementID, &m.FederationContactID, &m.FederationContactRevision,
			&m.FederationAuthorizationMode, &m.FederationLinkedRelation); err != nil {
			return nil, err
		}
		m.CreatedAt = parseTime(createdAt)
		m.ExpiresAt = parseTime(expiresAt)
		m.ClaimedAt = parseTimePtr(claimedAt)
		m.CompletedAt = parseTimePtr(completedAt)
		if err := s.decryptPipelineFields(&m); err != nil {
			return nil, err
		}
		items = append(items, &m)
	}
	return items, rows.Err()
}

func (s *SQLiteStore) ClaimPipeline(ctx context.Context, pipeID, agentID string) error {
	res, err := s.writeExecContext(ctx,
		`UPDATE pipeline_messages SET status = 'claimed', claimed_by = ?, claimed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		 WHERE pipe_id = ? AND status = 'pending'`,
		agentID, pipeID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("pipeline message %s not available for claiming", pipeID)
	}
	return nil
}

func (s *SQLiteStore) CompletePipeline(ctx context.Context, pipeID, agentID, result, journalID string) error {
	// Size cap (E8c) — bound the single result written back to a pipe.
	if len(result) > MaxPipeContentBytes {
		return ErrPipeResultTooLarge
	}
	encryptedResult, err := s.encryptContent(result)
	if err != nil {
		return fmt.Errorf("encrypt pipeline result: %w", err)
	}
	res, err := s.writeExecContext(ctx,
		`UPDATE pipeline_messages SET status = 'completed', result = ?, journal_id = ?, claimed_by = CASE WHEN claimed_by = '' THEN ? ELSE claimed_by END,
		 completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		 WHERE pipe_id = ? AND status = 'claimed' AND (claimed_by = ? OR claimed_by = '')`, encryptedResult, journalID, agentID, pipeID, agentID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("pipeline message %s not available for completion by %s (must be claimed by this agent first)", pipeID, agentID)
	}
	return nil
}

func (s *SQLiteStore) GetCompletedForSender(ctx context.Context, agentID string, limit int) ([]*PipelineMessage, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT pipe_id, from_agent, from_provider, to_agent, to_provider, intent,
		        COALESCE(result, ''), status, created_at, completed_at, expires_at, COALESCE(journal_id, ''),
		        source_chain_id, source_pipe_id, destination_chain_id, federation_policy_epoch, federation_agreement_id, federation_contact_id, federation_contact_revision,
		        federation_authorization_mode, federation_linked_relation
		 FROM pipeline_messages
		 WHERE from_agent = ? AND source_chain_id = '' AND status = 'completed'
		 ORDER BY completed_at DESC LIMIT ?`,
		agentID, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	items := make([]*PipelineMessage, 0)
	for rows.Next() {
		var m PipelineMessage
		var createdAt, expiresAt string
		var completedAt *string
		if err := rows.Scan(&m.PipeID, &m.FromAgent, &m.FromProvider, &m.ToAgent, &m.ToProvider,
			&m.Intent, &m.Result, &m.Status, &createdAt, &completedAt, &expiresAt, &m.JournalID,
			&m.SourceChainID, &m.SourcePipeID, &m.DestinationChainID, &m.FederationPolicyEpoch,
			&m.FederationAgreementID, &m.FederationContactID, &m.FederationContactRevision,
			&m.FederationAuthorizationMode, &m.FederationLinkedRelation); err != nil {
			return nil, err
		}
		m.CreatedAt = parseTime(createdAt)
		m.ExpiresAt = parseTime(expiresAt)
		m.CompletedAt = parseTimePtr(completedAt)
		if err := s.decryptPipelineFields(&m); err != nil {
			return nil, err
		}
		items = append(items, &m)
	}
	return items, rows.Err()
}

func (s *SQLiteStore) ListPipelines(ctx context.Context, status string, limit int) ([]*PipelineMessage, error) {
	if limit <= 0 {
		limit = 50
	}
	query := `SELECT pipe_id, from_agent, from_provider, to_agent, to_provider, intent, payload,
		                 COALESCE(result, ''), status, created_at, claimed_at, completed_at, expires_at, COALESCE(journal_id, ''),
		                 source_chain_id, source_pipe_id, destination_chain_id, federation_policy_epoch, federation_agreement_id, federation_contact_id, federation_contact_revision,
		                 federation_authorization_mode, federation_linked_relation
	          FROM pipeline_messages`
	var args []any
	if status != "" {
		query += ` WHERE status = ?`
		args = append(args, status)
	}
	query += ` ORDER BY created_at DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var items []*PipelineMessage
	for rows.Next() {
		var m PipelineMessage
		var createdAt, expiresAt string
		var claimedAt, completedAt *string
		if err := rows.Scan(&m.PipeID, &m.FromAgent, &m.FromProvider, &m.ToAgent, &m.ToProvider,
			&m.Intent, &m.Payload, &m.Result, &m.Status, &createdAt, &claimedAt, &completedAt,
			&expiresAt, &m.JournalID, &m.SourceChainID, &m.SourcePipeID, &m.DestinationChainID,
			&m.FederationPolicyEpoch, &m.FederationAgreementID, &m.FederationContactID, &m.FederationContactRevision,
			&m.FederationAuthorizationMode, &m.FederationLinkedRelation); err != nil {
			return nil, err
		}
		m.CreatedAt = parseTime(createdAt)
		m.ExpiresAt = parseTime(expiresAt)
		m.ClaimedAt = parseTimePtr(claimedAt)
		m.CompletedAt = parseTimePtr(completedAt)
		if err := s.decryptPipelineFields(&m); err != nil {
			return nil, err
		}
		items = append(items, &m)
	}
	return items, nil
}

func (s *SQLiteStore) PipelineStats(ctx context.Context) (map[string]int, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT status, COUNT(*) FROM pipeline_messages GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	stats := make(map[string]int)
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		stats[status] = count
	}
	return stats, nil
}

func (s *SQLiteStore) ExpirePipelines(ctx context.Context) (int, error) {
	res, err := s.writeExecContext(ctx,
		`UPDATE pipeline_messages SET status = 'expired'
		 WHERE status IN ('pending', 'claimed')
		   AND expires_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now')`)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// ExpireStalePipelines is the legacy pipeline anti-DoS backstop. Canonical
// msg-* inbox rows are deliberately email-like and remain until an agent reads,
// claims, or explicitly completes them; their open-row quota is the bound.
func (s *SQLiteStore) ExpireStalePipelines(ctx context.Context, olderThan time.Time) (int, error) {
	res, err := s.writeExecContext(ctx,
		`UPDATE pipeline_messages SET status = 'expired'
		 WHERE status IN ('pending', 'claimed')
		   AND pipe_id NOT LIKE 'msg-%'
		   AND created_at < ?`, formatTime(olderThan))
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *SQLiteStore) PurgePipelines(ctx context.Context, olderThan time.Time) (int, error) {
	var purged int64
	err := s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		if _, err := tx.writeExecContext(ctx, `DELETE FROM pipeline_transport_outbox
			WHERE pipe_id IN (SELECT p.pipe_id FROM pipeline_messages p
				WHERE p.status IN ('completed','expired','failed')
				AND p.pipe_id NOT LIKE 'msg-%'
				AND COALESCE(p.terminal_at,p.completed_at,p.created_at) < ?
				AND NOT EXISTS (SELECT 1 FROM message_read_receipts receipt
					WHERE receipt.message_id=p.pipe_id AND receipt.read_at >= ?)
				AND NOT EXISTS (SELECT 1 FROM pipeline_transport_outbox keep
					WHERE keep.pipe_id=p.pipe_id AND (keep.state='pending'
						OR (keep.state='failed' AND keep.reported_at IS NULL)))
				AND NOT EXISTS (SELECT 1 FROM pipeline_receipt_v2_outbox receipt_v2
					WHERE receipt_v2.local_pipe_id=p.pipe_id AND receipt_v2.state='pending'
					AND julianday(receipt_v2.expires_at)>julianday('now')))`,
			formatTime(olderThan), formatTime(olderThan)); err != nil {
			return err
		}
		res, err := tx.writeExecContext(ctx, `DELETE FROM pipeline_messages
			WHERE status IN ('completed', 'expired', 'failed')
			AND pipe_id NOT LIKE 'msg-%'
			AND COALESCE(terminal_at,completed_at,created_at) < ?
			AND NOT EXISTS (SELECT 1 FROM message_read_receipts receipt
				WHERE receipt.message_id=pipeline_messages.pipe_id AND receipt.read_at >= ?)
			AND NOT EXISTS (SELECT 1 FROM pipeline_transport_outbox keep
				WHERE keep.pipe_id=pipeline_messages.pipe_id AND (keep.state='pending'
					OR (keep.state='failed' AND keep.reported_at IS NULL)))
			AND NOT EXISTS (SELECT 1 FROM pipeline_receipt_v2_outbox receipt_v2
				WHERE receipt_v2.local_pipe_id=pipeline_messages.pipe_id AND receipt_v2.state='pending'
				AND julianday(receipt_v2.expires_at)>julianday('now'))`,
			formatTime(olderThan), formatTime(olderThan))
		if err != nil {
			return err
		}
		purged, _ = res.RowsAffected()
		return nil
	})
	return int(purged), err
}

// --- Dynamic Validator Governance ---

// GovProposal represents a governance proposal in SQLite.
type GovProposal struct {
	ProposalID     string `json:"proposal_id"`
	Operation      string `json:"operation"`
	TargetAgentID  string `json:"target_agent_id"`
	TargetPubkey   string `json:"target_pubkey,omitempty"`
	TargetPower    int64  `json:"target_power,omitempty"`
	ProposerID     string `json:"proposer_id"`
	Status         string `json:"status"`
	CreatedHeight  int64  `json:"created_height"`
	ExpiryHeight   int64  `json:"expiry_height"`
	ExecutedHeight *int64 `json:"executed_height,omitempty"`
	Reason         string `json:"reason,omitempty"`
	CreatedAt      string `json:"created_at,omitempty"`
}

// GovVote represents a governance vote in SQLite.
type GovVote struct {
	ProposalID  string `json:"proposal_id"`
	ValidatorID string `json:"validator_id"`
	Decision    string `json:"decision"`
	Height      int64  `json:"height"`
}

// InsertGovProposal inserts a governance proposal into SQLite.
func (s *SQLiteStore) InsertGovProposal(ctx context.Context, p *GovProposal) error {
	_, err := s.writeExecContext(ctx, `
		INSERT INTO governance_proposals (proposal_id, operation, target_agent_id, target_pubkey,
			target_power, proposer_id, status, created_height, expiry_height, executed_height, reason)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (proposal_id) DO NOTHING`,
		p.ProposalID, p.Operation, p.TargetAgentID, p.TargetPubkey,
		p.TargetPower, p.ProposerID, p.Status, p.CreatedHeight,
		p.ExpiryHeight, p.ExecutedHeight, p.Reason)
	if err != nil {
		return fmt.Errorf("insert gov proposal: %w", err)
	}
	return nil
}

// GetGovProposal retrieves a governance proposal by ID.
func (s *SQLiteStore) GetGovProposal(ctx context.Context, proposalID string) (*GovProposal, error) {
	row := s.conn.QueryRowContext(ctx, `
		SELECT proposal_id, operation, target_agent_id, COALESCE(target_pubkey,''),
			COALESCE(target_power,0), proposer_id, status, created_height,
			expiry_height, executed_height, COALESCE(reason,''),
			COALESCE(created_at,'')
		FROM governance_proposals WHERE proposal_id = ?`, proposalID)

	var p GovProposal
	err := row.Scan(&p.ProposalID, &p.Operation, &p.TargetAgentID, &p.TargetPubkey,
		&p.TargetPower, &p.ProposerID, &p.Status, &p.CreatedHeight,
		&p.ExpiryHeight, &p.ExecutedHeight, &p.Reason, &p.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("gov proposal not found: %s", proposalID)
	}
	if err != nil {
		return nil, fmt.Errorf("get gov proposal: %w", err)
	}
	return &p, nil
}

// UpdateGovProposalStatus updates the status (and optionally executed_height) of a proposal.
func (s *SQLiteStore) UpdateGovProposalStatus(ctx context.Context, proposalID, status string, executedHeight *int64) error {
	_, err := s.writeExecContext(ctx, `
		UPDATE governance_proposals SET status = ?, executed_height = ?
		WHERE proposal_id = ?`, status, executedHeight, proposalID)
	if err != nil {
		return fmt.Errorf("update gov proposal status: %w", err)
	}
	return nil
}

// ListGovProposals lists governance proposals, optionally filtered by status.
func (s *SQLiteStore) ListGovProposals(ctx context.Context, status string) ([]*GovProposal, error) {
	var query string
	var args []any
	if status != "" {
		query = `SELECT proposal_id, operation, target_agent_id, COALESCE(target_pubkey,''),
			COALESCE(target_power,0), proposer_id, status, created_height,
			expiry_height, executed_height, COALESCE(reason,''),
			COALESCE(created_at,'')
			FROM governance_proposals WHERE status = ? ORDER BY created_height DESC`
		args = append(args, status)
	} else {
		query = `SELECT proposal_id, operation, target_agent_id, COALESCE(target_pubkey,''),
			COALESCE(target_power,0), proposer_id, status, created_height,
			expiry_height, executed_height, COALESCE(reason,''),
			COALESCE(created_at,'')
			FROM governance_proposals ORDER BY created_height DESC`
	}

	rows, err := s.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list gov proposals: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var proposals []*GovProposal
	for rows.Next() {
		var p GovProposal
		if err := rows.Scan(&p.ProposalID, &p.Operation, &p.TargetAgentID, &p.TargetPubkey,
			&p.TargetPower, &p.ProposerID, &p.Status, &p.CreatedHeight,
			&p.ExpiryHeight, &p.ExecutedHeight, &p.Reason, &p.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan gov proposal: %w", err)
		}
		proposals = append(proposals, &p)
	}
	return proposals, nil
}

// InsertGovVote inserts or replaces a governance vote in SQLite.
func (s *SQLiteStore) InsertGovVote(ctx context.Context, v *GovVote) error {
	_, err := s.writeExecContext(ctx, `
		INSERT OR REPLACE INTO governance_votes (proposal_id, validator_id, decision, height)
		VALUES (?, ?, ?, ?)`,
		v.ProposalID, v.ValidatorID, v.Decision, v.Height)
	if err != nil {
		return fmt.Errorf("insert gov vote: %w", err)
	}
	return nil
}

// GetGovVotes retrieves all votes for a governance proposal.
func (s *SQLiteStore) GetGovVotes(ctx context.Context, proposalID string) ([]*GovVote, error) {
	rows, err := s.conn.QueryContext(ctx, `
		SELECT proposal_id, validator_id, decision, height
		FROM governance_votes WHERE proposal_id = ? ORDER BY validator_id`,
		proposalID)
	if err != nil {
		return nil, fmt.Errorf("get gov votes: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var votes []*GovVote
	for rows.Next() {
		var v GovVote
		if err := rows.Scan(&v.ProposalID, &v.ValidatorID, &v.Decision, &v.Height); err != nil {
			return nil, fmt.Errorf("scan gov vote: %w", err)
		}
		votes = append(votes, &v)
	}
	return votes, nil
}
