package store

// Domain-sync storage for v11.5 federation shared-domain replication.
//
// Three off-consensus tables (SQLite-only, mcp_tokens precedent — methods live
// on *SQLiteStore, NOT on the OffchainStore interface; Postgres-backed nodes
// disable sync entirely rather than half-running without these):
//
//   - sync_domains: which domains the LOCAL operator has consented to sync
//     with a given peer chain. Local config, deliberately NOT on the on-chain
//     cross_fed record — that blob is AppHash-visible and its tx-33 codec is
//     positional with no version byte, so extending it is a fork. Consent is
//     therefore asymmetric-by-construction: each side configures its own rows
//     and the receiver enforces its OWN rows on every push.
//
//   - sync_outbox: durable store-and-forward queue, keyed (remote_chain_id,
//     memory_id). Survives restarts; every enqueue path is INSERT OR IGNORE
//     on the PK because ABCI block replay after a flush panic legitimately
//     re-fires enqueue signals.
//
//   - sync_origin: provenance + admission ledger on the RECEIVING side, keyed
//     (origin_chain_id, origin_memory_id). Records every terminal admission
//     decision (including rejections) so redelivery replays the recorded
//     outcome instead of re-running gates, the anti-entropy digest never
//     re-offers refused items, and locally-committed copies are never
//     re-forwarded to anyone (loop prevention via local_memory_id lookup).
//
// last_error / outcome columns hold enum codes and status text only — never
// peer memory content (vault hygiene: content at rest is encrypted in the
// memories table; these tables must not leak plaintext beside it).

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Sync outbox states. pending -> delivering (claimed) -> delivered | rejected
// | failed; delivering falls back to pending on retryable errors.
const (
	SyncStatePending    = "pending"    // due (or backing off) for delivery
	SyncStateDelivering = "delivering" // claimed by a drainer pass
	SyncStateDelivered  = "delivered"  // handed to the peer's pipeline (peer lifecycle stays sovereign)
	SyncStateRejected   = "rejected"   // peer terminally refused (B-D1 cross-domain dup, clearance, consent, scope)
	SyncStateFailed     = "failed"     // terminal local failure (e.g. memory content no longer available)
)

func (s *SQLiteStore) LockSyncOriginRead() func() {
	s.syncOriginGate.RLock()
	return s.syncOriginGate.RUnlock
}

func (s *SQLiteStore) LockSyncOriginWrite() func() {
	s.syncOriginGate.Lock()
	return s.syncOriginGate.Unlock
}

// Sync origin outcomes recorded at admission time on the receiving side.
const (
	SyncOutcomeAdmitted             = "admitted"
	SyncOutcomeRejectedDupXDomain   = "rejected_dup_cross_domain"
	SyncOutcomeRejectedClearance    = "rejected_clearance"
	SyncOutcomeRejectedNotConsented = "rejected_not_consented"
	SyncOutcomeRejectedDomainScope  = "rejected_domain_scope"
)

// SyncOutboxItem is one queued delivery toward a peer chain.
type SyncOutboxItem struct {
	RemoteChainID string
	MemoryID      string
	State         string
	Attempts      int
	NextAttemptAt time.Time
	LastError     string
	CreatedAt     time.Time
	// OriginChainID is '' for a NATIVE row (this node authored MemoryID; the
	// drainer signs the item with the operator key) or the ORIGINAL origin chain
	// for a v11.8 RELAYED row (the drainer re-serves the stored origin_sig and
	// does NOT re-sign — docs §9.2).
	OriginChainID string
}

// SyncOrigin records where a synced copy came from and what the admission
// decision was. LocalMemoryID is set only for admitted copies.
type SyncOrigin struct {
	OriginChainID   string
	OriginMemoryID  string
	OriginCreatedAt string // opaque origin timestamp, stored verbatim
	LocalMemoryID   string
	DomainTag       string
	Outcome         string
	// OriginSig is the ORIGIN agent's ed25519 signature persisted at admission
	// (v11.8 mesh relay, docs §9.2). Nil for pre-v11.8 pairwise rows / rejections;
	// a relayer re-serves it verbatim so the receiver verifies authenticity
	// against the origin's roster key, never the relayer's.
	OriginSig []byte
	CreatedAt time.Time
}

// SyncOriginPending is a durable pre-broadcast quarantine. Its local ID is
// excluded from all re-forward scans before the receiver submits the copy to
// consensus, closing the commit-to-provenance crash window.
type SyncOriginPending struct {
	OriginChainID   string
	OriginMemoryID  string
	OriginCreatedAt string
	LocalMemoryID   string
	DomainTag       string
	ContentHash     string
	Classification  int
	MemoryType      string
	SubmittingAgent string
}

type SyncControl struct {
	RemoteChainID     string
	Role              string
	ControllerChainID string
	ControllerAgentID string
	PolicyEpoch       string
	RemoteCAPin       string
	BindingState      string
	Revision          int64
	PolicyHash        string
	DeliveredRevision int64
}

// CommittedHashMatch is one committed memory row matching a content hash,
// used by the B-D1 cross-domain duplicate gate.
type CommittedHashMatch struct {
	MemoryID  string
	DomainTag string
}

// likeEscapeSubtree builds the LIKE pattern that matches a domain's subtree
// ("hr" -> "hr.%"), escaping any LIKE metacharacters in the domain itself so a
// tag containing % or _ can't widen the match (defense-in-depth: sync_domains
// are validated concrete at the CRUD layer, but the store shouldn't rely on
// that). Pair with `ESCAPE '\'` in the query.
func likeEscapeSubtree(domain string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return r.Replace(domain) + ".%"
}

// migrateSyncTables creates the three domain-sync tables on first boot.
// Idempotent (CREATE IF NOT EXISTS), same shape as migrateMCPTokens.
func (s *SQLiteStore) migrateSyncTables(ctx context.Context) {
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS sync_domains (
		remote_chain_id TEXT NOT NULL,
		domain_tag      TEXT NOT NULL,
		created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		PRIMARY KEY (remote_chain_id, domain_tag)
	)`)
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS sync_outbox (
		remote_chain_id TEXT NOT NULL,
		memory_id       TEXT NOT NULL,
		state           TEXT NOT NULL DEFAULT 'pending'
		                CHECK (state IN ('pending','delivering','delivered','rejected','failed')),
		attempts        INTEGER NOT NULL DEFAULT 0,
		next_attempt_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		last_error      TEXT,
		created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		PRIMARY KEY (remote_chain_id, memory_id)
	)`)
	_, _ = s.writeExecContext(ctx,
		`CREATE INDEX IF NOT EXISTS idx_sync_outbox_due ON sync_outbox(state, next_attempt_at)`)
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS sync_origin (
		origin_chain_id   TEXT NOT NULL,
		origin_memory_id  TEXT NOT NULL,
		origin_created_at TEXT NOT NULL DEFAULT '',
		local_memory_id   TEXT NOT NULL DEFAULT '',
		domain_tag        TEXT NOT NULL DEFAULT '',
		outcome           TEXT NOT NULL
		                  CHECK (outcome IN ('admitted','rejected_dup_cross_domain','rejected_clearance','rejected_not_consented','rejected_domain_scope')),
		created_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		PRIMARY KEY (origin_chain_id, origin_memory_id)
	)`)
	_, _ = s.writeExecContext(ctx,
		`CREATE INDEX IF NOT EXISTS idx_sync_origin_domain ON sync_origin(origin_chain_id, domain_tag)`)
	_, _ = s.writeExecContext(ctx,
		`CREATE INDEX IF NOT EXISTS idx_sync_origin_local ON sync_origin(local_memory_id) WHERE local_memory_id != ''`)
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS sync_origin_pending (
		origin_chain_id   TEXT NOT NULL,
		origin_memory_id  TEXT NOT NULL,
		origin_created_at TEXT NOT NULL DEFAULT '',
		local_memory_id   TEXT NOT NULL,
		domain_tag        TEXT NOT NULL DEFAULT '',
		content_hash      TEXT NOT NULL,
		classification    INTEGER NOT NULL,
		memory_type       TEXT NOT NULL,
		submitting_agent  TEXT NOT NULL,
		created_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		PRIMARY KEY (origin_chain_id, origin_memory_id)
	)`)
	_, _ = s.writeExecContext(ctx,
		`CREATE INDEX IF NOT EXISTS idx_sync_origin_pending_local ON sync_origin_pending(local_memory_id)`)
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS sync_control (
		remote_chain_id      TEXT PRIMARY KEY,
		role                 TEXT NOT NULL CHECK (role IN ('host','guest')),
		controller_chain_id  TEXT NOT NULL,
		controller_agent_id  TEXT NOT NULL,
		policy_epoch         TEXT NOT NULL,
		remote_ca_pin        TEXT NOT NULL,
		binding_state        TEXT NOT NULL CHECK (binding_state IN ('pending','active')),
		revision             INTEGER NOT NULL DEFAULT 0,
		policy_hash          TEXT NOT NULL DEFAULT '',
		delivered_revision   INTEGER NOT NULL DEFAULT 0,
		updated_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	)`)
	// fed_peer_names: the friendly label a peer network chose for itself, learned
	// at join time. Purely a local display convenience (the connections list shows
	// it in place of the raw chain id); never authoritative, never on-chain.
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS fed_peer_names (
		remote_chain_id TEXT PRIMARY KEY,
		name            TEXT NOT NULL,
		updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	)`)
	// v11.8 mesh pull-relay (docs §9.2 must-fix #1). Both additive + idempotent,
	// pragma-guarded (the migrateTaskPickup / sync_control.group_id idiom):
	//   - sync_origin.origin_sig: the ORIGIN agent's ed25519 signature over the
	//     admitted copy, persisted so a relayer can re-serve the copy VERBATIM
	//     and the receiver verifies it against the origin's roster key. Without
	//     it a relayer cannot re-attribute authentically (the blocking gap).
	//   - sync_outbox.origin_chain_id: '' = a NATIVE row (this node is the
	//     origin, sign at drain with the operator key); non-'' = a RELAYED row
	//     carrying the ORIGINAL origin chain while memory_id still points at the
	//     LOCAL copy for content, so one drain loop serves both.
	var hasOriginSig int
	if err := s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info('sync_origin') WHERE name='origin_sig'`).Scan(&hasOriginSig); err == nil && hasOriginSig == 0 {
		_, _ = s.writeExecContext(ctx, `ALTER TABLE sync_origin ADD COLUMN origin_sig BLOB`)
	}
	var hasOriginChain int
	if err := s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info('sync_outbox') WHERE name='origin_chain_id'`).Scan(&hasOriginChain); err == nil && hasOriginChain == 0 {
		_, _ = s.writeExecContext(ctx, `ALTER TABLE sync_outbox ADD COLUMN origin_chain_id TEXT NOT NULL DEFAULT ''`)
	}
}

func (s *SQLiteStore) PrepareSyncControl(ctx context.Context, c SyncControl) error {
	if c.RemoteChainID == "" || (c.Role != "host" && c.Role != "guest") || c.ControllerChainID == "" ||
		c.ControllerAgentID == "" || c.PolicyEpoch == "" || c.RemoteCAPin == "" {
		return fmt.Errorf("incomplete sync control binding")
	}
	_, err := s.writeExecContext(ctx, `
		INSERT INTO sync_control (remote_chain_id, role, controller_chain_id, controller_agent_id,
			policy_epoch, remote_ca_pin, binding_state)
		VALUES (?, ?, ?, ?, ?, ?, 'pending')
		ON CONFLICT(remote_chain_id) DO UPDATE SET
			role=excluded.role, controller_chain_id=excluded.controller_chain_id,
			controller_agent_id=excluded.controller_agent_id, policy_epoch=excluded.policy_epoch,
			remote_ca_pin=excluded.remote_ca_pin, revision=0, policy_hash='', delivered_revision=0,
			updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		WHERE sync_control.binding_state='pending'`,
		c.RemoteChainID, c.Role, c.ControllerChainID, c.ControllerAgentID, c.PolicyEpoch, c.RemoteCAPin)
	if err != nil {
		return err
	}
	existing, err := s.GetSyncControl(ctx, c.RemoteChainID)
	if err != nil {
		return err
	}
	if existing == nil || existing.Role != c.Role || existing.ControllerChainID != c.ControllerChainID ||
		existing.ControllerAgentID != c.ControllerAgentID || existing.PolicyEpoch != c.PolicyEpoch ||
		existing.RemoteCAPin != c.RemoteCAPin {
		return fmt.Errorf("different sync controller binding already exists; revoke before re-enrolling")
	}
	return nil
}

func (s *SQLiteStore) ActivateSyncControl(ctx context.Context, remoteChainID, epoch string) error {
	unlock := s.LockSyncPolicyWrite()
	defer unlock()
	return s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		var state string
		if err := tx.conn.QueryRowContext(ctx, `SELECT binding_state FROM sync_control WHERE remote_chain_id=? AND policy_epoch=?`,
			remoteChainID, epoch).Scan(&state); err != nil {
			return fmt.Errorf("sync control binding not found: %w", err)
		}
		if state == "active" {
			return nil
		}
		if _, err := tx.writeExecContext(ctx, `DELETE FROM sync_domains WHERE remote_chain_id=?`, remoteChainID); err != nil {
			return err
		}
		if _, err := tx.writeExecContext(ctx, `DELETE FROM sync_outbox WHERE remote_chain_id=?`, remoteChainID); err != nil {
			return err
		}
		_, err := tx.writeExecContext(ctx, `UPDATE sync_control SET binding_state='active', revision=0,
			policy_hash='', delivered_revision=0, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
			WHERE remote_chain_id=? AND policy_epoch=?`, remoteChainID, epoch)
		return err
	})
}

func (s *SQLiteStore) DeleteSyncControl(ctx context.Context, remoteChainID string) error {
	_, err := s.writeExecContext(ctx, `DELETE FROM sync_control WHERE remote_chain_id=?`, remoteChainID)
	return err
}

func (s *SQLiteStore) PurgeSyncPeerState(ctx context.Context, remoteChainID string) error {
	unlock := s.LockSyncPolicyWrite()
	defer unlock()
	originUnlock := s.LockSyncOriginWrite()
	defer originUnlock()
	return s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		if _, err := tx.writeExecContext(ctx, `DELETE FROM sync_domains WHERE remote_chain_id=?`, remoteChainID); err != nil {
			return fmt.Errorf("purge sync domains: %w", err)
		}
		if _, err := tx.writeExecContext(ctx, `DELETE FROM sync_outbox WHERE remote_chain_id=?`, remoteChainID); err != nil {
			return fmt.Errorf("purge sync outbox: %w", err)
		}
		if _, err := tx.writeExecContext(ctx, `DELETE FROM sync_control WHERE remote_chain_id=?`, remoteChainID); err != nil {
			return fmt.Errorf("purge sync control: %w", err)
		}
		// A revoke removes transport/policy state, not the fact that an already
		// committed local memory originated elsewhere. Promote crash-window
		// quarantines whose deterministic copy exists before clearing pending.
		if _, err := tx.writeExecContext(ctx, `INSERT OR IGNORE INTO sync_origin
			(origin_chain_id, origin_memory_id, origin_created_at, local_memory_id, domain_tag, outcome)
			SELECT p.origin_chain_id, p.origin_memory_id, p.origin_created_at, p.local_memory_id, p.domain_tag, 'admitted'
			FROM sync_origin_pending p WHERE p.origin_chain_id=?
			AND EXISTS (SELECT 1 FROM memories m WHERE m.memory_id=p.local_memory_id
				AND m.domain_tag=p.domain_tag AND lower(hex(m.content_hash))=lower(p.content_hash)
				AND m.classification=p.classification AND m.memory_type=p.memory_type
				AND m.submitting_agent=p.submitting_agent)`, remoteChainID); err != nil {
			return fmt.Errorf("promote pending sync origins: %w", err)
		}
		if _, err := tx.writeExecContext(ctx, `DELETE FROM sync_origin_pending
			WHERE origin_chain_id=? AND EXISTS (
				SELECT 1 FROM sync_origin so WHERE so.origin_chain_id=sync_origin_pending.origin_chain_id
					AND so.origin_memory_id=sync_origin_pending.origin_memory_id
					AND so.local_memory_id=sync_origin_pending.local_memory_id AND so.local_memory_id!='')`, remoteChainID); err != nil {
			return fmt.Errorf("purge pending sync origins: %w", err)
		}
		return nil
	})
}

func (s *SQLiteStore) GetSyncControl(ctx context.Context, remoteChainID string) (*SyncControl, error) {
	c := &SyncControl{}
	err := s.conn.QueryRowContext(ctx, `SELECT remote_chain_id, role, controller_chain_id,
		controller_agent_id, policy_epoch, remote_ca_pin, binding_state, revision,
		policy_hash, delivered_revision FROM sync_control WHERE remote_chain_id=?`, remoteChainID).
		Scan(&c.RemoteChainID, &c.Role, &c.ControllerChainID, &c.ControllerAgentID, &c.PolicyEpoch,
			&c.RemoteCAPin, &c.BindingState, &c.Revision, &c.PolicyHash, &c.DeliveredRevision)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return c, err
}

func (s *SQLiteStore) ListPendingSyncControls(ctx context.Context) ([]SyncControl, error) {
	rows, err := s.conn.QueryContext(ctx, `SELECT remote_chain_id, role, controller_chain_id,
		controller_agent_id, policy_epoch, remote_ca_pin, binding_state, revision,
		policy_hash, delivered_revision FROM sync_control
		WHERE role='host' AND binding_state='active' AND revision > delivered_revision ORDER BY remote_chain_id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []SyncControl
	for rows.Next() {
		var c SyncControl
		if err := rows.Scan(&c.RemoteChainID, &c.Role, &c.ControllerChainID, &c.ControllerAgentID,
			&c.PolicyEpoch, &c.RemoteCAPin, &c.BindingState, &c.Revision, &c.PolicyHash, &c.DeliveredRevision); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) MarkSyncPolicyDelivered(ctx context.Context, remoteChainID, epoch string, revision int64) error {
	_, err := s.writeExecContext(ctx, `UPDATE sync_control SET delivered_revision = CASE
		WHEN delivered_revision < ? THEN ? ELSE delivered_revision END,
		updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		WHERE remote_chain_id=? AND policy_epoch=?`, revision, revision, remoteChainID, epoch)
	return err
}

// ApplySyncPolicy atomically advances the controller snapshot and replaces the
// effective domains. The caller has already authenticated treaty/controller.
func (s *SQLiteStore) ApplySyncPolicy(ctx context.Context, remoteChainID, epoch string, revision int64, policyHash string, domains []string) (string, error) {
	unlock := s.LockSyncPolicyWrite()
	defer unlock()
	result := "applied"
	err := s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		var current int64
		var currentHash, state string
		if err := tx.conn.QueryRowContext(ctx, `SELECT revision, policy_hash, binding_state FROM sync_control
			WHERE remote_chain_id=? AND policy_epoch=?`, remoteChainID, epoch).Scan(&current, &currentHash, &state); err != nil {
			return err
		}
		if state != "active" {
			return fmt.Errorf("sync control is not active")
		}
		if revision < current {
			return fmt.Errorf("stale sync policy revision")
		}
		if revision == current {
			if policyHash != currentHash {
				return fmt.Errorf("sync policy revision conflict")
			}
			result = "duplicate"
			return nil
		}
		if _, err := tx.writeExecContext(ctx, `DELETE FROM sync_domains WHERE remote_chain_id=?`, remoteChainID); err != nil {
			return err
		}
		for _, domain := range domains {
			if _, err := tx.writeExecContext(ctx, `INSERT INTO sync_domains (remote_chain_id, domain_tag) VALUES (?, ?)`, remoteChainID, domain); err != nil {
				return err
			}
		}
		_, err := tx.writeExecContext(ctx, `UPDATE sync_control SET revision=?, policy_hash=?,
			updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE remote_chain_id=? AND policy_epoch=?`,
			revision, policyHash, remoteChainID, epoch)
		return err
	})
	return result, err
}

// ---- fed_peer_names (local display labels for federated peers) ----

// SetPeerName records (or clears, when name=="") the friendly label a peer
// network reported for itself at join time. Best-effort display metadata; a
// failure never blocks the ceremony.
func (s *SQLiteStore) SetPeerName(ctx context.Context, remoteChainID, name string) error {
	if remoteChainID == "" {
		return fmt.Errorf("remote_chain_id is required")
	}
	if name == "" {
		_, err := s.writeExecContext(ctx, `DELETE FROM fed_peer_names WHERE remote_chain_id = ?`, remoteChainID)
		return err
	}
	_, err := s.writeExecContext(ctx, `
		INSERT INTO fed_peer_names (remote_chain_id, name) VALUES (?, ?)
		ON CONFLICT(remote_chain_id) DO UPDATE SET name = excluded.name,
			updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')`, remoteChainID, name)
	return err
}

// GetPeerNames returns every known peer display label keyed by remote chain id.
func (s *SQLiteStore) GetPeerNames(ctx context.Context) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT remote_chain_id, name FROM fed_peer_names`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make(map[string]string)
	for rows.Next() {
		var id, name string
		if scanErr := rows.Scan(&id, &name); scanErr != nil {
			return nil, scanErr
		}
		out[id] = name
	}
	return out, rows.Err()
}

// ---- sync_domains ----

// SetSyncDomains replaces the full consented-domain set for a peer chain.
// Empty domains list clears consent entirely (same as DeleteSyncDomains).
// Validation (non-empty, concrete, subtree-covered by the on-chain agreement)
// is the caller's job — the store records what the operator consented to.
func (s *SQLiteStore) SetSyncDomains(ctx context.Context, remoteChainID string, domains []string) error {
	if remoteChainID == "" {
		return fmt.Errorf("remote_chain_id is required")
	}
	unlock := s.LockSyncPolicyWrite()
	defer unlock()
	return s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx, ok := txStore.(*SQLiteStore)
		if !ok {
			// RunInTx always hands back a tx-scoped *SQLiteStore (sqlite.go);
			// anything else means sync is running against the wrong backend.
			return fmt.Errorf("sync domains require the SQLite store")
		}
		if _, err := tx.writeExecContext(ctx,
			`DELETE FROM sync_domains WHERE remote_chain_id = ?`, remoteChainID); err != nil {
			return fmt.Errorf("clear sync domains: %w", err)
		}
		for _, d := range domains {
			if d == "" {
				return fmt.Errorf("empty domain in sync set")
			}
			if _, err := tx.writeExecContext(ctx,
				`INSERT OR IGNORE INTO sync_domains (remote_chain_id, domain_tag) VALUES (?, ?)`,
				remoteChainID, d); err != nil {
				return fmt.Errorf("insert sync domain %q: %w", d, err)
			}
		}
		return nil
	})
}

// GetSyncDomains returns the consented sync domains for one peer chain,
// sorted for stable output. Empty slice = sync not configured for this peer.
func (s *SQLiteStore) GetSyncDomains(ctx context.Context, remoteChainID string) ([]string, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT domain_tag FROM sync_domains WHERE remote_chain_id = ? ORDER BY domain_tag`, remoteChainID)
	if err != nil {
		return nil, fmt.Errorf("get sync domains: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, fmt.Errorf("scan sync domain: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// ListSyncDomainChains returns the distinct peer chains that have at least
// one consented sync domain — the drainer's iteration set.
func (s *SQLiteStore) ListSyncDomainChains(ctx context.Context) ([]string, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT DISTINCT remote_chain_id FROM sync_domains ORDER BY remote_chain_id`)
	if err != nil {
		return nil, fmt.Errorf("list sync chains: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, fmt.Errorf("scan sync chain: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// DeleteSyncDomains removes all sync consent for a peer chain (agreement
// revocation path — called next to the TOTP-seed and CA purges).
func (s *SQLiteStore) DeleteSyncDomains(ctx context.Context, remoteChainID string) error {
	unlock := s.LockSyncPolicyWrite()
	defer unlock()
	_, err := s.writeExecContext(ctx,
		`DELETE FROM sync_domains WHERE remote_chain_id = ?`, remoteChainID)
	if err != nil {
		return fmt.Errorf("delete sync domains: %w", err)
	}
	return nil
}

// LockSyncPolicyRead leases the effective sync policy across the final gate
// recheck and network push. A completed policy removal therefore guarantees no
// later egress begins under its old snapshot. No SQLite transaction is held.
func (s *SQLiteStore) LockSyncPolicyRead() func() {
	if s.syncPolicyGate == nil {
		return func() {}
	}
	s.syncPolicyGate.RLock()
	return s.syncPolicyGate.RUnlock
}

func (s *SQLiteStore) LockSyncPolicyWrite() func() {
	if s.syncPolicyGate == nil {
		return func() {}
	}
	s.syncPolicyGate.Lock()
	return s.syncPolicyGate.Unlock
}

// ---- sync_outbox ----

// EnqueueSyncOutbox queues a memory for delivery to a peer. INSERT OR IGNORE
// on the PK: idempotent under ABCI block replay, watcher/poll overlap, and
// anti-entropy re-discovery. Returns true if a new row was created.
func (s *SQLiteStore) EnqueueSyncOutbox(ctx context.Context, remoteChainID, memoryID string) (bool, error) {
	if remoteChainID == "" || memoryID == "" {
		return false, fmt.Errorf("remote_chain_id and memory_id are required")
	}
	res, err := s.writeExecContext(ctx,
		`INSERT OR IGNORE INTO sync_outbox (remote_chain_id, memory_id) VALUES (?, ?)`,
		remoteChainID, memoryID)
	if err != nil {
		return false, fmt.Errorf("enqueue sync outbox: %w", err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// EnqueueRelayedSyncOutbox queues a RELAYED mesh-backfill copy toward a peer
// (v11.8, docs §9.2). MemoryID is the LOCAL copy this node holds; originChainID
// is the ORIGINAL author chain (never this node) so the drainer re-serves the
// stored origin_sig verbatim and does NOT re-sign — the relayer is a pure cache.
// INSERT OR IGNORE on the same (remote_chain_id, memory_id) PK: idempotent under
// re-discovery, and a pre-existing NATIVE row for the same pair is left intact
// (a synced copy never has a native outbox row — ListSyncCandidates excludes it).
func (s *SQLiteStore) EnqueueRelayedSyncOutbox(ctx context.Context, remoteChainID, localMemoryID, originChainID string) (bool, error) {
	if remoteChainID == "" || localMemoryID == "" || originChainID == "" {
		return false, fmt.Errorf("remote_chain_id, memory_id, and origin_chain_id are required")
	}
	res, err := s.writeExecContext(ctx,
		`INSERT OR IGNORE INTO sync_outbox (remote_chain_id, memory_id, origin_chain_id) VALUES (?, ?, ?)`,
		remoteChainID, localMemoryID, originChainID)
	if err != nil {
		return false, fmt.Errorf("enqueue relayed sync outbox: %w", err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ResetDeliveringToPending returns all rows stuck in 'delivering' back to
// 'pending' — crash/shutdown recovery. A row is claimed (pending->delivering)
// before a network push; if the process dies before the outcome is recorded,
// the row would otherwise be stranded (nothing re-claims 'delivering', and
// both re-enqueue paths anti-join the outbox). Call once at drainer start,
// BEFORE the first scan. next_attempt_at is left as-is so a mid-backoff row
// resumes its schedule rather than firing immediately.
func (s *SQLiteStore) ResetDeliveringToPending(ctx context.Context) (int, error) {
	res, err := s.writeExecContext(ctx,
		`UPDATE sync_outbox SET state = 'pending' WHERE state = 'delivering'`)
	if err != nil {
		return 0, fmt.Errorf("reset delivering rows: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// ClaimDueSyncOutbox claims up to limit due pending rows for one peer chain,
// flipping them pending -> delivering. Same CAS shape as ClaimTask: the
// UPDATE's WHERE clause is the mutual exclusion, so an overlapping drainer
// pass can never double-claim a row. Each claim is a single-row transaction
// that never spans network I/O (ABCI Commit's flush panics the node if the
// write lock is held too long — see the Commit contention budget).
func (s *SQLiteStore) ClaimDueSyncOutbox(ctx context.Context, remoteChainID string, limit int) ([]SyncOutboxItem, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.conn.QueryContext(ctx, `
		SELECT memory_id, attempts, created_at, COALESCE(origin_chain_id, '') FROM sync_outbox
		 WHERE remote_chain_id = ? AND state = 'pending'
		   AND next_attempt_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		 ORDER BY created_at ASC
		 LIMIT ?`, remoteChainID, limit)
	if err != nil {
		return nil, fmt.Errorf("select due sync outbox: %w", err)
	}
	type cand struct {
		memoryID    string
		attempts    int
		createdAt   string
		originChain string
	}
	var cands []cand
	for rows.Next() {
		var c cand
		if scanErr := rows.Scan(&c.memoryID, &c.attempts, &c.createdAt, &c.originChain); scanErr != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan due sync outbox: %w", scanErr)
		}
		cands = append(cands, c)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate due sync outbox: %w", err)
	}

	var claimed []SyncOutboxItem
	for _, c := range cands {
		res, err := s.writeExecContext(ctx, `
			UPDATE sync_outbox SET state = 'delivering'
			 WHERE remote_chain_id = ? AND memory_id = ? AND state = 'pending'`,
			remoteChainID, c.memoryID)
		if err != nil {
			return claimed, fmt.Errorf("claim sync outbox row: %w", err)
		}
		if n, _ := res.RowsAffected(); n > 0 {
			claimed = append(claimed, SyncOutboxItem{
				RemoteChainID: remoteChainID,
				MemoryID:      c.memoryID,
				State:         SyncStateDelivering,
				Attempts:      c.attempts,
				CreatedAt:     parseTime(c.createdAt),
				OriginChainID: c.originChain,
			})
		}
	}
	return claimed, nil
}

// MarkSyncOutboxDelivered flips a row to the delivered terminal state.
// "Delivered" means accepted into the peer's own validation pipeline — the
// peer's sovereign voter may still reject the copy; that never re-opens
// delivery (the digest compares admission, not committed state).
func (s *SQLiteStore) MarkSyncOutboxDelivered(ctx context.Context, remoteChainID, memoryID string) error {
	return s.setSyncOutboxState(ctx, remoteChainID, memoryID, SyncStateDelivered, "")
}

// MarkSyncOutboxRejected records a terminal peer-side refusal (B-D1
// cross-domain dup, clearance, consent, scope). reason is an enum code /
// short status string, never content.
func (s *SQLiteStore) MarkSyncOutboxRejected(ctx context.Context, remoteChainID, memoryID, reason string) error {
	return s.setSyncOutboxState(ctx, remoteChainID, memoryID, SyncStateRejected, reason)
}

// MarkSyncOutboxFailed records a terminal local failure (memory content no
// longer available, etc.).
func (s *SQLiteStore) MarkSyncOutboxFailed(ctx context.Context, remoteChainID, memoryID, reason string) error {
	return s.setSyncOutboxState(ctx, remoteChainID, memoryID, SyncStateFailed, reason)
}

func (s *SQLiteStore) setSyncOutboxState(ctx context.Context, remoteChainID, memoryID, state, lastErr string) error {
	_, err := s.writeExecContext(ctx,
		`UPDATE sync_outbox SET state = ?, last_error = ? WHERE remote_chain_id = ? AND memory_id = ?`,
		state, lastErr, remoteChainID, memoryID)
	if err != nil {
		return fmt.Errorf("set sync outbox state %s: %w", state, err)
	}
	return nil
}

// MarkSyncOutboxRetry returns a claimed row to pending with an explicit
// attempts count and next-attempt time. The caller owns backoff policy;
// vault-locked deferrals pass the attempts count through UNCHANGED so a
// locked vault never burns the retry budget.
func (s *SQLiteStore) MarkSyncOutboxRetry(ctx context.Context, remoteChainID, memoryID string, attempts int, nextAttemptAt time.Time, lastErr string) error {
	_, err := s.writeExecContext(ctx, `
		UPDATE sync_outbox
		   SET state = 'pending', attempts = ?, next_attempt_at = ?, last_error = ?
		 WHERE remote_chain_id = ? AND memory_id = ?`,
		attempts, nextAttemptAt.UTC().Format("2006-01-02T15:04:05.000Z"), lastErr, remoteChainID, memoryID)
	if err != nil {
		return fmt.Errorf("mark sync outbox retry: %w", err)
	}
	return nil
}

// CountSyncOutboxByState returns per-state row counts for one peer chain
// (the sync status surface).
func (s *SQLiteStore) CountSyncOutboxByState(ctx context.Context, remoteChainID string) (map[string]int, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT state, COUNT(*) FROM sync_outbox WHERE remote_chain_id = ? GROUP BY state`, remoteChainID)
	if err != nil {
		return nil, fmt.Errorf("count sync outbox: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := make(map[string]int)
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return nil, fmt.Errorf("scan sync outbox count: %w", err)
		}
		out[state] = n
	}
	return out, rows.Err()
}

// ListSyncOutbox returns rows in one state for one peer chain, oldest first —
// the status surface uses this for rejected rows (with last_error) and
// pending rows (attempts / next_attempt_at).
func (s *SQLiteStore) ListSyncOutbox(ctx context.Context, remoteChainID, state string, limit int) ([]SyncOutboxItem, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.conn.QueryContext(ctx, `
		SELECT memory_id, state, attempts, next_attempt_at, COALESCE(last_error, ''), created_at
		  FROM sync_outbox
		 WHERE remote_chain_id = ? AND state = ?
		 ORDER BY created_at ASC
		 LIMIT ?`, remoteChainID, state, limit)
	if err != nil {
		return nil, fmt.Errorf("list sync outbox: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []SyncOutboxItem
	for rows.Next() {
		var it SyncOutboxItem
		var nextAt, createdAt string
		if err := rows.Scan(&it.MemoryID, &it.State, &it.Attempts, &nextAt, &it.LastError, &createdAt); err != nil {
			return nil, fmt.Errorf("scan sync outbox row: %w", err)
		}
		it.RemoteChainID = remoteChainID
		it.NextAttemptAt = parseTime(nextAt)
		it.CreatedAt = parseTime(createdAt)
		out = append(out, it)
	}
	return out, rows.Err()
}

// RequeueSyncOutbox resets terminal-but-recoverable rows (rejected / failed)
// back to pending & due now, so the operator can retry after fixing whatever
// caused the rejection (e.g. the peer widened consent, or content changed). If
// memoryID is empty, every rejected/failed row for the chain is requeued;
// otherwise just that one. Delivered rows are left alone. Returns how many
// rows were requeued.
func (s *SQLiteStore) RequeueSyncOutbox(ctx context.Context, remoteChainID, memoryID string) (int, error) {
	q := `UPDATE sync_outbox
	         SET state = 'pending', attempts = 0, last_error = NULL,
	             next_attempt_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
	       WHERE remote_chain_id = ? AND state IN ('rejected','failed')`
	args := []any{remoteChainID}
	if memoryID != "" {
		q += ` AND memory_id = ?`
		args = append(args, memoryID)
	}
	res, err := s.writeExecContext(ctx, q, args...)
	if err != nil {
		return 0, fmt.Errorf("requeue sync outbox: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// PurgeSyncOutbox removes ALL outbox rows for a peer chain (agreement
// revocation path).
func (s *SQLiteStore) PurgeSyncOutbox(ctx context.Context, remoteChainID string) error {
	_, err := s.writeExecContext(ctx,
		`DELETE FROM sync_outbox WHERE remote_chain_id = ?`, remoteChainID)
	if err != nil {
		return fmt.Errorf("purge sync outbox: %w", err)
	}
	return nil
}

// ---- sync_origin ----

func (s *SQLiteStore) StageSyncOrigin(ctx context.Context, o SyncOriginPending) error {
	if o.OriginChainID == "" || o.OriginMemoryID == "" || o.LocalMemoryID == "" ||
		o.ContentHash == "" || o.MemoryType == "" || o.SubmittingAgent == "" {
		return fmt.Errorf("pending origin identity fields are required")
	}
	_, err := s.writeExecContext(ctx, `
		INSERT INTO sync_origin_pending
			(origin_chain_id, origin_memory_id, origin_created_at, local_memory_id, domain_tag,
			 content_hash, classification, memory_type, submitting_agent)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(origin_chain_id, origin_memory_id) DO UPDATE SET
			origin_created_at=excluded.origin_created_at,
			local_memory_id=excluded.local_memory_id,
			domain_tag=excluded.domain_tag,
			content_hash=excluded.content_hash,
			classification=excluded.classification,
			memory_type=excluded.memory_type,
			submitting_agent=excluded.submitting_agent`,
		o.OriginChainID, o.OriginMemoryID, o.OriginCreatedAt, o.LocalMemoryID, o.DomainTag,
		o.ContentHash, o.Classification, o.MemoryType, o.SubmittingAgent)
	if err != nil {
		return fmt.Errorf("stage sync origin: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetPendingSyncOrigin(ctx context.Context, originChainID, originMemoryID string) (*SyncOriginPending, error) {
	p := &SyncOriginPending{}
	err := s.conn.QueryRowContext(ctx, `SELECT origin_chain_id, origin_memory_id, origin_created_at,
		local_memory_id, domain_tag, content_hash, classification, memory_type, submitting_agent
		FROM sync_origin_pending WHERE origin_chain_id=? AND origin_memory_id=?`, originChainID, originMemoryID).
		Scan(&p.OriginChainID, &p.OriginMemoryID, &p.OriginCreatedAt, &p.LocalMemoryID, &p.DomainTag,
			&p.ContentHash, &p.Classification, &p.MemoryType, &p.SubmittingAgent)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, sql.ErrNoRows
	}
	if err != nil {
		return nil, fmt.Errorf("get pending sync origin: %w", err)
	}
	return p, nil
}

func (s *SQLiteStore) DeletePendingSyncOrigin(ctx context.Context, originChainID, originMemoryID string) error {
	_, err := s.writeExecContext(ctx, `DELETE FROM sync_origin_pending WHERE origin_chain_id=? AND origin_memory_id=?`,
		originChainID, originMemoryID)
	if err != nil {
		return fmt.Errorf("delete pending sync origin: %w", err)
	}
	return nil
}

func (s *SQLiteStore) PendingSyncMemoryState(ctx context.Context, p SyncOriginPending) (exists, matches bool, err error) {
	var existsInt, matchesInt int
	if err := s.conn.QueryRowContext(ctx, `SELECT
		EXISTS(SELECT 1 FROM memories WHERE memory_id=?),
		EXISTS(SELECT 1 FROM memories WHERE memory_id=? AND domain_tag=?
			AND lower(hex(content_hash))=lower(?) AND classification=?
			AND memory_type=? AND submitting_agent=?)`, p.LocalMemoryID, p.LocalMemoryID, p.DomainTag, p.ContentHash,
		p.Classification, p.MemoryType, p.SubmittingAgent).Scan(&existsInt, &matchesInt); err != nil {
		return false, false, fmt.Errorf("check pending sync memory: %w", err)
	}
	return existsInt == 1, matchesInt == 1, nil
}

// RecordSyncOrigin persists an admission decision. INSERT OR IGNORE: the
// FIRST recorded decision wins and redelivery replays it via GetSyncOrigin —
// gates must never re-run for an origin pair that already has an outcome.
func (s *SQLiteStore) RecordSyncOrigin(ctx context.Context, o SyncOrigin) error {
	if o.OriginChainID == "" || o.OriginMemoryID == "" || o.Outcome == "" {
		return fmt.Errorf("origin_chain_id, origin_memory_id, and outcome are required")
	}
	// origin_sig is stored as NULL when absent (pre-v11.8 / rejections) so the
	// BLOB column stays clean; a relayer serving a NULL sig is caught fail-closed
	// at the receiver (an unsigned relay is terminally rejected at Gate 5.5).
	var sig any
	if len(o.OriginSig) > 0 {
		sig = o.OriginSig
	}
	_, err := s.writeExecContext(ctx, `
		INSERT OR IGNORE INTO sync_origin
			(origin_chain_id, origin_memory_id, origin_created_at, local_memory_id, domain_tag, outcome, origin_sig)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		o.OriginChainID, o.OriginMemoryID, o.OriginCreatedAt, o.LocalMemoryID, o.DomainTag, o.Outcome, sig)
	if err != nil {
		return fmt.Errorf("record sync origin: %w", err)
	}
	return nil
}

// GetSyncOrigin returns the recorded admission decision for an origin pair,
// or sql.ErrNoRows if this pair has never been decided.
func (s *SQLiteStore) GetSyncOrigin(ctx context.Context, originChainID, originMemoryID string) (*SyncOrigin, error) {
	row := s.conn.QueryRowContext(ctx, `
		SELECT origin_chain_id, origin_memory_id, origin_created_at, local_memory_id, domain_tag, outcome, origin_sig, created_at
		  FROM sync_origin
		 WHERE origin_chain_id = ? AND origin_memory_id = ?`, originChainID, originMemoryID)
	var o SyncOrigin
	var createdAt string
	if err := row.Scan(&o.OriginChainID, &o.OriginMemoryID, &o.OriginCreatedAt,
		&o.LocalMemoryID, &o.DomainTag, &o.Outcome, &o.OriginSig, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("get sync origin: %w", err)
	}
	o.CreatedAt = parseTime(createdAt)
	return &o, nil
}

// RelayOrigin is the stored provenance a relayer re-serves verbatim for a local
// copy: the ORIGINAL origin memory id, its opaque origin timestamp, and the
// origin agent's persisted signature (v11.8 mesh relay, docs §9.2).
type RelayOrigin struct {
	OriginMemoryID  string
	OriginCreatedAt string
	OriginSig       []byte
}

// GetRelayOrigin resolves the stored origin provenance for one local copy this
// node ADMITTED from originChainID — the drainer's per-relayed-row lookup. Keyed
// on (origin_chain_id, local_memory_id) via idx_sync_origin_local (local id is
// the deterministic syncMemoryID, unique per origin pair). Returns sql.ErrNoRows
// when there is no admitted copy (the relayed row is then dropped fail-closed).
// A row whose origin_sig is NULL is returned with a nil OriginSig so the caller
// can refuse to serve an unsigned copy rather than forge one.
func (s *SQLiteStore) GetRelayOrigin(ctx context.Context, originChainID, localMemoryID string) (*RelayOrigin, error) {
	if originChainID == "" || localMemoryID == "" {
		return nil, fmt.Errorf("origin_chain_id and local_memory_id are required")
	}
	var ro RelayOrigin
	err := s.conn.QueryRowContext(ctx, `
		SELECT origin_memory_id, origin_created_at, origin_sig
		  FROM sync_origin
		 WHERE origin_chain_id = ? AND local_memory_id = ? AND outcome = 'admitted'`,
		originChainID, localMemoryID).Scan(&ro.OriginMemoryID, &ro.OriginCreatedAt, &ro.OriginSig)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, sql.ErrNoRows
	}
	if err != nil {
		return nil, fmt.Errorf("get relay origin: %w", err)
	}
	return &ro, nil
}

// IsSyncedCopy reports whether a LOCAL memory ID was admitted as a synced
// copy from some peer — the loop-prevention gate: copies are never
// re-forwarded to anyone (kills both the 2-chain echo and A->B->C fanout).
func (s *SQLiteStore) IsSyncedCopy(ctx context.Context, localMemoryID string) (bool, error) {
	if localMemoryID == "" {
		return false, nil
	}
	var n int
	err := s.conn.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM sync_origin WHERE local_memory_id = ?) +
		(SELECT COUNT(*) FROM sync_origin_pending WHERE local_memory_id = ?)`, localMemoryID, localMemoryID).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("check synced copy: %w", err)
	}
	return n > 0, nil
}

// ListSyncOriginIDs pages the ADMITTED set for one origin chain + domain
// SUBTREE (an asked "hr" covers recorded "hr.public" — DomainAllowed
// semantics, matching how consent is expressed), sorted ascending by
// origin_memory_id — the anti-entropy digest source.
//
// ADMITTED-ONLY on purpose: the digest answers "what has the receiver
// accepted", so the sender backfills anything not on the list. Rejections are
// deliberately NOT recorded (see the handler) and NOT surfaced here — a
// rejection is receiver-config-dependent (consent/clearance) and must be able
// to succeed on a later push after the operator adjusts scope; and settling a
// reconciled row as "delivered" off a rejection record would be a lie. after
// is an exclusive cursor ("" = from the start).
func (s *SQLiteStore) ListSyncOriginIDs(ctx context.Context, originChainID, domain, after string, limit int) ([]string, error) {
	if limit <= 0 || limit > 2000 {
		limit = 2000
	}
	rows, err := s.conn.QueryContext(ctx, `
		SELECT origin_memory_id FROM sync_origin
		 WHERE origin_chain_id = ? AND (domain_tag = ? OR domain_tag LIKE ? ESCAPE '\')
		   AND origin_memory_id > ? AND outcome = 'admitted'
		 ORDER BY origin_memory_id ASC
		 LIMIT ?`, originChainID, domain, likeEscapeSubtree(domain), after, limit)
	if err != nil {
		return nil, fmt.Errorf("list sync origin ids: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan sync origin id: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ---- B-D1 domain-aware duplicate lookup ----

// FindCommittedByContentHashDomains returns the committed memories carrying
// this content hash with their domains (capped small — identical content
// realistically lives in at most a handful of domains). The sync-push
// handler's B-D1 gate: a match in the SAME domain as the incoming item is an
// idempotent duplicate (success); a match ONLY in different domains is the
// cross-domain dup that gets rejected + surfaced, never silently moved.
//
// Domain-aware sibling of FindByContentHash (which is committed-only for the
// same self-match reason documented there); content_hash is stored as raw
// bytes, so the hex parameter is decoded before comparison.
func (s *SQLiteStore) FindCommittedByContentHashDomains(ctx context.Context, contentHash string) ([]CommittedHashMatch, error) {
	hashBytes, err := hex.DecodeString(contentHash)
	if err != nil {
		return nil, fmt.Errorf("decode content hash: %w", err)
	}
	rows, err := s.conn.QueryContext(ctx, `
		SELECT memory_id, COALESCE(domain_tag, '') FROM memories
		 WHERE content_hash = ? AND status = 'committed'
		 LIMIT 16`, hashBytes)
	if err != nil {
		return nil, fmt.Errorf("find committed by content hash: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []CommittedHashMatch
	for rows.Next() {
		var m CommittedHashMatch
		if err := rows.Scan(&m.MemoryID, &m.DomainTag); err != nil {
			return nil, fmt.Errorf("scan content hash match: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// ---- sender-side scan ----

// SyncCandidate is one committed memory eligible for enqueueing toward a peer.
type SyncCandidate struct {
	MemoryID       string
	DomainTag      string
	Classification int
}

// ListSyncCandidates returns committed memories inside the consented domain
// subtrees that have no outbox row for the peer AND are not themselves synced
// copies (loop prevention folded into SQL: any memory with a sync_origin
// local_memory_id mapping is never re-forwarded to anyone). Subtree semantics
// match DomainAllowed: a consented "hr" covers "hr" and "hr.public". Pure
// anti-join on (status, domain) — deliberately NOT a committed_at watermark
// (legacy rows have committed_at NULL and unindexed), so the same call is
// both the steady-state scan and restart recovery.
func (s *SQLiteStore) ListSyncCandidates(ctx context.Context, remoteChainID string, domains []string, limit int) ([]SyncCandidate, error) {
	unlockOrigin := s.LockSyncOriginRead()
	defer unlockOrigin()
	if len(domains) == 0 {
		return nil, nil
	}
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	clause := ""
	args := make([]any, 0, len(domains)*2+2)
	for _, d := range domains {
		if d == "" {
			continue // guard on emitted-clause count below, NOT the loop index
		}
		if clause != "" {
			clause += " OR "
		}
		clause += `(m.domain_tag = ? OR m.domain_tag LIKE ? ESCAPE '\')`
		args = append(args, d, likeEscapeSubtree(d))
	}
	if clause == "" {
		return nil, nil
	}
	args = append(args, remoteChainID, limit)
	rows, err := s.conn.QueryContext(ctx, `
		SELECT m.memory_id, m.domain_tag, m.classification FROM memories m
		 WHERE m.status = 'committed' AND (`+clause+`)
		   AND NOT EXISTS (SELECT 1 FROM sync_outbox o
		                    WHERE o.remote_chain_id = ? AND o.memory_id = m.memory_id)
		   AND NOT EXISTS (SELECT 1 FROM sync_origin so
		                    WHERE so.local_memory_id = m.memory_id)
		   AND NOT EXISTS (SELECT 1 FROM sync_origin_pending sop
		                    WHERE sop.local_memory_id = m.memory_id)
		 ORDER BY m.created_at ASC
		 LIMIT ?`, args...)
	if err != nil {
		return nil, fmt.Errorf("list sync candidates: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []SyncCandidate
	for rows.Next() {
		var c SyncCandidate
		if err := rows.Scan(&c.MemoryID, &c.DomainTag, &c.Classification); err != nil {
			return nil, fmt.Errorf("scan sync candidate: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// GetSyncCandidateByID resolves one COMMITTED memory's sync metadata
// (domain + classification) from the mirror, or (nil, nil) when the row is
// absent or not committed — the commit-tail watcher's per-ID lookup.
func (s *SQLiteStore) GetSyncCandidateByID(ctx context.Context, memoryID string) (*SyncCandidate, error) {
	row := s.conn.QueryRowContext(ctx, `
		SELECT memory_id, COALESCE(domain_tag, ''), classification FROM memories
		 WHERE memory_id = ? AND status = 'committed'`, memoryID)
	var c SyncCandidate
	if err := row.Scan(&c.MemoryID, &c.DomainTag, &c.Classification); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get sync candidate: %w", err)
	}
	return &c, nil
}

// GetMemoryClassificationLocal reads the mirror's classification column (the
// sender-side clearance gate; the receiver independently enforces its own
// ceiling). Errors, including not-found, must be treated fail-closed by the
// caller — never default a missing row toward disclosure.
func (s *SQLiteStore) GetMemoryClassificationLocal(ctx context.Context, memoryID string) (int, error) {
	var c int
	err := s.conn.QueryRowContext(ctx,
		`SELECT classification FROM memories WHERE memory_id = ?`, memoryID).Scan(&c)
	if err != nil {
		return 0, fmt.Errorf("get local classification: %w", err)
	}
	return c, nil
}
