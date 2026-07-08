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
	CreatedAt       time.Time
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
	_, err := s.writeExecContext(ctx,
		`DELETE FROM sync_domains WHERE remote_chain_id = ?`, remoteChainID)
	if err != nil {
		return fmt.Errorf("delete sync domains: %w", err)
	}
	return nil
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
		SELECT memory_id, attempts, created_at FROM sync_outbox
		 WHERE remote_chain_id = ? AND state = 'pending'
		   AND next_attempt_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		 ORDER BY created_at ASC
		 LIMIT ?`, remoteChainID, limit)
	if err != nil {
		return nil, fmt.Errorf("select due sync outbox: %w", err)
	}
	type cand struct {
		memoryID  string
		attempts  int
		createdAt string
	}
	var cands []cand
	for rows.Next() {
		var c cand
		if scanErr := rows.Scan(&c.memoryID, &c.attempts, &c.createdAt); scanErr != nil {
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

// RecordSyncOrigin persists an admission decision. INSERT OR IGNORE: the
// FIRST recorded decision wins and redelivery replays it via GetSyncOrigin —
// gates must never re-run for an origin pair that already has an outcome.
func (s *SQLiteStore) RecordSyncOrigin(ctx context.Context, o SyncOrigin) error {
	if o.OriginChainID == "" || o.OriginMemoryID == "" || o.Outcome == "" {
		return fmt.Errorf("origin_chain_id, origin_memory_id, and outcome are required")
	}
	_, err := s.writeExecContext(ctx, `
		INSERT OR IGNORE INTO sync_origin
			(origin_chain_id, origin_memory_id, origin_created_at, local_memory_id, domain_tag, outcome)
		VALUES (?, ?, ?, ?, ?, ?)`,
		o.OriginChainID, o.OriginMemoryID, o.OriginCreatedAt, o.LocalMemoryID, o.DomainTag, o.Outcome)
	if err != nil {
		return fmt.Errorf("record sync origin: %w", err)
	}
	return nil
}

// GetSyncOrigin returns the recorded admission decision for an origin pair,
// or sql.ErrNoRows if this pair has never been decided.
func (s *SQLiteStore) GetSyncOrigin(ctx context.Context, originChainID, originMemoryID string) (*SyncOrigin, error) {
	row := s.conn.QueryRowContext(ctx, `
		SELECT origin_chain_id, origin_memory_id, origin_created_at, local_memory_id, domain_tag, outcome, created_at
		  FROM sync_origin
		 WHERE origin_chain_id = ? AND origin_memory_id = ?`, originChainID, originMemoryID)
	var o SyncOrigin
	var createdAt string
	if err := row.Scan(&o.OriginChainID, &o.OriginMemoryID, &o.OriginCreatedAt,
		&o.LocalMemoryID, &o.DomainTag, &o.Outcome, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("get sync origin: %w", err)
	}
	o.CreatedAt = parseTime(createdAt)
	return &o, nil
}

// IsSyncedCopy reports whether a LOCAL memory ID was admitted as a synced
// copy from some peer — the loop-prevention gate: copies are never
// re-forwarded to anyone (kills both the 2-chain echo and A->B->C fanout).
func (s *SQLiteStore) IsSyncedCopy(ctx context.Context, localMemoryID string) (bool, error) {
	if localMemoryID == "" {
		return false, nil
	}
	var n int
	err := s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sync_origin WHERE local_memory_id = ?`, localMemoryID).Scan(&n)
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
