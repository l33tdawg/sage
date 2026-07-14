package store

// v11.8 synchronization-GROUP overlay storage.
//
// A synchronization group generalizes the strictly-pairwise v11.5/v11.6 domain
// sync (one cross_fed agreement + one sync_control row = one 2-node link) into a
// first-class N-member group. Like the rest of the sync plane these tables are
// OFF-CONSENSUS and SQLite-only (methods on *SQLiteStore, never on the
// OffchainStore interface — a Postgres node disables Sharing & Sync loudly, it
// does not half-run it). NOTHING here is AppHash-visible: no new tx type, no
// badger key, no consensus fork (docs/v11.8-PLAN.md, FORK DECISION = NONE).
//
// Five tables + one column:
//   - sync_group          : the group object / roster head (controller, epoch,
//                           monotonic roster_revision + anti-rollback floor,
//                           manifest_hash, roster_journal_head). min_quorum /
//                           quorum_mode are INERT v11.9 seam columns — never read
//                           by any acceptance path (docs §6.3, §13).
//   - sync_group_member   : the membership set + state machine
//                           (invited->active->{resyncing,left,removed}); role is
//                           the enrollment-ceremony answer; voting_power is INERT
//                           display metadata, ZERO-PINNED at the write path here.
//   - sync_group_domain   : the shared-domain set, each owner-signed by its owner
//                           chain; max_clearance is DISPLAY-ONLY (never an
//                           admission input — docs §9.2 must-fix #12).
//   - sync_group_log      : the PARTITIONED, hash-chained, ed25519-signed audit
//                           journal. subchain = 'roster' (all members) OR
//                           'domain:<tag>' (only members sharing that domain), so
//                           a member never learns of a domain it does not share
//                           (docs §5.2 metadata isolation, I1).
//   - sync_tombstone      : removal records. member/domain scope = journaled +
//                           advisory-across-sovereignty; memory scope =
//                           LOCAL-ONLY local_suppress (anti-resurrection).
//   - sync_control.group_id: binds a pairwise control row to its group.
//
// This file is build-step 1 (docs §15): purely additive schema + CRUD. No
// acceptance/fan-out/journal-fold logic reads these yet, so existing behaviour
// is unchanged and existing tests stay green.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Group member roles — the enrollment ceremony's three-way choice
// (docs §6.1): Everything / Just some topics / Not yet.
const (
	GroupRoleFullSync       = "full-sync"        // all SHARED group domains (never the operator's unselected ones)
	GroupRoleSelectiveSync  = "selective-sync"   // an explicit subset of the shared set
	GroupRoleEnrolledNoSync = "enrolled-no-sync" // connectivity only, no domains
)

// Group member lifecycle states (docs §4.2). resyncing is the rebuild barrier:
// while set, the drainer/digest/handleSyncPush refuse to run for the group until
// the journal + local_suppress tombstone set are restored (docs §10 must-fix #3).
const (
	GroupMemberInvited   = "invited"
	GroupMemberActive    = "active"
	GroupMemberResyncing = "resyncing"
	GroupMemberLeft      = "left"
	GroupMemberRemoved   = "removed"
)

// Tombstone scopes and enforcement (docs §4.2 / §10).
const (
	TombstoneScopeMember = "member"
	TombstoneScopeDomain = "domain"
	TombstoneScopeMemory = "memory"

	TombstoneEnforceAdvisory      = "advisory"       // signal to peers, never a delete command
	TombstoneEnforceLocalSuppress = "local_suppress" // THIS node will not re-admit the origin pair
)

// SyncGroup is the roster head — one row per group this node belongs to.
type SyncGroup struct {
	GroupID               string
	ControllerChainID     string
	ControllerAgentPubkey string
	Epoch                 string
	RosterRevision        int64
	RosterRevisionFloor   int64 // anti-rollback: highest revision ever durably applied (docs §5.5)
	ManifestHash          string
	RosterJournalHead     string
	AnchoredHead          string
	// MinQuorum / QuorumMode are INERT v11.9 seam columns. They exist only so
	// v11.9 can attach real >2/3 voting-power quorum without reshaping this
	// schema (docs §13). No v11.8 code path reads them for any decision.
	MinQuorum   int64
	QuorumMode  string
	DisplayName string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// SyncGroupMember is one member of a group.
type SyncGroupMember struct {
	GroupID           string
	MemberChainID     string
	MemberAgentPubkey string // identity = ed25519 pubkey@chain_id
	Role              string
	// VotingPower is INERT display-only metadata (docs §6.3). It is ZERO-PINNED
	// at the write path (UpsertSyncGroupMember rejects a non-zero value in
	// v11.8) and must never reach any acceptance/authz/quorum decision. Real
	// voting weight is v11.9.
	VotingPower             float64
	MemberState             string
	JoinedRevision          int64
	LeftRevision            int64
	CAPin                   string // reused from the pairwise cross_fed PeerPubKey (the trust edge)
	LastAckedRosterRevision int64
	LastSeenJournalHead     string
	LastSyncAt              string
}

// SyncGroupDomain is one shared domain owned by a member.
type SyncGroupDomain struct {
	GroupID         string
	DomainTag       string
	OwnerChainID    string
	OwnerSig        string // ed25519 by the owner's domain-owning agent over the domain_add payload (docs §7)
	MaxClearance    int    // DISPLAY-ONLY; never an admission input (docs §9.2)
	AddedRevision   int64
	RemovedRevision int64 // 0 = active
}

// SyncGroupLogEntry is one signed entry in a partitioned audit sub-chain.
// The federation layer computes PrevHash/EntryHash/AuthorSig over a canonical
// payload (docs §5.3); the store persists what it is given.
type SyncGroupLogEntry struct {
	GroupID               string
	Subchain              string // 'roster' or 'domain:<tag>'
	Seq                   int64
	PrevHash              string
	EntryHash             string
	EntryType             string
	PayloadJSON           string // canonical JSON; NEVER memory content
	AuthorChainID         string
	AuthorAgentPubkey     string
	AuthorSig             string
	ControllerEpoch       string
	ControllerChainID     string
	ControllerAgentPubkey string
	ControllerSig         string
	CreatedAt             time.Time
}

// SyncTombstone is one removal record.
type SyncTombstone struct {
	GroupID        string
	Scope          string
	Enforcement    string
	MemberChainID  string
	DomainTag      string
	OriginChainID  string
	OriginMemoryID string
	Reason         string
	Revision       int64
	Subchain       string
	JournalSeq     int64
	AuthorChainID  string
	AuthorSig      string
	CreatedAt      time.Time
}

// migrateSyncGroupTables creates the group overlay tables + the sync_control
// binding column. Idempotent (CREATE IF NOT EXISTS + pragma-guarded ALTER),
// same discipline as migrateSyncTables / migrateTaskPickup.
func (s *SQLiteStore) migrateSyncGroupTables(ctx context.Context) {
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS sync_group (
		group_id                TEXT PRIMARY KEY,
		controller_chain_id     TEXT NOT NULL,
		controller_agent_pubkey TEXT NOT NULL,
		epoch                   TEXT NOT NULL DEFAULT '',
		roster_revision         INTEGER NOT NULL DEFAULT 0,
		roster_revision_floor   INTEGER NOT NULL DEFAULT 0,
		manifest_hash           TEXT NOT NULL DEFAULT '',
		roster_journal_head     TEXT NOT NULL DEFAULT '',
		anchored_head           TEXT NOT NULL DEFAULT '',
		min_quorum              INTEGER NOT NULL DEFAULT 0,
		quorum_mode             TEXT NOT NULL DEFAULT 'advisory',
		display_name            TEXT NOT NULL DEFAULT '',
		created_at              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		updated_at              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	)`)
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS sync_group_member (
		group_id                    TEXT NOT NULL,
		member_chain_id             TEXT NOT NULL,
		member_agent_pubkey         TEXT NOT NULL DEFAULT '',
		role                        TEXT NOT NULL
		                            CHECK (role IN ('full-sync','selective-sync','enrolled-no-sync')),
		voting_power                REAL NOT NULL DEFAULT 0,
		member_state                TEXT NOT NULL DEFAULT 'invited'
		                            CHECK (member_state IN ('invited','active','resyncing','left','removed')),
		joined_revision             INTEGER NOT NULL DEFAULT 0,
		left_revision               INTEGER NOT NULL DEFAULT 0,
		ca_pin                      TEXT NOT NULL DEFAULT '',
		last_acked_roster_revision  INTEGER NOT NULL DEFAULT 0,
		last_seen_journal_head      TEXT NOT NULL DEFAULT '',
		last_sync_at                TEXT NOT NULL DEFAULT '',
		PRIMARY KEY (group_id, member_chain_id)
	)`)
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS sync_group_domain (
		group_id         TEXT NOT NULL,
		domain_tag       TEXT NOT NULL,
		owner_chain_id   TEXT NOT NULL,
		owner_sig        TEXT NOT NULL DEFAULT '',
		max_clearance    INTEGER NOT NULL DEFAULT 0,
		added_revision   INTEGER NOT NULL DEFAULT 0,
		removed_revision INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (group_id, domain_tag)
	)`)
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS sync_group_log (
		group_id            TEXT NOT NULL,
		subchain            TEXT NOT NULL,
		seq                 INTEGER NOT NULL,
		prev_hash           TEXT NOT NULL DEFAULT '',
		entry_hash          TEXT NOT NULL,
		entry_type          TEXT NOT NULL
		                    CHECK (entry_type IN ('group_create','member_invite','member_activate',
		                        'member_remove','member_leave','role_change','epoch_rotate','manifest',
		                        'domain_add','domain_remove','tombstone','anchor')),
		payload_json        TEXT NOT NULL DEFAULT '',
		author_chain_id     TEXT NOT NULL DEFAULT '',
		author_agent_pubkey TEXT NOT NULL DEFAULT '',
		author_sig          TEXT NOT NULL DEFAULT '',
		controller_epoch        TEXT NOT NULL DEFAULT '',
		controller_chain_id     TEXT NOT NULL DEFAULT '',
		controller_agent_pubkey TEXT NOT NULL DEFAULT '',
		controller_sig          TEXT NOT NULL DEFAULT '',
		created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		PRIMARY KEY (group_id, subchain, seq)
	)`)
	// Additive migration for databases created before controller co-signatures
	// were made explicit on the journal wire.
	for _, col := range []struct{ name, ddl string }{
		{"controller_epoch", `ALTER TABLE sync_group_log ADD COLUMN controller_epoch TEXT NOT NULL DEFAULT ''`},
		{"controller_chain_id", `ALTER TABLE sync_group_log ADD COLUMN controller_chain_id TEXT NOT NULL DEFAULT ''`},
		{"controller_agent_pubkey", `ALTER TABLE sync_group_log ADD COLUMN controller_agent_pubkey TEXT NOT NULL DEFAULT ''`},
		{"controller_sig", `ALTER TABLE sync_group_log ADD COLUMN controller_sig TEXT NOT NULL DEFAULT ''`},
	} {
		var present int
		if err := s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('sync_group_log') WHERE name=?`, col.name).Scan(&present); err == nil && present == 0 {
			_, _ = s.writeExecContext(ctx, col.ddl)
		}
	}
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS sync_tombstone (
		group_id         TEXT NOT NULL,
		scope            TEXT NOT NULL CHECK (scope IN ('member','domain','memory')),
		enforcement      TEXT NOT NULL CHECK (enforcement IN ('advisory','local_suppress')),
		member_chain_id  TEXT NOT NULL DEFAULT '',
		domain_tag       TEXT NOT NULL DEFAULT '',
		origin_chain_id  TEXT NOT NULL DEFAULT '',
		origin_memory_id TEXT NOT NULL DEFAULT '',
		reason           TEXT NOT NULL DEFAULT '',
		revision         INTEGER NOT NULL DEFAULT 0,
		subchain         TEXT NOT NULL DEFAULT '',
		journal_seq      INTEGER NOT NULL DEFAULT 0,
		author_chain_id  TEXT NOT NULL DEFAULT '',
		author_sig       TEXT NOT NULL DEFAULT '',
		created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		-- enforcement is part of the PK so a local_suppress row can always be
		-- recorded even if an advisory row for the same target already exists;
		-- INSERT OR IGNORE must never let a pre-existing advisory tombstone
		-- swallow a later local_suppress (which would defeat Gate 6.5).
		PRIMARY KEY (group_id, scope, enforcement, member_chain_id, domain_tag, origin_chain_id, origin_memory_id)
	)`)
	// Fast anti-resurrection lookup on the receive path (docs §10 Gate 6.5).
	_, _ = s.writeExecContext(ctx,
		`CREATE INDEX IF NOT EXISTS idx_sync_tombstone_suppress ON sync_tombstone(origin_chain_id, origin_memory_id)
		 WHERE scope='memory' AND enforcement='local_suppress'`)

	// sync_group_member_domain — the v11.8 selective-sync consent overlay (docs §8,
	// decision #4). One row per (group, member, consented domain) for members whose
	// role is 'selective-sync'. It records WHICH shared-set domains a selective
	// member chose to receive; the nine fail-closed getters + authorizeJournalSubchain
	// widen their entitlement predicate through it. removed_revision is the monotonic
	// anti-rollback stamp (0 = active) mirroring sync_group_domain, so replaying the
	// self-authored role_change that carries the subset is idempotent. This table is
	// READ/SERVE/FAN-OUT ONLY: it is NEVER an input to any write-authz path (a synced
	// item is still persisted only by the receiver's own operator-signed MemorySubmit).
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS sync_group_member_domain (
		group_id         TEXT NOT NULL,
		member_chain_id  TEXT NOT NULL,
		domain_tag       TEXT NOT NULL,
		added_revision   INTEGER NOT NULL DEFAULT 0,
		removed_revision INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (group_id, member_chain_id, domain_tag)
	)`)
	// Member-authenticated selectors that are not yet within the live shared set.
	// They are deliberately separate from sync_group_member_domain because every
	// entitlement getter reads only the latter. A matching domain_add promotes a
	// selector transactionally; until then it grants nothing.
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS sync_group_member_pending_domain (
		group_id        TEXT NOT NULL,
		member_chain_id TEXT NOT NULL,
		domain_tag      TEXT NOT NULL,
		added_revision  INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (group_id, member_chain_id, domain_tag)
	)`)
	// Controller-acknowledged owner capabilities from the invitee-signed
	// member_invite.  This is NOT a content entitlement table: it is consulted
	// only by the remote domain-add ceremony to decide whether the authenticated
	// member may learn/use the exact domain subchain head.
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS sync_group_member_owner_domain (
		group_id        TEXT NOT NULL,
		member_chain_id TEXT NOT NULL,
		domain_tag      TEXT NOT NULL,
		PRIMARY KEY (group_id, member_chain_id, domain_tag)
	)`)

	// sync_group_removed_domain_entitlement is an immutable snapshot of who was
	// entitled to a domain immediately before a domain_remove took effect. It is
	// deliberately separate from the live consent overlay: after removal, that
	// overlay is retired, but prior sharers must still be able to receive the
	// signed removal suffix without learning the old domain journal.
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS sync_group_removed_domain_entitlement (
		group_id         TEXT NOT NULL,
		domain_tag       TEXT NOT NULL,
		member_chain_id  TEXT NOT NULL,
		removed_revision INTEGER NOT NULL,
		PRIMARY KEY (group_id, domain_tag, member_chain_id, removed_revision)
	)`)

	// sync_control.group_id — bind a pairwise control row to its group. Pragma-
	// guarded ALTER, the migrateTaskPickup idiom.
	var hasGroupID int
	if err := s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info('sync_control') WHERE name='group_id'`).Scan(&hasGroupID); err == nil && hasGroupID == 0 {
		_, _ = s.writeExecContext(ctx, `ALTER TABLE sync_control ADD COLUMN group_id TEXT NOT NULL DEFAULT ''`)
	}
}

// ---- sync_group ----

// UpsertSyncGroup inserts or updates the roster head. The anti-rollback floor is
// advanced monotonically (never lowered) so a stale/truncated roster cannot roll
// the group back (docs §5.5).
func (s *SQLiteStore) UpsertSyncGroup(ctx context.Context, g SyncGroup) error {
	if g.GroupID == "" || g.ControllerChainID == "" || g.ControllerAgentPubkey == "" {
		return fmt.Errorf("group_id, controller_chain_id, and controller_agent_pubkey are required")
	}
	if g.QuorumMode == "" {
		g.QuorumMode = "advisory"
	}
	if g.RosterRevisionFloor < g.RosterRevision {
		g.RosterRevisionFloor = g.RosterRevision
	}
	_, err := s.writeExecContext(ctx, `
		INSERT INTO sync_group
			(group_id, controller_chain_id, controller_agent_pubkey, epoch, roster_revision,
			 roster_revision_floor, manifest_hash, roster_journal_head, anchored_head,
			 min_quorum, quorum_mode, display_name)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(group_id) DO UPDATE SET
			controller_chain_id=excluded.controller_chain_id,
			controller_agent_pubkey=excluded.controller_agent_pubkey,
			epoch=excluded.epoch,
			-- roster_revision is monotonic (MAX): it only advances, so a stale
			-- full-row writeback can never regress it below its floor (docs §5.5).
			roster_revision=MAX(sync_group.roster_revision, excluded.roster_revision),
			roster_revision_floor=MAX(sync_group.roster_revision_floor, excluded.roster_revision_floor),
			manifest_hash=excluded.manifest_hash,
			roster_journal_head=excluded.roster_journal_head,
			anchored_head=excluded.anchored_head,
			min_quorum=excluded.min_quorum,
			quorum_mode=excluded.quorum_mode,
			display_name=excluded.display_name,
			updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')`,
		g.GroupID, g.ControllerChainID, g.ControllerAgentPubkey, g.Epoch, g.RosterRevision,
		g.RosterRevisionFloor, g.ManifestHash, g.RosterJournalHead, g.AnchoredHead,
		g.MinQuorum, g.QuorumMode, g.DisplayName)
	if err != nil {
		return fmt.Errorf("upsert sync group: %w", err)
	}
	return nil
}

func scanSyncGroup(row interface{ Scan(...any) error }) (*SyncGroup, error) {
	var g SyncGroup
	var createdAt, updatedAt string
	if err := row.Scan(&g.GroupID, &g.ControllerChainID, &g.ControllerAgentPubkey, &g.Epoch,
		&g.RosterRevision, &g.RosterRevisionFloor, &g.ManifestHash, &g.RosterJournalHead, &g.AnchoredHead,
		&g.MinQuorum, &g.QuorumMode, &g.DisplayName, &createdAt, &updatedAt); err != nil {
		return nil, err
	}
	g.CreatedAt = parseTime(createdAt)
	g.UpdatedAt = parseTime(updatedAt)
	return &g, nil
}

const syncGroupCols = `group_id, controller_chain_id, controller_agent_pubkey, epoch, roster_revision,
	roster_revision_floor, manifest_hash, roster_journal_head, anchored_head, min_quorum, quorum_mode,
	display_name, created_at, updated_at`

// GetSyncGroup returns one group, or (nil, nil) if absent.
func (s *SQLiteStore) GetSyncGroup(ctx context.Context, groupID string) (*SyncGroup, error) {
	g, err := scanSyncGroup(s.conn.QueryRowContext(ctx,
		`SELECT `+syncGroupCols+` FROM sync_group WHERE group_id=?`, groupID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get sync group: %w", err)
	}
	return g, nil
}

// ListSyncGroups returns every group this node belongs to, newest first.
func (s *SQLiteStore) ListSyncGroups(ctx context.Context) ([]SyncGroup, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT `+syncGroupCols+` FROM sync_group ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list sync groups: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []SyncGroup
	for rows.Next() {
		g, err := scanSyncGroup(rows)
		if err != nil {
			return nil, fmt.Errorf("scan sync group: %w", err)
		}
		out = append(out, *g)
	}
	return out, rows.Err()
}

// SetSyncGroupRosterJournalHead advances ONLY the roster head cache (a
// projection re-derivable from sync_group_log). Targeted so a routine head bump
// never rewrites the whole sync_group row — which could otherwise regress
// roster_revision/manifest_hash from a stale snapshot once a concurrent writer
// exists (docs §5.5). No-op if the group row is absent.
func (s *SQLiteStore) SetSyncGroupRosterJournalHead(ctx context.Context, groupID, head string) error {
	if groupID == "" {
		return fmt.Errorf("group_id is required")
	}
	_, err := s.writeExecContext(ctx,
		`UPDATE sync_group SET roster_journal_head=?, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE group_id=?`,
		head, groupID)
	if err != nil {
		return fmt.Errorf("set roster journal head: %w", err)
	}
	return nil
}

// ---- sync_group_member ----

// UpsertSyncGroupMember inserts or REPLACES a member as a FULL ROW: on conflict
// every durable column is overwritten from m. It therefore REQUIRES an explicit,
// valid member_state (no silent default) so a caller can never accidentally
// demote an active member to 'invited' or wipe its ca_pin trust edge /
// member_agent_pubkey origin key by re-upserting a partially-filled struct. For
// partial updates use the targeted mutators — UpdateSyncGroupMemberProgress
// (catch-up cursors) and SetSyncGroupMemberState (state transitions) — which
// touch only their own columns and never reset identity or state.
//
// voting_power is ZERO-PINNED (docs §6.3 guard layer b): a non-zero value is
// rejected in v11.8 so display metadata can never be mistaken for or promoted
// into a v11.9 quorum weight without a fork.
func (s *SQLiteStore) UpsertSyncGroupMember(ctx context.Context, m SyncGroupMember) error {
	if m.GroupID == "" || m.MemberChainID == "" {
		return fmt.Errorf("group_id and member_chain_id are required")
	}
	switch m.Role {
	case GroupRoleFullSync, GroupRoleSelectiveSync, GroupRoleEnrolledNoSync:
	default:
		return fmt.Errorf("invalid group member role %q", m.Role)
	}
	if m.VotingPower != 0 {
		return fmt.Errorf("voting_power is reserved for v11.9 and must be 0 in v11.8")
	}
	switch m.MemberState {
	case GroupMemberInvited, GroupMemberActive, GroupMemberResyncing, GroupMemberLeft, GroupMemberRemoved:
	default:
		return fmt.Errorf("full-row upsert requires an explicit member_state, got %q", m.MemberState)
	}
	_, err := s.writeExecContext(ctx, `
		INSERT INTO sync_group_member
			(group_id, member_chain_id, member_agent_pubkey, role, voting_power, member_state,
			 joined_revision, left_revision, ca_pin, last_acked_roster_revision,
			 last_seen_journal_head, last_sync_at)
		VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(group_id, member_chain_id) DO UPDATE SET
			member_agent_pubkey=excluded.member_agent_pubkey,
			role=excluded.role,
			member_state=excluded.member_state,
			joined_revision=excluded.joined_revision,
			left_revision=excluded.left_revision,
			ca_pin=excluded.ca_pin,
			last_acked_roster_revision=excluded.last_acked_roster_revision,
			last_seen_journal_head=excluded.last_seen_journal_head,
			last_sync_at=excluded.last_sync_at`,
		m.GroupID, m.MemberChainID, m.MemberAgentPubkey, m.Role, m.MemberState,
		m.JoinedRevision, m.LeftRevision, m.CAPin, m.LastAckedRosterRevision,
		m.LastSeenJournalHead, m.LastSyncAt)
	if err != nil {
		return fmt.Errorf("upsert sync group member: %w", err)
	}
	return nil
}

const syncGroupMemberCols = `group_id, member_chain_id, member_agent_pubkey, role, voting_power,
	member_state, joined_revision, left_revision, ca_pin, last_acked_roster_revision,
	last_seen_journal_head, last_sync_at`

func scanSyncGroupMember(row interface{ Scan(...any) error }) (*SyncGroupMember, error) {
	var m SyncGroupMember
	if err := row.Scan(&m.GroupID, &m.MemberChainID, &m.MemberAgentPubkey, &m.Role, &m.VotingPower,
		&m.MemberState, &m.JoinedRevision, &m.LeftRevision, &m.CAPin, &m.LastAckedRosterRevision,
		&m.LastSeenJournalHead, &m.LastSyncAt); err != nil {
		return nil, err
	}
	return &m, nil
}

// GetSyncGroupMember returns one member, or (nil, nil) if absent.
func (s *SQLiteStore) GetSyncGroupMember(ctx context.Context, groupID, memberChainID string) (*SyncGroupMember, error) {
	m, err := scanSyncGroupMember(s.conn.QueryRowContext(ctx,
		`SELECT `+syncGroupMemberCols+` FROM sync_group_member WHERE group_id=? AND member_chain_id=?`,
		groupID, memberChainID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get sync group member: %w", err)
	}
	return m, nil
}

// ListSyncGroupMembers returns every member of a group, ordered by chain id.
func (s *SQLiteStore) ListSyncGroupMembers(ctx context.Context, groupID string) ([]SyncGroupMember, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT `+syncGroupMemberCols+` FROM sync_group_member WHERE group_id=? ORDER BY member_chain_id`, groupID)
	if err != nil {
		return nil, fmt.Errorf("list sync group members: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []SyncGroupMember
	for rows.Next() {
		m, err := scanSyncGroupMember(rows)
		if err != nil {
			return nil, fmt.Errorf("scan sync group member: %w", err)
		}
		out = append(out, *m)
	}
	return out, rows.Err()
}

// UpdateSyncGroupMemberProgress advances ONLY a member's catch-up cursors
// (last_acked_roster_revision, last_seen_journal_head, last_sync_at) — the
// per-member convergence the #3 health surface renders (docs §9.3). It never
// touches role, member_state, ca_pin, or member_agent_pubkey, so a routine
// progress bump can never demote or de-identify a member.
func (s *SQLiteStore) UpdateSyncGroupMemberProgress(ctx context.Context, groupID, memberChainID string, lastAckedRosterRevision int64, lastSeenJournalHead, lastSyncAt string) error {
	if groupID == "" || memberChainID == "" {
		return fmt.Errorf("group_id and member_chain_id are required")
	}
	_, err := s.writeExecContext(ctx, `
		UPDATE sync_group_member
		   SET last_acked_roster_revision = ?, last_seen_journal_head = ?, last_sync_at = ?
		 WHERE group_id = ? AND member_chain_id = ?`,
		lastAckedRosterRevision, lastSeenJournalHead, lastSyncAt, groupID, memberChainID)
	if err != nil {
		return fmt.Errorf("update sync group member progress: %w", err)
	}
	return nil
}

// SetSyncGroupMemberSeen records ONLY where a peer member was last observed
// (last_seen_journal_head, last_sync_at) — used by the journal pull. It never
// touches last_acked_roster_revision (which the step-5 apply layer advances), so
// a pull can never regress a member's acknowledged revision via a read-modify-write.
func (s *SQLiteStore) SetSyncGroupMemberSeen(ctx context.Context, groupID, memberChainID, seenHead, syncAt string) error {
	if groupID == "" || memberChainID == "" {
		return fmt.Errorf("group_id and member_chain_id are required")
	}
	_, err := s.writeExecContext(ctx, `
		UPDATE sync_group_member SET last_seen_journal_head = ?, last_sync_at = ?
		 WHERE group_id = ? AND member_chain_id = ?`,
		seenHead, syncAt, groupID, memberChainID)
	if err != nil {
		return fmt.Errorf("set sync group member seen: %w", err)
	}
	return nil
}

// SetSyncGroupMemberState transitions ONLY a member's lifecycle state (+ its
// left_revision when leaving/removed). It never touches identity/trust columns.
func (s *SQLiteStore) SetSyncGroupMemberState(ctx context.Context, groupID, memberChainID, state string, leftRevision int64) error {
	switch state {
	case GroupMemberInvited, GroupMemberActive, GroupMemberResyncing, GroupMemberLeft, GroupMemberRemoved:
	default:
		return fmt.Errorf("invalid group member state %q", state)
	}
	_, err := s.writeExecContext(ctx, `
		UPDATE sync_group_member SET member_state = ?, left_revision = ?
		 WHERE group_id = ? AND member_chain_id = ?`,
		state, leftRevision, groupID, memberChainID)
	if err != nil {
		return fmt.Errorf("set sync group member state: %w", err)
	}
	return nil
}

// SetSyncGroupMemberRole transitions ONLY a member's role (a role_change apply).
func (s *SQLiteStore) SetSyncGroupMemberRole(ctx context.Context, groupID, memberChainID, role string) error {
	switch role {
	case GroupRoleFullSync, GroupRoleSelectiveSync, GroupRoleEnrolledNoSync:
	default:
		return fmt.Errorf("invalid group member role %q", role)
	}
	_, err := s.writeExecContext(ctx,
		`UPDATE sync_group_member SET role=? WHERE group_id=? AND member_chain_id=?`, role, groupID, memberChainID)
	if err != nil {
		return fmt.Errorf("set sync group member role: %w", err)
	}
	return nil
}

// SetSyncGroupController rotates the controller identity + epoch (an epoch_rotate
// apply). controller_agent_pubkey becomes the key subsequent controller-authored
// entries are verified against.
func (s *SQLiteStore) SetSyncGroupController(ctx context.Context, groupID, controllerChainID, controllerAgentPubkey, epoch string) error {
	if controllerChainID == "" || controllerAgentPubkey == "" {
		return fmt.Errorf("controller chain and pubkey are required")
	}
	_, err := s.writeExecContext(ctx, `
		UPDATE sync_group SET controller_chain_id=?, controller_agent_pubkey=?, epoch=?,
			updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE group_id=?`,
		controllerChainID, controllerAgentPubkey, epoch, groupID)
	if err != nil {
		return fmt.Errorf("set sync group controller: %w", err)
	}
	return nil
}

// SetSyncGroupManifest advances roster_revision + manifest_hash on a manifest
// apply. roster_revision and its floor are monotonic (MAX): a stale/rolled-back
// manifest can never lower them (docs §5.5).
func (s *SQLiteStore) SetSyncGroupManifest(ctx context.Context, groupID string, rosterRevision int64, manifestHash string) error {
	_, err := s.writeExecContext(ctx, `
		UPDATE sync_group SET
			roster_revision = MAX(roster_revision, ?),
			roster_revision_floor = MAX(roster_revision_floor, ?),
			manifest_hash = ?,
			updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
		 WHERE group_id=?`, rosterRevision, rosterRevision, manifestHash, groupID)
	if err != nil {
		return fmt.Errorf("set sync group manifest: %w", err)
	}
	return nil
}

// SetSyncGroupDomainRemoved stamps the current domain lifecycle with its
// monotonic removal generation (journal seq + 1), without disturbing
// owner/clearance. The active-only predicate prevents a repeated domain_remove
// from overwriting the generation that gates the terminal-only serve suffix.
func (s *SQLiteStore) SetSyncGroupDomainRemoved(ctx context.Context, groupID, domainTag string, removedGeneration int64) error {
	if removedGeneration < 1 {
		return fmt.Errorf("removed domain generation must be positive")
	}
	_, err := s.writeExecContext(ctx,
		`UPDATE sync_group_domain SET removed_revision=? WHERE group_id=? AND domain_tag=? AND removed_revision=0`,
		removedGeneration, groupID, domainTag)
	if err != nil {
		return fmt.Errorf("set sync group domain removed: %w", err)
	}
	return nil
}

// ---- sync_group_domain ----

// UpsertSyncGroupDomain inserts or updates a shared-domain row.
func (s *SQLiteStore) UpsertSyncGroupDomain(ctx context.Context, d SyncGroupDomain) error {
	if d.GroupID == "" || d.DomainTag == "" || d.OwnerChainID == "" {
		return fmt.Errorf("group_id, domain_tag, and owner_chain_id are required")
	}
	if d.MaxClearance < 0 || d.MaxClearance > 4 {
		return fmt.Errorf("max_clearance out of range")
	}
	_, err := s.writeExecContext(ctx, `
		INSERT INTO sync_group_domain
			(group_id, domain_tag, owner_chain_id, owner_sig, max_clearance, added_revision, removed_revision)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(group_id, domain_tag) DO UPDATE SET
			owner_chain_id=excluded.owner_chain_id,
			owner_sig=excluded.owner_sig,
			max_clearance=excluded.max_clearance,
			added_revision=excluded.added_revision,
			removed_revision=excluded.removed_revision`,
		d.GroupID, d.DomainTag, d.OwnerChainID, d.OwnerSig, d.MaxClearance, d.AddedRevision, d.RemovedRevision)
	if err != nil {
		return fmt.Errorf("upsert sync group domain: %w", err)
	}
	return nil
}

// ListSyncGroupDomains returns the shared domains of a group. activeOnly filters
// to rows whose removal has not been recorded (removed_revision = 0).
func (s *SQLiteStore) ListSyncGroupDomains(ctx context.Context, groupID string, activeOnly bool) ([]SyncGroupDomain, error) {
	q := `SELECT group_id, domain_tag, owner_chain_id, owner_sig, max_clearance, added_revision, removed_revision
	        FROM sync_group_domain WHERE group_id=?`
	if activeOnly {
		q += ` AND removed_revision=0`
	}
	q += ` ORDER BY domain_tag`
	rows, err := s.conn.QueryContext(ctx, q, groupID)
	if err != nil {
		return nil, fmt.Errorf("list sync group domains: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []SyncGroupDomain
	for rows.Next() {
		var d SyncGroupDomain
		if err := rows.Scan(&d.GroupID, &d.DomainTag, &d.OwnerChainID, &d.OwnerSig, &d.MaxClearance,
			&d.AddedRevision, &d.RemovedRevision); err != nil {
			return nil, fmt.Errorf("scan sync group domain: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// ---- sync_group_member_domain (selective-sync consent) ----

// isDomainDescendant reports whether child is a STRICT descendant of ancestor in
// the dotted domain hierarchy ("hr.payroll" is a descendant of "hr"). Equal tags
// are NOT descendants. Pairs with the LIKE-subtree serve predicates so consent
// covers a tag and everything nested beneath it.
func isDomainDescendant(child, ancestor string) bool {
	return strings.HasPrefix(child, ancestor+".")
}

// normalizeConsentSubtree trims/de-dups the requested consent set and drops any
// tag already covered by an ancestor in the same set (consenting to "hr" already
// covers "hr.payroll"), so the stored rows are the minimal covering set and a
// redundant child can never linger as a stale row after its ancestor is removed.
func normalizeConsentSubtree(tags []string) []string {
	seen := make(map[string]struct{}, len(tags))
	uniq := make([]string, 0, len(tags))
	for _, t := range tags {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if _, dup := seen[t]; dup {
			continue
		}
		seen[t] = struct{}{}
		uniq = append(uniq, t)
	}
	out := make([]string, 0, len(uniq))
	for _, t := range uniq {
		covered := false
		for _, other := range uniq {
			if other != t && isDomainDescendant(t, other) {
				covered = true
				break
			}
		}
		if !covered {
			out = append(out, t)
		}
	}
	return out
}

// consentTagInSharedSet reports whether a requested consent tag is anchored to the
// active shared set: it must be EQUAL to or a descendant of an active shared
// domain.  Ancestors are deliberately rejected: consenting to "hr" while the
// group currently shares only "hr.public" would otherwise silently opt the
// member into a future sibling such as "hr.secret".
// A tag with no such relationship (a domain the group does
// not share at all) is rejected — a member can never consent to a domain outside
// the shared set, so the getters can never be tricked into over-serving.
func consentTagInSharedSet(tag string, shared []SyncGroupDomain) bool {
	for _, d := range shared {
		if d.DomainTag == tag || isDomainDescendant(tag, d.DomainTag) {
			return true
		}
	}
	return false
}

// ReplaceGroupMemberConsentDomains records a selective-sync member's chosen subset
// of the group's shared domains as a FULL-SET REPLACE, folding in from the
// self-authored role_change that carries pkSelectedDomains (docs §8). It is
// REVISION-GUARDED for idempotent journal replay: removed_revision advances
// monotonically (only a currently-active row is ever stamped, and only when it
// leaves the desired set), so re-applying the SAME entry is a no-op. Each requested
// tag is validated to be within the active shared set and subtree-normalized before
// storage. Passing an EMPTY set (a member that switched to full-sync / enrolled-no-
// sync) stamps ALL of the member's active consent rows removed. Rows are advisory
// serve metadata ONLY — never consulted by any write-authz path.
func (s *SQLiteStore) ReplaceGroupMemberConsentDomains(ctx context.Context, groupID, memberChainID string, domainTags []string, revision int64) error {
	if groupID == "" || memberChainID == "" {
		return fmt.Errorf("group_id and member_chain_id are required")
	}
	removedRev := revision
	if removedRev < 1 {
		removedRev = 1
	}
	return s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx, ok := txStore.(*SQLiteStore)
		if !ok {
			return fmt.Errorf("group member consent requires the SQLite store backend")
		}
		selectors := normalizeConsentSubtree(domainTags)
		if _, err := tx.writeExecContext(ctx, `DELETE FROM sync_group_member_pending_domain WHERE group_id=? AND member_chain_id=?`, groupID, memberChainID); err != nil {
			return fmt.Errorf("replace pending member consent: %w", err)
		}
		for _, tag := range selectors {
			if _, err := tx.writeExecContext(ctx, `
				INSERT INTO sync_group_member_pending_domain(group_id,member_chain_id,domain_tag,added_revision)
				VALUES(?,?,?,?)`, groupID, memberChainID, tag, revision); err != nil {
				return fmt.Errorf("store pending member consent: %w", err)
			}
		}
		shared, err := tx.ListSyncGroupDomains(ctx, groupID, true)
		if err != nil {
			return err
		}
		// Only selectors already inside a live shared root become active. An
		// ancestor selector (hr) does NOT activate merely because hr.public is
		// shared, preventing silent consent to a future sibling hr.secret.
		desired := make([]string, 0, len(selectors))
		for _, tag := range selectors {
			if consentTagInSharedSet(tag, shared) {
				desired = append(desired, tag)
			}
		}
		// Step 1 — ensure each desired tag is present and ACTIVE (insert, or
		// re-activate a previously-removed row). Re-applying the same entry leaves
		// an already-active row unchanged.
		for _, tag := range desired {
			if _, err := tx.writeExecContext(ctx, `
				INSERT INTO sync_group_member_domain
					(group_id, member_chain_id, domain_tag, added_revision, removed_revision)
				VALUES (?, ?, ?, ?, 0)
				ON CONFLICT(group_id, member_chain_id, domain_tag) DO UPDATE SET
					added_revision=excluded.added_revision,
					removed_revision=0`,
				groupID, memberChainID, tag, revision); err != nil {
				return fmt.Errorf("upsert member consent domain: %w", err)
			}
		}
		// Step 2 — stamp every currently-active row NOT in the desired set as
		// removed. The `removed_revision=0` guard makes it monotonic + idempotent: a
		// row already stamped is never re-stamped, and a replay finds nothing active
		// outside the desired set.
		q := `UPDATE sync_group_member_domain SET removed_revision=?
		        WHERE group_id=? AND member_chain_id=? AND removed_revision=0`
		args := []any{removedRev, groupID, memberChainID}
		if len(desired) > 0 {
			placeholders := make([]string, len(desired))
			for i, t := range desired {
				placeholders[i] = "?"
				args = append(args, t)
			}
			q += ` AND domain_tag NOT IN (` + strings.Join(placeholders, ",") + `)`
		}
		if _, err := tx.writeExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("retire member consent domains: %w", err)
		}
		return nil
	})
}

// ListPendingGroupMemberConsentDomains is an audit/status helper. Pending rows
// are never consulted by entitlement getters or write authorization.
func (s *SQLiteStore) ListPendingGroupMemberConsentDomains(ctx context.Context, groupID, memberChainID string) ([]string, error) {
	rows, err := s.conn.QueryContext(ctx, `SELECT domain_tag FROM sync_group_member_pending_domain WHERE group_id=? AND member_chain_id=? ORDER BY domain_tag`, groupID, memberChainID)
	if err != nil {
		return nil, fmt.Errorf("list pending member consent: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, err
		}
		out = append(out, tag)
	}
	return out, rows.Err()
}

// ReplaceGroupMemberOwnerDomains stores the immutable capability tuple carried
// by an invitee+controller signed member_invite.  It never widens content reads
// and is intentionally separate from both active and pending consent.
func (s *SQLiteStore) ReplaceGroupMemberOwnerDomains(ctx context.Context, groupID, memberChainID string, domainTags []string) error {
	if groupID == "" || memberChainID == "" {
		return fmt.Errorf("group_id and member_chain_id are required")
	}
	if _, err := s.writeExecContext(ctx, `DELETE FROM sync_group_member_owner_domain WHERE group_id=? AND member_chain_id=?`, groupID, memberChainID); err != nil {
		return fmt.Errorf("replace member owner domains: %w", err)
	}
	for _, tag := range normalizeConsentSubtree(domainTags) {
		if _, err := s.writeExecContext(ctx, `INSERT INTO sync_group_member_owner_domain(group_id,member_chain_id,domain_tag) VALUES(?,?,?)`, groupID, memberChainID, tag); err != nil {
			return fmt.Errorf("store member owner domain: %w", err)
		}
	}
	return nil
}

// GroupMemberMayOwnDomain checks the exact capability that the invitee offered
// and the controller acknowledged at enrollment.  Ancestor and wildcard scopes
// cover descendants; descendants never cover an ancestor or sibling.
func (s *SQLiteStore) GroupMemberMayOwnDomain(ctx context.Context, groupID, memberChainID, domainTag string) (bool, error) {
	if groupID == "" || memberChainID == "" || domainTag == "" {
		return false, nil
	}
	rows, err := s.conn.QueryContext(ctx, `SELECT domain_tag FROM sync_group_member_owner_domain WHERE group_id=? AND member_chain_id=?`, groupID, memberChainID)
	if err != nil {
		return false, fmt.Errorf("read member owner domains: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var allowed string
		if err := rows.Scan(&allowed); err != nil {
			return false, err
		}
		if allowed == "*" || allowed == domainTag || isDomainDescendant(domainTag, allowed) {
			return true, nil
		}
	}
	return false, rows.Err()
}

// ActivatePendingGroupMemberConsentForDomain promotes only selectors that are
// equal to, or descendants of, the newly live shared root. Ancestor selectors
// remain pending, closing the sibling-widening case described above.
func (s *SQLiteStore) ActivatePendingGroupMemberConsentForDomain(ctx context.Context, groupID, domainTag string, revision int64) error {
	if groupID == "" || domainTag == "" {
		return fmt.Errorf("group_id and domain_tag are required")
	}
	rows, err := s.conn.QueryContext(ctx, `
		SELECT p.member_chain_id,p.domain_tag
		  FROM sync_group_member_pending_domain p
		  JOIN sync_group_member m ON m.group_id=p.group_id AND m.member_chain_id=p.member_chain_id
		 WHERE p.group_id=? AND m.role='selective-sync' AND m.member_state IN ('active','resyncing','invited')`, groupID)
	if err != nil {
		return fmt.Errorf("scan pending member consent: %w", err)
	}
	type pending struct{ member, tag string }
	var matches []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.member, &p.tag); err != nil {
			_ = rows.Close()
			return err
		}
		if p.tag == domainTag || isDomainDescendant(p.tag, domainTag) {
			matches = append(matches, p)
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, p := range matches {
		if _, err := s.writeExecContext(ctx, `
			INSERT INTO sync_group_member_domain(group_id,member_chain_id,domain_tag,added_revision,removed_revision)
			VALUES(?,?,?,?,0)
			ON CONFLICT(group_id,member_chain_id,domain_tag) DO UPDATE SET added_revision=excluded.added_revision,removed_revision=0`,
			groupID, p.member, p.tag, revision); err != nil {
			return fmt.Errorf("activate pending member consent: %w", err)
		}
	}
	return nil
}

// StampGroupMemberConsentDomainRemoved retires the consent rows made stale by a
// domain_remove: every ACTIVE consent row across all members that references the
// removed tag OR a domain nested beneath it (the owner removed "hr", so consent to
// "hr" and "hr.payroll" is now stale). Monotonic + idempotent via the
// `removed_revision=0` guard, mirroring SetSyncGroupDomainRemoved. A consent to an
// ANCESTOR of the removed tag is left intact — it still covers other shared domains.
func (s *SQLiteStore) StampGroupMemberConsentDomainRemoved(ctx context.Context, groupID, domainTag string, revision int64) error {
	if groupID == "" || domainTag == "" {
		return fmt.Errorf("group_id and domain_tag are required")
	}
	removedRev := revision
	if removedRev < 1 {
		removedRev = 1
	}
	_, err := s.writeExecContext(ctx, `
		UPDATE sync_group_member_domain SET removed_revision=?
		 WHERE group_id=? AND removed_revision=0
		   AND (domain_tag=? OR domain_tag LIKE ? ESCAPE '\')`,
		removedRev, groupID, domainTag, likeEscapeSubtree(domainTag))
	if err != nil {
		return fmt.Errorf("stamp member consent domain removed: %w", err)
	}
	return nil
}

// SnapshotRemovedDomainEntitlements records the active members that were
// entitled to domainTag immediately before its removal. This is called by the
// domain_remove apply transaction BEFORE it retires the domain and live consent
// rows. It is append-only/idempotent per removal revision, so a later re-add and
// second removal cannot overwrite the history of the first lifecycle.
func (s *SQLiteStore) SnapshotRemovedDomainEntitlements(ctx context.Context, groupID, domainTag string, removalGeneration int64) error {
	if groupID == "" || domainTag == "" {
		return fmt.Errorf("group_id and domain_tag are required")
	}
	if removalGeneration < 1 {
		return fmt.Errorf("removal generation must be positive")
	}
	_, err := s.writeExecContext(ctx, `
		INSERT OR IGNORE INTO sync_group_removed_domain_entitlement
			(group_id, domain_tag, member_chain_id, removed_revision)
		SELECT gd.group_id, gd.domain_tag, gm.member_chain_id, ?
		  FROM sync_group_domain gd
		  JOIN sync_group_member gm ON gm.group_id = gd.group_id
		 WHERE gd.group_id = ? AND gd.domain_tag = ? AND gd.removed_revision = 0
		   AND gm.member_state IN ('active','resyncing')
		   AND (gm.role = 'full-sync' OR gm.member_chain_id = gd.owner_chain_id
		        OR (gm.role = 'selective-sync' AND EXISTS (
		            SELECT 1 FROM sync_group_member_domain c
		             WHERE c.group_id = gd.group_id AND c.member_chain_id = gm.member_chain_id
		               AND c.removed_revision = 0
		               AND (c.domain_tag = ? OR ? LIKE REPLACE(REPLACE(REPLACE(c.domain_tag, '\', '\\'), '%', '\%'), '_', '\_') || '.%' ESCAPE '\')
		        )))`, removalGeneration, groupID, domainTag, domainTag, domainTag)
	if err != nil {
		return fmt.Errorf("snapshot removed domain entitlements: %w", err)
	}
	return nil
}

// WasMemberEntitledAtDomainRemoval answers from the immutable removal snapshot,
// never the mutable live-consent overlay. It is a narrow journal-serve helper:
// callers may reveal only the removal/tombstone/anchor suffix when it returns
// true.
func (s *SQLiteStore) WasMemberEntitledAtDomainRemoval(ctx context.Context, groupID, domainTag, memberChainID string, removalGeneration int64) (bool, error) {
	if groupID == "" || domainTag == "" || memberChainID == "" || removalGeneration < 1 {
		return false, nil
	}
	var n int
	if err := s.conn.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sync_group_removed_domain_entitlement
		 WHERE group_id=? AND domain_tag=? AND member_chain_id=? AND removed_revision=?`,
		groupID, domainTag, memberChainID, removalGeneration).Scan(&n); err != nil {
		return false, fmt.Errorf("check removed domain entitlement: %w", err)
	}
	return n > 0, nil
}

// ListGroupMemberConsentDomains returns a member's ACTIVE consent tags (a
// selective-sync member's chosen subset), ordered by tag. A member with no consent
// rows returns the empty set — which the getters treat as sharing NOTHING beyond
// its own owned domains (fail-closed under-serve).
func (s *SQLiteStore) ListGroupMemberConsentDomains(ctx context.Context, groupID, memberChainID string) ([]string, error) {
	if groupID == "" || memberChainID == "" {
		return nil, nil
	}
	rows, err := s.conn.QueryContext(ctx, `
		SELECT domain_tag FROM sync_group_member_domain
		 WHERE group_id=? AND member_chain_id=? AND removed_revision=0
		 ORDER BY domain_tag`, groupID, memberChainID)
	if err != nil {
		return nil, fmt.Errorf("list member consent domains: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, fmt.Errorf("scan member consent domain: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// MemberConsentsGroupDomain reports whether a selective-sync member holds an ACTIVE
// consent row covering domainTag (exact OR an ancestor of it, subtree-aware). It is
// the Go-level entitlement probe for authorizeJournalSubchain, where the shared-set
// membership is already established by the caller. Fail-closed: no covering row =>
// false (never over-serve a sub-chain the member did not select).
func (s *SQLiteStore) MemberConsentsGroupDomain(ctx context.Context, groupID, memberChainID, domainTag string) (bool, error) {
	if groupID == "" || memberChainID == "" || domainTag == "" {
		return false, nil
	}
	var n int
	if err := s.conn.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sync_group_member_domain c
		 WHERE c.group_id=? AND c.member_chain_id=? AND c.removed_revision=0
		   AND (c.domain_tag=? OR ? LIKE REPLACE(REPLACE(REPLACE(c.domain_tag, '\', '\\'), '%', '\%'), '_', '\_') || '.%' ESCAPE '\')`,
		groupID, memberChainID, domainTag, domainTag).Scan(&n); err != nil {
		return false, fmt.Errorf("member consents group domain: %w", err)
	}
	return n > 0, nil
}

// ---- sync_group_log (partitioned audit journal) ----

// AppendSyncGroupLog persists one pre-signed, pre-hashed journal entry. The
// federation layer owns canonicalization/hash-chaining/signing (docs §5.3-5.4);
// the store enforces only append-uniqueness via the (group_id, subchain, seq) PK.
func (s *SQLiteStore) AppendSyncGroupLog(ctx context.Context, e SyncGroupLogEntry) error {
	if e.GroupID == "" || e.Subchain == "" || e.EntryHash == "" || e.EntryType == "" {
		return fmt.Errorf("group_id, subchain, entry_hash, and entry_type are required")
	}
	_, err := s.writeExecContext(ctx, `
		INSERT INTO sync_group_log
			(group_id, subchain, seq, prev_hash, entry_hash, entry_type, payload_json,
			 author_chain_id, author_agent_pubkey, author_sig, controller_epoch,
			 controller_chain_id, controller_agent_pubkey, controller_sig)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.GroupID, e.Subchain, e.Seq, e.PrevHash, e.EntryHash, e.EntryType, e.PayloadJSON,
		e.AuthorChainID, e.AuthorAgentPubkey, e.AuthorSig, e.ControllerEpoch,
		e.ControllerChainID, e.ControllerAgentPubkey, e.ControllerSig)
	if err != nil {
		return fmt.Errorf("append sync group log: %w", err)
	}
	return nil
}

// SubchainHead is the tip of one journal sub-chain.
type SubchainHead struct {
	Seq       int64
	EntryHash string
}

// GetSyncGroupSubchainHead returns the highest (seq, entry_hash) for a sub-chain,
// or (nil, nil) if the sub-chain is empty. The next append uses Seq+1 and
// PrevHash=EntryHash.
func (s *SQLiteStore) GetSyncGroupSubchainHead(ctx context.Context, groupID, subchain string) (*SubchainHead, error) {
	var h SubchainHead
	err := s.conn.QueryRowContext(ctx,
		`SELECT seq, entry_hash FROM sync_group_log WHERE group_id=? AND subchain=? ORDER BY seq DESC LIMIT 1`,
		groupID, subchain).Scan(&h.Seq, &h.EntryHash)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get subchain head: %w", err)
	}
	return &h, nil
}

// GetSyncGroupLogEntry returns the entry at a specific (group, subchain, seq),
// or (nil, nil) if absent — the fork-detection lookup for journal ingest (an
// incoming entry whose seq already exists locally must match by entry_hash).
func (s *SQLiteStore) GetSyncGroupLogEntry(ctx context.Context, groupID, subchain string, seq int64) (*SyncGroupLogEntry, error) {
	var e SyncGroupLogEntry
	var createdAt string
	err := s.conn.QueryRowContext(ctx, `
		SELECT group_id, subchain, seq, prev_hash, entry_hash, entry_type, payload_json,
		       author_chain_id, author_agent_pubkey, author_sig, controller_epoch,
		       controller_chain_id, controller_agent_pubkey, controller_sig, created_at
		  FROM sync_group_log WHERE group_id=? AND subchain=? AND seq=?`, groupID, subchain, seq).
		Scan(&e.GroupID, &e.Subchain, &e.Seq, &e.PrevHash, &e.EntryHash, &e.EntryType, &e.PayloadJSON,
			&e.AuthorChainID, &e.AuthorAgentPubkey, &e.AuthorSig, &e.ControllerEpoch,
			&e.ControllerChainID, &e.ControllerAgentPubkey, &e.ControllerSig, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get sync group log entry: %w", err)
	}
	e.CreatedAt = parseTime(createdAt)
	return &e, nil
}

// ListSyncGroupLog pages a sub-chain in append order from an exclusive seq cursor
// (afterSeq < 0 starts at the beginning).
func (s *SQLiteStore) ListSyncGroupLog(ctx context.Context, groupID, subchain string, afterSeq int64, limit int) ([]SyncGroupLogEntry, error) {
	if limit <= 0 || limit > 2000 {
		limit = 2000
	}
	rows, err := s.conn.QueryContext(ctx, `
		SELECT group_id, subchain, seq, prev_hash, entry_hash, entry_type, payload_json,
		       author_chain_id, author_agent_pubkey, author_sig, controller_epoch,
		       controller_chain_id, controller_agent_pubkey, controller_sig, created_at
		  FROM sync_group_log
		 WHERE group_id=? AND subchain=? AND seq>?
		 ORDER BY seq ASC LIMIT ?`, groupID, subchain, afterSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("list sync group log: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []SyncGroupLogEntry
	for rows.Next() {
		var e SyncGroupLogEntry
		var createdAt string
		if err := rows.Scan(&e.GroupID, &e.Subchain, &e.Seq, &e.PrevHash, &e.EntryHash, &e.EntryType,
			&e.PayloadJSON, &e.AuthorChainID, &e.AuthorAgentPubkey, &e.AuthorSig, &e.ControllerEpoch,
			&e.ControllerChainID, &e.ControllerAgentPubkey, &e.ControllerSig, &createdAt); err != nil {
			return nil, fmt.Errorf("scan sync group log: %w", err)
		}
		e.CreatedAt = parseTime(createdAt)
		out = append(out, e)
	}
	return out, rows.Err()
}

// ListSyncGroupTerminalLog returns only the signed terminal suffix entries of a
// removed domain sub-chain. It exists solely for prior-entitled non-owners: the
// normal journal reader must never filter its chain because that would expose a
// misleading partial history to a currently entitled member.
func (s *SQLiteStore) ListSyncGroupTerminalLog(ctx context.Context, groupID, subchain string, fromSeq, afterSeq int64, limit int) ([]SyncGroupLogEntry, error) {
	if limit <= 0 || limit > 2000 {
		limit = 2000
	}
	rows, err := s.conn.QueryContext(ctx, `
		SELECT group_id, subchain, seq, prev_hash, entry_hash, entry_type, payload_json,
		       author_chain_id, author_agent_pubkey, author_sig, controller_epoch,
		       controller_chain_id, controller_agent_pubkey, controller_sig, created_at
		  FROM sync_group_log
		 WHERE group_id=? AND subchain=? AND seq>=? AND seq>?
		   AND entry_type IN ('domain_remove','tombstone','anchor')
		 ORDER BY seq ASC LIMIT ?`, groupID, subchain, fromSeq, afterSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("list sync group terminal log: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []SyncGroupLogEntry
	for rows.Next() {
		var e SyncGroupLogEntry
		var createdAt string
		if err := rows.Scan(&e.GroupID, &e.Subchain, &e.Seq, &e.PrevHash, &e.EntryHash, &e.EntryType,
			&e.PayloadJSON, &e.AuthorChainID, &e.AuthorAgentPubkey, &e.AuthorSig, &e.ControllerEpoch,
			&e.ControllerChainID, &e.ControllerAgentPubkey, &e.ControllerSig, &createdAt); err != nil {
			return nil, fmt.Errorf("scan sync group terminal log: %w", err)
		}
		e.CreatedAt = parseTime(createdAt)
		out = append(out, e)
	}
	return out, rows.Err()
}

// ---- sync_tombstone ----

// InsertSyncTombstone records a removal. Idempotent on the composite PK (a
// replayed journal entry re-inserting the same tombstone is a no-op).
func (s *SQLiteStore) InsertSyncTombstone(ctx context.Context, t SyncTombstone) error {
	switch t.Scope {
	case TombstoneScopeMember, TombstoneScopeDomain, TombstoneScopeMemory:
	default:
		return fmt.Errorf("invalid tombstone scope %q", t.Scope)
	}
	switch t.Enforcement {
	case TombstoneEnforceAdvisory, TombstoneEnforceLocalSuppress:
	default:
		return fmt.Errorf("invalid tombstone enforcement %q", t.Enforcement)
	}
	if t.GroupID == "" {
		return fmt.Errorf("group_id is required")
	}
	_, err := s.writeExecContext(ctx, `
		INSERT OR IGNORE INTO sync_tombstone
			(group_id, scope, enforcement, member_chain_id, domain_tag, origin_chain_id, origin_memory_id,
			 reason, revision, subchain, journal_seq, author_chain_id, author_sig)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		t.GroupID, t.Scope, t.Enforcement, t.MemberChainID, t.DomainTag, t.OriginChainID, t.OriginMemoryID,
		t.Reason, t.Revision, t.Subchain, t.JournalSeq, t.AuthorChainID, t.AuthorSig)
	if err != nil {
		return fmt.Errorf("insert sync tombstone: %w", err)
	}
	return nil
}

// IsLocallySuppressed reports whether an origin memory pair carries a
// memory-scope local_suppress tombstone in this group — the anti-resurrection
// check the receive path consults before admitting a backfilled item
// (docs §10 Gate 6.5). group_id="" matches any group (a local delete suppresses
// re-admission regardless of which group later offers it back).
func (s *SQLiteStore) IsLocallySuppressed(ctx context.Context, groupID, originChainID, originMemoryID string) (bool, error) {
	if originChainID == "" || originMemoryID == "" {
		return false, nil
	}
	q := `SELECT COUNT(*) FROM sync_tombstone
	       WHERE scope='memory' AND enforcement='local_suppress'
	         AND origin_chain_id=? AND origin_memory_id=?`
	args := []any{originChainID, originMemoryID}
	if groupID != "" {
		q += ` AND group_id=?`
		args = append(args, groupID)
	}
	var n int
	if err := s.conn.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
		return false, fmt.Errorf("check local suppress: %w", err)
	}
	return n > 0, nil
}

// ListSyncTombstones returns tombstones for a group, optionally filtered by scope
// (scope="" = all).
func (s *SQLiteStore) ListSyncTombstones(ctx context.Context, groupID, scope string) ([]SyncTombstone, error) {
	q := `SELECT group_id, scope, enforcement, member_chain_id, domain_tag, origin_chain_id, origin_memory_id,
	             reason, revision, subchain, journal_seq, author_chain_id, author_sig, created_at
	        FROM sync_tombstone WHERE group_id=?`
	args := []any{groupID}
	if scope != "" {
		q += ` AND scope=?`
		args = append(args, scope)
	}
	q += ` ORDER BY created_at ASC`
	rows, err := s.conn.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list sync tombstones: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []SyncTombstone
	for rows.Next() {
		var t SyncTombstone
		var createdAt string
		if err := rows.Scan(&t.GroupID, &t.Scope, &t.Enforcement, &t.MemberChainID, &t.DomainTag,
			&t.OriginChainID, &t.OriginMemoryID, &t.Reason, &t.Revision, &t.Subchain, &t.JournalSeq,
			&t.AuthorChainID, &t.AuthorSig, &createdAt); err != nil {
			return nil, fmt.Errorf("scan sync tombstone: %w", err)
		}
		t.CreatedAt = parseTime(createdAt)
		out = append(out, t)
	}
	return out, rows.Err()
}

// ---- v11.8 build-step 6: group-aware fan-out read models ----
//
// These concrete *SQLiteStore getters drive the group-aware sender/receiver
// paths in internal/federation (per-member star fan-out, the digest hard-gate,
// relayed origin-auth, and the effective group consent that keeps a group-only
// member's outbox draining). Every one is READ-ONLY and role-aware: entitlement
// to a shared domain is full-sync members + the domain's owner. Selective-sync
// subsets have no per-member storage yet (docs deferral, sync_journal_exchange.go:
// authorizeJournalSubchain), so a selective-sync member is treated as sharing
// NOTHING but its own owned domains — UNDER-serving, never over-serving (I1/I5).

// GroupFanoutTarget is one active group member that should receive an owner's
// native memory in a shared domain (docs §9.1 star fan-out).
type GroupFanoutTarget struct {
	GroupID       string
	MemberChainID string
}

// ListGroupFanoutTargets returns the active members (other than the owner) that
// share domainTag (or an ancestor of it) where ownerChainID owns the domain —
// the §9.1 STAR fan-out set. Only full-sync members are targeted; selective-sync
// per-member consent is deferred (fail closed = no leak). The owner is excluded
// so a memory is never fanned back to its author.
func (s *SQLiteStore) ListGroupFanoutTargets(ctx context.Context, ownerChainID, domainTag string) ([]GroupFanoutTarget, error) {
	if ownerChainID == "" || domainTag == "" {
		return nil, nil
	}
	rows, err := s.conn.QueryContext(ctx, `
		SELECT gm.group_id, gm.member_chain_id
		  FROM sync_group_domain gd
		  JOIN sync_group_member gm ON gm.group_id = gd.group_id
		 WHERE gd.owner_chain_id = ? AND gd.removed_revision = 0
		   AND (gd.domain_tag = ? OR ? LIKE REPLACE(REPLACE(REPLACE(gd.domain_tag, '\', '\\'), '%', '\%'), '_', '\_') || '.%' ESCAPE '\')
		   AND gm.member_state = 'active'
		   AND gm.member_chain_id != gd.owner_chain_id
		   AND (gm.role = 'full-sync' OR gm.member_chain_id = gd.owner_chain_id
		        OR (gm.role = 'selective-sync' AND EXISTS (
		            SELECT 1 FROM sync_group_member_domain c
		             WHERE c.group_id = gd.group_id AND c.member_chain_id = gm.member_chain_id
		               AND c.removed_revision = 0
		       AND (c.domain_tag = ? OR ? LIKE REPLACE(REPLACE(REPLACE(c.domain_tag, '\', '\\'), '%', '\%'), '_', '\_') || '.%' ESCAPE '\'))))`, ownerChainID, domainTag, domainTag, domainTag, domainTag)
	if err != nil {
		return nil, fmt.Errorf("list group fanout targets: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []GroupFanoutTarget
	for rows.Next() {
		var t GroupFanoutTarget
		if err := rows.Scan(&t.GroupID, &t.MemberChainID); err != nil {
			return nil, fmt.Errorf("scan group fanout target: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// ListActiveGroupMemberChains returns every DISTINCT chain that is an active
// member of any group. Unioned with ListSyncDomainChains in the drainer so a
// group-only member (a cross_fed edge but no pairwise sync_domains consent) is
// still enumerated and its fan-out outbox rows actually drain (the strand fix).
func (s *SQLiteStore) ListActiveGroupMemberChains(ctx context.Context) ([]string, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT DISTINCT member_chain_id FROM sync_group_member WHERE member_state='active' ORDER BY member_chain_id`)
	if err != nil {
		return nil, fmt.Errorf("list active group member chains: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, fmt.Errorf("scan active group member chain: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// GroupDomainsForMember returns the active shared domains one member is entitled
// to across a single group (full-sync: all active domains; any role: its own
// owned domains). Feeds the digest ConsentedDomains clamp (docs §9.2).
func (s *SQLiteStore) GroupDomainsForMember(ctx context.Context, groupID, memberChainID string) ([]string, error) {
	if groupID == "" || memberChainID == "" {
		return nil, nil
	}
	rows, err := s.conn.QueryContext(ctx, `
		SELECT DISTINCT CASE
		       WHEN gm.role = 'selective-sync' AND gm.member_chain_id != gd.owner_chain_id
		       THEN CASE WHEN c.domain_tag = gd.domain_tag OR gd.domain_tag LIKE REPLACE(REPLACE(REPLACE(c.domain_tag, '\', '\\'), '%', '\%'), '_', '\_') || '.%' ESCAPE '\'
		                 THEN gd.domain_tag ELSE c.domain_tag END
		       ELSE gd.domain_tag END AS effective_domain
		  FROM sync_group_domain gd
		  JOIN sync_group_member gm ON gm.group_id = gd.group_id
		  LEFT JOIN sync_group_member_domain c
		    ON c.group_id = gd.group_id AND c.member_chain_id = gm.member_chain_id
		   AND c.removed_revision = 0
		   AND (c.domain_tag = gd.domain_tag
		        OR c.domain_tag LIKE REPLACE(REPLACE(REPLACE(gd.domain_tag, '\', '\\'), '%', '\%'), '_', '\_') || '.%' ESCAPE '\'
		        OR gd.domain_tag LIKE REPLACE(REPLACE(REPLACE(c.domain_tag, '\', '\\'), '%', '\%'), '_', '\_') || '.%' ESCAPE '\')
		 WHERE gd.group_id = ? AND gm.member_chain_id = ? AND gm.member_state = 'active'
		   AND gd.removed_revision = 0
		   AND (gm.role = 'full-sync' OR gm.member_chain_id = gd.owner_chain_id
		        OR (gm.role = 'selective-sync' AND c.domain_tag IS NOT NULL))
		 ORDER BY effective_domain`, groupID, memberChainID)
	if err != nil {
		return nil, fmt.Errorf("group domains for member: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, fmt.Errorf("scan group domain for member: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// MemberSharesGroupDomain is the digest hard-gate precondition (docs §9.2): true
// iff memberChainID is an active member of groupID AND domainTag is covered by an
// active group domain the member is entitled to (subtree match, role-aware).
func (s *SQLiteStore) MemberSharesGroupDomain(ctx context.Context, groupID, memberChainID, domainTag string) (bool, error) {
	if groupID == "" || memberChainID == "" || domainTag == "" {
		return false, nil
	}
	var n int
	if err := s.conn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		  FROM sync_group_domain gd
		  JOIN sync_group_member gm ON gm.group_id = gd.group_id
		 WHERE gd.group_id = ? AND gm.member_chain_id = ? AND gm.member_state = 'active'
		   AND gd.removed_revision = 0
		   AND (gd.domain_tag = ? OR ? LIKE REPLACE(REPLACE(REPLACE(gd.domain_tag, '\', '\\'), '%', '\%'), '_', '\_') || '.%' ESCAPE '\')
		   AND (gm.role = 'full-sync' OR gm.member_chain_id = gd.owner_chain_id
		        OR (gm.role = 'selective-sync' AND EXISTS (
		            SELECT 1 FROM sync_group_member_domain c
		             WHERE c.group_id = gd.group_id AND c.member_chain_id = gm.member_chain_id
		               AND c.removed_revision = 0
		               AND (c.domain_tag = ? OR ? LIKE REPLACE(REPLACE(REPLACE(c.domain_tag, '\', '\\'), '%', '\%'), '_', '\_') || '.%' ESCAPE '\'))))`,
		groupID, memberChainID, domainTag, domainTag, domainTag, domainTag).Scan(&n); err != nil {
		return false, fmt.Errorf("member shares group domain: %w", err)
	}
	return n > 0, nil
}

// GroupSharedDomains returns the active group domains that BOTH chains are
// entitled to (across every group where both are active members). This is the
// effective group consent between two peers — unioned with pairwise sync_domains
// so a group-only member's outbox both enqueues and drains (docs §9.1 strand fix)
// and the receive-side consent gate admits group-shared domains.
func (s *SQLiteStore) GroupSharedDomains(ctx context.Context, chainA, chainB string) ([]string, error) {
	if chainA == "" || chainB == "" {
		return nil, nil
	}
	refs, err := s.groupSharedDomainRefs(ctx, chainA, chainB)
	if err != nil {
		return nil, err
	}
	domains := make([]string, 0, len(refs))
	seen := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		if _, ok := seen[ref.DomainTag]; !ok {
			seen[ref.DomainTag] = struct{}{}
			domains = append(domains, ref.DomainTag)
		}
	}
	return domains, nil
}

// ResolveGroupRelay authorizes a relayed backfill item (docs §9.2 must-fix #1/#6):
// returns the SINGLE group in which the RECEIVER (localChainID, this node), the
// relayerChainID (the authenticated pushing peer), AND originChainID (the memory's
// author) are ALL active members and domainTag is an active shared domain (subtree).
// Binding all three to ONE group closes cross-group laundering: because (group_id,
// domain_tag) is per-group, the same tag can exist in multiple groups, so a
// dual-group bridge relayer must NOT be able to move an origin's memory into a group
// the origin never joined. ok=false denies relay — the receiver keeps the door shut
// (validateSyncItem/originVerifyKey stay fail-closed).
func (s *SQLiteStore) ResolveGroupRelay(ctx context.Context, localChainID, relayerChainID, originChainID, domainTag string) (string, bool, error) {
	if localChainID == "" || relayerChainID == "" || originChainID == "" || domainTag == "" {
		return "", false, nil
	}
	rows, err := s.conn.QueryContext(ctx, `
		SELECT gd.group_id
		  FROM sync_group_domain gd
		  JOIN sync_group_member relayer ON relayer.group_id = gd.group_id
		       AND relayer.member_chain_id = ? AND relayer.member_state = 'active'
		  JOIN sync_group_member origin ON origin.group_id = gd.group_id
		       AND origin.member_chain_id = ? AND origin.member_state = 'active'
		  JOIN sync_group_member receiver ON receiver.group_id = gd.group_id
		       AND receiver.member_chain_id = ? AND receiver.member_state = 'active'
		 WHERE gd.removed_revision = 0
		   AND (gd.domain_tag = ? OR ? LIKE REPLACE(REPLACE(REPLACE(gd.domain_tag, '\', '\\'), '%', '\%'), '_', '\_') || '.%' ESCAPE '\')
		 ORDER BY gd.group_id`, relayerChainID, originChainID, localChainID, domainTag, domainTag)
	if err != nil {
		return "", false, fmt.Errorf("resolve group relay: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var groupID string
		if err := rows.Scan(&groupID); err != nil {
			return "", false, err
		}
		allowed := true
		for _, member := range []string{localChainID, relayerChainID, originChainID} {
			ok, err := s.MemberSharesGroupDomain(ctx, groupID, member, domainTag)
			if err != nil {
				return "", false, err
			}
			if !ok {
				allowed = false
				break
			}
		}
		if allowed {
			return groupID, true, nil
		}
	}
	return "", false, rows.Err()
}

// GetGroupMemberAgentPubkey resolves the pinned ed25519 pubkey (hex) that a
// relayed item's origin_sig must verify against (docs §9.2 must-fix #1). Only an
// active/resyncing member's key is returned — a removed/left/invited member can
// never be an authenticatable origin. Fails closed (error) when the key is
// unresolved so Gate 5.5 rejects rather than trusting the relayer's own key.
func (s *SQLiteStore) GetGroupMemberAgentPubkey(ctx context.Context, groupID, memberChainID string) (string, error) {
	if groupID == "" || memberChainID == "" {
		return "", fmt.Errorf("group_id and member_chain_id are required")
	}
	var key string
	err := s.conn.QueryRowContext(ctx, `
		SELECT member_agent_pubkey FROM sync_group_member
		 WHERE group_id = ? AND member_chain_id = ? AND member_state IN ('active','resyncing')`,
		groupID, memberChainID).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("no active roster key for %s in group %s", memberChainID, groupID)
	}
	if err != nil {
		return "", fmt.Errorf("get group member agent pubkey: %w", err)
	}
	if key == "" {
		return "", fmt.Errorf("empty roster key for %s in group %s", memberChainID, groupID)
	}
	return key, nil
}

// ListGroupServableOriginIDs is the group-scoped, leak-safe backfill source for the
// multi-node digest (docs §9.2, must-fix #1/#7/#12). It supersedes an origin-chain-
// agnostic raw-subtree serve (which leaked across groups and over classified children):
// it serves an admitted origin id ONLY when, for the row's OWN domain_tag:
//   - the ORIGIN chain is an active member of groupID — no cross-group provenance
//     leak (sync_origin has no group_id, and a domain tag can exist in many groups); AND
//   - the MOST-SPECIFIC active group domain in groupID that covers the row (the longest
//     covering tag) is servable by THIS responder — unclassified (max_clearance=0) OR
//     owned by responderChainID (classified serving is owner-star-only, must-fix #12); AND
//   - BOTH the requester and the responder are entitled to that most-specific covering
//     domain (role-aware), so a member sharing only an ancestor is never served a nested
//     domain's ids.
//
// Binding these to the row's most-specific covering domain (not the requester-supplied
// umbrella tag) is what closes the classified-child-via-unclassified-parent oracle.
// Paged by the composite (origin_memory_id, origin_chain_id) cursor.
func (s *SQLiteStore) ListGroupServableOriginIDs(ctx context.Context, groupID, requesterChainID, responderChainID, domain, after string, limit int) ([]string, string, error) {
	if groupID == "" || requesterChainID == "" || responderChainID == "" || domain == "" {
		return nil, "", nil
	}
	if limit <= 0 || limit > 2000 {
		limit = 2000
	}
	afterMem, afterChain := decodeOriginCursor(after)
	rows, err := s.conn.QueryContext(ctx, `
		SELECT so.origin_memory_id, so.origin_chain_id
		  FROM sync_origin so
		  JOIN sync_group_member om
		       ON om.group_id = ? AND om.member_chain_id = so.origin_chain_id AND om.member_state = 'active'
		 WHERE so.outcome = 'admitted'
		   AND (so.domain_tag = ? OR so.domain_tag LIKE ? ESCAPE '\')
		   AND EXISTS (
		       SELECT 1 FROM sync_group_domain mc
		        WHERE mc.group_id = ? AND mc.removed_revision = 0
		          AND (mc.domain_tag = so.domain_tag OR so.domain_tag LIKE REPLACE(REPLACE(REPLACE(mc.domain_tag, '\', '\\'), '%', '\%'), '_', '\_') || '.%' ESCAPE '\')
		          AND NOT EXISTS (
		              SELECT 1 FROM sync_group_domain mc2
		               WHERE mc2.group_id = mc.group_id AND mc2.removed_revision = 0
		                 AND (mc2.domain_tag = so.domain_tag OR so.domain_tag LIKE REPLACE(REPLACE(REPLACE(mc2.domain_tag, '\', '\\'), '%', '\%'), '_', '\_') || '.%' ESCAPE '\')
		                 AND length(mc2.domain_tag) > length(mc.domain_tag)
		          )
		          AND (mc.max_clearance = 0 OR mc.owner_chain_id = ?)
		          AND EXISTS (SELECT 1 FROM sync_group_member rm
		                       WHERE rm.group_id = mc.group_id AND rm.member_chain_id = ? AND rm.member_state = 'active'
		                         AND (rm.role = 'full-sync' OR rm.member_chain_id = mc.owner_chain_id
		                              OR (rm.role = 'selective-sync' AND EXISTS (
		                                  SELECT 1 FROM sync_group_member_domain c
		                                   WHERE c.group_id = mc.group_id AND c.member_chain_id = rm.member_chain_id
		                                     AND c.removed_revision = 0
			                                     AND (c.domain_tag = so.domain_tag OR so.domain_tag LIKE REPLACE(REPLACE(REPLACE(c.domain_tag, '\', '\\'), '%', '\%'), '_', '\_') || '.%' ESCAPE '\')))))
		          AND EXISTS (SELECT 1 FROM sync_group_member sm
		                       WHERE sm.group_id = mc.group_id AND sm.member_chain_id = ? AND sm.member_state = 'active'
		                         AND (sm.role = 'full-sync' OR sm.member_chain_id = mc.owner_chain_id
		                              OR (sm.role = 'selective-sync' AND EXISTS (
		                                  SELECT 1 FROM sync_group_member_domain c
		                                   WHERE c.group_id = mc.group_id AND c.member_chain_id = sm.member_chain_id
		                                     AND c.removed_revision = 0
			                                     AND (c.domain_tag = so.domain_tag OR so.domain_tag LIKE REPLACE(REPLACE(REPLACE(c.domain_tag, '\', '\\'), '%', '\%'), '_', '\_') || '.%' ESCAPE '\')))))
		   )
		   AND (so.origin_memory_id > ? OR (so.origin_memory_id = ? AND so.origin_chain_id > ?))
		 ORDER BY so.origin_memory_id ASC, so.origin_chain_id ASC
		 LIMIT ?`,
		groupID, domain, likeEscapeSubtree(domain),
		groupID, responderChainID, requesterChainID, responderChainID,
		afterMem, afterMem, afterChain, limit)
	if err != nil {
		return nil, "", fmt.Errorf("list group servable origin ids: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	var lastMem, lastChain string
	for rows.Next() {
		var mem, chain string
		if err := rows.Scan(&mem, &chain); err != nil {
			return nil, "", fmt.Errorf("scan group servable origin id: %w", err)
		}
		out = append(out, mem)
		lastMem, lastChain = mem, chain
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) == limit {
		next = encodeOriginCursor(lastMem, lastChain)
	}
	return out, next, nil
}

// GroupRelayCandidate is one admitted copy this node HOLDS that it may relay to a
// group member who lacks it (v11.8 mesh pull-relay, docs §9.2). LocalMemoryID is
// the deterministic local copy id (drain content source); OriginChainID/OriginMemoryID
// are the ORIGINAL provenance (never this node) re-served verbatim with the stored sig.
type GroupRelayCandidate struct {
	OriginChainID  string
	OriginMemoryID string
	LocalMemoryID  string
}

// ListGroupRelayCandidates is the leak-safe SENDER-side twin of
// ListGroupServableOriginIDs (docs §9.2 must-fix #1/#7/#12): the admitted COPIES
// this responder (this node) HOLDS that it may relay to requesterChainID for
// `domain`, applying the IDENTICAL most-specific-covering-domain predicate
// (origin an active group member; the row's longest covering active group domain
// is unclassified OR owned by this responder — classified is owner-star-only; and
// BOTH requester and responder are entitled to that covering domain). It differs
// from the digest twin in four ways that make it a RELAY-ENQUEUE source rather
// than a digest-serve source:
//   - it returns the LOCAL copy id (so.local_memory_id) alongside the origin pair,
//     and requires local_memory_id != ” — we can only relay a copy we actually hold
//     as an immutable local memory (a same-domain-dup admission with an empty local
//     id is NOT relayable — there is no distinct copy to serve);
//   - it EXCLUDES origin_chain_id = requesterChainID — the requester IS the origin of
//     its own memories (that is a pairwise star, not a relay), so never relay them back;
//   - it EXCLUDES rows under a memory-scope local_suppress tombstone (Gate 6.5:
//     a locally-deleted item must never be re-originated to anyone); and
//   - it does NOT constrain by requester-supplied umbrella beyond the covering-domain
//     entitlement, so the caller subtracts what the peer already holds (the digest set).
//
// Paged by the same composite (origin_memory_id, origin_chain_id) cursor.
func (s *SQLiteStore) ListGroupRelayCandidates(ctx context.Context, groupID, requesterChainID, responderChainID, domain, after string, limit int) ([]GroupRelayCandidate, string, error) {
	if groupID == "" || requesterChainID == "" || responderChainID == "" || domain == "" {
		return nil, "", nil
	}
	if limit <= 0 || limit > 2000 {
		limit = 2000
	}
	afterMem, afterChain := decodeOriginCursor(after)
	rows, err := s.conn.QueryContext(ctx, `
		SELECT so.origin_memory_id, so.origin_chain_id, so.local_memory_id
		  FROM sync_origin so
		  JOIN sync_group_member om
		       ON om.group_id = ? AND om.member_chain_id = so.origin_chain_id AND om.member_state = 'active'
		 WHERE so.outcome = 'admitted'
		   AND so.local_memory_id != ''
		   AND so.origin_chain_id != ?
		   AND (so.domain_tag = ? OR so.domain_tag LIKE ? ESCAPE '\')
		   AND NOT EXISTS (
		       SELECT 1 FROM sync_tombstone t
		        WHERE t.scope = 'memory' AND t.enforcement = 'local_suppress'
		          AND t.origin_chain_id = so.origin_chain_id AND t.origin_memory_id = so.origin_memory_id)
		   AND EXISTS (
		       SELECT 1 FROM sync_group_domain mc
		        WHERE mc.group_id = ? AND mc.removed_revision = 0
		          AND (mc.domain_tag = so.domain_tag OR so.domain_tag LIKE REPLACE(REPLACE(REPLACE(mc.domain_tag, '\', '\\'), '%', '\%'), '_', '\_') || '.%' ESCAPE '\')
		          AND NOT EXISTS (
		              SELECT 1 FROM sync_group_domain mc2
		               WHERE mc2.group_id = mc.group_id AND mc2.removed_revision = 0
		                 AND (mc2.domain_tag = so.domain_tag OR so.domain_tag LIKE REPLACE(REPLACE(REPLACE(mc2.domain_tag, '\', '\\'), '%', '\%'), '_', '\_') || '.%' ESCAPE '\')
		                 AND length(mc2.domain_tag) > length(mc.domain_tag)
		          )
		          AND (mc.max_clearance = 0 OR mc.owner_chain_id = ?)
		          AND EXISTS (SELECT 1 FROM sync_group_member rm
		                       WHERE rm.group_id = mc.group_id AND rm.member_chain_id = ? AND rm.member_state = 'active'
		                         AND (rm.role = 'full-sync' OR rm.member_chain_id = mc.owner_chain_id
		                              OR (rm.role = 'selective-sync' AND EXISTS (
		                                  SELECT 1 FROM sync_group_member_domain c
		                                   WHERE c.group_id = mc.group_id AND c.member_chain_id = rm.member_chain_id
		                                     AND c.removed_revision = 0
			                                     AND (c.domain_tag = so.domain_tag OR so.domain_tag LIKE REPLACE(REPLACE(REPLACE(c.domain_tag, '\', '\\'), '%', '\%'), '_', '\_') || '.%' ESCAPE '\')))))
		          AND EXISTS (SELECT 1 FROM sync_group_member sm
		                       WHERE sm.group_id = mc.group_id AND sm.member_chain_id = ? AND sm.member_state = 'active'
		                         AND (sm.role = 'full-sync' OR sm.member_chain_id = mc.owner_chain_id
		                              OR (sm.role = 'selective-sync' AND EXISTS (
		                                  SELECT 1 FROM sync_group_member_domain c
		                                   WHERE c.group_id = mc.group_id AND c.member_chain_id = sm.member_chain_id
		                                     AND c.removed_revision = 0
			                                     AND (c.domain_tag = so.domain_tag OR so.domain_tag LIKE REPLACE(REPLACE(REPLACE(c.domain_tag, '\', '\\'), '%', '\%'), '_', '\_') || '.%' ESCAPE '\')))))
		   )
		   AND (so.origin_memory_id > ? OR (so.origin_memory_id = ? AND so.origin_chain_id > ?))
		 ORDER BY so.origin_memory_id ASC, so.origin_chain_id ASC
		 LIMIT ?`,
		groupID, requesterChainID, domain, likeEscapeSubtree(domain),
		groupID, responderChainID, requesterChainID, responderChainID,
		afterMem, afterMem, afterChain, limit)
	if err != nil {
		return nil, "", fmt.Errorf("list group relay candidates: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []GroupRelayCandidate
	var lastMem, lastChain string
	for rows.Next() {
		var c GroupRelayCandidate
		if err := rows.Scan(&c.OriginMemoryID, &c.OriginChainID, &c.LocalMemoryID); err != nil {
			return nil, "", fmt.Errorf("scan group relay candidate: %w", err)
		}
		out = append(out, c)
		lastMem, lastChain = c.OriginMemoryID, c.OriginChainID
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) == limit {
		next = encodeOriginCursor(lastMem, lastChain)
	}
	return out, next, nil
}

// GroupOwnedDomainsForPeer returns the active group domains THIS node (ownerChainID)
// OWNS that peerChainID is entitled to receive (peer is an active full-sync member of
// the same group). This is the SENDER-side group scope (docs §9.1 must-fix #3): a node
// only ORIGINATES star fan-out for domains it owns, so anti-entropy scan/enqueue must
// use this (NOT the symmetric GroupSharedDomains, which includes domains a full-sync
// member holds but does not own and would wrongly re-export).
func (s *SQLiteStore) GroupOwnedDomainsForPeer(ctx context.Context, ownerChainID, peerChainID string) ([]string, error) {
	if ownerChainID == "" || peerChainID == "" {
		return nil, nil
	}
	rows, err := s.conn.QueryContext(ctx, `
		SELECT DISTINCT gd.group_id, gd.domain_tag, peer.role
		  FROM sync_group_domain gd
		  JOIN sync_group_member peer ON peer.group_id = gd.group_id
		       AND peer.member_chain_id = ? AND peer.member_state = 'active'
		 WHERE gd.owner_chain_id = ? AND gd.removed_revision = 0
		 ORDER BY gd.group_id, gd.domain_tag`, peerChainID, ownerChainID)
	if err != nil {
		return nil, fmt.Errorf("group owned domains for peer: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type owned struct{ groupID, domain, role string }
	var candidates []owned
	for rows.Next() {
		var d owned
		if err := rows.Scan(&d.groupID, &d.domain, &d.role); err != nil {
			return nil, fmt.Errorf("scan group owned domain: %w", err)
		}
		candidates = append(candidates, d)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	var out []string
	for _, d := range candidates {
		if d.role == GroupRoleFullSync {
			out = append(out, d.domain)
			continue
		}
		if d.role != GroupRoleSelectiveSync {
			continue
		}
		consent, err := s.ListGroupMemberConsentDomains(ctx, d.groupID, peerChainID)
		if err != nil {
			return nil, err
		}
		for _, c := range consent {
			if c == d.domain || isDomainDescendant(c, d.domain) {
				out = append(out, c)
			}
		}
	}
	return normalizeConsentSubtree(out), nil
}

// GroupDomainRef is one (group, domain) pair both peers are entitled to.
type GroupDomainRef struct {
	GroupID   string
	DomainTag string
}

// GroupSharedDomainsWithGroup returns the (group_id, domain_tag) pairs that BOTH
// chains are entitled to as active members — the same set as GroupSharedDomains but
// carrying which GROUP each domain belongs to, so the reconciler can issue a
// group-scoped digest (SyncDigestRequest.GroupID set) per pair (docs §9.2 must-fix
// #8: without this the multi-node digest handler is never reached in production).
func (s *SQLiteStore) GroupSharedDomainsWithGroup(ctx context.Context, chainA, chainB string) ([]GroupDomainRef, error) {
	if chainA == "" || chainB == "" {
		return nil, nil
	}
	return s.groupSharedDomainRefs(ctx, chainA, chainB)
}

// groupSharedDomainRefs computes the exact intersection of each member's
// effective scopes.  Returning the group root when one side selected only a
// descendant would turn receiver consent into an ancestor wildcard and leak
// siblings, so the narrower overlapping scope always wins.
func (s *SQLiteStore) groupSharedDomainRefs(ctx context.Context, chainA, chainB string) ([]GroupDomainRef, error) {
	rows, err := s.conn.QueryContext(ctx, `
		SELECT DISTINCT ma.group_id
		  FROM sync_group_member ma
		  JOIN sync_group_member mb ON mb.group_id=ma.group_id
		 WHERE ma.member_chain_id=? AND mb.member_chain_id=?
		   AND ma.member_state='active' AND mb.member_state='active'
		 ORDER BY ma.group_id`, chainA, chainB)
	if err != nil {
		return nil, fmt.Errorf("group shared domains with group: %w", err)
	}
	var groups []string
	for rows.Next() {
		var groupID string
		if err := rows.Scan(&groupID); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan shared group: %w", err)
		}
		groups = append(groups, groupID)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	var out []GroupDomainRef
	for _, groupID := range groups {
		a, err := s.GroupDomainsForMember(ctx, groupID, chainA)
		if err != nil {
			return nil, err
		}
		b, err := s.GroupDomainsForMember(ctx, groupID, chainB)
		if err != nil {
			return nil, err
		}
		var overlap []string
		for _, da := range a {
			for _, db := range b {
				switch {
				case da == db || isDomainDescendant(da, db):
					overlap = append(overlap, da)
				case isDomainDescendant(db, da):
					overlap = append(overlap, db)
				}
			}
		}
		seen := map[string]struct{}{}
		for _, domain := range overlap {
			if _, dup := seen[domain]; dup {
				continue
			}
			seen[domain] = struct{}{}
			out = append(out, GroupDomainRef{GroupID: groupID, DomainTag: domain})
		}
	}
	return out, nil
}

// origin cursor codec — the composite (origin_memory_id, origin_chain_id) page
// key for ListGroupServableOriginIDs, packed into the single wire cursor string.
const originCursorSep = "\x1f"

func encodeOriginCursor(mem, chain string) string { return mem + originCursorSep + chain }

func decodeOriginCursor(cur string) (mem, chain string) {
	if cur == "" {
		return "", ""
	}
	if i := indexByte(cur, originCursorSep[0]); i >= 0 {
		return cur[:i], cur[i+1:]
	}
	// A legacy (pairwise) cursor is a bare origin_memory_id.
	return cur, ""
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// SelfResyncingSharedWithPeer reports whether THIS node (localChainID) is in the
// 'resyncing' rebuild state in any group where peerChainID is also present — the
// watermark that excludes tombstoned items / backfill until the journal and the
// local_suppress set are rebuilt (docs §9.2/§10 must-fix #3). Gates digest
// REQUESTS so backfill can't resurrect a suppressed memory mid-rebuild.
func (s *SQLiteStore) SelfResyncingSharedWithPeer(ctx context.Context, localChainID, peerChainID string) (bool, error) {
	if localChainID == "" || peerChainID == "" {
		return false, nil
	}
	var n int
	if err := s.conn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		  FROM sync_group_member self
		  JOIN sync_group_member peer ON peer.group_id = self.group_id
		 WHERE self.member_chain_id = ? AND self.member_state = 'resyncing'
		   AND peer.member_chain_id = ? AND peer.member_state IN ('active','resyncing')`,
		localChainID, peerChainID).Scan(&n); err != nil {
		return false, fmt.Errorf("self resyncing shared with peer: %w", err)
	}
	return n > 0, nil
}

// ---- group-scoped removal purge (build step 7, docs §10 rows 2-3, I10) ----

// PurgeGroupSyncPeerState stops GROUP sync (outbox/backfill) toward a chain that
// was removed from — or voluntarily left — a sync group, WITHOUT disturbing an
// INDEPENDENT PAIRWISE relationship the group removal has no authority over
// (docs §10 row 2, invariant I10: "cross_fed kept unless separately tx-34
// revoked"). It deletes ONLY sync_outbox rows toward remoteChainID whose synced
// memory's domain is NOT covered — subtree-aware — by any surviving pairwise
// sync_domains consent row for that same peer. It touches NEITHER sync_domains
// (the pairwise consent table), NOR cross_fed, NOR sync_control.
//
// CONTRAST with PurgeSyncPeerState (sync_tables.go), which DELETEs sync_domains
// + sync_outbox + sync_control for the peer: that primitive is for a PAIRWISE
// revoke and MUST NEVER be used for a group removal — it would wipe an
// independent pairwise cross_fed / sync_domains relationship (I10). Here a
// group-only peer (no pairwise sync_domains rows) has ALL of its outbox purged,
// while a peer that also holds an independent pairwise relationship keeps exactly
// its pairwise-covered NATIVE outbox rows. The drain-time consent gate
// (sync_outbox.go: DomainAllowed(effectiveConsent, ...) where effectiveConsent =
// pairwise ∪ GroupSharedDomains, PLUS a ResolveGroupRelay re-check for relayed rows)
// is the authoritative send-time backstop that terminally rejects any straggler; this
// purge is hygiene + an explicit backfill-stop on top.
//
// RELAYED rows (origin_chain_id != ”) are ALWAYS purged on a group removal (docs §10
// R1): a pairwise relationship authorizes this node to originate its OWN memories to the
// peer, but NEVER to relay a THIRD member's group copy — so a relayed row must not be
// spared just because a pairwise domain coincidentally covers its tag (that was a
// cross-group leak to an ex-member). Only NATIVE rows may be spared by pairwise coverage.
// (A relayed row still owed to the peer via ANOTHER surviving group re-enqueues on the
// next reconcile via ListGroupRelayCandidates — transient churn, not data loss.)
func (s *SQLiteStore) PurgeGroupSyncPeerState(ctx context.Context, remoteChainID string) error {
	if remoteChainID == "" {
		return fmt.Errorf("remote_chain_id is required")
	}
	unlock := s.LockSyncPolicyWrite()
	defer unlock()
	return s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx, ok := txStore.(*SQLiteStore)
		if !ok {
			return fmt.Errorf("group sync purge requires the SQLite store backend")
		}
		// A row survives ONLY if it is NATIVE (origin_chain_id = '') AND a pairwise
		// sync_domains row for this peer covers the outbox memory's domain (exact or
		// ancestor, subtree-aware via the same LIKE-escape idiom as likeEscapeSubtree).
		// Everything else — every relayed row, and any native row no pairwise consent
		// covers — is purged.
		if _, err := tx.writeExecContext(ctx, `
			DELETE FROM sync_outbox
			 WHERE remote_chain_id = ?
			   AND NOT (
			       sync_outbox.origin_chain_id = ''
			       AND EXISTS (
			           SELECT 1
			             FROM memories m
			             JOIN sync_domains sd ON sd.remote_chain_id = sync_outbox.remote_chain_id
			            WHERE m.memory_id = sync_outbox.memory_id
			              AND (sd.domain_tag = m.domain_tag
			                   OR m.domain_tag LIKE REPLACE(REPLACE(REPLACE(sd.domain_tag, '\', '\\'), '%', '\%'), '_', '\_') || '.%' ESCAPE '\')
			       )
			   )`, remoteChainID); err != nil {
			return fmt.Errorf("purge group sync outbox: %w", err)
		}
		return nil
	})
}

// ---- sync_control binding ----

// SetSyncControlGroupID binds an existing pairwise sync_control row to a group.
func (s *SQLiteStore) SetSyncControlGroupID(ctx context.Context, remoteChainID, groupID string) error {
	if remoteChainID == "" {
		return fmt.Errorf("remote_chain_id is required")
	}
	_, err := s.writeExecContext(ctx,
		`UPDATE sync_control SET group_id=? WHERE remote_chain_id=?`, groupID, remoteChainID)
	if err != nil {
		return fmt.Errorf("set sync control group id: %w", err)
	}
	return nil
}
