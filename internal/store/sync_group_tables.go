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
	"time"
)

// Group member roles — the enrollment ceremony's three-way choice
// (docs §6.1): Everything / Just some topics / Not yet.
const (
	GroupRoleFullSync      = "full-sync"        // all SHARED group domains (never the operator's unselected ones)
	GroupRoleSelectiveSync = "selective-sync"   // an explicit subset of the shared set
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

	TombstoneEnforceAdvisory     = "advisory"      // signal to peers, never a delete command
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
	GroupID        string
	DomainTag      string
	OwnerChainID   string
	OwnerSig       string // ed25519 by the owner's domain-owning agent over the domain_add payload (docs §7)
	MaxClearance   int    // DISPLAY-ONLY; never an admission input (docs §9.2)
	AddedRevision  int64
	RemovedRevision int64 // 0 = active
}

// SyncGroupLogEntry is one signed entry in a partitioned audit sub-chain.
// The federation layer computes PrevHash/EntryHash/AuthorSig over a canonical
// payload (docs §5.3); the store persists what it is given.
type SyncGroupLogEntry struct {
	GroupID           string
	Subchain          string // 'roster' or 'domain:<tag>'
	Seq               int64
	PrevHash          string
	EntryHash         string
	EntryType         string
	PayloadJSON       string // canonical JSON; NEVER memory content
	AuthorChainID     string
	AuthorAgentPubkey string
	AuthorSig         string
	CreatedAt         time.Time
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
		created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		PRIMARY KEY (group_id, subchain, seq)
	)`)
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
			 author_chain_id, author_agent_pubkey, author_sig)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.GroupID, e.Subchain, e.Seq, e.PrevHash, e.EntryHash, e.EntryType, e.PayloadJSON,
		e.AuthorChainID, e.AuthorAgentPubkey, e.AuthorSig)
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
		       author_chain_id, author_agent_pubkey, author_sig, created_at
		  FROM sync_group_log WHERE group_id=? AND subchain=? AND seq=?`, groupID, subchain, seq).
		Scan(&e.GroupID, &e.Subchain, &e.Seq, &e.PrevHash, &e.EntryHash, &e.EntryType, &e.PayloadJSON,
			&e.AuthorChainID, &e.AuthorAgentPubkey, &e.AuthorSig, &createdAt)
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
		       author_chain_id, author_agent_pubkey, author_sig, created_at
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
			&e.PayloadJSON, &e.AuthorChainID, &e.AuthorAgentPubkey, &e.AuthorSig, &createdAt); err != nil {
			return nil, fmt.Errorf("scan sync group log: %w", err)
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
