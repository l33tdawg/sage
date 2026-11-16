package store

// Durable introduced-link records (RFC #443 Phase B, design §3).
//
// An org-introduced federation link is activated through a durable state
// machine, because this node-local SQLite row and the tx-33 broadcast are
// separate commit domains:
//
//	intent → signed → committed → active (host)
//	                            → bound_pending_peer → active (guest)
//
// with the side exits aborted(reason), revoked(reason) and paused(reason), and
// active → rekey_required / paused / revoked. Every transition is a
// compare-and-set on the expected state (and, once signed, on the exact tx
// hash), recorded in an append-only history in the same transaction.
//
// A partial unique index allows at most one in-flight row (intent, signed,
// committed, bound_pending_peer) per remote chain, which is what rules out a
// second agreement generation being built concurrently.
//
// Like org_policy_state this lives in the node's own SQLite database, never in
// Badger: it is written outside consensus and must not feed the AppHash.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Introduced-link states.
const (
	IntroducedLinkIntent           = "intent"
	IntroducedLinkSigned           = "signed"
	IntroducedLinkCommitted        = "committed"
	IntroducedLinkBoundPendingPeer = "bound_pending_peer"
	IntroducedLinkActive           = "active"
	IntroducedLinkRekeyRequired    = "rekey_required"
	IntroducedLinkPaused           = "paused"
	IntroducedLinkAborted          = "aborted"
	IntroducedLinkRevoked          = "revoked"
)

// Introduced-link kinds.
const (
	IntroducedLinkKindNew   = "new"
	IntroducedLinkKindRekey = "rekey"
)

const (
	maxIntroducedLinkReasonBytes = 512
	// MaxIntroducedLinkBlobBytes bounds each stored blob (E_intro, terms,
	// evidence, tx bytes). The intro routes cap their wire objects well below
	// this; the bound only keeps one bad caller from bloating the table.
	MaxIntroducedLinkBlobBytes = 1 << 20
	maxIntroducedLinkList      = 4096
)

// introducedLinkTransitions is the complete allowed-transition table.
var introducedLinkTransitions = map[string]map[string]bool{
	IntroducedLinkIntent:           {IntroducedLinkSigned: true, IntroducedLinkAborted: true},
	IntroducedLinkSigned:           {IntroducedLinkCommitted: true, IntroducedLinkAborted: true, IntroducedLinkPaused: true},
	IntroducedLinkCommitted:        {IntroducedLinkActive: true, IntroducedLinkBoundPendingPeer: true, IntroducedLinkRevoked: true},
	IntroducedLinkBoundPendingPeer: {IntroducedLinkActive: true, IntroducedLinkRevoked: true},
	IntroducedLinkActive:           {IntroducedLinkRekeyRequired: true, IntroducedLinkPaused: true, IntroducedLinkRevoked: true},
	IntroducedLinkRekeyRequired:    {IntroducedLinkActive: true, IntroducedLinkPaused: true, IntroducedLinkRevoked: true},
	IntroducedLinkPaused:           {IntroducedLinkActive: true, IntroducedLinkRevoked: true},
}

// IntroducedLinkInFlight reports whether state is between intent and a settled
// outcome.
func IntroducedLinkInFlight(state string) bool {
	switch state {
	case IntroducedLinkIntent, IntroducedLinkSigned, IntroducedLinkCommitted, IntroducedLinkBoundPendingPeer:
		return true
	}
	return false
}

// ErrIntroducedLinkInFlight means the remote chain already has an in-flight
// introduced-link row.
var ErrIntroducedLinkInFlight = errors.New("introduced link: another link for this remote chain is in flight")

// ErrIntroducedLinkInvalid wraps a malformed record or transition.
var ErrIntroducedLinkInvalid = errors.New("introduced link: invalid")

// ErrIntroducedLinkNotFound means no row has the link id.
var ErrIntroducedLinkNotFound = errors.New("introduced link: not found")

// IntroducedLink is one row.
type IntroducedLink struct {
	LinkID        string `json:"link_id"`
	OrgID         string `json:"org_id"`
	RemoteChainID string `json:"remote_chain_id"`
	PeerAgentPub  string `json:"peer_agent_pub"`
	OfferID       string `json:"offer_id"`
	SessionID     string `json:"session_id"`
	Kind          string `json:"kind"`
	Role          string `json:"role"` // host | guest
	State         string `json:"state"`
	StateReason   string `json:"state_reason,omitempty"`
	EIntro        []byte `json:"e_intro"`
	HostAcceptSig []byte `json:"host_accept_sig"`
	GuestAccept   []byte `json:"guest_accept_sig"`
	GuestAckSig   []byte `json:"guest_ack_sig"`
	// Evidence is the canonical JSON of the policy/roster/card/envelope
	// revisions and digests on both sides.
	Evidence []byte `json:"evidence"`
	// Terms is the encoded CrossFedTerms this link commits.
	Terms []byte `json:"terms"`
	// PriorTermsDigest is the hex sha256 of the chain terms for the remote
	// when the intent was written, or "absent".
	PriorTermsDigest string `json:"prior_terms_digest"`
	TxBytes          []byte `json:"tx_bytes,omitempty"`
	TxHash           string `json:"tx_hash,omitempty"`
	TxNonce          uint64 `json:"tx_nonce,omitempty"`
	StagedRef        string `json:"staged_ref"`
	CreatedAt        int64  `json:"created_at"`
	Deadline         int64  `json:"deadline"`
	UpdatedAt        int64  `json:"updated_at"`
}

// IntroducedLinkHistory is one history entry.
type IntroducedLinkHistory struct {
	LinkID string `json:"link_id"`
	At     int64  `json:"at"`
	From   string `json:"from"`
	To     string `json:"to"`
	Reason string `json:"reason,omitempty"`
}

var (
	introLinkIDPattern    = regexp.MustCompile(`^[0-9a-f]{64}$`)
	introChainIDPattern   = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)
	introPriorTermsDigest = regexp.MustCompile(`^(absent|[0-9a-f]{64})$`)
)

func (l *IntroducedLink) validateIntent() error {
	switch {
	case !introLinkIDPattern.MatchString(l.LinkID):
		return errors.New("link_id must be 64 lowercase hex")
	case strings.TrimSpace(l.OrgID) == "":
		return errors.New("org_id is required")
	case !introChainIDPattern.MatchString(l.RemoteChainID):
		return errors.New("remote_chain_id is not a valid chain id")
	case !introLinkIDPattern.MatchString(l.PeerAgentPub):
		return errors.New("peer_agent_pub must be 64 lowercase hex")
	case !introLinkIDPattern.MatchString(l.OfferID):
		return errors.New("offer_id must be 64 lowercase hex")
	case strings.TrimSpace(l.SessionID) == "":
		return errors.New("session_id is required")
	case l.Kind != IntroducedLinkKindNew && l.Kind != IntroducedLinkKindRekey:
		return errors.New("kind must be new or rekey")
	case l.Role != "host" && l.Role != "guest":
		return errors.New("role must be host or guest")
	case len(l.EIntro) == 0, len(l.HostAcceptSig) == 0, len(l.GuestAccept) == 0, len(l.GuestAckSig) == 0:
		return errors.New("e_intro and all three acceptance signatures are required at intent")
	case len(l.Evidence) == 0 || len(l.Terms) == 0:
		return errors.New("evidence and terms are required at intent")
	case !introPriorTermsDigest.MatchString(l.PriorTermsDigest):
		return errors.New("prior_terms_digest must be 64 lowercase hex or absent")
	case len(l.TxBytes) != 0 || l.TxHash != "" || l.TxNonce != 0:
		return errors.New("tx bytes are only recorded at signed")
	case l.CreatedAt <= 0 || l.Deadline <= l.CreatedAt:
		return errors.New("deadline must follow a positive created_at")
	}
	for _, b := range [][]byte{l.EIntro, l.HostAcceptSig, l.GuestAccept, l.GuestAckSig, l.Evidence, l.Terms} {
		if len(b) > MaxIntroducedLinkBlobBytes {
			return fmt.Errorf("a stored blob exceeds %d bytes", MaxIntroducedLinkBlobBytes)
		}
	}
	return nil
}

func (s *SQLiteStore) migrateIntroducedLinks(ctx context.Context) error {
	stmts := []string{`
	CREATE TABLE IF NOT EXISTS introduced_link (
		seq                INTEGER PRIMARY KEY AUTOINCREMENT,
		link_id            TEXT NOT NULL UNIQUE,
		org_id             TEXT NOT NULL,
		remote_chain_id    TEXT NOT NULL,
		peer_agent_pub     TEXT NOT NULL,
		offer_id           TEXT NOT NULL,
		session_id         TEXT NOT NULL,
		kind               TEXT NOT NULL CHECK (kind IN ('new','rekey')),
		role               TEXT NOT NULL CHECK (role IN ('host','guest')),
		state              TEXT NOT NULL CHECK (state IN ('intent','signed','committed','bound_pending_peer','active','rekey_required','paused','aborted','revoked')),
		state_reason       TEXT NOT NULL DEFAULT '',
		e_intro            BLOB NOT NULL,
		host_accept_sig    BLOB NOT NULL,
		guest_accept_sig   BLOB NOT NULL,
		guest_ack_sig      BLOB NOT NULL,
		evidence           BLOB NOT NULL,
		terms              BLOB NOT NULL,
		prior_terms_digest TEXT NOT NULL,
		tx_bytes           BLOB,
		tx_hash            TEXT,
		tx_nonce           INTEGER,
		staged_ref         TEXT NOT NULL DEFAULT '',
		created_at         INTEGER NOT NULL,
		deadline           INTEGER NOT NULL,
		updated_at         INTEGER NOT NULL
	)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS introduced_link_one_in_flight
		ON introduced_link(remote_chain_id)
		WHERE state IN ('intent','signed','committed','bound_pending_peer')`,
		`CREATE INDEX IF NOT EXISTS introduced_link_by_chain ON introduced_link(remote_chain_id, seq)`,
		`CREATE TABLE IF NOT EXISTS introduced_link_history (
		id        INTEGER PRIMARY KEY AUTOINCREMENT,
		link_id   TEXT NOT NULL,
		at        INTEGER NOT NULL,
		from_state TEXT NOT NULL,
		to_state  TEXT NOT NULL,
		reason    TEXT NOT NULL DEFAULT ''
	)`,
		`CREATE INDEX IF NOT EXISTS introduced_link_history_by_link ON introduced_link_history(link_id, id)`,
	}
	for _, q := range stmts {
		if _, err := s.writeExecContext(ctx, q); err != nil {
			return fmt.Errorf("create introduced link tables: %w", err)
		}
	}
	return nil
}

// InsertIntroducedLinkIntent writes a new row in state intent and its first
// history entry in one transaction. It refuses (ErrIntroducedLinkInFlight)
// when the remote chain already has an in-flight row.
func (s *SQLiteStore) InsertIntroducedLinkIntent(ctx context.Context, l *IntroducedLink) error {
	if err := l.validateIntent(); err != nil {
		return fmt.Errorf("%w: %v", ErrIntroducedLinkInvalid, err)
	}
	tx, unlock, err := s.beginTxLocked(ctx)
	if err != nil {
		return fmt.Errorf("begin introduced link insert: %w", err)
	}
	defer unlock()
	var inFlight int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM introduced_link WHERE remote_chain_id = ?
		AND state IN ('intent','signed','committed','bound_pending_peer')`, l.RemoteChainID).Scan(&inFlight); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("check in-flight introduced link: %w", err)
	}
	if inFlight > 0 {
		_ = tx.Rollback()
		return ErrIntroducedLinkInFlight
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO introduced_link (
		link_id, org_id, remote_chain_id, peer_agent_pub, offer_id, session_id, kind, role,
		state, state_reason, e_intro, host_accept_sig, guest_accept_sig, guest_ack_sig,
		evidence, terms, prior_terms_digest, staged_ref, created_at, deadline, updated_at
	) VALUES (?,?,?,?,?,?,?,?,?,'',?,?,?,?,?,?,?,?,?,?,?)`,
		l.LinkID, l.OrgID, l.RemoteChainID, l.PeerAgentPub, l.OfferID, l.SessionID, l.Kind, l.Role,
		IntroducedLinkIntent, l.EIntro, l.HostAcceptSig, l.GuestAccept, l.GuestAckSig,
		l.Evidence, l.Terms, l.PriorTermsDigest, l.StagedRef, l.CreatedAt, l.Deadline, l.CreatedAt,
	); err != nil {
		_ = tx.Rollback()
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			if strings.Contains(err.Error(), "link_id") {
				return fmt.Errorf("%w: link_id already exists", ErrIntroducedLinkInvalid)
			}
			return ErrIntroducedLinkInFlight
		}
		return fmt.Errorf("insert introduced link: %w", err)
	}
	if err := insertIntroducedLinkHistory(ctx, tx, l.LinkID, l.CreatedAt, "", IntroducedLinkIntent, ""); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit introduced link insert: %w", err)
	}
	return nil
}

func insertIntroducedLinkHistory(ctx context.Context, tx *sql.Tx, linkID string, at int64, from, to, reason string) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO introduced_link_history (link_id, at, from_state, to_state, reason)
		VALUES (?,?,?,?,?)`, linkID, at, from, to, reason); err != nil {
		return fmt.Errorf("insert introduced link history: %w", err)
	}
	return nil
}

// IntroducedLinkTransition is one compare-and-set state change.
type IntroducedLinkTransition struct {
	LinkID string
	From   string
	To     string
	Reason string
	At     int64
	// ExpectTxHash must equal the stored tx_hash for any transition out of
	// signed or a later state; it keeps a late caller from moving a row whose
	// bytes it did not sign.
	ExpectTxHash string
	// TxBytes / TxNonce are recorded on intent → signed only. The stored
	// tx_hash is the hex sha256 of TxBytes.
	TxBytes []byte
	TxNonce uint64
}

// TransitionIntroducedLink applies t if the row is still in t.From (and, once
// signed, still carries t.ExpectTxHash). It returns false, with nothing
// written, when the row moved on. Disallowed transitions, and terminal or
// paused states without a reason, are refused as invalid.
func (s *SQLiteStore) TransitionIntroducedLink(ctx context.Context, t IntroducedLinkTransition) (bool, error) {
	if !introducedLinkTransitions[t.From][t.To] {
		return false, fmt.Errorf("%w: transition %s → %s is not allowed", ErrIntroducedLinkInvalid, t.From, t.To)
	}
	switch t.To {
	case IntroducedLinkAborted, IntroducedLinkRevoked, IntroducedLinkPaused:
		if strings.TrimSpace(t.Reason) == "" {
			return false, fmt.Errorf("%w: %s needs a reason", ErrIntroducedLinkInvalid, t.To)
		}
	}
	if len(t.Reason) > maxIntroducedLinkReasonBytes {
		return false, fmt.Errorf("%w: reason exceeds %d bytes", ErrIntroducedLinkInvalid, maxIntroducedLinkReasonBytes)
	}
	if t.At <= 0 {
		return false, fmt.Errorf("%w: transition time is required", ErrIntroducedLinkInvalid)
	}
	signing := t.From == IntroducedLinkIntent && t.To == IntroducedLinkSigned
	if signing {
		if len(t.TxBytes) == 0 || len(t.TxBytes) > MaxIntroducedLinkBlobBytes {
			return false, fmt.Errorf("%w: signed needs the exact tx bytes", ErrIntroducedLinkInvalid)
		}
	} else if len(t.TxBytes) != 0 || t.TxNonce != 0 {
		return false, fmt.Errorf("%w: tx bytes are only recorded on intent → signed", ErrIntroducedLinkInvalid)
	}
	if t.From != IntroducedLinkIntent && !introLinkIDPattern.MatchString(t.ExpectTxHash) {
		return false, fmt.Errorf("%w: a transition out of %s must name the stored tx hash", ErrIntroducedLinkInvalid, t.From)
	}

	tx, unlock, err := s.beginTxLocked(ctx)
	if err != nil {
		return false, fmt.Errorf("begin introduced link transition: %w", err)
	}
	defer unlock()
	var res sql.Result
	switch {
	case signing:
		sum := sha256.Sum256(t.TxBytes)
		res, err = tx.ExecContext(ctx, `UPDATE introduced_link
			SET state = ?, state_reason = ?, tx_bytes = ?, tx_hash = ?, tx_nonce = ?, updated_at = ?
			WHERE link_id = ? AND state = ? AND tx_hash IS NULL`,
			t.To, t.Reason, t.TxBytes, hex.EncodeToString(sum[:]), int64(t.TxNonce), t.At, // #nosec G115 -- nonces fit int64
			t.LinkID, t.From)
	case t.From == IntroducedLinkIntent:
		res, err = tx.ExecContext(ctx, `UPDATE introduced_link SET state = ?, state_reason = ?, updated_at = ?
			WHERE link_id = ? AND state = ? AND tx_hash IS NULL`,
			t.To, t.Reason, t.At, t.LinkID, t.From)
	default:
		res, err = tx.ExecContext(ctx, `UPDATE introduced_link SET state = ?, state_reason = ?, updated_at = ?
			WHERE link_id = ? AND state = ? AND tx_hash = ?`,
			t.To, t.Reason, t.At, t.LinkID, t.From, t.ExpectTxHash)
	}
	if err != nil {
		_ = tx.Rollback()
		return false, fmt.Errorf("update introduced link: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		_ = tx.Rollback()
		return false, fmt.Errorf("update introduced link: %w", err)
	}
	if n != 1 {
		_ = tx.Rollback()
		return false, nil
	}
	if err := insertIntroducedLinkHistory(ctx, tx, t.LinkID, t.At, t.From, t.To, t.Reason); err != nil {
		_ = tx.Rollback()
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit introduced link transition: %w", err)
	}
	return true, nil
}

const introducedLinkColumns = `link_id, org_id, remote_chain_id, peer_agent_pub, offer_id, session_id, kind, role,
	state, state_reason, e_intro, host_accept_sig, guest_accept_sig, guest_ack_sig, evidence, terms,
	prior_terms_digest, tx_bytes, tx_hash, tx_nonce, staged_ref, created_at, deadline, updated_at`

type introducedLinkScanner interface{ Scan(dest ...any) error }

func scanIntroducedLink(r introducedLinkScanner) (*IntroducedLink, error) {
	var l IntroducedLink
	var txHash sql.NullString
	var txNonce sql.NullInt64
	if err := r.Scan(&l.LinkID, &l.OrgID, &l.RemoteChainID, &l.PeerAgentPub, &l.OfferID, &l.SessionID,
		&l.Kind, &l.Role, &l.State, &l.StateReason, &l.EIntro, &l.HostAcceptSig, &l.GuestAccept,
		&l.GuestAckSig, &l.Evidence, &l.Terms, &l.PriorTermsDigest, &l.TxBytes, &txHash, &txNonce,
		&l.StagedRef, &l.CreatedAt, &l.Deadline, &l.UpdatedAt); err != nil {
		return nil, err //nolint:wrapcheck // callers wrap
	}
	l.TxHash = txHash.String
	if txNonce.Valid {
		l.TxNonce = uint64(txNonce.Int64) // #nosec G115 -- stored from a uint64
	}
	return &l, nil
}

// GetIntroducedLink returns one row by link id.
func (s *SQLiteStore) GetIntroducedLink(ctx context.Context, linkID string) (*IntroducedLink, error) {
	l, err := scanIntroducedLink(s.conn.QueryRowContext(ctx,
		`SELECT `+introducedLinkColumns+` FROM introduced_link WHERE link_id = ?`, linkID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrIntroducedLinkNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get introduced link: %w", err)
	}
	return l, nil
}

// LatestIntroducedLinkByChain returns the newest row for a remote chain, or
// (nil, nil) when the chain has none (a manual or no agreement).
func (s *SQLiteStore) LatestIntroducedLinkByChain(ctx context.Context, remoteChainID string) (*IntroducedLink, error) {
	l, err := scanIntroducedLink(s.conn.QueryRowContext(ctx,
		`SELECT `+introducedLinkColumns+` FROM introduced_link WHERE remote_chain_id = ? ORDER BY seq DESC LIMIT 1`,
		remoteChainID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("latest introduced link: %w", err)
	}
	return l, nil
}

// LatestIntroducedLinkStates maps every remote chain that has at least one
// row to the state of its newest row.
func (s *SQLiteStore) LatestIntroducedLinkStates(ctx context.Context) (map[string]string, error) {
	rows, err := s.conn.QueryContext(ctx, `SELECT l.remote_chain_id, l.state FROM introduced_link l
		JOIN (SELECT remote_chain_id, MAX(seq) AS seq FROM introduced_link GROUP BY remote_chain_id) m
		ON l.seq = m.seq`)
	if err != nil {
		return nil, fmt.Errorf("list introduced link states: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]string{}
	for rows.Next() {
		var chain, state string
		if err := rows.Scan(&chain, &state); err != nil {
			return nil, fmt.Errorf("scan introduced link state: %w", err)
		}
		out[chain] = state
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list introduced link states: %w", err)
	}
	return out, nil
}

// ListIntroducedLinks returns rows newest first, optionally for one chain. It
// fails rather than truncating past the list bound.
func (s *SQLiteStore) ListIntroducedLinks(ctx context.Context, remoteChainID string) ([]IntroducedLink, error) {
	q := `SELECT ` + introducedLinkColumns + ` FROM introduced_link`
	args := []any{}
	if remoteChainID != "" {
		q += ` WHERE remote_chain_id = ?`
		args = append(args, remoteChainID)
	}
	q += ` ORDER BY seq DESC LIMIT ?`
	args = append(args, maxIntroducedLinkList+1)
	rows, err := s.conn.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list introduced links: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []IntroducedLink
	for rows.Next() {
		l, err := scanIntroducedLink(rows)
		if err != nil {
			return nil, fmt.Errorf("scan introduced link: %w", err)
		}
		out = append(out, *l)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list introduced links: %w", err)
	}
	if len(out) > maxIntroducedLinkList {
		return nil, fmt.Errorf("list introduced links: more than %d rows", maxIntroducedLinkList)
	}
	return out, nil
}

// ListIntroducedLinkHistory returns a row's history, oldest first.
func (s *SQLiteStore) ListIntroducedLinkHistory(ctx context.Context, linkID string) ([]IntroducedLinkHistory, error) {
	rows, err := s.conn.QueryContext(ctx, `SELECT link_id, at, from_state, to_state, reason
		FROM introduced_link_history WHERE link_id = ? ORDER BY id LIMIT ?`, linkID, maxIntroducedLinkList+1)
	if err != nil {
		return nil, fmt.Errorf("list introduced link history: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []IntroducedLinkHistory
	for rows.Next() {
		var h IntroducedLinkHistory
		if err := rows.Scan(&h.LinkID, &h.At, &h.From, &h.To, &h.Reason); err != nil {
			return nil, fmt.Errorf("scan introduced link history: %w", err)
		}
		out = append(out, h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list introduced link history: %w", err)
	}
	if len(out) > maxIntroducedLinkList {
		return nil, fmt.Errorf("list introduced link history: more than %d entries", maxIntroducedLinkList)
	}
	return out, nil
}
