package store

// RFC #443 Phase B2c additions to the node-local introduced-link state:
//
//   - introduced_link_card: the peer card revision + digest a link last
//     followed. It starts as the card in E_intro; an endpoint- or route-only
//     card change applied without a tap moves it on (with a history entry),
//     and the paused → active auto-resume rule compares against it.
//   - introduced_link_refusal: stale-intent gate refusals. B2a refused before
//     any row existed, so nothing recorded them.
//   - RekeySyncControlPin: a rekey keeps the active sync-control binding (and
//     its delivered policy) and only moves the pinned remote CA.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// maxIntroducedLinkRefusals bounds the refusal log; older entries are pruned.
const maxIntroducedLinkRefusals = 1024

// IntroducedLinkCard is the peer card a link last followed.
type IntroducedLinkCard struct {
	LinkID     string `json:"link_id"`
	CardRev    uint64 `json:"card_rev"`
	CardDigest string `json:"card_digest"`
	UpdatedAt  int64  `json:"updated_at"`
}

// GetIntroducedLinkCard returns the followed card for a link, or (nil, nil)
// when none was recorded (the card in E_intro still applies).
func (s *SQLiteStore) GetIntroducedLinkCard(ctx context.Context, linkID string) (*IntroducedLinkCard, error) {
	var c IntroducedLinkCard
	var rev int64
	err := s.conn.QueryRowContext(ctx, `SELECT link_id, card_rev, card_digest, updated_at
		FROM introduced_link_card WHERE link_id = ?`, linkID).Scan(&c.LinkID, &rev, &c.CardDigest, &c.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get introduced link card: %w", err)
	}
	c.CardRev = uint64(rev) // #nosec G115 -- stored from a uint64
	return &c, nil
}

// RecordIntroducedLinkCard records that the link now follows card rev/digest,
// with a history entry, if the row is still in expectState. It returns false,
// with nothing written, when the row moved on.
func (s *SQLiteStore) RecordIntroducedLinkCard(ctx context.Context, linkID, expectState string, cardRev uint64, cardDigest, reason string, at int64) (bool, error) {
	switch {
	case !introLinkIDPattern.MatchString(linkID):
		return false, fmt.Errorf("%w: link_id must be 64 lowercase hex", ErrIntroducedLinkInvalid)
	case !introLinkIDPattern.MatchString(cardDigest):
		return false, fmt.Errorf("%w: card digest must be 64 lowercase hex", ErrIntroducedLinkInvalid)
	case strings.TrimSpace(reason) == "" || len(reason) > maxIntroducedLinkReasonBytes:
		return false, fmt.Errorf("%w: a reason of at most %d bytes is required", ErrIntroducedLinkInvalid, maxIntroducedLinkReasonBytes)
	}
	tx, unlock, err := s.beginTxLocked(ctx)
	if err != nil {
		return false, fmt.Errorf("begin introduced link card: %w", err)
	}
	defer unlock()
	var state string
	err = tx.QueryRowContext(ctx, `SELECT state FROM introduced_link WHERE link_id = ?`, linkID).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		return false, ErrIntroducedLinkNotFound
	}
	if err != nil {
		_ = tx.Rollback()
		return false, fmt.Errorf("read introduced link: %w", err)
	}
	if state != expectState {
		_ = tx.Rollback()
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO introduced_link_card (link_id, card_rev, card_digest, updated_at)
		VALUES (?,?,?,?) ON CONFLICT(link_id) DO UPDATE SET
		card_rev=excluded.card_rev, card_digest=excluded.card_digest, updated_at=excluded.updated_at`,
		linkID, int64(cardRev), cardDigest, at); err != nil { // #nosec G115 -- card revisions fit in int64
		_ = tx.Rollback()
		return false, fmt.Errorf("record introduced link card: %w", err)
	}
	if err := insertIntroducedLinkHistory(ctx, tx, linkID, at, state, state, reason); err != nil {
		_ = tx.Rollback()
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE introduced_link SET updated_at = ? WHERE link_id = ?`, at, linkID); err != nil {
		_ = tx.Rollback()
		return false, fmt.Errorf("touch introduced link: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit introduced link card: %w", err)
	}
	return true, nil
}

// IntroducedLinkRefusal is one stale-intent gate refusal: an accepted session
// that did not activate because something E_intro binds had moved.
type IntroducedLinkRefusal struct {
	ID            int64  `json:"id"`
	At            int64  `json:"at"`
	OrgID         string `json:"org_id"`
	OfferID       string `json:"offer_id"`
	RemoteChainID string `json:"remote_chain_id"`
	Kind          string `json:"kind"`
	Role          string `json:"role"`
	Reason        string `json:"reason"`
}

// InsertIntroducedLinkRefusal appends a refusal and prunes the log to its
// bound.
func (s *SQLiteStore) InsertIntroducedLinkRefusal(ctx context.Context, r IntroducedLinkRefusal) error {
	reason := r.Reason
	if len(reason) > maxIntroducedLinkReasonBytes {
		reason = reason[:maxIntroducedLinkReasonBytes]
	}
	switch {
	case r.At <= 0:
		return fmt.Errorf("%w: refusal time is required", ErrIntroducedLinkInvalid)
	case !introChainIDPattern.MatchString(r.RemoteChainID):
		return fmt.Errorf("%w: remote_chain_id is not a valid chain id", ErrIntroducedLinkInvalid)
	case strings.TrimSpace(reason) == "":
		return fmt.Errorf("%w: a refusal reason is required", ErrIntroducedLinkInvalid)
	case len(r.OrgID) > maxIntroducedLinkReasonBytes || len(r.OfferID) > 64 || len(r.Kind) > 16 || len(r.Role) > 16:
		return fmt.Errorf("%w: refusal field too long", ErrIntroducedLinkInvalid)
	}
	tx, unlock, err := s.beginTxLocked(ctx)
	if err != nil {
		return fmt.Errorf("begin introduced link refusal: %w", err)
	}
	defer unlock()
	if _, err := tx.ExecContext(ctx, `INSERT INTO introduced_link_refusal
		(at, org_id, offer_id, remote_chain_id, kind, role, reason) VALUES (?,?,?,?,?,?,?)`,
		r.At, r.OrgID, r.OfferID, r.RemoteChainID, r.Kind, r.Role, reason); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("insert introduced link refusal: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM introduced_link_refusal WHERE id <= (
		SELECT id FROM introduced_link_refusal ORDER BY id DESC LIMIT 1 OFFSET ?)`, maxIntroducedLinkRefusals); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("prune introduced link refusals: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit introduced link refusal: %w", err)
	}
	return nil
}

// ListIntroducedLinkRefusals returns refusals newest first, optionally for one
// remote chain.
func (s *SQLiteStore) ListIntroducedLinkRefusals(ctx context.Context, remoteChainID string) ([]IntroducedLinkRefusal, error) {
	q := `SELECT id, at, org_id, offer_id, remote_chain_id, kind, role, reason FROM introduced_link_refusal`
	args := []any{}
	if remoteChainID != "" {
		q += ` WHERE remote_chain_id = ?`
		args = append(args, remoteChainID)
	}
	q += ` ORDER BY id DESC LIMIT ?`
	args = append(args, maxIntroducedLinkRefusals)
	rows, err := s.conn.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list introduced link refusals: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := []IntroducedLinkRefusal{}
	for rows.Next() {
		var r IntroducedLinkRefusal
		if err := rows.Scan(&r.ID, &r.At, &r.OrgID, &r.OfferID, &r.RemoteChainID, &r.Kind, &r.Role, &r.Reason); err != nil {
			return nil, fmt.Errorf("scan introduced link refusal: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list introduced link refusals: %w", err)
	}
	return out, nil
}

// RekeySyncControlPin moves the pinned remote CA of an ACTIVE sync-control
// binding to newPin, keeping its epoch, controller and delivered policy. It
// is the rekey counterpart of PrepareSyncControl + ActivateSyncControl, which
// would reset the binding. It is idempotent and refuses a missing or
// non-active binding.
func (s *SQLiteStore) RekeySyncControlPin(ctx context.Context, remoteChainID, newPin string) error {
	if remoteChainID == "" || !introLinkIDPattern.MatchString(newPin) {
		return fmt.Errorf("rekey sync control: a remote chain and a 64 lowercase hex pin are required")
	}
	unlock := s.LockSyncPolicyWrite()
	defer unlock()
	res, err := s.writeExecContext(ctx, `UPDATE sync_control SET remote_ca_pin = ?,
		updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		WHERE remote_chain_id = ? AND binding_state = 'active'`, newPin, remoteChainID)
	if err != nil {
		return fmt.Errorf("rekey sync control: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("rekey sync control: no active binding for %s", remoteChainID)
	}
	return nil
}
