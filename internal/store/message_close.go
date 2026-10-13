package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

const MaxMessageCloseReasonBytes = 1024

var ErrMessageCloseConflict = errors.New("message already has a different terminal outcome")

// MessageCloseStore is an additive, receiver-local disposition capability.
// Closing does not create a reply, read receipt, or federation transport event.
type MessageCloseStore interface {
	CloseMessage(context.Context, string, string, string, string) (bool, error)
}

func (s *SQLiteStore) CloseMessage(ctx context.Context, receiverID, messageID, sessionID, reason string) (bool, error) {
	if receiverID == "" || messageID == "" || sessionID == "" || len(sessionID) > MaxMessageClaimantSessionBytes ||
		strings.TrimSpace(reason) == "" || len(reason) > MaxMessageCloseReasonBytes {
		return false, ErrMessageNotFound
	}
	if s.db != nil {
		// Hold vault publication through commit so an enable/lock transition
		// cannot race a plaintext reason or a stale transaction vault snapshot.
		unlock, err := s.lockVaultWrite(ctx)
		if err != nil {
			return false, err
		}
		defer unlock()
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return false, err
		}
		defer tx.Rollback() //nolint:errcheck
		replayed, err := s.transactionClone(tx, false).CloseMessage(ctx, receiverID, messageID, sessionID, reason)
		if err != nil {
			return false, err
		}
		return replayed, tx.Commit()
	}
	var replayed bool
	err := s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		var addressed, provider, claimed, status, destination, expiry string
		if err := tx.conn.QueryRowContext(ctx, `SELECT to_agent,to_provider,COALESCE(claimed_by,''),status,destination_chain_id,expires_at
			FROM pipeline_messages WHERE pipe_id=?`, messageID).Scan(&addressed, &provider, &claimed, &status, &destination, &expiry); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrMessageNotFound
			}
			return err
		}
		allowed := addressed == receiverID || (addressed == "" && provider != "" && claimed == receiverID)
		if destination != "" || !allowed {
			return ErrMessageNotFound
		}
		var closer, storedReason string
		err := tx.conn.QueryRowContext(ctx, `SELECT receiver_agent_id,reason FROM message_closures WHERE message_id=?`, messageID).Scan(&closer, &storedReason)
		if err == nil {
			if closer != receiverID {
				return ErrMessageNotFound
			}
			original, openErr := tx.openMessageFingerprint(storedReason)
			if openErr != nil {
				return openErr
			}
			if original != reason {
				return ErrMessageCloseConflict
			}
			replayed = true
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if status != "pending" && status != "claimed" {
			return ErrMessageCloseConflict
		}
		if status == "claimed" {
			if claimed != receiverID {
				return ErrMessageNotFound
			}
			var owner string
			if ownerErr := tx.conn.QueryRowContext(ctx, `SELECT claimant_session_id FROM message_fetch_receipts
				WHERE message_id=? AND receiver_agent_id=?`, messageID, receiverID).Scan(&owner); ownerErr != nil {
				return ErrMessageNotFound
			}
			if owner != sessionID {
				return ErrMessageClaimedByOtherSession
			}
		}
		sealedReason, err := tx.encryptContent(reason)
		if err != nil {
			return fmt.Errorf("encrypt message close reason: %w", err)
		}
		// Keep the existing terminal storage state and retention invariants. The
		// separate disposition distinguishes closure from a completed reply.
		res, err := tx.writeExecContext(ctx, `UPDATE pipeline_messages SET status='completed',result=NULL,
			completed_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
			WHERE pipe_id=? AND status IN ('pending','claimed') AND expires_at>strftime('%Y-%m-%dT%H:%M:%fZ','now')`, messageID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrMessageNotFound
		}
		_, err = tx.writeExecContext(ctx, `INSERT INTO message_closures(message_id,receiver_agent_id,claimant_session_id,reason)
			VALUES(?,?,?,?)`, messageID, receiverID, sessionID, sealedReason)
		return err
	})
	return replayed, err
}
