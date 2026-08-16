package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	// MaxMessageTokenBytes bounds caller-controlled idempotency and receive
	// tokens before hashing/persisting them.
	MaxMessageTokenBytes             = 256
	MaxMessageClaimantSessionBytes   = 128
	maxMessageReceiveBatchesPerAgent = 4096
	messageReceiveBatchRetention     = 48 * time.Hour
	// CanonicalMessageLifetime is a storage/transport sentinel for the public
	// email-like default: unread and unclaimed Messages do not age out. Keeping
	// a concrete far-future time preserves the existing signed wire shape and
	// mixed-version database schema without treating zero time as already due.
	CanonicalMessageLifetime = 100 * 365 * 24 * time.Hour
)

func (s *SQLiteStore) migrateMessages(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS message_send_idempotency (
			sender_agent_id TEXT NOT NULL,
			key_hash TEXT NOT NULL,
			request_hash TEXT NOT NULL,
			message_id TEXT NOT NULL,
			created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
			PRIMARY KEY (sender_agent_id, key_hash),
			FOREIGN KEY (message_id) REFERENCES pipeline_messages(pipe_id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS message_receive_batches (
			receiver_agent_id TEXT NOT NULL,
			token_hash TEXT NOT NULL,
			requested_limit INTEGER NOT NULL,
			claimed_count INTEGER NOT NULL DEFAULT 0,
			claimant_session_id TEXT NOT NULL DEFAULT 'legacy',
			created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
			PRIMARY KEY (receiver_agent_id, token_hash)
		)`,
		`CREATE TABLE IF NOT EXISTS message_receive_batch_items (
			receiver_agent_id TEXT NOT NULL,
			token_hash TEXT NOT NULL,
			ordinal INTEGER NOT NULL,
			message_id TEXT NOT NULL,
			PRIMARY KEY (receiver_agent_id, token_hash, ordinal),
			UNIQUE (receiver_agent_id, token_hash, message_id),
			FOREIGN KEY (receiver_agent_id, token_hash)
				REFERENCES message_receive_batches(receiver_agent_id, token_hash) ON DELETE CASCADE,
			FOREIGN KEY (message_id) REFERENCES pipeline_messages(pipe_id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_message_receive_item_agent_message
			ON message_receive_batch_items(receiver_agent_id, message_id)`,
		// Fetch evidence outlives replay-token metadata. Receive batches are
		// intentionally pruned after 48 hours to bound empty-poll growth, while a
		// retained inbox item must remain acknowledgeable/replyable for as long as
		// the authoritative pipeline row exists.
		`CREATE TABLE IF NOT EXISTS message_fetch_receipts (
			message_id TEXT PRIMARY KEY,
			receiver_agent_id TEXT NOT NULL,
			claimant_session_id TEXT NOT NULL DEFAULT 'legacy',
			fetched_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
			FOREIGN KEY (message_id) REFERENCES pipeline_messages(pipe_id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_message_fetch_receipts_receiver
			ON message_fetch_receipts(receiver_agent_id, message_id)`,
		`CREATE TABLE IF NOT EXISTS message_replies (
			message_id TEXT PRIMARY KEY,
			receiver_agent_id TEXT NOT NULL,
			result_hash TEXT NOT NULL,
			created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
			FOREIGN KEY (message_id) REFERENCES pipeline_messages(pipe_id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS message_read_receipts (
			message_id TEXT PRIMARY KEY,
			receiver_agent_id TEXT NOT NULL,
			read_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
			FOREIGN KEY (message_id) REFERENCES pipeline_messages(pipe_id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS message_wake_state (
			recipient_agent_id TEXT PRIMARY KEY,
			seq INTEGER NOT NULL CHECK(seq >= 0)
		)`,
	}
	for _, statement := range statements {
		if _, err := s.writeExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate canonical messages: %w", err)
		}
	}
	// Upgrade databases that created the v11.17 preview table before exact
	// replay cardinality was persisted. Backfill from the durable item table so
	// a later pipeline-retention sweep cannot turn a known non-empty replay into
	// a misleading successful empty response.
	_, _ = s.writeExecContext(ctx, `ALTER TABLE message_receive_batches
		ADD COLUMN claimed_count INTEGER NOT NULL DEFAULT 0`)
	_, _ = s.writeExecContext(ctx, `ALTER TABLE message_receive_batches
		ADD COLUMN claimant_session_id TEXT NOT NULL DEFAULT 'legacy'`)
	_, _ = s.writeExecContext(ctx, `ALTER TABLE message_fetch_receipts
		ADD COLUMN claimant_session_id TEXT NOT NULL DEFAULT 'legacy'`)
	if _, err := s.writeExecContext(ctx, `UPDATE message_receive_batches
		SET claimed_count=(SELECT COUNT(*) FROM message_receive_batch_items i
			WHERE i.receiver_agent_id=message_receive_batches.receiver_agent_id
			  AND i.token_hash=message_receive_batches.token_hash)
		WHERE claimed_count=0`); err != nil {
		return fmt.Errorf("backfill canonical message receive cardinality: %w", err)
	}
	// Preserve fetch authority for preview databases that already contain
	// claimed receive batches. Batch metadata can then age out without making a
	// still-retained inbox item impossible to acknowledge or reply to.
	if _, err := s.writeExecContext(ctx, `INSERT OR IGNORE INTO message_fetch_receipts(message_id,receiver_agent_id)
		SELECT message_id,receiver_agent_id FROM message_receive_batch_items`); err != nil {
		return fmt.Errorf("backfill canonical message fetch evidence: %w", err)
	}
	// v11.17.8 stamped the old 24-hour pipeline TTL onto canonical msg-* rows.
	// Extend still-live inbox/outbox items during upgrade so a recipient that
	// was offline through the release does not lose unread work.
	if _, err := s.writeExecContext(ctx, `UPDATE pipeline_messages
		SET expires_at=strftime('%Y-%m-%dT%H:%M:%fZ',created_at,'+100 years')
		WHERE pipe_id LIKE 'msg-%' AND status IN ('pending','claimed')`); err != nil {
		return fmt.Errorf("extend canonical message inbox retention: %w", err)
	}
	// An upgraded node may already hold canonical pending work. Give each exact
	// recipient one durable baseline sequence so a fresh after_seq=0 consumer
	// receives a catch-up event after restart rather than waiting for a new send.
	if _, err := s.writeExecContext(ctx, `INSERT OR IGNORE INTO message_wake_state(recipient_agent_id,seq)
		SELECT DISTINCT to_agent,1 FROM pipeline_messages
		WHERE source_chain_id='' AND destination_chain_id='' AND to_provider=''
		  AND to_agent<>'' AND status='pending' AND expires_at>strftime('%Y-%m-%dT%H:%M:%fZ','now')`); err != nil {
		return fmt.Errorf("backfill canonical message wake state: %w", err)
	}
	return nil
}

func messageKeyHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func localMessageRequestHash(msg *PipelineMessage) string {
	h := sha256.New()
	for _, value := range []string{
		// FromProvider is server-derived display metadata and may change after a
		// send. It is not part of the caller's idempotent request. Including it
		// would make an exact retry conflict merely because the sender profile was
		// edited between attempts.
		msg.FromAgent, msg.ToAgent, msg.ToProvider, msg.DestinationChainID,
		msg.Intent, msg.Payload, strconv.FormatInt(msg.ExpiresAt.Sub(msg.CreatedAt).Nanoseconds(), 10),
	} {
		_, _ = h.Write([]byte(strconv.Itoa(len(value))))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(value))
		_, _ = h.Write([]byte{0xff})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// SendFederatedMessage gives the canonical Messages surface the same durable,
// caller-bound retry contract as a local send while retaining the existing
// federation transport and wire protocol. The message row, transport event,
// and idempotency binding commit in one SQLite transaction.
func (s *SQLiteStore) SendFederatedMessage(ctx context.Context, idempotencyKey string, msg *PipelineMessage, event *PipelineTransportOutbox) (*PipelineMessage, bool, error) {
	if msg == nil || event == nil || strings.TrimSpace(msg.FromAgent) == "" ||
		strings.TrimSpace(msg.ToAgent) == "" || msg.DestinationChainID == "" ||
		msg.SourceChainID != "" || msg.ToProvider != "" || event.PipeID != msg.PipeID ||
		event.RemoteChainID != msg.DestinationChainID || event.SourceAgentID != msg.FromAgent ||
		event.TargetAgentID != msg.ToAgent || event.ContactID != msg.FederationContactID ||
		event.ContactRevision != msg.FederationContactRevision || event.PolicyEpoch != msg.FederationPolicyEpoch ||
		event.AgreementID != msg.FederationAgreementID || event.AuthorizationMode != msg.FederationAuthorizationMode ||
		!bytes.Equal(event.LinkedRelation, msg.FederationLinkedRelation) {
		return nil, false, fmt.Errorf("canonical federated message binding is invalid")
	}
	if idempotencyKey == "" || len(idempotencyKey) > MaxMessageTokenBytes {
		return nil, false, fmt.Errorf("idempotency key must be between 1 and %d bytes", MaxMessageTokenBytes)
	}
	keyHash := messageKeyHash(idempotencyKey)
	requestHash := localMessageRequestHash(msg)
	var result *PipelineMessage
	var replayed bool
	err := s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		var priorHash, messageID string
		err := tx.conn.QueryRowContext(ctx,
			`SELECT request_hash, message_id FROM message_send_idempotency
			 WHERE sender_agent_id=? AND key_hash=?`, msg.FromAgent, keyHash).Scan(&priorHash, &messageID)
		switch {
		case err == nil:
			openedHash, openErr := tx.openMessageFingerprint(priorHash)
			if openErr != nil {
				return openErr
			}
			if openedHash != requestHash {
				return ErrMessageIdempotencyConflict
			}
			var getErr error
			result, getErr = tx.GetPipeline(ctx, messageID)
			replayed = true
			return getErr
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}
		if insertErr := tx.InsertPipeline(ctx, msg); insertErr != nil {
			return insertErr
		}
		if insertErr := tx.insertPipelineTransport(ctx, event); insertErr != nil {
			return insertErr
		}
		sealedHash, sealErr := tx.sealMessageFingerprint(requestHash)
		if sealErr != nil {
			return sealErr
		}
		if _, writeErr := tx.writeExecContext(ctx,
			`INSERT INTO message_send_idempotency(sender_agent_id,key_hash,request_hash,message_id)
			 VALUES(?,?,?,?)`, msg.FromAgent, keyHash, sealedHash, msg.PipeID); writeErr != nil {
			return writeErr
		}
		copy := *msg
		result = &copy
		return nil
	})
	return result, replayed, err
}

// sealMessageFingerprint prevents the deterministic content fingerprints used
// for idempotency/equivocation checks from becoming offline guessing oracles
// when the local vault is active. On an intentionally unencrypted node this is
// plaintext, matching the confidentiality boundary of pipeline content itself.
func (s *SQLiteStore) sealMessageFingerprint(fingerprint string) (string, error) {
	return s.encryptContent(fingerprint)
}

func (s *SQLiteStore) openMessageFingerprint(stored string) (string, error) {
	opened, err := s.decryptContent(stored)
	if err != nil {
		return "", err
	}
	if opened == VaultLockedPlaceholder {
		return "", ErrPipeContentUnavailable
	}
	return opened, nil
}

// SendLocalMessage inserts one local pipeline row and its caller-bound
// idempotency mapping atomically. Repeating the same key and exact request
// returns the original row; reusing the key for different content fails.
func (s *SQLiteStore) SendLocalMessage(ctx context.Context, idempotencyKey string, msg *PipelineMessage) (*PipelineMessage, bool, error) {
	if msg == nil || strings.TrimSpace(msg.FromAgent) == "" || strings.TrimSpace(msg.ToAgent) == "" ||
		msg.SourceChainID != "" || msg.DestinationChainID != "" || msg.ToProvider != "" || msg.Status != "pending" {
		return nil, false, fmt.Errorf("canonical local message requires exact local sender and recipient")
	}
	if idempotencyKey == "" || len(idempotencyKey) > MaxMessageTokenBytes {
		return nil, false, fmt.Errorf("idempotency key must be between 1 and %d bytes", MaxMessageTokenBytes)
	}
	keyHash := messageKeyHash(idempotencyKey)
	requestHash := localMessageRequestHash(msg)
	var result *PipelineMessage
	var replayed bool
	err := s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		var priorHash, messageID string
		err := tx.conn.QueryRowContext(ctx,
			`SELECT request_hash, message_id FROM message_send_idempotency
			 WHERE sender_agent_id=? AND key_hash=?`, msg.FromAgent, keyHash).Scan(&priorHash, &messageID)
		switch {
		case err == nil:
			openedHash, openErr := tx.openMessageFingerprint(priorHash)
			if openErr != nil {
				return openErr
			}
			if openedHash != requestHash {
				return ErrMessageIdempotencyConflict
			}
			var getErr error
			result, getErr = tx.GetPipeline(ctx, messageID)
			replayed = true
			return getErr
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}
		if insertErr := tx.InsertPipeline(ctx, msg); insertErr != nil {
			return insertErr
		}
		var wakeSeq int64
		if writeErr := tx.conn.QueryRowContext(ctx,
			`INSERT INTO message_wake_state(recipient_agent_id,seq) VALUES(?,1)
			 ON CONFLICT(recipient_agent_id) DO UPDATE SET seq=message_wake_state.seq+1
			 RETURNING seq`, msg.ToAgent).Scan(&wakeSeq); writeErr != nil {
			return writeErr
		}
		if wakeSeq < 1 {
			return errors.New("canonical message wake sequence did not advance")
		}
		sealedHash, err := tx.sealMessageFingerprint(requestHash)
		if err != nil {
			return err
		}
		if _, writeErr := tx.writeExecContext(ctx,
			`INSERT INTO message_send_idempotency(sender_agent_id,key_hash,request_hash,message_id)
			 VALUES(?,?,?,?)`, msg.FromAgent, keyHash, sealedHash, msg.PipeID); writeErr != nil {
			return writeErr
		}
		copy := *msg
		copy.WakeSeq = uint64(wakeSeq)
		result = &copy
		return nil
	})
	return result, replayed, err
}

// GetMessageWakeState returns only the authenticated caller's exact durable
// wake sequence and whether unfinished canonical local work exists. Claimed
// work remains unfinished: a claimant session may crash, so claim ownership
// alone cannot make the wake surface say the recipient has nothing to handle.
// It never decrypts a message and does not claim, read, acknowledge, or mutate.
func (s *SQLiteStore) GetMessageWakeState(ctx context.Context, recipientID string) (MessageWakeState, error) {
	recipientID = strings.TrimSpace(recipientID)
	if recipientID == "" {
		return MessageWakeState{}, errors.New("message wake recipient is required")
	}
	var seq int64
	err := s.conn.QueryRowContext(ctx,
		`SELECT seq FROM message_wake_state WHERE recipient_agent_id=?`, recipientID).Scan(&seq)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return MessageWakeState{}, err
	}
	if errors.Is(err, sql.ErrNoRows) {
		seq = 0
	}
	if seq < 0 {
		return MessageWakeState{}, errors.New("message wake sequence is invalid")
	}
	var pending int
	if err := s.conn.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM pipeline_messages
		 WHERE source_chain_id='' AND destination_chain_id='' AND to_provider=''
		   AND to_agent=? AND status IN ('pending','claimed')
		   AND expires_at>strftime('%Y-%m-%dT%H:%M:%fZ','now'))`, recipientID).Scan(&pending); err != nil {
		return MessageWakeState{}, err
	}
	return MessageWakeState{Seq: uint64(seq), Pending: pending != 0}, nil
}

func loadReceiveBatch(ctx context.Context, s *SQLiteStore, receiverID, tokenHash string, expectedCount int) ([]*PipelineMessage, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT message_id FROM message_receive_batch_items
		 WHERE receiver_agent_id=? AND token_hash=? ORDER BY ordinal`, receiverID, tokenHash)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) != expectedCount {
		return nil, ErrMessageReceiveExpired
	}
	items := make([]*PipelineMessage, 0, len(ids))
	for _, id := range ids {
		item, err := s.GetPipeline(ctx, id)
		if err != nil {
			return nil, err
		}
		// Canonical receive replays the exact work snapshot, not later reply or
		// workflow transitions on the authoritative row. Request fields are
		// immutable; normalize the mutable fields to their claimed-batch state.
		item.Status = "claimed"
		item.ClaimedBy = receiverID
		item.Result = ""
		item.CompletedAt = nil
		item.JournalID = ""
		_ = s.conn.QueryRowContext(ctx, `SELECT claimant_session_id FROM message_fetch_receipts
			WHERE receiver_agent_id=? AND message_id=?`, receiverID, id).Scan(&item.ClaimedSessionID)
		items = append(items, item)
	}
	return items, nil
}

// pendingExactLocalMessages deliberately excludes provider-addressed and
// federated rows. Canonical messages always name one exact local recipient;
// allowing older provider/federation queue entries to consume the SQL LIMIT
// would make local delivery depend on unrelated traffic ordering.
func pendingExactLocalMessages(ctx context.Context, s *SQLiteStore, receiverID string, limit int) ([]*PipelineMessage, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT pipe_id FROM pipeline_messages
		 WHERE status='pending'
		   AND source_chain_id=''
		   AND destination_chain_id=''
		   AND to_agent=?
		   AND to_provider=''
		   AND expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		 ORDER BY created_at ASC, pipe_id ASC LIMIT ?`, receiverID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	items := make([]*PipelineMessage, 0, len(ids))
	for _, id := range ids {
		item, err := s.GetPipeline(ctx, id)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

// ReceiveLocalMessages claims at most limit pending rows and persists the
// exact ordered batch behind a caller-supplied token. A retry after a lost
// HTTP response replays that batch and never claims later work.
func (s *SQLiteStore) ReceiveLocalMessages(ctx context.Context, agentID, provider, receiveToken string, limit int, claimantSessionIDs ...string) ([]*PipelineMessage, bool, error) {
	if agentID == "" || receiveToken == "" || len(receiveToken) > MaxMessageTokenBytes {
		return nil, false, fmt.Errorf("receive requires an exact agent and a token of at most %d bytes", MaxMessageTokenBytes)
	}
	if limit <= 0 || limit > 20 {
		return nil, false, fmt.Errorf("receive limit must be between 1 and 20")
	}
	claimantSessionID := "legacy"
	if len(claimantSessionIDs) > 0 && strings.TrimSpace(claimantSessionIDs[0]) != "" {
		claimantSessionID = strings.TrimSpace(claimantSessionIDs[0])
	}
	if len(claimantSessionID) > MaxMessageClaimantSessionBytes {
		return nil, false, fmt.Errorf("claimant session id must be at most %d bytes", MaxMessageClaimantSessionBytes)
	}
	tokenHash := messageKeyHash(receiveToken)
	var items []*PipelineMessage
	var replayed bool
	err := s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		var priorLimit, priorCount int
		err := tx.conn.QueryRowContext(ctx,
			`SELECT requested_limit,claimed_count FROM message_receive_batches
			 WHERE receiver_agent_id=? AND token_hash=?`, agentID, tokenHash).Scan(&priorLimit, &priorCount)
		switch {
		case err == nil:
			if priorLimit != limit {
				return ErrMessageReceiveConflict
			}
			var loadErr error
			items, loadErr = loadReceiveBatch(ctx, tx, agentID, tokenHash, priorCount)
			replayed = true
			return loadErr
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}
		// Receive tokens are durable lost-response evidence, but retaining an
		// unbounded stream of unique empty-poll tokens is a disk-exhaustion path.
		// Keep a generous 48-hour replay window, then prune metadata only; inbox
		// rows/history are governed by their independent pipeline retention.
		if _, pruneErr := tx.writeExecContext(ctx,
			`DELETE FROM message_receive_batches WHERE receiver_agent_id=? AND created_at < ?`,
			agentID, formatTime(time.Now().UTC().Add(-messageReceiveBatchRetention))); pruneErr != nil {
			return pruneErr
		}
		var retained int
		if countErr := tx.conn.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM message_receive_batches WHERE receiver_agent_id=?`, agentID).Scan(&retained); countErr != nil {
			return countErr
		}
		if retained >= maxMessageReceiveBatchesPerAgent {
			return ErrMessageReceiveQuota
		}
		if _, insertErr := tx.writeExecContext(ctx,
			`INSERT INTO message_receive_batches(receiver_agent_id,token_hash,requested_limit,claimant_session_id)
			 VALUES(?,?,?,?)`, agentID, tokenHash, limit, claimantSessionID); insertErr != nil {
			return insertErr
		}
		pending, err := pendingExactLocalMessages(ctx, tx, agentID, limit)
		if err != nil {
			return err
		}
		items = make([]*PipelineMessage, 0, len(pending))
		for _, item := range pending {
			if item.SourceChainID != "" || item.DestinationChainID != "" {
				continue
			}
			if err := tx.ClaimPipeline(ctx, item.PipeID, agentID); err != nil {
				// A concurrent receiver may legitimately win the claim between
				// selection and CAS. Only that exact state is skippable. A real
				// storage/context error must roll back the batch token rather than
				// permanently replaying a false empty batch.
				current, getErr := tx.GetPipeline(ctx, item.PipeID)
				if getErr == nil && current.Status != "pending" {
					continue
				}
				return err
			}
			if _, err := tx.writeExecContext(ctx,
				`INSERT INTO message_receive_batch_items(receiver_agent_id,token_hash,ordinal,message_id)
				 VALUES(?,?,?,?)`, agentID, tokenHash, len(items), item.PipeID); err != nil {
				return err
			}
			if _, err := tx.writeExecContext(ctx,
				`INSERT OR IGNORE INTO message_fetch_receipts(message_id,receiver_agent_id,claimant_session_id)
				 VALUES(?,?,?)`, item.PipeID, agentID, claimantSessionID); err != nil {
				return err
			}
			claimed, err := tx.GetPipeline(ctx, item.PipeID)
			if err != nil {
				return err
			}
			claimed.ClaimedSessionID = claimantSessionID
			items = append(items, claimed)
		}
		if _, err := tx.writeExecContext(ctx,
			`UPDATE message_receive_batches SET claimed_count=?
			 WHERE receiver_agent_id=? AND token_hash=?`, len(items), agentID, tokenHash); err != nil {
			return err
		}
		return nil
	})
	return items, replayed, err
}

// CountClaimedLocalMessagesElsewhere returns only an exact-recipient scalar.
// It deliberately queries the authoritative claim rows rather than a bounded
// history page, so older stranded claims cannot disappear behind newer work.
// No message identifier, sender, intent, payload, or claimant identity crosses
// this boundary.
func (s *SQLiteStore) CountClaimedLocalMessagesElsewhere(
	ctx context.Context, receiverID, claimantSessionID string,
) (int, error) {
	receiverID = strings.TrimSpace(receiverID)
	claimantSessionID = strings.TrimSpace(claimantSessionID)
	if receiverID == "" || claimantSessionID == "" || len(claimantSessionID) > MaxMessageClaimantSessionBytes {
		return 0, ErrMessageNotFound
	}
	var count int
	err := s.conn.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM pipeline_messages p
		JOIN message_fetch_receipts r
		  ON r.message_id=p.pipe_id AND r.receiver_agent_id=?
		WHERE p.to_agent=? AND p.to_provider=''
		  AND p.source_chain_id='' AND p.destination_chain_id=''
		  AND p.status='claimed' AND p.completed_at IS NULL
		  AND r.claimant_session_id!=?`,
		receiverID, receiverID, claimantSessionID).Scan(&count)
	return count, err
}

// HandoffLocalMessageClaim transfers only the session-level coordination
// marker for one already-claimed canonical message. Agent authorization and
// workflow ownership do not change. The compare-and-swap makes concurrent or
// stale handoffs visible instead of silently stealing work.
func (s *SQLiteStore) HandoffLocalMessageClaim(ctx context.Context, receiverID, messageID, fromSessionID, toSessionID string) (bool, error) {
	if receiverID == "" || messageID == "" || fromSessionID == "" || toSessionID == "" ||
		len(fromSessionID) > MaxMessageClaimantSessionBytes || len(toSessionID) > MaxMessageClaimantSessionBytes {
		return false, ErrMessageNotFound
	}
	var replayed bool
	err := s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		var addressed, provider, claimed, status, sourceChain, destinationChain, current string
		if err := tx.conn.QueryRowContext(ctx, `SELECT p.to_agent,p.to_provider,p.claimed_by,p.status,
			p.source_chain_id,p.destination_chain_id,r.claimant_session_id
			FROM pipeline_messages p JOIN message_fetch_receipts r ON r.message_id=p.pipe_id
			WHERE p.pipe_id=? AND r.receiver_agent_id=?`, messageID, receiverID).
			Scan(&addressed, &provider, &claimed, &status, &sourceChain, &destinationChain, &current); err != nil {
			return ErrMessageNotFound
		}
		if addressed != receiverID || provider != "" || claimed != receiverID || status != "claimed" || sourceChain != "" || destinationChain != "" {
			return ErrMessageNotFound
		}
		if current == toSessionID {
			replayed = true
			return nil
		}
		if current != fromSessionID {
			return ErrMessageReceiveConflict
		}
		result, err := tx.writeExecContext(ctx, `UPDATE message_fetch_receipts SET claimant_session_id=?
			WHERE message_id=? AND receiver_agent_id=? AND claimant_session_id=?`, toSessionID, messageID, receiverID, fromSessionID)
		if err != nil {
			return err
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			return ErrMessageReceiveConflict
		}
		return nil
	})
	return replayed, err
}

func hasExactMessageFetch(ctx context.Context, s *SQLiteStore, receiverID, messageID string) (bool, error) {
	var one int
	err := s.conn.QueryRowContext(ctx,
		`SELECT 1 FROM message_fetch_receipts
		 WHERE receiver_agent_id=? AND message_id=? LIMIT 1`, receiverID, messageID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// AcknowledgeLocalMessageRead records exact recipient evidence only after the
// canonical receive service durably returned that exact row to this caller.
func (s *SQLiteStore) AcknowledgeLocalMessageRead(ctx context.Context, receiverID, messageID string) (bool, error) {
	var replayed bool
	err := s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		var addressed, provider, claimed, sourceChain, destinationChain string
		if queryErr := tx.conn.QueryRowContext(ctx,
			`SELECT to_agent, to_provider, claimed_by, source_chain_id, destination_chain_id
			 FROM pipeline_messages WHERE pipe_id=?`, messageID).
			Scan(&addressed, &provider, &claimed, &sourceChain, &destinationChain); queryErr != nil {
			return ErrMessageNotFound
		}
		fetched, err := hasExactMessageFetch(ctx, tx, receiverID, messageID)
		if err != nil || !fetched || provider != "" || sourceChain != "" || destinationChain != "" ||
			(claimed != receiverID && addressed != receiverID) {
			return ErrMessageNotFound
		}
		var existing string
		if receiptErr := tx.conn.QueryRowContext(ctx,
			`SELECT receiver_agent_id FROM message_read_receipts WHERE message_id=?`, messageID).Scan(&existing); receiptErr == nil {
			if existing != receiverID {
				return ErrMessageNotFound
			}
			replayed = true
			return nil
		} else if !errors.Is(receiptErr, sql.ErrNoRows) {
			return receiptErr
		}
		_, err = tx.writeExecContext(ctx,
			`INSERT INTO message_read_receipts(message_id,receiver_agent_id) VALUES(?,?)`, messageID, receiverID)
		return err
	})
	return replayed, err
}

// ReplyLocalMessage completes an exact receiver-fetched local message. The
// same response is idempotent; a different second response is equivocation.
// Completion and exact read evidence share one transaction.
func (s *SQLiteStore) ReplyLocalMessage(ctx context.Context, receiverID, messageID, result string, claimantSessionIDs ...string) (bool, error) {
	if len(result) > MaxPipeContentBytes {
		return false, ErrPipeResultTooLarge
	}
	resultHash := messageKeyHash(result)
	claimantSessionID := ""
	if len(claimantSessionIDs) > 0 {
		claimantSessionID = strings.TrimSpace(claimantSessionIDs[0])
	}
	if len(claimantSessionID) > MaxMessageClaimantSessionBytes {
		return false, ErrMessageNotFound
	}
	var replayed bool
	err := s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		var existingReceiver, existingHash string
		err := tx.conn.QueryRowContext(ctx,
			`SELECT receiver_agent_id,result_hash FROM message_replies WHERE message_id=?`, messageID).
			Scan(&existingReceiver, &existingHash)
		switch {
		case err == nil:
			if existingReceiver != receiverID {
				return ErrMessageNotFound
			}
			openedHash, openErr := tx.openMessageFingerprint(existingHash)
			if openErr != nil {
				return openErr
			}
			if openedHash != resultHash {
				return ErrMessageReplyConflict
			}
			replayed = true
			return nil
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}
		var provider, claimed, sourceChain, destinationChain string
		if queryErr := tx.conn.QueryRowContext(ctx,
			`SELECT to_provider,claimed_by,source_chain_id,destination_chain_id FROM pipeline_messages WHERE pipe_id=?`, messageID).
			Scan(&provider, &claimed, &sourceChain, &destinationChain); queryErr != nil {
			return ErrMessageNotFound
		}
		fetched, err := hasExactMessageFetch(ctx, tx, receiverID, messageID)
		if err != nil || !fetched || provider != "" || claimed != receiverID || sourceChain != "" || destinationChain != "" {
			return ErrMessageNotFound
		}
		// The fence runs only after the caller has been proven to be this
		// message's addressed recipient (claimed == receiverID above), so it
		// separates SESSIONS of one agent, never one agent from another. It
		// therefore reports a distinct error rather than collapsing into
		// ErrMessageNotFound: a 404 is indistinguishable from an absent route,
		// and the MCP client treats an absent route as licence to retry the
		// same call without a session id — bypassing this check entirely.
		if claimantSessionID != "" {
			var currentSessionID string
			if sessionErr := tx.conn.QueryRowContext(ctx, `SELECT claimant_session_id FROM message_fetch_receipts
				WHERE receiver_agent_id=? AND message_id=?`, receiverID, messageID).Scan(&currentSessionID); sessionErr != nil {
				return ErrMessageNotFound
			} else if currentSessionID != claimantSessionID {
				return ErrMessageClaimedByOtherSession
			}
		}
		if completeErr := tx.CompletePipeline(ctx, messageID, receiverID, result, ""); completeErr != nil {
			return ErrMessageNotFound
		}
		sealedHash, err := tx.sealMessageFingerprint(resultHash)
		if err != nil {
			return err
		}
		if _, insertErr := tx.writeExecContext(ctx,
			`INSERT INTO message_replies(message_id,receiver_agent_id,result_hash) VALUES(?,?,?)`,
			messageID, receiverID, sealedHash); insertErr != nil {
			return insertErr
		}
		_, err = tx.writeExecContext(ctx,
			`INSERT OR IGNORE INTO message_read_receipts(message_id,receiver_agent_id) VALUES(?,?)`,
			messageID, receiverID)
		return err
	})
	return replayed, err
}

// GetMessageStatusForSender is deliberately metadata-only and exact-sender
// scoped. The same ErrMessageNotFound covers absent and unrelated IDs.
func (s *SQLiteStore) GetMessageStatusForSender(ctx context.Context, senderID, messageID string) (*MessageStatus, error) {
	var status MessageStatus
	var workflow, sentAt, expiresAt string
	var completedAt, terminalAt, readAt *string
	err := s.conn.QueryRowContext(ctx,
		`SELECT p.pipe_id,p.status,p.created_at,p.completed_at,p.terminal_at,p.expires_at,r.read_at
		 FROM pipeline_messages p
		 LEFT JOIN message_read_receipts r ON r.message_id=p.pipe_id
		 WHERE p.pipe_id=? AND p.from_agent=? AND p.source_chain_id='' AND p.destination_chain_id=''
		   AND p.to_agent!='' AND p.to_provider=''`,
		messageID, senderID).Scan(&status.MessageID, &workflow, &sentAt, &completedAt, &terminalAt, &expiresAt, &readAt)
	if errors.Is(err, sql.ErrNoRows) {
		return s.getFederatedMessageStatusForSender(ctx, senderID, messageID)
	}
	if err != nil {
		return nil, ErrMessageNotFound
	}
	status.Scope = "local"
	status.TransportStatus = "delivered"
	status.WorkflowStatus = workflow
	status.SentAt = parseTime(sentAt)
	status.ExpiresAt = parseTime(expiresAt)
	status.DeliveredAt = &status.SentAt
	status.CompletedAt = parseTimePtr(completedAt)
	status.TerminalAt = parseTimePtr(terminalAt)
	status.ReadAt = parseTimePtr(readAt)
	status.ReadStatus = "not_confirmed"
	if status.ReadAt != nil {
		status.ReadStatus = "confirmed"
		status.ReadEvidence = "local_exact_ack"
	}
	switch workflow {
	case "completed":
		if status.TerminalAt == nil {
			status.TerminalAt = status.CompletedAt
		}
		status.TerminalReason = "completed"
	case "expired", "failed":
		if status.TerminalAt == nil {
			status.TerminalAt = &status.ExpiresAt
		}
		status.TerminalReason = workflow
	}
	return &status, nil
}

// getFederatedMessageStatusForSender merges the locally authoritative
// workflow row with its payload-free transport and receipt-v2 projections.
// The exact sender predicate is repeated here rather than trusting a receipt
// lookup, so receivers, operators, and unrelated local agents cannot use the
// canonical status route as a federated message-existence oracle.
func (s *SQLiteStore) getFederatedMessageStatusForSender(
	ctx context.Context, senderID, messageID string,
) (*MessageStatus, error) {
	var status MessageStatus
	var workflow, sentAt, expiresAt, transportState string
	var completedAt, terminalAt, transportDeliveredAt, receiptDeliveredAt, readAt *string
	var receiptProtocolVersion int
	err := s.conn.QueryRowContext(ctx, `SELECT p.pipe_id,p.status,p.created_at,p.completed_at,
		p.terminal_at,p.expires_at,o.state,o.delivered_at,o.receipt_protocol_version,
		r.delivered_at,r.read_at
		FROM pipeline_messages p
		JOIN pipeline_transport_outbox o ON o.pipe_id=p.pipe_id AND o.event_kind='send'
		LEFT JOIN pipeline_receipt_v2_projection r ON r.local_pipe_id=p.pipe_id
		WHERE p.pipe_id=? AND p.from_agent=? AND p.source_chain_id=''
		  AND p.destination_chain_id!='' AND p.to_agent!='' AND p.to_provider=''`,
		messageID, senderID).Scan(&status.MessageID, &workflow, &sentAt, &completedAt,
		&terminalAt, &expiresAt, &transportState, &transportDeliveredAt,
		&receiptProtocolVersion, &receiptDeliveredAt, &readAt)
	if err != nil {
		return nil, ErrMessageNotFound
	}
	status.Scope = "federated"
	status.WorkflowStatus = workflow
	status.SentAt = parseTime(sentAt)
	status.ExpiresAt = parseTime(expiresAt)
	status.CompletedAt = parseTimePtr(completedAt)
	status.TerminalAt = parseTimePtr(terminalAt)
	status.DeliveredAt = parseTimePtr(receiptDeliveredAt)
	if status.DeliveredAt == nil {
		status.DeliveredAt = parseTimePtr(transportDeliveredAt)
	}
	switch transportState {
	case "pending":
		status.TransportStatus = "queued"
	case "delivered", "failed":
		status.TransportStatus = transportState
	default:
		return nil, ErrMessageNotFound
	}
	status.ReadStatus = "unsupported"
	if receiptProtocolVersion == FederatedPipelineReceiptVersion {
		status.ReadStatus = "not_confirmed"
		status.ReadAt = parseTimePtr(readAt)
		if status.ReadAt != nil {
			status.ReadStatus = "confirmed"
			status.ReadEvidence = "federated_receipt_v2"
		}
	}
	switch workflow {
	case "completed":
		if status.TerminalAt == nil {
			status.TerminalAt = status.CompletedAt
		}
		status.TerminalReason = "completed"
	case "expired", "failed":
		if status.TerminalAt == nil {
			status.TerminalAt = &status.ExpiresAt
		}
		status.TerminalReason = workflow
	}
	return &status, nil
}

// Compile-time assertion: SQLite's canonical service and legacy pipeline
// methods share one authoritative row model.
var _ MessageStore = (*SQLiteStore)(nil)

// Retain time import even when a future metadata projection changes nullable
// timestamp scanning independently of pipeline rows.
var _ = time.Time{}
