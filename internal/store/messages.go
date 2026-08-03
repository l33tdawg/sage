package store

import (
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
	maxMessageReceiveBatchesPerAgent = 4096
	messageReceiveBatchRetention     = 48 * time.Hour
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
		msg.FromAgent, msg.ToAgent, msg.ToProvider,
		msg.Intent, msg.Payload, strconv.FormatInt(msg.ExpiresAt.Sub(msg.CreatedAt).Nanoseconds(), 10),
	} {
		_, _ = h.Write([]byte(strconv.Itoa(len(value))))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(value))
		_, _ = h.Write([]byte{0xff})
	}
	return hex.EncodeToString(h.Sum(nil))
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
		msg.SourceChainID != "" || msg.DestinationChainID != "" || msg.ToProvider != "" {
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
		result = &copy
		return nil
	})
	return result, replayed, err
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
func (s *SQLiteStore) ReceiveLocalMessages(ctx context.Context, agentID, provider, receiveToken string, limit int) ([]*PipelineMessage, bool, error) {
	if agentID == "" || receiveToken == "" || len(receiveToken) > MaxMessageTokenBytes {
		return nil, false, fmt.Errorf("receive requires an exact agent and a token of at most %d bytes", MaxMessageTokenBytes)
	}
	if limit <= 0 || limit > 20 {
		return nil, false, fmt.Errorf("receive limit must be between 1 and 20")
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
			`INSERT INTO message_receive_batches(receiver_agent_id,token_hash,requested_limit)
			 VALUES(?,?,?)`, agentID, tokenHash, limit); insertErr != nil {
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
				`INSERT OR IGNORE INTO message_fetch_receipts(message_id,receiver_agent_id)
				 VALUES(?,?)`, item.PipeID, agentID); err != nil {
				return err
			}
			claimed, err := tx.GetPipeline(ctx, item.PipeID)
			if err != nil {
				return err
			}
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
func (s *SQLiteStore) ReplyLocalMessage(ctx context.Context, receiverID, messageID, result string) (bool, error) {
	if len(result) > MaxPipeContentBytes {
		return false, ErrPipeResultTooLarge
	}
	resultHash := messageKeyHash(result)
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

// Compile-time assertion: SQLite's canonical service and legacy pipeline
// methods share one authoritative row model.
var _ MessageStore = (*SQLiteStore)(nil)

// Retain time import even when a future metadata projection changes nullable
// timestamp scanning independently of pipeline rows.
var _ = time.Time{}
