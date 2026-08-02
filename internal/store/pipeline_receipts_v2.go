package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const FederatedPipelineReceiptVersion = 2

var (
	ErrFederatedReceiptInvalid  = errors.New("federated pipeline receipt binding is invalid")
	ErrFederatedReceiptConflict = errors.New("federated pipeline receipt conflicts with accepted evidence")
	ErrFederatedReceiptNotFound = errors.New("federated pipeline receipt not found")
)

// FederatedReceiptBinding is the immutable identity of one cross-node send.
// Sender/recipient chain names retain the original message direction even when
// the receipt travels back from recipient to sender.
type FederatedReceiptBinding struct {
	MessageID         string
	LocalPipeID       string
	SenderChainID     string
	RecipientChainID  string
	SenderAgentID     string
	RecipientAgentID  string
	ContentDigest     string
	PolicyEpoch       string
	AgreementID       string
	ContactID         string
	ContactRevision   string
	AuthorizationMode string
	RelationDigest    string
}

// FederatedReceiptEvent is accepted only after transport authentication,
// exact-agent proof verification, and live generation revalidation. EventAt is
// evidence metadata; it never orders authority or overwrites prior evidence.
type FederatedReceiptEvent struct {
	FederatedReceiptBinding
	EventID   string
	Kind      string // claimed or read
	EventAt   time.Time
	ProofHash []byte
}

// FederatedReceiptProjection is sender-visible, payload-free participant state.
// Claimed, Read and Terminal are independent monotonic dimensions.
type FederatedReceiptProjection struct {
	FederatedReceiptBinding
	DeliveredAt      *time.Time
	DeliveryEvidence string
	ClaimedAt        *time.Time
	ReadAt           *time.Time
	TerminalKind     string
	TerminalAt       *time.Time
	UpdatedAt        time.Time
}

type FederatedReceiptOutbox struct {
	FederatedReceiptBinding
	EventID         string
	RecipientPipeID string
	Kind            string
	EventAt         time.Time
	Proof           PipelineAgentProof
	State           string
	Attempts        int
	NextAttemptAt   time.Time
	CreatedAt       time.Time
	ExpiresAt       time.Time
	DeliveredAt     *time.Time
	LastError       string
}

func validateFederatedReceiptOutbox(event *FederatedReceiptOutbox) error {
	if event == nil || validateFederatedReceiptBinding(event.FederatedReceiptBinding) != nil ||
		event.EventID == "" || len(event.EventID) > 200 || event.RecipientPipeID == "" ||
		event.RecipientPipeID != event.LocalPipeID ||
		(event.Kind != "claimed" && event.Kind != "read") || event.EventAt.IsZero() ||
		event.Proof.AgentID != event.RecipientAgentID || len(event.Proof.Signature) != 64 ||
		event.Proof.Timestamp <= 0 || len(event.Proof.Nonce) < 8 || len(event.Proof.Nonce) > 64 ||
		len(event.Proof.CanonicalRequest) == 0 || len(event.Proof.CanonicalRequest) > maxPipelineProofBytes {
		return ErrFederatedReceiptInvalid
	}
	return nil
}

func (s *SQLiteStore) insertFederatedReceiptOutbox(ctx context.Context, event *FederatedReceiptOutbox) (bool, error) {
	if err := validateFederatedReceiptOutbox(event); err != nil {
		return false, err
	}
	proofEnvelope, err := json.Marshal(pipelineProofEnvelope{Version: pipelineProofEnvelopeVersion, Proof: event.Proof})
	if err != nil {
		return false, err
	}
	encryptedProof, err := s.encryptContent(string(proofEnvelope))
	if err != nil {
		return false, err
	}
	now := event.CreatedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	next := event.NextAttemptAt
	if next.IsZero() {
		next = now
	}
	expires := event.ExpiresAt
	if expires.IsZero() {
		expires = now.Add(24 * time.Hour)
	}
	_, err = s.writeExecContext(ctx, `INSERT INTO pipeline_receipt_v2_outbox
		(event_id,message_id,local_pipe_id,remote_chain_id,recipient_pipe_id,event_kind,
		 sender_chain_id,recipient_chain_id,sender_agent_id,recipient_agent_id,content_digest,
		 policy_epoch,agreement_id,contact_id,contact_revision,authorization_mode,relation_digest,
		 event_at,proof_envelope,state,attempts,next_attempt_at,created_at,expires_at,last_error)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, event.EventID, event.MessageID,
		event.LocalPipeID, event.SenderChainID, event.RecipientPipeID, event.Kind,
		event.SenderChainID, event.RecipientChainID, event.SenderAgentID, event.RecipientAgentID,
		event.ContentDigest, event.PolicyEpoch, event.AgreementID, event.ContactID,
		event.ContactRevision, event.AuthorizationMode, event.RelationDigest, formatTime(event.EventAt),
		encryptedProof, "pending", 0, formatTime(next), formatTime(now), formatTime(expires), "")
	if err == nil {
		return false, nil
	}
	var prior FederatedReceiptBinding
	var priorRemoteChain, priorRecipientPipe, priorKind string
	lookupErr := s.conn.QueryRowContext(ctx, `SELECT message_id,local_pipe_id,remote_chain_id,recipient_pipe_id,event_kind,
		sender_chain_id,recipient_chain_id,sender_agent_id,recipient_agent_id,content_digest,policy_epoch,
		agreement_id,contact_id,contact_revision,authorization_mode,relation_digest
		FROM pipeline_receipt_v2_outbox WHERE message_id=? AND event_kind=?`, event.MessageID, event.Kind).
		Scan(&prior.MessageID, &prior.LocalPipeID, &priorRemoteChain, &priorRecipientPipe, &priorKind,
			&prior.SenderChainID, &prior.RecipientChainID, &prior.SenderAgentID, &prior.RecipientAgentID,
			&prior.ContentDigest, &prior.PolicyEpoch, &prior.AgreementID, &prior.ContactID,
			&prior.ContactRevision, &prior.AuthorizationMode, &prior.RelationDigest)
	if lookupErr != nil {
		return false, err
	}
	// A fresh exact-recipient proof for the same already queued fact is a
	// semantic retry, not new evidence. Preserve the first write-once proof and
	// EventAt; compare every immutable binding before returning duplicate.
	if !sameReceiptBinding(prior, event.FederatedReceiptBinding) ||
		priorRemoteChain != event.SenderChainID || priorRecipientPipe != event.RecipientPipeID ||
		priorKind != event.Kind {
		return false, ErrFederatedReceiptConflict
	}
	return true, nil
}

// RecordImportedFederatedReceipt atomically performs an exact recipient claim
// (for claimed) and queues the bound v2 evidence. Read is accepted only after
// that exact recipient owns the claim. A legacy imported row has no inbound
// binding and therefore cannot manufacture a receipt.
func (s *SQLiteStore) RecordImportedFederatedReceipt(
	ctx context.Context, localPipeID, recipientID string, event *FederatedReceiptOutbox,
) (bool, error) {
	if event == nil || event.LocalPipeID != localPipeID || event.RecipientAgentID != recipientID {
		return false, ErrFederatedReceiptInvalid
	}
	var replayed bool
	err := s.runPipelineTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		binding, err := tx.GetFederatedReceiptInbound(ctx, localPipeID)
		if err != nil || !sameReceiptBinding(*binding, event.FederatedReceiptBinding) {
			return ErrFederatedReceiptInvalid
		}
		msg, err := tx.GetPipeline(ctx, localPipeID)
		if err != nil || msg.SourceChainID == "" || msg.DestinationChainID != "" || msg.ToAgent != recipientID {
			return ErrFederatedReceiptInvalid
		}
		switch event.Kind {
		case "claimed":
			if msg.Status == "pending" {
				if !msg.ExpiresAt.After(time.Now().UTC()) {
					return ErrFederatedReceiptConflict
				}
				if err := tx.ClaimPipeline(ctx, localPipeID, recipientID); err != nil {
					return err
				}
			} else if msg.ClaimedBy != recipientID {
				return ErrFederatedReceiptConflict
			}
		case "read":
			if msg.ClaimedBy != recipientID || (msg.Status != "claimed" && msg.Status != "completed") {
				return ErrFederatedReceiptConflict
			}
		default:
			return ErrFederatedReceiptInvalid
		}
		replayed, err = tx.insertFederatedReceiptOutbox(ctx, event)
		return err
	})
	return replayed, err
}

func (s *SQLiteStore) ListPendingFederatedReceiptOutbox(
	ctx context.Context, now time.Time, limit int,
) ([]*FederatedReceiptOutbox, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	rows, err := s.conn.QueryContext(ctx, `SELECT event_id,message_id,local_pipe_id,
		remote_chain_id,recipient_pipe_id,event_kind,sender_chain_id,recipient_chain_id,
		sender_agent_id,recipient_agent_id,content_digest,policy_epoch,agreement_id,contact_id,
		contact_revision,authorization_mode,relation_digest,event_at,proof_envelope,state,
		attempts,next_attempt_at,created_at,expires_at,delivered_at,last_error
		FROM pipeline_receipt_v2_outbox WHERE state='pending'
		AND julianday(next_attempt_at)<=julianday(?) AND julianday(expires_at)>julianday(?)
		ORDER BY next_attempt_at,created_at LIMIT ?`, formatTime(now), formatTime(now), limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make([]*FederatedReceiptOutbox, 0)
	for rows.Next() {
		var event FederatedReceiptOutbox
		var remoteChainID, eventAt, proofEnvelope, next, created, expires string
		var delivered *string
		if err := rows.Scan(&event.EventID, &event.MessageID, &event.LocalPipeID,
			&remoteChainID, &event.RecipientPipeID, &event.Kind, &event.SenderChainID,
			&event.RecipientChainID, &event.SenderAgentID, &event.RecipientAgentID,
			&event.ContentDigest, &event.PolicyEpoch, &event.AgreementID, &event.ContactID,
			&event.ContactRevision, &event.AuthorizationMode, &event.RelationDigest,
			&eventAt, &proofEnvelope, &event.State, &event.Attempts, &next, &created,
			&expires, &delivered, &event.LastError); err != nil {
			return nil, err
		}
		if remoteChainID != event.SenderChainID {
			return nil, ErrFederatedReceiptConflict
		}
		plaintext, err := s.decryptContent(proofEnvelope)
		if err != nil {
			return nil, err
		}
		if plaintext == VaultLockedPlaceholder {
			return nil, ErrPipeContentUnavailable
		}
		var envelope pipelineProofEnvelope
		if json.Unmarshal([]byte(plaintext), &envelope) != nil || envelope.Version != pipelineProofEnvelopeVersion {
			return nil, ErrFederatedReceiptConflict
		}
		event.Proof = envelope.Proof
		event.EventAt, event.NextAttemptAt = parseTime(eventAt), parseTime(next)
		event.CreatedAt, event.ExpiresAt = parseTime(created), parseTime(expires)
		event.DeliveredAt = parseTimePtr(delivered)
		if err := validateFederatedReceiptOutbox(&event); err != nil {
			return nil, err
		}
		out = append(out, &event)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) MarkFederatedReceiptOutboxDelivered(ctx context.Context, eventID string) error {
	res, err := s.writeExecContext(ctx, `UPDATE pipeline_receipt_v2_outbox SET state='delivered',
		delivered_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'),last_error=''
		WHERE event_id=? AND state='pending'`, eventID)
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows != 1 {
		return ErrFederatedReceiptConflict
	}
	return nil
}

func (s *SQLiteStore) RecordFederatedReceiptOutboxFailure(
	ctx context.Context, eventID, detail string, next time.Time, terminalState string,
) error {
	if len(detail) > 1000 {
		detail = detail[:1000]
	}
	state := "pending"
	if terminalState != "" {
		if terminalState != "failed" && terminalState != "unsupported" {
			return ErrFederatedReceiptInvalid
		}
		state = terminalState
	}
	res, err := s.writeExecContext(ctx, `UPDATE pipeline_receipt_v2_outbox SET state=?,
		attempts=attempts+1,next_attempt_at=?,last_error=? WHERE event_id=? AND state='pending'`,
		state, formatTime(next), detail, eventID)
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows != 1 {
		return ErrFederatedReceiptConflict
	}
	return nil
}

// GetFederatedReceiptInbound returns the immutable v2 negotiation binding
// admitted atomically with an imported pipeline row. A missing row means the
// original send was legacy/unconfirmed; callers must not synthesize receipts.
func (s *SQLiteStore) GetFederatedReceiptInbound(ctx context.Context, localPipeID string) (*FederatedReceiptBinding, error) {
	var b FederatedReceiptBinding
	err := s.conn.QueryRowContext(ctx, `SELECT message_id,local_pipe_id,sender_chain_id,
		recipient_chain_id,sender_agent_id,recipient_agent_id,content_digest,policy_epoch,
		agreement_id,contact_id,contact_revision,authorization_mode,relation_digest
		FROM pipeline_receipt_v2_inbound WHERE local_pipe_id=?`, localPipeID).Scan(
		&b.MessageID, &b.LocalPipeID, &b.SenderChainID, &b.RecipientChainID,
		&b.SenderAgentID, &b.RecipientAgentID, &b.ContentDigest, &b.PolicyEpoch,
		&b.AgreementID, &b.ContactID, &b.ContactRevision, &b.AuthorizationMode,
		&b.RelationDigest,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrFederatedReceiptNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := validateFederatedReceiptBinding(b); err != nil {
		return nil, err
	}
	return &b, nil
}

func validReceiptDigest(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func validReceiptAgentID(value string) bool {
	return validReceiptDigest(value)
}

func validateFederatedReceiptBinding(b FederatedReceiptBinding) error {
	if b.MessageID == "" || len(b.MessageID) > 200 || b.LocalPipeID == "" || len(b.LocalPipeID) > 200 ||
		b.SenderChainID == "" || len(b.SenderChainID) > 256 ||
		b.RecipientChainID == "" || len(b.RecipientChainID) > 256 || b.SenderChainID == b.RecipientChainID ||
		!validReceiptAgentID(b.SenderAgentID) || !validReceiptAgentID(b.RecipientAgentID) || b.SenderAgentID == b.RecipientAgentID ||
		b.PolicyEpoch == "" || len(b.PolicyEpoch) > 256 ||
		!validReceiptDigest(b.ContentDigest) || !validReceiptDigest(b.AgreementID) ||
		!validReceiptDigest(b.ContactID) || !validReceiptDigest(b.ContactRevision) {
		return ErrFederatedReceiptInvalid
	}
	if b.AuthorizationMode == "linked-v23" {
		if !validReceiptDigest(b.RelationDigest) {
			return ErrFederatedReceiptInvalid
		}
	} else if b.AuthorizationMode != "" || b.RelationDigest != "" {
		return ErrFederatedReceiptInvalid
	}
	return nil
}

func validateFederatedReceiptEvent(event *FederatedReceiptEvent) error {
	if event == nil || event.EventID == "" || len(event.EventID) > 200 ||
		(event.Kind != "claimed" && event.Kind != "read") || event.EventAt.IsZero() ||
		len(event.ProofHash) != sha256.Size {
		return ErrFederatedReceiptInvalid
	}
	return validateFederatedReceiptBinding(event.FederatedReceiptBinding)
}

// migrateFederatedPipelineReceiptsV2 is fatal: v2 must never activate against
// a historical partial table. Legacy pipeline rows are deliberately not
// backfilled because older transport carries no generation-bound evidence.
func (s *SQLiteStore) migrateFederatedPipelineReceiptsV2(ctx context.Context) error {
	if err := s.addSQLiteColumnIfMissing(ctx, "pipeline_transport_outbox", "receipt_protocol_version",
		`ALTER TABLE pipeline_transport_outbox ADD COLUMN receipt_protocol_version INTEGER NOT NULL DEFAULT 0 CHECK (receipt_protocol_version IN (0,2))`); err != nil {
		return fmt.Errorf("migrate federated receipt v2 negotiation binding: %w", err)
	}
	statements := []string{
		`CREATE TABLE IF NOT EXISTS pipeline_receipt_v2_inbound (
			message_id TEXT PRIMARY KEY,
			local_pipe_id TEXT NOT NULL UNIQUE,
			sender_chain_id TEXT NOT NULL,
			recipient_chain_id TEXT NOT NULL,
			sender_agent_id TEXT NOT NULL,
			recipient_agent_id TEXT NOT NULL,
			content_digest TEXT NOT NULL,
			policy_epoch TEXT NOT NULL,
			agreement_id TEXT NOT NULL,
			contact_id TEXT NOT NULL,
			contact_revision TEXT NOT NULL,
			authorization_mode TEXT NOT NULL DEFAULT '',
			relation_digest TEXT NOT NULL DEFAULT '',
			protocol_version INTEGER NOT NULL CHECK (protocol_version = 2),
			admitted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
			FOREIGN KEY(local_pipe_id) REFERENCES pipeline_messages(pipe_id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS pipeline_receipt_v2_events (
			event_id TEXT PRIMARY KEY,
			message_id TEXT NOT NULL,
			local_pipe_id TEXT NOT NULL,
			sender_chain_id TEXT NOT NULL,
			recipient_chain_id TEXT NOT NULL,
			sender_agent_id TEXT NOT NULL,
			recipient_agent_id TEXT NOT NULL,
			content_digest TEXT NOT NULL,
			policy_epoch TEXT NOT NULL,
			agreement_id TEXT NOT NULL,
			contact_id TEXT NOT NULL,
			contact_revision TEXT NOT NULL,
			authorization_mode TEXT NOT NULL DEFAULT '',
			relation_digest TEXT NOT NULL DEFAULT '',
			event_kind TEXT NOT NULL CHECK (event_kind IN ('claimed','read')),
			event_at TEXT NOT NULL,
			proof_hash BLOB NOT NULL UNIQUE,
			accepted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
			UNIQUE(message_id,event_kind),
			FOREIGN KEY(local_pipe_id) REFERENCES pipeline_messages(pipe_id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS pipeline_receipt_v2_projection (
			message_id TEXT PRIMARY KEY,
			local_pipe_id TEXT NOT NULL UNIQUE,
			sender_chain_id TEXT NOT NULL,
			recipient_chain_id TEXT NOT NULL,
			sender_agent_id TEXT NOT NULL,
			recipient_agent_id TEXT NOT NULL,
			content_digest TEXT NOT NULL,
			policy_epoch TEXT NOT NULL,
			agreement_id TEXT NOT NULL,
			contact_id TEXT NOT NULL,
			contact_revision TEXT NOT NULL,
			authorization_mode TEXT NOT NULL DEFAULT '',
			relation_digest TEXT NOT NULL DEFAULT '',
			delivered_at TEXT,
			delivery_evidence TEXT NOT NULL DEFAULT '',
			claimed_at TEXT,
			read_at TEXT,
			terminal_kind TEXT,
			terminal_at TEXT,
			updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
			FOREIGN KEY(local_pipe_id) REFERENCES pipeline_messages(pipe_id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_pipeline_receipt_v2_sender
			ON pipeline_receipt_v2_projection(sender_agent_id,local_pipe_id)`,
		`CREATE TABLE IF NOT EXISTS pipeline_receipt_v2_outbox (
			event_id TEXT PRIMARY KEY,
			message_id TEXT NOT NULL,
			local_pipe_id TEXT NOT NULL,
			remote_chain_id TEXT NOT NULL,
			recipient_pipe_id TEXT NOT NULL,
			event_kind TEXT NOT NULL CHECK (event_kind IN ('claimed','read')),
			sender_chain_id TEXT NOT NULL,
			recipient_chain_id TEXT NOT NULL,
			sender_agent_id TEXT NOT NULL,
			recipient_agent_id TEXT NOT NULL,
			content_digest TEXT NOT NULL,
			policy_epoch TEXT NOT NULL,
			agreement_id TEXT NOT NULL,
			contact_id TEXT NOT NULL,
			contact_revision TEXT NOT NULL,
			authorization_mode TEXT NOT NULL DEFAULT '',
			relation_digest TEXT NOT NULL DEFAULT '',
			event_at TEXT NOT NULL,
			proof_envelope TEXT NOT NULL,
			state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','delivered','unsupported','failed')),
			attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
			next_attempt_at TEXT NOT NULL,
			created_at TEXT NOT NULL,
			expires_at TEXT NOT NULL,
			delivered_at TEXT,
			last_error TEXT NOT NULL DEFAULT '',
			UNIQUE(message_id,event_kind),
			FOREIGN KEY(local_pipe_id) REFERENCES pipeline_messages(pipe_id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_pipeline_receipt_v2_outbox_pending
			ON pipeline_receipt_v2_outbox(state,next_attempt_at,expires_at)`,
	}
	for _, statement := range statements {
		if _, err := s.writeExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate federated pipeline receipts v2: %w", err)
		}
	}
	for table, columns := range map[string][]string{
		"pipeline_receipt_v2_inbound":    {"message_id", "local_pipe_id", "sender_chain_id", "recipient_chain_id", "sender_agent_id", "recipient_agent_id", "content_digest", "policy_epoch", "agreement_id", "contact_id", "contact_revision", "authorization_mode", "relation_digest", "protocol_version", "admitted_at"},
		"pipeline_receipt_v2_events":     {"event_id", "message_id", "local_pipe_id", "sender_chain_id", "recipient_chain_id", "sender_agent_id", "recipient_agent_id", "content_digest", "policy_epoch", "agreement_id", "contact_id", "contact_revision", "authorization_mode", "relation_digest", "event_kind", "event_at", "proof_hash", "accepted_at"},
		"pipeline_receipt_v2_projection": {"message_id", "local_pipe_id", "sender_chain_id", "recipient_chain_id", "sender_agent_id", "recipient_agent_id", "content_digest", "policy_epoch", "agreement_id", "contact_id", "contact_revision", "authorization_mode", "relation_digest", "delivered_at", "delivery_evidence", "claimed_at", "read_at", "terminal_kind", "terminal_at", "updated_at"},
		"pipeline_receipt_v2_outbox":     {"event_id", "message_id", "local_pipe_id", "remote_chain_id", "recipient_pipe_id", "event_kind", "sender_chain_id", "recipient_chain_id", "sender_agent_id", "recipient_agent_id", "content_digest", "policy_epoch", "agreement_id", "contact_id", "contact_revision", "authorization_mode", "relation_digest", "event_at", "proof_envelope", "state", "attempts", "next_attempt_at", "created_at", "expires_at", "delivered_at", "last_error"},
	} {
		for _, column := range columns {
			exists, err := s.sqliteTableHasColumn(ctx, table, column)
			if err != nil || !exists {
				return fmt.Errorf("verify federated receipt v2 schema %s.%s: %w", table, column, err)
			}
		}
	}
	for table, fragments := range map[string][]string{
		"pipeline_receipt_v2_inbound": {
			"message_id text primary key", "local_pipe_id text not null unique",
			"check (protocol_version = 2)",
			"foreign key(local_pipe_id) references pipeline_messages(pipe_id) on delete cascade",
		},
		"pipeline_receipt_v2_events": {
			"event_id text primary key", "proof_hash blob not null unique",
			"check (event_kind in ('claimed','read'))", "unique(message_id,event_kind)",
			"foreign key(local_pipe_id) references pipeline_messages(pipe_id) on delete cascade",
		},
		"pipeline_receipt_v2_projection": {
			"message_id text primary key", "local_pipe_id text not null unique",
			"foreign key(local_pipe_id) references pipeline_messages(pipe_id) on delete cascade",
		},
		"pipeline_receipt_v2_outbox": {
			"event_id text primary key", "check (event_kind in ('claimed','read'))",
			"check (state in ('pending','delivered','unsupported','failed'))",
			"unique(message_id,event_kind)",
			"foreign key(local_pipe_id) references pipeline_messages(pipe_id) on delete cascade",
		},
	} {
		if err := s.verifyReceiptV2TableConstraints(ctx, table, fragments); err != nil {
			return err
		}
	}
	if exists, err := s.sqliteTableHasColumn(ctx, "pipeline_transport_outbox", "receipt_protocol_version"); err != nil || !exists {
		return fmt.Errorf("verify federated receipt v2 negotiation binding: %w", err)
	}
	if err := s.verifyReceiptV2TableConstraints(ctx, "pipeline_transport_outbox", []string{
		"check (receipt_protocol_version in (0,2))",
	}); err != nil {
		return fmt.Errorf("verify federated receipt v2 negotiation constraint: %w", err)
	}
	var indexSQL string
	if err := s.conn.QueryRowContext(ctx,
		`SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_pipeline_receipt_v2_sender'`).
		Scan(&indexSQL); err != nil || !strings.Contains(strings.ToLower(indexSQL),
		"on pipeline_receipt_v2_projection(sender_agent_id,local_pipe_id)") {
		return fmt.Errorf("verify federated receipt v2 sender index: %w", err)
	}
	if err := s.conn.QueryRowContext(ctx,
		`SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_pipeline_receipt_v2_outbox_pending'`).
		Scan(&indexSQL); err != nil || !strings.Contains(strings.ToLower(indexSQL),
		"on pipeline_receipt_v2_outbox(state,next_attempt_at,expires_at)") {
		return fmt.Errorf("verify federated receipt v2 outbox index: %w", err)
	}
	return nil
}

func (s *SQLiteStore) verifyReceiptV2TableConstraints(
	ctx context.Context, table string, fragments []string,
) error {
	var tableSQL string
	if err := s.conn.QueryRowContext(ctx,
		`SELECT sql FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&tableSQL); err != nil {
		return fmt.Errorf("verify federated receipt v2 table %s: %w", table, err)
	}
	normalized := strings.ToLower(strings.Join(strings.Fields(tableSQL), " "))
	normalized = strings.ReplaceAll(normalized, " (", "(")
	normalized = strings.ReplaceAll(normalized, ", ", ",")
	for _, fragment := range fragments {
		want := strings.ToLower(strings.Join(strings.Fields(fragment), " "))
		want = strings.ReplaceAll(want, " (", "(")
		want = strings.ReplaceAll(want, ", ", ",")
		if !strings.Contains(normalized, want) {
			return fmt.Errorf("verify federated receipt v2 table %s: required constraint %q is absent", table, fragment)
		}
	}
	return nil
}

func sameReceiptBinding(a, b FederatedReceiptBinding) bool {
	return a == b
}

func scanReceiptEvent(row *sql.Row) (*FederatedReceiptEvent, error) {
	var event FederatedReceiptEvent
	var eventAt string
	err := row.Scan(&event.EventID, &event.MessageID, &event.LocalPipeID,
		&event.SenderChainID, &event.RecipientChainID, &event.SenderAgentID,
		&event.RecipientAgentID, &event.ContentDigest, &event.PolicyEpoch,
		&event.AgreementID, &event.ContactID, &event.ContactRevision,
		&event.AuthorizationMode, &event.RelationDigest, &event.Kind, &eventAt,
		&event.ProofHash)
	if err != nil {
		return nil, err
	}
	event.EventAt = parseTime(eventAt)
	return &event, nil
}

// CheckFederatedReceiptEventReplay distinguishes an exact already-accepted
// retry from a new transition without mutating state. Callers may return an
// exact replay after relationship revocation, but must revalidate every new
// transition before ApplyFederatedReceiptEvent.
func (s *SQLiteStore) CheckFederatedReceiptEventReplay(ctx context.Context, event *FederatedReceiptEvent) (bool, error) {
	if err := validateFederatedReceiptEvent(event); err != nil {
		return false, err
	}
	prior, err := scanReceiptEvent(s.conn.QueryRowContext(ctx,
		`SELECT event_id,message_id,local_pipe_id,sender_chain_id,recipient_chain_id,
		 sender_agent_id,recipient_agent_id,content_digest,policy_epoch,agreement_id,
		 contact_id,contact_revision,authorization_mode,relation_digest,event_kind,event_at,proof_hash
		 FROM pipeline_receipt_v2_events WHERE message_id=? AND event_kind=?`,
		event.MessageID, event.Kind))
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if prior.EventID != event.EventID || !sameReceiptBinding(prior.FederatedReceiptBinding, event.FederatedReceiptBinding) ||
		!prior.EventAt.Equal(event.EventAt) || !bytes.Equal(prior.ProofHash, event.ProofHash) {
		return false, ErrFederatedReceiptConflict
	}
	return true, nil
}

func validateOutboundReceiptPipe(ctx context.Context, s *SQLiteStore, b FederatedReceiptBinding) error {
	var localFrom, localTo, destinationChain string
	if err := s.conn.QueryRowContext(ctx,
		`SELECT from_agent,to_agent,destination_chain_id FROM pipeline_messages WHERE pipe_id=?`,
		b.LocalPipeID).Scan(&localFrom, &localTo, &destinationChain); err != nil ||
		localFrom != b.SenderAgentID || localTo != b.RecipientAgentID ||
		destinationChain != b.RecipientChainID {
		return ErrFederatedReceiptInvalid
	}
	return nil
}

// RecordFederatedReceiptDelivery stores the sender node's durable evidence
// that the authenticated peer accepted the original message. It is not read
// evidence and never advances claimed/read dimensions.
func (s *SQLiteStore) RecordFederatedReceiptDelivery(
	ctx context.Context, binding FederatedReceiptBinding, deliveredAt time.Time,
) (bool, error) {
	if err := validateFederatedReceiptBinding(binding); err != nil || deliveredAt.IsZero() {
		return false, ErrFederatedReceiptInvalid
	}
	var replayed bool
	err := s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		if err := validateOutboundReceiptPipe(ctx, tx, binding); err != nil {
			return err
		}
		var priorDelivered sql.NullString
		priorErr := tx.conn.QueryRowContext(ctx,
			`SELECT delivered_at FROM pipeline_receipt_v2_projection WHERE message_id=?`,
			binding.MessageID).Scan(&priorDelivered)
		if priorErr != nil && !errors.Is(priorErr, sql.ErrNoRows) {
			return priorErr
		}
		result, err := tx.writeExecContext(ctx, `INSERT INTO pipeline_receipt_v2_projection
			(message_id,local_pipe_id,sender_chain_id,recipient_chain_id,sender_agent_id,
			 recipient_agent_id,content_digest,policy_epoch,agreement_id,contact_id,contact_revision,
			 authorization_mode,relation_digest,delivered_at,delivery_evidence)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(message_id) DO UPDATE SET
			 delivered_at=CASE WHEN delivered_at IS NULL THEN excluded.delivered_at ELSE delivered_at END,
			 delivery_evidence=CASE WHEN delivery_evidence='' THEN excluded.delivery_evidence ELSE delivery_evidence END,
			 updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
			WHERE local_pipe_id=excluded.local_pipe_id AND sender_chain_id=excluded.sender_chain_id
			 AND recipient_chain_id=excluded.recipient_chain_id AND sender_agent_id=excluded.sender_agent_id
			 AND recipient_agent_id=excluded.recipient_agent_id AND content_digest=excluded.content_digest
			 AND policy_epoch=excluded.policy_epoch AND agreement_id=excluded.agreement_id
			 AND contact_id=excluded.contact_id AND contact_revision=excluded.contact_revision
			 AND authorization_mode=excluded.authorization_mode AND relation_digest=excluded.relation_digest`,
			binding.MessageID, binding.LocalPipeID, binding.SenderChainID, binding.RecipientChainID,
			binding.SenderAgentID, binding.RecipientAgentID, binding.ContentDigest, binding.PolicyEpoch,
			binding.AgreementID, binding.ContactID, binding.ContactRevision, binding.AuthorizationMode,
			binding.RelationDigest, formatTime(deliveredAt), "peer_operator_durable_admission")
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil || changed != 1 {
			return ErrFederatedReceiptConflict
		}
		replayed = priorErr == nil && priorDelivered.Valid
		return nil
	})
	return replayed, err
}

// ApplyFederatedReceiptEvent atomically accepts one exact event and advances
// only its independent write-once projection dimension. Exact retries are
// idempotent; conflicting same-kind or proof reuse fails closed.
func (s *SQLiteStore) ApplyFederatedReceiptEvent(ctx context.Context, event *FederatedReceiptEvent) (bool, error) {
	if err := validateFederatedReceiptEvent(event); err != nil {
		return false, err
	}
	var replayed bool
	err := s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		prior, err := scanReceiptEvent(tx.conn.QueryRowContext(ctx,
			`SELECT event_id,message_id,local_pipe_id,sender_chain_id,recipient_chain_id,
			 sender_agent_id,recipient_agent_id,content_digest,policy_epoch,agreement_id,
			 contact_id,contact_revision,authorization_mode,relation_digest,event_kind,event_at,proof_hash
			 FROM pipeline_receipt_v2_events WHERE message_id=? AND event_kind=?`,
			event.MessageID, event.Kind))
		switch {
		case err == nil:
			if prior.EventID != event.EventID || !sameReceiptBinding(prior.FederatedReceiptBinding, event.FederatedReceiptBinding) ||
				!prior.EventAt.Equal(event.EventAt) || !bytes.Equal(prior.ProofHash, event.ProofHash) {
				return ErrFederatedReceiptConflict
			}
			replayed = true
			return nil
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}
		var proofEvent string
		if err := tx.conn.QueryRowContext(ctx,
			`SELECT event_id FROM pipeline_receipt_v2_events WHERE proof_hash=?`, event.ProofHash).Scan(&proofEvent); err == nil {
			return ErrFederatedReceiptConflict
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err := validateOutboundReceiptPipe(ctx, tx, event.FederatedReceiptBinding); err != nil {
			return err
		}
		_, err = tx.writeExecContext(ctx, `INSERT INTO pipeline_receipt_v2_events
			(event_id,message_id,local_pipe_id,sender_chain_id,recipient_chain_id,sender_agent_id,
			 recipient_agent_id,content_digest,policy_epoch,agreement_id,contact_id,contact_revision,
			 authorization_mode,relation_digest,event_kind,event_at,proof_hash)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, event.EventID, event.MessageID, event.LocalPipeID,
			event.SenderChainID, event.RecipientChainID, event.SenderAgentID, event.RecipientAgentID,
			event.ContentDigest, event.PolicyEpoch, event.AgreementID, event.ContactID,
			event.ContactRevision, event.AuthorizationMode, event.RelationDigest, event.Kind,
			formatTime(event.EventAt), event.ProofHash)
		if err != nil {
			return err
		}
		result, err := tx.writeExecContext(ctx, `INSERT INTO pipeline_receipt_v2_projection
			(message_id,local_pipe_id,sender_chain_id,recipient_chain_id,sender_agent_id,
			 recipient_agent_id,content_digest,policy_epoch,agreement_id,contact_id,contact_revision,
			 authorization_mode,relation_digest,claimed_at,read_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(message_id) DO UPDATE SET
			 claimed_at=CASE WHEN excluded.claimed_at IS NOT NULL AND claimed_at IS NULL THEN excluded.claimed_at ELSE claimed_at END,
			 read_at=CASE WHEN excluded.read_at IS NOT NULL AND read_at IS NULL THEN excluded.read_at ELSE read_at END,
			 updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
			WHERE local_pipe_id=excluded.local_pipe_id AND sender_chain_id=excluded.sender_chain_id
			 AND recipient_chain_id=excluded.recipient_chain_id AND sender_agent_id=excluded.sender_agent_id
			 AND recipient_agent_id=excluded.recipient_agent_id AND content_digest=excluded.content_digest
			 AND policy_epoch=excluded.policy_epoch AND agreement_id=excluded.agreement_id
			 AND contact_id=excluded.contact_id AND contact_revision=excluded.contact_revision
			 AND authorization_mode=excluded.authorization_mode AND relation_digest=excluded.relation_digest`,
			event.MessageID, event.LocalPipeID, event.SenderChainID, event.RecipientChainID,
			event.SenderAgentID, event.RecipientAgentID, event.ContentDigest, event.PolicyEpoch,
			event.AgreementID, event.ContactID, event.ContactRevision, event.AuthorizationMode,
			event.RelationDigest,
			nullableReceiptTime(event.Kind == "claimed", event.EventAt),
			nullableReceiptTime(event.Kind == "read", event.EventAt))
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return ErrFederatedReceiptConflict
		}
		return nil
	})
	return replayed, err
}

func nullableReceiptTime(include bool, value time.Time) any {
	if !include {
		return nil
	}
	return formatTime(value)
}

// RecordFederatedReceiptTerminal advances only the transport terminal
// dimension. It never erases peer admission, claim, or exact-recipient read
// evidence, and a different second terminal reason is a conflict rather than a
// last-writer-wins rewrite.
func (s *SQLiteStore) RecordFederatedReceiptTerminal(
	ctx context.Context, binding FederatedReceiptBinding, kind string, terminalAt time.Time,
) (bool, error) {
	if err := validateFederatedReceiptBinding(binding); err != nil || terminalAt.IsZero() ||
		(kind != "failed" && kind != "expired" && kind != "revoked") {
		return false, ErrFederatedReceiptInvalid
	}
	var replayed bool
	err := s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		if err := validateOutboundReceiptPipe(ctx, tx, binding); err != nil {
			return err
		}
		var priorKind string
		var priorAt sql.NullString
		priorErr := tx.conn.QueryRowContext(ctx,
			`SELECT COALESCE(terminal_kind,''),terminal_at FROM pipeline_receipt_v2_projection WHERE message_id=?`,
			binding.MessageID).Scan(&priorKind, &priorAt)
		if priorErr != nil && !errors.Is(priorErr, sql.ErrNoRows) {
			return priorErr
		}
		if priorErr == nil && priorKind != "" {
			if priorKind != kind || !priorAt.Valid || !parseTime(priorAt.String).Equal(terminalAt) {
				return ErrFederatedReceiptConflict
			}
			replayed = true
			return nil
		}
		result, err := tx.writeExecContext(ctx, `INSERT INTO pipeline_receipt_v2_projection
			(message_id,local_pipe_id,sender_chain_id,recipient_chain_id,sender_agent_id,
			 recipient_agent_id,content_digest,policy_epoch,agreement_id,contact_id,contact_revision,
			 authorization_mode,relation_digest,terminal_kind,terminal_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(message_id) DO UPDATE SET
			 terminal_kind=CASE WHEN terminal_kind IS NULL OR terminal_kind='' THEN excluded.terminal_kind ELSE terminal_kind END,
			 terminal_at=CASE WHEN terminal_at IS NULL THEN excluded.terminal_at ELSE terminal_at END,
			 updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
			WHERE local_pipe_id=excluded.local_pipe_id AND sender_chain_id=excluded.sender_chain_id
			 AND recipient_chain_id=excluded.recipient_chain_id AND sender_agent_id=excluded.sender_agent_id
			 AND recipient_agent_id=excluded.recipient_agent_id AND content_digest=excluded.content_digest
			 AND policy_epoch=excluded.policy_epoch AND agreement_id=excluded.agreement_id
			 AND contact_id=excluded.contact_id AND contact_revision=excluded.contact_revision
			 AND authorization_mode=excluded.authorization_mode AND relation_digest=excluded.relation_digest
			 AND (terminal_kind IS NULL OR terminal_kind='')`, binding.MessageID, binding.LocalPipeID,
			binding.SenderChainID, binding.RecipientChainID, binding.SenderAgentID,
			binding.RecipientAgentID, binding.ContentDigest, binding.PolicyEpoch, binding.AgreementID,
			binding.ContactID, binding.ContactRevision, binding.AuthorizationMode,
			binding.RelationDigest, kind, formatTime(terminalAt))
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil || rows != 1 {
			return ErrFederatedReceiptConflict
		}
		return nil
	})
	return replayed, err
}

// GetFederatedReceiptForSender is exact-sender scoped and payload-free.
func (s *SQLiteStore) GetFederatedReceiptForSender(ctx context.Context, senderID, localPipeID string) (*FederatedReceiptProjection, error) {
	var out FederatedReceiptProjection
	var deliveredAt, claimedAt, readAt, terminalAt *string
	var updatedAt string
	err := s.conn.QueryRowContext(ctx, `SELECT r.message_id,r.local_pipe_id,r.sender_chain_id,
		r.recipient_chain_id,r.sender_agent_id,r.recipient_agent_id,r.content_digest,r.policy_epoch,
		r.agreement_id,r.contact_id,r.contact_revision,r.authorization_mode,r.relation_digest,
		r.delivered_at,r.delivery_evidence,r.claimed_at,r.read_at,
		COALESCE(r.terminal_kind,''),r.terminal_at,r.updated_at
		FROM pipeline_receipt_v2_projection r
		JOIN pipeline_messages p ON p.pipe_id=r.local_pipe_id
		WHERE r.local_pipe_id=? AND r.sender_agent_id=? AND p.from_agent=?
		  AND p.source_chain_id='' AND p.destination_chain_id!=''`,
		localPipeID, senderID, senderID).Scan(&out.MessageID, &out.LocalPipeID,
		&out.SenderChainID, &out.RecipientChainID, &out.SenderAgentID, &out.RecipientAgentID,
		&out.ContentDigest, &out.PolicyEpoch, &out.AgreementID, &out.ContactID,
		&out.ContactRevision, &out.AuthorizationMode, &out.RelationDigest,
		&deliveredAt, &out.DeliveryEvidence, &claimedAt, &readAt,
		&out.TerminalKind, &terminalAt, &updatedAt)
	if err != nil {
		return nil, ErrFederatedReceiptNotFound
	}
	out.DeliveredAt = parseTimePtr(deliveredAt)
	out.ClaimedAt = parseTimePtr(claimedAt)
	out.ReadAt = parseTimePtr(readAt)
	out.TerminalAt = parseTimePtr(terminalAt)
	out.UpdatedAt = parseTime(updatedAt)
	return &out, nil
}
