package store

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrPipelineTransportEquivocation = errors.New("federated pipeline event reuses an id with different content")
	ErrPipelineTransportReplay       = errors.New("federated pipeline agent proof was already used")
)

const maxPipelineProofBytes = 1 << 20

const pipelineProofEnvelopeVersion = 1

type pipelineProofEnvelope struct {
	Version int                `json:"version"`
	Proof   PipelineAgentProof `json:"proof"`
}

func (s *SQLiteStore) migratePipelineTransport(ctx context.Context) {
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS pipeline_transport_outbox (
		event_id          TEXT PRIMARY KEY,
		pipe_id           TEXT NOT NULL,
		remote_chain_id   TEXT NOT NULL,
		event_kind        TEXT NOT NULL CHECK (event_kind IN ('send','result','claim','failure','expiry','revoke')),
		policy_epoch      TEXT NOT NULL,
		agreement_id      TEXT NOT NULL,
		contact_id        TEXT NOT NULL,
		contact_revision  TEXT NOT NULL,
		source_agent_id   TEXT NOT NULL,
		target_agent_id   TEXT NOT NULL,
		proof_signature   BLOB NOT NULL,
		proof_timestamp   INTEGER NOT NULL,
		proof_nonce       BLOB,
		proof_canonical   TEXT NOT NULL,
		state             TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','delivered','failed')),
		attempts          INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
		next_attempt_at   TEXT NOT NULL,
		created_at        TEXT NOT NULL,
		expires_at        TEXT NOT NULL,
		delivered_at      TEXT,
		last_error        TEXT NOT NULL DEFAULT '',
		reported_at       TEXT,
		UNIQUE (pipe_id, event_kind)
	)`)
	_, _ = s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_pipe_transport_pending
		ON pipeline_transport_outbox(state, next_attempt_at, expires_at)`)
	_, _ = s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_pipe_transport_remote
		ON pipeline_transport_outbox(remote_chain_id, state)`)
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS pipeline_transport_dedup (
		remote_chain_id TEXT NOT NULL,
		policy_epoch    TEXT NOT NULL,
		agreement_id    TEXT NOT NULL,
		contact_id      TEXT NOT NULL,
		contact_revision TEXT NOT NULL,
		source_agent_id TEXT NOT NULL,
		target_agent_id TEXT NOT NULL,
		event_kind      TEXT NOT NULL,
		remote_pipe_id  TEXT NOT NULL,
		content_hash    BLOB NOT NULL,
		proof_hash      BLOB NOT NULL,
		local_pipe_id   TEXT NOT NULL,
		outcome         TEXT NOT NULL,
		expires_at      TEXT NOT NULL,
		PRIMARY KEY (remote_chain_id, policy_epoch, agreement_id, source_agent_id, event_kind, remote_pipe_id)
	)`)
	// Development builds may already have created the pre-v11.10 draft table.
	// These additive migrations are harmless on the final schema; errors mean
	// the column already exists and are intentionally ignored.
	_, _ = s.writeExecContext(ctx, `ALTER TABLE pipeline_transport_dedup ADD COLUMN contact_id TEXT NOT NULL DEFAULT ''`)
	_, _ = s.writeExecContext(ctx, `ALTER TABLE pipeline_transport_dedup ADD COLUMN contact_revision TEXT NOT NULL DEFAULT ''`)
	_, _ = s.writeExecContext(ctx, `ALTER TABLE pipeline_transport_dedup ADD COLUMN target_agent_id TEXT NOT NULL DEFAULT ''`)
	_, _ = s.writeExecContext(ctx, `ALTER TABLE pipeline_transport_dedup ADD COLUMN proof_hash BLOB`)
	_, _ = s.writeExecContext(ctx, `ALTER TABLE pipeline_transport_outbox ADD COLUMN reported_at TEXT`)
	_, _ = s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_pipe_transport_dedup_expiry
		ON pipeline_transport_dedup(expires_at)`)
	_, _ = s.writeExecContext(ctx, `DROP INDEX IF EXISTS idx_pipe_transport_proof_once`)
	_, _ = s.writeExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS idx_pipe_transport_proof_once
		ON pipeline_transport_dedup(remote_chain_id, event_kind, proof_hash)
		WHERE proof_hash IS NOT NULL`)
}

func validatePipelineAgentProof(proof PipelineAgentProof, sourceAgentID string) error {
	decoded, err := hex.DecodeString(sourceAgentID)
	if err != nil || len(decoded) != ed25519.PublicKeySize || hex.EncodeToString(decoded) != sourceAgentID ||
		proof.AgentID != sourceAgentID {
		return fmt.Errorf("pipeline source agent proof identity is invalid")
	}
	if len(proof.Signature) != ed25519.SignatureSize || proof.Timestamp <= 0 {
		return fmt.Errorf("pipeline source agent proof signature is invalid")
	}
	if len(proof.Nonce) > 64 || len(proof.CanonicalRequest) == 0 || len(proof.CanonicalRequest) > maxPipelineProofBytes {
		return fmt.Errorf("pipeline source agent proof request is invalid")
	}
	return nil
}

func validatePipelineTransportOutbox(event *PipelineTransportOutbox) error {
	if event == nil || event.EventID == "" || event.PipeID == "" || event.RemoteChainID == "" ||
		event.PolicyEpoch == "" || event.AgreementID == "" || event.ContactID == "" ||
		event.ContactRevision == "" || event.TargetAgentID == "" {
		return fmt.Errorf("federated pipeline transport binding is incomplete")
	}
	switch event.EventKind {
	case "send", "result", "claim", "failure", "expiry", "revoke":
	default:
		return fmt.Errorf("unsupported federated pipeline event kind %q", event.EventKind)
	}
	if err := validatePipelineAgentProof(event.Proof, event.SourceAgentID); err != nil {
		return err
	}
	if event.CreatedAt.IsZero() || event.ExpiresAt.IsZero() || !event.ExpiresAt.After(event.CreatedAt) {
		return fmt.Errorf("federated pipeline transport lifetime is invalid")
	}
	return nil
}

func (s *SQLiteStore) insertPipelineTransport(ctx context.Context, event *PipelineTransportOutbox) error {
	if err := validatePipelineTransportOutbox(event); err != nil {
		return err
	}
	proofEnvelope, err := json.Marshal(pipelineProofEnvelope{Version: pipelineProofEnvelopeVersion, Proof: event.Proof})
	if err != nil {
		return fmt.Errorf("encode pipeline agent proof: %w", err)
	}
	encryptedProof, err := s.encryptContent(string(proofEnvelope))
	if err != nil {
		return fmt.Errorf("encrypt pipeline agent proof: %w", err)
	}
	state := event.State
	if state == "" {
		state = "pending"
	}
	nextAttempt := event.NextAttemptAt
	if nextAttempt.IsZero() {
		nextAttempt = event.CreatedAt
	}
	_, err = s.writeExecContext(ctx, `INSERT INTO pipeline_transport_outbox
		(event_id, pipe_id, remote_chain_id, event_kind, policy_epoch, agreement_id,
		 contact_id, contact_revision, source_agent_id, target_agent_id,
		 proof_signature, proof_timestamp, proof_nonce, proof_canonical,
		 state, attempts, next_attempt_at, created_at, expires_at, last_error)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		event.EventID, event.PipeID, event.RemoteChainID, event.EventKind, event.PolicyEpoch,
		event.AgreementID, event.ContactID, event.ContactRevision, event.SourceAgentID,
		event.TargetAgentID, []byte{}, int64(0), []byte{},
		encryptedProof, state, event.Attempts, formatTime(nextAttempt), formatTime(event.CreatedAt),
		formatTime(event.ExpiresAt), event.LastError)
	return err
}

func (s *SQLiteStore) InsertPipelineWithTransport(ctx context.Context, msg *PipelineMessage, event *PipelineTransportOutbox) error {
	if msg == nil || event == nil || event.PipeID != msg.PipeID || msg.DestinationChainID == "" ||
		event.RemoteChainID != msg.DestinationChainID || event.ContactID != msg.FederationContactID ||
		event.ContactRevision != msg.FederationContactRevision || event.PolicyEpoch != msg.FederationPolicyEpoch ||
		event.AgreementID != msg.FederationAgreementID || event.SourceAgentID != msg.FromAgent || event.TargetAgentID != msg.ToAgent {
		return fmt.Errorf("pipeline row and transport event binding mismatch")
	}
	return s.runPipelineTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		if err := tx.InsertPipeline(ctx, msg); err != nil {
			return err
		}
		return tx.insertPipelineTransport(ctx, event)
	})
}

func validatePipelineTransportDedup(dedup *PipelineTransportDedup) error {
	if dedup == nil || dedup.RemoteChainID == "" || dedup.PolicyEpoch == "" || dedup.AgreementID == "" ||
		dedup.ContactID == "" || dedup.ContactRevision == "" || dedup.SourceAgentID == "" ||
		dedup.TargetAgentID == "" || dedup.EventKind == "" || dedup.RemotePipeID == "" ||
		len(dedup.ContentHash) != 32 || len(dedup.ProofHash) != 32 || dedup.LocalPipeID == "" ||
		dedup.Outcome == "" || dedup.ExpiresAt.IsZero() {
		return fmt.Errorf("federated pipeline dedup binding is incomplete")
	}
	if dedup.EventKind != "send" && dedup.EventKind != "result" {
		return fmt.Errorf("unsupported federated pipeline dedup kind %q", dedup.EventKind)
	}
	return nil
}

func (s *SQLiteStore) AdmitFederatedPipeline(ctx context.Context, msg *PipelineMessage, dedup *PipelineTransportDedup) (localPipeID string, duplicate bool, err error) {
	if msg == nil || msg.SourceChainID == "" || msg.SourcePipeID == "" || msg.DestinationChainID != "" {
		return "", false, fmt.Errorf("imported pipeline provenance is invalid")
	}
	if validationErr := validatePipelineTransportDedup(dedup); validationErr != nil {
		return "", false, validationErr
	}
	if dedup.RemoteChainID != msg.SourceChainID || dedup.RemotePipeID != msg.SourcePipeID ||
		dedup.SourceAgentID != msg.FromAgent || dedup.LocalPipeID != msg.PipeID ||
		dedup.TargetAgentID != msg.ToAgent || dedup.EventKind != "send" ||
		dedup.PolicyEpoch != msg.FederationPolicyEpoch || dedup.AgreementID != msg.FederationAgreementID ||
		dedup.ContactID != msg.FederationContactID || dedup.ContactRevision != msg.FederationContactRevision {
		return "", false, fmt.Errorf("imported pipeline row and dedup binding mismatch")
	}
	err = s.runPipelineTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		var existingHash []byte
		var existingLocalID string
		lookupErr := tx.conn.QueryRowContext(ctx, `SELECT content_hash, local_pipe_id
			FROM pipeline_transport_dedup WHERE remote_chain_id=? AND policy_epoch=? AND agreement_id=?
			AND source_agent_id=? AND event_kind=? AND remote_pipe_id=?`,
			dedup.RemoteChainID, dedup.PolicyEpoch, dedup.AgreementID, dedup.SourceAgentID,
			dedup.EventKind, dedup.RemotePipeID).Scan(&existingHash, &existingLocalID)
		if lookupErr == nil {
			if !bytes.Equal(existingHash, dedup.ContentHash) {
				return ErrPipelineTransportEquivocation
			}
			localPipeID, duplicate = existingLocalID, true
			return nil
		}
		if !errors.Is(lookupErr, sql.ErrNoRows) {
			return fmt.Errorf("read pipeline transport dedup: %w", lookupErr)
		}
		var replayPipeID string
		replayErr := tx.conn.QueryRowContext(ctx, `SELECT local_pipe_id FROM pipeline_transport_dedup
			WHERE remote_chain_id=? AND event_kind=? AND proof_hash=?`,
			dedup.RemoteChainID, dedup.EventKind, dedup.ProofHash).Scan(&replayPipeID)
		if replayErr == nil {
			return fmt.Errorf("%w: proof already admitted as %s", ErrPipelineTransportReplay, replayPipeID)
		}
		if !errors.Is(replayErr, sql.ErrNoRows) {
			return fmt.Errorf("read pipeline proof replay key: %w", replayErr)
		}
		if insertErr := tx.InsertPipeline(ctx, msg); insertErr != nil {
			return insertErr
		}
		if _, execErr := tx.writeExecContext(ctx, `INSERT INTO pipeline_transport_dedup
			(remote_chain_id, policy_epoch, agreement_id, contact_id, contact_revision,
			 source_agent_id, target_agent_id, event_kind, remote_pipe_id, content_hash,
			 proof_hash, local_pipe_id, outcome, expires_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, dedup.RemoteChainID, dedup.PolicyEpoch,
			dedup.AgreementID, dedup.ContactID, dedup.ContactRevision, dedup.SourceAgentID,
			dedup.TargetAgentID, dedup.EventKind, dedup.RemotePipeID, dedup.ContentHash,
			dedup.ProofHash, dedup.LocalPipeID, dedup.Outcome, formatTime(dedup.ExpiresAt)); execErr != nil {
			return fmt.Errorf("insert pipeline transport dedup: %w", execErr)
		}
		localPipeID = msg.PipeID
		return nil
	})
	return localPipeID, duplicate, err
}

// CompleteFederatedPipelineWithTransport atomically records a foreign work
// result and its return event. A crash can therefore produce either neither
// change or a completed row that the durable outbox will retry.
func (s *SQLiteStore) CompleteFederatedPipelineWithTransport(ctx context.Context, pipeID, agentID, result string, event *PipelineTransportOutbox) error {
	if len(result) > MaxPipeContentBytes {
		return ErrPipeResultTooLarge
	}
	if event == nil || event.EventKind != "result" || event.PipeID != pipeID || event.SourceAgentID != agentID {
		return fmt.Errorf("federated result transport binding is incomplete")
	}
	return s.runPipelineTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		msg, err := tx.GetPipeline(ctx, pipeID)
		if err != nil {
			return err
		}
		if msg.SourceChainID == "" || msg.DestinationChainID != "" || msg.SourcePipeID == "" ||
			event.RemoteChainID != msg.SourceChainID || event.PolicyEpoch != msg.FederationPolicyEpoch ||
			event.AgreementID != msg.FederationAgreementID || event.ContactID != msg.FederationContactID ||
			event.ContactRevision != msg.FederationContactRevision || event.TargetAgentID != msg.FromAgent ||
			msg.ToAgent != agentID {
			return fmt.Errorf("foreign pipeline row and result transport binding mismatch")
		}
		if err := tx.CompletePipeline(ctx, pipeID, agentID, result, ""); err != nil {
			return err
		}
		return tx.insertPipelineTransport(ctx, event)
	})
}

// ApplyFederatedPipelineResult atomically deduplicates one authenticated peer
// result and completes the original outbound local pipe. The remote source's
// private imported pipe id is the replay key; local callers continue using the
// original local pipe id.
func (s *SQLiteStore) ApplyFederatedPipelineResult(ctx context.Context, pipeID, result string, dedup *PipelineTransportDedup) (duplicate bool, err error) {
	if len(result) > MaxPipeContentBytes {
		return false, ErrPipeResultTooLarge
	}
	if validationErr := validatePipelineTransportDedup(dedup); validationErr != nil {
		return false, validationErr
	}
	if dedup.EventKind != "result" || dedup.LocalPipeID != pipeID {
		return false, fmt.Errorf("federated result dedup binding mismatch")
	}
	err = s.runPipelineTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		var existingHash []byte
		var existingLocalID string
		lookupErr := tx.conn.QueryRowContext(ctx, `SELECT content_hash, local_pipe_id
			FROM pipeline_transport_dedup WHERE remote_chain_id=? AND policy_epoch=? AND agreement_id=?
			AND source_agent_id=? AND event_kind=? AND remote_pipe_id=?`,
			dedup.RemoteChainID, dedup.PolicyEpoch, dedup.AgreementID, dedup.SourceAgentID,
			dedup.EventKind, dedup.RemotePipeID).Scan(&existingHash, &existingLocalID)
		if lookupErr == nil {
			if !bytes.Equal(existingHash, dedup.ContentHash) || existingLocalID != pipeID {
				return ErrPipelineTransportEquivocation
			}
			duplicate = true
			return nil
		}
		if !errors.Is(lookupErr, sql.ErrNoRows) {
			return fmt.Errorf("read pipeline result dedup: %w", lookupErr)
		}
		var replayPipeID string
		replayErr := tx.conn.QueryRowContext(ctx, `SELECT local_pipe_id FROM pipeline_transport_dedup
			WHERE remote_chain_id=? AND event_kind=? AND proof_hash=?`,
			dedup.RemoteChainID, dedup.EventKind, dedup.ProofHash).Scan(&replayPipeID)
		if replayErr == nil {
			return fmt.Errorf("%w: proof already applied to %s", ErrPipelineTransportReplay, replayPipeID)
		}
		if !errors.Is(replayErr, sql.ErrNoRows) {
			return fmt.Errorf("read pipeline result proof replay key: %w", replayErr)
		}

		msg, getErr := tx.GetPipeline(ctx, pipeID)
		if getErr != nil {
			return getErr
		}
		if msg.SourceChainID != "" || msg.DestinationChainID != dedup.RemoteChainID ||
			msg.ToAgent != dedup.SourceAgentID || msg.FromAgent != dedup.TargetAgentID ||
			msg.FederationPolicyEpoch != dedup.PolicyEpoch || msg.FederationAgreementID != dedup.AgreementID ||
			msg.FederationContactID != dedup.ContactID || msg.FederationContactRevision != dedup.ContactRevision {
			return fmt.Errorf("outbound pipeline row and federated result binding mismatch")
		}
		encryptedResult, encErr := tx.encryptContent(result)
		if encErr != nil {
			return fmt.Errorf("encrypt pipeline result: %w", encErr)
		}
		res, updateErr := tx.writeExecContext(ctx, `UPDATE pipeline_messages SET status='completed', result=?,
			journal_id='', completed_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
			WHERE pipe_id=? AND status IN ('pending','claimed')`, encryptedResult, pipeID)
		if updateErr != nil {
			return updateErr
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return fmt.Errorf("outbound pipeline %s is not awaiting a result", pipeID)
		}
		_, insertErr := tx.writeExecContext(ctx, `INSERT INTO pipeline_transport_dedup
			(remote_chain_id, policy_epoch, agreement_id, contact_id, contact_revision,
			 source_agent_id, target_agent_id, event_kind, remote_pipe_id, content_hash,
			 proof_hash, local_pipe_id, outcome, expires_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, dedup.RemoteChainID, dedup.PolicyEpoch,
			dedup.AgreementID, dedup.ContactID, dedup.ContactRevision, dedup.SourceAgentID,
			dedup.TargetAgentID, dedup.EventKind, dedup.RemotePipeID, dedup.ContentHash,
			dedup.ProofHash, dedup.LocalPipeID, dedup.Outcome, formatTime(dedup.ExpiresAt))
		return insertErr
	})
	return duplicate, err
}

func (s *SQLiteStore) ListPendingPipelineTransport(ctx context.Context, now time.Time, limit int) ([]*PipelineTransportOutbox, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	rows, err := s.conn.QueryContext(ctx, `SELECT event_id, pipe_id, remote_chain_id,
		event_kind, policy_epoch, agreement_id, contact_id, contact_revision,
		source_agent_id, target_agent_id, proof_signature, proof_timestamp,
		COALESCE(proof_nonce, x''), proof_canonical, state, attempts,
		next_attempt_at, created_at, expires_at, delivered_at, last_error
		FROM pipeline_transport_outbox WHERE state='pending'
		AND julianday(next_attempt_at)<=julianday(?) AND julianday(expires_at)>julianday(?)
		ORDER BY next_attempt_at, created_at LIMIT ?`, formatTime(now), formatTime(now), limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make([]*PipelineTransportOutbox, 0)
	for rows.Next() {
		event, err := s.scanPipelineTransport(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, event)
	}
	return out, rows.Err()
}

type pipelineTransportScanner interface {
	Scan(dest ...any) error
}

func (s *SQLiteStore) scanPipelineTransport(scanner pipelineTransportScanner) (*PipelineTransportOutbox, error) {
	var event PipelineTransportOutbox
	var canonical, next, created, expires string
	var legacySignature, legacyNonce []byte
	var legacyTimestamp int64
	var delivered *string
	if err := scanner.Scan(&event.EventID, &event.PipeID, &event.RemoteChainID,
		&event.EventKind, &event.PolicyEpoch, &event.AgreementID, &event.ContactID,
		&event.ContactRevision, &event.SourceAgentID, &event.TargetAgentID,
		&legacySignature, &legacyTimestamp, &legacyNonce,
		&canonical, &event.State, &event.Attempts, &next, &created, &expires,
		&delivered, &event.LastError); err != nil {
		return nil, err
	}
	plaintext, err := s.decryptContent(canonical)
	if err != nil {
		return nil, fmt.Errorf("decrypt pipeline agent proof: %w", err)
	}
	if plaintext == VaultLockedPlaceholder {
		return nil, ErrPipeContentUnavailable
	}
	var envelope pipelineProofEnvelope
	if json.Unmarshal([]byte(plaintext), &envelope) == nil && envelope.Version == pipelineProofEnvelopeVersion &&
		envelope.Proof.AgentID != "" && len(envelope.Proof.CanonicalRequest) != 0 {
		event.Proof = envelope.Proof
	} else {
		// Backward-compatible read of v11.10 development rows that encrypted only
		// the canonical request. New writes leave these legacy tuple columns empty
		// so a copied database cannot verify payload guesses while the vault locks.
		event.Proof = PipelineAgentProof{
			AgentID: event.SourceAgentID, Signature: legacySignature, Timestamp: legacyTimestamp,
			Nonce: legacyNonce, CanonicalRequest: []byte(plaintext),
		}
	}
	if event.Proof.AgentID != event.SourceAgentID {
		return nil, fmt.Errorf("pipeline agent proof identity does not match its transport row")
	}
	event.NextAttemptAt, event.CreatedAt, event.ExpiresAt = parseTime(next), parseTime(created), parseTime(expires)
	event.DeliveredAt = parseTimePtr(delivered)
	return &event, nil
}

func (s *SQLiteStore) GetPipelineTransport(ctx context.Context, eventID string) (*PipelineTransportOutbox, error) {
	row := s.conn.QueryRowContext(ctx, `SELECT event_id, pipe_id, remote_chain_id,
		event_kind, policy_epoch, agreement_id, contact_id, contact_revision,
		source_agent_id, target_agent_id, proof_signature, proof_timestamp,
		COALESCE(proof_nonce, x''), proof_canonical, state, attempts,
		next_attempt_at, created_at, expires_at, delivered_at, last_error
		FROM pipeline_transport_outbox WHERE event_id=?`, eventID)
	event, err := s.scanPipelineTransport(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("pipeline transport event %s not found", eventID)
	}
	return event, err
}

func (s *SQLiteStore) MarkPipelineTransportDelivered(ctx context.Context, eventID string) error {
	res, err := s.writeExecContext(ctx, `UPDATE pipeline_transport_outbox SET state='delivered',
		delivered_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'), last_error=''
		WHERE event_id=? AND state='pending'`, eventID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("pipeline transport event %s is not pending", eventID)
	}
	return nil
}

func (s *SQLiteStore) RecordPipelineTransportFailure(ctx context.Context, eventID, detail string, nextAttempt time.Time, terminal bool) error {
	if len(detail) > 1000 {
		detail = detail[:1000]
	}
	state := "pending"
	if terminal {
		state = "failed"
	}
	return s.runPipelineTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		res, err := tx.writeExecContext(ctx, `UPDATE pipeline_transport_outbox SET state=?,
			attempts=attempts+1, next_attempt_at=?, last_error=? WHERE event_id=? AND state='pending'`,
			state, formatTime(nextAttempt), detail, eventID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return fmt.Errorf("pipeline transport event %s is not pending", eventID)
		}
		if terminal {
			_, err = tx.writeExecContext(ctx, `UPDATE pipeline_messages SET status='failed'
				WHERE pipe_id=(SELECT pipe_id FROM pipeline_transport_outbox WHERE event_id=?)
				AND status IN ('pending','claimed')`, eventID)
		}
		return err
	})
}

// ListPipelineDeliveryUpdates atomically claims payload-free terminal notices
// for the local agent that signed the transport event. A failed result event
// belongs to its local completer even though the imported pipeline row is
// already completed; source_agent_id covers both send and result directions.
func (s *SQLiteStore) ListPipelineDeliveryUpdates(ctx context.Context, agentID string, limit int) ([]*PipelineDeliveryUpdate, error) {
	if strings.TrimSpace(agentID) == "" {
		return nil, fmt.Errorf("pipeline delivery update agent is required")
	}
	if limit <= 0 || limit > 20 {
		limit = 5
	}
	updates := make([]*PipelineDeliveryUpdate, 0, limit)
	err := s.runPipelineTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		rows, err := tx.conn.QueryContext(ctx, `SELECT event_id, pipe_id, event_kind,
			remote_chain_id, target_agent_id, state, attempts, last_error, created_at
			FROM pipeline_transport_outbox
			WHERE source_agent_id=? AND state='failed' AND reported_at IS NULL
			ORDER BY next_attempt_at ASC, event_id ASC LIMIT ?`, agentID, limit)
		if err != nil {
			return err
		}
		for rows.Next() {
			var update PipelineDeliveryUpdate
			var createdAt string
			if err := rows.Scan(&update.EventID, &update.PipeID, &update.EventKind,
				&update.RemoteChainID, &update.TargetAgentID, &update.State,
				&update.Attempts, &update.LastError, &createdAt); err != nil {
				_ = rows.Close()
				return err
			}
			update.CreatedAt = parseTime(createdAt)
			updates = append(updates, &update)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if err := rows.Err(); err != nil {
			return err
		}
		for _, update := range updates {
			if _, err := tx.writeExecContext(ctx, `UPDATE pipeline_transport_outbox
				SET reported_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
				WHERE event_id=? AND state='failed' AND reported_at IS NULL`, update.EventID); err != nil {
				return err
			}
		}
		return nil
	})
	return updates, err
}

func (s *SQLiteStore) PurgeExpiredPipelineTransport(ctx context.Context, now time.Time) (int, error) {
	var removed int64
	err := s.runPipelineTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		if _, err := tx.writeExecContext(ctx, `UPDATE pipeline_messages SET status='failed'
			WHERE status IN ('pending','claimed') AND pipe_id IN (
				SELECT pipe_id FROM pipeline_transport_outbox WHERE state='pending'
				AND julianday(expires_at)<=julianday(?)
			)`, formatTime(now)); err != nil {
			return err
		}
		res, err := tx.writeExecContext(ctx, `UPDATE pipeline_transport_outbox SET state='failed',
			last_error='pipeline transport event expired' WHERE state='pending'
			AND julianday(expires_at)<=julianday(?)`, formatTime(now))
		if err != nil {
			return err
		}
		if n, rowsErr := res.RowsAffected(); rowsErr == nil {
			removed += n
		}
		res, err = tx.writeExecContext(ctx, `DELETE FROM pipeline_transport_dedup
			WHERE julianday(expires_at)<=julianday(?)`, formatTime(now))
		if err != nil {
			return err
		}
		if n, rowsErr := res.RowsAffected(); rowsErr == nil {
			removed += n
		}
		return nil
	})
	return int(removed), err
}
