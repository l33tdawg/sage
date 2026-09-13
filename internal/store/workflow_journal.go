package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/l33tdawg/sage/internal/vault"
)

const (
	MaxWorkflowJournalPayload           = 16 * 1024
	MaxWorkflowJournalRecords           = 4096
	MaxWorkflowJournalNodeRecords       = 16384
	MaxWorkflowJournalRevision    int64 = (1 << 53) - 1
	maxWorkflowJournalSealed            = 128 * 1024
	workflowJournalSchema               = "sage.workflow-journal.v1"
)

var (
	ErrWorkflowJournalInvalid            = errors.New("invalid workflow journal argument")
	ErrWorkflowJournalConflict           = errors.New("workflow journal revision or identity conflict")
	ErrWorkflowJournalNotFound           = errors.New("workflow journal record not found")
	ErrWorkflowJournalLimit              = errors.New("workflow journal actor limit reached")
	ErrWorkflowJournalVaultRequired      = errors.New("workflow journal requires expected active vault")
	ErrWorkflowJournalCorrupt            = errors.New("workflow journal record corrupt")
	ErrWorkflowJournalStandaloneRequired = errors.New("workflow journal requires standalone store")
)

// WorkflowJournalRecord is untrusted auxiliary application state, not canonical
// memory, enrollment, provenance, public approval, or a request to send anything.
// The API boundary must authorize the exact ordinary agent; this store does not
// authenticate callers or grant access based on payload content.
type WorkflowJournalRecord struct {
	AgentID  string          `json:"agent_id"`
	RecordID string          `json:"record_id"`
	Revision int64           `json:"revision"`
	Kind     string          `json:"kind"`
	Payload  json.RawMessage `json:"payload"`
}

type WorkflowJournalGuard struct {
	RecordID         string `json:"record_id"`
	ExpectedRevision int64  `json:"expected_revision"`
}

type sealedWorkflowJournal struct {
	Schema   string `json:"schema"`
	AgentID  string `json:"agent_id"`
	RecordID string `json:"record_id"`
	Revision int64  `json:"revision"`
	Kind     string `json:"kind"`
	Payload  string `json:"payload"`
}

type WorkflowJournalPage struct {
	Records    []*WorkflowJournalRecord `json:"records"`
	More       bool                     `json:"more"`
	NextCursor string                   `json:"next_cursor"`
}

func (s *SQLiteStore) ListWorkflowJournal(ctx context.Context, agentID, afterUUID string, limit int) (*WorkflowJournalPage, error) {
	if s == nil || s.db == nil {
		return nil, ErrWorkflowJournalStandaloneRequired
	}
	if !validReceiptAgentID(agentID) || limit < 1 || limit > 50 ||
		(afterUUID != "" && !validWorkflowJournalIdentity(agentID, afterUUID)) {
		return nil, ErrWorkflowJournalInvalid
	}
	s.vaultPublicationMu.RLock()
	defer s.vaultPublicationMu.RUnlock()
	active := s.vault.Load()
	if !s.vaultExpected.Load() || active == nil {
		return nil, ErrWorkflowJournalVaultRequired
	}
	transaction, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin workflow journal page: %w", err)
	}
	defer transaction.Rollback()
	rows, err := transaction.QueryContext(ctx, `SELECT record_id FROM workflow_journal WHERE agent_id = ? AND record_id > ? ORDER BY record_id LIMIT ?`, agentID, afterUUID, limit+1)
	if err != nil {
		return nil, fmt.Errorf("list workflow journal: %w", err)
	}
	identifiers := make([]string, 0, limit+1)
	for rows.Next() {
		var identifier string
		if err := rows.Scan(&identifier); err != nil {
			rows.Close()
			return nil, fmt.Errorf("read workflow journal cursor: %w", err)
		}
		if !validWorkflowJournalIdentity(agentID, identifier) {
			rows.Close()
			return nil, ErrWorkflowJournalCorrupt
		}
		identifiers = append(identifiers, identifier)
	}
	iterationErr := rows.Err()
	closeErr := rows.Close()
	if iterationErr != nil {
		return nil, fmt.Errorf("iterate workflow journal: %w", iterationErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close workflow journal page: %w", closeErr)
	}
	page := &WorkflowJournalPage{Records: make([]*WorkflowJournalRecord, 0, limit), More: len(identifiers) > limit}
	for index, identifier := range identifiers {
		record, err := readWorkflowJournal(ctx, transaction, active, agentID, identifier)
		if err != nil {
			return nil, err
		}
		if index < limit {
			page.Records = append(page.Records, record)
		}
	}
	if page.More {
		page.NextCursor = page.Records[len(page.Records)-1].RecordID
	}
	if err := transaction.Commit(); err != nil {
		return nil, fmt.Errorf("finish workflow journal page: %w", err)
	}
	return page, nil
}

func (s *SQLiteStore) migrateWorkflowJournal(ctx context.Context) error {
	_, err := s.writeExecContext(ctx, `CREATE TABLE IF NOT EXISTS workflow_journal (
		agent_id TEXT NOT NULL,
		record_id TEXT NOT NULL,
		revision INTEGER NOT NULL CHECK (revision BETWEEN 1 AND 9007199254740991),
		sealed_record BLOB NOT NULL CHECK (length(sealed_record) BETWEEN 1 AND 131072),
		PRIMARY KEY (agent_id, record_id)
	)`)
	if err != nil {
		return fmt.Errorf("create workflow journal: %w", err)
	}
	return nil
}

func validWorkflowJournalIdentity(agentID, recordID string) bool {
	parsed, err := uuid.Parse(recordID)
	return validReceiptAgentID(agentID) && err == nil && parsed != uuid.Nil && parsed.String() == recordID
}

func validWorkflowJournalKind(kind string) bool {
	return kind == "mesh_outbound" || kind == "mesh_inbound" || kind == "public_proposal" ||
		kind == "conversation_control" || kind == "conversation_session"
}

func validWorkflowUnicodeEscapes(payload []byte) bool {
	for index := 0; index < len(payload); index++ {
		if payload[index] != '\\' {
			continue
		}
		index++
		if index >= len(payload) {
			return false
		}
		if payload[index] != 'u' {
			continue
		}
		if index+4 >= len(payload) {
			return false
		}
		code, err := strconv.ParseUint(string(payload[index+1:index+5]), 16, 16)
		if err != nil {
			return false
		}
		index += 4
		if code >= 0xd800 && code <= 0xdbff {
			if index+6 >= len(payload) || payload[index+1] != '\\' || payload[index+2] != 'u' {
				return false
			}
			low, err := strconv.ParseUint(string(payload[index+3:index+7]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return false
			}
			index += 6
		} else if code >= 0xdc00 && code <= 0xdfff {
			return false
		}
	}
	return true
}

func strictWorkflowJSON(payload []byte) bool {
	if len(payload) == 0 || !utf8.Valid(payload) || !validWorkflowUnicodeEscapes(payload) {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var consume func(int) bool
	consume = func(depth int) bool {
		if depth > 64 {
			return false
		}
		token, err := decoder.Token()
		if err != nil {
			return false
		}
		delimiter, nested := token.(json.Delim)
		if !nested {
			if number, numeric := token.(json.Number); numeric {
				value, err := number.Float64()
				return err == nil && !math.IsInf(value, 0) && !math.IsNaN(value)
			}
			return true
		}
		switch delimiter {
		case '{':
			keys := make(map[string]bool)
			for decoder.More() {
				key, keyErr := decoder.Token()
				name, text := key.(string)
				if keyErr != nil || !text || keys[name] {
					return false
				}
				keys[name] = true
				if !consume(depth + 1) {
					return false
				}
			}
		case '[':
			for decoder.More() {
				if !consume(depth + 1) {
					return false
				}
			}
		default:
			return false
		}
		closing, closeErr := decoder.Token()
		return closeErr == nil && (delimiter == '{' && closing == json.Delim('}') || delimiter == '[' && closing == json.Delim(']'))
	}
	if !consume(0) {
		return false
	}
	_, err := decoder.Token()
	return errors.Is(err, io.EOF)
}

func readWorkflowJournal(ctx context.Context, connection sqlQuerier, active *vault.Vault, agentID, recordID string) (*WorkflowJournalRecord, error) {
	var revision, sealedLength int64
	var sealed []byte
	err := connection.QueryRowContext(ctx, `SELECT revision, length(sealed_record),
		CASE WHEN length(sealed_record) <= 131072 THEN sealed_record ELSE NULL END
		FROM workflow_journal WHERE agent_id = ? AND record_id = ?`, agentID, recordID).Scan(&revision, &sealedLength, &sealed)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrWorkflowJournalNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read workflow journal: %w", err)
	}
	if revision < 1 || revision > MaxWorkflowJournalRevision || sealedLength < 1 || sealedLength > maxWorkflowJournalSealed {
		return nil, ErrWorkflowJournalCorrupt
	}
	plaintext, err := active.Decrypt(sealed)
	if err != nil || !strictWorkflowJSON(plaintext) {
		return nil, ErrWorkflowJournalCorrupt
	}
	var record sealedWorkflowJournal
	decoder := json.NewDecoder(bytes.NewReader(plaintext))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return nil, ErrWorkflowJournalCorrupt
	}
	if record.Schema != workflowJournalSchema || record.AgentID != agentID || record.RecordID != recordID ||
		record.Revision != revision || !validWorkflowJournalKind(record.Kind) ||
		len(record.Payload) > MaxWorkflowJournalPayload || !strictWorkflowJSON([]byte(record.Payload)) {
		return nil, ErrWorkflowJournalCorrupt
	}
	return &WorkflowJournalRecord{AgentID: agentID, RecordID: recordID, Revision: revision,
		Kind: record.Kind, Payload: json.RawMessage(record.Payload)}, nil
}

// GetWorkflowJournal returns only one actor-bound record. The publication read
// lock covers the entire database/decrypt operation; there is no plaintext or
// locked-placeholder fallback. Transaction clones are intentionally rejected:
// their publication mutex is not the standalone store's vault lifetime gate.
func (s *SQLiteStore) GetWorkflowJournal(ctx context.Context, agentID, recordID string) (*WorkflowJournalRecord, error) {
	if s == nil || s.db == nil {
		return nil, ErrWorkflowJournalStandaloneRequired
	}
	s.vaultPublicationMu.RLock()
	defer s.vaultPublicationMu.RUnlock()
	active := s.vault.Load()
	if !s.vaultExpected.Load() || active == nil {
		return nil, ErrWorkflowJournalVaultRequired
	}
	if !validWorkflowJournalIdentity(agentID, recordID) {
		return nil, ErrWorkflowJournalInvalid
	}
	return readWorkflowJournal(ctx, s.conn, active, agentID, recordID)
}

func (s *SQLiteStore) lockVaultWrite(ctx context.Context) (func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if s.vaultPublicationMu.TryRLock() {
			if s.writeMu.TryLock() {
				return func() {
					s.writeMu.Unlock()
					s.vaultPublicationMu.RUnlock()
				}, nil
			}
			s.vaultPublicationMu.RUnlock()
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// PutWorkflowJournal creates at expectedRevision zero or updates by exact CAS.
// Create replay requires revision one and byte-identical kind/payload. An update
// retry conflicts; reconcile by Get, never by automatic retry or a fresh UUID.
// The original JSON bytes are retained inside the encrypted envelope. Identity,
// revision and schema are also sealed to detect cross-row ciphertext swapping.
// No delete or external side effect is implemented.
func (s *SQLiteStore) PutWorkflowJournal(ctx context.Context, agentID, recordID, kind string, payload json.RawMessage, expectedRevision int64) (*WorkflowJournalRecord, error) {
	return s.PutWorkflowJournalGuarded(ctx, agentID, recordID, kind, payload, expectedRevision, nil)
}

func (s *SQLiteStore) PutWorkflowJournalGuarded(ctx context.Context, agentID, recordID, kind string, payload json.RawMessage, expectedRevision int64, guard *WorkflowJournalGuard) (*WorkflowJournalRecord, error) {
	if s == nil || s.db == nil {
		return nil, ErrWorkflowJournalStandaloneRequired
	}
	var condition WorkflowJournalGuard
	if guard != nil {
		condition = *guard
	}
	unlock, err := s.lockVaultWrite(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	active := s.vault.Load()
	if !s.vaultExpected.Load() || active == nil {
		return nil, ErrWorkflowJournalVaultRequired
	}
	if !validWorkflowJournalIdentity(agentID, recordID) || !validWorkflowJournalKind(kind) ||
		expectedRevision < 0 || expectedRevision >= MaxWorkflowJournalRevision || len(payload) > MaxWorkflowJournalPayload || !strictWorkflowJSON(payload) {
		return nil, ErrWorkflowJournalInvalid
	}
	if guard != nil && (kind != "conversation_session" || condition.RecordID == recordID ||
		!validWorkflowJournalIdentity(agentID, condition.RecordID) ||
		condition.ExpectedRevision < 1 || condition.ExpectedRevision > MaxWorkflowJournalRevision) {
		return nil, ErrWorkflowJournalInvalid
	}
	payload = append(json.RawMessage(nil), payload...)
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin workflow journal: %w", err)
	}
	defer transaction.Rollback()
	if guard != nil {
		control, err := readWorkflowJournal(ctx, transaction, active, agentID, condition.RecordID)
		if errors.Is(err, ErrWorkflowJournalNotFound) {
			return nil, ErrWorkflowJournalConflict
		}
		if err != nil {
			return nil, err
		}
		if control.Kind != "conversation_control" || control.Revision != condition.ExpectedRevision {
			return nil, ErrWorkflowJournalConflict
		}
	}
	current, err := readWorkflowJournal(ctx, transaction, active, agentID, recordID)
	if err != nil && !errors.Is(err, ErrWorkflowJournalNotFound) {
		return nil, err
	}
	if current != nil {
		if current.Kind != kind {
			return nil, ErrWorkflowJournalConflict
		}
		if expectedRevision == 0 {
			if current.Revision == 1 && bytes.Equal(current.Payload, payload) {
				return current, nil
			}
			return nil, ErrWorkflowJournalConflict
		}
		if expectedRevision != current.Revision {
			return nil, ErrWorkflowJournalConflict
		}
	} else {
		if expectedRevision != 0 {
			return nil, ErrWorkflowJournalConflict
		}
		var count int
		if err := transaction.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_journal WHERE agent_id = ?`, agentID).Scan(&count); err != nil {
			return nil, fmt.Errorf("count workflow journal: %w", err)
		}
		if count >= MaxWorkflowJournalRecords {
			return nil, ErrWorkflowJournalLimit
		}
		if err := transaction.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_journal`).Scan(&count); err != nil {
			return nil, fmt.Errorf("count workflow journal node: %w", err)
		}
		if count >= MaxWorkflowJournalNodeRecords {
			return nil, ErrWorkflowJournalLimit
		}
	}
	record := sealedWorkflowJournal{Schema: workflowJournalSchema, AgentID: agentID, RecordID: recordID,
		Revision: expectedRevision + 1, Kind: kind, Payload: string(payload)}
	plaintext, err := json.Marshal(record)
	if err != nil {
		return nil, ErrWorkflowJournalInvalid
	}
	sealed, err := active.Encrypt(plaintext)
	if err != nil {
		return nil, ErrWorkflowJournalVaultRequired
	}
	if len(sealed) > maxWorkflowJournalSealed {
		return nil, ErrWorkflowJournalInvalid
	}
	if current == nil {
		_, err = transaction.ExecContext(ctx, `INSERT INTO workflow_journal (agent_id, record_id, revision, sealed_record) VALUES (?, ?, ?, ?)`, agentID, recordID, record.Revision, sealed)
	} else {
		var result sql.Result
		result, err = transaction.ExecContext(ctx, `UPDATE workflow_journal SET revision = ?, sealed_record = ? WHERE agent_id = ? AND record_id = ? AND revision = ?`, record.Revision, sealed, agentID, recordID, expectedRevision)
		if err == nil {
			var count int64
			count, err = result.RowsAffected()
			if err == nil && count != 1 {
				return nil, ErrWorkflowJournalConflict
			}
		}
	}
	if err != nil {
		return nil, fmt.Errorf("write workflow journal: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return nil, fmt.Errorf("commit workflow journal: %w", err)
	}
	return &WorkflowJournalRecord{AgentID: agentID, RecordID: recordID, Revision: record.Revision, Kind: kind, Payload: payload}, nil
}
