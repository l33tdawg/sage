package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/memory"
)

// Node-local storage for the optional memory gate (internal/voter.Gate).
//
// Nothing here is consensus state. Semantic verdicts are this node's judge
// answers, cached per judge version; review decisions are this node's
// operator's answers for memories the gate held.
const writeGateSchema = `
	CREATE TABLE IF NOT EXISTS memory_gate_verdicts (
		memory_id     TEXT NOT NULL,
		judge_version TEXT NOT NULL,
		verdict       TEXT NOT NULL CHECK (verdict IN ('pass','reject','abstain')),
		p_yes         REAL NOT NULL,
		reason        TEXT NOT NULL DEFAULT '',
		created_at    TEXT NOT NULL,
		PRIMARY KEY (memory_id, judge_version)
	);
	CREATE INDEX IF NOT EXISTS idx_memory_gate_verdicts_held
		ON memory_gate_verdicts(judge_version, verdict);

	CREATE TABLE IF NOT EXISTS memory_review_decisions (
		memory_id  TEXT PRIMARY KEY,
		decision   TEXT NOT NULL CHECK (decision IN ('accept','reject')),
		decided_by TEXT NOT NULL DEFAULT '',
		note       TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL
	);
`

func (s *SQLiteStore) migrateWriteGate(ctx context.Context) error {
	if _, err := s.writeExecContext(ctx, writeGateSchema); err != nil {
		return fmt.Errorf("create memory-gate tables: %w", err)
	}
	return nil
}

// ErrContentUnavailable means a memory's plaintext cannot be produced (the
// vault is locked or the content does not decrypt). Callers must not fall
// back to the stored value.
var ErrContentUnavailable = errors.New("memory content is not available in plaintext")

// JudgeableContent returns a memory's plaintext for the gate's judges, or
// ErrContentUnavailable. It never returns stored ciphertext or the
// locked-vault placeholder, so nothing unreadable leaves the node.
func (s *SQLiteStore) JudgeableContent(ctx context.Context, memoryID string) (string, error) {
	var stored string
	err := s.conn.QueryRowContext(ctx, `SELECT content FROM memories WHERE memory_id = ?`, memoryID).Scan(&stored)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("%w: %s", ErrMemoryNotFound, memoryID)
	}
	if err != nil {
		return "", fmt.Errorf("judgeable content: %w", err)
	}
	return s.strictPlaintext(stored)
}

// strictPlaintext decrypts stored content or reports ErrContentUnavailable.
func (s *SQLiteStore) strictPlaintext(stored string) (string, error) {
	if !strings.HasPrefix(stored, encPrefix) {
		return stored, nil
	}
	plain, err := s.decryptContent(stored)
	if err != nil || plain == VaultLockedPlaceholder || strings.HasPrefix(plain, encPrefix) {
		return "", ErrContentUnavailable
	}
	return plain, nil
}

// SemanticVerdict returns the gate's verdict for memoryID under version.
func (s *SQLiteStore) SemanticVerdict(ctx context.Context, memoryID, version string) (memory.SemanticVerdict, bool, error) {
	var v memory.SemanticVerdict
	err := s.conn.QueryRowContext(ctx,
		`SELECT verdict, p_yes, reason FROM memory_gate_verdicts WHERE memory_id = ? AND judge_version = ?`,
		memoryID, version).Scan(&v.Verdict, &v.P, &v.Reason)
	if errors.Is(err, sql.ErrNoRows) {
		return memory.SemanticVerdict{}, false, nil
	}
	if err != nil {
		return memory.SemanticVerdict{}, false, fmt.Errorf("semantic verdict: %w", err)
	}
	return v, true, nil
}

// RecordSemanticVerdict stores (or replaces) the verdict for memoryID under
// version.
func (s *SQLiteStore) RecordSemanticVerdict(ctx context.Context, memoryID, version string, v memory.SemanticVerdict) error {
	if _, err := s.writeExecContext(ctx,
		`INSERT OR REPLACE INTO memory_gate_verdicts (memory_id, judge_version, verdict, p_yes, reason, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		memoryID, version, v.Verdict, v.P, v.Reason, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("record semantic verdict: %w", err)
	}
	return nil
}

// GateVerdict is one stored verdict, for the review UI.
type GateVerdict struct {
	JudgeVersion string    `json:"judge_version"`
	Verdict      string    `json:"verdict"`
	P            float64   `json:"p_yes"`
	Reason       string    `json:"reason"`
	CreatedAt    time.Time `json:"created_at"`
}

// GateVerdicts lists every verdict recorded for a memory, newest first.
func (s *SQLiteStore) GateVerdicts(ctx context.Context, memoryID string) ([]GateVerdict, error) {
	rows, err := s.conn.QueryContext(ctx,
		`SELECT judge_version, verdict, p_yes, reason, created_at FROM memory_gate_verdicts
		 WHERE memory_id = ? ORDER BY created_at DESC`, memoryID)
	if err != nil {
		return nil, fmt.Errorf("gate verdicts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []GateVerdict
	for rows.Next() {
		var v GateVerdict
		var created string
		if err := rows.Scan(&v.JudgeVersion, &v.Verdict, &v.P, &v.Reason, &created); err != nil {
			return nil, fmt.Errorf("gate verdicts: %w", err)
		}
		v.CreatedAt = parseTime(created)
		out = append(out, v)
	}
	return out, rows.Err()
}

// ReviewDecision returns the operator's decision for a held memory.
func (s *SQLiteStore) ReviewDecision(ctx context.Context, memoryID string) (string, bool, error) {
	var d string
	err := s.conn.QueryRowContext(ctx,
		`SELECT decision FROM memory_review_decisions WHERE memory_id = ?`, memoryID).Scan(&d)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("review decision: %w", err)
	}
	return d, true, nil
}

// ErrNotAwaitingReview means the memory is not held for review.
var ErrNotAwaitingReview = errors.New("memory is not awaiting review")

// SetReviewDecision records the operator's decision for a memory the gate
// held under version. It resolves only the semantic question: the built-in
// checks still run when the node votes. A decision is final — once voted it is
// on-chain — and survives later judge-version changes.
func (s *SQLiteStore) SetReviewDecision(ctx context.Context, memoryID, version, decision, decidedBy, note string) error {
	if decision != memory.VerdictAccept && decision != memory.VerdictReject {
		return fmt.Errorf("decision must be %q or %q", memory.VerdictAccept, memory.VerdictReject)
	}
	v, ok, err := s.SemanticVerdict(ctx, memoryID, version)
	if err != nil {
		return err
	}
	var status string
	if statusErr := s.conn.QueryRowContext(ctx, `SELECT status FROM memories WHERE memory_id = ?`, memoryID).Scan(&status); statusErr != nil {
		return ErrNotAwaitingReview
	}
	if !ok || v.Verdict != memory.VerdictAbstain || status != string(memory.StatusProposed) {
		return ErrNotAwaitingReview
	}
	res, err := s.writeExecContext(ctx,
		`INSERT OR IGNORE INTO memory_review_decisions (memory_id, decision, decided_by, note, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		memoryID, decision, decidedBy, note, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("set review decision: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotAwaitingReview // already decided
	}
	return nil
}

// ReviewQueueCursor identifies the last scanned row, even after it is decided.
// CreatedAt preserves the stored timestamp exactly to match SQLite's ordering.
type ReviewQueueCursor struct {
	JudgeVersion string `json:"judge_version"`
	CreatedAt    string `json:"created_at"`
	MemoryID     string `json:"memory_id"`
}

// HeldForReview is one memory the gate held, as stored — IDs and the gate's
// reason only. Callers load the memory itself through the dashboard's normal
// record path, which applies projection integrity and content handling.
type HeldForReview struct {
	MemoryID  string            `json:"memory_id"`
	Reason    string            `json:"reason"`
	P         float64           `json:"p_yes"`
	HeldAt    time.Time         `json:"held_at"`
	JudgeVers string            `json:"judge_version"`
	Cursor    ReviewQueueCursor `json:"-"`
}

// ReviewQueue lists memories held under version that are still proposed and
// not yet decided, oldest first, as one RAW page after the cursor. Callers that
// filter rows for visibility must walk pages until their visible page is full
// (see web/write_gate.go); a raw limit alone would let hidden rows starve it.
func (s *SQLiteStore) ReviewQueue(ctx context.Context, version string, after ReviewQueueCursor, limit int) ([]HeldForReview, error) {
	if limit <= 0 || limit > 1024 {
		limit = 100
	}
	if after != (ReviewQueueCursor{}) &&
		(after.JudgeVersion != version || after.CreatedAt == "" || after.MemoryID == "") {
		return nil, errors.New("invalid review queue cursor for judge version")
	}
	rows, err := s.conn.QueryContext(ctx,
		`SELECT v.memory_id, v.reason, v.p_yes, v.created_at, v.judge_version
		 FROM memory_gate_verdicts AS v
		 JOIN memories AS m ON m.memory_id = v.memory_id
		 WHERE v.judge_version = ? AND v.verdict = 'abstain' AND m.status = 'proposed'
		   AND NOT EXISTS (SELECT 1 FROM memory_review_decisions AS d WHERE d.memory_id = v.memory_id)
		   AND (v.created_at, v.memory_id) > (?, ?)
		 ORDER BY v.created_at ASC, v.memory_id ASC LIMIT ?`, version, after.CreatedAt, after.MemoryID, limit)
	if err != nil {
		return nil, fmt.Errorf("review queue: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []HeldForReview
	for rows.Next() {
		var it HeldForReview
		var held string
		if err := rows.Scan(&it.MemoryID, &it.Reason, &it.P, &held, &it.JudgeVers); err != nil {
			return nil, fmt.Errorf("review queue: %w", err)
		}
		it.HeldAt = parseTime(held)
		it.Cursor = ReviewQueueCursor{JudgeVersion: it.JudgeVers, CreatedAt: held, MemoryID: it.MemoryID}
		out = append(out, it)
	}
	return out, rows.Err()
}

// ReviewMemory loads a held memory for the operator's review. It returns the
// full record (the dashboard's projection-integrity classifier needs its
// canonical fields), and contentAvailable=false with Content cleared when the
// content cannot be produced in plaintext — a locked vault or a decryption
// failure never surfaces the stored ciphertext or the locked placeholder as
// review content.
func (s *SQLiteStore) ReviewMemory(ctx context.Context, memoryID string) (rec *memory.MemoryRecord, contentAvailable bool, err error) {
	rec, err = s.GetMemory(ctx, memoryID)
	if err != nil {
		return nil, false, err
	}
	var stored string
	if err := s.conn.QueryRowContext(ctx, `SELECT content FROM memories WHERE memory_id = ?`, memoryID).Scan(&stored); err != nil {
		return nil, false, fmt.Errorf("review memory: %w", err)
	}
	plain, perr := s.strictPlaintext(stored)
	if perr != nil {
		rec.Content = ""
		return rec, false, nil
	}
	rec.Content = plain
	return rec, true, nil
}
