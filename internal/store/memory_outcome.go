package store

// Node-local recall outcome feedback.
//
// An agent that recalled a memory can report whether it helped, hurt, or went
// unused. The counts live in one off-chain SQLite table on the node that
// received the report. They are never part of a transaction, the AppHash, a
// memory's content hash, or a state-sync snapshot, so they cannot fork the
// chain and differ freely between nodes. Recall reads them to nudge ranking
// by a bounded amount (api/rest/memory_outcome.go).
//
// Rows are keyed by (memory_id, agent_id): one row per reporting agent, so a
// single agent's reports are capped when they are aggregated and cannot
// outweigh several independent agents.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Memory outcome values accepted by RecordMemoryOutcome.
const (
	MemoryOutcomeHelped = "helped"
	MemoryOutcomeHurt   = "hurt"
	MemoryOutcomeUnused = "unused"
)

// ErrMemoryOutcomeRateLimited means the same agent reported an outcome for the
// same memory less than the minimum interval ago.
var ErrMemoryOutcomeRateLimited = errors.New("memory outcome: reported too recently")

// MemoryOutcomeStats is the aggregate feedback for one memory. Each count is
// the sum over reporting agents of that agent's count capped at the per-agent
// cap passed to GetMemoryOutcomeStats.
type MemoryOutcomeStats struct {
	Helped int
	Hurt   int
	Unused int
	Agents int
	LastAt time.Time
}

// MemoryOutcomeStore is implemented by stores that keep node-local outcome
// feedback. Only the SQLite store does today.
type MemoryOutcomeStore interface {
	RecordMemoryOutcome(ctx context.Context, memoryID, agentID, outcome string, now time.Time, minInterval time.Duration) error
	GetMemoryOutcomeStats(ctx context.Context, memoryIDs []string, perAgentCap int) (map[string]MemoryOutcomeStats, error)
}

func validMemoryOutcome(outcome string) bool {
	switch outcome {
	case MemoryOutcomeHelped, MemoryOutcomeHurt, MemoryOutcomeUnused:
		return true
	}
	return false
}

func (s *SQLiteStore) migrateMemoryOutcomes(ctx context.Context) error {
	if _, err := s.writeExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS memory_outcomes (
			memory_id TEXT NOT NULL,
			agent_id  TEXT NOT NULL,
			helped    INTEGER NOT NULL DEFAULT 0 CHECK (helped >= 0),
			hurt      INTEGER NOT NULL DEFAULT 0 CHECK (hurt >= 0),
			unused    INTEGER NOT NULL DEFAULT 0 CHECK (unused >= 0),
			last_at   INTEGER NOT NULL,
			PRIMARY KEY (memory_id, agent_id)
		)`); err != nil {
		return fmt.Errorf("create memory outcomes: %w", err)
	}
	return nil
}

// RecordMemoryOutcome adds one outcome report from agentID for memoryID. A
// report that arrives less than minInterval after the same agent's previous
// report for the same memory is refused with ErrMemoryOutcomeRateLimited and
// changes nothing. The check and the increment are one statement.
func (s *SQLiteStore) RecordMemoryOutcome(ctx context.Context, memoryID, agentID, outcome string, now time.Time, minInterval time.Duration) error {
	if memoryID == "" || agentID == "" {
		return errors.New("memory outcome: memory and agent are required")
	}
	if !validMemoryOutcome(outcome) {
		return fmt.Errorf("memory outcome: unknown outcome %q", outcome)
	}
	var helped, hurt, unused int
	switch outcome {
	case MemoryOutcomeHelped:
		helped = 1
	case MemoryOutcomeHurt:
		hurt = 1
	default:
		unused = 1
	}
	nowMS := now.UnixMilli()
	cutoff := nowMS - minInterval.Milliseconds()
	res, err := s.writeExecContext(ctx, `
		INSERT INTO memory_outcomes (memory_id, agent_id, helped, hurt, unused, last_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(memory_id, agent_id) DO UPDATE SET
			helped = helped + excluded.helped,
			hurt = hurt + excluded.hurt,
			unused = unused + excluded.unused,
			last_at = excluded.last_at
		WHERE memory_outcomes.last_at <= ?`,
		memoryID, agentID, helped, hurt, unused, nowMS, cutoff)
	if err != nil {
		return fmt.Errorf("record memory outcome: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("record memory outcome: %w", err)
	}
	if n == 0 {
		return ErrMemoryOutcomeRateLimited
	}
	return nil
}

// GetMemoryOutcomeStats returns aggregate feedback for the given memories.
// Memories with no reports are absent from the map.
func (s *SQLiteStore) GetMemoryOutcomeStats(ctx context.Context, memoryIDs []string, perAgentCap int) (map[string]MemoryOutcomeStats, error) {
	if perAgentCap < 1 {
		perAgentCap = 1
	}
	stats := make(map[string]MemoryOutcomeStats, len(memoryIDs))
	for start := 0; start < len(memoryIDs); start += 900 {
		end := start + 900
		if end > len(memoryIDs) {
			end = len(memoryIDs)
		}
		chunk := memoryIDs[start:end]
		ph := make([]string, len(chunk))
		args := make([]any, 0, len(chunk)+3)
		args = append(args, perAgentCap, perAgentCap, perAgentCap)
		for i, id := range chunk {
			ph[i] = "?"
			args = append(args, id)
		}
		rows, err := s.conn.QueryContext(ctx, `
			SELECT memory_id, SUM(MIN(helped, ?)), SUM(MIN(hurt, ?)), SUM(MIN(unused, ?)), COUNT(*), MAX(last_at)
			FROM memory_outcomes WHERE memory_id IN (`+strings.Join(ph, ",")+`)
			GROUP BY memory_id`, args...)
		if err != nil {
			return nil, fmt.Errorf("get memory outcome stats: %w", err)
		}
		for rows.Next() {
			var id string
			var st MemoryOutcomeStats
			var lastMS int64
			if scanErr := rows.Scan(&id, &st.Helped, &st.Hurt, &st.Unused, &st.Agents, &lastMS); scanErr != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan memory outcome stats: %w", scanErr)
			}
			st.LastAt = time.UnixMilli(lastMS).UTC()
			stats[id] = st
		}
		rowsErr := rows.Err()
		_ = rows.Close()
		if rowsErr != nil {
			return nil, fmt.Errorf("iterate memory outcome stats: %w", rowsErr)
		}
	}
	return stats, nil
}
