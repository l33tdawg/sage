package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
)

// AdvanceInboxActivity advances one exact agent's payload-free coordination
// clock. Callers invoke it inside the same transaction that makes the task
// notice or reply visible, so rollback cannot manufacture activity.
func (s *SQLiteStore) AdvanceInboxActivity(ctx context.Context, agentID string) (uint64, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return 0, errors.New("inbox activity agent is required")
	}
	var seq int64
	if err := s.conn.QueryRowContext(ctx, `INSERT INTO agent_inbox_activity(agent_id,seq) VALUES(?,1)
		ON CONFLICT(agent_id) DO UPDATE SET seq=agent_inbox_activity.seq+1
		RETURNING seq`, agentID).Scan(&seq); err != nil {
		return 0, err
	}
	if seq < 1 {
		return 0, errors.New("inbox activity sequence did not advance")
	}
	return uint64(seq), nil
}

func (s *SQLiteStore) GetInboxActivitySequence(ctx context.Context, agentID string) (uint64, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return 0, errors.New("inbox activity agent is required")
	}
	var seq int64
	err := s.conn.QueryRowContext(ctx, `SELECT seq FROM agent_inbox_activity WHERE agent_id=?`, agentID).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if seq < 0 {
		return 0, errors.New("inbox activity sequence is invalid")
	}
	return uint64(seq), nil
}

func (s *SQLiteStore) GetInboxActivityEpoch(ctx context.Context) (string, error) {
	var epoch string
	if err := s.conn.QueryRowContext(ctx, `SELECT epoch FROM inbox_activity_meta WHERE singleton=1`).Scan(&epoch); err != nil {
		return "", err
	}
	if !validInboxActivityEpoch(epoch) {
		return "", errors.New("inbox activity epoch is invalid")
	}
	return epoch, nil
}

func (s *PostgresStore) AdvanceInboxActivity(ctx context.Context, agentID string) (uint64, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return 0, errors.New("inbox activity agent is required")
	}
	var seq int64
	if err := s.db.QueryRow(ctx, `INSERT INTO agent_inbox_activity(agent_id,seq) VALUES($1,1)
		ON CONFLICT(agent_id) DO UPDATE SET seq=agent_inbox_activity.seq+1
		RETURNING seq`, agentID).Scan(&seq); err != nil {
		return 0, err
	}
	if seq < 1 {
		return 0, errors.New("inbox activity sequence did not advance")
	}
	return uint64(seq), nil
}

func (s *PostgresStore) GetInboxActivitySequence(ctx context.Context, agentID string) (uint64, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return 0, errors.New("inbox activity agent is required")
	}
	var seq int64
	err := s.db.QueryRow(ctx, `SELECT seq FROM agent_inbox_activity WHERE agent_id=$1`, agentID).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if seq < 0 {
		return 0, errors.New("inbox activity sequence is invalid")
	}
	return uint64(seq), nil
}

func (s *PostgresStore) GetInboxActivityEpoch(ctx context.Context) (string, error) {
	var epoch string
	if err := s.db.QueryRow(ctx, `SELECT epoch FROM inbox_activity_meta WHERE singleton=1`).Scan(&epoch); err != nil {
		return "", err
	}
	if !validInboxActivityEpoch(epoch) {
		return "", errors.New("inbox activity epoch is invalid")
	}
	return epoch, nil
}

func validInboxActivityEpoch(epoch string) bool {
	if len(epoch) != 32 {
		return false
	}
	_, err := hex.DecodeString(epoch)
	return err == nil
}
