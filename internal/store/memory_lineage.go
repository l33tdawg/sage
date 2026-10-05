package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// MemoryParent is the bounded identity metadata needed to decide whether a
// lineage target can be considered. Resolving it never loads or decrypts the
// parent's content. Handlers must still validate visibility and the canonical
// projection of any full record they disclose.
type MemoryParent struct {
	MemoryID        string
	SubmittingAgent string
	DomainTag       string
	ContentHash     []byte
}

// MemoryLineageStore is an optional serving-projection lookup. New corrections
// use the parent's content hash; legacy callers may have supplied its exact ID.
type MemoryLineageStore interface {
	FindMemoryParent(ctx context.Context, parentHash string) (*MemoryParent, error)
}

const (
	sqliteMemoryParentIDSQL     = `SELECT memory_id, submitting_agent, domain_tag, content_hash FROM memories WHERE memory_id = ?`
	sqliteMemoryParentHashSQL   = `SELECT memory_id, submitting_agent, domain_tag, content_hash FROM memories WHERE content_hash = ? LIMIT 2`
	postgresMemoryParentIDSQL   = `SELECT memory_id::text, submitting_agent, domain_tag, content_hash FROM memories WHERE memory_id = $1`
	postgresMemoryParentHashSQL = `SELECT memory_id::text, submitting_agent, domain_tag, content_hash FROM memories WHERE content_hash = $1 LIMIT 2`
)

func memoryParentHash(pointer string) []byte {
	hash, err := hex.DecodeString(pointer)
	if err != nil || len(hash) != 32 {
		return nil
	}
	return hash
}

func (s *SQLiteStore) FindMemoryParent(ctx context.Context, pointer string) (*MemoryParent, error) {
	// Preserve legacy exact-ID pointers, including IDs not shaped like UUIDs.
	var parent MemoryParent
	err := s.conn.QueryRowContext(ctx, sqliteMemoryParentIDSQL, pointer).Scan(
		&parent.MemoryID, &parent.SubmittingAgent, &parent.DomainTag, &parent.ContentHash,
	)
	if err == nil {
		return &parent, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("find memory parent: %w", err)
	}
	hash := memoryParentHash(pointer)
	if hash == nil {
		return nil, nil
	}
	rows, err := s.conn.QueryContext(ctx, sqliteMemoryParentHashSQL, hash)
	if err != nil {
		return nil, fmt.Errorf("find memory parent: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var found *MemoryParent
	count := 0
	for rows.Next() {
		var candidate MemoryParent
		if err := rows.Scan(&candidate.MemoryID, &candidate.SubmittingAgent, &candidate.DomainTag, &candidate.ContentHash); err != nil {
			return nil, fmt.Errorf("read memory parent: %w", err)
		}
		found = &candidate
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read memory parent: %w", err)
	}
	// Content identifies content, not a unique record. Even a duplicate outside
	// the caller's visibility or graph sample must leave lineage unresolved.
	if count != 1 {
		return nil, nil
	}
	return found, nil
}

func (s *PostgresStore) FindMemoryParent(ctx context.Context, pointer string) (*MemoryParent, error) {
	// PostgreSQL memory IDs are UUIDs. A hash must never be sent to a UUID
	// equality predicate, which rejects it before hash fallback.
	if id, err := uuid.Parse(pointer); err == nil {
		var parent MemoryParent
		err := s.db.QueryRow(ctx, postgresMemoryParentIDSQL, id.String()).Scan(
			&parent.MemoryID, &parent.SubmittingAgent, &parent.DomainTag, &parent.ContentHash,
		)
		if err == nil {
			return &parent, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("find memory parent: %w", err)
		}
	}
	hash := memoryParentHash(pointer)
	if hash == nil {
		return nil, nil
	}
	rows, err := s.db.Query(ctx, postgresMemoryParentHashSQL, hash)
	if err != nil {
		return nil, fmt.Errorf("find memory parent: %w", err)
	}
	defer rows.Close()
	var found *MemoryParent
	count := 0
	for rows.Next() {
		var candidate MemoryParent
		if err := rows.Scan(&candidate.MemoryID, &candidate.SubmittingAgent, &candidate.DomainTag, &candidate.ContentHash); err != nil {
			return nil, fmt.Errorf("read memory parent: %w", err)
		}
		found = &candidate
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read memory parent: %w", err)
	}
	if count != 1 {
		return nil, nil
	}
	return found, nil
}
