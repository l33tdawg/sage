package store

import (
	"context"
	"fmt"
	"math"
)

// TaintAwareDomainSpaceCompletenessStore is only an index-completeness probe.
// It does not authorize general recall prefilters. The expected revision and
// exclusion rows must be read in the SAME SQL statement snapshot, including
// an empty result. Callers still own canonical/projection/space/auth fences.
type TaintAwareDomainSpaceCompletenessStore interface {
	DomainSpaceCompletenessAtTaintRevision(
		ctx context.Context,
		domain, activeSpace string,
		cap int,
		expectedMirrorRevision uint64,
	) (hasOutside, established, mirrorMatched bool, err error)
}

const sqliteDomainSpaceAtTaintRevisionQuery = `WITH mirror AS (
	SELECT CASE WHEN revision = ? THEN 1 ELSE 0 END AS matched
	FROM memory_taint_mirror_revision WHERE singleton = 1
), candidates AS (
	SELECT 1 AS present, CASE WHEN embedding IS NULL THEN 1 ELSE 0 END AS emb_null,
	       COALESCE(embedding_provider, '') AS provider
	FROM memories
	WHERE domain_tag = ? AND status IN ('committed','challenged')
	  AND NOT EXISTS (
		SELECT 1 FROM memory_taint_mirror tm WHERE tm.memory_id = memories.memory_id
	  )
	ORDER BY status, memory_id LIMIT ?
)
SELECT COALESCE((SELECT matched FROM mirror), 0),
       COALESCE(c.present, 0), COALESCE(c.emb_null, 0), COALESCE(c.provider, '')
FROM (SELECT 1) AS sentinel LEFT JOIN candidates c ON 1 = 1`

const postgresDomainSpaceAtTaintRevisionQuery = `WITH mirror AS (
	SELECT CASE WHEN revision = $1 THEN 1 ELSE 0 END AS matched
	FROM memory_taint_mirror_revision WHERE singleton = 1
), candidates AS (
	SELECT 1 AS present, CASE WHEN embedding IS NULL THEN 1 ELSE 0 END AS emb_null,
	       COALESCE(embedding_provider, '') AS provider
	FROM memories
	WHERE domain_tag = $2 AND status IN ('committed','challenged')
	  AND NOT EXISTS (
		SELECT 1 FROM memory_taint_mirror tm WHERE tm.memory_id = memories.memory_id
	  )
	ORDER BY status, memory_id LIMIT $3
)
SELECT COALESCE((SELECT matched FROM mirror), 0),
       COALESCE(c.present, 0), COALESCE(c.emb_null, 0), COALESCE(c.provider, '')
FROM (SELECT 1) AS sentinel LEFT JOIN candidates c ON 1 = 1`

func validateTaintCompletenessProbe(cap int, revision uint64) error {
	if cap <= 0 || cap > CandidateFilterScanBudget || revision > math.MaxInt64 {
		return fmt.Errorf("invalid bounded taint completeness probe")
	}
	return nil
}

func (s *SQLiteStore) DomainSpaceCompletenessAtTaintRevision(
	ctx context.Context, domain, activeSpace string, cap int,
	expectedMirrorRevision uint64,
) (hasOutside, established, mirrorMatched bool, err error) {
	if validateErr := validateTaintCompletenessProbe(cap, expectedMirrorRevision); validateErr != nil {
		return false, false, false, validateErr
	}
	rows, err := s.conn.QueryContext(ctx, sqliteDomainSpaceAtTaintRevisionQuery,
		int64(expectedMirrorRevision), domain, cap+1) // #nosec G115 -- validated above
	if err != nil {
		return false, false, false, fmt.Errorf("taint completeness scan: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return readTaintCompletenessRows(rows, activeSpace, cap)
}

func (s *PostgresStore) DomainSpaceCompletenessAtTaintRevision(
	ctx context.Context, domain, activeSpace string, cap int,
	expectedMirrorRevision uint64,
) (hasOutside, established, mirrorMatched bool, err error) {
	if validateErr := validateTaintCompletenessProbe(cap, expectedMirrorRevision); validateErr != nil {
		return false, false, false, validateErr
	}
	rows, err := s.db.Query(ctx, postgresDomainSpaceAtTaintRevisionQuery,
		int64(expectedMirrorRevision), domain, cap+1) // #nosec G115 -- validated above
	if err != nil {
		return false, false, false, fmt.Errorf("taint completeness scan: %w", err)
	}
	defer rows.Close()
	return readTaintCompletenessRows(rows, activeSpace, cap)
}

type taintCompletenessRows interface {
	Next() bool
	Scan(...any) error
	Err() error
}

func readTaintCompletenessRows(
	rows taintCompletenessRows, activeSpace string, cap int,
) (hasOutside, established, mirrorMatched bool, err error) {
	scanned := 0
	sawSnapshot := false
	for rows.Next() {
		var matched, present, embNull int
		var provider string
		if err := rows.Scan(&matched, &present, &embNull, &provider); err != nil {
			return false, false, false, err
		}
		sawSnapshot = true
		if matched != 1 {
			return false, false, false, nil
		}
		if present == 0 { // mandatory snapshot sentinel for an empty candidate set
			continue
		}
		scanned++
		if embNull != 0 || provider != activeSpace {
			return true, true, true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, false, false, err
	}
	if !sawSnapshot {
		return false, false, false, nil
	}
	if scanned > cap {
		return false, false, true, nil
	}
	return false, true, true, nil
}
