package store

import (
	"context"
	"errors"
	"fmt"
	"sort"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/taint"
)

var ErrMemoryTaintMirrorNotConverged = errors.New("memory taint mirror did not converge")

type MemoryTaintMirrorStore interface {
	ReplaceMemoryTaintMirror(ctx context.Context, revision uint64, memoryIDs []string) error
	MemoryTaintMirrorRevision(ctx context.Context) (uint64, error)
}

// TaintAwareDomainSpaceCompletenessStore applies the revision-fenced SQL taint
// mirror before the bounded completeness scan. Callers must enable it only
// while the mirror exactly matches canonical taint revision.
type TaintAwareDomainSpaceCompletenessStore interface {
	DomainSpaceCompletenessFiltered(
		ctx context.Context,
		domain, activeSpace string,
		cap int,
		excludeTainted bool,
	) (hasOutside, established bool, err error)
}

func ensureSQLiteMemoryTaintMirrorSchema(ctx context.Context, s *SQLiteStore) error {
	_, err := s.writeExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS memory_taint_mirror (
			memory_id TEXT PRIMARY KEY
		);
		CREATE TABLE IF NOT EXISTS memory_taint_mirror_revision (
			singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
			revision  INTEGER NOT NULL CHECK (revision >= 0)
		);
		INSERT OR IGNORE INTO memory_taint_mirror_revision(singleton, revision)
			VALUES (1, 0);
	`)
	if err != nil {
		return fmt.Errorf("install memory taint mirror schema: %w", err)
	}
	return nil
}

func ensurePostgresMemoryTaintMirrorSchema(ctx context.Context, s *PostgresStore) error {
	return s.RunInTx(ctx, func(tx OffchainStore) error {
		ps := tx.(*PostgresStore)
		for _, statement := range []string{
			`CREATE TABLE IF NOT EXISTS memory_taint_mirror (
				memory_id TEXT PRIMARY KEY
			)`,
			`CREATE TABLE IF NOT EXISTS memory_taint_mirror_revision (
				singleton SMALLINT PRIMARY KEY CHECK (singleton = 1),
				revision BIGINT NOT NULL CHECK (revision >= 0)
			)`,
			`INSERT INTO memory_taint_mirror_revision(singleton, revision)
			 VALUES (1, 0) ON CONFLICT (singleton) DO NOTHING`,
		} {
			if _, err := ps.db.Exec(ctx, statement); err != nil {
				return fmt.Errorf("install memory taint mirror schema: %w", err)
			}
		}
		return nil
	})
}

func (s *SQLiteStore) ReplaceMemoryTaintMirror(
	ctx context.Context,
	revision uint64,
	memoryIDs []string,
) error {
	memoryIDs, err := canonicalTaintMirrorIDs(memoryIDs)
	if err != nil {
		return err
	}
	return s.RunInTx(ctx, func(tx OffchainStore) error {
		store := tx.(*SQLiteStore)
		if _, err := store.writeExecContext(ctx, `DELETE FROM memory_taint_mirror`); err != nil {
			return err
		}
		for _, memoryID := range memoryIDs {
			if _, err := store.writeExecContext(ctx,
				`INSERT INTO memory_taint_mirror(memory_id) VALUES (?)`,
				memoryID,
			); err != nil {
				return err
			}
		}
		_, err := store.writeExecContext(ctx,
			`UPDATE memory_taint_mirror_revision SET revision = ? WHERE singleton = 1`,
			revision,
		)
		return err
	})
}

func (s *SQLiteStore) MemoryTaintMirrorRevision(ctx context.Context) (uint64, error) {
	var revision int64
	if err := s.conn.QueryRowContext(ctx,
		`SELECT revision FROM memory_taint_mirror_revision WHERE singleton = 1`,
	).Scan(&revision); err != nil {
		return 0, fmt.Errorf("read memory taint mirror revision: %w", err)
	}
	if revision < 0 {
		return 0, errors.New("memory taint mirror revision is negative")
	}
	return uint64(revision), nil
}

func (s *PostgresStore) ReplaceMemoryTaintMirror(
	ctx context.Context,
	revision uint64,
	memoryIDs []string,
) error {
	memoryIDs, err := canonicalTaintMirrorIDs(memoryIDs)
	if err != nil {
		return err
	}
	return s.RunInTx(ctx, func(tx OffchainStore) error {
		store := tx.(*PostgresStore)
		if _, err := store.db.Exec(ctx, `DELETE FROM memory_taint_mirror`); err != nil {
			return err
		}
		for _, memoryID := range memoryIDs {
			if _, err := store.db.Exec(ctx,
				`INSERT INTO memory_taint_mirror(memory_id) VALUES ($1)`,
				memoryID,
			); err != nil {
				return err
			}
		}
		_, err := store.db.Exec(ctx,
			`UPDATE memory_taint_mirror_revision SET revision = $1 WHERE singleton = 1`,
			int64(revision),
		)
		return err
	})
}

func (s *PostgresStore) MemoryTaintMirrorRevision(ctx context.Context) (uint64, error) {
	var revision int64
	if err := s.db.QueryRow(ctx,
		`SELECT revision FROM memory_taint_mirror_revision WHERE singleton = 1`,
	).Scan(&revision); err != nil {
		return 0, fmt.Errorf("read memory taint mirror revision: %w", err)
	}
	if revision < 0 {
		return 0, errors.New("memory taint mirror revision is negative")
	}
	return uint64(revision), nil
}

func (s *BadgerStore) SnapshotEffectiveTaintMemberships() (
	revision uint64,
	memoryIDs []string,
	err error,
) {
	tainted := make(map[string]struct{})
	err = s.view(func(txn *badger.Txn) error {
		var readErr error
		revision, readErr = readTaintUint64Txn(txn, taint.RevisionKey())
		if readErr != nil {
			return readErr
		}
		decisionPrefix := []byte("taintdec:")
		options := badger.DefaultIteratorOptions
		options.Prefix = decisionPrefix
		options.PrefetchValues = false
		it := txn.NewIterator(options)
		defer it.Close()
		for it.Seek(decisionPrefix); it.ValidForPrefix(decisionPrefix); it.Next() {
			decisionID := string(it.Item().Key()[len(decisionPrefix):])
			if validateErr := taint.ValidateDecisionID(decisionID); validateErr != nil {
				return fmt.Errorf("%w: %v", ErrTaintStateMalformed, validateErr)
			}
			header, found, readErr := readTaintHeaderTxn(txn, decisionID)
			if readErr != nil {
				return readErr
			}
			if !found || !header.Status.Active() {
				continue
			}
			indexPrefix := taint.DecisionIndexPrefix(decisionID)
			indexOptions := badger.DefaultIteratorOptions
			indexOptions.Prefix = indexPrefix
			indexOptions.PrefetchValues = false
			index := txn.NewIterator(indexOptions)
			for index.Seek(indexPrefix); index.ValidForPrefix(indexPrefix); index.Next() {
				memoryID := string(index.Item().Key()[len(indexPrefix):])
				if len(memoryID) == 0 || len(memoryID) > taint.MaxMemoryIDBytes {
					index.Close()
					return fmt.Errorf("%w: invalid taint mirror memory ID", ErrTaintStateMalformed)
				}
				raw, found, readErr := readTaintValueTxn(
					txn,
					taint.MembershipKey(memoryID, decisionID),
				)
				if readErr != nil {
					index.Close()
					return readErr
				}
				if !found {
					continue
				}
				membership, decodeErr := taint.DecodeMembership(raw)
				if decodeErr != nil {
					index.Close()
					return fmt.Errorf("%w: %v", ErrTaintStateMalformed, decodeErr)
				}
				if header.MembershipEffective(membership) {
					tainted[memoryID] = struct{}{}
				}
			}
			index.Close()
		}
		return nil
	})
	if err != nil {
		return 0, nil, err
	}
	memoryIDs = make([]string, 0, len(tainted))
	for memoryID := range tainted {
		memoryIDs = append(memoryIDs, memoryID)
	}
	sort.Strings(memoryIDs)
	return revision, memoryIDs, nil
}

func RebuildMemoryTaintMirror(
	ctx context.Context,
	canonical *BadgerStore,
	mirror MemoryTaintMirrorStore,
	maxAttempts int,
) (revision uint64, count int, err error) {
	if canonical == nil || mirror == nil {
		return 0, 0, errors.New("taint mirror rebuild requires canonical and mirror stores")
	}
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	for attempt := 0; attempt < maxAttempts; attempt++ {
		revision, memoryIDs, snapshotErr := canonical.SnapshotEffectiveTaintMemberships()
		if snapshotErr != nil {
			return 0, 0, snapshotErr
		}
		current, revisionErr := canonical.TaintRevision()
		if revisionErr != nil {
			return 0, 0, revisionErr
		}
		if current != revision {
			continue
		}
		if replaceErr := mirror.ReplaceMemoryTaintMirror(ctx, revision, memoryIDs); replaceErr != nil {
			return 0, 0, replaceErr
		}
		current, revisionErr = canonical.TaintRevision()
		if revisionErr != nil {
			return 0, 0, revisionErr
		}
		if current == revision {
			return revision, len(memoryIDs), nil
		}
	}
	return 0, 0, ErrMemoryTaintMirrorNotConverged
}

func MemoryTaintMirrorFresh(
	ctx context.Context,
	canonical *BadgerStore,
	mirror MemoryTaintMirrorStore,
) (bool, uint64, error) {
	if canonical == nil || mirror == nil {
		return false, 0, errors.New("taint mirror freshness requires canonical and mirror stores")
	}
	current, err := canonical.TaintRevision()
	if err != nil {
		return false, 0, err
	}
	mirrored, err := mirror.MemoryTaintMirrorRevision(ctx)
	if err != nil {
		return false, current, err
	}
	return current == mirrored, current, nil
}

func canonicalTaintMirrorIDs(memoryIDs []string) ([]string, error) {
	sorted := append([]string(nil), memoryIDs...)
	sort.Strings(sorted)
	out := sorted[:0]
	for _, memoryID := range sorted {
		if len(memoryID) == 0 || len(memoryID) > taint.MaxMemoryIDBytes {
			return nil, fmt.Errorf("invalid taint mirror memory ID length %d", len(memoryID))
		}
		if len(out) == 0 || out[len(out)-1] != memoryID {
			out = append(out, memoryID)
		}
	}
	return out, nil
}
