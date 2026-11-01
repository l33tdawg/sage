package store

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/memory"
)

// ErrMemoryProjectionUnpublished marks an off-chain row that does not match
// the canonical Badger snapshot currently published to readers. Callers must
// fail closed: the row may be from the SQL-before-Badger crash window and will
// become visible only after consensus replay completes the matching Commit.
var ErrMemoryProjectionUnpublished = errors.New("memory projection is not canonically published")

// ErrMemoryProjectionQuarantined identifies a canonical memory entry that
// exists, but whose content cannot be safely disclosed. In particular, legacy
// ordinary lifecycle transitions stored a zero-length canonical content hash.
// A narrowly eligible pre-app-v23 terminal record may use the compatibility
// disposition below; every other zero-hash record remains quarantined.
//
// This sentinel is always wrapped together with ErrMemoryProjectionUnpublished
// so exact/detail callers continue to fail closed while broad collection
// callers may omit the single unsafe row without disclosing it or consuming a
// visible result/count.
var ErrMemoryProjectionQuarantined = errors.New("memory projection is quarantined")

// ErrMemoryDisclosureCorrupt marks a record-local canonical envelope that was
// found but cannot be decoded or is internally incomplete. It is distinct from
// a Badger/backend failure: broad reads may quarantine this one record, while
// exact reads still fail closed and infrastructure errors still fail globally.
var ErrMemoryDisclosureCorrupt = errors.New("canonical memory disclosure is corrupt")

// ErrMemoryProjectionTainted marks a canonical row withheld by an effective
// app-v30 governance taint decision.
var ErrMemoryProjectionTainted = errors.New("memory projection is tainted")

// ErrMemoryProjectionTaintPending marks a row held because an incomplete taint
// decision still covers an authenticated ancestor.
var ErrMemoryProjectionTaintPending = errors.New("memory projection taint is pending")

// MemoryProjectionDisposition describes a serving-layer row's publication state.
// Only Exact and LegacyTerminalHashless are returned without an error.
type MemoryProjectionDisposition string

const (
	MemoryProjectionExact MemoryProjectionDisposition = "exact"
	// MemoryProjectionLegacyTerminalHashless is the replay-neutral compatibility
	// path for a pre-app-v23 ordinary terminal memory whose historical lifecycle
	// transition erased its canonical hash. It verifies the surviving SQL
	// content/hash pair and every canonical envelope field still present, but it
	// must not be represented as a chain-retained content commitment.
	MemoryProjectionLegacyTerminalHashless MemoryProjectionDisposition = "legacy_terminal_hashless"
	// MemoryProjectionLegacyUnanchored identifies a completely absent canonical
	// memory envelope. It remains unpublished and exact reads fail; a broad
	// reader may omit it only while reporting degraded projection state.
	MemoryProjectionLegacyUnanchored MemoryProjectionDisposition = "legacy_unanchored"
	MemoryProjectionQuarantined      MemoryProjectionDisposition = "quarantined"
	MemoryProjectionUnpublished      MemoryProjectionDisposition = "unpublished"
	MemoryProjectionTainted          MemoryProjectionDisposition = "tainted"
	MemoryProjectionTaintPending     MemoryProjectionDisposition = "taint_pending"
)

// MemoryProjectionClassification is one row's exact result from a shared
// Badger read snapshot. Err retains the same typed error chain returned by
// ClassifyMemoryProjection.
type MemoryProjectionClassification struct {
	Disposition MemoryProjectionDisposition
	State       *MemoryDisclosureState
	Err         error
}

// ClassifyMemoryProjections validates a SQL page inside one Badger read
// transaction. The single-row classifier historically opened a transaction
// for every record (and performed up to seven gets), making a 17k-memory
// CEREBRUM audit needlessly expensive. This batch form preserves disposition
// and error semantics while pinning the whole page to one canonical snapshot.
// Per-row degraded dispositions are still observed by the shared process
// tracker so broad readers can surface a proven unsafe row immediately. A
// complete stable audit remains the only path that may replace that tracker
// with a healthy result.
func (s *BadgerStore) ClassifyMemoryProjections(
	records []*memory.MemoryRecord,
) ([]MemoryProjectionClassification, error) {
	if s == nil {
		return nil, errors.New("canonical store is unavailable")
	}
	results := make([]MemoryProjectionClassification, len(records))
	err := s.view(func(txn *badger.Txn) error {
		reader := &BadgerStore{
			db:                              s.db,
			txn:                             txn,
			canonicalMemoryProjectionHealth: s.canonicalMemoryProjectionHealth,
		}
		for i, record := range records {
			requireCompleteEnvelope := false
			if record != nil {
				var gateErr error
				requireCompleteEnvelope, gateErr =
					reader.appV25MemoryEnvelopeRequired(record.MemoryID)
				if gateErr != nil {
					return gateErr
				}
			}
			state, disposition, classifyErr :=
				reader.classifyCanonicalMemoryProjection(record, requireCompleteEnvelope)
			if classifyErr == nil {
				classifyErr = reader.applyMemoryTaintDisposition(record.MemoryID, &disposition)
			}
			reader.observeMemoryProjectionDisposition(disposition)
			results[i] = MemoryProjectionClassification{
				Disposition: disposition,
				State:       state,
				Err:         classifyErr,
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

// ValidateMemoryProjection verifies that a serving-layer record is the exact
// projection of one committed on-chain memory snapshot. It also returns the
// classification from that same Badger read transaction so disclosure policy
// cannot combine fields from opposite sides of a concurrent Commit.
func (s *BadgerStore) ValidateMemoryProjection(
	record *memory.MemoryRecord,
) (*MemoryDisclosureState, error) {
	state, _, err := s.ClassifyMemoryProjection(record)
	return state, err
}

// ValidateMemoryProjectionWithoutTaint validates the canonical SQL envelope
// without disclosing taint state. Authorization layers use this first so a
// caller who cannot read the record receives the ordinary denial rather than a
// 451 that confirms the protected record exists.
func (s *BadgerStore) ValidateMemoryProjectionWithoutTaint(
	record *memory.MemoryRecord,
) (*MemoryDisclosureState, error) {
	requireCompleteEnvelope := false
	var err error
	if record != nil {
		requireCompleteEnvelope, err =
			s.appV25MemoryEnvelopeRequired(record.MemoryID)
	}
	if err != nil {
		return nil, fmt.Errorf(
			"%w: read app-v25 disclosure gate: %w",
			ErrMemoryProjectionUnpublished, err,
		)
	}
	state, disposition, err := s.classifyCanonicalMemoryProjection(record, requireCompleteEnvelope)
	s.observeMemoryProjectionDisposition(disposition)
	return state, err
}

// ClassifyMemoryProjection performs the same fail-closed validation as
// ValidateMemoryProjection and additionally reports the narrow compatibility
// disposition used for pre-app-v23 terminal zero-hash records.
func (s *BadgerStore) ClassifyMemoryProjection(
	record *memory.MemoryRecord,
) (projection *MemoryDisclosureState, disposition MemoryProjectionDisposition, resultErr error) {
	requireCompleteEnvelope := false
	var err error
	if record != nil {
		requireCompleteEnvelope, err =
			s.appV25MemoryEnvelopeRequired(record.MemoryID)
	}
	if err != nil {
		return nil, MemoryProjectionUnpublished, fmt.Errorf(
			"%w: read app-v25 disclosure gate: %w",
			ErrMemoryProjectionUnpublished, err,
		)
	}
	state, disposition, resultErr := s.classifyCanonicalMemoryProjection(
		record,
		requireCompleteEnvelope,
	)
	if resultErr == nil {
		resultErr = s.applyMemoryTaintDisposition(record.MemoryID, &disposition)
	}
	s.observeMemoryProjectionDisposition(disposition)
	if resultErr != nil {
		return nil, disposition, resultErr
	}
	return state, disposition, nil
}

func (s *BadgerStore) appV25MemoryEnvelopeRequired(memoryID string) (bool, error) {
	if s == nil {
		return false, errors.New("canonical store is unavailable")
	}
	applied, err := s.GetAppliedUpgrade("app-v25")
	if err != nil {
		return false, err
	}
	if applied == nil {
		fresh, freshErr := s.GetFreshGenesisActivation()
		if freshErr != nil {
			return false, freshErr
		}
		// A fresh-v25+ origin has no historical corpus eligible for the
		// compatibility projection, and carries no synthetic applied record.
		return fresh != nil && fresh.AppVersion >= 25, nil
	}
	// app-v25 records the first consensus submission height for every memory
	// born under its rules. That AppHash-covered marker is the safe disclosure
	// cutoff: new records must always carry a complete canonical envelope, while
	// historical records retain the narrow pre-v25 compatibility view until the
	// governed background adoption worker installs their missing fields. A
	// global upgrade switch would hide the historical corpus during migration.
	_, found, err := s.GetMemorySubmissionHeight(memoryID)
	if err != nil {
		return false, err
	}
	return found, nil
}

func (s *BadgerStore) classifyCanonicalMemoryProjection(
	record *memory.MemoryRecord,
	requireCompleteEnvelope bool,
) (projection *MemoryDisclosureState, disposition MemoryProjectionDisposition, resultErr error) {
	if s == nil || record == nil || record.MemoryID == "" {
		return nil, MemoryProjectionUnpublished, fmt.Errorf(
			"%w: missing store or record identity", ErrMemoryProjectionUnpublished,
		)
	}
	state, err := s.GetMemoryDisclosureState(record.MemoryID)
	if err != nil {
		disposition = MemoryProjectionUnpublished
		if errors.Is(err, ErrMemoryDisclosureNotFound) {
			disposition = MemoryProjectionLegacyUnanchored
		}
		if errors.Is(err, ErrMemoryDisclosureCorrupt) {
			return nil, MemoryProjectionQuarantined, fmt.Errorf(
				"%w: %w: %w",
				ErrMemoryProjectionUnpublished, ErrMemoryProjectionQuarantined, err,
			)
		}
		return nil, disposition, fmt.Errorf(
			"%w: %w", ErrMemoryProjectionUnpublished, err,
		)
	}
	if requireCompleteEnvelope &&
		(!state.DomainRecorded || !state.AuthorRecorded || !state.ClassificationRecorded) {
		return nil, MemoryProjectionQuarantined, fmt.Errorf(
			"%w: %w: app-v25 canonical envelope is incomplete for %s",
			ErrMemoryProjectionUnpublished, ErrMemoryProjectionQuarantined, record.MemoryID,
		)
	}
	if string(record.Status) != state.Status {
		return nil, MemoryProjectionQuarantined, fmt.Errorf(
			"%w: %w: status mismatch for %s",
			ErrMemoryProjectionUnpublished, ErrMemoryProjectionQuarantined, record.MemoryID,
		)
	}
	if state.DomainRecorded && record.DomainTag != state.Domain {
		return nil, MemoryProjectionQuarantined, fmt.Errorf(
			"%w: %w: domain mismatch for %s",
			ErrMemoryProjectionUnpublished, ErrMemoryProjectionQuarantined, record.MemoryID,
		)
	}
	if state.AuthorRecorded && record.SubmittingAgent != state.Author {
		return nil, MemoryProjectionQuarantined, fmt.Errorf(
			"%w: %w: author mismatch for %s",
			ErrMemoryProjectionUnpublished, ErrMemoryProjectionQuarantined, record.MemoryID,
		)
	}

	if len(state.ContentHash) == 0 {
		if legacyTerminalHashlessProjectionEligible(record, state) {
			return state, MemoryProjectionLegacyTerminalHashless, nil
		}
		return nil, MemoryProjectionQuarantined, fmt.Errorf(
			"%w: %w: canonical content hash is unavailable for %s",
			ErrMemoryProjectionUnpublished, ErrMemoryProjectionQuarantined, record.MemoryID,
		)
	}
	if len(state.ContentHash) != sha256.Size ||
		len(record.ContentHash) != sha256.Size ||
		!bytes.Equal(record.ContentHash, state.ContentHash) {
		return nil, MemoryProjectionQuarantined, fmt.Errorf(
			"%w: %w: content hash mismatch for %s",
			ErrMemoryProjectionUnpublished, ErrMemoryProjectionQuarantined, record.MemoryID,
		)
	}
	if record.Content == "" {
		if !state.CoCommitRecorded {
			return nil, MemoryProjectionQuarantined, fmt.Errorf(
				"%w: %w: empty content is not a canonical hash-only co-commit for %s",
				ErrMemoryProjectionUnpublished, ErrMemoryProjectionQuarantined, record.MemoryID,
			)
		}
	} else if computed := memory.ComputeContentHash(record.Content); !bytes.Equal(computed, state.ContentHash) {
		return nil, MemoryProjectionQuarantined, fmt.Errorf(
			"%w: %w: content does not match canonical hash for %s",
			ErrMemoryProjectionUnpublished, ErrMemoryProjectionQuarantined, record.MemoryID,
		)
	}
	return state, MemoryProjectionExact, nil
}

func (s *BadgerStore) applyMemoryTaintDisposition(
	memoryID string,
	disposition *MemoryProjectionDisposition,
) error {
	verdict, err := s.ClassifyMemoryTaint(memoryID)
	if err != nil {
		*disposition = MemoryProjectionUnpublished
		return fmt.Errorf(
			"%w: classify taint: %w",
			ErrMemoryProjectionUnpublished, err,
		)
	}
	switch verdict.Outcome {
	case TaintServingTainted:
		*disposition = MemoryProjectionTainted
		return fmt.Errorf(
			"%w: %w",
			ErrMemoryProjectionUnpublished, ErrMemoryProjectionTainted,
		)
	case TaintServingPending:
		*disposition = MemoryProjectionTaintPending
		return fmt.Errorf(
			"%w: %w",
			ErrMemoryProjectionUnpublished, ErrMemoryProjectionTaintPending,
		)
	default:
		return nil
	}
}

func legacyTerminalHashlessProjectionEligible(
	record *memory.MemoryRecord,
	state *MemoryDisclosureState,
) bool {
	if record == nil || state == nil ||
		state.AuthorPrincipalRecorded || state.CoCommitRecorded ||
		len(record.ContentHash) != sha256.Size || record.Content == "" {
		return false
	}
	switch record.Status {
	case memory.StatusCommitted, memory.StatusDeprecated:
	default:
		return false
	}
	return bytes.Equal(record.ContentHash, memory.ComputeContentHash(record.Content))
}
