package store

import (
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

// PublicMemoryEnvelopeDefect names one PUBLIC memory whose canonical envelope
// cannot produce an app-v28 public-memory leaf, together with every check it
// failed. It is diagnostic output only: it never carries memory contents.
type PublicMemoryEnvelopeDefect struct {
	MemoryID        string   `json:"memory_id"`
	Status          string   `json:"status"`
	Reasons         []string `json:"reasons"`
	BornUnderAppV25 bool     `json:"born_under_app_v25"`
	Quarantined     bool     `json:"quarantined"`
}

// PublicMemoryAudit is a read-only census of the PUBLIC memories that the
// app-v28 activation stage must fold into its sparse index. Blocking defects
// are records the stage would refuse; Quarantined records are the legacy class
// it skips by design.
type PublicMemoryAudit struct {
	Scanned          int                          `json:"scanned"`
	Public           int                          `json:"public"`
	Quarantined      int                          `json:"quarantined"`
	Blocking         int                          `json:"blocking"`
	Defects          []PublicMemoryEnvelopeDefect `json:"defects"`
	Truncated        bool                         `json:"truncated"`
	rawIdentityRules bool
}

// AuditPublicMemoryEnvelopes walks every persisted memory and reports the
// PUBLIC ones that cannot become a canonical leaf. maxDefects bounds the
// returned list; the counters still describe the whole corpus. Blocks are never
// returned, and no memory content is decrypted.
func (s *BadgerStore) AuditPublicMemoryEnvelopes(maxDefects int) (*PublicMemoryAudit, error) {
	if s == nil || s.db == nil {
		return nil, ErrPublicMemoryIndex
	}
	if s.txn != nil {
		return nil, fmt.Errorf("%w: audit requires a read-only root store", ErrPublicMemoryIndex)
	}
	var audit *PublicMemoryAudit
	err := s.withMemoryIdentitySnapshot(func(reader *BadgerStore, mode memoryIdentityMode) error {
		var err error
		audit, err = reader.auditPublicMemoryEnvelopesInSnapshot(maxDefects, mode)
		return err
	})
	return audit, err
}

func (s *BadgerStore) auditPublicMemoryEnvelopesInSnapshot(maxDefects int, mode memoryIdentityMode) (*PublicMemoryAudit, error) {
	if maxDefects <= 0 {
		maxDefects = 100
	}
	var identifiers []string
	err := s.view(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.PrefetchValues = false
		iterator := txn.NewIterator(options)
		defer iterator.Close()
		prefix := []byte("memory:")
		for iterator.Seek(prefix); iterator.ValidForPrefix(prefix); iterator.Next() {
			identifiers = append(identifiers, string(iterator.Item().Key()[len(prefix):]))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	audit := &PublicMemoryAudit{rawIdentityRules: mode.raw, Defects: make([]PublicMemoryEnvelopeDefect, 0, min(maxDefects, 16))}
	for _, identifier := range identifiers {
		audit.Scanned++
		state, stateErr := s.memoryDisclosureForIdentityMode(identifier, mode)
		if stateErr != nil {
			if !errors.Is(stateErr, ErrMemoryDisclosureCorrupt) &&
				!errors.Is(stateErr, ErrMemoryDisclosureNotFound) {
				return nil, stateErr
			}
			audit.Blocking++
			audit.appendDefect(maxDefects, PublicMemoryEnvelopeDefect{
				MemoryID: identifier,
				Reasons:  []string{"disclosure_state_" + disclosureErrorReason(stateErr)},
			})
			continue
		}
		// Classification is the public-set selector. A missing class record is
		// an INTERNAL default, not a public record, so it is outside this
		// census — the app-v28 stage's per-record authority checks own it.
		if !state.ClassificationRecorded || state.Classification != 0 {
			continue
		}
		audit.Public++
		defect := s.classifyPublicEnvelope(identifier, state, mode)
		if len(defect.Reasons) == 0 {
			continue
		}
		if defect.Quarantined {
			audit.Quarantined++
		} else {
			audit.Blocking++
		}
		audit.appendDefect(maxDefects, defect)
	}
	return audit, nil
}

func (a *PublicMemoryAudit) appendDefect(maxDefects int, defect PublicMemoryEnvelopeDefect) {
	if len(a.Defects) >= maxDefects {
		a.Truncated = true
		return
	}
	a.Defects = append(a.Defects, defect)
}

func disclosureErrorReason(err error) string {
	switch {
	case errors.Is(err, ErrMemoryDisclosureNotFound):
		return "not_found"
	case errors.Is(err, ErrMemoryDisclosureCorrupt):
		return "corrupt"
	default:
		return "unreadable"
	}
}

// classifyPublicEnvelope applies the same envelope rules as
// publicLeafFromCanonical. A pre-app-v25 PUBLIC record is eligible for the
// legacy quarantine for any envelope defect, because those records predate the
// canonical envelope contract and historical lifecycle transitions could erase
// or truncate their recorded fields. A record born under app-v25 or later must
// carry a complete envelope; every defect there is blocking.
func (s *BadgerStore) classifyPublicEnvelope(
	identifier string,
	state *MemoryDisclosureState,
	mode memoryIdentityMode,
) PublicMemoryEnvelopeDefect {
	defect := PublicMemoryEnvelopeDefect{
		MemoryID: identifier,
		Status:   state.Status,
	}
	_, bornUnderAppV25, markerErr := s.GetMemorySubmissionHeight(identifier)
	if markerErr != nil {
		defect.Reasons = []string{"submission_height_unreadable"}
		return defect
	}
	defect.BornUnderAppV25 = bornUnderAppV25

	switch len(state.ContentHash) {
	case 0:
		defect.Reasons = append(defect.Reasons, "content_hash_empty")
	case 32:
	default:
		defect.Reasons = append(defect.Reasons, fmt.Sprintf("content_hash_length_%d", len(state.ContentHash)))
	}
	if !state.DomainRecorded {
		defect.Reasons = append(defect.Reasons, "domain_not_recorded")
	}
	if !state.AuthorRecorded {
		defect.Reasons = append(defect.Reasons, "author_not_recorded")
	}
	if !state.AuthorPrincipalRecorded {
		defect.Reasons = append(defect.Reasons, "author_principal_not_recorded")
	}
	if state.CoCommitRecorded {
		defect.Reasons = append(defect.Reasons, "co_commit_tombstone")
	}
	if len(defect.Reasons) != 0 {
		defect.Quarantined = !bornUnderAppV25
		return defect
	}

	leaf := PublicMemoryLeaf{
		MemoryID:        identifier,
		Author:          state.Author,
		AuthorPrincipal: state.AuthorPrincipal,
		Domain:          state.Domain,
		Status:          state.Status,
	}
	copy(leaf.ContentHash[:], state.ContentHash)
	_, encodeErr := leaf.canonical()
	identityErr := validatePublicMemoryIdentity(identifier, mode.useRaw(identifier))
	if encodeErr != nil || identityErr != nil {
		defect.Reasons = []string{"canonical_encode_failed"}
		// An explicitly recorded but empty/malformed field is not the legacy
		// missing-binding class; keep it blocking on every record.
		defect.Quarantined = false
	}
	return defect
}
