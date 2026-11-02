package rest

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/l33tdawg/sage/internal/appv23disclosure"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
)

const (
	appV23DisclosurePageSize = 128
	// appV23DisclosureScanBudget bounds live canonical/RBAC decisions per
	// authenticated request. A caller with access to only a sparse tail of a
	// broad result set must narrow the query rather than force a full-corpus
	// authorization walk on every retry.
	appV23DisclosureScanBudget = store.CandidateFilterScanBudget
)

var errAppV23RecordDisclosureUnavailable = errors.New(
	"app-v23 record disclosure state is unavailable",
)
var errAppV23DisclosureScanBudget = errors.New(
	"app-v23 authorization scan budget exceeded",
)

func appV23RecordDisclosureError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", errAppV23RecordDisclosureUnavailable, err)
}

func isUnsafeAppV23Projection(err error) bool {
	return errors.Is(err, store.ErrMemoryProjectionUnpublished) ||
		errors.Is(err, store.ErrMemoryProjectionQuarantined) ||
		errors.Is(err, store.ErrMemoryProjectionTainted) ||
		errors.Is(err, store.ErrMemoryProjectionTaintPending)
}

// appV23RecordDisclosure is the single post-v23 content-disclosure decision.
// Authorship and assignment are immutable provenance, not continuing read
// authority: every record is checked against the caller's current enrollment,
// legacy migration visibility envelope, domain/group/grant scope, and
// clearance. Pre-v23 handlers retain their historical policy paths.
type appV23RecordDisclosure struct {
	Allowed        bool
	Classification uint8
}

func (s *Server) evaluateAppV23RecordDisclosure(
	credentialID string,
	record *memory.MemoryRecord,
	at time.Time,
) (appV23RecordDisclosure, error) {
	return s.evaluateAppV23RecordDisclosureScoped(credentialID, record, at, nil)
}

// A nil request scope is the strict contract used by exact-only callers.
// Broad serving paths explicitly supply one shared scope for candidate filters,
// final authorization rechecks, and every raw page in the request.
func (s *Server) evaluateAppV23RecordDisclosureScoped(
	credentialID string,
	record *memory.MemoryRecord,
	at time.Time,
	scope *appV23ReadScope,
) (appV23RecordDisclosure, error) {
	if !s.isPostV23ForNextTx() {
		return appV23RecordDisclosure{Allowed: true}, nil
	}
	if s.badgerStore == nil || credentialID == "" {
		return appV23RecordDisclosure{}, errors.New("app-v23 record authorization state is unavailable")
	}
	if record == nil || record.MemoryID == "" {
		return appV23RecordDisclosure{}, fmt.Errorf(
			"%w: missing serving record identity", store.ErrMemoryProjectionQuarantined,
		)
	}

	var readScope *store.TaintServingReadScope
	if scope != nil {
		readScope = scope.taint
	}
	canonical, _, policyAllowed, err := s.badgerStore.ClassifyMemoryProjectionForRead(
		record, readScope,
		func(reader *store.BadgerStore, canonical *store.MemoryDisclosureState) (bool, error) {
			domain := record.DomainTag
			if canonical.DomainRecorded {
				domain = canonical.Domain
			}
			// Keep the explicit legacy projection fallback, but never derive
			// policy from a row that failed canonical envelope validation.
			if domainErr := store.ValidateAppV23DomainName(domain); domainErr != nil {
				return false, fmt.Errorf("%w: invalid canonical memory domain: %w",
					store.ErrMemoryProjectionQuarantined, domainErr)
			}
			author := record.SubmittingAgent
			if canonical.AuthorRecorded {
				author = canonical.Author
			}
			decision, policyErr := appv23disclosure.Evaluate(
				reader, credentialID,
				appv23disclosure.Record{
					SubmittingAgent: author,
					Domain:          domain,
					Classification:  canonical.Classification,
				}, at,
			)
			return decision.Allowed, policyErr
		},
	)
	if err != nil {
		// Only an authorized, typed ancestor-budget uncertainty has a partial
		// response shape. Backend, malformed state, and policy errors still abort.
		if scope != nil && scope.allowPartial && policyAllowed &&
			errors.Is(err, store.ErrMemoryProjectionTaintIndeterminate) &&
			errors.Is(err, store.ErrTaintServingWalkBudget) {
			scope.uncertain.Store(true)
			return appV23RecordDisclosure{}, nil
		}
		return appV23RecordDisclosure{}, err
	}
	if !policyAllowed {
		return appV23RecordDisclosure{}, nil
	}
	return appV23RecordDisclosure{Allowed: true, Classification: canonical.Classification}, nil
}

// collectAppV23VisibleRecords fills a caller-visible page from a stable raw
// paging source. Every store request is bounded, denied records consume no
// visible limit, and no denied/raw count is returned to the caller.
func (s *Server) collectAppV23VisibleRecords(
	ctx context.Context,
	credentialID string,
	visibleLimit int,
	fetch func(context.Context, int, int) ([]*memory.MemoryRecord, error),
) ([]*memory.MemoryRecord, error) {
	return s.collectAppV23VisibleRecordsScoped(ctx, credentialID, visibleLimit, fetch, newAppV23StrictReadScope())
}

func (s *Server) collectAppV23VisibleRecordsScoped(
	ctx context.Context,
	credentialID string,
	visibleLimit int,
	fetch func(context.Context, int, int) ([]*memory.MemoryRecord, error),
	scope *appV23ReadScope,
) ([]*memory.MemoryRecord, error) {
	if visibleLimit <= 0 {
		return []*memory.MemoryRecord{}, nil
	}
	visible := make([]*memory.MemoryRecord, 0, visibleLimit)
	now := time.Now()
	for offset := 0; len(visible) < visibleLimit; offset += appV23DisclosurePageSize {
		if offset >= appV23DisclosureScanBudget {
			return nil, errAppV23DisclosureScanBudget
		}
		pageLimit := min(appV23DisclosurePageSize, appV23DisclosureScanBudget-offset)
		batch, err := fetch(ctx, pageLimit, offset)
		if err != nil {
			return nil, err
		}
		rawCount := len(batch)
		for _, rec := range batch {
			disclosure, disclosureErr := s.evaluateAppV23RecordDisclosureScoped(
				credentialID, rec, now, scope,
			)
			if disclosureErr != nil {
				if isUnsafeAppV23Projection(disclosureErr) {
					continue
				}
				return nil, appV23RecordDisclosureError(disclosureErr)
			}
			if disclosure.Allowed {
				visible = append(visible, rec)
				if len(visible) == visibleLimit {
					break
				}
			}
		}
		if rawCount < pageLimit {
			break
		}
	}
	return visible, nil
}
