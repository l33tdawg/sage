package web

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
)

const (
	appV23DashboardProjectionPageSize = 1024
	// One interactive CEREBRUM request must stay responsive on encrypted
	// laptops. Continuation cursors preserve access to deeper results without
	// decrypting the store-wide recall budget in one HTTP request.
	appV23CerebrumInteractiveScanBudget = 4096
)

const appV23PartialProjectionMessage = "Some memories are temporarily hidden because their canonical state could not be verified. Your stored data was not changed."

var errAppV23DashboardProjectionUnavailable = errors.New(
	"app-v23 dashboard memory projection is unavailable",
)

var errAppV23DashboardListCursorInvalid = errors.New(
	"app-v23 dashboard list continuation is invalid",
)

type memoryProjectionRevisionProvider interface {
	MemoryProjectionRevision(context.Context) (uint64, error)
	VaultGeneration() uint64
}

type graphProjectionRevisionProvider interface {
	GraphProjectionRevision(context.Context) (uint64, error)
}

type projectionPublicationReader interface {
	LockProjectionPublicationRead() func()
}

type appV23GraphSourceToken struct {
	Memory        appV23ProjectionSourceToken
	Metadata      uint64
	Authorization uint64
}

func (t appV23GraphSourceToken) cacheKey() string {
	return fmt.Sprintf(
		"memory={%s}|metadata=%d|authorization=%d",
		t.Memory.cacheKey(), t.Metadata, t.Authorization,
	)
}

type appV23ProjectionSourceToken struct {
	SQLRevision       uint64
	CanonicalRevision uint64
	VaultGeneration   uint64
	CanonicalSubset   bool
}

func (t appV23ProjectionSourceToken) cacheKey() string {
	return fmt.Sprintf(
		"sql=%d|canonical=%d|vault=%d|subset=%t",
		t.SQLRevision, t.CanonicalRevision, t.VaultGeneration,
		t.CanonicalSubset,
	)
}

type appV23ProjectionAuditResult struct {
	stats            *store.StoreStats
	activity         map[string]string
	projection       *appV23ProjectionResponse
	source           appV23ProjectionSourceToken
	cacheable        bool
	legacyCompatible bool
	quarantined      bool
	subset           bool
	stale            bool
	cacheState       string
}

func (r *appV23ProjectionAuditResult) clone() *appV23ProjectionAuditResult {
	if r == nil {
		return nil
	}
	clone := *r
	if r.stats != nil {
		stats := *r.stats
		stats.ByDomain = cloneIntMap(r.stats.ByDomain)
		stats.ByStatus = cloneIntMap(r.stats.ByStatus)
		stats.ByAgent = cloneIntMap(r.stats.ByAgent)
		if r.stats.LastActivity != nil {
			last := *r.stats.LastActivity
			stats.LastActivity = &last
		}
		clone.stats = &stats
	}
	clone.activity = cloneStringMap(r.activity)
	if r.projection != nil {
		projection := *r.projection
		clone.projection = &projection
	}
	return &clone
}

func (r *appV23ProjectionAuditResult) staleWhileRefreshing() *appV23ProjectionAuditResult {
	clone := r.clone()
	if clone == nil {
		return nil
	}
	// Preserve the exact source generation that was audited. The caller also
	// knows the current generation changed (Stale=true), but an old verified
	// graph may only be looked up under the generation that built it.
	clone.stale = true
	if clone.projection == nil {
		clone.projection = &appV23ProjectionResponse{}
	}
	clone.projection.Complete = false
	clone.projection.Partial = true
	clone.projection.State = "refreshing"
	clone.projection.HiddenCount = 0
	clone.projection.Stale = true
	clone.projection.Refreshing = true
	clone.projection.Message = "Memory totals are refreshing after a local change. Every displayed memory is still canonically verified."
	return clone
}

func appV23ProjectionChecking(
	source appV23ProjectionSourceToken,
) *appV23ProjectionAuditResult {
	return &appV23ProjectionAuditResult{
		source: source,
		stale:  true,
		projection: &appV23ProjectionResponse{
			Complete:     false,
			Partial:      true,
			VerifiedOnly: true,
			State:        store.CanonicalMemoryProjectionChecking,
			Message:      "Memory totals are checking in the background. Every displayed memory is still canonically verified.",
			Stale:        true,
			Refreshing:   true,
		},
		cacheState: store.CanonicalMemoryProjectionChecking,
	}
}

func cloneIntMap(in map[string]int) map[string]int {
	if in == nil {
		return nil
	}
	out := make(map[string]int, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

type appV23ProjectionAuditFlight struct {
	done    chan struct{}
	result  *appV23ProjectionAuditResult
	err     error
	waiters int
}

// appV23ProjectionResponse accompanies authenticated broad memory routes.
// HiddenCount is an aggregate only: identities, domains, authors, statuses,
// and failure reasons remain private. Public health must use the sanitized copy
// returned by publicAppV23ProjectionResponse.
type appV23ProjectionResponse struct {
	Complete     bool   `json:"complete"`
	Partial      bool   `json:"partial"`
	VerifiedOnly bool   `json:"verified_only"`
	State        string `json:"state"`
	HiddenCount  int    `json:"hidden_count,omitempty"`
	Message      string `json:"message,omitempty"`
	Stale        bool   `json:"stale,omitempty"`
	Refreshing   bool   `json:"refreshing,omitempty"`
}

func publicAppV23ProjectionResponse(
	projection *appV23ProjectionResponse,
) *appV23ProjectionResponse {
	if projection == nil {
		return nil
	}
	public := *projection
	public.HiddenCount = 0
	if public.Partial {
		public.Message = appV23PartialProjectionMessage
	}
	return &public
}

// projectionResponseForRequest reserves the hidden aggregate for the local
// human operator. Ordinary signed agents retain the partial/complete signal but
// cannot use the historical quarantine count to inventory records outside
// their own RBAC scope.
func (h *DashboardHandler) projectionResponseForRequest(
	r *http.Request,
	projection *appV23ProjectionResponse,
) *appV23ProjectionResponse {
	if projection == nil || h.isCEREBRUMOperatorRequest(r) {
		return projection
	}
	return publicAppV23ProjectionResponse(projection)
}

func (h *DashboardHandler) appV23ProjectionResponse() *appV23ProjectionResponse {
	if !h.appV23IsActive() || h.BadgerStore == nil {
		return nil
	}
	health := h.BadgerStore.CanonicalMemoryProjectionHealth()
	complete := health.Checked && health.Required && health.OK && !health.Quarantined
	response := &appV23ProjectionResponse{
		Complete:     complete,
		Partial:      !complete,
		VerifiedOnly: !health.LegacyCompatible,
		State:        health.State,
	}
	if !complete {
		response.Message = appV23PartialProjectionMessage
	}
	return response
}

func appV23ProjectionResponseFromAudit(
	legacyCompatible, quarantined, subset bool,
	hiddenCount int,
) *appV23ProjectionResponse {
	state := store.CanonicalMemoryProjectionExact
	switch {
	case quarantined:
		state = store.CanonicalMemoryProjectionQuarantined
	case subset:
		state = store.CanonicalMemoryProjectionSubset
	case legacyCompatible:
		state = store.CanonicalMemoryProjectionLegacyCompatible
	}
	response := &appV23ProjectionResponse{
		Complete:     !quarantined,
		Partial:      quarantined,
		VerifiedOnly: !legacyCompatible,
		State:        state,
		HiddenCount:  hiddenCount,
	}
	if quarantined {
		response.Message = appV23PartialProjectionMessage
		if hiddenCount > 0 {
			noun := "memories"
			verb := "are"
			if hiddenCount == 1 {
				noun = "memory"
				verb = "is"
			}
			response.Message = fmt.Sprintf(
				"%d historical %s could not be canonically verified and %s hidden. Readable memories remain available; hidden records stay preserved for governed recovery or deprecation.",
				hiddenCount, noun, verb,
			)
		}
	}
	return response
}

func appV23DashboardProjectionError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", errAppV23DashboardProjectionUnavailable, err)
}

func writeAppV23DashboardProjectionFailure(w http.ResponseWriter, err error) bool {
	if !errors.Is(err, errAppV23DashboardProjectionUnavailable) {
		return false
	}
	writeError(
		w,
		http.StatusServiceUnavailable,
		"canonical memory projection is temporarily unavailable",
	)
	return true
}

// validateAppV23DashboardRecord prevents CEREBRUM from treating the SQL
// serving projection as an authority boundary. After app-v23, content may be
// serialized or used in an aggregate only when the complete SQL record matches
// the canonical Badger disclosure snapshot.
func (h *DashboardHandler) validateAppV23DashboardRecord(
	record *memory.MemoryRecord,
) error {
	_, err := h.classifyAppV23DashboardRecord(record)
	return err
}

func (h *DashboardHandler) classifyAppV23DashboardRecord(
	record *memory.MemoryRecord,
) (store.MemoryProjectionDisposition, error) {
	if !h.appV23IsActive() {
		return store.MemoryProjectionExact, nil
	}
	if h.BadgerStore == nil {
		return store.MemoryProjectionUnpublished,
			appV23DashboardProjectionError(errors.New("canonical store is unavailable"))
	}
	_, disposition, err := h.BadgerStore.ClassifyMemoryProjection(record)
	if err != nil {
		return disposition, appV23DashboardProjectionError(err)
	}
	return disposition, nil
}

func isAppV23LegacyUnanchoredDashboardRecord(
	disposition store.MemoryProjectionDisposition,
	err error,
) bool {
	return disposition == store.MemoryProjectionLegacyUnanchored &&
		errors.Is(err, store.ErrMemoryProjectionUnpublished) &&
		errors.Is(err, store.ErrMemoryDisclosureNotFound)
}

func isAppV23BroadOmittableDashboardRecord(
	disposition store.MemoryProjectionDisposition,
	err error,
) bool {
	if isAppV23LegacyUnanchoredDashboardRecord(disposition, err) {
		return true
	}
	return disposition == store.MemoryProjectionQuarantined &&
		errors.Is(err, store.ErrMemoryProjectionUnpublished) &&
		errors.Is(err, store.ErrMemoryProjectionQuarantined)
}

// filterAppV23BroadDashboardRecords omits unsafe rows before they consume a
// visible result/count. Exact/detail and export paths intentionally do not use
// this helper: they retain fail-closed behavior. Broad callers must surface the
// aggregate projection health alongside their verified result so an all-hidden
// projection can never be mistaken for a genuinely empty brain.
func (h *DashboardHandler) filterAppV23BroadDashboardRecords(
	records []*memory.MemoryRecord,
) ([]*memory.MemoryRecord, error) {
	if !h.appV23IsActive() {
		return records, nil
	}
	kept := make([]*memory.MemoryRecord, 0, len(records))
	classifications, err := h.BadgerStore.ClassifyMemoryProjections(records)
	if err != nil {
		return nil, appV23DashboardProjectionError(err)
	}
	for i, record := range records {
		disposition := classifications[i].Disposition
		projectionErr := classifications[i].Err
		if projectionErr != nil {
			projectionErr = appV23DashboardProjectionError(projectionErr)
			if isAppV23BroadOmittableDashboardRecord(disposition, projectionErr) {
				// The shared batched classifier already records this proven
				// degradation while retaining one canonical Badger snapshot for
				// the page. Never reopen one transaction per hidden row here.
				continue
			}
			return nil, projectionErr
		}
		kept = append(kept, record)
	}
	return kept, nil
}

type appV23ProjectionWalkAudit struct {
	legacyCompatible bool
	quarantined      bool
	hiddenCount      int
	seenMemoryIDs    map[string]struct{}
}

func (h *DashboardHandler) appV23ProjectionSource(
	ctx context.Context,
) (appV23ProjectionSourceToken, bool, error) {
	token := appV23ProjectionSourceToken{
		CanonicalSubset: h.CanonicalProjectionMissingAllowedFn != nil,
	}
	if h.BadgerStore == nil {
		return token, false,
			appV23DashboardProjectionError(errors.New("canonical store is unavailable"))
	}
	provider, ok := h.store.(memoryProjectionRevisionProvider)
	if !ok {
		// Third-party/non-SQLite stores retain strict per-request auditing. They
		// still participate in in-flight coalescing, but no completed result is
		// reused without a transactionally maintained source generation.
		return token, false, nil
	}
	revision, err := provider.MemoryProjectionRevision(ctx)
	if err != nil {
		return token, false, err
	}
	token.SQLRevision = revision
	token.CanonicalRevision = h.BadgerStore.CanonicalMemoryProjectionRevision()
	token.VaultGeneration = provider.VaultGeneration()
	return token, true, nil
}

func (h *DashboardHandler) appV23GraphSource(
	ctx context.Context,
) (appV23GraphSourceToken, bool, error) {
	var token appV23GraphSourceToken
	memorySource, cacheable, err := h.appV23ProjectionSource(ctx)
	if err != nil {
		return token, false, err
	}
	token.Memory = memorySource
	provider, ok := h.store.(graphProjectionRevisionProvider)
	if !ok {
		return token, false, nil
	}
	token.Metadata, err = provider.GraphProjectionRevision(ctx)
	if err != nil {
		return token, false, err
	}
	if h.BadgerStore == nil {
		return token, false,
			appV23DashboardProjectionError(errors.New("canonical store is unavailable"))
	}
	token.Authorization = h.BadgerStore.GraphAuthorizationRevision()
	return token, cacheable, nil
}

func (h *DashboardHandler) publishAppV23ProjectionAudit(
	result *appV23ProjectionAuditResult,
) {
	if result == nil || h.BadgerStore == nil {
		return
	}
	if result.subset {
		h.BadgerStore.PublishCanonicalMemoryProjectionSubsetAudit(
			result.legacyCompatible, result.quarantined,
		)
		return
	}
	h.BadgerStore.PublishCanonicalMemoryProjectionAuditAt(
		true, result.legacyCompatible, result.quarantined, result.source.SQLRevision,
		result.source.CanonicalRevision, result.source.VaultGeneration,
	)
}

// appV23ProjectionAudit coalesces concurrent complete inventory walks. Broad
// callers may reuse the last immutable result only when both independently
// maintained memory-specific source revisions still match; strict callers
// always start or join a fresh walk.
func (h *DashboardHandler) appV23ProjectionAudit(
	ctx context.Context,
	allowCompletedSnapshot bool,
) (*appV23ProjectionAuditResult, error) {
	for attempt := 0; attempt < 3; attempt++ {
		source, cacheable, err := h.appV23ProjectionSource(ctx)
		if err != nil {
			return nil, err
		}

		h.projectionAuditMu.Lock()
		if allowCompletedSnapshot && cacheable &&
			h.projectionAuditLast != nil &&
			h.projectionAuditLast.cacheable &&
			h.projectionAuditLast.source == source {
			result := h.projectionAuditLast.clone()
			h.projectionAuditMu.Unlock()
			return result, nil
		}
		var stale *appV23ProjectionAuditResult
		if allowCompletedSnapshot && cacheable &&
			h.projectionAuditLast != nil &&
			h.projectionAuditLast.cacheable {
			stale = h.projectionAuditLast.staleWhileRefreshing()
		}
		flight := h.projectionAuditFlight
		if flight == nil {
			flight = &appV23ProjectionAuditFlight{
				done:    make(chan struct{}),
				waiters: 1,
			}
			h.projectionAuditFlight = flight
			h.projectionAuditMu.Unlock()

			h.runBackground(func(runCtx context.Context) {
				result, runErr := h.cerebrumVisibleStatsAndActivityOnce(runCtx)
				if runErr == nil {
					after, afterCacheable, tokenErr :=
						h.appV23ProjectionSource(runCtx)
					if tokenErr != nil {
						runErr = tokenErr
					} else if cacheable != afterCacheable ||
						(cacheable && source != after) {
						runErr = appV23DashboardProjectionError(
							errors.New("memory projection changed during audit"),
						)
					} else {
						result.source = after
						result.cacheable = afterCacheable
						h.publishAppV23ProjectionAudit(result)
					}
				}

				h.projectionAuditMu.Lock()
				flight.result = result
				flight.err = runErr
				if runErr == nil && result != nil && result.cacheable {
					h.projectionAuditLast = result.clone()
				}
				if h.projectionAuditFlight == flight {
					h.projectionAuditFlight = nil
				}
				close(flight.done)
				h.projectionAuditMu.Unlock()
			})
		} else {
			flight.waiters++
			h.projectionAuditMu.Unlock()
		}
		if stale != nil {
			return stale, nil
		}
		if allowCompletedSnapshot && cacheable {
			// Cold broad reads must not wait for an inventory-wide audit. The
			// flight continues in the background; handlers classify their
			// bounded candidates before serialization and omit global totals.
			return appV23ProjectionChecking(source), nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-flight.done:
			if flight.err != nil {
				if errors.Is(flight.err, errAppV23DashboardProjectionUnavailable) &&
					attempt < 2 {
					continue
				}
				return nil, flight.err
			}
			if flight.result == nil {
				return nil, appV23DashboardProjectionError(
					errors.New("canonical memory projection audit returned no result"),
				)
			}
			// A different-source caller may have joined the only safe in-flight
			// walk. Re-evaluate its token rather than accepting that result.
			if cacheable && (!flight.result.cacheable ||
				flight.result.source != source) {
				continue
			}
			return flight.result.clone(), nil
		}
	}
	return nil, appV23DashboardProjectionError(
		errors.New("memory projection kept changing during audit"),
	)
}

// requireAppV23DashboardProjectionAudited performs a complete canonical
// inventory audit and returns only the verified rows' aggregates. A quarantined
// row is deliberately not an error here: broad routes omit it and surface the
// aggregate health explicitly. Backend failures and an incomplete audit still
// fail closed.
func (h *DashboardHandler) requireAppV23DashboardProjectionAudited(
	ctx context.Context,
) (*store.StoreStats, map[string]string, *appV23ProjectionResponse, error) {
	if !h.appV23IsActive() {
		return nil, nil, nil, nil
	}
	if h.BadgerStore == nil {
		return nil, nil, nil,
			appV23DashboardProjectionError(errors.New("canonical store is unavailable"))
	}
	result, err := h.appV23ProjectionAudit(ctx, false)
	if err != nil {
		return nil, nil, nil, appV23DashboardProjectionError(err)
	}
	return result.stats, result.activity, result.projection, nil
}

// requireAppV23DashboardProjectionSnapshot is the broad/read-only companion to
// the strict audit gate. The returned aggregates are instant on a hot source
// token, while every record serialized by the route is still independently
// classified against Badger.
func (h *DashboardHandler) requireAppV23DashboardProjectionSnapshot(
	ctx context.Context,
) (*store.StoreStats, map[string]string, *appV23ProjectionResponse,
	appV23ProjectionSourceToken, error) {
	if !h.appV23IsActive() {
		return nil, nil, nil, appV23ProjectionSourceToken{}, nil
	}
	if h.BadgerStore == nil {
		return nil, nil, nil, appV23ProjectionSourceToken{},
			appV23DashboardProjectionError(errors.New("canonical store is unavailable"))
	}
	result, err := h.appV23ProjectionAudit(ctx, true)
	if err != nil {
		return nil, nil, nil, appV23ProjectionSourceToken{},
			appV23DashboardProjectionError(err)
	}
	return result.stats, result.activity, result.projection, result.source, nil
}

// requireAppV23DashboardProjectionAvailable performs a complete canonical
// inventory audit before every memory-derived CEREBRUM read. A missing or
// filter-hidden canonical memory has no row for a route-local query to validate,
// and a previously healthy audit cannot detect later SQL tamper. Re-auditing
// here also prevents a cached graph or tag aggregate from bypassing validation.
// Emergency correctness intentionally takes priority over read performance.
// Older app versions retain their historical behavior.
func (h *DashboardHandler) requireAppV23DashboardProjectionAvailable(
	ctx context.Context,
) (*store.StoreStats, map[string]string, *appV23ProjectionResponse, error) {
	stats, activity, projection, err := h.requireAppV23DashboardProjectionAudited(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	if !h.appV23IsActive() {
		return stats, activity, projection, nil
	}
	if projection == nil || projection.Partial {
		return nil, nil, nil, appV23DashboardProjectionError(
			errors.New("canonical memory projection audit is unavailable"),
		)
	}
	return stats, activity, projection, nil
}

// AuditAppV23CanonicalMemoryProjection performs the complete, deterministic
// ordinary-memory inventory walk used by readiness. It publishes only the
// safe finite-state result through BadgerStore; no memory identifiers or raw
// counts enter the public health surface.
func (h *DashboardHandler) AuditAppV23CanonicalMemoryProjection(
	ctx context.Context,
) error {
	if h.BadgerStore == nil {
		return appV23DashboardProjectionError(errors.New("canonical store is unavailable"))
	}
	if !h.appV23IsActive() {
		h.BadgerStore.PublishCanonicalMemoryProjectionAudit(false, false, false)
		return nil
	}
	_, _, _, err := h.requireAppV23DashboardProjectionAvailable(ctx)
	return err
}

// walkAppV23CanonicalDashboardRecords pages deterministically through one SQL
// result set and validates each row before passing it to visit. It deliberately
// returns no raw count: even aggregate metadata must be derived only from
// canonically published records.
func (h *DashboardHandler) walkAppV23CanonicalDashboardRecords(
	ctx context.Context,
	opts store.ListOptions,
	visit func(*memory.MemoryRecord) error,
) error {
	if !h.appV23IsActive() {
		return errors.New("app-v23 canonical dashboard walk requested before activation")
	}
	if h.BadgerStore == nil {
		return appV23DashboardProjectionError(errors.New("canonical store is unavailable"))
	}
	return h.walkAppV23DashboardProjectionPages(ctx, opts, func(
		records []*memory.MemoryRecord,
	) error {
		classifications, classifyErr :=
			h.BadgerStore.ClassifyMemoryProjections(records)
		if classifyErr != nil {
			return appV23DashboardProjectionError(classifyErr)
		}
		for i, record := range records {
			if classifications[i].Err != nil {
				return appV23DashboardProjectionError(
					classifications[i].Err,
				)
			}
			if visit != nil {
				if err := visit(record); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// walkAppV23TaintFilteredDashboardRecords is the export-specific canonical walk.
// Effective taint and pending taint holds are omitted as governed
// non-disclosable rows; every other classification failure still aborts the
// export so a corrupt backup cannot be presented as complete.
func (h *DashboardHandler) walkAppV23TaintFilteredDashboardRecords(
	ctx context.Context,
	opts store.ListOptions,
	visit func(*memory.MemoryRecord) error,
) error {
	if !h.appV23IsActive() {
		return errors.New("app-v23 canonical dashboard walk requested before activation")
	}
	if h.BadgerStore == nil {
		return appV23DashboardProjectionError(errors.New("canonical store is unavailable"))
	}
	return h.walkAppV23DashboardProjectionPages(ctx, opts, func(
		records []*memory.MemoryRecord,
	) error {
		classifications, classifyErr := h.BadgerStore.ClassifyMemoryProjections(records)
		if classifyErr != nil {
			return appV23DashboardProjectionError(classifyErr)
		}
		for i, record := range records {
			if classifications[i].Err != nil {
				if errors.Is(classifications[i].Err, store.ErrMemoryProjectionTainted) ||
					errors.Is(classifications[i].Err, store.ErrMemoryProjectionTaintPending) {
					continue
				}
				return appV23DashboardProjectionError(classifications[i].Err)
			}
			if visit != nil {
				if err := visit(record); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// walkAppV23DashboardProjectionPages prefers the built-in no-count keyset
// inventory capability. Complete audits need every row but never use the raw
// SQL total, so running COUNT(*) and then increasingly deep OFFSET scans on
// every page is pure overhead. A third-party store, or a caller requiring a
// non-chronological order, safely retains the historical bounded fallback.
func (h *DashboardHandler) walkAppV23DashboardProjectionPages(
	ctx context.Context,
	opts store.ListOptions,
	visitPage func([]*memory.MemoryRecord) error,
) error {
	opts.Limit = appV23DashboardProjectionPageSize
	opts.Offset = 0
	opts.StablePaging = true

	pager, keyset := h.store.(store.MemoryProjectionPageStore)
	if keyset && opts.Sort == "oldest" {
		var after store.MemoryProjectionPageCursor
		seenCursors := make(map[store.MemoryProjectionPageCursor]struct{})
		for {
			records, next, err := pager.ListMemoryProjectionPage(
				ctx, opts, after, appV23DashboardProjectionPageSize,
			)
			if err != nil {
				return err
			}
			if len(records) > appV23DashboardProjectionPageSize {
				return appV23DashboardProjectionError(
					errors.New("memory projection pager exceeded the requested page size"),
				)
			}
			if len(records) > 0 {
				last := records[len(records)-1]
				if last == nil || next.CreatedAt == "" || next.MemoryID == "" ||
					next.MemoryID != last.MemoryID {
					return appV23DashboardProjectionError(
						errors.New("memory projection pager returned a non-advancing cursor"),
					)
				}
				if _, repeated := seenCursors[next]; repeated {
					return appV23DashboardProjectionError(
						errors.New("memory projection pager returned a repeated cursor"),
					)
				}
				seenCursors[next] = struct{}{}
			}
			if err := visitPage(records); err != nil {
				return err
			}
			if len(records) < appV23DashboardProjectionPageSize {
				return nil
			}
			after = next
		}
	}

	for offset := 0; ; offset += appV23DashboardProjectionPageSize {
		opts.Offset = offset
		records, _, err := h.store.ListMemories(ctx, opts)
		if err != nil {
			return err
		}
		if len(records) > appV23DashboardProjectionPageSize {
			return appV23DashboardProjectionError(
				errors.New("memory projection fallback exceeded the requested page size"),
			)
		}
		if err := visitPage(records); err != nil {
			return err
		}
		if len(records) < appV23DashboardProjectionPageSize {
			return nil
		}
	}
}

// walkAppV23BroadDashboardRecords is the collection-safe companion to the exact
// walker above. Unsafe rows are omitted before visit, pagination, and aggregate
// counts. The audit retains health booleans, an internal inventory, and one
// aggregate hidden count. It never exposes a hidden identity, domain, author,
// status, or failure reason.
func (h *DashboardHandler) walkAppV23BroadDashboardRecords(
	ctx context.Context,
	opts store.ListOptions,
	visit func(*memory.MemoryRecord) error,
) (appV23ProjectionWalkAudit, error) {
	audit := appV23ProjectionWalkAudit{
		seenMemoryIDs: make(map[string]struct{}),
	}
	if !h.appV23IsActive() {
		return audit, errors.New("app-v23 broad dashboard walk requested before activation")
	}
	if h.BadgerStore == nil {
		return audit, appV23DashboardProjectionError(errors.New("canonical store is unavailable"))
	}
	err := h.walkAppV23DashboardProjectionPages(ctx, opts, func(
		records []*memory.MemoryRecord,
	) error {
		classifications, classifyErr :=
			h.BadgerStore.ClassifyMemoryProjections(records)
		if classifyErr != nil {
			return appV23DashboardProjectionError(classifyErr)
		}
		for i, record := range records {
			if record != nil {
				audit.seenMemoryIDs[record.MemoryID] = struct{}{}
			}
			disposition := classifications[i].Disposition
			projectionErr := classifications[i].Err
			if projectionErr != nil {
				projectionErr = appV23DashboardProjectionError(projectionErr)
				if isAppV23BroadOmittableDashboardRecord(disposition, projectionErr) {
					audit.quarantined = true
					audit.hiddenCount++
					continue
				}
				return projectionErr
			}
			if disposition == store.MemoryProjectionLegacyTerminalHashless {
				audit.legacyCompatible = true
			}
			if visit != nil {
				if visitErr := visit(record); visitErr != nil {
					return visitErr
				}
			}
		}
		return nil
	})
	if err != nil {
		return audit, err
	}
	return audit, nil
}

// spoolAppV23CanonicalDashboardExport builds the entire portable backup in a
// private temporary file before the HTTP response is committed. App-v23 must
// never validate one live SQL walk and serialize a second: a projection ghost
// appearing between those walks could otherwise turn a 200 response into a
// plausible, truncated JSONL backup. The caller owns and removes the returned
// file. CreateTemp uses mode 0600, which matters because an unlocked encrypted
// node writes plaintext export content here briefly.
func (h *DashboardHandler) spoolAppV23CanonicalDashboardExport(
	ctx context.Context,
	opts store.ListOptions,
) (*os.File, int64, error) {
	snapshot, err := os.CreateTemp("", "sage-appv23-export-*.jsonl")
	if err != nil {
		return nil, 0, fmt.Errorf("create app-v23 export snapshot: %w", err)
	}
	keepSnapshot := false
	defer func() {
		if keepSnapshot {
			return
		}
		_ = snapshot.Close()
		_ = os.Remove(snapshot.Name())
	}()

	encoder := json.NewEncoder(snapshot)
	exported := 0
	seenMemoryIDs := make(map[string]struct{})
	// The sealed walk must inventory every SQL memory, including deprecated and
	// internal rows that are not portable export content. Comparing a separate
	// preflight walk with this file would create a deletion/commit race that
	// could still label a partial file as a complete backup.
	inventoryOpts := opts
	inventoryOpts.Status = ""
	inventoryOpts.ExcludeDomainPrefixes = nil
	if walkErr := h.walkAppV23TaintFilteredDashboardRecords(
		ctx,
		inventoryOpts,
		func(record *memory.MemoryRecord) error {
			seenMemoryIDs[record.MemoryID] = struct{}{}
			if record.Status == memory.StatusDeprecated ||
				isCerebrumInternalMemoryDomain(record.DomainTag) {
				return nil
			}
			if encodeErr := encoder.Encode(portableDashboardMemoryRecord(record)); encodeErr != nil {
				return fmt.Errorf("encode app-v23 export snapshot: %w", encodeErr)
			}
			exported++
			return nil
		},
	); walkErr != nil {
		return nil, 0, walkErr
	}
	canonicalMemoryIDs, inventoryErr := store.CanonicalMemoryIDs(h.BadgerStore)
	if inventoryErr != nil {
		return nil, 0, appV23DashboardProjectionError(inventoryErr)
	}
	for _, memoryID := range canonicalMemoryIDs {
		if _, ok := seenMemoryIDs[memoryID]; !ok {
			verdict, taintErr := h.BadgerStore.ClassifyMemoryTaint(memoryID)
			if taintErr != nil {
				return nil, 0, appV23DashboardProjectionError(taintErr)
			}
			if verdict.Outcome != store.TaintServingClear {
				continue
			}
			return nil, 0, appV23DashboardProjectionError(
				errors.New("serving projection is missing canonical memory records"),
			)
		}
	}
	if exported == 0 {
		if encodeErr := encoder.Encode(dashboardExportManifest()); encodeErr != nil {
			return nil, 0, fmt.Errorf("encode app-v23 export manifest: %w", encodeErr)
		}
	}

	info, err := snapshot.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("stat app-v23 export snapshot: %w", err)
	}
	if _, seekErr := snapshot.Seek(0, io.SeekStart); seekErr != nil {
		return nil, 0, fmt.Errorf("rewind app-v23 export snapshot: %w", seekErr)
	}
	keepSnapshot = true
	return snapshot, info.Size(), nil
}

// appV23CanonicalDashboardPage validates only the bounded SQL candidate page.
// Unsafe candidates are omitted before serialization. The returned total is a
// safe pagination hint, not the raw SQL inventory count: it never includes a
// hidden row, and adds at most one sentinel when another raw page may exist.
// Exact/global inventory semantics are reserved for export and aggregate audit.
type appV23DashboardListCursor struct {
	Source      string `json:"source"`
	Query       string `json:"query"`
	RawOffset   int    `json:"raw_offset"`
	VisibleSeen int    `json:"visible_seen"`
}

func appV23DashboardListQueryKey(opts store.ListOptions) string {
	return fmt.Sprintf(
		"domain=%q|tag=%q|provider=%q|status=%q|agent=%q|agents=%q|from=%q|to=%q|sort=%q",
		opts.DomainTag, opts.Tag, opts.Provider, opts.Status,
		opts.SubmittingAgent, opts.SubmittingAgents,
		opts.CreatedFrom, opts.CreatedTo, opts.Sort,
	)
}

func encodeAppV23DashboardListCursor(cursor appV23DashboardListCursor) string {
	body, err := json.Marshal(cursor)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(body)
}

func decodeAppV23DashboardListCursor(
	raw string,
) (appV23DashboardListCursor, error) {
	var cursor appV23DashboardListCursor
	body, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return cursor, errAppV23DashboardListCursorInvalid
	}
	if err := json.Unmarshal(body, &cursor); err != nil {
		return cursor, errAppV23DashboardListCursorInvalid
	}
	if cursor.RawOffset < 0 || cursor.VisibleSeen < 0 ||
		cursor.VisibleSeen > cursor.RawOffset {
		return cursor, errAppV23DashboardListCursorInvalid
	}
	return cursor, nil
}

func (h *DashboardHandler) appV23ProjectionIsExactForCurrentSource(
	ctx context.Context,
) (appV23ProjectionSourceToken, bool, error) {
	source, cacheable, err := h.appV23ProjectionSource(ctx)
	if err != nil || !cacheable {
		return source, false, err
	}
	h.projectionAuditMu.Lock()
	defer h.projectionAuditMu.Unlock()
	last := h.projectionAuditLast
	return source, last != nil &&
		last.cacheable &&
		last.source == source &&
		last.projection != nil &&
		last.projection.Complete &&
		!last.projection.Partial &&
		!last.projection.Stale, nil
}

func (h *DashboardHandler) appV23CanonicalDashboardPage(
	ctx context.Context,
	opts store.ListOptions,
	continuation string,
) ([]*memory.MemoryRecord, int, string, bool, error) {
	records, total, next, continuationRequired, _, err :=
		h.appV23CanonicalDashboardPageObserved(ctx, opts, continuation)
	return records, total, next, continuationRequired, err
}

func (h *DashboardHandler) appV23CanonicalDashboardPageObserved(
	ctx context.Context,
	opts store.ListOptions,
	continuation string,
) ([]*memory.MemoryRecord, int, string, bool, int, error) {
	visibleLimit := opts.Limit
	if visibleLimit <= 0 {
		visibleLimit = 50
	}
	if visibleLimit > appV23DashboardProjectionPageSize {
		visibleLimit = appV23DashboardProjectionPageSize
	}
	visibleOffset := opts.Offset
	if visibleOffset < 0 {
		visibleOffset = 0
	}
	source, exact, err := h.appV23ProjectionIsExactForCurrentSource(ctx)
	if err != nil {
		return nil, 0, "", false, 0, err
	}
	if exact && continuation == "" {
		pageOpts := opts
		pageOpts.StablePaging = true
		records, total, listErr := h.store.ListMemories(ctx, pageOpts)
		if listErr != nil {
			return nil, 0, "", false, 0, listErr
		}
		safe, filterErr := h.filterAppV23BroadDashboardRecords(records)
		if filterErr != nil {
			return nil, 0, "", false, 0, filterErr
		}
		if len(safe) != len(records) {
			return nil, 0, "", false, 0, appV23DashboardProjectionError(
				errors.New("exact memory projection changed during page read"),
			)
		}
		return safe, total, "", false, 0, nil
	}

	const rawPageSize = appV23DashboardProjectionPageSize
	page := make([]*memory.MemoryRecord, 0, appV23DashboardProjectionPageSize)
	visibleSeen := 0
	rawOffset := 0
	queryKey := appV23DashboardListQueryKey(opts)
	if continuation != "" {
		cursor, cursorErr := decodeAppV23DashboardListCursor(continuation)
		if cursorErr != nil || cursor.Source != source.cacheKey() ||
			cursor.Query != queryKey {
			return nil, 0, "", false, 0, errAppV23DashboardListCursorInvalid
		}
		rawOffset = cursor.RawOffset
		visibleSeen = cursor.VisibleSeen
	}
	scanStart := rawOffset
	rawExhausted := false
	hasMoreVisible := false
	nextCursor := ""
	observedHidden := 0
	for rawOffset-scanStart < appV23CerebrumInteractiveScanBudget &&
		len(page) < visibleLimit {
		batchLimit := rawPageSize
		if remaining := appV23CerebrumInteractiveScanBudget -
			(rawOffset - scanStart); remaining < batchLimit {
			batchLimit = remaining
		}
		batchOpts := opts
		batchOpts.Offset = rawOffset
		batchOpts.Limit = batchLimit
		batchOpts.StablePaging = true
		records, rawTotal, err := h.store.ListMemories(ctx, batchOpts)
		if err != nil {
			return nil, 0, "", false, observedHidden, err
		}
		safe, err := h.filterAppV23BroadDashboardRecords(records)
		if err != nil {
			return nil, 0, "", false, observedHidden, err
		}
		observedHidden += len(records) - len(safe)
		safeIndex := 0
		for rawIndex, record := range records {
			if safeIndex >= len(safe) || safe[safeIndex] != record {
				continue
			}
			safeIndex++
			if visibleSeen < visibleOffset {
				visibleSeen++
				continue
			}
			page = append(page, record)
			visibleSeen++
			if len(page) == visibleLimit {
				rawOffset += rawIndex + 1
				rawExhausted = rawOffset >= rawTotal
				hasMoreVisible = !rawExhausted
				if hasMoreVisible {
					nextCursor = encodeAppV23DashboardListCursor(
						appV23DashboardListCursor{
							Source: source.cacheKey(), Query: queryKey,
							RawOffset: rawOffset, VisibleSeen: visibleSeen,
						},
					)
				}
				break
			}
		}
		if len(page) == visibleLimit {
			break
		}
		rawOffset += len(records)
		rawExhausted = len(records) < batchLimit || rawOffset >= rawTotal
		if rawExhausted {
			break
		}
	}
	if !rawExhausted && len(page) < visibleLimit {
		nextCursor = encodeAppV23DashboardListCursor(
			appV23DashboardListCursor{
				Source: source.cacheKey(), Query: queryKey,
				RawOffset: rawOffset, VisibleSeen: visibleSeen,
			},
		)
	}
	// Never expose rawTotal: it may include quarantined rows. When another raw
	// candidate exists, one sentinel keeps pagination moving without claiming a
	// hidden inventory size.
	total := visibleOffset + len(page)
	if !rawExhausted || hasMoreVisible {
		total++
	}
	return page, total, nextCursor,
		!rawExhausted && len(page) < visibleLimit, observedHidden, nil
}

// appV23CanonicalKeywordPage is the encrypted-vault/deprecated-index fallback
// for CEREBRUM text search. It scans at most one authorization budget per HTTP
// request, validates candidates in canonical batches, and returns a source/query
// bound continuation. This keeps old encrypted memories searchable without
// turning one request into an unbounded full-corpus decrypt.
func (h *DashboardHandler) appV23CanonicalKeywordPage(
	ctx context.Context,
	opts store.ListOptions,
	keyword, continuation string,
) ([]*memory.MemoryRecord, int, string, bool, int, error) {
	visibleLimit := opts.Limit
	if visibleLimit <= 0 {
		visibleLimit = 50
	}
	if visibleLimit > appV23DashboardProjectionPageSize {
		visibleLimit = appV23DashboardProjectionPageSize
	}
	visibleOffset := opts.Offset
	if visibleOffset < 0 {
		visibleOffset = 0
	}
	source, _, err := h.appV23ProjectionIsExactForCurrentSource(ctx)
	if err != nil {
		return nil, 0, "", false, 0, err
	}
	queryKey := appV23DashboardListQueryKey(opts) + "|keyword=" + keyword
	rawOffset := 0
	matchesSeen := 0
	if continuation != "" {
		cursor, cursorErr := decodeAppV23DashboardListCursor(continuation)
		if cursorErr != nil || cursor.Source != source.cacheKey() ||
			cursor.Query != queryKey {
			return nil, 0, "", false, 0, errAppV23DashboardListCursorInvalid
		}
		rawOffset = cursor.RawOffset
		matchesSeen = cursor.VisibleSeen
	}

	const rawPageSize = appV23DashboardProjectionPageSize
	needle := strings.ToLower(keyword)
	page := make([]*memory.MemoryRecord, 0, appV23DashboardProjectionPageSize)
	scanStart := rawOffset
	rawExhausted := false
	observedHidden := 0
	for rawOffset-scanStart < appV23CerebrumInteractiveScanBudget &&
		len(page) < visibleLimit {
		batchLimit := rawPageSize
		if remaining := appV23CerebrumInteractiveScanBudget -
			(rawOffset - scanStart); remaining < batchLimit {
			batchLimit = remaining
		}
		batchOpts := opts
		batchOpts.Offset = rawOffset
		batchOpts.Limit = batchLimit
		batchOpts.StablePaging = true
		records, rawTotal, listErr := h.store.ListMemories(ctx, batchOpts)
		if listErr != nil {
			return nil, 0, "", false, observedHidden, listErr
		}
		safe, filterErr := h.filterAppV23BroadDashboardRecords(records)
		if filterErr != nil {
			return nil, 0, "", false, observedHidden, filterErr
		}
		observedHidden += len(records) - len(safe)
		safeIDs := make(map[string]struct{}, len(safe))
		for _, record := range safe {
			safeIDs[record.MemoryID] = struct{}{}
		}
		filledAt := -1
		for rawIndex, record := range records {
			if _, ok := safeIDs[record.MemoryID]; !ok {
				continue
			}
			if !strings.Contains(strings.ToLower(record.Content), needle) &&
				!strings.Contains(strings.ToLower(record.DomainTag), needle) {
				continue
			}
			if matchesSeen < visibleOffset {
				matchesSeen++
				continue
			}
			page = append(page, record)
			matchesSeen++
			if len(page) == visibleLimit {
				filledAt = rawIndex
				break
			}
		}
		if filledAt >= 0 {
			rawOffset += filledAt + 1
			rawExhausted = rawOffset >= rawTotal
			break
		}
		rawOffset += len(records)
		rawExhausted = len(records) < batchLimit || rawOffset >= rawTotal
		if rawExhausted {
			break
		}
	}

	nextCursor := ""
	if !rawExhausted {
		nextCursor = encodeAppV23DashboardListCursor(
			appV23DashboardListCursor{
				Source: source.cacheKey(), Query: queryKey,
				RawOffset: rawOffset, VisibleSeen: matchesSeen,
			},
		)
	}
	total := visibleOffset + len(page)
	if nextCursor != "" {
		total++
	}
	return page, total, nextCursor, nextCursor != "", observedHidden, nil
}

// appV23CanonicalDashboardCandidates fills a bounded presentation sample with
// canonically visible records rather than letting raw SQL rows consume the
// caller's limit before disclosure validation. It is used by graph rendering,
// where returning zero nodes merely because the newest raw window is
// quarantined would falsely present a non-empty brain as empty.
func (h *DashboardHandler) appV23CanonicalDashboardCandidates(
	ctx context.Context,
	opts store.ListOptions,
	visibleLimit int,
	scanBudget int,
) ([]*memory.MemoryRecord, bool, error) {
	if visibleLimit <= 0 {
		return nil, false, nil
	}
	if visibleLimit > appV23CerebrumInteractiveScanBudget {
		visibleLimit = appV23CerebrumInteractiveScanBudget
	}
	if scanBudget > appV23CerebrumInteractiveScanBudget {
		scanBudget = appV23CerebrumInteractiveScanBudget
	}
	if scanBudget < visibleLimit {
		scanBudget = visibleLimit
	}
	// Sample fixed-size windows across the entire raw ordering. Sequentially
	// walking from offset zero made a 2,500-node graph decrypt 40k+ rows when a
	// recovered brain had a quarantined prefix, and it biased the MRI toward the
	// newest slice. Fixed 512-row windows cover newest, middle, and oldest
	// history while keeping encrypted first-paint work bounded.
	const rawPageSize = 512
	firstOpts := opts
	firstOpts.Offset = 0
	firstOpts.Limit = min(rawPageSize, scanBudget)
	firstOpts.StablePaging = true
	first, rawTotal, err := h.store.ListMemories(ctx, firstOpts)
	if err != nil {
		return nil, false, err
	}
	windowCount := (min(scanBudget, rawTotal) + rawPageSize - 1) / rawPageSize
	if windowCount < 1 {
		windowCount = 1
	}
	maxOffset := rawTotal - rawPageSize
	if maxOffset < 0 {
		maxOffset = 0
	}
	offsets := make(
		[]int,
		0,
		appV23CerebrumInteractiveScanBudget/rawPageSize,
	)
	for i := 0; i < windowCount; i++ {
		offset := 0
		if windowCount > 1 {
			offset = i * maxOffset / (windowCount - 1)
		}
		if len(offsets) == 0 || offsets[len(offsets)-1] != offset {
			offsets = append(offsets, offset)
		}
	}

	candidates := make(
		[]*memory.MemoryRecord,
		0,
		appV23CerebrumInteractiveScanBudget,
	)
	seen := make(map[string]struct{}, appV23CerebrumInteractiveScanBudget)
	scanned := 0
	for windowIndex, rawOffset := range offsets {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		batchLimit := min(rawPageSize, scanBudget-scanned)
		if batchLimit <= 0 {
			break
		}
		var records []*memory.MemoryRecord
		if windowIndex == 0 {
			records = first
			if len(records) > batchLimit {
				records = records[:batchLimit]
			}
		} else {
			pageOpts := opts
			pageOpts.Offset = rawOffset
			pageOpts.Limit = batchLimit
			pageOpts.StablePaging = true
			// rawTotal came from the first page. Recounting the whole filtered
			// table for every representative window multiplied cold-start cost.
			pageOpts.SkipTotal = true
			var listErr error
			records, _, listErr = h.store.ListMemories(ctx, pageOpts)
			if listErr != nil {
				return nil, false, listErr
			}
		}
		scanned += len(records)
		safe, filterErr := h.filterAppV23BroadDashboardRecords(records)
		if filterErr != nil {
			return nil, false, filterErr
		}
		for _, record := range safe {
			if record == nil {
				continue
			}
			if _, duplicate := seen[record.MemoryID]; duplicate {
				continue
			}
			seen[record.MemoryID] = struct{}{}
			candidates = append(candidates, record)
		}
	}
	return representativeGraphRecords(candidates, visibleLimit),
		rawTotal > scanned || len(candidates) > visibleLimit, nil
}

// representativeGraphRecords keeps one deterministic candidate per domain,
// then fills the remaining slots evenly across the time/confidence-distributed
// candidate pool. Rare domains therefore remain visible without turning the
// graph build into one query per domain.
func representativeGraphRecords(
	candidates []*memory.MemoryRecord,
	limit int,
) []*memory.MemoryRecord {
	if limit <= 0 || len(candidates) == 0 {
		return nil
	}
	if len(candidates) <= limit {
		return candidates
	}
	selected := make([]bool, len(candidates))
	domains := make(map[string]struct{})
	selectedCount := 0
	for i, record := range candidates {
		if record == nil {
			continue
		}
		if _, exists := domains[record.DomainTag]; exists {
			continue
		}
		domains[record.DomainTag] = struct{}{}
		selected[i] = true
		selectedCount++
		if selectedCount == limit {
			break
		}
	}
	if selectedCount < limit {
		remaining := make([]int, 0, len(candidates)-selectedCount)
		for i := range candidates {
			if !selected[i] {
				remaining = append(remaining, i)
			}
		}
		slots := limit - selectedCount
		for slot := 0; slot < slots && len(remaining) > 0; slot++ {
			index := slot * len(remaining) / slots
			if index >= len(remaining) {
				index = len(remaining) - 1
			}
			selected[remaining[index]] = true
		}
	}
	result := make([]*memory.MemoryRecord, 0, limit)
	for i, record := range candidates {
		if selected[i] {
			result = append(result, record)
			if len(result) == limit {
				break
			}
		}
	}
	return result
}

func (h *DashboardHandler) cerebrumVisibleStatsAndActivity(
	ctx context.Context,
) (*store.StoreStats, map[string]string, *appV23ProjectionResponse, error) {
	if !h.appV23IsActive() {
		stats, err := h.legacyCerebrumVisibleStats(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		var activity map[string]string
		if provider, ok := h.store.(domainActivityProvider); ok {
			activity, _ = provider.GetDomainLastActivity(ctx)
			for domain := range activity {
				if isCerebrumInternalMemoryDomain(domain) {
					delete(activity, domain)
				}
			}
		}
		return stats, activity, nil, nil
	}
	result, err := h.appV23ProjectionAudit(ctx, true)
	if err != nil {
		return nil, nil, nil, err
	}
	return result.stats, result.activity, result.projection, nil
}

func (h *DashboardHandler) cerebrumVisibleStatsAndActivityOnce(
	ctx context.Context,
) (*appV23ProjectionAuditResult, error) {
	h.BadgerStore.BeginCanonicalMemoryProjectionAudit()

	// Preserve backend-local size metadata, but never any SQL-derived counts.
	rawStats, err := h.store.GetStats(ctx)
	if err != nil {
		return nil, err
	}
	stats := &store.StoreStats{
		ByDomain: make(map[string]int),
		ByStatus: make(map[string]int),
		ByAgent:  make(map[string]int),
	}
	if rawStats != nil {
		stats.DBSizeBytes = rawStats.DBSizeBytes
	}
	latestByDomain := make(map[string]time.Time)
	audit, err := h.walkAppV23BroadDashboardRecords(
		ctx,
		store.ListOptions{Sort: "oldest"},
		func(record *memory.MemoryRecord) error {
			if isCerebrumInternalMemoryDomain(record.DomainTag) {
				return nil
			}
			status := string(record.Status)
			stats.ByStatus[status]++
			stats.ByAgent[record.SubmittingAgent]++
			if stats.LastActivity == nil || record.CreatedAt.After(*stats.LastActivity) {
				at := record.CreatedAt
				stats.LastActivity = &at
			}
			if record.Status != memory.StatusDeprecated {
				stats.TotalMemories++
				stats.ByDomain[record.DomainTag]++
				if latest, ok := latestByDomain[record.DomainTag]; !ok || record.CreatedAt.After(latest) {
					latestByDomain[record.DomainTag] = record.CreatedAt
				}
			}
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	subsetAllowed := h.CanonicalProjectionMissingAllowedFn != nil
	canonicalMemoryIDs, inventoryErr := store.CanonicalMemoryIDs(h.BadgerStore)
	if inventoryErr != nil {
		return nil, appV23DashboardProjectionError(inventoryErr)
	}
	for _, memoryID := range canonicalMemoryIDs {
		if _, ok := audit.seenMemoryIDs[memoryID]; ok {
			continue
		}
		if subsetAllowed && h.CanonicalProjectionMissingAllowedFn(memoryID) {
			continue
		}
		audit.quarantined = true
		audit.hiddenCount++
	}
	activity := make(map[string]string, len(latestByDomain))
	for domain, at := range latestByDomain {
		activity[domain] = at.UTC().Format(time.RFC3339Nano)
	}
	projection := appV23ProjectionResponseFromAudit(
		audit.legacyCompatible, audit.quarantined, subsetAllowed,
		audit.hiddenCount,
	)
	return &appV23ProjectionAuditResult{
		stats:            stats,
		activity:         activity,
		projection:       projection,
		legacyCompatible: audit.legacyCompatible,
		quarantined:      audit.quarantined,
		subset:           subsetAllowed,
		cacheState:       projection.State,
	}, nil
}

func (h *DashboardHandler) appV23CanonicalTimeline(
	ctx context.Context,
	from, to time.Time,
	domain, bucket string,
) ([]store.TimelineBucket, error) {
	counts := make(map[string]int)
	formatter, _ := h.store.(store.TimelinePeriodFormatter)
	// SQLite's RFC3339Nano text timestamps do not sort chronologically at
	// fractional-second boundaries. Widen SQL candidates and enforce the
	// exact public range below, as the REST timeline does.
	_, err := h.walkAppV23BroadDashboardRecords(
		ctx,
		cerebrumListOptions(store.ListOptions{
			DomainTag:   domain,
			CreatedFrom: from.UTC().Add(-time.Second).Format(time.RFC3339Nano),
			CreatedTo:   to.UTC().Add(time.Second).Format(time.RFC3339Nano),
			Sort:        "oldest",
		}),
		func(record *memory.MemoryRecord) error {
			// Exclude widened candidates outside the exact public time bounds,
			// including for custom stores without CreatedFrom/CreatedTo support.
			if record.CreatedAt.Before(from) || record.CreatedAt.After(to) {
				return nil
			}
			period := record.CreatedAt.UTC().Format("2006-01-02")
			if formatter != nil {
				period = formatter.FormatTimelinePeriod(record.CreatedAt, bucket)
			}
			counts[period]++
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	periods := make([]string, 0, len(counts))
	for period := range counts {
		periods = append(periods, period)
	}
	sort.Strings(periods)
	result := make([]store.TimelineBucket, 0, len(periods))
	for _, period := range periods {
		result = append(result, store.TimelineBucket{
			Period: period,
			Count:  counts[period],
			Domain: domain,
		})
	}
	return result, nil
}
