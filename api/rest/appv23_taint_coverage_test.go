package rest

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

// This is a manual canonical-lineage/decision fixture and a mock SQL projection,
// not a signed transaction, consensus, or actual expansion-completion proof.
func appV23TaintCoverageFixture(t *testing.T, complete, denied bool) (
	*Server, *store.BadgerStore, *appV23PagingStore, string, *memory.MemoryRecord,
) {
	t.Helper()
	srv, canonical, reader, owner, outsider := setupAppV23RESTAccess(t)
	rows := &appV23PagingStore{rbacMockMemoryStore: newRBACMockMemoryStore()}
	rows.badger = canonical
	srv.store = rows
	domain, author := "member.home", reader
	if denied {
		domain, author = "outsider.home", outsider
	}
	now := time.Now()
	deep := appV23PagingRecord("deep-serving-leaf", author, domain, "serving withheld lineage",
		memory.TypeObservation, memory.StatusCommitted, now)
	publishAppV23RESTRecord(t, canonical, deep, 0)
	child := deep.MemoryID
	for index := 0; index < taint.MaxServingAncestorWalk; index++ {
		id := fmt.Sprintf("serving-ancestor-%03d", index)
		hash := sha256.Sum256([]byte(id))
		require.NoError(t, canonical.SetMemoryHash(id, hash[:], string(memory.StatusCommitted)))
		require.NoError(t, canonical.SetMemoryLineage(child, []tx.ParentRef{{
			SourceChainID: "sage", MemoryID: id, ContentHash: hash,
		}}))
		child = id
	}
	idHash := sha256.Sum256([]byte("unrelated serving header"))
	decisionID := fmt.Sprintf("%x", idHash)
	targetHash := sha256.Sum256([]byte("unrelated serving target"))
	target := tx.ParentRef{SourceChainID: "sage", MemoryID: "unrelated-serving-target", ContentHash: targetHash}
	require.NoError(t, canonical.SetMemoryHash(target.MemoryID, targetHash[:], string(memory.StatusCommitted)))
	created, err := canonical.ApplyTaintDecision("sage", decisionID, target, sha256.Sum256([]byte("serving payload")))
	require.NoError(t, err)
	require.True(t, created)
	header, found, err := canonical.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	if complete {
		// Materialize and finish the real unrelated frontier. A raw status flip
		// alone would not establish the complete-membership invariant.
		_, expandErr := canonical.ExpandTaintDecision("sage", decisionID, 128)
		require.NoError(t, expandErr)
		header, found, err = canonical.GetTaintDecision(decisionID)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, taint.StatusComplete, header.Status)
	} else {
		require.True(t, header.Status.Incomplete())
	}
	// Keep the real retained-slot count/index consistent. Corrupting only the
	// count would test malformed state, not the intended ancestor-walk budget.
	active, activeErr := canonical.ActiveTaintDecisionCount()
	require.NoError(t, activeErr)
	require.Equal(t, uint64(1), active)
	require.NoError(t, canonical.ValidateTaintState())
	if !complete {
		_, budgetErr := canonical.ClassifyMemoryTaint(deep.MemoryID)
		require.ErrorIs(t, budgetErr, store.ErrTaintServingWalkBudget,
			"the valid incomplete fixture must reach canonical ancestor-walk uncertainty")
	}
	rows.listed = append(rows.listed, deep)
	for index := 0; index < 2; index++ {
		normal := appV23PagingRecord(fmt.Sprintf("normal-serving-%d", index), owner, "owner.home",
			"serving readable normal content", memory.TypeObservation, memory.StatusCommitted, now)
		publishAppV23RESTRecord(t, canonical, normal, 0)
		rows.listed = append(rows.listed, normal)
	}
	rows.ranked = rows.listed
	for _, record := range rows.listed {
		rows.memories[record.MemoryID] = record
	}
	return srv, canonical, rows, reader, deep
}

func TestAppV23TaintCoverageListAuthorizedBudgetIsPartialDeniedIsNot(t *testing.T) {
	for _, denied := range []bool{false, true} {
		t.Run(fmt.Sprintf("denied_%t", denied), func(t *testing.T) {
			srv, _, _, reader, deep := appV23TaintCoverageFixture(t, false, denied)
			strict, err := srv.evaluateAppV23RecordDisclosure(reader, deep, time.Now())
			if denied {
				require.NoError(t, err, "denial must occur before the deep taint walk")
				require.False(t, strict.Allowed)
			} else {
				require.ErrorIs(t, err, store.ErrMemoryProjectionTaintIndeterminate)
				require.ErrorIs(t, err, store.ErrTaintServingWalkBudget)
				require.False(t, isUnsafeAppV23Projection(err))
			}
			for _, offset := range []int{0, 1, 2} {
				out := httptest.NewRecorder()
				srv.handleListMemoriesAuth(out, appV23DisclosureRequest(reader, http.MethodGet,
					fmt.Sprintf("/v1/memory/list?limit=1&offset=%d", offset), nil))
				require.Equal(t, http.StatusOK, out.Code, out.Body.String())
				var response struct {
					Memories []*memory.MemoryRecord `json:"memories"`
					Total    int                    `json:"total"`
					Exact    bool                   `json:"total_exact"`
					More     bool                   `json:"has_more"`
					Coverage *taintCoverage         `json:"taint_coverage"`
				}
				require.NoError(t, json.Unmarshal(out.Body.Bytes(), &response))
				require.Equal(t, 2, response.Total)
				require.Equal(t, offset == 0, response.More)
				require.Equal(t, denied && offset != 0, response.Exact)
				if denied {
					require.Nil(t, response.Coverage, "denied rows must disclose no uncertainty")
				} else {
					require.Equal(t, &taintCoverage{Complete: false, Truncated: true, Reason: "ancestor_walk_budget"}, response.Coverage)
				}
				if offset < 2 {
					require.Len(t, response.Memories, 1)
					require.Equal(t, fmt.Sprintf("normal-serving-%d", offset), response.Memories[0].MemoryID)
				} else {
					require.Empty(t, response.Memories)
				}
				require.NotContains(t, out.Body.String(), deep.MemoryID)
				require.NotContains(t, out.Body.String(), deep.Content)
				require.NotContains(t, out.Body.String(), deep.DomainTag)
			}
		})
	}
}

func TestAppV23TaintCoverageCompleteHeaderDoesNotWithholdDeepClearRow(t *testing.T) {
	srv, _, _, reader, deep := appV23TaintCoverageFixture(t, true, false)
	out := httptest.NewRecorder()
	srv.handleListMemoriesAuth(out, appV23DisclosureRequest(reader, http.MethodGet, "/v1/memory/list?limit=50", nil))
	require.Equal(t, http.StatusOK, out.Code, out.Body.String())
	require.Contains(t, out.Body.String(), deep.MemoryID)
	require.NotContains(t, out.Body.String(), "taint_coverage")
	require.Contains(t, out.Body.String(), `"total_exact":true`)
}

func TestAppV23TaintCoverageRecallFilterAndEmptyAbsence(t *testing.T) {
	for _, route := range []string{"query", "search", "hybrid"} {
		for _, empty := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/empty_%t", route, empty), func(t *testing.T) {
				srv, _, rows, reader, deep := appV23TaintCoverageFixture(t, false, false)
				if empty {
					rows.ranked = []*memory.MemoryRecord{deep}
				}
				body := []byte(`{"embedding":[0.1,0.2,0.3],"query":"serving","top_k":5}`)
				req := appV23DisclosureRequest(reader, http.MethodPost, "/v1/memory/"+route, body)
				out := httptest.NewRecorder()
				switch route {
				case "query":
					srv.handleQueryMemory(out, req)
				case "search":
					srv.handleSearchMemory(out, req)
				case "hybrid":
					srv.handleHybridSearchMemory(out, req)
				}
				require.Equal(t, http.StatusOK, out.Code, out.Body.String())
				var response QueryMemoryResponse
				require.NoError(t, json.Unmarshal(out.Body.Bytes(), &response))
				require.NotNil(t, response.TaintCoverage)
				require.NotContains(t, out.Body.String(), deep.MemoryID)
				require.NotContains(t, out.Body.String(), deep.Content)
				if empty {
					require.Empty(t, response.Results)
					require.Equal(t, "unavailable", response.IndexStatus)
				} else {
					require.Len(t, response.Results, 2)
				}
			})
		}
	}
}

func TestAppV23TaintCoverageStickyAcrossCandidateAndFinalRecheck(t *testing.T) {
	srv, _, rows, reader, deep := appV23TaintCoverageFixture(t, false, false)
	scope := newAppV23ReadScope()
	filter := srv.appV23RecallCandidateFilterScoped(reader, time.Now(), scope)
	allowed, err := filter(deep)
	require.NoError(t, err)
	require.False(t, allowed)
	normal := rows.listed[1]
	final, err := srv.evaluateAppV23RecordDisclosureScoped(reader, normal, time.Now(), scope)
	require.NoError(t, err)
	require.True(t, final.Allowed)
	require.NotNil(t, scope.coverage(), "a later successful recheck must not erase uncertainty")
	// Conversely, a row first encountered at final recheck sets the same signal.
	finalScope := newAppV23ReadScope()
	allowed, err = srv.appV23RecallCandidateFilterScoped(reader, time.Now(), finalScope)(normal)
	require.NoError(t, err)
	require.True(t, allowed)
	final, err = srv.evaluateAppV23RecordDisclosureScoped(reader, deep, time.Now(), finalScope)
	require.NoError(t, err)
	require.False(t, final.Allowed)
	require.NotNil(t, finalScope.coverage())
	// Exact-only collectors must keep the error contract, without partial data.
	_, err = srv.collectAppV23VisibleRecords(context.Background(), reader, 1,
		func(context.Context, int, int) ([]*memory.MemoryRecord, error) {
			return []*memory.MemoryRecord{deep}, nil
		})
	require.True(t, errors.Is(err, store.ErrTaintServingWalkBudget))
}

func TestAppV23TaintCoverageConcurrentScopeHasNoRecordMetadata(t *testing.T) {
	scope := newAppV23ReadScope()
	var workers sync.WaitGroup
	for index := 0; index < 16; index++ {
		workers.Add(1)
		go func() { defer workers.Done(); scope.uncertain.Store(true); _ = scope.coverage() }()
	}
	workers.Wait()
	response := QueryMemoryResponse{Results: []*MemoryResult{}, IndexStatus: "complete"}
	scope.applyRecallCoverage(&response)
	require.Equal(t, "unavailable", response.IndexStatus)
	wire, err := json.Marshal(response.TaintCoverage)
	require.NoError(t, err)
	require.JSONEq(t, `{"complete":false,"truncated":true,"reason":"ancestor_walk_budget"}`, string(wire))
}

func TestAppV23TaintCoverageStrictBatchRoutesKeep503(t *testing.T) {
	for _, route := range []string{"pending", "tasks", "timeline"} {
		t.Run(route, func(t *testing.T) {
			srv, _, rows, reader, deep := appV23TaintCoverageFixture(t, false, false)
			rows.pending = []*memory.MemoryRecord{deep}
			rows.tasks = []*memory.MemoryRecord{deep}
			out := httptest.NewRecorder()
			switch route {
			case "pending":
				srv.handleGetPending(out, appV23DisclosureRequest(reader, http.MethodGet, "/v1/validator/pending", nil))
			case "tasks":
				srv.handleGetOpenTasks(out, appV23DisclosureRequest(reader, http.MethodGet, "/v1/memory/tasks", nil))
			case "timeline":
				_, err := srv.getAppV23VisibleTimeline(context.Background(), reader,
					time.Now().Add(-time.Hour), time.Now().Add(time.Hour), "", "day")
				require.ErrorIs(t, err, store.ErrTaintServingWalkBudget)
				return
			}
			require.Equal(t, http.StatusServiceUnavailable, out.Code, out.Body.String())
			require.NotContains(t, out.Body.String(), "taint_coverage")
			require.NotContains(t, out.Body.String(), deep.Content)
		})
	}
}

// Model a candidate source returning a row that was not visited during its
// filter pass. The handler must still gate that row during final serialization.
type appV23FinalRecheckStore struct {
	*appV23PagingStore
	deep *memory.MemoryRecord
}

func (rows *appV23FinalRecheckStore) QuerySimilar(
	_ context.Context, _ []float32, opts store.QueryOptions,
) ([]*memory.MemoryRecord, error) {
	normal := rows.listed[1]
	allowed, err := opts.CandidateFilter(normal)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, fmt.Errorf("normal control unexpectedly denied")
	}
	return []*memory.MemoryRecord{rows.deep, normal}, nil
}

func TestAppV23TaintCoverageFinalSerializationCanSetUncertainty(t *testing.T) {
	srv, _, rows, reader, deep := appV23TaintCoverageFixture(t, false, false)
	srv.store = &appV23FinalRecheckStore{appV23PagingStore: rows, deep: deep}
	out := httptest.NewRecorder()
	srv.handleQueryMemory(out, appV23DisclosureRequest(reader, http.MethodPost, "/v1/memory/query",
		[]byte(`{"embedding":[0.1,0.2,0.3],"top_k":5}`)))
	require.Equal(t, http.StatusOK, out.Code, out.Body.String())
	var response QueryMemoryResponse
	require.NoError(t, json.Unmarshal(out.Body.Bytes(), &response))
	require.Len(t, response.Results, 1)
	require.Equal(t, rows.listed[1].MemoryID, response.Results[0].MemoryID)
	require.NotNil(t, response.TaintCoverage)
	require.NotContains(t, out.Body.String(), deep.MemoryID)
}

func TestAppV23TaintCoverageDeniedRecallHasNoUncertainty(t *testing.T) {
	for _, route := range []string{"query", "search", "hybrid"} {
		t.Run(route, func(t *testing.T) {
			srv, _, _, reader, deep := appV23TaintCoverageFixture(t, false, true)
			out := httptest.NewRecorder()
			req := appV23DisclosureRequest(reader, http.MethodPost, "/v1/memory/"+route,
				[]byte(`{"embedding":[0.1,0.2,0.3],"query":"serving","top_k":5}`))
			switch route {
			case "query":
				srv.handleQueryMemory(out, req)
			case "search":
				srv.handleSearchMemory(out, req)
			case "hybrid":
				srv.handleHybridSearchMemory(out, req)
			}
			require.Equal(t, http.StatusOK, out.Code, out.Body.String())
			var response QueryMemoryResponse
			require.NoError(t, json.Unmarshal(out.Body.Bytes(), &response))
			require.Len(t, response.Results, 2)
			require.Nil(t, response.TaintCoverage)
			require.NotContains(t, out.Body.String(), deep.MemoryID)
			require.NotContains(t, out.Body.String(), deep.DomainTag)
		})
	}
}

func TestAppV23TaintCoverageSurvivesRawPageBoundaryAndLookahead(t *testing.T) {
	srv, canonical, rows, reader, deep := appV23TaintCoverageFixture(t, false, false)
	normals := rows.listed[1:]
	rows.listed = []*memory.MemoryRecord{deep}
	for index := 0; index < 199; index++ {
		denied := appV23PagingRecord(fmt.Sprintf("denied-page-%03d", index), appV23RESTAgentID("44"),
			"outsider.home", "non-disclosable page filler", memory.TypeObservation, memory.StatusCommitted, time.Now())
		publishAppV23RESTRecord(t, canonical, denied, 0)
		rows.listed = append(rows.listed, denied)
	}
	rows.listed = append(rows.listed, normals...)
	out := httptest.NewRecorder()
	srv.handleListMemoriesAuth(out, appV23DisclosureRequest(reader, http.MethodGet, "/v1/memory/list?limit=1", nil))
	require.Equal(t, http.StatusOK, out.Code, out.Body.String())
	require.Equal(t, 2, rows.listCalls)
	var response struct {
		Memories []*memory.MemoryRecord `json:"memories"`
		More     bool                   `json:"has_more"`
		Exact    bool                   `json:"total_exact"`
		Coverage *taintCoverage         `json:"taint_coverage"`
	}
	require.NoError(t, json.Unmarshal(out.Body.Bytes(), &response))
	require.Len(t, response.Memories, 1)
	require.Equal(t, normals[0].MemoryID, response.Memories[0].MemoryID)
	require.True(t, response.More)
	require.False(t, response.Exact)
	require.NotNil(t, response.Coverage)
	require.NotContains(t, out.Body.String(), "denied-page")
	require.NotContains(t, out.Body.String(), "outsider.home")
}

// Keep the former zero-counter adversarial control separate from the valid
// ancestor-budget fixture: an indexed retained decision with a zero count is
// malformed canonical state, never a partial-coverage or successful-empty case.
func TestAppV23TaintCoverageZeroRetainedCountFailsClosed(t *testing.T) {
	srv, canonical, rows, reader, deep := appV23TaintCoverageFixture(t, false, false)
	// The corrected fixture has already proved a valid incomplete decision and
	// an actual walk-budget error. Change only the retained-slot singleton.
	require.NoError(t, canonical.SetRawForTest(taint.ActiveCountKey(), make([]byte, 8)))
	active, activeErr := canonical.ActiveTaintDecisionCount()
	require.NoError(t, activeErr)
	require.Zero(t, active)

	_, classifyErr := canonical.ClassifyMemoryTaint(deep.MemoryID)
	require.ErrorIs(t, classifyErr, store.ErrTaintStateMalformed)
	require.NotErrorIs(t, classifyErr, store.ErrTaintServingWalkBudget)
	require.NoError(t, canonical.ValidateLineageIndexes(), "lineage remains valid; only the retained count was corrupted")
	disclosure, disclosureErr := srv.evaluateAppV23RecordDisclosure(reader, deep, time.Now())
	require.ErrorIs(t, disclosureErr, store.ErrTaintStateMalformed)
	require.NotErrorIs(t, disclosureErr, store.ErrMemoryProjectionTaintIndeterminate)
	require.NotErrorIs(t, disclosureErr, store.ErrTaintServingWalkBudget)
	require.False(t, disclosure.Allowed)
	require.False(t, isUnsafeAppV23Projection(disclosureErr), "malformed state must abort instead of becoming a skippable record denial")

	out := httptest.NewRecorder()
	srv.handleListMemoriesAuth(out, appV23DisclosureRequest(reader, http.MethodGet, "/v1/memory/list?limit=50", nil))
	require.Equal(t, http.StatusServiceUnavailable, out.Code, out.Body.String())
	require.NotContains(t, out.Body.String(), "taint_coverage", "malformed state is not partial ancestor-walk coverage")
	for _, record := range rows.listed {
		require.NotContains(t, out.Body.String(), record.MemoryID)
		require.NotContains(t, out.Body.String(), record.Content)
	}
	require.NotContains(t, out.Body.String(), deep.DomainTag)
}
