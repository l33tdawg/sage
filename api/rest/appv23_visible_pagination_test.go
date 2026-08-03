package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
)

// appV23PagingStore models stable backend ordering explicitly. It lets the REST
// tests pin the contract that live authorization is applied before public page
// limits, independently of Go map iteration in the broad handler mock.
type appV23PagingStore struct {
	*rbacMockMemoryStore
	ranked    []*memory.MemoryRecord
	listed    []*memory.MemoryRecord
	pending   []*memory.MemoryRecord
	tasks     []*memory.MemoryRecord
	searchErr error
	listCalls int
}

func appV23RawPage(
	records []*memory.MemoryRecord,
	limit, offset int,
) []*memory.MemoryRecord {
	if offset >= len(records) {
		return []*memory.MemoryRecord{}
	}
	end := offset + limit
	if end > len(records) {
		end = len(records)
	}
	return records[offset:end]
}

func (m *appV23PagingStore) rankedPage(
	opts store.QueryOptions,
	textQuery string,
) ([]*memory.MemoryRecord, error) {
	results := make([]*memory.MemoryRecord, 0, opts.TopK)
	for _, rec := range m.ranked {
		if opts.DomainTag != "" && rec.DomainTag != opts.DomainTag {
			continue
		}
		if textQuery != "" &&
			!strings.Contains(strings.ToLower(rec.Content), strings.ToLower(textQuery)) {
			continue
		}
		if opts.CandidateFilter != nil {
			allowed, err := opts.CandidateFilter(rec)
			if err != nil {
				return nil, err
			}
			if !allowed {
				continue
			}
		}
		results = append(results, rec)
		if opts.TopK > 0 && len(results) == opts.TopK {
			break
		}
	}
	return results, nil
}

func (m *appV23PagingStore) QuerySimilar(
	_ context.Context,
	_ []float32,
	opts store.QueryOptions,
) ([]*memory.MemoryRecord, error) {
	return m.rankedPage(opts, "")
}

func (m *appV23PagingStore) SearchByText(
	_ context.Context,
	query string,
	opts store.QueryOptions,
) ([]*memory.MemoryRecord, error) {
	if m.searchErr != nil {
		return nil, m.searchErr
	}
	return m.rankedPage(opts, query)
}

func (m *appV23PagingStore) SearchHybrid(
	_ context.Context,
	query string,
	_ []float32,
	opts store.QueryOptions,
) ([]*memory.MemoryRecord, error) {
	return m.rankedPage(opts, query)
}

func (m *appV23PagingStore) ListMemories(
	_ context.Context,
	opts store.ListOptions,
) ([]*memory.MemoryRecord, int, error) {
	m.listCalls++
	return appV23RawPage(m.listed, opts.Limit, opts.Offset), len(m.listed), nil
}

func (m *appV23PagingStore) GetPendingByDomainPage(
	_ context.Context,
	_ string,
	limit, offset int,
) ([]*memory.MemoryRecord, error) {
	return appV23RawPage(m.pending, limit, offset), nil
}

func (m *appV23PagingStore) GetOpenTasksPage(
	_ context.Context,
	_, _, _ string,
	limit, offset int,
) ([]*memory.MemoryRecord, error) {
	return appV23RawPage(m.tasks, limit, offset), nil
}

func appV23PagingRecord(
	id, submitter, domain, content string,
	memoryType memory.MemoryType,
	status memory.MemoryStatus,
	createdAt time.Time,
) *memory.MemoryRecord {
	rec := &memory.MemoryRecord{
		MemoryID:        id,
		SubmittingAgent: submitter,
		Content:         content,
		ContentHash:     memory.ComputeContentHash(content),
		MemoryType:      memoryType,
		DomainTag:       domain,
		ConfidenceScore: 0.9,
		Status:          status,
		CreatedAt:       createdAt,
	}
	if memoryType == memory.TypeTask {
		rec.TaskStatus = memory.TaskStatusPlanned
	}
	return rec
}

func TestAppV23MemoryListFirstPageOverFiveThousandDoesNotFail(t *testing.T) {
	srv, badger, readerID, ownerID, _ := setupAppV23RESTAccess(t)
	memStore := &appV23PagingStore{rbacMockMemoryStore: newRBACMockMemoryStore()}
	memStore.badger = badger
	srv.store = memStore

	now := time.Now()
	memStore.listed = make([]*memory.MemoryRecord, 5001)
	for i := range memStore.listed {
		memStore.listed[i] = appV23PagingRecord(
			fmt.Sprintf("visible-%05d", i),
			ownerID,
			"owner.home",
			"visible mature-node memory",
			memory.TypeObservation,
			memory.StatusCommitted,
			now.Add(-time.Duration(i)*time.Second),
		)
		// These records intentionally model a mature legacy inventory: the
		// canonical hash/status exists, while domain/author/classification
		// continue to use their compatible serving-row fallbacks.
		require.NoError(t, badger.SetMemoryHash(
			memStore.listed[i].MemoryID,
			memStore.listed[i].ContentHash,
			string(memStore.listed[i].Status),
		))
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/memory/list?limit=50", nil)
	req = req.WithContext(middleware.WithAgentID(req.Context(), readerID))
	out := httptest.NewRecorder()
	srv.handleListMemoriesAuth(out, req)
	require.Equal(t, http.StatusOK, out.Code, out.Body.String())

	var response struct {
		Memories   []*memory.MemoryRecord `json:"memories"`
		Total      int                    `json:"total"`
		HasMore    bool                   `json:"has_more"`
		TotalExact bool                   `json:"total_exact"`
	}
	require.NoError(t, json.Unmarshal(out.Body.Bytes(), &response))
	require.Len(t, response.Memories, 50)
	require.Equal(t, 51, response.Total,
		"while has_more is true total is the privacy-safe visible lower bound")
	require.True(t, response.HasMore)
	require.False(t, response.TotalExact)
}

func TestAppV23MemoryListSmallFirstPageDoesNotClaimLowerBoundIsExact(t *testing.T) {
	srv, badger, readerID, ownerID, _ := setupAppV23RESTAccess(t)
	memStore := &appV23PagingStore{rbacMockMemoryStore: newRBACMockMemoryStore()}
	memStore.badger = badger
	srv.store = memStore

	now := time.Now()
	for i := range 4 {
		rec := appV23PagingRecord(
			fmt.Sprintf("small-visible-%d", i), ownerID, "owner.home",
			"visible memory", memory.TypeObservation, memory.StatusCommitted,
			now.Add(-time.Duration(i)*time.Second),
		)
		memStore.listed = append(memStore.listed, rec)
		require.NoError(t, badger.SetMemoryHash(
			rec.MemoryID, rec.ContentHash, string(rec.Status),
		))
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/memory/list?limit=1", nil)
	req = req.WithContext(middleware.WithAgentID(req.Context(), readerID))
	out := httptest.NewRecorder()
	srv.handleListMemoriesAuth(out, req)
	require.Equal(t, http.StatusOK, out.Code, out.Body.String())

	var response struct {
		Memories   []*memory.MemoryRecord `json:"memories"`
		Total      int                    `json:"total"`
		HasMore    bool                   `json:"has_more"`
		TotalExact bool                   `json:"total_exact"`
	}
	require.NoError(t, json.Unmarshal(out.Body.Bytes(), &response))
	require.Len(t, response.Memories, 1)
	require.Equal(t, 2, response.Total)
	require.True(t, response.HasMore)
	require.False(t, response.TotalExact,
		"a look-ahead lower bound must not be advertised as an exact total")
}

func TestAppV23MemoryListQuarantinesMalformedHistoricalDomainWithoutHidingValidRows(t *testing.T) {
	srv, badger, readerID, ownerID, _ := setupAppV23RESTAccess(t)
	memStore := &appV23PagingStore{rbacMockMemoryStore: newRBACMockMemoryStore()}
	memStore.badger = badger
	srv.store = memStore

	now := time.Now()
	invalid := appV23PagingRecord(
		"legacy-invalid-domain", ownerID, "legacy domain with whitespace",
		"preserved historical row", memory.TypeObservation,
		memory.StatusCommitted, now,
	)
	valid := appV23PagingRecord(
		"valid-domain-row", ownerID, "owner.home", "readable current row",
		memory.TypeObservation, memory.StatusCommitted, now.Add(-time.Second),
	)
	memStore.listed = []*memory.MemoryRecord{invalid, valid}
	for _, rec := range memStore.listed {
		memStore.memories[rec.MemoryID] = rec
		require.NoError(t, badger.SetMemoryHash(
			rec.MemoryID, rec.ContentHash, string(rec.Status),
		))
	}

	// The exact row remains non-disclosable: the broad-read compatibility is a
	// skip, not permission to interpret malformed historical metadata.
	_, exactErr := srv.evaluateAppV23RecordDisclosure(readerID, invalid, now)
	require.Error(t, exactErr)
	require.ErrorIs(t, exactErr, store.ErrMemoryProjectionQuarantined)
	require.True(t, isUnsafeAppV23Projection(exactErr))

	req := httptest.NewRequest(http.MethodGet, "/v1/memory/list?limit=50", nil)
	req = req.WithContext(middleware.WithAgentID(req.Context(), readerID))
	out := httptest.NewRecorder()
	srv.handleListMemoriesAuth(out, req)
	require.Equal(t, http.StatusOK, out.Code, out.Body.String())
	var response struct {
		Memories   []*memory.MemoryRecord `json:"memories"`
		Total      int                    `json:"total"`
		HasMore    bool                   `json:"has_more"`
		TotalExact bool                   `json:"total_exact"`
	}
	require.NoError(t, json.Unmarshal(out.Body.Bytes(), &response))
	require.Len(t, response.Memories, 1)
	require.Equal(t, valid.MemoryID, response.Memories[0].MemoryID)
	require.Equal(t, 1, response.Total)
	require.False(t, response.HasMore)
	require.True(t, response.TotalExact)

	// Missing policy infrastructure is a genuine request-wide authorization
	// failure and must never be reclassified as a skippable legacy row.
	srv.badgerStore = nil
	_, backendErr := srv.evaluateAppV23RecordDisclosure(readerID, valid, now)
	require.Error(t, backendErr)
	require.False(t, isUnsafeAppV23Projection(backendErr))
}

func TestAppV23VisiblePaginationFillsPastRevokedPrefixAcrossRoutes(t *testing.T) {
	srv, badger, readerID, ownerID, outsiderID := setupAppV23RESTAccess(t)
	memStore := &appV23PagingStore{rbacMockMemoryStore: newRBACMockMemoryStore()}
	memStore.badger = badger
	srv.store = memStore

	now := time.Now()
	const deniedPrefix = 520
	for i := 0; i < deniedPrefix; i++ {
		id := fmt.Sprintf("denied-%04d", i)
		denied := appV23PagingRecord(
			id, outsiderID, "outsider.home", "needle denied content",
			memory.TypeObservation, memory.StatusCommitted,
			now.Add(-time.Duration(i)*time.Second),
		)
		memStore.ranked = append(memStore.ranked, denied)
		memStore.listed = append(memStore.listed, denied)
		require.NoError(t, badger.SetMemoryHash(
			denied.MemoryID, denied.ContentHash, string(denied.Status),
		))

		pending := *denied
		pending.MemoryID += "-pending"
		pending.Status = memory.StatusProposed
		memStore.pending = append(memStore.pending, &pending)
		require.NoError(t, badger.SetMemoryHash(
			pending.MemoryID, pending.ContentHash, string(pending.Status),
		))

		task := *denied
		task.MemoryType = memory.TypeTask
		task.TaskStatus = memory.TaskStatusPlanned
		task.Assignee = readerID
		memStore.tasks = append(memStore.tasks, &task)
	}
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("allowed-%02d", i)
		allowed := appV23PagingRecord(
			id, ownerID, "owner.home", "needle allowed content",
			memory.TypeObservation, memory.StatusCommitted,
			now.Add(-time.Duration(deniedPrefix+i)*time.Second),
		)
		memStore.ranked = append(memStore.ranked, allowed)
		memStore.listed = append(memStore.listed, allowed)
		require.NoError(t, badger.SetMemoryHash(
			allowed.MemoryID, allowed.ContentHash, string(allowed.Status),
		))

		pending := *allowed
		pending.MemoryID += "-pending"
		pending.Status = memory.StatusProposed
		memStore.pending = append(memStore.pending, &pending)
		require.NoError(t, badger.SetMemoryHash(
			pending.MemoryID, pending.ContentHash, string(pending.Status),
		))

		task := *allowed
		task.MemoryType = memory.TypeTask
		task.TaskStatus = memory.TaskStatusPlanned
		task.Assignee = readerID
		memStore.tasks = append(memStore.tasks, &task)
	}

	recallRoutes := []struct {
		name    string
		path    string
		body    string
		handler http.HandlerFunc
	}{
		{
			name: "query", path: "/v1/memory/query",
			body:    `{"embedding":[0.1,0.2,0.3],"top_k":2}`,
			handler: srv.handleQueryMemory,
		},
		{
			name: "search", path: "/v1/memory/search",
			body:    `{"query":"needle","top_k":2}`,
			handler: srv.handleSearchMemory,
		},
		{
			name: "hybrid", path: "/v1/memory/hybrid",
			body:    `{"query":"needle","embedding":[0.1,0.2,0.3],"top_k":2}`,
			handler: srv.handleHybridSearchMemory,
		},
	}
	for _, route := range recallRoutes {
		t.Run(route.name, func(t *testing.T) {
			req := httptest.NewRequest(
				http.MethodPost, route.path, bytes.NewBufferString(route.body),
			)
			req = req.WithContext(middleware.WithAgentID(req.Context(), readerID))
			out := httptest.NewRecorder()
			route.handler(out, req)
			require.Equal(t, http.StatusOK, out.Code, out.Body.String())
			var response QueryMemoryResponse
			require.NoError(t, json.Unmarshal(out.Body.Bytes(), &response))
			require.Len(t, response.Results, 2)
			for _, result := range response.Results {
				require.Contains(t, result.MemoryID, "allowed-")
				require.NotContains(t, result.Content, "denied")
			}
		})
	}

	t.Run("list", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/memory/list?limit=2", nil)
		req = req.WithContext(middleware.WithAgentID(req.Context(), readerID))
		out := httptest.NewRecorder()
		srv.handleListMemoriesAuth(out, req)
		require.Equal(t, http.StatusOK, out.Code, out.Body.String())
		var response struct {
			Memories []*memory.MemoryRecord `json:"memories"`
			HasMore  bool                   `json:"has_more"`
		}
		require.NoError(t, json.Unmarshal(out.Body.Bytes(), &response))
		require.Len(t, response.Memories, 2)
		require.True(t, response.HasMore)
		for _, result := range response.Memories {
			require.Contains(t, result.MemoryID, "allowed-")
		}

		req = httptest.NewRequest(
			http.MethodGet, "/v1/memory/list?limit=2&offset=2", nil,
		)
		req = req.WithContext(middleware.WithAgentID(req.Context(), readerID))
		out = httptest.NewRecorder()
		srv.handleListMemoriesAuth(out, req)
		require.Equal(t, http.StatusOK, out.Code, out.Body.String())
		var finalPage struct {
			Memories   []*memory.MemoryRecord `json:"memories"`
			Total      int                    `json:"total"`
			HasMore    bool                   `json:"has_more"`
			TotalExact bool                   `json:"total_exact"`
		}
		require.NoError(t, json.Unmarshal(out.Body.Bytes(), &finalPage))
		require.Len(t, finalPage.Memories, 1)
		require.Equal(t, "allowed-02", finalPage.Memories[0].MemoryID)
		require.Equal(t, 3, finalPage.Total)
		require.False(t, finalPage.HasMore)
		require.True(t, finalPage.TotalExact)
	})

	t.Run("validator pending", func(t *testing.T) {
		req := httptest.NewRequest(
			http.MethodGet, "/v1/validator/pending?limit=2", nil,
		)
		req = req.WithContext(middleware.WithAgentID(req.Context(), readerID))
		out := httptest.NewRecorder()
		srv.handleGetPending(out, req)
		require.Equal(t, http.StatusOK, out.Code, out.Body.String())
		var response PendingMemoriesResponse
		require.NoError(t, json.Unmarshal(out.Body.Bytes(), &response))
		require.Len(t, response.Memories, 2)
		for _, result := range response.Memories {
			require.Contains(t, result.MemoryID, "allowed-")
		}
	})

	t.Run("open tasks", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/memory/tasks", nil)
		req = req.WithContext(middleware.WithAgentID(req.Context(), readerID))
		out := httptest.NewRecorder()
		srv.handleGetOpenTasks(out, req)
		require.Equal(t, http.StatusOK, out.Code, out.Body.String())
		var response struct {
			Tasks []struct {
				MemoryID string `json:"memory_id"`
			} `json:"tasks"`
		}
		require.NoError(t, json.Unmarshal(out.Body.Bytes(), &response))
		require.Len(t, response.Tasks, 3)
		for _, result := range response.Tasks {
			require.Contains(t, result.MemoryID, "allowed-")
		}
	})
}

func TestAppV23TextSearchStoreFailureIsNotMisreportedAsAuthorization(t *testing.T) {
	srv, _, readerID, _, _ := setupAppV23RESTAccess(t)
	memStore := &appV23PagingStore{
		rbacMockMemoryStore: newRBACMockMemoryStore(),
		searchErr:           errors.New("text search unavailable while encrypted"),
	}
	srv.store = memStore

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/memory/search",
		bytes.NewBufferString(`{"query":"needle","top_k":2}`),
	)
	req = req.WithContext(middleware.WithAgentID(req.Context(), readerID))
	out := httptest.NewRecorder()
	srv.handleSearchMemory(out, req)
	require.Equal(t, http.StatusInternalServerError, out.Code, out.Body.String())
	require.Contains(t, out.Body.String(), "text search unavailable while encrypted")
	require.NotContains(t, out.Body.String(), "Authorization unavailable")
}

func TestAppV23MemoryListRejectsHugeOffsetBeforeStoreAccess(t *testing.T) {
	srv, badger, readerID, _, _ := setupAppV23RESTAccess(t)
	memStore := &appV23PagingStore{rbacMockMemoryStore: newRBACMockMemoryStore()}
	memStore.badger = badger
	srv.store = memStore

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/memory/list?limit=50&offset=1000000000",
		nil,
	)
	req = req.WithContext(middleware.WithAgentID(req.Context(), readerID))
	out := httptest.NewRecorder()
	srv.handleListMemoriesAuth(out, req)

	require.Equal(t, http.StatusUnprocessableEntity, out.Code, out.Body.String())
	require.Zero(t, memStore.listCalls, "rejected offsets must not scan the store")
}

func TestAppV23MemoryListBoundsDeniedCandidateWalk(t *testing.T) {
	srv, badger, readerID, _, outsiderID := setupAppV23RESTAccess(t)
	memStore := &appV23PagingStore{rbacMockMemoryStore: newRBACMockMemoryStore()}
	memStore.badger = badger
	srv.store = memStore

	denied := appV23PagingRecord(
		"same-denied-record",
		outsiderID,
		"outsider.home",
		"not visible to this caller",
		memory.TypeObservation,
		memory.StatusCommitted,
		time.Now(),
	)
	require.NoError(t, badger.SetMemoryHash(
		denied.MemoryID, denied.ContentHash, string(denied.Status),
	))
	memStore.listed = make([]*memory.MemoryRecord, appV23DisclosureScanBudget+1)
	for i := range memStore.listed {
		memStore.listed[i] = denied
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/memory/list?limit=50", nil)
	req = req.WithContext(middleware.WithAgentID(req.Context(), readerID))
	out := httptest.NewRecorder()
	srv.handleListMemoriesAuth(out, req)

	require.Equal(t, http.StatusUnprocessableEntity, out.Code, out.Body.String())
	require.Contains(t, out.Body.String(), "Query too broad")
	require.LessOrEqual(t, memStore.listCalls,
		(appV23DisclosureScanBudget+199)/200,
		"one signed request must never authorize the whole corpus")
}

func TestAppV23VisiblePaginationDefensivelyRejectsOversizedOffset(t *testing.T) {
	srv, _, readerID, _, _ := setupAppV23RESTAccess(t)
	_, _, _, _, err := srv.listAppV23VisibleMemories(
		context.Background(),
		store.ListOptions{Limit: 50, Offset: appV23MaxVisibleOffset + 1},
		readerID,
	)
	require.ErrorIs(t, err, errAppV23VisibleOffsetTooLarge)
}
