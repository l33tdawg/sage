package rest

import (
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/scope"
	"github.com/l33tdawg/sage/internal/store"
)

func scopeReadRouter(server *Server, callerID string) http.Handler {
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req.WithContext(middleware.WithAgentID(req.Context(), callerID)))
		})
	})
	r.Get("/v1/scopes", server.handleListScopes)
	r.Get("/v1/scopes/{scope_id}", server.handleGetScope)
	return r
}

func TestScopeReadSurfaceIsCanonicalAndOperatorOnly(t *testing.T) {
	bs, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })
	record := scope.Record{
		ScopeID: "scope-a", Revision: 1, State: scope.StateActive,
		ControllerValidatorID: "validator-a", CreatedHeight: 10, UpdatedHeight: 10,
		Domains: []scope.Domain{{Name: "research"}},
		Members: []scope.Member{{ValidatorID: "validator-a", AssignedWeight: 7, JoinedRevision: 1, Active: true}},
	}
	require.NoError(t, bs.SetScopeRecord(record))
	contentHash := sha256.Sum256([]byte("pending scoped memory"))
	require.NoError(t, bs.SetScopedMemorySubmission(scope.Ballot{
		MemoryID: "memory-a", ScopeID: "scope-a", ScopeRevision: 1, SubmittedHeight: 11,
		State:   scope.BallotPending,
		Members: []scope.BallotMember{{ValidatorID: "validator-a", EffectiveWeight: 7}}, TotalWeight: 7,
	}, scope.Content{
		MemoryID: "memory-a", ScopeID: "scope-a", ScopeRevision: 1, SubmittingAgentID: "agent-a",
		ContentHash: contentHash[:], MemoryType: 1, Domain: "research", ConfidenceScore: 0.9,
		Content: "pending scoped memory", SubmittedHeight: 11, SubmittedUnix: 100,
	}))
	server := &Server{badgerStore: bs, nodeOperatorID: "operator", logger: zerolog.Nop()}

	rr := httptest.NewRecorder()
	scopeReadRouter(server, "operator").ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/scopes", nil))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var listed struct {
		Scopes []scopeRecordResponse `json:"scopes"`
		Count  int                   `json:"count"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &listed))
	require.Equal(t, 1, listed.Count)
	require.Len(t, listed.Scopes, 1)
	assert.Equal(t, "scope-a", listed.Scopes[0].ScopeID)
	assert.Equal(t, "active", listed.Scopes[0].State)
	assert.Len(t, listed.Scopes[0].RevisionHash, 64)
	assert.Equal(t, uint64(7), listed.Scopes[0].Members[0].AssignedWeight)
	assert.True(t, listed.Scopes[0].Members[0].ValidatorRemovalBlocked)
	assert.Equal(t, 1, listed.Scopes[0].Members[0].PendingBallotCount)
	assert.Equal(t, 1, listed.Scopes[0].Drain.PendingBallotCount)
	assert.Equal(t, []string{"memory-a"}, listed.Scopes[0].Drain.PendingMemoryIDs)
	assert.Equal(t, []string{"validator-a"}, listed.Scopes[0].Drain.BlockingValidatorIDs)

	rr = httptest.NewRecorder()
	scopeReadRouter(server, "operator").ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/scopes/scope-a", nil))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var got scopeRecordResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &got))
	assert.Equal(t, listed.Scopes[0], got)

	rr = httptest.NewRecorder()
	scopeReadRouter(server, "ordinary-agent").ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/scopes", nil))
	assert.Equal(t, http.StatusForbidden, rr.Code)

	rr = httptest.NewRecorder()
	scopeReadRouter(server, "operator").ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/scopes/missing", nil))
	assert.Equal(t, http.StatusNotFound, rr.Code)
}
