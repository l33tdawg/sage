package rest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
)

func taskStatusRouterAs(s *Server, agentID string) http.Handler {
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req.WithContext(middleware.WithAgentID(req.Context(), agentID)))
		})
	})
	r.Put("/v1/memory/{memory_id}/task-status", s.handleUpdateTaskStatus)
	return r
}

func TestRESTTaskStatusEnforcesActiveCurrentOwnerAndNoAgentReopen(t *testing.T) {
	s, sqliteStore := newPipeServer(t)
	ctx := context.Background()
	hash := sha256.Sum256([]byte("owned REST task"))
	require.NoError(t, sqliteStore.InsertMemory(ctx, &memory.MemoryRecord{
		MemoryID: "rest-owned-task", SubmittingAgent: "author", Content: "[TASK] owned REST task",
		ContentHash: hash[:], MemoryType: memory.TypeTask, DomainTag: "work", Provider: "codex",
		ConfidenceScore: 0.9, Status: memory.StatusCommitted, TaskStatus: memory.TaskStatusPlanned,
		CreatedAt: time.Now().UTC(),
	}))
	require.NoError(t, sqliteStore.UpdateMemoryClassification(ctx, "rest-owned-task", store.ClearancePublic))
	for _, id := range []string{"agent-a", "agent-b"} {
		require.NoError(t, sqliteStore.CreateAgent(ctx, &store.AgentEntry{
			AgentID: id, Name: id, RegisteredName: id, Status: "active",
		}))
	}
	_, err := sqliteStore.AssignTaskAndNotify(ctx, "rest-owned-task", "agent-a")
	require.NoError(t, err)

	put := func(agentID, status string) *httptest.ResponseRecorder {
		t.Helper()
		body := bytes.NewBufferString(fmt.Sprintf(`{"task_status":%q}`, status))
		req := httptest.NewRequest(http.MethodPut, "/v1/memory/rest-owned-task/task-status", body)
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		taskStatusRouterAs(s, agentID).ServeHTTP(rr, req)
		return rr
	}

	for _, status := range []string{"done", "dropped"} {
		rr := put("agent-b", status)
		require.Equal(t, http.StatusConflict, rr.Code, rr.Body.String())
	}
	require.Equal(t, http.StatusForbidden, put("agent-b", "planned").Code)
	require.NoError(t, sqliteStore.UpdateAgentStatus(ctx, "agent-a", "inactive"))
	require.Equal(t, http.StatusForbidden, put("agent-a", "done").Code)
	require.NoError(t, sqliteStore.UpdateAgentStatus(ctx, "agent-a", "active"))
	require.Equal(t, http.StatusOK, put("agent-a", "done").Code)
	require.Equal(t, http.StatusForbidden, put("agent-a", "planned").Code)

	tasks, err := sqliteStore.GetAllTasks(ctx, "work", 10)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, memory.TaskStatusDone, tasks[0].TaskStatus)
	require.Equal(t, "agent-a", tasks[0].Assignee)
}
