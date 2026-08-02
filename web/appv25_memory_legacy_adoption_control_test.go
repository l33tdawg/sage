package web

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
)

func appV25RecoveryControlFixture(
	t *testing.T,
) (*DashboardHandler, *store.SQLiteStore, appV23AccessFixture, uint64, string) {
	t.Helper()
	ctx := context.Background()
	fixture := newAppV23AccessFixture(t)
	dbPath := filepath.Join(t.TempDir(), "memories.db")
	sqlite, err := store.NewSQLiteStore(ctx, dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlite.Close()) })
	content := "preserved but unverifiable"
	require.NoError(t, sqlite.InsertMemory(ctx, &memory.MemoryRecord{
		MemoryID:        "legacy-unconvertible",
		SubmittingAgent: fixture.agentID,
		Content:         content,
		ContentHash:     make([]byte, sha256.Size),
		MemoryType:      memory.TypeFact,
		DomainTag:       "historical/domain",
		Status:          memory.StatusCommitted,
	}))
	revision, err := sqlite.MemoryProjectionRevision(ctx)
	require.NoError(t, err)
	require.NotZero(t, revision)
	require.NoError(t, sqlite.SyncLegacyMemoryRecoveryQueue(ctx, revision,
		[]store.LegacyMemoryRecoveryItem{{
			MemoryID: "legacy-unconvertible", Reason: "content_hash_mismatch",
		}}))
	progress := store.LegacyMemoryAdoptionProgress{
		State: "recovery", Discovered: 1, Recovery: 1, Revision: revision,
		Message: "Preserved historical records require review.",
	}
	require.NoError(t, sqlite.PublishLegacyMemoryAdoptionProgress(ctx, progress))
	handler := NewDashboardHandler(sqlite, "test")
	handler.BadgerStore = fixture.badger
	handler.AdminSigningKey = fixture.rootKey
	handler.AppV23ActiveFn = func() bool { return true }
	handler.ConfigureAppV25Maintenance(true)
	handler.noteAppV25MaintenanceProgress(progress)
	handler.ResolveAgentKeyFn = func(id string) (ed25519.PrivateKey, bool) {
		if id == fixture.rootID {
			return fixture.rootKey, true
		}
		if id == fixture.agentID {
			return fixture.agentKey, true
		}
		return nil, false
	}
	return handler, sqlite, fixture, revision, dbPath
}

func appV25RecoveryControlRequest(
	t *testing.T,
	path string,
	revision uint64,
	count int,
	confirmation string,
	signingKey ...ed25519.PrivateKey,
) *http.Request {
	t.Helper()
	body, err := json.Marshal(appV25LegacyRecoveryControlRequest{
		ProjectionRevision: revision,
		ExpectedCount:      count,
		Confirmation:       confirmation,
	})
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Origin", "http://localhost:8080")
	request.Header.Set("Sec-Fetch-Site", "same-origin")
	request.Host = "localhost:8080"
	request.RemoteAddr = "127.0.0.1:54321"
	if len(signingKey) != 0 {
		signAgentRequest(t, request, signingKey[0], body)
	}
	return request
}

func TestAppV25LegacyRecoveryControlsRequireCurrentRoot(t *testing.T) {
	for _, test := range []struct {
		name         string
		path         string
		confirmation string
		wantRoot     int
		invoke       func(*DashboardHandler, http.ResponseWriter, *http.Request)
	}{
		{
			name: "retry", path: "/v1/dashboard/memory/adoption-retry",
			wantRoot: http.StatusAccepted,
			invoke:   (*DashboardHandler).handleAppV25LegacyAdoptionRetry,
		},
		{
			name: "deprecate", path: "/v1/dashboard/memory/adoption-deprecate",
			confirmation: "DEPRECATE 1", wantRoot: http.StatusOK,
			invoke: (*DashboardHandler).handleAppV25LegacyAdoptionDeprecate,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler, sqlite, fixture, revision, _ := appV25RecoveryControlFixture(t)
			require.NoError(t, fixture.badger.SetAppV23Policy(
				fixture.rootID, fixture.agentID,
				store.AppV23RoleAdmin, store.AppV23ProfileCompanion,
				store.AppV23ProfileStandard, 4, store.AgentCapabilityReadAllDomains,
				1, 1, 2,
			))

			adminRequest := appV25RecoveryControlRequest(t,
				test.path, revision, 1, test.confirmation, fixture.agentKey)
			adminRecorder := httptest.NewRecorder()
			test.invoke(handler, adminRecorder, adminRequest)
			require.Equal(t, http.StatusForbidden, adminRecorder.Code,
				adminRecorder.Body.String())
			require.Contains(t, adminRecorder.Body.String(), "current_root_required")
			require.Zero(t, handler.appV25AdoptionRetry.Load())
			dispositions, err := sqlite.ListLegacyMemoryRecoveryDispositions(
				context.Background(),
			)
			require.NoError(t, err)
			require.Empty(t, dispositions)

			rootRequest := appV25RecoveryControlRequest(t,
				test.path, revision, 1, test.confirmation, fixture.rootKey)
			rootRecorder := httptest.NewRecorder()
			test.invoke(handler, rootRecorder, rootRequest)
			require.Equal(t, test.wantRoot, rootRecorder.Code,
				rootRecorder.Body.String())
			require.Equal(t, uint64(1), handler.appV25AdoptionRetry.Load())
		})
	}
}

func TestAppV25LegacyAdoptionRetryRequiresExactCurrentRecoverySnapshot(t *testing.T) {
	handler, _, _, revision, _ := appV25RecoveryControlFixture(t)

	stale := appV25RecoveryControlRequest(t,
		"/v1/dashboard/memory/adoption-retry", revision+1, 1, "")
	staleRecorder := httptest.NewRecorder()
	handler.handleAppV25LegacyAdoptionRetry(staleRecorder, stale)
	require.Equal(t, http.StatusConflict, staleRecorder.Code, staleRecorder.Body.String())
	require.Zero(t, handler.appV25AdoptionRetry.Load())

	request := appV25RecoveryControlRequest(t,
		"/v1/dashboard/memory/adoption-retry", revision, 1, "")
	recorder := httptest.NewRecorder()
	handler.handleAppV25LegacyAdoptionRetry(recorder, request)
	require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
	require.Equal(t, uint64(1), handler.appV25AdoptionRetry.Load())
	select {
	case <-handler.appV25LegacyAdoptionWakeChannel():
	default:
		t.Fatal("retry did not wake the adoption worker")
	}
}

func TestAppV25LegacyAdoptionDeprecatePreservesRowsAndSkipsFuturePlans(t *testing.T) {
	handler, sqlite, fixture, revision, dbPath := appV25RecoveryControlFixture(t)

	wrong := appV25RecoveryControlRequest(t,
		"/v1/dashboard/memory/adoption-deprecate", revision, 1, "DEPRECATE")
	wrongRecorder := httptest.NewRecorder()
	handler.handleAppV25LegacyAdoptionDeprecate(wrongRecorder, wrong)
	require.Equal(t, http.StatusBadRequest, wrongRecorder.Code, wrongRecorder.Body.String())
	dispositions, err := sqlite.ListLegacyMemoryRecoveryDispositions(context.Background())
	require.NoError(t, err)
	require.Empty(t, dispositions)

	request := appV25RecoveryControlRequest(t,
		"/v1/dashboard/memory/adoption-deprecate", revision, 1,
		"DEPRECATE 1")
	recorder := httptest.NewRecorder()
	handler.handleAppV25LegacyAdoptionDeprecate(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	record, err := sqlite.GetMemory(context.Background(), "legacy-unconvertible")
	require.NoError(t, err)
	require.Equal(t, memory.StatusCommitted, record.Status,
		"explicit projection disposition must not rewrite memory lifecycle")
	require.Equal(t, "preserved but unverifiable", record.Content)
	dispositions, err = sqlite.ListLegacyMemoryRecoveryDispositions(context.Background())
	require.NoError(t, err)
	require.Equal(t, []store.LegacyMemoryRecoveryDisposition{{
		MemoryID: "legacy-unconvertible", Reason: "content_hash_mismatch",
		ProjectionRevision: revision, AuthorizedBy: fixture.rootID,
	}}, dispositions)
	progress, err := sqlite.GetLegacyMemoryAdoptionProgress(context.Background())
	require.NoError(t, err)
	require.NotNil(t, progress)
	require.Equal(t, "complete", progress.State)
	require.Zero(t, progress.Recovery)
	require.Zero(t, progress.Remaining)

	// A stale worker observation must not be able to resurrect a record that
	// Root explicitly retired from automatic repair.
	require.NoError(t, sqlite.SyncLegacyMemoryRecoveryQueue(
		context.Background(), revision, []store.LegacyMemoryRecoveryItem{{
			MemoryID: "legacy-unconvertible", Reason: "content_hash_mismatch",
		}},
	))
	recovery, err := sqlite.ListLegacyMemoryRecoveryQueue(context.Background(), false)
	require.NoError(t, err)
	require.Empty(t, recovery)

	plan, err := handler.buildAppV25LegacyAdoptionPlan(context.Background(), sqlite)
	require.NoError(t, err)
	require.Zero(t, plan.Discovered)
	require.Zero(t, plan.Unresolved)
	require.Empty(t, plan.Recovery)
	require.Empty(t, plan.Entries)

	replayed := appV25RecoveryControlRequest(t,
		"/v1/dashboard/memory/adoption-deprecate", revision, 1,
		"DEPRECATE 1")
	replayedRecorder := httptest.NewRecorder()
	handler.handleAppV25LegacyAdoptionDeprecate(replayedRecorder, replayed)
	require.Equal(t, http.StatusConflict, replayedRecorder.Code,
		replayedRecorder.Body.String())

	// A new store/handler instance must see the completed progress and durable
	// disposition, so reload or process restart cannot restore the notice.
	restartedStore, err := store.NewSQLiteStore(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restartedStore.Close()) })
	restartedProgress, err := restartedStore.GetLegacyMemoryAdoptionProgress(
		context.Background(),
	)
	require.NoError(t, err)
	require.NotNil(t, restartedProgress)
	require.Equal(t, "complete", restartedProgress.State)
	require.Zero(t, restartedProgress.Recovery)
	require.Zero(t, restartedProgress.Remaining)
	restartedDispositions, err := restartedStore.ListLegacyMemoryRecoveryDispositions(
		context.Background(),
	)
	require.NoError(t, err)
	require.Len(t, restartedDispositions, 1)
	restartedHandler := NewDashboardHandler(restartedStore, "test")
	restartedHandler.BadgerStore = fixture.badger
	restartedPlan, err := restartedHandler.buildAppV25LegacyAdoptionPlan(
		context.Background(), restartedStore,
	)
	require.NoError(t, err)
	require.Zero(t, restartedPlan.Unresolved)
	require.Empty(t, restartedPlan.Recovery)
}

func TestAppV25LegacyAdoptionDeprecateUsesDurableRecoverySnapshotAfterRestart(t *testing.T) {
	handler, sqlite, _, revision, _ := appV25RecoveryControlFixture(t)
	handler.ConfigureAppV25Maintenance(true)

	request := appV25RecoveryControlRequest(t,
		"/v1/dashboard/memory/adoption-deprecate", revision, 1,
		"DEPRECATE 1")
	recorder := httptest.NewRecorder()
	handler.handleAppV25LegacyAdoptionDeprecate(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	dispositions, err := sqlite.ListLegacyMemoryRecoveryDispositions(context.Background())
	require.NoError(t, err)
	require.Len(t, dispositions, 1)
}

func TestAppV25LegacyAdoptionDeprecateSurvivesOrdinaryMemoryWrites(t *testing.T) {
	handler, sqlite, fixture, revision, _ := appV25RecoveryControlFixture(t)
	content := "ordinary post-upgrade memory must not invalidate recovery"
	contentHash := sha256.Sum256([]byte(content))
	require.NoError(t, sqlite.InsertMemory(context.Background(), &memory.MemoryRecord{
		MemoryID:        "ordinary-current-memory",
		SubmittingAgent: fixture.agentID,
		Content:         content,
		ContentHash:     contentHash[:],
		MemoryType:      memory.TypeFact,
		DomainTag:       "current/domain",
		Status:          memory.StatusCommitted,
	}))
	currentRevision, err := sqlite.MemoryProjectionRevision(context.Background())
	require.NoError(t, err)
	require.NotEqual(t, revision, currentRevision,
		"the regression only appears after an ordinary write advances global projection state")

	request := appV25RecoveryControlRequest(t,
		"/v1/dashboard/memory/adoption-deprecate", revision, 1,
		"DEPRECATE 1")
	recorder := httptest.NewRecorder()
	handler.handleAppV25LegacyAdoptionDeprecate(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	dispositions, err := sqlite.ListLegacyMemoryRecoveryDispositions(context.Background())
	require.NoError(t, err)
	require.Len(t, dispositions, 1)
}

func TestAppV25LegacyRecoveryControlsRejectNonLocalRequests(t *testing.T) {
	handler, _, _, revision, _ := appV25RecoveryControlFixture(t)
	request := appV25RecoveryControlRequest(t,
		"/v1/dashboard/memory/adoption-retry", revision, 1, "")
	request.RemoteAddr = "192.0.2.10:54321"
	recorder := httptest.NewRecorder()
	handler.handleAppV25LegacyAdoptionRetry(recorder, request)
	require.Equal(t, http.StatusForbidden, recorder.Code, recorder.Body.String())
}

func appV26RecoverySelectionRequest(
	t *testing.T, path string, request appV25LegacyRecoveryControlRequest,
	key ed25519.PrivateKey,
) *http.Request {
	t.Helper()
	body, err := json.Marshal(request)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://localhost:8080")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Host = "localhost:8080"
	req.RemoteAddr = "127.0.0.1:54321"
	signAgentRequest(t, req, key, body)
	return req
}

func TestAppV26LegacyRecoveryInventoryAndCompanionAssignment(t *testing.T) {
	handler, sqlite, fixture, _, _ := appV25RecoveryControlFixture(t)
	handler.AppV26ActiveFn = func() bool { return true }
	content := "A verified historical memory whose old application label has no current key."
	digest := sha256.Sum256([]byte(content))
	require.NoError(t, sqlite.InsertMemory(context.Background(), &memory.MemoryRecord{
		MemoryID: "legacy-assignable", SubmittingAgent: "retired application label",
		Content: content, ContentHash: digest[:], MemoryType: memory.TypeFact,
		DomainTag: "historical/verified", Status: memory.StatusCommitted,
	}))
	revision, err := sqlite.MemoryProjectionRevision(context.Background())
	require.NoError(t, err)
	require.NoError(t, sqlite.SyncLegacyMemoryRecoveryQueue(
		context.Background(), revision, []store.LegacyMemoryRecoveryItem{{
			MemoryID: "legacy-assignable", Reason: "author_identity_unresolved",
		}},
	))
	require.NoError(t, sqlite.PublishLegacyMemoryAdoptionProgress(
		context.Background(), store.LegacyMemoryAdoptionProgress{
			State: "recovery", Discovered: 1, Recovery: 1, Revision: revision,
		},
	))

	get := httptest.NewRequest(http.MethodGet,
		"/v1/dashboard/memory/adoption-inventory?limit=50", nil)
	get.Host = "localhost:8080"
	get.RemoteAddr = "127.0.0.1:54321"
	get.Header.Set("Origin", "http://localhost:8080")
	get.Header.Set("Sec-Fetch-Site", "same-origin")
	signAgentRequest(t, get, fixture.rootKey, nil)
	getRecorder := httptest.NewRecorder()
	handler.handleAppV26LegacyRecoveryInventory(getRecorder, get)
	require.Equal(t, http.StatusOK, getRecorder.Code, getRecorder.Body.String())
	var inventory struct {
		Items  []appV26LegacyRecoveryInventoryView `json:"items"`
		Agents []struct {
			AgentID string `json:"agent_id"`
		} `json:"agents"`
	}
	require.NoError(t, json.Unmarshal(getRecorder.Body.Bytes(), &inventory))
	require.Len(t, inventory.Items, 1)
	require.True(t, inventory.Items[0].Assignable)
	require.Contains(t, inventory.Items[0].ContentPreview, "verified historical memory")
	require.Contains(t, inventory.Agents, struct {
		AgentID string `json:"agent_id"`
	}{AgentID: fixture.agentID},
		"an active Companion is a valid ordinary assignment target")

	assign := appV26RecoverySelectionRequest(t,
		"/v1/dashboard/memory/adoption-assign",
		appV25LegacyRecoveryControlRequest{
			ProjectionRevision: revision, ExpectedCount: 1,
			MemoryIDs: []string{"legacy-assignable"}, TargetAgentID: fixture.agentID,
		}, fixture.rootKey,
	)
	assignRecorder := httptest.NewRecorder()
	handler.handleAppV26LegacyAdoptionAssign(assignRecorder, assign)
	require.Equal(t, http.StatusAccepted, assignRecorder.Code, assignRecorder.Body.String())
	assignments, err := sqlite.ListLegacyMemoryRecoveryAssignments(context.Background())
	require.NoError(t, err)
	require.Equal(t, fixture.agentID, assignments["legacy-assignable"].TargetAgentID)
	record, err := sqlite.GetMemory(context.Background(), "legacy-assignable")
	require.NoError(t, err)
	require.Equal(t, "retired application label", record.SubmittingAgent,
		"local assignment intent must not rewrite historical authorship")
}

func TestAppV26LegacyRecoveryAssignableMatchesAtomicLifecycleRules(t *testing.T) {
	content := "verified"
	digest := sha256.Sum256([]byte(content))
	base := store.LegacyMemoryRecoveryInventoryItem{
		MemoryID: "legacy", Reason: "author_identity_unresolved",
		SubmittingAgent: "historical", Content: content, ContentHash: digest[:],
		Domain: "historical/domain", Classification: uint8(store.ClearanceInternal),
	}
	base.Status = memory.StatusProposed
	require.True(t, appV26LegacyRecoveryAssignable(base))
	base.Status = memory.StatusCommitted
	require.True(t, appV26LegacyRecoveryAssignable(base))
	for _, status := range []memory.MemoryStatus{
		memory.StatusValidated, memory.StatusChallenged, memory.StatusDeprecated,
	} {
		base.Status = status
		require.False(t, appV26LegacyRecoveryAssignable(base), status)
	}
}

func TestAppV26LegacyRecoveryAssignmentRejectsReadOnlyAgent(t *testing.T) {
	handler, sqlite, fixture, revision, _ := appV25RecoveryControlFixture(t)
	handler.AppV26ActiveFn = func() bool { return true }
	enrollment, err := fixture.badger.GetAppV23Enrollment(fixture.agentID)
	require.NoError(t, err)
	role, err := fixture.badger.GetAppV23Role(fixture.agentID)
	require.NoError(t, err)
	require.NoError(t, fixture.badger.SetAppV23Policy(
		fixture.rootID, fixture.agentID, store.AppV23RoleMember,
		enrollment.Profile, store.AppV23ProfileReadOnly, 1,
		store.AgentCapabilityReadAllDomains, role.Revision, enrollment.Revision, 2,
	))
	request := appV26RecoverySelectionRequest(t,
		"/v1/dashboard/memory/adoption-assign",
		appV25LegacyRecoveryControlRequest{
			ProjectionRevision: revision, ExpectedCount: 1,
			MemoryIDs: []string{"legacy-unconvertible"}, TargetAgentID: fixture.agentID,
		}, fixture.rootKey,
	)
	recorder := httptest.NewRecorder()
	handler.handleAppV26LegacyAdoptionAssign(recorder, request)
	require.Equal(t, http.StatusConflict, recorder.Code, recorder.Body.String())
	assignments, err := sqlite.ListLegacyMemoryRecoveryAssignments(context.Background())
	require.NoError(t, err)
	require.Empty(t, assignments)
}
