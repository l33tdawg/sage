package abci

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

func activateAppV30ForMemorySubmitTest(t *testing.T, app *SageApp, height int64) {
	t.Helper()
	app.appV20AppliedHeight = 2
	app.appV21AppliedHeight = 3
	app.appV22AppliedHeight = 4
	app.appV23AppliedHeight = 5
	app.appV24AppliedHeight = 6
	app.appV25AppliedHeight = 7
	app.appV26AppliedHeight = 8
	app.appV27AppliedHeight = 9
	app.appV28AppliedHeight = 10
	app.appV29AppliedHeight = 11
	app.appV30AppliedHeight = height - 1
}

func TestAppV30MemorySubmitWritesImmutableProvenance(t *testing.T) {
	app := setupTestApp(t)
	root := newAgentKey(t)
	registerAppV23Agent(t, app, root, store.AppV23RoleAdmin, 2, store.AgentCapabilityReadAllDomains)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("app-v30-provenance", 2))
	rootState, err := app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.NotNil(t, rootState)
	activateAppV30ForMemorySubmitTest(t, app, 20)

	result := app.processMemorySubmit(
		makeMemorySubmitTx(t, root, "general", "app-v30 provenance content"),
		20,
		appV23BlockTime(),
	)
	require.Equal(t, uint32(0), result.Code, result.Log)
	memoryID := string(result.Data)

	signers, found, err := app.badgerStore.GetAppV30Provenance(memoryID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []store.AppV30ProvenanceSigner{{
		CredentialID:   root.id,
		PrincipalID:    rootState.PrincipalID,
		RootGeneration: rootState.Generation,
		Exact:          true,
	}}, signers)
}

func TestAppV30ProvenanceStaysDormantBeforeHPlusOne(t *testing.T) {
	app := setupTestApp(t)
	root := newAgentKey(t)
	registerAppV23Agent(t, app, root, store.AppV23RoleAdmin, 2, store.AgentCapabilityReadAllDomains)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("app-v30-dormant", 2))
	activateAppV30ForMemorySubmitTest(t, app, 20)
	app.appV30AppliedHeight = 20

	result := app.processMemorySubmit(
		makeMemorySubmitTx(t, root, "general", "activation block has no v30 marker"),
		20,
		appV23BlockTime(),
	)
	require.Equal(t, uint32(0), result.Code, result.Log)
	_, found, err := app.badgerStore.GetAppV30Provenance(string(result.Data))
	require.NoError(t, err)
	require.False(t, found)
}

func TestAppV30ProvenanceConflictRejectsBeforeMemoryWrites(t *testing.T) {
	app := setupTestApp(t)
	root := newAgentKey(t)
	other := newAgentKey(t)
	registerAppV23Agent(t, app, root, store.AppV23RoleAdmin, 2, store.AgentCapabilityReadAllDomains)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("app-v30-conflict", 2))
	rootState, err := app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.NotNil(t, rootState)
	activateAppV30ForMemorySubmitTest(t, app, 20)

	submit := makeMemorySubmitTx(t, root, "general", "conflicting provenance content")
	memoryID := memoryIDForSubmit(submit.MemorySubmit, 20, root.id)
	require.NoError(t, app.badgerStore.SetAppV30Provenance(
		memoryID,
		[]store.AppV30ProvenanceSigner{{
			CredentialID:   other.id,
			PrincipalID:    rootState.PrincipalID,
			RootGeneration: 0,
			Exact:          true,
		}},
	))
	pendingBefore := len(app.pendingWrites)

	result := app.processMemorySubmit(submit, 20, appV23BlockTime())
	require.Equal(t, uint32(11), result.Code)
	require.Contains(t, result.Log, store.ErrAppV30ProvenanceConflict.Error())
	require.Len(t, app.pendingWrites, pendingBefore)
}
