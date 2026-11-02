package abci

import (
	"crypto/sha256"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/l33tdawg/sage/internal/lineage"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/stretchr/testify/require"
)

func TestAppV30StrictBoundaryAndDormantAutoVote(t *testing.T) {
	require.Equal(t, tx.CanonicalUpgradeName(30), appV30UpgradeName)
	require.Equal(t, uint64(30), MaxCompiledAppVersion())
	require.Equal(t, uint64(29), MaxSupportedAppVersion())

	app := setupTestApp(t)
	app.state.Height = 101
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV29UpgradeName, 29, 100))
	app.appV29AppliedHeight = 100
	require.NoError(t, app.refreshAppV30Fork())
	require.Equal(t, int64(0), app.appV30AppliedHeight)

	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV30UpgradeName, 30, 100))
	require.NoError(t, app.refreshAppV30Fork())
	require.Equal(t, int64(100), app.appV30AppliedHeight)
	require.False(t, app.postAppV30Fork(100), "the activation block keeps app-v29 semantics")
	require.True(t, app.postAppV30Fork(101), "v30 rules begin strictly at H+1")
	require.Equal(t, uint64(30), app.currentAppVersion())
}

func TestAppV30RefreshRequiresV29Predecessor(t *testing.T) {
	app := setupTestApp(t)
	app.state.Height = 101
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV30UpgradeName, 30, 100))
	require.NoError(t, app.refreshAppV30Fork())
	require.Equal(t, int64(100), app.appV30AppliedHeight)
	require.ErrorContains(t, app.validateAppV30Prerequisite(), "missing active app-v29 predecessor")
}

func TestAppV30DerivedFromExtensionUsesStrictForkBoundary(t *testing.T) {
	app := setupTestApp(t)
	app.appV30AppliedHeight = 100

	parsed := makeMemorySubmitTx(t, newAgentKey(t), "general", "lineage child")
	parsed.MemorySubmit.DerivedFrom = []tx.ParentRef{{
		SourceChainID: "sage",
		MemoryID:      "parent",
		ContentHash:   sha256.Sum256([]byte("parent content")),
	}}
	raw, err := tx.EncodeTx(parsed)
	require.NoError(t, err)

	activationBlock, err := tx.DecodeTx(raw)
	require.NoError(t, err)
	require.ErrorContains(
		t,
		tx.ActivateMemorySubmitExtensions(
			activationBlock,
			true,
			app.postAppV30Fork(100),
		),
		"derived_from extension is not active",
	)
	direct := app.processMemorySubmit(parsed, 100, appV23BlockTime())
	require.Equal(t, uint32(10), direct.Code)
	require.Contains(t, direct.Log, "derived_from extension is not active")

	firstV30Block, err := tx.DecodeTx(raw)
	require.NoError(t, err)
	require.NoError(t, tx.ActivateMemorySubmitExtensions(
		firstV30Block,
		true,
		app.postAppV30Fork(101),
	))
	require.Len(t, firstV30Block.MemorySubmit.DerivedFrom, 1)
}

func TestAppV30RecoveryAuditRebuildsLineageAndRejectsTaintDamage(t *testing.T) {
	app := setupTestApp(t)
	app.appV30AppliedHeight = 100
	parent := tx.ParentRef{
		SourceChainID: "sage",
		MemoryID:      "recovery-parent",
		ContentHash:   sha256.Sum256([]byte("recovery parent")),
	}
	require.NoError(t, app.badgerStore.SetMemoryLineage(
		"recovery-child",
		[]tx.ParentRef{parent},
	))
	parentID, err := tx.ParentRefIDHex(parent)
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.DB().Update(func(txn *badger.Txn) error {
		return txn.Delete(lineage.ReverseKey(parentID, "recovery-child"))
	}))
	require.NoError(t, app.validateAppV30RecoveryState())

	decisionID := strings.Repeat("a", taint.DecisionIDBytes)
	created, err := app.badgerStore.ApplyTaintDecision(
		"sage",
		decisionID,
		parent,
		sha256.Sum256([]byte("recovery taint")),
	)
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, app.badgerStore.DB().Update(func(txn *badger.Txn) error {
		return txn.Delete(taint.DecisionIndexKey(decisionID, parent.MemoryID))
	}))
	require.Error(t, app.validateAppV30RecoveryState())
}
