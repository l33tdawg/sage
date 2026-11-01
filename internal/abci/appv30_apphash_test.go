package abci

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestAppV30CanonicalStateAppHashGolden(t *testing.T) {
	badgerStore, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, badgerStore.CloseBadger()) })

	parentContent := sha256.Sum256([]byte("app-v30 parent content"))
	childContent := sha256.Sum256([]byte("app-v30 child content"))
	credentialID := strings.Repeat("a", 64)
	principalID := strings.Repeat("b", 64)
	for _, item := range []struct {
		memoryID string
		hash     [32]byte
		domain   string
	}{
		{"app-v30-parent", parentContent, "general"},
		{"app-v30-child", childContent, "general"},
	} {
		require.NoError(t, badgerStore.SetMemoryHash(
			item.memoryID,
			item.hash[:],
			"committed",
		))
		require.NoError(t, badgerStore.SetMemoryDomain(item.memoryID, item.domain))
		require.NoError(t, badgerStore.SetMemoryAuthor(item.memoryID, credentialID))
		require.NoError(t, badgerStore.SetMemoryAuthorPrincipal(item.memoryID, principalID))
		require.NoError(t, badgerStore.SetMemoryClassification(
			item.memoryID,
			uint8(store.ClearanceInternal),
		))
		require.NoError(t, badgerStore.SetMemorySubmissionHeight(item.memoryID, 10))
	}

	parent := tx.ParentRef{
		SourceChainID: "app-v30-golden-chain",
		MemoryID:      "app-v30-parent",
		ContentHash:   parentContent,
	}
	require.NoError(t, badgerStore.SetMemoryLineage(
		"app-v30-child",
		[]tx.ParentRef{parent},
	))
	decisionID := strings.Repeat("c", taint.DecisionIDBytes)
	created, err := badgerStore.ApplyTaintDecision(
		parent.SourceChainID,
		decisionID,
		parent,
		sha256.Sum256([]byte("app-v30 decision payload")),
	)
	require.NoError(t, err)
	require.True(t, created)
	_, err = badgerStore.ExpandTaintDecision(
		parent.SourceChainID,
		decisionID,
		taint.PerExecutionUnitBudget,
	)
	require.NoError(t, err)
	require.NoError(t, badgerStore.SetAppV30Provenance(
		"app-v30-child",
		[]store.AppV30ProvenanceSigner{{
			CredentialID:   credentialID,
			PrincipalID:    principalID,
			RootGeneration: 0,
			Exact:          true,
		}},
	))
	adoptionReceipt := sha256.Sum256([]byte("app-v30 adoption receipt"))
	require.NoError(t, badgerStore.DB().Update(func(txn *badger.Txn) error {
		if err := txn.Set(
			[]byte("memprov:migrated:app-v30-child"),
			[]byte{1},
		); err != nil {
			return err
		}
		return txn.Set(
			[]byte("appv25:legacy-adoption:app-v30-parent"),
			adoptionReceipt[:],
		)
	}))

	appHash, err := ComputeAppHash(badgerStore)
	require.NoError(t, err)
	t.Logf("app-v30 AppHash: %s", hex.EncodeToString(appHash))
	require.Equal(
		t,
		"c900515adc1a455b307fe053d458664cb9fe04cd8400e0eef06b012b16aad253",
		hex.EncodeToString(appHash),
	)
}
