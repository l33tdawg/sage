package abci

import (
	"context"
	"strings"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	cmttypes "github.com/cometbft/cometbft/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
)

func appV20ResourceInvalidRegister(t *testing.T, key agentKey, nonce uint64) []byte {
	t.Helper()
	parsed := makeAgentRegisterTx(
		t,
		key,
		strings.Repeat("n", maxAppV20IdentifierBytes+1),
		"member",
		"",
		"test",
		"",
	)
	parsed.Nonce = nonce
	require.NoError(t, tx.SignTx(parsed, key.priv))
	return encodeGovernanceReplayTx(t, parsed)
}

func appV20ValidRegister(t *testing.T, key agentKey, nonce uint64, name string) []byte {
	t.Helper()
	parsed := makeAgentRegisterTx(t, key, name, "member", "", "test", "")
	parsed.Nonce = nonce
	require.NoError(t, tx.SignTx(parsed, key.priv))
	return encodeGovernanceReplayTx(t, parsed)
}

// A resource-invalid transaction may have entered the mempool before the
// authenticated app-v20 ceremony committed. Strict proposal preparation must
// seat that bounded stale transaction instead of filtering it forever: Comet's
// Update removes every transaction included in a committed block regardless of
// its ExecTxResult code. FinalizeBlock remains the deterministic enforcement
// boundary and rejects it before nonce or handler mutation.
func TestAppV20StrictProposalDrainsStaleResourceInvalidTxWithoutStarvingValidFollower(t *testing.T) {
	h := newGovernanceReplayHarness(t)
	invalidKey := newAgentKey(t)
	validKey := newAgentKey(t)
	invalidRaw := appV20ResourceInvalidRegister(t, invalidKey, 1)
	validRaw := appV20ValidRegister(t, validKey, 1, "valid-after-stale")

	require.Less(t, len(invalidRaw)+len(validRaw), maxAppV20AtomicFinalizeTxBytes)
	maxTxBytes := int64(2 << 20)
	input := [][]byte{invalidRaw, validRaw}
	prepared, err := h.app.PrepareProposal(context.Background(), &abcitypes.RequestPrepareProposal{
		Height: 2, Time: time.Unix(21_002, 0).UTC(), MaxTxBytes: maxTxBytes, Txs: input,
	})
	require.NoError(t, err)
	require.Len(t, prepared.Txs, 2, "bounded stale tx must be seated so Commit can evict it; a valid follower must not starve")
	assert.Equal(t, invalidRaw, prepared.Txs[0])
	assert.Equal(t, validRaw, prepared.Txs[1])
	require.NoError(t, cmttypes.ToTxs(prepared.Txs).Validate(maxTxBytes))

	processed, err := h.app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
		Height: 2, Time: time.Unix(21_002, 0).UTC(), Txs: prepared.Txs,
	})
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseProcessProposal_ACCEPT, processed.Status)

	finalized := finalizeGovernanceReplayBlock(t, h.app, &abcitypes.RequestFinalizeBlock{
		Height: 2, Time: time.Unix(21_002, 0).UTC(), Txs: prepared.Txs,
	})
	require.Len(t, finalized.TxResults, 2)
	assert.Equal(t, appV20ResourceLimitCode, finalized.TxResults[0].Code)
	assert.Contains(t, finalized.TxResults[0].Log, "agent name")
	assert.Zero(t, finalized.TxResults[1].Code, finalized.TxResults[1].Log)
	commitGovernanceReplayBlock(t, h.app)

	assert.False(t, h.app.badgerStore.IsAgentRegistered(invalidKey.id))
	invalidNonce, err := h.app.badgerStore.GetNonce(invalidKey.id)
	require.NoError(t, err)
	assert.Zero(t, invalidNonce, "resource rejection must happen before nonce mutation")
	assert.True(t, h.app.badgerStore.IsAgentRegistered(validKey.id))
	validNonce, err := h.app.badgerStore.GetNonce(validKey.id)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), validNonce)
}

// CheckTx is advisory and may enforce the future app-v20 resource ceiling
// before activation to keep poison transactions out of fresh mempools. The
// consensus path must remain fork-gated: directly delivered pre-v20 blocks,
// including historical replay, retain their legacy execution semantics.
func TestAppV20ResourceCheckTxHygieneDoesNotChangePreActivationFinalizeParity(t *testing.T) {
	app := setupTestApp(t)
	app.appV19AppliedHeight = 1
	key := newAgentKey(t)
	raw := appV20ResourceInvalidRegister(t, key, 1)

	checked, err := app.CheckTx(context.Background(), &abcitypes.RequestCheckTx{Tx: raw})
	require.NoError(t, err)
	assert.Equal(t, appV20ResourceLimitCode, checked.Code)
	assert.Contains(t, checked.Log, "agent name")

	finalized := finalizeGovernanceReplayBlock(t, app, &abcitypes.RequestFinalizeBlock{
		Height: 2, Time: time.Unix(21_102, 0).UTC(), Txs: [][]byte{raw},
	})
	require.Len(t, finalized.TxResults, 1)
	assert.Zero(t, finalized.TxResults[0].Code, finalized.TxResults[0].Log)
	commitGovernanceReplayBlock(t, app)
	assert.True(t, app.badgerStore.IsAgentRegistered(key.id), "pre-v20 direct delivery must retain legacy consensus semantics")
	nonce, err := app.badgerStore.GetNonce(key.id)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), nonce)
}

// An authenticated bootstrap that cannot fit this proposal's protobuf byte
// budget must not turn otherwise-eligible legacy traffic into an empty block.
// It also cannot be mixed into the fallback: ProcessProposal would correctly
// reject that. Skip only the oversized recognized bootstrap, byte-pack the
// ordinary follower, and leave the bootstrap in the mempool for a proposal
// whose consensus byte budget can actually contain it.
func TestAppV20OversizedAuthenticatedBootstrapDoesNotStarveOrdinaryFollower(t *testing.T) {
	app, admin := setupGovTestApp(t)
	app.appV19AppliedHeight = 1
	blockTime := time.Unix(21_202, 0).UTC()
	bootstrap := makeUpgradeProposeTx(t, admin, appV20UpgradeName, 20, strings.Repeat("a", maxAppV20IdentifierBytes), defaultUpgradeDelayBlocks)
	bootstrap.UpgradePropose.GovernanceDomain = governanceReplayTestDomain
	bootstrap.Nonce = 1
	bootstrap.Timestamp = blockTime
	require.NoError(t, tx.SignTx(bootstrap, admin.priv))
	bootstrapRaw := encodeGovernanceReplayTx(t, bootstrap)
	require.True(t, app.isAuthenticatedAppV20BootstrapProposal(bootstrapRaw, 2, blockTime))

	ordinaryKey := newAgentKey(t)
	ordinaryRaw := appV20ValidRegister(t, ordinaryKey, 1, "ordinary-follower")
	bootstrapProtoBytes := cmttypes.ComputeProtoSizeForTxs([]cmttypes.Tx{cmttypes.Tx(bootstrapRaw)})
	ordinaryProtoBytes := cmttypes.ComputeProtoSizeForTxs([]cmttypes.Tx{cmttypes.Tx(ordinaryRaw)})
	maxTxBytes := bootstrapProtoBytes - 1
	require.Positive(t, maxTxBytes)
	require.LessOrEqual(t, ordinaryProtoBytes, maxTxBytes, "fixture ordinary transaction must fit below the padded bootstrap")

	prepared, err := app.PrepareProposal(context.Background(), &abcitypes.RequestPrepareProposal{
		Height: 2, Time: blockTime, MaxTxBytes: maxTxBytes,
		Txs: [][]byte{bootstrapRaw, ordinaryRaw},
	})
	require.NoError(t, err)
	require.Len(t, prepared.Txs, 1, "oversized recognized bootstrap must not suppress a follower that fits")
	assert.Equal(t, ordinaryRaw, prepared.Txs[0])
	require.NoError(t, cmttypes.ToTxs(prepared.Txs).Validate(maxTxBytes))

	processed, err := app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
		Height: 2, Time: blockTime, Txs: prepared.Txs,
	})
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseProcessProposal_ACCEPT, processed.Status)
}
