package abci

import (
	"context"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// TestV75_EndToEnd_ProposeEncodeFinalizeActivate is the v7.5
// integration test: a real UpgradePropose tx is built via the same
// signAgentProof helper the watchdog uses, encoded with tx.EncodeTx,
// decoded with tx.DecodeTx, dispatched through processTx, then
// FinalizeBlock is invoked at heights spanning the activation point.
//
// This is the consensus-path equivalent of the watchdog → ABCI →
// activation flow, minus the CometBFT RPC hop. It proves the codec +
// dispatch + handler + state-mutation + FinalizeBlock activation
// hand off cleanly to each other.
func TestV75_EndToEnd_ProposeEncodeFinalizeActivate(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	const proposalName = "v7.5-e2e-test"
	const targetAppVersion uint64 = 9
	const proposeHeight = int64(50)

	// 1. Build a signed UpgradePropose tx the way the watchdog does.
	body := []byte(proposalName)
	pubKey, sig, bodyHash, ts := signAgentProof(t, ak, body)
	parsedTx := &tx.ParsedTx{
		Type:           tx.TxTypeUpgradePropose,
		Nonce:          uint64(time.Now().UnixNano()), // #nosec G115 -- nonce derived from timestamp
		Timestamp:      time.Unix(ts, 0),
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
		UpgradePropose: &tx.UpgradePropose{
			Name:               proposalName,
			TargetAppVersion:   targetAppVersion,
			BinarySHA256:       "0123456789abcdef",
			ProposerID:         ak.id,
			UpgradeDelayBlocks: 0, // chain floor will raise to defaultUpgradeDelayBlocks (200)
		},
	}
	require.NoError(t, tx.SignTx(parsedTx, ak.priv), "sign outer tx")

	// 2. Encode + decode — exercises the full wire format the watchdog
	//    sends through CometBFT RPC.
	encoded, err := tx.EncodeTx(parsedTx)
	require.NoError(t, err, "encode tx")
	decoded, err := tx.DecodeTx(encoded)
	require.NoError(t, err, "decode tx")
	require.Equal(t, tx.TxTypeUpgradePropose, decoded.Type)
	require.NotNil(t, decoded.UpgradePropose)
	require.Equal(t, proposalName, decoded.UpgradePropose.Name)
	require.Equal(t, targetAppVersion, decoded.UpgradePropose.TargetAppVersion)

	// 3. Dispatch via processTx (the same path FinalizeBlock would take
	//    for a real tx). This proves the routing case in the switch is
	//    wired correctly post-state-mutation refactor.
	result := app.processTx(decoded, proposeHeight, time.Unix(ts, 0))
	require.Equal(t, uint32(0), result.Code, "processTx rejected propose: %s", result.Log)
	assert.Contains(t, result.Log, "upgrade plan accepted")

	// 4. Plan is persisted in BadgerDB. Activation height should be
	//    proposeHeight + max(payload.delay=0, floor=200) = 250.
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)
	assert.Equal(t, proposalName, plan.Name)
	assert.Equal(t, targetAppVersion, plan.TargetAppVersion)
	expectedActivation := proposeHeight + defaultUpgradeDelayBlocks
	assert.Equal(t, expectedActivation, plan.ActivationHeight,
		"chain floor should raise zero UpgradeDelayBlocks to %d", defaultUpgradeDelayBlocks)

	// 5. FinalizeBlock on a height before activation — no
	//    ConsensusParamUpdates, plan still pending.
	respBefore, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: expectedActivation - 1,
		Time:   time.Now(),
	})
	require.NoError(t, err)
	assert.Nil(t, respBefore.ConsensusParamUpdates,
		"pre-activation block must not emit ConsensusParamUpdates")
	stillPending, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, stillPending)

	// 6. FinalizeBlock at activation height. CometBFT applies the new
	//    app version at H+1 across every node, but the response is
	//    emitted at H.
	respAt, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: expectedActivation,
		Time:   time.Now(),
	})
	require.NoError(t, err)
	require.NotNil(t, respAt.ConsensusParamUpdates,
		"activation block must emit ConsensusParamUpdates")
	require.NotNil(t, respAt.ConsensusParamUpdates.Version)
	assert.Equal(t, targetAppVersion, respAt.ConsensusParamUpdates.Version.App,
		"activation block must set Version.App to the proposal's target")

	// 7. Post-activation state. Plan deleted; audit record persisted.
	planAfter, err := app.badgerStore.GetUpgradePlan()
	assert.ErrorIs(t, err, store.ErrNoUpgradePlan, "plan should be cleared")
	assert.Nil(t, planAfter)

	applied, err := app.badgerStore.GetAppliedUpgrade(proposalName)
	require.NoError(t, err)
	require.NotNil(t, applied, "applied audit record must exist")
	assert.Equal(t, targetAppVersion, applied.TargetAppVersion)
	assert.Equal(t, expectedActivation, applied.AppliedHeight)

	// 8. Subsequent blocks — no further ConsensusParamUpdates emitted
	//    (CometBFT only needs the bump once; replaying would be a bug).
	respAfter, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: expectedActivation + 1,
		Time:   time.Now(),
	})
	require.NoError(t, err)
	assert.Nil(t, respAfter.ConsensusParamUpdates,
		"post-activation block must not re-emit ConsensusParamUpdates")
}

// TestV75_EndToEnd_CancelBeforeActivation exercises the cancel path:
// propose lands at height 100, cancel arrives at height 150 (before
// activation at 300), FinalizeBlock at 300 must NOT activate the
// cancelled plan.
func TestV75_EndToEnd_CancelBeforeActivation(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	// Propose at height 100.
	body := []byte("v7.5-cancel-e2e")
	pubKey, sig, bodyHash, ts := signAgentProof(t, ak, body)
	proposeTx := &tx.ParsedTx{
		Type:           tx.TxTypeUpgradePropose,
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
		UpgradePropose: &tx.UpgradePropose{
			Name:             "v7.5-cancel-e2e",
			TargetAppVersion: 10,
			ProposerID:       ak.id,
		},
	}
	require.Equal(t, uint32(0), app.processTx(proposeTx, 100, time.Unix(ts, 0)).Code)

	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)
	activation := plan.ActivationHeight // 100 + 200 = 300

	// Cancel at height 150 (well before activation).
	cancelBody := []byte("v7.5-cancel-e2e")
	cpub, csig, cbh, cts := signAgentProof(t, ak, cancelBody)
	cancelTx := &tx.ParsedTx{
		Type:           tx.TxTypeUpgradeCancel,
		AgentPubKey:    cpub,
		AgentSig:       csig,
		AgentBodyHash:  cbh,
		AgentTimestamp: cts,
		UpgradeCancel: &tx.UpgradeCancel{
			Name:        "v7.5-cancel-e2e",
			CancellerID: ak.id,
			Reason:      "second thoughts",
		},
	}
	require.Equal(t, uint32(0), app.processTx(cancelTx, 150, time.Unix(cts, 0)).Code)

	// FinalizeBlock at the formerly-scheduled activation height must
	// NOT emit ConsensusParamUpdates (plan was cancelled).
	resp, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: activation,
		Time:   time.Now(),
	})
	require.NoError(t, err)
	assert.Nil(t, resp.ConsensusParamUpdates,
		"cancelled plan must not activate at its former activation height")

	// And no applied record was written.
	applied, err := app.badgerStore.GetAppliedUpgrade("v7.5-cancel-e2e")
	require.NoError(t, err)
	assert.Nil(t, applied, "applied audit record must not exist for cancelled plan")
}

// TestV85_EndToEnd_AppV6ActivationAndGuards drives a full canonical app-v6
// activation through real processTx/FinalizeBlock, then exercises all three
// app-v6 guards end-to-end against the live consensus-param version:
//
//	(a) a non-canonical propose → Code 47, no plan (Change 1);
//	(b) a downgrade propose to 5 → Code 47 while Info().AppVersion stays 6 (Change 2);
//	(c) a revert above activation → Code 90 (Change 3);
//	(d) a canonical app-v7/7 → Code 0 + activation (proves app-v6 didn't self-block).
func TestV85_EndToEnd_AppV6ActivationAndGuards(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	// A real chain reaches app-v6 only after the lower forks. Seed v8..v8.4 so
	// FinalizeBlock's reconcile yields a coherent currentAppVersion()==5 before
	// the app-v6 activation, and the app-v6 propose isn't a regression.
	activateV85(app, 5)
	app.v8_5AppliedHeight = 0 // drive the app-v6 gate via real activation below
	require.Equal(t, uint64(5), app.currentAppVersion())

	// 1. Propose canonical app-v6 (pre-fork, so the guards don't block its own
	//    activation — the self-bootstrapping case).
	proposeTx := makeUpgradeProposeTx(t, ak, v8_5UpgradeName, 6, "", 0)
	require.Equal(t, uint32(0), app.processTx(proposeTx, 100, time.Now()).Code)
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)
	actH := plan.ActivationHeight // 100 + 200 floor

	// 2. FinalizeBlock at activation height flips the gate and bumps version.app.
	respAt, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actH, Time: time.Now()})
	require.NoError(t, err)
	require.NotNil(t, respAt.ConsensusParamUpdates)
	require.NotNil(t, respAt.ConsensusParamUpdates.Version)
	require.Equal(t, uint64(6), respAt.ConsensusParamUpdates.Version.App)
	require.Equal(t, actH, app.v8_5AppliedHeight, "app-v6 gate must flip on activation")

	// Guards engage at H_act+1. Use a post-activation height for the rest.
	postH := actH + 10
	require.True(t, app.postV8_5Fork(postH))

	// (a) non-canonical propose → Code 47, no plan.
	nonCanon := makeUpgradeProposeTx(t, ak, "v8.6.0", 7, "", 0)
	resA := app.processTx(nonCanon, postH, time.Now())
	assert.Equal(t, uint32(47), resA.Code)
	assert.Contains(t, resA.Log, "non-canonical name")
	_, errA := app.badgerStore.GetUpgradePlan()
	assert.ErrorIs(t, errA, store.ErrNoUpgradePlan, "non-canonical propose must persist no plan")

	// (b) downgrade propose to 5 → Code 47; Info().AppVersion stays 6.
	downgrade := makeUpgradeProposeTx(t, ak, "app-v5", 5, "", 0)
	resB := app.processTx(downgrade, postH, time.Now())
	assert.Equal(t, uint32(47), resB.Code)
	assert.Contains(t, resB.Log, "regression/no-op rejected")
	info, err := app.Info(context.Background(), &abcitypes.RequestInfo{})
	require.NoError(t, err)
	assert.Equal(t, uint64(6), info.AppVersion, "downgrade reject must not drop the live app version")

	// (c) revert above activation → Code 90.
	revert := makeUpgradeRevertTx(t, ak, "app-v6", 6, actH)
	resC := app.processTx(revert, postH, time.Now())
	assert.Equal(t, uint32(90), resC.Code)
	assert.Contains(t, resC.Log, "in-band downgrade unsupported")

	// (d) canonical app-v7/7 → Code 0 + persisted plan (app-v6 didn't self-block).
	forward := makeUpgradeProposeTx(t, ak, "app-v7", 7, "", 0)
	resD := app.processTx(forward, postH, time.Now())
	require.Equal(t, uint32(0), resD.Code, "canonical forward upgrade must be accepted: %s", resD.Log)
	planD, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, planD)
	assert.Equal(t, "app-v7", planD.Name)
	assert.Equal(t, uint64(7), planD.TargetAppVersion)
}
