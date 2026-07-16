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
			UpgradeDelayBlocks: defaultUpgradeDelayBlocks,
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

	// 4. Plan is persisted in BadgerDB. This historical scenario carries the
	//    production delay explicitly, so activation remains at height 250 even
	//    in the shortened v11.9 fixture build.
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)
	assert.Equal(t, proposalName, plan.Name)
	assert.Equal(t, targetAppVersion, plan.TargetAppVersion)
	expectedActivation := proposeHeight + defaultUpgradeDelayBlocks
	assert.Equal(t, expectedActivation, plan.ActivationHeight,
		"explicit production delay should remain %d blocks", defaultUpgradeDelayBlocks)

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
			Name:               "v7.5-cancel-e2e",
			TargetAppVersion:   10,
			ProposerID:         ak.id,
			UpgradeDelayBlocks: defaultUpgradeDelayBlocks,
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

// TestV87_EndToEnd_AppV7ActivationReportsVersion7 drives a REAL app-v7
// content-validator fork activation all the way to its activation block — the
// step the pre-existing app-v7 coverage stopped short of ("plan persisted" in
// TestV85_EndToEnd_AppV6ActivationAndGuards step (d)) — and asserts the fix for
// the version-regression handshake halt:
//
//	FinalizeBlock commits ConsensusParamUpdates.Version.App == 7 AND
//	currentAppVersion()/Info().AppVersion == 7 (== the committed param) AND
//	the app-v7 gate (postAppV7Fork) bites at H_act+1.
//
// Pre-fix, FinalizeBlock bumped the committed param to 7 while Info() kept
// reporting 6 (currentAppVersion had no app-v7 case), so a restarting node
// handed CometBFT a 7→6 app-version regression and every node halted on the next
// handshake. This test locks the gate→version coherence that prevents it.
func TestV87_EndToEnd_AppV7ActivationReportsVersion7(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	info := func() uint64 {
		resp, err := app.Info(context.Background(), &abcitypes.RequestInfo{})
		require.NoError(t, err)
		return resp.AppVersion
	}

	// Reach app-v6 first (a real chain activates app-v7 on top of the PoE ladder),
	// so the app-v6 version-regression guard accepts the 7>6 propose.
	activateV85(app, 5)
	require.Equal(t, uint64(6), app.currentAppVersion(), "precondition: chain at app-v6")
	require.Equal(t, uint64(6), info(), "precondition: Info reports 6 before app-v7")
	require.False(t, app.postAppV7Fork(1_000_000), "app-v7 gate dormant before activation")

	// 1. Propose canonical app-v7/7 (post-app-v6 fork: canonical-name + 7>6 guards pass).
	postH := int64(1_000)
	require.True(t, app.postV8_5Fork(postH))
	proposeTx := makeUpgradeProposeTx(t, ak, "app-v7", 7, "", 0)
	require.Equal(t, uint32(0), app.processTx(proposeTx, postH, time.Now()).Code,
		"canonical app-v7 propose must be accepted")
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Equal(t, "app-v7", plan.Name)
	require.Equal(t, uint64(7), plan.TargetAppVersion)
	actH := plan.ActivationHeight // postH + 200 floor

	// 2. FinalizeBlock at the activation height: commits version.app=7 AND flips
	//    the app-v7 gate. THIS is where the pre-fix regression was born.
	respAt, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actH, Time: time.Now()})
	require.NoError(t, err)
	require.NotNil(t, respAt.ConsensusParamUpdates)
	require.NotNil(t, respAt.ConsensusParamUpdates.Version)
	assert.Equal(t, uint64(7), respAt.ConsensusParamUpdates.Version.App,
		"activation block must commit consensus version.app=7")
	assert.Equal(t, actH, app.appV7AppliedHeight, "app-v7 gate must flip on activation")

	// 3. THE HALT FIX: Info()/currentAppVersion() must now report 7 — matching the
	//    committed consensus param. A mismatch here is the regression that halts
	//    every node on its next restart/handshake.
	assert.Equal(t, uint64(7), app.currentAppVersion(), "currentAppVersion must report 7 post-app-v7")
	assert.Equal(t, uint64(7), info(), "Info().AppVersion must equal the committed version.app (7)")

	// 4. The app-v7 fork bites at H_act+1 (strict >, matching CometBFT H+1).
	assert.False(t, app.postAppV7Fork(actH), "at activation block: still pre-fork (H+1 semantic)")
	assert.True(t, app.postAppV7Fork(actH+1), "first post-activation block: app-v7 fork active")

	// 5. Simulate a restart: refreshAppV7Fork rehydrates the gate from the
	//    persisted audit record, and Info() STILL reports 7 — the handshake-safe
	//    invariant survives a reboot.
	app.appV7AppliedHeight = 0
	app.refreshAppV7Fork()
	assert.Equal(t, actH, app.appV7AppliedHeight, "restart must rehydrate app-v7 gate from audit record")
	assert.Equal(t, uint64(7), info(), "post-restart Info() must still report 7 (no regression on handshake)")
}

// TestV87_AppV7ActivationBlocksLaterAppV6VersionRegression reproduces the
// out-of-order halt window the version-non-regression floor closes. On a YOUNG
// chain (pre-app-v6, so the propose-time guards are skipped) an operator can
// activate app-v7 (version 7) before the watchdog walks the PoE ladder up to
// app-v6. When app-v6 (version 6) later activates, the committed consensus
// version.app must NOT regress 7->6 — a regression would make the next CometBFT
// handshake see a lower app version and halt every node (the v8.4.1/8.4.2 bug
// class) — while the app-v6 feature gate still turns on.
func TestV87_AppV7ActivationBlocksLaterAppV6VersionRegression(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	info := func() uint64 {
		resp, err := app.Info(context.Background(), &abcitypes.RequestInfo{})
		require.NoError(t, err)
		return resp.AppVersion
	}

	// 1. Activate app-v7 out of order on a young chain (propose guards are
	//    postV8_5Fork-gated, so the chain accepts app-v7 before app-v6).
	proposeV7 := makeUpgradeProposeTx(t, ak, "app-v7", 7, "", 0)
	require.Equal(t, uint32(0), app.processTx(proposeV7, int64(1_000), time.Now()).Code,
		"app-v7 propose accepted on a young chain")
	planV7, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	actV7 := planV7.ActivationHeight
	respV7, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actV7, Time: time.Now()})
	require.NoError(t, err)
	require.NotNil(t, respV7.ConsensusParamUpdates)
	require.Equal(t, uint64(7), respV7.ConsensusParamUpdates.Version.App, "app-v7 commits version.app=7")
	require.Equal(t, uint64(7), info(), "chain reports version 7 after app-v7")
	require.Equal(t, uint64(7), app.currentAppVersion())

	// 2. A LATER app-v6 plan activates (the watchdog finally reaches app-v6),
	//    targeting version 6 < the current 7. Inject it directly to model the
	//    out-of-order activation without a second propose.
	actV6 := actV7 + 50
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name:             v8_5UpgradeName, // "app-v6"
		TargetAppVersion: 6,
		ActivationHeight: actV6,
	}))

	// 3. THE FLOOR: app-v6 activation must NOT commit a backward version.app.
	respV6, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actV6, Time: time.Now()})
	require.NoError(t, err)
	if respV6.ConsensusParamUpdates != nil && respV6.ConsensusParamUpdates.Version != nil {
		assert.GreaterOrEqual(t, respV6.ConsensusParamUpdates.Version.App, uint64(7),
			"app-v6 activation must not commit a version.app below 7")
	}
	assert.Equal(t, uint64(7), info(),
		"committed version.app must stay 7 — a 7->6 regression would halt the next handshake")
	assert.Equal(t, uint64(7), app.currentAppVersion())

	// 4. The app-v6 FEATURE gate still activated — only the version number was floored.
	assert.Greater(t, app.v8_5AppliedHeight, int64(0),
		"app-v6 feature gate must still activate despite the version floor")
}
