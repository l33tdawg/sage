package abci

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"
)

// app-v8 replay parity. app-v8 routes UpgradePropose through governance once
// active, but ships DORMANT (appV8AppliedHeight==0 on every existing chain), so
// the only consensus change visible to a historical block is gated behind
// postAppV8Fork. These tests pin:
//   A1 PreForkProposeByteIdentical — with the gate OFF, a propose self-activates
//      a plan exactly as before app-v8, and two independent runs produce a
//      byte-identical AppHash (the historical-block replay guarantee).
//   A2 PostForkRejectInertAcceptMoves — post-fork, a REJECTED propose (admin
//      gate) writes nothing (AppHash unchanged); an ACCEPTED propose moves the
//      AppHash only by the governance keys it writes, and re-reading is
//      deterministic.

// TestReplayAppV8_A1_PreForkProposeByteIdentical drives the SAME pre-fork
// propose through two fresh apps with the app-v8 gate OFF and asserts the
// post-propose AppHash is byte-identical — a historical block replays the
// unchanged self-activating path identically on every node.
func TestReplayAppV8_A1_PreForkProposeByteIdentical(t *testing.T) {
	run := func() []byte {
		app := buildPreForkApp(t)
		require.Equal(t, int64(0), app.appV8AppliedHeight, "precondition: app-v8 gate dormant")

		// Fixed proposer key so both runs persist a byte-identical plan record.
		fixed := deterministicAgentKey(t)
		ptx := makeUpgradeProposeTx(t, fixed, "app-v8", 8, "deadbeef", 200)
		result := app.processUpgradePropose(ptx, 100, time.Unix(0, 0))
		require.Equal(t, uint32(0), result.Code, "pre-fork propose self-activates: %s", result.Log)

		h, err := ComputeAppHash(app.badgerStore)
		require.NoError(t, err)
		return h
	}

	h1 := run()
	h2 := run()
	assert.Equal(t, h1, h2, "pre-fork app-v8 propose must replay to a byte-identical AppHash")
}

// TestReplayAppV8_A2_PostForkRejectInertAcceptMoves asserts post-fork:
//   - a rejected (non-admin) propose leaves ComputeAppHash byte-identical;
//   - an accepted (admin, canonical, forward) propose moves the hash by the
//     governance keys it writes, and re-reading is deterministic.
func TestReplayAppV8_A2_PostForkRejectInertAcceptMoves(t *testing.T) {
	app := buildPreForkApp(t)
	activateV85(app, 5)
	app.appV8AppliedHeight = 5 // post-fork for height > 5
	require.Equal(t, uint64(8), app.currentAppVersion())

	// Register an admin validator (the only one allowed to propose post-fork).
	admin := newAgentKey(t)
	registerAgent(t, app, admin, "admin-agent", "admin")
	require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{ID: admin.id, PublicKey: admin.pub, Power: 10}))
	require.NoError(t, app.badgerStore.SaveValidators(map[string]int64{admin.id: 10}))

	hBefore, err := ComputeAppHash(app.badgerStore)
	require.NoError(t, err)

	// Rejected: a non-admin proposer → admin gate → no write.
	member := newAgentKey(t)
	registerAgent(t, app, member, "member-agent", "member")
	hAfterReg, err := ComputeAppHash(app.badgerStore)
	require.NoError(t, err)

	badTx := makeUpgradeProposeTx(t, member, "app-v9", 9, "", 200)
	require.NoError(t, tx.SignTx(badTx, member.priv))
	require.Equal(t, uint32(47), app.processUpgradePropose(badTx, 10, time.Now()).Code)
	hAfterReject, err := ComputeAppHash(app.badgerStore)
	require.NoError(t, err)
	assert.Equal(t, hAfterReg, hAfterReject, "rejected (non-admin) propose must not move the AppHash")

	// Accepted: admin propose creates a governance proposal → AppHash moves.
	goodTx := makeUpgradeProposeTx(t, admin, "app-v9", 9, "feed", 200)
	require.NoError(t, tx.SignTx(goodTx, admin.priv))
	require.Equal(t, uint32(0), app.processUpgradePropose(goodTx, 10, time.Now()).Code)
	hAfterAccept, err := ComputeAppHash(app.badgerStore)
	require.NoError(t, err)
	assert.NotEqual(t, hAfterReject, hAfterAccept, "accepted propose must write governance keys and move the AppHash")
	_ = hBefore

	// Deterministic re-read over the post-propose keyspace.
	hReplay, err := ComputeAppHash(app.badgerStore)
	require.NoError(t, err)
	assert.Equal(t, hAfterAccept, hReplay, "ComputeAppHash must be deterministic after the governance write")
}
