package abci

import (
	"crypto/ed25519"
	"encoding/hex"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// app-v6 (v8.5) replay parity. The three app-v6 changes are pure tx-admission
// predicates / reject paths — app-v6 writes NO new on-chain keys. So the only
// observable post-fork delta is a result CODE/LOG on txs that, by construction,
// only exist in blocks that did not exist before app-v6. These tests pin:
//   R1 NonCanonicalProposeByteIdentical — a pre-fork non-canonical propose is
//      accepted (Code 0) and its AppHash effect is byte-identical across two
//      fresh-app runs (the historical-block replay guarantee).
//   R2 ProposeGuardNoAppHashDelta — post-fork, a REJECTED propose leaves the
//      AppHash unchanged; an ACCEPTED propose moves it only by the upgrade:plan
//      key. ComputeAppHash is deterministic on re-read.
//   R3 RevertRejectIsAppHashInert — post-fork revert reject writes nothing.
//   R4 PreForkRevertByteIdentical — pre-fork revert stubs are AppHash-inert and
//      return the identical Code 0 on every run (pre/post code parity anchor).

// buildPreForkApp returns a fresh app with the app-v6 gate OFF (v8_5AppliedHeight
// == 0) — the state of every chain that exists today.
func buildPreForkApp(t *testing.T) *SageApp {
	return newAppWithLogger(t, zerolog.Nop())
}

// deterministicAgentKey returns the SAME Ed25519 keypair on every call, so a
// persisted plan/audit record (which embeds the proposer's hex pubkey) is
// byte-identical across two independent replay runs. The seed is fixed.
func deterministicAgentKey(t *testing.T) agentKey {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(i + 1) // any fixed pattern; must be stable across runs
	}
	priv := ed25519.NewKeyFromSeed(seed)
	pub := priv.Public().(ed25519.PublicKey)
	return agentKey{pub: pub, priv: priv, id: hex.EncodeToString(pub)}
}

// TestReplayV8_5_R1_NonCanonicalProposeByteIdentical drives the SAME pre-fork
// non-canonical propose through two independent fresh apps and asserts the
// post-propose AppHash is byte-identical — a historical block accepting a
// non-canonical name replays the same on both nodes. Also asserts Code 0
// (the pre-fork leniency the rule must never retroactively reclassify).
func TestReplayV8_5_R1_NonCanonicalProposeByteIdentical(t *testing.T) {
	run := func() []byte {
		app := buildPreForkApp(t)
		require.Equal(t, int64(0), app.v8_5AppliedHeight, "precondition: pre-fork gate")

		// Non-canonical name (would be rejected post-fork) accepted here. Use a
		// FIXED proposer key so both runs persist an identical plan record.
		fixed := deterministicAgentKey(t)
		ptx := makeUpgradeProposeTx(t, fixed, "v8.5.0", 6, "deadbeef", 200)
		result := app.processUpgradePropose(ptx, 100, time.Unix(0, 0))
		require.Equal(t, uint32(0), result.Code, "pre-fork non-canonical accepted: %s", result.Log)

		h, err := ComputeAppHash(app.badgerStore)
		require.NoError(t, err)
		return h
	}

	h1 := run()
	h2 := run()
	assert.Equal(t, h1, h2, "pre-fork non-canonical propose must replay to a byte-identical AppHash")
}

// TestReplayV8_5_R2_ProposeGuardNoAppHashDelta asserts post-fork:
//   - a rejected (non-canonical) propose leaves ComputeAppHash byte-identical;
//   - an accepted (canonical, forward) propose moves the hash (the upgrade:plan
//     key) and re-reading is deterministic.
func TestReplayV8_5_R2_ProposeGuardNoAppHashDelta(t *testing.T) {
	app := buildPreForkApp(t)
	ak := newAgentKey(t)
	activateV85(app, 50) // gate at 50, cur=6, post-fork for height>50

	hBefore, err := ComputeAppHash(app.badgerStore)
	require.NoError(t, err)

	// Rejected propose (non-canonical) → no AppHash delta.
	bad := makeUpgradeProposeTx(t, ak, "v8.5.0", 7, "", 200)
	require.Equal(t, uint32(47), app.processUpgradePropose(bad, 100, time.Now()).Code)
	hAfterReject, err := ComputeAppHash(app.badgerStore)
	require.NoError(t, err)
	assert.Equal(t, hBefore, hAfterReject, "rejected propose must not move the AppHash")

	// Rejected propose (regression) → still no AppHash delta.
	bad2 := makeUpgradeProposeTx(t, ak, "app-v5", 5, "", 200)
	require.Equal(t, uint32(47), app.processUpgradePropose(bad2, 100, time.Now()).Code)
	hAfterReject2, err := ComputeAppHash(app.badgerStore)
	require.NoError(t, err)
	assert.Equal(t, hBefore, hAfterReject2, "rejected regression propose must not move the AppHash")

	// Accepted propose (canonical, forward) → AppHash moves by upgrade:plan.
	good := makeUpgradeProposeTx(t, ak, "app-v7", 7, "", 200)
	require.Equal(t, uint32(0), app.processUpgradePropose(good, 100, time.Now()).Code)
	hAfterAccept, err := ComputeAppHash(app.badgerStore)
	require.NoError(t, err)
	assert.NotEqual(t, hBefore, hAfterAccept, "accepted propose must write upgrade:plan and move the AppHash")

	// Deterministic re-read.
	hReplay, err := ComputeAppHash(app.badgerStore)
	require.NoError(t, err)
	assert.Equal(t, hAfterAccept, hReplay, "ComputeAppHash must be deterministic over the post-propose keyspace")
}

// TestReplayV8_5_R3_RevertRejectIsAppHashInert asserts a post-fork revert reject
// writes nothing: the AppHash equals the pre-reject digest, and re-reading is
// deterministic — replaying the revert-reject block reproduces the same hash.
func TestReplayV8_5_R3_RevertRejectIsAppHashInert(t *testing.T) {
	app := buildPreForkApp(t)
	ak := newAgentKey(t)
	activateV85(app, 1) // post-fork for height>1

	hBefore, err := ComputeAppHash(app.badgerStore)
	require.NoError(t, err)

	revert := makeUpgradeRevertTx(t, ak, "app-v6", 6, 150)
	require.Equal(t, uint32(90), app.processUpgradeRevert(revert, 200, time.Now()).Code)

	hAfter, err := ComputeAppHash(app.badgerStore)
	require.NoError(t, err)
	assert.Equal(t, hBefore, hAfter, "post-fork revert reject must be AppHash-inert")

	hReplay, err := ComputeAppHash(app.badgerStore)
	require.NoError(t, err)
	assert.Equal(t, hAfter, hReplay, "ComputeAppHash must be deterministic after a revert reject")
}

// TestReplayV8_5_R4_PreForkRevertByteIdentical asserts the pre-fork revert stub
// is AppHash-inert and returns the identical Code 0 on every fresh run — the
// pre/post-fork code-parity anchor (pre-fork ⇒ Code 0 stub, never Code 90).
func TestReplayV8_5_R4_PreForkRevertByteIdentical(t *testing.T) {
	run := func() (uint32, []byte) {
		app := buildPreForkApp(t)
		fixed := deterministicAgentKey(t)
		require.Equal(t, int64(0), app.v8_5AppliedHeight, "precondition: pre-fork gate")

		hBefore, err := ComputeAppHash(app.badgerStore)
		require.NoError(t, err)

		revert := makeUpgradeRevertTx(t, fixed, "v7.4.0-recovery", 6, 12345)
		result := app.processUpgradeRevert(revert, 100, time.Unix(0, 0))

		hAfter, err := ComputeAppHash(app.badgerStore)
		require.NoError(t, err)
		assert.Equal(t, hBefore, hAfter, "pre-fork revert stub must be AppHash-inert")
		return result.Code, hAfter
	}

	c1, h1 := run()
	c2, h2 := run()
	assert.Equal(t, uint32(0), c1, "pre-fork revert returns the Code-0 stub, never Code 90")
	assert.Equal(t, c1, c2, "pre-fork revert code must be identical across runs")
	assert.Equal(t, h1, h2, "pre-fork revert AppHash must be byte-identical across runs")
}
