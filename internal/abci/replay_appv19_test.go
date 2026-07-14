package abci

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// app-v19 gate parity (v11.8): the EMPTY next-free scaffolding gate that carries
// the local-agents-default-READ flip OFF-consensus (web/handler.go) while adding
// ZERO consensus behavior. Cloned from replay_appv17_test.go, this pins the
// dormant default, the strict-'>' activation boundary, skip-ahead subsumption,
// the version-machinery lockstep (the handshake-halt class of bug — a missing
// currentAppVersion() case ⇒ committed version.app outruns Info()), the
// constructor boot-restore, and the crash-replay version-bump re-emit. It ALSO
// pins the invariant unique to a behavior-empty gate: an ACTIVE app-v19 gate
// commits a BYTE-IDENTICAL AppHash to a dormant one, because no processTx /
// FinalizeBlock branch reads it and it is deliberately NOT OR'd into the
// mutually-exclusive AppHash-rule selectors (postAppV12Rules/postAppV13Rules).

// TestAppV19_ForkGateDefaultsAndSubsumption pins the dormant default, the
// strict-'>' activation boundary, that a live gate subsumes the lower additive
// rules (v8..v17) but pointedly NOT app-v18's admin override, and that the
// version machinery reports 19.
func TestAppV19_ForkGateDefaultsAndSubsumption(t *testing.T) {
	app := setupTestApp(t)
	assert.False(t, app.postAppV19Fork(100), "gate dormant by default (appV19AppliedHeight == 0)")
	assert.Equal(t, uint64(1), app.currentAppVersion(), "no gate ⇒ genesis version 1")

	app.appV19AppliedHeight = 5
	assert.False(t, app.postAppV19Fork(5), "activation block itself is not past-fork (strict >)")
	assert.True(t, app.postAppV19Fork(6), "live at H_act+1")

	assert.True(t, app.postAppV8Rules(6), "app-v19 subsumes app-v8 rules")
	assert.True(t, app.postAppV11Rules(6), "app-v19 subsumes app-v11 rules")
	assert.True(t, app.postAppV15Rules(6), "app-v19 subsumes app-v15 rules")
	assert.True(t, app.postAppV16Rules(6), "app-v19 subsumes app-v16 rules")
	assert.True(t, app.postAppV17Rules(6), "app-v19 subsumes app-v17 rules")
	assert.False(t, app.postAppV18Rules(6), "app-v19 must NOT enable app-v18's live admin RBAC override (behavior-empty gate)")
	assert.Equal(t, uint64(19), app.currentAppVersion(), "app-v19 active ⇒ version 19 (ranks above 18)")
	assert.GreaterOrEqual(t, MaxSupportedAppVersion(), uint64(19), "binary advertises at least the v19 fork gate (no handshake-halt footgun)")
}

// TestAppV19_CanonicalNameAndVersionLockstep couples the activation-name constant
// to tx.CanonicalUpgradeName (the single source of truth plan names derive from)
// and the gate→version arm: a bare chain with only the app-v19 gate set must
// report 19, or FinalizeBlock's committed version.app=19 outruns Info() and the
// next handshake halts on a regression.
func TestAppV19_CanonicalNameAndVersionLockstep(t *testing.T) {
	assert.Equal(t, tx.CanonicalUpgradeName(19), appV19UpgradeName)
	v19 := setupTestApp(t)
	v19.appV19AppliedHeight = 100
	assert.Equal(t, uint64(19), v19.currentAppVersion(), "app-v19 active with no lower gate still reports 19 (top case)")
}

// TestReplayAppV19_SkipAheadSubsumption: a hand-crafted skip to 19 (only
// appV19AppliedHeight set) must keep the app-v8..v11 + v15 + v16 + v17 additive
// rules TRUE while the mutually-exclusive AppHash-replacement helpers (v12/v13)
// stay FALSE — else the FinalizeBlock hash rule is misselected and the AppHash
// diverges. app-v18's admin override is INTENTIONALLY NOT subsumed (app-v19 is
// behavior-empty and must never light up v18). Two replicas agree.
func TestReplayAppV19_SkipAheadSubsumption(t *testing.T) {
	mk := func() *SageApp {
		a := setupTestApp(t)
		a.appV19AppliedHeight = 100 // only the top gate; every lower field stays 0
		return a
	}
	app, replica := mk(), mk()

	assert.True(t, app.postAppV8Rules(101), "v19 subsumes app-v8 rules")
	assert.True(t, app.postAppV9Rules(101), "v19 subsumes app-v9 rules")
	assert.True(t, app.postAppV10Rules(101), "v19 subsumes app-v10 rules")
	assert.True(t, app.postAppV11Rules(101), "v19 subsumes app-v11 rules")
	assert.True(t, app.postAppV15Rules(101), "v19 subsumes app-v15 rules")
	assert.True(t, app.postAppV16Rules(101), "v19 subsumes app-v16 rules")
	assert.True(t, app.postAppV17Rules(101), "v19 subsumes app-v17 rules")

	assert.False(t, app.postAppV18Rules(101), "v19 does NOT imply app-v18's admin override (behavior-empty)")
	assert.False(t, app.postAppV12Rules(101), "v19 does NOT imply the v12 AppHash rule")
	assert.False(t, app.postAppV13Rules(101), "v19 does NOT imply the v13 AppHash rule")

	assert.Equal(t, uint64(19), app.currentAppVersion())
	assert.Equal(t, int64(0), app.v8AppliedHeight, "skip-ahead: PoE ladder untouched")

	for _, h := range []int64{1, 100, 101, 1000} {
		assert.Equalf(t, app.postAppV8Rules(h), replica.postAppV8Rules(h), "replicas agree on v8 rules at %d", h)
		assert.Equalf(t, app.postAppV19Fork(h), replica.postAppV19Fork(h), "replicas agree on v19 gate at %d", h)
	}
}

// TestAppV19_SkipAheadDelegatedProofEnvelopeParity pins the REST/CheckTx-to-
// FinalizeBlock security boundary for a chain that activates app-v19 directly.
// app-v19 subsumes app-v17's delegated-proof hardening even when the v17/v18
// height fields are both zero. The off-consensus constructor therefore must
// include AgentRequest at H_act, before the transaction can execute at H_act+1;
// otherwise CheckTx and consensus disagree and every delegated write is dead.
func TestAppV19_SkipAheadDelegatedProofEnvelopeParity(t *testing.T) {
	const activationHeight int64 = 50
	blockTime := time.Now().Truncate(time.Second)
	request := []byte("POST /v1/domain/register\n{\"name\":\"v19-skip-ahead\"}")
	agent := newAgentKey(t)
	outer := newAgentKey(t)

	newV19OnlyApp := func() *SageApp {
		t.Helper()
		app := setupTestApp(t)
		app.appV19AppliedHeight = activationHeight
		require.Zero(t, app.appV17AppliedHeight)
		require.Zero(t, app.appV18AppliedHeight)
		return app
	}

	app := newV19OnlyApp()
	app.state.Height = activationHeight - 1
	assert.False(t, app.IsAppV17ActiveForNextTx(), "before activation, REST must preserve the historical envelope")
	app.state.Height = activationHeight
	require.True(t, app.IsAppV17ActiveForNextTx(), "at activation commit, REST must construct the envelope required at H+1")

	withEnvelope := makeDelegatedDomainRegisterTx(
		t, agent, outer, request, blockTime, "v19-skip-ahead", "", true,
	)
	rawWithEnvelope, err := tx.EncodeTx(withEnvelope)
	require.NoError(t, err)

	check, err := app.CheckTx(context.Background(), &abcitypes.RequestCheckTx{Tx: rawWithEnvelope})
	require.NoError(t, err)
	require.Equal(t, uint32(0), check.Code, check.Log)

	finalized, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: activationHeight + 1,
		Time:   blockTime,
		Txs:    [][]byte{rawWithEnvelope},
	})
	require.NoError(t, err)
	require.Len(t, finalized.TxResults, 1)
	require.Equal(t, uint32(0), finalized.TxResults[0].Code, finalized.TxResults[0].Log)

	// The same v19-only state must fail closed when a delegated constructor omits
	// AgentRequest: both mempool admission and deterministic execution reject it.
	missingEnvelopeApp := newV19OnlyApp()
	missingEnvelopeApp.state.Height = activationHeight
	missingEnvelope := makeDelegatedDomainRegisterTx(
		t, agent, outer, request, blockTime, "v19-missing-envelope", "", false,
	)
	rawMissingEnvelope, err := tx.EncodeTx(missingEnvelope)
	require.NoError(t, err)

	check, err = missingEnvelopeApp.CheckTx(context.Background(), &abcitypes.RequestCheckTx{Tx: rawMissingEnvelope})
	require.NoError(t, err)
	assert.Equal(t, uint32(109), check.Code, check.Log)
	assert.Contains(t, check.Log, "missing its signed request")

	finalized, err = missingEnvelopeApp.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: activationHeight + 1,
		Time:   blockTime,
		Txs:    [][]byte{rawMissingEnvelope},
	})
	require.NoError(t, err)
	require.Len(t, finalized.TxResults, 1)
	assert.Equal(t, uint32(109), finalized.TxResults[0].Code, finalized.TxResults[0].Log)
	assert.Contains(t, finalized.TxResults[0].Log, "missing its signed request")
}

// TestReplayAppV19_BootRefreshThroughConstructor: a node restarting on a
// post-app-v19 chain restores the activation height through the REAL constructor
// path (refreshAppV19Fork) and reports version 19.
func TestReplayAppV19_BootRefreshThroughConstructor(t *testing.T) {
	bs := setupTestBadger(t)
	dbPath := filepath.Join(t.TempDir(), "appv19-boot-refresh.db")
	sqlite, err := store.NewSQLiteStore(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { sqlite.Close() })

	require.NoError(t, bs.MarkUpgradeApplied(appV19UpgradeName, 19, 4200))

	app, err := NewSageAppWithStores(bs, sqlite, zerolog.Nop())
	require.NoError(t, err)
	assert.Equal(t, int64(4200), app.appV19AppliedHeight, "constructor must restore the app-v19 activation height")
	assert.Equal(t, uint64(19), app.currentAppVersion())
	assert.False(t, app.postAppV19Fork(4200), "gate dormant at the activation block (strict >)")
	assert.True(t, app.postAppV19Fork(4201), "gate live at H_act+1")
}

// TestReplayAppV19_CrashReplayReEmitsVersionBump drives the REAL propose→activate
// path and the crash-before-Commit replay: the FinalizeBlock activation arm sets
// the height in-memory and (both original and replay) re-emits
// ConsensusParamUpdates version.app=19 from the audit trail — the arm the
// manual-height unit tests bypass.
func TestReplayAppV19_CrashReplayReEmitsVersionBump(t *testing.T) {
	app := setupTestApp(t)
	proposer := deterministicAgentKey(t)
	ptx := makeUpgradeProposeTx(t, proposer, appV19UpgradeName, 19, "feed", 0)
	require.Equal(t, uint32(0), app.processUpgradePropose(ptx, 11, time.Unix(0, 0)).Code)
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	actH := plan.ActivationHeight

	respOrig, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actH, Time: time.Now()})
	require.NoError(t, err)
	require.NotNil(t, respOrig.ConsensusParamUpdates)
	require.Equal(t, uint64(19), respOrig.ConsensusParamUpdates.Version.App)
	assert.Equal(t, actH, app.appV19AppliedHeight, "activation sets the app-v19 height in memory")

	respReplay, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actH, Time: time.Now()})
	require.NoError(t, err)
	require.NotNil(t, respReplay.ConsensusParamUpdates, "replayed activation must re-emit the version bump from the audit trail")
	assert.Equal(t, uint64(19), respReplay.ConsensusParamUpdates.Version.App)

	respOther, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actH + 1, Time: time.Now()})
	require.NoError(t, err)
	assert.Nil(t, respOther.ConsensusParamUpdates)
}

// TestReplayAppV19_DormantGateByteIdenticalAppHash is the replay-safety linchpin
// for the new binary on an unactivated chain. Every chain in existence today has
// appV19AppliedHeight==0, so the gate is dormant (postAppV19Fork false at every
// height) and every OR-chained lower rules helper collapses to EXACTLY its
// pre-v19 value — the added `|| postAppV19Fork(h)` term is inert. Committing the
// SAME establishing tx at the same height/time on two replicas of that dormant
// chain must therefore yield a byte-identical AppHash: deploying the new binary
// replays historical blocks unchanged. (app-v19's ONLY behavioral effect lives
// OFF-consensus in web/handler.go once governance activates it; the consensus
// path adds no new rule — activation merely turns on the SAME additive rules a
// laddered chain already carried, verified by the subsumption test above.)
func TestReplayAppV19_DormantGateByteIdenticalAppHash(t *testing.T) {
	blockTime := time.Now().Truncate(time.Second)
	// Fixed key material + a single pre-encoded tx reused across replicas so the
	// committed BadgerDB state (hence the AppHash) is a pure function of the gate
	// state under test.
	regTx := encodeSelfSignedRegister(t, deterministicAgentKey(t), "operator")

	commit := func() []byte {
		app := setupTestApp(t)
		// appV19AppliedHeight stays 0 — the dormant gate on every existing chain.
		require.Equal(t, int64(0), app.appV19AppliedHeight)
		require.False(t, app.postAppV19Fork(5), "dormant gate: the added OR term is inert on this chain")
		resp, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
			Height: 5, Time: blockTime, Txs: [][]byte{regTx},
		})
		require.NoError(t, err)
		require.Len(t, resp.TxResults, 1)
		require.Equal(t, uint32(0), resp.TxResults[0].Code, resp.TxResults[0].Log)
		return resp.AppHash
	}

	assert.Equal(t, commit(), commit(), "the new binary replays an appV19AppliedHeight==0 chain byte-identically across replicas")
}
