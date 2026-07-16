package abci

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"
)

// TestV83_EndToEnd_PoESignalsAcrossActivation is the v8.3 acceptance test
// (devnet step 6, in-process): it drives the REAL consensus pipeline — a signed
// UpgradePropose for app-v4 → FinalizeBlock activation, then real memory_submit
// + memory_vote txs through processMemorySubmit/processMemoryVote (which run
// checkAndApplyQuorum) — across a pre-fork and a post-fork epoch boundary, and
// asserts the `epoch score computed` log lines flip from Phase-1 accept-ratio /
// zero corroboration to verdict-correctness EWMA / real corroboration.
//
// The captured log lines are echoed via t.Logf so the actual on-node output is
// visible in `go test -v`, exactly what an operator would read on a devnet.
func TestV83_EndToEnd_PoESignalsAcrossActivation(t *testing.T) {
	var logBuf bytes.Buffer
	app := newAppWithLogger(t, zerolog.New(&logBuf))

	// v8.2 is already shipped/active on any chain that reaches v8.3; set its
	// gate directly (proven separately by the v8.2 suite) and activate app-v4
	// below through the real propose→FinalizeBlock path.
	app.v8_2AppliedHeight = 1

	// Four validator agents. Validator ID == hex(pubkey) == ak.id, which is
	// exactly what PublicKeyToAgentID derives from a vote tx's PublicKey.
	vs := make([]agentKey, 4)
	for i := range vs {
		vs[i] = newAgentKey(t)
		require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{ID: vs[i].id, Power: 1}))
	}

	// --- Schedule the app-v4 activation via a real signed UpgradePropose. ---
	proposer := vs[0]
	body := []byte(v8_3UpgradeName)
	pubKey, sig, bodyHash, ts := signAgentProof(t, proposer, body)
	proposeTx := &tx.ParsedTx{
		Type:           tx.TxTypeUpgradePropose,
		Nonce:          1,
		Timestamp:      time.Unix(ts, 0),
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
		UpgradePropose: &tx.UpgradePropose{
			Name:               v8_3UpgradeName,
			TargetAppVersion:   4,
			BinarySHA256:       "deadbeef",
			ProposerID:         proposer.id,
			UpgradeDelayBlocks: defaultUpgradeDelayBlocks,
		},
	}
	require.NoError(t, tx.SignTx(proposeTx, proposer.priv))
	res := app.processTx(proposeTx, 50, time.Unix(ts, 0))
	require.Equal(t, uint32(0), res.Code, "propose app-v4: %s", res.Log)

	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)
	activation := plan.ActivationHeight // 50 + 200 = 250
	require.Equal(t, int64(250), activation)

	// ---------- PRE-FORK phase: all four validators vote ACCEPT. ----------
	// Three memories, unanimous accept → each commits; every validator builds
	// TotalVotes=3 / AcceptVotes=3. Heights are below the 250 activation, so the
	// v8.3 gate is still closed (no verdict crediting).
	for i, mid := range []string{"mem-pre-1", "mem-pre-2", "mem-pre-3"} {
		submitMemory(t, app, proposer, mid, int64(100+i*10))
		for _, v := range vs {
			castVote(t, app, v, mid, true, int64(100+i*10))
		}
		require.Equal(t, string(memory.StatusCommitted), statusOf(t, app, mid),
			"pre-fork memory %s should commit on unanimous accept", mid)
	}

	// Pre-fork epoch boundary at H=200 (200 < 250 → gate closed).
	require.False(t, app.postV8_3Fork(200), "H=200 must be pre-fork")
	logBuf.Reset()
	app.pendingWrites = nil
	app.processEpoch(200, time.Unix(2000, 0))
	preScores := epochScoresFromPending(app)
	t.Logf("── PRE-FORK epoch (H=200) — Phase-1 accept-ratio accuracy, zero corroboration ──")
	logEpochLines(t, &logBuf)

	for _, v := range vs {
		s := preScores[v.id]
		require.NotNil(t, s)
		// 3/3 accept ratio, blended: 0.3*1.0 + 0.7*0.5 = 0.65.
		assert.InDelta(t, 0.65, s.Accuracy, 1e-9, "pre-fork accuracy = accept-ratio blend")
		assert.InDelta(t, 0.0, s.CorrScore, 1e-9, "pre-fork corroboration = 0 (Phase-1 stub)")
	}

	// ---------- Activate app-v4 at H=250 via real FinalizeBlock. ----------
	respAt, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: activation,
		Time:   time.Now(),
	})
	require.NoError(t, err)
	require.NotNil(t, respAt.ConsensusParamUpdates, "activation block must bump app version")
	require.Equal(t, uint64(4), respAt.ConsensusParamUpdates.Version.App)
	require.Equal(t, activation, app.v8_3AppliedHeight, "v8.3 gate must be set at activation")
	require.True(t, app.postV8_3Fork(activation+1), "H>activation must be post-fork")

	// ---------- POST-FORK phase: v0,v1,v2 ACCEPT, v3 dissents (REJECT). ----------
	// Four memories, each committing 3-accept vs 1-reject → v0/v1/v2 match the
	// committed verdict (correct), v3 mismatches every time.
	// Vote order matters: the dissenter (vs[3]) and two accepters vote while the
	// memory is still proposed, then the 3rd accept (vs[2]) is the verdict-reaching
	// vote — so ALL FOUR votes are in the tally when it commits and all four are
	// credited (3 match, the dissenter mismatches). A late vote on an already-
	// committed memory would not be credited (correctly), which is why the
	// committing vote is cast last.
	postMems := []string{"mem-post-1", "mem-post-2", "mem-post-3", "mem-post-4"}
	for i, mid := range postMems {
		h := activation + int64(10+i*10) // 260, 270, 280, 290 — all post-fork
		submitMemory(t, app, proposer, mid, h)
		castVote(t, app, vs[3], mid, false, h) // dissenter, while still proposed
		castVote(t, app, vs[0], mid, true, h)
		castVote(t, app, vs[1], mid, true, h)
		castVote(t, app, vs[2], mid, true, h) // 4th vote present → quorum → all credited
		require.Equal(t, string(memory.StatusCommitted), statusOf(t, app, mid),
			"post-fork memory %s should commit 3-vs-1", mid)
	}

	// Post-fork epoch boundary at H=300 (300 > 250 → gate open).
	logBuf.Reset()
	app.pendingWrites = nil
	app.processEpoch(300, time.Unix(3000, 0))
	postScores := epochScoresFromPending(app)
	t.Logf("── POST-FORK epoch (H=300) — verdict-correctness EWMA, real corroboration ──")
	logEpochLines(t, &logBuf)

	// v0/v1/v2 matched 4 committed verdicts: EWMA over 4×1.0 →
	// 0.4*1.0 + 0.6*0.5 = 0.70; CorroborationScore(4,20) = ln5/ln21 ≈ 0.5286.
	for _, v := range []agentKey{vs[0], vs[1], vs[2]} {
		s := postScores[v.id]
		require.NotNil(t, s)
		assert.InDelta(t, 0.70, s.Accuracy, 1e-9, "post-fork accuracy = verdict-correctness EWMA (4 correct)")
		assert.Greater(t, s.CorrScore, 0.5, "post-fork corroboration ramps with verdict matches")
		assert.InDelta(t, 0.52859, s.CorrScore, 1e-4)
	}
	// v3 dissented on 4 committed memories: EWMA over 4×0.0 → 0.30; corr stays 0.
	s3 := postScores[vs[3].id]
	require.NotNil(t, s3)
	assert.InDelta(t, 0.30, s3.Accuracy, 1e-9, "dissenter's verdict-correctness accuracy drops below the prior")
	assert.InDelta(t, 0.0, s3.CorrScore, 1e-9, "dissenter earns no corroboration")

	// The whole point: post-fork, the consensus-aligned validators out-score the
	// dissenter on accuracy — the opposite of what pre-fork accept-ratio gave a
	// rubber-stamper.
	assert.Greater(t, postScores[vs[0].id].Accuracy, s3.Accuracy,
		"verdict-correctness rewards being right, not just voting accept")
}

// newAppWithLogger builds a SageApp on fresh badger+sqlite with a caller-supplied
// logger so the test can capture `epoch score computed` output.
func newAppWithLogger(t *testing.T, logger zerolog.Logger) *SageApp {
	t.Helper()
	tmp := t.TempDir()
	bs, err := store.NewBadgerStore(filepath.Join(tmp, "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { bs.CloseBadger() })
	sqlite, err := store.NewSQLiteStore(context.Background(), filepath.Join(tmp, "off.db"))
	require.NoError(t, err)
	t.Cleanup(func() { sqlite.Close() })
	app, err := NewSageAppWithStores(bs, sqlite, logger)
	require.NoError(t, err)
	return app
}

// submitMemory drives a real memory_submit through processMemorySubmit (the same
// handler FinalizeBlock dispatches to) into the shared "general" domain.
func submitMemory(t *testing.T, app *SageApp, ak agentKey, memoryID string, height int64) {
	t.Helper()
	submitMemoryDomain(t, app, ak, memoryID, "general", height)
}

// submitMemoryDomain is submitMemory with an explicit domain tag, so v8.4 tests
// can submit into a non-shared domain (where the domain factor engages).
func submitMemoryDomain(t *testing.T, app *SageApp, ak agentKey, memoryID, domain string, height int64) {
	t.Helper()
	body := []byte(memoryID + domain)
	pubKey, sig, bodyHash, ts := signAgentProof(t, ak, body)
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeMemorySubmit,
		MemorySubmit: &tx.MemorySubmit{
			MemoryID:        memoryID,
			MemoryType:      tx.MemoryTypeObservation,
			DomainTag:       domain,
			ConfidenceScore: 0.8,
			Content:         "content-" + memoryID,
		},
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
	}
	r := app.processMemorySubmit(parsed, height, time.Unix(height, 0))
	require.Equal(t, uint32(0), r.Code, "submit %s: %s", memoryID, r.Log)
}

// castVote drives a real memory_vote through processMemoryVote, which calls
// checkAndApplyQuorum (and, post-fork, credits verdict-match).
func castVote(t *testing.T, app *SageApp, ak agentKey, memoryID string, accept bool, height int64) {
	t.Helper()
	decision := tx.VoteDecisionAccept
	if !accept {
		decision = tx.VoteDecisionReject
	}
	parsed := &tx.ParsedTx{
		Type:        tx.TxTypeMemoryVote,
		PublicKey:   ak.pub,
		MemoryVote:  &tx.MemoryVote{MemoryID: memoryID, Decision: decision},
		AgentPubKey: ak.pub,
	}
	r := app.processMemoryVote(parsed, height, time.Unix(height, 0))
	require.Equal(t, uint32(0), r.Code, "vote %s by %s: %s", memoryID, ak.id[:8], r.Log)
}

// logEpochLines parses the captured zerolog JSON buffer and echoes every
// `epoch score computed` line as a readable accuracy/corr summary.
func logEpochLines(t *testing.T, buf *bytes.Buffer) {
	t.Helper()
	for _, line := range bytes.Split(buf.Bytes(), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var m map[string]interface{}
		if err := json.Unmarshal(line, &m); err != nil {
			continue
		}
		if m["message"] != "epoch score computed" {
			continue
		}
		vid, _ := m["validator"].(string)
		short := vid
		if len(short) > 8 {
			short = short[:8]
		}
		t.Logf("  validator=%s accuracy=%.4f domain=%.3f recency=%.4f corr=%.4f raw_weight=%.6f",
			short, m["accuracy"], m["domain"], m["recency"], m["corr"], m["raw_weight"])
	}
}
