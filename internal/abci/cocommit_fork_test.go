package abci

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/tx"
)

// ---------------------------------------------------------------------------
// co-commit (v11 / app-v15): tx 31 CoCommitSubmit + tx 32 CoCommitAttest.
// A jointly-signed envelope is committed NATIVELY to each chain as a local memory
// keyed by the content-derived, height-free SharedID; peers cross-anchor via
// signed CommitReceipts. Both txs are dual-gated on postAppV15Fork (byte-identical
// pre-activation). The LOCAL submitter must be one of the coauthors; foreign
// coauthors are verified by standalone ed25519 signature only. A co-commit is
// COMMITTED immediately (block inclusion is decisive), never routed through the
// content-quality voter.
// ---------------------------------------------------------------------------

type testCoauthor struct {
	pub   ed25519.PublicKey
	priv  ed25519.PrivateKey
	chain string
}

func genTestCoauthor(t *testing.T, chain string) testCoauthor {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return testCoauthor{pub: pub, priv: priv, chain: chain}
}

// buildCoCommitEnvelope builds a fully-signed 2-coauthor envelope where `local`
// is a coauthor (the realistic dual-commit model) plus one foreign coauthor on
// `foreignChain`. Returns the envelope and the foreign coauthor (whose key signs
// the peer receipt in attest tests).
func buildCoCommitEnvelope(t *testing.T, local agentKey, domain string, nonce []byte, foreignChain string) (*tx.CoCommitSubmit, testCoauthor) {
	t.Helper()
	foreign := genTestCoauthor(t, foreignChain)
	ch := sha256.Sum256([]byte("co-committed content " + domain))
	env := &tx.CoCommitSubmit{
		SchemaVersion:   1,
		ContentHash:     ch[:],
		MemoryType:      tx.MemoryTypeFact,
		Domain:          domain,
		Classification:  tx.ClearanceInternal,
		ConfidenceScore: 0.9,
		CreatedAtUnix:   1_700_000_000,
		AgreementNonce:  nonce,
		Coauthors: []tx.CoCommitCoauthor{
			{PubKey: local.pub, ChainID: "sage-local"},
			{PubKey: foreign.pub, ChainID: foreign.chain},
		},
	}
	core := tx.CanonicalCoreBytes(env)
	env.Coauthors[0].Sig = ed25519.Sign(local.priv, core)
	env.Coauthors[1].Sig = ed25519.Sign(foreign.priv, core)
	env.SharedID = tx.ComputeSharedID(tx.CoreHashOf(env), env.Coauthors, nonce)
	return env, foreign
}

func coCommitSubmitTx(t *testing.T, local agentKey, env *tx.CoCommitSubmit) *tx.ParsedTx {
	t.Helper()
	pubKey, sig, bodyHash, ts := signAgentProof(t, local, []byte(env.SharedID))
	return &tx.ParsedTx{
		Type:           tx.TxTypeCoCommitSubmit,
		CoCommitSubmit: env,
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
	}
}

// signedReceiptAttest builds a CoCommitAttest whose receipt is signed by `signer`
// and advertised as coming from chain `peerChain` with pubkey `peerPub`.
func signedReceiptAttest(sharedID, peerChain string, peerPub ed25519.PublicKey, signer ed25519.PrivateKey, coreHash []byte) *tx.ParsedTx {
	receipt := &tx.CommitReceipt{
		ChainID: peerChain, SharedID: sharedID, LocalMemID: "peer-mem-1",
		Height: 7, CommitTime: 1_700_000_500, CoreHash: coreHash,
	}
	rbytes := tx.EncodeCommitReceipt(receipt)
	return &tx.ParsedTx{
		Type: tx.TxTypeCoCommitAttest,
		CoCommitAttest: &tx.CoCommitAttest{
			SharedID: sharedID, PeerChainID: peerChain, PeerPubKey: peerPub,
			Receipt: rbytes, PeerSig: ed25519.Sign(signer, rbytes),
			CommitTime: receipt.CommitTime, CoreHash: coreHash,
		},
	}
}

// TestCoCommit_DualGatePreFork: pre-activation, the exec gate rejects both new tx
// types with Code 10 and writes NOTHING (byte-identical AppHash).
func TestCoCommit_DualGatePreFork(t *testing.T) {
	app := setupTestApp(t) // app-v15 dormant
	require.Equal(t, int64(0), app.appV15AppliedHeight)
	local := newAgentKey(t)
	env, _ := buildCoCommitEnvelope(t, local, "family.photos", []byte("n1"), "sage-b")

	before, err := ComputeAppHash(app.badgerStore)
	require.NoError(t, err)

	sub := app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, time.Now())
	assert.Equal(t, uint32(10), sub.Code, "pre-fork submit rejected as unknown tx")

	att := app.processCoCommitAttest(&tx.ParsedTx{Type: tx.TxTypeCoCommitAttest, CoCommitAttest: &tx.CoCommitAttest{SharedID: env.SharedID}}, 10, time.Now())
	assert.Equal(t, uint32(10), att.Code, "pre-fork attest rejected as unknown tx")

	after, err := ComputeAppHash(app.badgerStore)
	require.NoError(t, err)
	assert.Equal(t, before, after, "pre-fork rejects write no key — AppHash byte-identical")
	core, _ := app.badgerStore.GetCoCommitCore(env.SharedID)
	assert.Nil(t, core, "no cocommit:core written pre-fork")
}

// TestCoCommit_SubmitPostFork: a valid 2-coauthor envelope becomes a native local
// memory keyed by SharedID, COMMITTED immediately, with the anchor keys and
// local-submitter author.
func TestCoCommit_SubmitPostFork(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5
	local := newAgentKey(t)
	env, _ := buildCoCommitEnvelope(t, local, "family.photos", []byte("n1"), "sage-b")

	res := app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, time.Now())
	require.Equal(t, uint32(0), res.Code, "submit: %s", res.Log)
	assert.Equal(t, env.SharedID, string(res.Data))

	_, st, err := app.badgerStore.GetMemoryHash(env.SharedID)
	require.NoError(t, err)
	assert.Equal(t, string(memory.StatusCommitted), st, "co-commit is committed immediately (never routed through the voter)")

	author, _ := app.badgerStore.GetMemoryAuthor(env.SharedID)
	assert.Equal(t, local.id, author, "memauthor = LOCAL submitter")

	core, _ := app.badgerStore.GetCoCommitCore(env.SharedID)
	assert.Equal(t, tx.CoreHashOf(env), core, "cocommit:core = CoreHashOf(envelope)")

	dom, _ := app.badgerStore.GetMemoryDomain(env.SharedID)
	assert.Equal(t, "family.photos", dom)

	// M1: the auto-registered owner holds a level-2 self-grant (not locked out).
	ok, err := app.badgerStore.HasAccessOrAncestor("family.photos", local.id, 2, time.Now())
	require.NoError(t, err)
	assert.True(t, ok, "auto-registered owner has a level-2 self-grant")
}

// TestCoCommit_SubmitterNotCoauthorRejected (P1): a submitter who is not one of
// the coauthors is rejected.
func TestCoCommit_SubmitterNotCoauthorRejected(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5
	alice := newAgentKey(t)
	env, _ := buildCoCommitEnvelope(t, alice, "d", []byte("n"), "sage-b")

	bob := newAgentKey(t) // not a coauthor
	res := app.processCoCommitSubmit(coCommitSubmitTx(t, bob, env), 10, time.Now())
	assert.Equal(t, uint32(95), res.Code, "non-coauthor submitter rejected")
}

// TestCoCommit_SchemaVersionRejected (P2): an unsupported schema version is rejected.
func TestCoCommit_SchemaVersionRejected(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5
	local := newAgentKey(t)
	env, _ := buildCoCommitEnvelope(t, local, "d", []byte("n"), "sage-b")
	env.SchemaVersion = 2
	// Re-sign for the new core (SchemaVersion is in the signed core) so we test the
	// version gate, not a signature failure.
	core := tx.CanonicalCoreBytes(env)
	env.Coauthors[0].Sig = ed25519.Sign(local.priv, core)
	// coauthor[1] sig now stale, but the version check precedes sig verification.
	res := app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, time.Now())
	assert.Equal(t, uint32(95), res.Code, "unsupported schema version rejected")
	assert.Contains(t, res.Log, "schema version")
}

// TestCoCommit_SharedIDMismatchRejected (Code 96): a SharedID not derivable from
// the signed core is rejected.
func TestCoCommit_SharedIDMismatchRejected(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5
	local := newAgentKey(t)
	env, _ := buildCoCommitEnvelope(t, local, "d", []byte("n"), "sage-b")
	env.SharedID = "deadbeefdeadbeef" // tamper (after sigs + P1 pass)

	res := app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, time.Now())
	assert.Equal(t, uint32(96), res.Code, "tampered SharedID rejected")
}

// TestCoCommit_BadCoauthorSigRejected (Code 95): a corrupted coauthor signature.
func TestCoCommit_BadCoauthorSigRejected(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5
	local := newAgentKey(t)
	env, _ := buildCoCommitEnvelope(t, local, "d", []byte("n"), "sage-b")
	env.Coauthors[1].Sig[0] ^= 0xff // corrupt the foreign coauthor's sig

	res := app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, time.Now())
	assert.Equal(t, uint32(95), res.Code, "corrupted coauthor sig rejected")
}

// TestCoCommit_AttestPostFork: a receipt signed by a DECLARED coauthor for its
// chain is recorded as a cross-anchor.
func TestCoCommit_AttestPostFork(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5
	local := newAgentKey(t)
	env, foreign := buildCoCommitEnvelope(t, local, "family.photos", []byte("n1"), "sage-b")
	require.Equal(t, uint32(0), app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, time.Now()).Code)

	// The peer IS the foreign coauthor (key + chain match the recorded set).
	att := signedReceiptAttest(env.SharedID, foreign.chain, foreign.pub, foreign.priv, tx.CoreHashOf(env))
	res := app.processCoCommitAttest(att, 11, time.Now())
	assert.Equal(t, uint32(0), res.Code, "attest by a declared coauthor: %s", res.Log)
}

// TestCoCommit_AttestForgedPeerRejected (H2): a receipt signed over the PUBLIC
// CoreHash with a key that is NOT a recorded coauthor is rejected — no forged
// cross-anchor.
func TestCoCommit_AttestForgedPeerRejected(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5
	local := newAgentKey(t)
	env, _ := buildCoCommitEnvelope(t, local, "family.photos", []byte("n1"), "sage-b")
	require.Equal(t, uint32(0), app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, time.Now()).Code)

	// Attacker: fresh key, correct public CoreHash, claims to be "sage-b".
	fakePub, fakePriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	att := signedReceiptAttest(env.SharedID, "sage-b", fakePub, fakePriv, tx.CoreHashOf(env))
	res := app.processCoCommitAttest(att, 11, time.Now())
	assert.Equal(t, uint32(95), res.Code, "forged-peer attest rejected (key not a recorded coauthor)")

	anchor, _ := app.badgerStore.GetCoCommitCore(env.SharedID) // core exists; anchor must NOT
	require.NotNil(t, anchor)
}

// TestCoCommit_AttestFailClosed (Code 97): an attest for a SharedID this chain
// never co-committed is rejected (fail-closed, no anchor).
func TestCoCommit_AttestFailClosed(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5

	peerPub, peerPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	att := signedReceiptAttest("unknown-shared", "sage-b", peerPub, peerPriv, []byte("x"))
	res := app.processCoCommitAttest(att, 10, time.Now())
	assert.Equal(t, uint32(97), res.Code, "attest with no local co-commit is fail-closed")
}

// TestCoCommit_CheckTxDualGate (L1): the load-bearing CheckTx gate returns Code 10
// pre-fork (keeps type 31/32 out of the mempool) and admits post-fork.
func TestCoCommit_CheckTxDualGate(t *testing.T) {
	app := setupTestApp(t)
	local := newAgentKey(t)
	registerAgent(t, app, local, "local", "member")
	env, _ := buildCoCommitEnvelope(t, local, "family.photos", []byte("n1"), "sage-b")

	ptx := coCommitSubmitTx(t, local, env)
	require.NoError(t, tx.SignTx(ptx, local.priv))
	encoded, err := tx.EncodeTx(ptx)
	require.NoError(t, err)

	// Pre-fork: gate rejects.
	app.state.Height = 0
	resp, err := app.CheckTx(context.TODO(), &abcitypes.RequestCheckTx{Tx: encoded})
	require.NoError(t, err)
	assert.Equal(t, uint32(10), resp.Code, "pre-fork CheckTx rejects co-commit with Code 10")
	assert.Contains(t, resp.Log, "unknown tx type")

	// Post-fork: gate admits.
	app.appV15AppliedHeight = 5
	app.state.Height = 100
	resp2, err := app.CheckTx(context.TODO(), &abcitypes.RequestCheckTx{Tx: encoded})
	require.NoError(t, err)
	assert.NotEqual(t, uint32(10), resp2.Code, "post-fork CheckTx admits co-commit (gate passed): %s", resp2.Log)
}

// TestCoCommit_ReclaimsFrontRunSquat (#3): a co-commit reclaims its cryptographic
// SharedID slot from a front-run normal-memory squat (denial defeated + author
// corrected), and a later normal submit can no longer clobber the committed
// co-commit.
func TestCoCommit_ReclaimsFrontRunSquat(t *testing.T) {
	app := setupTestApp(t)
	app.appV10AppliedHeight = 1 // so the squat records an author
	app.appV15AppliedHeight = 1
	local := newAgentKey(t)
	env, _ := buildCoCommitEnvelope(t, local, "family.photos", []byte("n1"), "sage-b")

	// Attacker front-runs a normal memory into memory:<sharedID> (shared domain).
	attacker := newAgentKey(t)
	squat := makeMemorySubmitTx(t, attacker, "general", "attacker squat content long enough to pass")
	squat.MemorySubmit.MemoryID = env.SharedID
	require.Equal(t, uint32(0), app.processMemorySubmit(squat, 5, time.Now()).Code, "squat submit")
	a0, _ := app.badgerStore.GetMemoryAuthor(env.SharedID)
	require.Equal(t, attacker.id, a0, "precondition: squatter is recorded author")

	// The co-commit RECLAIMS its slot.
	res := app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, time.Now())
	require.Equal(t, uint32(0), res.Code, "co-commit reclaims squatted slot: %s", res.Log)
	_, st, _ := app.badgerStore.GetMemoryHash(env.SharedID)
	assert.Equal(t, string(memory.StatusCommitted), st)
	author, _ := app.badgerStore.GetMemoryAuthor(env.SharedID)
	assert.Equal(t, local.id, author, "reclaim overwrites the squatter's author")
	core, _ := app.badgerStore.GetCoCommitCore(env.SharedID)
	assert.Equal(t, tx.CoreHashOf(env), core)

	// A later normal submit can no longer clobber the co-commit.
	squat2 := makeMemorySubmitTx(t, attacker, "general", "another attempt to clobber the cocommit id")
	squat2.MemorySubmit.MemoryID = env.SharedID
	res2 := app.processMemorySubmit(squat2, 11, time.Now())
	assert.Equal(t, uint32(11), res2.Code, "normal submit cannot overwrite a committed co-commit")
}

// TestCoCommit_ReSubmitRejected (#3): a genuine co-commit re-submit is rejected.
func TestCoCommit_ReSubmitRejected(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 1
	local := newAgentKey(t)
	env, _ := buildCoCommitEnvelope(t, local, "family.photos", []byte("n1"), "sage-b")
	require.Equal(t, uint32(0), app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, time.Now()).Code)
	res := app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 11, time.Now())
	assert.Equal(t, uint32(98), res.Code, "re-submit of an already-committed co-commit rejected")
}

// TestCoCommit_TooManyCoauthorsRejected (#5): the coauthor-count cap.
func TestCoCommit_TooManyCoauthorsRejected(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5
	local := newAgentKey(t)
	ch := sha256.Sum256([]byte("many"))
	env := &tx.CoCommitSubmit{
		SchemaVersion: 1, ContentHash: ch[:], MemoryType: tx.MemoryTypeFact,
		Domain: "d", Classification: tx.ClearanceInternal, ConfidenceScore: 0.5,
		CreatedAtUnix: 1, AgreementNonce: []byte("n"),
		Coauthors: []tx.CoCommitCoauthor{{PubKey: local.pub, ChainID: "sage-local"}},
	}
	privs := []ed25519.PrivateKey{local.priv}
	for i := 0; i < maxCoCommitCoauthors; i++ { // local + 64 = 65 > 64
		p, s, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		env.Coauthors = append(env.Coauthors, tx.CoCommitCoauthor{PubKey: p, ChainID: fmt.Sprintf("sage-%d", i)})
		privs = append(privs, s)
	}
	core := tx.CanonicalCoreBytes(env)
	for i := range env.Coauthors {
		env.Coauthors[i].Sig = ed25519.Sign(privs[i], core)
	}
	env.SharedID = tx.ComputeSharedID(tx.CoreHashOf(env), env.Coauthors, env.AgreementNonce)
	res := app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, time.Now())
	assert.Equal(t, uint32(95), res.Code)
	assert.Contains(t, res.Log, "too many coauthors")
}

// TestCoCommit_SingleAndDuplicateCoauthorRejected (#4).
func TestCoCommit_SingleAndDuplicateCoauthorRejected(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5
	local := newAgentKey(t)
	ch := sha256.Sum256([]byte("dc"))

	mk := func(coauthors []tx.CoCommitCoauthor, signers []ed25519.PrivateKey) *tx.ParsedTx {
		env := &tx.CoCommitSubmit{
			SchemaVersion: 1, ContentHash: ch[:], MemoryType: tx.MemoryTypeFact,
			Domain: "d", Classification: tx.ClearanceInternal, ConfidenceScore: 0.5,
			CreatedAtUnix: 1, AgreementNonce: []byte("n"), Coauthors: coauthors,
		}
		core := tx.CanonicalCoreBytes(env)
		for i := range env.Coauthors {
			env.Coauthors[i].Sig = ed25519.Sign(signers[i], core)
		}
		env.SharedID = tx.ComputeSharedID(tx.CoreHashOf(env), env.Coauthors, env.AgreementNonce)
		return coCommitSubmitTx(t, local, env)
	}

	// Single self-coauthor -> "at least 2 distinct".
	single := mk(
		[]tx.CoCommitCoauthor{{PubKey: local.pub, ChainID: "sage-local"}},
		[]ed25519.PrivateKey{local.priv},
	)
	r1 := app.processCoCommitSubmit(single, 10, time.Now())
	assert.Equal(t, uint32(95), r1.Code)
	assert.Contains(t, r1.Log, "at least 2 distinct")

	// Duplicate coauthor pubkey -> "duplicate".
	foreign := genTestCoauthor(t, "sage-b")
	dup := mk(
		[]tx.CoCommitCoauthor{
			{PubKey: local.pub, ChainID: "sage-local"},
			{PubKey: foreign.pub, ChainID: "sage-b"},
			{PubKey: foreign.pub, ChainID: "sage-b"},
		},
		[]ed25519.PrivateKey{local.priv, foreign.priv, foreign.priv},
	)
	r2 := app.processCoCommitSubmit(dup, 11, time.Now())
	assert.Equal(t, uint32(95), r2.Code)
	assert.Contains(t, r2.Log, "duplicate")
}

// TestCoCommit_EmitsStatusUpdate (#1/#2): a status_update pendingWrite is emitted
// so the off-chain committed_at is populated.
func TestCoCommit_EmitsStatusUpdate(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5
	local := newAgentKey(t)
	env, _ := buildCoCommitEnvelope(t, local, "family.photos", []byte("n1"), "sage-b")
	require.Equal(t, uint32(0), app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, time.Now()).Code)

	found := false
	for _, pw := range app.pendingWrites {
		if pw.writeType == "status_update" {
			if su, ok := pw.data.(*statusUpdate); ok && su.MemoryID == env.SharedID && su.Status == memory.StatusCommitted {
				found = true
			}
		}
	}
	assert.True(t, found, "co-commit must emit a status_update pendingWrite (committed_at population)")
}

// TestCoCommit_CoauthorCannotSelfCorroborate (M2): a recorded coauthor cannot
// corroborate the jointly-authored memory; a non-coauthor can.
func TestCoCommit_CoauthorCannotSelfCorroborate(t *testing.T) {
	app := setupTestApp(t)
	app.appV10AppliedHeight = 1 // corroboration guard active
	app.appV15AppliedHeight = 5
	local := newAgentKey(t)
	env, foreign := buildCoCommitEnvelope(t, local, "family.photos", []byte("n1"), "sage-b")
	require.Equal(t, uint32(0), app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, time.Now()).Code)

	// The foreign coauthor (a recorded coauthor) tries to corroborate -> rejected.
	foreignAgent := agentKey{pub: foreign.pub, priv: foreign.priv, id: hex.EncodeToString(foreign.pub)}
	self := app.processMemoryCorroborate(makeMemoryCorroborateTx(t, foreignAgent, env.SharedID, "self"), 11, time.Now())
	assert.Equal(t, uint32(17), self.Code, "a coauthor cannot self-corroborate a co-authored memory")
	assert.Contains(t, self.Log, "co-authored")

	// A non-coauthor CAN corroborate.
	outsider := newAgentKey(t)
	ok := app.processMemoryCorroborate(makeMemoryCorroborateTx(t, outsider, env.SharedID, "independent"), 12, time.Now())
	assert.Equal(t, uint32(0), ok.Code, "a non-coauthor may corroborate: %s", ok.Log)
}

// ---------------------------------------------------------------------------
// Footgun E — envelope validity window + federation-status bind (app-v15).
// ---------------------------------------------------------------------------

// resignEnvelope recomputes the coauthor sigs + SharedID after mutating a signed
// field (the window lives in CanonicalCoreBytes, so it must be re-signed).
func resignEnvelope(env *tx.CoCommitSubmit, local agentKey, foreign testCoauthor) {
	core := tx.CanonicalCoreBytes(env)
	env.Coauthors[0].Sig = ed25519.Sign(local.priv, core)
	env.Coauthors[1].Sig = ed25519.Sign(foreign.priv, core)
	env.SharedID = tx.ComputeSharedID(tx.CoreHashOf(env), env.Coauthors, env.AgreementNonce)
}

// TestCoCommit_ValidityWindow: an envelope submitted outside its jointly-signed
// [NotBefore, NotAfter) window is rejected vs the block's deterministic time.
func TestCoCommit_ValidityWindow(t *testing.T) {
	blockTime := time.Unix(1_700_001_000, 0)

	// Expired: NotAfter in the past.
	appExp := setupTestApp(t)
	appExp.appV15AppliedHeight = 5
	local := newAgentKey(t)
	env, foreign := buildCoCommitEnvelope(t, local, "d", []byte("nw1"), "sage-b")
	env.NotAfter = blockTime.Unix() - 1
	resignEnvelope(env, local, foreign)
	res := appExp.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, blockTime)
	assert.Equal(t, uint32(98), res.Code, "expired envelope rejected")
	assert.Contains(t, res.Log, "NotAfter")

	// Not yet valid: NotBefore in the future.
	appNb := setupTestApp(t)
	appNb.appV15AppliedHeight = 5
	local2 := newAgentKey(t)
	env2, foreign2 := buildCoCommitEnvelope(t, local2, "d", []byte("nw2"), "sage-b")
	env2.NotBefore = blockTime.Unix() + 1000
	resignEnvelope(env2, local2, foreign2)
	res2 := appNb.processCoCommitSubmit(coCommitSubmitTx(t, local2, env2), 10, blockTime)
	assert.Equal(t, uint32(98), res2.Code, "not-yet-valid envelope rejected")
	assert.Contains(t, res2.Log, "NotBefore")

	// In-window: accepted.
	appOk := setupTestApp(t)
	appOk.appV15AppliedHeight = 5
	local3 := newAgentKey(t)
	env3, foreign3 := buildCoCommitEnvelope(t, local3, "d", []byte("nw3"), "sage-b")
	env3.NotBefore = blockTime.Unix() - 100
	env3.NotAfter = blockTime.Unix() + 100
	resignEnvelope(env3, local3, foreign3)
	res3 := appOk.processCoCommitSubmit(coCommitSubmitTx(t, local3, env3), 10, blockTime)
	assert.Equal(t, uint32(0), res3.Code, "in-window envelope accepted: %s", res3.Log)
}

// TestCoCommit_RevokedFederationRejected: if a coauthor's home chain has a
// cross_fed record that is not active (revoked), the co-commit is rejected —
// closing stale-envelope replay after a federation is revoked.
func TestCoCommit_RevokedFederationRejected(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5
	local := newAgentKey(t)
	env, _ := buildCoCommitEnvelope(t, local, "d", []byte("nr1"), "sage-b")

	// Active federation with the foreign coauthor's chain -> accepted.
	require.NoError(t, app.badgerStore.SetCrossFed("sage-b", "https://x", make([]byte, 32), 4, 0, []string{"*"}, nil, "active"))
	require.Equal(t, uint32(0), app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, time.Now()).Code)

	// Revoke it, then a FRESH envelope (new nonce) with the same foreign chain -> rejected.
	require.NoError(t, app.badgerStore.UpdateCrossFedStatus("sage-b", "revoked"))
	local2 := newAgentKey(t)
	env2, _ := buildCoCommitEnvelope(t, local2, "d", []byte("nr2"), "sage-b")
	res := app.processCoCommitSubmit(coCommitSubmitTx(t, local2, env2), 11, time.Now())
	assert.Equal(t, uint32(98), res.Code, "co-commit under a revoked federation rejected")
	assert.Contains(t, res.Log, "not active")
}

// TestCoCommit_NoWindowStillWorks: a legacy-style envelope with no window (0/0)
// and no federation record commits normally (permanent envelope, local coauthor
// has no self-fed record so the status gate does not fire).
func TestCoCommit_NoWindowStillWorks(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5
	local := newAgentKey(t)
	env, _ := buildCoCommitEnvelope(t, local, "d", []byte("nw0"), "sage-b")
	assert.Equal(t, int64(0), env.NotBefore)
	assert.Equal(t, int64(0), env.NotAfter)
	res := app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, time.Now())
	assert.Equal(t, uint32(0), res.Code, "permanent envelope with no fed record commits: %s", res.Log)
}

// TestProcessAccessQuery_ForkGatedReject: post-app-v15 the on-chain similarity
// query is a deterministic Code 10 reject (closing the per-node SQLite read that
// folded into LastResultsHash); pre-fork the handler still runs.
func TestProcessAccessQuery_ForkGatedReject(t *testing.T) {
	app := setupTestApp(t)
	agent := newAgentKey(t)

	// Post-fork: deterministic reject BEFORE any offchainStore read.
	app.appV15AppliedHeight = 5
	pubKey, sig, bodyHash, ts := signAgentProof(t, agent, []byte("q"))
	qtx := &tx.ParsedTx{
		Type:           tx.TxTypeAccessQuery,
		AccessQuery:    &tx.AccessQuery{Domain: "d", Embedding: []float32{0.1, 0.2}, TopK: 5},
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
	}
	res := app.processAccessQuery(qtx, 10, time.Now())
	assert.Equal(t, uint32(10), res.Code, "post-fork access_query rejected")
	assert.Empty(t, res.Data, "reject must emit empty Data (deterministic across nodes)")

	// Pre-fork (gate dormant): the handler runs (payload validation reached).
	app.appV15AppliedHeight = 0
	resPre := app.processAccessQuery(qtx, 10, time.Now())
	assert.NotEqual(t, uint32(10), resPre.Code, "pre-fork the handler still runs (not the app-v15 reject)")
}
