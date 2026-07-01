package abci

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"testing"
	"time"

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
// pre-activation). The LOCAL submitter passes local authz; FOREIGN coauthors are
// verified by standalone ed25519 signature only.
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

func buildCoCommitEnvelope(t *testing.T, domain string, nonce []byte, cas []testCoauthor) *tx.CoCommitSubmit {
	t.Helper()
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
	}
	for _, c := range cas {
		env.Coauthors = append(env.Coauthors, tx.CoCommitCoauthor{PubKey: c.pub, ChainID: c.chain})
	}
	core := tx.CanonicalCoreBytes(env)
	for i, c := range cas {
		env.Coauthors[i].Sig = ed25519.Sign(c.priv, core)
	}
	env.SharedID = tx.ComputeSharedID(tx.CoreHashOf(env), env.Coauthors, nonce)
	return env
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

// TestCoCommit_DualGatePreFork: pre-activation, the exec gate rejects both new tx
// types with Code 10 and writes NOTHING (byte-identical AppHash).
func TestCoCommit_DualGatePreFork(t *testing.T) {
	app := setupTestApp(t) // app-v15 dormant
	require.Equal(t, int64(0), app.appV15AppliedHeight)
	local := newAgentKey(t)
	env := buildCoCommitEnvelope(t, "family.photos", []byte("n1"),
		[]testCoauthor{genTestCoauthor(t, "sage-a"), genTestCoauthor(t, "sage-b")})

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
// memory keyed by SharedID, with the co-commit anchor keys and local-submitter author.
func TestCoCommit_SubmitPostFork(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5
	local := newAgentKey(t)
	env := buildCoCommitEnvelope(t, "family.photos", []byte("n1"),
		[]testCoauthor{genTestCoauthor(t, "sage-a"), genTestCoauthor(t, "sage-b")})

	res := app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, time.Now())
	require.Equal(t, uint32(0), res.Code, "submit: %s", res.Log)
	assert.Equal(t, env.SharedID, string(res.Data))

	_, st, err := app.badgerStore.GetMemoryHash(env.SharedID)
	require.NoError(t, err)
	assert.Equal(t, string(memory.StatusProposed), st, "native local memory is proposed")

	author, _ := app.badgerStore.GetMemoryAuthor(env.SharedID)
	assert.Equal(t, local.id, author, "memauthor = LOCAL submitter, not a foreign coauthor")

	core, _ := app.badgerStore.GetCoCommitCore(env.SharedID)
	assert.Equal(t, tx.CoreHashOf(env), core, "cocommit:core = CoreHashOf(envelope)")

	dom, _ := app.badgerStore.GetMemoryDomain(env.SharedID)
	assert.Equal(t, "family.photos", dom)
}

// TestCoCommit_SharedIDMismatchRejected: a SharedID not derivable from the signed
// core is rejected (Code 96) — binds the id to the content.
func TestCoCommit_SharedIDMismatchRejected(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5
	local := newAgentKey(t)
	env := buildCoCommitEnvelope(t, "d", []byte("n"),
		[]testCoauthor{genTestCoauthor(t, "a"), genTestCoauthor(t, "b")})
	env.SharedID = "deadbeefdeadbeef" // tamper

	res := app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, time.Now())
	assert.Equal(t, uint32(96), res.Code, "tampered SharedID rejected")
}

// TestCoCommit_BadCoauthorSigRejected: a corrupted coauthor signature is rejected
// (Code 95) — every coauthor must standalone-verify over the canonical core.
func TestCoCommit_BadCoauthorSigRejected(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5
	local := newAgentKey(t)
	env := buildCoCommitEnvelope(t, "d", []byte("n"),
		[]testCoauthor{genTestCoauthor(t, "a"), genTestCoauthor(t, "b")})
	env.Coauthors[0].Sig[0] ^= 0xff // corrupt

	res := app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, time.Now())
	assert.Equal(t, uint32(95), res.Code, "corrupted coauthor sig rejected")
}

// TestCoCommit_AttestPostFork: a peer receipt over the SAME shared core is recorded
// as a cross-anchor.
func TestCoCommit_AttestPostFork(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5
	local := newAgentKey(t)
	env := buildCoCommitEnvelope(t, "family.photos", []byte("n1"),
		[]testCoauthor{genTestCoauthor(t, "sage-a"), genTestCoauthor(t, "sage-b")})
	require.Equal(t, uint32(0), app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, time.Now()).Code)

	peerPub, peerPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	receipt := &tx.CommitReceipt{
		ChainID: "sage-b", SharedID: env.SharedID, LocalMemID: "peer-mem-1",
		Height: 7, CommitTime: 1_700_000_500, CoreHash: tx.CoreHashOf(env),
	}
	rbytes := tx.EncodeCommitReceipt(receipt)
	att := &tx.CoCommitAttest{
		SharedID: env.SharedID, PeerChainID: "sage-b", PeerPubKey: peerPub,
		Receipt: rbytes, PeerSig: ed25519.Sign(peerPriv, rbytes),
		CommitTime: receipt.CommitTime, CoreHash: receipt.CoreHash,
	}
	res := app.processCoCommitAttest(&tx.ParsedTx{Type: tx.TxTypeCoCommitAttest, CoCommitAttest: att}, 11, time.Now())
	assert.Equal(t, uint32(0), res.Code, "attest over matching core: %s", res.Log)
}

// TestCoCommit_AttestFailClosed: an attest for a SharedID this chain never
// co-committed is rejected (Code 97) — fail-closed, no anchor written.
func TestCoCommit_AttestFailClosed(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5

	peerPub, peerPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	receipt := &tx.CommitReceipt{ChainID: "sage-b", SharedID: "unknown-shared", CoreHash: []byte("x")}
	rbytes := tx.EncodeCommitReceipt(receipt)
	att := &tx.CoCommitAttest{
		SharedID: "unknown-shared", PeerChainID: "sage-b", PeerPubKey: peerPub,
		Receipt: rbytes, PeerSig: ed25519.Sign(peerPriv, rbytes),
	}
	res := app.processCoCommitAttest(&tx.ParsedTx{Type: tx.TxTypeCoCommitAttest, CoCommitAttest: att}, 10, time.Now())
	assert.Equal(t, uint32(97), res.Code, "attest with no local co-commit is fail-closed")
}
