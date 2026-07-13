package abci

// Piece C (v11.8) consensus-safety regression: per-token signing identity.
//
// Issuing an MCP token mints an ed25519 keypair OFF-consensus in the REST
// handler and auto-registers it by issuing MORE standard TxTypeAgentRegister
// transactions — no new tx field, no key generation inside FinalizeBlock. These
// tests pin the two invariants that keep that safe under CometBFT replay:
//   - FinalizeBlock is a pure function of the tx bytes, so N token registrations
//     produce byte-identical AppHashes on every replica (if key generation ever
//     leaked into consensus the hashes would diverge), and issuing tokens never
//     rewrites a pre-existing block's AppHash.
//   - processAgentRegister stays idempotent, so a background registration retry
//     that lands after the first attempt never mutates consensus state.

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/tx"
)

// encodeSelfSignedRegister builds an agent_register tx for a freshly-generated
// key (simulating an MCP token issuance whose keypair was minted off-consensus)
// and outer-signs it with the SAME key — a node-originated same-key
// registration. Returns the wire bytes FinalizeBlock decodes.
func encodeSelfSignedRegister(t *testing.T, ak agentKey, name string) []byte {
	t.Helper()
	ptx := makeAgentRegisterTx(t, ak, name, "member", "", "mcp-token", "")
	require.NoError(t, tx.SignTx(ptx, ak.priv))
	raw, err := tx.EncodeTx(ptx)
	require.NoError(t, err)
	return raw
}

func finalizeBlockPieceC(t *testing.T, app *SageApp, height int64, blockTime time.Time, txs ...[]byte) *abcitypes.ResponseFinalizeBlock {
	t.Helper()
	resp, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: height,
		Time:   blockTime,
		Txs:    txs,
	})
	require.NoError(t, err)
	return resp
}

func TestPieceC_TokenIssuanceRegisterDeterminism(t *testing.T) {
	blockTime := time.Now().Truncate(time.Second)

	// Fixed key material + fixed tx bytes so BOTH replicas commit exactly the
	// same block sequence. FinalizeBlock must reduce to the same AppHash; any
	// divergence would betray non-determinism (e.g. key generation) on the
	// consensus path.
	base := newAgentKey(t)
	baseTx := encodeSelfSignedRegister(t, base, "operator")

	const tokenCount = 5
	tokenTxs := make([][]byte, tokenCount)
	for i := range tokenTxs {
		tokenTxs[i] = encodeSelfSignedRegister(t, newAgentKey(t), "mcp-token")
	}

	replay := func() (h1, h2 []byte) {
		app := setupTestApp(t)
		r1 := finalizeBlockPieceC(t, app, 1, blockTime, baseTx)
		require.Len(t, r1.TxResults, 1)
		require.Equal(t, uint32(0), r1.TxResults[0].Code, r1.TxResults[0].Log)

		r2 := finalizeBlockPieceC(t, app, 2, blockTime, tokenTxs...)
		require.Len(t, r2.TxResults, tokenCount)
		for i, res := range r2.TxResults {
			require.Equal(t, uint32(0), res.Code, "token register %d rejected: %s", i, res.Log)
		}
		return r1.AppHash, r2.AppHash
	}

	leftH1, leftH2 := replay()
	rightH1, rightH2 := replay()

	assert.Equal(t, leftH1, rightH1, "pre-existing block-1 AppHash must be identical across replicas")
	assert.Equal(t, leftH2, rightH2, "N token-issuance registers must be deterministic — no key-gen in FinalizeBlock")

	// Issuing tokens at height 2 must not retroactively alter the height-1
	// AppHash: an app that ONLY commits block 1 reproduces the same H1 hash.
	baseOnly := setupTestApp(t)
	rBaseOnly := finalizeBlockPieceC(t, baseOnly, 1, blockTime, baseTx)
	assert.Equal(t, leftH1, rBaseOnly.AppHash, "token issuance must leave pre-existing block AppHashes unchanged")
}

func TestPieceC_ProcessAgentRegisterIdempotent(t *testing.T) {
	blockTime := time.Now().Truncate(time.Second)
	app := setupTestApp(t)

	regTx := encodeSelfSignedRegister(t, newAgentKey(t), "mcp-token")

	r1 := finalizeBlockPieceC(t, app, 1, blockTime, regTx)
	require.Len(t, r1.TxResults, 1)
	require.Equal(t, uint32(0), r1.TxResults[0].Code, r1.TxResults[0].Log)
	firstHash := r1.AppHash

	// A background registration retry re-broadcasts the identity: the same
	// register landing again hits the idempotent branch (agent already
	// registered) — still Code 0, and BadgerDB state, hence the AppHash, is
	// unchanged.
	r2 := finalizeBlockPieceC(t, app, 2, blockTime, regTx)
	require.Len(t, r2.TxResults, 1)
	require.Equal(t, uint32(0), r2.TxResults[0].Code, r2.TxResults[0].Log)
	assert.Equal(t, firstHash, r2.AppHash, "idempotent re-register must not mutate consensus state")
}

// encodeDelegatedMintedRegister builds an agent_register tx byte-for-byte the way
// the REST handler's registerMintedAgentIdentity does for MCP token issuance — a
// DELEGATED-proof register, NOT the same-key self-signed one the tests above
// exercise:
//   - the agent proof (AgentPubKey/AgentSig/AgentBodyHash/AgentTimestamp/AgentNonce)
//     is signed by the freshly-minted TOKEN key via auth.SignRequestWithNonce over
//     the canonical "POST /v1/agent/register\n"+body request, exactly like the REST
//     auth middleware,
//   - AgentRequest carries that canonical request envelope (only set post-app-v17),
//   - the OUTER transaction is signed by the node/validator key (`outer`), so
//     registerTx.PublicKey (outer.pub) != AgentPubKey (the token key) — the
//     delegated-signing shape enforceDelegatedAgentProof independently binds.
//
// It returns the outer-signed ParsedTx; encodeDelegatedMintedRegister wraps it to
// the wire bytes FinalizeBlock decodes.
func buildDelegatedMintedRegister(t *testing.T, token, outer agentKey, name, provider string, proofTime time.Time, nonce []byte) *tx.ParsedTx {
	t.Helper()
	agentID := token.id
	if name == "" {
		name = "mcp-token-" + agentID[:8]
	}

	// The signed body reconstructs the exact AgentRegister payload consensus
	// rebuilds under verifySignedAgentAction (name required, role defaults to
	// "member"); every field is emitted verbatim, mirroring the production regBody.
	regBody := struct {
		Name       string `json:"name"`
		Role       string `json:"role"`
		BootBio    string `json:"boot_bio"`
		Provider   string `json:"provider"`
		P2PAddress string `json:"p2p_address"`
	}{Name: name, Role: "member", Provider: provider}
	body, err := json.Marshal(regBody)
	require.NoError(t, err)

	const method, path = "POST", "/v1/agent/register"
	timestamp := proofTime.Unix()
	// Canonical request + body hash exactly as the REST auth middleware computes
	// for a token-key-signed POST /v1/agent/register.
	canonical := []byte(method + " " + path + "\n")
	canonical = append(canonical, body...)
	bodyHash := sha256.Sum256(canonical)
	// AgentSig: the TOKEN key signs sha256(canonical) || bigEndian(ts) || nonce.
	sig := auth.SignRequestWithNonce(token.priv, method, path, body, timestamp, nonce)

	registerTx := &tx.ParsedTx{
		Type:      tx.TxTypeAgentRegister,
		Nonce:     tx.MonotonicNonce(outer.priv), // non-zero: app-v9 sentinel rejects nonce 0
		Timestamp: proofTime,
		AgentRegister: &tx.AgentRegister{
			AgentID:  agentID,
			Name:     name,
			Role:     "member",
			Provider: provider,
		},
		AgentPubKey:    append([]byte(nil), token.pub...),
		AgentSig:       sig,
		AgentTimestamp: timestamp,
		AgentBodyHash:  bodyHash[:],
		AgentNonce:     append([]byte(nil), nonce...),
		AgentRequest:   append([]byte(nil), canonical...), // post-app-v17 envelope
	}
	// OUTER signature by the node/validator key — this is what makes it delegated:
	// registerTx.PublicKey becomes outer.pub, distinct from AgentPubKey (token key).
	require.NoError(t, tx.SignTx(registerTx, outer.priv))
	return registerTx
}

// encodeDelegatedMintedRegister returns the wire bytes of a delegated minted
// register (see buildDelegatedMintedRegister).
func encodeDelegatedMintedRegister(t *testing.T, token, outer agentKey, name, provider string, proofTime time.Time, nonce []byte) []byte {
	t.Helper()
	raw, err := tx.EncodeTx(buildDelegatedMintedRegister(t, token, outer, name, provider, proofTime, nonce))
	require.NoError(t, err)
	return raw
}

// TestPieceC_DelegatedRegisterReplicaDeterminism exercises the ACTUAL delegated
// register path that registerMintedAgentIdentity emits in production (token-key
// agent proof + node-key outer signature + app-v17 request envelope), rather than
// the same-key self-signed shortcut the other Piece C tests use. It pins the same
// consensus-safety invariant on that real path: FinalizeBlock is a pure function
// of the tx bytes, so committing an accepted delegated register — including its
// single-use proof claim — yields byte-identical AppHashes on every replica and
// never rewrites a pre-existing block's AppHash.
func TestPieceC_DelegatedRegisterReplicaDeterminism(t *testing.T) {
	blockTime := time.Now().Truncate(time.Second)
	const activationHeight = 5 // app-v17 rules are in force for every height > 5

	// Pre-existing self-signed base register. app-v17 activates the app-v8 outer
	// signature check and the app-v9 nonce-0 sentinel, so the base needs a non-zero
	// outer nonce (encodeSelfSignedRegister leaves it 0). Built ONCE so both
	// replicas commit byte-identical bytes.
	base := newAgentKey(t)
	baseTx := makeAgentRegisterTx(t, base, "operator", "member", "", "mcp-token", "")
	baseTx.Nonce = tx.MonotonicNonce(base.priv)
	require.NoError(t, tx.SignTx(baseTx, base.priv))
	baseRaw, err := tx.EncodeTx(baseTx)
	require.NoError(t, err)

	// Two DISTINCT delegated token registers — different token key, outer key, and
	// proof nonce, hence different single-use proof fingerprints. Built ONCE and
	// replayed so the block sequence is byte-identical on both replicas.
	del1 := encodeDelegatedMintedRegister(t, newAgentKey(t), newAgentKey(t), "", "mcp-token", blockTime, []byte{1, 2, 3, 4, 5, 6, 7, 8})
	del2 := encodeDelegatedMintedRegister(t, newAgentKey(t), newAgentKey(t), "", "mcp-token", blockTime, []byte{8, 7, 6, 5, 4, 3, 2, 1})

	replay := func() (hBase, hDel1, hDel2 []byte) {
		app := setupTestApp(t)
		app.appV17AppliedHeight = activationHeight

		rBase := finalizeBlockPieceC(t, app, 6, blockTime, baseRaw)
		require.Len(t, rBase.TxResults, 1)
		require.Equal(t, uint32(0), rBase.TxResults[0].Code, rBase.TxResults[0].Log)

		// The delegated token register must be ACCEPTED (Code 0) on the real
		// post-app-v17 path: outer node signs, token key proves the action, the
		// single-use proof is claimed, then processAgentRegister registers hex(pub).
		rDel1 := finalizeBlockPieceC(t, app, 7, blockTime, del1)
		require.Len(t, rDel1.TxResults, 1)
		require.Equal(t, uint32(0), rDel1.TxResults[0].Code, "delegated register must be accepted post-v17: %s", rDel1.TxResults[0].Log)

		// A second DISTINCT delegated register (different token key + nonce) is also
		// accepted and deterministic — the single-use claim only blocks verbatim replays.
		rDel2 := finalizeBlockPieceC(t, app, 8, blockTime, del2)
		require.Len(t, rDel2.TxResults, 1)
		require.Equal(t, uint32(0), rDel2.TxResults[0].Code, "second distinct delegated register must be accepted: %s", rDel2.TxResults[0].Log)

		return rBase.AppHash, rDel1.AppHash, rDel2.AppHash
	}

	leftBase, leftDel1, leftDel2 := replay()
	rightBase, rightDel1, rightDel2 := replay()

	assert.Equal(t, leftBase, rightBase, "pre-existing base-register AppHash must be identical across replicas")
	assert.Equal(t, leftDel1, rightDel1, "delegated proof claim must enter AppHash identically on every replica — no non-determinism on the delegated path")
	assert.Equal(t, leftDel2, rightDel2, "a second distinct delegated register must also be deterministic across replicas")

	// Committing the delegated registers at heights 7/8 must not retroactively
	// alter the height-6 base AppHash: an app that ONLY commits the base block
	// reproduces H_base.
	baseOnly := setupTestApp(t)
	baseOnly.appV17AppliedHeight = activationHeight
	rBaseOnly := finalizeBlockPieceC(t, baseOnly, 6, blockTime, baseRaw)
	assert.Equal(t, leftBase, rBaseOnly.AppHash, "delegated token issuance must leave pre-existing block AppHashes unchanged")

	// Subtlety (app-v17 single-use proof): a delegated register is deliberately NOT
	// idempotent. ClaimAgentProof consumes the proof fingerprint once. A verbatim
	// byte-identical replay is already stopped by the app-v9 outer-nonce gate, so to
	// isolate the single-use PROOF marker itself we model a Byzantine relay that
	// re-wraps the SAME captured agent proof under a FRESH outer node nonce
	// (re-signed outer signature). The outer-nonce gate then passes, but the
	// consensus proof marker still CORRECTLY rejects the authorization replay (code
	// 109, "already consumed") — never re-applying it. Idempotency is asserted only
	// on the same-key self-signed path (TestPieceC_ProcessAgentRegisterIdempotent).
	suToken := newAgentKey(t)
	suOuter := newAgentKey(t)
	suTx := buildDelegatedMintedRegister(t, suToken, suOuter, "", "mcp-token", blockTime, []byte{9, 9, 9, 9, 9, 9, 9, 9})
	suRaw, err := tx.EncodeTx(suTx)
	require.NoError(t, err)

	singleUse := setupTestApp(t)
	singleUse.appV17AppliedHeight = activationHeight
	accepted := finalizeBlockPieceC(t, singleUse, 6, blockTime, suRaw)
	require.Len(t, accepted.TxResults, 1)
	require.Equal(t, uint32(0), accepted.TxResults[0].Code, accepted.TxResults[0].Log)

	relayed := *suTx
	relayed.Nonce = suTx.Nonce + 1 // fresh outer node nonce clears the app-v9 replay gate
	require.NoError(t, tx.SignTx(&relayed, suOuter.priv))
	relayedRaw, err := tx.EncodeTx(&relayed)
	require.NoError(t, err)
	replayed := finalizeBlockPieceC(t, singleUse, 7, blockTime, relayedRaw)
	require.Len(t, replayed.TxResults, 1)
	assert.Equal(t, uint32(109), replayed.TxResults[0].Code, "re-wrapped delegated proof must be rejected as a consumed single-use proof")
	assert.Contains(t, replayed.TxResults[0].Log, "already consumed")
}
