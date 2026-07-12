package abci

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"strings"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
)

func makeDelegatedDomainRegisterTx(
	t *testing.T,
	agent, outer agentKey,
	request []byte,
	proofTime time.Time,
	domain, description string,
	includeRequest bool,
) *tx.ParsedTx {
	t.Helper()
	bodyHash := sha256.Sum256(request)
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(proofTime.Unix())) // #nosec G115 -- test preserves timestamp bits
	message := append(append([]byte(nil), bodyHash[:]...), ts[:]...)

	parsed := &tx.ParsedTx{
		Type:      tx.TxTypeDomainRegister,
		Nonce:     1,
		Timestamp: proofTime,
		DomainRegister: &tx.DomainRegister{
			DomainName:   domain,
			OwnerAgentID: agent.id,
			Description:  description,
		},
		AgentPubKey:    agent.pub,
		AgentSig:       ed25519.Sign(agent.priv, message),
		AgentTimestamp: proofTime.Unix(),
		AgentBodyHash:  bodyHash[:],
	}
	if includeRequest {
		parsed.AgentRequest = append([]byte(nil), request...)
	}
	require.NoError(t, tx.SignTx(parsed, outer.priv))
	return parsed
}

func TestAppV17DelegatedProofRejectsCapturedDifferentAction(t *testing.T) {
	app := setupTestApp(t)
	app.appV17AppliedHeight = 5
	agent := newAgentKey(t)
	attackerNode := newAgentKey(t)
	blockTime := time.Now().Truncate(time.Second)

	// The agent authorized only a harmless profile update. The outer signer
	// tries to transplant that valid historical proof onto a domain-register
	// transaction, which was accepted by the original v11.5 candidate.
	request := []byte("PUT /v1/agent/update\n{\"name\":\"harmless\",\"boot_bio\":\"\"}")
	parsed := makeDelegatedDomainRegisterTx(t, agent, attackerNode, request, blockTime, "captured-proof-domain", "forged", true)

	result := app.processTx(parsed, 6, blockTime)
	assert.Equal(t, uint32(109), result.Code, result.Log)
	assert.Contains(t, result.Log, "does not authorize POST")
	_, err := app.badgerStore.GetDomainOwner("captured-proof-domain")
	assert.Error(t, err, "rejected delegated action must not mutate domain state")
}

func TestAppV17DelegatedProofRejectsPayloadMutation(t *testing.T) {
	app := setupTestApp(t)
	app.appV17AppliedHeight = 5
	agent := newAgentKey(t)
	outer := newAgentKey(t)
	blockTime := time.Now().Truncate(time.Second)
	request := []byte("POST /v1/domain/register\n{\"name\":\"authorized\",\"description\":\"safe\"}")
	parsed := makeDelegatedDomainRegisterTx(t, agent, outer, request, blockTime, "substituted", "unsafe", true)

	result := app.processTx(parsed, 6, blockTime)
	assert.Equal(t, uint32(109), result.Code, result.Log)
	assert.Contains(t, result.Log, "payload differs")
}

func TestAppV17DelegatedProofAcceptedOnce(t *testing.T) {
	app := setupTestApp(t)
	app.appV17AppliedHeight = 5
	agent := newAgentKey(t)
	outer := newAgentKey(t)
	blockTime := time.Now().Truncate(time.Second)
	request := []byte("POST /v1/domain/register\n{\"name\":\"authorized\",\"description\":\"safe\"}")
	parsed := makeDelegatedDomainRegisterTx(t, agent, outer, request, blockTime, "authorized", "safe", true)

	first := app.processTx(parsed, 6, blockTime)
	require.Equal(t, uint32(0), first.Code, first.Log)

	second := app.processTx(parsed, 6, blockTime)
	assert.Equal(t, uint32(109), second.Code, second.Log)
	assert.Contains(t, second.Log, "already consumed")
}

func TestAppV17DelegatedProofConsensusFreshness(t *testing.T) {
	app := setupTestApp(t)
	app.appV17AppliedHeight = 5
	agent := newAgentKey(t)
	outer := newAgentKey(t)
	blockTime := time.Now().Truncate(time.Second)
	proofTime := blockTime.Add(-5*time.Minute - time.Second)
	request := []byte("POST /v1/domain/register\n{\"name\":\"freshness-stale\"}")
	parsed := makeDelegatedDomainRegisterTx(t, agent, outer, request, proofTime, "freshness-stale", "", true)

	result := app.processTx(parsed, 6, blockTime)
	assert.Equal(t, uint32(109), result.Code, result.Log)
	assert.Contains(t, result.Log, "older than the 5-minute consensus window")
}

func TestAppV17DelegatedProofAcceptsWallClockAheadOfIdleConsensus(t *testing.T) {
	app := setupTestApp(t)
	app.appV17AppliedHeight = 5
	agent := newAgentKey(t)
	outer := newAgentKey(t)
	blockTime := time.Now().Truncate(time.Second)
	proofTime := blockTime.Add(30 * time.Minute)
	request := []byte("POST /v1/domain/register\n{\"name\":\"after-idle\"}")
	parsed := makeDelegatedDomainRegisterTx(t, agent, outer, request, proofTime, "after-idle", "", true)

	result := app.processTx(parsed, 6, blockTime)
	assert.Equal(t, uint32(0), result.Code, result.Log)
}

func TestAppV17SameKeyTransactionNeedsNoHTTPRequestEnvelope(t *testing.T) {
	app := setupTestApp(t)
	app.appV17AppliedHeight = 5
	agent := newAgentKey(t)
	blockTime := time.Now().Truncate(time.Second)
	parsed := makeDelegatedDomainRegisterTx(t, agent, agent, []byte("node-originated-domain-register"), blockTime, "same-key", "", false)

	result := app.processTx(parsed, 6, blockTime)
	assert.Equal(t, uint32(0), result.Code, result.Log)
}

func TestPreAppV17HistoricalProofBehaviorUnchanged(t *testing.T) {
	app := setupTestApp(t)
	app.appV8AppliedHeight = 1 // keep consensus-path outer signature verification active
	app.appV9AppliedHeight = 1 // and the historical outer-signer nonce rule
	agent := newAgentKey(t)
	outer := newAgentKey(t)
	blockTime := time.Now().Truncate(time.Second)
	parsed := makeDelegatedDomainRegisterTx(t, agent, outer, []byte("historical unrelated proof"), blockTime, "historical-domain", "", false)

	result := app.processTx(parsed, 5, blockTime)
	assert.Equal(t, uint32(0), result.Code, result.Log)
	assert.False(t, strings.Contains(result.Log, "agent proof rejected"))
}

func TestIsAppV17ActiveForNextTxFlipsAtActivationCommit(t *testing.T) {
	app := setupTestApp(t)
	app.appV17AppliedHeight = 50

	app.state.Height = 49
	assert.False(t, app.IsAppV17ActiveForNextTx())
	app.state.Height = 50
	assert.True(t, app.IsAppV17ActiveForNextTx(), "REST must envelope txs destined for H_activation+1")
}

func TestAppV17DelegatedProofFinalizeBlockDeterminismAndFreshOuterReplay(t *testing.T) {
	left := setupTestApp(t)
	right := setupTestApp(t)
	left.appV17AppliedHeight = 5
	right.appV17AppliedHeight = 5
	agent := newAgentKey(t)
	outer := newAgentKey(t)
	blockTime := time.Now().Truncate(time.Second)
	request := []byte("POST /v1/domain/register\n{\"name\":\"deterministic\"}")
	parsed := makeDelegatedDomainRegisterTx(t, agent, outer, request, blockTime, "deterministic", "", true)
	raw, err := tx.EncodeTx(parsed)
	require.NoError(t, err)

	finalize := func(app *SageApp, height int64, rawTx []byte) *abcitypes.ResponseFinalizeBlock {
		t.Helper()
		response, finalizeErr := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
			Height: height,
			Time:   blockTime,
			Txs:    [][]byte{rawTx},
		})
		require.NoError(t, finalizeErr)
		require.Len(t, response.TxResults, 1)
		return response
	}

	leftResult := finalize(left, 6, raw)
	rightResult := finalize(right, 6, raw)
	require.Equal(t, uint32(0), leftResult.TxResults[0].Code, leftResult.TxResults[0].Log)
	require.Equal(t, uint32(0), rightResult.TxResults[0].Code, rightResult.TxResults[0].Log)
	assert.Equal(t, leftResult.AppHash, rightResult.AppHash, "proof claim must enter AppHash identically on every validator")

	// A Byzantine relay can outer-sign the same captured agent proof again with
	// a fresh node nonce. The outer replay gate therefore passes, but the
	// consensus proof marker still rejects the authorization replay.
	relayed := *parsed
	relayed.Nonce = 2
	require.NoError(t, tx.SignTx(&relayed, outer.priv))
	relayedRaw, err := tx.EncodeTx(&relayed)
	require.NoError(t, err)
	replayResult := finalize(left, 7, relayedRaw)
	assert.Equal(t, uint32(109), replayResult.TxResults[0].Code, replayResult.TxResults[0].Log)
	assert.Contains(t, replayResult.TxResults[0].Log, "already consumed")
}
