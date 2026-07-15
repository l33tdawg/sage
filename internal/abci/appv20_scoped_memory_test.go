package abci

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"sort"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/scope"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"
)

func deterministicScopedAgent(seedByte byte) agentKey {
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = seedByte + byte(i)
	}
	priv := ed25519.NewKeyFromSeed(seed)
	pub := priv.Public().(ed25519.PublicKey)
	return agentKey{pub: pub, priv: priv, id: hex.EncodeToString(pub)}
}

func setupScopedMemoryApp(t *testing.T, validatorCount int) (*SageApp, []agentKey, string) {
	t.Helper()
	app := setupTestApp(t)
	app.appV20AppliedHeight = 1
	validators := make([]agentKey, validatorCount)
	for i := range validators {
		validators[i] = newAgentKey(t)
		require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{
			ID: validators[i].id, PublicKey: validators[i].pub, Power: 10,
		}))
	}
	sort.Slice(validators, func(i, j int) bool { return validators[i].id < validators[j].id })
	domain := "research"
	require.NoError(t, app.badgerStore.RegisterDomain(domain, validators[0].id, "", 1))
	require.NoError(t, app.badgerStore.SetAccessGrant(domain, validators[0].id, 2, 0, validators[0].id))
	return app, validators, domain
}

func installScopeForValidators(t *testing.T, app *SageApp, scopeID, domain string, revision uint64, state scope.State, validators []agentKey) scope.Record {
	t.Helper()
	members := make([]scope.Member, 0, len(validators))
	for _, member := range validators {
		members = append(members, scope.Member{
			ValidatorID: member.id, AssignedWeight: 1, JoinedRevision: 1, Active: true,
		})
	}
	record := scope.Record{
		ScopeID: scopeID, Revision: revision, State: state,
		ControllerValidatorID: validators[0].id,
		CreatedHeight:         1,
		UpdatedHeight:         int64(revision),
		Domains:               []scope.Domain{{Name: domain}},
		Members:               members,
	}
	require.NoError(t, app.badgerStore.SetScopeRecord(record))
	return record
}

func scopedVote(t *testing.T, app *SageApp, validatorKey agentKey, memoryID string, decision tx.VoteDecision, height int64) uint32 {
	t.Helper()
	result := app.processMemoryVote(&tx.ParsedTx{
		PublicKey: validatorKey.pub,
		MemoryVote: &tx.MemoryVote{
			MemoryID: memoryID,
			Decision: decision,
		},
	}, height, time.Unix(height, 0))
	return result.Code
}

func TestAppV20ScopedBallotPinsRosterAcrossScopeUpdate(t *testing.T) {
	app, validators, domain := setupScopedMemoryApp(t, 5)
	// The fifth current validator is deliberately outside the four-member scope.
	record := installScopeForValidators(t, app, "scope-research", domain, 1, scope.StateActive, validators[:4])

	submit := makeMemorySubmitTx(t, validators[0], domain, "pinned canonical memory")
	result := app.processMemorySubmit(submit, 2, time.Unix(2, 0))
	require.Zero(t, result.Code, result.Log)
	memoryID := string(result.Data)
	ballot, err := app.badgerStore.GetScopeBallot(memoryID)
	require.NoError(t, err)
	require.NotNil(t, ballot)
	assert.Equal(t, record.ScopeID, ballot.ScopeID)
	assert.Equal(t, uint64(1), ballot.ScopeRevision)
	assert.Len(t, ballot.Members, 4)
	assert.Equal(t, uint64(4), ballot.TotalWeight)
	content, err := app.badgerStore.GetScopedContent(memoryID)
	require.NoError(t, err)
	require.NotNil(t, content)
	assert.Equal(t, submit.MemorySubmit.Content, content.Content)

	assert.Equal(t, uint32(13), scopedVote(t, app, validators[4], memoryID, tx.VoteDecisionAccept, 3), "current-chain outsider is not a scoped ballot member")
	require.Zero(t, scopedVote(t, app, validators[0], memoryID, tx.VoteDecisionAccept, 4))
	require.Zero(t, scopedVote(t, app, validators[1], memoryID, tx.VoteDecisionAccept, 5))
	_, status, err := app.badgerStore.GetMemoryHash(memoryID)
	require.NoError(t, err)
	assert.Equal(t, string(memory.StatusProposed), status, "two of four is below strict scoped quorum")

	// Prospective revision removes validators[2:]; the old ballot must remain
	// four members, and a removed-from-scope (but still on-chain) validator can
	// finish that historical ballot under its pinned authority.
	record.Revision = 2
	record.UpdatedHeight = 6
	record.Members = record.Members[:2]
	require.NoError(t, app.badgerStore.SetScopeRecord(record))
	ballotAfter, err := app.badgerStore.GetScopeBallot(memoryID)
	require.NoError(t, err)
	assert.Equal(t, ballot, ballotAfter)
	require.Zero(t, scopedVote(t, app, validators[3], memoryID, tx.VoteDecisionAccept, 7))

	hash, status, err := app.badgerStore.GetMemoryHash(memoryID)
	require.NoError(t, err)
	assert.Equal(t, string(memory.StatusCommitted), status)
	assert.Equal(t, submit.MemorySubmit.ContentHash, hash, "scoped terminal transition preserves the canonical hash")
	terminal, err := app.badgerStore.GetScopeBallot(memoryID)
	require.NoError(t, err)
	assert.Equal(t, scope.BallotCommitted, terminal.State)
}

func TestAppV20ScopedQuorumRequiresStrictlyMoreThanTwoThirds(t *testing.T) {
	app, validators, domain := setupScopedMemoryApp(t, 3)
	installScopeForValidators(t, app, "scope-strict", domain, 1, scope.StateActive, validators)
	result := app.processMemorySubmit(makeMemorySubmitTx(t, validators[0], domain, "strict threshold"), 2, time.Unix(2, 0))
	require.Zero(t, result.Code, result.Log)
	memoryID := string(result.Data)

	require.Zero(t, scopedVote(t, app, validators[0], memoryID, tx.VoteDecisionAccept, 3))
	require.Zero(t, scopedVote(t, app, validators[1], memoryID, tx.VoteDecisionAccept, 4))
	_, status, err := app.badgerStore.GetMemoryHash(memoryID)
	require.NoError(t, err)
	assert.Equal(t, string(memory.StatusProposed), status, "exactly 2/3 must not commit")
	require.Zero(t, scopedVote(t, app, validators[2], memoryID, tx.VoteDecisionReject, 5))
	_, status, err = app.badgerStore.GetMemoryHash(memoryID)
	require.NoError(t, err)
	assert.Equal(t, string(memory.StatusDeprecated), status, "all voted without strict supermajority is terminal")
}

func TestAppV20ScopedQuorumTopologyBoundaries(t *testing.T) {
	t.Run("three of four commits", func(t *testing.T) {
		app, validators, domain := setupScopedMemoryApp(t, 4)
		installScopeForValidators(t, app, "scope-three-of-four", domain, 1, scope.StateActive, validators)
		result := app.processMemorySubmit(makeMemorySubmitTx(t, validators[0], domain, "three of four topology"), 2, time.Unix(2, 0))
		require.Zero(t, result.Code, result.Log)
		memoryID := string(result.Data)

		for i := 0; i < 3; i++ {
			require.Zero(t, scopedVote(t, app, validators[i], memoryID, tx.VoteDecisionAccept, int64(3+i)))
		}
		_, status, err := app.badgerStore.GetMemoryHash(memoryID)
		require.NoError(t, err)
		assert.Equal(t, string(memory.StatusCommitted), status, "3/4 is strictly greater than 2/3")
	})

	t.Run("two of three remains pending while third is absent", func(t *testing.T) {
		app, validators, domain := setupScopedMemoryApp(t, 3)
		installScopeForValidators(t, app, "scope-two-of-three", domain, 1, scope.StateActive, validators)
		result := app.processMemorySubmit(makeMemorySubmitTx(t, validators[0], domain, "two of three topology"), 2, time.Unix(2, 0))
		require.Zero(t, result.Code, result.Log)
		memoryID := string(result.Data)

		for i := 0; i < 2; i++ {
			require.Zero(t, scopedVote(t, app, validators[i], memoryID, tx.VoteDecisionAccept, int64(3+i)))
		}
		_, status, err := app.badgerStore.GetMemoryHash(memoryID)
		require.NoError(t, err)
		assert.Equal(t, string(memory.StatusProposed), status, "exactly 2/3 cannot commit and an absent vote is not rejection")
		ballot, err := app.badgerStore.GetScopeBallot(memoryID)
		require.NoError(t, err)
		require.NotNil(t, ballot)
		assert.Equal(t, scope.BallotPending, ballot.State)
	})
}

func TestAppV20ScopedSubmitFailsClosedWithoutPartialState(t *testing.T) {
	app, validators, domain := setupScopedMemoryApp(t, 1)
	record := installScopeForValidators(t, app, "scope-guard", domain, 1, scope.StateActive, validators)

	badHash := makeMemorySubmitTx(t, validators[0], domain, "hash mismatch")
	badHash.MemorySubmit.MemoryID = "bad-hash-memory"
	wrong := sha256.Sum256([]byte("different"))
	badHash.MemorySubmit.ContentHash = wrong[:]
	result := app.processMemorySubmit(badHash, 2, time.Unix(2, 0))
	assert.Equal(t, uint32(19), result.Code)
	_, _, err := app.badgerStore.GetMemoryHash("bad-hash-memory")
	require.Error(t, err)
	ballot, err := app.badgerStore.GetScopeBallot("bad-hash-memory")
	require.NoError(t, err)
	assert.Nil(t, ballot)

	record.Revision = 2
	record.State = scope.StatePaused
	record.UpdatedHeight = 3
	require.NoError(t, app.badgerStore.SetScopeRecord(record))
	paused := makeMemorySubmitTx(t, validators[0], domain, "paused scope")
	paused.MemorySubmit.MemoryID = "paused-memory"
	result = app.processMemorySubmit(paused, 4, time.Unix(4, 0))
	assert.Equal(t, uint32(19), result.Code)
	assert.Contains(t, result.Log, "not active")
	_, _, err = app.badgerStore.GetMemoryHash("paused-memory")
	require.Error(t, err)
	require.ErrorContains(t, app.rejectScopedCoCommit(domain, 4), "unsupported for scoped domains")
}

func TestAppV20ScopedReplicaAppHashDeterminism(t *testing.T) {
	validators := []agentKey{
		deterministicScopedAgent(1),
		deterministicScopedAgent(33),
		deterministicScopedAgent(65),
		deterministicScopedAgent(97),
	}
	sort.Slice(validators, func(i, j int) bool { return validators[i].id < validators[j].id })
	buildReplica := func() *SageApp {
		app := setupTestApp(t)
		app.appV20AppliedHeight = 1
		for _, key := range validators {
			require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{ID: key.id, PublicKey: key.pub, Power: 10}))
		}
		require.NoError(t, app.badgerStore.RegisterDomain("research", validators[0].id, "", 1))
		require.NoError(t, app.badgerStore.SetAccessGrant("research", validators[0].id, 2, 0, validators[0].id))
		installScopeForValidators(t, app, "scope-replica", "research", 1, scope.StateActive, validators)
		return app
	}
	left, right := buildReplica(), buildReplica()

	submit := makeMemorySubmitTx(t, validators[0], "research", "replicated canonical envelope")
	submit.Nonce = 1
	require.NoError(t, tx.SignTx(submit, validators[0].priv))
	rawSubmit, err := tx.EncodeTx(submit)
	require.NoError(t, err)
	blockTime := time.Unix(1000, 0)
	finalizeBoth := func(height int64, raw []byte) (*abcitypes.ResponseFinalizeBlock, *abcitypes.ResponseFinalizeBlock) {
		leftResp, err := left.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: height, Time: blockTime, Txs: [][]byte{raw}})
		require.NoError(t, err)
		rightResp, err := right.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: height, Time: blockTime, Txs: [][]byte{raw}})
		require.NoError(t, err)
		assert.Equal(t, leftResp.AppHash, rightResp.AppHash, "replicas diverged at height %d", height)
		return leftResp, rightResp
	}
	leftResp, rightResp := finalizeBoth(2, rawSubmit)
	require.Zero(t, leftResp.TxResults[0].Code, leftResp.TxResults[0].Log)
	require.Zero(t, rightResp.TxResults[0].Code, rightResp.TxResults[0].Log)
	memoryID := string(leftResp.TxResults[0].Data)
	assert.Equal(t, memoryID, string(rightResp.TxResults[0].Data))

	for i := 0; i < 3; i++ {
		nonce := uint64(1)
		if i == 0 {
			nonce = 2 // validator[0] authored the submit with nonce 1
		}
		vote := &tx.ParsedTx{
			Type: tx.TxTypeMemoryVote, Nonce: nonce,
			MemoryVote: &tx.MemoryVote{MemoryID: memoryID, Decision: tx.VoteDecisionAccept},
		}
		require.NoError(t, tx.SignTx(vote, validators[i].priv))
		rawVote, err := tx.EncodeTx(vote)
		require.NoError(t, err)
		leftResp, rightResp = finalizeBoth(int64(3+i), rawVote)
		require.Zero(t, leftResp.TxResults[0].Code, leftResp.TxResults[0].Log)
		require.Zero(t, rightResp.TxResults[0].Code, rightResp.TxResults[0].Log)
	}
	leftBallot, err := left.badgerStore.GetScopeBallot(memoryID)
	require.NoError(t, err)
	rightBallot, err := right.badgerStore.GetScopeBallot(memoryID)
	require.NoError(t, err)
	assert.Equal(t, leftBallot, rightBallot)
	assert.Equal(t, scope.BallotCommitted, leftBallot.State)
	leftContent, err := left.badgerStore.GetScopedContent(memoryID)
	require.NoError(t, err)
	rightContent, err := right.badgerStore.GetScopedContent(memoryID)
	require.NoError(t, err)
	assert.Equal(t, leftContent, rightContent)
}

func TestAppV20ScopedProjectionRebuildFromCanonicalBadger(t *testing.T) {
	app, validators, domain := setupScopedMemoryApp(t, 1)
	installScopeForValidators(t, app, "scope-recovery", domain, 1, scope.StateActive, validators)
	submit := makeMemorySubmitTx(t, validators[0], domain, "recoverable after projection loss")
	result := app.processMemorySubmit(submit, 2, time.Unix(200, 0))
	require.Zero(t, result.Code, result.Log)
	memoryID := string(result.Data)
	require.Zero(t, scopedVote(t, app, validators[0], memoryID, tx.VoteDecisionAccept, 3))

	projection, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "rebuilt.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = projection.Close() })
	// Seed a poisoned local row: rebuild must replace canonical fields from
	// Badger rather than accepting the cache as authority.
	require.NoError(t, projection.InsertMemory(context.Background(), &memory.MemoryRecord{
		MemoryID: memoryID, SubmittingAgent: "attacker", Content: "poisoned",
		ContentHash: []byte("wrong"), MemoryType: memory.TypeFact,
		DomainTag: "other", ConfidenceScore: 0.1, Status: memory.StatusProposed,
		CreatedAt: time.Unix(1, 0),
	}))
	app.offchainStore = projection
	rebuilt, err := app.RebuildScopedProjection(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, rebuilt)
	rebuilt, err = app.RebuildScopedProjection(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, rebuilt, "projection recovery is idempotent")

	projected, err := projection.GetMemory(context.Background(), memoryID)
	require.NoError(t, err)
	assert.Equal(t, submit.MemorySubmit.Content, projected.Content)
	assert.Equal(t, submit.MemorySubmit.ContentHash, projected.ContentHash)
	assert.Equal(t, validators[0].id, projected.SubmittingAgent)
	assert.Equal(t, domain, projected.DomainTag)
	assert.Equal(t, memory.StatusCommitted, projected.Status)
	assert.Equal(t, time.Unix(200, 0).UTC(), projected.CreatedAt.UTC())
	classification, err := projection.GetMemoryClassificationLocal(context.Background(), memoryID)
	require.NoError(t, err)
	assert.Equal(t, int(submit.MemorySubmit.Classification), classification)

	challenge := app.processMemoryChallenge(makeMemoryChallengeTx(t, validators[0], memoryID, "retire bad knowledge"), 4, time.Unix(400, 0))
	require.Zero(t, challenge.Code, challenge.Log)
	hashAfterChallenge, statusAfterChallenge, err := app.badgerStore.GetMemoryHash(memoryID)
	require.NoError(t, err)
	assert.Equal(t, submit.MemorySubmit.ContentHash, hashAfterChallenge, "later scoped lifecycle transitions must preserve the recovery hash")
	assert.Equal(t, string(memory.StatusDeprecated), statusAfterChallenge)
	acceptedBallot, err := app.badgerStore.GetScopeBallot(memoryID)
	require.NoError(t, err)
	assert.Equal(t, scope.BallotCommitted, acceptedBallot.State, "later deprecation does not erase the original acceptance proof")
	rebuilt, err = app.RebuildScopedProjection(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, rebuilt)
	projected, err = projection.GetMemory(context.Background(), memoryID)
	require.NoError(t, err)
	assert.Equal(t, memory.StatusDeprecated, projected.Status)
}
