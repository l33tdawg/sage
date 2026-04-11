package governance

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Mock GovStore ---

type mockGovStore struct {
	data map[string][]byte
}

func newMockGovStore() *mockGovStore {
	return &mockGovStore{data: make(map[string][]byte)}
}

func (m *mockGovStore) GetState(key string) ([]byte, error) {
	v, ok := m.data[key]
	if !ok {
		return nil, nil
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, nil
}

func (m *mockGovStore) SetState(key string, value []byte) error {
	cp := make([]byte, len(value))
	copy(cp, value)
	m.data[key] = cp
	return nil
}

func (m *mockGovStore) DeleteState(key string) error {
	delete(m.data, key)
	return nil
}

func (m *mockGovStore) PrefixKeys(prefix string) ([]string, error) {
	var keys []string
	for k := range m.data {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

// --- Mock ValidatorProvider ---

type mockValidatorProvider struct {
	vals map[string]int64
}

func newMockValidatorProvider(vals map[string]int64) *mockValidatorProvider {
	return &mockValidatorProvider{vals: vals}
}

func (m *mockValidatorProvider) GetValidator(id string) (int64, bool) {
	p, ok := m.vals[id]
	return p, ok
}

func (m *mockValidatorProvider) GetAll() map[string]int64 {
	cp := make(map[string]int64, len(m.vals))
	for k, v := range m.vals {
		cp[k] = v
	}
	return cp
}

func (m *mockValidatorProvider) Size() int {
	return len(m.vals)
}

// --- Helper ---

func makeEngine(vals map[string]int64) (*Engine, *mockGovStore, *mockValidatorProvider) {
	store := newMockGovStore()
	vp := newMockValidatorProvider(vals)
	eng := NewEngine(store, vp)
	return eng, store, vp
}

// --- Tests ---

func TestFullProposalLifecycle(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{
		"val-a": 10,
		"val-b": 10,
		"val-c": 10,
	})

	// Propose: val-a proposes adding a new validator.
	proposalID, err := eng.Propose("val-a", OpAddValidator, "new-val", []byte("pubkey"), 5, 0, "add new validator", 100)
	require.NoError(t, err)
	require.NotEmpty(t, proposalID)

	// Active proposal should exist.
	active, err := eng.GetActiveProposal()
	require.NoError(t, err)
	require.NotNil(t, active)
	assert.Equal(t, proposalID, active.ProposalID)
	assert.Equal(t, StatusVoting, active.Status)

	// val-b votes accept.
	require.NoError(t, eng.Vote(proposalID, "val-b", "accept", 105))

	// ProcessBlock at height 110 (>= 100 + MinVotingBlocks=10).
	// 2 of 3 accepted (20/30), 20*3=60 >= 30*2=60 => passed.
	executed, err := eng.ProcessBlock(110)
	require.NoError(t, err)
	require.NotNil(t, executed)
	assert.Equal(t, StatusExecuted, executed.Status)
	assert.Equal(t, proposalID, executed.ProposalID)

	// No active proposal anymore.
	active, err = eng.GetActiveProposal()
	require.NoError(t, err)
	assert.Nil(t, active)
}

func TestSingleNodeAutoApprove(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{
		"sole-val": 10,
	})

	// Propose: sole validator proposes adding a new validator.
	_, err := eng.Propose("sole-val", OpAddValidator, "new-val", []byte("pk"), 3, 0, "bootstrap network", 50)
	require.NoError(t, err)

	// ProcessBlock: single validator, so MinVotingBlocks is skipped.
	// sole-val auto-voted accept. 10*3=30 >= 10*2=20 => passed.
	executed, err := eng.ProcessBlock(50)
	require.NoError(t, err)
	require.NotNil(t, executed)
	assert.Equal(t, StatusExecuted, executed.Status)
}

func TestProposalExpiry(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{
		"val-a": 10,
		"val-b": 10,
		"val-c": 10,
	})

	// Propose with default expiry (100 blocks).
	proposalID, err := eng.Propose("val-a", OpAddValidator, "new-val", []byte("pk"), 5, 0, "test expiry", 100)
	require.NoError(t, err)

	// Advance past expiry: 100 + 100 = 200, so height 201 should expire.
	executed, err := eng.ProcessBlock(201)
	require.NoError(t, err)
	assert.Nil(t, executed, "expired proposal should not be executed")

	// Proposal should be marked expired.
	active, err := eng.GetActiveProposal()
	require.NoError(t, err)
	assert.Nil(t, active, "no active proposal after expiry")

	// Verify the auto-vote was recorded before expiry.
	votes, err := eng.GetProposalVotes(proposalID)
	require.NoError(t, err)
	assert.Equal(t, "accept", votes["val-a"]) // auto-vote was recorded
}

func TestProposalRejection(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{
		"val-a": 10,
		"val-b": 10,
		"val-c": 10,
	})

	proposalID, err := eng.Propose("val-a", OpAddValidator, "new-val", []byte("pk"), 5, 0, "test rejection", 100)
	require.NoError(t, err)

	// val-b and val-c reject: rejectPower=20, 20*3=60 > 30 => rejected.
	require.NoError(t, eng.Vote(proposalID, "val-b", "reject", 105))
	require.NoError(t, eng.Vote(proposalID, "val-c", "reject", 105))

	executed, err := eng.ProcessBlock(111)
	require.NoError(t, err)
	assert.Nil(t, executed, "rejected proposal should not be executed")

	// No active proposal.
	active, err := eng.GetActiveProposal()
	require.NoError(t, err)
	assert.Nil(t, active)
}

func TestProposerCooldown(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{
		"val-a": 10,
		"val-b": 10,
		"val-c": 10,
	})

	// First proposal succeeds.
	proposalID, err := eng.Propose("val-a", OpAddValidator, "new-val", []byte("pk"), 5, 0, "first", 100)
	require.NoError(t, err)

	// Execute it so we can try another.
	require.NoError(t, eng.Vote(proposalID, "val-b", "accept", 105))
	executed, err := eng.ProcessBlock(111)
	require.NoError(t, err)
	require.NotNil(t, executed)

	// Second proposal within cooldown (100 + 50 = 150). Height 140 < 150.
	_, err = eng.Propose("val-a", OpAddValidator, "another-val", []byte("pk2"), 5, 0, "too soon", 140)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cooldown")

	// After cooldown expires: height 150 >= 100 + 50.
	_, err = eng.Propose("val-a", OpAddValidator, "another-val", []byte("pk2"), 5, 0, "after cooldown", 150)
	require.NoError(t, err)
}

func TestPowerConstraint(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{
		"val-a": 10,
		"val-b": 10,
		"val-c": 10,
	})

	// Total power = 30. New validator power must satisfy: targetPower*3 <= 30.
	// Power 11: 11*3=33 > 30 => rejected.
	_, err := eng.Propose("val-a", OpAddValidator, "new-val", []byte("pk"), 11, 0, "too much power", 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds 1/3")

	// Power 10: 10*3=30 <= 30 => allowed.
	_, err = eng.Propose("val-a", OpAddValidator, "new-val", []byte("pk"), 10, 0, "just right", 100)
	require.NoError(t, err)
}

func TestMinValidatorsConstraint(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{
		"val-a": 10,
		"val-b": 10,
	})

	// With only 2 validators, removal should be rejected.
	_, err := eng.Propose("val-a", OpRemoveValidator, "val-b", nil, 0, 0, "remove one of two", 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "minimum 2 validators")
}

func TestDuplicateVoteRejected(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{
		"val-a": 10,
		"val-b": 10,
		"val-c": 10,
	})

	proposalID, err := eng.Propose("val-a", OpAddValidator, "new-val", []byte("pk"), 5, 0, "test dup", 100)
	require.NoError(t, err)

	// val-a already auto-voted. Voting again should fail.
	err = eng.Vote(proposalID, "val-a", "reject", 105)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already voted")

	// val-b votes once, then tries again.
	require.NoError(t, eng.Vote(proposalID, "val-b", "accept", 105))
	err = eng.Vote(proposalID, "val-b", "reject", 106)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already voted")
}

func TestNonValidatorVoteRejected(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{
		"val-a": 10,
		"val-b": 10,
		"val-c": 10,
	})

	proposalID, err := eng.Propose("val-a", OpAddValidator, "new-val", []byte("pk"), 5, 0, "test non-val", 100)
	require.NoError(t, err)

	err = eng.Vote(proposalID, "outsider", "accept", 105)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not a validator")
}

func TestCancelByProposer(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{
		"val-a": 10,
		"val-b": 10,
		"val-c": 10,
	})

	proposalID, err := eng.Propose("val-a", OpAddValidator, "new-val", []byte("pk"), 5, 0, "to cancel", 100)
	require.NoError(t, err)

	// Cancel by proposer.
	require.NoError(t, eng.Cancel(proposalID, "val-a", 105))

	// No active proposal.
	active, err := eng.GetActiveProposal()
	require.NoError(t, err)
	assert.Nil(t, active)
}

func TestCancelByNonProposerRejected(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{
		"val-a": 10,
		"val-b": 10,
		"val-c": 10,
	})

	proposalID, err := eng.Propose("val-a", OpAddValidator, "new-val", []byte("pk"), 5, 0, "no cancel", 100)
	require.NoError(t, err)

	err = eng.Cancel(proposalID, "val-b", 105)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "only the proposer")

	// Proposal should still be active.
	active, err := eng.GetActiveProposal()
	require.NoError(t, err)
	require.NotNil(t, active)
}

func TestNoActiveProposal_ProcessBlockReturnsNil(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{
		"val-a": 10,
	})

	executed, err := eng.ProcessBlock(100)
	require.NoError(t, err)
	assert.Nil(t, executed)
}

func TestMinVotingBlocksEnforcement(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{
		"val-a": 10,
		"val-b": 10,
		"val-c": 10,
	})

	proposalID, err := eng.Propose("val-a", OpAddValidator, "new-val", []byte("pk"), 5, 0, "test min blocks", 100)
	require.NoError(t, err)

	// val-b votes accept immediately — now 2/3 have voted accept.
	require.NoError(t, eng.Vote(proposalID, "val-b", "accept", 101))

	// ProcessBlock at height 105: quorum reached BUT min voting period not met (100+10=110).
	executed, err := eng.ProcessBlock(105)
	require.NoError(t, err)
	assert.Nil(t, executed, "should not execute before MinVotingBlocks")

	// ProcessBlock at height 109: still before min period.
	executed, err = eng.ProcessBlock(109)
	require.NoError(t, err)
	assert.Nil(t, executed, "should not execute at height 109")

	// ProcessBlock at height 110: exactly at min period. Should execute.
	executed, err = eng.ProcessBlock(110)
	require.NoError(t, err)
	require.NotNil(t, executed, "should execute at height 110 (createdHeight 100 + MinVotingBlocks 10)")
	assert.Equal(t, StatusExecuted, executed.Status)
}

func TestActiveProposalBlocksNewProposal(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{
		"val-a": 10,
		"val-b": 10,
		"val-c": 10,
	})

	_, err := eng.Propose("val-a", OpAddValidator, "new-val", []byte("pk"), 5, 0, "first", 100)
	require.NoError(t, err)

	// val-b tries to propose while val-a's proposal is active.
	_, err = eng.Propose("val-b", OpAddValidator, "another", []byte("pk2"), 5, 0, "blocked", 101)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "active proposal already exists")
}

func TestExpiryBounds(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{
		"val-a": 10,
	})

	// Below minimum.
	_, err := eng.Propose("val-a", OpAddValidator, "new-val", []byte("pk"), 3, MinExpiryBlocks-1, "too short", 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "below minimum")

	// Above maximum.
	_, err = eng.Propose("val-a", OpAddValidator, "new-val", []byte("pk"), 3, MaxExpiryBlocks+1, "too long", 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum")
}

func TestVoteOnExpiredProposal(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{
		"val-a": 10,
		"val-b": 10,
		"val-c": 10,
	})

	proposalID, err := eng.Propose("val-a", OpAddValidator, "new-val", []byte("pk"), 5, 10, "short expiry", 100)
	require.NoError(t, err)

	// Try to vote after expiry (100 + 10 = 110).
	err = eng.Vote(proposalID, "val-b", "accept", 111)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expired")
}

func TestVoteOnWrongProposal(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{
		"val-a": 10,
		"val-b": 10,
	})

	_, err := eng.Propose("val-a", OpAddValidator, "new-val", []byte("pk"), 3, 0, "active", 100)
	require.NoError(t, err)

	err = eng.Vote("nonexistent-id", "val-b", "accept", 105)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not the active proposal")
}

func TestInvalidVoteDecision(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{
		"val-a": 10,
		"val-b": 10,
	})

	proposalID, err := eng.Propose("val-a", OpAddValidator, "new-val", []byte("pk"), 3, 0, "test", 100)
	require.NoError(t, err)

	err = eng.Vote(proposalID, "val-b", "maybe", 105)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid decision")
}

func TestGetProposalVotes(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{
		"val-a": 10,
		"val-b": 10,
		"val-c": 10,
	})

	proposalID, err := eng.Propose("val-a", OpAddValidator, "new-val", []byte("pk"), 5, 0, "test votes", 100)
	require.NoError(t, err)

	require.NoError(t, eng.Vote(proposalID, "val-b", "reject", 105))
	require.NoError(t, eng.Vote(proposalID, "val-c", "abstain", 106))

	votes, err := eng.GetProposalVotes(proposalID)
	require.NoError(t, err)
	assert.Equal(t, "accept", votes["val-a"])  // auto-vote
	assert.Equal(t, "reject", votes["val-b"])
	assert.Equal(t, "abstain", votes["val-c"])
	assert.Len(t, votes, 3)
}

func TestComputeProposalID_Deterministic(t *testing.T) {
	id1 := ComputeProposalID("val-a", 100, OpAddValidator, "new-val")
	id2 := ComputeProposalID("val-a", 100, OpAddValidator, "new-val")
	assert.Equal(t, id1, id2, "same inputs should produce same ID")
	assert.Len(t, id1, 32)

	// Different inputs should produce different IDs.
	id3 := ComputeProposalID("val-a", 101, OpAddValidator, "new-val")
	assert.NotEqual(t, id1, id3)

	id4 := ComputeProposalID("val-b", 100, OpAddValidator, "new-val")
	assert.NotEqual(t, id1, id4)

	id5 := ComputeProposalID("val-a", 100, OpRemoveValidator, "new-val")
	assert.NotEqual(t, id1, id5)
}

func TestUpdatePowerOperation(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{
		"val-a": 10,
		"val-b": 10,
		"val-c": 10,
	})

	// UpdatePower on existing validator should succeed.
	proposalID, err := eng.Propose("val-a", OpUpdatePower, "val-b", nil, 20, 0, "boost val-b", 100)
	require.NoError(t, err)
	require.NotEmpty(t, proposalID)

	// UpdatePower on non-existent validator should fail.
	// First, execute the current proposal.
	require.NoError(t, eng.Vote(proposalID, "val-b", "accept", 105))
	_, err = eng.ProcessBlock(111)
	require.NoError(t, err)

	// Wait for cooldown.
	_, err = eng.Propose("val-a", OpUpdatePower, "nonexistent", nil, 20, 0, "bad target", 200)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestCancelSetsCooldown(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{
		"val-a": 10,
		"val-b": 10,
		"val-c": 10,
	})

	proposalID, err := eng.Propose("val-a", OpAddValidator, "new-val", []byte("pk"), 5, 0, "to cancel", 100)
	require.NoError(t, err)

	// Cancel at height 110.
	require.NoError(t, eng.Cancel(proposalID, "val-a", 110))

	// Try to propose again within cooldown (110 + 50 = 160). Height 150 < 160.
	_, err = eng.Propose("val-a", OpAddValidator, "new-val2", []byte("pk2"), 5, 0, "too soon", 150)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cooldown")

	// After cooldown: height 160 >= 110 + 50.
	_, err = eng.Propose("val-a", OpAddValidator, "new-val2", []byte("pk2"), 5, 0, "ok now", 160)
	require.NoError(t, err)
}

func TestProcessBlock_MultipleBlocks_EventualExecution(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{
		"val-a": 10,
		"val-b": 10,
		"val-c": 10,
	})

	proposalID, err := eng.Propose("val-a", OpAddValidator, "new-val", []byte("pk"), 5, 0, "gradual", 100)
	require.NoError(t, err)

	// Process several blocks — no quorum yet (only val-a auto-voted).
	for h := int64(110); h <= 115; h++ {
		executed, pErr := eng.ProcessBlock(h)
		require.NoError(t, pErr)
		assert.Nil(t, executed, fmt.Sprintf("should not execute at height %d without quorum", h))
	}

	// val-b votes accept.
	require.NoError(t, eng.Vote(proposalID, "val-b", "accept", 116))

	// Now ProcessBlock should execute.
	executed, err := eng.ProcessBlock(117)
	require.NoError(t, err)
	require.NotNil(t, executed)
	assert.Equal(t, StatusExecuted, executed.Status)
}
