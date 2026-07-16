//go:build multiprocess

package abci

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/scope"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"
)

// This file is deliberately build-tagged. It launches copies of the current Go
// test executable as independent SAGE processes, each with its own Badger and
// SQLite files. Run it through deploy/scripts/run-v11.9-multiprocess.sh.
//
// The parent is a deterministic block-delivery harness, not a CometBFT/P2P
// emulator. Withholding blocks from a live child proves application behavior
// across a transport partition and ordered catch-up. It does not claim TCP
// partition, validator-only peer authorization, or ABCI state-sync endpoint
// coverage; those remain endpoint-enablement release gates.

const (
	v119WorkerPlanEnv = "SAGE_V119_MULTIPROCESS_PLAN"
	v119ScopeID       = "scope-v11-9-multiprocess"
	v119Domain        = "research"
)

type v119FaultBlock struct {
	Height   int64    `json:"height"`
	UnixTime int64    `json:"unix_time"`
	Txs      [][]byte `json:"txs,omitempty"`
}

type v119WorkerPlan struct {
	DataDir          string           `json:"data_dir"`
	ResultPath       string           `json:"result_path"`
	Blocks           []v119FaultBlock `json:"blocks,omitempty"`
	InspectMemoryIDs []string         `json:"inspect_memory_ids,omitempty"`
	CrashAfterBlock  int64            `json:"crash_after_block,omitempty"`
	Hold             bool             `json:"hold,omitempty"`
}

type v119BlockResult struct {
	Height           int64    `json:"height"`
	AppHash          string   `json:"app_hash"`
	TxCodes          []uint32 `json:"tx_codes,omitempty"`
	TxData           [][]byte `json:"tx_data,omitempty"`
	ValidatorUpdates int      `json:"validator_updates"`
}

type v119BallotSummary struct {
	MemoryID      string `json:"memory_id"`
	ScopeRevision uint64 `json:"scope_revision"`
	MemberCount   int    `json:"member_count"`
	TotalWeight   uint64 `json:"total_weight"`
	State         byte   `json:"state"`
	MemoryStatus  string `json:"memory_status"`
}

type v119WorkerResult struct {
	Blocks         []v119BlockResult   `json:"blocks,omitempty"`
	Height         int64               `json:"height"`
	AppHash        string              `json:"app_hash"`
	AppVersion     uint64              `json:"app_version"`
	ValidatorCount int                 `json:"validator_count"`
	ScopeRevision  uint64              `json:"scope_revision"`
	Ballots        []v119BallotSummary `json:"ballots,omitempty"`
}

func v119Validators() []agentKey {
	keys := []agentKey{
		deterministicScopedAgent(1),
		deterministicScopedAgent(33),
		deterministicScopedAgent(65),
		deterministicScopedAgent(97),
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].id < keys[j].id })
	return keys
}

// v119Operator is deliberately not a validator. It supplies the embedded
// app-v20 request authorization while each outer validator key independently
// owns proposal identity, nonce, and voting power.
func v119Operator() agentKey {
	return deterministicScopedAgent(161)
}

func v119OpenWorkerApp(t *testing.T, dataDir string) *SageApp {
	t.Helper()
	require.NoError(t, os.MkdirAll(dataDir, 0o700))
	badgerDir := filepath.Join(dataDir, "badger")
	projectionPath := filepath.Join(dataDir, "projection.db")
	initializedPath := filepath.Join(dataDir, "initialized")
	_, statErr := os.Stat(initializedPath)
	fresh := errors.Is(statErr, os.ErrNotExist)
	require.True(t, statErr == nil || fresh, "stat worker marker: %v", statErr)

	badgerStore, err := store.NewBadgerStore(badgerDir)
	require.NoError(t, err)
	projection, err := store.NewSQLiteStore(context.Background(), projectionPath)
	if err != nil {
		_ = badgerStore.CloseBadger()
		require.NoError(t, err)
	}
	app, err := NewSageAppWithStores(badgerStore, projection, zerolog.Nop())
	if err != nil {
		_ = projection.Close()
		_ = badgerStore.CloseBadger()
		require.NoError(t, err)
	}

	if !fresh {
		return app
	}

	keys := v119Validators()
	for i, key := range keys {
		register := makeAgentRegisterTx(t, key, fmt.Sprintf("validator-%d", i), "member", "v11.9 fault worker", "test", "")
		result := app.processAgentRegister(register, 1, time.Unix(1, 0).UTC())
		require.Zero(t, result.Code, result.Log)
		require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{
			ID: key.id, PublicKey: key.pub, Power: 10,
		}))
	}
	operator := v119Operator()
	operatorRegister := makeAgentRegisterTx(t, operator, "operator-admin", "admin", "v11.9 fault worker", "test", "")
	operatorResult := app.processAgentRegister(operatorRegister, 1, time.Unix(1, 0).UTC())
	require.Zero(t, operatorResult.Code, operatorResult.Log)
	powers := make(map[string]int64, len(keys))
	for _, key := range keys {
		powers[key.id] = 10
	}
	require.NoError(t, app.badgerStore.SaveValidators(powers))
	require.NoError(t, app.badgerStore.RegisterDomain(v119Domain, keys[0].id, "", 1))
	require.NoError(t, app.badgerStore.SetAccessGrant(v119Domain, keys[0].id, 2, 0, keys[0].id))
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV20UpgradeName, 20, 1))
	seedTestGovernanceDelegationDomain(t, app.badgerStore)
	app.appV20AppliedHeight = 1

	// Initialization is deterministic consensus seed state. Do not carry its
	// direct helper writes into the first scenario block's projection batch.
	app.pendingWrites = nil
	resp, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 1,
		Time:   time.Unix(1, 0).UTC(),
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.AppHash)
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(initializedPath, []byte("v1\n"), 0o600))
	return app
}

func v119SummarizeWorker(t *testing.T, app *SageApp, result *v119WorkerResult, memoryIDs []string) {
	t.Helper()
	info, err := app.Info(context.Background(), &abcitypes.RequestInfo{})
	require.NoError(t, err)
	result.Height = info.LastBlockHeight
	result.AppHash = hex.EncodeToString(info.LastBlockAppHash)
	result.AppVersion = info.AppVersion
	result.ValidatorCount = app.validators.Size()
	record, err := app.badgerStore.GetScopeRecord(v119ScopeID)
	require.NoError(t, err)
	if record != nil {
		result.ScopeRevision = record.Revision
	}
	for _, memoryID := range memoryIDs {
		ballot, ballotErr := app.badgerStore.GetScopeBallot(memoryID)
		require.NoError(t, ballotErr)
		if ballot == nil {
			continue
		}
		_, status, statusErr := app.badgerStore.GetMemoryHash(memoryID)
		require.NoError(t, statusErr)
		result.Ballots = append(result.Ballots, v119BallotSummary{
			MemoryID: memoryID, ScopeRevision: ballot.ScopeRevision,
			MemberCount: len(ballot.Members), TotalWeight: ballot.TotalWeight,
			State: byte(ballot.State), MemoryStatus: status,
		})
	}
}

func v119WriteWorkerResult(t *testing.T, path string, result v119WorkerResult) {
	t.Helper()
	data, err := json.Marshal(result)
	require.NoError(t, err)
	tmp := path + ".tmp"
	require.NoError(t, os.WriteFile(tmp, data, 0o600))
	require.NoError(t, os.Rename(tmp, path))
}

// TestV119MultiProcessWorker is invoked only as a subprocess by the parent
// harness. A Hold worker remains alive with its database open while the parent
// withholds blocks. A crash worker publishes the speculative FinalizeBlock
// response alongside a summary of the still-committed app graph, then waits for
// the parent to SIGKILL it before the atomic Commit.
func TestV119MultiProcessWorker(t *testing.T) {
	planPath := os.Getenv(v119WorkerPlanEnv)
	if planPath == "" {
		t.Skip("subprocess worker only")
	}
	data, err := os.ReadFile(planPath)
	require.NoError(t, err)
	var plan v119WorkerPlan
	require.NoError(t, json.Unmarshal(data, &plan))
	require.NotEmpty(t, plan.DataDir)
	require.NotEmpty(t, plan.ResultPath)

	app := v119OpenWorkerApp(t, plan.DataDir)
	defer func() { require.NoError(t, app.Close()) }()
	result := v119WorkerResult{}
	if plan.Hold {
		v119SummarizeWorker(t, app, &result, plan.InspectMemoryIDs)
		v119WriteWorkerResult(t, plan.ResultPath, result)
		for {
			time.Sleep(time.Second)
		}
	}

	for _, block := range plan.Blocks {
		resp, finalizeErr := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
			Height: block.Height,
			Time:   time.Unix(block.UnixTime, 0).UTC(),
			Txs:    block.Txs,
		})
		require.NoError(t, finalizeErr)
		blockResult := v119BlockResult{
			Height: block.Height, AppHash: hex.EncodeToString(resp.AppHash),
			ValidatorUpdates: len(resp.ValidatorUpdates),
		}
		for _, txResult := range resp.TxResults {
			blockResult.TxCodes = append(blockResult.TxCodes, txResult.Code)
			blockResult.TxData = append(blockResult.TxData, append([]byte(nil), txResult.Data...))
		}
		result.Blocks = append(result.Blocks, blockResult)
		if plan.CrashAfterBlock == block.Height {
			// The block result above is speculative. The summary deliberately
			// reads the committed root app, whose height/validators/ballots must
			// remain unchanged until Commit publishes the cloned graph.
			v119SummarizeWorker(t, app, &result, plan.InspectMemoryIDs)
			v119WriteWorkerResult(t, plan.ResultPath, result)
			for {
				time.Sleep(time.Second)
			}
		}
		_, commitErr := app.Commit(context.Background(), &abcitypes.RequestCommit{})
		require.NoError(t, commitErr)
	}
	v119SummarizeWorker(t, app, &result, plan.InspectMemoryIDs)
	v119WriteWorkerResult(t, plan.ResultPath, result)
}

type v119Child struct {
	cmd        *exec.Cmd
	resultPath string
	output     *lockedBuffer
}

// lockedBuffer is enough for concurrent stdout/stderr capture without pulling
// in a process-management dependency.
type lockedBuffer struct {
	mu   sync.Mutex
	data []byte
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.data)
}

func v119StartChild(t *testing.T, plan v119WorkerPlan) *v119Child {
	t.Helper()
	planRoot := t.TempDir()
	planPath := filepath.Join(planRoot, "plan.json")
	plan.ResultPath = filepath.Join(planRoot, "result.json")
	data, err := json.Marshal(plan)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(planPath, data, 0o600))

	childTimeout := 30 * time.Second
	if plan.Hold || plan.CrashAfterBlock != 0 {
		// Hold spans the complete partition/reconfiguration scenario, while crash
		// workers must remain alive until the parent observes their durable marker
		// and delivers SIGKILL. Keep both below the outer 240s test bound without
		// inheriting the short normal-batch startup limit.
		childTimeout = 3 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), childTimeout)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestV119MultiProcessWorker$", "-test.count=1")
	cmd.Env = append(os.Environ(), v119WorkerPlanEnv+"="+planPath)
	output := &lockedBuffer{}
	cmd.Stdout = output
	cmd.Stderr = output
	require.NoError(t, cmd.Start())
	return &v119Child{cmd: cmd, resultPath: plan.ResultPath, output: output}
}

func v119ReadResult(t *testing.T, path string) v119WorkerResult {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var result v119WorkerResult
	require.NoError(t, json.Unmarshal(data, &result))
	return result
}

func v119WaitNormalChild(t *testing.T, child *v119Child) v119WorkerResult {
	t.Helper()
	if err := child.cmd.Wait(); err != nil {
		t.Fatalf("worker failed: %v\n%s", err, child.output.String())
	}
	return v119ReadResult(t, child.resultPath)
}

func v119WaitReadyAndKill(t *testing.T, child *v119Child) v119WorkerResult {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := os.Stat(child.resultPath); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stat worker result: %v", err)
		}
		if time.Now().After(deadline) {
			_ = child.cmd.Process.Kill()
			_ = child.cmd.Wait()
			t.Fatalf("worker did not reach kill point\n%s", child.output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	result := v119ReadResult(t, child.resultPath)
	require.NoError(t, child.cmd.Process.Kill())
	waitErr := child.cmd.Wait()
	require.Error(t, waitErr, "SIGKILL worker must not exit cleanly")
	return result
}

func v119RunBatch(t *testing.T, plans []v119WorkerPlan) []v119WorkerResult {
	t.Helper()
	children := make([]*v119Child, len(plans))
	for i := range plans {
		children[i] = v119StartChild(t, plans[i])
	}
	results := make([]v119WorkerResult, len(children))
	for i, child := range children {
		results[i] = v119WaitNormalChild(t, child)
	}
	return results
}

func v119Block(height int64, raws ...[]byte) v119FaultBlock {
	return v119FaultBlock{Height: height, UnixTime: 1_000 + height, Txs: raws}
}

func v119RequireSameFinalState(t *testing.T, results []v119WorkerResult) {
	t.Helper()
	require.NotEmpty(t, results)
	want := results[0]
	for i := 1; i < len(results); i++ {
		assert.Equal(t, want.Height, results[i].Height, "replica %d height", i)
		assert.Equal(t, want.AppHash, results[i].AppHash, "replica %d AppHash", i)
		assert.Equal(t, want.AppVersion, results[i].AppVersion, "replica %d app version", i)
		assert.Equal(t, want.ValidatorCount, results[i].ValidatorCount, "replica %d validator count", i)
	}
}

func v119RequireBlockCodesZero(t *testing.T, result v119WorkerResult) {
	t.Helper()
	for _, block := range result.Blocks {
		for i, code := range block.TxCodes {
			require.Zero(t, code, "height %d tx %d", block.Height, i)
		}
	}
}

func v119Ballot(t *testing.T, result v119WorkerResult, memoryID string) v119BallotSummary {
	t.Helper()
	for _, ballot := range result.Ballots {
		if ballot.MemoryID == memoryID {
			return ballot
		}
	}
	t.Fatalf("ballot %s not found", memoryID)
	return v119BallotSummary{}
}

func v119SignedMemorySubmit(t *testing.T, signer agentKey, nonce uint64, content string) []byte {
	t.Helper()
	parsed := makeMemorySubmitTx(t, signer, v119Domain, content)
	parsed.Nonce = nonce
	return signedV20Tx(t, parsed, signer)
}

func v119SignedMemoryVote(t *testing.T, signer agentKey, nonce uint64, memoryID string) []byte {
	t.Helper()
	return signedV20Tx(t, &tx.ParsedTx{
		Type: tx.TxTypeMemoryVote, Nonce: nonce,
		MemoryVote: &tx.MemoryVote{MemoryID: memoryID, Decision: tx.VoteDecisionAccept},
	}, signer)
}

func v119ScopeProposalTemplate(keys []agentKey, revision uint64, memberCount int) scope.ProposalTemplate {
	members := make([]scope.ProposalMember, 0, memberCount)
	for _, key := range keys[:memberCount] {
		members = append(members, scope.ProposalMember{
			ValidatorID: key.id, AssignedWeight: 1, JoinedRevision: 1,
		})
	}
	return scope.ProposalTemplate{
		ScopeID: v119ScopeID, Revision: revision, State: "active",
		ControllerValidatorID: keys[0].id,
		Domains:               []string{v119Domain},
		Members:               members,
	}
}

func v119DelegatedScopeProposal(
	t *testing.T,
	authorizer, outer agentKey,
	template scope.ProposalTemplate,
	nonce uint64,
	height int64,
	proofNonce string,
) []byte {
	t.Helper()
	blockTime := time.Unix(1_000+height, 0).UTC()
	reason := "multiprocess signed scope formation or revision"
	payload, err := scope.EncodeProposalTemplate(template)
	require.NoError(t, err)
	body, err := json.Marshal(struct {
		ValidatorID      string                  `json:"validator_id"`
		GovernanceDomain string                  `json:"governance_domain"`
		Operation        string                  `json:"operation"`
		Reason           string                  `json:"reason"`
		Scope            *scope.ProposalTemplate `json:"scope"`
	}{
		ValidatorID: outer.id, GovernanceDomain: governanceReplayTestDomain,
		Operation: "scope_action", Reason: reason, Scope: &template,
	})
	require.NoError(t, err)
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeGovPropose, Nonce: nonce, Timestamp: blockTime,
		GovPropose: &tx.GovPropose{
			Operation: tx.GovOpScopeAction, TargetID: template.ScopeID,
			Reason: reason, Payload: payload,
		},
	}
	attachGovernanceRequestProof(
		t, parsed, authorizer, outer, "POST", "/v1/governance/propose",
		body, blockTime, []byte(proofNonce),
	)
	encoded, err := tx.EncodeTx(parsed)
	require.NoError(t, err)
	return encoded
}

func v119DelegatedAddValidator(t *testing.T, authorizer, outer, candidate agentKey, nonce uint64, height int64, proofNonce string) []byte {
	t.Helper()
	blockTime := time.Unix(1_000+height, 0).UTC()
	reason := "multiprocess delegated validator addition"
	body, err := json.Marshal(map[string]any{
		"validator_id":      outer.id,
		"governance_domain": governanceReplayTestDomain,
		"operation":         "add_validator",
		"target_id":         candidate.id,
		"target_pubkey":     candidate.id,
		"target_power":      5,
		"reason":            reason,
	})
	require.NoError(t, err)
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeGovPropose, Nonce: nonce, Timestamp: blockTime,
		GovPropose: &tx.GovPropose{
			Operation: tx.GovOpAddValidator, TargetID: candidate.id,
			TargetPubKey: candidate.pub, TargetPower: 5, Reason: reason,
		},
	}
	attachGovernanceRequestProof(
		t, parsed, authorizer, outer, "POST", "/v1/governance/propose",
		body, blockTime, []byte(proofNonce),
	)
	encoded, err := tx.EncodeTx(parsed)
	require.NoError(t, err)
	return encoded
}

func v119DelegatedGovernanceVote(t *testing.T, authorizer, outer agentKey, proposalID string, nonce uint64, height int64, proofNonce string) []byte {
	t.Helper()
	blockTime := time.Unix(1_000+height, 0).UTC()
	body, err := json.Marshal(map[string]any{
		"validator_id":      outer.id,
		"governance_domain": governanceReplayTestDomain,
		"proposal_id":       proposalID,
		"decision":          "accept",
	})
	require.NoError(t, err)
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeGovVote, Nonce: nonce, Timestamp: blockTime,
		GovVote: &tx.GovVote{ProposalID: proposalID, Decision: tx.VoteDecisionAccept},
	}
	attachGovernanceRequestProof(
		t, parsed, authorizer, outer, "POST", "/v1/governance/vote",
		body, blockTime, []byte(proofNonce),
	)
	encoded, err := tx.EncodeTx(parsed)
	require.NoError(t, err)
	return encoded
}

// TestV119SignedScopeReconfigurationFaultHarness proves the application-level
// release-gate invariants with real OS processes and independent databases:
//   - a live replica can miss a bounded block interval and catch up exactly;
//   - SIGKILL after FinalizeBlock/fsync but before Commit replays cleanly;
//   - a non-validator operator and outer validator keys form and revise the
//     scope through real app-v20 dual-principal governance envelopes;
//   - a 4-member ballot keeps its denominator across a prospective 3-member
//     scope revision, while new ballots use only the new revision;
//   - 3/4 commits and 2/3 remains pending until the final vote.
func TestV119SignedScopeReconfigurationFaultHarness(t *testing.T) {
	if os.Getenv(v119WorkerPlanEnv) != "" {
		t.Skip("parent harness only")
	}
	keys := v119Validators()
	replicaDirs := make([]string, 4)
	initPlans := make([]v119WorkerPlan, 4)
	for i := range replicaDirs {
		replicaDirs[i] = filepath.Join(t.TempDir(), fmt.Sprintf("replica-%d", i))
		initPlans[i] = v119WorkerPlan{DataDir: replicaDirs[i]}
	}
	initialized := v119RunBatch(t, initPlans)
	v119RequireSameFinalState(t, initialized)
	require.Equal(t, int64(1), initialized[0].Height)
	require.Equal(t, uint64(20), initialized[0].AppVersion)
	require.Zero(t, initialized[0].ScopeRevision, "scope must be formed by signed governance, not fixture seeding")
	require.Equal(t, 4, initialized[0].ValidatorCount)

	operator := v119Operator()
	formationTemplate := v119ScopeProposalTemplate(keys, 1, 4)
	formationProposal := v119DelegatedScopeProposal(t, operator, keys[0], formationTemplate, 1, 2, "scopef01")
	formationProposalID := governance.ComputeProposalID(keys[0].id, 2, governance.OpScopeAction, v119ScopeID)
	formationVote1 := v119DelegatedGovernanceVote(t, operator, keys[1], formationProposalID, 1, 3, "scopef02")
	formationVote2 := v119DelegatedGovernanceVote(t, operator, keys[2], formationProposalID, 1, 4, "scopef03")
	formationBlocks := []v119FaultBlock{
		v119Block(2, formationProposal),
		v119Block(3, formationVote1),
		v119Block(4, formationVote2),
		v119Block(5), v119Block(6), v119Block(7), v119Block(8),
		v119Block(9), v119Block(10), v119Block(11), v119Block(12),
	}
	formationPlans := make([]v119WorkerPlan, 4)
	for i := range formationPlans {
		formationPlans[i] = v119WorkerPlan{DataDir: replicaDirs[i], Blocks: formationBlocks}
	}
	formed := v119RunBatch(t, formationPlans)
	v119RequireSameFinalState(t, formed)
	for _, result := range formed {
		v119RequireBlockCodesZero(t, result)
		require.Equal(t, uint64(1), result.ScopeRevision)
		require.Equal(t, 4, result.ValidatorCount, "the embedded operator must not gain validator power")
	}

	oldSubmit := v119SignedMemorySubmit(t, keys[0], 2, "four-member ballot survives a partition and reconfiguration")
	stageOnePlans := make([]v119WorkerPlan, 4)
	for i := range stageOnePlans {
		stageOnePlans[i] = v119WorkerPlan{DataDir: replicaDirs[i], Blocks: []v119FaultBlock{v119Block(13, oldSubmit)}}
	}
	stageOne := v119RunBatch(t, stageOnePlans)
	v119RequireSameFinalState(t, stageOne)
	for _, result := range stageOne {
		v119RequireBlockCodesZero(t, result)
	}
	require.Len(t, stageOne[0].Blocks, 1)
	require.Len(t, stageOne[0].Blocks[0].TxData, 1)
	oldMemoryID := string(stageOne[0].Blocks[0].TxData[0])
	require.NotEmpty(t, oldMemoryID)

	oldVote0 := v119SignedMemoryVote(t, keys[0], 3, oldMemoryID)
	oldVote1 := v119SignedMemoryVote(t, keys[1], 2, oldMemoryID)
	stageTwoBlocks := []v119FaultBlock{v119Block(14, oldVote0), v119Block(15, oldVote1)}
	stageTwoPlans := make([]v119WorkerPlan, 4)
	for i := range stageTwoPlans {
		stageTwoPlans[i] = v119WorkerPlan{DataDir: replicaDirs[i], Blocks: stageTwoBlocks, InspectMemoryIDs: []string{oldMemoryID}}
	}
	stageTwo := v119RunBatch(t, stageTwoPlans)
	v119RequireSameFinalState(t, stageTwo)
	oldPending := v119Ballot(t, stageTwo[0], oldMemoryID)
	assert.Equal(t, uint64(1), oldPending.ScopeRevision)
	assert.Equal(t, 4, oldPending.MemberCount)
	assert.Equal(t, byte(scope.BallotPending), oldPending.State)

	// Keep replica 3 alive with its database open but deliver it no blocks.
	// The parent is the deterministic transport, so this is the harness's
	// bounded partition interval. It intentionally makes no Comet TCP claim.
	partitioned := v119StartChild(t, v119WorkerPlan{
		DataDir: replicaDirs[3], Hold: true, InspectMemoryIDs: []string{oldMemoryID},
	})
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := os.Stat(partitioned.resultPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = partitioned.cmd.Process.Kill()
			_ = partitioned.cmd.Wait()
			t.Fatalf("partitioned replica did not become ready\n%s", partitioned.output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}

	nextScope := v119ScopeProposalTemplate(keys, 2, 3)
	proposeHeight := int64(16)
	propose := v119DelegatedScopeProposal(t, operator, keys[1], nextScope, 3, proposeHeight, "scoper01")
	proposalID := governance.ComputeProposalID(keys[1].id, proposeHeight, governance.OpScopeAction, v119ScopeID)
	voteProposal1 := v119DelegatedGovernanceVote(t, operator, keys[0], proposalID, 4, 17, "scoper02")
	voteProposal2 := v119DelegatedGovernanceVote(t, operator, keys[2], proposalID, 2, 18, "scoper03")
	throughReconfiguration := []v119FaultBlock{
		v119Block(16, propose),
		v119Block(17, voteProposal1),
		v119Block(18, voteProposal2),
		v119Block(19), v119Block(20), v119Block(21), v119Block(22),
		v119Block(23), v119Block(24), v119Block(25), v119Block(26),
	}
	onlinePlans := make([]v119WorkerPlan, 3)
	for i := range onlinePlans {
		onlinePlans[i] = v119WorkerPlan{
			DataDir: replicaDirs[i], Blocks: throughReconfiguration,
			InspectMemoryIDs: []string{oldMemoryID},
		}
	}
	online := v119RunBatch(t, onlinePlans)
	v119RequireSameFinalState(t, online)
	for _, result := range online {
		v119RequireBlockCodesZero(t, result)
		require.Equal(t, uint64(2), result.ScopeRevision)
		require.Equal(t, 4, result.ValidatorCount)
		pinned := v119Ballot(t, result, oldMemoryID)
		assert.Equal(t, uint64(1), pinned.ScopeRevision)
		assert.Equal(t, 4, pinned.MemberCount)
	}

	newSubmit := v119SignedMemorySubmit(t, keys[0], 5, "new ballot uses the prospective three-member roster")
	newSubmitPlans := []v119WorkerPlan{
		{DataDir: replicaDirs[0], Blocks: []v119FaultBlock{v119Block(27, newSubmit)}},
		{DataDir: replicaDirs[1], Blocks: []v119FaultBlock{v119Block(27, newSubmit)}},
	}
	newSubmitResults := v119RunBatch(t, newSubmitPlans)
	v119RequireSameFinalState(t, newSubmitResults)
	for _, result := range newSubmitResults {
		v119RequireBlockCodesZero(t, result)
	}
	newMemoryID := string(newSubmitResults[0].Blocks[0].TxData[0])
	require.NotEmpty(t, newMemoryID)

	// Replica 2 reaches the same FinalizeBlock result, then the parent sends a
	// real SIGKILL before Commit. Restarting it must replay height 27 exactly.
	crashing := v119StartChild(t, v119WorkerPlan{
		DataDir: replicaDirs[2], Blocks: []v119FaultBlock{v119Block(27, newSubmit)},
		CrashAfterBlock: 27,
	})
	crashResult := v119WaitReadyAndKill(t, crashing)
	require.Len(t, crashResult.Blocks, 1)
	assert.Equal(t, newSubmitResults[0].Blocks[0].AppHash, crashResult.Blocks[0].AppHash)
	// The killed process's in-memory Info already reflects FinalizeBlock. A
	// fresh process must reload the durable Commit height (26), which is the
	// signal that makes Comet replay height 27.
	reopenedBeforeReplay := v119RunBatch(t, []v119WorkerPlan{{DataDir: replicaDirs[2]}})[0]
	assert.Equal(t, int64(26), reopenedBeforeReplay.Height, "durable Commit height must remain behind after SIGKILL")
	replayed := v119RunBatch(t, []v119WorkerPlan{{
		DataDir: replicaDirs[2], Blocks: []v119FaultBlock{v119Block(27, newSubmit)},
	}})[0]
	v119RequireBlockCodesZero(t, replayed)
	assert.Equal(t, newSubmitResults[0].AppHash, replayed.AppHash)
	assert.Equal(t, int64(27), replayed.Height)

	newVote0 := v119SignedMemoryVote(t, keys[0], 6, newMemoryID)
	newVote1 := v119SignedMemoryVote(t, keys[1], 4, newMemoryID)
	twoOfThree := []v119FaultBlock{v119Block(28, newVote0), v119Block(29, newVote1)}
	twoOfThreePlans := make([]v119WorkerPlan, 3)
	for i := range twoOfThreePlans {
		twoOfThreePlans[i] = v119WorkerPlan{
			DataDir: replicaDirs[i], Blocks: twoOfThree,
			InspectMemoryIDs: []string{oldMemoryID, newMemoryID},
		}
	}
	twoOfThreeResults := v119RunBatch(t, twoOfThreePlans)
	v119RequireSameFinalState(t, twoOfThreeResults)
	for _, result := range twoOfThreeResults {
		v119RequireBlockCodesZero(t, result)
		oldBallot := v119Ballot(t, result, oldMemoryID)
		newBallot := v119Ballot(t, result, newMemoryID)
		assert.Equal(t, 4, oldBallot.MemberCount)
		assert.Equal(t, uint64(1), oldBallot.ScopeRevision)
		assert.Equal(t, byte(scope.BallotPending), oldBallot.State)
		assert.Equal(t, 3, newBallot.MemberCount)
		assert.Equal(t, uint64(2), newBallot.ScopeRevision)
		assert.Equal(t, byte(scope.BallotPending), newBallot.State, "exactly 2/3 must remain pending")
	}

	oldVote2 := v119SignedMemoryVote(t, keys[2], 3, oldMemoryID)
	threeOfFourPlans := make([]v119WorkerPlan, 3)
	for i := range threeOfFourPlans {
		threeOfFourPlans[i] = v119WorkerPlan{
			DataDir: replicaDirs[i], Blocks: []v119FaultBlock{v119Block(30, oldVote2)},
			InspectMemoryIDs: []string{oldMemoryID, newMemoryID},
		}
	}
	threeOfFour := v119RunBatch(t, threeOfFourPlans)
	v119RequireSameFinalState(t, threeOfFour)
	for _, result := range threeOfFour {
		v119RequireBlockCodesZero(t, result)
		assert.Equal(t, byte(scope.BallotCommitted), v119Ballot(t, result, oldMemoryID).State, "3/4 must commit under pinned revision 1")
		assert.Equal(t, byte(scope.BallotPending), v119Ballot(t, result, newMemoryID).State)
	}

	newVote2 := v119SignedMemoryVote(t, keys[2], 4, newMemoryID)
	finalPlans := make([]v119WorkerPlan, 3)
	for i := range finalPlans {
		finalPlans[i] = v119WorkerPlan{
			DataDir: replicaDirs[i], Blocks: []v119FaultBlock{v119Block(31, newVote2)},
			InspectMemoryIDs: []string{oldMemoryID, newMemoryID},
		}
	}
	finalOnline := v119RunBatch(t, finalPlans)
	v119RequireSameFinalState(t, finalOnline)
	for _, result := range finalOnline {
		v119RequireBlockCodesZero(t, result)
		assert.Equal(t, byte(scope.BallotCommitted), v119Ballot(t, result, oldMemoryID).State)
		assert.Equal(t, byte(scope.BallotCommitted), v119Ballot(t, result, newMemoryID).State)
	}

	// End the transport partition with SIGKILL (no graceful close), reopen the
	// same replica, and replay every exact missed block. Compare every AppHash
	// against the canonical online sequence, not only the final state.
	partitionResult := v119ReadResult(t, partitioned.resultPath)
	assert.Equal(t, int64(15), partitionResult.Height)
	require.NoError(t, partitioned.cmd.Process.Kill())
	require.Error(t, partitioned.cmd.Wait())
	catchupBlocks := append([]v119FaultBlock{}, throughReconfiguration...)
	catchupBlocks = append(catchupBlocks,
		v119Block(27, newSubmit),
		v119Block(28, newVote0),
		v119Block(29, newVote1),
		v119Block(30, oldVote2),
		v119Block(31, newVote2),
	)
	catchup := v119RunBatch(t, []v119WorkerPlan{{
		DataDir: replicaDirs[3], Blocks: catchupBlocks,
		InspectMemoryIDs: []string{oldMemoryID, newMemoryID},
	}})[0]
	v119RequireBlockCodesZero(t, catchup)
	assert.Equal(t, finalOnline[0].Height, catchup.Height)
	assert.Equal(t, finalOnline[0].AppHash, catchup.AppHash)
	assert.Equal(t, finalOnline[0].AppVersion, catchup.AppVersion)
	assert.Equal(t, uint64(2), catchup.ScopeRevision)
	assert.Equal(t, byte(scope.BallotCommitted), v119Ballot(t, catchup, oldMemoryID).State)
	assert.Equal(t, byte(scope.BallotCommitted), v119Ballot(t, catchup, newMemoryID).State)

	canonicalHashes := make(map[int64]string)
	for _, stage := range []v119WorkerResult{online[0], newSubmitResults[0], twoOfThreeResults[0], threeOfFour[0], finalOnline[0]} {
		for _, block := range stage.Blocks {
			canonicalHashes[block.Height] = block.AppHash
		}
	}
	for _, block := range catchup.Blocks {
		assert.Equal(t, canonicalHashes[block.Height], block.AppHash, "catch-up AppHash at height %d", block.Height)
	}
}

// TestV119DelegatedGovernanceReconfigurationCrashReplay drives the real
// app-v20 dual-principal envelope through independent OS processes. The final
// quorum vote speculatively adds a validator during FinalizeBlock, the worker
// is SIGKILLed before Commit, and a fresh process must start from the old set,
// replay the whole block, and re-emit the identical ValidatorUpdate.
func TestV119DelegatedGovernanceReconfigurationCrashReplay(t *testing.T) {
	if os.Getenv(v119WorkerPlanEnv) != "" {
		t.Skip("parent harness only")
	}
	keys := v119Validators()
	candidate := deterministicScopedAgent(129)
	authorizer := v119Operator()

	healthyDir := filepath.Join(t.TempDir(), "healthy")
	crashDir := filepath.Join(t.TempDir(), "crash")
	initialized := v119RunBatch(t, []v119WorkerPlan{
		{DataDir: healthyDir},
		{DataDir: crashDir},
	})
	v119RequireSameFinalState(t, initialized)
	require.Equal(t, 4, initialized[0].ValidatorCount)

	propose := v119DelegatedAddValidator(t, authorizer, keys[0], candidate, 1, 2, "mprop001")
	proposalID := governance.ComputeProposalID(keys[0].id, 2, governance.OpAddValidator, candidate.id)
	firstVote := v119DelegatedGovernanceVote(t, authorizer, keys[1], proposalID, 1, 12, "mvote001")
	quorumVote := v119DelegatedGovernanceVote(t, authorizer, keys[2], proposalID, 1, 13, "mvote002")

	prefix := []v119FaultBlock{v119Block(2, propose)}
	for height := int64(3); height < 12; height++ {
		prefix = append(prefix, v119Block(height))
	}
	prefix = append(prefix, v119Block(12, firstVote))
	prefixResults := v119RunBatch(t, []v119WorkerPlan{
		{DataDir: healthyDir, Blocks: prefix},
		{DataDir: crashDir, Blocks: prefix},
	})
	v119RequireSameFinalState(t, prefixResults)
	for _, result := range prefixResults {
		v119RequireBlockCodesZero(t, result)
		require.Equal(t, int64(12), result.Height)
		require.Equal(t, 4, result.ValidatorCount)
	}

	healthy := v119RunBatch(t, []v119WorkerPlan{{
		DataDir: healthyDir, Blocks: []v119FaultBlock{v119Block(13, quorumVote)},
	}})[0]
	v119RequireBlockCodesZero(t, healthy)
	require.Len(t, healthy.Blocks, 1)
	require.Equal(t, 1, healthy.Blocks[0].ValidatorUpdates)
	require.Equal(t, 5, healthy.ValidatorCount)

	crashing := v119StartChild(t, v119WorkerPlan{
		DataDir: crashDir, Blocks: []v119FaultBlock{v119Block(13, quorumVote)},
		CrashAfterBlock: 13,
	})
	crashed := v119WaitReadyAndKill(t, crashing)
	v119RequireBlockCodesZero(t, crashed)
	require.Len(t, crashed.Blocks, 1)
	assert.Equal(t, healthy.Blocks[0].AppHash, crashed.Blocks[0].AppHash)
	assert.Equal(t, 1, crashed.Blocks[0].ValidatorUpdates)
	assert.Equal(t, 4, crashed.ValidatorCount, "the worker summary is committed state, not the speculative response")

	// Neither the validator mutation nor the height survived: both belong to the
	// discarded app-v20 transaction. Replay therefore executes governance from
	// the same four-validator committed state and reproduces the update.
	reopened := v119RunBatch(t, []v119WorkerPlan{{DataDir: crashDir}})[0]
	assert.Equal(t, int64(12), reopened.Height)
	assert.Equal(t, 4, reopened.ValidatorCount)

	replayed := v119RunBatch(t, []v119WorkerPlan{{
		DataDir: crashDir, Blocks: []v119FaultBlock{v119Block(13, quorumVote)},
	}})[0]
	v119RequireBlockCodesZero(t, replayed)
	require.Len(t, replayed.Blocks, 1)
	assert.Equal(t, 1, replayed.Blocks[0].ValidatorUpdates)
	assert.Equal(t, healthy.Blocks[0].AppHash, replayed.Blocks[0].AppHash)
	assert.Equal(t, healthy.AppHash, replayed.AppHash)
	assert.Equal(t, healthy.Height, replayed.Height)
	assert.Equal(t, healthy.ValidatorCount, replayed.ValidatorCount)
}
