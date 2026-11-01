//go:build v119testfixture

package abci

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/scope"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

const (
	appV30CrashConfigEnv = "SAGE_APPV30_PROJECTION_CRASH_CONFIG"
	appV30CrashChain     = "app30-projection-crash"
)

type appV30CrashRecord struct {
	ID, Error              string
	SQL, Canonical, Scoped bool
	Lineage, Signers       int
	Decisions              []string
}

type appV30CrashSnapshot struct {
	Height                   int64
	AppHash                  string
	Revision, MirrorRevision uint64
	MirrorFresh              bool
	MirrorIDs                []string
	Memories, Receipts       int
	Records                  []appV30CrashRecord
}

type appV30CrashConfig struct {
	Mode, Data, Ready, Result string
	Stage                     AppV20CommitBoundaryStage
	Block                     *abcitypes.RequestFinalizeBlock
	IDs                       []string
}

type appV30CrashCheckpoint struct {
	Name, Directory string
	Block           *abcitypes.RequestFinalizeBlock
	Before, After   appV30CrashSnapshot
}

func appV30CrashOpen(t *testing.T, directory string) *SageApp {
	t.Helper()
	require.NoError(t, os.MkdirAll(directory, 0o700))
	app := openFreshGenesisH0TestApp(t, filepath.Join(directory, "badger"), filepath.Join(directory, "projection.db"))
	// The node supplies its immutable Comet chain ID again after a restart.
	require.NoError(t, app.SetExpectedGovernanceDelegationDomain(appV30CrashChain))
	return app
}

// Storage constructors initialize schemas and legacy indexes. Observe before
// NewSageApp reconciles the advisory mirror; this is not migration-free I/O.
func appV30CrashRead(t *testing.T, directory string, ids []string) appV30CrashSnapshot {
	t.Helper()
	canonical, err := store.NewBadgerStore(filepath.Join(directory, "badger"))
	require.NoError(t, err)
	defer func() { require.NoError(t, canonical.CloseBadger()) }()
	projection, err := store.NewSQLiteStore(context.Background(), filepath.Join(directory, "projection.db"))
	require.NoError(t, err)
	defer func() { require.NoError(t, projection.Close()) }()
	return appV30CrashObserve(t, canonical, projection, directory, ids)
}

func appV30CrashObserve(t *testing.T, canonical *store.BadgerStore, projection *store.SQLiteStore, directory string, ids []string) appV30CrashSnapshot {
	t.Helper()
	ctx := context.Background()
	state, err := LoadState(canonical)
	require.NoError(t, err)
	rules, err := appHashRuleInputsForOfflineState(canonical)
	require.NoError(t, err)
	computed, err := computeAppHashForRule(canonical, selectAppHashRule(state.Height, rules))
	require.NoError(t, err)
	require.Equal(t, state.AppHash, computed, "inspect durable canonical bytes before SageApp mirror reconciliation")
	result := appV30CrashSnapshot{Height: state.Height, AppHash: hex.EncodeToString(computed)}
	result.Revision, err = canonical.TaintRevision()
	require.NoError(t, err)
	result.MirrorFresh, _, err = store.MemoryTaintMirrorFresh(ctx, canonical, projection)
	require.NoError(t, err)
	result.MirrorRevision, err = projection.MemoryTaintMirrorRevision(ctx)
	require.NoError(t, err)
	db, err := sql.Open("sqlite", filepath.Join(directory, "projection.db"))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM memories").Scan(&result.Memories))
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM abci_projection_batches").Scan(&result.Receipts))
	rows, err := db.QueryContext(ctx, "SELECT memory_id FROM memory_taint_mirror ORDER BY memory_id")
	require.NoError(t, err)
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		result.MirrorIDs = append(result.MirrorIDs, id)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	for _, id := range ids {
		item := appV30CrashRecord{ID: id}
		_, _, canonicalErr := canonical.GetMemoryHash(id)
		item.Canonical = canonicalErr == nil
		content, contentErr := canonical.GetScopedContent(id)
		require.NoError(t, contentErr)
		item.Scoped = content != nil
		if item.Canonical {
			item.Decisions, err = canonical.GetEffectiveTaintDecisions(id)
			require.NoError(t, err)
			refs, lineageErr := canonical.GetMemoryLineage(id)
			require.NoError(t, lineageErr)
			item.Lineage = len(refs)
			signers, found, signerErr := canonical.GetAppV30Provenance(id)
			require.NoError(t, signerErr)
			require.True(t, found)
			item.Signers = len(signers)
		}
		record, readErr := projection.GetMemory(ctx, id)
		if readErr == nil {
			item.SQL = true
			_, _, classifyErr := canonical.ClassifyMemoryProjection(record)
			switch {
			case classifyErr == nil:
				item.Error = "visible"
			case errors.Is(classifyErr, store.ErrMemoryProjectionTainted):
				item.Error = "tainted"
			case errors.Is(classifyErr, store.ErrMemoryProjectionTaintPending):
				item.Error = "pending"
			case errors.Is(classifyErr, store.ErrMemoryProjectionUnpublished):
				item.Error = "unpublished"
			default:
				t.Fatalf("unexpected canonical projection verdict for %s: %v", id, classifyErr)
			}
		}
		result.Records = append(result.Records, item)
	}
	return result
}

// Only closed, test-owned directories are copied. SQL is local crash-fixture
// state, never consensus input or a network/state-sync payload.
func appV30CrashCopy(t *testing.T, source, target string) {
	t.Helper()
	require.NotEqual(t, source, target)
	require.NoError(t, filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		destination := filepath.Join(target, relative)
		if entry.IsDir() {
			return os.MkdirAll(destination, 0o700)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("nonregular test fixture file %s", relative)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(destination, data, 0o600)
	}))
}

func appV30CrashFinalize(t *testing.T, app *SageApp, block *abcitypes.RequestFinalizeBlock) {
	t.Helper()
	finalized, err := app.FinalizeBlock(context.Background(), block)
	require.NoError(t, err)
	require.Len(t, finalized.TxResults, len(block.Txs))
	for _, result := range finalized.TxResults {
		require.Zero(t, result.Code, result.Log)
	}
}

func appV30CrashCheckpoints(t *testing.T) ([]appV30CrashCheckpoint, []string) {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "baseline")
	app := appV30CrashOpen(t, directory)
	t.Cleanup(func() {
		if app != nil {
			require.NoError(t, app.Close())
		}
	})
	manifest, root, agent := freshGenesisFixture(t, 30, 1, true)
	manifest.Validators[0].ValidatorID = root.id
	const chainID = appV30CrashChain
	_, err := app.InitChain(context.Background(), freshGenesisRequest(t, chainID, manifest, root, agent))
	require.NoError(t, err)
	ids := []string{"ordinary-parent", "scoped-parent", "child-000", "child-001", "admitted-ordinary", "admitted-scoped", "child-398", "child-399"}
	var checkpoints []appV30CrashCheckpoint
	var height int64
	var rootNonce, agentNonce uint64
	start := time.Now().UTC().Truncate(time.Second)
	sign := func(parsed *tx.ParsedTx, key agentKey, nonce *uint64) []byte {
		*nonce++
		parsed.Nonce = *nonce
		parsed.Timestamp = start
		if parsed.Type == tx.TxTypeMemoryTaintWork && len(parsed.AgentPubKey) == 0 {
			parsed.AgentPubKey, parsed.AgentSig, parsed.AgentBodyHash, parsed.AgentTimestamp =
				signAgentProof(t, key, []byte("owned app30 commit recovery proof"))
		}
		require.NoError(t, tx.SignTx(parsed, key.priv))
		raw, encodeErr := tx.EncodeTx(parsed)
		require.NoError(t, encodeErr)
		return raw
	}
	commit := func(name string, raws ...[]byte) {
		height++
		block := &abcitypes.RequestFinalizeBlock{Height: height, Time: start.Add(time.Duration(height) * time.Second), Txs: raws}
		var checkpoint appV30CrashCheckpoint
		if name != "" {
			checkpoint.Name, checkpoint.Block = name, block
			require.NoError(t, app.Close())
			app = nil
			checkpoint.Before = appV30CrashRead(t, directory, ids)
			checkpoint.Directory = filepath.Join(t.TempDir(), name)
			appV30CrashCopy(t, directory, checkpoint.Directory)
			app = appV30CrashOpen(t, directory)
		}
		appV30CrashFinalize(t, app, block)
		_, commitErr := app.Commit(context.Background(), &abcitypes.RequestCommit{})
		require.NoError(t, commitErr)
		if name != "" {
			checkpoint.After = appV30CrashObserve(t, app.badgerStore, app.offchainStore.(*store.SQLiteStore), directory, ids)
			checkpoints = append(checkpoints, checkpoint)
		}
	}
	// The scope is governed by actual signed singleton transactions too. The
	// tagged fixture only shortens the existing governance cooldown to one block.
	record := scopeTemplate("crash-scope", root.id, "fresh-home", 1, scope.StateActive,
		scope.Member{ValidatorID: root.id, AssignedWeight: 10, JoinedRevision: 1, Active: true})
	rootNonce++
	commit("", makeScopeGovProposeTx(t, root, record, record.ScopeID, rootNonce))
	selected, err := app.badgerStore.GetScopeForDomain("fresh-home")
	require.NoError(t, err)
	require.NotNil(t, selected)
	parents := []tx.ParentRef{
		{SourceChainID: chainID, MemoryID: ids[0], ContentHash: sha256.Sum256([]byte("ordinary parent"))},
		{SourceChainID: chainID, MemoryID: ids[1], ContentHash: sha256.Sum256([]byte("scoped parent"))},
	}
	submissions := make([][]byte, 0, len(parents))
	for i, parent := range parents {
		domain := "general"
		writer, nonce := root, &rootNonce
		if i == 1 {
			domain = "fresh-home"
			writer, nonce = *agent, &agentNonce
		}
		parsed := makeMemorySubmitTx(t, writer, domain, strings.TrimSuffix(parent.MemoryID, "-parent")+" parent")
		parsed.MemorySubmit.MemoryID = parent.MemoryID
		submissions = append(submissions, sign(parsed, writer, nonce))
	}
	commit("", submissions...)
	votes := func(memoryIDs []string) [][]byte {
		raws := make([][]byte, 0, len(memoryIDs))
		for _, id := range memoryIDs {
			raws = append(raws, sign(&tx.ParsedTx{Type: tx.TxTypeMemoryVote, MemoryVote: &tx.MemoryVote{MemoryID: id, Decision: tx.VoteDecisionAccept}}, root, &rootNonce))
		}
		return raws
	}
	commit("", votes(ids[:2])...)
	// Four hundred signed children leave apply and expand bounded/incomplete;
	// clear then leaves a genuine retained queue for the explicit cleanup tx.
	for offset := 0; offset < 400; offset += 50 {
		var raws [][]byte
		var childIDs []string
		for i := offset; i < offset+50; i++ {
			id := fmt.Sprintf("child-%03d", i)
			domain := "general"
			writer, nonce := root, &rootNonce
			if i%2 == 1 {
				domain = "fresh-home"
				writer, nonce = *agent, &agentNonce
			}
			parsed := makeMemorySubmitTx(t, writer, domain, "signed crash child "+id)
			parsed.MemorySubmit.MemoryID = id
			parsed.MemorySubmit.DerivedFrom = []tx.ParentRef{parents[0]}
			raws = append(raws, sign(parsed, writer, nonce))
			childIDs = append(childIDs, id)
		}
		name := ""
		if offset == 0 {
			name = "admission"
		}
		commit(name, raws...)
		commit("", votes(childIDs)...)
	}
	apply := appV30ApplyPayload(parents[0])
	apply.ScopeKind = tx.TaintScopeCredentialSet
	apply.Credentials = []string{root.id, agent.id}
	sort.Strings(apply.Credentials)
	payload, err := tx.EncodeTaintApplyPayload(apply)
	require.NoError(t, err)
	decisionID, err := tx.TaintApplyDecisionID(payload)
	require.NoError(t, err)
	propose := func(target string, action []byte) string {
		createdHeight := height + 1
		commit("", sign(&tx.ParsedTx{Type: tx.TxTypeGovPropose, GovPropose: &tx.GovPropose{Operation: tx.GovOpMemoryTaint, TargetID: target, Payload: action, Reason: "signed app30 crash matrix"}}, root, &rootNonce))
		return governance.ComputeProposalID(root.id, createdHeight, governance.OpMemoryTaint, target)
	}
	proposalID := propose(decisionID, payload)
	commit("apply", sign(&tx.ParsedTx{Type: tx.TxTypeGovVote, GovVote: &tx.GovVote{ProposalID: proposalID, Decision: tx.VoteDecisionAccept}}, root, &rootNonce))
	for _, index := range []int{0, 2, 3} {
		record := checkpoints[len(checkpoints)-1].After.Records[index]
		require.Equal(t, []string{decisionID}, record.Decisions)
		require.Equal(t, "tainted", record.Error)
	}
	status := func(want taint.Status) {
		header, found, statusErr := app.badgerStore.GetTaintDecision(decisionID)
		require.NoError(t, statusErr)
		require.True(t, found)
		require.Equal(t, want, header.Status)
	}
	status(taint.StatusExpanding)
	work := func(kind tx.MemoryTaintWorkKind) []byte {
		header, found, headerErr := app.badgerStore.GetTaintDecision(decisionID)
		require.NoError(t, headerErr)
		require.True(t, found)
		request := &tx.MemoryTaintWork{Version: tx.MemoryTaintWorkVersion, Kind: kind,
			DecisionID: decisionID, Generation: header.Generation, Cursor: header.LastEdge}
		if kind == tx.MemoryTaintWorkExpand {
			request.FrontierHead, request.ParentRefID = header.FrontierHead, header.ParentRefID
		} else {
			request.CleanupPhase = uint8(header.CleanupPhase)
		}
		return sign(&tx.ParsedTx{Type: tx.TxTypeMemoryTaintWork, MemoryTaintWork: request}, root, &rootNonce)
	}
	commit("expand", work(tx.MemoryTaintWorkExpand))
	status(taint.StatusExpanding)
	submissions = make([][]byte, 0, len(ids[4:6]))
	for i, id := range ids[4:6] {
		domain := "general"
		writer, nonce := root, &rootNonce
		if i == 1 {
			domain = "fresh-home"
			writer, nonce = *agent, &agentNonce
		}
		parsed := makeMemorySubmitTx(t, writer, domain, "admitted while tainted "+id)
		parsed.MemorySubmit.MemoryID = id
		parsed.MemorySubmit.DerivedFrom = []tx.ParentRef{parents[0]}
		submissions = append(submissions, sign(parsed, writer, nonce))
	}
	commit("inherited-admission", submissions...)
	inherited := checkpoints[len(checkpoints)-1].After.Records
	for i, record := range inherited[4:6] {
		require.True(t, record.Canonical)
		require.Equal(t, i == 1, record.Scoped)
		require.Equal(t, 1, record.Lineage)
		require.Positive(t, record.Signers)
		require.Equal(t, []string{decisionID}, record.Decisions, "admission must actually materialize the membership")
		require.Equal(t, "tainted", record.Error, "both ordinary and scoped admission must inherit the active decision")
	}
	commit("", votes(ids[4:6])...)
	decision, err := hex.DecodeString(decisionID)
	require.NoError(t, err)
	evidence := sha256.Sum256([]byte("clear crash evidence"))
	clearPayload, err := tx.EncodeTaintClearPayload(tx.TaintClearPayload{Version: tx.TaintClearPayloadVersion, Action: tx.TaintActionClear, DecisionID: decision, ReasonCategory: 2, EvidenceDigest: evidence[:]})
	require.NoError(t, err)
	clearTarget, err := tx.TaintClearTargetID(clearPayload)
	require.NoError(t, err)
	proposalID = propose(clearTarget, clearPayload)
	commit("clear", sign(&tx.ParsedTx{Type: tx.TxTypeGovVote, GovVote: &tx.GovVote{ProposalID: proposalID, Decision: tx.VoteDecisionAccept}}, root, &rootNonce))
	status(taint.StatusCleaning)
	for _, record := range checkpoints[len(checkpoints)-1].After.Records {
		require.True(t, record.SQL)
		require.True(t, record.Canonical)
		require.Empty(t, record.Decisions)
		require.Equal(t, "visible", record.Error, "logical clear must withdraw the canonical hold before cleanup completes")
	}
	beforeCleanup, _, err := app.badgerStore.GetTaintDecision(decisionID)
	require.NoError(t, err)
	commit("cleanup", work(tx.MemoryTaintWorkCleanup))
	afterCleanup, _, err := app.badgerStore.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.NotEqual(t, beforeCleanup, afterCleanup, "the checkpoint must execute real bounded cleanup work")
	// The first explicit batch is the crash checkpoint; finish any remaining
	// queue in the uninterrupted control without assuming it fits one batch.
	for batches := 0; afterCleanup.Status == taint.StatusCleaning && batches < 8; batches++ {
		commit("", work(tx.MemoryTaintWorkCleanup))
		afterCleanup, _, err = app.badgerStore.GetTaintDecision(decisionID)
		require.NoError(t, err)
	}
	status(taint.StatusClean)
	return checkpoints, ids
}

func TestAppV30ProjectionCrashWorker(t *testing.T) {
	configPath := os.Getenv(appV30CrashConfigEnv)
	if configPath == "" {
		t.Skip("owned child-process fixture only")
	}
	encoded, err := os.ReadFile(configPath)
	require.NoError(t, err)
	var config appV30CrashConfig
	require.NoError(t, json.Unmarshal(encoded, &config))
	app := appV30CrashOpen(t, config.Data)
	defer func() { require.NoError(t, app.Close()) }()
	if config.Mode == "crash" {
		restore := SetAppV20CommitBoundaryHookForTest(func(stage AppV20CommitBoundaryStage) {
			if stage == config.Stage {
				require.NoError(t, os.WriteFile(config.Ready, []byte("owned crash seam\n"), 0o600))
				select {}
			}
		})
		defer restore()
	}
	if config.Mode == "crash" || config.Mode == "replay" {
		appV30CrashFinalize(t, app, config.Block)
		_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
		require.NoError(t, err)
	}
	result := appV30CrashObserve(t, app.badgerStore, app.offchainStore.(*store.SQLiteStore), config.Data, config.IDs)
	encoded, err = json.Marshal(result)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(config.Result, encoded, 0o600))
}

func appV30CrashRun(t *testing.T, config appV30CrashConfig) appV30CrashSnapshot {
	t.Helper()
	config.Result = filepath.Join(t.TempDir(), "result.json")
	configPath := filepath.Join(t.TempDir(), "config.json")
	encoded, err := json.Marshal(config)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(configPath, encoded, 0o600))
	command := exec.Command(os.Args[0], "-test.run=^TestAppV30ProjectionCrashWorker$", "-test.count=1", "-test.timeout=45s")
	command.Env = append(os.Environ(), appV30CrashConfigEnv+"="+configPath)
	if config.Mode == "crash" {
		logPath := filepath.Join(t.TempDir(), "worker.log")
		logFile, logErr := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY, 0o600)
		require.NoError(t, logErr)
		defer func() { require.NoError(t, logFile.Close()) }()
		command.Stdout, command.Stderr = logFile, logFile
		t.Cleanup(func() {
			if t.Failed() {
				data, _ := os.ReadFile(logPath)
				t.Logf("owned crash worker: %s", data)
			}
		})
		require.NoError(t, command.Start())
		waitProjectionCrashReady(t, command, config.Ready)
		waitStatus, ok := command.ProcessState.Sys().(syscall.WaitStatus)
		require.True(t, ok)
		require.True(t, waitStatus.Signaled())
		require.Equal(t, syscall.SIGKILL, waitStatus.Signal(), "the owned worker must actually stop on SIGKILL")
		return appV30CrashSnapshot{}
	}
	output, runErr := command.CombinedOutput()
	require.NoError(t, runErr, "owned app30 worker failed: %s", output)
	encoded, err = os.ReadFile(config.Result)
	require.NoError(t, err)
	var result appV30CrashSnapshot
	require.NoError(t, json.Unmarshal(encoded, &result))
	return result
}

func appV30CrashMirrorLoss(t *testing.T, source string, expected appV30CrashSnapshot, config appV30CrashConfig) {
	t.Helper()
	for _, shape := range []string{"missing", "stale"} {
		t.Run(shape+"-mirror", func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "node")
			appV30CrashCopy(t, source, directory)
			projection, err := store.NewSQLiteStore(context.Background(), filepath.Join(directory, "projection.db"))
			require.NoError(t, err)
			if shape == "stale" {
				require.NoError(t, projection.ReplaceMemoryTaintMirror(context.Background(), expected.Revision-1, []string{"wrong-local-mirror-row"}))
			}
			require.NoError(t, projection.Close())
			if shape == "missing" {
				db, openErr := sql.Open("sqlite", filepath.Join(directory, "projection.db"))
				require.NoError(t, openErr)
				_, err = db.ExecContext(context.Background(), "DELETE FROM memory_taint_mirror; DELETE FROM memory_taint_mirror_revision")
				require.NoError(t, err)
				require.NoError(t, db.Close())
			}
			beforeRepair := appV30CrashRead(t, directory, config.IDs)
			require.False(t, beforeRepair.MirrorFresh)
			require.Equal(t, expected.AppHash, beforeRepair.AppHash)
			require.Equal(t, expected.Records, beforeRepair.Records,
				"Badger classification is authoritative while either SQL mirror shape is unavailable")
			config.Mode, config.Data = "inspect", directory
			require.Equal(t, expected, appV30CrashRun(t, config), "constructor repairs the advisory mirror without changing canonical state")
		})
	}
}

func TestAppV30SIGKILLProjectionAndTaintRecovery(t *testing.T) {
	checkpoints, ids := appV30CrashCheckpoints(t)
	require.Len(t, checkpoints, 6)
	for _, checkpoint := range checkpoints {
		for _, stage := range []AppV20CommitBoundaryStage{AppV20CommitAfterOffchainFlush, AppV20CommitAfterBadgerSync} {
			t.Run(checkpoint.Name+"/"+string(stage), func(t *testing.T) {
				directory := filepath.Join(t.TempDir(), "node")
				appV30CrashCopy(t, checkpoint.Directory, directory)
				config := appV30CrashConfig{Mode: "crash", Data: directory, Ready: filepath.Join(t.TempDir(), "ready"), Stage: stage, Block: checkpoint.Block, IDs: ids}
				appV30CrashRun(t, config)
				crashed := appV30CrashRead(t, directory, ids)
				expected := checkpoint.Before
				if stage == AppV20CommitAfterBadgerSync {
					expected = checkpoint.After
				}
				require.Equal(t, expected.Height, crashed.Height)
				require.Equal(t, expected.AppHash, crashed.AppHash)
				require.Equal(t, expected.Revision, crashed.Revision)
				require.Equal(t, checkpoint.After.Memories, crashed.Memories)
				require.Equal(t, checkpoint.After.Receipts, crashed.Receipts)
				for i, item := range crashed.Records {
					if expected.Records[i].Canonical {
						require.Equal(t, expected.Records[i], item)
					} else if item.SQL {
						require.False(t, item.Canonical)
						require.Equal(t, "unpublished", item.Error)
					}
				}
				if stage == AppV20CommitAfterBadgerSync && checkpoint.Before.Revision != checkpoint.After.Revision {
					require.False(t, crashed.MirrorFresh, "taint mirror refresh follows durable Badger publication")
				}
				if stage == AppV20CommitAfterBadgerSync && (checkpoint.Name == "apply" || checkpoint.Name == "clear") {
					appV30CrashMirrorLoss(t, directory, checkpoint.After, config)
				}
				config.Mode = "inspect"
				if stage == AppV20CommitAfterOffchainFlush {
					config.Mode = "replay"
				}
				recovered := appV30CrashRun(t, config)
				require.Equal(t, checkpoint.After, recovered, "exact replay/reopen must equal uninterrupted canonical+SQL result")
				config.Mode = "inspect"
				require.Equal(t, recovered, appV30CrashRun(t, config), "second durable reopen is stable")
			})
		}
	}
}
