//go:build v119testfixture

package abci

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
)

const (
	projectionCrashModeEnv   = "SAGE_V119_PROJECTION_CRASH_MODE"
	projectionCrashDataEnv   = "SAGE_V119_PROJECTION_CRASH_DATA"
	projectionCrashReadyEnv  = "SAGE_V119_PROJECTION_CRASH_READY"
	projectionCrashResultEnv = "SAGE_V119_PROJECTION_CRASH_RESULT"
)

type projectionCrashResult struct {
	Height         int64  `json:"height"`
	AppHash        string `json:"app_hash"`
	Memories       int    `json:"memories"`
	Triples        int    `json:"triples"`
	Challenges     int    `json:"challenges"`
	Corroborations int    `json:"corroborations"`
	AccessLogs     int    `json:"access_logs"`
	GovProposals   int    `json:"gov_proposals"`
	Receipts       int    `json:"receipts"`
}

func openProjectionCrashApp(t *testing.T, dataDir string) (*SageApp, *store.SQLiteStore) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dataDir, 0o700))
	badgerPath := filepath.Join(dataDir, "badger")
	require.NoError(t, os.MkdirAll(badgerPath, 0o700))
	badgerStore, err := store.NewBadgerStore(badgerPath)
	require.NoError(t, err)

	persistedHeight, err := badgerStore.GetState(stateHeightKey)
	require.NoError(t, err)
	if len(persistedHeight) == 0 {
		seedTestGovernanceDelegationDomain(t, badgerStore)
		require.NoError(t, badgerStore.MarkUpgradeApplied(appV20UpgradeName, 20, 1))
		appHash, hashErr := badgerStore.ComputeAppHashExcludingBookkeeping()
		require.NoError(t, hashErr)
		require.NoError(t, SaveState(badgerStore, &AppState{Height: 1, AppHash: appHash}))
		require.NoError(t, badgerStore.DB().Sync())
	}

	projection, err := store.NewSQLiteStore(context.Background(), filepath.Join(dataDir, "projection.db"))
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
	return app, projection
}

func projectionCrashWrites() (string, []pendingWrite) {
	blockTime := time.Unix(88_002, 456_000_000).UTC()
	content := "projection survives the SQL-to-Badger crash window"
	contentHash := sha256.Sum256([]byte(content))
	memoryID := hex.EncodeToString(contentHash[:])
	return memoryID, []pendingWrite{
		{writeType: "memory", data: &memory.MemoryRecord{
			MemoryID: memoryID, SubmittingAgent: "projection-agent", Content: content,
			ContentHash: contentHash[:], MemoryType: memory.TypeFact, DomainTag: "research",
			ConfidenceScore: 0.95, Status: memory.StatusProposed, CreatedAt: blockTime,
		}},
		{writeType: "triples", data: &triplesData{MemoryID: memoryID, Triples: []memory.KnowledgeTriple{
			{Subject: "SAGE", Predicate: "survives", Object: "replay"},
			{Subject: "replay", Predicate: "preserves", Object: "projection"},
		}}},
		{writeType: "challenge", data: &store.ChallengeEntry{
			MemoryID: memoryID, ChallengerID: "challenger-a", Reason: "fault injection",
			Evidence: "crash boundary", BlockHeight: 2, CreatedAt: blockTime,
		}},
		{writeType: "corroborate", data: &store.Corroboration{
			MemoryID: memoryID, AgentID: "corroborator-a", Evidence: "same batch",
			CreatedAt: blockTime,
		}},
		{writeType: "access_log", data: &store.AccessLogEntry{
			AgentID: "projection-agent", Domain: "research", Action: "query",
			MemoryIDs: []string{memoryID}, BlockHeight: 2, CreatedAt: blockTime,
		}},
		{writeType: "gov_proposal", data: govProposalData{
			ProposalID: "projection-proposal", Operation: "update_power",
			TargetID: "validator-a", TargetPower: 20, ProposerID: "validator-b",
			Status: "voting", CreatedHeight: 2, ExpiryHeight: 102,
			Reason: "projection replay",
		}},
	}
}

func stageProjectionCrashCommit(t *testing.T, app *SageApp) {
	t.Helper()
	memoryID, writes := projectionCrashWrites()
	contentHash, err := hex.DecodeString(memoryID)
	require.NoError(t, err)
	scoped := app.badgerStore.BeginConsensusTransaction(nil)
	working := app.cloneForAppV20Finalize(scoped)
	require.NoError(t, working.badgerStore.SetMemoryHash(memoryID, contentHash, string(memory.StatusProposed)))
	appHash, err := working.badgerStore.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	working.state.Height = 2
	working.state.AppHash = appHash
	working.pendingWrites = writes
	app.pendingAppV20Finalize = &appV20AtomicFinalize{app: working, store: scoped}
}

func projectionCrashSnapshot(t *testing.T, app *SageApp, dataDir string) projectionCrashResult {
	t.Helper()
	state, err := LoadState(app.badgerStore)
	require.NoError(t, err)
	result := projectionCrashResult{Height: state.Height, AppHash: hex.EncodeToString(state.AppHash)}
	db, err := sql.Open("sqlite", filepath.Join(dataDir, "projection.db"))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	for _, item := range []struct {
		query  string
		target *int
	}{
		{`SELECT COUNT(*) FROM memories`, &result.Memories},
		{`SELECT COUNT(*) FROM knowledge_triples`, &result.Triples},
		{`SELECT COUNT(*) FROM challenges`, &result.Challenges},
		{`SELECT COUNT(*) FROM corroborations`, &result.Corroborations},
		{`SELECT COUNT(*) FROM access_logs`, &result.AccessLogs},
		{`SELECT COUNT(*) FROM governance_proposals`, &result.GovProposals},
		{`SELECT COUNT(*) FROM abci_projection_batches`, &result.Receipts},
	} {
		require.NoError(t, db.QueryRowContext(context.Background(), item.query).Scan(item.target))
	}
	return result
}

func writeProjectionCrashResult(t *testing.T, path string, result projectionCrashResult) {
	t.Helper()
	data, err := json.Marshal(result)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))
}

// TestAppV20OffchainProjectionCrashWorker is re-executed by the parent below.
// The crash mode blocks at the tagged post-SQL/pre-Badger seam until the parent
// delivers a real SIGKILL.
func TestAppV20OffchainProjectionCrashWorker(t *testing.T) {
	mode := os.Getenv(projectionCrashModeEnv)
	if mode == "" {
		t.Skip("child-process fixture only")
	}
	dataDir := os.Getenv(projectionCrashDataEnv)
	require.NotEmpty(t, dataDir)
	app, _ := openProjectionCrashApp(t, dataDir)
	defer func() { require.NoError(t, app.Close()) }()

	switch mode {
	case "inspect":
		writeProjectionCrashResult(t, os.Getenv(projectionCrashResultEnv), projectionCrashSnapshot(t, app, dataDir))
	case "crash":
		readyPath := os.Getenv(projectionCrashReadyEnv)
		require.NotEmpty(t, readyPath)
		restore := SetAppV20CommitBoundaryHookForTest(func(stage AppV20CommitBoundaryStage) {
			if stage != AppV20CommitAfterOffchainFlush {
				return
			}
			require.NoError(t, os.WriteFile(readyPath, []byte("projection durable\n"), 0o600))
			select {}
		})
		defer restore()
		stageProjectionCrashCommit(t, app)
		_, err := app.Commit(context.Background(), nil)
		require.NoError(t, err)
		t.Fatal("crash hook did not stop Commit")
	case "replay":
		stageProjectionCrashCommit(t, app)
		_, err := app.Commit(context.Background(), nil)
		require.NoError(t, err)
		writeProjectionCrashResult(t, os.Getenv(projectionCrashResultEnv), projectionCrashSnapshot(t, app, dataDir))
	default:
		t.Fatalf("unknown projection crash mode %q", mode)
	}
}

func runProjectionCrashChild(t *testing.T, mode, dataDir, readyPath string) (*exec.Cmd, string) {
	t.Helper()
	resultPath := filepath.Join(t.TempDir(), mode+"-result.json")
	cmd := exec.Command(os.Args[0], "-test.run=^TestAppV20OffchainProjectionCrashWorker$", "-test.count=1")
	cmd.Env = append(os.Environ(),
		projectionCrashModeEnv+"="+mode,
		projectionCrashDataEnv+"="+dataDir,
		projectionCrashReadyEnv+"="+readyPath,
		projectionCrashResultEnv+"="+resultPath,
	)
	if mode == "crash" {
		require.NoError(t, cmd.Start())
		return cmd, resultPath
	}
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s child failed:\n%s", mode, output)
	return cmd, resultPath
}

func readProjectionCrashResult(t *testing.T, path string) projectionCrashResult {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var result projectionCrashResult
	require.NoError(t, json.Unmarshal(data, &result))
	return result
}

func waitProjectionCrashReady(t *testing.T, cmd *exec.Cmd, readyPath string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, err := os.Stat(readyPath); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stat crash marker: %v", err)
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatal("projection crash worker did not reach the post-flush seam")
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.NoError(t, cmd.Process.Kill())
	require.Error(t, cmd.Wait(), "SIGKILL fixture must not exit cleanly")
}

func requireOneProjectionBatch(t *testing.T, result projectionCrashResult) {
	t.Helper()
	assert.Equal(t, 1, result.Memories)
	assert.Equal(t, 2, result.Triples)
	assert.Equal(t, 1, result.Challenges)
	assert.Equal(t, 1, result.Corroborations)
	assert.Equal(t, 1, result.AccessLogs)
	assert.Equal(t, 1, result.GovProposals)
	assert.Equal(t, 1, result.Receipts)
}

func TestAppV20SIGKILLAfterOffchainFlushReplaysWithoutProjectionDuplicates(t *testing.T) {
	if os.Getenv(projectionCrashModeEnv) != "" {
		t.Skip("parent fixture only")
	}
	dataDir := filepath.Join(t.TempDir(), "node")
	readyPath := filepath.Join(t.TempDir(), "projection-ready")
	crashing, _ := runProjectionCrashChild(t, "crash", dataDir, readyPath)
	waitProjectionCrashReady(t, crashing, readyPath)

	_, beforePath := runProjectionCrashChild(t, "inspect", dataDir, "")
	before := readProjectionCrashResult(t, beforePath)
	assert.Equal(t, int64(1), before.Height, "Badger Commit must remain behind the durable SQL batch")
	requireOneProjectionBatch(t, before)

	_, replayPath := runProjectionCrashChild(t, "replay", dataDir, "")
	replayed := readProjectionCrashResult(t, replayPath)
	assert.Equal(t, int64(2), replayed.Height)
	require.NotEmpty(t, replayed.AppHash)
	requireOneProjectionBatch(t, replayed)

	_, reopenedPath := runProjectionCrashChild(t, "inspect", dataDir, "")
	reopened := readProjectionCrashResult(t, reopenedPath)
	assert.Equal(t, replayed, reopened, "reopen must preserve the exact Badger/SQL result")
}
