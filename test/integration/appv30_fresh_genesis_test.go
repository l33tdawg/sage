//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	cmttypes "github.com/cometbft/cometbft/types"
	"github.com/stretchr/testify/require"

	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/tx"
)

// TestPrepareAppV30FreshGenesisFixture runs before the isolated devnet starts.
// It signs one complete genesis and copies those exact bytes to every validator.
// Its generated Root key stays in the throwaway testnet directory, never in CI
// evidence artifacts. Ordinary integration test runs do not alter genesis.
func TestPrepareAppV30FreshGenesisFixture(t *testing.T) {
	if os.Getenv("SAGE_TEST_PREPARE_FRESH_V30") != "1" {
		t.Skip("fixture preparation requires the isolated fresh-v30 harness")
	}
	dir := devnetGenesisDir()
	genesis, err := cmttypes.GenesisDocFromFile(filepath.Join(dir, "node0/config/genesis.json"))
	require.NoError(t, err)
	require.Len(t, genesis.Validators, cometRPCCount)
	_, root, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	manifest := sageabci.FreshGenesisManifest{
		Version: sageabci.FreshGenesisManifestVersion, AppVersion: 30,
		RootID: agentIDFor(root),
	}
	for _, validator := range genesis.Validators {
		manifest.Validators = append(manifest.Validators, sageabci.FreshGenesisValidator{
			ValidatorID: hex.EncodeToString(validator.PubKey.Bytes()), Power: validator.Power,
		})
	}
	sort.Slice(manifest.Validators, func(i, j int) bool {
		return manifest.Validators[i].ValidatorID < manifest.Validators[j].ValidatorID
	})
	manifest.RootSignature = hex.EncodeToString(ed25519.Sign(root,
		sageabci.FreshGenesisManifestSignBytes(genesis.ChainID, manifest)))
	_, err = sageabci.VerifyFreshGenesisManifest(genesis.ChainID, manifest)
	require.NoError(t, err)
	genesis.AppState, err = json.Marshal(map[string]any{"sage": map[string]any{
		"initial_admin": manifest.RootID, "fresh_bootstrap": manifest,
	}})
	require.NoError(t, err)
	genesis.ConsensusParams.Version.App = 30
	for node := 0; node < cometRPCCount; node++ {
		require.NoError(t, genesis.SaveAs(filepath.Join(dir, fmt.Sprintf("node%d/config/genesis.json", node))))
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "fresh-v30-root.key"), root, 0o600))
	t.Log("prepared identical signed app-v30 genesis for four validators; no upgrade plan")
}

// TestAppV30FreshGenesisFourValidators observes app-v30 on real Comet/ABCI
// processes, from block1 through memory/lineage/taint decisions and an abrupt
// validator restart. Mixed-architecture mode verifies the live process
// architectures in run-determinism.sh. Authorized snapshot transfer is covered
// separately; catching up this restarted validator must not be called state sync.
func TestAppV30FreshGenesisFourValidators(t *testing.T) {
	if os.Getenv("SAGE_TEST_FRESH_V30_PROJECT") != "sage-fresh-v30" {
		t.Skip("requires the owned fresh-v30 four-validator harness")
	}
	requireNetwork(t)
	rpcs, apis := allCometRPCs(), allAPIURLs()
	rootBytes, err := os.ReadFile(filepath.Join(devnetGenesisDir(), "fresh-v30-root.key"))
	require.NoError(t, err)
	require.Len(t, rootBytes, ed25519.PrivateKeySize)
	root := ed25519.PrivateKey(rootBytes)
	for _, rpc := range rpcs {
		require.Equal(t, uint64(30), readAppVersion(t, rpc), "fresh node must start at app30")
	}
	waitForAllNodesHeight(t, rpcs, 3, 2*time.Minute)
	for _, height := range []int64{1, 2, 3} {
		t.Logf("fresh app30 matched height%d AppHash=%s", height, assertAppHashAgreement(t, rpcs, height))
	}

	const parentContent = "fresh app-v30 live parent"
	parent := freshV30Submit(t, root, apis[0], parentContent, nil)
	freshV30WaitReadable(t, root, apis, parent, true)
	hash := sha256.Sum256([]byte(parentContent))
	parentRef := tx.ParentRef{SourceChainID: readChainID(t, rpcs[0]), MemoryID: parent, ContentHash: hash}
	child := freshV30Submit(t, root, apis[1], "fresh app-v30 derived child", []tx.ParentRef{parentRef})
	freshV30WaitReadable(t, root, apis, child, true)

	evidence := sha256.Sum256([]byte("fresh app30 process taint evidence"))
	apply, err := tx.EncodeTaintApplyPayload(tx.TaintApplyPayload{
		Version: tx.TaintApplyPayloadVersion, Action: tx.TaintActionApply,
		TargetChainID: parentRef.SourceChainID, TargetMemoryID: parent,
		TargetContentHash: hash[:], ScopeKind: tx.TaintScopeDirect,
		ReasonCategory: 1, EvidenceDigest: evidence[:],
	})
	require.NoError(t, err)
	decision, err := tx.TaintApplyDecisionID(apply)
	require.NoError(t, err)
	applyHeight := freshV30Govern(t, root, rpcs, decision, apply)
	freshV30WaitReadable(t, root, apis, parent, false)
	freshV30WaitReadable(t, root, apis, child, false)
	applyObserved := readLatestHeight(t, rpcs[0])
	waitForAllNodesHeight(t, rpcs, applyObserved+1, 2*time.Minute)
	t.Logf("app30 apply/lineage hold matched height%d AppHash=%s", applyObserved+1,
		assertAppHashAgreement(t, rpcs, applyObserved+1))

	// Kill only containers whose Compose ownership label matches this test.
	for _, service := range []string{"cometbft3", "abci3"} {
		container := "sage-fresh-v30-" + service + "-1"
		label := freshV30Docker(t, "inspect", "--format", "{{index .Config.Labels \"com.docker.compose.project\"}}", container)
		require.Equal(t, "sage-fresh-v30\n", label)
		freshV30Docker(t, "kill", "--signal", "KILL", container)
	}
	whileDown := readLatestHeight(t, rpcs[0])
	waitForAllNodesHeight(t, rpcs[:3], whileDown+3, 2*time.Minute)
	for _, service := range []string{"abci3", "cometbft3"} {
		freshV30Docker(t, "start", "sage-fresh-v30-"+service+"-1")
	}
	freshV30WaitHealthy(t, apis[3])
	waitForAllNodesAppVersion(t, rpcs, 30, 2*time.Minute)
	joined := readLatestHeight(t, rpcs[0]) + 3
	waitForAllNodesHeight(t, rpcs, joined, 2*time.Minute)
	t.Logf("app30 crash/restart matched height%d AppHash=%s", joined, assertAppHashAgreement(t, rpcs, joined))
	freshV30WaitReadable(t, root, apis, parent, false)
	freshV30WaitReadable(t, root, apis, child, false)

	// Respect the real governance proposer cooldown rather than shortening it
	// in the production binary used by this gate.
	waitForAllNodesHeight(t, rpcs, applyHeight+governance.CooldownBlocks+2, 3*time.Minute)
	decisionBytes, err := hex.DecodeString(decision)
	require.NoError(t, err)
	clear, err := tx.EncodeTaintClearPayload(tx.TaintClearPayload{
		Version: tx.TaintClearPayloadVersion, Action: tx.TaintActionClear,
		DecisionID: decisionBytes, ReasonCategory: 2, EvidenceDigest: evidence[:],
	})
	require.NoError(t, err)
	clearTarget, err := tx.TaintClearTargetID(clear)
	require.NoError(t, err)
	freshV30Govern(t, root, rpcs, clearTarget, clear)
	freshV30WaitReadable(t, root, apis, parent, true)
	freshV30WaitReadable(t, root, apis, child, true)
	clearObserved := readLatestHeight(t, rpcs[0])
	boundary := ((clearObserved / 100) + 1) * 100
	waitForAllNodesHeight(t, rpcs, boundary+2, 3*time.Minute)
	for _, height := range []int64{clearObserved + 1, boundary, boundary + 1} {
		t.Logf("app30 clear/epoch matched height%d AppHash=%s", height, assertAppHashAgreement(t, rpcs, height))
	}
}

func freshV30Request(t *testing.T, root ed25519.PrivateKey, method, url string, value any) (int, map[string]any) {
	t.Helper()
	var body []byte
	var err error
	if value != nil {
		body, err = json.Marshal(value)
		require.NoError(t, err)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, url, bytes.NewReader(body))
	require.NoError(t, err)
	nonce := make([]byte, 8)
	_, err = rand.Read(nonce)
	require.NoError(t, err)
	ts := time.Now().Unix()
	sig := auth.SignRequestWithNonce(root, method, req.URL.Path, body, ts, nonce)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-ID", agentIDFor(root))
	req.Header.Set("X-Timestamp", strconv.FormatInt(ts, 10))
	req.Header.Set("X-Nonce", hex.EncodeToString(nonce))
	req.Header.Set("X-Signature", hex.EncodeToString(sig))
	client := &http.Client{Timeout: time.Minute}
	response, err := client.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	require.NoError(t, err)
	var result map[string]any
	require.NoError(t, json.Unmarshal(raw, &result), "HTTP%d body%s", response.StatusCode, raw)
	return response.StatusCode, result
}

func freshV30Submit(t *testing.T, root ed25519.PrivateKey, api, content string, parents []tx.ParentRef) string {
	t.Helper()
	body := map[string]any{"content": content, "memory_type": "observation", "domain_tag": "general", "confidence_score": 0.9}
	if len(parents) > 0 {
		refs := make([]map[string]string, 0, len(parents))
		for _, parent := range parents {
			refs = append(refs, map[string]string{"source_chain_id": parent.SourceChainID, "memory_id": parent.MemoryID, "content_hash": hex.EncodeToString(parent.ContentHash[:])})
		}
		body["derived_from"] = refs
	}
	status, result := freshV30Request(t, root, http.MethodPost, api+"/v1/memory/submit", body)
	require.Equal(t, http.StatusCreated, status, "%v", result)
	id, ok := result["memory_id"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)
	return id
}

func freshV30WaitReadable(t *testing.T, root ed25519.PrivateKey, apis []string, id string, readable bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, api := range apis {
			status, result := freshV30Request(t, root, http.MethodGet, api+"/v1/memory/"+id, nil)
			if readable {
				if status != http.StatusOK || result["status"] != "committed" {
					return false
				}
			} else {
				kind := result["type"]
				if status != http.StatusUnavailableForLegalReasons ||
					(kind != "https://sage.dev/errors/memory-tainted" && kind != "https://sage.dev/errors/memory-taint-pending") {
					return false
				}
			}
		}
		return true
	}, 2*time.Minute, time.Second, "memory%s readable=%t on every validator", id, readable)
}

func freshV30Govern(t *testing.T, root ed25519.PrivateKey, rpcs []string, target string, payload []byte) int64 {
	t.Helper()
	proposal := &tx.ParsedTx{Type: tx.TxTypeGovPropose, Nonce: uint64(time.Now().UnixNano()), Timestamp: time.Now(), //nolint:gosec // positive test nonce
		GovPropose: &tx.GovPropose{Operation: tx.GovOpMemoryTaint, TargetID: target, Payload: payload, Reason: "fresh app30 process evidence"}}
	require.NoError(t, tx.SignTx(proposal, root))
	result := broadcastParsedTxCommit(t, rpcs[0], proposal)
	require.Zero(t, result.CheckCode, result.CheckLog)
	require.Zero(t, result.TxCode, result.TxLog)
	id := governance.ComputeProposalID(agentIDFor(root), result.Height, governance.OpMemoryTaint, target)
	for node := 0; node < 3; node++ {
		vote := castValidatorUpgradeVote(t, rpcs[node], loadDevnetValidatorKey(t, node), id)
		require.Zero(t, vote.CheckCode, vote.CheckLog)
		require.Zero(t, vote.TxCode, vote.TxLog)
		if node == 2 {
			return vote.Height
		}
	}
	t.Fatal("no governance vote committed")
	return 0
}

func freshV30Docker(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	require.NoError(t, err, "docker%v:%s", args, output)
	return string(output)
}

func freshV30WaitHealthy(t *testing.T, api string) {
	t.Helper()
	client := &http.Client{Timeout: 3 * time.Second}
	require.Eventually(t, func() bool {
		response, err := client.Get(api + "/health") //nolint:noctx // client has bounded timeout
		if err != nil {
			return false
		}
		response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, 2*time.Minute, time.Second)
}
