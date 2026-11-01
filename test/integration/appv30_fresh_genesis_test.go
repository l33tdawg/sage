//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
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
	"github.com/l33tdawg/sage/internal/store"
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
	_, agent, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	manifest := sageabci.FreshGenesisManifest{
		Version: sageabci.FreshGenesisManifestVersion, AppVersion: 30,
		RootID: agentIDFor(root),
		Agent: &sageabci.FreshGenesisAgent{
			AgentID: agentIDFor(agent), Profile: store.AppV23ProfileCompanion,
			Clearance: 1, Capabilities: 15, HomeDomain: "fresh-v30",
		},
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
	manifest.Agent.Signature = hex.EncodeToString(ed25519.Sign(agent,
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
	require.NoError(t, os.WriteFile(filepath.Join(dir, "fresh-v30-agent.key"), agent, 0o600))
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
	genesis, err := cmttypes.GenesisDocFromFile(filepath.Join(devnetGenesisDir(), "node0/config/genesis.json"))
	require.NoError(t, err)
	validatorKeys, err := readFreshV30ValidatorKeys(genesis, os.Getenv("SAGE_TEST_FRESH_V30_VALIDATOR_KEY_DIR"))
	require.NoError(t, err)
	requireNetwork(t)
	rpcs, apis := allCometRPCs(), allAPIURLs()
	rootBytes, err := os.ReadFile(filepath.Join(devnetGenesisDir(), "fresh-v30-root.key"))
	require.NoError(t, err)
	require.Len(t, rootBytes, ed25519.PrivateKeySize)
	root := ed25519.PrivateKey(rootBytes)
	agentBytes, err := os.ReadFile(filepath.Join(devnetGenesisDir(), "fresh-v30-agent.key"))
	require.NoError(t, err)
	require.Len(t, agentBytes, ed25519.PrivateKeySize)
	agent := ed25519.PrivateKey(agentBytes)
	for _, rpc := range rpcs {
		require.Equal(t, uint64(30), readAppVersion(t, rpc), "fresh node must start at app30")
	}
	waitForAllNodesHeight(t, rpcs, 3, 2*time.Minute)
	for _, height := range []int64{1, 2, 3} {
		t.Logf("fresh app30 matched height%d AppHash=%s", height, assertAppHashAgreement(t, rpcs, height))
	}

	const parentContent = "fresh app-v30 live parent"
	// Root REST control is local-only; Docker port forwarding is a remote socket.
	// Exercise the signed Companion's ordinary network boundary for memory I/O.
	parent := freshV30Submit(t, agent, apis[0], parentContent, nil)
	freshV30WaitReadable(t, agent, apis, parent, true)
	hash := sha256.Sum256([]byte(parentContent))
	parentRef := tx.ParentRef{SourceChainID: readChainID(t, rpcs[0]), MemoryID: parent, ContentHash: hash}
	child := freshV30Submit(t, agent, apis[1], "fresh app-v30 derived child", []tx.ParentRef{parentRef})
	freshV30WaitReadable(t, agent, apis, child, true)

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
	applyHeight := freshV30Govern(t, root, rpcs, validatorKeys, decision, apply)
	freshV30WaitReadable(t, agent, apis, parent, false)
	freshV30WaitReadable(t, agent, apis, child, false)
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
	freshV30WaitReadable(t, agent, apis, parent, false)
	freshV30WaitReadable(t, agent, apis, child, false)

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
	freshV30Govern(t, root, rpcs, validatorKeys, clearTarget, clear)
	freshV30WaitReadable(t, agent, apis, parent, true)
	freshV30WaitReadable(t, agent, apis, child, true)
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
	body := map[string]any{"content": content, "memory_type": "observation", "domain_tag": "fresh-v30", "confidence_score": 0.9}
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

// readFreshV30ValidatorKeys validates the host-owned vote-key export against
// the original public genesis. The export must cover its distinct full roster;
// it does not replace the canonical genesis or alter the container-owned keys.
func readFreshV30ValidatorKeys(genesis *cmttypes.GenesisDoc, directory string) ([]ed25519.PrivateKey, error) {
	if directory == "" || genesis == nil || len(genesis.Validators) != cometRPCCount {
		return nil, fmt.Errorf("fresh-v30 vote keys require an explicit export directory and four-validator genesis")
	}
	roster := make(map[string]struct{}, cometRPCCount)
	for _, validator := range genesis.Validators {
		if validator.PubKey == nil || validator.PubKey.Type() != "ed25519" ||
			len(validator.PubKey.Bytes()) != ed25519.PublicKeySize || validator.Power <= 0 {
			return nil, fmt.Errorf("fresh-v30 genesis has an invalid validator")
		}
		identity := string(validator.PubKey.Bytes())
		if _, duplicate := roster[identity]; duplicate {
			return nil, fmt.Errorf("fresh-v30 genesis has duplicate validators")
		}
		roster[identity] = struct{}{}
	}
	keys := make([]ed25519.PrivateKey, 0, cometRPCCount)
	seen := make(map[string]struct{}, cometRPCCount)
	for node := 0; node < cometRPCCount; node++ {
		raw, err := os.ReadFile(filepath.Join(directory, fmt.Sprintf("node%d.json", node)))
		if err != nil {
			return nil, fmt.Errorf("read fresh validator%d exported vote key: %w", node, err)
		}
		var document struct {
			Public struct {
				Type  string `json:"type"`
				Value string `json:"value"`
			} `json:"pub_key"`
			Private struct {
				Type  string `json:"type"`
				Value string `json:"value"`
			} `json:"priv_key"`
		}
		if err := json.Unmarshal(raw, &document); err != nil {
			return nil, fmt.Errorf("parse fresh validator%d exported vote key: %w", node, err)
		}
		public, publicErr := base64.StdEncoding.DecodeString(document.Public.Value)
		private, privateErr := base64.StdEncoding.DecodeString(document.Private.Value)
		if document.Public.Type != "tendermint/PubKeyEd25519" ||
			document.Private.Type != "tendermint/PrivKeyEd25519" ||
			publicErr != nil || privateErr != nil ||
			len(public) != ed25519.PublicKeySize || len(private) != ed25519.PrivateKeySize {
			return nil, fmt.Errorf("fresh validator%d exported vote key has invalid Ed25519 encoding", node)
		}
		key := ed25519.PrivateKey(private)
		if !bytes.Equal(ed25519.NewKeyFromSeed(key.Seed()), private) ||
			!bytes.Equal(public, private[ed25519.SeedSize:]) {
			return nil, fmt.Errorf("fresh validator%d exported private/public key pair does not match", node)
		}
		identity := string(public)
		if _, member := roster[identity]; !member {
			return nil, fmt.Errorf("fresh validator%d exported vote key is outside the genesis roster", node)
		}
		if _, duplicate := seen[identity]; duplicate {
			return nil, fmt.Errorf("fresh validator%d exported vote key duplicates another validator", node)
		}
		seen[identity] = struct{}{}
		keys = append(keys, key)
	}
	return keys, nil
}

func freshV30Govern(t *testing.T, root ed25519.PrivateKey, rpcs []string, validatorKeys []ed25519.PrivateKey, target string, payload []byte) int64 {
	t.Helper()
	proposal := &tx.ParsedTx{Type: tx.TxTypeGovPropose, Nonce: uint64(time.Now().UnixNano()), Timestamp: time.Now(), //nolint:gosec // positive test nonce
		GovPropose: &tx.GovPropose{Operation: tx.GovOpMemoryTaint, TargetID: target, Payload: payload, Reason: "fresh app30 process evidence"}}
	require.NoError(t, tx.SignTx(proposal, root))
	result := broadcastParsedTxCommit(t, rpcs[0], proposal)
	require.Zero(t, result.CheckCode, result.CheckLog)
	require.Zero(t, result.TxCode, result.TxLog)
	id := governance.ComputeProposalID(agentIDFor(root), result.Height, governance.OpMemoryTaint, target)
	for node := 0; node < 3; node++ {
		vote := castValidatorUpgradeVote(t, rpcs[node], validatorKeys[node], id)
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
