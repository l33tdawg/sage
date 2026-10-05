package main

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	cmtconfig "github.com/cometbft/cometbft/config"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/privval"
	rpctypes "github.com/cometbft/cometbft/rpc/jsonrpc/types"
	cmttypes "github.com/cometbft/cometbft/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/hunch"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/voter"
)

// fakeHunchServer answers the memory gate's check from keywords in the
// context, so the test drives every gate branch deterministically.
func fakeHunchServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Context map[string]string         `json:"context"`
			Checks  map[string]map[string]any `json:"checks"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		results := map[string]any{}
		for id := range req.Checks {
			p := 0.02
			switch id {
			case "lasting":
				m := req.Context["memory"]
				switch {
				case strings.Contains(m, "re-send"):
					p = 0.02
				case strings.Contains(m, "around two hundred"):
					p = 0.7
				default:
					p = 0.98
				}
			case "supported":
				if strings.Contains(req.Context["evidence"], "today") {
					p = 0.97
				}
			}
			results[id] = map[string]any{"kind": "yesno", "p_yes": p}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"model": "fake", "results": results})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func freeLocalPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

// gateMemoryRaw builds a signed memory submission with a caller-chosen ID.
func gateMemoryRaw(t *testing.T, agentKey ed25519.PrivateKey, memoryID, domain, content string, nonce uint64) []byte {
	t.Helper()
	contentHash := sha256.Sum256([]byte(content))
	proofHash := sha256.Sum256([]byte(content + domain))
	proofTime := time.Now().Unix()
	var proofTimeBytes [8]byte
	binary.BigEndian.PutUint64(proofTimeBytes[:], uint64(proofTime))
	proofMessage := append(append([]byte(nil), proofHash[:]...), proofTimeBytes[:]...)
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeMemorySubmit,
		MemorySubmit: &tx.MemorySubmit{
			MemoryID: memoryID, ContentHash: contentHash[:], MemoryType: tx.MemoryTypeFact,
			DomainTag: domain, ConfidenceScore: 0.9, Content: content, Classification: tx.ClearanceInternal,
		},
		AgentPubKey: agentKey.Public().(ed25519.PublicKey),
		AgentSig:    ed25519.Sign(agentKey, proofMessage), AgentBodyHash: proofHash[:],
		AgentTimestamp: proofTime, Nonce: nonce,
	}
	require.NoError(t, tx.SignTx(parsed, agentKey))
	raw, err := tx.EncodeTx(parsed)
	require.NoError(t, err)
	return raw
}

// TestMemoryGateEndToEndOnARealNode boots a real single-validator node, runs
// the real voter with the memory gate against a scripted judge, and checks
// every gate outcome on committed state: accept, reject, abstain-then-review.
func TestMemoryGateEndToEndOnARealNode(t *testing.T) {
	const gateVersion = "e2e-judges"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cometHome := t.TempDir()
	sageHome := t.TempDir()
	t.Setenv("SAGE_HOME", sageHome)
	rootKeyPath := filepath.Join(sageHome, "agent.key")
	bootstrap := &VendoredAgentBootstrapConfig{
		AgentKeyFile: filepath.Join(sageHome, "agents", "gate", "agent.key"),
		HomeDomain:   "depot-notes",
		Clearance:    1,
	}
	require.NoError(t, initCometBFTConfigWithBootstrap(cometHome, rootKeyPath, bootstrap))
	genesis, err := cmttypes.GenesisDocFromFile(filepath.Join(cometHome, "config", "genesis.json"))
	require.NoError(t, err)
	badgerPath := filepath.Join(t.TempDir(), "badger")
	app, badgerStore, projection := openVendoredTestApp(t, badgerPath, filepath.Join(t.TempDir(), "projection.db"))
	require.NoError(t, app.SetExpectedGovernanceDelegationDomain(genesis.ChainID))

	port := freeLocalPort(t)
	cfg := vendoredCometConfig(cometHome)
	cfg.RPC.ListenAddress = fmt.Sprintf("tcp://127.0.0.1:%d", port)
	cfg.Consensus.CreateEmptyBlocks = true
	controller := startVendoredCometTestNodeWithConfig(t, cfg, filepath.Dir(badgerPath), app)
	t.Cleanup(func() { _ = controller.StopChain() })
	rpcEnv, err := controller.GetCometNode().ConfigureRPC()
	require.NoError(t, err)

	// Activate app-v24 exactly as the vendored real-Comet test does, so the
	// Companion agent's memory submissions are the first safe writes.
	rootKey, ok := parseKeyFile(rootKeyPath)
	require.True(t, ok)
	require.NoError(t, badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: tx.CanonicalUpgradeName(24), TargetAppVersion: 24, ActivationHeight: 1,
	}))
	heartbeatRaw, err := buildOperatorRegisterTx(upgradeWatchdogConfig{
		ResolveSigningKey: func() (ed25519.PrivateKey, error) { return rootKey, nil },
	})
	require.NoError(t, err)
	_, err = rpcEnv.BroadcastTxCommit(&rpctypes.Context{}, cmttypes.Tx(heartbeatRaw))
	require.NoError(t, err)

	judge := fakeHunchServer(t)
	selfKey := loadNodeSigningKey(cfg.PrivValidatorKeyFile(), zerolog.Nop())
	require.NotNil(t, selfKey)
	go voter.Run(ctx, app, projection, voter.Config{
		Key: selfKey, CometRPC: fmt.Sprintf("http://127.0.0.1:%d", port), PollInterval: 200 * time.Millisecond,
		Gate: &voter.Gate{Judges: []voter.LastingJudge{hunch.LastingJudge{Client: hunch.New(judge.URL, "", "", 10*time.Second)}},
			SupportJudges: []voter.SupportJudge{hunch.SupportJudge{Client: hunch.New(judge.URL, "", "", 10*time.Second)}},
			Version:       gateVersion},
	}, zerolog.Nop())

	agentKey := agentKeyFromBootstrap(t, bootstrap)
	nonce := uint64(0)
	attachEvidence := func(memoryID, evidence string) {
		t.Helper()
		evID, err := projection.CreateMemoryEvidence(ctx, "gate-agent", evidence)
		require.NoError(t, err)
		require.NoError(t, projection.ClaimMemoryEvidence(ctx, evID, "gate-agent", memoryID))
	}
	submit := func(id, content string) {
		t.Helper()
		nonce++
		res, err := rpcEnv.BroadcastTxCommit(&rpctypes.Context{},
			cmttypes.Tx(gateMemoryRaw(t, agentKey, id, bootstrap.HomeDomain, content, nonce)))
		require.NoError(t, err)
		require.Zero(t, res.CheckTx.Code, res.CheckTx.Log)
		require.Zero(t, res.TxResult.Code, res.TxResult.Log)
	}
	statusOf := func(id string) memory.MemoryStatus {
		m, err := projection.GetMemory(ctx, id)
		if err != nil {
			return ""
		}
		return m.Status
	}
	waitStatus := func(id string, want func(memory.MemoryStatus) bool, what string) {
		t.Helper()
		require.Eventually(t, func() bool { return want(statusOf(id)) }, 45*time.Second, 200*time.Millisecond,
			"%s: %s is %q", what, id, statusOf(id))
	}
	committed := func(s memory.MemoryStatus) bool { return s == memory.StatusCommitted }

	// 1. An ordinary fact is judged lasting and committed.
	factID := "00000000-0000-4000-8000-000000000001"
	submit(factID, "The depot opens at 07:00 on weekdays.")
	waitStatus(factID, committed, "a lasting fact is accepted")

	// 2. A remark about the agent's own session is voted down.
	remarkID := "00000000-0000-4000-8000-000000000002"
	submit(remarkID, "The attachment was lost; the user must re-send the ten numbers.")
	waitStatus(remarkID, func(s memory.MemoryStatus) bool { return s != "" && s != memory.StatusProposed },
		"a session remark is voted on")
	require.NotEqual(t, memory.StatusCommitted, statusOf(remarkID), "a session remark must not be committed")

	// 3. An uncertain memory is NOT voted: it waits in the review queue until
	// the operator decides, and is then voted that way.
	unsureID := "00000000-0000-4000-8000-000000000003"
	submit(unsureID, "The quarterly shipment count was around two hundred units.")
	require.Eventually(t, func() bool {
		q, err := projection.ReviewQueue(ctx, gateVersion, store.ReviewQueueCursor{}, 10)
		return err == nil && len(q) == 1 && q[0].MemoryID == unsureID
	}, 45*time.Second, 200*time.Millisecond, "the uncertain memory reaches the review queue")
	require.Equal(t, memory.StatusProposed, statusOf(unsureID), "an abstained memory stays proposed (no vote)")
	require.NoError(t, projection.SetReviewDecision(ctx, unsureID, gateVersion, memory.VerdictAccept, "operator", "checked"))
	waitStatus(unsureID, committed, "the reviewed memory is voted and committed")

	// 4. A memory submitted with evidence is also asked whether the evidence
	// supports it: evidence about an earlier time does not support a claim
	// stated as current, and the memory is voted down although it is lasting.
	staleID := "00000000-0000-4000-8000-000000000004"
	attachEvidence(staleID, "At the 2023 inspection the north gate alarm was disabled.")
	submit(staleID, "The north gate alarm is disabled.")
	waitStatus(staleID, func(s memory.MemoryStatus) bool { return s != "" && s != memory.StatusProposed },
		"an unsupported memory is voted on")
	require.NotEqual(t, memory.StatusCommitted, statusOf(staleID), "a memory its evidence does not support is not committed")
	sv, ok, err := projection.SemanticVerdict(ctx, staleID, gateVersion)
	require.NoError(t, err)
	require.True(t, ok)
	require.Contains(t, sv.Reason, "not supported by the evidence")

	supportedID := "00000000-0000-4000-8000-000000000005"
	attachEvidence(supportedID, "Work order 118: north gate alarm disabled today.")
	submit(supportedID, "The north gate alarm was disabled under work order 118.")
	waitStatus(supportedID, committed, "a memory its evidence supports is accepted")

	v, ok, err := projection.SemanticVerdict(ctx, remarkID, gateVersion)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, memory.VerdictReject, v.Verdict)
	require.Contains(t, v.Reason, "not lasting memory", "the stored verdict names the judged cause")
}

// startVendoredCometTestNodeWithConfig is startVendoredCometTestNode with a
// caller-supplied Comet config (here: a live RPC listener for the voter).
func startVendoredCometTestNodeWithConfig(
	t *testing.T,
	cfg *cmtconfig.Config,
	dataDir string,
	application abcitypes.Application,
) *SageNodeController {
	t.Helper()
	pv := privval.LoadFilePV(cfg.PrivValidatorKeyFile(), cfg.PrivValidatorStateFile())
	nodeKey, err := p2p.LoadNodeKey(cfg.NodeKeyFile())
	require.NoError(t, err)
	controller := NewSageNodeController(cfg, application, pv, nodeKey, cmtlog.NewNopLogger(), zerolog.Nop(), dataDir)
	require.NoError(t, controller.StartChain())
	return controller
}
