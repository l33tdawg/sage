//go:build integration

package integration

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/tx"
)

// TestAppHashDeterminism_AppV28Activation is the determinism half of the
// app-v28 evidence bar (docs/reference/concepts/app-v28-lifecycle.md): walk the
// FULL app-v2..app-v27 ladder on the four-validator devnet with real
// governance-approved activations, cross the app-v28 seam, and assert every
// node's committed AppHash is byte-identical at H-1, H and H+1 of EVERY rung.
//
// The ladder cannot be shortened. app-v20 requires app-v19 as its immediate
// predecessor, app-v21..app-v28 each require their immediate predecessor, and
// app-v22/app-v23 validate the persisted applied-upgrade records behind them,
// so app-v28 is unreachable by a skip-ahead. That is exactly why the run needs
// the fast-block devnet knob: the 200-block consensus activation floor is NOT
// env-overridable (it is a governance rule), but block time is a devnet knob,
// so a 300ms block time turns the ~4.5h ladder into ~25min.
//
// Requirements, all enforced loudly rather than degraded:
//   - a FRESH devnet (genesis at app_version 0). The harness registers the
//     node-0 validator key as the chain's legacy operator-admin while the
//     pre-app-v9 open-door self-grant still exists — app-v23 activation
//     refuses a chain with no legacy admin, and every rung from app-v9 on is
//     admin-gated. Below app-v8 the legacy self-activating path is also what
//     lets app-v6..app-v8 activate without a governance ballot.
//   - the validator keys the devnet genesis generated under
//     deploy/genesis/node{0..3}/config (override with SAGE_TEST_GENESIS_DIR).
//   - run it through deploy/scripts/run-determinism.sh with
//     DET_TEST=TestAppHashDeterminism_AppV28Activation and a short
//     SAGE_TESTNET_TIMEOUT_COMMIT.
func TestAppHashDeterminism_AppV28Activation(t *testing.T) {
	requireNetwork(t)
	rpcs := allCometRPCs()
	if len(rpcs) < cometRPCCount {
		t.Skipf("need %d validator RPCs, have %d", cometRPCCount, len(rpcs))
	}
	if testing.Short() {
		t.Skip("skipping the app-v2..app-v28 ladder in -short mode")
	}

	preVer := readAppVersion(t, rpcs[0])
	require.Lessf(t, preVer, uint64(8),
		"the ladder needs a FRESH devnet, chain is already at app-v%d: the operator-admin self-grant door closes at app-v9 and app-v6..app-v8 need the pre-app-v8 legacy path", preVer)

	chainID := readChainID(t, rpcs[0])
	govDomain, err := governance.DelegationDomainForChainID(chainID)
	require.NoError(t, err, "derive the app-v20 governance domain for chain %q", chainID)
	t.Logf("ladder starting: chain %s at app-v%d, app-v20 governance domain %s", chainID, preVer, govDomain)

	// The operator-admin key is deliberately NOT a validator key: it is the
	// production shape (an operator agent that proposes, while each node's
	// auto-voter casts its own validator-power ballot), and it keeps this
	// harness's signatures from interleaving with the auto-voters' nonce
	// sequence on the validator keys.
	_, adminKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err, "generate operator-admin key")
	registerDevnetOperatorAdmin(t, rpcs[0], adminKey)

	started := time.Now()
	for target := uint64(6); target <= 28; target++ {
		climbRung(t, rpcs, adminKey, target, govDomain)
	}
	t.Logf("LADDER PASS: app-v2..app-v28 activated one rung at a time in %s with byte-identical AppHash at every seam",
		time.Since(started).Round(time.Second))
}

// appV28ActivationFloorBlocks mirrors the chain's defaultUpgradeDelayBlocks: the
// consensus floor between a quorum-approved plan and its activation height. It
// is a governance rule, not a knob, so the harness waits it out honestly.
const appV28ActivationFloorBlocks = 200

// climbRung drives one rung (target-1 -> target) and asserts the seam.
func climbRung(t *testing.T, rpcs []string, adminKey ed25519.PrivateKey, target uint64, govDomain string) {
	t.Helper()
	cur := readAppVersion(t, rpcs[0])
	if cur >= target {
		t.Logf("app-v%d already active — rung skipped", target)
		return
	}
	// app-v6 is the one legal jump (it backfills app-v2..app-v5); every later
	// rung is strictly next (app-v20..app-v28 require their immediate
	// predecessor outright, and the harness walks the rest in order).
	require.Greaterf(t, target, cur, "chain is at app-v%d, asked to climb to app-v%d", cur, target)

	name := fmt.Sprintf("app-v%d", target)
	domain := ""
	if target == 20 {
		// The app-v20 ceremony is the one transition tagged with the
		// chain-derived governance domain; every other rung must leave it empty.
		domain = govDomain
	}

	ptx, err := buildUpgradeProposeTx(adminKey, name, target, appV28ActivationFloorBlocks, domain)
	require.NoError(t, err, "build upgrade propose %s", name)

	rungStart := time.Now()
	res := broadcastParsedTxCommit(t, rpcs[0], ptx)
	require.Equalf(t, uint32(0), res.CheckCode, "upgrade propose %s rejected in CheckTx: %s", name, res.CheckLog)
	require.Equalf(t, uint32(0), res.TxCode, "upgrade propose %s rejected at execution: %s", name, res.TxLog)
	activation := res.Height + appV28ActivationFloorBlocks

	// From app-v8 on the propose runs the governance ballot; each node's
	// auto-voter accepts every target at or below its auto-vote ceiling. While
	// app-v28's gate was dormant that ceiling sat at 27, so the final rung was
	// the deliberate case: the auto-voter abstained and quorum was cast
	// explicitly — one validator vote per node — which is exactly what the
	// operator vote surfaces exist for. The release that carries the fork's
	// evidence raises the ceiling to meet the gate, so the harness follows the
	// binary it is built with: explicit votes only while the target sits above
	// the auto-vote ceiling.
	if target > sageabci.MaxSupportedAppVersion() {
		proposalID := governance.ComputeProposalID(agentIDFor(adminKey), res.Height, governance.OpUpgrade, name)
		t.Logf("app-v%d ballot %s opened at height %d; casting explicit accept votes (target is above the auto-vote ceiling %d)", target, proposalID, res.Height, sageabci.MaxSupportedAppVersion())
		for node := 1; node < cometRPCCount; node++ {
			vote := castValidatorUpgradeVote(t, rpcs[node], loadDevnetValidatorKey(t, node), proposalID)
			require.Equalf(t, uint32(0), vote.CheckCode, "node%d app-v%d vote rejected in CheckTx: %s", node, target, vote.CheckLog)
			require.Equalf(t, uint32(0), vote.TxCode, "node%d app-v%d vote rejected at execution: %s", node, target, vote.TxLog)
			t.Logf("node%d accept vote committed at height %d", node, vote.Height)
		}
	}

	waitForAllNodesAppVersion(t, rpcs, target, ladderRungTimeout())
	t.Logf("app-v%d activated at height %d (rung took %s)", target, activation, time.Since(rungStart).Round(time.Second))

	for _, h := range []int64{activation - 1, activation, activation + 1} {
		agreed := assertAppHashAgreement(t, rpcs, h)
		t.Logf("app-v%d seam: all %d nodes byte-identical at height %d (%s)", target, cometRPCCount, h, shortHash(agreed))
	}
}

func shortHash(h string) string {
	if len(h) <= 16 {
		return h
	}
	return h[:16] + "…"
}

func agentIDFor(key ed25519.PrivateKey) string {
	pub, _ := key.Public().(ed25519.PublicKey)
	return hex.EncodeToString(pub)
}

// ladderRungTimeout bounds one rung: the 200-block floor plus quorum latency.
func ladderRungTimeout() time.Duration {
	if v := os.Getenv("SAGE_TEST_LADDER_RUNG_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return 20 * time.Minute
}

// waitForAllNodesAppVersion polls /abci_info until every node reports at least
// target. The chain lifts every node at the same activation height, so this is
// the convergence check: a straggler here would be the production halt symptom.
func waitForAllNodesAppVersion(t *testing.T, rpcs []string, target uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		versions := make([]uint64, 0, len(rpcs))
		all := true
		for _, rpc := range rpcs {
			v := readAppVersion(t, rpc)
			versions = append(versions, v)
			if v < target {
				all = false
			}
		}
		if all {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for app-v%d on all nodes (have %v)", timeout, target, versions)
		}
		time.Sleep(2 * time.Second)
	}
}

// readChainID returns the CometBFT chain id from /status.
func readChainID(t *testing.T, rpc string) string {
	t.Helper()
	resp, err := http.Get(rpc + "/status") //nolint:noctx
	require.NoError(t, err, "GET %s/status", rpc)
	defer resp.Body.Close()
	var out struct {
		Result struct {
			NodeInfo struct {
				Network string `json:"network"`
			} `json:"node_info"`
		} `json:"result"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.NotEmpty(t, out.Result.NodeInfo.Network, "chain id missing from /status")
	return out.Result.NodeInfo.Network
}

// devnetGenesisDir locates the generated devnet genesis. The harness regenerates
// it on every run; the validator keys inside are the chain's validator
// identities, which is what makes an operator vote a validator vote.
func devnetGenesisDir() string {
	if dir := os.Getenv("SAGE_TEST_GENESIS_DIR"); dir != "" {
		return dir
	}
	return filepath.Join("..", "..", "deploy", "genesis")
}

func loadDevnetValidatorKey(t *testing.T, node int) ed25519.PrivateKey {
	t.Helper()
	path := filepath.Join(devnetGenesisDir(), fmt.Sprintf("node%d", node), "config", "priv_validator_key.json")
	raw, err := os.ReadFile(path)
	require.NoErrorf(t, err, "read devnet validator key %s (run through deploy/scripts/run-determinism.sh)", path)
	var doc struct {
		PrivKey struct {
			Type  string `json:"type"`
			Value string `json:"value"`
		} `json:"priv_key"`
	}
	require.NoError(t, json.Unmarshal(raw, &doc), "parse %s", path)
	require.Equal(t, "tendermint/PrivKeyEd25519", doc.PrivKey.Type, "validator key type in %s", path)
	keyBytes, err := base64.StdEncoding.DecodeString(doc.PrivKey.Value)
	require.NoError(t, err, "decode validator key in %s", path)
	require.Len(t, keyBytes, ed25519.PrivateKeySize, "validator key length in %s", path)
	return ed25519.PrivateKey(keyBytes)
}

// parsedTxCommit is the CometBFT /broadcast_tx_commit verdict for one tx.
type parsedTxCommit struct {
	Hash      string
	Height    int64
	CheckCode uint32
	CheckLog  string
	TxCode    uint32
	TxLog     string
}

// broadcastParsedTxCommit pushes a signed tx through CometBFT's
// /broadcast_tx_commit RPC and returns both verdicts plus the commit height —
// the height is what pins the chain-computed activation height.
func broadcastParsedTxCommit(t *testing.T, rpc string, ptx *tx.ParsedTx) parsedTxCommit {
	t.Helper()
	encoded, err := tx.EncodeTx(ptx)
	require.NoError(t, err, "encode tx")
	url := fmt.Sprintf("%s/broadcast_tx_commit?tx=0x%s", rpc, hex.EncodeToString(encoded))
	client := &http.Client{Timeout: 2 * time.Minute}
	resp, err := client.Post(url, "application/json", bytes.NewReader(nil)) //nolint:noctx
	require.NoError(t, err, "broadcast tx to %s", rpc)
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	require.NoError(t, err)

	var out struct {
		Result struct {
			Hash    string `json:"hash"`
			Height  string `json:"height"`
			CheckTx struct {
				Code uint32 `json:"code"`
				Log  string `json:"log"`
			} `json:"check_tx"`
			TxResult struct {
				Code uint32 `json:"code"`
				Log  string `json:"log"`
			} `json:"tx_result"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
			Data    string `json:"data"`
		} `json:"error"`
	}
	require.NoErrorf(t, json.Unmarshal(body, &out), "decode broadcast response %s", string(body))
	require.Nilf(t, out.Error, "broadcast rpc error: %s %s", out.Error, string(body))
	height, _ := strconv.ParseInt(out.Result.Height, 10, 64)
	return parsedTxCommit{
		Hash:      out.Result.Hash,
		Height:    height,
		CheckCode: out.Result.CheckTx.Code,
		CheckLog:  out.Result.CheckTx.Log,
		TxCode:    out.Result.TxResult.Code,
		TxLog:     out.Result.TxResult.Log,
	}
}

// agentProof mirrors the agent identity proof every agent-signed tx carries:
// Ed25519 over sha256(body) || bigEndian(timestamp). Mirrors
// cmd/sage-gui's buildOperatorRegisterTxWithNonce.
func agentProof(key ed25519.PrivateKey, body []byte, ts int64) (bodyHash, sig []byte) {
	sum := sha256.Sum256(body)
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(ts)) //nolint:gosec // timestamps are non-negative
	msg := append(append([]byte{}, sum[:]...), tsBytes...)
	return sum[:], ed25519.Sign(key, msg)
}

// registerDevnetOperatorAdmin seeds the chain's legacy admin from the node-0
// validator key while the pre-app-v9 open-door self-grant still exists. Every
// rung from app-v9 on is admin-gated, and app-v23 activation refuses a chain
// with no legacy admin, so this registration is load-bearing for the ladder.
// Mirrors cmd/sage-gui's ensureOperatorAdminRegistered.
func registerDevnetOperatorAdmin(t *testing.T, rpc string, key ed25519.PrivateKey) {
	t.Helper()
	pub, ok := key.Public().(ed25519.PublicKey)
	require.True(t, ok)
	const name, role, bio = "operator-admin", "admin", "node operator key"
	ts := time.Now().Unix()
	bodyHash, sig := agentProof(key, []byte(name+role+bio), ts)
	ptx := &tx.ParsedTx{
		Type:      tx.TxTypeAgentRegister,
		Nonce:     uint64(time.Now().UnixNano()), //nolint:gosec // unique nonce source, same idiom as the sibling helpers
		Timestamp: time.Unix(ts, 0),
		AgentRegister: &tx.AgentRegister{
			AgentID: hex.EncodeToString(pub),
			Name:    name,
			Role:    role,
			BootBio: bio,
		},
		AgentPubKey:    pub,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
	}
	require.NoError(t, tx.SignTx(ptx, key), "sign operator register tx")
	res := broadcastParsedTxCommit(t, rpc, ptx)
	// Idempotent on re-run: the consensus handler answers Code 0
	// "already registered" for a known identity.
	ok = res.TxCode == 0 || strings.Contains(res.TxLog, "already registered")
	require.Truef(t, ok && res.CheckCode == 0,
		"operator admin registration rejected: check=%d %q tx=%d %q", res.CheckCode, res.CheckLog, res.TxCode, res.TxLog)
	t.Logf("operator admin %s registered (tx code %d, log %q)", agentIDFor(key), res.TxCode, res.TxLog)
}

// buildUpgradeProposeTx builds an UpgradePropose carrying the agent identity
// proof; the outer signature is the identity the app-v8+ admin gate and the
// governance ballot key on.
func buildUpgradeProposeTx(key ed25519.PrivateKey, name string, target uint64, delay int64, govDomain string) (*tx.ParsedTx, error) {
	pub, ok := key.Public().(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("proposer key is not ed25519")
	}
	ts := time.Now().Unix()
	bodyHash, sig := agentProof(key, []byte(name), ts)
	ptx := &tx.ParsedTx{
		Type:      tx.TxTypeUpgradePropose,
		Nonce:     uint64(time.Now().UnixNano()), //nolint:gosec // unique nonce source, same idiom as the sibling helpers
		Timestamp: time.Unix(ts, 0),
		UpgradePropose: &tx.UpgradePropose{
			Name:               name,
			TargetAppVersion:   target,
			ProposerID:         hex.EncodeToString(pub),
			UpgradeDelayBlocks: delay,
			GovernanceDomain:   govDomain,
		},
		AgentPubKey:    pub,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
	}
	if err := tx.SignTx(ptx, key); err != nil {
		return nil, fmt.Errorf("sign upgrade propose: %w", err)
	}
	return ptx, nil
}

// castValidatorUpgradeVote casts an explicit ACCEPT on the active app-v28
// ballot, signed by a validator key. A direct GovVote is a validator vote
// (routed by the outer signature), which is what gives it quorum weight.
func castValidatorUpgradeVote(t *testing.T, rpc string, key ed25519.PrivateKey, proposalID string) parsedTxCommit {
	t.Helper()
	ptx := &tx.ParsedTx{
		Type:      tx.TxTypeGovVote,
		Nonce:     uint64(time.Now().UnixNano()), //nolint:gosec // unique nonce source, same idiom as the sibling helpers
		Timestamp: time.Now(),
		GovVote: &tx.GovVote{
			ProposalID: proposalID,
			Decision:   tx.VoteDecisionAccept,
		},
	}
	require.NoError(t, tx.SignTx(ptx, key), "sign gov vote")
	return broadcastParsedTxCommit(t, rpc, ptx)
}
