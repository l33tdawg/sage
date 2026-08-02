package web

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/tx"
)

// This file is the commit-confirmed signing/broadcast plumbing for the v11.3
// RBAC reassign + access-control flow. The existing dashboard broadcast path
// (broadcastTxSync) is fire-and-forget: it cannot confirm a tx executed or
// enforce the strict propose -> executed -> reassign -> grant ordering the
// flow needs, so those handlers use the helpers here instead. Nothing here
// changes consensus; it only builds/signs/broadcasts existing tx types.

// cometCommitResult mirrors the /broadcast_tx_commit JSON envelope (a subset of
// the api/rest cometCommitResponse). It surfaces both the CheckTx and the
// FinalizeBlock (TxResult) codes so a consensus-side rejection becomes a real
// error rather than a silent success.
type cometCommitResult struct {
	Result struct {
		CheckTx struct {
			Code int    `json:"code"`
			Log  string `json:"log"`
		} `json:"check_tx"`
		TxResult struct {
			Code int    `json:"code"`
			Log  string `json:"log"`
		} `json:"tx_result"`
		Hash   string `json:"hash"`
		Height int64  `json:"height,string"`
	} `json:"result"`
	Error *struct {
		Message string `json:"message"`
		Data    string `json:"data"`
	} `json:"error"`
}

// rbacCommitTimeout bounds how long a commit-confirmed broadcast waits for
// /broadcast_tx_commit. Matches the api/rest client default (60s) so slow
// single-validator commits have headroom; overridable via SAGE_TX_COMMIT_TIMEOUT_MS.
func rbacCommitTimeout() time.Duration {
	if v := os.Getenv("SAGE_TX_COMMIT_TIMEOUT_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return 60 * time.Second
}

// broadcastTxCommitWeb sends a tx via /broadcast_tx_commit and waits for block
// finalization, returning (hash, committedHeight, finalizeLog). It returns an
// error if the RPC fails, or if the tx is rejected in CheckTx or FinalizeBlock,
// so callers can surface real consensus rejections.
func broadcastTxCommitWeb(cometRPC string, txBytes []byte) (hash string, height int64, txLog string, err error) {
	return broadcastTxCommitWebContext(context.Background(), cometRPC, txBytes)
}

// broadcastTxCommitWebContext is the request-aware variant used by CEREBRUM
// mutations. A closed browser tab or an explicit client deadline must cancel
// the in-flight Comet request instead of leaving the server goroutine detached
// for the full commit timeout. Callers must still treat cancellation as an
// indeterminate result: consensus may already have accepted the transaction.
func broadcastTxCommitWebContext(parent context.Context, cometRPC string, txBytes []byte) (hash string, height int64, txLog string, err error) {
	txHex := hex.EncodeToString(txBytes)
	url := fmt.Sprintf("%s/broadcast_tx_commit?tx=0x%s", cometRPC, txHex)

	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, rbacCommitTimeout())
	defer cancel()

	req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, url, nil) // #nosec G107 -- internal CometBFT RPC
	if reqErr != nil {
		return "", 0, "", fmt.Errorf("create broadcast request: %w", reqErr)
	}
	resp, doErr := http.DefaultClient.Do(req)
	if doErr != nil {
		return "", 0, "", fmt.Errorf("broadcast tx commit: %w", doErr)
	}
	defer resp.Body.Close()

	var result cometCommitResult
	if decErr := json.NewDecoder(resp.Body).Decode(&result); decErr != nil {
		return "", 0, "", fmt.Errorf("decode broadcast commit response: %w", decErr)
	}
	if result.Error != nil {
		if result.Error.Data != "" {
			return "", 0, "", fmt.Errorf("broadcast error: %s: %s", result.Error.Message, result.Error.Data)
		}
		return "", 0, "", fmt.Errorf("broadcast error: %s", result.Error.Message)
	}
	if result.Result.CheckTx.Code != 0 {
		return "", 0, "", fmt.Errorf("tx rejected in CheckTx (code %d): %s", result.Result.CheckTx.Code, result.Result.CheckTx.Log)
	}
	if result.Result.TxResult.Code != 0 {
		return "", 0, "", fmt.Errorf("tx rejected in FinalizeBlock (code %d): %s", result.Result.TxResult.Code, result.Result.TxResult.Log)
	}
	return result.Result.Hash, result.Result.Height, result.Result.TxResult.Log, nil
}

// signAndBroadcastCommit stamps the nonce, adds the legacy same-key proof for
// non-governance RBAC transactions, signs the envelope, encodes it, and waits
// for commit. Governance callers either supply a modern request-bound operator
// proof before calling this helper or intentionally use the proofless direct
// compatibility lane.
func (h *DashboardHandler) signAndBroadcastCommit(ptx *tx.ParsedTx, key ed25519.PrivateKey) (hash string, height int64, txLog string, err error) {
	return h.signAndBroadcastCommitContext(context.Background(), ptx, key)
}

func (h *DashboardHandler) signAndBroadcastCommitContext(ctx context.Context, ptx *tx.ParsedTx, key ed25519.PrivateKey) (hash string, height int64, txLog string, err error) {
	ptx.Nonce = tx.MonotonicNonce(key)
	if ptx.Timestamp.IsZero() {
		ptx.Timestamp = time.Now()
	}
	// Direct governance is authorized by the outer operator/validator signature
	// and deliberately carries no HTTP-agent proof. App-v20+ treats any proof
	// material on governance as a modern request-bound proof (8-byte request
	// nonce + canonical request body). The generic legacy dashboard proof lacks
	// those fields and is therefore correctly rejected. Keep the legacy same-key
	// proof for non-governance RBAC transactions, whose consensus path still
	// accepts it.
	switch ptx.Type {
	case tx.TxTypeGovPropose, tx.TxTypeGovVote, tx.TxTypeGovCancel:
	default:
		embedDashboardAgentProof(ptx, key)
	}
	if signErr := tx.SignTx(ptx, key); signErr != nil {
		return "", 0, "", fmt.Errorf("sign tx: %w", signErr)
	}
	encoded, encErr := tx.EncodeTx(ptx)
	if encErr != nil {
		return "", 0, "", fmt.Errorf("encode tx: %w", encErr)
	}
	return broadcastTxCommitWebContext(ctx, h.CometBFTRPC, encoded)
}

// isIndeterminateCommitError reports whether a commit-confirmed request could
// have reached consensus but did not yield a trustworthy response to this
// process. CheckTx/FinalizeBlock failures are definitive rejections and must
// remain errors; transport and RPC response faults are not proof of no change.
func isIndeterminateCommitError(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.HasPrefix(message, "broadcast tx commit:") ||
		strings.HasPrefix(message, "decode broadcast commit response:") ||
		strings.HasPrefix(message, "broadcast error:")
}

// agentIDForKey returns the on-chain agent id (hex(pubkey)) for an Ed25519 key,
// matching auth.PublicKeyToAgentID. Empty for a nil/invalid key.
func agentIDForKey(key ed25519.PrivateKey) string {
	if len(key) != ed25519.PrivateKeySize {
		return ""
	}
	pub, ok := key.Public().(ed25519.PublicKey)
	if !ok {
		return ""
	}
	return hex.EncodeToString(pub)
}

// rbacPurgeRe extracts the purged-grant count from processDomainReassign's
// success log ("... purged N grants ...").
var rbacPurgeRe = regexp.MustCompile(`purged\s+(\d+)\s+grants`)

// parsePurgedGrantsWeb pulls the purged-grant count out of a DomainReassign
// FinalizeBlock log, or 0 if absent.
func parsePurgedGrantsWeb(log string) int {
	m := rbacPurgeRe.FindStringSubmatch(log)
	if len(m) < 2 {
		return 0
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return 0
	}
	return n
}
