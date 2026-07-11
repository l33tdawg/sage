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
	txHex := hex.EncodeToString(txBytes)
	url := fmt.Sprintf("%s/broadcast_tx_commit?tx=0x%s", cometRPC, txHex)

	ctx, cancel := context.WithTimeout(context.Background(), rbacCommitTimeout())
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

// signAndBroadcastCommit stamps the nonce for the signing key, embeds that
// key's agent proof, signs the envelope with the same key, encodes, and
// broadcasts commit-confirmed. The signing key is BOTH the envelope signer and
// the embedded agent proof, so ABCI derives one consistent sender identity from
// it (that is what the admin gate on GovPropose/DomainReassign and the owner
// gate on AccessGrant/AccessRevoke each check).
func (h *DashboardHandler) signAndBroadcastCommit(ptx *tx.ParsedTx, key ed25519.PrivateKey) (hash string, height int64, txLog string, err error) {
	ptx.Nonce = tx.MonotonicNonce(key)
	if ptx.Timestamp.IsZero() {
		ptx.Timestamp = time.Now()
	}
	embedDashboardAgentProof(ptx, key)
	if signErr := tx.SignTx(ptx, key); signErr != nil {
		return "", 0, "", fmt.Errorf("sign tx: %w", signErr)
	}
	encoded, encErr := tx.EncodeTx(ptx)
	if encErr != nil {
		return "", 0, "", fmt.Errorf("encode tx: %w", encErr)
	}
	return broadcastTxCommitWeb(h.CometBFTRPC, encoded)
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
