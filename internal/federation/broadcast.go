package federation

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"
)

// Local CometBFT broadcast for node-originated federation txs (CoCommitAttest).
// Mirrors api/rest's broadcastTxCommitWithHeight — duplicated here because
// internal/federation must not import api/rest (rest imports federation).

const defaultBroadcastTimeout = 60 * time.Second

// JoinConfirmationPeerTimeout covers the host's one consensus commit plus
// response headroom. JoinConfirmationOperationTimeout covers the guest and host
// commits sequentially for the browser-facing request. Both derive from the
// operator's broadcast override so raising it cannot silently recreate a
// shorter ceremony deadline on the same node.
func JoinConfirmationPeerTimeout() time.Duration {
	return broadcastTimeout() + 5*time.Second
}

func JoinConfirmationOperationTimeout() time.Duration {
	return 2*broadcastTimeout() + 10*time.Second
}

func broadcastTimeout() time.Duration {
	if v := os.Getenv("SAGE_TX_COMMIT_TIMEOUT_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return defaultBroadcastTimeout
}

// broadcastTxCommit submits tx bytes to the local CometBFT RPC and waits for
// block finalization, returning (txHash, height). CheckTx and FinalizeBlock
// rejections surface as errors.
func (m *Manager) broadcastTxCommit(txBytes []byte) (string, int64, error) {
	url := fmt.Sprintf("%s/broadcast_tx_commit?tx=0x%s", m.cometRPC, hex.EncodeToString(txBytes))

	ctx, cancel := context.WithTimeout(context.Background(), broadcastTimeout())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil) // #nosec G107 -- internal CometBFT RPC
	if err != nil {
		return "", 0, fmt.Errorf("create broadcast request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("broadcast tx commit: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Result struct {
			CheckTx struct {
				Code uint32 `json:"code"`
				Log  string `json:"log"`
			} `json:"check_tx"`
			TxResult struct {
				Code uint32 `json:"code"`
				Log  string `json:"log"`
			} `json:"tx_result"`
			Hash   string `json:"hash"`
			Height string `json:"height"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
			Data    string `json:"data"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", 0, fmt.Errorf("decode broadcast response: %w", err)
	}
	if result.Error != nil {
		return "", 0, fmt.Errorf("broadcast rejected: %s %s", result.Error.Message, result.Error.Data)
	}
	if result.Result.CheckTx.Code != 0 {
		return "", 0, fmt.Errorf("tx rejected by CheckTx (code %d): %s", result.Result.CheckTx.Code, result.Result.CheckTx.Log)
	}
	if result.Result.TxResult.Code != 0 {
		return "", 0, fmt.Errorf("tx rejected in FinalizeBlock (code %d): %s", result.Result.TxResult.Code, result.Result.TxResult.Log)
	}
	height, _ := strconv.ParseInt(result.Result.Height, 10, 64)
	return result.Result.Hash, height, nil
}
