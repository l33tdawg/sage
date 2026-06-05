package voter

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"

	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/tx"
)

// broadcastTimeout bounds a single broadcast so a slow/hung CometBFT RPC can't
// wedge the voter's tick loop.
const broadcastTimeout = 10 * time.Second

// broadcastVoteTx sends an encoded vote transaction to CometBFT via
// broadcast_tx_sync. Fire-and-forget: a dropped broadcast self-heals because the
// voter re-votes (memory) / re-checks UpgradeProposalHasVote (upgrade) until the
// vote is confirmed on-chain. The request is derived from the voter ctx (so a
// shutdown cancels in-flight broadcasts) and bounded by broadcastTimeout.
func broadcastVoteTx(ctx context.Context, cometRPC string, encoded []byte, logger zerolog.Logger) {
	txHex := hex.EncodeToString(encoded)
	url := fmt.Sprintf("%s/broadcast_tx_sync?tx=0x%s", cometRPC, txHex)
	reqCtx, cancel := context.WithTimeout(ctx, broadcastTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil) //nolint:gosec
	if err != nil {
		logger.Debug().Err(err).Msg("failed to create broadcast request")
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logger.Debug().Err(err).Msg("failed to broadcast vote tx")
		return
	}
	resp.Body.Close()
}

// voteDecisionFromString maps a decision string to the on-chain enum.
func voteDecisionFromString(s string) tx.VoteDecision {
	switch s {
	case "accept":
		return tx.VoteDecisionAccept
	case "reject":
		return tx.VoteDecisionReject
	case "abstain":
		return tx.VoteDecisionAbstain
	default:
		return tx.VoteDecisionAccept
	}
}
