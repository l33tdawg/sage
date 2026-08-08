package federation

import (
	"fmt"

	"github.com/l33tdawg/sage/internal/tx"
)

// buildSyncSubmitTx is retained only as a test helper for signer-identity and
// wire-shape assertions. Production callers must use broadcastSyncSubmit, which
// owns the nonce lease through CometBFT admission.
func (m *Manager) buildSyncSubmitTx(localID string, item *SyncItem) ([]byte, error) {
	signingKey, signingPub, err := m.localConsensusSigningKey()
	if err != nil {
		return nil, fmt.Errorf("resolve sync submit authority: %w", err)
	}
	return m.buildSyncSubmitTxWithSigner(
		localID, item, signingKey, signingPub, tx.MonotonicNonce(signingKey),
	)
}
