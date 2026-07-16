package governance

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// DelegationDomainForChainID derives the governance authorization domain that
// validators approve as part of the app-v20 upgrade proposal. The ABCI app
// cannot reliably read CometBFT's chain_id in every deployment mode, so the
// upgrade quorum commits this derived value explicitly before the fork turns
// it into a consensus-enforced request field.
func DelegationDomainForChainID(chainID string) (string, error) {
	if chainID == "" {
		return "", fmt.Errorf("chain_id must be non-empty")
	}
	// CometBFT's consensus MaxChainIDLen is 50. Keeping the same bound prevents
	// alternate oversized domains that no real SAGE chain can name.
	if len(chainID) > 50 {
		return "", fmt.Errorf("chain_id exceeds 50 bytes")
	}
	digest := sha256.Sum256(append([]byte("sage/governance-delegation-domain/v20\x00"), []byte(chainID)...))
	return hex.EncodeToString(digest[:]), nil
}
