package abci

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"sort"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	cmted25519 "github.com/cometbft/cometbft/crypto/ed25519"

	"github.com/l33tdawg/sage/internal/auth"
)

// validateAppV20ActivationValidatorCommit binds the app-v20 activation to the
// exact validator set CometBFT used for the preceding decided commit. The
// pending app-v20 plan freezes validator reconfiguration, so at activation the
// application roster and DecidedLastCommit must describe the same addresses and
// powers. This closes the legacy power-only persistence ambiguity: the Comet
// address is derived from the canonical public key encoded in each validator ID,
// not trusted from a process-local hydrated key.
//
// CommitInfo includes every preceding validator, including absent signers; the
// BlockIdFlag therefore does not participate in set equality.
func (app *SageApp) validateAppV20ActivationValidatorCommit(commit abcitypes.CommitInfo) error {
	if len(commit.Votes) == 0 {
		return fmt.Errorf("decided last commit is empty")
	}

	expected := make(map[string]int64)
	expectedAddresses := make([]string, 0, len(app.validators.GetAll()))
	for _, current := range app.validators.GetAll() {
		canonicalPubKey, err := auth.AgentIDToPublicKey(current.ID)
		if err != nil {
			return fmt.Errorf("validator %q has non-canonical id: %w", current.ID, err)
		}
		if len(current.PublicKey) != ed25519.PublicKeySize || !bytes.Equal(current.PublicKey, canonicalPubKey) {
			return fmt.Errorf("validator %q public key does not match its canonical id", current.ID)
		}
		address := hex.EncodeToString(cmted25519.PubKey(canonicalPubKey).Address())
		if _, duplicate := expected[address]; duplicate {
			return fmt.Errorf("application validator set derives duplicate Comet address %s", address)
		}
		expected[address] = current.Power
		expectedAddresses = append(expectedAddresses, address)
	}
	sort.Strings(expectedAddresses)

	seen := make(map[string]struct{}, len(commit.Votes))
	for index, vote := range commit.Votes {
		address := hex.EncodeToString(vote.Validator.Address)
		if _, duplicate := seen[address]; duplicate {
			return fmt.Errorf("decided last commit contains duplicate validator address %s", address)
		}
		seen[address] = struct{}{}

		expectedPower, ok := expected[address]
		if !ok {
			return fmt.Errorf("decided last commit vote %d has unexpected validator address %s", index, address)
		}
		if vote.Validator.Power != expectedPower {
			return fmt.Errorf(
				"decided last commit validator %s has power %d, application expects %d",
				address,
				vote.Validator.Power,
				expectedPower,
			)
		}
	}

	for _, address := range expectedAddresses {
		if _, ok := seen[address]; !ok {
			return fmt.Errorf("decided last commit is missing application validator address %s", address)
		}
	}
	return nil
}
