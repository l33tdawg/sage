package abci

import (
	"fmt"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/tx"
)

// authorizeDelegatedScopeMutation applies only to app-v20 scope votes and
// cancellations carrying a distinct embedded signer. Proposal creation is
// globally admin-authorized, but voting belongs to each active validator: its
// node-local operator authorizes the exact, validator-bound request and the
// outer validator signature supplies the voting power/ownership principal.
// Requiring every validator operator to share the genesis-admin key would both
// strand honest votes and destroy key separation.
func (app *SageApp) authorizeDelegatedScopeMutation(parsedTx *tx.ParsedTx, proposalID string) error {
	if !isDelegatedGovernanceProof(parsedTx) {
		return nil
	}
	proposal, err := app.govEngine.LoadProposal(proposalID)
	if err != nil {
		return fmt.Errorf("load delegated governance proposal: %w", err)
	}
	if proposal.Operation != governance.OpScopeAction {
		return nil
	}

	validatorID := auth.PublicKeyToAgentID(parsedTx.PublicKey)
	validatorInfo, exists := app.validators.GetValidator(validatorID)
	if !exists || validatorInfo.Power <= 0 {
		return fmt.Errorf("outer governance actor %s is not an active validator", validatorID)
	}
	return nil
}
