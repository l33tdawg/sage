package governance

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	cmttypes "github.com/cometbft/cometbft/types"
)

// GovStore is the interface the governance engine needs from the storage layer.
// This allows testing with mocks.
type GovStore interface {
	GetState(key string) ([]byte, error)
	SetState(key string, value []byte) error
	DeleteState(key string) error
	// PrefixKeys returns all keys with the given prefix, sorted lexicographically.
	PrefixKeys(prefix string) ([]string, error)
}

// ValidatorProvider gives the engine access to the current validator set.
type ValidatorProvider interface {
	GetValidator(id string) (power int64, exists bool)
	GetAll() map[string]int64 // validatorID -> power
	Size() int
}

// Engine is the governance engine that manages validator proposals and voting.
type Engine struct {
	store      GovStore
	validators ValidatorProvider
}

// NewEngine creates a new governance engine.
func NewEngine(store GovStore, validators ValidatorProvider) *Engine {
	return &Engine{store: store, validators: validators}
}

// ValidateValidatorOperationV20 applies the strict validator-set invariants
// introduced by app-v20 without changing the replay behavior of Engine.Propose
// for historical blocks. The ABCI layer additionally validates that targetID is
// the canonical Ed25519 identity encoded by targetPubKey; this engine-level
// check owns the live set membership and overflow-safe power constraints.
func (e *Engine) ValidateValidatorOperationV20(op ProposalOp, targetID string, targetPower int64) error {
	allVals := e.validators.GetAll()
	totalPower, err := checkedValidatorPowerTotal(allVals)
	if err != nil {
		return err
	}

	currentPower, exists := e.validators.GetValidator(targetID)
	switch op {
	case OpAddValidator:
		if exists {
			return fmt.Errorf("target validator %s already exists", targetID)
		}
		if targetPower <= 0 {
			return fmt.Errorf("target power must be positive for add_validator, got %d", targetPower)
		}
		// Division is equivalent to targetPower*3 > totalPower for positive
		// values, but cannot overflow near math.MaxInt64.
		if totalPower <= 0 || targetPower > totalPower/3 {
			return fmt.Errorf("target power %d exceeds 1/3 of total power %d", targetPower, totalPower)
		}
		if totalPower > cmttypes.MaxTotalVotingPower ||
			targetPower > cmttypes.MaxTotalVotingPower-totalPower {
			return fmt.Errorf("resulting validator power exceeds CometBFT maximum %d", cmttypes.MaxTotalVotingPower)
		}

	case OpRemoveValidator:
		if targetPower != 0 {
			return fmt.Errorf("target power must be 0 for remove_validator, got %d", targetPower)
		}
		if !exists {
			return fmt.Errorf("target validator %s does not exist", targetID)
		}
		if e.validators.Size() <= 2 {
			return fmt.Errorf("cannot remove validator: minimum 2 validators required, currently %d", e.validators.Size())
		}

	case OpUpdatePower:
		if !exists {
			return fmt.Errorf("target validator %s does not exist", targetID)
		}
		if currentPower < 0 {
			return fmt.Errorf("target validator %s has invalid negative power", targetID)
		}
		if targetPower <= 0 {
			return fmt.Errorf("target power must be positive for update_power, got %d; use remove_validator to remove a validator", targetPower)
		}
		diff := targetPower - currentPower
		if diff < 0 {
			diff = -diff
		}
		maxChange := totalPower / 3
		if maxChange == 0 && totalPower > 0 {
			maxChange = 1
		}
		if diff > maxChange {
			return fmt.Errorf("power change %d exceeds max allowed %d (1/3 of total %d)", diff, maxChange, totalPower)
		}
		if currentPower > totalPower {
			return fmt.Errorf("target validator power exceeds validator total power")
		}
		remainingPower := totalPower - currentPower
		if remainingPower > cmttypes.MaxTotalVotingPower ||
			targetPower > cmttypes.MaxTotalVotingPower-remainingPower {
			return fmt.Errorf("resulting validator power exceeds CometBFT maximum %d", cmttypes.MaxTotalVotingPower)
		}

	default:
		return fmt.Errorf("operation %d is not a validator-set operation", op)
	}
	return nil
}

func checkedValidatorPowerTotal(validators map[string]int64) (int64, error) {
	var total int64
	for _, power := range validators {
		if power < 0 {
			return 0, fmt.Errorf("validator set contains negative power")
		}
		if total > math.MaxInt64-power {
			// Keep the error independent of map iteration order: ABCI logs are
			// consensus-visible through TxResults.
			return 0, fmt.Errorf("validator total power overflows int64")
		}
		total += power
	}
	return total, nil
}

// ComputeProposalID returns a deterministic proposal identifier.
// Format: hex(SHA256(proposerID + ":" + height + ":" + op + ":" + targetID))[:32]
func ComputeProposalID(proposerID string, height int64, op ProposalOp, targetID string) string {
	raw := proposerID + ":" + strconv.FormatInt(height, 10) + ":" + strconv.FormatUint(uint64(op), 10) + ":" + targetID
	h := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(h[:])[:32]
}

// CheckProposerEligibility is factored out so deterministic proposal selection
// can ask the exact same active-slot/cooldown question without mutating state.
func (e *Engine) CheckProposerEligibility(proposerID string, height int64) error {
	// Check no active proposal exists.
	active, err := e.store.GetState("gov:active")
	if err != nil {
		return fmt.Errorf("check active proposal: %w", err)
	}
	if active != nil {
		return fmt.Errorf("an active proposal already exists: %s", string(active))
	}

	// Check proposer cooldown.
	cooldownKey := "gov:cooldown:" + proposerID
	cooldownData, err := e.store.GetState(cooldownKey)
	if err != nil {
		return fmt.Errorf("check cooldown: %w", err)
	}
	if len(cooldownData) == 8 {
		cooldownHeight := int64(binary.BigEndian.Uint64(cooldownData))
		cooldownBlocks := effectiveCooldownBlocks()
		if height < cooldownHeight+cooldownBlocks {
			return fmt.Errorf("proposer %s is in cooldown until block %d (current: %d)", proposerID, cooldownHeight+cooldownBlocks, height)
		}
	}
	return nil
}

// Propose creates a new governance proposal.
//
// Payload is the operation-specific JSON body (added v8.0). It is stored
// verbatim on the proposal record so the executing tx (e.g. DomainReassign)
// can verify body-vs-proposal parity. Pass nil for legacy validator-set ops.
func (e *Engine) Propose(proposerID string, op ProposalOp, targetID string, targetPubKey []byte, targetPower int64, expiryBlocks int64, reason string, height int64, payload []byte) (string, error) {
	if err := e.CheckProposerEligibility(proposerID, height); err != nil {
		return "", err
	}

	// Validate operation-specific constraints.
	allVals := e.validators.GetAll()
	var totalPower int64
	for _, p := range allVals {
		totalPower += p
	}

	switch op {
	case OpAddValidator:
		// New validator's power must not exceed 1/3 of total power.
		if totalPower > 0 && targetPower*3 > totalPower {
			return "", fmt.Errorf("target power %d exceeds 1/3 of total power %d", targetPower, totalPower)
		}
	case OpRemoveValidator:
		// Must leave at least 2 validators after removal.
		if e.validators.Size() <= 2 {
			return "", fmt.Errorf("cannot remove validator: minimum 2 validators required, currently %d", e.validators.Size())
		}
	case OpUpdatePower:
		// Target must be an existing validator.
		if _, exists := e.validators.GetValidator(targetID); !exists {
			return "", fmt.Errorf("target validator %s does not exist", targetID)
		}
	case OpDomainReassign:
		// Payload is required — the domain/owner identity is carried in the
		// JSON body, not in the scalar TargetID/TargetPower fields.
		if len(payload) == 0 {
			return "", fmt.Errorf("op_domain_reassign requires a non-empty payload")
		}
		// NOTE: OpUpgrade (app-v8) is DELIBERATELY not validated here. This engine
		// is fork-unaware, and op==5 was an accepted no-op on pre-app-v8 chains
		// (it fell through this switch and created an inert proposal). Adding a
		// payload-required reject here would change that pre-fork result and
		// diverge historical replay. The payload requirement is enforced where it
		// is fork-aware: processUpgradePropose's post-fork branch always marshals a
		// non-empty payload, and the generic GovPropose path rejects OpUpgrade
		// post-fork (Code 72) so it never reaches here with an empty body.
	}

	// Validate expiry range.
	if expiryBlocks == 0 {
		expiryBlocks = DefaultExpiryBlocks
	}
	if expiryBlocks < MinExpiryBlocks {
		return "", fmt.Errorf("expiry blocks %d below minimum %d", expiryBlocks, MinExpiryBlocks)
	}
	if expiryBlocks > MaxExpiryBlocks {
		return "", fmt.Errorf("expiry blocks %d exceeds maximum %d", expiryBlocks, MaxExpiryBlocks)
	}

	// Compute deterministic proposal ID.
	proposalID := ComputeProposalID(proposerID, height, op, targetID)

	// Build and store proposal state.
	proposal := &ProposalState{
		ProposalID:    proposalID,
		Operation:     op,
		TargetID:      targetID,
		TargetPubKey:  targetPubKey,
		TargetPower:   targetPower,
		ProposerID:    proposerID,
		Status:        StatusVoting,
		CreatedHeight: height,
		ExpiryHeight:  height + expiryBlocks,
		Reason:        reason,
		Payload:       payload,
	}

	data, err := json.Marshal(proposal)
	if err != nil {
		return "", fmt.Errorf("marshal proposal: %w", err)
	}
	if err := e.store.SetState("gov:proposal:"+proposalID, data); err != nil {
		return "", fmt.Errorf("store proposal: %w", err)
	}

	// Set active proposal marker.
	if err := e.store.SetState("gov:active", []byte(proposalID)); err != nil {
		return "", fmt.Errorf("set active: %w", err)
	}

	// Auto-vote accept from proposer.
	voteKey := "gov:vote:" + proposalID + ":" + proposerID
	if err := e.store.SetState(voteKey, []byte("accept")); err != nil {
		return "", fmt.Errorf("auto-vote: %w", err)
	}

	// Set proposer cooldown.
	cooldownKey := "gov:cooldown:" + proposerID
	cooldownVal := make([]byte, 8)
	binary.BigEndian.PutUint64(cooldownVal, uint64(height))
	if err := e.store.SetState(cooldownKey, cooldownVal); err != nil {
		return "", fmt.Errorf("set cooldown: %w", err)
	}

	return proposalID, nil
}

// Vote records a validator's vote on the active proposal.
func (e *Engine) Vote(proposalID string, voterID string, decision string, height int64) error {
	// Validate decision.
	if decision != "accept" && decision != "reject" && decision != "abstain" {
		return fmt.Errorf("invalid decision %q: must be accept, reject, or abstain", decision)
	}

	// Load active proposal.
	active, err := e.store.GetState("gov:active")
	if err != nil {
		return fmt.Errorf("check active proposal: %w", err)
	}
	if active == nil {
		return fmt.Errorf("no active proposal")
	}
	if string(active) != proposalID {
		return fmt.Errorf("proposal %s is not the active proposal (active: %s)", proposalID, string(active))
	}

	// Load proposal to check expiry.
	proposal, err := e.LoadProposal(proposalID)
	if err != nil {
		return err
	}
	if height > proposal.ExpiryHeight {
		return fmt.Errorf("proposal %s has expired at block %d (current: %d)", proposalID, proposal.ExpiryHeight, height)
	}

	// Check for duplicate vote.
	voteKey := "gov:vote:" + proposalID + ":" + voterID
	existing, err := e.store.GetState(voteKey)
	if err != nil {
		return fmt.Errorf("check existing vote: %w", err)
	}
	if existing != nil {
		return fmt.Errorf("validator %s has already voted on proposal %s", voterID, proposalID)
	}

	// Verify voter is a validator.
	if _, exists := e.validators.GetValidator(voterID); !exists {
		return fmt.Errorf("voter %s is not a validator", voterID)
	}

	// Store vote.
	if err := e.store.SetState(voteKey, []byte(decision)); err != nil {
		return fmt.Errorf("store vote: %w", err)
	}

	return nil
}

// Cancel cancels the active proposal. Only the proposer can cancel.
func (e *Engine) Cancel(proposalID string, cancellerID string, height int64) error {
	// Load active proposal.
	active, err := e.store.GetState("gov:active")
	if err != nil {
		return fmt.Errorf("check active proposal: %w", err)
	}
	if active == nil {
		return fmt.Errorf("no active proposal")
	}
	if string(active) != proposalID {
		return fmt.Errorf("proposal %s is not the active proposal", proposalID)
	}

	proposal, err := e.LoadProposal(proposalID)
	if err != nil {
		return err
	}

	// Only proposer can cancel.
	if proposal.ProposerID != cancellerID {
		return fmt.Errorf("only the proposer (%s) can cancel, got %s", proposal.ProposerID, cancellerID)
	}

	// Mark cancelled.
	proposal.Status = StatusCancelled
	if err := e.saveProposal(proposal); err != nil {
		return err
	}

	// Clear active.
	if err := e.store.DeleteState("gov:active"); err != nil {
		return fmt.Errorf("clear active: %w", err)
	}

	// Set cooldown for proposer.
	cooldownVal := make([]byte, 8)
	binary.BigEndian.PutUint64(cooldownVal, uint64(height))
	if err := e.store.SetState("gov:cooldown:"+cancellerID, cooldownVal); err != nil {
		return fmt.Errorf("set cooldown: %w", err)
	}

	return nil
}

// ProcessBlock checks the active proposal at the given block height.
// If quorum is reached (and min voting period has passed), the proposal is executed.
// If expired, the proposal is marked expired.
// Returns the executed proposal if one was executed, nil otherwise.
func (e *Engine) ProcessBlock(height int64) (*ProposalState, error) {
	return e.processBlock(height, nil)
}

// ProcessBlockValidated is the app-v20 execution path. preExecute runs only
// after quorum passes and before the proposal is marked executed or the active
// marker is cleared. A validation failure therefore leaves the proposal in the
// recoverable voting state instead of committing a false-success record.
func (e *Engine) ProcessBlockValidated(height int64, preExecute func(*ProposalState) error) (*ProposalState, error) {
	return e.processBlock(height, preExecute)
}

func (e *Engine) processBlock(height int64, preExecute func(*ProposalState) error) (*ProposalState, error) {
	// Load active proposal.
	active, err := e.store.GetState("gov:active")
	if err != nil {
		return nil, fmt.Errorf("check active proposal: %w", err)
	}
	if active == nil {
		return nil, nil
	}

	proposalID := string(active)
	proposal, err := e.LoadProposal(proposalID)
	if err != nil {
		return nil, err
	}

	// Check expiry.
	if height > proposal.ExpiryHeight {
		proposal.Status = StatusExpired
		if saveErr := e.saveProposal(proposal); saveErr != nil {
			return nil, saveErr
		}
		if clearErr := e.store.DeleteState("gov:active"); clearErr != nil {
			return nil, fmt.Errorf("clear active: %w", clearErr)
		}
		return nil, nil
	}

	// Enforce MinVotingBlocks (skip for single validator).
	if height < proposal.CreatedHeight+MinVotingBlocks && e.validators.Size() > 1 {
		return nil, nil
	}

	// Gather all votes for this proposal.
	votePrefix := "gov:vote:" + proposalID + ":"
	voteKeys, err := e.store.PrefixKeys(votePrefix)
	if err != nil {
		return nil, fmt.Errorf("scan votes: %w", err)
	}

	votes := make(map[string]string, len(voteKeys))
	for _, key := range voteKeys {
		voterID := strings.TrimPrefix(key, votePrefix)
		voteData, getErr := e.store.GetState(key)
		if getErr != nil {
			return nil, fmt.Errorf("load vote %s: %w", key, getErr)
		}
		if voteData != nil {
			votes[voterID] = string(voteData)
		}
	}

	// Get all validator powers.
	powers := e.validators.GetAll()

	// Check quorum. Op-aware so OpDomainReassign uses 3/4 supermajority
	// while validator-set ops keep the historical 2/3.
	passed, rejected, _, _, _ := CheckGovQuorumOp(votes, powers, proposal.Operation)

	if passed {
		if preExecute != nil {
			if err := preExecute(proposal); err != nil {
				return nil, fmt.Errorf("proposal %s execution validation failed: %w", proposal.ProposalID, err)
			}
		}
		proposal.Status = StatusExecuted
		if err := e.saveProposal(proposal); err != nil {
			return nil, err
		}
		if err := e.store.DeleteState("gov:active"); err != nil {
			return nil, fmt.Errorf("clear active: %w", err)
		}
		return proposal, nil
	}

	if rejected {
		proposal.Status = StatusRejected
		if err := e.saveProposal(proposal); err != nil {
			return nil, err
		}
		if err := e.store.DeleteState("gov:active"); err != nil {
			return nil, fmt.Errorf("clear active: %w", err)
		}
		return nil, nil
	}

	// Still voting.
	return nil, nil
}

// GetActiveProposal loads and returns the currently active proposal, or nil if none.
func (e *Engine) GetActiveProposal() (*ProposalState, error) {
	active, err := e.store.GetState("gov:active")
	if err != nil {
		return nil, fmt.Errorf("check active proposal: %w", err)
	}
	if active == nil {
		return nil, nil
	}
	return e.LoadProposal(string(active))
}

// GetProposalVotes returns all votes for a given proposal.
func (e *Engine) GetProposalVotes(proposalID string) (map[string]string, error) {
	votePrefix := "gov:vote:" + proposalID + ":"
	voteKeys, err := e.store.PrefixKeys(votePrefix)
	if err != nil {
		return nil, fmt.Errorf("scan votes: %w", err)
	}

	votes := make(map[string]string, len(voteKeys))
	for _, key := range voteKeys {
		voterID := strings.TrimPrefix(key, votePrefix)
		voteData, getErr := e.store.GetState(key)
		if getErr != nil {
			return nil, fmt.Errorf("load vote %s: %w", key, getErr)
		}
		if voteData != nil {
			votes[voterID] = string(voteData)
		}
	}

	return votes, nil
}

// LoadProposal reads and unmarshals a proposal from the store. Exported so
// the ABCI layer can verify a TxTypeDomainReassign references an accepted
// proposal of the correct shape (operation, payload, status, freshness).
func (e *Engine) LoadProposal(proposalID string) (*ProposalState, error) {
	data, err := e.store.GetState("gov:proposal:" + proposalID)
	if err != nil {
		return nil, fmt.Errorf("load proposal %s: %w", proposalID, err)
	}
	if data == nil {
		return nil, fmt.Errorf("proposal %s not found", proposalID)
	}

	var proposal ProposalState
	if err := json.Unmarshal(data, &proposal); err != nil {
		return nil, fmt.Errorf("unmarshal proposal %s: %w", proposalID, err)
	}
	return &proposal, nil
}

// saveProposal marshals and stores a proposal.
func (e *Engine) saveProposal(proposal *ProposalState) error {
	data, err := json.Marshal(proposal)
	if err != nil {
		return fmt.Errorf("marshal proposal: %w", err)
	}
	return e.store.SetState("gov:proposal:"+proposal.ProposalID, data)
}
