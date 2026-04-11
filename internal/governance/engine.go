package governance

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
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

// ComputeProposalID returns a deterministic proposal identifier.
// Format: hex(SHA256(proposerID + ":" + height + ":" + op + ":" + targetID))[:32]
func ComputeProposalID(proposerID string, height int64, op ProposalOp, targetID string) string {
	raw := proposerID + ":" + strconv.FormatInt(height, 10) + ":" + strconv.FormatUint(uint64(op), 10) + ":" + targetID
	h := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(h[:])[:32]
}

// Propose creates a new governance proposal.
func (e *Engine) Propose(proposerID string, op ProposalOp, targetID string, targetPubKey []byte, targetPower int64, expiryBlocks int64, reason string, height int64) (string, error) {
	// Check no active proposal exists.
	active, err := e.store.GetState("gov:active")
	if err != nil {
		return "", fmt.Errorf("check active proposal: %w", err)
	}
	if active != nil {
		return "", fmt.Errorf("an active proposal already exists: %s", string(active))
	}

	// Check proposer cooldown.
	cooldownKey := "gov:cooldown:" + proposerID
	cooldownData, err := e.store.GetState(cooldownKey)
	if err != nil {
		return "", fmt.Errorf("check cooldown: %w", err)
	}
	if len(cooldownData) == 8 {
		cooldownHeight := int64(binary.BigEndian.Uint64(cooldownData))
		if height < cooldownHeight+CooldownBlocks {
			return "", fmt.Errorf("proposer %s is in cooldown until block %d (current: %d)", proposerID, cooldownHeight+CooldownBlocks, height)
		}
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
	proposal, err := e.loadProposal(proposalID)
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

	proposal, err := e.loadProposal(proposalID)
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
	// Load active proposal.
	active, err := e.store.GetState("gov:active")
	if err != nil {
		return nil, fmt.Errorf("check active proposal: %w", err)
	}
	if active == nil {
		return nil, nil
	}

	proposalID := string(active)
	proposal, err := e.loadProposal(proposalID)
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

	// Check quorum.
	passed, rejected, _, _, _ := CheckGovQuorum(votes, powers)

	if passed {
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
	return e.loadProposal(string(active))
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

// loadProposal reads and unmarshals a proposal from the store.
func (e *Engine) loadProposal(proposalID string) (*ProposalState, error) {
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
