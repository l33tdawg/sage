package governance

const (
	// DefaultExpiryBlocks is the default number of blocks before a proposal expires (~5 min at 3s/block).
	DefaultExpiryBlocks int64 = 100
	// MinExpiryBlocks is the minimum allowed expiry window.
	MinExpiryBlocks int64 = 10
	// MaxExpiryBlocks is the maximum allowed expiry window (~25 min at 3s/block).
	MaxExpiryBlocks int64 = 500
	// CooldownBlocks is the number of blocks a proposer must wait between proposals.
	CooldownBlocks int64 = 50
	// MinVotingBlocks is the minimum number of blocks before a proposal can be executed.
	MinVotingBlocks int64 = 10
	// DefaultPower is the default voting power assigned to new validators.
	DefaultPower int64 = 10
)

// ProposalOp represents the type of governance operation.
type ProposalOp uint8

const (
	OpAddValidator    ProposalOp = 1
	OpRemoveValidator ProposalOp = 2
	OpUpdatePower     ProposalOp = 3
)

// ProposalStatus represents the current state of a governance proposal.
type ProposalStatus string

const (
	StatusVoting    ProposalStatus = "voting"
	StatusExecuted  ProposalStatus = "executed"
	StatusRejected  ProposalStatus = "rejected"
	StatusExpired   ProposalStatus = "expired"
	StatusCancelled ProposalStatus = "cancelled"
)

// ProposalState holds the full state of a governance proposal.
type ProposalState struct {
	ProposalID    string         `json:"id"`
	Operation     ProposalOp     `json:"op"`
	TargetID      string         `json:"target_id"`
	TargetPubKey  []byte         `json:"target_pubkey"`
	TargetPower   int64          `json:"target_power"`
	ProposerID    string         `json:"proposer_id"`
	Status        ProposalStatus `json:"status"`
	CreatedHeight int64          `json:"created_height"`
	ExpiryHeight  int64          `json:"expiry_height"`
	Reason        string         `json:"reason"`
}
