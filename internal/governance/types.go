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
	// OpDomainReassign (v8.0) is the governance op authorizing a
	// TxTypeDomainReassign execution. Requires a 3/4 supermajority (see
	// ThresholdFor) — stricter than the default 2/3 used for validator-set
	// changes, because ownership reassignment is a recovery primitive.
	OpDomainReassign ProposalOp = 4
	// OpUpgrade (app-v8) authorizes a chain-wide app-version upgrade. Routed
	// through the DEFAULT 2/3 quorum (ThresholdFor). The proposal Payload
	// carries the JSON-encoded upgrade plan (name, target version, binary
	// digest, delay); on quorum the ABCI layer persists the UpgradePlanRecord,
	// which then activates at its chain-computed ActivationHeight. This is what
	// turns UpgradePropose from a single-signer self-activating op (pre-app-v8)
	// into a supermajority-gated one (post-app-v8).
	OpUpgrade ProposalOp = 5
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
	// Payload (v8.0) carries the operation-specific body for non-validator
	// ops. For OpDomainReassign it's the JSON-encoded DomainReassign body.
	// Empty/omitted for legacy ops (1/2/3). Stored verbatim by Propose so
	// the executing DomainReassign tx can verify body-vs-proposal parity.
	Payload []byte `json:"payload,omitempty"`
}
