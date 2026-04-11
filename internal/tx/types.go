package tx

import "time"

// TxType identifies the type of transaction.
type TxType uint8

const (
	TxTypeMemorySubmit      TxType = 1
	TxTypeMemoryVote        TxType = 2
	TxTypeMemoryChallenge   TxType = 3
	TxTypeMemoryCorroborate TxType = 4
	TxTypeAccessRequest     TxType = 5
	TxTypeAccessGrant       TxType = 6
	TxTypeAccessRevoke      TxType = 7
	TxTypeAccessQuery       TxType = 8
	TxTypeDomainRegister    TxType = 9
	TxTypeOrgRegister       TxType = 10
	TxTypeOrgAddMember      TxType = 11
	TxTypeOrgRemoveMember   TxType = 12
	TxTypeOrgSetClearance   TxType = 13
	TxTypeFederationPropose TxType = 14
	TxTypeFederationApprove TxType = 15
	TxTypeFederationRevoke  TxType = 16
	TxTypeDeptRegister      TxType = 17
	TxTypeDeptAddMember     TxType = 18
	TxTypeDeptRemoveMember   TxType = 19
	TxTypeAgentRegister      TxType = 20
	TxTypeAgentUpdate        TxType = 21
	TxTypeAgentSetPermission TxType = 22
	TxTypeMemoryReassign     TxType = 23
	TxTypeGovPropose         TxType = 24
	TxTypeGovVote            TxType = 25
	TxTypeGovCancel          TxType = 26
)

// GovProposalOp identifies the governance operation being proposed.
type GovProposalOp uint8

const (
	GovOpAddValidator    GovProposalOp = 1
	GovOpRemoveValidator GovProposalOp = 2
	GovOpUpdatePower     GovProposalOp = 3
)

// VoteDecision represents a validator's vote on a proposed memory.
type VoteDecision uint8

const (
	VoteDecisionAccept  VoteDecision = 1
	VoteDecisionReject  VoteDecision = 2
	VoteDecisionAbstain VoteDecision = 3
)

// MemoryType classifies the nature of a memory object.
type MemoryType uint8

const (
	MemoryTypeFact        MemoryType = 1
	MemoryTypeObservation MemoryType = 2
	MemoryTypeInference   MemoryType = 3
	MemoryTypeTask        MemoryType = 4
)

// ClearanceLevel defines the security classification tier.
type ClearanceLevel uint8

const (
	ClearancePublic       ClearanceLevel = 0 // Readable by any federated org
	ClearanceInternal     ClearanceLevel = 1 // Own org only (default)
	ClearanceConfidential ClearanceLevel = 2 // Own org + explicit cross-org grants
	ClearanceSecret       ClearanceLevel = 3 // Own org, specific department, explicit grant
	ClearanceTopSecret    ClearanceLevel = 4 // Named agents only, dual-approval
)

// MemorySubmit proposes a new memory object for consensus validation.
type MemorySubmit struct {
	MemoryID        string
	ContentHash     []byte
	EmbeddingHash   []byte
	MemoryType      MemoryType
	DomainTag       string
	ConfidenceScore float64
	Content         string
	ParentHash      string
	Classification  ClearanceLevel // Defaults to ClearanceInternal (1)
	TaskStatus      string         // For task memories: planned, in_progress, done, dropped
}

// MemoryVote records a validator's vote on a proposed memory.
type MemoryVote struct {
	MemoryID  string
	Decision  VoteDecision
	Rationale string
}

// MemoryChallenge disputes an existing committed memory.
type MemoryChallenge struct {
	MemoryID string
	Reason   string
	Evidence string
}

// MemoryCorroborate corroborates an existing memory with supporting evidence.
type MemoryCorroborate struct {
	MemoryID string
	Evidence string
}

// AccessRequest requests access to a federated domain.
type AccessRequest struct {
	RequesterID    string
	TargetDomain   string
	Justification  string
	RequestedLevel uint8 // 1=read, 2=read+write
}

// AccessGrant grants an agent access to a domain.
type AccessGrant struct {
	GranterID string
	GranteeID string
	Domain    string
	Level     uint8  // 1=read, 2=read+write
	ExpiresAt int64  // Unix timestamp, 0=permanent
	RequestID string // Links to original AccessRequest (optional)
}

// AccessRevoke revokes an agent's access to a domain.
type AccessRevoke struct {
	RevokerID string
	GranteeID string
	Domain    string
	Reason    string
}

// AccessQuery queries memories in a federated domain with semantic search.
type AccessQuery struct {
	AgentID   string
	Domain    string
	Embedding []float32 // 768-dim vector
	TopK      int32
}

// DomainRegister registers a new federated domain.
type DomainRegister struct {
	DomainName   string
	OwnerAgentID string
	Description  string
	ParentDomain string // "" for top-level
}

// OrgRegister registers a new organization on-chain.
type OrgRegister struct {
	OrgID       string // Deterministic: SHA256(admin_agent_pubkey + name)
	Name        string
	Description string
	AdminAgent  string // Agent who becomes admin
}

// OrgAddMember adds an agent to an organization with a clearance level.
type OrgAddMember struct {
	OrgID     string
	AgentID   string
	Clearance ClearanceLevel
	Role      string // "admin", "member", "observer"
}

// OrgRemoveMember removes an agent from an organization.
type OrgRemoveMember struct {
	OrgID   string
	AgentID string
	Reason  string
}

// OrgSetClearance changes an agent's clearance within their org.
type OrgSetClearance struct {
	OrgID     string
	AgentID   string
	Clearance ClearanceLevel
}

// FederationPropose proposes a bilateral federation agreement between two orgs.
type FederationPropose struct {
	ProposerOrgID    string         // Org proposing the agreement
	TargetOrgID      string         // Org being invited
	AllowedDomains   []string       // Which domains are covered ("*" = all)
	MaxClearance     ClearanceLevel // Ceiling clearance for cross-org access
	ExpiresAt        int64          // Unix timestamp, 0=permanent
	RequiresApproval bool           // Whether each query needs explicit approval
	AllowedDepts     []string       // Which departments can access ("*" = all, empty = all)
}

// FederationApprove approves a pending federation proposal.
type FederationApprove struct {
	FederationID  string // Links to the proposal
	ApproverOrgID string
}

// FederationRevoke revokes an active federation agreement.
type FederationRevoke struct {
	FederationID string
	RevokerOrgID string
	Reason       string
}

// DeptRegister registers a new department within an organization.
type DeptRegister struct {
	OrgID       string
	DeptID      string // Deterministic: SHA256(orgID + name)[:16] hex
	DeptName    string
	Description string
	ParentDept  string // "" for top-level department
}

// DeptAddMember adds an agent to a department with a clearance level.
type DeptAddMember struct {
	OrgID     string
	DeptID    string
	AgentID   string
	Clearance ClearanceLevel
	Role      string // "admin", "member", "observer"
}

// DeptRemoveMember removes an agent from a department.
type DeptRemoveMember struct {
	OrgID   string
	DeptID  string
	AgentID string
	Reason  string
}

// AgentRegister registers an agent on-chain with its identity and metadata.
type AgentRegister struct {
	AgentID    string
	Name       string
	Role       string // "admin", "member", "observer"
	BootBio    string
	Provider   string // "claude-code", "chatgpt", etc.
	P2PAddress string
}

// AgentUpdate updates an agent's own metadata on-chain.
type AgentUpdate struct {
	AgentID string
	Name    string
	BootBio string
}

// MemoryReassign reassigns all memories from one agent to another (admin only).
type MemoryReassign struct {
	SourceAgentID string // agent whose memories will be moved
	TargetAgentID string // agent receiving the memories
}

// GovPropose proposes a validator governance action (add, remove, update power).
type GovPropose struct {
	Operation    GovProposalOp
	TargetID     string // hex-encoded validator/agent ID
	TargetPubKey []byte // Ed25519 pubkey (32 bytes, required for add)
	TargetPower  int64  // power for add/update (0 for remove)
	ExpiryBlocks int64  // 0 = default
	Reason       string // human-readable justification
}

// GovVote records a validator's vote on a governance proposal.
type GovVote struct {
	ProposalID string
	Decision   VoteDecision // reuse existing accept/reject/abstain enum
}

// GovCancel cancels a pending governance proposal.
type GovCancel struct {
	ProposalID string
}

// AgentSetPermission sets permissions on an agent (admin only).
type AgentSetPermission struct {
	AgentID       string
	Clearance     uint8
	DomainAccess  string // JSON string of domain access rules
	VisibleAgents string // JSON array of agent IDs or "*" for all
	OrgID         string
	DeptID        string
}

// ParsedTx is the top-level transaction envelope.
type ParsedTx struct {
	Type               TxType
	MemorySubmit       *MemorySubmit
	MemoryVote         *MemoryVote
	MemoryChallenge    *MemoryChallenge
	MemoryCorroborate  *MemoryCorroborate
	AccessRequest      *AccessRequest
	AccessGrant        *AccessGrant
	AccessRevoke       *AccessRevoke
	AccessQuery        *AccessQuery
	DomainRegister     *DomainRegister
	OrgRegister        *OrgRegister
	OrgAddMember       *OrgAddMember
	OrgRemoveMember    *OrgRemoveMember
	OrgSetClearance    *OrgSetClearance
	FederationPropose  *FederationPropose
	FederationApprove  *FederationApprove
	FederationRevoke   *FederationRevoke
	DeptRegister       *DeptRegister
	DeptAddMember      *DeptAddMember
	DeptRemoveMember   *DeptRemoveMember
	AgentRegister      *AgentRegister
	AgentUpdateTx      *AgentUpdate        // Named AgentUpdateTx to avoid collision with existing method names
	AgentSetPermission *AgentSetPermission
	MemoryReassign     *MemoryReassign
	GovPropose         *GovPropose
	GovVote            *GovVote
	GovCancel          *GovCancel
	Signature          []byte // Node validator Ed25519 signature (64 bytes)
	PublicKey          []byte // Node validator Ed25519 public key (32 bytes)
	Nonce              uint64
	Timestamp          time.Time

	// Agent identity proof — allows ABCI to verify agent identity on-chain.
	// The agent signed SHA256(requestBody) + bigEndian(AgentTimestamp) [+ nonce] with their key.
	// ABCI re-verifies this signature to establish the authenticated agent identity
	// independently of the REST layer.
	AgentPubKey   []byte // Agent Ed25519 public key (32 bytes)
	AgentSig      []byte // Agent Ed25519 signature (64 bytes)
	AgentTimestamp int64  // Unix seconds timestamp used in signing
	AgentBodyHash []byte // SHA256 of original request body (32 bytes)
	AgentNonce    []byte // Optional nonce used in signing (variable length, 0 if legacy)
}
