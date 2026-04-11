package store

import (
	"context"
	"time"

	"github.com/l33tdawg/sage/internal/memory"
)

// ValidationVote represents a validator's vote on a memory.
type ValidationVote struct {
	ID           int64     `json:"id"`
	MemoryID     string    `json:"memory_id"`
	ValidatorID  string    `json:"validator_id"`
	Decision     string    `json:"decision"` // accept, reject, abstain
	Rationale    string    `json:"rationale,omitempty"`
	WeightAtVote float64   `json:"weight_at_vote"`
	BlockHeight  int64     `json:"block_height"`
	CreatedAt    time.Time `json:"created_at"`
}

// ChallengeEntry represents a challenge against a memory.
type ChallengeEntry struct {
	ID           int64     `json:"id"`
	MemoryID     string    `json:"memory_id"`
	ChallengerID string    `json:"challenger_id"`
	Reason       string    `json:"reason"`
	Evidence     string    `json:"evidence,omitempty"`
	BlockHeight  int64     `json:"block_height"`
	CreatedAt    time.Time `json:"created_at"`
}

// Corroboration represents a corroboration of a memory.
type Corroboration struct {
	ID        int64     `json:"id"`
	MemoryID  string    `json:"memory_id"`
	AgentID   string    `json:"agent_id"`
	Evidence  string    `json:"evidence,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// ValidatorScore represents a validator's PoE score state.
type ValidatorScore struct {
	ValidatorID   string     `json:"validator_id"`
	WeightedSum   float64    `json:"weighted_sum"`
	WeightDenom   float64    `json:"weight_denom"`
	VoteCount     int64      `json:"vote_count"`
	ExpertiseVec  []float64  `json:"expertise_vec"`
	LastActiveTS  *time.Time `json:"last_active_ts"`
	CurrentWeight float64    `json:"current_weight"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

// EpochScore represents a validator's score at a specific epoch.
type EpochScore struct {
	EpochNum         int64   `json:"epoch_num"`
	BlockHeight      int64   `json:"block_height"`
	ValidatorID      string  `json:"validator_id"`
	Accuracy         float64 `json:"accuracy"`
	DomainScore      float64 `json:"domain_score"`
	RecencyScore     float64 `json:"recency_score"`
	CorrScore        float64 `json:"corr_score"`
	RawWeight        float64 `json:"raw_weight"`
	CappedWeight     float64 `json:"capped_weight"`
	NormalizedWeight float64 `json:"normalized_weight"`
}

// QueryOptions defines parameters for similarity queries.
type QueryOptions struct {
	DomainTag       string   `json:"domain_tag,omitempty"`
	Provider        string   `json:"provider,omitempty"`
	MinConfidence   float64  `json:"min_confidence,omitempty"`
	StatusFilter    string   `json:"status_filter,omitempty"`
	TopK            int      `json:"top_k"`
	Cursor          string   `json:"cursor,omitempty"`
	SubmittingAgents []string `json:"submitting_agents,omitempty"` // RBAC: restrict to these agent IDs
}

// ListOptions defines parameters for listing memories.
type ListOptions struct {
	DomainTag        string
	Tag              string // filter by user-defined tag
	Provider         string
	Status           string
	SubmittingAgent  string   // filter memories by agent_id (single)
	SubmittingAgents []string // RBAC: restrict to these agent IDs
	Limit            int
	Offset           int
	Sort             string // "newest", "oldest", "confidence"
}

// StoreStats holds aggregate statistics.
type StoreStats struct {
	TotalMemories int            `json:"total_memories"`
	ByDomain      map[string]int `json:"by_domain"`
	ByStatus      map[string]int `json:"by_status"`
	ByAgent       map[string]int `json:"by_agent,omitempty"`
	DBSizeBytes   int64          `json:"db_size_bytes"`
	LastActivity  *time.Time     `json:"last_activity,omitempty"`
}

// TimelineBucket holds aggregated counts for a time period.
type TimelineBucket struct {
	Period string `json:"period"` // ISO date string
	Count  int    `json:"count"`
	Domain string `json:"domain,omitempty"`
}

// MemoryStore defines the interface for memory storage operations.
type MemoryStore interface {
	InsertMemory(ctx context.Context, record *memory.MemoryRecord) error
	GetMemory(ctx context.Context, memoryID string) (*memory.MemoryRecord, error)
	UpdateStatus(ctx context.Context, memoryID string, status memory.MemoryStatus, now time.Time) error
	QuerySimilar(ctx context.Context, embedding []float32, opts QueryOptions) ([]*memory.MemoryRecord, error)
	SearchByText(ctx context.Context, query string, opts QueryOptions) ([]*memory.MemoryRecord, error)
	InsertTriples(ctx context.Context, memoryID string, triples []memory.KnowledgeTriple) error
	InsertVote(ctx context.Context, vote *ValidationVote) error
	GetVotes(ctx context.Context, memoryID string) ([]*ValidationVote, error)
	InsertChallenge(ctx context.Context, challenge *ChallengeEntry) error
	InsertCorroboration(ctx context.Context, corr *Corroboration) error
	GetCorroborations(ctx context.Context, memoryID string) ([]*Corroboration, error)
	GetPendingByDomain(ctx context.Context, domainTag string, limit int) ([]*memory.MemoryRecord, error)
	ListMemories(ctx context.Context, opts ListOptions) ([]*memory.MemoryRecord, int, error)
	GetStats(ctx context.Context) (*StoreStats, error)
	GetTimeline(ctx context.Context, from, to time.Time, domain string, bucket string) ([]TimelineBucket, error)
	DeleteMemory(ctx context.Context, memoryID string) error
	UpdateDomainTag(ctx context.Context, memoryID string, domain string) error
	UpdateMemoryAgent(ctx context.Context, memoryID string, agentID string) error
	UpdateTaskStatus(ctx context.Context, memoryID string, taskStatus memory.TaskStatus) error
	LinkMemories(ctx context.Context, sourceID, targetID, linkType string) error
	GetLinkedMemories(ctx context.Context, memoryID string) ([]memory.MemoryLink, error)
	GetOpenTasks(ctx context.Context, domain string, provider string) ([]*memory.MemoryRecord, error)
	GetAllTasks(ctx context.Context, domain string, limit int) ([]*memory.MemoryRecord, error)
	// Tags
	SetTags(ctx context.Context, memoryID string, tags []string) error
	GetTags(ctx context.Context, memoryID string) ([]string, error)
	GetTagsBatch(ctx context.Context, memoryIDs []string) (map[string][]string, error)
	ListAllTags(ctx context.Context) ([]TagCount, error)
	ListMemoriesByTag(ctx context.Context, tag string, limit, offset int) ([]*memory.MemoryRecord, int, error)
	// FindByContentHash checks if a committed memory with this content hash exists.
	FindByContentHash(ctx context.Context, contentHash string) (bool, error)
	Close() error
}

// TagCount represents a tag and how many memories use it.
type TagCount struct {
	Tag   string `json:"tag"`
	Count int    `json:"count"`
}

// ValidatorScoreStore defines the interface for validator score storage.
type ValidatorScoreStore interface {
	GetScore(ctx context.Context, validatorID string) (*ValidatorScore, error)
	UpdateScore(ctx context.Context, score *ValidatorScore) error
	GetAllScores(ctx context.Context) ([]*ValidatorScore, error)
	InsertEpochScore(ctx context.Context, epoch *EpochScore) error
}

// AccessGrantEntry represents a domain access grant.
type AccessGrantEntry struct {
	Domain        string     `json:"domain"`
	GranteeID     string     `json:"grantee_id"`
	GranterID     string     `json:"granter_id"`
	Level         uint8      `json:"access_level"`
	ExpiresAt     *time.Time `json:"expires_at,omitempty"`
	CreatedHeight int64      `json:"created_height"`
	CreatedAt     time.Time  `json:"created_at"`
	RevokedAt     *time.Time `json:"revoked_at,omitempty"`
}

// AccessRequestEntry represents a pending access request.
type AccessRequestEntry struct {
	RequestID      string     `json:"request_id"`
	RequesterID    string     `json:"requester_id"`
	TargetDomain   string     `json:"target_domain"`
	Justification  string     `json:"justification,omitempty"`
	Status         string     `json:"status"`
	CreatedHeight  int64      `json:"created_height"`
	ResolvedHeight *int64     `json:"resolved_height,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
}

// AccessLogEntry represents an audit log entry for domain access.
type AccessLogEntry struct {
	AgentID     string    `json:"agent_id"`
	Domain      string    `json:"domain"`
	Action      string    `json:"action"`
	MemoryIDs   []string  `json:"memory_ids,omitempty"`
	BlockHeight int64     `json:"block_height"`
	CreatedAt   time.Time `json:"created_at"`
}

// DomainEntry represents a registered domain in the federation.
type DomainEntry struct {
	DomainName    string    `json:"domain_name"`
	OwnerAgentID  string    `json:"owner_agent_id"`
	ParentDomain  string    `json:"parent_domain,omitempty"`
	Description   string    `json:"description,omitempty"`
	CreatedHeight int64     `json:"created_height"`
	CreatedAt     time.Time `json:"created_at"`
}

// AccessStore defines the interface for federation access control storage.
type AccessStore interface {
	InsertAccessGrant(ctx context.Context, grant *AccessGrantEntry) error
	GetActiveGrants(ctx context.Context, agentID string) ([]*AccessGrantEntry, error)
	RevokeGrant(ctx context.Context, domain, granteeID string, height int64) error
	InsertAccessRequest(ctx context.Context, req *AccessRequestEntry) error
	UpdateAccessRequestStatus(ctx context.Context, requestID, status string, height int64) error
	InsertAccessLog(ctx context.Context, log *AccessLogEntry) error
	InsertDomain(ctx context.Context, domain *DomainEntry) error
	GetDomain(ctx context.Context, name string) (*DomainEntry, error)
}

// ClearanceLevel mirrors tx.ClearanceLevel for store use.
type ClearanceLevel uint8

const (
	ClearancePublic       ClearanceLevel = 0
	ClearanceInternal     ClearanceLevel = 1
	ClearanceConfidential ClearanceLevel = 2
	ClearanceSecret       ClearanceLevel = 3
	ClearanceTopSecret    ClearanceLevel = 4
)

// OrgEntry represents a registered organization.
type OrgEntry struct {
	OrgID         string    `json:"org_id"`
	Name          string    `json:"name"`
	Description   string    `json:"description,omitempty"`
	AdminAgentID  string    `json:"admin_agent_id"`
	CreatedHeight int64     `json:"created_height"`
	CreatedAt     time.Time `json:"created_at"`
}

// OrgMemberEntry represents a member within an organization.
type OrgMemberEntry struct {
	OrgID         string         `json:"org_id"`
	AgentID       string         `json:"agent_id"`
	Clearance     ClearanceLevel `json:"clearance"`
	Role          string         `json:"role"` // admin, member, observer
	CreatedHeight int64          `json:"created_height"`
	CreatedAt     time.Time      `json:"created_at"`
	RemovedAt     *time.Time     `json:"removed_at,omitempty"`
}

// FederationEntry represents a federation agreement between two organizations.
type FederationEntry struct {
	FederationID     string         `json:"federation_id"`
	ProposerOrgID    string         `json:"proposer_org_id"`
	TargetOrgID      string         `json:"target_org_id"`
	AllowedDomains   []string       `json:"allowed_domains"`
	AllowedDepts     []string       `json:"allowed_depts,omitempty"` // Department-level federation scope
	MaxClearance     ClearanceLevel `json:"max_clearance"`
	ExpiresAt        *time.Time     `json:"expires_at,omitempty"`
	RequiresApproval bool           `json:"requires_approval"`
	Status           string         `json:"status"` // proposed, active, revoked
	CreatedHeight    int64          `json:"created_height"`
	ApprovedHeight   *int64         `json:"approved_height,omitempty"`
	CreatedAt        time.Time      `json:"created_at"`
	RevokedAt        *time.Time     `json:"revoked_at,omitempty"`
}

// DeptEntry represents a department within an organization.
type DeptEntry struct {
	OrgID         string    `json:"org_id"`
	DeptID        string    `json:"dept_id"`
	DeptName      string    `json:"dept_name"`
	Description   string    `json:"description,omitempty"`
	ParentDept    string    `json:"parent_dept,omitempty"`
	CreatedHeight int64     `json:"created_height"`
	CreatedAt     time.Time `json:"created_at"`
}

// DeptMemberEntry represents a member within a department.
type DeptMemberEntry struct {
	OrgID         string         `json:"org_id"`
	DeptID        string         `json:"dept_id"`
	AgentID       string         `json:"agent_id"`
	Clearance     ClearanceLevel `json:"clearance"`
	Role          string         `json:"role"` // admin, member, observer
	CreatedHeight int64          `json:"created_height"`
	CreatedAt     time.Time      `json:"created_at"`
	RemovedAt     *time.Time     `json:"removed_at,omitempty"`
}

// AgentEntry represents a network agent (validator/peer node).
type AgentEntry struct {
	AgentID         string     `json:"agent_id"`
	Name            string     `json:"name"`
	RegisteredName  string     `json:"registered_name,omitempty"` // Immutable name from initial registration
	Role            string     `json:"role"`
	Avatar          string     `json:"avatar,omitempty"`
	BootBio         string     `json:"boot_bio,omitempty"`
	ValidatorPubkey string     `json:"validator_pubkey,omitempty"`
	NodeID          string     `json:"node_id,omitempty"`
	P2PAddress      string     `json:"p2p_address,omitempty"`
	Status          string     `json:"status"`
	Clearance       int        `json:"clearance"`
	OrgID           string     `json:"org_id,omitempty"`
	DeptID          string     `json:"dept_id,omitempty"`
	DomainAccess    string     `json:"domain_access,omitempty"`
	BundlePath      string     `json:"bundle_path,omitempty"`
	FirstSeen       *time.Time `json:"first_seen,omitempty"`
	LastSeen        *time.Time `json:"last_seen,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	RemovedAt       *time.Time `json:"removed_at,omitempty"`
	OnChainHeight   int64      `json:"on_chain_height,omitempty"`  // Block height where registered (0 = legacy)
	VisibleAgents   string     `json:"visible_agents,omitempty"`   // JSON array of agent IDs ("*" = all)
	Provider        string     `json:"provider,omitempty"`         // "claude-code", "chatgpt", etc.
	MemoryCount     int        `json:"memory_count,omitempty"`
	ClaimToken      string     `json:"claim_token,omitempty"`      // One-time token for CLI agent install
	ClaimExpiresAt  *time.Time `json:"claim_expires_at,omitempty"` // When the claim token expires
}

// RedeploymentLogEntry represents a redeployment operation log entry.
type RedeploymentLogEntry struct {
	ID            int64      `json:"id"`
	Operation     string     `json:"operation"`
	AgentID       string     `json:"agent_id"`
	Phase         string     `json:"phase"`
	Status        string     `json:"status"`
	Details       string     `json:"details,omitempty"`
	SQLiteBackup  string     `json:"sqlite_backup,omitempty"`
	GenesisBackup string     `json:"genesis_backup,omitempty"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	CompletedAt   *time.Time `json:"completed_at,omitempty"`
	Error         string     `json:"error,omitempty"`
}

// RedeploymentLock represents the singleton redeployment lock.
type RedeploymentLock struct {
	LockedBy  string    `json:"locked_by"`
	LockedAt  time.Time `json:"locked_at"`
	Operation string    `json:"operation"`
	ExpiresAt time.Time `json:"expires_at"`
}

// AgentStore defines the interface for network agent management.
type AgentStore interface {
	ListAgents(ctx context.Context) ([]*AgentEntry, error)
	GetAgent(ctx context.Context, agentID string) (*AgentEntry, error)
	// GetAgentByName looks up an agent by its display name (e.g. "claude-code/sage").
	// Returns nil, nil if no agent matches.
	GetAgentByName(ctx context.Context, name string) (*AgentEntry, error)
	CreateAgent(ctx context.Context, agent *AgentEntry) error
	UpdateAgent(ctx context.Context, agent *AgentEntry) error
	RemoveAgent(ctx context.Context, agentID string) error
	UpdateAgentStatus(ctx context.Context, agentID, status string) error
	UpdateAgentLastSeen(ctx context.Context, agentID string, lastSeen time.Time) error
	BackfillFirstSeen(ctx context.Context, agentID string, firstSeen time.Time) error
	// RotateAgentKey generates a new Ed25519 keypair for the agent, updates the
	// agent record and re-attributes all memories from oldAgentID to the new ID.
	// Both operations run in a single transaction. Returns the new agent_id
	// (hex-encoded public key) and the Ed25519 seed for bundle generation.
	RotateAgentKey(ctx context.Context, oldAgentID string) (newAgentID string, seed []byte, err error)
	// ReassignMemories atomically moves all memories from sourceAgentID to targetAgentID.
	// Returns the count of memories reassigned.
	ReassignMemories(ctx context.Context, sourceAgentID, targetAgentID string) (int64, error)
	// ListAgentTags returns all tags used by memories belonging to a specific agent
	ListAgentTags(ctx context.Context, agentID string) ([]TagCount, error)
	// ReassignMemoriesByTag moves memories with a specific tag from source to target agent
	ReassignMemoriesByTag(ctx context.Context, sourceAgentID, targetAgentID, tag string) (int64, error)
	// ReassignMemoriesByDomain moves memories in a specific domain from source to target agent
	ReassignMemoriesByDomain(ctx context.Context, sourceAgentID, targetAgentID, domain string) (int64, error)
	// Redeployment
	AcquireRedeployLock(ctx context.Context, lockedBy, operation string, ttl time.Duration) error
	ReleaseRedeployLock(ctx context.Context) error
	GetRedeployLock(ctx context.Context) (*RedeploymentLock, error)
	InsertRedeployLog(ctx context.Context, entry *RedeploymentLogEntry) error
	GetRedeployLog(ctx context.Context, operation string) ([]*RedeploymentLogEntry, error)
	UpdateRedeployLog(ctx context.Context, id int64, status, errorMsg string) error
}

// PipelineMessage represents an ephemeral agent-to-agent work item.
// These are NOT memories — they are transient messages that auto-expire.
type PipelineMessage struct {
	PipeID       string     `json:"pipe_id"`
	FromAgent    string     `json:"from_agent"`
	FromProvider string     `json:"from_provider"`
	ToAgent      string     `json:"to_agent,omitempty"`
	ToProvider   string     `json:"to_provider"`
	Intent       string     `json:"intent"`
	Payload      string     `json:"payload"`
	Result       string     `json:"result,omitempty"`
	Status       string     `json:"status"` // pending, claimed, completed, expired, failed
	CreatedAt    time.Time  `json:"created_at"`
	ClaimedAt    *time.Time `json:"claimed_at,omitempty"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
	ExpiresAt    time.Time  `json:"expires_at"`
	JournalID    string     `json:"journal_id,omitempty"`
}

// PipelineStore defines the interface for agent-to-agent pipeline storage.
type PipelineStore interface {
	InsertPipeline(ctx context.Context, msg *PipelineMessage) error
	GetPipeline(ctx context.Context, pipeID string) (*PipelineMessage, error)
	GetInbox(ctx context.Context, agentID, provider string, limit int) ([]*PipelineMessage, error)
	ClaimPipeline(ctx context.Context, pipeID, agentID string) error
	CompletePipeline(ctx context.Context, pipeID, result, journalID string) error
	GetCompletedForSender(ctx context.Context, agentID string, limit int) ([]*PipelineMessage, error)
	ListPipelines(ctx context.Context, status string, limit int) ([]*PipelineMessage, error)
	PipelineStats(ctx context.Context) (map[string]int, error)
	ExpirePipelines(ctx context.Context) (int, error)
	PurgePipelines(ctx context.Context, olderThan time.Time) (int, error)
}

// GovernanceStore defines the interface for governance proposal and vote storage.
type GovernanceStore interface {
	InsertGovProposal(ctx context.Context, proposal *GovProposal) error
	GetGovProposal(ctx context.Context, proposalID string) (*GovProposal, error)
	UpdateGovProposalStatus(ctx context.Context, proposalID, status string, executedHeight *int64) error
	ListGovProposals(ctx context.Context, status string) ([]*GovProposal, error)
	InsertGovVote(ctx context.Context, vote *GovVote) error
	GetGovVotes(ctx context.Context, proposalID string) ([]*GovVote, error)
}

// OffchainStore is the combined interface for all off-chain storage operations.
// Both PostgresStore and SQLiteStore implement this interface, allowing the ABCI
// app to work with either backend.
type OffchainStore interface {
	MemoryStore
	ValidatorScoreStore
	AccessStore
	OrgStore
	AgentStore
	PipelineStore
	GovernanceStore
	Ping(ctx context.Context) error
	// RunInTx executes fn within a database transaction. If fn returns an error,
	// the transaction is rolled back; otherwise it is committed. The OffchainStore
	// passed to fn is scoped to the transaction — all writes through it are atomic.
	RunInTx(ctx context.Context, fn func(tx OffchainStore) error) error
}

// OrgStore defines the interface for organization, department, and federation storage.
type OrgStore interface {
	InsertOrg(ctx context.Context, org *OrgEntry) error
	GetOrg(ctx context.Context, orgID string) (*OrgEntry, error)
	InsertOrgMember(ctx context.Context, member *OrgMemberEntry) error
	RemoveOrgMember(ctx context.Context, orgID, agentID string, height int64) error
	UpdateMemberClearance(ctx context.Context, orgID, agentID string, clearance ClearanceLevel) error
	GetOrgMembers(ctx context.Context, orgID string) ([]*OrgMemberEntry, error)
	InsertFederation(ctx context.Context, fed *FederationEntry) error
	GetFederation(ctx context.Context, federationID string) (*FederationEntry, error)
	ApproveFederation(ctx context.Context, federationID string, height int64) error
	RevokeFederation(ctx context.Context, federationID string, height int64) error
	GetActiveFederations(ctx context.Context, orgID string) ([]*FederationEntry, error)
	UpdateMemoryClassification(ctx context.Context, memoryID string, classification ClearanceLevel) error
	// Department methods
	InsertDept(ctx context.Context, dept *DeptEntry) error
	GetDept(ctx context.Context, orgID, deptID string) (*DeptEntry, error)
	GetOrgDepts(ctx context.Context, orgID string) ([]*DeptEntry, error)
	InsertDeptMember(ctx context.Context, member *DeptMemberEntry) error
	RemoveDeptMember(ctx context.Context, orgID, deptID, agentID string, height int64) error
	GetDeptMembers(ctx context.Context, orgID, deptID string) ([]*DeptMemberEntry, error)
	UpdateDeptMemberClearance(ctx context.Context, orgID, deptID, agentID string, clearance ClearanceLevel) error
}
