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
	DomainTag     string  `json:"domain_tag,omitempty"`
	MinConfidence float64 `json:"min_confidence,omitempty"`
	StatusFilter  string  `json:"status_filter,omitempty"`
	TopK          int     `json:"top_k"`
	Cursor        string  `json:"cursor,omitempty"`
}

// ListOptions defines parameters for listing memories.
type ListOptions struct {
	DomainTag string
	Status    string
	Limit     int
	Offset    int
	Sort      string // "newest", "oldest", "confidence"
}

// StoreStats holds aggregate statistics.
type StoreStats struct {
	TotalMemories int            `json:"total_memories"`
	ByDomain      map[string]int `json:"by_domain"`
	ByStatus      map[string]int `json:"by_status"`
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
	Close() error
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

// OffchainStore is the combined interface for all off-chain storage operations.
// Both PostgresStore and SQLiteStore implement this interface, allowing the ABCI
// app to work with either backend.
type OffchainStore interface {
	MemoryStore
	ValidatorScoreStore
	AccessStore
	OrgStore
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
