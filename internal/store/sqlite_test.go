package store

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := NewSQLiteStore(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

func testMemory(id, agent, content, domain string) *memory.MemoryRecord {
	h := sha256.Sum256([]byte(content))
	return &memory.MemoryRecord{
		MemoryID:        id,
		SubmittingAgent: agent,
		Content:         content,
		ContentHash:     h[:],
		MemoryType:      memory.TypeObservation,
		DomainTag:       domain,
		ConfidenceScore: 0.85,
		Status:          memory.StatusProposed,
		CreatedAt:       time.Now().UTC(),
	}
}

func TestNewSQLiteStore(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := NewSQLiteStore(context.Background(), dbPath)
	require.NoError(t, err)
	defer s.Close()

	// Verify we can ping
	require.NoError(t, s.Ping(context.Background()))

	// Verify tables exist by inserting a memory
	rec := testMemory("m1", "agent1", "hello", "general")
	require.NoError(t, s.InsertMemory(context.Background(), rec))
}

func TestInsertAndGetMemory(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rec := testMemory("m1", "agent1", "test content", "security")
	rec.Embedding = []float32{0.1, 0.2, 0.3}
	rec.ParentHash = "parent123"
	require.NoError(t, s.InsertMemory(ctx, rec))

	got, err := s.GetMemory(ctx, "m1")
	require.NoError(t, err)
	assert.Equal(t, "m1", got.MemoryID)
	assert.Equal(t, "agent1", got.SubmittingAgent)
	assert.Equal(t, "test content", got.Content)
	assert.Equal(t, memory.TypeObservation, got.MemoryType)
	assert.Equal(t, "security", got.DomainTag)
	assert.InDelta(t, 0.85, got.ConfidenceScore, 0.001)
	assert.Equal(t, memory.StatusProposed, got.Status)
	assert.Equal(t, "parent123", got.ParentHash)
	assert.InDeltaSlice(t, []float32{0.1, 0.2, 0.3}, got.Embedding, 0.001)

	// Not found
	_, err = s.GetMemory(ctx, "nonexistent")
	assert.Error(t, err)
}

func TestUpdateStatus(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rec := testMemory("m1", "agent1", "content", "general")
	require.NoError(t, s.InsertMemory(ctx, rec))

	now := time.Now().UTC()

	// proposed -> committed
	require.NoError(t, s.UpdateStatus(ctx, "m1", memory.StatusCommitted, now))
	got, err := s.GetMemory(ctx, "m1")
	require.NoError(t, err)
	assert.Equal(t, memory.StatusCommitted, got.Status)
	assert.NotNil(t, got.CommittedAt)

	// committed -> deprecated
	require.NoError(t, s.UpdateStatus(ctx, "m1", memory.StatusDeprecated, now))
	got, err = s.GetMemory(ctx, "m1")
	require.NoError(t, err)
	assert.Equal(t, memory.StatusDeprecated, got.Status)
	assert.NotNil(t, got.DeprecatedAt)
}

func TestQuerySimilar(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Insert memories with different embeddings
	for i := 0; i < 5; i++ {
		rec := testMemory(fmt.Sprintf("m%d", i), "agent1", fmt.Sprintf("content %d", i), "general")
		emb := make([]float32, 3)
		emb[0] = float32(i) * 0.2
		emb[1] = 1.0 - float32(i)*0.2
		emb[2] = 0.5
		rec.Embedding = emb
		require.NoError(t, s.InsertMemory(ctx, rec))
	}

	// Query with embedding close to m4
	query := []float32{0.8, 0.2, 0.5}
	results, err := s.QuerySimilar(ctx, query, QueryOptions{TopK: 3})
	require.NoError(t, err)
	assert.Len(t, results, 3)

	// Domain filter
	rec := testMemory("msec", "agent1", "secure content", "security")
	rec.Embedding = []float32{0.9, 0.1, 0.5}
	require.NoError(t, s.InsertMemory(ctx, rec))

	results, err = s.QuerySimilar(ctx, query, QueryOptions{TopK: 10, DomainTag: "security"})
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "msec", results[0].MemoryID)
}

func TestInsertTriples(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rec := testMemory("m1", "agent1", "Go is a language", "general")
	require.NoError(t, s.InsertMemory(ctx, rec))

	triples := []memory.KnowledgeTriple{
		{Subject: "Go", Predicate: "is_a", Object: "language"},
		{Subject: "Go", Predicate: "created_by", Object: "Google"},
	}
	require.NoError(t, s.InsertTriples(ctx, "m1", triples))
}

func TestInsertVoteAndGetVotes(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rec := testMemory("m1", "agent1", "content", "general")
	require.NoError(t, s.InsertMemory(ctx, rec))

	vote := &ValidationVote{
		MemoryID:     "m1",
		ValidatorID:  "val1",
		Decision:     "accept",
		Rationale:    "looks good",
		WeightAtVote: 0.5,
		BlockHeight:  100,
		CreatedAt:    time.Now().UTC(),
	}
	require.NoError(t, s.InsertVote(ctx, vote))

	vote2 := &ValidationVote{
		MemoryID:     "m1",
		ValidatorID:  "val2",
		Decision:     "reject",
		Rationale:    "not sure",
		WeightAtVote: 0.3,
		BlockHeight:  100,
		CreatedAt:    time.Now().UTC(),
	}
	require.NoError(t, s.InsertVote(ctx, vote2))

	votes, err := s.GetVotes(ctx, "m1")
	require.NoError(t, err)
	assert.Len(t, votes, 2)
	assert.Equal(t, "val1", votes[0].ValidatorID)
	assert.Equal(t, "accept", votes[0].Decision)
}

func TestInsertCorroborationAndGetCorroborations(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rec := testMemory("m1", "agent1", "content", "general")
	require.NoError(t, s.InsertMemory(ctx, rec))

	corr := &Corroboration{
		MemoryID:  "m1",
		AgentID:   "agent2",
		Evidence:  "I confirm this",
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, s.InsertCorroboration(ctx, corr))

	corrs, err := s.GetCorroborations(ctx, "m1")
	require.NoError(t, err)
	assert.Len(t, corrs, 1)
	assert.Equal(t, "agent2", corrs[0].AgentID)
	assert.Equal(t, "I confirm this", corrs[0].Evidence)
}

func TestGetPendingByDomain(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Insert proposed memories in different domains
	for i := 0; i < 3; i++ {
		rec := testMemory(fmt.Sprintf("sec%d", i), "agent1", fmt.Sprintf("sec content %d", i), "security")
		require.NoError(t, s.InsertMemory(ctx, rec))
	}
	rec := testMemory("gen1", "agent1", "general content", "general")
	require.NoError(t, s.InsertMemory(ctx, rec))

	// Exact match
	results, err := s.GetPendingByDomain(ctx, "security", 10)
	require.NoError(t, err)
	assert.Len(t, results, 3)

	// Wildcard
	results, err = s.GetPendingByDomain(ctx, "%", 10)
	require.NoError(t, err)
	assert.Len(t, results, 4)
}

func TestListMemories(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		domain := "general"
		if i%2 == 0 {
			domain = "security"
		}
		rec := testMemory(fmt.Sprintf("m%d", i), "agent1", fmt.Sprintf("content %d", i), domain)
		rec.CreatedAt = time.Now().UTC().Add(time.Duration(i) * time.Second)
		require.NoError(t, s.InsertMemory(ctx, rec))
	}

	// Pagination
	records, total, err := s.ListMemories(ctx, ListOptions{Limit: 3, Offset: 0})
	require.NoError(t, err)
	assert.Equal(t, 10, total)
	assert.Len(t, records, 3)

	// Domain filter
	records, total, err = s.ListMemories(ctx, ListOptions{DomainTag: "security", Limit: 50})
	require.NoError(t, err)
	assert.Equal(t, 5, total)
	assert.Len(t, records, 5)

	// Sort by oldest
	records, _, err = s.ListMemories(ctx, ListOptions{Limit: 2, Sort: "oldest"})
	require.NoError(t, err)
	assert.Equal(t, "m0", records[0].MemoryID)
}

func TestGetStats(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rec := testMemory("m1", "agent1", "content1", "security")
	require.NoError(t, s.InsertMemory(ctx, rec))
	rec2 := testMemory("m2", "agent1", "content2", "general")
	require.NoError(t, s.InsertMemory(ctx, rec2))

	stats, err := s.GetStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, stats.TotalMemories)
	assert.Equal(t, 1, stats.ByDomain["security"])
	assert.Equal(t, 1, stats.ByDomain["general"])
	assert.Equal(t, 2, stats.ByStatus["proposed"])
	assert.NotNil(t, stats.LastActivity)
}

func TestGetTimeline(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC()
	for i := 0; i < 3; i++ {
		rec := testMemory(fmt.Sprintf("m%d", i), "agent1", fmt.Sprintf("content %d", i), "general")
		rec.CreatedAt = now.Add(-time.Duration(i) * time.Hour)
		require.NoError(t, s.InsertMemory(ctx, rec))
	}

	buckets, err := s.GetTimeline(ctx, now.Add(-24*time.Hour), now.Add(time.Hour), "", "hour")
	require.NoError(t, err)
	assert.NotEmpty(t, buckets)

	total := 0
	for _, b := range buckets {
		total += b.Count
	}
	assert.Equal(t, 3, total)
}

func TestDeleteMemory(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rec := testMemory("m1", "agent1", "content", "general")
	require.NoError(t, s.InsertMemory(ctx, rec))

	require.NoError(t, s.DeleteMemory(ctx, "m1"))

	got, err := s.GetMemory(ctx, "m1")
	require.NoError(t, err)
	assert.Equal(t, memory.StatusDeprecated, got.Status)
	assert.NotNil(t, got.DeprecatedAt)
}

func TestUpdateDomainTag(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rec := testMemory("m1", "agent1", "content", "general")
	require.NoError(t, s.InsertMemory(ctx, rec))

	require.NoError(t, s.UpdateDomainTag(ctx, "m1", "security"))

	got, err := s.GetMemory(ctx, "m1")
	require.NoError(t, err)
	assert.Equal(t, "security", got.DomainTag)
}

func TestValidatorScores(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC()
	score := &ValidatorScore{
		ValidatorID:   "val1",
		WeightedSum:   10.5,
		WeightDenom:   5.0,
		VoteCount:     20,
		ExpertiseVec:  []float64{0.1, 0.2, 0.3},
		LastActiveTS:  &now,
		CurrentWeight: 0.75,
		UpdatedAt:     now,
	}
	require.NoError(t, s.UpdateScore(ctx, score))

	got, err := s.GetScore(ctx, "val1")
	require.NoError(t, err)
	assert.Equal(t, "val1", got.ValidatorID)
	assert.InDelta(t, 10.5, got.WeightedSum, 0.001)
	assert.Equal(t, int64(20), got.VoteCount)
	assert.InDeltaSlice(t, []float64{0.1, 0.2, 0.3}, got.ExpertiseVec, 0.001)

	// Not found
	_, err = s.GetScore(ctx, "nonexistent")
	assert.Error(t, err)

	// GetAllScores
	score2 := &ValidatorScore{
		ValidatorID:   "val2",
		WeightedSum:   5.0,
		WeightDenom:   2.0,
		VoteCount:     10,
		ExpertiseVec:  []float64{0.4, 0.5},
		CurrentWeight: 0.5,
		UpdatedAt:     now,
	}
	require.NoError(t, s.UpdateScore(ctx, score2))

	all, err := s.GetAllScores(ctx)
	require.NoError(t, err)
	assert.Len(t, all, 2)

	// InsertEpochScore
	epoch := &EpochScore{
		EpochNum:         1,
		BlockHeight:      100,
		ValidatorID:      "val1",
		Accuracy:         0.9,
		DomainScore:      0.8,
		RecencyScore:     0.7,
		CorrScore:        0.6,
		RawWeight:        0.5,
		CappedWeight:     0.45,
		NormalizedWeight: 0.4,
	}
	require.NoError(t, s.InsertEpochScore(ctx, epoch))
}

func TestAccessStore(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC()

	// InsertAccessGrant
	grant := &AccessGrantEntry{
		Domain:        "security",
		GranteeID:     "agent1",
		GranterID:     "admin",
		Level:         2,
		CreatedHeight: 100,
		CreatedAt:     now,
	}
	require.NoError(t, s.InsertAccessGrant(ctx, grant))

	// GetActiveGrants
	grants, err := s.GetActiveGrants(ctx, "agent1")
	require.NoError(t, err)
	assert.Len(t, grants, 1)
	assert.Equal(t, "security", grants[0].Domain)
	assert.Equal(t, uint8(2), grants[0].Level)

	// RevokeGrant
	require.NoError(t, s.RevokeGrant(ctx, "security", "agent1", 200))
	grants, err = s.GetActiveGrants(ctx, "agent1")
	require.NoError(t, err)
	assert.Len(t, grants, 0)

	// InsertAccessRequest + UpdateAccessRequestStatus
	req := &AccessRequestEntry{
		RequestID:     "req1",
		RequesterID:   "agent2",
		TargetDomain:  "security",
		Justification: "need access",
		Status:        "pending",
		CreatedHeight: 100,
		CreatedAt:     now,
	}
	require.NoError(t, s.InsertAccessRequest(ctx, req))
	require.NoError(t, s.UpdateAccessRequestStatus(ctx, "req1", "approved", 200))

	// InsertAccessLog
	log := &AccessLogEntry{
		AgentID:     "agent1",
		Domain:      "security",
		Action:      "read",
		MemoryIDs:   []string{"m1", "m2"},
		BlockHeight: 100,
		CreatedAt:   now,
	}
	require.NoError(t, s.InsertAccessLog(ctx, log))

	// InsertDomain + GetDomain
	domain := &DomainEntry{
		DomainName:    "test-domain",
		OwnerAgentID:  "admin",
		Description:   "a test domain",
		CreatedHeight: 100,
		CreatedAt:     now,
	}
	require.NoError(t, s.InsertDomain(ctx, domain))

	got, err := s.GetDomain(ctx, "test-domain")
	require.NoError(t, err)
	assert.Equal(t, "test-domain", got.DomainName)
	assert.Equal(t, "admin", got.OwnerAgentID)
	assert.Equal(t, "a test domain", got.Description)
}

func TestOrgStore(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC()

	// InsertOrg + GetOrg
	org := &OrgEntry{
		OrgID:         "org1",
		Name:          "Test Org",
		Description:   "test org desc",
		AdminAgentID:  "admin1",
		CreatedHeight: 100,
		CreatedAt:     now,
	}
	require.NoError(t, s.InsertOrg(ctx, org))

	gotOrg, err := s.GetOrg(ctx, "org1")
	require.NoError(t, err)
	assert.Equal(t, "Test Org", gotOrg.Name)
	assert.Equal(t, "admin1", gotOrg.AdminAgentID)

	// InsertOrgMember + GetOrgMembers
	member := &OrgMemberEntry{
		OrgID:         "org1",
		AgentID:       "agent1",
		Clearance:     ClearanceConfidential,
		Role:          "member",
		CreatedHeight: 100,
		CreatedAt:     now,
	}
	require.NoError(t, s.InsertOrgMember(ctx, member))

	members, err := s.GetOrgMembers(ctx, "org1")
	require.NoError(t, err)
	assert.Len(t, members, 1)
	assert.Equal(t, ClearanceConfidential, members[0].Clearance)

	// UpdateMemberClearance
	require.NoError(t, s.UpdateMemberClearance(ctx, "org1", "agent1", ClearanceSecret))
	members, err = s.GetOrgMembers(ctx, "org1")
	require.NoError(t, err)
	assert.Equal(t, ClearanceSecret, members[0].Clearance)

	// RemoveOrgMember
	require.NoError(t, s.RemoveOrgMember(ctx, "org1", "agent1", 200))
	members, err = s.GetOrgMembers(ctx, "org1")
	require.NoError(t, err)
	assert.Len(t, members, 0)

	// Federation
	fed := &FederationEntry{
		FederationID:     "fed1",
		ProposerOrgID:    "org1",
		TargetOrgID:      "org2",
		AllowedDomains:   []string{"security", "general"},
		MaxClearance:     ClearanceConfidential,
		RequiresApproval: true,
		Status:           "proposed",
		CreatedHeight:    100,
		CreatedAt:        now,
	}
	require.NoError(t, s.InsertFederation(ctx, fed))

	gotFed, err := s.GetFederation(ctx, "fed1")
	require.NoError(t, err)
	assert.Equal(t, "org1", gotFed.ProposerOrgID)
	assert.True(t, gotFed.RequiresApproval)
	assert.Equal(t, []string{"security", "general"}, gotFed.AllowedDomains)

	// ApproveFederation
	require.NoError(t, s.ApproveFederation(ctx, "fed1", 200))
	gotFed, err = s.GetFederation(ctx, "fed1")
	require.NoError(t, err)
	assert.Equal(t, "active", gotFed.Status)

	// GetActiveFederations
	feds, err := s.GetActiveFederations(ctx, "org1")
	require.NoError(t, err)
	assert.Len(t, feds, 1)

	// RevokeFederation
	require.NoError(t, s.RevokeFederation(ctx, "fed1", 300))
	gotFed, err = s.GetFederation(ctx, "fed1")
	require.NoError(t, err)
	assert.Equal(t, "revoked", gotFed.Status)

	// Departments
	dept := &DeptEntry{
		OrgID:         "org1",
		DeptID:        "dept1",
		DeptName:      "Engineering",
		Description:   "eng dept",
		CreatedHeight: 100,
	}
	require.NoError(t, s.InsertDept(ctx, dept))

	gotDept, err := s.GetDept(ctx, "org1", "dept1")
	require.NoError(t, err)
	assert.Equal(t, "Engineering", gotDept.DeptName)

	depts, err := s.GetOrgDepts(ctx, "org1")
	require.NoError(t, err)
	assert.Len(t, depts, 1)

	// Department members
	deptMember := &DeptMemberEntry{
		OrgID:         "org1",
		DeptID:        "dept1",
		AgentID:       "agent2",
		Clearance:     ClearanceInternal,
		Role:          "member",
		CreatedHeight: 100,
		CreatedAt:     now,
	}
	require.NoError(t, s.InsertDeptMember(ctx, deptMember))

	deptMembers, err := s.GetDeptMembers(ctx, "org1", "dept1")
	require.NoError(t, err)
	assert.Len(t, deptMembers, 1)

	require.NoError(t, s.UpdateDeptMemberClearance(ctx, "org1", "dept1", "agent2", ClearanceSecret))
	deptMembers, err = s.GetDeptMembers(ctx, "org1", "dept1")
	require.NoError(t, err)
	assert.Equal(t, ClearanceSecret, deptMembers[0].Clearance)

	require.NoError(t, s.RemoveDeptMember(ctx, "org1", "dept1", "agent2", 200))
	deptMembers, err = s.GetDeptMembers(ctx, "org1", "dept1")
	require.NoError(t, err)
	assert.Len(t, deptMembers, 0)
}

func TestPing(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Ping(context.Background()))
}

// --- Agent store tests ---

func testAgent(id, name, role string) *AgentEntry {
	return &AgentEntry{
		AgentID:   id,
		Name:      name,
		Role:      role,
		Status:    "pending",
		Clearance: 1,
		CreatedAt: time.Now().UTC(),
	}
}

func TestCreateAndGetAgent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	agent := testAgent("agent-1", "Agent One", "validator")
	agent.Avatar = "avatar.png"
	agent.BootBio = "I am agent one"
	agent.ValidatorPubkey = "pubkey123"
	agent.NodeID = "node-1"
	agent.P2PAddress = "tcp://127.0.0.1:26656"
	agent.OrgID = "org-1"
	agent.DeptID = "dept-1"
	agent.DomainAccess = "security,general"
	agent.BundlePath = "/bundles/agent-1.tar.gz"

	require.NoError(t, s.CreateAgent(ctx, agent))

	got, err := s.GetAgent(ctx, "agent-1")
	require.NoError(t, err)
	assert.Equal(t, "agent-1", got.AgentID)
	assert.Equal(t, "Agent One", got.Name)
	assert.Equal(t, "validator", got.Role)
	assert.Equal(t, "avatar.png", got.Avatar)
	assert.Equal(t, "I am agent one", got.BootBio)
	assert.Equal(t, "pubkey123", got.ValidatorPubkey)
	assert.Equal(t, "node-1", got.NodeID)
	assert.Equal(t, "tcp://127.0.0.1:26656", got.P2PAddress)
	assert.Equal(t, "pending", got.Status)
	assert.Equal(t, 1, got.Clearance)
	assert.Equal(t, "org-1", got.OrgID)
	assert.Equal(t, "dept-1", got.DeptID)
	assert.Equal(t, "security,general", got.DomainAccess)
	assert.Equal(t, "/bundles/agent-1.tar.gz", got.BundlePath)

	// Not found
	_, err = s.GetAgent(ctx, "nonexistent")
	assert.Error(t, err)
}

func TestListAgents(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		a := testAgent(fmt.Sprintf("agent-%d", i), fmt.Sprintf("Agent %d", i), "validator")
		require.NoError(t, s.CreateAgent(ctx, a))
	}

	agents, err := s.ListAgents(ctx)
	require.NoError(t, err)
	assert.Len(t, agents, 3)

	// Remove one agent — ListAgents excludes removed agents
	require.NoError(t, s.RemoveAgent(ctx, "agent-1"))

	agents, err = s.ListAgents(ctx)
	require.NoError(t, err)
	assert.Len(t, agents, 2, "removed agent should be excluded from list")

	// But GetAgent still returns the removed agent with status "removed"
	removed, err := s.GetAgent(ctx, "agent-1")
	require.NoError(t, err)
	assert.Equal(t, "removed", removed.Status)
	assert.NotNil(t, removed.RemovedAt)
}

func TestUpdateAgent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	agent := testAgent("agent-1", "Original Name", "observer")
	require.NoError(t, s.CreateAgent(ctx, agent))

	agent.Name = "Updated Name"
	agent.Role = "validator"
	agent.Clearance = 3
	require.NoError(t, s.UpdateAgent(ctx, agent))

	got, err := s.GetAgent(ctx, "agent-1")
	require.NoError(t, err)
	assert.Equal(t, "Updated Name", got.Name)
	assert.Equal(t, "validator", got.Role)
	assert.Equal(t, 3, got.Clearance)
}

func TestRemoveAgent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	agent := testAgent("agent-1", "Agent One", "validator")
	require.NoError(t, s.CreateAgent(ctx, agent))

	require.NoError(t, s.RemoveAgent(ctx, "agent-1"))

	got, err := s.GetAgent(ctx, "agent-1")
	require.NoError(t, err)
	assert.Equal(t, "removed", got.Status)
	assert.NotNil(t, got.RemovedAt)
}

func TestUpdateAgentStatus(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	agent := testAgent("agent-1", "Agent One", "validator")
	require.NoError(t, s.CreateAgent(ctx, agent))

	require.NoError(t, s.UpdateAgentStatus(ctx, "agent-1", "active"))

	got, err := s.GetAgent(ctx, "agent-1")
	require.NoError(t, err)
	assert.Equal(t, "active", got.Status)
}

func TestUpdateAgentLastSeen(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	agent := testAgent("agent-1", "Agent One", "validator")
	require.NoError(t, s.CreateAgent(ctx, agent))

	now := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, s.UpdateAgentLastSeen(ctx, "agent-1", now))

	got, err := s.GetAgent(ctx, "agent-1")
	require.NoError(t, err)
	require.NotNil(t, got.LastSeen)
	assert.WithinDuration(t, now, *got.LastSeen, time.Second)
	assert.Equal(t, "active", got.Status, "UpdateAgentLastSeen should also set status to active")
}

func TestAcquireRedeployLock(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Acquire lock
	require.NoError(t, s.AcquireRedeployLock(ctx, "agent-1", "add_agent", 10*time.Minute))

	// Try to acquire again — should fail because lock is held
	err := s.AcquireRedeployLock(ctx, "agent-2", "add_agent", 10*time.Minute)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "agent-1")

	// Verify lock info
	lock, err := s.GetRedeployLock(ctx)
	require.NoError(t, err)
	assert.Equal(t, "agent-1", lock.LockedBy)
	assert.Equal(t, "add_agent", lock.Operation)

	// Release and re-acquire
	require.NoError(t, s.ReleaseRedeployLock(ctx))
	require.NoError(t, s.AcquireRedeployLock(ctx, "agent-2", "remove_agent", 10*time.Minute))

	lock, err = s.GetRedeployLock(ctx)
	require.NoError(t, err)
	assert.Equal(t, "agent-2", lock.LockedBy)
}

func TestRedeployLockExpiry(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Acquire lock with 2s TTL (RFC3339 has second precision)
	require.NoError(t, s.AcquireRedeployLock(ctx, "agent-1", "add_agent", 2*time.Second))

	// Sleep to let it expire
	time.Sleep(3 * time.Second)

	// Should succeed because the lock has expired
	require.NoError(t, s.AcquireRedeployLock(ctx, "agent-2", "add_agent", 10*time.Minute))

	lock, err := s.GetRedeployLock(ctx)
	require.NoError(t, err)
	assert.Equal(t, "agent-2", lock.LockedBy)
}

func TestRedeployLog(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	entry := &RedeploymentLogEntry{
		Operation: "add_agent",
		AgentID:   "agent-1",
		Phase:     "LOCK_ACQUIRED",
		Status:    "in_progress",
		Details:   "acquiring lock",
	}
	require.NoError(t, s.InsertRedeployLog(ctx, entry))
	assert.NotZero(t, entry.ID, "InsertRedeployLog should populate ID")

	// Get log entries
	logs, err := s.GetRedeployLog(ctx, "add_agent")
	require.NoError(t, err)
	require.Len(t, logs, 1)
	assert.Equal(t, "LOCK_ACQUIRED", logs[0].Phase)
	assert.Equal(t, "in_progress", logs[0].Status)
	assert.Equal(t, "acquiring lock", logs[0].Details)
	assert.NotNil(t, logs[0].StartedAt)

	// Update log entry
	require.NoError(t, s.UpdateRedeployLog(ctx, entry.ID, "completed", ""))

	logs, err = s.GetRedeployLog(ctx, "add_agent")
	require.NoError(t, err)
	require.Len(t, logs, 1)
	assert.Equal(t, "completed", logs[0].Status)
	assert.NotNil(t, logs[0].CompletedAt)

	// Update with error
	entry2 := &RedeploymentLogEntry{
		Operation: "add_agent",
		AgentID:   "agent-1",
		Phase:     "CHAIN_STOPPED",
		Status:    "in_progress",
	}
	require.NoError(t, s.InsertRedeployLog(ctx, entry2))
	require.NoError(t, s.UpdateRedeployLog(ctx, entry2.ID, "failed", "chain stop timeout"))

	logs, err = s.GetRedeployLog(ctx, "add_agent")
	require.NoError(t, err)
	assert.Len(t, logs, 2)
	assert.Equal(t, "failed", logs[1].Status)
	assert.Equal(t, "chain stop timeout", logs[1].Error)
}

func TestListMemoriesWithAgentFilter(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Insert memories from different agents
	rec1 := testMemory("m1", "agent-alpha", "content from alpha", "general")
	require.NoError(t, s.InsertMemory(ctx, rec1))

	rec2 := testMemory("m2", "agent-alpha", "more content from alpha", "general")
	require.NoError(t, s.InsertMemory(ctx, rec2))

	rec3 := testMemory("m3", "agent-beta", "content from beta", "general")
	require.NoError(t, s.InsertMemory(ctx, rec3))

	// Filter by agent-alpha
	records, total, err := s.ListMemories(ctx, ListOptions{
		SubmittingAgent: "agent-alpha",
		Limit:           50,
	})
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Len(t, records, 2)
	for _, r := range records {
		assert.Equal(t, "agent-alpha", r.SubmittingAgent)
	}

	// Filter by agent-beta
	records, total, err = s.ListMemories(ctx, ListOptions{
		SubmittingAgent: "agent-beta",
		Limit:           50,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Len(t, records, 1)
	assert.Equal(t, "agent-beta", records[0].SubmittingAgent)

	// No filter — returns all
	records, total, err = s.ListMemories(ctx, ListOptions{Limit: 50})
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Len(t, records, 3)
}

// --- Key rotation tests ---

func TestRotateAgentKey(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Create an agent with known Ed25519 key
	agent := testAgent("old-agent-id", "Rotation Test Agent", "validator")
	agent.ValidatorPubkey = "oldpubkey"
	require.NoError(t, s.CreateAgent(ctx, agent))

	// Insert memories attributed to this agent
	rec1 := testMemory("m1", "old-agent-id", "memory one", "general")
	require.NoError(t, s.InsertMemory(ctx, rec1))
	rec2 := testMemory("m2", "old-agent-id", "memory two", "security")
	require.NoError(t, s.InsertMemory(ctx, rec2))
	rec3 := testMemory("m3", "other-agent", "memory three", "general")
	require.NoError(t, s.InsertMemory(ctx, rec3))

	// Rotate the key
	newAgentID, seed, err := s.RotateAgentKey(ctx, "old-agent-id")
	require.NoError(t, err)
	assert.NotEmpty(t, newAgentID)
	assert.NotEqual(t, "old-agent-id", newAgentID)
	assert.Len(t, seed, ed25519.SeedSize, "seed should be 32 bytes")

	// Verify newAgentID is hex-encoded Ed25519 public key
	pubBytes, err := hex.DecodeString(newAgentID)
	require.NoError(t, err)
	assert.Len(t, pubBytes, ed25519.PublicKeySize, "decoded agent_id should be 32 bytes (Ed25519 pubkey)")

	// Verify the seed reconstructs the same public key
	privKey := ed25519.NewKeyFromSeed(seed)
	pubKey := privKey.Public().(ed25519.PublicKey)
	assert.Equal(t, newAgentID, hex.EncodeToString(pubKey))

	// Verify the new agent exists with correct properties
	newAgent, err := s.GetAgent(ctx, newAgentID)
	require.NoError(t, err)
	assert.Equal(t, "Rotation Test Agent", newAgent.Name)
	assert.Equal(t, "validator", newAgent.Role)
	assert.NotEmpty(t, newAgent.ValidatorPubkey)
	assert.NotEqual(t, "oldpubkey", newAgent.ValidatorPubkey)

	// Verify old agent is marked as removed
	oldAgent, err := s.GetAgent(ctx, "old-agent-id")
	require.NoError(t, err)
	assert.Equal(t, "removed", oldAgent.Status)
	assert.NotNil(t, oldAgent.RemovedAt)

	// Verify memories were re-attributed to the new agent
	mem1, err := s.GetMemory(ctx, "m1")
	require.NoError(t, err)
	assert.Equal(t, newAgentID, mem1.SubmittingAgent)

	mem2, err := s.GetMemory(ctx, "m2")
	require.NoError(t, err)
	assert.Equal(t, newAgentID, mem2.SubmittingAgent)

	// Verify other agent's memory was NOT re-attributed
	mem3, err := s.GetMemory(ctx, "m3")
	require.NoError(t, err)
	assert.Equal(t, "other-agent", mem3.SubmittingAgent)
}

func TestRotateAgentKey_RemovedAgent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	agent := testAgent("removed-agent", "Removed Agent", "validator")
	require.NoError(t, s.CreateAgent(ctx, agent))
	require.NoError(t, s.RemoveAgent(ctx, "removed-agent"))

	_, _, err := s.RotateAgentKey(ctx, "removed-agent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "removed")
}

func TestRotateAgentKey_NonexistentAgent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	_, _, err := s.RotateAgentKey(ctx, "does-not-exist")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestRotateAgentKey_MemoryCountTransfers(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	agent := testAgent("rotate-mem-agent", "Memory Count Agent", "member")
	require.NoError(t, s.CreateAgent(ctx, agent))

	// Insert 3 memories
	for i := 0; i < 3; i++ {
		rec := testMemory(fmt.Sprintf("rm%d", i), "rotate-mem-agent", fmt.Sprintf("content %d", i), "general")
		require.NoError(t, s.InsertMemory(ctx, rec))
	}

	newAgentID, _, err := s.RotateAgentKey(ctx, "rotate-mem-agent")
	require.NoError(t, err)

	// New agent should now have 3 memories attributed
	newAgent, err := s.GetAgent(ctx, newAgentID)
	require.NoError(t, err)
	assert.Equal(t, 3, newAgent.MemoryCount)

	// Old agent should have 0 memories (they were re-attributed)
	oldAgent, err := s.GetAgent(ctx, "rotate-mem-agent")
	require.NoError(t, err)
	assert.Equal(t, 0, oldAgent.MemoryCount)
}
