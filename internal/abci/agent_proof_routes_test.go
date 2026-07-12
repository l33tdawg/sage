package abci

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tlsca"
	"github.com/l33tdawg/sage/internal/tx"
)

func canonicalAgentRequest(t *testing.T, method, path string, body any) []byte {
	t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		require.NoError(t, err)
	}
	return append([]byte(method+" "+path+"\n"), raw...)
}

func TestAppV17DelegatedRESTRouteMatrix(t *testing.T) { //nolint:maintidx // protocol matrix intentionally exhaustive
	app := setupTestApp(t)
	agent := newAgentKey(t)
	require.NoError(t, app.badgerStore.RegisterOrg("org-home", "Home", "", agent.id, 1))
	require.NoError(t, app.badgerStore.AddOrgMember("org-home", agent.id, 4, "admin", 1))

	contentHash := sha256.Sum256([]byte("hello"))
	// The node owns the active embedding space and may regenerate a vector that
	// differs from the stale client hint in the signed REST body.
	embeddingHash := sha256.Sum256([]byte("node-authoritative-vector"))
	orgHash := sha256.Sum256([]byte(agent.id + "Acme"))
	deptHash := sha256.Sum256([]byte("org-home" + "Security"))

	coCommitHash := sha256.Sum256([]byte("joint"))
	coCommitNonce := []byte{1, 2}
	coCommit := &tx.CoCommitSubmit{
		SchemaVersion:   1,
		ContentHash:     coCommitHash[:],
		MemoryType:      tx.MemoryTypeFact,
		Domain:          "research",
		Classification:  tx.ClearanceConfidential,
		ConfidenceScore: 0.8,
		CreatedAtUnix:   123,
		AgreementNonce:  coCommitNonce,
	}
	coCommit.SharedID = tx.ComputeSharedID(tx.CoreHashOf(coCommit), nil, coCommitNonce)

	ca, _, err := tlsca.GenerateCA("remote-chain")
	require.NoError(t, err)
	caPEM := tlsca.EncodeCertPEM(ca)
	caPin := sha256.Sum256(ca.RawSubjectPublicKeyInfo)

	tests := []struct {
		name    string
		request []byte
		parsed  *tx.ParsedTx
	}{
		{
			name: "memory submit",
			request: canonicalAgentRequest(t, "POST", "/v1/memory/submit", map[string]any{
				"content": "hello", "memory_type": "fact", "domain_tag": "research",
				"confidence_score": 0.9, "classification": 2, "embedding": []float32{1.25},
				"parent_hash": "parent", "task_status": "planned",
			}),
			parsed: &tx.ParsedTx{Type: tx.TxTypeMemorySubmit, MemorySubmit: &tx.MemorySubmit{
				MemoryID: "00000000-0000-4000-8000-000000000000", ContentHash: contentHash[:], EmbeddingHash: embeddingHash[:],
				MemoryType: tx.MemoryTypeFact, DomainTag: "research", ConfidenceScore: 0.9, Content: "hello",
				ParentHash: "parent", Classification: tx.ClearanceConfidential, TaskStatus: "planned",
			}},
		},
		{
			name:    "memory challenge",
			request: canonicalAgentRequest(t, "POST", "/v1/memory/m1/challenge", map[string]any{"reason": "wrong", "evidence": "source"}),
			parsed:  &tx.ParsedTx{Type: tx.TxTypeMemoryChallenge, MemoryChallenge: &tx.MemoryChallenge{MemoryID: "m1", Reason: "wrong", Evidence: "source"}},
		},
		{
			name:    "memory forget default reason",
			request: canonicalAgentRequest(t, "POST", "/v1/memory/m2/forget", map[string]any{}),
			parsed:  &tx.ParsedTx{Type: tx.TxTypeMemoryChallenge, MemoryChallenge: &tx.MemoryChallenge{MemoryID: "m2", Reason: "deprecated by user"}},
		},
		{
			name:    "memory reinstate",
			request: canonicalAgentRequest(t, "POST", "/v1/memory/m3/reinstate", map[string]any{"reason": "withdraw"}),
			parsed:  &tx.ParsedTx{Type: tx.TxTypeMemoryReinstate, MemoryReinstate: &tx.MemoryReinstate{MemoryID: "m3", Reason: "withdraw"}},
		},
		{
			name:    "memory corroborate",
			request: canonicalAgentRequest(t, "POST", "/v1/memory/m4/corroborate", map[string]any{"evidence": "source"}),
			parsed:  &tx.ParsedTx{Type: tx.TxTypeMemoryCorroborate, MemoryCorroborate: &tx.MemoryCorroborate{MemoryID: "m4", Evidence: "source"}},
		},
		{
			name:    "access request default level",
			request: canonicalAgentRequest(t, "POST", "/v1/access/request", map[string]any{"target_domain": "research", "justification": "work"}),
			parsed:  &tx.ParsedTx{Type: tx.TxTypeAccessRequest, AccessRequest: &tx.AccessRequest{RequesterID: agent.id, TargetDomain: "research", Justification: "work", RequestedLevel: 1}},
		},
		{
			name:    "access grant",
			request: canonicalAgentRequest(t, "POST", "/v1/access/grant", map[string]any{"grantee_id": "agent-b", "domain": "research", "level": 2, "expires_at": int64(99), "request_id": "r1"}),
			parsed:  &tx.ParsedTx{Type: tx.TxTypeAccessGrant, AccessGrant: &tx.AccessGrant{GranterID: agent.id, GranteeID: "agent-b", Domain: "research", Level: 2, ExpiresAt: 99, RequestID: "r1"}},
		},
		{
			name:    "access revoke",
			request: canonicalAgentRequest(t, "POST", "/v1/access/revoke", map[string]any{"grantee_id": "agent-b", "domain": "research", "reason": "done"}),
			parsed:  &tx.ParsedTx{Type: tx.TxTypeAccessRevoke, AccessRevoke: &tx.AccessRevoke{RevokerID: agent.id, GranteeID: "agent-b", Domain: "research", Reason: "done"}},
		},
		{
			name:    "domain register",
			request: canonicalAgentRequest(t, "POST", "/v1/domain/register", map[string]any{"name": "security", "description": "d", "parent": "root"}),
			parsed:  &tx.ParsedTx{Type: tx.TxTypeDomainRegister, DomainRegister: &tx.DomainRegister{DomainName: "security", OwnerAgentID: agent.id, Description: "d", ParentDomain: "root"}},
		},
		{
			name:    "org register",
			request: canonicalAgentRequest(t, "POST", "/v1/org/register", map[string]any{"name": "Acme", "description": "d"}),
			parsed:  &tx.ParsedTx{Type: tx.TxTypeOrgRegister, OrgRegister: &tx.OrgRegister{OrgID: hex.EncodeToString(orgHash[:16]), Name: "Acme", Description: "d", AdminAgent: agent.id}},
		},
		{
			name:    "org add member defaults",
			request: canonicalAgentRequest(t, "POST", "/v1/org/org-home/member", map[string]any{"agent_id": "agent-b"}),
			parsed:  &tx.ParsedTx{Type: tx.TxTypeOrgAddMember, OrgAddMember: &tx.OrgAddMember{OrgID: "org-home", AgentID: "agent-b", Clearance: tx.ClearanceInternal, Role: "member"}},
		},
		{
			name:    "org remove member",
			request: canonicalAgentRequest(t, "DELETE", "/v1/org/org-home/member/agent-b", nil),
			parsed:  &tx.ParsedTx{Type: tx.TxTypeOrgRemoveMember, OrgRemoveMember: &tx.OrgRemoveMember{OrgID: "org-home", AgentID: "agent-b"}},
		},
		{
			name:    "org clearance",
			request: canonicalAgentRequest(t, "POST", "/v1/org/org-home/clearance", map[string]any{"agent_id": "agent-b", "clearance": 3}),
			parsed:  &tx.ParsedTx{Type: tx.TxTypeOrgSetClearance, OrgSetClearance: &tx.OrgSetClearance{OrgID: "org-home", AgentID: "agent-b", Clearance: tx.ClearanceSecret}},
		},
		{
			name:    "federation propose",
			request: canonicalAgentRequest(t, "POST", "/v1/federation/propose", map[string]any{"target_org_id": "org-away", "allowed_domains": []string{"*"}}),
			parsed:  &tx.ParsedTx{Type: tx.TxTypeFederationPropose, FederationPropose: &tx.FederationPropose{ProposerOrgID: "org-home", TargetOrgID: "org-away", AllowedDomains: []string{"*"}, MaxClearance: tx.ClearanceConfidential}},
		},
		{
			name:    "federation approve",
			request: canonicalAgentRequest(t, "POST", "/v1/federation/fed-1/approve", nil),
			parsed:  &tx.ParsedTx{Type: tx.TxTypeFederationApprove, FederationApprove: &tx.FederationApprove{FederationID: "fed-1", ApproverOrgID: "org-home"}},
		},
		{
			name:    "federation revoke",
			request: canonicalAgentRequest(t, "POST", "/v1/federation/fed-1/revoke", map[string]any{"reason": "done"}),
			parsed:  &tx.ParsedTx{Type: tx.TxTypeFederationRevoke, FederationRevoke: &tx.FederationRevoke{FederationID: "fed-1", RevokerOrgID: "org-home", Reason: "done"}},
		},
		{
			name:    "department register",
			request: canonicalAgentRequest(t, "POST", "/v1/org/org-home/dept", map[string]any{"name": "Security", "description": "d", "parent_dept": "root"}),
			parsed:  &tx.ParsedTx{Type: tx.TxTypeDeptRegister, DeptRegister: &tx.DeptRegister{OrgID: "org-home", DeptID: hex.EncodeToString(deptHash[:8]), DeptName: "Security", Description: "d", ParentDept: "root"}},
		},
		{
			name:    "department add member",
			request: canonicalAgentRequest(t, "POST", "/v1/org/org-home/dept/sec/member", map[string]any{"agent_id": "agent-b", "clearance": 0, "role": "observer"}),
			parsed:  &tx.ParsedTx{Type: tx.TxTypeDeptAddMember, DeptAddMember: &tx.DeptAddMember{OrgID: "org-home", DeptID: "sec", AgentID: "agent-b", Clearance: tx.ClearancePublic, Role: "observer"}},
		},
		{
			name:    "department remove member",
			request: canonicalAgentRequest(t, "DELETE", "/v1/org/org-home/dept/sec/member/agent-b", nil),
			parsed:  &tx.ParsedTx{Type: tx.TxTypeDeptRemoveMember, DeptRemoveMember: &tx.DeptRemoveMember{OrgID: "org-home", DeptID: "sec", AgentID: "agent-b"}},
		},
		{
			name:    "agent register role default",
			request: canonicalAgentRequest(t, "POST", "/v1/agent/register", map[string]any{"name": "Agent", "boot_bio": "bio", "provider": "codex", "p2p_address": "p2p"}),
			parsed:  &tx.ParsedTx{Type: tx.TxTypeAgentRegister, AgentRegister: &tx.AgentRegister{AgentID: agent.id, Name: "Agent", Role: "member", BootBio: "bio", Provider: "codex", P2PAddress: "p2p"}},
		},
		{
			name:    "agent update",
			request: canonicalAgentRequest(t, "PUT", "/v1/agent/update", map[string]any{"name": "Renamed", "boot_bio": "bio"}),
			parsed:  &tx.ParsedTx{Type: tx.TxTypeAgentUpdate, AgentUpdateTx: &tx.AgentUpdate{AgentID: agent.id, Name: "Renamed", BootBio: "bio"}},
		},
		{
			name:    "agent permission defaults",
			request: canonicalAgentRequest(t, "PUT", "/v1/agent/new-agent/permission", map[string]any{"visible_agents": "*"}),
			parsed:  &tx.ParsedTx{Type: tx.TxTypeAgentSetPermission, AgentSetPermission: &tx.AgentSetPermission{AgentID: "new-agent", Clearance: 1, VisibleAgents: "*"}},
		},
		{
			name:    "domain reassign",
			request: canonicalAgentRequest(t, "POST", "/v1/domain/reassign", map[string]any{"domain": "security", "new_owner_id": strings.Repeat("ab", 32), "parent_domain": "root", "proposal_id": "ab", "open_to_shared": true}),
			parsed:  &tx.ParsedTx{Type: tx.TxTypeDomainReassign, DomainReassign: &tx.DomainReassign{Domain: "security", NewOwnerID: strings.Repeat("ab", 32), ParentDomain: "root", ProposalID: "ab", OpenToShared: true}},
		},
		{
			name: "co-commit submit",
			request: canonicalAgentRequest(t, "POST", "/v1/cocommit/submit", map[string]any{
				"content": "joint", "memory_type": "fact", "domain_tag": "research", "classification": 2,
				"confidence_score": 0.8, "created_at_unix": int64(123), "agreement_nonce": "0102", "coauthors": []any{},
			}),
			parsed: &tx.ParsedTx{Type: tx.TxTypeCoCommitSubmit, CoCommitSubmit: coCommit},
		},
		{
			name: "cross-federation set",
			request: canonicalAgentRequest(t, "POST", "/v1/federation/cross", map[string]any{
				"remote_chain_id": "remote-chain", "endpoint": "https://remote:8444", "remote_ca_pem": caPEM,
				"max_clearance": 2, "allowed_domains": []string{"research"}, "allowed_depts": []string{"*"}, "expires_at": int64(999),
			}),
			parsed: &tx.ParsedTx{Type: tx.TxTypeCrossFedSet, CrossFedTerms: &tx.CrossFedTerms{
				RemoteChainID: "remote-chain", Endpoint: "https://remote:8444", PeerPubKey: caPin[:], MaxClearance: tx.ClearanceConfidential,
				AllowedDomains: []string{"research"}, AllowedDepts: []string{"*"}, ExpiresAt: 999, Status: "active",
			}},
		},
		{
			name:    "cross-federation revoke",
			request: canonicalAgentRequest(t, "POST", "/v1/federation/cross/remote-chain/revoke", map[string]any{"reason": "done"}),
			parsed:  &tx.ParsedTx{Type: tx.TxTypeCrossFedRevoke, CrossFedRevoke: &tx.CrossFedRevoke{RemoteChainID: "remote-chain", Reason: "done"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := parseSignedAgentRequest(tc.request)
			require.NoError(t, err)
			require.NoError(t, app.verifySignedAgentAction(tc.parsed, agent.id, req))
		})
	}
}

func TestAppV17DelegatedMemorySubmitAllowsNodeRegeneratedEmbedding(t *testing.T) {
	app := setupTestApp(t)
	contentHash := sha256.Sum256([]byte("a longer turn observation"))
	nodeEmbeddingHash := sha256.Sum256([]byte("vector regenerated by active provider"))
	request := canonicalAgentRequest(t, "POST", "/v1/memory/submit", map[string]any{
		"content": "a longer turn observation", "memory_type": "observation",
		"domain_tag": "sage-mcp-reliability", "confidence_score": 0.8,
		"embedding": []float32{1.25},
	})
	req, err := parseSignedAgentRequest(request)
	require.NoError(t, err)

	actual := &tx.ParsedTx{Type: tx.TxTypeMemorySubmit, MemorySubmit: &tx.MemorySubmit{
		MemoryID: "00000000-0000-4000-8000-000000000000", ContentHash: contentHash[:],
		EmbeddingHash: nodeEmbeddingHash[:], MemoryType: tx.MemoryTypeObservation,
		DomainTag: "sage-mcp-reliability", ConfidenceScore: 0.8,
		Content: "a longer turn observation",
	}}
	require.NoError(t, app.verifySignedAgentAction(actual, "delegated-agent", req))

	actual.MemorySubmit.EmbeddingHash = []byte("not-a-sha256")
	require.ErrorContains(t, app.verifySignedAgentAction(actual, "delegated-agent", req), "invalid node-generated embedding hash")
}

func TestAppV17DelegatedNonRESTRoutesFailClosed(t *testing.T) {
	app := setupTestApp(t)
	req, err := parseSignedAgentRequest([]byte("POST /v1/domain/register\n{\"name\":\"x\"}"))
	require.NoError(t, err)

	for _, parsed := range []*tx.ParsedTx{
		{Type: tx.TxTypeMemoryReassign, MemoryReassign: &tx.MemoryReassign{SourceAgentID: "a", TargetAgentID: "b"}},
		{Type: tx.TxTypeUpgradePropose, UpgradePropose: &tx.UpgradePropose{Name: "app-v17", TargetAppVersion: 17}},
		{Type: tx.TxTypeUpgradeCancel, UpgradeCancel: &tx.UpgradeCancel{Name: "app-v17"}},
		{Type: tx.TxTypeUpgradeRevert, UpgradeRevert: &tx.UpgradeRevert{Name: "app-v17", TargetAppVersion: 16}},
	} {
		t.Run(fmt.Sprintf("type-%d", parsed.Type), func(t *testing.T) {
			require.ErrorContains(t, app.verifySignedAgentAction(parsed, "agent", req), "no delegated REST action")
		})
	}
}
