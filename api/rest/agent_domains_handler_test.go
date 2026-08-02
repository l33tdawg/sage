package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
)

type indexedGrantAgentStore struct {
	*mockAgentStore
	grants []*store.AccessGrantEntry
	block  bool
}

func (s *indexedGrantAgentStore) GetActiveGrants(ctx context.Context, agentID string) ([]*store.AccessGrantEntry, error) {
	if s.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	out := make([]*store.AccessGrantEntry, 0, len(s.grants))
	for _, grant := range s.grants {
		if grant != nil && grant.GranteeID == agentID {
			out = append(out, grant)
		}
	}
	return out, nil
}

func (*indexedGrantAgentStore) InsertAccessGrant(context.Context, *store.AccessGrantEntry) error {
	return nil
}
func (*indexedGrantAgentStore) RevokeGrant(context.Context, string, string, int64) error {
	return nil
}
func (*indexedGrantAgentStore) InsertAccessRequest(context.Context, *store.AccessRequestEntry) error {
	return nil
}
func (*indexedGrantAgentStore) UpdateAccessRequestStatus(context.Context, string, string, int64) error {
	return nil
}
func (*indexedGrantAgentStore) InsertAccessLog(context.Context, *store.AccessLogEntry) error {
	return nil
}
func (*indexedGrantAgentStore) InsertDomain(context.Context, *store.DomainEntry) error { return nil }
func (*indexedGrantAgentStore) GetDomain(context.Context, string) (*store.DomainEntry, error) {
	return nil, nil
}

func TestAppV23ReadableDomainsAreBoundedCurrentCallerAuthority(t *testing.T) {
	srv, badger, memberID, _, outsiderID := setupAppV23RESTAccess(t)
	agents := srv.agentStore.(*mockAgentStore)
	// Historical association is only a candidate. The unrelated domain must
	// not be disclosed because current RBAC denies it.
	agents.domains[memberID] = []string{"member.home", "owner.home", "outsider.home"}
	require.NoError(t, badger.SetAccessGrant("direct.read", memberID, 1, 0, outsiderID))
	srv.agentStore = &indexedGrantAgentStore{
		mockAgentStore: agents,
		grants: []*store.AccessGrantEntry{{
			Domain: "direct.read", GranteeID: memberID, GranterID: outsiderID, Level: 1,
		}},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/agent/me/domains", nil)
	req = req.WithContext(middleware.WithAgentID(context.Background(), memberID))
	rec := httptest.NewRecorder()
	srv.handleGetAgentReadableDomains(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var response callerReadableDomainsResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.Equal(t, "bounded_policy_and_provenance", response.Scope)
	require.Contains(t, response.Domains, "member.home")
	require.Contains(t, response.Domains, "owner.home",
		"a current group peer's home domain is a useful scoped-recall target")
	require.Contains(t, response.Domains, "direct.read")
	require.NotContains(t, response.Domains, "outsider.home",
		"historical authorship must never be inferred as current access")
}

func TestAppV23ReadableDomainsReauthorizesIndexedGrantCandidates(t *testing.T) {
	srv, badger, memberID, _, outsiderID := setupAppV23RESTAccess(t)
	agents := srv.agentStore.(*mockAgentStore)
	expired := time.Now().Add(-time.Minute)
	srv.agentStore = &indexedGrantAgentStore{
		mockAgentStore: agents,
		grants: []*store.AccessGrantEntry{
			{Domain: "live.direct", GranteeID: memberID, GranterID: outsiderID, Level: 1},
			{Domain: "stale.revoked", GranteeID: memberID, GranterID: outsiderID, Level: 1},
			{Domain: "expired.direct", GranteeID: memberID, GranterID: outsiderID, Level: 1, ExpiresAt: &expired},
			{Domain: "other.agent", GranteeID: outsiderID, GranterID: outsiderID, Level: 1},
		},
	}
	require.NoError(t, badger.SetAccessGrant("live.direct", memberID, 1, 0, outsiderID))

	req := httptest.NewRequest(http.MethodGet, "/v1/agent/me/domains", nil)
	req = req.WithContext(middleware.WithAgentID(context.Background(), memberID))
	rec := httptest.NewRecorder()
	srv.handleGetAgentReadableDomains(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var response callerReadableDomainsResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.Contains(t, response.Domains, "live.direct")
	require.NotContains(t, response.Domains, "stale.revoked",
		"an indexed mirror candidate is not authority after consensus revocation")
	require.NotContains(t, response.Domains, "expired.direct")
	require.NotContains(t, response.Domains, "other.agent")
}

func TestAppV23ReadableDomainsIndexedProjectionStallIsBounded(t *testing.T) {
	srv, _, memberID, _, _ := setupAppV23RESTAccess(t)
	agents := srv.agentStore.(*mockAgentStore)
	srv.agentStore = &indexedGrantAgentStore{mockAgentStore: agents, block: true}

	req := httptest.NewRequest(http.MethodGet, "/v1/agent/me/domains", nil)
	req = req.WithContext(middleware.WithAgentID(context.Background(), memberID))
	rec := httptest.NewRecorder()
	started := time.Now()
	srv.handleGetAgentReadableDomains(rec, req)
	require.Less(t, time.Since(started), time.Second)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var response callerReadableDomainsResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.True(t, response.Truncated)
	require.Contains(t, response.Domains, "member.home",
		"consensus standing must survive an optional SQL projection stall")
}

func TestAppV23ReadableDomainsLargeHistoryStaysBounded(t *testing.T) {
	srv, _, memberID, _, _ := setupAppV23RESTAccess(t)
	agents := srv.agentStore.(*mockAgentStore)
	agents.domains[memberID] = make([]string, 10_000)
	for i := range agents.domains[memberID] {
		agents.domains[memberID][i] = fmt.Sprintf("historical-%05d", i)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/agent/me/domains", nil)
	req = req.WithContext(middleware.WithAgentID(context.Background(), memberID))
	rec := httptest.NewRecorder()
	started := time.Now()
	srv.handleGetAgentReadableDomains(rec, req)
	require.Less(t, time.Since(started), time.Second,
		"large historical association sets must not become an unbounded policy walk")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var response callerReadableDomainsResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.True(t, response.Truncated)
	require.LessOrEqual(t, len(response.Domains), callerReadableDomainLimit)
	require.Contains(t, response.Domains, "member.home")
}

func TestAppV23ReadableDomainsMarksConsensusCandidateCapTruncated(t *testing.T) {
	srv, badger, memberID, _, outsiderID := setupAppV23RESTAccess(t)
	agents := srv.agentStore.(*mockAgentStore)
	indexed := &indexedGrantAgentStore{mockAgentStore: agents}
	srv.agentStore = indexed
	// Exercise the grantee-indexed mirror path. The mirror is a candidate source
	// only; each row is still authorized against the exact consensus grant.
	for i := 0; i < callerReadableDomainCandidateScan+20; i++ {
		domain := fmt.Sprintf("granted-%03d", i)
		require.NoError(t, badger.SetAccessGrant(
			domain, memberID, 1, 0, outsiderID,
		))
		indexed.grants = append(indexed.grants, &store.AccessGrantEntry{
			Domain: domain, GranteeID: memberID, GranterID: outsiderID, Level: 1,
		})
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/agent/me/domains", nil)
	req = req.WithContext(middleware.WithAgentID(context.Background(), memberID))
	rec := httptest.NewRecorder()
	srv.handleGetAgentReadableDomains(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var response callerReadableDomainsResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.True(t, response.Truncated,
		"dropping consensus-derived candidates at the cap must be explicit")
	require.LessOrEqual(t, len(response.Domains), callerReadableDomainLimit)
	require.Contains(t, response.Domains, "member.home")
}
