package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

// staleMirrorOrgStore implements just enough of store.OrgStore to stand
// in for an off-chain mirror that disagrees with Badger. After a chain
// reset / Badger wipe where the mirror tables weren't dropped, the
// mirror keeps org + member rows the chain no longer recognises. The
// REST read paths must defer to Badger so callers don't infer
// org-admin status they can't actually exercise (would cause Code-54
// rejection on add_org_member / set_clearance / etc.).
type staleMirrorOrgStore struct {
	org     *store.OrgEntry
	members []*store.OrgMemberEntry
}

func (m *staleMirrorOrgStore) GetOrg(_ context.Context, orgID string) (*store.OrgEntry, error) {
	if m.org != nil && m.org.OrgID == orgID {
		return m.org, nil
	}
	return nil, fmt.Errorf("org not found: %s", orgID)
}
func (m *staleMirrorOrgStore) GetOrgMembers(_ context.Context, _ string) ([]*store.OrgMemberEntry, error) {
	return m.members, nil
}

func (m *staleMirrorOrgStore) InsertOrg(_ context.Context, _ *store.OrgEntry) error { return nil }
func (m *staleMirrorOrgStore) InsertOrgMember(_ context.Context, _ *store.OrgMemberEntry) error {
	return nil
}
func (m *staleMirrorOrgStore) RemoveOrgMember(_ context.Context, _, _ string, _ int64) error {
	return nil
}
func (m *staleMirrorOrgStore) UpdateMemberClearance(_ context.Context, _, _ string, _ store.ClearanceLevel) error {
	return nil
}
func (m *staleMirrorOrgStore) InsertFederation(_ context.Context, _ *store.FederationEntry) error {
	return nil
}
func (m *staleMirrorOrgStore) GetFederation(_ context.Context, _ string) (*store.FederationEntry, error) {
	return nil, fmt.Errorf("not implemented")
}
func (m *staleMirrorOrgStore) ApproveFederation(_ context.Context, _ string, _ int64) error {
	return nil
}
func (m *staleMirrorOrgStore) RevokeFederation(_ context.Context, _ string, _ int64) error {
	return nil
}
func (m *staleMirrorOrgStore) GetActiveFederations(_ context.Context, _ string) ([]*store.FederationEntry, error) {
	return nil, nil
}
func (m *staleMirrorOrgStore) UpdateMemoryClassification(_ context.Context, _ string, _ store.ClearanceLevel) error {
	return nil
}
func (m *staleMirrorOrgStore) InsertDept(_ context.Context, _ *store.DeptEntry) error { return nil }
func (m *staleMirrorOrgStore) GetDept(_ context.Context, _, _ string) (*store.DeptEntry, error) {
	return nil, fmt.Errorf("not implemented")
}
func (m *staleMirrorOrgStore) GetOrgDepts(_ context.Context, _ string) ([]*store.DeptEntry, error) {
	return nil, nil
}
func (m *staleMirrorOrgStore) InsertDeptMember(_ context.Context, _ *store.DeptMemberEntry) error {
	return nil
}
func (m *staleMirrorOrgStore) RemoveDeptMember(_ context.Context, _, _, _ string, _ int64) error {
	return nil
}
func (m *staleMirrorOrgStore) GetDeptMembers(_ context.Context, _, _ string) ([]*store.DeptMemberEntry, error) {
	return nil, nil
}
func (m *staleMirrorOrgStore) UpdateDeptMemberClearance(_ context.Context, _, _, _ string, _ store.ClearanceLevel) error {
	return nil
}

const testOrgID = "3b90d2ebad3987f7e224c847af881d67"

// TestGetOrg_BadgerIsAuthoritative_StaleMirrorAdminIgnored: mirror claims
// agent X is org admin, but Badger says agent Y. Response must report Y.
// Otherwise a probe based on this endpoint hands add_org_member a false
// sense of admin and the chain Code-54s it.
func TestGetOrg_BadgerIsAuthoritative_StaleMirrorAdminIgnored(t *testing.T) {
	srv, _, bs, _ := newRBACTestServer(t)

	chainAdmin := "aaaa00000000000000000000000000000000000000000000000000000000aaaa"
	staleAdmin := "bbbb00000000000000000000000000000000000000000000000000000000bbbb"

	require.NoError(t, bs.RegisterOrg(testOrgID, "levelup", "the LU tenant", chainAdmin, 100))

	srv.orgStore = &staleMirrorOrgStore{
		org: &store.OrgEntry{
			OrgID:        testOrgID,
			Name:         "levelup",
			AdminAgentID: staleAdmin,
			Description:  "Stale mirror description",
			CreatedAt:    time.Unix(1700000000, 0),
		},
	}

	req, _ := signedRequest(t, http.MethodGet, "/v1/org/"+testOrgID, nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())

	var got store.OrgEntry
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &got))
	assert.Equal(t, chainAdmin, got.AdminAgentID, "admin must come from Badger, not stale mirror")
	assert.Equal(t, "levelup", got.Name)
	assert.Equal(t, int64(100), got.CreatedHeight)
}

// TestGetOrg_BadgerMissing_ReturnsNotFound: this is the exact levelup
// scenario from this session — Badger wiped on a pre-v7.5.5 reset, mirror
// retains the org row. Endpoint must 404 so the bootstrap probe re-fires
// register_org instead of believing the chain knows about the org.
func TestGetOrg_BadgerMissing_ReturnsNotFound(t *testing.T) {
	srv, _, _, _ := newRBACTestServer(t)

	srv.orgStore = &staleMirrorOrgStore{
		org: &store.OrgEntry{
			OrgID:        testOrgID,
			Name:         "levelup",
			AdminAgentID: "ccccc000000000000000000000000000000000000000000000000000000cccc",
		},
	}

	req, _ := signedRequest(t, http.MethodGet, "/v1/org/"+testOrgID, nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code, "stale mirror org must not surface when Badger has no record")
}

// TestGetOrg_NoMirror_StillWorks: when the mirror isn't wired (test
// fixtures, minimal deployments), the endpoint still returns the
// Badger-sourced fields without panicking on the nil orgStore.
func TestGetOrg_NoMirror_StillWorks(t *testing.T) {
	srv, _, bs, _ := newRBACTestServer(t)
	srv.orgStore = nil

	chainAdmin := "aaaa00000000000000000000000000000000000000000000000000000000aaaa"
	require.NoError(t, bs.RegisterOrg(testOrgID, "chain-only", "", chainAdmin, 7))

	req, _ := signedRequest(t, http.MethodGet, "/v1/org/"+testOrgID, nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	var got store.OrgEntry
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &got))
	assert.Equal(t, chainAdmin, got.AdminAgentID)
	assert.Equal(t, int64(7), got.CreatedHeight)
}

// TestListOrgMembers_FiltersStaleMirrorMembers: only the on-chain member
// survives the response; the stale-mirror-only member is dropped.
func TestListOrgMembers_FiltersStaleMirrorMembers(t *testing.T) {
	srv, _, bs, _ := newRBACTestServer(t)

	chainAdmin := "aaaa00000000000000000000000000000000000000000000000000000000aaaa"
	chainMember := "dddd00000000000000000000000000000000000000000000000000000000dddd"
	staleMember := "eeee00000000000000000000000000000000000000000000000000000000eeee"

	require.NoError(t, bs.RegisterOrg(testOrgID, "levelup", "", chainAdmin, 1))
	require.NoError(t, bs.AddOrgMember(testOrgID, chainAdmin, 4, "admin", 1))
	require.NoError(t, bs.AddOrgMember(testOrgID, chainMember, 2, "member", 2))

	srv.orgStore = &staleMirrorOrgStore{
		members: []*store.OrgMemberEntry{
			{OrgID: testOrgID, AgentID: chainAdmin, Role: "admin", Clearance: 4, CreatedHeight: 1, CreatedAt: time.Unix(1700000000, 0)},
			{OrgID: testOrgID, AgentID: chainMember, Role: "member", Clearance: 2, CreatedHeight: 2, CreatedAt: time.Unix(1700000100, 0)},
			{OrgID: testOrgID, AgentID: staleMember, Role: "admin", Clearance: 4, CreatedHeight: 999, CreatedAt: time.Unix(1700000200, 0)},
		},
	}

	req, _ := signedRequest(t, http.MethodGet, "/v1/org/"+testOrgID+"/members", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())

	var got []*store.OrgMemberEntry
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &got))
	require.Len(t, got, 2, "stale mirror-only member must be filtered out")

	seen := map[string]bool{}
	for _, m := range got {
		seen[m.AgentID] = true
	}
	assert.True(t, seen[chainAdmin], "chain admin must appear")
	assert.True(t, seen[chainMember], "chain member must appear")
	assert.False(t, seen[staleMember], "stale member absent from chain must NOT appear")
}
