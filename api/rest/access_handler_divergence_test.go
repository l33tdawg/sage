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

// staleMirrorAccessStore implements just enough of store.AccessStore to
// stand in for an off-chain mirror that disagrees with Badger. Background:
// after a chain reset that doesn't drop the accessStore tables, the mirror
// retains domain rows and grant rows the chain no longer recognises. The
// REST read paths must defer to Badger so callers don't infer ownership
// or grants they can't actually exercise (would cause Code-34 rejection
// on the next access tx).
type staleMirrorAccessStore struct {
	domain *store.DomainEntry
	grants []*store.AccessGrantEntry
}

func (m *staleMirrorAccessStore) GetDomain(_ context.Context, name string) (*store.DomainEntry, error) {
	if m.domain != nil && m.domain.DomainName == name {
		return m.domain, nil
	}
	return nil, fmt.Errorf("domain not found: %s", name)
}

func (m *staleMirrorAccessStore) InsertDomain(_ context.Context, _ *store.DomainEntry) error {
	return nil
}

func (m *staleMirrorAccessStore) InsertAccessGrant(_ context.Context, _ *store.AccessGrantEntry) error {
	return nil
}

func (m *staleMirrorAccessStore) GetActiveGrants(_ context.Context, _ string) ([]*store.AccessGrantEntry, error) {
	return m.grants, nil
}

func (m *staleMirrorAccessStore) RevokeGrant(_ context.Context, _, _ string, _ int64) error {
	return nil
}

func (m *staleMirrorAccessStore) InsertAccessRequest(_ context.Context, _ *store.AccessRequestEntry) error {
	return nil
}

func (m *staleMirrorAccessStore) UpdateAccessRequestStatus(_ context.Context, _, _ string, _ int64) error {
	return nil
}

func (m *staleMirrorAccessStore) InsertAccessLog(_ context.Context, _ *store.AccessLogEntry) error {
	return nil
}

// TestGetDomain_BadgerIsAuthoritative_StaleMirrorOwnerIgnored: the mirror
// claims agent B owns pipeline.failures.boot2root but Badger says it's
// agent A. GET must report A — otherwise a probe based on this endpoint
// hands grant_access a false sense of ownership.
func TestGetDomain_BadgerIsAuthoritative_StaleMirrorOwnerIgnored(t *testing.T) {
	srv, _, bs, _ := newRBACTestServer(t)

	chainOwner := "0000000000000000000000000000000000000000000000000000000000000aaa"
	staleOwner := "0000000000000000000000000000000000000000000000000000000000000bbb"

	require.NoError(t, bs.RegisterDomain("pipeline.failures.boot2root", chainOwner, "pipeline.failures", 100))

	srv.accessStore = &staleMirrorAccessStore{
		domain: &store.DomainEntry{
			DomainName:   "pipeline.failures.boot2root",
			OwnerAgentID: staleOwner,
			ParentDomain: "pipeline.failures",
			Description:  "Mirror description from pre-reset chain",
			CreatedAt:    time.Unix(1700000000, 0),
		},
	}

	req, _ := signedRequest(t, http.MethodGet, "/v1/domain/pipeline.failures.boot2root", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())

	var got store.DomainEntry
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &got))
	assert.Equal(t, chainOwner, got.OwnerAgentID, "owner must come from Badger, not stale mirror")
	assert.Equal(t, "pipeline.failures", got.ParentDomain)
	assert.Equal(t, int64(100), got.CreatedHeight)
	assert.Equal(t, "Mirror description from pre-reset chain", got.Description, "description still sourced from mirror — Badger doesn't store it")
}

// TestGetDomain_BadgerMissing_ReturnsNotFound: mirror has a row, chain
// doesn't — endpoint must 404, not surface the orphan.
func TestGetDomain_BadgerMissing_ReturnsNotFound(t *testing.T) {
	srv, _, _, _ := newRBACTestServer(t)

	srv.accessStore = &staleMirrorAccessStore{
		domain: &store.DomainEntry{
			DomainName:   "ghost.domain",
			OwnerAgentID: "0000000000000000000000000000000000000000000000000000000000000ccc",
		},
	}

	req, _ := signedRequest(t, http.MethodGet, "/v1/domain/ghost.domain", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code, "stale mirror entries must not surface when Badger has no record")
}

// TestGetDomain_NoMirror_StillWorks: the accessStore enrichment is optional
// — when memStore doesn't satisfy AccessStore (common in minimal deployments
// or test fixtures), the endpoint still returns Badger-sourced owner/parent.
func TestGetDomain_NoMirror_StillWorks(t *testing.T) {
	srv, _, bs, _ := newRBACTestServer(t)
	srv.accessStore = nil

	chainOwner := "0000000000000000000000000000000000000000000000000000000000000aaa"
	require.NoError(t, bs.RegisterDomain("chain.only", chainOwner, "", 7))

	req, _ := signedRequest(t, http.MethodGet, "/v1/domain/chain.only", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())

	var got store.DomainEntry
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &got))
	assert.Equal(t, chainOwner, got.OwnerAgentID)
	assert.Equal(t, int64(7), got.CreatedHeight)
	assert.Empty(t, got.Description, "no mirror → no description, no panic")
}

// TestListGrants_FiltersStaleMirrorGrants: only the on-chain grant survives
// the response; the stale-domain mirror row is dropped.
func TestListGrants_FiltersStaleMirrorGrants(t *testing.T) {
	srv, _, bs, _ := newRBACTestServer(t)

	liveDomain := "live.domain"
	staleDomain := "stale.domain"
	grantee := "0000000000000000000000000000000000000000000000000000000000000ddd"
	granter := "0000000000000000000000000000000000000000000000000000000000000eee"

	require.NoError(t, bs.SetAccessGrant(liveDomain, grantee, 1, 0, granter))

	srv.accessStore = &staleMirrorAccessStore{
		grants: []*store.AccessGrantEntry{
			{Domain: liveDomain, GranteeID: grantee, GranterID: granter, Level: 1, CreatedHeight: 1},
			{Domain: staleDomain, GranteeID: grantee, GranterID: granter, Level: 1, CreatedHeight: 1},
		},
	}

	req, _ := signedRequest(t, http.MethodGet, "/v1/access/grants/"+grantee, nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())

	var got []store.AccessGrantEntry
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &got))
	require.Len(t, got, 1, "stale mirror grant absent from Badger must be filtered out")
	assert.Equal(t, liveDomain, got[0].Domain)
}
