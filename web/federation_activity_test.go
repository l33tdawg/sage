package web

import (
	"bytes"
	"context"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

type activityTestStore struct {
	store.MemoryStore
	calls int
}

func (s *activityTestStore) RecentFederationActivity(context.Context) ([]store.FederationActivity, error) {
	s.calls++
	return []store.FederationActivity{{ID: "visible", ChainID: "active", State: "delivered"}, {ID: "hidden", ChainID: "revoked", State: "failed"}}, nil
}
func TestFederationActivityOperatorAndActiveTrustBoundary(t *testing.T) {
	h, _ := newTestHandler(t)
	fake := &activityTestStore{MemoryStore: h.store}
	h.store = fake
	bs, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	defer bs.CloseBadger()
	h.BadgerStore = bs
	require.NoError(t, bs.SetCrossFed("active", "https://peer:8444", bytes.Repeat([]byte{1}, 32), 4, 0, nil, nil, "active"))
	req := httptest.NewRequest(http.MethodGet, "http://localhost/v1/dashboard/federation/activity", nil)
	w := httptest.NewRecorder()
	h.handleFedActivity(w, req)
	require.Equal(t, http.StatusForbidden, w.Code)
	require.Zero(t, fake.calls)
	markLocalCEREBRUM(h, req)
	w = httptest.NewRecorder()
	h.handleFedActivity(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), "visible")
	require.NotContains(t, w.Body.String(), "hidden")
	require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
	ctx, cancel := context.WithCancel(req.Context())
	cancel()
	req = req.WithContext(ctx)
	req.Header.Set("Accept", "text/event-stream")
	w = httptest.NewRecorder()
	h.handleFedActivity(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), "event: federation_activity")
	require.NotContains(t, w.Body.String(), "hidden")
	require.Zero(t, h.SSE.ClientCount())
}
