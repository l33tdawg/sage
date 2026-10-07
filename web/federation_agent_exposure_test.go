package web

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/require"
)

type agentExposureContractDriver struct {
	FederationJoinDriver
	exposure   *store.FederatedAgentExposure
	getErr     error
	setErr     error
	getCalls   int
	setCalls   int
	lastChain  string
	lastMode   string
	lastAgents []string
	lastExpect int64
}

func (d *agentExposureContractDriver) GetFederatedAgentExposure(
	_ context.Context, chain string,
) (*store.FederatedAgentExposure, error) {
	d.getCalls++
	d.lastChain = chain
	if d.getErr != nil {
		return nil, d.getErr
	}
	if d.exposure != nil {
		return d.exposure, nil
	}
	return &store.FederatedAgentExposure{
		RemoteChainID: chain,
		Mode:          store.FederatedAgentExposureModeAll,
		AgentIDs:      []string{},
	}, nil
}

func (d *agentExposureContractDriver) SetFederatedAgentExposure(
	_ context.Context, chain, mode string, agentIDs []string, expectedRevision int64,
) (*store.FederatedAgentExposure, error) {
	d.setCalls++
	d.lastChain, d.lastMode, d.lastExpect = chain, mode, expectedRevision
	d.lastAgents = append([]string(nil), agentIDs...)
	if d.setErr != nil {
		return nil, d.setErr
	}
	return &store.FederatedAgentExposure{
		RemoteChainID: chain,
		Mode:          mode,
		AgentIDs:      append([]string(nil), agentIDs...),
		Revision:      expectedRevision + 1,
		Configured:    true,
	}, nil
}

func newFederatedExposureBaseFixture(t *testing.T) (*DashboardHandler, *store.SQLiteStore, *store.BadgerStore) {
	t.Helper()
	ctx := context.Background()
	ss, err := store.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "dashboard-exposure.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ss.Close() })
	bs, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })

	operatorID := strings.Repeat("a", 64)
	require.NoError(t, bs.RegisterAgent(operatorID, "operator", "admin", "", "test", "", 1))
	require.NoError(t, bs.SetCrossFed(
		"chain-peer", "https://peer:8444", bytes.Repeat([]byte{0x44}, 32),
		4, 0, nil, nil, "active",
	))
	h := NewDashboardHandler(ss, "test")
	h.BadgerStore = bs
	h.NodeOperatorAgentID = operatorID
	return h, ss, bs
}

func newAgentExposureDashboardFixture(t *testing.T) (*DashboardHandler, *agentExposureContractDriver) {
	t.Helper()
	handler, _, _ := newFederatedExposureBaseFixture(t)
	driver := &agentExposureContractDriver{}
	handler.Federation = driver
	return handler, driver
}

func agentExposureDashboardRequest(
	t *testing.T, h *DashboardHandler, method, body string, operator bool,
) *httptest.ResponseRecorder {
	t.Helper()
	reader := bytes.NewReader(nil)
	if body != "" {
		reader = bytes.NewReader([]byte(body))
	}
	req := withFederationChain(httptest.NewRequest(method,
		"http://localhost/v1/dashboard/federation/connections/chain-peer/agent-exposure",
		reader), "chain-peer")
	if operator {
		markLocalCEREBRUM(h, req)
	}
	rr := httptest.NewRecorder()
	if method == http.MethodGet {
		h.handleFedAgentExposureGet(rr, req)
	} else {
		h.handleFedAgentExposurePut(rr, req)
	}
	return rr
}

func TestFederatedAgentExposureDashboardIsOperatorOnlyAndAgreementBound(t *testing.T) {
	handler, _ := newAgentExposureDashboardFixture(t)
	require.Equal(t, http.StatusForbidden,
		agentExposureDashboardRequest(t, handler, http.MethodGet, "", false).Code)
	require.Equal(t, http.StatusForbidden,
		agentExposureDashboardRequest(t, handler, http.MethodPut,
			`{"mode":"none","agent_ids":[],"expected_revision":0}`, false).Code)

	// No agreement for this chain: the operator surface stays unavailable
	// rather than inventing a policy for a connection that does not exist.
	bare, _, _ := newFederatedExposureBaseFixture(t)
	bare.Federation = &agentExposureContractDriver{}
	req := withFederationChain(httptest.NewRequest(http.MethodGet,
		"http://localhost/v1/dashboard/federation/connections/absent-chain/agent-exposure",
		bytes.NewReader(nil)), "absent-chain")
	markLocalCEREBRUM(bare, req)
	rr := httptest.NewRecorder()
	bare.handleFedAgentExposureGet(rr, req)
	require.Equal(t, http.StatusConflict, rr.Code)
}

func TestFederatedAgentExposureDashboardUnconfiguredReportsDefault(t *testing.T) {
	handler, driver := newAgentExposureDashboardFixture(t)
	rr := agentExposureDashboardRequest(t, handler, http.MethodGet, "", true)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, 1, driver.getCalls)
	require.Equal(t, "chain-peer", driver.lastChain)
	body := rr.Body.String()
	require.Contains(t, body, `"mode":"all"`)
	require.Contains(t, body, `"configured":false`)
}

func TestFederatedAgentExposureDashboardPutForwardsExactIntent(t *testing.T) {
	handler, driver := newAgentExposureDashboardFixture(t)
	agentID := strings.Repeat("c", 64)
	rr := agentExposureDashboardRequest(t, handler, http.MethodPut,
		`{"mode":"selected","agent_ids":["`+agentID+`"],"expected_revision":4}`, true)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, 1, driver.setCalls)
	require.Equal(t, "selected", driver.lastMode)
	require.Equal(t, []string{agentID}, driver.lastAgents)
	require.Equal(t, int64(4), driver.lastExpect)
	require.Contains(t, rr.Body.String(), `"mode":"selected"`)
}

func TestFederatedAgentExposureDashboardRejectsInvalidWireIntent(t *testing.T) {
	handler, driver := newAgentExposureDashboardFixture(t)
	for name, body := range map[string]string{
		"unknown mode":      `{"mode":"sometimes","agent_ids":[],"expected_revision":0}`,
		"missing mode":      `{"agent_ids":[],"expected_revision":0}`,
		"negative revision": `{"mode":"none","agent_ids":[],"expected_revision":-1}`,
		"malformed body":    `{"mode":`,
		"trailing json":     `{"mode":"none","agent_ids":[],"expected_revision":0}{"mode":"all"}`,
	} {
		rr := agentExposureDashboardRequest(t, handler, http.MethodPut, body, true)
		require.Equal(t, http.StatusBadRequest, rr.Code, name)
	}
	require.Zero(t, driver.setCalls, "an invalid request must never reach the driver")
}

func TestFederatedAgentExposureDashboardConflictsAreDistinguishable(t *testing.T) {
	handler, driver := newAgentExposureDashboardFixture(t)
	driver.setErr = store.ErrFederatedAgentExposureRevisionConflict
	rr := agentExposureDashboardRequest(t, handler, http.MethodPut,
		`{"mode":"none","agent_ids":[],"expected_revision":1}`, true)
	require.Equal(t, http.StatusConflict, rr.Code)
	require.Contains(t, rr.Body.String(), "Refresh this connection")

	driver.setErr = store.ErrFederatedAgentExposureBindingMismatch
	rr = agentExposureDashboardRequest(t, handler, http.MethodPut,
		`{"mode":"none","agent_ids":[],"expected_revision":1}`, true)
	require.Equal(t, http.StatusConflict, rr.Code)

	driver.setErr = errors.New("store unavailable")
	rr = agentExposureDashboardRequest(t, handler, http.MethodPut,
		`{"mode":"selected","agent_ids":["`+strings.Repeat("d", 64)+`"],"expected_revision":1}`, true)
	require.Equal(t, http.StatusConflict, rr.Code)
}
