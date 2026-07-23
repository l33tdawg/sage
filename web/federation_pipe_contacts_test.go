package web

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/store"
)

type pipeContactContractDriver struct {
	FederationJoinDriver
	local       *federation.PipeContactGrant
	remote      *federation.PipeContactGrant
	setErr      error
	setCalls    int
	statusCalls int
}

func (d *pipeContactContractDriver) LocalPipeContacts(context.Context, string) (*federation.PipeContactGrant, error) {
	return d.local, nil
}

func (d *pipeContactContractDriver) SetPipeContactAcceptance(_ context.Context, _ string, agentID, contactID string, accepting bool) (*federation.PipeContactGrant, error) {
	d.setCalls++
	if d.setErr != nil {
		return nil, d.setErr
	}
	for i := range d.local.Contacts {
		if d.local.Contacts[i].AgentID == agentID && d.local.Contacts[i].ContactID == contactID {
			d.local.Contacts[i].Accepting = accepting
		}
	}
	return d.local, nil
}

func (d *pipeContactContractDriver) PeerStatus(context.Context, string) (*federation.StatusResponse, error) {
	d.statusCalls++
	return &federation.StatusResponse{PipeContacts: d.remote}, nil
}

func TestFederatedPipeContactDashboardReadAndMutation(t *testing.T) {
	ctx := context.Background()
	ss, err := store.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "contacts.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ss.Close() })
	bs, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })
	require.NoError(t, bs.SetCrossFed("chain-peer", "https://peer:8444", bytes.Repeat([]byte{0x55}, 32), 4, 0, nil, nil, "active"))

	agentID := strings.Repeat("ab", 32)
	contactID := strings.Repeat("cd", 32)
	driver := &pipeContactContractDriver{
		local: &federation.PipeContactGrant{Version: federation.PipeContactVersion, Revision: "local", Contacts: []federation.PipeContact{{
			AgentID: agentID, ContactID: contactID, DisplayName: "tii-sentinel", Available: true,
		}}},
		remote: &federation.PipeContactGrant{Version: federation.PipeContactVersion, Revision: "remote", Contacts: []federation.PipeContact{{
			AgentID: strings.Repeat("ef", 32), ContactID: strings.Repeat("12", 32), Address: strings.Repeat("ef", 32) + "@chain-peer", Accepting: true, Available: true,
		}}},
	}
	h := NewDashboardHandler(ss, "test")
	h.BadgerStore = bs
	h.Federation = driver

	getReq := withFederationChain(httptest.NewRequest(http.MethodGet,
		"http://localhost/v1/dashboard/federation/connections/chain-peer/pipe-contacts", nil), "chain-peer")
	getReq.RemoteAddr = "127.0.0.1:4242"
	getReq.Header.Set("Sec-Fetch-Site", "same-origin")
	getRR := httptest.NewRecorder()
	h.handleFedPipeContactsGet(getRR, getReq)
	require.Equal(t, http.StatusOK, getRR.Code, getRR.Body.String())
	var getBody struct {
		Local       *federation.PipeContactGrant `json:"local_contacts"`
		Remote      *federation.PipeContactGrant `json:"remote_contacts"`
		RemoteKnown bool                         `json:"remote_known"`
	}
	require.NoError(t, json.NewDecoder(getRR.Body).Decode(&getBody))
	require.False(t, getBody.Local.Contacts[0].Accepting)
	require.True(t, getBody.RemoteKnown)
	require.True(t, getBody.Remote.Contacts[0].Accepting)
	require.Equal(t, 1, driver.statusCalls)

	localOnlyReq := withFederationChain(httptest.NewRequest(http.MethodGet,
		"http://localhost/v1/dashboard/federation/connections/chain-peer/pipe-contacts?live=0", nil), "chain-peer")
	localOnlyReq.RemoteAddr = "127.0.0.1:4242"
	localOnlyReq.Header.Set("Sec-Fetch-Site", "same-origin")
	localOnlyRR := httptest.NewRecorder()
	h.handleFedPipeContactsGet(localOnlyRR, localOnlyReq)
	require.Equal(t, http.StatusOK, localOnlyRR.Code, localOnlyRR.Body.String())
	require.Equal(t, 1, driver.statusCalls, "local-only dashboard paint must not dial the peer")
	var localOnlyBody struct {
		Local       *federation.PipeContactGrant `json:"local_contacts"`
		RemoteKnown bool                         `json:"remote_known"`
	}
	require.NoError(t, json.NewDecoder(localOnlyRR.Body).Decode(&localOnlyBody))
	require.NotNil(t, localOnlyBody.Local)
	require.False(t, localOnlyBody.RemoteKnown)

	putReq := withFederationChain(httptest.NewRequest(http.MethodPut,
		"http://localhost/v1/dashboard/federation/connections/chain-peer/pipe-contacts",
		bytes.NewBufferString(`{"agent_id":"`+agentID+`","contact_id":"`+contactID+`","accepting":true}`)), "chain-peer")
	putReq.RemoteAddr = "127.0.0.1:4242"
	putReq.Header.Set("Sec-Fetch-Site", "same-origin")
	putRR := httptest.NewRecorder()
	h.handleFedPipeContactsPut(putRR, putReq)
	require.Equal(t, http.StatusOK, putRR.Code, putRR.Body.String())
	require.Equal(t, 1, driver.setCalls)
	require.True(t, driver.local.Contacts[0].Accepting)
}

func TestFederatedPipeContactDashboardRejectsStaleMutation(t *testing.T) {
	ctx := context.Background()
	ss, err := store.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "contacts.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ss.Close() })
	bs, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })
	require.NoError(t, bs.SetCrossFed("chain-peer", "https://peer:8444", bytes.Repeat([]byte{0x55}, 32), 4, 0, nil, nil, "active"))
	driver := &pipeContactContractDriver{
		local:  &federation.PipeContactGrant{},
		setErr: federation.ErrPipeContactChanged,
	}
	h := NewDashboardHandler(ss, "test")
	h.BadgerStore = bs
	h.Federation = driver

	req := withFederationChain(httptest.NewRequest(http.MethodPut,
		"http://localhost/v1/dashboard/federation/connections/chain-peer/pipe-contacts",
		bytes.NewBufferString(`{"agent_id":"agent","contact_id":"contact","accepting":true}`)), "chain-peer")
	req.RemoteAddr = "127.0.0.1:4242"
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	rr := httptest.NewRecorder()
	h.handleFedPipeContactsPut(rr, req)
	require.Equal(t, http.StatusConflict, rr.Code, rr.Body.String())
	require.Contains(t, rr.Body.String(), "changed")
}
