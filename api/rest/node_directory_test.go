package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/require"
)

type nodePageFailureFederation struct{ *fakeFederation }

func (f *nodePageFailureFederation) ListRemoteNodeContacts(context.Context, string, *federation.StatusResponse, string) (*federation.PipeContactLookupResponse, error) {
	return nil, errors.New("peer page unavailable")
}

func TestNodeDirectoryExposesAgentsWithoutReadAndReportsPageFailure(t *testing.T) {
	for _, continuation := range []bool{false, true} {
		t.Run(map[bool]string{false: "first page", true: "failed continuation"}[continuation], func(t *testing.T) {
			srv, _, _ := newTestServer(t, "")
			badger, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
			require.NoError(t, err)
			t.Cleanup(func() { _ = badger.CloseBadger() })
			srv.badgerStore = badger
			require.NoError(t, badger.SetCrossFed("chain-peer", "https://redacted.invalid", []byte("pin"), 4, 0, nil, nil, "active"))
			agentID := strings.Repeat("a", 64)
			srv.SetFederation(&nodePageFailureFederation{&fakeFederation{peerStatus: &federation.StatusResponse{
				ChainID: "chain-peer", Capabilities: []string{federation.CapabilityNodeMessaging},
				PeerRBACGrant: &federation.PeerRBACGrant{},
				PipeContacts: &federation.PipeContactGrant{
					Version: federation.PipeContactVersion, AgreementID: strings.Repeat("b", 64), Revision: strings.Repeat("c", 64),
					Contacts: []federation.PipeContact{{AgentID: agentID, Address: agentID + "@chain-peer", Handle: "#peer/a", ContactID: strings.Repeat("d", 64),
						AuthorizationMode: federation.NodeMessageAuthorizationMode, Available: true, Accepting: true}},
				},
			}}})
			url := "/v1/federation/available"
			if continuation {
				url += "?peer_chain=chain-peer&agent_cursor=opaque-cursor"
			}
			req, callerID := signedRequest(t, http.MethodGet, url, nil)
			require.NoError(t, badger.RegisterAgent(callerID, "ordinary", "member", "", "test", "", 1))
			require.NoError(t, badger.SetAgentPermission(callerID, 1, "[]", "*", "", ""))
			rr := httptest.NewRecorder()
			srv.Router().ServeHTTP(rr, req)
			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
			var out struct {
				Connections []availableFederationConnection `json:"connections"`
			}
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &out))
			require.Len(t, out.Connections, 1)
			connection := out.Connections[0]
			require.Empty(t, connection.SharedReadDomains)
			require.Empty(t, connection.CopyOfferedDomains)
			if continuation {
				require.True(t, connection.AgentDirectoryUnavailable)
				require.True(t, connection.RemoteAgentsTruncated)
				require.Equal(t, "opaque-cursor", connection.NextAgentCursor)
				require.Empty(t, connection.RemoteAgents, "failed continuation must not silently return the first page")
			} else {
				require.Len(t, connection.RemoteAgents, 1)
				require.Equal(t, agentID+"@chain-peer", connection.RemoteAgents[0].Address)
				require.Empty(t, connection.RemoteAgents[0].Domains)
			}
		})
	}
}
