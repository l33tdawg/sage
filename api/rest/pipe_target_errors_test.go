package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/require"
)

func TestRemotePipeTargetErrorDistinguishesLocalStorageAndPeerCapabilities(t *testing.T) {
	for _, tt := range []struct {
		name   string
		err    error
		status int
		title  string
	}{
		{"local storage", federation.ErrRemotePipeLocalStoreUnavailable, http.StatusServiceUnavailable, "Local federated pipeline unavailable"},
		{"remote capability", federation.ErrRemotePipePeerUnsupported, http.StatusNotImplemented, "Peer update required"},
		{"incomplete peer lookup", federation.ErrRemotePipeResolutionIncomplete, http.StatusServiceUnavailable, "Federated agent lookup incomplete"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, memStore := newPipeServer(t)
			s.SetFederation(&remotePipeResolver{fakeFederation: &fakeFederation{}, err: fmt.Errorf("resolve: %w", tt.err)})
			rr := httptest.NewRecorder()
			pipeRouterAs(s, "local-sender").ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
				"/v1/pipe/resolve", bytes.NewBufferString(`{"to":"`+strings.Repeat("ab", 32)+`@chain-peer"}`)))
			require.Equal(t, tt.status, rr.Code)
			require.Contains(t, rr.Body.String(), tt.title)
			if tt.err == federation.ErrRemotePipeLocalStoreUnavailable {
				require.NotContains(t, rr.Body.String(), "receiving SAGE")
				require.NotContains(t, rr.Body.String(), "Peer update required")
			}
			rows, err := memStore.ListPipelines(context.Background(), "", 10)
			require.NoError(t, err)
			require.Empty(t, rows, "resolution errors must not queue a message")
		})
	}
}

func TestLegacyPipeSendUnknownTargetExplainsExactResolutionWithoutQueueing(t *testing.T) {
	for _, targetKind := range []string{"unknown alias", "inactive name", "Root name"} {
		t.Run(targetKind, func(t *testing.T) {
			s, memStore := newPipeServer(t)
			target := "provider-alias"
			switch targetKind {
			case "inactive name":
				target = "pending-agent"
				_, _, currentRoot, companionID := configurePostV23PipeRoot(t, s, memStore)
				require.NoError(t, memStore.CreateAgent(context.Background(), &store.AgentEntry{
					AgentID: companionID, Name: target, Provider: "other-provider", Status: "active",
				}))
				require.NoError(t, s.badgerStore.ApproveAppV23LocalAgent(store.AppV23LocalEnrollment{
					AgentID: companionID, Active: false, Profile: store.AppV23ProfileStandard,
					ApprovedBy: currentRoot, RootGeneration: 3, UpdatedHeight: 4,
				}, store.AppV23RoleMember, 1, 1))
			case "Root name":
				configurePostV23PipeRoot(t, s, memStore)
				target = "root-principal"
			}
			body, err := json.Marshal(map[string]any{"to_provider": target, "payload": "work"})
			require.NoError(t, err)
			rr := httptest.NewRecorder()
			pipeRouterAs(s, "sender").ServeHTTP(rr,
				httptest.NewRequest(http.MethodPost, "/v1/pipe/send", bytes.NewReader(body)))
			require.Equal(t, http.StatusNotFound, rr.Code, rr.Body.String())
			for _, guidance := range []string{"POST /v1/pipe/resolve", "to_agent", "source_chain_id", "destination_chain_id", "sage_find_agent", "sage_directory", "shared provider inbox", "arbitrary provider aliases are not inferred"} {
				require.Contains(t, rr.Body.String(), guidance)
			}
			rows, err := memStore.ListPipelines(context.Background(), "", 10)
			require.NoError(t, err)
			require.Empty(t, rows)
		})
	}
}

func TestLegacyPipeSendProviderInboxCanBeClaimedByEitherMatchingAgent(t *testing.T) {
	const provider = "shared-provider"
	for _, first := range []string{"agent-a", "agent-b"} {
		t.Run(first, func(t *testing.T) {
			s, memStore := newPipeServer(t)
			for _, id := range []string{"agent-a", "agent-b"} {
				require.NoError(t, memStore.CreateAgent(context.Background(), &store.AgentEntry{
					AgentID: id, Name: id, Provider: provider, Status: "active",
				}))
			}
			body, err := json.Marshal(map[string]any{"to_provider": provider, "payload": "shared work"})
			require.NoError(t, err)
			rr := httptest.NewRecorder()
			pipeRouterAs(s, "sender").ServeHTTP(rr,
				httptest.NewRequest(http.MethodPost, "/v1/pipe/send", bytes.NewReader(body)))
			require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())
			var sent struct {
				PipeID string `json:"pipe_id"`
			}
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &sent))
			stored, err := memStore.GetPipeline(context.Background(), sent.PipeID)
			require.NoError(t, err)
			require.Empty(t, stored.ToAgent)
			require.Equal(t, provider, stored.ToProvider, "direct legacy provider send must remain shared")
			readInbox := func(agent string) int {
				rr := httptest.NewRecorder()
				pipeRouterAs(s, agent).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/pipe/inbox", nil))
				require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
				var inbox struct {
					Count int `json:"count"`
				}
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &inbox))
				return inbox.Count
			}
			require.Equal(t, 1, readInbox(first), "either matching agent must be eligible for the first claim")
			other := "agent-a"
			if first == other {
				other = "agent-b"
			}
			require.Equal(t, 1, readInbox(other), "inspection does not reserve work to either matching agent")
			claim := httptest.NewRecorder()
			pipeRouterAs(s, first).ServeHTTP(claim, httptest.NewRequest(http.MethodPut, "/v1/pipe/"+sent.PipeID+"/claim?claimant_session_id=winner", nil))
			require.Equal(t, 200, claim.Code, claim.Body.String())
			require.Zero(t, readInbox(other), "the second agent must not receive explicitly claimed provider work")
			stored, err = memStore.GetPipeline(context.Background(), sent.PipeID)
			require.NoError(t, err)
			require.Equal(t, first, stored.ClaimedBy)
		})
	}
}
