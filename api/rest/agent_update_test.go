package rest

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestAgentUpdatePartialBodyPreservesOmittedMetadata(t *testing.T) {
	tests := []struct {
		name            string
		body            string
		expectedName    string
		expectedBootBio string
	}{
		{
			name:            "name only preserves boot bio",
			body:            `{"name":"renamed display"}`,
			expectedName:    "renamed display",
			expectedBootBio: "original boot bio",
		},
		{
			name:            "bio only preserves display name",
			body:            `{"boot_bio":"updated boot bio"}`,
			expectedName:    "original display",
			expectedBootBio: "updated boot bio",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			srv, _, badgerStore, _ := newRBACTestServer(t)
			agentID := appV23RESTAgentID(test.name)
			require.NoError(t, badgerStore.RegisterAgentWithCapabilities(
				agentID,
				"original display",
				store.AppV23RoleMember,
				"original boot bio",
				"test",
				"",
				1,
				0,
			))

			comet := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, err := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
				require.NoError(t, err)
				parsed, err := tx.DecodeTx(raw)
				require.NoError(t, err)
				require.NotNil(t, parsed.AgentUpdateTx)
				require.Equal(t, agentID, parsed.AgentUpdateTx.AgentID)
				require.Equal(t, test.expectedName, parsed.AgentUpdateTx.Name)
				require.Equal(t, test.expectedBootBio, parsed.AgentUpdateTx.BootBio)
				_, _ = fmt.Fprint(w, `{"result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"UPDATE","height":"2"}}`)
			}))
			defer comet.Close()
			srv.cometbftRPC = comet.URL

			req := appV23RESTRequest(http.MethodPut, "/v1/agent/update", agentID, test.body, nil)
			rec := httptest.NewRecorder()
			srv.handleAgentUpdate(rec, req)

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.JSONEq(t, fmt.Sprintf(
				`{"agent_id":%q,"name":%q,"status":"updated","tx_hash":"UPDATE"}`,
				agentID, test.expectedName,
			), rec.Body.String())
		})
	}
}
