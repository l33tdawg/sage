package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/require"
)

// pipeSendActivityRow is the EXACT dashboard Chain Activity content that
// /v1/pipe/send is allowed to publish for a 10-byte payload. It is spelled out
// here rather than obtained from pipelineSendActivitySummary so that widening
// the production row is a test failure, not a silently mirrored change.
const pipeSendActivityRow = "[Pipeline] Local agent pipeline opened. Work request queued (10 chars). " +
	"Untrusted endpoints and intent omitted from the activity stream."

// TestPipeSendActivityEventCarriesNoEndpointIdentity pins the privacy contract
// of the pipeline_send event.
//
// That event is handed to Server.OnEvent, which cmd/sage-gui bridges straight
// into web.SSEBroadcaster.Broadcast. That broadcaster is a global fan-out with
// no per-subscriber identity — Subscribe() takes no arguments and Broadcast
// pushes identical bytes to every attached client — while handlePipeStatus
// deliberately returns a byte-identical 404 to any caller that is not a party
// to the pipe. So the sending provider, the recipient agent id, the recipient
// provider/name, and the caller-supplied intent must never reach this row.
func TestPipeSendActivityEventCarriesNoEndpointIdentity(t *testing.T) {
	// The two ids differ inside their first 16 bytes so that a leak of a
	// truncated id names the endpoint it actually came from.
	const (
		senderID       = "a1a1a1a1a1a1a1a1000000000000000000000000000000000000000000000000"
		senderProvider = "sentinel-sender-provider"
		targetID       = "b2b2b2b2b2b2b2b2000000000000000000000000000000000000000000000000"
		targetName     = "sentinel-recipient-name"
		targetProvider = "sentinel-target-provider"
		sentinelIntent = "sentinel-private-work-request"
		payload        = "0123456789" // 10 bytes -> "(10 chars)"
	)

	// Both addressing modes reach the same emit site: to_agent carries a raw
	// agent id, to_provider carries a human provider label. Cover both, because
	// scrubbing only one of the two branches would still leak.
	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{
			name: "addressed by agent id",
			body: map[string]any{"to_agent": targetID, "intent": sentinelIntent, "payload": payload},
		},
		{
			name: "addressed by provider label",
			body: map[string]any{"to_provider": targetProvider, "intent": sentinelIntent, "payload": payload},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, memStore := newPipeServer(t)
			ctx := context.Background()
			require.NoError(t, memStore.CreateAgent(ctx, &store.AgentEntry{
				AgentID: senderID, Name: "sentinel-sender-name",
				Provider: senderProvider, Status: "active",
			}))
			require.NoError(t, memStore.CreateAgent(ctx, &store.AgentEntry{
				AgentID: targetID, Name: targetName,
				Provider: targetProvider, Status: "active",
			}))

			var (
				calls      int
				gotType    string
				gotPipeID  string
				gotDomain  string
				gotContent string
				gotData    any
			)
			srv.OnEvent = func(eventType, memoryID, domain, content string, data any) {
				calls++
				gotType, gotPipeID, gotDomain, gotContent, gotData =
					eventType, memoryID, domain, content, data
			}

			body, err := json.Marshal(tc.body)
			require.NoError(t, err)
			rr := httptest.NewRecorder()
			pipeRouterAs(srv, senderID).ServeHTTP(rr,
				httptest.NewRequest(http.MethodPost, "/v1/pipe/send", bytes.NewReader(body)))
			require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())

			require.Equal(t, 1, calls, "exactly one activity row per accepted send")
			require.Equal(t, "pipeline_send", gotType)
			require.Equal(t, "agent-pipeline", gotDomain)

			// Name every identifier that must not appear, so a regression says
			// which one came back. This runs BEFORE the exact-match assertion
			// below on purpose: require.* aborts the test, so a leak check
			// placed after an equality check could never fire.
			for _, banned := range []struct{ name, secret string }{
				{"sender agent id", senderID},
				{"sender provider", senderProvider},
				{"target agent id", targetID},
				{"target agent name", targetName},
				{"target provider", targetProvider},
				{"caller intent", sentinelIntent},
			} {
				require.NotContains(t, gotContent, banned.secret,
					"%s leaked into the globally fanned-out pipeline_send row", banned.name)
				// A truncated identifier is still an identifier: the historical
				// row published only the first 16 bytes of the recipient id.
				if len(banned.secret) > 16 {
					require.NotContains(t, gotContent, banned.secret[:16],
						"truncated %s leaked into the globally fanned-out pipeline_send row",
						banned.name)
				}
			}

			// The structured Data field is a second, easier-to-miss channel onto
			// the same global stream; pipeline_send must not use it at all.
			require.Nil(t, gotData, "pipeline_send must publish no structured data")

			// Exact content, not a substring probe: a substring assertion would
			// still pass if an endpoint name were appended to a correct prefix,
			// and the loop above only rejects the identifiers it knows about.
			require.Equal(t, pipeSendActivityRow, gotContent,
				"pipeline_send activity content must stay metadata-only")

			// The pipe id stays as the row's correlation handle (it is random,
			// carries no agent identity, and matches pipeline_complete), so
			// assert it is exactly the id returned to the sender.
			var resp struct {
				PipeID string `json:"pipe_id"`
			}
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
			require.NotEmpty(t, resp.PipeID)
			require.Equal(t, resp.PipeID, gotPipeID)
		})
	}
}
