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
// /v1/pipe/send is allowed to publish. It is spelled out here rather than
// obtained from pipelineSendActivitySummary so that widening the production row
// is a test failure, not a silently mirrored change — a test constant derived
// from the thing it guards cannot detect that thing growing.
//
// It carries no payload size any more. A length is not neutral on a stream every
// client reads: it correlates with content, and a series of lengths profiles a
// private channel without ever naming it.
const pipeSendActivityRow = "[Pipeline] Local agent pipeline opened. Details omitted from the activity stream."

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

			// THE EVENT ID MUST BE EMPTY. It reaches the global stream as
			// SSEEvent.MemoryID, and the pipe id is a private identifier for one
			// channel between two agents — publishing it lets any connected
			// client correlate every later event about that pipe, even when the
			// text says nothing.
			require.Empty(t, gotPipeID,
				"the pipe id must not cross the identity-free stream as the event id")

			// CONSTANT CONTENT. Asserted as exact equality, not absence of
			// secrets: a size or a duration leaks nothing by name yet is still a
			// side channel on a stream every client reads, and only an equality
			// catches a field that is added later.

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

			// THE PIPE ID MUST NOT BE THE ROW'S CORRELATION HANDLE. An earlier
			// version of this test asserted the opposite, on the reasoning that
			// the id is random and names no agent. That reasoning was wrong: the
			// id reaches the identity-free stream as SSEEvent.MemoryID, and a
			// stable handle lets any connected client correlate every later
			// event about one private channel even when each row says nothing.
			// Randomness prevents guessing the id, not linking on it.
			var resp struct {
				PipeID string `json:"pipe_id"`
			}
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
			require.NotEmpty(t, resp.PipeID, "precondition: the send produced a pipe id")
			require.Empty(t, gotPipeID,
				"the pipe id must not cross the identity-free stream as the event id")
			require.NotContains(t, gotContent, resp.PipeID,
				"nor may it be embedded in the row text")
		})
	}
}

// TestPipeCompleteActivityEventCarriesNoPipeIdentity covers the SECOND emit
// mode. It is the stricter of the two: pipeline_complete published the pipe id
// in the event id field AND embedded it again in the default summary text
// ("federated pipeline <id> completed"), and in the journaled variant it also
// carried the result length and the elapsed duration.
//
// The detailed summary is deliberately still built — autoJournalPipeline writes
// it to a memory, which is an AUTHORIZED record rather than a broadcast. What
// must not happen is that same string reaching the identity-free stream.
func TestPipeCompleteActivityEventCarriesNoPipeIdentity(t *testing.T) {
	srv, sqlite := newPipeServer(t)
	const (
		senderID   = "agent-complete-sender"
		workerID   = "agent-complete-worker"
		resultText = "ZZQX-SENTINEL-RESULT-do-not-broadcast"
	)
	addMessageAgent(t, sqlite, senderID)
	addMessageAgent(t, sqlite, workerID)

	sendBody, err := json.Marshal(map[string]any{
		"to_agent": workerID, "intent": "review", "payload": "work",
	})
	require.NoError(t, err)
	rr := httptest.NewRecorder()
	pipeRouterAs(srv, senderID).ServeHTTP(rr,
		httptest.NewRequest(http.MethodPost, "/v1/pipe/send", bytes.NewReader(sendBody)))
	require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())

	var sent struct {
		PipeID string `json:"pipe_id"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &sent))
	require.NotEmpty(t, sent.PipeID, "precondition: the send produced a pipe id")

	claimRR := httptest.NewRecorder()
	pipeRouterAs(srv, workerID).ServeHTTP(claimRR,
		httptest.NewRequest(http.MethodPut, "/v1/pipe/"+sent.PipeID+"/claim",
			bytes.NewReader([]byte(`{}`))))
	require.Equal(t, http.StatusOK, claimRR.Code,
		"precondition: the worker must claim the pipe before completing it: %s", claimRR.Body.String())

	var (
		calls      int
		gotType    string
		gotPipeID  string
		gotContent string
		gotData    any
	)
	srv.OnEvent = func(eventType, memoryID, domain, content string, data any) {
		if eventType != "pipeline_complete" {
			return
		}
		calls++
		gotType, gotPipeID, gotContent, gotData = eventType, memoryID, content, data
	}

	resultBody, err := json.Marshal(map[string]any{"result": resultText})
	require.NoError(t, err)
	doneRR := httptest.NewRecorder()
	pipeRouterAs(srv, workerID).ServeHTTP(doneRR,
		httptest.NewRequest(http.MethodPut, "/v1/pipe/"+sent.PipeID+"/result",
			bytes.NewReader(resultBody)))
	// NO SKIP HERE, DELIBERATELY. An earlier version of this test skipped when
	// the result path returned an unexpected status — and it did, because the
	// request used the wrong method. The test reported SKIP, which reads as
	// harmless, while covering nothing: two mutations that reintroduced the pipe
	// id on this very event survived it. A test that cannot reach its subject
	// must FAIL and say so.
	require.Equal(t, http.StatusOK, doneRR.Code,
		"the completion path must be reachable or this test proves nothing: %s", doneRR.Body.String())
	require.Equal(t, 1, calls, "exactly one completion activity row")
	require.Equal(t, "pipeline_complete", gotType)

	require.Empty(t, gotPipeID,
		"the pipe id must not cross the identity-free stream as the event id")
	require.Equal(t, pipelineCompleteActivitySummary, gotContent,
		"the completion row must be the constant summary")
	require.NotContains(t, gotContent, sent.PipeID,
		"the pipe id must not be embedded in the summary text either")
	require.NotContains(t, gotContent, resultText,
		"the result content must never reach the activity stream")
	require.Nil(t, gotData, "no structured payload may ride the completion row")
}
