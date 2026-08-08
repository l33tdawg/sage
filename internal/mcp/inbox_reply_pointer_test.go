package mcp

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
)

// v11.18.2 — the payload-free reply pointer inside sage_inbox.
//
// sage_inbox is a claim-on-read surface for work addressed TO the caller. A
// reply to a message the caller SENT is not work, so it must never become an
// inbox item. The inbox instead carries a scalar pointer telling the agent that
// an explicit, passive read exists — which is what fixes the "clean inbox is
// mistaken for no reply" failure that made replies invisible.

type inboxReplyPointerStub struct {
	mu             sync.Mutex
	requests       []string
	countOnlyCalls int
}

func (s *inboxReplyPointerStub) record(r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, r.Method+" "+r.URL.Path+"?"+r.URL.RawQuery)
}

func (s *inboxReplyPointerStub) calls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.requests...)
}

// newClearInboxServer stubs an entirely empty inbox: no canonical messages, no
// legacy/federated pipeline work, no task notices.
func newClearInboxServer(t *testing.T, stub *inboxReplyPointerStub, replyCount int, serveResults bool) *Server {
	t.Helper()
	mux := http.NewServeMux()
	empty := func(w http.ResponseWriter, r *http.Request) {
		stub.record(r)
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	}
	mux.HandleFunc("/v1/messages/receive", empty)
	mux.HandleFunc("/v1/pipe/inbox", empty)
	mux.HandleFunc("/v1/dashboard/task-notifications", empty)
	if serveResults {
		mux.HandleFunc("/v1/pipe/results", func(w http.ResponseWriter, r *http.Request) {
			stub.record(r)
			require.Equal(t, http.MethodGet, r.Method)
			require.Equal(t, "1", r.URL.Query().Get("count_only"),
				"the inbox pointer must use the payload-free probe, never pull reply bodies")
			stub.mu.Lock()
			stub.countOnlyCalls++
			stub.mu.Unlock()
			probe := map[string]any{"count": replyCount, "retained": replyCount > 0}
			if replyCount > 0 {
				probe["newest_completed_at"] = inboxProbeNewestCompletedAt
			}
			_ = json.NewEncoder(w).Encode(probe)
		})
	}
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	return NewServer(ts.URL, priv)
}

const inboxProbeNewestCompletedAt = "2026-08-08T00:05:00Z"

// pendingStatePhrases are the words that turn a factual retained archive size
// into an assertion that work is owed. No read state exists, so any of these
// could be re-asserted about replies the agent has already read.
var pendingStatePhrases = []string{
	"are waiting", "is waiting", "waiting.", "Read them", "read them",
	"pending", "you must", "action required", "requires",
}

// TestSageInboxSurfacesReplyPointerOnAnOtherwiseClearInbox is the case the old
// inbox description actively misdirected: nothing is addressed to this agent,
// but a reply it asked for is waiting. The pointer must appear, and it must not
// manufacture an inbox item.
func TestSageInboxSurfacesReplyPointerOnAnOtherwiseClearInbox(t *testing.T) {
	stub := &inboxReplyPointerStub{}
	s := newClearInboxServer(t, stub, 2, true)

	result, err := s.toolInbox(context.Background(), map[string]any{})
	require.NoError(t, err)
	response := result.(map[string]any)

	require.Equal(t, 0, response["count"], "a reply must never inflate the inbox item count")
	items, _ := response["items"].([]any)
	require.Empty(t, items, "replies must never enter sage_inbox.items[]")
	require.Equal(t, 2, response["retained_reply_count"])
	require.Equal(t, false, response["retained_reply_count_is_unread"],
		"the count is an archive snapshot and must not be labelled unread")
	note, ok := response["replies_note"].(string)
	require.True(t, ok, "a non-zero reply count must name the explicit read")
	require.Contains(t, note, "sage_message_replies")
	require.Contains(t, note, "not new work",
		"the pointer must not read as an alert about work owed")
	require.Contains(t, note, "current retained archive size")
	require.NotContains(t, response, "replies_check_error")

	// The high-water mark lets an agent poll with an inclusive boundary. Equal
	// millisecond rows may repeat so that a later reply can never be hidden.
	require.Equal(t, inboxProbeNewestCompletedAt, response["newest_reply_completed_at"])

	encoded, err := json.Marshal(response)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "requires_reply",
		"a clear inbox carrying only a reply pointer must not claim anything requires a reply")

	stub.mu.Lock()
	require.Equal(t, 1, stub.countOnlyCalls, "the pointer costs exactly one payload-free probe")
	stub.mu.Unlock()
}

// TestSageInboxReplyPointerNeverAssertsRepliesArePending is the regression for
// the release's own stated design: retained_reply_count is "a retained total,
// not an unread counter … it must not be treated as an alert about work owed".
// A pointer that says replies "are waiting" and orders the agent to "read them"
// can re-issue work about replies the agent already handled. The model reads
// the runtime string, not the doc, so the string is what this test pins.
func TestSageInboxReplyPointerNeverAssertsRepliesArePending(t *testing.T) {
	stub := &inboxReplyPointerStub{}
	s := newClearInboxServer(t, stub, 57, true)

	result, err := s.toolInbox(context.Background(), map[string]any{})
	require.NoError(t, err)
	response := result.(map[string]any)
	require.Equal(t, 57, response["retained_reply_count"])

	note := response["replies_note"].(string)
	for _, phrase := range pendingStatePhrases {
		require.NotContains(t, note, phrase,
			"%q asserts pending work about a passive retained archive snapshot", phrase)
	}
	require.NotContains(t, response, "replies_action",
		"naming the field an 'action' is itself the pending-state claim")
	require.Contains(t, note, "not an unread count")
	require.Contains(t, note, "already read",
		"the pointer must say the total includes replies the agent has already handled")

	// A repeat inbox call is byte-identical: nothing about the pointer decays,
	// so it must not read as something that was supposed to.
	repeat, err := s.toolInbox(context.Background(), map[string]any{})
	require.NoError(t, err)
	firstJSON, err := json.Marshal(response)
	require.NoError(t, err)
	repeatJSON, err := json.Marshal(repeat)
	require.NoError(t, err)
	require.JSONEq(t, string(firstJSON), string(repeatJSON))

	// The pointer must not advertise a number the tool it names cannot deliver
	// in one call without also naming the way to reach the rest.
	require.Contains(t, note, "newest_reply_completed_at",
		"a count of 57 against a 20-row page cap needs an explicit polling watermark")
}

// TestSageInboxReplyPointerReportsZeroWithoutSuggestingAction keeps the common
// case quiet: no replies means no action string at all.
func TestSageInboxReplyPointerReportsZeroWithoutSuggestingAction(t *testing.T) {
	stub := &inboxReplyPointerStub{}
	s := newClearInboxServer(t, stub, 0, true)

	result, err := s.toolInbox(context.Background(), map[string]any{})
	require.NoError(t, err)
	response := result.(map[string]any)
	require.Equal(t, 0, response["retained_reply_count"])
	require.NotContains(t, response, "replies_action")
	require.NotContains(t, response, "replies_check_error")
}

// TestSageInboxReplyPointerFailureIsNonFatal keeps a node that cannot answer
// the probe (older peer, Postgres backend, transient outage) from breaking the
// inbox itself. The failure is reported, not swallowed and not fatal.
func TestSageInboxReplyPointerFailureIsNonFatal(t *testing.T) {
	stub := &inboxReplyPointerStub{}
	s := newClearInboxServer(t, stub, 0, false)

	result, err := s.toolInbox(context.Background(), map[string]any{})
	require.NoError(t, err, "an unavailable reply probe must not break the inbox")
	response := result.(map[string]any)
	require.Contains(t, response, "replies_check_error",
		"a failed probe must be reported, never silently reported as zero replies")
	require.NotContains(t, response, "retained_reply_count",
		"a failed probe must not assert a count it could not read")
}

// TestSageInboxDefersReplyPointerWhenTheLimitIsFilled protects the request
// budget already pinned by TestUnifiedInboxTwentyFederatedReceiptsUsesFourLocalRequests:
// a limit-filled inbox defers the reply probe exactly as it defers task notices.
func TestSageInboxDefersReplyPointerWhenTheLimitIsFilled(t *testing.T) {
	stub := &inboxReplyPointerStub{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/messages/receive", func(w http.ResponseWriter, r *http.Request) {
		stub.record(r)
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	})
	mux.HandleFunc("/v1/pipe/inbox", func(w http.ResponseWriter, r *http.Request) {
		stub.record(r)
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{
			{"pipe_id": "work-1", "from_agent": "agent-a", "payload": "work one"},
			{"pipe_id": "work-2", "from_agent": "agent-a", "payload": "work two"},
		}, "count": 2})
	})
	mux.HandleFunc("/v1/pipe/results", func(w http.ResponseWriter, r *http.Request) {
		stub.record(r)
		t.Error("a limit-filled inbox must defer the reply probe, not spend a request on it")
		_ = json.NewEncoder(w).Encode(map[string]any{"count": 0, "retained": false})
	})
	mux.HandleFunc("/v1/dashboard/task-notifications", func(w http.ResponseWriter, r *http.Request) {
		stub.record(r)
		t.Error("a limit-filled inbox must defer task notices")
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	result, err := NewServer(ts.URL, priv).toolInbox(context.Background(), map[string]any{"limit": 2})
	require.NoError(t, err)
	response := result.(map[string]any)
	require.Equal(t, 2, response["count"])
	require.Equal(t, true, response["task_assignments_deferred"])
	require.NotContains(t, response, "retained_reply_count")
	require.Len(t, stub.calls(), 2, "receive + legacy inbox only")
}

// TestSageInboxReplyPointerCatchUpInstructionIsTrue pins inclusive polling at
// the advertised millisecond. Excluding equality loses a reply that lands after
// the probe but shares its SQLite timestamp; inclusion may repeat a boundary
// row, which callers can safely deduplicate by message_id.
func TestSageInboxReplyPointerCatchUpInstructionIsTrue(t *testing.T) {
	stub := &inboxReplyPointerStub{}
	s := newClearInboxServer(t, stub, 1, true)

	inbox, err := s.toolInbox(context.Background(), map[string]any{})
	require.NoError(t, err)
	response := inbox.(map[string]any)
	highWater := response["newest_reply_completed_at"].(string)
	note := response["replies_note"].(string)

	// A reply at the exact advertised high-water millisecond must remain visible.
	replyMux := http.NewServeMux()
	replyMux.HandleFunc("/v1/pipe/results", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{{
			"pipe_id": "msg-highwater", "to_agent": "recipient", "replied_by": "recipient",
			"intent": "review", "result": "the recipient's answer", "status": "completed",
			"completed_at": highWater,
		}}, "count": 1})
	})
	replyTS := httptest.NewServer(replyMux)
	t.Cleanup(replyTS.Close)
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	replyServer := NewServer(replyTS.URL, priv)

	echoed, err := replyServer.toolMessageReplies(context.Background(),
		map[string]any{"since": highWater})
	require.NoError(t, err)
	require.Equal(t, 1, echoed.(map[string]any)["count"],
		"inclusive since must not hide a later reply that shares the recorded millisecond")

	unfiltered, err := replyServer.toolMessageReplies(context.Background(), map[string]any{})
	require.NoError(t, err)
	require.Equal(t, 1, unfiltered.(map[string]any)["count"],
		"the unfiltered projection remains equivalent for this one-row fixture")

	require.Contains(t, note, "inclusive")
	require.Contains(t, note, "same millisecond")
	require.Contains(t, note, "deduplicate by message_id")
}

// TestSageInboxReplyPointerRejectsAKeylessBearerCallerBeforeSigning is the
// mirror of TestSageMessageRepliesRejectsAKeylessBearerCallerBeforeSigning for
// the inbox pointer, which reaches the SAME sender-exact route with the same
// signer. A pre-v23 keyless bearer token installs a token fingerprint but no
// per-token signing key, so prepareSignedRequest falls back to the node
// operator's key: without a guard the probe is signed as the operator and the
// operator's reply totals and newest_reply_completed_at are returned to a
// different declared identity. Contract item 2 is exact original sender only.
func TestSageInboxReplyPointerRejectsAKeylessBearerCallerBeforeSigning(t *testing.T) {
	stub := &inboxReplyPointerStub{}
	s := newClearInboxServer(t, stub, 9, true)

	var toolErr error
	var toolResult any
	handler := middleware.MCPBearerAuthMiddleware(func(
		_ context.Context, _, _ string,
	) (string, ed25519.PrivateKey, error) {
		return "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", nil, nil
	})(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		require.NotEmpty(t, middleware.ContextMCPTokenFingerprint(r.Context()),
			"precondition: the bearer middleware must bind a token fingerprint")
		require.Nil(t, middleware.ContextMCPSigner(r.Context()),
			"precondition: a legacy keyless token installs no signer")
		toolResult, toolErr = s.toolInbox(r.Context(), map[string]any{})
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/mcp", nil)
	req.Header.Set("Authorization", "Bearer legacy-keyless-token")
	handler.ServeHTTP(httptest.NewRecorder(), req)

	require.NoError(t, toolErr, "the pointer guard must not break the inbox itself")
	response, ok := toolResult.(map[string]any)
	require.True(t, ok)
	for _, leaked := range []string{
		"retained_reply_count", "retained_reply_count_is_unread",
		"newest_reply_completed_at", "replies_note",
	} {
		require.NotContains(t, response, leaked,
			"%s would report another identity's replies to a keyless bearer caller", leaked)
	}
	require.Contains(t, response, "replies_check_error",
		"a caller that may not be checked must be told so, never told it has no replies")
	require.Contains(t, response["replies_check_error"], "keyed token")

	stub.mu.Lock()
	require.Zero(t, stub.countOnlyCalls,
		"the identity guard must fire before the probe is signed, so nothing is read as the operator")
	stub.mu.Unlock()
	for _, call := range stub.calls() {
		require.NotContains(t, call, "/v1/pipe/results",
			"the sender-exact reply route must not be reached at all by a keyless bearer caller")
	}

	// Sanity: an ordinary stdio context still gets the pointer, so the assertions
	// above are about the guard and not about a broken stub.
	ok2, err := s.toolInbox(context.Background(), map[string]any{})
	require.NoError(t, err)
	require.Equal(t, 9, ok2.(map[string]any)["retained_reply_count"])
}

// TestSageInboxKeepsRepliesOutOfWorkCountsWhenMessagesExist is contract item 6
// in the presence of real work: the reply pointer must not inflate any count
// that tells the agent how much it owes.
func TestSageInboxKeepsRepliesOutOfWorkCountsWhenMessagesExist(t *testing.T) {
	stub := &inboxReplyPointerStub{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/messages/receive", func(w http.ResponseWriter, r *http.Request) {
		stub.record(r)
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{
			{"message_id": "local-a", "from_agent": "alice", "intent": "ask", "payload": "real work"},
		}, "count": 1})
	})
	mux.HandleFunc("/v1/messages/local-a/read", func(w http.ResponseWriter, r *http.Request) {
		stub.record(r)
		_ = json.NewEncoder(w).Encode(map[string]any{"read_status": "confirmed"})
	})
	mux.HandleFunc("/v1/pipe/inbox", func(w http.ResponseWriter, r *http.Request) {
		stub.record(r)
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	})
	mux.HandleFunc("/v1/pipe/results", func(w http.ResponseWriter, r *http.Request) {
		stub.record(r)
		require.Equal(t, "1", r.URL.Query().Get("count_only"))
		_ = json.NewEncoder(w).Encode(map[string]any{"count": 3, "retained": true})
	})
	mux.HandleFunc("/v1/dashboard/task-notifications", func(w http.ResponseWriter, r *http.Request) {
		stub.record(r)
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	result, err := NewServer(ts.URL, priv).toolInbox(context.Background(), map[string]any{"limit": 5})
	require.NoError(t, err)
	response := result.(map[string]any)

	require.Equal(t, 1, response["count"], "3 retained replies must not become 4 inbox items")
	require.Equal(t, 1, response["message_count"])
	require.Equal(t, 0, response["task_assignment_count"])
	require.Equal(t, 3, response["retained_reply_count"])
	items := response["items"].([]map[string]any)
	require.Len(t, items, 1)
	require.Equal(t, "local-a", items[0]["message_id"], "only genuine inbound work is an item")
	require.Contains(t, response["message"], "1 message(s) require sage_message_reply",
		"the work sentence must count only real inbound messages")
}
