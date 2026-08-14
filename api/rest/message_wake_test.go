package rest

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

func TestMessageWakeBrokerIsExactCoalescedAndSingleConsumer(t *testing.T) {
	broker := newMessageWakeBroker(time.Minute)
	bob, err := broker.acquire("bob", "supervisor")
	require.NoError(t, err)
	t.Cleanup(bob.release)
	mallory, err := broker.acquire("mallory", "supervisor")
	require.NoError(t, err)
	t.Cleanup(mallory.release)

	broker.publish("bob", 1)
	broker.publish("bob", 2)
	require.Equal(t, uint64(2), <-bob.events,
		"a bounded slow consumer retains only the newest monotonic sequence")
	select {
	case seq := <-mallory.events:
		t.Fatalf("exact-recipient wake crossed principals with seq %d", seq)
	default:
	}

	_, err = broker.acquire("bob", "different-runtime")
	require.ErrorIs(t, err, errMessageWakeLeaseHeld)
	replacement, err := broker.acquire("bob", "supervisor")
	require.NoError(t, err, "the same consumer_id may safely supersede its stale stream")
	t.Cleanup(replacement.release)
	select {
	case <-bob.done:
	case <-time.After(time.Second):
		t.Fatal("same-consumer reconnect did not cancel its prior stream")
	}
	bob.release() // stale release must not delete the replacement lease.
	_, err = broker.acquire("bob", "different-runtime")
	require.ErrorIs(t, err, errMessageWakeLeaseHeld)
	replacement.release()
	other, err := broker.acquire("bob", "different-runtime")
	require.NoError(t, err)
	other.release()
}

func TestMessageWakeWireContractAndProductionBoundsAreFixed(t *testing.T) {
	require.Equal(t, 15*time.Second, messageWakeHeartbeat,
		"heartbeat inflation can strand half-open supervisor connections")
	require.Equal(t, 5*time.Minute, messageWakeLeaseTTL,
		"lease inflation can strand a replacement single consumer")
	require.Equal(t, 5*time.Second, messageWakeWriteBudget,
		"write-budget inflation can let a slow client pin the handler")
	require.Equal(t, 128, maxMessageWakeConsumerBytes)

	recorder := httptest.NewRecorder()
	require.NoError(t, writeMessageWakeEvent(recorder, store.MessageWakeState{Seq: 7, Pending: true}))
	require.Equal(t, "id: 7\nevent: wake\ndata: {\"version\":1,\"seq\":7,\"pending\":true}\n\n", recorder.Body.String(),
		"the supervisor contract is byte-stable and contains no extensible message metadata")
}

func TestMessageWakeBrokerLeaseExpires(t *testing.T) {
	broker := newMessageWakeBroker(25 * time.Millisecond)
	first, err := broker.acquire("bob", "first")
	require.NoError(t, err)
	select {
	case <-first.done:
	case <-time.After(time.Second):
		t.Fatal("wake lease did not expire within the bounded test TTL")
	}
	second, err := broker.acquire("bob", "second")
	require.NoError(t, err, "TTL expiry must release the exact-agent lease")
	second.release()
}

func TestMessageSendPublishesWakeOnlyAfterFreshCommit(t *testing.T) {
	s, sqlite := newPipeServer(t)
	addMessageAgent(t, sqlite, "alice")
	addMessageAgent(t, sqlite, "bob")
	subscription, err := s.messageWakeBroker().acquire("bob", "observer")
	require.NoError(t, err)
	defer subscription.release()
	body := map[string]any{
		"to_agent": "bob", "payload": "work", "idempotency_key": "publish-once",
	}
	first := callMessageJSON(t, messageRouterAs(s, "alice", true), http.MethodPost, "/v1/messages", body)
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())
	select {
	case seq := <-subscription.events:
		require.Equal(t, uint64(1), seq)
	case <-time.After(time.Second):
		t.Fatal("fresh committed admission did not publish its durable sequence")
	}
	replay := callMessageJSON(t, messageRouterAs(s, "alice", true), http.MethodPost, "/v1/messages", body)
	require.Equal(t, http.StatusOK, replay.Code, replay.Body.String())
	select {
	case seq := <-subscription.events:
		t.Fatalf("idempotent replay emitted a second wake sequence %d", seq)
	case <-time.After(50 * time.Millisecond):
	}
}

func readWakeEvent(t *testing.T, body io.Reader) (string, map[string]any) {
	t.Helper()
	scanner := bufio.NewScanner(body)
	var id string
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "id: "):
			id = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "data: "):
			var payload map[string]any
			require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &payload))
			return id, payload
		}
	}
	require.NoError(t, scanner.Err())
	t.Fatal("wake SSE closed without an event")
	return "", nil
}

func TestMessageWakeSSECatchUpReconnectAndExactPayload(t *testing.T) {
	s, sqlite := newPipeServer(t)
	addMessageAgent(t, sqlite, "alice")
	addMessageAgent(t, sqlite, "bob")
	firstSend := callMessageJSON(t, messageRouterAs(s, "alice", true), http.MethodPost, "/v1/messages", map[string]any{
		"to_agent": "bob", "payload": "secret-one", "idempotency_key": "wake-sse-one",
	})
	require.Equal(t, http.StatusCreated, firstSend.Code, firstSend.Body.String())

	bobServer := httptest.NewServer(messageRouterAs(s, "bob", true))
	defer bobServer.Close()
	response, err := http.Get(bobServer.URL + "/v1/messages/wake?after_seq=0&consumer_id=supervisor") //nolint:gosec
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Equal(t, "text/event-stream", response.Header.Get("Content-Type"))
	id, payload := readWakeEvent(t, response.Body)
	require.Equal(t, "1", id)
	require.Equal(t, map[string]any{"version": float64(1), "seq": float64(1), "pending": true}, payload)
	require.Len(t, payload, 3, "wake payload must contain exactly version/seq/pending")
	require.NoError(t, response.Body.Close())

	reconnect, err := http.NewRequest(http.MethodGet,
		bobServer.URL+"/v1/messages/wake?consumer_id=supervisor", nil)
	require.NoError(t, err)
	reconnect.Header.Set("Last-Event-ID", "1")
	reconnected, err := http.DefaultClient.Do(reconnect)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, reconnected.StatusCode)
	defer reconnected.Body.Close() //nolint:errcheck

	secondSend := callMessageJSON(t, messageRouterAs(s, "alice", true), http.MethodPost, "/v1/messages", map[string]any{
		"to_agent": "bob", "payload": "secret-two", "idempotency_key": "wake-sse-two",
	})
	require.Equal(t, http.StatusCreated, secondSend.Code, secondSend.Body.String())
	id, payload = readWakeEvent(t, reconnected.Body)
	require.Equal(t, "2", id)
	require.Equal(t, map[string]any{"version": float64(1), "seq": float64(2), "pending": true}, payload)
	require.NotContains(t, string(mustJSON(t, payload)), "secret")
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	return raw
}

func TestMessageWakeAuthCursorAndLeaseFailuresAreLoud(t *testing.T) {
	s, sqlite := newPipeServer(t)
	addMessageAgent(t, sqlite, "alice")
	addMessageAgent(t, sqlite, "bob")
	addMessageAgent(t, sqlite, "mallory")

	unsigned := callMessageJSON(t, messageRouterAs(s, "bob", false), http.MethodGet,
		"/v1/messages/wake?after_seq=0&consumer_id=unsigned", nil)
	require.Equal(t, http.StatusForbidden, unsigned.Code)
	missingConsumer := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodGet,
		"/v1/messages/wake?after_seq=0", nil)
	require.Equal(t, http.StatusBadRequest, missingConsumer.Code)
	conflictingCursor := httptest.NewRequest(http.MethodGet,
		"/v1/messages/wake?after_seq=2&consumer_id=cursor", nil)
	conflictingCursor.Header.Set("Last-Event-ID", "1")
	conflicting := httptest.NewRecorder()
	messageRouterAs(s, "bob", true).ServeHTTP(conflicting, conflictingCursor)
	require.Equal(t, http.StatusBadRequest, conflicting.Code)
	sent := callMessageJSON(t, messageRouterAs(s, "alice", true), http.MethodPost, "/v1/messages", map[string]any{
		"to_agent": "bob", "payload": "bob-only", "idempotency_key": "exact-wake",
	})
	require.Equal(t, http.StatusCreated, sent.Code, sent.Body.String())
	malloryCannotUseBobSeq := callMessageJSON(t, messageRouterAs(s, "mallory", true), http.MethodGet,
		"/v1/messages/wake?after_seq=1&consumer_id=mallory", nil)
	require.Equal(t, http.StatusConflict, malloryCannotUseBobSeq.Code,
		"the cursor is checked against the signed caller, never another recipient")
	ahead := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodGet,
		"/v1/messages/wake?after_seq=2&consumer_id=ahead", nil)
	require.Equal(t, http.StatusConflict, ahead.Code)

	server := httptest.NewServer(messageRouterAs(s, "bob", true))
	defer server.Close()
	first, err := http.Get(server.URL + "/v1/messages/wake?after_seq=0&consumer_id=first") //nolint:gosec
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, first.StatusCode)
	defer first.Body.Close()                                                                 //nolint:errcheck
	second, err := http.Get(server.URL + "/v1/messages/wake?after_seq=0&consumer_id=second") //nolint:gosec
	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, second.StatusCode)
	require.Equal(t, "2", second.Header.Get("Retry-After"))
	require.NoError(t, second.Body.Close())
}

func TestMessageWakeSSEEmitsHeartbeatWithoutInventingEvent(t *testing.T) {
	s, sqlite := newPipeServer(t)
	addMessageAgent(t, sqlite, "bob")
	s.messageWakeHeartbeat = 10 * time.Millisecond
	server := httptest.NewServer(messageRouterAs(s, "bob", true))
	defer server.Close()
	response, err := http.Get(server.URL + "/v1/messages/wake?after_seq=0&consumer_id=heartbeat") //nolint:gosec
	require.NoError(t, err)
	defer response.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, response.StatusCode)
	scanner := bufio.NewScanner(response.Body)
	require.True(t, scanner.Scan())
	require.Equal(t, ": heartbeat", scanner.Text())
	require.True(t, scanner.Scan())
	require.Empty(t, scanner.Text())
}
