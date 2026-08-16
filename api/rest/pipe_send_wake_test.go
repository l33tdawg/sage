package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

// The deprecated pipe route admitted exact-local canonical rows through a bare
// InsertPipeline, which allocates no wake generation. The row is real work the
// recipient can never be told about: the durable sequence never moves, so every
// "is this newer than what I last saw" consumer answers no, forever. These pin
// that an exact-local send advances the sequence atomically with the insert.

func sendPipe(t *testing.T, s *Server, caller string, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	rr := httptest.NewRecorder()
	pipeRouterAs(s, caller).ServeHTTP(rr,
		httptest.NewRequest(http.MethodPost, "/v1/pipe/send", bytes.NewReader(raw)))
	return rr
}

func wakeSeqOf(t *testing.T, st *store.SQLiteStore, agent string) uint64 {
	t.Helper()
	state, err := st.GetMessageWakeState(context.Background(), agent)
	require.NoError(t, err)
	return state.Seq
}

func TestPipeSendExactLocalAdvancesDurableWakeSequence(t *testing.T) {
	s, st := newPipeServer(t)
	addMessageAgent(t, st, "alice")
	addMessageAgent(t, st, "bob")

	// Start from a non-zero baseline so the assertion is n -> n+1 rather than
	// "any non-zero value", which a fresh allocation would satisfy by accident.
	first, _, err := st.SendLocalMessage(context.Background(), "seed",
		&store.PipelineMessage{PipeID: "msg-seed", FromAgent: "alice", ToAgent: "bob",
			Intent: "seed", Payload: "seed", Status: "pending"})
	require.NoError(t, err)
	require.NotNil(t, first)
	baseline := wakeSeqOf(t, st, "bob")
	require.Equal(t, uint64(1), baseline)

	var published []uint64
	broker := s.messageWakeBroker()
	subscription, err := broker.acquire("bob", "test-consumer")
	require.NoError(t, err)
	defer subscription.release()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for seq := range subscription.events {
			published = append(published, seq)
			return
		}
	}()

	rr := sendPipe(t, s, "alice", map[string]any{"to_agent": "bob", "payload": "unkeyed local work"})
	require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())

	require.Equal(t, baseline+1, wakeSeqOf(t, st, "bob"),
		"an exact-local send must advance the recipient's durable wake sequence")
	<-done
	require.Len(t, published, 1, "a fresh admission publishes exactly one wake")
	assert.Equal(t, baseline+1, published[0], "the published wake carries the new durable sequence")
}

func TestPipeSendKeyedExactLocalAdvancesAndPublishesOnce(t *testing.T) {
	s, st := newPipeServer(t)
	addMessageAgent(t, st, "alice")
	addMessageAgent(t, st, "bob")

	body := map[string]any{"to_agent": "bob", "payload": "keyed local work", "idempotency_key": "k-1"}
	first := sendPipe(t, s, "alice", body)
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())
	afterFirst := wakeSeqOf(t, st, "bob")
	require.Equal(t, uint64(1), afterFirst)

	replay := sendPipe(t, s, "alice", body)
	require.Contains(t, []int{http.StatusOK, http.StatusCreated}, replay.Code, replay.Body.String())
	var replayBody map[string]any
	require.NoError(t, json.Unmarshal(replay.Body.Bytes(), &replayBody))
	assert.Equal(t, true, replayBody["idempotent_replay"], "a keyed replay must report itself as one")
	assert.Equal(t, afterFirst, wakeSeqOf(t, st, "bob"),
		"a replay must not advance the sequence a second time")
}

func TestPipeSendNonExactLocalDoesNotAllocateASequence(t *testing.T) {
	s, st := newPipeServer(t)
	addMessageAgent(t, st, "alice")
	addMessageAgent(t, st, "bob")

	// Provider-addressed and unresolved: there is no exact recipient to
	// allocate a sequence for, so none may be allocated.
	rr := sendPipe(t, s, "alice", map[string]any{"to_provider": "nobody-here", "payload": "unresolved"})
	require.NotEqual(t, http.StatusCreated, rr.Code, rr.Body.String())
	assert.Zero(t, wakeSeqOf(t, st, "bob"), "an unresolved provider send must not wake any local agent")
}
