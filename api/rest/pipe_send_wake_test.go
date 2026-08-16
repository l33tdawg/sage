package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

// The deprecated pipe route admitted exact-local canonical rows through a bare
// InsertPipeline, which allocates no wake generation. The durable sequence never
// moves, so wake consumers cannot observe a newer generation for that recipient
// — legacy polling can still discover the row, which is why the narrow claim is
// the accurate one. These pin that an exact-local send allocates atomically.

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

// watchWake subscribes to the exact-agent broker before the send under test, so
// a MISSING publication fails on a bounded deadline instead of hanging.
func watchWake(t *testing.T, s *Server, agent string) (next func(time.Duration) (uint64, bool), release func()) {
	t.Helper()
	subscription, err := s.messageWakeBroker().acquire(agent, "wake-test-consumer")
	require.NoError(t, err)
	return func(d time.Duration) (uint64, bool) {
		select {
		case seq := <-subscription.events:
			return seq, true
		case <-time.After(d):
			return 0, false
		}
	}, subscription.release
}

func TestPipeSendExactLocalAdvancesDurableWakeSequence(t *testing.T) {
	s, st := newPipeServer(t)
	addMessageAgent(t, st, "alice")
	addMessageAgent(t, st, "bob")

	// Start from a non-zero baseline so this asserts n -> n+1 rather than "some
	// non-zero value", which a fresh allocation would satisfy by accident.
	_, _, err := st.SendLocalMessage(context.Background(), "seed",
		&store.PipelineMessage{PipeID: "msg-seed", FromAgent: "alice", ToAgent: "bob",
			Intent: "seed", Payload: "seed", Status: "pending"})
	require.NoError(t, err)
	baseline := wakeSeqOf(t, st, "bob")
	require.Equal(t, uint64(1), baseline)

	next, release := watchWake(t, s, "bob")
	defer release()

	rr := sendPipe(t, s, "alice", map[string]any{"to_agent": "bob", "payload": "unkeyed local work"})
	require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())

	require.Equal(t, baseline+1, wakeSeqOf(t, st, "bob"),
		"an exact-local send must advance the recipient's durable wake sequence")
	seq, got := next(2 * time.Second)
	require.True(t, got, "a fresh admission must publish a wake")
	assert.Equal(t, baseline+1, seq, "the published wake carries the newly allocated generation")
}

func TestPipeSendKeyedExactLocalPublishesExactlyOnceAcrossReplay(t *testing.T) {
	s, st := newPipeServer(t)
	addMessageAgent(t, st, "alice")
	addMessageAgent(t, st, "bob")

	next, release := watchWake(t, s, "bob")
	defer release()

	body := map[string]any{"to_agent": "bob", "payload": "keyed local work", "idempotency_key": "k-1"}
	first := sendPipe(t, s, "alice", body)
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())
	require.Equal(t, uint64(1), wakeSeqOf(t, st, "bob"))

	seq, got := next(2 * time.Second)
	require.True(t, got, "the fresh keyed admission must publish")
	require.Equal(t, uint64(1), seq)

	replay := sendPipe(t, s, "alice", body)
	require.Equal(t, http.StatusOK, replay.Code, replay.Body.String())
	var replayBody map[string]any
	require.NoError(t, json.Unmarshal(replay.Body.Bytes(), &replayBody))
	assert.Equal(t, true, replayBody["idempotent_replay"], "a keyed replay must report itself as one")
	assert.Equal(t, uint64(1), wakeSeqOf(t, st, "bob"), "a replay must not advance the sequence")

	_, second := next(500 * time.Millisecond)
	assert.False(t, second, "a replay must NOT publish a second wake")
}

// Provider-addressed work has no exact local recipient to allocate for. The
// earlier version of this test sent to an unknown provider, which validation
// rejected before admission — so it proved nothing about the insertion branch.
// This one succeeds with 201 and still must allocate nothing.
func TestPipeSendProviderAddressedAllocatesNoExactRecipientSequence(t *testing.T) {
	s, st := newPipeServer(t)
	addMessageAgent(t, st, "alice")
	addMessageAgent(t, st, "bob")

	rr := sendPipe(t, s, "alice", map[string]any{"to_provider": "test", "payload": "provider-addressed work"})
	require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())

	assert.Zero(t, wakeSeqOf(t, st, "bob"),
		"a provider-addressed admission must not allocate a sequence for a concrete agent")
	assert.Zero(t, wakeSeqOf(t, st, "alice"))
}

// A store that cannot allocate a generation must REFUSE an exact-local
// admission rather than fall back to a bare insert. Falling back would return
// 201 for work the wake contract cannot represent — recreating the very
// invariant violation this change removes, under a capability gap.
type wakelessPipeStore struct {
	store.MemoryStore
	store.PipelineStore
}

func TestPipeSendExactLocalFailsClosedWithoutMessageStore(t *testing.T) {
	_, st := newPipeServer(t)
	addMessageAgent(t, st, "alice")
	addMessageAgent(t, st, "bob")
	s := &Server{
		store:      wakelessPipeStore{MemoryStore: st, PipelineStore: st},
		agentStore: st,
		logger:     zerolog.Nop(),
	}

	rr := sendPipe(t, s, "alice", map[string]any{"to_agent": "bob", "payload": "must not be admitted"})
	require.Equal(t, http.StatusNotImplemented, rr.Code, rr.Body.String())

	pipes, err := st.ListPipelines(context.Background(), "", 10)
	require.NoError(t, err)
	assert.Empty(t, pipes, "a refused admission must write no row")
	assert.Zero(t, wakeSeqOf(t, st, "bob"))
}
