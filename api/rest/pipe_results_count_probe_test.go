package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

// v11.18.2 additive count_only probe on GET /v1/pipe/results. It exists so the
// unified inbox can carry a payload-free scalar pointer ("you have replies
// waiting") without ever pulling reply bodies, intents, or a second page into
// the model's context.

func decodeReplyCountProbe(t *testing.T, rr *httptest.ResponseRecorder) (count int, retained bool) {
	t.Helper()
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var probe struct {
		Count    int  `json:"count"`
		Retained bool `json:"retained"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &probe))
	return probe.Count, probe.Retained
}

// TestPipeResultsCountOnlyCarriesTheNewestReplyInstant covers the field that
// makes a never-decreasing retained total usable. The count alone can only grow
// and can never say "you are caught up"; newest_completed_at is what a caller
// feeds back as `since` to get a genuinely empty page, with no server-held read
// state and therefore no loss of replay safety.
func TestPipeResultsCountOnlyCarriesTheNewestReplyInstant(t *testing.T) {
	s, memStore := newPipeServer(t)
	ctx := context.Background()
	now := time.Now().UTC()

	var probe struct {
		Count             int    `json:"count"`
		Retained          bool   `json:"retained"`
		NewestCompletedAt string `json:"newest_completed_at"`
	}
	empty := getPipeResultsAs(t, s, replyRESTSender, "?count_only=1")
	require.Equal(t, http.StatusOK, empty.Code)
	require.NoError(t, json.Unmarshal(empty.Body.Bytes(), &probe))
	assert.Zero(t, probe.Count)
	assert.Empty(t, probe.NewestCompletedAt,
		"with no replies there is no instant to report and none must be invented")

	for _, id := range []string{"msg-newest-a", "msg-newest-b"} {
		require.NoError(t, memStore.InsertPipeline(ctx, &store.PipelineMessage{
			PipeID: id, FromAgent: replyRESTSender, ToAgent: replyRESTRecipient,
			Intent: "review", Payload: replyRESTPayload, Status: "pending",
			CreatedAt: now, ExpiresAt: now.Add(time.Hour),
		}))
		require.NoError(t, memStore.ClaimPipeline(ctx, id, replyRESTRecipient))
		require.NoError(t, memStore.CompletePipeline(ctx, id, replyRESTRecipient, replyRESTResult, ""))
		time.Sleep(2 * time.Millisecond)
	}

	rr := getPipeResultsAs(t, s, replyRESTSender, "?count_only=1")
	require.Equal(t, http.StatusOK, rr.Code)
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &probe))
	assert.Equal(t, 2, probe.Count)
	require.NotEmpty(t, probe.NewestCompletedAt)

	// It is a timestamp, not an identifier: no message id, intent, or body.
	body := rr.Body.String()
	assert.NotContains(t, body, "msg-newest", "the probe must stay free of message identifiers")
	assert.NotContains(t, body, replyRESTResult)
	assert.NotContains(t, body, replyRESTPayload)

	// It matches the newest row the projection returns, so feeding it back as a
	// filter cannot skip a reply.
	items, _ := decodePipeResults(t, getPipeResultsAs(t, s, replyRESTSender, "?limit=20"))
	require.Len(t, items, 2)
	newest, err := time.Parse(time.RFC3339Nano, probe.NewestCompletedAt)
	require.NoError(t, err)
	projected, err := time.Parse(time.RFC3339Nano, items[0]["completed_at"].(string))
	require.NoError(t, err)
	assert.WithinDuration(t, projected, newest, time.Millisecond)

	// Another agent learns nothing, not even the instant.
	for _, other := range []string{replyRESTRecipient, "unrelated-agent", "root", ""} {
		otherRR := getPipeResultsAs(t, s, other, "?count_only=1")
		require.Equal(t, http.StatusOK, otherRR.Code)
		var otherProbe struct {
			Count             int    `json:"count"`
			NewestCompletedAt string `json:"newest_completed_at"`
		}
		require.NoError(t, json.Unmarshal(otherRR.Body.Bytes(), &otherProbe))
		assert.Zero(t, otherProbe.Count, "caller %q must not be told a reply exists", other)
		assert.Empty(t, otherProbe.NewestCompletedAt,
			"caller %q must not learn when another agent's reply landed", other)
	}
}

// TestPipeResultsCountOnlyIsPayloadFreeAndSenderExact is the probe's whole
// contract: it answers only for the exact original sender, and its body carries
// no reply text, no intent, and no message identifiers.
func TestPipeResultsCountOnlyIsPayloadFreeAndSenderExact(t *testing.T) {
	s, memStore := newPipeServer(t)
	ctx := context.Background()
	now := time.Now().UTC()
	require.NoError(t, memStore.InsertPipeline(ctx, &store.PipelineMessage{
		PipeID: "pipe-count-probe", FromAgent: replyRESTSender, ToAgent: replyRESTRecipient,
		Intent: "DISTINCTIVE INTENT", Payload: replyRESTPayload, Status: "pending",
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}))

	zero, retained := decodeReplyCountProbe(t, getPipeResultsAs(t, s, replyRESTSender, "?count_only=1"))
	assert.Zero(t, zero, "a pending request is not a retained reply")
	assert.False(t, retained)

	require.NoError(t, memStore.ClaimPipeline(ctx, "pipe-count-probe", replyRESTRecipient))
	require.NoError(t, memStore.CompletePipeline(ctx, "pipe-count-probe", replyRESTRecipient, replyRESTResult, ""))

	rr := getPipeResultsAs(t, s, replyRESTSender, "?count_only=1")
	count, retained := decodeReplyCountProbe(t, rr)
	assert.Equal(t, 1, count)
	assert.True(t, retained)
	body := rr.Body.String()
	assert.NotContains(t, body, replyRESTResult, "the probe must not carry the reply body")
	assert.NotContains(t, body, replyRESTPayload, "the probe must not carry the request payload")
	assert.NotContains(t, body, "DISTINCTIVE INTENT", "the probe must not carry the retained intent")
	assert.NotContains(t, body, "pipe-count-probe", "the probe must not carry message identifiers")
	assert.NotContains(t, body, "items", "the probe must not return a collection")

	for _, other := range []string{replyRESTRecipient, "unrelated-agent", "root", ""} {
		otherCount, otherRetained := decodeReplyCountProbe(t, getPipeResultsAs(t, s, other, "?count_only=1"))
		assert.Zero(t, otherCount, "caller %q must not be told a reply exists for it", other)
		assert.False(t, otherRetained)
	}
}

// TestPipeResultsCountOnlyIsPassiveAndRepeatable keeps the probe safe to call
// on every sage_inbox: it is a retained total, not an unread counter, so a
// repeat call must return the same number and must not consume anything.
func TestPipeResultsCountOnlyIsPassiveAndRepeatable(t *testing.T) {
	s, memStore := newPipeServer(t)
	ctx := context.Background()
	now := time.Now().UTC()
	for _, id := range []string{"pipe-count-repeat-a", "pipe-count-repeat-b"} {
		require.NoError(t, memStore.InsertPipeline(ctx, &store.PipelineMessage{
			PipeID: id, FromAgent: replyRESTSender, ToAgent: replyRESTRecipient,
			Intent: "review", Payload: replyRESTPayload, Status: "pending",
			CreatedAt: now, ExpiresAt: now.Add(time.Hour),
		}))
		require.NoError(t, memStore.ClaimPipeline(ctx, id, replyRESTRecipient))
		require.NoError(t, memStore.CompletePipeline(ctx, id, replyRESTRecipient, replyRESTResult, ""))
	}

	first := getPipeResultsAs(t, s, replyRESTSender, "?count_only=1")
	second := getPipeResultsAs(t, s, replyRESTSender, "?count_only=1")
	assert.JSONEq(t, first.Body.String(), second.Body.String(),
		"a retained total must not decay into an unread counter")
	count, retained := decodeReplyCountProbe(t, second)
	assert.Equal(t, 2, count)
	assert.True(t, retained)
	assert.NotContains(t, second.Body.String(), "items",
		"the probe answers with a scalar, never a page of replies")

	// The full projection still agrees with the probe.
	items, _ := decodePipeResults(t, getPipeResultsAs(t, s, replyRESTSender, "?limit=20"))
	assert.Len(t, items, count)
}

// replyCounterlessStore implements the pipeline surface WITHOUT the optional
// PipelineResultCounter, mirroring PostgresStore. A backend that cannot answer
// the probe must report a capability gap, never a 200 that would make the inbox
// silently claim there are no replies.
type replyCounterlessStore struct {
	store.MemoryStore
	store.PipelineStore
}

func TestPipeResultsCountOnlyReportsCapabilityGapNotSilentZero(t *testing.T) {
	s, memStore := newPipeServer(t)
	ctx := context.Background()
	now := time.Now().UTC()
	require.NoError(t, memStore.InsertPipeline(ctx, &store.PipelineMessage{
		PipeID: "pipe-count-gap", FromAgent: replyRESTSender, ToAgent: replyRESTRecipient,
		Intent: "review", Payload: replyRESTPayload, Status: "pending",
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}))
	require.NoError(t, memStore.ClaimPipeline(ctx, "pipe-count-gap", replyRESTRecipient))
	require.NoError(t, memStore.CompletePipeline(ctx, "pipe-count-gap", replyRESTRecipient, replyRESTResult, ""))

	s.store = &replyCounterlessStore{MemoryStore: memStore, PipelineStore: memStore}
	rr := getPipeResultsAs(t, s, replyRESTSender, "?count_only=1")
	require.Equal(t, http.StatusNotImplemented, rr.Code,
		"a backend without the counter must report a capability gap, not 200 count=0 and not 500")
	assert.NotContains(t, rr.Body.String(), replyRESTResult)

	// The full projection is unaffected: it only needs PipelineStore.
	items, _ := decodePipeResults(t, getPipeResultsAs(t, s, replyRESTSender, "?limit=5"))
	require.Len(t, items, 1)
}

// unsupportedReplyProjectionStore models a backend (PostgresStore today) whose
// GetCompletedForSender is a stub. The route must report a capability gap so a
// caller can tell "this node cannot do replies" from "you have no replies".
type unsupportedReplyProjectionStore struct {
	store.MemoryStore
	store.PipelineStore
}

func (unsupportedReplyProjectionStore) GetCompletedForSender(
	context.Context, string, int,
) ([]*store.PipelineMessage, error) {
	return nil, fmt.Errorf("GetCompletedForSender: %w", store.ErrPipelineUnsupported)
}

// lockedVaultReplyStore models a node whose content vault is locked. Replies
// are encrypted at rest, so the route must say "retry later", not "no replies".
type lockedVaultReplyStore struct {
	store.MemoryStore
	store.PipelineStore
}

func (lockedVaultReplyStore) GetCompletedForSender(
	context.Context, string, int,
) ([]*store.PipelineMessage, error) {
	return nil, fmt.Errorf("read replies: %w", store.ErrPipeContentUnavailable)
}

func TestPipeResultsDistinguishesCapabilityGapAndLockedVaultFromNoReplies(t *testing.T) {
	s, memStore := newPipeServer(t)

	s.store = unsupportedReplyProjectionStore{MemoryStore: memStore, PipelineStore: memStore}
	unsupported := getPipeResultsAs(t, s, replyRESTSender, "")
	require.Equal(t, http.StatusNotImplemented, unsupported.Code,
		"a backend without the projection must not answer 200 with an empty list")

	s.store = lockedVaultReplyStore{MemoryStore: memStore, PipelineStore: memStore}
	locked := getPipeResultsAs(t, s, replyRESTSender, "")
	require.Equal(t, http.StatusServiceUnavailable, locked.Code,
		"a locked content vault must be a retryable condition, not an empty reply list")
	assert.Contains(t, locked.Body.String(), "safe to repeat",
		"the locked-vault answer must tell the caller this passive read can be retried")
}
