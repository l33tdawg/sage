package rest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"time"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taskidempotency"
)

type taskIdempotencyLock struct {
	gate chan struct{}
	refs int
}

// acquireTaskIdempotencyLock coalesces concurrent requests for one
// principal/key pair. Entries exist only while a request is active or waiting,
// so attacker-chosen keys cannot create a process-lifetime cache.
func (s *Server) acquireTaskIdempotencyLock(principalID, key string) func() {
	sum, _ := taskidempotency.BindingKeyDigest(principalID, key)
	lockKey := taskidempotency.Hex(sum)
	s.taskIdempotencyMu.Lock()
	if s.taskIdempotencyLocks == nil {
		s.taskIdempotencyLocks = make(map[string]*taskIdempotencyLock)
	}
	entry := s.taskIdempotencyLocks[lockKey]
	if entry == nil {
		entry = &taskIdempotencyLock{gate: make(chan struct{}, 1)}
		entry.gate <- struct{}{}
		s.taskIdempotencyLocks[lockKey] = entry
	}
	entry.refs++
	s.taskIdempotencyMu.Unlock()

	<-entry.gate
	return func() {
		entry.gate <- struct{}{}
		s.taskIdempotencyMu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(s.taskIdempotencyLocks, lockKey)
		}
		s.taskIdempotencyMu.Unlock()
	}
}

func (s *Server) matchingTaskIdempotencyBinding(
	req SubmitMemoryRequest,
	principalID, assigneeID, payloadDigest string,
) (*store.AppV23TaskIdempotencyBinding, error) {
	if s.badgerStore == nil {
		return nil, errors.New("consensus task idempotency state is unavailable")
	}
	binding, err := s.badgerStore.GetAppV23TaskIdempotencyBinding(
		principalID, req.IdempotencyKey,
	)
	if err != nil || binding == nil {
		return binding, err
	}
	if binding.PrincipalID != principalID ||
		binding.AssigneeID != assigneeID ||
		binding.PayloadDigest != payloadDigest {
		return nil, store.ErrAppV23TaskIdempotencyConflict
	}
	expectedMemoryID, err := taskidempotency.MemoryID(principalID, req.IdempotencyKey)
	if err != nil || binding.MemoryID != expectedMemoryID {
		return nil, store.ErrAppV23TaskIdempotencyConflict
	}
	contentHash, _, err := s.badgerStore.GetMemoryHash(binding.MemoryID)
	if err != nil {
		return nil, err
	}
	expectedHash := sha256.Sum256([]byte(req.Content))
	if !bytes.Equal(contentHash, expectedHash[:]) {
		return nil, store.ErrAppV23TaskIdempotencyConflict
	}
	domain, err := s.badgerStore.GetMemoryDomain(binding.MemoryID)
	if err != nil || domain != req.DomainTag {
		return nil, store.ErrAppV23TaskIdempotencyConflict
	}
	classification, err := s.badgerStore.GetMemoryClassification(binding.MemoryID)
	if err != nil || classification != uint8(req.Classification) {
		return nil, store.ErrAppV23TaskIdempotencyConflict
	}
	authorPrincipal, err := s.badgerStore.GetMemoryAuthorPrincipal(binding.MemoryID)
	if err != nil || authorPrincipal != principalID {
		return nil, store.ErrAppV23TaskIdempotencyConflict
	}
	return binding, nil
}

func canonicalTaskTagsEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

// reconcileCommittedTaskTags verifies the local serving projection against
// the exact canonical tag set covered by the committed payload digest. The
// request is safe repair material only after matchingTaskIdempotencyBinding
// has proved that digest: an arbitrary retry can never retag a committed task.
//
// New app-v23 keyed tasks write tags in Commit's atomic projection batch. The
// repair branch remains necessary for tasks committed by an older binary in
// the historical post-Commit gap, or for an operator-restored projection.
func (s *Server) reconcileCommittedTaskTags(memoryID string, expected []string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	current, err := s.store.GetTags(ctx, memoryID)
	if err == nil && canonicalTaskTagsEqual(current, expected) {
		return true
	}
	if setErr := s.store.SetTags(ctx, memoryID, expected); setErr != nil {
		return false
	}
	current, err = s.store.GetTags(ctx, memoryID)
	return err == nil && canonicalTaskTagsEqual(current, expected)
}

// writeTaskIdempotencyReplayIfCommitted returns true whenever it wrote a
// terminal HTTP response. An absent binding lets the caller become the leader;
// a mismatch is a stable 409; unreadable consensus proof fails closed.
func (s *Server) writeTaskIdempotencyReplayIfCommitted(
	w http.ResponseWriter,
	req SubmitMemoryRequest,
	principalID, assigneeID, payloadDigest string,
) bool {
	binding, err := s.matchingTaskIdempotencyBinding(
		req, principalID, assigneeID, payloadDigest,
	)
	if errors.Is(err, store.ErrAppV23TaskIdempotencyConflict) {
		writeProblem(w, http.StatusConflict, "Task idempotency conflict", "This idempotency_key is already bound to a different canonical task payload.")
		return true
	}
	if err != nil {
		writeProblem(w, http.StatusServiceUnavailable, "Task idempotency state unavailable", "The node cannot verify the authoritative task receipt; no retry was broadcast.")
		return true
	}
	if binding == nil {
		return false
	}
	currentTaskStatus, confirmed := s.confirmCommittedTaskRecord(
		binding.MemoryID,
		req.DomainTag,
		binding.AssigneeID,
	)
	if confirmed {
		confirmed = s.reconcileCommittedTaskTags(binding.MemoryID, req.Tags)
	}
	response := SubmitMemoryResponse{
		MemoryID:            binding.MemoryID,
		TxHash:              binding.TxHash,
		Status:              string(memory.StatusProposed),
		TaskStatus:          string(currentTaskStatus),
		Committed:           true,
		CommittedHeight:     binding.CommittedHeight,
		ProjectionConfirmed: &confirmed,
		IdempotencyKey:      req.IdempotencyKey,
		IdempotentReplay:    true,
		IgnoredFields:       req.ignoredFields,
	}
	if !confirmed {
		retryable := false
		response.Status = "committed_unconfirmed"
		response.Retryable = &retryable
		response.Message = "The transaction committed, but the exact task projection could not be confirmed. Reconcile this memory_id; do not resubmit the task."
		writeJSON(w, http.StatusAccepted, response)
		return true
	}
	writeJSON(w, http.StatusOK, response)
	return true
}
