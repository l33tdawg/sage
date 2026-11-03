package rest

import (
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taskidempotency"
	"github.com/l33tdawg/sage/internal/tx"
)

// The real Badger receipt and canonical envelope exercise the pre-broadcast
// replay path. This is not a signed-route, parent-admission, or Comet fixture.
func TestTaskRESTReplayRejectsChangedDerivedFrom(t *testing.T) {
	srv, badger, principal, _, _ := setupAppV23RESTAccess(t)
	const key = "lineage-task"
	memoryID, err := taskidempotency.MemoryID(principal, key)
	require.NoError(t, err)
	contentHash := sha256.Sum256([]byte("lineage task"))
	ref := tx.ParentRef{
		SourceChainID: "chain", MemoryID: "parent",
		ContentHash: sha256.Sum256([]byte("parent")),
	}
	submit := &tx.MemorySubmit{
		MemoryID: memoryID, ContentHash: contentHash[:],
		MemoryType: tx.MemoryTypeTask, DomainTag: "member.home",
		Content: "lineage task", TaskStatus: "planned",
		Classification: tx.ClearanceInternal, DerivedFrom: []tx.ParentRef{ref},
	}
	digest, err := taskidempotency.PayloadDigest(principal, principal, submit)
	require.NoError(t, err)
	keyDigest, err := taskidempotency.BindingKeyDigest(principal, key)
	require.NoError(t, err)
	binding := &store.AppV23TaskIdempotencyBinding{
		Version: 1, PrincipalID: principal, BindingKeyDigest: taskidempotency.Hex(keyDigest),
		PayloadDigest: taskidempotency.Hex(digest), MemoryID: memoryID, AssigneeID: principal,
		CommittedHeight: 7, TxHash: strings.Repeat("A", 64),
	}
	require.NoError(t, badger.SetAppV23TaskIdempotencyBinding(principal, key, binding))
	require.NoError(t, badger.SetMemoryHash(memoryID, contentHash[:], "committed"))
	require.NoError(t, badger.SetMemoryDomain(memoryID, submit.DomainTag))
	require.NoError(t, badger.SetMemoryClassification(memoryID, uint8(submit.Classification)))
	require.NoError(t, badger.SetMemoryAuthorPrincipal(memoryID, principal))
	req := SubmitMemoryRequest{
		Content: submit.Content, MemoryType: "task", DomainTag: submit.DomainTag,
		TaskStatus: submit.TaskStatus, Classification: int(submit.Classification), IdempotencyKey: key,
	}
	matched, err := srv.matchingTaskIdempotencyBinding(req, principal, principal, taskidempotency.Hex(digest))
	require.NoError(t, err)
	require.Equal(t, binding, matched)

	for _, tc := range []struct {
		name   string
		mutate func(*tx.MemorySubmit)
	}{
		{"chain", func(s *tx.MemorySubmit) { s.DerivedFrom[0].SourceChainID = "other" }},
		{"memory", func(s *tx.MemorySubmit) { s.DerivedFrom[0].MemoryID = "other" }},
		{"hash", func(s *tx.MemorySubmit) { s.DerivedFrom[0].ContentHash[0] ^= 1 }},
		{"empty", func(s *tx.MemorySubmit) { s.DerivedFrom = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := *submit
			changed.DerivedFrom = []tx.ParentRef{ref}
			tc.mutate(&changed)
			otherDigest, digestErr := taskidempotency.PayloadDigest(principal, principal, &changed)
			require.NoError(t, digestErr)
			recorder := httptest.NewRecorder()
			require.True(t, srv.writeTaskIdempotencyReplayIfCommitted(
				recorder, req, principal, principal, taskidempotency.Hex(otherDigest),
			))
			require.Equal(t, http.StatusConflict, recorder.Code, recorder.Body.String())
			require.Contains(t, recorder.Body.String(), "different canonical task payload")
			require.NotContains(t, recorder.Body.String(), memoryID)
			require.NotContains(t, recorder.Body.String(), ref.MemoryID)
			retained, readErr := badger.GetAppV23TaskIdempotencyBinding(principal, key)
			require.NoError(t, readErr)
			require.Equal(t, binding, retained)
		})
	}
}
