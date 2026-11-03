package taskidempotency

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
)

// legacyTaskPayloadDigestForTest fixes the previous field framing and domain.
// It deliberately does not call PayloadDigest or its new lineage branch.
func legacyTaskPayloadDigestForTest(principal, assignee string, submit *tx.MemorySubmit) [sha256.Size]byte {
	parts := make([][]byte, 0, 11+len(submit.Tags))
	parts = append(parts, [][]byte{
		[]byte(principal), []byte(assignee), []byte(submit.MemoryID),
		submit.ContentHash, {byte(submit.MemoryType)}, []byte(submit.DomainTag),
		nil, []byte(submit.Content), []byte(submit.ParentHash),
		{byte(submit.Classification)}, []byte(submit.TaskStatus),
	}...)
	var confidence [8]byte
	binary.BigEndian.PutUint64(confidence[:], math.Float64bits(submit.ConfidenceScore))
	parts[6] = confidence[:]
	for _, tag := range submit.Tags {
		parts = append(parts, []byte(tag))
	}
	h := sha256.New()
	_, _ = h.Write([]byte("sage/app-v23/task-idempotency/payload/v1\x00"))
	var size [4]byte
	for _, part := range parts {
		binary.BigEndian.PutUint32(size[:], uint32(len(part))) // #nosec G115 -- test inputs are bounded
		_, _ = h.Write(size[:])
		_, _ = h.Write(part)
	}
	var result [sha256.Size]byte
	copy(result[:], h.Sum(nil))
	return result
}

func TestTaskPayloadDigestPreservesEmptyLineageV1(t *testing.T) {
	principal, assignee := strings.Repeat("a1", 32), strings.Repeat("b2", 32)
	contentHash := sha256.Sum256([]byte("exact task"))
	submit := &tx.MemorySubmit{
		MemoryID:    "00000000-0000-4000-8000-000000000000",
		ContentHash: contentHash[:], MemoryType: tx.MemoryTypeTask,
		DomainTag: "tasks", ConfidenceScore: 0.75, Content: "exact task",
		ParentHash: "legacy-parent", Classification: tx.ClearanceInternal,
		TaskStatus: "planned", Tags: []string{"one", "two"},
	}
	want := legacyTaskPayloadDigestForTest(principal, assignee, submit)
	for _, refs := range [][]tx.ParentRef{nil, {}} {
		submit.DerivedFrom = refs
		got, err := PayloadDigest(principal, assignee, submit)
		require.NoError(t, err)
		require.Equal(t, want, got, "existing empty-lineage receipts retain the exact v1 preimage")
	}
}

func TestTaskPayloadDigestBindsCanonicalDerivedFrom(t *testing.T) {
	principal, assignee := strings.Repeat("a1", 32), strings.Repeat("b2", 32)
	contentHash := sha256.Sum256([]byte("exact task"))
	refs := []tx.ParentRef{
		{SourceChainID: "chain", MemoryID: "parent-b", ContentHash: sha256.Sum256([]byte("b"))},
		{SourceChainID: "chain", MemoryID: "parent-a", ContentHash: sha256.Sum256([]byte("a"))},
	}
	submit := &tx.MemorySubmit{
		MemoryID:    "00000000-0000-4000-8000-000000000000",
		ContentHash: contentHash[:], MemoryType: tx.MemoryTypeTask,
		DomainTag: "tasks", Content: "exact task", TaskStatus: "planned",
		DerivedFrom: append([]tx.ParentRef(nil), refs...),
	}
	got, err := PayloadDigest(principal, assignee, submit)
	require.NoError(t, err)
	require.Equal(t, refs, submit.DerivedFrom, "digest canonicalization must not mutate the caller")
	require.NotEqual(t, legacyTaskPayloadDigestForTest(principal, assignee, submit), got)

	ordered := *submit
	ordered.DerivedFrom = append([]tx.ParentRef(nil), refs...)
	tx.SortParentRefs(ordered.DerivedFrom)
	reordered, err := PayloadDigest(principal, assignee, &ordered)
	require.NoError(t, err)
	require.Equal(t, got, reordered, "the existing transaction codec determines parent order")

	for _, tc := range []struct {
		name   string
		mutate func(*tx.MemorySubmit)
	}{
		{"chain", func(s *tx.MemorySubmit) { s.DerivedFrom[0].SourceChainID = "other-chain" }},
		{"memory", func(s *tx.MemorySubmit) { s.DerivedFrom[0].MemoryID = "other-parent" }},
		{"hash", func(s *tx.MemorySubmit) { s.DerivedFrom[0].ContentHash[0] ^= 1 }},
		{"omitted", func(s *tx.MemorySubmit) { s.DerivedFrom = nil }},
		{"one-parent", func(s *tx.MemorySubmit) { s.DerivedFrom = s.DerivedFrom[:1] }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := *submit
			changed.DerivedFrom = append([]tx.ParentRef(nil), refs...)
			tc.mutate(&changed)
			digest, digestErr := PayloadDigest(principal, assignee, &changed)
			require.NoError(t, digestErr)
			require.NotEqual(t, got, digest)
		})
	}

	arbitrary := *submit
	arbitrary.DerivedFrom = []tx.ParentRef{{
		SourceChainID: "chain\x00suffix",
		MemoryID:      string([]byte{0xff, 0x00, 0x80}),
		ContentHash:   refs[0].ContentHash,
	}}
	rawDigest, err := PayloadDigest(principal, assignee, &arbitrary)
	require.NoError(t, err, "task receipts use the codec's byte contract, not an extra UTF-8/UUID restriction")
	arbitrary.DerivedFrom[0].MemoryID = string([]byte{0xff, 0x00, 0x81})
	otherRawDigest, err := PayloadDigest(principal, assignee, &arbitrary)
	require.NoError(t, err)
	require.NotEqual(t, rawDigest, otherRawDigest)
}

func TestTaskPayloadDigestRejectsMalformedDerivedFrom(t *testing.T) {
	principal, assignee := strings.Repeat("a1", 32), strings.Repeat("b2", 32)
	ref := tx.ParentRef{SourceChainID: "chain", MemoryID: "parent"}
	for _, refs := range [][]tx.ParentRef{
		{ref, ref},
		{{SourceChainID: "", MemoryID: "parent"}},
		{{SourceChainID: "chain", MemoryID: strings.Repeat("x", tx.MaxDerivedFromMemoryIDBytes+1)}},
		make([]tx.ParentRef, tx.MaxDerivedFromParents+1),
	} {
		_, err := PayloadDigest(principal, assignee, &tx.MemorySubmit{
			MemoryType: tx.MemoryTypeTask, DerivedFrom: refs,
		})
		require.Error(t, err)
	}
}
