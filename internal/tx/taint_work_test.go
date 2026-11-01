package tx

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testMemoryTaintWork(kind MemoryTaintWorkKind) *MemoryTaintWork {
	work := &MemoryTaintWork{
		Version:      MemoryTaintWorkVersion,
		Kind:         kind,
		DecisionID:   strings.Repeat("d", 64),
		Generation:   2,
		FrontierHead: 7,
		Cursor:       "last-memory",
	}
	if kind == MemoryTaintWorkExpand {
		work.ParentRefID = strings.Repeat("a", 64)
	} else {
		work.FrontierHead = 0
		work.CleanupPhase = 1
	}
	return work
}

func TestMemoryTaintWorkRoundTripAndStrictness(t *testing.T) {
	for _, kind := range []MemoryTaintWorkKind{
		MemoryTaintWorkExpand,
		MemoryTaintWorkCleanup,
	} {
		parsed := &ParsedTx{
			Type:            TxTypeMemoryTaintWork,
			MemoryTaintWork: testMemoryTaintWork(kind),
			Nonce:           1,
			Timestamp:       time.Unix(1_700_000_000, 0).UTC(),
		}
		encoded, err := EncodeTx(parsed)
		require.NoError(t, err)
		decoded, err := DecodeTx(encoded)
		require.NoError(t, err)
		assert.Equal(t, parsed.MemoryTaintWork, decoded.MemoryTaintWork)
		reencoded, err := EncodeTx(decoded)
		require.NoError(t, err)
		assert.Equal(t, encoded, reencoded)
	}
}

func TestMemoryTaintWorkValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*MemoryTaintWork)
		want   string
	}{
		{"version", func(w *MemoryTaintWork) { w.Version = 2 }, "unsupported version"},
		{"kind", func(w *MemoryTaintWork) { w.Kind = 9 }, "unsupported kind"},
		{"decision", func(w *MemoryTaintWork) { w.DecisionID = "short" }, "decision ID"},
		{"generation", func(w *MemoryTaintWork) { w.Generation = 0 }, "generation"},
		{"parent", func(w *MemoryTaintWork) { w.ParentRefID = "bad" }, "parent ref ID"},
		{"cursor", func(w *MemoryTaintWork) { w.Cursor = strings.Repeat("c", maxMemoryTaintCursorBytes+1) }, "cursor length"},
		{"expand phase", func(w *MemoryTaintWork) { w.CleanupPhase = 1 }, "cannot carry a cleanup phase"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			work := testMemoryTaintWork(MemoryTaintWorkExpand)
			tc.mutate(work)
			_, err := encodeMemoryTaintWork(work)
			require.ErrorContains(t, err, tc.want)
		})
	}

	cleanup := testMemoryTaintWork(MemoryTaintWorkCleanup)
	cleanup.ParentRefID = strings.Repeat("a", 64)
	_, err := encodeMemoryTaintWork(cleanup)
	require.ErrorContains(t, err, "cleanup cannot carry an expansion cursor")
	cleanup = testMemoryTaintWork(MemoryTaintWorkCleanup)
	cleanup.CleanupPhase = 0
	_, err = encodeMemoryTaintWork(cleanup)
	require.ErrorContains(t, err, "invalid cleanup phase")
}

func TestMemoryTaintWorkRejectsTrailingAndTruncatedBytes(t *testing.T) {
	encoded, err := encodeMemoryTaintWork(testMemoryTaintWork(MemoryTaintWorkExpand))
	require.NoError(t, err)
	_, err = decodeMemoryTaintWork(append(encoded, 0))
	require.ErrorContains(t, err, "trailing bytes")
	_, err = decodeMemoryTaintWork(encoded[:len(encoded)-1])
	require.Error(t, err)
}
