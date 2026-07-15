package scope

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testBallot() Ballot {
	return Ballot{
		MemoryID:        "memory-a",
		ScopeID:         "scope-a",
		ScopeRevision:   2,
		SubmittedHeight: 50,
		State:           BallotPending,
		Members: []BallotMember{
			{ValidatorID: "validator-a", EffectiveWeight: 4},
			{ValidatorID: "validator-b", EffectiveWeight: 3},
		},
		TotalWeight: 7,
	}
}

func TestBallotCodecRoundTripAndStrictThreshold(t *testing.T) {
	ballot := testBallot()
	encoded, err := EncodeBallot(ballot)
	require.NoError(t, err)
	decoded, err := DecodeBallot(encoded)
	require.NoError(t, err)
	assert.Equal(t, ballot, decoded)

	assert.True(t, HasStrictSupermajority(3, 4), "three of four equal weights is strictly above two thirds")
	assert.False(t, HasStrictSupermajority(2, 3), "exactly two thirds is not a strict supermajority")
	assert.True(t, HasStrictSupermajority(math.MaxUint64-1, math.MaxUint64), "comparison must not overflow near uint64 max")
	assert.False(t, HasStrictSupermajority(math.MaxUint64/2, math.MaxUint64))
}

func TestBallotCodecRejectsNonCanonicalAndMalformed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Ballot)
	}{
		{"unsorted", func(b *Ballot) { b.Members[0], b.Members[1] = b.Members[1], b.Members[0] }},
		{"zero weight", func(b *Ballot) { b.Members[0].EffectiveWeight = 0 }},
		{"wrong total", func(b *Ballot) { b.TotalWeight++ }},
		{"zero revision", func(b *Ballot) { b.ScopeRevision = 0 }},
		{"invalid state", func(b *Ballot) { b.State = 99 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ballot := testBallot()
			tc.mutate(&ballot)
			_, err := EncodeBallot(ballot)
			require.Error(t, err)
		})
	}
	encoded, err := EncodeBallot(testBallot())
	require.NoError(t, err)
	_, err = DecodeBallot(append(encoded, 0))
	require.ErrorContains(t, err, "trailing")
}

func testContent() Content {
	return Content{
		MemoryID:          "memory-a",
		ScopeID:           "scope-a",
		ScopeRevision:     2,
		SubmittingAgentID: "agent-a",
		ContentHash:       make([]byte, 32),
		MemoryType:        1,
		Domain:            "research",
		ConfidenceScore:   0.9,
		Content:           "canonical recoverable content",
		ParentHash:        "parent",
		Classification:    2,
		TaskStatus:        "planned",
		SubmittedHeight:   50,
		SubmittedUnix:     500,
	}
}

func TestContentCodecRoundTripAndRejectsInvalidValues(t *testing.T) {
	content := testContent()
	content.Tags = []string{"alpha", "zeta"}
	encoded, err := EncodeContent(content)
	require.NoError(t, err)
	decoded, err := DecodeContent(encoded)
	require.NoError(t, err)
	assert.Equal(t, content, decoded)

	badHash := content
	badHash.ContentHash = []byte("short")
	_, err = EncodeContent(badHash)
	require.ErrorContains(t, err, "32 bytes")
	badConfidence := content
	badConfidence.ConfidenceScore = math.NaN()
	_, err = EncodeContent(badConfidence)
	require.ErrorContains(t, err, "finite")
	_, err = DecodeContent(append(encoded, 0))
	require.ErrorContains(t, err, "trailing")

	nonCanonicalTags := content
	nonCanonicalTags.Tags = []string{"zeta", "alpha"}
	_, err = EncodeContent(nonCanonicalTags)
	require.ErrorContains(t, err, "canonical")

	legacyUntagged := testContent()
	legacyBytes, err := EncodeContent(legacyUntagged)
	require.NoError(t, err)
	legacyRoundTrip, err := DecodeContent(legacyBytes)
	require.NoError(t, err)
	assert.Empty(t, legacyRoundTrip.Tags)
	reencodedLegacy, err := EncodeContent(legacyRoundTrip)
	require.NoError(t, err)
	assert.Equal(t, legacyBytes, reencodedLegacy)
}

func FuzzDecodeBallot(f *testing.F) {
	canonical, err := EncodeBallot(testBallot())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(canonical)
	f.Add([]byte("not a ballot"))
	f.Fuzz(func(t *testing.T, input []byte) {
		ballot, decodeErr := DecodeBallot(input)
		if decodeErr != nil {
			return
		}
		reencoded, encodeErr := EncodeBallot(ballot)
		if encodeErr != nil {
			t.Fatalf("decoded ballot failed validation on re-encode: %v", encodeErr)
		}
		if !bytes.Equal(reencoded, input) {
			t.Fatal("accepted scope ballot has a second wire encoding")
		}
	})
}

func FuzzDecodeContent(f *testing.F) {
	canonical, err := EncodeContent(testContent())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(canonical)
	f.Add([]byte("not scoped content"))
	f.Fuzz(func(t *testing.T, input []byte) {
		content, decodeErr := DecodeContent(input)
		if decodeErr != nil {
			return
		}
		reencoded, encodeErr := EncodeContent(content)
		if encodeErr != nil {
			t.Fatalf("decoded content failed validation on re-encode: %v", encodeErr)
		}
		if !bytes.Equal(reencoded, input) {
			t.Fatal("accepted scoped content has a second wire encoding")
		}
	})
}
