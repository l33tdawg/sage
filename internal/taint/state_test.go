package taint

import (
	"crypto/sha256"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testDecisionID() string {
	return strings.Repeat("d", DecisionIDBytes)
}

func testPayloadHash() [sha256.Size]byte {
	return sha256.Sum256([]byte("taint-payload"))
}

func testParentRefID() string {
	return strings.Repeat("a", DecisionIDBytes)
}

func TestAcceptedLimits(t *testing.T) {
	assert.Equal(t, 32, MaxActiveDecisions)
	assert.Equal(t, 16, MaxParents)
	assert.Equal(t, 64, MaxSigners)
	assert.Equal(t, 256, MaxServingAncestorWalk)
	assert.Equal(t, 64, MaxDepth)
	assert.Equal(t, 1024, PerExecutionUnitBudget)
	assert.Equal(t, 625, MaxAdmissionTaintUnits)
	assert.Equal(t, 204, MaxNewMembershipsPerExec)
	assert.Equal(t, 16384, MaxExpansionMemberships)
	assert.Equal(t, 4<<20, MaxPerBlockBytes)
	assert.Equal(t, 65536, MaxServingRequestUnits)
}

func TestCanonicalKeys(t *testing.T) {
	assert.Equal(t, "taintdec:decision", string(DecisionKey("decision")))
	assert.Equal(t, "taintm:memory:decision", string(MembershipKey("memory", "decision")))
	assert.Equal(t, "taintm:memory:", string(MembershipPrefix("memory")))
	assert.Equal(t, "taintd:decision:memory", string(DecisionIndexKey("decision", "memory")))
	assert.Equal(t, "taintd:decision:", string(DecisionIndexPrefix("decision")))
	assert.Equal(t, "taintq:decision:", string(FrontierPrefix("decision")))
	assert.Equal(t, "taintrev", string(RevisionKey()))
	assert.Equal(t, "taintactive", string(ActiveCountKey()))

	key := FrontierKey("decision", 7, 9)
	expected := append([]byte("taintq:decision:"), 0, 0, 0, 0, 0, 0, 0, 7, ':')
	expected = binary.BigEndian.AppendUint64(expected, 9)
	assert.Equal(t, expected, key)
}

func TestHeaderRoundTrip(t *testing.T) {
	tests := []Header{
		{
			Action: ActionApply, Status: StatusExpanding, Generation: 1,
			FrontierTail: 1, Materialized: 1, MaxDepth: 1, PayloadHash: testPayloadHash(),
		},
		{
			Action: ActionApply, Status: StatusExpanding, Generation: 2,
			FrontierHead: 3, FrontierTail: 9, Scanned: 10, Materialized: 6,
			MaxDepth: 4, ParentRefID: testParentRefID(), ParentMemoryID: "parent", ParentDepth: 3,
			LastEdge: "last-child", PayloadHash: testPayloadHash(),
		},
		{
			Action: ActionClear, Status: StatusCleaning, Generation: 3,
			FrontierHead: 4, FrontierTail: 4, Scanned: 20, Materialized: 20,
			MaxDepth: 7, PayloadHash: testPayloadHash(),
		},
	}
	for _, header := range tests {
		encoded, err := EncodeHeader(header)
		require.NoError(t, err)
		decoded, err := DecodeHeader(encoded)
		require.NoError(t, err)
		assert.Equal(t, header, decoded)
		reencoded, err := EncodeHeader(decoded)
		require.NoError(t, err)
		assert.Equal(t, encoded, reencoded)
	}
}

func TestHeaderValidation(t *testing.T) {
	valid := Header{
		Action: ActionApply, Status: StatusExpanding, Generation: 1,
		PayloadHash: testPayloadHash(),
	}
	tests := []struct {
		name   string
		mutate func(*Header)
		want   string
	}{
		{"action", func(h *Header) { h.Action = 9 }, "header action"},
		{"status", func(h *Header) { h.Status = 9 }, "header status"},
		{"generation", func(h *Header) { h.Generation = 0 }, "generation must be positive"},
		{"frontier", func(h *Header) { h.FrontierHead = 2; h.FrontierTail = 1 }, "frontier head"},
		{"parent depth", func(h *Header) { h.ParentDepth = MaxDepth + 1 }, "depth exceeds"},
		{"max depth", func(h *Header) { h.MaxDepth = MaxDepth + 1 }, "depth exceeds"},
		{"both parent fields empty", func(h *Header) { h.ParentRefID = ""; h.ParentMemoryID = "" }, ""},
		{"parent ref without memory", func(h *Header) { h.ParentRefID = testParentRefID() }, "must be set together"},
		{"parent memory without ref", func(h *Header) { h.ParentMemoryID = "parent" }, "must be set together"},
		{"invalid parent ref", func(h *Header) { h.ParentRefID = strings.Repeat("z", DecisionIDBytes); h.ParentMemoryID = "parent" }, "parent ref ID"},
		{"parent too long", func(h *Header) {
			h.ParentRefID = testParentRefID()
			h.ParentMemoryID = strings.Repeat("p", MaxMemoryIDBytes+1)
		}, "memory ID length"},
		{"cursor too long", func(h *Header) { h.LastEdge = strings.Repeat("c", MaxCursorBytes+1) }, "cursor length"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			header := valid
			tc.mutate(&header)
			_, err := EncodeHeader(header)
			if tc.want == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestDecodeHeaderRejectsMalformedLengthsAndTrailingBytes(t *testing.T) {
	header := Header{
		Action: ActionApply, Status: StatusExpanding, Generation: 1,
		PayloadHash: testPayloadHash(),
	}
	encoded, err := EncodeHeader(header)
	require.NoError(t, err)
	require.Len(t, encoded, minimumHeaderBytes)

	_, err = DecodeHeader(encoded[:len(encoded)-1])
	require.ErrorIs(t, err, errState)

	_, err = DecodeHeader(append(encoded, 0))
	require.ErrorContains(t, err, "trailing bytes")

	badParent := append([]byte(nil), encoded...)
	binary.BigEndian.PutUint16(badParent[40:42], uint16(DecisionIDBytes+1))
	_, err = DecodeHeader(badParent)
	require.ErrorContains(t, err, "parent ref length")
}

func TestMembershipRoundTripAndEffectiveness(t *testing.T) {
	membership := Membership{Generation: 3, Depth: 4, Origin: OriginExpand}
	encoded, err := EncodeMembership(membership)
	require.NoError(t, err)
	decoded, err := DecodeMembership(encoded)
	require.NoError(t, err)
	assert.Equal(t, membership, decoded)

	header := Header{Status: StatusExpanding, Generation: 3}
	assert.True(t, header.MembershipEffective(membership))
	header.Generation = 4
	assert.False(t, header.MembershipEffective(membership))
	header.Generation = 3
	header.Status = StatusCleaning
	assert.False(t, header.MembershipEffective(membership))
}

func TestMembershipValidation(t *testing.T) {
	for _, origin := range []Origin{OriginTarget, OriginExpand, OriginAdmit} {
		encoded, err := EncodeMembership(Membership{Generation: 1, Origin: origin})
		require.NoError(t, err)
		_, err = DecodeMembership(encoded)
		require.NoError(t, err)
	}

	_, err := EncodeMembership(Membership{Generation: 1, Origin: 0})
	require.ErrorContains(t, err, "origin")
	_, err = EncodeMembership(Membership{Generation: 0, Origin: OriginTarget})
	require.ErrorContains(t, err, "generation must be positive")
	_, err = EncodeMembership(Membership{Generation: 1, Depth: MaxDepth + 1, Origin: OriginTarget})
	require.ErrorContains(t, err, "depth")
	_, err = DecodeMembership([]byte{1, 2, 3})
	require.ErrorContains(t, err, "membership is")

	encoded, err := EncodeMembership(Membership{Generation: 1, Origin: OriginTarget})
	require.NoError(t, err)
	binary.BigEndian.PutUint16(encoded[8:], MaxDepth+1)
	_, err = DecodeMembership(encoded)
	require.ErrorContains(t, err, "depth")

	encoded, err = EncodeMembership(Membership{Generation: 1, Origin: OriginTarget})
	require.NoError(t, err)
	binary.BigEndian.PutUint64(encoded[:8], 0)
	_, err = DecodeMembership(encoded)
	require.ErrorContains(t, err, "generation must be positive")
}

func TestQueueEntryRoundTripAndValidation(t *testing.T) {
	entry := QueueEntry{
		ParentRefID: testParentRefID(),
		MemoryID:    strings.Repeat("m", MaxMemoryIDBytes),
		Depth:       MaxDepth,
	}
	encoded, err := EncodeQueueEntry(entry)
	require.NoError(t, err)
	decoded, err := DecodeQueueEntry(encoded)
	require.NoError(t, err)
	assert.Equal(t, entry, decoded)

	_, err = EncodeQueueEntry(QueueEntry{ParentRefID: testParentRefID(), Depth: 1})
	require.ErrorContains(t, err, "memory ID length")
	_, err = EncodeQueueEntry(QueueEntry{ParentRefID: testParentRefID(), MemoryID: "memory", Depth: MaxDepth + 1})
	require.ErrorContains(t, err, "queue depth")
	_, err = EncodeQueueEntry(QueueEntry{MemoryID: "memory", Depth: 1})
	require.ErrorContains(t, err, "parent ref ID")
	_, err = DecodeQueueEntry([]byte{0, 1})
	require.ErrorContains(t, err, "short queue entry")

	bad := append([]byte(nil), encoded...)
	binary.BigEndian.PutUint16(bad, uint16(MaxMemoryIDBytes+1))
	_, err = DecodeQueueEntry(bad)
	require.ErrorContains(t, err, "parent ref length")

	bad = append([]byte(nil), encoded...)
	memoryLengthOffset := 2 + DecisionIDBytes
	binary.BigEndian.PutUint16(bad[memoryLengthOffset:], uint16(MaxMemoryIDBytes+1))
	_, err = DecodeQueueEntry(bad)
	require.ErrorContains(t, err, "invalid queue entry length")
}

func TestStatusTransitions(t *testing.T) {
	assert.True(t, StatusExpanding.Active())
	assert.True(t, StatusComplete.Active())
	assert.True(t, StatusDepthExhausted.Active())
	assert.True(t, StatusWorkExhausted.Active())
	assert.False(t, StatusCleaning.Active())
	assert.False(t, StatusClean.Active())

	assert.True(t, StatusExpanding.Incomplete())
	assert.True(t, StatusDepthExhausted.Incomplete())
	assert.True(t, StatusWorkExhausted.Incomplete())
	assert.False(t, StatusComplete.Incomplete())
}

func TestValidateDecisionID(t *testing.T) {
	require.NoError(t, ValidateDecisionID(testDecisionID()))
	require.Error(t, ValidateDecisionID(""))
	require.Error(t, ValidateDecisionID(strings.Repeat("D", DecisionIDBytes)))
	require.Error(t, ValidateDecisionID(strings.Repeat("g", DecisionIDBytes)))
}

func TestHeaderRejectsInvalidMembershipGeneration(t *testing.T) {
	header := Header{Status: StatusComplete, Generation: 2}
	membership := Membership{Generation: 1, Origin: OriginExpand}
	assert.False(t, header.MembershipEffective(membership))
}
