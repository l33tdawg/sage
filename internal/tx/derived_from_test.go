package tx

import (
	"crypto/sha256"
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testParentRef(chain, memory, content string) ParentRef {
	return ParentRef{
		SourceChainID: chain,
		MemoryID:      memory,
		ContentHash:   sha256.Sum256([]byte(content)),
	}
}

func TestDerivedFromExtensionRoundTripAndCanonicalSort(t *testing.T) {
	refs := []ParentRef{
		testParentRef("sage-b", "memory-b", "content-b"),
		testParentRef("sage-a", "memory-c", "content-c"),
		testParentRef("sage-a", "memory-a", "content-a"),
	}
	SortParentRefs(refs)
	assert.Equal(t, "memory-a", refs[0].MemoryID)
	assert.Equal(t, "memory-c", refs[1].MemoryID)
	assert.Equal(t, "memory-b", refs[2].MemoryID)

	encoded, err := EncodeDerivedFromExtension(refs)
	require.NoError(t, err)
	decoded, err := DecodeDerivedFromExtension(encoded)
	require.NoError(t, err)
	assert.Equal(t, refs, decoded)

	reencoded, err := EncodeDerivedFromExtension(decoded)
	require.NoError(t, err)
	assert.Equal(t, encoded, reencoded)
}

func TestDerivedFromParentIdentityBindsEveryField(t *testing.T) {
	base := testParentRef("sage", "memory", "content")
	baseID, err := ParentRefIDHex(base)
	require.NoError(t, err)

	chainChanged := base
	chainChanged.SourceChainID = "sage-other"
	memoryChanged := base
	memoryChanged.MemoryID = "memory-other"
	hashChanged := base
	hashChanged.ContentHash = sha256.Sum256([]byte("other-content"))

	for _, changed := range []ParentRef{chainChanged, memoryChanged, hashChanged} {
		changedID, idErr := ParentRefIDHex(changed)
		require.NoError(t, idErr)
		assert.NotEqual(t, baseID, changedID)
	}

	repeatedID, err := ParentRefIDHex(base)
	require.NoError(t, err)
	assert.Equal(t, baseID, repeatedID)
}

func TestDerivedFromExtensionValidation(t *testing.T) {
	valid := []ParentRef{
		testParentRef("sage-a", "memory-a", "content-a"),
		testParentRef("sage-b", "memory-b", "content-b"),
	}
	tests := []struct {
		name string
		refs []ParentRef
		want string
	}{
		{"empty", nil, "parent count"},
		{"too many", make([]ParentRef, MaxDerivedFromParents+1), "parent count"},
		{"empty chain", []ParentRef{testParentRef("", "memory", "content")}, "source chain ID length"},
		{"long chain", []ParentRef{testParentRef(strings.Repeat("c", MaxDerivedFromChainIDBytes+1), "memory", "content")}, "source chain ID length"},
		{"empty memory", []ParentRef{testParentRef("sage", "", "content")}, "memory ID length"},
		{"long memory", []ParentRef{testParentRef("sage", strings.Repeat("m", MaxDerivedFromMemoryIDBytes+1), "content")}, "memory ID length"},
		{"duplicate", []ParentRef{valid[0], valid[0]}, "strictly canonical-sorted and unique"},
		{"unsorted", []ParentRef{valid[1], valid[0]}, "strictly canonical-sorted and unique"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := EncodeDerivedFromExtension(tc.refs)
			require.ErrorContains(t, err, tc.want)
		})
	}

	encoded, err := EncodeDerivedFromExtension(valid)
	require.NoError(t, err)
	_, err = DecodeDerivedFromExtension(append(encoded, 0))
	require.ErrorContains(t, err, "trailing bytes")

	badVersion := append([]byte(nil), encoded...)
	badVersion[len(memorySubmitDerivedFromMarker)] = 2
	_, err = DecodeDerivedFromExtension(badVersion)
	require.ErrorContains(t, err, "unsupported version")

	badCount := append([]byte(nil), encoded...)
	badCount[len(memorySubmitDerivedFromMarker)+1] = MaxDerivedFromParents + 1
	_, err = DecodeDerivedFromExtension(badCount)
	require.ErrorContains(t, err, "parent count")

	badLength := append([]byte(nil), encoded...)
	lengthOffset := len(memorySubmitDerivedFromMarker) + 2
	binary.BigEndian.PutUint16(badLength[lengthOffset:], uint16(MaxDerivedFromChainIDBytes+1))
	_, err = DecodeDerivedFromExtension(badLength)
	require.ErrorContains(t, err, "chain ID length")
}

func TestMemorySubmitActivatesTagsAndDerivedFromOnlyAtV30(t *testing.T) {
	refs := []ParentRef{testParentRef("sage", "parent", "parent-content")}
	submit := &MemorySubmit{
		MemoryID:       "child",
		Content:        "child content",
		DomainTag:      "general",
		MemoryType:     MemoryTypeFact,
		Classification: ClearanceInternal,
		Tags:           []string{"alpha", "beta"},
		DerivedFrom:    refs,
	}
	parsed := &ParsedTx{
		Type:         TxTypeMemorySubmit,
		MemorySubmit: submit,
		Nonce:        1,
		Timestamp:    time.Unix(1_700_000_000, 0).UTC(),
	}
	encoded, err := EncodeTx(parsed)
	require.NoError(t, err)
	decoded, err := DecodeTx(encoded)
	require.NoError(t, err)

	require.ErrorContains(
		t,
		ActivateMemorySubmitTags(decoded),
		"derived_from extension is not active",
	)

	decoded, err = DecodeTx(encoded)
	require.NoError(t, err)
	require.NoError(t, ActivateMemorySubmitExtensions(decoded, true, true))
	assert.Equal(t, submit.Tags, decoded.MemorySubmit.Tags)
	assert.Equal(t, refs, decoded.MemorySubmit.DerivedFrom)
}

func TestMemorySubmitDerivedFromWithoutTags(t *testing.T) {
	refs := []ParentRef{testParentRef("sage", "parent", "parent-content")}
	parsed := &ParsedTx{
		Type: TxTypeMemorySubmit,
		MemorySubmit: &MemorySubmit{
			MemoryID: "child", Content: "content", DomainTag: "general",
			MemoryType: MemoryTypeObservation, Classification: ClearanceInternal,
			DerivedFrom: refs,
		},
		Nonce: 2, Timestamp: time.Unix(1_700_000_001, 0).UTC(),
	}
	encoded, err := EncodeTx(parsed)
	require.NoError(t, err)

	decoded, err := DecodeTx(encoded)
	require.NoError(t, err)
	require.ErrorContains(t, ActivateMemorySubmitTags(decoded), "not active")

	decoded, err = DecodeTx(encoded)
	require.NoError(t, err)
	require.NoError(t, ActivateMemorySubmitExtensions(decoded, true, true))
	assert.Empty(t, decoded.MemorySubmit.Tags)
	assert.Equal(t, refs, decoded.MemorySubmit.DerivedFrom)
}

func TestMemorySubmitExtensionOrderIsStrict(t *testing.T) {
	refs := []ParentRef{testParentRef("sage", "parent", "parent-content")}
	derived, err := EncodeDerivedFromExtension(refs)
	require.NoError(t, err)

	core := &MemorySubmit{
		MemoryID: "child", Content: "content", DomainTag: "general",
		MemoryType: MemoryTypeFact, Classification: ClearanceInternal,
	}
	encoded, err := encodeMemorySubmit(core)
	require.NoError(t, err)

	// Build derived_from before tags. The combined activator must reject the
	// wrong order rather than accepting both extensions independently.
	payload := append([]byte(nil), encoded...)
	payload = append(payload, derived...)
	payload = append(payload, memorySubmitTagsMarker...)
	payload = appendStringSlice(payload, []string{"tag"})
	decoded, err := decodeMemorySubmit(payload)
	require.NoError(t, err)
	parsed := &ParsedTx{Type: TxTypeMemorySubmit, MemorySubmit: decoded}
	require.ErrorContains(t, ActivateMemorySubmitExtensions(parsed, true, true), "trailing bytes")
}
