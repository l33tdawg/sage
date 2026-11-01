package store

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testAppV30Signer(seed string) AppV30ProvenanceSigner {
	credential := strings.Repeat(seed, 64)
	return AppV30ProvenanceSigner{
		CredentialID:   credential,
		PrincipalID:    credential,
		RootGeneration: 0,
		Exact:          true,
	}
}

func TestAppV30ProvenanceCanonicalRoundTripAndReplay(t *testing.T) {
	s := openTaintStore(t)
	first := testAppV30Signer("a")
	second := testAppV30Signer("b")
	second.PrincipalID = strings.Repeat("c", 64)
	second.RootGeneration = 3

	require.NoError(t, s.SetAppV30Provenance("memory", []AppV30ProvenanceSigner{
		second,
		first,
	}))
	require.NoError(t, s.ValidateAppV30Provenance("memory", []AppV30ProvenanceSigner{
		first,
		second,
	}))
	require.NoError(t, s.SetAppV30Provenance("memory", []AppV30ProvenanceSigner{
		first,
		second,
	}))

	got, found, err := s.GetAppV30Provenance("memory")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, []AppV30ProvenanceSigner{first, second}, got)

	err = s.SetAppV30Provenance("memory", []AppV30ProvenanceSigner{first})
	require.ErrorIs(t, err, ErrAppV30ProvenanceConflict)
	err = s.ValidateAppV30Provenance("memory", []AppV30ProvenanceSigner{first})
	require.ErrorIs(t, err, ErrAppV30ProvenanceConflict)
}

func TestAppV30ProvenanceValidation(t *testing.T) {
	valid := testAppV30Signer("d")

	_, err := encodeAppV30Provenance(nil)
	require.Error(t, err)
	bad := valid
	bad.CredentialID = "short"
	_, err = encodeAppV30Provenance([]AppV30ProvenanceSigner{bad})
	require.ErrorContains(t, err, "non-canonical identity")
	bad = valid
	bad.Exact = false
	_, err = encodeAppV30Provenance([]AppV30ProvenanceSigner{bad})
	require.ErrorContains(t, err, "not exact")
	_, err = encodeAppV30Provenance([]AppV30ProvenanceSigner{valid, valid})
	require.ErrorContains(t, err, "duplicated")

	encoded, err := encodeAppV30Provenance([]AppV30ProvenanceSigner{valid})
	require.NoError(t, err)
	encoded[0] = 0
	_, err = decodeAppV30Provenance(encoded)
	require.ErrorContains(t, err, "signer count")

	encoded, err = encodeAppV30Provenance([]AppV30ProvenanceSigner{valid})
	require.NoError(t, err)
	encoded[len(encoded)-1] = 0
	_, err = decodeAppV30Provenance(encoded)
	require.ErrorContains(t, err, "not exact")

	assert.True(t, isCanonicalMemoryProjectionKey(AppV30ProvenanceKey("memory")))
}
