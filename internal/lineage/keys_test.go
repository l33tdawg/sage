package lineage

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanonicalIndexKeys(t *testing.T) {
	parentID := strings.Repeat("a", ParentIDBytes*2)
	assert.Equal(t, "lineage:\x00\x05child:"+parentID, string(ForwardKey("child", parentID)))
	assert.Equal(t, "lineage:\x00\x05child:", string(ForwardPrefix("child")))
	assert.Equal(t, "lineage-ref:"+parentID+":child", string(ReverseKey(parentID, "child")))
	assert.Equal(t, "lineage-ref:"+parentID+":", string(ReversePrefix(parentID)))
}

func TestForwardPrefixesAreExactArbitraryIdentities(t *testing.T) {
	ids := []string{"x", strings.Repeat("x", 255), strings.Repeat("x", 256), strings.Repeat("x", 511), strings.Repeat("é", 256), "victim", "victim:x", "victim\x00", "\x00:\xff", "é:", strings.Repeat("x", 512)}
	for _, id := range ids {
		for _, other := range ids {
			assert.Equal(t, id == other, bytes.HasPrefix(ForwardKey(other, strings.Repeat("a", ParentIDBytes*2)), ForwardPrefix(id)),
				"identity %q versus %q", id, other)
		}
	}
	for _, invalid := range []string{strings.Repeat("x", 513), "victim:" + strings.Repeat("x", 65535)} {
		for _, id := range ids {
			assert.False(t, bytes.HasPrefix(ForwardKey(invalid, strings.Repeat("a", ParentIDBytes*2)), ForwardPrefix(id)))
		}
	}
}

func TestValidateIDs(t *testing.T) {
	parentID := strings.Repeat("a", ParentIDBytes*2)
	require.NoError(t, ValidateIDs("child", parentID))
	require.Error(t, ValidateIDs("", parentID))
	require.Error(t, ValidateIDs(strings.Repeat("c", 513), parentID))
	require.Error(t, ValidateIDs("child", ""))
	require.Error(t, ValidateIDs("child", strings.Repeat("A", ParentIDBytes*2)))
	require.Error(t, ValidateIDs("child", strings.Repeat("z", ParentIDBytes*2)))
}
