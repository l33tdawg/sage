package lineage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanonicalIndexKeys(t *testing.T) {
	parentID := strings.Repeat("a", ParentIDBytes*2)
	assert.Equal(t, "lineage:child:"+parentID, string(ForwardKey("child", parentID)))
	assert.Equal(t, "lineage:child:", string(ForwardPrefix("child")))
	assert.Equal(t, "lineage-ref:"+parentID+":child", string(ReverseKey(parentID, "child")))
	assert.Equal(t, "lineage-ref:"+parentID+":", string(ReversePrefix(parentID)))
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
