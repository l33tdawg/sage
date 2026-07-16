package governance

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDelegationDomainForChainIDIsStableAndChainBound(t *testing.T) {
	a, err := DelegationDomainForChainID("sage-a")
	require.NoError(t, err)
	aAgain, err := DelegationDomainForChainID("sage-a")
	require.NoError(t, err)
	b, err := DelegationDomainForChainID("sage-b")
	require.NoError(t, err)

	assert.Len(t, a, 64)
	assert.Equal(t, a, aAgain)
	assert.NotEqual(t, a, b)
	spaceVariant, err := DelegationDomainForChainID(" sage-a")
	require.NoError(t, err)
	assert.NotEqual(t, a, spaceVariant, "the exact CometBFT chain_id bytes define the domain")
	for _, invalid := range []string{"", string(make([]byte, 51))} {
		_, err = DelegationDomainForChainID(invalid)
		assert.Error(t, err)
	}
}
