package tags

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalize(t *testing.T) {
	got, err := Normalize([]string{"zeta", "alpha", "alpha"})
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha", "zeta"}, got)

	for name, values := range map[string][]string{
		"empty":     {""},
		"padded":    {" alpha"},
		"oversized": {strings.Repeat("a", MaxBytes+1)},
		"too many":  make([]string, MaxCount+1),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Normalize(values)
			require.Error(t, err)
		})
	}
}

func TestValidateCanonical(t *testing.T) {
	require.NoError(t, ValidateCanonical(nil))
	require.NoError(t, ValidateCanonical([]string{"alpha", "zeta"}))
	require.Error(t, ValidateCanonical([]string{"zeta", "alpha"}))
	require.Error(t, ValidateCanonical([]string{"alpha", "alpha"}))
}
