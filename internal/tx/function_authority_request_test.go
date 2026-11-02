package tx

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeFunctionAuthorityRequestClosedContract(t *testing.T) {
	a := functionAuthorityFixture()
	valid, err := json.Marshal(a)
	require.NoError(t, err)
	for _, input := range [][]byte{valid, append(append([]byte(" \n"), valid...), '\n')} {
		got, err := DecodeFunctionAuthorityRequest(input)
		require.NoError(t, err)
		require.Equal(t, a, *got)
	}
	for name, input := range map[string]string{
		"empty":              "",
		"unknown":            strings.TrimSuffix(string(valid), "}") + `,"role":"admin"}`,
		"duplicate":          strings.TrimSuffix(string(valid), "}") + `,"action":"assign"}`,
		"escaped duplicate":  strings.TrimSuffix(string(valid), "}") + `,"act\u0069on":"assign"}`,
		"case alias":         strings.Replace(string(valid), `"function"`, `"Function"`, 1),
		"missing zero field": strings.Replace(string(valid), `"activation_height":0,`, "", 1),
		"null zero field":    strings.Replace(string(valid), `"activation_height":0`, `"activation_height":null`, 1),
		"missing scope":      strings.Replace(string(valid), `"scope":"example-chain",`, "", 1),
		"null scope":         strings.Replace(string(valid), `"scope":"example-chain"`, `"scope":null`, 1),
		"wrong type":         strings.Replace(string(valid), `"expected_revision":7`, `"expected_revision":"7"`, 1),
		"trailing":           string(valid) + `{}`,
		"array":              `[]`,
		"oversized":          strings.Repeat(" ", MaxFunctionAuthorityRequestSize+1),
	} {
		t.Run(name, func(t *testing.T) {
			got, err := DecodeFunctionAuthorityRequest([]byte(input))
			require.Error(t, err)
			require.Nil(t, got)
		})
	}
}
