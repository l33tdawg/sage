package tx

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAgentRequestOptionalWireTailPreservesLegacyBytes(t *testing.T) {
	legacy := sampleSubmitTx()
	legacy.AgentNonce = []byte("nonce")
	legacyBytes, err := EncodeTx(legacy)
	require.NoError(t, err)

	withRequest := *legacy
	withRequest.AgentRequest = []byte("POST /v1/memory/submit\n{\"content\":\"bound\"}")
	requestBytes, err := EncodeTx(&withRequest)
	require.NoError(t, err)

	require.Greater(t, len(requestBytes), len(legacyBytes))
	assert.True(t, bytes.Equal(legacyBytes, requestBytes[:len(legacyBytes)]),
		"the app-v17 request must be an optional tail; legacy bytes cannot move")

	decoded, err := DecodeTx(requestBytes)
	require.NoError(t, err)
	assert.Equal(t, withRequest.AgentRequest, decoded.AgentRequest)
}

func TestAgentRequestAbsentRoundTripsAbsent(t *testing.T) {
	parsed := sampleSubmitTx()
	encoded, err := EncodeTx(parsed)
	require.NoError(t, err)
	decoded, err := DecodeTx(encoded)
	require.NoError(t, err)
	assert.Empty(t, decoded.AgentRequest)

	reencoded, err := EncodeTx(decoded)
	require.NoError(t, err)
	assert.Equal(t, encoded, reencoded)
}

func TestAgentRequestEncodeCap(t *testing.T) {
	parsed := sampleSubmitTx()
	parsed.AgentRequest = make([]byte, MaxAgentRequestSize+1)
	_, err := EncodeTx(parsed)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "agent request too large")
}
