package auth

import (
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateKeypair(t *testing.T) {
	pub, priv, err := GenerateKeypair()
	require.NoError(t, err)
	assert.Len(t, pub, ed25519.PublicKeySize)
	assert.Len(t, priv, ed25519.PrivateKeySize)
}

func TestSignVerify(t *testing.T) {
	pub, priv, err := GenerateKeypair()
	require.NoError(t, err)

	message := []byte("test message for SAGE")
	sig := Sign(priv, message)

	assert.True(t, Verify(pub, message, sig))
	assert.False(t, Verify(pub, []byte("tampered message"), sig))
}

func TestPublicKeyAgentIDRoundtrip(t *testing.T) {
	pub, _, err := GenerateKeypair()
	require.NoError(t, err)

	agentID := PublicKeyToAgentID(pub)
	assert.Len(t, agentID, ed25519.PublicKeySize*2) // hex encoding doubles length

	recovered, err := AgentIDToPublicKey(agentID)
	require.NoError(t, err)
	assert.Equal(t, pub, recovered)
}

func TestAgentIDToPublicKeyInvalid(t *testing.T) {
	_, err := AgentIDToPublicKey("not-hex")
	assert.Error(t, err)

	_, err = AgentIDToPublicKey("aabb")
	assert.Error(t, err) // too short
}

func TestSignRequestVerifyRequest(t *testing.T) {
	pub, priv, err := GenerateKeypair()
	require.NoError(t, err)

	body := []byte(`{"content":"test memory","domain_tag":"crypto"}`)
	ts := time.Now().Unix()

	sig := SignRequest(priv, "POST", "/v1/memory/submit", body, ts)
	assert.True(t, VerifyRequest(pub, "POST", "/v1/memory/submit", body, ts, sig))
}

func TestVerifyRequestWrongKey(t *testing.T) {
	_, priv, err := GenerateKeypair()
	require.NoError(t, err)
	otherPub, _, err := GenerateKeypair()
	require.NoError(t, err)

	body := []byte(`{"content":"test"}`)
	ts := time.Now().Unix()
	sig := SignRequest(priv, "POST", "/v1/memory/submit", body, ts)

	assert.False(t, VerifyRequest(otherPub, "POST", "/v1/memory/submit", body, ts, sig))
}

func TestVerifyRequestTamperedBody(t *testing.T) {
	pub, priv, err := GenerateKeypair()
	require.NoError(t, err)

	body := []byte(`{"content":"original"}`)
	ts := time.Now().Unix()
	sig := SignRequest(priv, "POST", "/v1/memory/submit", body, ts)

	tampered := []byte(`{"content":"tampered"}`)
	assert.False(t, VerifyRequest(pub, "POST", "/v1/memory/submit", tampered, ts, sig))
}

func TestSignRequestEmptyBody(t *testing.T) {
	pub, priv, err := GenerateKeypair()
	require.NoError(t, err)

	ts := time.Now().Unix()
	sig := SignRequest(priv, "GET", "/v1/memory/query", nil, ts)
	assert.True(t, VerifyRequest(pub, "GET", "/v1/memory/query", nil, ts, sig))
	assert.True(t, VerifyRequest(pub, "GET", "/v1/memory/query", []byte{}, ts, sig))
}

func TestVerifyRequestCrossEndpointReplay(t *testing.T) {
	pub, priv, err := GenerateKeypair()
	require.NoError(t, err)

	body := []byte(`{"content":"test"}`)
	ts := time.Now().Unix()
	sig := SignRequest(priv, "POST", "/v1/memory/submit", body, ts)

	// Same body + timestamp but different path — must fail
	assert.False(t, VerifyRequest(pub, "POST", "/v1/memory/query", body, ts, sig))
	// Same body + timestamp but different method — must fail
	assert.False(t, VerifyRequest(pub, "GET", "/v1/memory/submit", body, ts, sig))
}
