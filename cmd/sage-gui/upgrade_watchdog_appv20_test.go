package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
)

func TestBuildUpgradeProposeTxAppV20BindsChainGovernanceDomain(t *testing.T) {
	const chainID = "sage-internet-federation-test"
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	built, err := buildUpgradeProposeTx(upgradeWatchdogConfig{
		AgentKey: privateKey,
		ChainID:  chainID,
	}, 20)
	require.NoError(t, err)
	require.NotNil(t, built.UpgradePropose)

	digest := sha256.Sum256(append(
		[]byte("sage/governance-delegation-domain/v20\x00"),
		[]byte(chainID)...,
	))
	wantDomain := hex.EncodeToString(digest[:])
	assert.Equal(t, wantDomain, built.UpgradePropose.GovernanceDomain)

	raw, err := tx.EncodeTx(built)
	require.NoError(t, err)
	decoded, err := tx.DecodeTx(raw)
	require.NoError(t, err)
	require.NotNil(t, decoded.UpgradePropose)
	assert.Equal(t, wantDomain, decoded.UpgradePropose.GovernanceDomain)

	valid, err := tx.VerifyTx(decoded)
	require.NoError(t, err)
	assert.True(t, valid, "outer signature must cover the governance-domain wire tail")

	var timestamp [8]byte
	binary.BigEndian.PutUint64(timestamp[:], uint64(decoded.AgentTimestamp)) // #nosec G115 -- builder timestamps are non-negative
	agentMessage := make([]byte, 0, len(decoded.AgentBodyHash)+len(timestamp))
	agentMessage = append(agentMessage, decoded.AgentBodyHash...)
	agentMessage = append(agentMessage, timestamp[:]...)
	assert.True(t, ed25519.Verify(decoded.AgentPubKey, agentMessage, decoded.AgentSig))

	reencoded, err := tx.EncodeTx(decoded)
	require.NoError(t, err)
	assert.Equal(t, raw, reencoded, "signed app-v20 proposal must round-trip canonically")
}

func TestBuildUpgradeProposeTxAppV20RejectsInvalidChainID(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	for _, test := range []struct {
		name      string
		chainID   string
		wantError string
	}{
		{name: "empty", chainID: "", wantError: "chain_id must be non-empty"},
		{name: "over CometBFT limit", chainID: strings.Repeat("x", 51), wantError: "chain_id exceeds 50 bytes"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, buildErr := buildUpgradeProposeTx(upgradeWatchdogConfig{
				AgentKey: privateKey,
				ChainID:  test.chainID,
			}, 20)
			require.ErrorContains(t, buildErr, test.wantError)
		})
	}
}

func TestBuildUpgradeProposeTxTarget19KeepsLegacyWirePayload(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	// An empty ChainID is deliberately accepted for pre-v20 proposals: the
	// governance-domain extension must remain absent outside its activation
	// target, preserving the historical UpgradePropose payload exactly.
	built, err := buildUpgradeProposeTx(upgradeWatchdogConfig{
		AgentKey: privateKey,
		ChainID:  "",
	}, 19)
	require.NoError(t, err)
	require.NotNil(t, built.UpgradePropose)
	assert.Empty(t, built.UpgradePropose.GovernanceDomain)
	assert.Zero(t, built.UpgradePropose.UpgradeDelayBlocks)

	raw, err := tx.EncodeTx(built)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(raw), 5)
	payloadLength := int(binary.BigEndian.Uint32(raw[1:5]))
	require.LessOrEqual(t, 5+payloadLength, len(raw))
	payload := raw[5 : 5+payloadLength]

	var expected []byte
	appendString := func(value string) {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(value))) // #nosec G115 -- encoded builder fields are tiny
		expected = append(expected, length[:]...)
		expected = append(expected, value...)
	}
	appendString(built.UpgradePropose.Name)
	var target [8]byte
	binary.BigEndian.PutUint64(target[:], 19)
	expected = append(expected, target[:]...)
	appendString(built.UpgradePropose.BinarySHA256)
	appendString(built.UpgradePropose.ProposerID)
	expected = append(expected, make([]byte, 8)...) // UpgradeDelayBlocks == 0
	assert.Equal(t, expected, payload, "target 19 must not append even an empty governance-domain length prefix")

	decoded, err := tx.DecodeTx(raw)
	require.NoError(t, err)
	require.NotNil(t, decoded.UpgradePropose)
	assert.Empty(t, decoded.UpgradePropose.GovernanceDomain)
	valid, err := tx.VerifyTx(decoded)
	require.NoError(t, err)
	assert.True(t, valid)
}
