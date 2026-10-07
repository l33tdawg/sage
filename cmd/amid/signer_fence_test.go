package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/tx"
	"github.com/stretchr/testify/require"
)

func TestAMIDSignerFenceStartupRefusesCorruptLedger(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, amidSignerFenceLedgerName), []byte("corrupt ledger"), 0600))
	cleanup, restored, err := prepareAMIDSignerFences(context.Background(), dir, "", nil)
	require.Error(t, err)
	require.Nil(t, cleanup)
	require.Zero(t, restored)
}

func TestAMIDSignerFenceSurvivesAbruptProcessExit(t *testing.T) {
	dir := t.TempDir()
	for _, phase := range []string{"submit", "restore", "restore", "prove", "clear"} {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		binary, err := os.Executable()
		require.NoError(t, err)
		child := exec.CommandContext(ctx, binary, "-test.run=^TestAMIDSignerFenceChild$")
		child.Env = append(os.Environ(), "SAGE_AMID_FENCE_TEST_PHASE="+phase, "SAGE_AMID_FENCE_TEST_DIR="+dir)
		out, err := child.CombinedOutput()
		cancel()
		require.NoError(t, err, "phase %s: %s", phase, out)
	}
	files, err := filepath.Glob(filepath.Join(dir, amidSignerFenceLedgerName+"*"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, file := range files {
		data, err := os.ReadFile(file)
		require.NoError(t, err)
		require.False(t, bytes.Contains(data, []byte("restart-fixture-private-target")),
			"signed vote payload leaked into ledger/WAL: %s", file)
	}
}

// Each stage runs in a fresh process, so no in-memory nonce/fence/store state
// can satisfy the restore assertion. The first process exits at the actual
// pre-broadcast registration boundary without running deferred cleanup.
func TestAMIDSignerFenceChild(t *testing.T) {
	phase := os.Getenv("SAGE_AMID_FENCE_TEST_PHASE")
	if phase == "" {
		t.Skip("subprocess fixture")
	}
	dir := os.Getenv("SAGE_AMID_FENCE_TEST_DIR")
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	signer := hex.EncodeToString(key.Public().(ed25519.PublicKey))
	receiptPath := filepath.Join(dir, "test-receipt.json")
	var receipt tx.FenceIntent
	if phase != "submit" {
		data, err := os.ReadFile(receiptPath)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(data, &receipt))
	}
	cleanup, restored, err := prepareAMIDSignerFences(context.Background(), dir, "", func(ed25519.PublicKey) (uint64, bool) {
		if phase == "prove" {
			return receipt.Nonce, true
		}
		return 0, false
	})
	require.NoError(t, err)
	defer cleanup()
	if phase == "submit" {
		require.Zero(t, restored)
		err = tx.WithNonceLease(context.Background(), key, func(nonce uint64) error {
			parsed := &tx.ParsedTx{Type: tx.TxTypeMemoryVote, Nonce: nonce,
				MemoryVote: &tx.MemoryVote{MemoryID: "restart-fixture-private-target", Decision: tx.VoteDecisionAccept}}
			require.NoError(t, tx.SignTx(parsed, key))
			encoded, encodeErr := tx.EncodeTx(parsed)
			require.NoError(t, encodeErr)
			tx.RegisterSubmittedTx(key, encoded, nil)
			hash := tx.CometTxHash(encoded)
			receipt := tx.FenceIntent{SignerPubKeyHex: signer, TxHash: strings.ToUpper(hex.EncodeToString(hash[:])), Nonce: nonce, HasNonce: true}
			data, marshalErr := json.Marshal(receipt)
			require.NoError(t, marshalErr)
			require.NoError(t, os.WriteFile(receiptPath, data, 0600))
			os.Exit(0)
			return nil
		})
		require.NoError(t, err)
		t.Fatal("fixture failed to exit at registration")
	}
	if phase == "clear" {
		require.Zero(t, restored, "a chain-proven fate must retire the durable record")
		require.Empty(t, tx.FencedSigners())
		return
	}
	require.Equal(t, 1, restored, "AMID must re-raise the previous process's durable fence before signing")
	if phase == "prove" {
		require.Eventually(t, func() bool { return len(tx.FencedSigners()) == 0 }, 5*time.Second, 5*time.Millisecond,
			"the same canonical nonce-floor proof used by allocation must settle the restored fence")
		return
	}
	require.Equal(t, "restore", phase)
	fences := tx.FencedSigners()
	require.Len(t, fences, 1)
	require.Equal(t, receipt.SignerPubKeyHex, fences[0].SignerPubKeyHex)
	require.Equal(t, receipt.TxHash, fences[0].TxHash)
	require.Equal(t, receipt.Nonce, fences[0].Nonce)
	require.True(t, fences[0].HasNonce)
	signed := false
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = tx.WithNonceLease(ctx, key, func(uint64) error { signed = true; return nil })
	require.True(t, errors.Is(err, tx.ErrSignerFenced), "an unproven restored fence must refuse the next nonce: %v", err)
	require.False(t, signed)
}
