package store

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSignerFenceLedgerUsesOnlyIdentityAndFullDurability(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "fence.sqlite")
	ledger, err := OpenSignerFenceIntentDB(ctx, path)
	require.NoError(t, err)
	var journal string
	var syncMode int
	require.NoError(t, ledger.sqlite.db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal))
	require.NoError(t, ledger.sqlite.db.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&syncMode))
	require.Equal(t, "wal", journal)
	require.Equal(t, 2, syncMode)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	intent := SignerFenceIntent{SignerPubKeyHex: strings.Repeat("12", 32), TxHash: strings.Repeat("AB", 32),
		Nonce: 1790000000000000123, HasNonce: true, CreatedAt: time.Now().UTC()}
	require.NoError(t, ledger.SaveSignerFenceIntent(ctx, intent))
	require.NoError(t, ledger.Close())
	ledger, err = OpenSignerFenceIntentDB(ctx, path)
	require.NoError(t, err)
	defer ledger.Close()
	rows, err := ledger.ListSignerFenceIntents(ctx)
	require.NoError(t, err)
	require.Equal(t, []SignerFenceIntent{intent}, rows)
	var tables int
	require.NoError(t, ledger.sqlite.db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table'").Scan(&tables))
	require.Equal(t, 1, tables, "the local fence ledger must not initialize serving/content tables")
	require.NoError(t, ledger.DeleteSignerFenceIntent(ctx, intent.SignerPubKeyHex))
	rows, err = ledger.ListSignerFenceIntents(ctx)
	require.NoError(t, err)
	require.Empty(t, rows)
}

func TestSignerFenceLedgerRejectsMalformedStartupRows(t *testing.T) {
	ctx := context.Background()
	ledger, err := OpenSignerFenceIntentDB(ctx, filepath.Join(t.TempDir(), "fence.sqlite"))
	require.NoError(t, err)
	defer ledger.Close()
	pub := hex.EncodeToString(make([]byte, 32))
	for _, intent := range []SignerFenceIntent{
		{SignerPubKeyHex: "not-an-ed25519-key"},
		{SignerPubKeyHex: pub, TxHash: "not-a-transaction-hash"},
	} {
		require.Error(t, ledger.SaveSignerFenceIntent(ctx, intent))
	}
	_, err = ledger.sqlite.db.ExecContext(ctx, "INSERT INTO signer_fence_intent(signer_pubkey_hex, created_at) VALUES (?, ?)", "corrupt", time.Now().UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)
	_, err = ledger.ListSignerFenceIntents(ctx)
	require.ErrorContains(t, err, "invalid Ed25519 signer", "startup must fail instead of skipping a corrupt signer")
}
