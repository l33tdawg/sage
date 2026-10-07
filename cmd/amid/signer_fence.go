package main

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"path/filepath"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

type amidSignerFenceIntentStore struct{ db *store.SignerFenceIntentDB }

func (s amidSignerFenceIntentStore) SaveFenceIntent(ctx context.Context, intent tx.FenceIntent) error {
	return s.db.SaveSignerFenceIntent(ctx, store.SignerFenceIntent{
		SignerPubKeyHex: intent.SignerPubKeyHex, TxHash: intent.TxHash,
		Nonce: intent.Nonce, HasNonce: intent.HasNonce, CreatedAt: intent.CreatedAt,
	})
}

func (s amidSignerFenceIntentStore) DeleteFenceIntent(ctx context.Context, signer string) error {
	return s.db.DeleteSignerFenceIntent(ctx, signer)
}

func (s amidSignerFenceIntentStore) ListFenceIntents(ctx context.Context) ([]tx.FenceIntent, error) {
	rows, err := s.db.ListSignerFenceIntents(ctx)
	if err != nil {
		return nil, err
	}
	intents := make([]tx.FenceIntent, 0, len(rows))
	for _, row := range rows {
		intents = append(intents, tx.FenceIntent{
			SignerPubKeyHex: row.SignerPubKeyHex, TxHash: row.TxHash,
			Nonce: row.Nonce, HasNonce: row.HasNonce, CreatedAt: row.CreatedAt,
		})
	}
	return intents, nil
}

const amidSignerFenceLedgerName = "signer-fence-intents.sqlite"

// Called before either AMID mode starts listeners or signing producers. The
// ledger contains identity only; it cannot recover bytes lost by an older,
// unprotected process. Restored fences resolve only on chain-proven fate.
func prepareAMIDSignerFences(ctx context.Context, badgerPath, cometRPC string, nonceFloor func(ed25519.PublicKey) (uint64, bool)) (func(), int, error) {
	db, err := store.OpenSignerFenceIntentDB(ctx, filepath.Join(badgerPath, amidSignerFenceLedgerName))
	if err != nil {
		return nil, 0, fmt.Errorf("open AMID signer fence ledger: %w", err)
	}
	tx.SetNonceFloorFunc(nonceFloor)
	tx.SetTxResolverFunc(tx.CometTxResolver(cometRPC))
	tx.SetFenceProverFunc(func(ctx context.Context, fence tx.FencedSigner) (tx.FenceLiftProof, error) {
		return tx.ProveFenceLiftFromChain(ctx, cometRPC, nonceFloor, fence)
	})
	// Multi-validator AMID nodes do not adopt the desktop's automatic
	// single-node abandonment policy. A missing lookup never clears a fence.
	tx.SetFenceAutoResolverFunc(func(context.Context, tx.FencedSigner) (bool, string, error) {
		return false, "automatic abandonment is disabled for AMID; waiting for chain-proven fate", nil
	})
	tx.SetFenceIntentStore(amidSignerFenceIntentStore{db: db})
	cleanup := func() {
		tx.SetFenceIntentStore(nil)
		tx.SetFenceProverFunc(nil)
		tx.SetFenceAutoResolverFunc(nil)
		tx.SetTxResolverFunc(nil)
		_ = db.Close()
	}
	restored, err := tx.RestoreFencesFromIntents(ctx)
	if err != nil {
		cleanup()
		return nil, 0, fmt.Errorf("restore AMID signer fences: %w", err)
	}
	return cleanup, restored, nil
}

func amidNonceFloor(db *store.BadgerStore) func(ed25519.PublicKey) (uint64, bool) {
	return func(pub ed25519.PublicKey) (uint64, bool) {
		nonce, err := db.GetNonce(auth.PublicKeyToAgentID(pub))
		return nonce, err == nil && nonce > 0
	}
}
