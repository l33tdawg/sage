package main

import (
	"context"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// signerFenceIntentStore adapts the node's local SQLite store to the fence's
// durability hook.
//
// The adapter exists so internal/tx keeps owning no storage and internal/store
// keeps owning no fence semantics: tx declares the shape it needs, the store
// persists rows, and this file is the only place the two meet. It is
// node-local by construction — SQLite is the serving projection, not consensus
// state, and nothing here may reach the chain or the AppHash.
type signerFenceIntentStore struct {
	store *store.SQLiteStore
}

func (a signerFenceIntentStore) SaveFenceIntent(ctx context.Context, intent tx.FenceIntent) error {
	return a.store.SaveSignerFenceIntent(ctx, store.SignerFenceIntent{
		SignerPubKeyHex: intent.SignerPubKeyHex,
		TxHash:          intent.TxHash,
		Nonce:           intent.Nonce,
		HasNonce:        intent.HasNonce,
		CreatedAt:       intent.CreatedAt,
	})
}

func (a signerFenceIntentStore) DeleteFenceIntent(ctx context.Context, signerPubKeyHex string) error {
	return a.store.DeleteSignerFenceIntent(ctx, signerPubKeyHex)
}

func (a signerFenceIntentStore) ListFenceIntents(ctx context.Context) ([]tx.FenceIntent, error) {
	rows, err := a.store.ListSignerFenceIntents(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]tx.FenceIntent, 0, len(rows))
	for _, row := range rows {
		out = append(out, tx.FenceIntent{
			SignerPubKeyHex: row.SignerPubKeyHex,
			TxHash:          row.TxHash,
			Nonce:           row.Nonce,
			HasNonce:        row.HasNonce,
			CreatedAt:       row.CreatedAt,
		})
	}
	return out, nil
}
