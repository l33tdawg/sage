package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// SignerFenceIntent is the node-local durable record of a submission whose fate
// was never proven — the shadow the signer fence needs in order to survive a
// restart.
//
// It is deliberately NOT consensus state and NOT AppHash-covered: it describes
// something only this node did (allocate a nonce and hand bytes to a transport),
// so putting it on-chain would make every validator agree about one node's
// in-flight request. It is also deliberately identity-only — no signed bytes.
// The bytes routinely carry memory content that must not be copied into a
// plaintext table, so the record names the transaction rather than reproducing
// it, and the fence resolves by proven fate instead of by re-submission (see the
// notes on tx.RestoreFencesFromIntents).
type SignerFenceIntent struct {
	SignerPubKeyHex string
	TxHash          string
	Nonce           uint64
	HasNonce        bool
	CreatedAt       time.Time
}

// migrateSignerFenceIntent creates the durable-intent table. One row per signer:
// the fence only ever asks "is anything unresolved for this key", so history
// would be a log, and this is not one.
func (s *SQLiteStore) migrateSignerFenceIntent(ctx context.Context) error {
	if _, err := s.writeExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS signer_fence_intent (
		signer_pubkey_hex TEXT PRIMARY KEY,
		tx_hash           TEXT NOT NULL DEFAULT '',
		nonce             INTEGER NOT NULL DEFAULT 0,
		has_nonce         INTEGER NOT NULL DEFAULT 0,
		created_at        TEXT NOT NULL
		)`); err != nil {
		return fmt.Errorf("create signer fence intent table: %w", err)
	}
	return nil
}

// SaveSignerFenceIntent records (or replaces) the unresolved intent for a
// signer. Replace rather than insert-and-keep: at most one submission per signer
// is ever unresolved, because the lease that guards nonce allocation also
// guards this write.
func (s *SQLiteStore) SaveSignerFenceIntent(ctx context.Context, intent SignerFenceIntent) error {
	signer := strings.ToLower(strings.TrimSpace(intent.SignerPubKeyHex))
	if signer == "" {
		return errors.New("signer fence intent requires a signer public key")
	}
	hasNonce := 0
	if intent.HasNonce {
		hasNonce = 1
	}
	createdAt := intent.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	if _, err := s.writeExecContext(ctx, `
		INSERT INTO signer_fence_intent (signer_pubkey_hex, tx_hash, nonce, has_nonce, created_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(signer_pubkey_hex) DO UPDATE SET
			tx_hash=excluded.tx_hash, nonce=excluded.nonce,
			has_nonce=excluded.has_nonce, created_at=excluded.created_at`,
		signer, strings.ToUpper(strings.TrimSpace(intent.TxHash)), intent.Nonce, hasNonce,
		createdAt.Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("save signer fence intent: %w", err)
	}
	return nil
}

// DeleteSignerFenceIntent retires a signer's intent once its fate is proven.
func (s *SQLiteStore) DeleteSignerFenceIntent(ctx context.Context, signerPubKeyHex string) error {
	signer := strings.ToLower(strings.TrimSpace(signerPubKeyHex))
	if signer == "" {
		return nil
	}
	if _, err := s.writeExecContext(ctx,
		`DELETE FROM signer_fence_intent WHERE signer_pubkey_hex = ?`, signer); err != nil {
		return fmt.Errorf("delete signer fence intent: %w", err)
	}
	return nil
}

// ListSignerFenceIntents returns every unresolved intent, oldest first, so a
// startup restore reports the longest-held fence first.
func (s *SQLiteStore) ListSignerFenceIntents(ctx context.Context) ([]SignerFenceIntent, error) {
	rows, err := s.conn.QueryContext(ctx, `
		SELECT signer_pubkey_hex, tx_hash, nonce, has_nonce, created_at
		FROM signer_fence_intent ORDER BY created_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("list signer fence intents: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []SignerFenceIntent
	for rows.Next() {
		var (
			intent    SignerFenceIntent
			nonce     int64
			hasNonce  int
			createdAt string
		)
		if err := rows.Scan(&intent.SignerPubKeyHex, &intent.TxHash, &nonce, &hasNonce, &createdAt); err != nil {
			return nil, fmt.Errorf("scan signer fence intent: %w", err)
		}
		if nonce < 0 {
			return nil, errors.New("signer fence intent carries a negative nonce")
		}
		intent.Nonce = uint64(nonce) // #nosec G115 -- guarded non-negative above
		intent.HasNonce = hasNonce != 0
		if parsed, parseErr := time.Parse(time.RFC3339Nano, createdAt); parseErr == nil {
			intent.CreatedAt = parsed
		}
		out = append(out, intent)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate signer fence intents: %w", err)
	}
	return out, nil
}
