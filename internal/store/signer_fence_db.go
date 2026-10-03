package store

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

// SignerFenceIntentDB stores only node-local, identity-only submission records.
// It is separate from canonical Badger state and from any shared PostgreSQL
// serving projection. Its file must stay with the node across restarts.
type SignerFenceIntentDB struct {
	sqlite *SQLiteStore
}

func OpenSignerFenceIntentDB(ctx context.Context, path string) (*SignerFenceIntentDB, error) {
	if info, err := os.Lstat(path); err == nil && !info.Mode().IsRegular() {
		return nil, fmt.Errorf("signer fence ledger must be a regular file")
	} else if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("inspect signer fence ledger: %w", err)
	}
	// Create privately before SQLite opens its WAL and shared-memory files.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open signer fence ledger: %w", err)
	}
	if chmodErr := f.Chmod(0600); chmodErr != nil {
		_ = f.Close()
		return nil, fmt.Errorf("protect signer fence ledger: %w", chmodErr)
	}
	if closeErr := f.Close(); closeErr != nil {
		return nil, fmt.Errorf("close signer fence ledger file: %w", closeErr)
	}
	db, err := openSQLiteDB(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("open signer fence ledger database: %w", err)
	}
	s := &SQLiteStore{conn: db, db: db, dbPath: path}
	if err := s.migrateSignerFenceIntent(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &SignerFenceIntentDB{sqlite: s}, nil
}

func (s *SignerFenceIntentDB) Close() error { return s.sqlite.Close() }

func validateSignerFenceIntent(intent SignerFenceIntent) error {
	pub, err := hex.DecodeString(strings.TrimSpace(intent.SignerPubKeyHex))
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("signer fence ledger contains an invalid Ed25519 signer")
	}
	if intent.TxHash != "" {
		hash, err := hex.DecodeString(strings.TrimSpace(intent.TxHash))
		if err != nil || len(hash) != sha256.Size {
			return fmt.Errorf("signer fence ledger contains an invalid transaction hash")
		}
	}
	return nil
}

func (s *SignerFenceIntentDB) SaveSignerFenceIntent(ctx context.Context, intent SignerFenceIntent) error {
	if err := validateSignerFenceIntent(intent); err != nil {
		return err
	}
	return s.sqlite.SaveSignerFenceIntent(ctx, intent)
}

func (s *SignerFenceIntentDB) DeleteSignerFenceIntent(ctx context.Context, signer string) error {
	return s.sqlite.DeleteSignerFenceIntent(ctx, signer)
}

func (s *SignerFenceIntentDB) ListSignerFenceIntents(ctx context.Context) ([]SignerFenceIntent, error) {
	intents, err := s.sqlite.ListSignerFenceIntents(ctx)
	if err != nil {
		return nil, err
	}
	// The tx restorer skips invalid signer rows. Reject the entire startup read
	// instead: a corrupt ledger must never silently leave a signer unprotected.
	for _, intent := range intents {
		if err := validateSignerFenceIntent(intent); err != nil {
			return nil, err
		}
	}
	return intents, nil
}
