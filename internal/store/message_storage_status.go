package store

import (
	"context"
	"errors"
	"fmt"
)

var ErrEncryptedMessageStorageRequired = errors.New("encrypted message storage required")
var ErrEncryptedMessageStandaloneRequired = errors.New("encrypted message requires standalone store")

type MessageStorageStatus struct {
	EncryptionExpected bool   `json:"encryption_expected"`
	VaultActive        bool   `json:"vault_active"`
	VaultGeneration    uint64 `json:"vault_generation"`
	Stable             bool   `json:"stable"`
}

func (s *SQLiteStore) MessageStorageStatus() MessageStorageStatus {
	s.vaultPublicationMu.RLock()
	defer s.vaultPublicationMu.RUnlock()
	before := s.VaultGeneration()
	status := MessageStorageStatus{
		EncryptionExpected: s.vaultExpected.Load(),
		VaultActive:        s.VaultActive(),
		VaultGeneration:    before,
	}
	status.Stable = before == s.VaultGeneration()
	return status
}

func (s *SQLiteStore) SendEncryptedLocalMessage(ctx context.Context, key string, message *PipelineMessage) (*PipelineMessage, bool, error) {
	if s == nil || s.db == nil {
		return nil, false, ErrEncryptedMessageStandaloneRequired
	}
	unlock, err := s.lockVaultWrite(ctx)
	if err != nil {
		return nil, false, err
	}
	defer unlock()
	if !s.vaultExpected.Load() || !s.VaultActive() {
		return nil, false, ErrEncryptedMessageStorageRequired
	}
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, fmt.Errorf("begin encrypted message: %w", err)
	}
	defer transaction.Rollback() //nolint:errcheck
	result, replayed, err := s.transactionClone(transaction, false).SendLocalMessage(ctx, key, message)
	if err != nil {
		return nil, false, err
	}
	if err := transaction.Commit(); err != nil {
		return nil, false, fmt.Errorf("commit encrypted message: %w", err)
	}
	return result, replayed, nil
}
