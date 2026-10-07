package store

import (
	"encoding/base64"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/l33tdawg/sage/internal/vault"
)

func publicationTestVault(t *testing.T) *vault.Vault {
	t.Helper()
	active, err := vault.FromDataKey(base64.StdEncoding.EncodeToString(make([]byte, 32)))
	if err != nil {
		t.Fatal("synthetic vault construction failed")
	}
	return active
}

func TestLanternVaultPublicationLatchesExpectedAndGeneration(t *testing.T) {
	projection := &SQLiteStore{vaultGeneration: &atomic.Uint64{}}
	active := publicationTestVault(t)
	initial := projection.MessageStorageStatus()
	if initial.EncryptionExpected || initial.VaultActive {
		t.Fatal("unexpected initial encryption state")
	}
	// Attaching a vault goes through ActivateVault: publication must never expose
	// an attached vault on a store that does not yet require encryption. Detaching
	// stays on SetVault, which deliberately leaves the requirement latched.
	for index, next := range []*vault.Vault{active, nil, active, active, nil} {
		if next == nil {
			projection.SetVault(nil)
		} else {
			projection.ActivateVault(next)
		}
		status := projection.MessageStorageStatus()
		if !status.EncryptionExpected || status.VaultActive != (next != nil) || !status.Stable || status.VaultGeneration != uint64(index+1) {
			t.Fatal("vault publication lost expected state or generation")
		}
		if next == nil {
			if !projection.VaultLocked() {
				t.Fatal("detached vault must remain locked")
			}
			if _, err := projection.encryptContent("synthetic content"); err == nil {
				t.Fatal("detached vault admitted plaintext")
			}
		} else {
			ciphertext, err := projection.encryptContent("synthetic content")
			if err != nil || ciphertext == "synthetic content" {
				t.Fatal("active vault failed encrypted write")
			}
		}
	}
}

func TestLanternVaultPublicationSnapshotIsAtomic(t *testing.T) {
	projection := &SQLiteStore{vaultGeneration: &atomic.Uint64{}}
	active := publicationTestVault(t)
	var invalid atomic.Bool
	var workers sync.WaitGroup
	start := make(chan struct{})
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		for iteration := 0; iteration < 1000; iteration++ {
			projection.ActivateVault(active)
			projection.SetVault(nil)
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		for iteration := 0; iteration < 1000; iteration++ {
			status := projection.MessageStorageStatus()
			if !status.Stable || (status.VaultActive && !status.EncryptionExpected) || (status.VaultGeneration > 0 && !status.EncryptionExpected) {
				invalid.Store(true)
			}
		}
	}()
	close(start)
	workers.Wait()
	if invalid.Load() {
		t.Fatal("observed partially published encryption state")
	}
}
