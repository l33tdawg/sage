package snapshot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/store"
)

// publicMemoryRootKey mirrors the store's sparse-index node key layout
// ("public-memory-index:v1:node:<depth>" + path); the root node lives at depth
// 0 with an all-zero path. Fixture-only: the expected digest is always taken
// from the store's own implementation, never from this layout.
func publicMemoryRootKey() []byte {
	key := []byte("public-memory-index:v1:node:")
	key = append(key, 0, 0)
	return append(key, make([]byte, 32)...)
}

// seedAppV28Commitment gives a seeded data dir the public-memory commitment an
// app-v28-active chain carries and returns the digest that chain would commit.
func seedAppV28Commitment(t *testing.T, badgerDir string) []byte {
	t.Helper()

	opts := badger.DefaultOptions(badgerDir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("open badger to seed app-v28 commitment: %v", err)
	}
	root := sha256.Sum256([]byte("app-v28 test public-memory root"))
	if err := db.Update(func(txn *badger.Txn) error {
		return txn.Set(publicMemoryRootKey(), root[:])
	}); err != nil {
		t.Fatalf("seed public-memory root: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close seeded badger: %v", err)
	}

	restored, err := store.OpenBadgerStoreReadOnly(badgerDir)
	if err != nil {
		t.Fatalf("open read-only store: %v", err)
	}
	composite, err := restored.ComputePublicMemoryAppHash()
	if err != nil {
		t.Fatalf("compute app-v28 composite AppHash: %v", err)
	}
	if err := restored.CloseBadger(); err != nil {
		t.Fatalf("close read-only store: %v", err)
	}

	// The fixture must be non-degenerate: an app-v28 commit is unprovable under
	// every earlier era, which is the whole reason this rule has to be in the
	// candidate set.
	all, err := computeAppHashAllRulesStandalone(badgerDir)
	if err != nil {
		t.Fatalf("standalone rules: %v", err)
	}
	for _, older := range splitConcatenatedHashes(all) {
		if bytes.Equal(older, composite) {
			t.Fatal("fixture is degenerate: app-v28 composite matched an older rule")
		}
	}
	return composite
}

// TestVerifyAcceptsAppV28CompositeAppHash is the regression for the updater
// deadlock this cost us on a v28-active node: TakeVerified records the
// committed AppHash, a v28 commit is the composite commitment rather than the
// app-v13 digest, and the proof only knew the three older eras. The updater
// then refused to install with "AppHash mismatch under every hash rule
// (legacy/app-v12/app-v13)" and the pre-upgrade snapshot gate never opened.
func TestVerifyAcceptsAppV28CompositeAppHash(t *testing.T) {
	parent := t.TempDir()
	srcData := filepath.Join(parent, "src", "data")
	if err := os.MkdirAll(srcData, 0o700); err != nil {
		t.Fatalf("mkdir srcData: %v", err)
	}
	vaultPath, _ := seedDataDir(t, srcData)
	composite := seedAppV28Commitment(t, filepath.Join(srcData, "badger"))

	const height = int64(57751)
	manifest, err := Take(context.Background(), srcData, height, composite, "pre-upgrade-v28", Options{
		BinaryVersion: "v11.23.2-test",
		VaultKeyPath:  vaultPath,
		IncludeBinary: false,
	})
	if err != nil {
		t.Fatalf("Take: %v", err)
	}
	if !bytes.Equal(manifest.AppHash, composite) {
		t.Fatalf("manifest app_hash: got %x want %x", manifest.AppHash, composite)
	}

	snapDir := filepath.Join(snapshotsRoot(srcData), fmt.Sprintf("%d", height))
	if err := VerifyWithOptions(snapDir, VerifyOptions{}); err != nil {
		t.Fatalf("verify a snapshot committed under the app-v28 rule: %v", err)
	}
}
