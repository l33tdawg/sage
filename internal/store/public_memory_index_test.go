package store

import (
	"crypto/sha256"
	"errors"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func publicTestRecord(t *testing.T, scoped *BadgerStore, identifier string) {
	t.Helper()
	hash := sha256.Sum256([]byte("synthetic:" + identifier))
	require.NoError(t, scoped.SetMemoryHash(identifier, hash[:], "committed"))
	require.NoError(t, scoped.SetMemoryAuthor(identifier, strings.Repeat("a", 64)))
	require.NoError(t, scoped.SetMemoryAuthorPrincipal(identifier, "synthetic-principal"))
	require.NoError(t, scoped.SetMemoryDomain(identifier, "public-test"))
	require.NoError(t, scoped.SetMemoryClassification(identifier, 0))
}

func TestPublicMemoryIndexOrderProofAndTampering(t *testing.T) {
	var roots [][32]byte
	for _, order := range [][]string{{"one", "two", "three"}, {"three", "one", "two"}} {
		base := newTestBadger(t)
		scoped := base.BeginConsensusTransaction(nil)
		require.NoError(t, scoped.InitializeEmptyPublicMemoryIndex())
		for _, identifier := range order {
			publicTestRecord(t, scoped, identifier)
			require.NoError(t, scoped.SyncPublicMemoryIndex(identifier))
		}
		require.NoError(t, scoped.CommitConsensusTransaction())
		proof, err := base.PublicMemoryBranch("one")
		require.NoError(t, err)
		require.NoError(t, VerifyPublicMemoryBranch(*proof, proof.Root))
		roots = append(roots, proof.Root)
		for _, change := range []func(*PublicMemoryBranch){
			func(value *PublicMemoryBranch) { value.Leaf.MemoryID = "two" },
			func(value *PublicMemoryBranch) { value.Leaf.Domain = "elsewhere" },
			func(value *PublicMemoryBranch) { value.Leaf.Author = strings.Repeat("b", 64) },
			func(value *PublicMemoryBranch) { value.Leaf.AuthorPrincipal = "another" },
			func(value *PublicMemoryBranch) { value.Leaf.Status = "challenged" },
			func(value *PublicMemoryBranch) { value.Leaf.ContentHash[0] ^= 1 },
			func(value *PublicMemoryBranch) { value.Siblings[255][0] ^= 1 },
		} {
			changed := *proof
			change(&changed)
			require.Error(t, VerifyPublicMemoryBranch(changed, proof.Root))
		}
	}
	require.Equal(t, roots[0], roots[1])
}

func TestPublicMemoryIndexLifecyclePrivacyAndAtomicity(t *testing.T) {
	base := newTestBadger(t)
	scoped := base.BeginConsensusTransaction(nil)
	require.NoError(t, scoped.InitializeEmptyPublicMemoryIndex())
	publicTestRecord(t, scoped, "one")
	require.NoError(t, scoped.SyncPublicMemoryIndex("one"))
	require.NoError(t, scoped.CommitConsensusTransaction())
	before, err := base.PublicMemoryBranch("one")
	require.NoError(t, err)
	scoped = base.BeginConsensusTransaction(nil)
	require.NoError(t, scoped.SetMemoryStatusPreservingHash("one", "challenged"))
	require.NoError(t, scoped.SyncPublicMemoryIndex("one"))
	_, err = scoped.PublicMemoryBranch("one")
	require.Error(t, err)
	unchanged, err := base.PublicMemoryBranch("one")
	require.NoError(t, err)
	require.Equal(t, before.Root, unchanged.Root)
	require.NoError(t, scoped.CommitConsensusTransaction())
	after, err := base.PublicMemoryBranch("one")
	require.NoError(t, err)
	require.NotEqual(t, before.Root, after.Root)
	require.Equal(t, "challenged", after.Leaf.Status)
	scoped = base.BeginConsensusTransaction(nil)
	require.NoError(t, scoped.SetMemoryClassification("one", 1))
	require.NoError(t, scoped.SyncPublicMemoryIndex("one"))
	require.NoError(t, scoped.CommitConsensusTransaction())
	_, err = base.PublicMemoryBranch("one")
	require.Error(t, err)
	transaction := base.db.NewTransaction(false)
	defer transaction.Discard()
	root, err := publicHashAt(transaction, [32]byte{}, 0)
	require.NoError(t, err)
	require.Equal(t, publicEmpty[0], root)
}

func TestPublicMemoryIndexFaultRollbackAndBoundedWrites(t *testing.T) {
	base := newTestBadger(t)
	scoped := base.BeginConsensusTransaction(nil)
	require.NoError(t, scoped.InitializeEmptyPublicMemoryIndex())
	publicTestRecord(t, scoped, "one")
	require.NoError(t, scoped.SyncPublicMemoryIndex("one"))
	require.NoError(t, scoped.CommitConsensusTransaction())
	before, err := base.PublicMemoryBranch("one")
	require.NoError(t, err)
	scoped = base.BeginConsensusTransaction(nil)
	require.NoError(t, scoped.SetMemoryStatusPreservingHash("one", "challenged"))
	writes := 0
	scoped.writeFaultHook = func(int) error {
		writes++
		if writes == 10 {
			return errors.New("synthetic fault")
		}
		return nil
	}
	require.Error(t, scoped.SyncPublicMemoryIndex("one"))
	require.Error(t, scoped.CommitConsensusTransaction())
	scoped.DiscardConsensusTransaction()
	after, err := base.PublicMemoryBranch("one")
	require.NoError(t, err)
	require.Equal(t, before, after)
	scoped = base.BeginConsensusTransaction(nil)
	require.NoError(t, scoped.SetMemoryStatusPreservingHash("one", "challenged"))
	writes = 0
	scoped.writeFaultHook = func(int) error { writes++; return nil }
	require.NoError(t, scoped.SyncPublicMemoryIndex("one"))
	require.Equal(t, 257, writes)
	require.NoError(t, scoped.SyncPublicMemoryIndex("one"))
	require.Equal(t, 257, writes)
	scoped.DiscardConsensusTransaction()
}

func TestPublicMemoryIndexPrewriteFailurePoisonsEarlierCanonicalChanges(t *testing.T) {
	for _, failure := range []string{"missing-binding", "corrupt-path", "invalid-id"} {
		t.Run(failure, func(t *testing.T) {
			base := newTestBadger(t)
			initial := base.BeginConsensusTransaction(nil)
			require.NoError(t, initial.InitializeEmptyPublicMemoryIndex())
			publicTestRecord(t, initial, "one")
			require.NoError(t, initial.SyncPublicMemoryIndex("one"))
			require.NoError(t, initial.CommitConsensusTransaction())
			before, err := base.PublicMemoryBranch("one")
			require.NoError(t, err)
			scoped := base.BeginConsensusTransaction(nil)
			require.NoError(t, scoped.SetMemoryStatusPreservingHash("one", "challenged"))
			identifier := "one"
			switch failure {
			case "missing-binding":
				require.NoError(t, scoped.SetMemoryAuthorPrincipal("one", ""))
			case "corrupt-path":
				require.NoError(t, scoped.update(func(txn *badger.Txn) error {
					return scoped.txnSet(txn, publicNodeKey(publicPath("one"), 256), make([]byte, 32))
				}))
			case "invalid-id":
				identifier = ""
			}
			attempts := scoped.writeAttempts
			require.Error(t, scoped.SyncPublicMemoryIndex(identifier))
			require.Equal(t, attempts, scoped.writeAttempts)
			require.Error(t, scoped.ConsensusTransactionError())
			require.Error(t, scoped.SetMemoryAuthorPrincipal("one", "synthetic-principal"))
			require.Error(t, scoped.CommitConsensusTransaction())
			scoped.DiscardConsensusTransaction()
			after, err := base.PublicMemoryBranch("one")
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestPublicMemoryIndexMissingCorruptAndUnsyncedRefuse(t *testing.T) {
	base := newTestBadger(t)
	require.Error(t, base.InitializeEmptyPublicMemoryIndex())
	scoped := base.BeginConsensusTransaction(nil)
	require.NoError(t, scoped.InitializeEmptyPublicMemoryIndex())
	publicTestRecord(t, scoped, "one")
	require.NoError(t, scoped.SyncPublicMemoryIndex("one"))
	require.NoError(t, scoped.CommitConsensusTransaction())
	require.NoError(t, base.SetMemoryStatusPreservingHash("one", "challenged"))
	_, err := base.PublicMemoryBranch("one")
	require.Error(t, err)
	require.NoError(t, base.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(publicNodeKey([32]byte{}, 0))
	}))
	scoped = base.BeginConsensusTransaction(nil)
	require.Error(t, scoped.InitializeEmptyPublicMemoryIndex())
	require.Error(t, scoped.SyncPublicMemoryIndex("one"))
	scoped.DiscardConsensusTransaction()
}

func TestPublicMemoryIndexDoesNotChangeUnwiredLegacyHash(t *testing.T) {
	base := newTestBadger(t)
	before, err := base.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	_, err = base.PublicMemoryBranch("absent")
	require.Error(t, err)
	after, err := base.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.Equal(t, before, after)
	_, err = PublicMemoryCompositeHash(before, []byte("short"))
	require.Error(t, err)
}

func TestPublicMemoryIndexRestartRetainsRootAndMissingRootRefuses(t *testing.T) {
	directory := t.TempDir()
	base, err := NewBadgerStore(directory)
	require.NoError(t, err)
	scoped := base.BeginConsensusTransaction(nil)
	require.NoError(t, scoped.InitializeEmptyPublicMemoryIndex())
	publicTestRecord(t, scoped, "restart")
	require.NoError(t, scoped.SyncPublicMemoryIndex("restart"))
	require.NoError(t, scoped.CommitConsensusTransaction())
	before, err := base.PublicMemoryBranch("restart")
	require.NoError(t, err)
	require.NoError(t, base.CloseBadger())
	base, err = NewBadgerStore(directory)
	require.NoError(t, err)
	defer base.CloseBadger()
	after, err := base.PublicMemoryBranch("restart")
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoError(t, base.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(publicNodeKey([32]byte{}, 0))
	}))
	_, err = base.PublicMemoryBranch("restart")
	require.Error(t, err)
	scoped = base.BeginConsensusTransaction(nil)
	require.Error(t, scoped.InitializeEmptyPublicMemoryIndex())
	scoped.DiscardConsensusTransaction()
}

func TestPublicMemoryIndexMissingBindingAndPrivateRefuse(t *testing.T) {
	base := newTestBadger(t)
	scoped := base.BeginConsensusTransaction(nil)
	require.NoError(t, scoped.InitializeEmptyPublicMemoryIndex())
	publicTestRecord(t, scoped, "private")
	require.NoError(t, scoped.SetMemoryClassification("private", 1))
	require.NoError(t, scoped.SyncPublicMemoryIndex("private"))
	publicTestRecord(t, scoped, "incomplete")
	require.NoError(t, scoped.update(func(txn *badger.Txn) error {
		return scoped.txnDelete(txn, memoryAuthorPrincipalKey("incomplete"))
	}))
	require.Error(t, scoped.SyncPublicMemoryIndex("incomplete"))
	scoped.DiscardConsensusTransaction()
}
