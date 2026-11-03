package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestPublicMemoryIdentityLegacyBytesAndHashes(t *testing.T) {
	// Fixed legacy-format vectors are independent of the widened serializer.
	leaf := PublicMemoryLeaf{
		MemoryID: "one", ContentHash: sha256.Sum256([]byte("synthetic:one")),
		Author: strings.Repeat("a", 64), AuthorPrincipal: "synthetic-principal",
		Domain: "public-test", Status: "committed",
	}
	encoded, err := leaf.canonical()
	require.NoError(t, err)
	require.Equal(t, "736167652e7075626c69632d6d656d6f72792e6c6561662e76310000036f6e65004061616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161001373796e7468657469632d7072696e636970616c000b7075626c69632d746573740009636f6d6d6974746564004b4803bb8a0845173f5b721ab784ed609b18650e5ac7341d0ef75d886427671b", hex.EncodeToString(encoded))
	leafHash := sha256.Sum256(encoded)
	require.Equal(t, "b9d55619ef6faee527d8476e4740944b7ee6a0030fbcfaa3130ee9a3b30f86d7", hex.EncodeToString(leafHash[:]))
	for _, invalid := range []func(*PublicMemoryLeaf){
		func(value *PublicMemoryLeaf) { value.AuthorPrincipal = strings.Repeat("p", 257) },
		func(value *PublicMemoryLeaf) { value.Domain = string([]byte{0xff}) },
		func(value *PublicMemoryLeaf) { value.Status = "unknown" },
	} {
		changed := leaf
		invalid(&changed)
		_, err := changed.canonical()
		require.ErrorIs(t, err, ErrPublicMemoryIndex, "only the identity boundary is widened")
	}
	path := publicPath(leaf.MemoryID)
	require.Equal(t, "4861cece780eb56353da3e77296f3472dc83bb1df2399682267e2dd94f202f6b", hex.EncodeToString(path[:]))
	for _, appV30 := range []bool{false, true} {
		base := newTestBadger(t)
		scoped := base.BeginConsensusTransaction(nil)
		t.Cleanup(scoped.DiscardConsensusTransaction)
		require.NoError(t, scoped.InitializeEmptyPublicMemoryIndex())
		publicTestRecord(t, scoped, "one")
		if appV30 {
			require.NoError(t, scoped.SyncPublicMemoryChangesAppV30())
		} else {
			require.NoError(t, scoped.SyncPublicMemoryChanges())
		}
		require.NoError(t, scoped.CommitConsensusTransaction())
		root, err := base.PublicMemoryRoot()
		require.NoError(t, err)
		require.Equal(t, "7bd74240b8563243c95340e1137b8bc55f4935c6785f30115f6bd54d71b1aca7", hex.EncodeToString(root[:]))
		legacy := make([]byte, 32)
		for i := range legacy {
			legacy[i] = byte(i)
		}
		composite, err := PublicMemoryCompositeHash(legacy, root[:])
		require.NoError(t, err)
		require.Equal(t, "41fb53f0ebf3fd7430ab47e6070c7d94e37a01853c8f3fb51237fc33b5a77da2", hex.EncodeToString(composite))
	}
}

func TestPublicMemoryIdentityWideProofsAndLegacyRefusal(t *testing.T) {
	for _, tc := range []struct {
		name     string
		id       string
		legacyOK bool
	}{
		{"one-byte", "x", true},
		{"ascii-256", strings.Repeat("a", 256), true},
		{"ascii-257", strings.Repeat("b", 257), false},
		{"ascii-512", strings.Repeat("c", 512), false},
		{"utf8-512-bytes", strings.Repeat("é", 256), false},
		{"opaque-with-delimiters", string([]byte{0, ':', 0xff}), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := newTestBadger(t)
			scoped := base.BeginConsensusTransaction(nil)
			t.Cleanup(scoped.DiscardConsensusTransaction)
			require.NoError(t, scoped.InitializeEmptyPublicMemoryIndex())
			publicTestRecord(t, scoped, tc.id)
			require.NoError(t, scoped.SyncPublicMemoryChangesAppV30())
			require.NoError(t, scoped.CommitConsensusTransaction())
			proof, err := base.PublicMemoryBranch(tc.id)
			require.NoError(t, err)
			require.Equal(t, []byte(tc.id), []byte(proof.Leaf.MemoryID))
			require.NoError(t, VerifyPublicMemoryBranch(*proof, proof.Root))
			for _, tamper := range []func(*PublicMemoryBranch){
				func(p *PublicMemoryBranch) { p.Leaf.MemoryID += "!" },
				func(p *PublicMemoryBranch) { p.Leaf.ContentHash[0] ^= 1 },
				func(p *PublicMemoryBranch) { p.Siblings[0][0] ^= 1 },
			} {
				changed := *proof
				tamper(&changed)
				require.ErrorIs(t, VerifyPublicMemoryBranch(changed, proof.Root), ErrPublicMemoryIndex)
			}
			legacy := newTestBadger(t)
			old := legacy.BeginConsensusTransaction(nil)
			t.Cleanup(old.DiscardConsensusTransaction)
			require.NoError(t, old.InitializeEmptyPublicMemoryIndex())
			publicTestRecord(t, old, tc.id)
			if tc.legacyOK {
				require.NoError(t, old.SyncPublicMemoryChanges())
				require.NoError(t, old.CommitConsensusTransaction())
				root, err := legacy.PublicMemoryRoot()
				require.NoError(t, err)
				require.Equal(t, proof.Root, root)
			} else {
				require.ErrorIs(t, old.SyncPublicMemoryChanges(), ErrPublicMemoryIndex)
				require.Error(t, old.CommitConsensusTransaction())
			}
		})
	}
}

func TestPublicMemoryIdentityPrivateSelectionAndLegacyTools(t *testing.T) {
	for _, id := range []string{strings.Repeat("p", 512), string([]byte{0xff})} {
		base := newTestBadger(t)
		scoped := base.BeginConsensusTransaction(nil)
		t.Cleanup(scoped.DiscardConsensusTransaction)
		require.NoError(t, scoped.InitializeEmptyPublicMemoryIndex())
		publicTestRecord(t, scoped, id)
		require.NoError(t, scoped.SetMemoryClassification(id, 1))
		require.NoError(t, scoped.SyncPublicMemoryChangesAppV30())
		require.NoError(t, scoped.CommitConsensusTransaction())
		root, err := base.PublicMemoryRoot()
		require.NoError(t, err)
		require.Equal(t, publicEmpty[0], root)
		_, err = base.PublicMemoryBranch(id)
		require.ErrorIs(t, err, ErrPublicMemoryIndex)
		_, err = publicLeafFromCanonical(base, id)
		require.ErrorIs(t, err, ErrPublicMemoryIndex, "legacy refusal precedes private classification")
		_, err = base.BuildPublicMemoryMigration(context.Background(), newTestBadger(t))
		require.ErrorIs(t, err, ErrPublicMemoryIndex)
		require.ErrorIs(t, base.PreparePublicMemoryStage(context.Background(), 100), ErrPublicMemoryIndex)
		// The legacy census intentionally excludes private records; preserve
		// that existing diagnostic behavior instead of widening the audit.
		audit, err := base.AuditPublicMemoryEnvelopes(10)
		require.NoError(t, err)
		require.Zero(t, audit.Public)
		require.Zero(t, audit.Blocking)

		public := newTestBadger(t)
		publicTestRecord(t, public, id)
		audit, err = public.AuditPublicMemoryEnvelopes(10)
		require.NoError(t, err)
		require.Equal(t, 1, audit.Blocking)
		require.Equal(t, []string{"canonical_encode_failed"}, audit.Defects[0].Reasons)
	}
	// A wide, complete canonical record with no synchronized leaf cannot be
	// invented by the read-only wide getter against an empty legacy root.
	base := newTestBadger(t)
	scoped := base.BeginConsensusTransaction(nil)
	t.Cleanup(scoped.DiscardConsensusTransaction)
	require.NoError(t, scoped.InitializeEmptyPublicMemoryIndex())
	publicTestRecord(t, scoped, strings.Repeat("u", 512))
	require.NoError(t, scoped.CommitConsensusTransaction())
	_, err := base.PublicMemoryBranch(strings.Repeat("u", 512))
	require.ErrorIs(t, err, ErrPublicMemoryIndex)
}

func TestPublicMemoryIdentityInvalidIDsAndEnvelopesStillRefuse(t *testing.T) {
	for _, id := range []string{"", strings.Repeat("x", 513)} {
		base := newTestBadger(t)
		scoped := base.BeginConsensusTransaction(nil)
		t.Cleanup(scoped.DiscardConsensusTransaction)
		require.NoError(t, scoped.InitializeEmptyPublicMemoryIndex())
		publicTestRecord(t, scoped, id)
		require.ErrorIs(t, scoped.SyncPublicMemoryChangesAppV30(), ErrPublicMemoryIndex)
		require.Error(t, scoped.CommitConsensusTransaction())
	}
	for _, defect := range []string{"missing-modern-principal", "malformed-principal", "malformed-hash"} {
		t.Run(defect, func(t *testing.T) {
			base := newTestBadger(t)
			scoped := base.BeginConsensusTransaction(nil)
			t.Cleanup(scoped.DiscardConsensusTransaction)
			require.NoError(t, scoped.InitializeEmptyPublicMemoryIndex())
			id := strings.Repeat("m", 512)
			publicTestRecord(t, scoped, id)
			require.NoError(t, scoped.SetMemorySubmissionHeight(id, 100))
			switch defect {
			case "missing-modern-principal":
				require.NoError(t, scoped.update(func(txn *badger.Txn) error {
					return scoped.txnDelete(txn, memoryAuthorPrincipalKey(id))
				}))
			case "malformed-principal":
				require.NoError(t, scoped.SetMemoryAuthorPrincipal(id, string([]byte{0xff})))
			case "malformed-hash":
				require.NoError(t, scoped.SetMemoryHash(id, []byte{1}, "committed"))
			}
			require.ErrorIs(t, scoped.SyncPublicMemoryChangesAppV30(), ErrPublicMemoryIndex)
			require.Error(t, scoped.CommitConsensusTransaction())
		})
	}
}

// This draft requires the actual PR506 migration source, whose existing metadata
// copy remains intact. It is not an old-base migration acceptance result.
func TestPublicMemoryIdentityAppV30MigrationPreservesRootAndMetadata(t *testing.T) {
	ctx := context.Background()
	source := newTestBadger(t)
	scoped := source.BeginConsensusTransaction(nil)
	t.Cleanup(scoped.DiscardConsensusTransaction)
	require.NoError(t, scoped.InitializeEmptyPublicMemoryIndex())
	ids := []string{"one", strings.Repeat("z", 512), string([]byte{0, 0xff, ':'})}
	for _, id := range ids {
		publicTestRecord(t, scoped, id)
	}
	privateID := strings.Repeat("private", 70)
	publicTestRecord(t, scoped, privateID)
	require.NoError(t, scoped.SetMemoryClassification(privateID, 1))
	signers := []AppV30ProvenanceSigner{{
		CredentialID: strings.Repeat("a", 64), PrincipalID: strings.Repeat("a", 64), Exact: true,
	}}
	require.NoError(t, scoped.SetAppV30Provenance(ids[1], signers))
	adoption := sha256.Sum256([]byte("opaque identity migration adoption"))
	require.NoError(t, scoped.update(func(txn *badger.Txn) error {
		return scoped.txnSet(txn, memoryLegacyAdoptionReceiptKey(ids[2]), adoption[:])
	}))
	require.NoError(t, scoped.SyncPublicMemoryChangesAppV30())
	require.NoError(t, scoped.CommitConsensusTransaction())
	before, err := source.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	root, err := source.PublicMemoryRoot()
	require.NoError(t, err)
	_, err = source.BuildPublicMemoryMigration(ctx, newTestBadger(t))
	require.ErrorIs(t, err, ErrPublicMemoryIndex)
	target := newTestBadger(t)
	result, err := source.BuildPublicMemoryMigrationAppV30(ctx, target)
	require.NoError(t, err)
	require.Equal(t, uint64(4), result.Scanned)
	require.Equal(t, uint64(3), result.Public)
	require.Equal(t, result.Public, result.Migrated)
	require.Equal(t, uint64(1), result.AppV30Provenance)
	require.Equal(t, uint64(1), result.AdoptionReceipts)
	require.Equal(t, root, result.Root)
	for _, id := range ids {
		proof, err := target.PublicMemoryBranch(id)
		require.NoError(t, err)
		require.Equal(t, []byte(id), []byte(proof.Leaf.MemoryID))
		require.NoError(t, VerifyPublicMemoryBranch(*proof, root))
		migrated, err := target.HasAppV30MigrationMarker(id)
		require.NoError(t, err)
		require.True(t, migrated)
		migrated, err = source.HasAppV30MigrationMarker(id)
		require.NoError(t, err)
		require.False(t, migrated)
	}
	actualSigners, found, err := target.GetAppV30Provenance(ids[1])
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, signers, actualSigners)
	require.Equal(t, adoption[:], readTaintValue(t, target, memoryLegacyAdoptionReceiptKey(ids[2])))
	// Only PUBLIC records migrate. With a valid raw identity the missing
	// private envelope must be reported as absent, without a proof.
	privateProof, err := target.PublicMemoryBranch(privateID)
	require.ErrorIs(t, err, ErrMemoryDisclosureNotFound)
	require.Nil(t, privateProof)
	after, err := source.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.Equal(t, before, after)
}
