package store

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	badger "github.com/dgraph-io/badger/v4"
)

const publicMigrationBatch = 32

type PublicMemoryMigration struct {
	Root                [32]byte
	SourceAppHash       [32]byte
	SourceReadTimestamp uint64
	Scanned             uint64
	Public              uint64
}

func (s *BadgerStore) BuildPublicMemoryMigration(ctx context.Context, target *BadgerStore) (*PublicMemoryMigration, error) {
	if s == nil || target == nil || s.db == target.db || s.txn != nil || target.txn != nil {
		return nil, ErrPublicMemoryIndex
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := target.db.View(func(txn *badger.Txn) error {
		iterator := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iterator.Close()
		iterator.Rewind()
		if iterator.Valid() {
			return ErrPublicMemoryIndex
		}
		return nil
	}); err != nil {
		return nil, err
	}
	snapshot := s.db.NewTransaction(false)
	defer snapshot.Discard()
	reader := &BadgerStore{db: s.db, txn: snapshot}
	hash, err := reader.ComputeAppHashExcludingBookkeeping()
	if err != nil || len(hash) != sha256.Size {
		return nil, ErrPublicMemoryIndex
	}
	result := &PublicMemoryMigration{SourceReadTimestamp: snapshot.ReadTs()}
	copy(result.SourceAppHash[:], hash)
	initial := target.BeginConsensusTransaction(nil)
	if initErr := initial.InitializeEmptyPublicMemoryIndex(); initErr != nil {
		initial.DiscardConsensusTransaction()
		return nil, initErr
	}
	if commitErr := initial.CommitConsensusTransaction(); commitErr != nil {
		return nil, commitErr
	}
	var batch []*PublicMemoryLeaf
	flush := func() error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		transaction := target.BeginConsensusTransaction(nil)
		defer transaction.DiscardConsensusTransaction()
		for _, leaf := range batch {
			if hashErr := transaction.SetMemoryHash(leaf.MemoryID, leaf.ContentHash[:], leaf.Status); hashErr != nil {
				return hashErr
			}
			if domainErr := transaction.SetMemoryDomain(leaf.MemoryID, leaf.Domain); domainErr != nil {
				return domainErr
			}
			if authorErr := transaction.SetMemoryAuthor(leaf.MemoryID, leaf.Author); authorErr != nil {
				return authorErr
			}
			if principalErr := transaction.SetMemoryAuthorPrincipal(leaf.MemoryID, leaf.AuthorPrincipal); principalErr != nil {
				return principalErr
			}
			if classErr := transaction.SetMemoryClassification(leaf.MemoryID, 0); classErr != nil {
				return classErr
			}
			if syncErr := transaction.SyncPublicMemoryIndex(leaf.MemoryID); syncErr != nil {
				return syncErr
			}
		}
		if flushCommitErr := transaction.CommitConsensusTransaction(); flushCommitErr != nil {
			return flushCommitErr
		}
		batch = nil
		return nil
	}
	options := badger.DefaultIteratorOptions
	options.PrefetchValues = false
	iterator := snapshot.NewIterator(options)
	defer iterator.Close()
	prefix := []byte("memory:")
	for iterator.Seek(prefix); iterator.ValidForPrefix(prefix); iterator.Next() {
		if scanCtxErr := ctx.Err(); scanCtxErr != nil {
			return nil, scanCtxErr
		}
		identifier := string(iterator.Item().Key()[len(prefix):])
		leaf, leafErr := publicLeafFromCanonical(reader, identifier)
		if leafErr != nil {
			return nil, leafErr
		}
		result.Scanned++
		if leaf != nil {
			result.Public++
			batch = append(batch, leaf)
		}
		if len(batch) == publicMigrationBatch {
			if flushErr := flush(); flushErr != nil {
				return nil, flushErr
			}
		}
	}
	if len(batch) != 0 {
		if tailFlushErr := flush(); tailFlushErr != nil {
			return nil, tailFlushErr
		}
	}
	if doneCtxErr := ctx.Err(); doneCtxErr != nil {
		return nil, doneCtxErr
	}
	result.Root, err = target.PublicMemoryRoot()
	if err != nil {
		return nil, err
	}
	manifest := append([]byte("sage.public-memory.migration.v1\x00"), result.SourceAppHash[:]...)
	manifest = append(manifest, result.Root[:]...)
	manifest = binary.BigEndian.AppendUint64(manifest, result.Scanned)
	manifest = binary.BigEndian.AppendUint64(manifest, result.Public)
	if err := target.db.Update(func(txn *badger.Txn) error {
		if _, err := txn.Get([]byte(publicIndexPrefix + "migration-complete")); !errors.Is(err, badger.ErrKeyNotFound) {
			return ErrPublicMemoryIndex
		}
		return txn.Set([]byte(publicIndexPrefix+"migration-complete"), manifest)
	}); err != nil {
		return nil, err
	}
	if err := target.db.Sync(); err != nil {
		return nil, err
	}
	return result, nil
}
