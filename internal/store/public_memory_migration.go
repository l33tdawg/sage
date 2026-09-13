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
	if err := initial.InitializeEmptyPublicMemoryIndex(); err != nil {
		initial.DiscardConsensusTransaction()
		return nil, err
	}
	if err := initial.CommitConsensusTransaction(); err != nil {
		return nil, err
	}
	var batch []*PublicMemoryLeaf
	flush := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		transaction := target.BeginConsensusTransaction(nil)
		defer transaction.DiscardConsensusTransaction()
		for _, leaf := range batch {
			if err := transaction.SetMemoryHash(leaf.MemoryID, leaf.ContentHash[:], leaf.Status); err != nil {
				return err
			}
			if err := transaction.SetMemoryDomain(leaf.MemoryID, leaf.Domain); err != nil {
				return err
			}
			if err := transaction.SetMemoryAuthor(leaf.MemoryID, leaf.Author); err != nil {
				return err
			}
			if err := transaction.SetMemoryAuthorPrincipal(leaf.MemoryID, leaf.AuthorPrincipal); err != nil {
				return err
			}
			if err := transaction.SetMemoryClassification(leaf.MemoryID, 0); err != nil {
				return err
			}
			if err := transaction.SyncPublicMemoryIndex(leaf.MemoryID); err != nil {
				return err
			}
		}
		if err := transaction.CommitConsensusTransaction(); err != nil {
			return err
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
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		identifier := string(iterator.Item().Key()[len(prefix):])
		leaf, err := publicLeafFromCanonical(reader, identifier)
		if err != nil {
			return nil, err
		}
		result.Scanned++
		if leaf != nil {
			result.Public++
			batch = append(batch, leaf)
		}
		if len(batch) == publicMigrationBatch {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}
	if len(batch) != 0 {
		if err := flush(); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
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
