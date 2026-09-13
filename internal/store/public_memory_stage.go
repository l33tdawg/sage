package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"sort"

	badger "github.com/dgraph-io/badger/v4"
)

var publicStagePrefix = []byte("\xffsage:public-memory:v1:stage:")
var publicStageReady = append(append([]byte(nil), publicStagePrefix...), []byte("ready")...)
var publicStagePromoted = []byte("public-memory:v28:promotion")

func publicStageKey(key []byte) []byte {
	return append(append([]byte(nil), publicStagePrefix...), key...)
}

func (s *BadgerStore) PreparePublicMemoryStage(ctx context.Context, height int64) error {
	if s.txn != nil || height <= 0 {
		return ErrPublicMemoryIndex
	}
	if err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(publicStagePromoted)
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return ErrPublicMemoryIndex
		}
		return nil
	}); err != nil {
		return err
	}
	directory, err := os.MkdirTemp("", "sage-public-index-stage-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(directory)
	target, err := NewBadgerStore(directory)
	if err != nil {
		return err
	}
	defer target.CloseBadger()
	migration, err := s.BuildPublicMemoryMigration(ctx, target)
	if err != nil {
		return err
	}
	if err := s.db.DropPrefix(publicStagePrefix); err != nil {
		return err
	}
	snapshot := target.db.NewTransaction(false)
	defer snapshot.Discard()
	options := badger.DefaultIteratorOptions
	options.PrefetchValues = false
	iterator := snapshot.NewIterator(options)
	defer iterator.Close()
	hash := sha256.New()
	batch := make(map[string][]byte)
	flush := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := s.db.Update(func(txn *badger.Txn) error {
			keys := make([]string, 0, len(batch))
			for key := range batch {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				if err := txn.Set(publicStageKey([]byte(key)), batch[key]); err != nil {
					return err
				}
			}
			return nil
		})
		batch = make(map[string][]byte)
		return err
	}
	prefix := []byte(publicIndexPrefix + "node:")
	for iterator.Seek(prefix); iterator.ValidForPrefix(prefix); iterator.Next() {
		key := iterator.Item().KeyCopy(nil)
		value, err := iterator.Item().ValueCopy(nil)
		if err != nil || len(value) != 32 {
			return ErrPublicMemoryIndex
		}
		hash.Write(key)
		hash.Write(value)
		batch[string(key)] = value
		if len(batch) == 256 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if len(batch) != 0 {
		if err := flush(); err != nil {
			return err
		}
	}
	manifest := binary.BigEndian.AppendUint64(nil, uint64(height))
	manifest = append(manifest, migration.SourceAppHash[:]...)
	manifest = append(manifest, migration.Root[:]...)
	manifest = append(manifest, hash.Sum(nil)...)
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.db.Update(func(txn *badger.Txn) error { return txn.Set(publicStageReady, manifest) }); err != nil {
		return err
	}
	return s.db.Sync()
}

func (s *BadgerStore) PromotePublicMemoryStage(height int64, previousAppHash []byte) (err error) {
	if s.txn == nil || height <= 0 || len(previousAppHash) != 32 {
		return ErrPublicMemoryIndex
	}
	defer func() {
		if err != nil && s.poisoned == nil {
			s.poisoned = ErrPublicMemoryIndex
		}
	}()
	return s.update(func(txn *badger.Txn) error {
		if _, err := txn.Get(publicStagePromoted); !errors.Is(err, badger.ErrKeyNotFound) {
			return ErrPublicMemoryIndex
		}
		item, err := txn.Get(publicStageReady)
		if err != nil {
			return ErrPublicMemoryIndex
		}
		manifest, err := item.ValueCopy(nil)
		if err != nil || len(manifest) != 104 || binary.BigEndian.Uint64(manifest[:8]) != uint64(height) || !bytes.Equal(manifest[8:40], previousAppHash) {
			return ErrPublicMemoryIndex
		}
		if err := s.txnSet(txn, publicStagePromoted, manifest); err != nil {
			return err
		}
		return s.txnSet(txn, publicNodeKey([32]byte{}, 0), bytes.Clone(manifest[40:72]))
	})
}

func (s *BadgerStore) ValidatePublicMemoryStage() error {
	return s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(publicStagePromoted)
		if err != nil {
			return ErrPublicMemoryIndex
		}
		manifest, err := item.ValueCopy(nil)
		if err != nil || len(manifest) != 104 || binary.BigEndian.Uint64(manifest[:8]) == 0 {
			return ErrPublicMemoryIndex
		}
		hash := sha256.New()
		options := badger.DefaultIteratorOptions
		options.PrefetchValues = false
		iterator := txn.NewIterator(options)
		defer iterator.Close()
		prefix := publicStageKey([]byte(publicIndexPrefix + "node:"))
		for iterator.Seek(prefix); iterator.ValidForPrefix(prefix); iterator.Next() {
			key := iterator.Item().Key()[len(publicStagePrefix):]
			if len(key) != len(publicNodeKey([32]byte{}, 0)) {
				return ErrPublicMemoryIndex
			}
			hash.Write(key)
			if err := iterator.Item().Value(func(value []byte) error {
				if len(value) != 32 {
					return ErrPublicMemoryIndex
				}
				hash.Write(value)
				return nil
			}); err != nil {
				return err
			}
		}
		if !bytes.Equal(hash.Sum(nil), manifest[72:104]) {
			return ErrPublicMemoryIndex
		}
		rootItem, err := txn.Get(publicStageKey(publicNodeKey([32]byte{}, 0)))
		if err != nil {
			return ErrPublicMemoryIndex
		}
		return rootItem.Value(func(value []byte) error {
			if !bytes.Equal(value, manifest[40:72]) {
				return ErrPublicMemoryIndex
			}
			_, err := publicHashAt(txn, [32]byte{}, 0)
			return err
		})
	})
}

func (s *BadgerStore) SyncPublicMemoryChanges() error {
	if s.txn == nil {
		return ErrPublicMemoryIndex
	}
	keys := make([]string, 0, len(s.publicMemoryChanged))
	for identifier := range s.publicMemoryChanged {
		keys = append(keys, identifier)
	}
	sort.Strings(keys)
	for _, identifier := range keys {
		if err := s.SyncPublicMemoryIndex(identifier); err != nil {
			return err
		}
	}
	return nil
}

func (s *BadgerStore) trackPublicMemoryChange(key []byte) {
	if s.txn == nil {
		return
	}
	for _, prefix := range []string{"memory:", "memdomain:", "memauthor:", "memauthorprincipal:", "mem_class:", "cocommit:core:", "cocommit:shared:"} {
		if bytes.HasPrefix(key, []byte(prefix)) {
			if s.publicMemoryChanged == nil {
				s.publicMemoryChanged = make(map[string]struct{})
			}
			s.publicMemoryChanged[string(key[len(prefix):])] = struct{}{}
			return
		}
	}
}

func (s *BadgerStore) ComputePublicMemoryAppHash() ([]byte, error) {
	var result []byte
	err := s.view(func(txn *badger.Txn) error {
		root, err := publicHashAt(txn, [32]byte{}, 0)
		if err != nil {
			return err
		}
		hash := sha256.New()
		options := badger.DefaultIteratorOptions
		options.PrefetchValues = false
		iterator := txn.NewIterator(options)
		defer iterator.Close()
	scan:
		for iterator.Rewind(); iterator.Valid(); iterator.Next() {
			key := iterator.Item().Key()
			if isIndexBackfillProgressKey(key) || bytes.HasPrefix(key, []byte(publicIndexPrefix)) {
				continue
			}
			for _, excluded := range appHashBookkeepingKeys {
				if bytes.Equal(key, excluded) {
					continue scan
				}
			}
			hash.Write(key)
			if err := iterator.Item().Value(func(value []byte) error { hash.Write(value); return nil }); err != nil {
				return err
			}
		}
		if err := s.visitPromotedAppV23Stage(txn, func(key, value []byte) error {
			hash.Write(key)
			hash.Write(value)
			return nil
		}); err != nil {
			return err
		}
		result, err = PublicMemoryCompositeHash(hash.Sum(nil), root[:])
		return err
	})
	return result, err
}
