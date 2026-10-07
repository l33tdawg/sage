package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"sort"

	badger "github.com/dgraph-io/badger/v4"
)

// ErrCoCommitTombstoneIndex is returned when the app-v28 co-commit tombstone
// reverse index is unavailable, incomplete, or inconsistent.
var ErrCoCommitTombstoneIndex = errors.New("co-commit tombstone index unavailable or inconsistent")

// The reverse index answers one question consensus state cannot: does any
// memory OTHER than this id carry these exact bytes and a status that is no
// longer proposed? State stores memory:<id> as hash+status, so answering it by
// scanning every memory is not a rule that can run per transaction. The index
// is one key per (content hash, memory id) that has left proposed, which keeps
// the lookup a bounded prefix scan and the maintenance a single write.
var coCommitTombstonePrefix = []byte("cocommit-tombstone:v1:")
var coCommitTombstoneStagePrefix = []byte("\xffsage:cocommit-tombstone:v1:stage:")
var coCommitTombstoneStageReady = append(append([]byte(nil), coCommitTombstoneStagePrefix...), []byte("ready")...)
var coCommitTombstonePromoted = []byte("cocommit-tombstone:v28:promotion")

func coCommitTombstoneEntryKey(contentHash []byte, memoryID string) []byte {
	key := append(append([]byte(nil), coCommitTombstonePrefix...), []byte(hex.EncodeToString(contentHash))...)
	return append(append(key, ':'), []byte(memoryID)...)
}

func coCommitTombstonePrefixForHash(contentHash []byte) []byte {
	return append(append(append([]byte(nil), coCommitTombstonePrefix...), []byte(hex.EncodeToString(contentHash))...), ':')
}

func coCommitTombstoneStageKey(entryKey []byte) []byte {
	return append(append([]byte(nil), coCommitTombstoneStagePrefix...), entryKey...)
}

// coCommitTombstoneSplitEntry returns the memory id of one index key, refusing
// anything that is not exactly <64 hex chars>:<id>.
func coCommitTombstoneSplitEntry(entryKey []byte) (string, bool) {
	if !bytes.HasPrefix(entryKey, coCommitTombstonePrefix) {
		return "", false
	}
	rest := entryKey[len(coCommitTombstonePrefix):]
	if len(rest) <= sha256.Size*2+1 || rest[sha256.Size*2] != ':' {
		return "", false
	}
	if _, err := hex.DecodeString(string(rest[:sha256.Size*2])); err != nil {
		return "", false
	}
	return string(rest[sha256.Size*2+1:]), true
}

// CoCommitTombstoned reports whether some memory OTHER than excludeMemoryID
// carries contentHash and has left the proposed state. A malformed hash or a
// missing index returns an error: the consensus caller must refuse, not guess.
func (s *BadgerStore) CoCommitTombstoned(contentHash []byte, excludeMemoryID string) (bool, error) {
	if len(contentHash) != sha256.Size || excludeMemoryID == "" {
		return false, ErrCoCommitTombstoneIndex
	}
	prefix := coCommitTombstonePrefixForHash(contentHash)
	found := false
	err := s.view(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.Prefix = prefix
		options.PrefetchValues = false
		iterator := txn.NewIterator(options)
		defer iterator.Close()
		for iterator.Seek(prefix); iterator.ValidForPrefix(prefix); iterator.Next() {
			key := iterator.Item().Key()
			if string(key[len(prefix):]) == excludeMemoryID {
				continue
			}
			found = true
			return nil
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return found, nil
}

// maintainCoCommitTombstoneTxn keeps the reverse index in step with one
// memory:<id> write. It is inert until the promotion marker is visible in this
// transaction, so every pre-fork block — and every block executed by a binary
// that never applied the fork — writes nothing, and replay stays byte-identical.
func (s *BadgerStore) maintainCoCommitTombstoneTxn(txn *badger.Txn, memoryKey, value []byte) error {
	if _, err := txn.Get(coCommitTombstonePromoted); err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		return err
	}
	memoryID := string(memoryKey[len("memory:"):])
	if memoryID == "" {
		return ErrCoCommitTombstoneIndex
	}
	contentHash, status, err := decodeMemoryHashEntry(value)
	if err != nil {
		return ErrCoCommitTombstoneIndex
	}
	// A legacy zero-length hash carries no bytes to match on; it can neither be
	// tombstoned nor tombstone anything.
	if len(contentHash) == 0 {
		return nil
	}
	if len(contentHash) != sha256.Size {
		return ErrCoCommitTombstoneIndex
	}
	entryKey := coCommitTombstoneEntryKey(contentHash, memoryID)
	if status == "proposed" {
		if err := txn.Delete(entryKey); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		return nil
	}
	return s.txnSetPrimitive(txn, entryKey, []byte(status))
}

// retractCoCommitTombstoneTxn removes the index entries a memory:<id> delete
// invalidates. The value is read before the delete so the hash is still known.
func (s *BadgerStore) retractCoCommitTombstoneTxn(txn *badger.Txn, memoryKey []byte) error {
	if _, err := txn.Get(coCommitTombstonePromoted); err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		return err
	}
	item, err := txn.Get(memoryKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var value []byte
	if err := item.Value(func(raw []byte) error {
		value = append([]byte(nil), raw...)
		return nil
	}); err != nil {
		return err
	}
	memoryID := string(memoryKey[len("memory:"):])
	contentHash, _, decodeErr := decodeMemoryHashEntry(value)
	if decodeErr != nil || len(contentHash) != sha256.Size || memoryID == "" {
		return nil
	}
	if err := txn.Delete(coCommitTombstoneEntryKey(contentHash, memoryID)); err != nil &&
		!errors.Is(err, badger.ErrKeyNotFound) {
		return err
	}
	return nil
}

// SyncCoCommitTombstoneChanges folds the identifiers this transaction already
// tracked into the reverse index. The inline maintenance covers every block
// after the activation; this exists for the activation block itself, whose
// promotion marker is written AFTER that block's transactions ran, so those
// writes would otherwise never be indexed.
func (s *BadgerStore) SyncCoCommitTombstoneChanges() error {
	if s.txn == nil {
		return ErrCoCommitTombstoneIndex
	}
	identifiers := make([]string, 0, len(s.publicMemoryChanged))
	for identifier := range s.publicMemoryChanged {
		identifiers = append(identifiers, identifier)
	}
	sort.Strings(identifiers)
	for _, identifier := range identifiers {
		item, err := s.txn.Get(memoryKey(identifier))
		if errors.Is(err, badger.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		var value []byte
		if valueErr := item.Value(func(raw []byte) error {
			value = append([]byte(nil), raw...)
			return nil
		}); valueErr != nil {
			return valueErr
		}
		if err := s.maintainCoCommitTombstoneTxn(s.txn, memoryKey(identifier), value); err != nil {
			return err
		}
	}
	return nil
}

// PrepareCoCommitTombstoneStage builds the durable, AppHash-excluded staged
// backfill for the reverse index. It scans memory records OUTSIDE any consensus
// transaction, so the activation block only has to copy the staged entries —
// the same reason the public-memory index is staged.
func (s *BadgerStore) PrepareCoCommitTombstoneStage(ctx context.Context, height int64) error {
	if s.txn != nil || height <= 0 {
		return ErrCoCommitTombstoneIndex
	}
	if err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(coCommitTombstonePromoted)
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return ErrCoCommitTombstoneIndex
		}
		return nil
	}); err != nil {
		return err
	}
	sourceAppHash, err := s.ComputeAppHashExcludingBookkeeping()
	if err != nil || len(sourceAppHash) != sha256.Size {
		return ErrCoCommitTombstoneIndex
	}
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
				if err := txn.Set(coCommitTombstoneStageKey([]byte(key)), batch[key]); err != nil {
					return err
				}
			}
			return nil
		})
		batch = make(map[string][]byte)
		return err
	}
	scanErr := s.db.View(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.Prefix = []byte("memory:")
		options.PrefetchValues = true
		iterator := txn.NewIterator(options)
		defer iterator.Close()
		for iterator.Seek(options.Prefix); iterator.ValidForPrefix(options.Prefix); iterator.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := iterator.Item().KeyCopy(nil)
			value, valueErr := iterator.Item().ValueCopy(nil)
			if valueErr != nil {
				return valueErr
			}
			contentHash, status, decodeErr := decodeMemoryHashEntry(value)
			if decodeErr != nil || len(contentHash) != sha256.Size || status == "proposed" {
				continue
			}
			memoryID := string(key[len("memory:"):])
			if memoryID == "" {
				continue
			}
			entryKey := coCommitTombstoneEntryKey(contentHash, memoryID)
			batch[string(entryKey)] = []byte(status)
			if len(batch) == 256 {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if scanErr != nil {
		return scanErr
	}
	if err := s.db.DropPrefix(coCommitTombstoneStagePrefix); err != nil {
		return err
	}
	if len(batch) != 0 {
		if err := flush(); err != nil {
			return err
		}
	}
	// Digest the STAGED keys in their own lexicographic order, not the order the
	// memory scan produced them: promotion and validation both re-read the stage
	// prefix, so the three must agree on one canonical order.
	hash := sha256.New()
	var entries uint64
	options := badger.DefaultIteratorOptions
	options.Prefix = coCommitTombstoneStagePrefix
	options.PrefetchValues = true
	if err := s.db.View(func(txn *badger.Txn) error {
		iterator := txn.NewIterator(options)
		defer iterator.Close()
		for iterator.Seek(options.Prefix); iterator.ValidForPrefix(options.Prefix); iterator.Next() {
			key := iterator.Item().Key()
			if bytes.Equal(key, coCommitTombstoneStageReady) {
				continue
			}
			value, valueErr := iterator.Item().ValueCopy(nil)
			if valueErr != nil {
				return valueErr
			}
			hash.Write(key[len(coCommitTombstoneStagePrefix):])
			hash.Write(value)
			entries++
		}
		return nil
	}); err != nil {
		return err
	}
	manifest := binary.BigEndian.AppendUint64(nil, uint64(height))
	manifest = append(manifest, sourceAppHash...)
	manifest = binary.BigEndian.AppendUint64(manifest, entries)
	manifest = append(manifest, hash.Sum(nil)...)
	if err := ctx.Err(); err != nil {
		return err
	}
	// The staged entries are written FIRST, then the manifest, so a manifest can
	// never describe a partial stage.
	if err := s.db.Update(func(txn *badger.Txn) error { return txn.Set(coCommitTombstoneStageReady, manifest) }); err != nil {
		return err
	}
	return s.db.Sync()
}

// PromoteCoCommitTombstoneStage publishes the staged backfill inside the
// activation block's consensus transaction and writes the promotion marker
// that turns the maintenance on. Both are bound to the committed H-1 AppHash
// exactly like the public-memory stage, so a stage built over any other state
// cannot be published over this one.
func (s *BadgerStore) PromoteCoCommitTombstoneStage(height int64, previousAppHash []byte) (err error) {
	if s.txn == nil || height <= 0 || len(previousAppHash) != sha256.Size {
		return ErrCoCommitTombstoneIndex
	}
	defer func() {
		if err != nil && s.poisoned == nil {
			s.poisoned = ErrCoCommitTombstoneIndex
		}
	}()
	return s.update(func(txn *badger.Txn) error {
		if _, err := txn.Get(coCommitTombstonePromoted); !errors.Is(err, badger.ErrKeyNotFound) {
			return ErrCoCommitTombstoneIndex
		}
		item, err := txn.Get(coCommitTombstoneStageReady)
		if err != nil {
			return ErrCoCommitTombstoneIndex
		}
		manifest, err := item.ValueCopy(nil)
		if err != nil || len(manifest) != 80 ||
			binary.BigEndian.Uint64(manifest[:8]) != uint64(height) ||
			!bytes.Equal(manifest[8:40], previousAppHash) {
			return ErrCoCommitTombstoneIndex
		}
		wantEntries := binary.BigEndian.Uint64(manifest[40:48])
		hash := sha256.New()
		var copied uint64
		options := badger.DefaultIteratorOptions
		options.Prefix = coCommitTombstoneStagePrefix
		options.PrefetchValues = true
		iterator := txn.NewIterator(options)
		defer iterator.Close()
		for iterator.Seek(options.Prefix); iterator.ValidForPrefix(options.Prefix); iterator.Next() {
			key := iterator.Item().Key()
			if bytes.Equal(key, coCommitTombstoneStageReady) {
				continue
			}
			entryKey := append([]byte(nil), key[len(coCommitTombstoneStagePrefix):]...)
			value, valueErr := iterator.Item().ValueCopy(nil)
			if valueErr != nil {
				return valueErr
			}
			hash.Write(entryKey)
			hash.Write(value)
			copied++
			if err := s.txnSetPrimitive(txn, entryKey, value); err != nil {
				return err
			}
		}
		if copied != wantEntries || !bytes.Equal(hash.Sum(nil), manifest[48:80]) {
			return ErrCoCommitTombstoneIndex
		}
		return s.txnSetPrimitive(txn, coCommitTombstonePromoted, manifest)
	})
}

// ValidateCoCommitTombstoneStage refuses a boot whose promoted backfill is
// missing or does not match the staged copy it was built from.
func (s *BadgerStore) ValidateCoCommitTombstoneStage() error {
	return s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(coCommitTombstonePromoted)
		if err != nil {
			return ErrCoCommitTombstoneIndex
		}
		manifest, err := item.ValueCopy(nil)
		if err != nil || len(manifest) != 80 || binary.BigEndian.Uint64(manifest[:8]) == 0 {
			return ErrCoCommitTombstoneIndex
		}
		wantEntries := binary.BigEndian.Uint64(manifest[40:48])
		hash := sha256.New()
		var staged uint64
		options := badger.DefaultIteratorOptions
		options.Prefix = coCommitTombstoneStagePrefix
		options.PrefetchValues = true
		iterator := txn.NewIterator(options)
		defer iterator.Close()
		for iterator.Seek(options.Prefix); iterator.ValidForPrefix(options.Prefix); iterator.Next() {
			key := iterator.Item().Key()
			if bytes.Equal(key, coCommitTombstoneStageReady) {
				continue
			}
			value, valueErr := iterator.Item().ValueCopy(nil)
			if valueErr != nil {
				return valueErr
			}
			hash.Write(key[len(coCommitTombstoneStagePrefix):])
			hash.Write(value)
			staged++
		}
		if staged != wantEntries || !bytes.Equal(hash.Sum(nil), manifest[48:80]) {
			return ErrCoCommitTombstoneIndex
		}
		// Every live entry must still be a well-formed hash:id key; a corrupt one
		// would silently stop matching the bytes it exists to tombstone.
		live := badger.DefaultIteratorOptions
		live.Prefix = coCommitTombstonePrefix
		live.PrefetchValues = false
		liveIterator := txn.NewIterator(live)
		defer liveIterator.Close()
		for liveIterator.Seek(live.Prefix); liveIterator.ValidForPrefix(live.Prefix); liveIterator.Next() {
			if _, ok := coCommitTombstoneSplitEntry(liveIterator.Item().Key()); !ok {
				return ErrCoCommitTombstoneIndex
			}
		}
		return nil
	})
}
