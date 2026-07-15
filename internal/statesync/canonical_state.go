package statesync

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	badger "github.com/dgraph-io/badger/v4"
)

const (
	canonicalStateVersion      byte   = 1
	canonicalRecordTag         byte   = 1
	canonicalEndTag            byte   = 0
	MaxCanonicalKeyBytes              = 64 << 10
	MaxCanonicalValueBytes     uint64 = 64 << 20
	canonicalRecordHeaderBytes        = 1 + 4 + 8
	canonicalFooterBytes              = 1 + 8
)

var canonicalStateMagic = [16]byte{'S', 'A', 'G', 'E', '-', 'C', 'A', 'N', 'O', 'N', '-', 'K', 'V', 0, 0, 0}

// WriteCanonicalState writes exactly the latest visible Badger key/value set in
// lexicographic key order. Physical LSM versions, tombstones and expired values
// never enter the Internet snapshot. TTL and user metadata are rejected because
// silently dropping either would change future application behaviour.
func WriteCanonicalState(ctx context.Context, db *badger.DB, output io.Writer) error {
	if db == nil || output == nil {
		return errors.New("canonical state export requires a database and writer")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := writeCanonicalStateBytes(output, canonicalStateMagic[:]); err != nil {
		return err
	}
	if err := writeCanonicalStateBytes(output, []byte{canonicalStateVersion}); err != nil {
		return err
	}

	var count uint64
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		iterator := txn.NewIterator(opts)
		defer iterator.Close()
		for iterator.Rewind(); iterator.Valid(); iterator.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			item := iterator.Item()
			key := item.KeyCopy(nil)
			if len(key) == 0 || len(key) > MaxCanonicalKeyBytes {
				return fmt.Errorf("canonical state key size %d is outside 1..%d", len(key), MaxCanonicalKeyBytes)
			}
			if item.ExpiresAt() != 0 || item.UserMeta() != 0 {
				return errors.New("canonical state forbids TTL and user metadata")
			}
			valueSize := item.ValueSize()
			if valueSize < 0 || uint64(valueSize) > MaxCanonicalValueBytes {
				return fmt.Errorf("canonical state value size %d exceeds %d", valueSize, MaxCanonicalValueBytes)
			}
			var header [canonicalRecordHeaderBytes]byte
			header[0] = canonicalRecordTag
			binary.BigEndian.PutUint32(header[1:5], uint32(len(key)))   // #nosec G115 -- bounded above
			binary.BigEndian.PutUint64(header[5:13], uint64(valueSize)) // #nosec G115 -- non-negative above
			if err := writeCanonicalStateBytes(output, header[:]); err != nil {
				return err
			}
			if err := writeCanonicalStateBytes(output, key); err != nil {
				return err
			}
			if err := item.Value(func(value []byte) error {
				return writeCanonicalStateBytes(output, value)
			}); err != nil {
				return err
			}
			count++
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("export canonical state: %w", err)
	}
	if count == 0 {
		return errors.New("canonical state export is empty")
	}
	var footer [canonicalFooterBytes]byte
	footer[0] = canonicalEndTag
	binary.BigEndian.PutUint64(footer[1:], count)
	if err := writeCanonicalStateBytes(output, footer[:]); err != nil {
		return err
	}
	return nil
}

// RestoreCanonicalState accepts only the canonical stream emitted above and
// writes it into an empty Badger database. Keys must be strictly increasing;
// duplicates, trailing bytes, truncation and oversized allocations fail closed.
func RestoreCanonicalState(ctx context.Context, input io.Reader, db *badger.DB) error {
	if input == nil || db == nil {
		return errors.New("canonical state restore requires a reader and database")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if empty, err := badgerDatabaseEmpty(db); err != nil {
		return err
	} else if !empty {
		return errors.New("canonical state restore requires an empty database")
	}

	magic := make([]byte, len(canonicalStateMagic))
	if _, err := io.ReadFull(input, magic); err != nil {
		return fmt.Errorf("read canonical state magic: %w", err)
	}
	if !bytes.Equal(magic, canonicalStateMagic[:]) {
		return errors.New("canonical state magic mismatch")
	}
	var version [1]byte
	if _, err := io.ReadFull(input, version[:]); err != nil {
		return fmt.Errorf("read canonical state version: %w", err)
	}
	if version[0] != canonicalStateVersion {
		return fmt.Errorf("unsupported canonical state version %d", version[0])
	}

	batch := db.NewWriteBatch()
	defer batch.Cancel()
	var previous []byte
	var count uint64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var tag [1]byte
		if _, err := io.ReadFull(input, tag[:]); err != nil {
			return fmt.Errorf("read canonical state record tag: %w", err)
		}
		if tag[0] == canonicalEndTag {
			var encodedCount [8]byte
			if _, err := io.ReadFull(input, encodedCount[:]); err != nil {
				return fmt.Errorf("read canonical state record count: %w", err)
			}
			if binary.BigEndian.Uint64(encodedCount[:]) != count || count == 0 {
				return errors.New("canonical state record count mismatch")
			}
			var trailing [1]byte
			n, err := io.ReadFull(input, trailing[:])
			if n != 0 {
				return errors.New("canonical state contains trailing data")
			}
			if err != nil && !errors.Is(err, io.EOF) {
				return fmt.Errorf("read canonical state trailing data: %w", err)
			}
			break
		}
		if tag[0] != canonicalRecordTag {
			return fmt.Errorf("canonical state record tag %d is invalid", tag[0])
		}
		var lengths [12]byte
		if _, err := io.ReadFull(input, lengths[:]); err != nil {
			return fmt.Errorf("read canonical state record lengths: %w", err)
		}
		keyLength := binary.BigEndian.Uint32(lengths[:4])
		valueLength := binary.BigEndian.Uint64(lengths[4:])
		if keyLength == 0 || keyLength > MaxCanonicalKeyBytes || valueLength > MaxCanonicalValueBytes {
			return errors.New("canonical state record length is invalid")
		}
		key := make([]byte, int(keyLength))
		if _, err := io.ReadFull(input, key); err != nil {
			return fmt.Errorf("read canonical state key: %w", err)
		}
		if previous != nil && bytes.Compare(previous, key) >= 0 {
			return errors.New("canonical state keys are duplicated or out of order")
		}
		value := make([]byte, int(valueLength)) // #nosec G115 -- bounded to 64 MiB
		if _, err := io.ReadFull(input, value); err != nil {
			return fmt.Errorf("read canonical state value: %w", err)
		}
		if err := batch.Set(key, value); err != nil {
			return fmt.Errorf("stage canonical state record: %w", err)
		}
		previous = key
		count++
	}
	if err := batch.Flush(); err != nil {
		return fmt.Errorf("flush canonical state: %w", err)
	}
	return db.Sync()
}

func writeCanonicalStateBytes(output io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := output.Write(data)
		if written < 0 || written > len(data) {
			return errors.New("canonical state writer returned an invalid byte count")
		}
		data = data[written:]
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func badgerDatabaseEmpty(db *badger.DB) (bool, error) {
	empty := true
	err := db.View(func(txn *badger.Txn) error {
		iterator := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iterator.Close()
		iterator.Rewind()
		empty = !iterator.Valid()
		return nil
	})
	return empty, err
}
