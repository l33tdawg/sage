package statesync

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/consensuskeys"
)

type canonicalStateTestRecord struct {
	key   []byte
	value []byte
}

type canonicalStateTestVersion struct {
	value   []byte
	deleted bool
}

func openCanonicalStateTestDB(t *testing.T) *badger.DB {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func canonicalStateTestStream(records ...canonicalStateTestRecord) []byte {
	var stream bytes.Buffer
	_, _ = stream.Write(canonicalStateMagic[:])
	_ = stream.WriteByte(canonicalStateVersion)
	for _, record := range records {
		var header [canonicalRecordHeaderBytes]byte
		header[0] = canonicalRecordTag
		binary.BigEndian.PutUint32(header[1:5], uint32(len(record.key))) // #nosec G115 -- test inputs are tiny
		binary.BigEndian.PutUint64(header[5:], uint64(len(record.value)))
		_, _ = stream.Write(header[:])
		_, _ = stream.Write(record.key)
		_, _ = stream.Write(record.value)
	}
	var footer [canonicalFooterBytes]byte
	footer[0] = canonicalEndTag
	binary.BigEndian.PutUint64(footer[1:], uint64(len(records)))
	_, _ = stream.Write(footer[:])
	return stream.Bytes()
}

func canonicalStateTestVersions(t *testing.T, db *badger.DB, key []byte) []canonicalStateTestVersion {
	t.Helper()
	versions := make([]canonicalStateTestVersion, 0)
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.AllVersions = true
		options.Prefix = key
		iterator := txn.NewIterator(options)
		defer iterator.Close()
		for iterator.Seek(key); iterator.ValidForPrefix(key); iterator.Next() {
			item := iterator.Item()
			if !bytes.Equal(item.Key(), key) {
				continue
			}
			version := canonicalStateTestVersion{deleted: item.IsDeletedOrExpired()}
			if !version.deleted {
				if err := item.Value(func(value []byte) error {
					version.value = append([]byte(nil), value...)
					return nil
				}); err != nil {
					return err
				}
			}
			versions = append(versions, version)
		}
		return nil
	}))
	return versions
}

func TestCanonicalStateOmitsHiddenVersionsAndTombstones(t *testing.T) {
	source := openCanonicalStateTestDB(t)
	require.NoError(t, source.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("retained"), []byte("hidden-old-version"))
	}))
	require.NoError(t, source.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("retained"), []byte("latest-visible-version"))
	}))
	require.NoError(t, source.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("removed"), []byte("deleted-value"))
	}))
	require.NoError(t, source.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte("removed"))
	}))
	require.GreaterOrEqual(t, len(canonicalStateTestVersions(t, source, []byte("retained"))), 2,
		"the source fixture must contain a hidden historical value")
	sourceRemovedVersions := canonicalStateTestVersions(t, source, []byte("removed"))
	require.GreaterOrEqual(t, len(sourceRemovedVersions), 2, "the source fixture must contain a value and tombstone")
	require.True(t, sourceRemovedVersions[0].deleted, "the latest source version must be a tombstone")

	var encoded bytes.Buffer
	require.NoError(t, WriteCanonicalState(context.Background(), source, &encoded))
	assert.NotContains(t, encoded.String(), "hidden-old-version")
	assert.NotContains(t, encoded.String(), "deleted-value")

	restored := openCanonicalStateTestDB(t)
	require.NoError(t, RestoreCanonicalState(context.Background(), bytes.NewReader(encoded.Bytes()), restored))
	restoredRetainedVersions := canonicalStateTestVersions(t, restored, []byte("retained"))
	require.Equal(t, []canonicalStateTestVersion{{value: []byte("latest-visible-version")}}, restoredRetainedVersions)
	assert.Empty(t, canonicalStateTestVersions(t, restored, []byte("removed")))
}

func TestCanonicalStateExcludesAndRejectsLocalMigrationProgress(t *testing.T) {
	source := openCanonicalStateTestDB(t)
	require.NoError(t, source.Update(func(txn *badger.Txn) error {
		if err := txn.Set([]byte("memory:retained"), []byte("consensus-value")); err != nil {
			return err
		}
		if err := txn.Set([]byte(consensuskeys.AgentOrgsIndexBackfillProgress), []byte{1}); err != nil {
			return err
		}
		return txn.Set([]byte(consensuskeys.OrgNameIndexBackfillProgress), append([]byte{0}, []byte("org:legacy")...))
	}))

	var encoded bytes.Buffer
	require.NoError(t, WriteCanonicalState(context.Background(), source, &encoded))
	assert.NotContains(t, encoded.String(), consensuskeys.AgentOrgsIndexBackfillProgress)
	assert.NotContains(t, encoded.String(), consensuskeys.OrgNameIndexBackfillProgress)
	restored := openCanonicalStateTestDB(t)
	require.NoError(t, RestoreCanonicalState(context.Background(), bytes.NewReader(encoded.Bytes()), restored))
	require.NoError(t, restored.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte("memory:retained"))
		return err
	}))

	for _, key := range []string{
		consensuskeys.AgentOrgsIndexBackfillProgress,
		consensuskeys.OrgNameIndexBackfillProgress,
	} {
		t.Run(key, func(t *testing.T) {
			target := openCanonicalStateTestDB(t)
			stream := canonicalStateTestStream(canonicalStateTestRecord{key: []byte(key), value: []byte{1}})
			err := RestoreCanonicalState(context.Background(), bytes.NewReader(stream), target)
			require.ErrorContains(t, err, "excluded local bookkeeping")
			empty, emptyErr := badgerDatabaseEmpty(target)
			require.NoError(t, emptyErr)
			assert.True(t, empty)
		})
	}
}

func TestCanonicalStateRejectsTTLAndUserMetadata(t *testing.T) {
	tests := []struct {
		name  string
		entry *badger.Entry
	}{
		{name: "ttl", entry: badger.NewEntry([]byte("ttl"), []byte("value")).WithTTL(time.Hour)},
		{name: "user metadata", entry: badger.NewEntry([]byte("metadata"), []byte("value")).WithMeta(0x42)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := openCanonicalStateTestDB(t)
			require.NoError(t, db.Update(func(txn *badger.Txn) error {
				return txn.SetEntry(test.entry)
			}))
			var encoded bytes.Buffer
			err := WriteCanonicalState(context.Background(), db, &encoded)
			require.ErrorContains(t, err, "forbids TTL and user metadata")
		})
	}
}

func TestRestoreCanonicalStateRejectsDuplicateOutOfOrderAndTrailingRecords(t *testing.T) {
	tests := []struct {
		name    string
		stream  []byte
		message string
	}{
		{
			name: "duplicate key",
			stream: canonicalStateTestStream(
				canonicalStateTestRecord{key: []byte("a"), value: []byte("first")},
				canonicalStateTestRecord{key: []byte("a"), value: []byte("second")},
			),
			message: "duplicated or out of order",
		},
		{
			name: "out of order key",
			stream: canonicalStateTestStream(
				canonicalStateTestRecord{key: []byte("b"), value: []byte("first")},
				canonicalStateTestRecord{key: []byte("a"), value: []byte("second")},
			),
			message: "duplicated or out of order",
		},
		{
			name: "trailing byte",
			stream: append(canonicalStateTestStream(
				canonicalStateTestRecord{key: []byte("a"), value: []byte("value")},
			), 0xff),
			message: "trailing data",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := openCanonicalStateTestDB(t)
			err := RestoreCanonicalState(context.Background(), bytes.NewReader(test.stream), db)
			require.ErrorContains(t, err, test.message)
			empty, emptyErr := badgerDatabaseEmpty(db)
			require.NoError(t, emptyErr)
			assert.True(t, empty, "a rejected canonical stream must not publish staged records")
		})
	}
}
