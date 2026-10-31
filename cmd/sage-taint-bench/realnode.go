package main

import (
	"strings"

	badger "github.com/dgraph-io/badger/v4"
)

const migratedUnmeasurable = "no marker distinguishes a migrated record until app-v30 adds one, so this count cannot be taken from today's state"

type realNodeCounts struct {
	Measured       bool   `json:"measured"`
	Reason         string `json:"reason,omitempty"`
	Records        *int64 `json:"memory_records"`
	OverCapIDs     *int64 `json:"over_cap_ids"`
	AdoptedRecords *int64 `json:"adopted_records"`
	Migrated       *int64 `json:"migrated_records"`
	MigratedReason string `json:"migrated_reason"`
}

func notMeasured() realNodeCounts {
	return realNodeCounts{
		Reason:         "no node copy supplied; pass -node-copy with a copy of a stopped node's Badger directory",
		MigratedReason: migratedUnmeasurable,
	}
}

// countRealNode opens a supplied Badger copy read-only and counts keys. It must
// be given a copy, never the directory of a running node.
func countRealNode(path string) (realNodeCounts, error) {
	counts := realNodeCounts{Measured: true, MigratedReason: migratedUnmeasurable}
	db, err := badger.Open(badger.DefaultOptions(path).WithReadOnly(true).WithLogger(nil))
	if err != nil {
		return counts, err
	}
	defer func() { _ = db.Close() }()
	var records, overCap, adopted int64
	err = db.View(func(txn *badger.Txn) error {
		if scanErr := eachKey(txn, []byte("memory:"), func(key []byte) {
			records++
			if len(strings.TrimPrefix(string(key), "memory:")) > memoryIDCap {
				overCap++
			}
		}); scanErr != nil {
			return scanErr
		}
		return eachKey(txn, []byte(adoptionReceiptPrefix), func([]byte) { adopted++ })
	})
	if err != nil {
		return counts, err
	}
	counts.Records, counts.OverCapIDs, counts.AdoptedRecords = &records, &overCap, &adopted
	return counts, nil
}

func eachKey(txn *badger.Txn, prefix []byte, visit func([]byte)) error {
	options := badger.DefaultIteratorOptions
	options.Prefix = prefix
	options.PrefetchValues = false
	it := txn.NewIterator(options)
	defer it.Close()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		visit(it.Item().Key())
	}
	return nil
}
