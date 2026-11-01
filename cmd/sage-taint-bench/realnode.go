package main

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"time"

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

// nodeReceipt is the committed evidence for real-node counts. It records a
// label instead of the copy's path, and no key or memory content.
type nodeReceipt struct {
	Schema       string         `json:"schema"`
	SourceLabel  string         `json:"source_label"`
	SnapshotDate string         `json:"snapshot_date"`
	Command      string         `json:"command"`
	Commit       commitContext  `json:"commit"`
	Counts       realNodeCounts `json:"counts"`
}

func writeReceipt(out, nodeCopy, label, snapshotDate string) error {
	if nodeCopy == "" || label == "" {
		return errors.New("-receipt needs -node-copy and -node-label")
	}
	if _, err := time.Parse(time.DateOnly, snapshotDate); err != nil {
		return errors.New("-snapshot-date must be YYYY-MM-DD")
	}
	counts, err := countRealNode(nodeCopy)
	if err != nil {
		return err
	}
	receipt := nodeReceipt{
		Schema: "sage-taint-bench/node-receipt/v1", SourceLabel: label, SnapshotDate: snapshotDate,
		Command: "sage-taint-bench -receipt <out> -node-copy <redacted> -node-label <label> -snapshot-date <date>",
		Commit:  detectCommit(), Counts: counts,
	}
	encoded, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(out, append(encoded, '\n'), 0o600)
}
