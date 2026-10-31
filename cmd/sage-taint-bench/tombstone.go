package main

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	badger "github.com/dgraph-io/badger/v4"
)

const (
	// badgerEntryOverhead is what a Badger transaction accounts per entry on
	// top of key and value; measureTombstone checks it against Badger itself.
	badgerEntryOverhead = 12
	// badgerVersionSuffix is the commit timestamp Badger appends to each key.
	badgerVersionSuffix = 8
	sstSampleDeletes    = 20000
)

type tombstoneSample struct {
	KeyBytes      int   `json:"key_bytes"`
	DeletesFit    int   `json:"deletes_fit_in_one_txn"`
	OverheadBytes int64 `json:"txn_accounted_overhead_bytes"`
}

type tombstoneDisk struct {
	KeyBytes       int     `json:"key_bytes"`
	SSTBytes       float64 `json:"sst_bytes_per_delete"`
	OverheadOnDisk float64 `json:"sst_overhead_bytes"`
}

type tombstoneReport struct {
	Samples  []tombstoneSample `json:"samples"`
	Constant int64             `json:"proposed_constant_bytes"`
	Disk     []tombstoneDisk   `json:"disk_cross_check"`
	Note     string            `json:"note"`
}

// measureTombstone reads the accounted size of a delete from the point where
// Badger itself refuses a transaction, and cross-checks it against SST bytes.
func measureTombstone(dir string) (tombstoneReport, error) {
	report := tombstoneReport{Note: "proposed constant = txn accounted overhead + 8-byte version suffix. Disk figures are snappy-compressed and include SST index and bloom data, so they are a cross-check and stay out of the digest."}
	maxSize := maximumSizes()
	for i, keyBytes := range []int{128, 256, int(maxSize.memberKey)} {
		path := filepath.Join(dir, fmt.Sprintf("tombstone-%d", i))
		sample, disk, err := measureKeySize(path, keyBytes)
		if err != nil {
			return report, err
		}
		report.Samples = append(report.Samples, sample)
		report.Disk = append(report.Disk, disk)
		report.Constant = max(report.Constant, sample.OverheadBytes+badgerVersionSuffix)
	}
	return report, nil
}

func measureKeySize(path string, keyBytes int) (tombstoneSample, tombstoneDisk, error) {
	sample := tombstoneSample{KeyBytes: keyBytes}
	disk := tombstoneDisk{KeyBytes: keyBytes}
	options := badger.DefaultOptions(path).WithLogger(nil)
	db, err := badger.Open(options)
	if err != nil {
		return sample, disk, err
	}
	txn := db.NewTransaction(true)
	for i := 0; ; i++ {
		err = txn.Delete(tombstoneKey(keyBytes, i))
		if errors.Is(err, badger.ErrTxnTooBig) {
			break
		}
		if err != nil {
			txn.Discard()
			_ = db.Close()
			return sample, disk, err
		}
		sample.DeletesFit++
	}
	txn.Discard()
	// Badger allows a transaction 15% of the memtable.
	maxBatchSize := 15 * options.MemTableSize / 100
	sample.OverheadBytes = maxBatchSize/int64(sample.DeletesFit) - int64(keyBytes)

	batch := db.NewWriteBatch()
	defer batch.Cancel()
	for i := range sstSampleDeletes {
		if err = batch.Delete(tombstoneKey(keyBytes, i)); err != nil {
			_ = db.Close()
			return sample, disk, err
		}
	}
	if err = batch.Flush(); err != nil {
		_ = db.Close()
		return sample, disk, err
	}
	if err = db.Close(); err != nil {
		return sample, disk, err
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return sample, disk, err
	}
	total := int64(0)
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".sst") {
			info, err := entry.Info()
			if err != nil {
				return sample, disk, err
			}
			total += info.Size()
		}
	}
	disk.SSTBytes = float64(total) / sstSampleDeletes
	disk.OverheadOnDisk = disk.SSTBytes - float64(keyBytes)
	return sample, disk, os.RemoveAll(path)
}

// tombstoneKey is incompressible, so snappy cannot hide the key bytes.
func tombstoneKey(length, n int) []byte {
	key := make([]byte, 0, length+sha256.Size)
	block := sha256.Sum256(fmt.Appendf(nil, "tombstone/%d", n))
	for len(key) < length {
		key = append(key, block[:]...)
		block = sha256.Sum256(block[:])
	}
	return key[:length]
}
