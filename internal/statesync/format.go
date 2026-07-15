// Package statesync defines SAGE's network-safe consensus snapshot substrate.
//
// This format contains only a chunked Badger backup plus canonical public
// metadata. It is intentionally unrelated to internal/snapshot, whose local
// rollback bundle can contain SQLite, CometBFT databases, validator/node keys,
// vault material, configuration, and binaries and must never cross the network.
package statesync

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

const (
	// Format is the CometBFT Snapshot.Format value for the first SAGE
	// consensus-only snapshot format.
	Format uint32 = 1

	metadataVersion byte = 1
	headerSize           = 12 + 1 + 8 + sha256.Size + 8 + 4 + 4 + sha256.Size

	MinChunkSize     uint32 = 64 << 10
	MaxChunkSize     uint32 = 8 << 20 // safely below CometBFT's 16 MB chunk channel
	MaxChunks        uint32 = 512
	MaxMetadata             = headerSize + int(MaxChunks)*sha256.Size
	MaxSnapshotBytes        = uint64(MaxChunkSize) * uint64(MaxChunks)
)

var metadataMagic = [12]byte{'S', 'A', 'G', 'E', '-', 'S', 'T', 'S', 'Y', 'N', 'C', 0}

// Metadata binds the trusted application height/hash to the exact Badger
// backup byte stream and each independently verifiable transport chunk.
type Metadata struct {
	Height      uint64
	AppHash     []byte
	BackupSize  uint64
	ChunkSize   uint32
	BackupHash  []byte
	ChunkHashes [][]byte
}

// BuildMetadata derives canonical metadata from an already chunked Badger
// backup. Exporters should stream the live Badger backup to disk and pass its
// bounded chunks here; this helper never accepts local rollback-bundle files.
// chunkSize is the declared transport size, so a one-chunk backup may be
// smaller than it while still using the standard minimum chunk size.
func BuildMetadata(height uint64, appHash []byte, chunkSize uint32, chunks [][]byte) (Metadata, []byte, []byte, error) {
	if height == 0 {
		return Metadata{}, nil, nil, errors.New("state sync height must be positive")
	}
	if len(appHash) != sha256.Size {
		return Metadata{}, nil, nil, fmt.Errorf("state sync app hash must be %d bytes", sha256.Size)
	}
	if len(chunks) == 0 || len(chunks) > int(MaxChunks) {
		return Metadata{}, nil, nil, fmt.Errorf("state sync chunk count must be 1..%d", MaxChunks)
	}
	if chunkSize < MinChunkSize || chunkSize > MaxChunkSize {
		return Metadata{}, nil, nil, fmt.Errorf("state sync chunk size must be %d..%d bytes", MinChunkSize, MaxChunkSize)
	}
	whole := sha256.New()
	chunkHashes := make([][]byte, len(chunks))
	var total uint64
	for i, chunk := range chunks {
		if len(chunk) == 0 || len(chunk) > int(chunkSize) || (i < len(chunks)-1 && len(chunk) != int(chunkSize)) {
			return Metadata{}, nil, nil, fmt.Errorf("state sync chunk %d has non-canonical size %d", i, len(chunk))
		}
		if total > math.MaxUint64-uint64(len(chunk)) {
			return Metadata{}, nil, nil, errors.New("state sync backup size overflow")
		}
		total += uint64(len(chunk))
		_, _ = whole.Write(chunk)
		hash := sha256.Sum256(chunk)
		chunkHashes[i] = append([]byte(nil), hash[:]...)
	}
	backupHash := whole.Sum(nil)
	return buildMetadataFromDigests(height, appHash, total, chunkSize, backupHash, chunkHashes)
}

func buildMetadataFromDigests(height uint64, appHash []byte, backupSize uint64, chunkSize uint32, backupHash []byte, chunkHashes [][]byte) (Metadata, []byte, []byte, error) {
	metadata := Metadata{
		Height: height, AppHash: append([]byte(nil), appHash...), BackupSize: backupSize,
		ChunkSize: chunkSize, BackupHash: append([]byte(nil), backupHash...), ChunkHashes: make([][]byte, len(chunkHashes)),
	}
	for i, hash := range chunkHashes {
		metadata.ChunkHashes[i] = append([]byte(nil), hash...)
	}
	encoded, err := EncodeMetadata(metadata)
	if err != nil {
		return Metadata{}, nil, nil, err
	}
	snapshotHash := sha256.Sum256(encoded)
	return metadata, encoded, append([]byte(nil), snapshotHash[:]...), nil
}

// EncodeMetadata emits the one canonical wire encoding accepted by v1.
func EncodeMetadata(metadata Metadata) ([]byte, error) {
	if err := validateMetadata(metadata); err != nil {
		return nil, err
	}
	encoded := make([]byte, headerSize+len(metadata.ChunkHashes)*sha256.Size)
	offset := 0
	copy(encoded[offset:], metadataMagic[:])
	offset += len(metadataMagic)
	encoded[offset] = metadataVersion
	offset++
	binary.BigEndian.PutUint64(encoded[offset:], metadata.Height)
	offset += 8
	copy(encoded[offset:], metadata.AppHash)
	offset += sha256.Size
	binary.BigEndian.PutUint64(encoded[offset:], metadata.BackupSize)
	offset += 8
	binary.BigEndian.PutUint32(encoded[offset:], metadata.ChunkSize)
	offset += 4
	binary.BigEndian.PutUint32(encoded[offset:], uint32(len(metadata.ChunkHashes)))
	offset += 4
	copy(encoded[offset:], metadata.BackupHash)
	offset += sha256.Size
	for _, hash := range metadata.ChunkHashes {
		copy(encoded[offset:], hash)
		offset += sha256.Size
	}
	return encoded, nil
}

// DecodeMetadata rejects unknown, oversized, non-canonical, and trailing-byte
// encodings before allocating a chunk-hash table.
func DecodeMetadata(encoded []byte) (Metadata, error) {
	if len(encoded) < headerSize || len(encoded) > MaxMetadata {
		return Metadata{}, fmt.Errorf("state sync metadata size %d is outside %d..%d", len(encoded), headerSize, MaxMetadata)
	}
	offset := 0
	if !bytes.Equal(encoded[offset:offset+len(metadataMagic)], metadataMagic[:]) {
		return Metadata{}, errors.New("state sync metadata magic mismatch")
	}
	offset += len(metadataMagic)
	if encoded[offset] != metadataVersion {
		return Metadata{}, fmt.Errorf("unsupported state sync metadata version %d", encoded[offset])
	}
	offset++
	height := binary.BigEndian.Uint64(encoded[offset:])
	offset += 8
	appHash := append([]byte(nil), encoded[offset:offset+sha256.Size]...)
	offset += sha256.Size
	backupSize := binary.BigEndian.Uint64(encoded[offset:])
	offset += 8
	chunkSize := binary.BigEndian.Uint32(encoded[offset:])
	offset += 4
	chunkCount := binary.BigEndian.Uint32(encoded[offset:])
	offset += 4
	backupHash := append([]byte(nil), encoded[offset:offset+sha256.Size]...)
	offset += sha256.Size
	if chunkCount == 0 || chunkCount > MaxChunks {
		return Metadata{}, fmt.Errorf("state sync chunk count %d is outside 1..%d", chunkCount, MaxChunks)
	}
	expectedLength := headerSize + int(chunkCount)*sha256.Size
	if len(encoded) != expectedLength {
		return Metadata{}, fmt.Errorf("state sync metadata length %d does not match canonical length %d", len(encoded), expectedLength)
	}
	chunkHashes := make([][]byte, int(chunkCount))
	for i := range chunkHashes {
		chunkHashes[i] = append([]byte(nil), encoded[offset:offset+sha256.Size]...)
		offset += sha256.Size
	}
	metadata := Metadata{
		Height: height, AppHash: appHash, BackupSize: backupSize, ChunkSize: chunkSize,
		BackupHash: backupHash, ChunkHashes: chunkHashes,
	}
	if err := validateMetadata(metadata); err != nil {
		return Metadata{}, err
	}
	return metadata, nil
}

// ValidateOffer binds untrusted ABCI snapshot metadata to CometBFT's trusted
// height/AppHash and to Snapshot.Hash before any chunks are accepted.
func ValidateOffer(format uint32, height uint64, chunks uint32, snapshotHash, encoded, trustedAppHash []byte) (Metadata, error) {
	if format != Format {
		return Metadata{}, fmt.Errorf("unsupported state sync format %d", format)
	}
	metadata, err := DecodeMetadata(encoded)
	if err != nil {
		return Metadata{}, err
	}
	wantSnapshotHash := sha256.Sum256(encoded)
	if len(snapshotHash) != sha256.Size || !bytes.Equal(snapshotHash, wantSnapshotHash[:]) {
		return Metadata{}, errors.New("state sync snapshot hash mismatch")
	}
	if metadata.Height != height || uint32(len(metadata.ChunkHashes)) != chunks {
		return Metadata{}, errors.New("state sync snapshot fields disagree with metadata")
	}
	if len(trustedAppHash) != sha256.Size || !bytes.Equal(metadata.AppHash, trustedAppHash) {
		return Metadata{}, errors.New("state sync metadata does not match trusted AppHash")
	}
	return metadata, nil
}

func validateMetadata(metadata Metadata) error {
	if metadata.Height == 0 {
		return errors.New("state sync height must be positive")
	}
	if len(metadata.AppHash) != sha256.Size || len(metadata.BackupHash) != sha256.Size {
		return errors.New("state sync hashes must be SHA-256 sized")
	}
	if metadata.ChunkSize < MinChunkSize || metadata.ChunkSize > MaxChunkSize {
		return fmt.Errorf("state sync chunk size %d is outside %d..%d", metadata.ChunkSize, MinChunkSize, MaxChunkSize)
	}
	count := uint64(len(metadata.ChunkHashes))
	if count == 0 || count > uint64(MaxChunks) {
		return fmt.Errorf("state sync chunk count %d is outside 1..%d", count, MaxChunks)
	}
	for i, hash := range metadata.ChunkHashes {
		if len(hash) != sha256.Size {
			return fmt.Errorf("state sync chunk hash %d is not SHA-256 sized", i)
		}
	}
	maxSize := count * uint64(metadata.ChunkSize)
	minSize := (count-1)*uint64(metadata.ChunkSize) + 1
	if metadata.BackupSize < minSize || metadata.BackupSize > maxSize {
		return fmt.Errorf("state sync backup size %d is inconsistent with %d chunks of %d", metadata.BackupSize, count, metadata.ChunkSize)
	}
	return nil
}

func expectedChunkSize(metadata Metadata, index uint32) (int, error) {
	count := uint32(len(metadata.ChunkHashes))
	if index >= count {
		return 0, fmt.Errorf("state sync chunk index %d out of range", index)
	}
	if index < count-1 {
		return int(metadata.ChunkSize), nil
	}
	last := metadata.BackupSize - uint64(count-1)*uint64(metadata.ChunkSize)
	return int(last), nil
}
