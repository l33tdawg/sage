package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"unicode/utf8"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/tx"
)

var ErrPublicMemoryIndex = errors.New("public memory index unavailable or inconsistent")

const publicIndexPrefix = "public-memory-index:v1:"

type PublicMemoryLeaf struct {
	MemoryID        string
	ContentHash     [32]byte
	Author          string
	AuthorPrincipal string
	Domain          string
	Status          string
}

type PublicMemoryBranch struct {
	Leaf     PublicMemoryLeaf
	Siblings [256][32]byte
	Root     [32]byte
}

func (leaf PublicMemoryLeaf) canonical() ([]byte, error) {
	if !isCanonicalAgentID(leaf.Author) {
		return nil, ErrPublicMemoryIndex
	}
	switch leaf.Status {
	case "proposed", "validated", "committed", "challenged", "deprecated":
	default:
		return nil, ErrPublicMemoryIndex
	}
	// Proof serialization accepts every app-v30 identity. Consensus and legacy
	// migration selection retain their own stricter historical policy.
	if err := validatePublicMemoryIdentity(leaf.MemoryID, true); err != nil {
		return nil, err
	}
	output := []byte("sage.public-memory.leaf.v1\x00")
	output = binary.BigEndian.AppendUint16(output, uint16(len(leaf.MemoryID))) // #nosec G115 -- identity is bounded to 512 bytes
	output = append(output, leaf.MemoryID...)
	for _, field := range []string{leaf.Author, leaf.AuthorPrincipal, leaf.Domain, leaf.Status} {
		if len(field) == 0 || len(field) > 256 || !utf8.ValidString(field) {
			return nil, ErrPublicMemoryIndex
		}
		output = binary.BigEndian.AppendUint16(output, uint16(len(field)))
		output = append(output, field...)
	}
	output = append(output, 0)
	return append(output, leaf.ContentHash[:]...), nil
}

func publicPath(identifier string) [32]byte {
	return sha256.Sum256(append([]byte("sage.public-memory.path.v1\x00"), identifier...))
}

func publicParent(left, right [32]byte) [32]byte {
	input := append([]byte("sage.public-memory.node.v1\x00"), left[:]...)
	return sha256.Sum256(append(input, right[:]...))
}

var publicEmpty = func() [257][32]byte {
	var empty [257][32]byte
	empty[256] = sha256.Sum256([]byte("sage.public-memory.empty.v1\x00"))
	for depth := 255; depth >= 0; depth-- {
		empty[depth] = publicParent(empty[depth+1], empty[depth+1])
	}
	return empty
}()

func publicNodeKey(path [32]byte, depth int) []byte {
	if depth < 256 {
		path[depth/8] &= byte(0xff << uint(8-depth%8))
		clear(path[depth/8+1:])
	}
	key := binary.BigEndian.AppendUint16([]byte(publicIndexPrefix+"node:"), uint16(depth))
	return append(key, path[:]...)
}

func publicHashAt(txn *badger.Txn, path [32]byte, depth int) ([32]byte, error) {
	item, err := txn.Get(publicNodeKey(path, depth))
	if errors.Is(err, badger.ErrKeyNotFound) && depth != 0 {
		if _, promotionErr := txn.Get(publicStagePromoted); promotionErr == nil {
			item, err = txn.Get(publicStageKey(publicNodeKey(path, depth)))
		} else if !errors.Is(promotionErr, badger.ErrKeyNotFound) {
			return [32]byte{}, ErrPublicMemoryIndex
		}
	}
	if errors.Is(err, badger.ErrKeyNotFound) && depth != 0 {
		return publicEmpty[depth], nil
	}
	var hash [32]byte
	if err != nil {
		return hash, ErrPublicMemoryIndex
	}
	err = item.Value(func(value []byte) error {
		if len(value) != 32 {
			return ErrPublicMemoryIndex
		}
		copy(hash[:], value)
		return nil
	})
	return hash, err
}

func publicBranch(txn *badger.Txn, identifier string) ([256][32]byte, [32]byte, error) {
	var siblings [256][32]byte
	path := publicPath(identifier)
	root, err := publicHashAt(txn, path, 0)
	if err != nil {
		return siblings, root, err
	}
	for depth := 0; depth < 256; depth++ {
		other := path
		other[depth/8] ^= 1 << uint(7-depth%8)
		siblings[depth], err = publicHashAt(txn, other, depth+1)
		if err != nil {
			return siblings, root, err
		}
	}
	return siblings, root, nil
}

func publicFold(path, hash [32]byte, siblings [256][32]byte) [32]byte {
	for depth := 255; depth >= 0; depth-- {
		if path[depth/8]&(1<<uint(7-depth%8)) == 0 {
			hash = publicParent(hash, siblings[depth])
		} else {
			hash = publicParent(siblings[depth], hash)
		}
	}
	return hash
}

func VerifyPublicMemoryBranch(proof PublicMemoryBranch, expectedRoot [32]byte) error {
	encoded, err := proof.Leaf.canonical()
	if err != nil {
		return err
	}
	if proof.Root != expectedRoot || publicFold(publicPath(proof.Leaf.MemoryID), sha256.Sum256(encoded), proof.Siblings) != expectedRoot {
		return ErrPublicMemoryIndex
	}
	return nil
}

func validatePublicMemoryIdentity(identifier string, appV30 bool) error {
	if len(identifier) == 0 || len(identifier) > tx.MaxDerivedFromMemoryIDBytes {
		return ErrPublicMemoryIndex
	}
	if !appV30 && (len(identifier) > 256 || !utf8.ValidString(identifier)) {
		return ErrPublicMemoryIndex
	}
	return nil
}

func publicLeafFromCanonical(reader *BadgerStore, identifier string) (*PublicMemoryLeaf, error) {
	return publicLeafFromCanonicalWithIdentityPolicy(reader, identifier, false)
}

func publicLeafFromCanonicalWithIdentityPolicy(reader *BadgerStore, identifier string, appV30 bool) (*PublicMemoryLeaf, error) {
	if err := validatePublicMemoryIdentity(identifier, appV30); err != nil {
		return nil, err
	}
	readState := reader.GetMemoryDisclosureState
	if appV30 {
		readState = reader.GetMemoryDisclosureStateAppV30
	}
	state, err := readState(identifier)
	if err != nil {
		return nil, err
	}
	if !state.ClassificationRecorded {
		// GetMemoryDisclosureState defaults a missing class record to INTERNAL,
		// so the record is not part of the public set. The corpus audit uses the
		// same selector and must agree with this path: treating the default as a
		// hard error made an audit-clean 72,761-memory corpus still abort the
		// activation stage. An app-v23 record that recorded a principal but lost
		// its class is caught earlier as ErrMemoryDisclosureCorrupt.
		return nil, nil
	}
	if state.Classification != 0 {
		return nil, nil
	}
	// A PUBLIC record whose historical lifecycle transition erased its canonical
	// content hash has nothing to commit: a leaf encodes a 32-byte content hash,
	// so there is no canonical form to fold into the tree. Treating one such
	// record as a build error is what makes an app-v28 activation block
	// unreplayable — the stage is prepared OUTSIDE the consensus transaction, so
	// the error aborts the same block on every replay and the node can never
	// start again. Quarantine it instead, exactly as the co-commit tombstone
	// index already does by skipping a record whose decoded hash is not 32 bytes
	// (internal/store/cocommit_tombstone.go). The record joins the committed
	// public set in the block that re-anchors its hash, because
	// SyncPublicMemoryChanges folds every changed memory back into the promoted
	// index.
	//
	// The quarantine covers every envelope defect on a pre-app-v25 record, not
	// only an erased hash. Those records predate the canonical envelope
	// contract, and a legacy lifecycle transition could erase or truncate any
	// recorded field, not just the hash. A record born under app-v25 or later
	// must carry a complete envelope, so every defect there is still a hard
	// inconsistency that refuses the build. The read-only corpus audit
	// (AuditPublicMemoryEnvelopes) names every quarantined record and the exact
	// checks it failed, so the omission is never silent.
	_, bornUnderAppV25, markerErr := reader.GetMemorySubmissionHeight(identifier)
	if markerErr != nil {
		return nil, markerErr
	}
	quarantineLegacy := func() (*PublicMemoryLeaf, error) {
		if !bornUnderAppV25 {
			return nil, nil
		}
		return nil, ErrPublicMemoryIndex
	}
	if !state.DomainRecorded || !state.AuthorRecorded || !state.AuthorPrincipalRecorded ||
		len(state.ContentHash) != 32 || state.CoCommitRecorded {
		return quarantineLegacy()
	}
	leaf := &PublicMemoryLeaf{MemoryID: identifier, Author: state.Author, AuthorPrincipal: state.AuthorPrincipal,
		Domain: state.Domain, Status: state.Status}
	copy(leaf.ContentHash[:], state.ContentHash)
	// A field that is recorded but empty or malformed is not the legacy
	// missing-binding class: someone wrote an explicit invalid value, so refuse
	// it even on an old record.
	if _, encodeErr := leaf.canonical(); encodeErr != nil {
		return nil, encodeErr
	}
	return leaf, nil
}

func (s *BadgerStore) InitializeEmptyPublicMemoryIndex() error {
	if s.txn == nil {
		return ErrPublicMemoryIndex
	}
	return s.update(func(txn *badger.Txn) error {
		for _, prefix := range []string{"memory:", publicIndexPrefix} {
			iterator := txn.NewIterator(badger.DefaultIteratorOptions)
			iterator.Seek([]byte(prefix))
			found := iterator.ValidForPrefix([]byte(prefix))
			iterator.Close()
			if found {
				return ErrPublicMemoryIndex
			}
		}
		return s.txnSet(txn, publicNodeKey([32]byte{}, 0), publicEmpty[0][:])
	})
}

// SyncPublicMemoryIndex preserves the legacy app-v28 identity rules.
func (s *BadgerStore) SyncPublicMemoryIndex(identifier string) error {
	return s.syncPublicMemoryIndex(identifier, false)
}

func (s *BadgerStore) syncPublicMemoryIndex(identifier string, appV30 bool) (err error) {
	if s.txn == nil {
		return ErrPublicMemoryIndex
	}
	defer func() {
		if err != nil && s.poisoned == nil {
			s.poisoned = ErrPublicMemoryIndex
		}
	}()
	return s.update(func(txn *badger.Txn) error {
		reader := &BadgerStore{db: s.db, txn: txn}
		leaf, err := publicLeafFromCanonicalWithIdentityPolicy(reader, identifier, appV30)
		if err != nil {
			return err
		}
		path := publicPath(identifier)
		siblings, root, err := publicBranch(txn, identifier)
		if err != nil {
			return err
		}
		old, err := publicHashAt(txn, path, 256)
		if err != nil || publicFold(path, old, siblings) != root {
			return ErrPublicMemoryIndex
		}
		hash := publicEmpty[256]
		if leaf != nil {
			encoded, encodeErr := leaf.canonical()
			if encodeErr != nil {
				return encodeErr
			}
			hash = sha256.Sum256(encoded)
		}
		if hash == old {
			return nil
		}
		for depth := 256; depth >= 0; depth-- {
			key := publicNodeKey(path, depth)
			err = s.txnSet(txn, key, append([]byte(nil), hash[:]...))
			if err != nil {
				return err
			}
			if depth > 0 {
				if path[(depth-1)/8]&(1<<uint(7-(depth-1)%8)) == 0 {
					hash = publicParent(hash, siblings[depth-1])
				} else {
					hash = publicParent(siblings[depth-1], hash)
				}
			}
		}
		return nil
	})
}

func (s *BadgerStore) PublicMemoryBranch(identifier string) (*PublicMemoryBranch, error) {
	if s.txn != nil {
		return nil, ErrPublicMemoryIndex
	}
	var proof *PublicMemoryBranch
	err := s.view(func(txn *badger.Txn) error {
		// The stored root, verified below, binds the exact raw identity bytes.
		// Wider proof reconstruction cannot add a leaf to a legacy root.
		leaf, err := publicLeafFromCanonicalWithIdentityPolicy(&BadgerStore{db: s.db, txn: txn}, identifier, true)
		if err != nil {
			return err
		}
		if leaf == nil {
			return ErrPublicMemoryIndex
		}
		siblings, root, err := publicBranch(txn, identifier)
		if err != nil {
			return err
		}
		candidate := PublicMemoryBranch{Leaf: *leaf, Siblings: siblings, Root: root}
		if err := VerifyPublicMemoryBranch(candidate, root); err != nil {
			return err
		}
		proof = &candidate
		return nil
	})
	return proof, err
}

func (s *BadgerStore) PublicMemoryRoot() ([32]byte, error) {
	var root [32]byte
	err := s.view(func(txn *badger.Txn) error {
		var err error
		root, err = publicHashAt(txn, [32]byte{}, 0)
		return err
	})
	return root, err
}

func PublicMemoryCompositeHash(legacy, memoryRoot []byte) ([]byte, error) {
	if len(legacy) != 32 || len(memoryRoot) != 32 {
		return nil, ErrPublicMemoryIndex
	}
	input := bytes.NewBufferString("sage.app.public-memory.v1\x00")
	input.Write(legacy)
	input.Write(memoryRoot)
	hash := sha256.Sum256(input.Bytes())
	return hash[:], nil
}
