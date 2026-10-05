package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"unicode/utf8"

	badger "github.com/dgraph-io/badger/v4"
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
	output := []byte("sage.public-memory.leaf.v1\x00")
	for _, field := range []string{leaf.MemoryID, leaf.Author, leaf.AuthorPrincipal, leaf.Domain, leaf.Status} {
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

func publicLeafFromCanonical(reader *BadgerStore, identifier string) (*PublicMemoryLeaf, error) {
	if len(identifier) == 0 || len(identifier) > 256 || !utf8.ValidString(identifier) {
		return nil, ErrPublicMemoryIndex
	}
	state, err := reader.GetMemoryDisclosureState(identifier)
	if err != nil {
		return nil, err
	}
	if !state.ClassificationRecorded {
		return nil, ErrPublicMemoryIndex
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
	// Deliberately narrow. Only a record accepted before app-v25 lacks a
	// submission-height marker, and only those could have their hash erased by a
	// legacy lifecycle transition. A record born under app-v25 or later must
	// carry a complete canonical envelope, so a missing hash there is still a
	// hard inconsistency that refuses the build.
	if len(state.ContentHash) == 0 {
		_, bornUnderAppV25, markerErr := reader.GetMemorySubmissionHeight(identifier)
		if markerErr != nil {
			return nil, markerErr
		}
		if !bornUnderAppV25 {
			return nil, nil
		}
		return nil, ErrPublicMemoryIndex
	}
	if !state.DomainRecorded || !state.AuthorRecorded || !state.AuthorPrincipalRecorded || len(state.ContentHash) != 32 || state.CoCommitRecorded {
		return nil, ErrPublicMemoryIndex
	}
	leaf := &PublicMemoryLeaf{MemoryID: identifier, Author: state.Author, AuthorPrincipal: state.AuthorPrincipal,
		Domain: state.Domain, Status: state.Status}
	copy(leaf.ContentHash[:], state.ContentHash)
	_, err = leaf.canonical()
	return leaf, err
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

func (s *BadgerStore) SyncPublicMemoryIndex(identifier string) (err error) {
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
		leaf, err := publicLeafFromCanonical(reader, identifier)
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
		leaf, err := publicLeafFromCanonical(&BadgerStore{db: s.db, txn: txn}, identifier)
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
