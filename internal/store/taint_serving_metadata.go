package store

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/lineage"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

// Discovery has at most MaxServingAncestorWalk charged reads across the whole
// request, independent of the mandatory header/ancestor proof allowances.
// One unit is a point read (including absence), or one iterator key/value.
// Iterator setup/seek is bounded by the charged node/edge/membership reads.
// These local caps are not consensus byte limits or wall-clock guarantees.
func (s *TaintServingReadScope) takeCoverageRead() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.coverageRemaining == 0 {
		return false
	}
	s.coverageRemaining--
	return true
}

var errTaintCoverageUnproved = errors.New("optional taint coverage is unproved")

type taintCoverageObservation struct {
	complete bool
	missing  bool
	depth    bool
	work     bool
}

type taintCoverageNode struct {
	id       string
	expected [sha256.Size]byte
	bound    bool
}

func (s *BadgerStore) discoverTaintCoverage(memoryID string, scope *TaintServingReadScope) (observation taintCoverageObservation) {
	queue := []taintCoverageNode{{id: memoryID}}
	hashes := make(map[string][sha256.Size]byte)
	graph := make(map[string][]string)
	headers := make(map[string]taint.Header)
	incomplete := false
	roots := 0
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if hash, seen := hashes[current.id]; seen {
			if current.bound && hash != current.expected {
				return observation
			}
			continue
		}
		if len(hashes) == taint.MaxServingAncestorWalk {
			return observation
		}
		// Existing canonical statuses are much smaller; this independent local
		// value ceiling also refuses oversized corrupt entries before copying.
		raw, found, err := s.taintCoverageValue(memoryKey(current.id), 4+sha256.Size+taint.MaxCursorBytes, scope)
		if err != nil || !found {
			return observation
		}
		hashBytes, _, err := decodeMemoryHashEntry(raw)
		if err != nil || len(hashBytes) != sha256.Size {
			return observation
		}
		var hash [sha256.Size]byte
		copy(hash[:], hashBytes)
		if current.bound && hash != current.expected {
			return observation
		}
		hashes[current.id] = hash
		if membershipErr := s.taintCoverageMemberships(current.id, scope, headers, &observation, &incomplete); membershipErr != nil {
			return observation
		}
		parents, err := s.taintCoverageParents(current.id, scope)
		if err != nil {
			return observation
		}
		graph[current.id] = nil
		if len(parents) == 0 {
			raw, found, err := s.taintCoverageValue(AppV30ProvenanceKey(current.id), 1+maxAppV30ProvenanceSigners*appV30ProvenanceSignerSize, scope)
			if err != nil {
				return observation
			}
			if !found {
				observation.missing = true
				continue
			}
			if _, err := decodeAppV30Provenance(raw); err != nil {
				return observation
			}
			roots++
		}
		for _, ref := range parents {
			graph[current.id] = append(graph[current.id], ref.MemoryID)
			queue = append(queue, taintCoverageNode{id: ref.MemoryID, expected: ref.ContentHash, bound: true})
		}
	}
	// A known leaf in one branch must not hide a cycle in another branch.
	colors := make(map[string]uint8)
	var acyclic func(string) bool
	acyclic = func(id string) bool {
		if colors[id] == 1 {
			return false
		}
		if colors[id] == 2 {
			return true
		}
		colors[id] = 1
		for _, parent := range graph[id] {
			if !acyclic(parent) {
				return false
			}
		}
		colors[id] = 2
		return true
	}
	observation.complete = roots > 0 && !observation.missing && !incomplete && acyclic(memoryID)
	return observation
}

func (s *BadgerStore) taintCoverageValue(key []byte, maximum int, scope *TaintServingReadScope) ([]byte, bool, error) {
	if !scope.takeCoverageRead() {
		return nil, false, errTaintCoverageUnproved
	}
	item, err := s.txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if item.ValueSize() > int64(maximum) {
		return nil, false, errTaintCoverageUnproved
	}
	raw, err := item.ValueCopy(nil)
	return raw, err == nil, err
}

func (s *BadgerStore) taintCoverageMemberships(memoryID string, scope *TaintServingReadScope, headers map[string]taint.Header, observation *taintCoverageObservation, incomplete *bool) error {
	prefix := taint.MembershipPrefix(memoryID)
	options := badger.DefaultIteratorOptions
	options.Prefix = prefix
	options.PrefetchValues = false
	it := s.txn.NewIterator(options)
	defer it.Close()
	entries := 0
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		entries++
		if entries > taint.MaxActiveDecisions || !scope.takeCoverageRead() {
			return errTaintCoverageUnproved
		}
		key := it.Item().Key()
		if len(key) != len(prefix)+taint.DecisionIDBytes {
			return errTaintCoverageUnproved
		}
		id := string(bytes.TrimPrefix(key, prefix))
		if err := taint.ValidateDecisionID(id); err != nil {
			return err
		}
		if it.Item().ValueSize() != 8+2+1 {
			return errTaintCoverageUnproved
		}
		raw, err := it.Item().ValueCopy(nil)
		if err != nil {
			return err
		}
		membership, err := taint.DecodeMembership(raw)
		if err != nil {
			return err
		}
		header, cached := headers[id]
		if !cached {
			// The codec's fixed 79 bytes plus its three bounded strings.
			raw, found, err := s.taintCoverageValue(taint.DecisionKey(id), 79+taint.DecisionIDBytes+taint.MaxMemoryIDBytes+taint.MaxCursorBytes, scope)
			if err != nil || !found {
				return errTaintCoverageUnproved
			}
			header, err = taint.DecodeHeader(raw)
			if err != nil {
				return err
			}
			headers[id] = header
		}
		if header.MembershipEffective(membership) {
			*incomplete = *incomplete || header.Status.Incomplete()
			observation.depth = observation.depth || header.Status == taint.StatusDepthExhausted
			observation.work = observation.work || header.Status == taint.StatusWorkExhausted
		}
	}
	return nil
}

func (s *BadgerStore) taintCoverageParents(memoryID string, scope *TaintServingReadScope) ([]tx.ParentRef, error) {
	prefix := lineage.ForwardPrefix(memoryID)
	options := badger.DefaultIteratorOptions
	options.Prefix = prefix
	options.PrefetchValues = false
	it := s.txn.NewIterator(options)
	defer it.Close()
	var parents []tx.ParentRef
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		if len(parents) == tx.MaxDerivedFromParents || !scope.takeCoverageRead() {
			return nil, errTaintCoverageUnproved
		}
		key := it.Item().Key()
		if len(key) != len(prefix)+2*lineage.ParentIDBytes {
			return nil, errTaintCoverageUnproved
		}
		id := string(bytes.TrimPrefix(key, prefix))
		if err := lineage.ValidateIDs(memoryID, id); err != nil {
			return nil, err
		}
		if it.Item().ValueSize() > 2+tx.MaxDerivedFromChainIDBytes+2+tx.MaxDerivedFromMemoryIDBytes+sha256.Size {
			return nil, errTaintCoverageUnproved
		}
		raw, err := it.Item().ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		ref, err := tx.DecodeParentRefValue(raw)
		if err != nil {
			return nil, err
		}
		derived, err := tx.ParentRefIDHex(ref)
		if err != nil || derived != id {
			return nil, fmt.Errorf("optional coverage parent identity mismatch")
		}
		if _, found, err := s.taintCoverageValue(lineage.ReverseKey(id, memoryID), 0, scope); err != nil || !found {
			return nil, errTaintCoverageUnproved
		}
		parents = append(parents, ref)
	}
	return parents, nil
}
