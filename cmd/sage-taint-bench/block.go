package main

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

type txKind string

const (
	txApply   txKind = "apply"
	txExpand  txKind = "expand"
	txCleanup txKind = "cleanup"
	txSubmit  txKind = "submit"
)

type txSpec struct {
	Kind     txKind
	Decision string
	Target   string
	Memory   string
	Parents  []string
}

// stage is the per-tx staging buffer: writes reach the block transaction only
// after the tx finishes within its reservation.
type stage struct {
	txn        *badger.Txn
	tombstone  int64
	overlay    map[string][]byte
	order      []string
	units      int64
	taintBytes int64
	otherBytes int64
	v30Bytes   int64
	entries    int64
	accounted  int64
}

func newStage(txn *badger.Txn, tombstone int64) *stage {
	return &stage{txn: txn, tombstone: tombstone, overlay: map[string][]byte{}}
}

func (s *stage) get(key []byte) ([]byte, bool, error) {
	if value, ok := s.overlay[string(key)]; ok {
		return value, value != nil, nil
	}
	item, err := s.txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	value, err := item.ValueCopy(nil)
	return value, true, err
}

func (s *stage) record(key []byte, value []byte) {
	if _, ok := s.overlay[string(key)]; !ok {
		s.order = append(s.order, string(key))
	}
	s.overlay[string(key)] = value
	s.entries++
	s.accounted += int64(len(key)+len(value)) + badgerEntryOverhead
}

func (s *stage) putTaint(key, value []byte) {
	s.taintBytes += int64(len(key) + len(value))
	s.record(key, append([]byte{}, value...))
}

func (s *stage) putOther(key, value []byte) {
	s.otherBytes += int64(len(key) + len(value))
	s.record(key, append([]byte{}, value...))
}

// putV30 is a write app-v30 adds on submit but E does not bound: the memprov
// marker and the lineage edges. They are reported against the ceiling both ways.
func (s *stage) putV30(key, value []byte) {
	s.v30Bytes += int64(len(key) + len(value))
	s.putOther(key, value)
}

func (s *stage) deleteTaint(key []byte) {
	s.taintBytes += int64(len(key)) + s.tombstone
	s.record(key, nil)
}

func (s *stage) flush() error {
	for _, key := range s.order {
		value := s.overlay[key]
		var err error
		if value == nil {
			err = s.txn.Delete([]byte(key))
		} else {
			err = s.txn.Set([]byte(key), value)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// list returns up to limit entries under prefix strictly after start.
func (s *stage) list(prefix, start []byte, limit int) ([][]byte, error) {
	options := badger.DefaultIteratorOptions
	options.Prefix = prefix
	options.PrefetchValues = false
	it := s.txn.NewIterator(options)
	defer it.Close()
	seek := prefix
	if len(start) > 0 {
		seek = append(append([]byte{}, start...), 0)
	}
	var out [][]byte
	for it.Seek(seek); it.ValidForPrefix(prefix) && len(out) < limit; it.Next() {
		key := it.Item().KeyCopy(nil)
		if value, ok := s.overlay[string(key)]; ok && value == nil {
			continue
		}
		out = append(out, key)
	}
	return out, nil
}

func (s *stage) loadHeader(decision string) (decisionHeader, error) {
	raw, found, err := s.get(headerKey(decision))
	if err != nil {
		return decisionHeader{}, err
	}
	if !found {
		return decisionHeader{}, fmt.Errorf("missing header for decision %s", decision)
	}
	return decodeHeader(raw)
}

func (s *stage) storeHeader(decision string, header decisionHeader) {
	s.putTaint(headerKey(decision), header.encode())
}

func (s *stage) bumpRevision() {
	s.putTaint(revisionKey, make([]byte, 8))
}

func (s *stage) materialize(decision, memory string, header *decisionHeader, depth uint16, origin byte) {
	s.putTaint(memberKey(memory, decision), encodeMember(header.Generation, depth, origin))
	s.putTaint(indexKey(decision, memory), nil)
	s.putTaint(queueKey(decision, header.Generation, header.Tail), encodeQueueEntry(memory, depth))
	header.Tail++
	header.Materialized++
}

// expand runs one bounded BFS batch. A child is touched only when its full
// five-unit transition fits, so the cursor never passes a partial transition.
func (s *stage) expand(decision string, header *decisionHeader, budget int64) error {
	fresh := 0
	for {
		if header.Parent == "" {
			if header.Head == header.Tail {
				header.Status = statusComplete
				return nil
			}
			if budget < 2 {
				return nil
			}
			key := queueKey(decision, header.Generation, header.Head)
			raw, found, err := s.get(key)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("queue gap at seq %d", header.Head)
			}
			parent, depth, err := decodeQueueEntry(raw)
			if err != nil {
				return err
			}
			s.deleteTaint(key)
			s.units += 2
			budget -= 2
			header.Parent, header.ParentDepth, header.LastEdge = parent, depth, ""
			header.Head++
		}
		if fresh == maxNewPerExecution || budget < 5 {
			return nil
		}
		prefix := lineageRefPrefix(header.Parent)
		var start []byte
		if header.LastEdge != "" {
			start = lineageRefKey(header.Parent, header.LastEdge)
		}
		edges, err := s.list(prefix, start, int(budget/2)+1)
		if err != nil {
			return err
		}
		if len(edges) == 0 {
			s.units++
			budget--
			header.Parent, header.LastEdge = "", ""
			continue
		}
		for _, edge := range edges {
			if fresh == maxNewPerExecution || budget < 5 {
				return nil
			}
			child := string(bytes.TrimPrefix(edge, prefix))
			s.units += 2
			budget -= 2
			header.Scanned++
			_, member, err := s.get(memberKey(child, decision))
			if err != nil {
				return err
			}
			switch {
			case member:
			case header.ParentDepth+1 > maxDepth:
				header.Status = statusDepthExhausted
			default:
				s.materialize(decision, child, header, header.ParentDepth+1, originExpand)
				s.units += 3
				budget -= 3
				fresh++
			}
			header.LastEdge = child
		}
	}
}

func (s *stage) execApply(tx txSpec) error {
	header := decisionHeader{Status: statusExpanding, Generation: 1}
	s.units += 3
	s.materialize(tx.Decision, tx.Target, &header, 0, originTarget)
	s.units += 3
	if err := s.expand(tx.Decision, &header, txUnitBudget-6); err != nil {
		return err
	}
	s.storeHeader(tx.Decision, header)
	s.bumpRevision()
	return nil
}

func (s *stage) execExpand(tx txSpec) error {
	header, err := s.loadHeader(tx.Decision)
	if err != nil {
		return err
	}
	s.units += 3
	if header.Status != statusExpanding {
		return fmt.Errorf("decision %s is not expanding", tx.Decision)
	}
	if err := s.expand(tx.Decision, &header, txUnitBudget-3); err != nil {
		return err
	}
	s.storeHeader(tx.Decision, header)
	s.bumpRevision()
	return nil
}

func (s *stage) execCleanup(tx txSpec) error {
	header, err := s.loadHeader(tx.Decision)
	if err != nil {
		return err
	}
	s.units += 3
	budget := int64(txUnitBudget - 3)
	prefix := indexPrefix(tx.Decision)
	indexed, err := s.list(prefix, nil, int(budget/3))
	if err != nil {
		return err
	}
	for _, key := range indexed {
		memory := string(bytes.TrimPrefix(key, prefix))
		s.deleteTaint(key)
		s.deleteTaint(memberKey(memory, tx.Decision))
		s.units += 3
		budget -= 3
	}
	if budget >= 2 {
		queued, err := s.list(queuePrefix(tx.Decision), nil, int(budget/2))
		if err != nil {
			return err
		}
		for _, key := range queued {
			s.deleteTaint(key)
			s.units += 2
			budget -= 2
		}
		if len(queued) == 0 && len(indexed) == 0 {
			header.Status = statusClean
		}
	}
	s.storeHeader(tx.Decision, header)
	s.bumpRevision()
	return nil
}

func (s *stage) execSubmit(tx txSpec, content, provenance []byte) error {
	s.putOther(memoryKey(tx.Memory), content)
	s.putOther(authorKey(tx.Memory), []byte(strings.Repeat("a", credentialLen)))
	s.putOther(authorPrincipalKey(tx.Memory), []byte(strings.Repeat("p", credentialLen)))
	s.putOther(heightKey(tx.Memory), make([]byte, 8))
	s.putV30(memProvKey(tx.Memory), provenance)
	headers := map[string]decisionHeader{}
	inherited := map[string]decisionHeader{}
	for _, parent := range tx.Parents {
		s.putV30(lineageKey(tx.Memory, parent), nil)
		s.putV30(lineageRefKey(parent, tx.Memory), nil)
		s.units++
		prefix := memberPrefix(parent)
		members, err := s.list(prefix, nil, maxActiveDecisions)
		if err != nil {
			return err
		}
		for _, key := range members {
			s.units++
			decision := string(bytes.TrimPrefix(key, prefix))
			header, ok := headers[decision]
			if !ok {
				if header, err = s.loadHeader(decision); err != nil {
					return err
				}
				s.units++
				headers[decision] = header
			}
			if header.Status <= statusDepthExhausted {
				inherited[decision] = header
			}
		}
	}
	for decision, header := range inherited {
		s.putTaint(memberKey(tx.Memory, decision), encodeMember(header.Generation, 0, originAdmit))
		s.putTaint(indexKey(decision, tx.Memory), nil)
		s.units += 2
	}
	s.bumpRevision()
	s.units++
	return nil
}

// estimator is the one bound used for proposal and execution. It reads only
// the tx and committed facts, so every validator computes the same answer.
type estimator struct {
	sizes     sizes
	tombstone int64
	// activeDecisions counts decisions at H-1 plus applies in this block. A
	// parent's memberships can grow inside the block, so a bound from the
	// parents' committed memberships would not be sound.
	activeDecisions int64
}

func (e estimator) expandBatch(units int64) int64 {
	popBytes := e.sizes.queueKey + e.tombstone
	limit := min(int64(maxNewPerExecution), units/5)
	best := int64(0)
	for fresh := int64(0); fresh <= limit; fresh++ {
		pops := (units - 5*fresh) / 2
		best = max(best, fresh*e.sizes.triple+pops*popBytes)
	}
	return best
}

func (e estimator) cleanup() int64 {
	units := int64(txUnitBudget - 3)
	pairBytes := e.sizes.indexKey + e.sizes.memberKey + 2*e.tombstone
	queueBytes := e.sizes.queueKey + e.tombstone
	best := int64(0)
	for pairs := int64(0); pairs <= units/3; pairs++ {
		best = max(best, pairs*pairBytes+(units-3*pairs)/2*queueBytes)
	}
	return e.sizes.header + e.sizes.revision + best
}

func (e estimator) estimate(tx txSpec) int64 {
	switch tx.Kind {
	case txApply:
		return e.sizes.header + e.sizes.revision + e.sizes.triple + e.expandBatch(txUnitBudget-6)
	case txExpand:
		return e.sizes.header + e.sizes.revision + e.expandBatch(txUnitBudget-3)
	case txCleanup:
		return e.cleanup()
	case txSubmit:
		decisions := min(e.activeDecisions, maxActiveDecisions)
		pair := int64(len(memberKey(tx.Memory, strings.Repeat("d", decisionIDLen)))) + memberValueLen +
			int64(len(indexKey(strings.Repeat("d", decisionIDLen), tx.Memory)))
		return decisions*pair + e.sizes.revision
	}
	return 0
}

type blockOptions struct {
	ceiling        int64
	tombstone      int64
	activeAtHeight int64
	contentBytes   int
	estimateCap    map[int]int64
}

// blockDeterministic must match byte for byte on both architectures.
type blockDeterministic struct {
	Txs                int              `json:"txs"`
	TxsByKind          map[txKind]int   `json:"txs_by_kind"`
	Units              int64            `json:"units"`
	MaxUnitsByKind     map[txKind]int64 `json:"max_units_by_kind"`
	GeneratedTaint     int64            `json:"generated_taint_bytes"`
	OtherWrites        int64            `json:"other_write_bytes"`
	V30Writes          int64            `json:"v30_write_bytes"`
	Estimate           int64            `json:"estimate_bytes"`
	WriteSetEntries    int64            `json:"write_set_entries"`
	WriteSetAccounted  int64            `json:"write_set_badger_accounted_bytes"`
	ProcessProposal    string           `json:"process_proposal"`
	ProposalWithV30    string           `json:"process_proposal_counting_v30_writes"`
	SelectedByPrepare  int              `json:"txs_selected_by_prepare_proposal"`
	OverflowedTxs      int              `json:"overflowed_txs"`
	NewMemberships     int64            `json:"new_expansion_memberships"`
	ActualWithinBudget bool             `json:"actual_le_estimate_every_tx"`
}

type blockOutcome struct {
	blockDeterministic
	finalize time.Duration
	commit   time.Duration
	sync     time.Duration
}

// runBlock is the stand-in for ProcessProposal plus FinalizeBlock. An
// over-ceiling block is still executed so its real size can be reported, and
// is marked as rejected.
func runBlock(db *badger.DB, txs []txSpec, options blockOptions) (blockOutcome, error) {
	est := estimator{sizes: maximumSizes(), tombstone: options.tombstone, activeDecisions: options.activeAtHeight}
	outcome := blockOutcome{blockDeterministic: blockDeterministic{
		Txs: len(txs), TxsByKind: map[txKind]int{}, MaxUnitsByKind: map[txKind]int64{},
		ProcessProposal: "accept", ActualWithinBudget: true,
	}}
	estimates := make([]int64, len(txs))
	for _, tx := range txs {
		if tx.Kind == txApply {
			est.activeDecisions++
		}
	}
	selected := int64(0)
	for i, tx := range txs {
		estimates[i] = est.estimate(tx)
		if bound, ok := options.estimateCap[i]; ok {
			estimates[i] = bound
		}
		outcome.Estimate += estimates[i]
		if selected+estimates[i] <= options.ceiling {
			selected += estimates[i]
			outcome.SelectedByPrepare++
		}
		outcome.TxsByKind[tx.Kind]++
	}
	if outcome.Estimate > options.ceiling {
		outcome.ProcessProposal = "reject"
	}

	content := bytes.Repeat([]byte{'c'}, options.contentBytes)
	provenance := encodeProvenance("block", maxSigners)
	txn := db.NewTransaction(true)
	defer txn.Discard()
	started := time.Now()
	for i, tx := range txs {
		s := newStage(txn, options.tombstone)
		var err error
		switch tx.Kind {
		case txApply:
			err = s.execApply(tx)
		case txExpand:
			err = s.execExpand(tx)
		case txCleanup:
			err = s.execCleanup(tx)
		case txSubmit:
			err = s.execSubmit(tx, content, provenance)
		default:
			err = fmt.Errorf("unknown tx kind %q", tx.Kind)
		}
		if err != nil {
			return outcome, fmt.Errorf("tx %d (%s): %w", i, tx.Kind, err)
		}
		outcome.MaxUnitsByKind[tx.Kind] = max(outcome.MaxUnitsByKind[tx.Kind], s.units)
		if s.taintBytes > estimates[i] {
			// An estimator bug: this tx fails alone and the rest of the block
			// commits. Its cursor is untouched because nothing was flushed.
			outcome.OverflowedTxs++
			outcome.ActualWithinBudget = false
			continue
		}
		if err := s.flush(); err != nil {
			return outcome, fmt.Errorf("flush tx %d (%s): %w", i, tx.Kind, err)
		}
		outcome.Units += s.units
		outcome.GeneratedTaint += s.taintBytes
		outcome.OtherWrites += s.otherBytes
		outcome.V30Writes += s.v30Bytes
		outcome.WriteSetEntries += s.entries
		outcome.WriteSetAccounted += s.accounted
		if tx.Kind == txExpand || tx.Kind == txApply {
			outcome.NewMemberships += countQueueWrites(s)
		}
	}
	outcome.finalize = time.Since(started)
	outcome.ProposalWithV30 = "accept"
	if outcome.Estimate+outcome.V30Writes > options.ceiling {
		outcome.ProposalWithV30 = "reject"
	}
	commitStarted := time.Now()
	if err := txn.Commit(); err != nil {
		return outcome, fmt.Errorf("commit block: %w", err)
	}
	outcome.commit = time.Since(commitStarted)
	syncStarted := time.Now()
	if err := db.Sync(); err != nil {
		return outcome, fmt.Errorf("sync block: %w", err)
	}
	outcome.sync = time.Since(syncStarted)
	return outcome, nil
}

func countQueueWrites(s *stage) int64 {
	count := int64(0)
	for _, key := range s.order {
		if strings.HasPrefix(key, "taintd:") && s.overlay[key] != nil {
			count++
		}
	}
	return count
}
