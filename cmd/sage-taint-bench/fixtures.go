package main

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

type idProfile struct {
	Name string `json:"name"`
	Len  int    `json:"id_bytes"`
}

var idProfiles = []idProfile{{Name: "uuid", Len: uuidIDLen}, {Name: "cap512", Len: memoryIDCap}}

// fanout gives each seeded parent more children than one batch can reach.
const fanout = 450

type seeder struct{ batch *badger.WriteBatch }

func (s seeder) set(key, value []byte) error {
	return s.batch.Set(key, append([]byte{}, value...))
}

func (s seeder) header(decision string, header decisionHeader) error {
	return s.set(headerKey(decision), header.encode())
}

func seedBackground(db *badger.DB, records, contentBytes int) error {
	batch := db.NewWriteBatch()
	defer batch.Cancel()
	content := make([]byte, contentBytes)
	for i := range records {
		if err := batch.Set(memoryKey(makeID(uuidIDLen, "background", i)), content); err != nil {
			return err
		}
	}
	return batch.Flush()
}

type readFixture struct {
	deep []string
	flat []string
	wide []string
}

// seedRead builds the read worst cases. Deep: 16-parent ancestries whose nodes
// have 64 signers and memberships in all 32 decisions, none of them effective,
// so the walk never stops early. 31 decisions are cleared. One stays expanding,
// which keeps the walk switched on, and its memberships are a stale generation.
func seedRead(s seeder, p idProfile, chains, frontier, frontierDecisions, flat int) (readFixture, error) {
	var fixture readFixture
	ns := p.Name + "/deep"
	if err := s.set(activeKey, make([]byte, 8)); err != nil {
		return fixture, err
	}
	var ineffective []string
	for i := range maxActiveDecisions {
		decision := makeID(decisionIDLen, ns+"/decision", i)
		header := decisionHeader{Status: statusCleaning, Generation: 1}
		if i == 0 {
			header = decisionHeader{Status: statusExpanding, Generation: 2}
		}
		ineffective = append(ineffective, decision)
		if err := s.header(decision, header); err != nil {
			return fixture, err
		}
	}
	provenance := encodeProvenance(ns, maxSigners)
	member := encodeMember(1, 1, originExpand)
	nodes := maxServingWalk + maxParents
	for chain := range chains {
		node := func(n int) string { return makeID(p.Len, fmt.Sprintf("%s/%d", ns, chain), n) }
		fixture.deep = append(fixture.deep, node(0))
		for n := range nodes {
			id := node(n)
			if err := s.set(memProvKey(id), provenance); err != nil {
				return fixture, err
			}
			for parent := n + 1; parent <= n+maxParents && parent < nodes; parent++ {
				if err := s.set(lineageKey(id, node(parent)), nil); err != nil {
					return fixture, err
				}
			}
			for _, decision := range ineffective {
				if err := s.set(memberKey(id, decision), member); err != nil {
					return fixture, err
				}
			}
		}
	}

	ns = p.Name + "/wide"
	for d := range frontierDecisions {
		decision := makeID(decisionIDLen, ns, d)
		fixture.wide = append(fixture.wide, decision)
		header := decisionHeader{Status: statusExpanding, Generation: 1, Tail: uint64(frontier)}
		if err := s.header(decision, header); err != nil {
			return fixture, err
		}
		for seq := range frontier {
			entry := encodeQueueEntry(makeID(p.Len, fmt.Sprintf("%s/%d", ns, d), seq), 1)
			if err := s.set(queueKey(decision, 1, uint64(seq)), entry); err != nil {
				return fixture, err
			}
		}
	}

	ns = p.Name + "/flat"
	coauthors := make([]byte, 0, maxSigners*credentialLen)
	for i := range maxSigners {
		credential := makeID(credentialLen, ns+"/credential", i)
		coauthors = append(coauthors, credential...)
		if err := s.set(rootMarkerKey(credential), make([]byte, 8)); err != nil {
			return fixture, err
		}
	}
	for i := range flat {
		id := makeID(p.Len, ns, i)
		fixture.flat = append(fixture.flat, id)
		if err := s.set(authorPrincipalKey(id), []byte(makeID(credentialLen, ns, 0))); err != nil {
			return fixture, err
		}
		if err := s.set(authorKey(id), []byte(makeID(credentialLen, ns, 1))); err != nil {
			return fixture, err
		}
		if err := s.set(coauthorsKey(id), coauthors); err != nil {
			return fixture, err
		}
	}
	return fixture, nil
}

type blockFixture struct {
	txs    []txSpec
	active int64
}

func seedExpander(s seeder, ns string, p idProfile, i int) (string, error) {
	decision := makeID(decisionIDLen, ns+"/expander", i)
	target := makeID(p.Len, ns+"/target", i)
	if err := s.header(decision, decisionHeader{Status: statusExpanding, Generation: 1, Tail: 1}); err != nil {
		return "", err
	}
	if err := s.set(memberKey(target, decision), encodeMember(1, 0, originTarget)); err != nil {
		return "", err
	}
	if err := s.set(indexKey(decision, target), nil); err != nil {
		return "", err
	}
	if err := s.set(queueKey(decision, 1, 0), encodeQueueEntry(target, 0)); err != nil {
		return "", err
	}
	return decision, seedChildren(s, ns+"/expander-child", p, i, target)
}

func seedChildren(s seeder, ns string, p idProfile, i int, parent string) error {
	for n := range fanout {
		child := makeID(p.Len, fmt.Sprintf("%s/%d", ns, i), n)
		if err := s.set(lineageRefKey(parent, child), nil); err != nil {
			return err
		}
	}
	return nil
}

func seedCleaner(s seeder, ns string, p idProfile, i int) (string, error) {
	decision := makeID(decisionIDLen, ns+"/cleaner", i)
	if err := s.header(decision, decisionHeader{Status: statusCleaning, Generation: 1, Tail: 200}); err != nil {
		return "", err
	}
	for n := range txUnitBudget / 2 {
		memory := makeID(p.Len, fmt.Sprintf("%s/cleaned/%d", ns, i), n)
		if err := s.set(memberKey(memory, decision), encodeMember(1, 1, originExpand)); err != nil {
			return "", err
		}
		if err := s.set(indexKey(decision, memory), nil); err != nil {
			return "", err
		}
	}
	for seq := range 200 {
		entry := encodeQueueEntry(makeID(p.Len, fmt.Sprintf("%s/queued/%d", ns, i), seq), 1)
		if err := s.set(queueKey(decision, 1, uint64(seq)), entry); err != nil {
			return "", err
		}
	}
	return decision, nil
}

func seedActive(s seeder, ns string, count int) ([]string, error) {
	decisions := make([]string, count)
	for i := range decisions {
		decisions[i] = makeID(decisionIDLen, ns+"/active", i)
		if err := s.header(decisions[i], decisionHeader{Status: statusExpanding, Generation: 1}); err != nil {
			return nil, err
		}
	}
	return decisions, nil
}

func seedParents(s seeder, ns string, p idProfile, count int, decisions []string) ([]string, error) {
	parents := make([]string, count)
	for i := range parents {
		parents[i] = makeID(p.Len, ns+"/parent", i)
		for _, decision := range decisions {
			if err := s.set(memberKey(parents[i], decision), encodeMember(1, 1, originExpand)); err != nil {
				return nil, err
			}
		}
	}
	return parents, nil
}

func submits(ns string, p idProfile, count int, parents []string) []txSpec {
	txs := make([]txSpec, count)
	for i := range txs {
		txs[i] = txSpec{Kind: txSubmit, Memory: makeID(p.Len, ns+"/child", i), Parents: parents}
	}
	return txs
}

func seedFullAdmission(s seeder, ns string, p idProfile) (blockFixture, error) {
	decisions, err := seedActive(s, ns, maxActiveDecisions)
	if err != nil {
		return blockFixture{}, err
	}
	parents, err := seedParents(s, ns, p, maxParents, decisions)
	if err != nil {
		return blockFixture{}, err
	}
	return blockFixture{txs: submits(ns, p, maxBlockTxs, parents), active: maxActiveDecisions}, nil
}

// seedMixed is the worst mixed block by tx shape: one apply, four expand or cleanup
// txs and 59 submits. The apply target is a parent of every submit, so its
// membership is inherited in the same block it was created.
func seedMixed(s seeder, ns string, p idProfile, cleanup bool) (blockFixture, error) {
	target := makeID(p.Len, ns+"/apply-target", 0)
	if err := seedChildren(s, ns+"/apply-child", p, 0, target); err != nil {
		return blockFixture{}, err
	}
	txs := []txSpec{{Kind: txApply, Decision: makeID(decisionIDLen, ns+"/apply", 0), Target: target}}
	var effective []string
	active := int64(0)
	for i := range maxExpandCleanupTxs {
		var decision string
		var err error
		if cleanup {
			decision, err = seedCleaner(s, ns, p, i)
			txs = append(txs, txSpec{Kind: txCleanup, Decision: decision})
		} else {
			decision, err = seedExpander(s, ns, p, i)
			txs = append(txs, txSpec{Kind: txExpand, Decision: decision})
			effective = append(effective, decision)
		}
		if err != nil {
			return blockFixture{}, err
		}
		active++
	}
	others, err := seedActive(s, ns, maxActiveDecisions-1-maxExpandCleanupTxs)
	if err != nil {
		return blockFixture{}, err
	}
	active += int64(len(others))
	parents, err := seedParents(s, ns, p, maxParents-1, append(effective, others...))
	if err != nil {
		return blockFixture{}, err
	}
	txs = append(txs, submits(ns, p, maxBlockTxs-1-maxExpandCleanupTxs, append(parents, target))...)
	return blockFixture{txs: txs, active: active}, nil
}

func seedCleanupBlock(s seeder, ns string, p idProfile) (blockFixture, error) {
	var txs []txSpec
	for i := range maxExpandCleanupTxs {
		decision, err := seedCleaner(s, ns, p, i)
		if err != nil {
			return blockFixture{}, err
		}
		txs = append(txs, txSpec{Kind: txCleanup, Decision: decision})
	}
	return blockFixture{txs: txs, active: maxExpandCleanupTxs}, nil
}

// seedExpandSweep deliberately ignores the four-tx limit so the block ceiling
// alone decides how much expansion fits.
func seedExpandSweep(s seeder, ns string, p idProfile, count int) (blockFixture, error) {
	var txs []txSpec
	for i := range count {
		decision, err := seedExpander(s, ns, p, i)
		if err != nil {
			return blockFixture{}, err
		}
		txs = append(txs, txSpec{Kind: txExpand, Decision: decision})
	}
	return blockFixture{txs: txs, active: int64(count)}, nil
}
