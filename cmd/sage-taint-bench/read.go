package main

import (
	"bytes"
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

var errUnitCeiling = errors.New("taint unit ceiling exceeded: narrow the query")

// meter charges one unit per key read, iterator advance, write and delete. A
// charge that would pass the ceiling fails before the operation happens.
type meter struct {
	units   int64
	ceiling int64
}

func (m *meter) charge(n int64) error {
	if m.ceiling > 0 && m.units+n > m.ceiling {
		return errUnitCeiling
	}
	m.units += n
	return nil
}

// request is a stand-in for one serving request over canonical state.
type request struct {
	txn      *badger.Txn
	meter    *meter
	headers  map[string]decisionHeader
	verdicts map[string]bool
}

func newRequest(txn *badger.Txn, ceiling int64) (*request, error) {
	r := &request{
		txn:      txn,
		meter:    &meter{ceiling: ceiling},
		headers:  map[string]decisionHeader{},
		verdicts: map[string]bool{},
	}
	if _, _, err := r.get(activeKey); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *request) get(key []byte) ([]byte, bool, error) {
	if err := r.meter.charge(1); err != nil {
		return nil, false, err
	}
	item, err := r.txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	value, err := item.ValueCopy(nil)
	return value, true, err
}

// scan charges 1 for the seek and 1 for each entry iterated.
func (r *request) scan(prefix []byte, visit func(key, value []byte) error) error {
	if err := r.meter.charge(1); err != nil {
		return err
	}
	options := badger.DefaultIteratorOptions
	options.Prefix = prefix
	options.PrefetchValues = false
	it := r.txn.NewIterator(options)
	defer it.Close()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		if err := r.meter.charge(1); err != nil {
			return err
		}
		item := it.Item()
		value, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if err := visit(item.Key(), value); err != nil {
			return err
		}
	}
	return nil
}

func (r *request) header(decision string) (decisionHeader, error) {
	if header, ok := r.headers[decision]; ok {
		return header, nil
	}
	raw, found, err := r.get(headerKey(decision))
	if err != nil {
		return decisionHeader{}, err
	}
	if !found {
		return decisionHeader{}, fmt.Errorf("missing header for decision %s", decision)
	}
	header, err := decodeHeader(raw)
	if err != nil {
		return decisionHeader{}, err
	}
	r.headers[decision] = header
	return header, nil
}

// effective reports whether a memory has an effective membership.
func (r *request) effective(memory string) (bool, error) {
	found := false
	prefix := memberPrefix(memory)
	err := r.scan(prefix, func(key, _ []byte) error {
		header, err := r.header(string(bytes.TrimPrefix(key, prefix)))
		if err != nil {
			return err
		}
		if header.Status <= statusDepthExhausted {
			found = true
		}
		return nil
	})
	return found, err
}

// provenance reads the signer list, from the v30 marker when one exists and
// through the pre-app-v30 fallback otherwise.
func (r *request) provenance(memory string) error {
	raw, found, err := r.get(memProvKey(memory))
	if err != nil {
		return err
	}
	if found {
		credentials, decodeErr := decodeProvenanceCredentials(raw)
		if decodeErr != nil {
			return decodeErr
		}
		return r.meter.charge(int64(len(credentials)))
	}
	for _, key := range [][]byte{authorPrincipalKey(memory), adoptionKey(memory), migratedKey(memory), authorKey(memory)} {
		if _, _, err = r.get(key); err != nil {
			return err
		}
	}
	raw, found, err = r.get(coauthorsKey(memory))
	if err != nil || !found {
		return err
	}
	for start := 0; start+credentialLen <= len(raw); start += credentialLen {
		if err := r.meter.charge(1); err != nil {
			return err
		}
		if _, _, err := r.get(rootMarkerKey(string(raw[start : start+credentialLen]))); err != nil {
			return err
		}
	}
	return nil
}

// verdict walks up to maxServingWalk ancestors, charging provenance at every
// node visited. Memoized nodes cost nothing further.
func (r *request) verdict(memory string) (bool, error) {
	if verdict, ok := r.verdicts[memory]; ok {
		return verdict, nil
	}
	pending := []string{memory}
	seen := map[string]bool{memory: true}
	held := false
	for visited := 0; len(pending) > 0 && visited < maxServingWalk; visited++ {
		node := pending[0]
		pending = pending[1:]
		tainted, err := r.effective(node)
		if err != nil {
			return false, err
		}
		if err = r.provenance(node); err != nil {
			return false, err
		}
		if tainted {
			held = true
			break
		}
		prefix := lineagePrefix(node)
		err = r.scan(prefix, func(key, _ []byte) error {
			parent := string(bytes.TrimPrefix(key, prefix))
			if !seen[parent] {
				seen[parent] = true
				pending = append(pending, parent)
			}
			return nil
		})
		if err != nil {
			return false, err
		}
	}
	r.verdicts[memory] = held
	return held, nil
}

// frontier reads every queued node of a decision.
func (r *request) frontier(decision string) error {
	if _, err := r.header(decision); err != nil {
		return err
	}
	return r.scan(queuePrefix(decision), func(_, value []byte) error {
		_, _, err := decodeQueueEntry(value)
		return err
	})
}
