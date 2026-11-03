package store

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

const taintTargetVersion byte = 1

const maxTaintTargetBytes = 1 + 2 + tx.MaxDerivedFromChainIDBytes +
	2 + tx.MaxDerivedFromMemoryIDBytes + 2*sha256.Size

type taintTargetAnchor struct {
	Target      tx.ParentRef
	PayloadHash [sha256.Size]byte
}

// The target is immutable decision history, not the mutable expansion cursor.
// Its authority comes from the validated Apply and its atomic consensus write.
func encodeTaintTargetAnchor(anchor taintTargetAnchor) ([]byte, error) {
	target, err := tx.EncodeParentRefValue(anchor.Target)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid target anchor: %v", ErrTaintStateMalformed, err)
	}
	encoded := make([]byte, 1, 1+len(target)+sha256.Size)
	encoded[0] = taintTargetVersion
	encoded = append(encoded, target...)
	return append(encoded, anchor.PayloadHash[:]...), nil
}

func decodeTaintTargetAnchor(raw []byte) (taintTargetAnchor, error) {
	var anchor taintTargetAnchor
	if len(raw) < 1+sha256.Size || len(raw) > maxTaintTargetBytes || raw[0] != taintTargetVersion {
		return anchor, fmt.Errorf("%w: invalid target anchor version or size", ErrTaintStateMalformed)
	}
	target, err := tx.DecodeParentRefValue(raw[1 : len(raw)-sha256.Size])
	if err != nil {
		return anchor, fmt.Errorf("%w: invalid target anchor: %v", ErrTaintStateMalformed, err)
	}
	anchor.Target = target
	copy(anchor.PayloadHash[:], raw[len(raw)-sha256.Size:])
	canonical, err := encodeTaintTargetAnchor(anchor)
	if err != nil || !bytes.Equal(canonical, raw) {
		return taintTargetAnchor{}, fmt.Errorf("%w: noncanonical target anchor", ErrTaintStateMalformed)
	}
	return anchor, nil
}

func readTaintTargetTxn(txn *badger.Txn, decisionID string) (taintTargetAnchor, bool, error) {
	raw, found, err := readTaintValueTxn(txn, taint.TargetKey(decisionID))
	if err != nil || !found {
		return taintTargetAnchor{}, found, err
	}
	anchor, err := decodeTaintTargetAnchor(raw)
	return anchor, true, err
}

func validateTaintTargetTxn(txn *badger.Txn, decisionID string, header taint.Header) error {
	anchor, found, err := readTaintTargetTxn(txn, decisionID)
	if err != nil {
		return err
	}
	if !found || anchor.PayloadHash != header.PayloadHash {
		return fmt.Errorf("%w: missing or inconsistent target anchor", ErrTaintStateMalformed)
	}
	if !header.Status.Active() {
		return nil
	}
	raw, found, err := readTaintValueTxn(txn, taint.MembershipKey(anchor.Target.MemoryID, decisionID))
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: active target membership is missing", ErrTaintStateMalformed)
	}
	membership, err := taint.DecodeMembership(raw)
	if err != nil || !header.MembershipEffective(membership) {
		return fmt.Errorf("%w: active target membership is invalid", ErrTaintStateMalformed)
	}
	index, found, err := readTaintValueTxn(txn, taint.DecisionIndexKey(decisionID, anchor.Target.MemoryID))
	if err != nil {
		return err
	}
	if !found || len(index) != 0 {
		return fmt.Errorf("%w: active target index is missing or nonempty", ErrTaintStateMalformed)
	}
	return nil
}
