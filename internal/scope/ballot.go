package scope

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
)

// BallotState is the immutable-roster proposal lifecycle. The normal memory
// status remains the public lifecycle source; this state lets recovery verify
// that the pinned scope record reached the same terminal verdict.
type BallotState byte

const (
	BallotPending BallotState = iota + 1
	BallotCommitted
	BallotDeprecated
)

// BallotMember is one active member copied from the accepted scope revision.
// EffectiveWeight is the final integer weight for this ballot and never changes.
type BallotMember struct {
	ValidatorID     string
	EffectiveWeight uint64
}

// Ballot is the pinned denominator for one scoped memory proposal.
type Ballot struct {
	MemoryID        string
	ScopeID         string
	ScopeRevision   uint64
	SubmittedHeight int64
	State           BallotState
	Members         []BallotMember
	TotalWeight     uint64
}

func EncodeBallot(ballot Ballot) ([]byte, error) {
	if err := ValidateBallot(ballot); err != nil {
		return nil, err
	}
	buf := []byte{SchemaV1}
	buf = appendString(buf, ballot.MemoryID)
	buf = appendString(buf, ballot.ScopeID)
	buf = appendUint64(buf, ballot.ScopeRevision)
	buf = appendInt64(buf, ballot.SubmittedHeight)
	buf = append(buf, byte(ballot.State))
	buf = appendUint32(buf, uint32(len(ballot.Members)))
	for _, member := range ballot.Members {
		buf = appendString(buf, member.ValidatorID)
		buf = appendUint64(buf, member.EffectiveWeight)
	}
	buf = appendUint64(buf, ballot.TotalWeight)
	return buf, nil
}

func DecodeBallot(data []byte) (Ballot, error) {
	if len(data) == 0 || data[0] != SchemaV1 {
		return Ballot{}, errors.New("unsupported or empty scope ballot schema")
	}
	off := 1
	var ballot Ballot
	var err error
	if ballot.MemoryID, off, err = readString(data, off, maxIDBytes); err != nil {
		return Ballot{}, fmt.Errorf("memory id: %w", err)
	}
	if ballot.ScopeID, off, err = readString(data, off, maxIDBytes); err != nil {
		return Ballot{}, fmt.Errorf("scope id: %w", err)
	}
	if ballot.ScopeRevision, off, err = readUint64(data, off); err != nil {
		return Ballot{}, fmt.Errorf("scope revision: %w", err)
	}
	if ballot.SubmittedHeight, off, err = readInt64(data, off); err != nil {
		return Ballot{}, fmt.Errorf("submitted height: %w", err)
	}
	if off >= len(data) {
		return Ballot{}, errors.New("ballot state: truncated")
	}
	ballot.State = BallotState(data[off])
	off++
	count, next, err := readUint32(data, off)
	if err != nil {
		return Ballot{}, fmt.Errorf("member count: %w", err)
	}
	off = next
	if count == 0 || count > maxMembers {
		return Ballot{}, fmt.Errorf("member count must be 1..%d", maxMembers)
	}
	ballot.Members = make([]BallotMember, 0, int(count))
	for i := uint32(0); i < count; i++ {
		var member BallotMember
		if member.ValidatorID, off, err = readString(data, off, maxIDBytes); err != nil {
			return Ballot{}, fmt.Errorf("member %d id: %w", i, err)
		}
		if member.EffectiveWeight, off, err = readUint64(data, off); err != nil {
			return Ballot{}, fmt.Errorf("member %d weight: %w", i, err)
		}
		ballot.Members = append(ballot.Members, member)
	}
	if ballot.TotalWeight, off, err = readUint64(data, off); err != nil {
		return Ballot{}, fmt.Errorf("total weight: %w", err)
	}
	if off != len(data) {
		return Ballot{}, errors.New("scope ballot has trailing bytes")
	}
	if err := ValidateBallot(ballot); err != nil {
		return Ballot{}, err
	}
	return ballot, nil
}

func ValidateBallot(ballot Ballot) error {
	if err := validateID("memory id", ballot.MemoryID); err != nil {
		return err
	}
	if err := validateID("scope id", ballot.ScopeID); err != nil {
		return err
	}
	if ballot.ScopeRevision == 0 || ballot.SubmittedHeight <= 0 {
		return errors.New("scope revision and submitted height must be positive")
	}
	if ballot.State != BallotPending && ballot.State != BallotCommitted && ballot.State != BallotDeprecated {
		return fmt.Errorf("invalid ballot state %d", ballot.State)
	}
	if len(ballot.Members) == 0 || len(ballot.Members) > maxMembers {
		return fmt.Errorf("member count must be 1..%d", maxMembers)
	}
	previous := ""
	var total uint64
	for i, member := range ballot.Members {
		if err := validateID("ballot member", member.ValidatorID); err != nil {
			return fmt.Errorf("member %d: %w", i, err)
		}
		if i > 0 && member.ValidatorID <= previous {
			return errors.New("ballot members must be strictly bytewise sorted")
		}
		if member.EffectiveWeight == 0 {
			return fmt.Errorf("member %d has zero effective weight", i)
		}
		if math.MaxUint64-total < member.EffectiveWeight {
			return errors.New("effective weight total overflows uint64")
		}
		total += member.EffectiveWeight
		previous = member.ValidatorID
	}
	if ballot.TotalWeight == 0 || ballot.TotalWeight != total {
		return fmt.Errorf("total weight %d does not equal member sum %d", ballot.TotalWeight, total)
	}
	return nil
}

// HasStrictSupermajority evaluates accept/total > 2/3 without overflow.
func HasStrictSupermajority(accept, total uint64) bool {
	if total == 0 || accept > total {
		return false
	}
	acceptHi, acceptLo := bits.Mul64(accept, 3)
	totalHi, totalLo := bits.Mul64(total, 2)
	return acceptHi > totalHi || (acceptHi == totalHi && acceptLo > totalLo)
}
