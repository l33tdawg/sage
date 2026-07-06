package governance

import "sort"

// ThresholdFor returns the (numerator, denominator) supermajority required
// to pass a proposal of the given operation type.
//
// Default: 2/3 — matches the historical CometBFT BFT supermajority used for
// validator-set changes.
//
// OpDomainReassign: 3/4 — domain ownership recovery is a power-redirecting
// primitive, so we require a stricter quorum than validator-set changes.
//
// The rejection threshold is the complement, derived deterministically as
// (den - num)/den. For 2/3 → reject when rejectPower > 1/3 totalPower; for
// 3/4 → reject when rejectPower > 1/4 totalPower.
func ThresholdFor(op ProposalOp) (num, den int64) {
	switch op {
	case OpDomainReassign:
		return 3, 4
	default:
		// Default 2/3, incl. OpMemoryDomainRepair (app-v16). ThresholdFor is
		// fork-UNAWARE, so it must not special-case a new op number: a pre-fork
		// op==6 proposal (createable via the generic gov path before app-v16) is
		// evaluated here during replay, and changing its threshold retroactively
		// would diverge historical quorum outcomes / the AppHash. The repair op's
		// safety comes from admin-propose + a supermajority + the idempotent,
		// existence-and-registered-domain-guarded apply — not a stricter quorum.
		return 2, 3
	}
}

// CheckGovQuorum checks if a governance proposal has reached quorum.
// Uses pure integer arithmetic for cross-platform determinism.
//
// Passed:   acceptPower * den >= totalPower * num   (num/den threshold)
// Rejected: rejectPower * den >  totalPower * (den - num)
//
// Parameters:
//   - votes: map[validatorID]decision ("accept"/"reject"/"abstain")
//   - powers: map[validatorID]int64 voting power
//
// totalPower is the sum of ALL validators' power (voted or not).
// Validators who haven't voted don't count toward accept/reject but DO count toward total.
//
// Returns passed, rejected, acceptPower, rejectPower, totalPower.
//
// Backward compatibility: callers that omit an op (legacy 2-arg form) get
// the default 2/3 threshold. The op-aware form CheckGovQuorumOp is used by
// the engine which knows the proposal's operation.
func CheckGovQuorum(votes map[string]string, powers map[string]int64) (passed bool, rejected bool, acceptPower, rejectPower, totalPower int64) {
	return CheckGovQuorumOp(votes, powers, OpAddValidator)
}

// CheckGovQuorumOp is the op-aware quorum check used by the governance
// engine. The op selects the threshold via ThresholdFor.
func CheckGovQuorumOp(votes map[string]string, powers map[string]int64, op ProposalOp) (passed bool, rejected bool, acceptPower, rejectPower, totalPower int64) {
	num, den := ThresholdFor(op)

	// Sort validator IDs for deterministic iteration.
	ids := make([]string, 0, len(powers))
	for id := range powers {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		p := powers[id]
		totalPower += p

		decision, voted := votes[id]
		if !voted {
			continue
		}
		switch decision {
		case "accept":
			acceptPower += p
		case "reject":
			rejectPower += p
			// "abstain" and anything else: counted in total but not accept/reject
		}
	}

	if totalPower == 0 {
		return false, false, 0, 0, 0
	}

	// Integer-only threshold math. acceptPower * den >= totalPower * num
	// <=> acceptPower/totalPower >= num/den.
	passed = acceptPower*den >= totalPower*num

	// Rejection threshold is the complement of the acceptance threshold:
	// rejectPower * den > totalPower * (den - num)
	// e.g. 2/3 accept → reject when rejectPower*3 > totalPower*1;
	//      3/4 accept → reject when rejectPower*4 > totalPower*1.
	rejected = rejectPower*den > totalPower*(den-num)

	return passed, rejected, acceptPower, rejectPower, totalPower
}
