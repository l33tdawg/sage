package governance

import "sort"

// CheckGovQuorum checks if a governance proposal has reached quorum.
// Uses pure integer arithmetic for cross-platform determinism.
//
// Passed:   acceptPower * 3 >= totalPower * 2  (2/3 threshold)
// Rejected: rejectPower * 3 > totalPower        (>1/3 explicitly reject)
//
// Parameters:
//   - votes: map[validatorID]decision ("accept"/"reject"/"abstain")
//   - powers: map[validatorID]int64 voting power
//
// totalPower is the sum of ALL validators' power (voted or not).
// Validators who haven't voted don't count toward accept/reject but DO count toward total.
//
// Returns passed, rejected, acceptPower, rejectPower, totalPower.
func CheckGovQuorum(votes map[string]string, powers map[string]int64) (passed bool, rejected bool, acceptPower, rejectPower, totalPower int64) {
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

	// Integer-only: acceptPower * 3 >= totalPower * 2  <==> acceptPower/totalPower >= 2/3
	passed = acceptPower*3 >= totalPower*2

	// Integer-only: rejectPower * 3 > totalPower  <==> rejectPower/totalPower > 1/3
	rejected = rejectPower*3 > totalPower

	return passed, rejected, acceptPower, rejectPower, totalPower
}
