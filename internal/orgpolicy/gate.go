package orgpolicy

// Per-operation offline/expiry behavior (RFC #443 point 4).
//
// Losing the management service must never implicitly lock or purge an
// existing brain. Freshness of the accepted org policy is therefore judged per
// operation class, not globally:
//
//   - local owner access   — always allowed; independent of policy freshness.
//   - enrollment           — needs a currently valid policy (and a live token,
//     enforced by the enroll endpoint); never offline.
//   - new peering          — authority-granting: an expired policy must not
//     authorize a NEW peering → hold.
//   - admission            — continue on the last valid policy; gating does not
//     stop because the center is unreachable.
//   - retention            — continue on the last valid policy.
//   - retention purge      — destructive: must not fire on an expired policy
//     without owner presence → hold.
//
// In short: new authority-granting or destructive operations hold on expiry;
// steady-state access and admission continue on last-valid. Gate is pure — the
// caller supplies the clock — so the decision is identical in tests and in
// production and never depends on reaching the management service.

// Operation is a class of node operation governed by org policy freshness.
type Operation string

const (
	OpLocalAccess    Operation = "local_access"
	OpEnrollment     Operation = "enrollment"
	OpNewPeering     Operation = "new_peering"
	OpAdmission      Operation = "admission"
	OpRetention      Operation = "retention"
	OpRetentionPurge Operation = "retention_purge"
)

// Operations lists every gated class, in a stable order for status surfaces.
var Operations = []Operation{OpLocalAccess, OpEnrollment, OpNewPeering, OpAdmission, OpRetention, OpRetentionPurge}

// GateMode says on what basis an operation may (or may not) proceed.
type GateMode string

const (
	// ModeCurrent: the accepted policy is within its validity window.
	ModeCurrent GateMode = "current"
	// ModeLastValid: the policy is stale but this operation continues on it.
	ModeLastValid GateMode = "last_valid"
	// ModeIndependent: the operation does not depend on policy freshness.
	ModeIndependent GateMode = "independent"
	// ModeOwnerPresent: a destructive operation proceeds on a stale policy only
	// because the owner is present to authorize it.
	ModeOwnerPresent GateMode = "owner_present"
	// ModeHold: the operation must not proceed until a current policy arrives.
	ModeHold GateMode = "hold"
)

// GateResult is the decision for one operation.
type GateResult struct {
	Operation Operation `json:"operation"`
	Allowed   bool      `json:"allowed"`
	Mode      GateMode  `json:"mode"`
	Reason    string    `json:"reason,omitempty"`
}

// CurrentAt reports whether the accepted policy is within its validity window
// at unix time now. NotBefore 0 means effective immediately; Expiry 0 means it
// never expires. A not-yet-valid policy is not current.
func (st AcceptedState) CurrentAt(now int64) bool {
	if st.NotBefore != 0 && now < st.NotBefore {
		return false
	}
	return st.Expiry == 0 || now < st.Expiry
}

// CurrentAt reports whether the policy is within its validity window at now.
func (p *Policy) CurrentAt(now int64) bool { return stateFrom(p).CurrentAt(now) }

// Gate decides whether op may proceed under the accepted state st at unix time
// now. ownerPresent is the caller's assertion that the node owner is locally
// present and authorizing this specific action; it only matters for
// destructive operations. An unknown operation, or a node holding no policy
// (st.Revision == 0) for an operation that needs one, is held — fail closed.
func Gate(st AcceptedState, op Operation, now int64, ownerPresent bool) GateResult {
	res := GateResult{Operation: op}
	if op == OpLocalAccess {
		res.Allowed, res.Mode = true, ModeIndependent
		return res
	}
	if st.Revision == 0 {
		res.Mode, res.Reason = ModeHold, "no org policy accepted"
		return res
	}
	current := st.CurrentAt(now)
	switch op {
	case OpEnrollment, OpNewPeering:
		if current {
			res.Allowed, res.Mode = true, ModeCurrent
			return res
		}
		res.Mode, res.Reason = ModeHold, "authority-granting operation requires a current org policy"
	case OpAdmission, OpRetention:
		res.Allowed, res.Mode = true, ModeCurrent
		if !current {
			res.Mode = ModeLastValid
		}
	case OpRetentionPurge:
		switch {
		case current:
			res.Allowed, res.Mode = true, ModeCurrent
		case ownerPresent:
			res.Allowed, res.Mode = true, ModeOwnerPresent
		default:
			res.Mode, res.Reason = ModeHold, "destructive operation on a stale org policy requires owner presence"
		}
	default:
		res.Mode, res.Reason = ModeHold, "unknown operation"
	}
	return res
}
