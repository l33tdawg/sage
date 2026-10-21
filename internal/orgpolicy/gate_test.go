package orgpolicy

import "testing"

func TestGate_PerOperationContract(t *testing.T) {
	const now = int64(5000)
	current := AcceptedState{OrgID: "o", Revision: 3, NotBefore: 1000, Expiry: 9000}
	expired := AcceptedState{OrgID: "o", Revision: 3, NotBefore: 1000, Expiry: 4000}
	notYet := AcceptedState{OrgID: "o", Revision: 3, NotBefore: 6000, Expiry: 9000}
	noExpiry := AcceptedState{OrgID: "o", Revision: 3}
	none := AcceptedState{}

	cases := []struct {
		name    string
		st      AcceptedState
		op      Operation
		owner   bool
		allowed bool
		mode    GateMode
	}{
		// Local owner access never depends on policy, even with none at all.
		{"local/none", none, OpLocalAccess, false, true, ModeIndependent},
		{"local/expired", expired, OpLocalAccess, false, true, ModeIndependent},

		// Authority-granting: hold on stale.
		{"enroll/current", current, OpEnrollment, false, true, ModeCurrent},
		{"enroll/expired", expired, OpEnrollment, false, false, ModeHold},
		{"enroll/not-yet-valid", notYet, OpEnrollment, false, false, ModeHold},
		{"peer/current", current, OpNewPeering, false, true, ModeCurrent},
		{"peer/expired", expired, OpNewPeering, false, false, ModeHold},
		{"peer/expired-owner-present", expired, OpNewPeering, true, false, ModeHold},

		// Steady state: continue on last-valid.
		{"admission/current", current, OpAdmission, false, true, ModeCurrent},
		{"admission/expired", expired, OpAdmission, false, true, ModeLastValid},
		{"retention/expired", expired, OpRetention, false, true, ModeLastValid},

		// Destructive: hold on stale unless the owner is present.
		{"purge/current", current, OpRetentionPurge, false, true, ModeCurrent},
		{"purge/expired", expired, OpRetentionPurge, false, false, ModeHold},
		{"purge/expired-owner", expired, OpRetentionPurge, true, true, ModeOwnerPresent},

		// Expiry 0 never expires.
		{"peer/no-expiry", noExpiry, OpNewPeering, false, true, ModeCurrent},

		// Fail closed: no policy, or an unknown operation.
		{"admission/none", none, OpAdmission, false, false, ModeHold},
		{"unknown", current, Operation("teleport"), true, false, ModeHold},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Gate(tc.st, tc.op, now, tc.owner)
			if got.Allowed != tc.allowed || got.Mode != tc.mode || got.Operation != tc.op {
				t.Fatalf("Gate(%s) = %+v, want allowed=%v mode=%s", tc.op, got, tc.allowed, tc.mode)
			}
			if !got.Allowed && got.Reason == "" {
				t.Fatalf("a held operation must say why: %+v", got)
			}
		})
	}
}

func TestGate_ExpiryBoundaryIsExclusive(t *testing.T) {
	st := AcceptedState{Revision: 1, NotBefore: 100, Expiry: 200}
	if !st.CurrentAt(100) || !st.CurrentAt(199) {
		t.Fatal("window must include NotBefore and every second before Expiry")
	}
	if st.CurrentAt(99) || st.CurrentAt(200) {
		t.Fatal("window must exclude the second before NotBefore and Expiry itself")
	}
}

func TestGate_EveryListedOperationIsHandled(t *testing.T) {
	st := AcceptedState{Revision: 1}
	for _, op := range Operations {
		if got := Gate(st, op, 0, false); got.Reason == "unknown operation" {
			t.Fatalf("listed operation %s falls through to unknown", op)
		}
	}
}
