package federation

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"
)

// drive a store session up to HOST_APPROVED and return everything a confirm
// needs (the frozen E is recomputable from the returned snapshot).
func approvedSession(t *testing.T) (st *JoinStore, id string, certSPKI []byte, e [32]byte, guestPriv ed25519.PrivateKey, rolledBack *bool) {
	t.Helper()
	st = NewJoinStore()
	seed := randN(20)
	hostPin, guestPin := randN(32), randN(32)
	certSPKI = randN(32)
	guestPub, gp, _ := ed25519.GenerateKey(rand.Reader)
	guestPriv = gp
	now := time.Now()

	js, err := st.Create("host-xxxxx", hostPin, "https://host:8444", seed, now)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	id = js.ID
	if setErr := st.SetExpectedGuest(id, guestPin, "https://guest:8444", now); setErr != nil {
		t.Fatalf("SetExpectedGuest: %v", setErr)
	}
	rb := false
	rolledBack = &rb
	scopeG := trustOnlyJoinScope
	bound, err := st.Request(id, now, GuestRequestInput{
		GuestChain:      "guest-yyyyy",
		GuestAgentPub:   guestPub,
		GuestNonce:      randN(16),
		GuestPin:        guestPin,
		GuestEndpoint:   "https://guest:8444",
		GuestScope:      scopeG.digest(),
		CertSPKI:        certSPKI,
		CommitGuestCA:   func() error { return nil },
		RollbackGuestCA: func() { rb = true },
	})
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	grant := trustOnlyJoinScope
	codeG, _ := bound.confirmCodes()
	locked, err := st.ApproveWithCode(id, codeG, HostGrant{
		Clearance: uint8(trustOnlyJoinScope.MaxClearance), Domains: nil,
		Mode: trustOnlyJoinScope.Mode, Direction: trustOnlyJoinScope.Direction, Scope: grant.digest(),
	})
	if err != nil || locked {
		t.Fatalf("ApproveWithCode: err=%v locked=%v", err, locked)
	}
	// The frozen E: attestation over the bound guest fields + the host grant scope.
	e = bound.attestation(grant.digest())
	return
}

// TestCheckConfirmBadSigRetryable: a bad-signature confirm must NOT brick the
// ceremony - it stays HOST_APPROVED (retryable) and does not consume the staged
// CA; a subsequent good-signature confirm succeeds and moves to CONFIRMING.
func TestCheckConfirmBadSigRetryable(t *testing.T) {
	st, id, certSPKI, e, guestPriv, rolledBack := approvedSession(t)
	now := time.Now()

	if _, err := st.CheckConfirm(id, certSPKI, randN(64), randN(64), now); err == nil {
		t.Fatal("CheckConfirm accepted bogus signatures")
	}
	if v, _ := st.Get(id, now); v.State != JoinHostApproved {
		t.Fatalf("state after bad-sig confirm = %s, want HOST_APPROVED (retryable)", v.State)
	}
	if *rolledBack {
		t.Fatal("a bad-sig confirm rolled back the staged CA (should stay pending for retry)")
	}

	goodSig := SignEnroll(guestPriv, e, false)
	goodAck := SignEnroll(guestPriv, e, true)
	if _, err := st.CheckConfirm(id, certSPKI, goodSig, goodAck, now); err != nil {
		t.Fatalf("CheckConfirm rejected valid signatures: %v", err)
	}
	if v, _ := st.Get(id, now); v.State != JoinConfirming {
		t.Fatalf("state after good-sig confirm = %s, want CONFIRMING", v.State)
	}
	// A SECOND confirm now finds CONFIRMING (not HOST_APPROVED) and is rejected -
	// the host broadcasts its tx-33 exactly once.
	if _, err := st.CheckConfirm(id, certSPKI, goodSig, goodAck, now); err == nil {
		t.Fatal("second confirm accepted - tx-33 would broadcast twice")
	}
}

// TestCheckConfirmWrongCertRejected: RT-2/RT-5 - a confirm from a different TLS
// client cert than the one bound at request is rejected.
func TestCheckConfirmWrongCertRejected(t *testing.T) {
	st, id, _, e, guestPriv, _ := approvedSession(t)
	now := time.Now()
	sig := SignEnroll(guestPriv, e, false)
	ack := SignEnroll(guestPriv, e, true)
	if _, err := st.CheckConfirm(id, randN(32), sig, ack, now); err == nil {
		t.Fatal("CheckConfirm accepted an unbound client certificate")
	}
}

// TestCleanupProtectsConfirming: a CONFIRMING session (staged CA being committed
// by the driver) is NOT reaped at the moment it passes ExpiresAt - only after
// the TTL grace - so the reaper cannot roll back a CA mid-commit.
func TestCleanupProtectsConfirming(t *testing.T) {
	st, id, certSPKI, e, guestPriv, rolledBack := approvedSession(t)
	now := time.Now()
	if _, err := st.CheckConfirm(id, certSPKI, SignEnroll(guestPriv, e, false), SignEnroll(guestPriv, e, true), now); err != nil {
		t.Fatalf("CheckConfirm: %v", err)
	}

	// Just past ExpiresAt: a CONFIRMING session survives, closures untouched.
	st.Cleanup(now.Add(joinSessionTTL + time.Minute))
	if _, ok := st.Get(id, now); !ok {
		t.Fatal("CONFIRMING session reaped at ExpiresAt (would race the in-flight commit)")
	}
	if *rolledBack {
		t.Fatal("reaper rolled back a CONFIRMING session's staged CA")
	}

	// After the grace window it is finally reaped (still no rollback: the driver
	// owns the closures, which CheckConfirm nil'd on the live session).
	st.Cleanup(now.Add(2*joinSessionTTL + time.Minute))
	if _, ok := st.Get(id, now); ok {
		t.Fatal("CONFIRMING session not reaped after the grace window")
	}
	if *rolledBack {
		t.Fatal("reaper rolled back the driver-owned staged CA")
	}
}

// TestAbandonedRequestReapsStagedCA: a session that lapses after /join/request
// (never approved, never aborted) has its staged CA rolled back by the reaper.
func TestAbandonedRequestReapsStagedCA(t *testing.T) {
	st := NewJoinStore()
	now := time.Now()
	js, _ := st.Create("host-zzzzz", randN(32), "https://host:8444", randN(20), now)
	_ = st.SetExpectedGuest(js.ID, make([]byte, 32), "https://guest:8444", now)
	guestPub, _, _ := ed25519.GenerateKey(rand.Reader)
	rb := false
	_, err := st.Request(js.ID, now, GuestRequestInput{
		GuestChain: "guest-wwwww", GuestAgentPub: guestPub, GuestNonce: randN(16),
		GuestPin: make([]byte, 32), GuestEndpoint: "https://guest:8444",
		CertSPKI: randN(32), CommitGuestCA: func() error { return nil }, RollbackGuestCA: func() { rb = true },
	})
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	st.Cleanup(now.Add(joinSessionTTL + time.Minute)) // past ExpiresAt, never approved
	if !rb {
		t.Fatal("abandoned request's staged CA was not rolled back by the reaper")
	}
	if _, ok := st.Get(js.ID, now); ok {
		t.Fatal("abandoned session not reaped")
	}
}
