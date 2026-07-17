package federation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/tx"
)

// TestJoinStopCannotOverwriteConfirming pins the operator-facing race: once a
// verified confirm atomically owns the session, neither local nor peer Stop may
// return success and overwrite it with ABORTED while activation continues.
func TestJoinStopCannotOverwriteConfirming(t *testing.T) {
	node := newCeremonyNode(t, "host-stop")
	joins, sessionID, certSPKI, attestation, guestKey, _ := approvedSession(t)
	node.mgr.joins = joins
	_, err := joins.CheckConfirm(sessionID, certSPKI,
		SignEnroll(guestKey, attestation, false),
		SignEnroll(guestKey, attestation, true), time.Now())
	if err != nil {
		t.Fatalf("CheckConfirm: %v", err)
	}
	if abortErr := node.mgr.HostAbort(sessionID); !errors.Is(abortErr, ErrJoinAbortConflict) {
		t.Fatalf("HostAbort after confirm = %v, want conflict", abortErr)
	}
	body, marshalErr := json.Marshal(JoinAbortWire{SessionID: sessionID})
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	req := httptest.NewRequest(http.MethodPost, "/fed/v1/join/abort", bytes.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), joinCertSPKIKey{}, certSPKI))
	rr := httptest.NewRecorder()
	node.mgr.handleJoinAbort(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("bound guest Abort HTTP status=%d body=%s, want 409", rr.Code, rr.Body.String())
	}
	view, err := node.mgr.HostSessionStatus(sessionID)
	if err != nil || view.State != JoinConfirming {
		t.Fatalf("state after rejected Stops = %q err=%v, want %s", view.State, err, JoinConfirming)
	}
	if activeErr := joins.MarkActive(sessionID); activeErr != nil {
		t.Fatalf("MarkActive: %v", activeErr)
	}
	view, err = node.mgr.HostSessionStatus(sessionID)
	if err != nil || !view.Active || view.State != JoinActive {
		t.Fatalf("final state = %+v err=%v", view, err)
	}
}

func TestHostConfirmPreActivationFailuresLeaveRestartableTerminalSession(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, node *ceremonyNode, joins *JoinStore, sessionID string)
	}{
		{
			name: "prepare sync control",
			setup: func(t *testing.T, node *ceremonyNode, _ *JoinStore, _ string) {
				t.Helper()
				if err := node.mgr.syncStore().Close(); err != nil {
					t.Fatalf("close SQLite failure seam: %v", err)
				}
			},
		},
		{
			name: "missing P2P persistence",
			setup: func(t *testing.T, _ *ceremonyNode, joins *JoinStore, sessionID string) {
				t.Helper()
				joins.mu.Lock()
				joins.sessions[sessionID].ExpectedGuestP2P = []string{"/ip4/203.0.113.10/tcp/4001"}
				joins.mu.Unlock()
			},
		},
		{
			name: "tx33 broadcast",
			setup: func(_ *testing.T, node *ceremonyNode, _ *JoinStore, _ string) {
				node.mgr.broadcastFn = func([]byte) (string, int64, error) {
					return "", 0, errors.New("forced tx33 failure")
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := newCeremonyNode(t, "host-xxxxx")
			joins, sessionID, certSPKI, attestation, guestKey, _ := approvedSession(t)
			node.mgr.joins = joins
			tc.setup(t, node, joins, sessionID)
			_, _, err := node.mgr.hostConfirm(sessionID, certSPKI,
				SignEnroll(guestKey, attestation, false),
				SignEnroll(guestKey, attestation, true), "")
			if err == nil {
				t.Fatal("hostConfirm unexpectedly succeeded")
			}
			view, statusErr := node.mgr.HostSessionStatus(sessionID)
			if statusErr != nil || view.State != JoinAborted || view.Active {
				t.Fatalf("failed confirm state=%+v err=%v, want ABORTED", view, statusErr)
			}
			if stopErr := node.mgr.HostAbort(sessionID); stopErr != nil {
				t.Fatalf("Stop after failed confirm must be honest and idempotent: %v", stopErr)
			}
		})
	}
}

// TestHostConfirmAgreementLeaseSpansLocalActivation pins the gap that used to
// exist after JOIN's tx-33 commit: revocation must not commit and purge while
// hostConfirm is still promoting the CA and installing seed/control/RBAC state.
func TestHostConfirmAgreementLeaseSpansLocalActivation(t *testing.T) {
	node := newCeremonyNode(t, "host-xxxxx")
	joins, sessionID, certSPKI, attestation, guestKey, _ := approvedSession(t)
	node.mgr.joins = joins

	commitEntered := make(chan struct{})
	commitRelease := make(chan struct{})
	var releaseOnce sync.Once
	releaseCommit := func() { releaseOnce.Do(func() { close(commitRelease) }) }
	defer releaseCommit()

	// Replace the approved session's staged-CA promotion with a deterministic
	// activation barrier. CheckConfirm transfers this exact closure to the
	// hostConfirm driver before the test blocks here.
	joins.mu.Lock()
	joins.sessions[sessionID].commitGuestCA = func() error {
		close(commitEntered)
		<-commitRelease
		return nil
	}
	joins.mu.Unlock()

	setCommitted := make(chan struct{})
	revokeCommitted := make(chan struct{})
	var setOnce, revokeOnce sync.Once
	node.mgr.broadcastFn = func(encoded []byte) (string, int64, error) {
		parsed, err := tx.DecodeTx(encoded)
		if err != nil {
			return "", 0, err
		}
		switch parsed.Type {
		case tx.TxTypeCrossFedSet:
			terms := parsed.CrossFedTerms
			if err := node.mgr.badger.SetCrossFed(terms.RemoteChainID, terms.Endpoint, terms.PeerPubKey,
				uint8(terms.MaxClearance), terms.ExpiresAt, terms.AllowedDomains, terms.AllowedDepts, terms.Status); err != nil {
				return "", 0, err
			}
			setOnce.Do(func() { close(setCommitted) })
			return "set-tx", 1, nil
		case tx.TxTypeCrossFedRevoke:
			if err := node.mgr.badger.UpdateCrossFedStatus(parsed.CrossFedRevoke.RemoteChainID, "revoked"); err != nil {
				return "", 0, err
			}
			revokeOnce.Do(func() { close(revokeCommitted) })
			return "revoke-tx", 2, nil
		default:
			return "other-tx", 1, nil
		}
	}

	type hostResult struct {
		hash string
		err  error
	}
	var hostRes hostResult
	hostDone := make(chan struct{})
	go func() {
		hostRes.hash, _, hostRes.err = node.mgr.hostConfirm(sessionID, certSPKI,
			SignEnroll(guestKey, attestation, false),
			SignEnroll(guestKey, attestation, true), "")
		close(hostDone)
	}()

	waitClosed := func(ch <-chan struct{}, label string) bool {
		t.Helper()
		select {
		case <-ch:
			return true
		case <-time.After(5 * time.Second):
			t.Errorf("timed out waiting for %s", label)
			return false
		}
	}
	if !waitClosed(setCommitted, "JOIN tx-33 commit") || !waitClosed(commitEntered, "JOIN CA promotion barrier") {
		releaseCommit()
		waitClosed(hostDone, "hostConfirm cleanup")
		return
	}

	// TryLock is the deterministic assertion: unlike a timing-only absence
	// check, it proves hostConfirm still owns the exact mutex RevokeAgreement
	// must acquire while the post-tx activation artifact is blocked.
	leaseHeld := !node.mgr.agreementMutationMu.TryLock()
	if !leaseHeld {
		node.mgr.agreementMutationMu.Unlock()
	}

	type revokeResult struct {
		hash string
		err  error
	}
	var revokeRes revokeResult
	revokeStarted := make(chan struct{})
	revokeDone := make(chan struct{})
	go func() {
		close(revokeStarted)
		revokeRes.hash, revokeRes.err = node.mgr.RevokeAgreement("guest-yyyyy")
		close(revokeDone)
	}()
	<-revokeStarted
	prematureRevoke := false
	select {
	case <-revokeCommitted:
		prematureRevoke = true
	default:
	}

	releaseCommit()
	if !waitClosed(hostDone, "host activation completion") || !waitClosed(revokeDone, "queued revoke completion") {
		return
	}

	if !leaseHeld {
		t.Error("hostConfirm released agreement mutation lease before CA/seed/control activation completed")
	}
	if prematureRevoke {
		t.Error("revoke tx committed while JOIN local activation was still blocked")
	}
	if hostRes.err != nil || hostRes.hash != "set-tx" {
		t.Fatalf("hostConfirm hash=%q err=%v", hostRes.hash, hostRes.err)
	}
	if revokeRes.err != nil || revokeRes.hash != "revoke-tx" {
		t.Fatalf("RevokeAgreement hash=%q err=%v", revokeRes.hash, revokeRes.err)
	}
	if _, _, _, _, _, _, status, err := node.mgr.badger.GetCrossFed("guest-yyyyy"); err != nil || status != "revoked" {
		t.Fatalf("final agreement status=%q err=%v, want revoked", status, err)
	}
	control, err := node.mgr.syncStore().GetSyncControl(context.Background(), "guest-yyyyy")
	if err != nil || control != nil {
		t.Fatalf("revoke did not purge JOIN sync control: control=%+v err=%v", control, err)
	}
	if node.mgr.seedEstablished("guest-yyyyy") {
		t.Fatal("revoke did not purge the JOIN seed committed by the preceding generation")
	}
}
