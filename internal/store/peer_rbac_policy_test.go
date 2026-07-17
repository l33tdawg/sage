package store

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

func testPeerAgentID(t *testing.T) string {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(pub)
}

func testPeerRBACBinding() (string, string) {
	return "epoch-test", strings.Repeat("ab", 32)
}

func testLegacyPeerRBACControl(policy PeerRBACPolicy) SyncControl {
	return SyncControl{
		RemoteChainID: policy.RemoteChainID, Role: "guest",
		ControllerChainID: policy.RemoteChainID, ControllerAgentID: policy.PeerAgentID,
		PeerAgentID: policy.PeerAgentID, PolicyEpoch: policy.PolicyEpoch,
		RemoteCAPin: policy.RemoteCAPin, PolicyVersion: 1,
	}
}

func TestPeerRBACPolicyDistinguishesLegacyFromEmptySnapshot(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)

	legacy, err := s.GetPeerRBACPolicy(ctx, "chain-peer")
	if err != nil || legacy != nil {
		t.Fatalf("legacy policy = %#v, err=%v; want nil,nil", legacy, err)
	}
	peerAgentID := testPeerAgentID(t)
	epoch, caPin := testPeerRBACBinding()
	configured, err := s.ReplacePeerRBACPolicy(ctx, PeerRBACPolicy{
		RemoteChainID: "chain-peer", PeerAgentID: peerAgentID,
		PolicyEpoch: epoch, RemoteCAPin: caPin,
		PolicyVersion: CurrentPeerRBACPolicyVersion, Domains: nil,
	})
	if err != nil {
		t.Fatalf("replace empty policy: %v", err)
	}
	if configured == nil || configured.Domains == nil || len(configured.Domains) != 0 {
		t.Fatalf("configured empty policy collapsed into legacy: %#v", configured)
	}

	loaded, err := s.GetPeerRBACPolicy(ctx, "chain-peer")
	if err != nil || loaded == nil || loaded.Domains == nil || len(loaded.Domains) != 0 {
		t.Fatalf("loaded empty policy = %#v, err=%v", loaded, err)
	}
}

func TestPeerRBACPolicyAtomicallyCutsMatchingSyncControlToV3(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	epoch, caPin := testPeerRBACBinding()
	policy := PeerRBACPolicy{
		RemoteChainID: "chain-cutover", PeerAgentID: testPeerAgentID(t),
		PolicyEpoch: epoch, RemoteCAPin: caPin, PolicyVersion: CurrentPeerRBACPolicyVersion,
		Domains: []PeerRBACDomainPermission{{Domain: "tii", Read: true}},
	}
	if err := s.PrepareSyncControl(ctx, testLegacyPeerRBACControl(policy)); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateSyncControl(ctx, policy.RemoteChainID, policy.PolicyEpoch); err != nil {
		t.Fatal(err)
	}
	if _, err := s.writeExecContext(ctx, `UPDATE sync_control SET remote_policy_version=2 WHERE remote_chain_id=?`, policy.RemoteChainID); err != nil {
		t.Fatal(err)
	}
	if err := s.SetSyncDomains(ctx, policy.RemoteChainID, []string{"legacy.tx33"}); err != nil {
		t.Fatal(err)
	}

	stored, err := s.ReplaceBoundPeerRBACPolicy(ctx, policy)
	if err != nil || stored == nil || len(stored.Domains) != 1 {
		t.Fatalf("bound policy replacement = %#v err=%v", stored, err)
	}
	control, err := s.GetSyncControl(ctx, policy.RemoteChainID)
	if err != nil || control == nil {
		t.Fatalf("sync control = %#v err=%v", control, err)
	}
	if control.PolicyVersion != peerRBACSyncPolicyVersion {
		t.Fatalf("local sync policy version=%d, want v%d", control.PolicyVersion, peerRBACSyncPolicyVersion)
	}
	if control.RemotePolicyVersion != 2 {
		t.Fatalf("cutover synthesized remote policy version: %+v", control)
	}
	if _, err := s.ApplySyncPolicyVersion(ctx, policy.RemoteChainID, policy.PolicyEpoch, 2, 1,
		"legacy-after-peer-rbac", []string{"legacy.tx33"}); err == nil {
		t.Fatal("legacy sync policy reactivated after peer-RBAC commit")
	}
}

func TestBoundPeerRBACPolicyRequiresExactActiveSyncControl(t *testing.T) {
	epoch, caPin := testPeerRBACBinding()
	base := PeerRBACPolicy{
		RemoteChainID: "chain-bound", PeerAgentID: testPeerAgentID(t),
		PolicyEpoch: epoch, RemoteCAPin: caPin, PolicyVersion: CurrentPeerRBACPolicyVersion,
		Domains: []PeerRBACDomainPermission{{Domain: "tii", Read: true}},
	}
	tests := []struct {
		name     string
		prepare  bool
		activate bool
		mutate   func(*SyncControl)
	}{
		{name: "missing control"},
		{name: "pending control", prepare: true},
		{name: "different peer", prepare: true, activate: true, mutate: func(c *SyncControl) {
			c.PeerAgentID = testPeerAgentID(t)
		}},
		{name: "different epoch", prepare: true, activate: true, mutate: func(c *SyncControl) {
			c.PolicyEpoch = "different-epoch"
		}},
		{name: "different CA", prepare: true, activate: true, mutate: func(c *SyncControl) {
			c.RemoteCAPin = strings.Repeat("cd", 32)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			s := newSyncTestStore(t)
			control := testLegacyPeerRBACControl(base)
			if test.mutate != nil {
				test.mutate(&control)
			}
			if test.prepare {
				if err := s.PrepareSyncControl(ctx, control); err != nil {
					t.Fatal(err)
				}
				if test.activate {
					if err := s.ActivateSyncControl(ctx, control.RemoteChainID, control.PolicyEpoch); err != nil {
						t.Fatal(err)
					}
				}
			}

			if _, err := s.ReplaceBoundPeerRBACPolicy(ctx, base); !errors.Is(err, ErrPeerRBACBindingMismatch) {
				t.Fatalf("replacement error=%v, want ErrPeerRBACBindingMismatch", err)
			}
			if stored, err := s.GetPeerRBACPolicy(ctx, base.RemoteChainID); err != nil || stored != nil {
				t.Fatalf("failed bound replacement committed policy=%#v err=%v", stored, err)
			}
			if test.prepare {
				storedControl, err := s.GetSyncControl(ctx, base.RemoteChainID)
				if err != nil || storedControl == nil || storedControl.PolicyVersion != 1 {
					t.Fatalf("failed bound replacement changed control=%+v err=%v", storedControl, err)
				}
			}
		})
	}
}

func TestPeerRBACPolicyAndSyncCutoverRollbackTogether(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	epoch, caPin := testPeerRBACBinding()
	policy := PeerRBACPolicy{
		RemoteChainID: "chain-rollback", PeerAgentID: testPeerAgentID(t),
		PolicyEpoch: epoch, RemoteCAPin: caPin, PolicyVersion: CurrentPeerRBACPolicyVersion,
		Domains: []PeerRBACDomainPermission{{Domain: "old", Read: true}},
	}
	// Seed a standalone snapshot first, modeling the pre-fix crash window.
	if _, err := s.ReplacePeerRBACPolicy(ctx, policy); err != nil {
		t.Fatal(err)
	}
	if err := s.PrepareSyncControl(ctx, testLegacyPeerRBACControl(policy)); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateSyncControl(ctx, policy.RemoteChainID, policy.PolicyEpoch); err != nil {
		t.Fatal(err)
	}
	if _, err := s.writeExecContext(ctx, `CREATE TRIGGER fail_peer_rbac_sync_cutover
		BEFORE UPDATE OF policy_version ON sync_control
		WHEN NEW.remote_chain_id='chain-rollback' AND NEW.policy_version>=3
		BEGIN SELECT RAISE(ABORT, 'forced peer RBAC cutover failure'); END`); err != nil {
		t.Fatal(err)
	}

	replacement := policy
	replacement.Domains = []PeerRBACDomainPermission{{Domain: "new", Copy: true}}
	if _, err := s.ReplaceBoundPeerRBACPolicy(ctx, replacement); err == nil {
		t.Fatal("forced sync cutover failure was ignored")
	}
	stored, err := s.GetPeerRBACPolicy(ctx, policy.RemoteChainID)
	if err != nil || stored == nil || len(stored.Domains) != 1 || stored.Domains[0].Domain != "old" {
		t.Fatalf("policy rows did not roll back with sync marker: %#v err=%v", stored, err)
	}
	control, err := s.GetSyncControl(ctx, policy.RemoteChainID)
	if err != nil || control == nil || control.PolicyVersion != 1 {
		t.Fatalf("failed cutover changed sync control: %+v err=%v", control, err)
	}
}

func TestPeerRBACPolicyAtomicDynamicReplaceAndCanonicalPermissions(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	peerAgentID := testPeerAgentID(t)
	epoch, caPin := testPeerRBACBinding()
	base := PeerRBACPolicy{RemoteChainID: "chain-peer", PeerAgentID: peerAgentID,
		PolicyEpoch: epoch, RemoteCAPin: caPin, PolicyVersion: CurrentPeerRBACPolicyVersion}

	base.Domains = []PeerRBACDomainPermission{
		{Domain: "zeta", Copy: true},
		{Domain: "alpha", Read: true},
	}
	got, err := s.ReplacePeerRBACPolicy(ctx, base)
	if err != nil {
		t.Fatalf("initial replace: %v", err)
	}
	want := []PeerRBACDomainPermission{
		{Domain: "alpha", Read: true},
		{Domain: "zeta", Read: true, Copy: true},
	}
	if len(got.Domains) != len(want) || got.Domains[0] != want[0] || got.Domains[1] != want[1] {
		t.Fatalf("canonical policy = %#v, want %#v", got.Domains, want)
	}

	base.Domains = []PeerRBACDomainPermission{{Domain: "replacement", Read: true}}
	got, err = s.ReplacePeerRBACPolicy(ctx, base)
	if err != nil {
		t.Fatalf("dynamic replace: %v", err)
	}
	if len(got.Domains) != 1 || got.Domains[0].Domain != "replacement" {
		t.Fatalf("full replacement retained stale rows: %#v", got.Domains)
	}

	// Validation runs before the transaction and cannot clobber the last good
	// full snapshot.
	base.Domains = []PeerRBACDomainPermission{{Domain: "dup", Read: true}, {Domain: "dup", Copy: true}}
	if _, replaceErr := s.ReplacePeerRBACPolicy(ctx, base); replaceErr == nil {
		t.Fatal("duplicate domain accepted")
	}
	got, err = s.GetPeerRBACPolicy(ctx, "chain-peer")
	if err != nil || len(got.Domains) != 1 || got.Domains[0].Domain != "replacement" {
		t.Fatalf("failed replacement clobbered prior snapshot: %#v err=%v", got, err)
	}
}

func TestBoundPeerRBACPausePreservesSnapshotAndRequiresExactActiveBinding(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	epoch, caPin := testPeerRBACBinding()
	policy := PeerRBACPolicy{
		RemoteChainID: "chain-pause", PeerAgentID: testPeerAgentID(t),
		PolicyEpoch: epoch, RemoteCAPin: caPin, PolicyVersion: CurrentPeerRBACPolicyVersion,
		Domains: []PeerRBACDomainPermission{{Domain: "tii", Read: true, Copy: true}},
	}
	if err := s.PrepareSyncControl(ctx, testLegacyPeerRBACControl(policy)); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateSyncControl(ctx, policy.RemoteChainID, policy.PolicyEpoch); err != nil {
		t.Fatal(err)
	}
	stored, err := s.ReplaceBoundPeerRBACPolicy(ctx, policy)
	if err != nil {
		t.Fatal(err)
	}
	paused, err := s.SetBoundPeerRBACPaused(ctx, *stored, true)
	if err != nil || paused == nil || !paused.Paused || len(paused.Domains) != 1 || !paused.Domains[0].Copy {
		t.Fatalf("paused policy=%+v err=%v", paused, err)
	}

	// Editing the complete permission snapshot while paused must not silently
	// reconnect the peer. Resume is the only operation that clears the bit.
	replacement := *paused
	replacement.Domains = []PeerRBACDomainPermission{{Domain: "replacement", Read: true}}
	replaced, err := s.ReplaceBoundPeerRBACPolicy(ctx, replacement)
	if err != nil || replaced == nil || !replaced.Paused || len(replaced.Domains) != 1 || replaced.Domains[0].Domain != "replacement" {
		t.Fatalf("paused replacement=%+v err=%v", replaced, err)
	}
	resumed, err := s.SetBoundPeerRBACPaused(ctx, *replaced, false)
	if err != nil || resumed == nil || resumed.Paused || len(resumed.Domains) != 1 {
		t.Fatalf("resumed policy=%+v err=%v", resumed, err)
	}

	// A stale UI request cannot change a retired generation after revocation.
	paused, err = s.SetBoundPeerRBACPaused(ctx, *resumed, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteSyncControl(ctx, policy.RemoteChainID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetBoundPeerRBACPaused(ctx, *paused, false); !errors.Is(err, ErrPeerRBACBindingMismatch) {
		t.Fatalf("retired generation pause error=%v, want ErrPeerRBACBindingMismatch", err)
	}
	stillPaused, err := s.GetPeerRBACPolicy(ctx, policy.RemoteChainID)
	if err != nil || stillPaused == nil || !stillPaused.Paused {
		t.Fatalf("failed stale resume changed policy=%+v err=%v", stillPaused, err)
	}
}

func TestPeerRBACPolicyValidationAndFrozenBinding(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	peerAgentID := testPeerAgentID(t)
	epoch, caPin := testPeerRBACBinding()
	valid := PeerRBACPolicy{RemoteChainID: "chain-peer", PeerAgentID: peerAgentID,
		PolicyEpoch: epoch, RemoteCAPin: caPin, PolicyVersion: CurrentPeerRBACPolicyVersion,
		Domains: []PeerRBACDomainPermission{{Domain: "tii", Read: true}}}
	if _, err := s.ReplacePeerRBACPolicy(ctx, valid); err != nil {
		t.Fatal(err)
	}

	rebound := valid
	rebound.PeerAgentID = testPeerAgentID(t)
	if _, err := s.ReplacePeerRBACPolicy(ctx, rebound); !errors.Is(err, ErrPeerRBACBindingMismatch) {
		t.Fatalf("rebind error = %v, want ErrPeerRBACBindingMismatch", err)
	}
	rebound = valid
	rebound.PolicyEpoch = "different-epoch"
	if _, err := s.ReplacePeerRBACPolicy(ctx, rebound); !errors.Is(err, ErrPeerRBACBindingMismatch) {
		t.Fatalf("epoch rebind error = %v, want ErrPeerRBACBindingMismatch", err)
	}
	rebound = valid
	rebound.RemoteCAPin = strings.Repeat("cd", 32)
	if _, err := s.ReplacePeerRBACPolicy(ctx, rebound); !errors.Is(err, ErrPeerRBACBindingMismatch) {
		t.Fatalf("CA rebind error = %v, want ErrPeerRBACBindingMismatch", err)
	}

	invalid := []PeerRBACPolicy{
		{RemoteChainID: "chain-peer", PeerAgentID: peerAgentID, PolicyEpoch: epoch, RemoteCAPin: caPin, PolicyVersion: 0},
		{RemoteChainID: "chain-peer", PeerAgentID: "not-a-key", PolicyEpoch: epoch, RemoteCAPin: caPin, PolicyVersion: CurrentPeerRBACPolicyVersion},
		{RemoteChainID: "chain-peer", PeerAgentID: peerAgentID, PolicyEpoch: epoch, RemoteCAPin: caPin, PolicyVersion: CurrentPeerRBACPolicyVersion,
			Domains: []PeerRBACDomainPermission{{Domain: "*", Read: true}}},
		{RemoteChainID: "chain-peer", PeerAgentID: peerAgentID, PolicyEpoch: epoch, RemoteCAPin: caPin, PolicyVersion: CurrentPeerRBACPolicyVersion,
			Domains: []PeerRBACDomainPermission{{Domain: "none"}}},
		{RemoteChainID: "chain-peer", PeerAgentID: peerAgentID, PolicyEpoch: epoch, RemoteCAPin: caPin, PolicyVersion: CurrentPeerRBACPolicyVersion,
			Domains: []PeerRBACDomainPermission{{Domain: " padded", Read: true}}},
		{RemoteChainID: "chain-peer", PeerAgentID: peerAgentID, PolicyEpoch: epoch, RemoteCAPin: caPin, PolicyVersion: CurrentPeerRBACPolicyVersion,
			Domains: []PeerRBACDomainPermission{{Domain: strings.Repeat("x", maxPeerRBACDomainBytes+1), Read: true}}},
		{RemoteChainID: "chain-peer", PeerAgentID: peerAgentID, PolicyEpoch: epoch, RemoteCAPin: caPin, PolicyVersion: CurrentPeerRBACPolicyVersion,
			Domains: []PeerRBACDomainPermission{{Domain: "write-preview", Write: true}}},
	}
	for i, policy := range invalid {
		if _, err := s.ReplacePeerRBACPolicy(ctx, policy); err == nil {
			t.Fatalf("invalid policy %d accepted", i)
		}
	}
}

func TestPeerRBACPolicyMigrationClearsPreviewWriteBits(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	peerAgentID := testPeerAgentID(t)
	epoch, caPin := testPeerRBACBinding()
	if _, err := s.ReplacePeerRBACPolicy(ctx, PeerRBACPolicy{
		RemoteChainID: "chain-peer", PeerAgentID: peerAgentID,
		PolicyEpoch: epoch, RemoteCAPin: caPin, PolicyVersion: CurrentPeerRBACPolicyVersion,
		Domains: []PeerRBACDomainPermission{{Domain: "tii", Read: true}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.writeExecContext(ctx, `UPDATE peer_rbac_domain SET can_write=1 WHERE remote_chain_id=?`, "chain-peer"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.writeExecContext(ctx, `
		INSERT INTO peer_rbac_domain(remote_chain_id, domain_tag, can_read, can_write, can_copy)
		VALUES(?,?,?,?,?),(?,?,?,?,?)`,
		"chain-peer", "write-only", false, true, false,
		"chain-peer", "copy-preview", false, false, true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetPeerRBACPolicy(ctx, "chain-peer"); !errors.Is(err, ErrPeerRBACWriteCapabilityUnavailable) {
		t.Fatalf("stored preview Write error=%v, want ErrPeerRBACWriteCapabilityUnavailable", err)
	}

	s.migratePeerRBACPolicies(ctx)
	got, err := s.GetPeerRBACPolicy(ctx, "chain-peer")
	if err != nil || got == nil || len(got.Domains) != 2 || got.Domains[0] != (PeerRBACDomainPermission{Domain: "copy-preview", Read: true, Copy: true}) ||
		got.Domains[1] != (PeerRBACDomainPermission{Domain: "tii", Read: true}) {
		t.Fatalf("migration did not clear preview Write: policy=%#v err=%v", got, err)
	}
}

func TestPeerRBACPolicyMigrationReconcilesOnlyExactActiveBinding(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	epoch, caPin := testPeerRBACBinding()
	makePolicy := func(chain string) PeerRBACPolicy {
		return PeerRBACPolicy{
			RemoteChainID: chain, PeerAgentID: testPeerAgentID(t),
			PolicyEpoch: epoch, RemoteCAPin: caPin, PolicyVersion: CurrentPeerRBACPolicyVersion,
			Domains: []PeerRBACDomainPermission{{Domain: "tii", Read: true}},
		}
	}
	matching := makePolicy("chain-migration-match")
	mismatched := makePolicy("chain-migration-mismatch")
	inactive := makePolicy("chain-migration-inactive")
	for _, policy := range []PeerRBACPolicy{matching, mismatched, inactive} {
		if _, err := s.ReplacePeerRBACPolicy(ctx, policy); err != nil {
			t.Fatal(err)
		}
	}

	matchingControl := testLegacyPeerRBACControl(matching)
	if err := s.PrepareSyncControl(ctx, matchingControl); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateSyncControl(ctx, matching.RemoteChainID, matching.PolicyEpoch); err != nil {
		t.Fatal(err)
	}
	if _, err := s.writeExecContext(ctx, `UPDATE sync_control SET remote_policy_version=2 WHERE remote_chain_id=?`, matching.RemoteChainID); err != nil {
		t.Fatal(err)
	}

	mismatchedControl := testLegacyPeerRBACControl(mismatched)
	mismatchedControl.PeerAgentID = testPeerAgentID(t)
	if err := s.PrepareSyncControl(ctx, mismatchedControl); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateSyncControl(ctx, mismatched.RemoteChainID, mismatched.PolicyEpoch); err != nil {
		t.Fatal(err)
	}
	if err := s.PrepareSyncControl(ctx, testLegacyPeerRBACControl(inactive)); err != nil {
		t.Fatal(err)
	}

	invalidChain := "chain-migration-invalid"
	if _, err := s.writeExecContext(ctx, `INSERT INTO peer_rbac_policy
		(remote_chain_id, peer_agent_id, policy_epoch, remote_ca_pin, policy_version)
		VALUES(?,?,?,?,?)`, invalidChain, "not-an-agent-key", epoch, caPin, CurrentPeerRBACPolicyVersion); err != nil {
		t.Fatal(err)
	}
	invalidControl := SyncControl{
		RemoteChainID: invalidChain, Role: "guest", ControllerChainID: invalidChain,
		ControllerAgentID: "not-an-agent-key", PeerAgentID: "not-an-agent-key",
		PolicyEpoch: epoch, RemoteCAPin: caPin, PolicyVersion: 1,
	}
	if err := s.PrepareSyncControl(ctx, invalidControl); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateSyncControl(ctx, invalidChain, epoch); err != nil {
		t.Fatal(err)
	}

	s.migratePeerRBACPolicies(ctx)
	for chain, wantVersion := range map[string]int{
		matching.RemoteChainID:   peerRBACSyncPolicyVersion,
		mismatched.RemoteChainID: 1,
		inactive.RemoteChainID:   1,
		invalidChain:             1,
	} {
		control, err := s.GetSyncControl(ctx, chain)
		if err != nil || control == nil || control.PolicyVersion != wantVersion {
			t.Fatalf("migration control %q=%+v err=%v, want version %d", chain, control, err, wantVersion)
		}
		if chain == matching.RemoteChainID && control.RemotePolicyVersion != 2 {
			t.Fatalf("migration synthesized remote version: %+v", control)
		}
	}
}

func TestPeerRBACPolicyFailsClosedOnCorruptStoredDomain(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	epoch, caPin := testPeerRBACBinding()
	if _, err := s.ReplacePeerRBACPolicy(ctx, PeerRBACPolicy{
		RemoteChainID: "chain-peer", PeerAgentID: testPeerAgentID(t),
		PolicyEpoch: epoch, RemoteCAPin: caPin, PolicyVersion: CurrentPeerRBACPolicyVersion,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.writeExecContext(ctx, `
		INSERT INTO peer_rbac_domain(remote_chain_id, domain_tag, can_read, can_write, can_copy)
		VALUES(?,?,?,?,?)`, "chain-peer", "*", true, false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetPeerRBACPolicy(ctx, "chain-peer"); err == nil {
		t.Fatal("corrupt wildcard domain was served as a valid policy")
	}
}

func TestPeerRBACPolicyDeleteAndFederationPurgePreservesCleanupHeader(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	epoch, caPin := testPeerRBACBinding()
	policy := PeerRBACPolicy{RemoteChainID: "chain-peer", PeerAgentID: testPeerAgentID(t),
		PolicyEpoch: epoch, RemoteCAPin: caPin, PolicyVersion: CurrentPeerRBACPolicyVersion,
		Domains: []PeerRBACDomainPermission{{Domain: "shared", Read: true}}}
	if _, err := s.ReplacePeerRBACPolicy(ctx, policy); err != nil {
		t.Fatal(err)
	}
	if err := s.DeletePeerRBACPolicy(ctx, "chain-peer"); err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetPeerRBACPolicy(ctx, "chain-peer"); err != nil || got != nil {
		t.Fatalf("delete left policy %#v err=%v", got, err)
	}
	if _, err := s.ReplacePeerRBACPolicy(ctx, policy); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertManagedPeerAccessGrant(ctx, ManagedPeerAccessGrant{
		RemoteChainID: "chain-peer", PeerAgentID: policy.PeerAgentID, Domain: "tii",
		Level: 2, State: ManagedPeerGrantPendingRevoke,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.PurgeSyncPeerState(ctx, "chain-peer"); err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetPeerRBACPolicy(ctx, "chain-peer"); err != nil || got == nil || len(got.Domains) != 0 {
		t.Fatalf("federation purge must retain only an empty grant-cleanup header %#v err=%v", got, err)
	}
	if err := s.DeleteManagedPeerAccessGrant(ctx, "chain-peer", policy.PeerAgentID, "tii"); err != nil {
		t.Fatal(err)
	}
	if err := s.PurgeSyncPeerState(ctx, "chain-peer"); err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetPeerRBACPolicy(ctx, "chain-peer"); err != nil || got != nil {
		t.Fatalf("purge retained a policy after its managed grant was cleaned: %#v err=%v", got, err)
	}
}
