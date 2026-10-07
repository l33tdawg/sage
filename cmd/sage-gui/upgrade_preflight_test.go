package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func capturePreflightStdout(t *testing.T, run func()) string {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	original := os.Stdout
	os.Stdout = write
	run()
	os.Stdout = original
	if closeErr := write.Close(); closeErr != nil {
		t.Fatalf("close writer: %v", closeErr)
	}
	out, err := io.ReadAll(read)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if err := read.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}
	return string(out)
}

// newPreflightStore builds a writable Badger store for ladder fixtures.
func newPreflightStore(t *testing.T) (*store.BadgerStore, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "badger")
	bs, err := store.NewBadgerStore(path)
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	t.Cleanup(func() { _ = bs.CloseBadger() })
	return bs, path
}

// seedLadder writes canonical applied-upgrade records for [floor, ceiling] with
// strictly increasing heights, skipping any version in omit.
func seedLadder(t *testing.T, bs *store.BadgerStore, floor, ceiling uint64, omit map[uint64]bool) {
	t.Helper()
	height := int64(10)
	for v := floor; v <= ceiling; v++ {
		height += 10
		if omit[v] {
			continue
		}
		if err := bs.MarkUpgradeApplied(tx.CanonicalUpgradeName(v), v, height); err != nil {
			t.Fatalf("mark app-v%d applied: %v", v, err)
		}
	}
}

// THE regression test. A healthy v10.7 chain sits at app-v14 with records for
// app-v6..v14 and legitimately none above — the climb creates those. Requiring
// the full app-v6..v21 range unconditionally reported "this chain CANNOT reach
// app-v22 ... not repairable" to every chain below app-v21, i.e. to the entire
// audience of docs/UPGRADING.md.
func TestLadderAcceptsChainThatHasNotClimbedYet(t *testing.T) {
	bs, _ := newPreflightStore(t)
	seedLadder(t, bs, appV22LadderFloor, 14, nil)

	reached := highestAppliedVersion(bs, sageabci.MaxSupportedAppVersion())
	if reached != 14 {
		t.Fatalf("reached = %d, want 14", reached)
	}

	rungs := inspectLadder(bs, 10_000, reached)
	ok, problem := ladderVerdict(rungs)
	if !ok {
		t.Fatalf("healthy app-v14 chain was reported as broken: %s", problem)
	}
	for _, r := range rungs {
		if r.Version > reached && !r.NotYet {
			t.Fatalf("app-v%d above the chain's version was not marked not-yet-reached", r.Version)
		}
		if r.Version <= reached && r.NotYet {
			t.Fatalf("app-v%d at or below the chain's version was wrongly skipped", r.Version)
		}
	}
}

func TestStoppedUpgradeGovernanceGuardUsesCompatibilityInsteadOfPresence(t *testing.T) {
	tests := []struct {
		name      string
		status    *sageabci.UpgradeGovernanceStatus
		wantError bool
		marker    string
	}{
		{
			name: "clear",
			status: &sageabci.UpgradeGovernanceStatus{
				CurrentAppVersion: 27,
			},
			marker: "VERDICT      : COMPATIBLE",
		},
		{
			name: "pending plan",
			status: &sageabci.UpgradeGovernanceStatus{
				CurrentAppVersion: 26,
				PendingPlan: &sageabci.UpgradeGovernancePendingPlan{
					Name: "app-v27", TargetAppVersion: 27, ActivationHeight: 900,
				},
			},
			marker: "VERDICT      : COMPATIBLE",
		},
		{
			name: "active upgrade ballot",
			status: &sageabci.UpgradeGovernanceStatus{
				CurrentAppVersion: 26,
				ActiveProposal: &sageabci.UpgradeGovernanceActiveProposal{
					ProposalID: "proposal-27", Operation: "upgrade", TargetID: "app-v27",
					OperationCode: 5, Status: "voting", TargetAppVersion: uint64Pointer(27),
				},
			},
			marker: "target app-v27",
		},
		{
			name: "unsupported pending plan",
			status: &sageabci.UpgradeGovernanceStatus{
				CurrentAppVersion: 27,
				PendingPlan: &sageabci.UpgradeGovernancePendingPlan{
					Name: "app-v29", TargetAppVersion: 29, ActivationHeight: 900,
				},
			},
			wantError: true,
			marker:    "VERDICT      : INCOMPATIBLE",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var compatibilityErr error
			output := capturePreflightStdout(t, func() {
				compatibilityErr = printStoppedUpgradeGovernanceStatus(tt.status, sageabci.MaxSupportedAppVersion())
			})
			if (compatibilityErr != nil) != tt.wantError {
				t.Fatalf("error = %v, wantError %v", compatibilityErr, tt.wantError)
			}
			if !strings.Contains(output, tt.marker) {
				t.Fatalf("output missing %q:\n%s", tt.marker, output)
			}
		})
	}
}

func uint64Pointer(value uint64) *uint64 { return &value }

func TestInspectLadderAcceptsCompleteEvidence(t *testing.T) {
	bs, _ := newPreflightStore(t)
	seedLadder(t, bs, appV22LadderFloor, appV22LadderCeiling, nil)

	rungs := inspectLadder(bs, 10_000, appV22LadderCeiling)
	ok, problem := ladderVerdict(rungs)
	if !ok {
		t.Fatalf("complete ladder rejected: %s", problem)
	}
}

// A genuine historical gap BELOW the chain's current version is the real defect
// this command exists to catch, and it must still fail closed.
func TestInspectLadderRejectsGapBelowReachedVersion(t *testing.T) {
	bs, _ := newPreflightStore(t)
	seedLadder(t, bs, appV22LadderFloor, appV22LadderCeiling, map[uint64]bool{17: true})

	reached := highestAppliedVersion(bs, sageabci.MaxSupportedAppVersion())
	rungs := inspectLadder(bs, 10_000, reached)
	ok, problem := ladderVerdict(rungs)
	if ok {
		t.Fatal("ladder with a missing app-v17 record was accepted")
	}
	if !strings.Contains(problem, "app-v17") {
		t.Fatalf("problem %q does not name app-v17", problem)
	}
}

// Out-of-order activation heights must fail exactly as the consensus validator
// fails them: strictly increasing is the rule, not merely present.
func TestInspectLadderRejectsOutOfOrderHeights(t *testing.T) {
	bs, _ := newPreflightStore(t)
	seedLadder(t, bs, appV22LadderFloor, appV22LadderCeiling, nil)
	if err := bs.MarkUpgradeApplied(tx.CanonicalUpgradeName(15), 15, 11); err != nil {
		t.Fatalf("rewrite app-v15: %v", err)
	}

	rungs := inspectLadder(bs, 10_000, appV22LadderCeiling)
	if ok, _ := ladderVerdict(rungs); ok {
		t.Fatal("out-of-order ladder was accepted")
	}
}

// A record ahead of the persisted app height is corruption, not progress. The
// consensus validator allows exactly +1 for the FinalizeBlock/Commit crash
// window; preflight must mirror that boundary rather than approximate it.
func TestInspectRungHeightAheadOfPersistedState(t *testing.T) {
	bs, _ := newPreflightStore(t)
	if err := bs.MarkUpgradeApplied(tx.CanonicalUpgradeName(9), 9, 100); err != nil {
		t.Fatalf("mark applied: %v", err)
	}

	if rung := inspectRung(bs, 9, 99, 0); rung.Problem != "" {
		t.Fatalf("crash-window height rejected: %s", rung.Problem)
	}
	if rung := inspectRung(bs, 9, 98, 0); rung.Problem == "" {
		t.Fatal("height ahead of persisted app state was accepted")
	}
}

func TestInspectRungRejectsMismatchedTarget(t *testing.T) {
	bs, _ := newPreflightStore(t)
	if err := bs.MarkUpgradeApplied(tx.CanonicalUpgradeName(12), 11, 50); err != nil {
		t.Fatalf("mark applied: %v", err)
	}
	if rung := inspectRung(bs, 12, 10_000, 0); rung.Problem == "" {
		t.Fatal("record with a mismatched target app version was accepted")
	}
}

// An invalid record must not count as progress. Counting it would let a corrupt
// app-v22 record produce "clear to climb" — the exact manufactured confidence
// this command exists to prevent.
func TestHighestAppliedVersionIgnoresInvalidRecords(t *testing.T) {
	bs, _ := newPreflightStore(t)
	seedLadder(t, bs, appV22LadderFloor, 14, nil)
	if err := bs.MarkUpgradeApplied(tx.CanonicalUpgradeName(15), 11, 500); err != nil {
		t.Fatalf("seed mismatched record: %v", err)
	}

	if got := highestAppliedVersion(bs, sageabci.MaxSupportedAppVersion()); got != 14 {
		t.Fatalf("highestAppliedVersion = %d, want 14 (the invalid app-v15 record must not count)", got)
	}
}

// highestAppliedVersion must be self-sufficient across the whole range, not just
// app-v23 and above: the backup manifest relies on it for pre-v23 chains.
func TestHighestAppliedVersionCoversPreV23Chains(t *testing.T) {
	bs, _ := newPreflightStore(t)
	seedLadder(t, bs, appV22LadderFloor, 11, nil)

	if got := highestAppliedVersion(bs, sageabci.MaxSupportedAppVersion()); got != 11 {
		t.Fatalf("highestAppliedVersion = %d, want 11", got)
	}
}

func TestHighestAppliedVersionScansPastLadder(t *testing.T) {
	bs, _ := newPreflightStore(t)
	seedLadder(t, bs, appV22LadderFloor, 24, nil)

	if got := highestAppliedVersion(bs, sageabci.MaxSupportedAppVersion()); got != 24 {
		t.Fatalf("highestAppliedVersion = %d, want 24", got)
	}
}

// Once the chain is at app-v22, that rung's own record becomes part of the
// evidence app-v23 requires, so it must be evaluated rather than skipped.
func TestInspectLadderIncludesAppV22OnceReached(t *testing.T) {
	bs, _ := newPreflightStore(t)
	seedLadder(t, bs, appV22LadderFloor, 22, nil)

	rungs := inspectLadder(bs, 10_000, 22)
	last := rungs[len(rungs)-1]
	if last.Version != 22 {
		t.Fatalf("last rung = app-v%d, want app-v22", last.Version)
	}
	if last.NotYet || !last.Present {
		t.Fatalf("app-v22 record was not evaluated: %+v", last)
	}
}

func TestLadderVerdictSkipsNotYetRungs(t *testing.T) {
	rungs := []ladderRung{
		{Version: 6, Height: 10, Present: true},
		{Version: 7, NotYet: true, Problem: "should be ignored"},
	}
	if ok, problem := ladderVerdict(rungs); !ok {
		t.Fatalf("not-yet-reached rung produced a failure: %s", problem)
	}
}

func TestPreflightMissingLineagePrintsBoundedRecoveryWorkflow(t *testing.T) {
	out := capturePreflightStdout(t, func() {
		printLadderFailureRecovery([]ladderRung{{
			Version: 17,
			Name:    "app-v17",
			Problem: "missing canonical applied app-v17 record",
		}})
	})

	for _, required := range []string{
		"stopped-node backup",
		"exactly at app-v21",
		"upgrade lineage doctor",
		"upgrade lineage verify",
		"virtual compatibility coverage",
		"replay retained app-version transitions",
		"never fabricates",
		"halt every validator",
		"deploy v11.18.1 everywhere",
		"manifest_digest",
		"explicit quorum proposal/vote",
		"Automatic voting is disabled",
	} {
		if !strings.Contains(out, required) {
			t.Fatalf("missing-lineage guidance does not contain %q:\n%s", required, out)
		}
	}
	if strings.Contains(out, "restore-only") {
		t.Fatalf("missing lineage was incorrectly described as restore-only:\n%s", out)
	}
}

func TestPreflightPresentInvalidLineageIsRestoreOnly(t *testing.T) {
	out := capturePreflightStdout(t, func() {
		printLadderFailureRecovery([]ladderRung{
			{Version: 16, Name: "app-v16", Present: true, Problem: "height 2 is not after the previous rung's height 3"},
			{Version: 17, Name: "app-v17", Problem: "missing canonical applied app-v17 record"},
		})
	})

	for _, required := range []string{"present but invalid", "forbidden from overwriting", "restore-only"} {
		if !strings.Contains(out, required) {
			t.Fatalf("invalid-lineage guidance does not contain %q:\n%s", required, out)
		}
	}
	if strings.Contains(out, "upgrade lineage doctor") {
		t.Fatalf("present-invalid lineage was incorrectly sent to doctor repair:\n%s", out)
	}
}

// The app-v23 Root election is deterministic: earliest RegisteredAt wins, with
// the canonical Agent ID as tie-breaker.
func TestAppV23RootElectionOrdering(t *testing.T) {
	admins := []store.OnChainAgent{
		{AgentID: "ffff", Role: store.AppV23RoleAdmin, RegisteredAt: 50},
		{AgentID: "bbbb", Role: store.AppV23RoleAdmin, RegisteredAt: 10},
		{AgentID: "aaaa", Role: store.AppV23RoleAdmin, RegisteredAt: 10},
	}
	if root := electAppV23Root(admins); root.AgentID != "aaaa" {
		t.Fatalf("root = %s, want aaaa (earliest height, lowest ID tie-break)", root.AgentID)
	}
}

// The preview's candidate set must be byte-identical to the migration's, which
// compares the role exactly. A case-insensitive match would preview a Root that
// consensus would never elect.
func TestListLegacyAdminsMatchesRoleExactly(t *testing.T) {
	bs, _ := newPreflightStore(t)
	if err := bs.RegisterAgent("aaaa", "real", store.AppV23RoleAdmin, "", "", "", 10); err != nil {
		t.Fatalf("register admin: %v", err)
	}
	if err := bs.RegisterAgent("bbbb", "casing", "Admin", "", "", "", 5); err != nil {
		t.Fatalf("register mixed-case role: %v", err)
	}

	admins, err := listLegacyAdmins(bs)
	if err != nil {
		t.Fatalf("listLegacyAdmins: %v", err)
	}
	for _, a := range admins {
		if a.Role != store.AppV23RoleAdmin {
			t.Fatalf("non-exact role %q was included as a Root candidate", a.Role)
		}
	}
	for _, a := range admins {
		if a.AgentID == "bbbb" {
			t.Fatal("mixed-case \"Admin\" was treated as a Root candidate; consensus compares exactly")
		}
	}
}

func TestDescribeAgentPrefersRegisteredName(t *testing.T) {
	a := store.OnChainAgent{
		AgentID:        "0123456789abcdef0123456789abcdef",
		Name:           "mutable",
		RegisteredName: "canonical",
	}
	got := describeAgent(a)
	if !strings.Contains(got, "canonical") {
		t.Fatalf("describeAgent = %q, want it to name the registered name", got)
	}
	if strings.Contains(got, a.AgentID) {
		t.Fatalf("describeAgent = %q, want the agent ID truncated", got)
	}
}

// A running node and a database needing recovery both surface as an open
// failure, but they need opposite operator actions. Telling someone with an
// unclean shutdown to "stop SAGE" sends them hunting a process that is not there.
func TestExplainBadgerOpenFailureDistinguishesLockFromCorruption(t *testing.T) {
	lockErr := explainBadgerOpenFailure("/x/badger",
		errTest("Cannot acquire directory lock on \"/x/badger\""))
	if !strings.Contains(lockErr.Error(), "SAGE is still running") {
		t.Fatalf("lock failure not identified as a running node: %v", lockErr)
	}

	replayErr := explainBadgerOpenFailure("/x/badger", errTest("value log truncate required"))
	if strings.Contains(replayErr.Error(), "SAGE is still running") {
		t.Fatalf("replay failure wrongly reported as a running node: %v", replayErr)
	}
	if !strings.Contains(replayErr.Error(), "needs recovery") {
		t.Fatalf("replay failure did not explain recovery: %v", replayErr)
	}
}

type errTest string

func (e errTest) Error() string { return string(e) }
