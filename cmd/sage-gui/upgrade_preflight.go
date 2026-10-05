// upgrade_preflight.go implements `sage-gui upgrade preflight`: the read-only
// check an operator runs BEFORE climbing the app-version ladder on an existing
// chain (typically a v10.x node adopting a current v11 binary).
//
// It exists because the two hardest upgrade failures are both discovered far
// too late without it:
//
//	(1) The app-v22/app-v23 predecessor-ladder invariant. Those two activations
//	    refuse unless consensus storage holds a canonical applied-upgrade record
//	    for app-v6 and every independent version from app-v7 up, in strictly
//	    increasing activation-height order. A chain with a historical gap climbs
//	    happily to app-v21 and only then fails closed — mid-ladder, after the
//	    operator has already committed to the upgrade.
//
//	(2) The app-v23 authority migration. The earliest legacy Admin by
//	    RegisteredAt (canonical Agent ID as tie-breaker) becomes the singleton
//	    CEREBRUM Root; every other legacy Admin is demoted to Member pending an
//	    explicit Root-attested review. Operators need to know which key that is
//	    while they can still plan for it, not after activation.
//
// CRITICAL SCOPING RULE: the ladder invariant is only meaningful up to the
// version the chain has ACTUALLY REACHED. Activation records are written as each
// rung activates (internal/abci/app.go MarkUpgradeApplied), and consensus only
// consults the ladder when app-v22/app-v23 is proposed, executed, or restored —
// which requires currentAppVersion()==21/22 respectively. A healthy chain at
// app-v14 therefore has records for app-v6..v14 and legitimately none above.
// Demanding the full app-v6..v21 range unconditionally would report a false
// "unrepairable gap" for every chain below app-v21 — that is, for the entire
// audience this command was written for.
//
// Read-only wherever the platform allows it: it opens BadgerDB with
// WithReadOnly(true) and never proposes or mutates consensus state. Windows is
// the exception — badger refuses read-only opens there — so on that platform it
// first proves through SAGE's instance lock that no node is running, then opens
// normally. Either way the node must be stopped.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// appV22LadderFloor and appV22LadderCeiling bound the predecessor evidence
// app-v22 requires. They mirror the loop in
// abci.validatePersistedAppV22PredecessorLadder: the canonical persisted app-v6
// record is the single compatibility proof for the historical cumulative app-v2
// through app-v5 activation, and app-v7 through app-v21 must each carry their
// own record. Keep these in lockstep with that validator.
const (
	appV22LadderFloor   uint64 = 6
	appV22LadderCeiling uint64 = 21
)

// ladderRung is one version's persisted activation evidence.
type ladderRung struct {
	Version uint64
	Name    string
	Height  int64
	Present bool
	NotYet  bool   // above the chain's current version — nothing is wrong
	Problem string // non-empty when this rung fails the consensus invariant
}

// runUpgradePreflight is the `upgrade preflight` entry point.
func runUpgradePreflight(args []string) error {
	fs := flag.NewFlagSet("upgrade preflight", flag.ContinueOnError)
	dataDir := fs.String("data-dir", "", "SAGE data directory holding badger/ (default: the configured data_dir)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	resolvedDataDir := *dataDir
	if resolvedDataDir == "" {
		cfg, err := LoadConfig()
		if err != nil {
			return fmt.Errorf("load config (pass --data-dir to check a specific node): %w", err)
		}
		resolvedDataDir = cfg.DataDir
	}
	badgerPath := filepath.Join(resolvedDataDir, "badger")
	if _, statErr := os.Stat(badgerPath); statErr != nil {
		return fmt.Errorf("no consensus database at %s (wrong --data-dir, or this node was never initialized)", badgerPath)
	}

	bs, err := openConsensusStoreForInspection(badgerPath)
	if err != nil {
		return err
	}
	defer func() { _ = bs.CloseBadger() }()

	state, err := sageabci.LoadState(bs)
	if err != nil {
		return fmt.Errorf("read persisted app state: %w", err)
	}
	// Two different questions, two different ceilings: can this binary SERVE
	// this chain (compiled gates), and how far will its auto-voter go on its own
	// (the readiness ceiling)? A dormant gate is compiled and servable while the
	// auto-voter still abstains (app-v28 before its ceiling bump), so
	// compatibility must use the compiled ceiling or a node that CAN run the
	// chain is reported incompatible.
	maxSupported := sageabci.MaxSupportedAppVersion()
	maxCompiled := sageabci.MaxCompiledAppVersion()
	reached := highestAppliedVersion(bs, maxCompiled)
	genesis, genesisErr := bs.GetAppV23GenesisActivation()
	currentAppVersion := reached
	if genesisErr == nil && genesis != nil && currentAppVersion < 23 {
		currentAppVersion = 23
	}
	if currentAppVersion == 0 {
		currentAppVersion = 1
	}
	governanceStatus, err := sageabci.InspectUpgradeGovernanceState(bs, currentAppVersion)
	if err != nil {
		return fmt.Errorf("inspect stopped-node upgrade governance state: %w", err)
	}

	fmt.Printf("SAGE upgrade preflight\n")
	fmt.Printf("  data dir        : %s\n", resolvedDataDir)
	fmt.Printf("  persisted height: %d\n", state.Height)
	fmt.Printf("  binary supports : up to app-v%d\n", maxCompiled)
	if maxCompiled != maxSupported {
		fmt.Printf("  auto-vote ceiling: app-v%d (a target above this needs explicit votes)\n", maxSupported)
	}
	fmt.Printf("  this binary     : sage-gui %s\n\n", version)
	if compatibilityErr := printStoppedUpgradeGovernanceStatus(governanceStatus, maxCompiled); compatibilityErr != nil {
		return fmt.Errorf("upgrade preflight: binary replacement is incompatible with canonical upgrade governance state: %w", compatibilityErr)
	}

	// A chain born directly at app-v23 has no app-v6..v21 history and consensus
	// explicitly exempts it (internal/abci/appv23_local_rbac.go validateAppV23Prerequisite
	// returns early on appV23GenesisActive). Applying the ladder to it would be
	// a pure false alarm.
	if genesisErr == nil && genesis != nil {
		return reportDirectV23Genesis(bs, maxCompiled)
	}

	fmt.Printf("Highest applied activation record: app-v%d\n\n", reached)

	rungs := inspectLadder(bs, state.Height, reached)
	printLadderTable(rungs)

	ladderOK, firstProblem := ladderVerdict(rungs)
	admins, adminErr := listLegacyAdmins(bs)

	fmt.Println()
	switch {
	case !ladderOK:
		fmt.Println("VERDICT: this chain CANNOT reach app-v22.")
		fmt.Printf("  %s\n", firstProblem)
		fmt.Println("  app-v22 and app-v23 refuse to be proposed, approved, activated, or restored")
		fmt.Println("  without complete, strictly ordered predecessor evidence. The climb will stop")
		fmt.Println("  safely at app-v21 and fail closed there.")
		printLadderFailureRecovery(rungs)
	case adminErr == nil && reached < 23 && len(admins) == 0:
		// app-v23 elects its singleton Root from the legacy Admin roster. With
		// an empty roster there is nobody to promote, so the activation cannot
		// complete. Surfacing this as a footnote under a green verdict would be
		// exactly the manufactured confidence this command exists to prevent.
		fmt.Println("VERDICT: this chain CANNOT safely activate app-v23.")
		fmt.Println("  No agent holds Role==admin on chain, so the migration has no legacy Admin")
		fmt.Println("  to promote to the singleton CEREBRUM Root.")
		fmt.Println("  Register or materialize an admin agent before starting the climb.")
	case reached >= maxCompiled:
		fmt.Printf("VERDICT: nothing to do — the chain is already at app-v%d, this binary's ceiling.\n", maxCompiled)
	default:
		fmt.Printf("VERDICT: clear to climb from app-v%d to app-v%d.\n", reached, maxCompiled)
		fmt.Printf("  %d fork activation(s) remain. Each waits out a governance delay of at least\n", maxCompiled-reached)
		fmt.Println("  200 blocks, so the full climb takes a while — see docs/UPGRADING.md.")
	}

	// The authority preview reads only the agent registry, so it is meaningful
	// even when the ladder verdict is negative — an operator facing a gap still
	// benefits from knowing what app-v23 would do.
	if reached < 23 {
		fmt.Println()
		if adminErr != nil {
			fmt.Printf("app-v23 authority preview unavailable: %v\n", adminErr)
		} else {
			printAppV23Preview(admins)
		}
	}

	fmt.Println()
	fmt.Println("Full procedure: docs/UPGRADING.md")
	return nil
}

// printStoppedUpgradeGovernanceStatus reports the same compatibility decision
// the in-app updater makes automatically. A supported pending plan or upgrade
// ballot is carried through the verified snapshot and is not a deadlock.
func printStoppedUpgradeGovernanceStatus(status *sageabci.UpgradeGovernanceStatus, maxSupported uint64) error {
	fmt.Println("Binary replacement guard (canonical stopped-node state):")
	if status.PendingPlan == nil {
		fmt.Println("  pending plan : none")
	} else {
		plan := status.PendingPlan
		fmt.Printf("  pending plan : %s (target app-v%d, activation height %d)\n",
			plan.Name, plan.TargetAppVersion, plan.ActivationHeight)
	}
	if status.ActiveProposal == nil {
		fmt.Println("  active ballot: none")
	} else {
		proposal := status.ActiveProposal
		fmt.Printf("  active ballot: %s (%s, target %s, status %s",
			proposal.ProposalID, proposal.Operation, proposal.TargetID, proposal.Status)
		if proposal.TargetAppVersion != nil {
			fmt.Printf(", target app-v%d", *proposal.TargetAppVersion)
		}
		fmt.Println(")")
	}
	compatibilityErr := status.ValidateBinaryReplacement(maxSupported)
	if compatibilityErr != nil {
		fmt.Println("  VERDICT      : INCOMPATIBLE — replacement cannot safely execute canonical app state.")
	} else if status.PendingPlan != nil || status.ActiveProposal != nil {
		fmt.Println("  VERDICT      : COMPATIBLE — the supported in-flight operation continues after restart.")
	} else {
		fmt.Println("  VERDICT      : COMPATIBLE — no in-flight upgrade governance state.")
	}
	fmt.Println()
	return compatibilityErr
}

// printLadderFailureRecovery keeps preflight's operator guidance aligned with
// the app-v22 lineage-repair contract. Missing canonical records can be
// reconstructed only through the bounded app-v21 doctor/verify/quorum path.
// A record that is already present but invalid is deliberately outside that
// path: repair may never overwrite persisted consensus evidence.
func printLadderFailureRecovery(rungs []ladderRung) {
	presentInvalid := false
	missingCanonical := false
	for _, rung := range rungs {
		if rung.NotYet || rung.Problem == "" {
			continue
		}
		if rung.Present {
			presentInvalid = true
		}
		if !rung.Present && strings.HasPrefix(rung.Problem, "missing canonical applied ") {
			missingCanonical = true
		}
	}

	fmt.Println()
	switch {
	case presentInvalid:
		fmt.Println("  At least one canonical record is present but invalid. Lineage repair is")
		fmt.Println("  forbidden from overwriting present consensus evidence. Restore a complete")
		fmt.Println("  stopped-node backup from before the invalid record; this is restore-only.")
	case missingCanonical:
		fmt.Println("  Missing canonical records are recoverable only as virtual compatibility coverage")
		fmt.Println("  through the explicit app-v21 lineage workflow; repair never fabricates an")
		fmt.Println("  independent activation or writes upgrade:applied records for skipped rungs.")
		fmt.Println("  First take and retain a complete stopped-node backup. Then,")
		fmt.Println("  halt every validator and deploy v11.18.1 everywhere before any doctor,")
		fmt.Println("  proposal, or vote; mixed-version repair ceremonies are unsupported.")
		fmt.Println("  with the chain exactly at app-v21, run `sage-gui upgrade lineage doctor`, have")
		fmt.Println("  every validator run `sage-gui upgrade lineage verify` on the exact manifest")
		fmt.Println("  to replay retained app-version transitions and compare manifest_digest, then")
		fmt.Println("  use the explicit quorum proposal/vote flow.")
		fmt.Println("  Automatic voting is disabled for lineage repair, including single-validator")
		fmt.Println("  chains. Do not invent heights, synthesize records, or delete the data directory.")
	default:
		fmt.Println("  This evidence could not be read or validated. Do not continue the climb or")
		fmt.Println("  mutate the data directory; retain a stopped-node backup and investigate the")
		fmt.Println("  reported storage error before choosing any recovery path.")
	}
}

// openConsensusStoreForInspection opens the chain database for reading.
//
// The read-only open is preferred everywhere it works, because a diagnostic must
// not mutate consensus state. It is NOT available on Windows: badger refuses
// read-only opens outright there (ErrWindowsNotSupported), returned before it
// touches any data. Failing there would make preflight — a mandatory step in
// docs/UPGRADING.md — a permanent brick wall on a shipped platform, and the
// error text would send the operator into a restart-and-replay loop that can
// never succeed.
//
// So on Windows, having first proven through SAGE's own instance lock that no
// node is running, fall back to a plain open. That path performs badger's normal
// recovery but runs no SAGE migrations, which is the same treatment a restored
// state-sync database gets.
func openConsensusStoreForInspection(badgerPath string) (*store.BadgerStore, error) {
	bs, err := store.OpenBadgerStoreReadOnly(badgerPath)
	if err == nil {
		return bs, nil
	}
	if runtime.GOOS != "windows" {
		return nil, explainBadgerOpenFailure(badgerPath, err)
	}
	if lockErr := requireStoppedNode(SageHome()); lockErr != nil {
		return nil, fmt.Errorf("%w\n\nStop SAGE, then re-run preflight", lockErr)
	}
	bs, fallbackErr := store.OpenBadgerStoreWithoutMigrations(badgerPath)
	if fallbackErr != nil {
		return nil, fmt.Errorf("open %s for inspection: %w", badgerPath, fallbackErr)
	}
	return bs, nil
}

// explainBadgerOpenFailure separates "the node is running" from "this database
// needs recovery". Both surface as an open failure, but they need opposite
// operator actions, and telling someone with an unclean shutdown to "stop SAGE"
// sends them looking for a process that is not there.
func explainBadgerOpenFailure(badgerPath string, err error) error {
	msg := err.Error()
	if strings.Contains(msg, "directory lock") || strings.Contains(msg, "Another process") {
		return fmt.Errorf("open %s read-only: %w\n\n"+
			"SAGE is still running — preflight reads the consensus database directly.\n"+
			"Stop the node, then: sage-gui backup --full, sage-gui upgrade preflight", badgerPath, err)
	}
	return fmt.Errorf("open %s read-only: %w\n\n"+
		"This does not look like a running node — the database needs recovery before it can\n"+
		"be read read-only (an unclean shutdown leaves a value log to replay). Start the node\n"+
		"once so it replays, stop it cleanly, then re-run preflight", badgerPath, err)
}

// reportDirectV23Genesis handles chains born at app-v23, which have no
// app-v6..v21 lineage by design.
func reportDirectV23Genesis(bs *store.BadgerStore, ceiling uint64) error {
	reached := highestAppliedVersion(bs, ceiling)
	if reached < 23 {
		reached = 23
	}
	fmt.Println("This chain was born directly at app-v23 (direct genesis).")
	fmt.Println("The app-v22 predecessor ladder does not apply to it — consensus exempts")
	fmt.Println("direct-v23 genesis chains from that invariant.")
	fmt.Printf("\nHighest applied activation record: app-v%d\n\n", reached)
	if reached >= ceiling {
		fmt.Printf("VERDICT: nothing to do — the chain is already at app-v%d, this binary's ceiling.\n", ceiling)
	} else {
		fmt.Printf("VERDICT: clear to climb from app-v%d to app-v%d.\n", reached, ceiling)
	}
	fmt.Println()
	fmt.Println("Full procedure: docs/UPGRADING.md")
	return nil
}

// inspectLadder evaluates the predecessor evidence, applying the consensus rules
// only to rungs at or below the chain's current version. Rungs above `reached`
// are marked NotYet: the binary will create them as it climbs.
func inspectLadder(bs *store.BadgerStore, persistedHeight int64, reached uint64) []ladderRung {
	ceiling := appV22LadderCeiling
	// Once the chain is at app-v22 or beyond, that rung's own record is part of
	// the evidence app-v23 requires, so include it.
	if reached >= 22 {
		ceiling = 22
	}
	rungs := make([]ladderRung, 0, ceiling-appV22LadderFloor+1)
	var previousHeight int64
	for v := appV22LadderFloor; v <= ceiling; v++ {
		if v > reached {
			rungs = append(rungs, ladderRung{Version: v, Name: tx.CanonicalUpgradeName(v), NotYet: true})
			continue
		}
		rung := inspectRung(bs, v, persistedHeight, previousHeight)
		rungs = append(rungs, rung)
		if rung.Present && rung.Problem == "" {
			previousHeight = rung.Height
		}
	}
	return rungs
}

// inspectRung evaluates a single version's record against previousHeight, using
// the same rules as abci.validatePersistedAppV22PredecessorLadder.
func inspectRung(bs *store.BadgerStore, v uint64, persistedHeight, previousHeight int64) ladderRung {
	name := tx.CanonicalUpgradeName(v)
	rung := ladderRung{Version: v, Name: name}

	rec, err := bs.GetAppliedUpgrade(name)
	if err != nil {
		rung.Problem = fmt.Sprintf("cannot read applied %s record: %v", name, err)
		return rung
	}
	if rec == nil {
		rung.Problem = fmt.Sprintf("missing canonical applied %s record", name)
		return rung
	}

	rung.Present = true
	rung.Height = rec.AppliedHeight
	switch {
	case rec.Name != name:
		rung.Problem = fmt.Sprintf("record is named %q, want %q", rec.Name, name)
	case rec.TargetAppVersion != v:
		rung.Problem = fmt.Sprintf("record targets app version %d, want %d", rec.TargetAppVersion, v)
	case rec.AppliedHeight <= 0:
		rung.Problem = fmt.Sprintf("non-positive activation height %d", rec.AppliedHeight)
	case previousHeight > 0 && rec.AppliedHeight <= previousHeight:
		rung.Problem = fmt.Sprintf("height %d is not after the previous rung's height %d", rec.AppliedHeight, previousHeight)
	case rec.AppliedHeight > persistedHeight+1:
		rung.Problem = fmt.Sprintf("height %d is ahead of the persisted app height %d", rec.AppliedHeight, persistedHeight)
	}
	return rung
}

// highestAppliedVersion reports the highest version with a VALID activation
// record. A present-but-invalid record must not count as reached: treating it as
// progress is what turns a corrupt app-v22 record into a "clear to climb".
func highestAppliedVersion(bs *store.BadgerStore, maxSupported uint64) uint64 {
	var highest uint64
	for v := appV22LadderFloor; v <= maxSupported; v++ {
		rec, err := bs.GetAppliedUpgrade(tx.CanonicalUpgradeName(v))
		if err != nil || rec == nil {
			continue
		}
		if rec.TargetAppVersion != v || rec.AppliedHeight <= 0 {
			continue
		}
		if v > highest {
			highest = v
		}
	}
	return highest
}

// ladderVerdict reports whether every evaluated rung is sound, and the first
// problem otherwise. NotYet rungs are skipped — they are not evidence of a gap.
func ladderVerdict(rungs []ladderRung) (bool, string) {
	for _, r := range rungs {
		if r.NotYet {
			continue
		}
		if r.Problem != "" {
			return false, fmt.Sprintf("app-v%d: %s", r.Version, r.Problem)
		}
	}
	return true, ""
}

func printLadderTable(rungs []ladderRung) {
	fmt.Println("Predecessor ladder evidence (required by app-v22 and app-v23):")
	for _, r := range rungs {
		label := fmt.Sprintf("  app-v%-2d", r.Version)
		switch {
		case r.NotYet:
			fmt.Printf("%s  --        not activated yet; this binary will climb it\n", label)
		case !r.Present:
			fmt.Printf("%s  MISSING   %s\n", label, r.Problem)
		case r.Problem != "":
			fmt.Printf("%s  INVALID   height %d — %s\n", label, r.Height, r.Problem)
		default:
			fmt.Printf("%s  ok        activated at height %d\n", label, r.Height)
		}
	}
	fmt.Println("  (app-v6's record is the canonical compatibility proof for the cumulative")
	fmt.Println("   app-v2 through app-v5 activation; app-v7+ each need their own record.)")
}

// listLegacyAdmins returns the on-chain agents app-v23 will consider for Root
// election, in the migration's deterministic order. The role comparison is exact
// (store.AppV23RoleAdmin), matching consensus rather than approximating it.
func listLegacyAdmins(bs *store.BadgerStore) ([]store.OnChainAgent, error) {
	agents, err := bs.ListRegisteredAgents()
	if err != nil {
		return nil, fmt.Errorf("list registered agents: %w", err)
	}
	admins := make([]store.OnChainAgent, 0, len(agents))
	for _, a := range agents {
		if a.Role == store.AppV23RoleAdmin {
			admins = append(admins, a)
		}
	}
	sortAppV23AdminOrder(admins)
	return admins, nil
}

// sortAppV23AdminOrder puts legacy Admins into the migration's deterministic
// Root-election order: earliest RegisteredAt first, canonical Agent ID breaking
// ties.
func sortAppV23AdminOrder(admins []store.OnChainAgent) {
	sort.Slice(admins, func(i, j int) bool {
		if admins[i].RegisteredAt != admins[j].RegisteredAt {
			return admins[i].RegisteredAt < admins[j].RegisteredAt
		}
		return admins[i].AgentID < admins[j].AgentID
	})
}

// electAppV23Root returns the legacy Admin that app-v23 activation will promote
// to the singleton CEREBRUM Root.
func electAppV23Root(admins []store.OnChainAgent) store.OnChainAgent {
	if len(admins) == 0 {
		return store.OnChainAgent{}
	}
	ordered := append([]store.OnChainAgent{}, admins...)
	sortAppV23AdminOrder(ordered)
	return ordered[0]
}

// printAppV23Preview reports which legacy Admin app-v23 will elect as the
// singleton CEREBRUM Root and which Admins will be demoted pending review.
func printAppV23Preview(admins []store.OnChainAgent) {
	if len(admins) == 0 {
		fmt.Println("app-v23 authority preview: no agent holds Role==admin on chain, so the")
		fmt.Println("migration has no legacy Admin to promote to Root. Fix this before climbing.")
		return
	}

	root := admins[0]
	fmt.Println("app-v23 authority preview (what activation will do to your admins):")
	fmt.Printf("  becomes CEREBRUM Root : %s\n", describeAgent(root))
	fmt.Printf("                          registered at height %d\n", root.RegisteredAt)
	if len(admins) == 1 {
		fmt.Println("  demoted to Member     : none — this chain has a single Admin.")
		fmt.Println()
		fmt.Println("  Keep that agent's key material on this machine. After app-v23 the upgrade")
		fmt.Println("  proposals themselves are signed with the CEREBRUM Root credential, not the")
		fmt.Println("  operator agent.key.")
		return
	}

	fmt.Printf("  demoted to Member     : %d other Admin(s), each becoming an active Member with the\n", len(admins)-1)
	fmt.Println("                          migration-only legacy_restricted profile and disposition")
	fmt.Println("                          legacy_admin_review:")
	for _, a := range admins[1:] {
		fmt.Printf("                            - %s (height %d)\n", describeAgent(a), a.RegisteredAt)
	}
	fmt.Println()
	fmt.Println("  No historical Admin is re-promoted automatically. Restoring any of them to")
	fmt.Println("  Admin requires an explicit review attested by the current Root in CEREBRUM.")
	fmt.Println("  The complete old Admin roster is retained as immutable audit evidence.")
}

// describeAgent renders the most identifying label available for an agent.
func describeAgent(a store.OnChainAgent) string {
	name := a.RegisteredName
	if name == "" {
		name = a.Name
	}
	shortID := a.AgentID
	if len(shortID) > 16 {
		shortID = shortID[:16] + "…"
	}
	if name == "" {
		return shortID
	}
	return fmt.Sprintf("%s (%s)", name, shortID)
}
