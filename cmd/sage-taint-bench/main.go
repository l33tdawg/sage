// Command sage-taint-bench measures the provisional constants of the governance
// taint RFC (#460). The protocol is not implemented yet, so this is a stand-in
// over real Badger using the planned key layouts and costs. It never touches a
// node's state unless given a copy through -node-copy, and then only reads it.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
)

type machineContext struct {
	OS        string `json:"os"`
	GOARCH    string `json:"goarch"`
	UnameM    string `json:"uname_m"`
	CPU       string `json:"cpu"`
	CPUs      int    `json:"logical_cpus"`
	GoVersion string `json:"go_version"`
}

type commitContext struct {
	SHA   string `json:"sha"`
	Dirty bool   `json:"dirty"`
}

func commandOutput(name string, args ...string) (string, error) {
	output, err := exec.CommandContext(context.Background(), name, args...).Output()
	return strings.TrimSpace(string(output)), err
}

func detectMachine() (machineContext, error) {
	unameM, err := commandOutput("uname", "-m")
	if err != nil {
		return machineContext{}, fmt.Errorf("uname -m: %w", err)
	}
	return machineContext{
		OS: runtime.GOOS, GOARCH: runtime.GOARCH, UnameM: unameM,
		CPU: cpuModel(), CPUs: runtime.NumCPU(), GoVersion: runtime.Version(),
	}, nil
}

func cpuModel() string {
	raw, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return ""
	}
	for line := range strings.SplitSeq(string(raw), "\n") {
		if name, value, ok := strings.Cut(line, ":"); ok && strings.TrimSpace(name) == "model name" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func detectCommit() commitContext {
	sha, err := commandOutput("git", "rev-parse", "HEAD")
	if err != nil {
		return commitContext{SHA: "unknown"}
	}
	status, err := commandOutput("git", "status", "--porcelain")
	return commitContext{SHA: sha, Dirty: err != nil || status != ""}
}

// assertArch stops a job that landed on the wrong machine from producing a
// result. Both the kernel and the binary must match.
func assertArch(machine machineContext, want string) error {
	if want == "" {
		return nil
	}
	unameByArch := map[string]string{"amd64": "x86_64", "arm64": "aarch64"}
	wantUname, ok := unameByArch[want]
	if !ok {
		return fmt.Errorf("unsupported -require-arch %q (want amd64 or arm64)", want)
	}
	if machine.GOARCH != want || machine.UnameM != wantUname {
		return fmt.Errorf("architecture mismatch: want GOARCH %s and uname -m %s, got GOARCH %s and uname -m %s",
			want, wantUname, machine.GOARCH, machine.UnameM)
	}
	return nil
}

func profileDefaults(name string) (config, error) {
	cfg := config{
		Profile: name, ContentBytes: 4096,
		UnitCeiling: defaultUnitCeiling, BlockCeiling: defaultBlockCeiling,
	}
	switch name {
	case "smoke":
		cfg.Repeats, cfg.BackgroundRecords, cfg.Chains = 3, 2000, 2
		cfg.FrontierEntries, cfg.FrontierDecisions, cfg.FlatRecords = 4096, 1, 64
	case "full":
		cfg.Repeats, cfg.BackgroundRecords, cfg.Chains = 20, 100000, 8
		cfg.FrontierEntries, cfg.FrontierDecisions, cfg.FlatRecords = expansionCap, 4, 600
	default:
		return cfg, fmt.Errorf("unknown -profile %q (want smoke or full)", name)
	}
	return cfg, nil
}

func printSummary(w io.Writer, result *benchResult) {
	constants := result.Constants
	fmt.Fprintf(w, "\n%s\n\n", standInNotice)
	fmt.Fprintf(w, "machine: %s, GOARCH %s, uname -m %s, commit %s (dirty=%v)\n", result.Machine.OS, result.Machine.GOARCH, result.Machine.UnameM, result.Commit.SHA, result.Commit.Dirty)
	fmt.Fprintf(w, "digest:  %s\n", result.DeterministicDigest)
	fmt.Fprintf(w, "tombstone constant: %d bytes\n\n", constants.Tombstone)
	fmt.Fprintf(w, "%-8s %-28s %10s %12s %8s %10s\n", "ids", "read", "charged", "demand", "closed", "p95 ms")
	for _, read := range result.Reads {
		det := read.Deterministic
		fmt.Fprintf(w, "%-8s %-28s %10d %12d %8v %10.2f\n", det.IDProfile, det.Name, det.UnitsCharged, det.DemandUnits, det.FailedClosed, read.Timing.P95MS)
	}
	fmt.Fprintf(w, "\n%-8s %-20s %5s %7s %10s %10s %10s %10s %8s %8s %9s\n", "ids", "block", "txs", "units", "taint B", "v30 B", "E B", "other B", "verdict", "selected", "p95 ms")
	for _, block := range result.Blocks {
		det := block.Deterministic
		fmt.Fprintf(w, "%-8s %-20s %5d %7d %10d %10d %10d %10d %8s %8d %9.2f\n", block.IDProfile, block.Name, det.Txs, det.Units, det.GeneratedTaint, det.V30Writes, det.Estimate, det.OtherWrites, det.ProcessProposal, det.SelectedByPrepare, block.Total.P95MS)
	}
	fmt.Fprintf(w, "\nworst submit: E %d bytes, %d fit the block ceiling\n", constants.Block.WorstSubmitE, constants.Block.WorstSubmits)
	fmt.Fprintf(w, "invariants passed: %v\n", result.Invariants.Passed)
	for _, failure := range result.Invariants.Failures {
		fmt.Fprintln(w, "  FAIL:", failure)
	}
}

func main() {
	profile := flag.String("profile", "full", "smoke or full")
	out := flag.String("out", "", "result JSON path (default stdout)")
	dataDir := flag.String("data-dir", "", "parent for the disposable Badger directory; use a real disk, not tmpfs (default system temp)")
	requireArch := flag.String("require-arch", "", "fail unless both GOARCH and uname -m match: amd64 or arm64")
	repeats := flag.Int("repeats", 0, "timing repeats per scenario (default from profile)")
	nodeCopy := flag.String("node-copy", "", "read-only: COPY of a stopped node's Badger directory, for real-node counts")
	receipt := flag.String("receipt", "", "write a counts-only receipt for -node-copy to this path and skip the benchmark")
	nodeLabel := flag.String("node-label", "", "redacted source label recorded in the receipt")
	snapshotDate := flag.String("snapshot-date", "", "date the node copy was taken, YYYY-MM-DD, recorded in the receipt")
	flag.Parse()

	if *receipt != "" {
		if err := writeReceipt(*receipt, *nodeCopy, *nodeLabel, *snapshotDate); err != nil {
			fmt.Fprintln(os.Stderr, "receipt failed:", err)
			os.Exit(1)
		}
		return
	}
	if err := execute(*profile, *out, *dataDir, *requireArch, *repeats, *nodeCopy); err != nil {
		fmt.Fprintln(os.Stderr, "benchmark failed:", err)
		os.Exit(1)
	}
}

func execute(profile, out, dataDir, requireArch string, repeats int, nodeCopy string) error {
	machine, err := detectMachine()
	if err != nil {
		return err
	}
	if err = assertArch(machine, requireArch); err != nil {
		return err
	}
	cfg, err := profileDefaults(profile)
	if err != nil {
		return err
	}
	cfg.DataDir, cfg.NodeCopy = dataDir, nodeCopy
	if repeats > 0 {
		cfg.Repeats = repeats
	}
	result, err := run(cfg, os.Stderr)
	if err != nil {
		return err
	}
	result.Machine, result.Commit = machine, detectCommit()
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if out == "" {
		fmt.Println(string(encoded))
	} else if err := os.WriteFile(out, append(encoded, '\n'), 0o600); err != nil {
		return err
	}
	printSummary(os.Stderr, result)
	if !result.Invariants.Passed {
		return errors.New("invariants failed")
	}
	return nil
}
