// Command sage-taint-realbench exercises the actual app-v30 taint store,
// provenance, scope, lineage, admission and expansion code on a real Badger
// database. It deliberately does not claim CometBFT, SQL-mirror or live-node
// coverage.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

const (
	resultSchema = "sage-taint-realbench/v1"
	realNotice   = "REAL CODE: actual app-v30 store mutation, provenance, scope, lineage, admission and serving methods over Badger. This is not yet a live CometBFT node run and does not exercise the SQL mirror."
)

type machineContext struct {
	OS        string `json:"os"`
	GOARCH    string `json:"goarch"`
	UnameM    string `json:"uname_m"`
	CPU       string `json:"cpu,omitempty"`
	CPUs      int    `json:"logical_cpus"`
	GoVersion string `json:"go_version"`
}

type commitContext struct {
	SHA   string `json:"sha"`
	Dirty bool   `json:"dirty"`
}

type timingSummary struct {
	MinMS  float64 `json:"min_ms"`
	P50MS  float64 `json:"p50_ms"`
	P95MS  float64 `json:"p95_ms"`
	MaxMS  float64 `json:"max_ms"`
	MeanMS float64 `json:"mean_ms"`
}

type deterministicScenario struct {
	Name          string `json:"name"`
	Outcome       string `json:"outcome,omitempty"`
	Status        string `json:"status,omitempty"`
	DecisionIDs   int    `json:"decision_ids,omitempty"`
	Nodes         int    `json:"nodes,omitempty"`
	Units         int    `json:"units,omitempty"`
	Decisions     int    `json:"decisions,omitempty"`
	Materialized  int    `json:"materialized,omitempty"`
	Scanned       int    `json:"scanned,omitempty"`
	Enqueued      int    `json:"enqueued,omitempty"`
	Coverage      bool   `json:"coverage_complete,omitempty"`
	AppHash       string `json:"app_hash"`
	Keys          int    `json:"keys"`
	KeyBytes      int64  `json:"key_bytes"`
	ValueBytes    int64  `json:"value_bytes"`
	DatabaseLSM   int64  `json:"database_lsm_bytes,omitempty"`
	DatabaseValue int64  `json:"database_value_log_bytes,omitempty"`
}

type scenarioResult struct {
	Deterministic deterministicScenario `json:"deterministic"`
	Timing        timingSummary         `json:"timing"`
}

type invariants struct {
	Passed   bool     `json:"passed"`
	Failures []string `json:"failures"`
}

type realBenchResult struct {
	Schema              string            `json:"schema"`
	RealCode            bool              `json:"real_code"`
	Notice              string            `json:"notice"`
	Profile             string            `json:"profile"`
	Repeats             int               `json:"repeats"`
	Machine             machineContext    `json:"machine"`
	Commit              commitContext     `json:"commit"`
	Scenarios           []scenarioResult  `json:"scenarios"`
	Invariants          invariants        `json:"invariants"`
	DeterministicDigest string            `json:"deterministic_digest"`
	StartedAt           string            `json:"started_at"`
	ElapsedMS           float64           `json:"elapsed_ms"`
	Constraints         map[string]int    `json:"constraints"`
	Notes               map[string]string `json:"notes"`
}

type profile struct {
	Name         string
	Repeats      int
	ChainNodes   int
	Fanout       int
	ScopeSigners int
}

func main() {
	profileName := flag.String("profile", "smoke", "smoke or full")
	out := flag.String("out", "", "result JSON path (default stdout)")
	dataDir := flag.String("data-dir", "", "parent for disposable Badger directories")
	requireArch := flag.String("require-arch", "", "fail unless GOARCH and uname -m match amd64 or arm64")
	repeats := flag.Int("repeats", 0, "timing repeats per scenario")
	compare := flag.String("compare", "", "compare comma-separated result JSON files instead of running")
	flag.Parse()

	if *compare != "" {
		if err := compareResults(strings.Split(*compare, ",")); err != nil {
			fmt.Fprintln(os.Stderr, "comparison failed:", err)
			os.Exit(1)
		}
		return
	}
	if err := execute(*profileName, *out, *dataDir, *requireArch, *repeats); err != nil {
		fmt.Fprintln(os.Stderr, "real benchmark failed:", err)
		os.Exit(1)
	}
}

func profileDefaults(name string) (profile, error) {
	switch name {
	case "smoke":
		return profile{Name: name, Repeats: 3, ChainNodes: 256, Fanout: 256, ScopeSigners: 64}, nil
	case "full":
		return profile{Name: name, Repeats: 10, ChainNodes: 256, Fanout: 4096, ScopeSigners: 64}, nil
	default:
		return profile{}, fmt.Errorf("unknown profile %q (want smoke or full)", name)
	}
}

func execute(profileName, out, dataDir, requireArch string, repeats int) error {
	machine, err := detectMachine()
	if err != nil {
		return err
	}
	if err := assertArch(machine, requireArch); err != nil {
		return err
	}
	selected, err := profileDefaults(profileName)
	if err != nil {
		return err
	}
	if repeats > 0 {
		selected.Repeats = repeats
	}
	started := time.Now().UTC()
	scenarios, err := runScenarios(selected, dataDir)
	if err != nil {
		return err
	}
	result := &realBenchResult{
		Schema:    resultSchema,
		RealCode:  true,
		Notice:    realNotice,
		Profile:   profileName,
		Repeats:   selected.Repeats,
		Machine:   machine,
		Commit:    detectCommit(),
		Scenarios: scenarios,
		StartedAt: started.Format(time.RFC3339),
		ElapsedMS: float64(time.Since(started)) / float64(time.Millisecond),
		Constraints: map[string]int{
			"serving_request_units":      65536,
			"per_execution_units":        1024,
			"new_memberships_per_exec":   204,
			"provenance_admission_units": 722,
			"max_active_decisions":       32,
			"max_serving_ancestor_nodes": 256,
		},
		Notes: map[string]string{
			"state":       "Badger consensus primitives only",
			"excluded":    "CometBFT block execution, SQL mirror, REST/MCP serving, live-node restart",
			"timing":      "Setup is excluded. Read scenarios reuse one canonical snapshot; mutation scenarios rebuild a fresh database per repeat.",
			"app_hash":    "Computed by BadgerStore.ComputeAppHash over the exact final store state.",
			"determinism": "Timings and machine metadata are excluded from deterministic_digest.",
		},
	}
	result.DeterministicDigest, err = deterministicDigest(result)
	if err != nil {
		return err
	}
	result.Invariants = checkInvariants(result)
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if out == "" {
		fmt.Println(string(encoded))
	} else {
		if parent := filepath.Dir(out); parent != "." {
			if err := os.MkdirAll(parent, 0o755); err != nil {
				return fmt.Errorf("create result directory: %w", err)
			}
		}
		if err := os.WriteFile(out, append(encoded, '\n'), 0o600); err != nil {
			return fmt.Errorf("write result: %w", err)
		}
	}
	printSummary(os.Stderr, result)
	if !result.Invariants.Passed {
		return errors.New("real-code invariants failed")
	}
	return nil
}

func compareResults(paths []string) error {
	if len(paths) != 2 || strings.TrimSpace(paths[0]) == "" || strings.TrimSpace(paths[1]) == "" {
		return errors.New("compare requires exactly two comma-separated result paths")
	}
	results := make([]realBenchResult, 0, 2)
	for _, path := range paths {
		raw, err := os.ReadFile(strings.TrimSpace(path))
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		var result realBenchResult
		if err := json.Unmarshal(raw, &result); err != nil {
			return fmt.Errorf("decode %s: %w", path, err)
		}
		if result.Schema != resultSchema || !result.RealCode {
			return fmt.Errorf("%s is not a real-code benchmark result", path)
		}
		if !result.Invariants.Passed {
			return fmt.Errorf("%s has failed invariants", path)
		}
		computed, err := deterministicDigest(&result)
		if err != nil {
			return fmt.Errorf("recompute digest for %s: %w", path, err)
		}
		if result.DeterministicDigest != computed {
			return fmt.Errorf("%s deterministic digest does not match its evidence", path)
		}
		results = append(results, result)
	}
	if results[0].Profile != results[1].Profile {
		return fmt.Errorf("profile mismatch: %s vs %s", results[0].Profile, results[1].Profile)
	}
	arches := map[string]bool{results[0].Machine.GOARCH: true, results[1].Machine.GOARCH: true}
	if !arches["amd64"] || !arches["arm64"] {
		return fmt.Errorf("compare needs one amd64 and one arm64 result, got %v", arches)
	}
	if results[0].DeterministicDigest != results[1].DeterministicDigest {
		return fmt.Errorf("deterministic digest mismatch: %s vs %s",
			results[0].DeterministicDigest, results[1].DeterministicDigest)
	}
	fmt.Printf("app-v30 real-code parity: %s\n", results[0].DeterministicDigest)
	return nil
}

func printSummary(w io.Writer, result *realBenchResult) {
	fmt.Fprintf(w, "\n%s, profile=%s, repeats=%d\n", realNotice, result.Profile, result.Repeats)
	fmt.Fprintf(w, "machine: GOARCH %s, uname -m %s, commit %s (dirty=%v)\n",
		result.Machine.GOARCH, result.Machine.UnameM, result.Commit.SHA, result.Commit.Dirty)
	fmt.Fprintf(w, "digest: %s\n\n", result.DeterministicDigest)
	fmt.Fprintf(w, "%-22s %10s %8s %12s %8s %10s\n",
		"scenario", "units", "nodes", "materialized", "keys", "p95 ms")
	for _, scenario := range result.Scenarios {
		det := scenario.Deterministic
		fmt.Fprintf(w, "%-22s %10d %8d %12d %8d %10.3f\n",
			det.Name, det.Units, det.Nodes, det.Materialized, det.Keys, scenario.Timing.P95MS)
	}
	fmt.Fprintf(w, "\ninvariants passed: %v\n", result.Invariants.Passed)
	for _, failure := range result.Invariants.Failures {
		fmt.Fprintf(w, "  FAIL: %s\n", failure)
	}
}

func deterministicDigest(result *realBenchResult) (string, error) {
	canonical, err := json.Marshal(struct {
		Profile     string                  `json:"profile"`
		Repeats     int                     `json:"repeats"`
		Scenarios   []deterministicScenario `json:"scenarios"`
		Constraints map[string]int          `json:"constraints"`
	}{
		Profile:     result.Profile,
		Repeats:     result.Repeats,
		Scenarios:   deterministicScenarios(result.Scenarios),
		Constraints: result.Constraints,
	})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}

func deterministicScenarios(results []scenarioResult) []deterministicScenario {
	scenarios := make([]deterministicScenario, 0, len(results))
	for _, result := range results {
		scenarios = append(scenarios, result.Deterministic)
	}
	sort.Slice(scenarios, func(i, j int) bool {
		return scenarios[i].Name < scenarios[j].Name
	})
	return scenarios
}

func checkInvariants(result *realBenchResult) invariants {
	var failures []string
	byName := make(map[string]deterministicScenario, len(result.Scenarios))
	for _, scenario := range result.Scenarios {
		det := scenario.Deterministic
		byName[det.Name] = det
		if len(det.AppHash) != 64 {
			failures = append(failures, det.Name+": app hash is not 32-byte hex")
		}
		if det.Keys <= 0 || det.KeyBytes <= 0 {
			failures = append(failures, det.Name+": empty canonical state")
		}
	}
	if det, ok := byName["scope_match"]; !ok {
		failures = append(failures, "scope_match missing")
	} else {
		if det.Outcome != "tainted" || det.DecisionIDs != 1 {
			failures = append(failures, "scope_match did not select exactly the matching decision")
		}
		if det.Units <= 0 || det.Units > 65536 {
			failures = append(failures, "scope_match exceeded the serving unit ceiling")
		}
	}
	if det, ok := byName["lineage_walk"]; !ok {
		failures = append(failures, "lineage_walk missing")
	} else {
		if det.Outcome != "pending" || det.Nodes != result.Constraints["max_serving_ancestor_nodes"] {
			failures = append(failures, "lineage_walk did not cover the bounded ancestor chain")
		}
	}
	if det, ok := byName["admission_plan"]; !ok {
		failures = append(failures, "admission_plan missing")
	} else {
		if det.Decisions != 32 || det.Units != result.Constraints["provenance_admission_units"] {
			failures = append(failures, "admission_plan did not hit the exact accepted bound")
		}
	}
	if det, ok := byName["fanout_expand"]; !ok {
		failures = append(failures, "fanout_expand missing")
	} else {
		if det.Materialized <= 0 || det.Materialized > 204 || det.Units > 1024 {
			failures = append(failures, "fanout_expand exceeded its execution budget")
		}
	}
	return invariants{Passed: len(failures) == 0, Failures: failures}
}

func commandOutput(name string, args ...string) (string, error) {
	output, err := exec.Command(name, args...).Output()
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
