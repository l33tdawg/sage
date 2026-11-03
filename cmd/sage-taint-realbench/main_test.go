package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/taint"
)

func TestRealBenchSmokeProducesDeterministicEvidence(t *testing.T) {
	resultPath := filepath.Join(t.TempDir(), "result.json")
	require.NoError(t, execute("smoke", resultPath, t.TempDir(), "", 1))
	raw, err := os.ReadFile(resultPath)
	require.NoError(t, err)
	var result realBenchResult
	require.NoError(t, json.Unmarshal(raw, &result))
	require.True(t, result.RealCode)
	require.Equal(t, resultSchema, result.Schema)
	require.True(t, result.Invariants.Passed, result.Invariants.Failures)
	require.Len(t, result.Scenarios, 4)
	require.Len(t, result.DeterministicDigest, 64)
	for _, scenario := range result.Scenarios {
		require.Len(t, scenario.Deterministic.AppHash, 64)
		require.Positive(t, scenario.Deterministic.Keys)
	}
}

func TestRealBenchDigestExcludesMachineAndTiming(t *testing.T) {
	result := &realBenchResult{
		Profile:     "full",
		Repeats:     10,
		Constraints: map[string]int{"unit": 65536},
		Scenarios: []scenarioResult{{
			Deterministic: deterministicScenario{Name: "scope", Units: 163},
			Timing:        timingSummary{P95MS: 1},
		}},
		Machine: machineContext{GOARCH: "arm64"},
	}
	first, err := deterministicDigest(result)
	require.NoError(t, err)
	result.Machine.GOARCH = "amd64"
	result.Scenarios[0].Timing.P95MS = 999
	second, err := deterministicDigest(result)
	require.NoError(t, err)
	require.Equal(t, first, second)
	result.Scenarios[0].Deterministic.Units++
	third, err := deterministicDigest(result)
	require.NoError(t, err)
	require.NotEqual(t, first, third)
}

func TestCompareRealBenchRequiresBothArchitecturesAndMatchingDigest(t *testing.T) {
	// Start from actual nonempty Badger scenario evidence, not an empty object
	// blessed by a serialized Passed flag. Machine/source labels below are test
	// metadata; comparing JSON does not attest either platform or provenance.
	config, err := profileDefaults("smoke")
	require.NoError(t, err)
	config.Repeats = 1
	scenarios, err := runScenarios(config, t.TempDir())
	require.NoError(t, err)
	base := realBenchResult{
		Schema: resultSchema, RealCode: true, Profile: config.Name, Repeats: config.Repeats,
		Machine:   machineContext{OS: "linux", GOARCH: "arm64", UnameM: "aarch64", CPUs: 1, GoVersion: "test"},
		Commit:    commitContext{SHA: strings.Repeat("a", 40)},
		Scenarios: scenarios, Constraints: expectedConstraints(),
	}
	base.Invariants = checkInvariants(&base)
	require.True(t, base.Invariants.Passed, base.Invariants.Failures)
	require.Len(t, base.Scenarios, 4)
	for _, scenario := range base.Scenarios {
		require.Positive(t, scenario.Deterministic.Keys)
		require.Positive(t, scenario.Deterministic.KeyBytes)
		require.Positive(t, scenario.Deterministic.ValueBytes)
	}
	seal := func(result realBenchResult) realBenchResult {
		result.DeterministicDigest, err = deterministicDigest(&result)
		require.NoError(t, err)
		return result
	}
	clone := func() realBenchResult {
		raw, marshalErr := json.Marshal(base)
		require.NoError(t, marshalErr)
		var result realBenchResult
		require.NoError(t, json.Unmarshal(raw, &result))
		return result
	}
	arm := seal(clone())
	amd := clone()
	amd.Machine.GOARCH, amd.Machine.UnameM = "amd64", "x86_64"
	amd = seal(amd)
	armPath := writeTestResult(t, arm)
	amdPath := writeTestResult(t, amd)
	require.NoError(t, compareResults([]string{armPath, amdPath}))
	require.NoError(t, compareResults([]string{amdPath, armPath}))
	require.Error(t, compareResults(nil))
	require.Error(t, compareResults([]string{armPath, ""}))
	cases := []struct {
		name   string
		change func(*realBenchResult)
	}{
		{"empty", func(r *realBenchResult) { r.Scenarios = nil }},
		{"missing", func(r *realBenchResult) { r.Scenarios = r.Scenarios[:3] }},
		{"duplicate", func(r *realBenchResult) { r.Scenarios[1] = r.Scenarios[0] }},
		{"extra_duplicate", func(r *realBenchResult) { r.Scenarios = append(r.Scenarios, r.Scenarios[0]) }},
		{"unknown", func(r *realBenchResult) { r.Scenarios[0].Deterministic.Name = "unknown" }},
		{"missing_constraints", func(r *realBenchResult) { r.Constraints = nil }},
		{"weakened_constraint", func(r *realBenchResult) { r.Constraints["provenance_admission_units"]-- }},
		{"inflated_constraint", func(r *realBenchResult) { r.Constraints["per_execution_units"]++ }},
		{"unknown_constraint", func(r *realBenchResult) { r.Constraints["anything"] = 1 }},
		{"unknown_profile", func(r *realBenchResult) { r.Profile = "tiny" }},
		{"zero_repeats", func(r *realBenchResult) { r.Repeats = 0 }},
		{"negative_repeats", func(r *realBenchResult) { r.Repeats = -1 }},
		{"nonhex_root", func(r *realBenchResult) { r.Scenarios[0].Deterministic.AppHash = strings.Repeat("g", 64) }},
		{"short_root", func(r *realBenchResult) { r.Scenarios[0].Deterministic.AppHash = "ab" }},
		{"uppercase_root", func(r *realBenchResult) { r.Scenarios[0].Deterministic.AppHash = strings.Repeat("A", 64) }},
		{"zero_root", func(r *realBenchResult) { r.Scenarios[0].Deterministic.AppHash = strings.Repeat("0", 64) }},
		{"empty_keys", func(r *realBenchResult) { r.Scenarios[0].Deterministic.Keys = 0 }},
		{"short_key_bytes", func(r *realBenchResult) { r.Scenarios[0].Deterministic.KeyBytes = 1 }},
		{"empty_values", func(r *realBenchResult) { r.Scenarios[0].Deterministic.ValueBytes = 0 }},
		{"negative_counter", func(r *realBenchResult) { r.Scenarios[0].Deterministic.Scanned = -1 }},
		{"scope_clear", func(r *realBenchResult) { r.Scenarios[0].Deterministic.Outcome = "clear" }},
		{"scope_no_match", func(r *realBenchResult) { r.Scenarios[0].Deterministic.DecisionIDs = 0 }},
		{"lineage_no_walk", func(r *realBenchResult) { r.Scenarios[1].Deterministic.Nodes = 0 }},
		{"lineage_clear", func(r *realBenchResult) { r.Scenarios[1].Deterministic.Outcome = "clear" }},
		{"admission_not_worst_bound", func(r *realBenchResult) { r.Scenarios[2].Deterministic.Decisions-- }},
		{"admission_wrong_units", func(r *realBenchResult) { r.Scenarios[2].Deterministic.Units-- }},
		{"fanout_no_progress", func(r *realBenchResult) { r.Scenarios[3].Deterministic.Materialized = 0 }},
		{"fanout_no_scan", func(r *realBenchResult) { r.Scenarios[3].Deterministic.Scanned = 0 }},
		{"fanout_no_enqueue", func(r *realBenchResult) { r.Scenarios[3].Deterministic.Enqueued = 0 }},
		{"fanout_over_budget", func(r *realBenchResult) { r.Scenarios[3].Deterministic.Units = taint.PerExecutionUnitBudget + 1 }},
		{"fanout_over_membership_cap", func(r *realBenchResult) {
			r.Scenarios[3].Deterministic.Materialized = taint.MaxNewMembershipsPerExec + 1
		}},
		{"fanout_unknown_status", func(r *realBenchResult) { r.Scenarios[3].Deterministic.Status = "unknown" }},
		{"mixed_shape", func(r *realBenchResult) { r.Scenarios[2].Deterministic.Outcome = "tainted" }},
		{"missing_timing", func(r *realBenchResult) { r.Scenarios[0].Timing = timingSummary{} }},
		{"invalid_timing_order", func(r *realBenchResult) { r.Scenarios[0].Timing.MinMS = r.Scenarios[0].Timing.MaxMS + 1 }},
		{"unknown_source", func(r *realBenchResult) { r.Commit.SHA = "unknown" }},
		{"dirty_source", func(r *realBenchResult) { r.Commit.Dirty = true }},
		{"wrong_source", func(r *realBenchResult) { r.Commit.SHA = strings.Repeat("b", 40) }},
		{"same_arch", func(r *realBenchResult) { r.Machine.GOARCH, r.Machine.UnameM = "amd64", "x86_64" }},
		{"inconsistent_arch", func(r *realBenchResult) { r.Machine.UnameM = "x86_64" }},
		{"missing_machine", func(r *realBenchResult) { r.Machine.CPUs = 0 }},
		{"serialized_failure", func(r *realBenchResult) { r.Invariants.Passed = false }},
		{"serialized_failure_list", func(r *realBenchResult) { r.Invariants.Failures = []string{"failed"} }},
		{"wrong_schema", func(r *realBenchResult) { r.Schema = "other" }},
		{"not_real_code", func(r *realBenchResult) { r.RealCode = false }},
		{"different_repeats", func(r *realBenchResult) { r.Repeats++ }},
		{"different_valid_profile", func(r *realBenchResult) { r.Profile = "full" }},
		{"different_root", func(r *realBenchResult) { r.Scenarios[0].Deterministic.AppHash = strings.Repeat("c", 64) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			changed := clone()
			tc.change(&changed)
			// Re-seal the tampered evidence: a valid digest and claimed Passed
			// must not rescue absent scenarios or failed actual invariants.
			changed = seal(changed)
			require.Error(t, compareResults([]string{writeTestResult(t, changed), amdPath}))
		})
	}
	t.Run("digest_tamper", func(t *testing.T) {
		changed := arm
		changed.DeterministicDigest = strings.Repeat("0", 64)
		require.ErrorContains(t, compareResults([]string{writeTestResult(t, changed), amdPath}), "does not match its evidence")
	})
	validJSON, err := json.Marshal(arm)
	require.NoError(t, err)
	malformed := []string{
		"", "null", "[]", `"text"`, "{", string(validJSON) + " {}",
		strings.Replace(string(validJSON), `"scenarios":[`, `"scenarios":{`, 1),
		strings.Replace(string(validJSON), `"constraints":{`, `"constraints":{ "unknown": 1,`, 1),
		strings.Replace(string(validJSON), `"schema":`, `"unexpected":true,"schema":`, 1),
		strings.Replace(string(validJSON), `"deterministic":{`, `"deterministic":{"unknown":true,`, 1),
	}
	for index, raw := range malformed {
		t.Run(fmt.Sprintf("malformed_json_%d", index), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "invalid.json")
			require.NoError(t, os.WriteFile(path, []byte(raw), 0o600))
			require.Error(t, compareResults([]string{path, amdPath}))
		})
	}
}

func TestSummarizeDurationsUsesNearestRankP95(t *testing.T) {
	summary := summarizeDurations([]time.Duration{
		5 * time.Millisecond,
		1 * time.Millisecond,
		3 * time.Millisecond,
		2 * time.Millisecond,
		4 * time.Millisecond,
	})
	require.Equal(t, 1.0, summary.MinMS)
	require.Equal(t, 3.0, summary.P50MS)
	require.Equal(t, 5.0, summary.P95MS)
	require.Equal(t, 5.0, summary.MaxMS)
	require.Equal(t, 3.0, summary.MeanMS)
}

func writeTestResult(t *testing.T, result realBenchResult) string {
	t.Helper()
	raw, err := json.Marshal(result)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), result.Machine.GOARCH+".json")
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	return path
}
