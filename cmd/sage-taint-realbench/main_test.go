package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	base := realBenchResult{
		Schema:              resultSchema,
		RealCode:            true,
		Profile:             "smoke",
		Invariants:          invariants{Passed: true},
		DeterministicDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	arm := base
	arm.Machine.GOARCH = "arm64"
	amd := base
	amd.Machine.GOARCH = "amd64"
	digest, err := deterministicDigest(&arm)
	require.NoError(t, err)
	arm.DeterministicDigest = digest
	amd.DeterministicDigest = digest
	armPath := writeTestResult(t, arm)
	amdPath := writeTestResult(t, amd)
	require.NoError(t, compareResults([]string{armPath, amdPath}))

	amd.Scenarios = []scenarioResult{{
		Deterministic: deterministicScenario{Name: "scope", Units: 164},
	}}
	amd.DeterministicDigest, err = deterministicDigest(&amd)
	require.NoError(t, err)
	mismatchPath := writeTestResult(t, amd)
	require.ErrorContains(t, compareResults([]string{armPath, mismatchPath}), "digest mismatch")
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
