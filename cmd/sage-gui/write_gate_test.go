package main

import (
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func gateVersionFromEnv(t *testing.T, env map[string]string) string {
	t.Helper()
	for _, k := range []string{"SAGE_HUNCH_URL", "SAGE_HUNCH_API_KEY", "SAGE_HUNCH_MODELS", "SAGE_HUNCH_POLICY",
		"SAGE_HUNCH_JUDGE_REVISION", "SAGE_HUNCH_INCLUDE_DOMAINS", "SAGE_HUNCH_EXEMPT_DOMAINS"} {
		t.Setenv(k, env[k])
	}
	g := writeGateFromEnv(zerolog.Nop())
	require.NotNil(t, g)
	return g.Version
}

func TestGateVersion_ChangesWithTheJudgeService(t *testing.T) {
	a := gateVersionFromEnv(t, map[string]string{"SAGE_HUNCH_URL": "http://judge-a.local:8791"})
	b := gateVersionFromEnv(t, map[string]string{"SAGE_HUNCH_URL": "http://judge-b.local:8791"})
	require.NotEqual(t, a, b, "with no models configured, switching the service must invalidate cached verdicts")
	require.Equal(t, a, gateVersionFromEnv(t, map[string]string{"SAGE_HUNCH_URL": "http://judge-a.local:8791/"}),
		"the same service is the same identity")
}

func TestGateVersion_NeverDependsOnCredentials(t *testing.T) {
	plain := gateVersionFromEnv(t, map[string]string{"SAGE_HUNCH_URL": "https://judge.local/v"})
	withSecrets := gateVersionFromEnv(t, map[string]string{
		"SAGE_HUNCH_URL":     "https://user:hunter2@judge.local/v?token=abc123",
		"SAGE_HUNCH_API_KEY": "sk-should-never-appear",
	})
	require.Equal(t, plain, withSecrets, "credentials, query and API key are not part of the identity")
	for _, secret := range []string{"hunter2", "abc123", "sk-should-never-appear", "user"} {
		require.False(t, strings.Contains(withSecrets, secret), "the cache version leaks %q", secret)
	}
}

func TestGateVersion_OperatorRevisionAndSettingsInvalidate(t *testing.T) {
	base := map[string]string{"SAGE_HUNCH_URL": "http://judge.local:8791"}
	v0 := gateVersionFromEnv(t, base)
	bumped := gateVersionFromEnv(t, map[string]string{"SAGE_HUNCH_URL": base["SAGE_HUNCH_URL"], "SAGE_HUNCH_JUDGE_REVISION": "2026-10-model-swap"})
	require.NotEqual(t, v0, bumped, "a revision bump re-judges when a service's default model changes")
	models := gateVersionFromEnv(t, map[string]string{"SAGE_HUNCH_URL": base["SAGE_HUNCH_URL"], "SAGE_HUNCH_MODELS": "m1,m2"})
	require.NotEqual(t, v0, models)
	policy := gateVersionFromEnv(t, map[string]string{"SAGE_HUNCH_URL": base["SAGE_HUNCH_URL"], "SAGE_HUNCH_POLICY": "all"})
	require.NotEqual(t, v0, policy)
	scoped := gateVersionFromEnv(t, map[string]string{"SAGE_HUNCH_URL": base["SAGE_HUNCH_URL"], "SAGE_HUNCH_INCLUDE_DOMAINS": "projects."})
	require.Equal(t, v0, scoped, "scope decides WHICH memories are judged, not HOW; it does not invalidate verdicts")
}

func TestDisplayJudgeURL_StripsCredentials(t *testing.T) {
	require.Equal(t, "https://judge.local/v", displayJudgeURL("https://user:hunter2@judge.local/v?token=abc#x"))
}
