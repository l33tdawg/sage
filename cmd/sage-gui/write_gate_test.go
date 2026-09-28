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
		"SAGE_HUNCH_JUDGE_REVISION", "SAGE_HUNCH_INCLUDE_DOMAINS", "SAGE_HUNCH_EXEMPT_DOMAINS", "SAGE_HUNCH_EVIDENCE"} {
		t.Setenv(k, env[k])
	}
	g := writeGateFromEnv(zerolog.Nop())
	require.NotNil(t, g)
	return g.Version
}

func TestGateVersion_ChangesWithTheJudgeService(t *testing.T) {
	a := gateVersionFromEnv(t, map[string]string{"SAGE_HUNCH_URL": "http://127.0.0.1:8791"})
	b := gateVersionFromEnv(t, map[string]string{"SAGE_HUNCH_URL": "http://127.0.0.1:8792"})
	require.NotEqual(t, a, b, "with no models configured, switching the service must invalidate cached verdicts")
	require.Equal(t, a, gateVersionFromEnv(t, map[string]string{"SAGE_HUNCH_URL": "http://127.0.0.1:8791/"}),
		"the same service is the same identity")
}

func TestGateVersion_NeverDependsOnCredentials(t *testing.T) {
	withoutKey := gateVersionFromEnv(t, map[string]string{"SAGE_HUNCH_URL": "http://127.0.0.1:8791"})
	withKey := gateVersionFromEnv(t, map[string]string{"SAGE_HUNCH_URL": "http://127.0.0.1:8791", "SAGE_HUNCH_API_KEY": "sk-must-not-appear"})
	require.Equal(t, withoutKey, withKey)
	require.NotContains(t, withKey, "sk-must-not-appear")
	// Invalid credential-bearing configuration is refused separately; the
	// service identity helper must still never retain those credentials.
	plain := judgeServiceIdentity("https://localhost/v")
	withSecrets := judgeServiceIdentity("https://user:hunter2@localhost/v?token=abc123")
	require.Equal(t, plain, withSecrets)
	for _, secret := range []string{"hunter2", "abc123", "user"} {
		require.False(t, strings.Contains(withSecrets, secret))
	}
}

func TestWriteGateFromEnv_RejectsNonLocalJudge(t *testing.T) {
	for _, raw := range []string{"https://judge.example", "http://192.168.1.2:8791", "http://0.0.0.0:8791", "https://user:secret@localhost:8791"} {
		t.Run(raw, func(t *testing.T) {
			t.Setenv("SAGE_HUNCH_URL", raw)
			require.Nil(t, writeGateFromEnv(zerolog.Nop()))
		})
	}
}

func TestGateVersion_OperatorRevisionAndSettingsInvalidate(t *testing.T) {
	base := map[string]string{"SAGE_HUNCH_URL": "http://127.0.0.1:8791"}
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

func TestWriteGateFromEnv_EvidenceCheckIsOnByDefaultAndCanBeTurnedOff(t *testing.T) {
	t.Setenv("SAGE_HUNCH_URL", "http://127.0.0.1:8791")
	t.Setenv("SAGE_HUNCH_MODELS", "m1,m2")
	t.Setenv("SAGE_HUNCH_EVIDENCE", "")
	on := writeGateFromEnv(zerolog.Nop())
	require.Len(t, on.SupportJudges, 2, "every configured model also judges evidence")
	t.Setenv("SAGE_HUNCH_EVIDENCE", "off")
	off := writeGateFromEnv(zerolog.Nop())
	require.Empty(t, off.SupportJudges)
	require.Len(t, off.Judges, 2)
	require.NotEqual(t, on.Version, off.Version, "turning the evidence check off re-judges pending memories")
}
