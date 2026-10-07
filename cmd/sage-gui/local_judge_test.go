package main

import (
	"testing"

	"github.com/l33tdawg/sage/internal/hunch"
	"github.com/l33tdawg/sage/internal/ollamad"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestLocalJudge_OffUnlessAModelIsNamed(t *testing.T) {
	t.Setenv("SAGE_LOCAL_JUDGE_MODEL", "")
	require.Nil(t, localJudgeFromEnv(zerolog.Nop(), "http://127.0.0.1:11434"))
	t.Setenv("SAGE_LOCAL_JUDGE_MODEL", "judge-build")
	require.Nil(t, localJudgeFromEnv(zerolog.Nop(), ""), "without a runtime address there is nothing to ask")
}

func TestLocalJudge_BuildsBothChecksAgainstTheLocalRuntime(t *testing.T) {
	t.Setenv("SAGE_LOCAL_JUDGE_MODEL", "judge-build")
	g := localJudgeFromEnv(zerolog.Nop(), "http://127.0.0.1:11434/")
	require.NotNil(t, g)
	require.Len(t, g.Judges, 1)
	require.Len(t, g.SupportJudges, 1, "a local judge answers the evidence check too")
	require.NotEmpty(t, g.Version)
	require.NotContains(t, g.Version, "11434", "the cache version must not carry the address verbatim")
}

func TestLocalJudgeVersion_TracksWhatChangesTheAnswer(t *testing.T) {
	base := localJudgeVersion("m", "http://127.0.0.1:11434", "")
	require.Equal(t, base, localJudgeVersion("m", "http://127.0.0.1:11434/", ""), "a trailing slash is the same runtime")
	require.NotEqual(t, base, localJudgeVersion("m2", "http://127.0.0.1:11434", ""), "a different model re-judges")
	require.NotEqual(t, base, localJudgeVersion("m", "http://127.0.0.1:11435", ""), "a different runtime re-judges")
	require.NotEqual(t, base, localJudgeVersion("m", "http://127.0.0.1:11434", "revision-2"), "an operator revision re-judges")
}

func TestLocalJudge_IsASeparateNamespaceFromTheServiceJudge(t *testing.T) {
	local := localJudgeVersion("m", "http://127.0.0.1:11434", "")
	remote := gateVersion("lead", []string{"m"}, judgeServiceIdentity("https://judge.example/v1"), "")
	require.NotEqual(t, local, remote, "switching a node between judge kinds must not reuse verdicts")
}

func TestLocalJudge_DebiasInvalidatesVerdictCache(t *testing.T) {
	t.Setenv("SAGE_LOCAL_JUDGE_MODEL", "judge-build")
	t.Setenv("SAGE_LOCAL_JUDGE_DEBIAS", "0")
	one := localJudgeFromEnv(zerolog.Nop(), "http://127.0.0.1:11434")
	t.Setenv("SAGE_LOCAL_JUDGE_DEBIAS", "1")
	both := localJudgeFromEnv(zerolog.Nop(), "http://127.0.0.1:11434")
	require.NotEqual(t, one.Version, both.Version)
}

func TestLocalJudge_ManagedModelPinsTheRegisteredBlob(t *testing.T) {
	t.Setenv("SAGE_LOCAL_JUDGE_MODEL", ollamad.JudgeModelTag)
	gate := localJudgeFromEnv(zerolog.Nop(), "http://127.0.0.1:11434")
	require.NotNil(t, gate)
	lasting, ok := gate.Judges[0].(hunch.LastingJudge)
	require.True(t, ok)
	client, ok := lasting.Client.(*hunch.LocalClient)
	require.True(t, ok)
	require.Equal(t, ollamad.JudgeModelBlobSHA256, client.ExpectedGGUFSHA256)
	require.NotEqual(t, ollamad.JudgeGGUFSHA256, client.ExpectedGGUFSHA256, "the runtime rewrites the downloaded GGUF during registration")
}
