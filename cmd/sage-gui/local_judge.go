package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/hunch"
	"github.com/l33tdawg/sage/internal/ollamad"
	"github.com/l33tdawg/sage/internal/voter"
)

// localJudgeFromEnv builds the memory gate against the model THIS node serves through its own
// managed Ollama runtime (internal/ollamad). It is the alternative to pointing the gate at a judge
// service, and exists so a node can judge memories with nothing leaving it and nothing else to
// install: no judge URL to configure, no second process, no inherited provider settings.
//
//	SAGE_LOCAL_JUDGE_MODEL   the served model to ask, e.g. the pinned judge build. Setting this is
//	                         what turns the local judge on.
//	SAGE_LOCAL_JUDGE_REVISION free-form tag folded into the verdict-cache version; change it when
//	                         the same model name starts serving different weights
//	SAGE_LOCAL_JUDGE_DEBIAS  "1" asks every check in both answer orders and averages (2x calls)
//	SAGE_LOCAL_JUDGE_TIMEOUT per-memory judge budget, e.g. "60s"
//
// The gate is otherwise unchanged: the built-in checks always run fresh, a verdict is cached per
// judge version, and a judgement that cannot be made — the model answered something other than a
// label, or the label probabilities were not where they had to be — HOLDS the memory for review
// rather than falling back to the built-in checks, which would accept it.
func localJudgeFromEnv(logger zerolog.Logger, ollamaURL string) *voter.Gate {
	model := strings.TrimSpace(os.Getenv("SAGE_LOCAL_JUDGE_MODEL"))
	if model == "" || ollamaURL == "" {
		return nil
	}
	timeout := 60 * time.Second
	if raw := os.Getenv("SAGE_LOCAL_JUDGE_TIMEOUT"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			timeout = d
		} else {
			logger.Warn().Str("SAGE_LOCAL_JUDGE_TIMEOUT", raw).Msg("invalid local judge timeout — using 60s")
		}
	}
	debias := os.Getenv("SAGE_LOCAL_JUDGE_DEBIAS") == "1"
	client := &hunch.LocalClient{
		BaseURL: strings.TrimRight(ollamaURL, "/") + "/v1",
		Model:   model, Timeout: timeout, Debias: debias,
	}
	if model == ollamad.JudgeModelTag {
		client.ExpectedGGUFSHA256 = ollamad.JudgeModelBlobSHA256
	}
	logger.Info().
		Str("ollama", ollamaURL).Str("model", model).Bool("debias", debias).
		Str("verdict_cache", localJudgeVersion(model, ollamaURL, os.Getenv("SAGE_LOCAL_JUDGE_REVISION"))+"|debias="+strconv.FormatBool(debias)).
		Msg("memory gate ON with the LOCAL judge — proposed memories are judged by the model this node serves; nothing leaves the node")
	return &voter.Gate{
		Judges:        []voter.LastingJudge{hunch.LastingJudge{Client: client}},
		SupportJudges: []voter.SupportJudge{hunch.SupportJudge{Client: client}},
		Timeout:       timeout,
		Version:       localJudgeVersion(model, ollamaURL, os.Getenv("SAGE_LOCAL_JUDGE_REVISION")) + "|debias=" + strconv.FormatBool(debias),
	}
}

// localJudgeVersion is the namespace verdicts are cached under for a local judge. It changes when
// the model, the runtime address or an operator revision changes. It never contains credentials —
// the local runtime has none — and it is deliberately not the same namespace as a remote judge's,
// so switching a node between the two re-judges rather than reusing verdicts from a different
// questioner.
func localJudgeVersion(model, ollamaURL, revision string) string {
	sum := sha256.Sum256([]byte(hunch.ChecksVersion + "|local-loopback-hold-v2|" + model + "|" + strings.TrimRight(ollamaURL, "/")))
	v := "local:" + hex.EncodeToString(sum[:])[:12]
	if r := strings.TrimSpace(revision); r != "" {
		v += "|rev=" + r
	}
	return v
}
