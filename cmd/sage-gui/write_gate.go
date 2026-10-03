package main

import (
	"crypto/sha256"
	"encoding/hex"
	neturl "net/url"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/hunch"
	"github.com/l33tdawg/sage/internal/voter"
)

// writeGateFromEnv builds the optional memory gate (internal/voter.Gate) from
// the environment. It is OFF unless SAGE_HUNCH_URL is set.
//
//	SAGE_HUNCH_URL           loopback URL of a local Hunch service (POST /v1/judge)
//	SAGE_HUNCH_API_KEY       bearer key for it, if the service requires one
//	SAGE_HUNCH_MODELS        comma-separated judge models, the FIRST leading
//	                         (empty = the service's default model, one judge)
//	SAGE_HUNCH_POLICY        "lead" (default: first judge decides, any other can
//	                         veto) or "all" (every judge must agree)
//	SAGE_HUNCH_INCLUDE_DOMAINS comma-separated domain prefixes to judge; when
//	                         set, ONLY these domains' memory content is sent
//	SAGE_HUNCH_EXEMPT_DOMAINS comma-separated domain prefixes never judged (e.g.
//	                         program-written catalogs); their content is not sent
//	SAGE_HUNCH_EVIDENCE      "off" disables the evidence check; by default a
//	                         memory submitted with evidence is also asked
//	                         whether the evidence supports it (every judge
//	                         must agree), and the evidence text is sent too
//	SAGE_HUNCH_TIMEOUT       per-memory judge budget, e.g. "60s"
//	SAGE_HUNCH_JUDGE_REVISION free-form tag folded into the verdict-cache
//	                         version; change it when a judge service's default
//	                         model changes behind the same URL, to re-judge
func writeGateFromEnv(logger zerolog.Logger) *voter.Gate {
	url := strings.TrimSpace(os.Getenv("SAGE_HUNCH_URL"))
	if url == "" {
		return nil
	}
	key := os.Getenv("SAGE_HUNCH_API_KEY")
	timeout := 60 * time.Second
	if raw := os.Getenv("SAGE_HUNCH_TIMEOUT"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			timeout = d
		} else {
			logger.Warn().Str("SAGE_HUNCH_TIMEOUT", raw).Msg("invalid write-gate timeout — using 60s")
		}
	}
	var models []string
	for _, m := range strings.Split(os.Getenv("SAGE_HUNCH_MODELS"), ",") {
		if m = strings.TrimSpace(m); m != "" {
			models = append(models, m)
		}
	}
	if len(models) == 0 {
		models = []string{""}
	}
	g := &voter.Gate{Timeout: timeout}
	evidenceCheck := !strings.EqualFold(strings.TrimSpace(os.Getenv("SAGE_HUNCH_EVIDENCE")), "off")
	for _, m := range models {
		client := hunch.New(url, key, m, timeout)
		g.Judges = append(g.Judges, hunch.LastingJudge{Client: client})
		if evidenceCheck {
			g.SupportJudges = append(g.SupportJudges, hunch.SupportJudge{Client: client})
		}
	}
	g.Policy = strings.TrimSpace(os.Getenv("SAGE_HUNCH_POLICY"))
	g.ExemptDomainPrefixes = splitList(os.Getenv("SAGE_HUNCH_EXEMPT_DOMAINS"))
	g.IncludeDomainPrefixes = splitList(os.Getenv("SAGE_HUNCH_INCLUDE_DOMAINS"))
	policy := g.Policy
	if policy != voter.PolicyAll {
		policy = voter.PolicyLead
	}
	g.Version = gateVersion(policy, models, judgeServiceIdentity(url), os.Getenv("SAGE_HUNCH_JUDGE_REVISION"))
	if !evidenceCheck {
		g.Version += "|evidence=off"
	}
	scope := "every domain"
	if len(g.IncludeDomainPrefixes) > 0 {
		scope = "domains " + strings.Join(g.IncludeDomainPrefixes, ", ")
	}
	shown := displayJudgeURL(url)
	logger.Info().Str("hunch_url", shown).Strs("judges", models).Str("policy", policy).Str("verdict_cache", g.Version).
		Strs("include_domains", g.IncludeDomainPrefixes).Strs("exempt_domains", g.ExemptDomainPrefixes).
		Bool("evidence_check", evidenceCheck).
		Msg("memory gate ON — the CONTENT of proposed memories in " + scope +
			" (minus exempt domains), and the evidence submitted with them when the evidence check is on," +
			" is sent to " + shown + " to be judged before this node votes; no ids, authors or other metadata")
	return g
}

func splitList(raw string) []string {
	var out []string
	for _, d := range strings.Split(raw, ",") {
		if d = strings.TrimSpace(d); d != "" {
			out = append(out, d)
		}
	}
	return out
}

// gateVersion is the namespace verdicts are cached under. It changes whenever
// anything that could change a judgement changes: the check wording, the
// policy, the configured models, the judge SERVICE (models may be empty, in
// which case the service's own default model decides), or an operator-set
// revision. It never contains credentials.
func gateVersion(policy string, models []string, serviceID, revision string) string {
	v := hunch.ChecksVersion + "|loopback-hold-v1|" + policy + ":" + strings.Join(models, "+") + "|svc=" + serviceID
	if r := strings.TrimSpace(revision); r != "" {
		v += "|rev=" + r
	}
	return v
}

// judgeServiceIdentity is a stable, credential-free identity for the judge
// service: a short hash of scheme, host and path only — no userinfo, query or
// fragment, and never the API key.
func judgeServiceIdentity(raw string) string {
	sum := sha256.Sum256([]byte(canonicalJudgeURL(raw)))
	return hex.EncodeToString(sum[:])[:12]
}

func canonicalJudgeURL(raw string) string {
	u, err := neturl.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return "unparseable"
	}
	return strings.ToLower(u.Scheme) + "://" + strings.ToLower(u.Host) + strings.TrimRight(u.Path, "/")
}

// displayJudgeURL is the judge URL as it may be logged or shown: without
// userinfo, query or fragment.
func displayJudgeURL(raw string) string { return canonicalJudgeURL(raw) }
