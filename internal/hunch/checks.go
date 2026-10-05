package hunch

import "context"

// The checks SAGE's memory gate asks. Each names its look-alikes in yes_if /
// no_if; the wording is part of the contract (changing it changes the judge's
// accuracy), so ChecksVersion is recorded with every judgement and must be
// bumped whenever any wording changes.
const ChecksVersion = "sage-lasting/1+supported/1"

// Lasting asks whether a memory belongs in long-term memory at all. The
// failure it targets: an agent stores a remark about its own session ("the
// attachment was lost, ask the user to re-send the numbers") as a fact, and
// later sessions recall it as if it were true of the world. Standing rules and
// methods are legitimate lasting memories and are named as such, so procedural
// memories are not rejected for being instruction-shaped.
var Lasting = Check{
	Kind: "yesno",
	Question: "Should this be stored as lasting memory: does it state something about the world, " +
		"or a standing rule or method, that stays true outside the conversation it came from?",
	YesIf: "a fact about people, places, systems or events, or a standing rule, convention or method " +
		"a later reader should follow",
	NoIf: "a statement about the current conversation or session itself: a lost or unreadable message " +
		"or attachment, truncated context, something that could not be done just now, a request to re-send",
}

// JudgeBackend is one question put to one model. Both backends satisfy it: the
// Hunch service client (remote, vLLM-shaped) and the local client (a model
// served on this machine, e.g. SAGE's own Ollama). The gate depends only on the
// adapters below, so a node can be pointed at either without other changes.
type JudgeBackend interface {
	YesNo(ctx context.Context, judgeContext any, checks map[string]Check) (map[string]float64, error)
}

// LastingJudge adapts a judge backend to the memory gate's provider-neutral
// judge interface (internal/voter.LastingJudge): it asks the Lasting check and
// returns p_yes. Only the memory's content is sent — no id, domain, author or
// other metadata.
type LastingJudge struct {
	Client JudgeBackend
}

// LastingProbability returns the probability that content is lasting memory.
func (j LastingJudge) LastingProbability(ctx context.Context, content string) (float64, error) {
	ps, err := j.Client.YesNo(ctx, map[string]string{"memory": content}, map[string]Check{"lasting": Lasting})
	if err != nil {
		return 0, err
	}
	return ps["lasting"], nil
}

// Supported asks whether a memory is backed by the evidence submitted with it.
// It is only ever asked WITH the evidence: without it the question degrades
// into "does this sound plausible", which is not a confidence.
//
// It names the two look-alikes that most often pass a plain "is it supported"
// question: evidence about an EARLIER time for a claim stated as current ("at
// the last inspection the alarm was disabled" -> "the alarm is disabled"), and
// evidence that only REPORTS what someone says for a claim stated as fact. A
// memory that keeps the evidence's time frame or attribution is supported.
var Supported = Check{
	Kind:     "yesno",
	Question: "Is the assertion in `memory` supported by `evidence`, as stated — including its time frame and certainty?",
	YesIf: "`evidence` states or directly shows what `memory` asserts; if `memory` keeps the evidence's time frame " +
		"(\"at the last inspection\", \"in 2024\") or its attribution (\"according to the operator\"), that still counts",
	NoIf: "`evidence` is about something else, only makes it plausible, contradicts it, or supports a weaker or different " +
		"claim than `memory` makes; or `evidence` describes an earlier time (a past inspection, report, date or \"at the time\") " +
		"while `memory` states it as true now; or `evidence` only reports what someone says, claims or believes " +
		"(\"reportedly\", \"according to\", \"X said\") while `memory` states it as fact",
}

// SupportJudge adapts a judge backend to the memory gate's evidence judge
// interface (internal/voter.SupportJudge): it asks the Supported check with the
// memory and its evidence and returns p_yes. Only those two texts are sent.
type SupportJudge struct {
	Client JudgeBackend
}

// SupportedProbability returns the probability that evidence supports content.
func (j SupportJudge) SupportedProbability(ctx context.Context, content, evidence string) (float64, error) {
	ps, err := j.Client.YesNo(ctx, map[string]string{"memory": content, "evidence": evidence},
		map[string]Check{"supported": Supported})
	if err != nil {
		return 0, err
	}
	return ps["supported"], nil
}
