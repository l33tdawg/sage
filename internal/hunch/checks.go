package hunch

import "context"

// The check SAGE's memory gate asks. It names its look-alikes in yes_if /
// no_if; the wording is part of the contract (changing it changes the judge's
// accuracy), so ChecksVersion is recorded with every judgement and must be
// bumped whenever the wording changes.
const ChecksVersion = "sage-lasting/1"

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

// LastingJudge adapts a Hunch service to the memory gate's provider-neutral
// judge interface (internal/voter.LastingJudge): it asks the Lasting check and
// returns p_yes. Only the memory's content is sent — no id, domain, author or
// other metadata.
type LastingJudge struct {
	Client *Client
}

// LastingProbability returns the probability that content is lasting memory.
func (j LastingJudge) LastingProbability(ctx context.Context, content string) (float64, error) {
	ps, err := j.Client.YesNo(ctx, map[string]string{"memory": content}, map[string]Check{"lasting": Lasting})
	if err != nil {
		return 0, err
	}
	return ps["lasting"], nil
}
