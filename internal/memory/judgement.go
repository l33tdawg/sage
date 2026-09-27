package memory

// SemanticVerdict is the optional memory gate's stored answer for one memory
// (see internal/voter.Gate): a node-local annotation, never consensus state.
type SemanticVerdict struct {
	Verdict string  // VerdictPass, VerdictReject or VerdictAbstain
	P       float64 // the probability the threshold was applied to
	Reason  string
}

// Gate verdicts and review decisions.
const (
	VerdictPass    = "pass"
	VerdictReject  = "reject"
	VerdictAbstain = "abstain"
	VerdictAccept  = "accept"
)
