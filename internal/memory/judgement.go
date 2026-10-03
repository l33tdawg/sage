package memory

import "errors"

// SemanticVerdict is the optional memory gate's stored answer for one memory
// (see internal/voter.Gate): a node-local annotation, never consensus state.
type SemanticVerdict struct {
	Verdict string  // VerdictPass, VerdictReject or VerdictAbstain
	P       float64 // the probability the threshold was applied to
	Reason  string
}

// ErrEvidenceExpired means a memory was submitted with evidence that this node
// has since deleted (its claim outlived the retention grace before the memory
// arrived). It is NOT "no evidence": the memory's support can no longer be
// judged, so the gate holds it for operator review.
var ErrEvidenceExpired = errors.New("the evidence submitted with this memory expired before the memory arrived")

// Gate verdicts and review decisions.
const (
	VerdictPass    = "pass"
	VerdictReject  = "reject"
	VerdictAbstain = "abstain"
	VerdictAccept  = "accept"
)
