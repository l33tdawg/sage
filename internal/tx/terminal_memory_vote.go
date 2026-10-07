package tx

// CheckTxCommittedMemoryVoteCodespace identifies a narrow, permanent refusal,
// not the general code-13 memory-vote rejection. A node may emit this only for
// a vote whose canonical target was submitted after app-v25 activation and is
// now committed. App-v25 prevents that envelope from returning to proposed;
// challenges/reinstatement and co-commit reclamation cannot reopen its ballot.
// Historical, hashless, missing and merely ineligible targets do not qualify.
// As with the nonce-floor proof, an out-of-band canonical-state rewind is not
// covered by this claim.
const CheckTxCommittedMemoryVoteCodespace = "sage/memory-vote-committed/v1"

const CheckTxCommittedMemoryVoteCode = 13

// The surrounding resolver binds the response hash to encoded first. Check
// the type and signature as well: this proof says nothing about other actions,
// even if a peer incorrectly attaches the codespace to their response.
func isCommittedMemoryVoteRefusal(code int, codespace string, encoded []byte) bool {
	if code != CheckTxCommittedMemoryVoteCode || codespace != CheckTxCommittedMemoryVoteCodespace {
		return false
	}
	parsed, err := DecodeTx(encoded)
	if err != nil || parsed.Type != TxTypeMemoryVote || parsed.MemoryVote == nil || parsed.MemoryVote.MemoryID == "" {
		return false
	}
	valid, err := VerifyTx(parsed)
	return err == nil && valid
}
