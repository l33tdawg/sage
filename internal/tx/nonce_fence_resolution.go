package tx

import (
	"sync"
	"time"
)

// FenceResolution is the LAST fence outcome this process observed, kept so a
// hold that ended is still visible afterwards.
//
// WHY THIS EXISTS. Everything else about a fence is either live or transient:
// the held-fence block disappears the moment the fence lifts, the status row
// goes with it, and the log line scrolls away. That leaves the operator unable
// to tell "the hold resolved itself while I was reading" from "the hold never
// happened" — and the first is the outcome the whole mechanism is trying to
// produce, so it is worth keeping in view.
//
// It records what ended the fence, the identifiers triage needs, and how long
// the key had been held. It is a DIAGNOSTIC, never a gate: nothing reads it to
// decide whether a key may sign.
type FenceResolution struct {
	// Mode is the resolution: a fate label (committed, rejected, spent,
	// abandoned) for a lift, or abandoned:<route> for an operator or automatic
	// decision with no proof behind it.
	Mode               string
	SignerPubKeyPrefix string
	TxHash             string
	Nonce              uint64
	HasNonce           bool
	HeldFor            time.Duration
	At                 time.Time
	Detail             string
}

var (
	lastFenceResolutionMu sync.Mutex
	lastFenceResolution   *FenceResolution
)

// recordFenceResolution stores the most recent outcome. Latest wins: the panel
// answers "what happened to the fence I was just looking at", not "list every
// fence that ever lifted" — the log is the audit trail for that.
func recordFenceResolution(
	mode, signerPrefix, txHash string,
	nonce uint64,
	hasNonce bool,
	heldFor time.Duration,
	detail string,
) {
	record := FenceResolution{
		Mode:               mode,
		SignerPubKeyPrefix: signerPrefix,
		TxHash:             txHash,
		Nonce:              nonce,
		HasNonce:           hasNonce,
		HeldFor:            heldFor,
		At:                 time.Now(),
		Detail:             detail,
	}
	lastFenceResolutionMu.Lock()
	lastFenceResolution = &record
	lastFenceResolutionMu.Unlock()
}

// LastFenceResolution reports the most recent fence outcome in this process, if
// any fence has been resolved since it started.
func LastFenceResolution() (FenceResolution, bool) {
	lastFenceResolutionMu.Lock()
	defer lastFenceResolutionMu.Unlock()
	if lastFenceResolution == nil {
		return FenceResolution{}, false
	}
	return *lastFenceResolution, true
}
