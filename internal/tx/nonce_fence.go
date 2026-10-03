package tx

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/l33tdawg/sage/internal/metrics"
)

// This file is the SIGNER FENCE: the half of the nonce lease that handles the
// one outcome the lease alone cannot survive — a submission whose fate this
// process never observed.
//
// The failure it exists to stop: WithNonceLease releases the key's slot the
// instant submit returns. When submit returns because the RPC connection broke
// (rather than because consensus answered), the transaction carrying nonce N
// may STILL be in flight. The next caller then takes the freed slot, allocates
// N+k, and commits it — and when the abandoned N finally lands, app-v9's replay
// gate rejects it Code 4 "nonce too low", because the gate compares against the
// highest COMMITTED nonce. The lease's own error path therefore reintroduced
// exactly the descending-arrival loss the lease was built to prevent.
//
// THE INVARIANT: ONLY A PROVEN FATE LIFTS A FENCE.
//
// Exactly two things are proof, and both are statements about the EXACT bytes
// that went out:
//   - those bytes reached a BLOCK — committed, or executed and
//     refused there. Either way they have had their turn.       -> lift
//   - a re-submission of those bytes was refused for a reason
//     that CANNOT UN-HAPPEN: the committed-nonce gate (code 4),
//     or the typed post-app-v25 committed-memory vote gate.
//     Both identify a monotone committed-state predicate.        -> lift
//
// "A non-zero CheckTx code" is NOT the second rule, and an earlier revision of
// this file getting that wrong is why cometResubmitOutcome now carries a
// permanence argument. A re-submission is judged against the state that exists
// NOW at the node WE are asking; the older copy — the one the fence is about —
// will be judged wherever and whenever IT arrives. Refusals that can be undone
// (a nonce lookup fault, admission backpressure, an authorization change, even a
// fork-gated decode code) therefore say nothing about it.
//
// Everything else keeps the fence UP, because none of it is evidence about the
// transaction:
//   - admission pending / duplicate-in-mempool -> keep asking about that hash
//   - a re-submit refused for a reversible reason -> stay fenced, ask again
//   - transport, decode or RPC fault           -> stay fenced, retry
//   - a deadline or a retry budget expiring    -> stay fenced; a clock knows
//     nothing about a transaction
//   - a panic out of the resolver              -> stay fenced; that is
//     caller-supplied code failing, not an answer
//   - no resolver wired, or no encoded bytes   -> stay fenced; the absence of a
//     way to check is not evidence
//   - "not found" from /tx                     -> stay fenced; CometBFT only
//     indexes a transaction once it is in a block, so a tx sitting unindexed in
//     a mempool one second before it commits answers exactly the same way
//
// WHY NOT CONCEDE ON A TIMER? An earlier revision of this file reopened the key
// when a reconciliation budget expired, arguing a permanently fenced key was
// worse than the race. That trade is wrong, and the asymmetry is the reason:
//   - A HELD fence fails LOUDLY. Every affected call site gets the typed
//     ErrSignerFenced, the hold is logged repeatedly, and FencedSigners()
//     names the key, the transaction and the last attempt. An operator can see
//     it and act.
//   - A WRONGLY LIFTED fence fails SILENTLY. Some later, unrelated user action
//     is rejected Code 4 for reasons nothing in the system can attribute. That
//     is the original bug, reintroduced as a "concession".
// Loud failure beats silent loss.
//
// THE ESCAPE HATCH IS EVIDENCE, AND ONLY EVIDENCE. Exactly one thing resolves a
// held fence: reconciliation forcing an answer. See reconcileFencedSubmission,
// which RE-SUBMITS the identical signed bytes rather than passively querying.
// Identical bytes carry the identical nonce and the identical hash, so
// re-submission is idempotent — it can never create a second transaction — and
// it converts "unknown" into "known" in the common case instead of hoping a
// lookup resolves itself. That mechanism is what keeps "never lift without
// proof" from meaning "fenced forever in practice".
//
// A RESTART IS NOT AN ESCAPE HATCH, AND MUST NOT BE DOCUMENTED AS ONE. An
// earlier revision of this file told operators that restarting cleared a fence
// safely, reasoning that the allocator re-seeds each key from the highest
// COMMITTED on-chain nonce via SetNonceFloorFunc (wired in cmd/sage-gui/node.go).
// That is FALSE, and false in the direction that loses transactions. The
// abandoned nonce N is unresolved PRECISELY BECAUSE it may still be sitting in
// the network or a peer's mempool while the committed floor is still BELOW N. A
// restart drops the in-process fence, seeds from that lower floor, and issues
// some M in the gap; M commits, the late N arrives afterwards and is rejected
// Code 4 — the exact silent loss this file exists to prevent. A crash during the
// original RPC loses the fence the same way, before the outcome was ever
// classified. The seed hook raises the floor above what is COMMITTED; it knows
// nothing about what is in flight, which is the only thing a fence is about.
//
// KNOWN LIMITATION, STATED PLAINLY: THIS FENCE IS IN-PROCESS ONLY. It does not
// survive a restart or a crash, so a transaction whose fate was never proven can
// still be lost across one. The concrete sequence is:
//
//	nonce N goes out, its fate is never observed  ->  the process restarts
//	->  the in-memory fence is gone  ->  the allocator re-seeds from the highest
//	COMMITTED nonce, which is still BELOW N  ->  it issues some M in the gap
//	->  M commits  ->  the late N finally arrives  ->  rejected Code 4.
//
// Closing that hole needs a DURABLE BROADCAST INTENT — the exact bytes and hash
// recorded BEFORE the send, cleared only on a proven fate, and reloaded and
// reconciled BEFORE any nonce is allocated on startup. That is persistence work
// and is deliberately out of scope here. The residual is written down rather
// than papered over: this whole workstream exists because a compliance claim was
// false, and a reassuring-but-false escape hatch is worse than an honestly
// stated gap.
//
// WHAT WE DO CONTROL, WE PREVENT. The dominant road into that hole is not a
// crash — it is this node's own updater deciding to restart. So while any fence
// is held, RestartVetoReason returns a non-empty reason and the coordinated
// restart path in cmd/sage-gui refuses to drain (see signer_fence_restart.go
// there),
// and QuiesceSigningForRestart stops new transactions being signed into a
// teardown that is already under way. Neither can help an operator SIGKILL or a
// power cut; both remove the case we schedule ourselves.
//
// OBSERVABILITY IS PART OF THE TRADE, NOT DECORATION. Holding a key indefinitely
// is only defensible while the hold is visible, so every transition emits a
// structured `SAGE: nonce_fence event=` line (fence_set, reconcile_retry,
// resolver_panic, fate_committed, fate_rejected, fence_lift, fence_held),
// FencedSigners() exposes the same facts to status surfaces, and
// internal/metrics carries the gauges and counters. What NEVER appears in any of
// them: the signing key's file path, the encoded transaction, or a raw error —
// see fenceCause for why the last one is a data leak rather than a style rule.

// ErrSubmitIndeterminate marks a submit outcome this process could not observe:
// the transaction may or may not have reached consensus. WithNonceLease fences
// the signing key when submit returns an error carrying this sentinel.
//
// This is a SENTINEL, not a classifier, and that is deliberate. internal/tx must
// not import web / api/rest, and must not sniff error strings to decide whether
// a transaction is still in flight — only the code that made the call knows
// whether "connection reset" happened before or after the bytes left the socket.
// So the classification flows INWARD: the adopter wraps its error with
// Indeterminate at the exact point the ambiguity arises, and the lease reacts to
// the type rather than to the text.
var ErrSubmitIndeterminate = errors.New("submit outcome indeterminate")

// ErrSignerFenced is returned to a caller whose context expired while this
// signing key was fenced. It means NOTHING WAS SIGNED OR SENT for that request.
//
// It must never be confused with a consensus rejection: a rejection is a verdict
// about a transaction that reached the chain, while this is a refusal to even
// allocate a nonce. A caller that retried this as though it were a rejection
// would be retrying something that never happened, and a caller that reported it
// as a rejection would tell an operator their change was refused when it was
// merely deferred.
//
// It is also the DESIGNED failure mode, not an anomaly: this error is what a
// held fence looks like from the outside, and it is preferred to the silent
// Code 4 loss that reopening the key on a clock would cause. Pair it with
// FencedSigners() to see which transaction the key is waiting on and what the
// last reconciliation attempt reported.
//
// An HTTP surface must map this to 503 + Retry-After ("not sent yet, ask
// again"), NEVER to a rejection status: nothing was signed, so there is no
// verdict to report and nothing for the operator to undo. Do NOT tell an
// operator that restarting clears this — it does not, and the header of this
// file explains why.
var ErrSignerFenced = errors.New("signer fenced: awaiting proof of an earlier indeterminate submission's fate")

// TxVerdict is what one reconciliation attempt learned about one exact
// transaction. Only the two DEFINITIVE values lift a fence.
type TxVerdict int

const (
	// TxVerdictUnresolved means "still no proof". It is the safe default and
	// the value every failure, timeout, panic and "not found" collapses to:
	// none of them says anything about where the transaction is.
	TxVerdictUnresolved TxVerdict = iota
	// TxVerdictCommitted means the EXACT bytes are in a committed block. The
	// abandoned nonce is now the signer's committed floor, so every later
	// allocation is strictly above it and the key is safe to reopen.
	TxVerdictCommitted
	// TxVerdictRejected means consensus PERMANENTLY refused to commit the EXACT
	// bytes AGAIN. That is precisely the property the fence guards — no nonce
	// inversion is possible once the key reopens — and it must be claimed no
	// wider. It is NOT "nothing is left in flight": a FinalizeBlock-failed
	// transaction does not consume the signer's nonce, and a byte-identical
	// copy in a peer mempool can still be re-included (see cometIndexedOutcome's
	// caveat), so redoing the action by hand can still apply it twice.
	//
	// "Permanently" is doing all the work here, and it is narrower than
	// "non-zero code" — see cometResubmitOutcome. A rejection observed in an
	// indexed block is proof outright. A rejection of a RE-SUBMISSION is only
	// proof when the reason cannot un-happen, because the older copy of those
	// bytes may still be sitting in some peer's mempool and will be judged
	// against whatever state exists when IT arrives.
	//
	// AND ONE HONESTY CAVEAT ABOUT THE FATE LABEL: a code-4 nonce-gate lift
	// proves supersession OR self-commit. App-v9's gate is `nonce <= committed`,
	// so a re-submission of a transaction that ITSELF committed answers code 4
	// exactly like one overtaken by a higher nonce, and without the node's tx
	// index the two are indistinguishable. The resolver re-checks the index once
	// before settling on this verdict, but on a node whose indexer is disabled
	// (indexer="null") or pruned, "rejected" can describe a transaction that
	// actually COMMITTED. The lift is safe either way — the committed floor is
	// at or above the fenced nonce — but the fate label can be wrong, so no
	// operator surface may treat it as proof the change needs redoing.
	TxVerdictRejected
	// TxVerdictSuperseded means a HIGHER nonce for the same signer has
	// committed, so the fenced allocation can never be included — consensus
	// refuses a stale nonce — and no nonce inversion is possible once the key
	// reopens. It is the same safety property as TxVerdictRejected with a
	// different cause, and it is the only verdict an operator can prove for a
	// fence whose signed bytes did not survive a restart. The payload IS lost:
	// unlike a rejection, there is no question of it applying later.
	TxVerdictSuperseded
	// TxVerdictSpent means the signer's COMMITTED nonce has reached the fenced
	// allocation (it equals it, rather than being above it). App-v9 refuses a
	// transaction whose nonce is <= the committed one, so the fenced bytes can
	// never commit again and no nonce inversion is possible once the key
	// reopens — the same safety property as TxVerdictRejected and
	// TxVerdictSuperseded.
	//
	// WHY IT IS ITS OWN LABEL RATHER THAN "superseded": a committed floor
	// exactly equal to the fenced nonce is reached either by the fenced
	// transaction ITSELF committing or by a different allocation of the same
	// nonce committing first, and the nonce floor alone cannot tell those
	// apart. Calling it "superseded" would assert the payload was lost when it
	// may be exactly the payload that landed. The honest claim is narrower:
	// the allocation is spent. Whether these bytes ever executed is only
	// knowable from the node's transaction index, which is precisely what a
	// fence restored from durable intent may not have.
	TxVerdictSpent
	// TxVerdictAbandoned is an OPERATOR DECISION, not a proof: the operator
	// accepted that a restored fence's payload may be lost, after the node
	// reported the evidence it could read (no peers, no copy in the mempool,
	// no committed fate, nonce not spent). It exists because the alternative
	// was a node that could never restart and never sign that key again, and
	// it is recorded as its own fate precisely so nobody later mistakes it for
	// evidence. See AbandonUnprovableFence.
	TxVerdictAbandoned
)

func (v TxVerdict) String() string {
	switch v {
	case TxVerdictCommitted:
		return "committed"
	case TxVerdictRejected:
		return "rejected by consensus"
	case TxVerdictSuperseded:
		return "superseded by a higher committed nonce"
	case TxVerdictSpent:
		return "spent (the signer's committed nonce has reached this allocation)"
	case TxVerdictAbandoned:
		return "abandoned by operator decision without proof of its fate"
	default:
		return "unresolved"
	}
}

// TxOutcome is one reconciliation attempt's result.
type TxOutcome struct {
	// Verdict is the ONLY field that can lift a fence.
	Verdict TxVerdict
	// Detail is operator-facing text for the log — the consensus rejection
	// reason, or why the attempt could not resolve. It is never parsed:
	// deciding anything from error text is what this design exists to avoid.
	Detail string
}

// TxResolveFunc drives ONE reconciliation attempt for the transaction with these
// exact encoded bytes, and must be prepared to be called forever.
//
// It is a RE-SUBMITTER, not merely a query. A passive lookup cannot distinguish
// "gone" from "about to commit", so an implementation should first check whether
// the exact hash is committed and otherwise re-broadcast the IDENTICAL bytes to
// force consensus to answer. Identical bytes mean an identical hash and nonce,
// so re-broadcasting is idempotent and cannot produce a second transaction.
// CometTxResolver is that implementation for a CometBFT endpoint.
//
// A non-nil error means the attempt could not resolve anything; the returned
// verdict is then IGNORED and treated as TxVerdictUnresolved, so an
// implementation can never accidentally lift a fence on a failed probe.
//
// THE PER-ATTEMPT DEADLINE IS COOPERATIVE, NOT PREEMPTIVE. ctx carries the
// attempt bound (fenceTimings.attempt) and Go cannot kill a goroutine, so an
// implementation that ignores ctx outlives the deadline and stalls THAT
// FENCE's reconciliation loop for as long as it blocks — no retry runs, no
// verdict can arrive, and the fence is effectively wedged on caller code. The
// blast radius is deliberately contained rather than eliminated: the held-
// fence alarm runs on its own goroutine and keeps reporting the hold, and
// other keys' fences and leases are untouched, but nothing can force the
// stuck attempt to end. An implementation MUST plumb ctx through every
// blocking call (CometTxResolver does, end to end, via
// http.NewRequestWithContext).
type TxResolveFunc func(ctx context.Context, encoded []byte) (TxOutcome, error)

// processTxResolverMu guards processTxResolver, the fallback resolver used by
// adopters that have no endpoint at the call site.
var (
	processTxResolverMu sync.RWMutex
	processTxResolver   TxResolveFunc
)

// SetTxResolverFunc installs the process-wide fallback resolver used when an
// adopter passes a nil TxResolveFunc to Indeterminate. Wire it once at boot
// (cmd/sage-gui/node.go does, from the CometBFT RPC URL). Passing nil clears it.
//
// Leaving it unwired is a real degradation with a real cost: an adopter that
// also passes no resolver fences its signing key with NOTHING able to prove that
// transaction's fate, so the fence is held for the remaining life of this
// process. That is deliberate — see the invariant at the top of this file — but
// it is an outage for that key, so wire this. Restarting does not "fix" it
// either; it drops the fence without resolving anything, which is the silent
// loss the fence was protecting against.
//
// It is read on EVERY reconciliation attempt rather than captured when the fence
// is raised, so installing a resolver later actually rescues fences that are
// already held instead of only helping future ones.
func SetTxResolverFunc(f TxResolveFunc) {
	processTxResolverMu.Lock()
	processTxResolver = f
	processTxResolverMu.Unlock()
}

func txResolverFallback() TxResolveFunc {
	processTxResolverMu.RLock()
	f := processTxResolver
	processTxResolverMu.RUnlock()
	return f
}

// FenceProofFunc re-reads the proofs available for a fence whose signed bytes
// did not survive the process that sent them.
//
// WHY THIS IS A SEPARATE HOOK FROM TxResolveFunc. A resolver is given the bytes
// and re-submits them; that is the right instrument for a fence raised in THIS
// process. A fence restored from durable intent has no bytes — the record
// deliberately carries identity (signer, hash, nonce) and not payload — so the
// only question left is the one an operator used to have to answer by hand:
// has the chain already proven this transaction's fate? The prover answers it
// by reading, not by sending: the recorded hash against the node's transaction
// index, and the signer's committed nonce against the fenced allocation.
//
// The proof is read from consensus state by the implementation, never asserted
// by the caller, and the fence still validates whatever it returns (see
// validateFenceLiftProof). Returning an error means "not proven yet" and is
// never a lift.
type FenceProofFunc func(ctx context.Context, fence FencedSigner) (FenceLiftProof, error)

// processFenceProverMu guards processFenceProver, the hook restored fences use
// to re-read their fate.
var (
	processFenceProverMu sync.RWMutex
	processFenceProver   FenceProofFunc
)

// SetFenceProverFunc installs the process-wide prover for fences restored from
// durable intent. Wire it once at boot (cmd/sage-gui/node.go does, from the
// same CometBFT RPC URL and nonce floor the rest of the node uses). Passing nil
// clears it.
//
// LEAVING IT UNWIRED IS THE BUG THIS HOOK EXISTS TO FIX, so it is worth being
// exact about the failure it used to cause. A restored fence has no
// reconciler: before this hook, its goroutine parked on the fence channel and
// reported "waiting for proof" once, and nothing ever asked the chain whether
// the proof had arrived. The fence could therefore only be lifted by an
// operator POST, and the restart veto — which is what an update must pass —
// refused for as long as the fence stood. Users upgrading a node that had been
// killed mid-submission saw exactly that: "SAGE is not safe to restart yet",
// held for hours, with the recovery the message named (re-submission)
// impossible by construction.
//
// It is read on EVERY attempt rather than captured when the fence is raised, so
// installing it later rescues fences that are already held.
func SetFenceProverFunc(f FenceProofFunc) {
	processFenceProverMu.Lock()
	processFenceProver = f
	processFenceProverMu.Unlock()
}

func fenceProverFallback() FenceProofFunc {
	processFenceProverMu.RLock()
	f := processFenceProver
	processFenceProverMu.RUnlock()
	return f
}

// FenceAutoResolverFunc resolves a restored fence that the node can prove
// NOTHING can deliver back (see AutoResolveUnprovableFence). It returns
// resolved=false to say "not this time" — the usual answer while the node is
// still catching up — and an error only when it could not even ask.
type FenceAutoResolverFunc func(ctx context.Context, fence FencedSigner) (resolved bool, detail string, err error)

var (
	processFenceAutoResolverMu sync.RWMutex
	processFenceAutoResolver   FenceAutoResolverFunc
)

// SetFenceAutoResolverFunc installs the startup resolution for restored fences
// that no proof can settle and no peer can deliver back. It is deliberately a
// separate hook from SetFenceProverFunc: proving a fate and concluding that no
// fate can ever exist are different claims, and only the second one is allowed
// to discard a payload — so it is wired, read, and reviewed on its own.
func SetFenceAutoResolverFunc(f FenceAutoResolverFunc) {
	processFenceAutoResolverMu.Lock()
	processFenceAutoResolver = f
	processFenceAutoResolverMu.Unlock()
}

func fenceAutoResolverFallback() FenceAutoResolverFunc {
	processFenceAutoResolverMu.RLock()
	f := processFenceAutoResolver
	processFenceAutoResolverMu.RUnlock()
	return f
}

// indeterminateSubmit is the typed signal an adopter hands back to
// WithNonceLease. It is intentionally unexported: adopters construct it through
// Indeterminate and match on ErrSubmitIndeterminate, so the fence's payload
// (the encoded bytes, the resolver) can change without breaking call sites.
//
// Error() returns the wrapped message VERBATIM. WithNonceLease's contract is
// that submit's error comes back undecorated so callers can keep classifying
// it, and several web handlers still surface that text to operators; a wrapper
// that prefixed itself would silently change every one of those messages.
//
// cause is derived from err ONCE, at construction, and it is the only thing the
// fence is allowed to know about the failure. err itself is never read by any
// fence code path — not stored, not logged, not matched. That is structural
// rather than a convention because historical GET broadcasters put the signed
// transaction in the URL and net/http then embedded it in errors. Large
// broadcasts now use JSON-RPC POST, but deriving the category here means no
// later transport edit can reintroduce the leak by reaching for a "more
// helpful" message.
type indeterminateSubmit struct {
	err     error
	cause   fenceCause
	encoded []byte
	resolve TxResolveFunc
	// recordedTxHash / recordedNonce / hasRecordedNonce carry the identity of a
	// submission restored from DURABLE INTENT, whose signed bytes did not
	// survive the process that sent them. They are used only when encoded is
	// empty, so a live submission always speaks for itself.
	recordedTxHash   string
	recordedNonce    uint64
	hasRecordedNonce bool
}

func (e *indeterminateSubmit) Error() string { return e.err.Error() }

func (e *indeterminateSubmit) Unwrap() error { return e.err }

// Is reports ErrSubmitIndeterminate in addition to whatever the wrapped error
// matches, so `errors.Is(err, tx.ErrSubmitIndeterminate)` works without the
// adopter having to wrap twice.
func (e *indeterminateSubmit) Is(target error) bool { return target == ErrSubmitIndeterminate }

// Indeterminate wraps err as the typed "may still be in flight" signal for
// WithNonceLease, carrying the EXACT bytes that were put on the wire.
//
// Call it only where the ambiguity actually originates — a transport fault, an
// undecodable response, an RPC-level error envelope. Do NOT call it for a
// CheckTx or FinalizeBlock rejection or for a pre-send failure (sign, encode):
// those are definitive, nothing is in flight, and fencing on them would turn
// every ordinary validation failure into an outage for that signing key.
//
// encoded must be the bytes handed to the node. Reconciliation both identifies
// AND re-submits the transaction by them, so a fence raised without them can
// never be proven and is held for the remaining life of this process. resolve
// reconciles that transaction; nil falls back to the resolver installed by
// SetTxResolverFunc, and if that is unwired too the same permanent hold applies.
//
// err is used ONLY to derive a typed cause category (transport / timeout /
// canceled / decode / rpc) for the fence's diagnostics. Its text is never
// stored, logged or matched on. That is not fastidiousness: net/http embeds the
// full request URL in its errors, and a broadcast URL carries the entire signed
// transaction as a query parameter, so keeping the message would put signed
// bytes into the log on every retry of a fence that retries forever.
//
// KNOW WHAT YOU ARE ASKING FOR: because reconciliation RE-SUBMITS, calling this
// upgrades "this transaction may have been sent" into "this transaction will be
// pushed until consensus accepts or refuses it". A submission the caller saw
// fail can therefore still commit, minutes later, from the reconciliation
// goroutine. That is the correct semantics for the fence — the allocated nonce
// must be either consumed or proven dead before a higher one may be issued, and
// the adopter has already declared it could not rule the transaction out — but
// it is the reason Indeterminate must NOT be used for a definitive failure. A
// sign or encode fault, or a real CheckTx / FinalizeBlock rejection, marked
// indeterminate here would be re-broadcast repeatedly on top of fencing the key.
//
// A nil err returns nil: nothing failed, so there is nothing to be uncertain
// about.
func Indeterminate(err error, encoded []byte, resolve TxResolveFunc) error {
	if err == nil {
		return nil
	}
	// Copy: the caller's buffer is usually a freshly encoded transaction, but
	// reconciliation outlives this call indefinitely and a reused encode buffer
	// would have it re-broadcasting a DIFFERENT transaction than the one that
	// was sent — which, unlike re-broadcasting the identical bytes, is not
	// idempotent and would put a second signed transaction on the chain.
	txBytes := make([]byte, len(encoded))
	copy(txBytes, encoded)
	return &indeterminateSubmit{
		err: err,
		// Classify HERE, once, so no fence code path ever holds a reason to
		// look at err again. See the type's doc comment.
		cause:   classifyFenceCause(err),
		encoded: txBytes,
		resolve: resolve,
	}
}

// IndeterminateDetails reports the identity of the exact transaction an
// indeterminate submit put on the wire: its CometBFT hash, uppercase hex, and
// — when the bytes still decode — the nonce it was signed with.
//
// This is the read side of Indeterminate for HTTP surfaces, and it exists
// because those surfaces previously had only the error text. An ambiguous
// outcome collapsed into the same opaque 500 as a genuine internal fault, so a
// caller could not tell "your write may already be committed" from "your write
// never happened", and the observed consequence in the field was a caller
// re-signing a claim that had already committed. The caller's only safe next
// step is to look the transaction up by hash, which it cannot do unless we hand
// over the hash we already hold.
//
// The format matches what FencedSigners reports and what /tx?hash= accepts, so
// one hash identifies the transaction on every surface. Nothing secret crosses
// this boundary: the hash and the nonce both travel in the clear and end up
// on-chain.
//
// ok is false when err is not an indeterminate submit, or when it carries no
// encoded transaction. A fence raised without bytes can never be proven, so
// there is genuinely no hash to report.
func IndeterminateDetails(err error) (hash string, nonce uint64, hasNonce bool, ok bool) {
	var ind *indeterminateSubmit
	if !errors.As(err, &ind) || len(ind.encoded) == 0 {
		return "", 0, false, false
	}
	sum := CometTxHash(ind.encoded)
	nonce, hasNonce = fencedTxNonce(ind.encoded)
	return strings.ToUpper(hex.EncodeToString(sum[:])), nonce, hasNonce, true
}

// fenceCause is the ONLY thing a fence keeps about the error that raised it: a
// category, never the message.
//
// KEEPING THE MESSAGE IS A DATA LEAK, NOT A STYLE CHOICE. Historical CometBFT
// broadcasters in this repo issued GET requests containing the entire signed
// transaction, and net/http embedded the full request URL in the *url.Error it
// returned. Storing that error would put the signed bytes in the
// fence record, in FencedSigners(), and in every "still held" line — and because
// reconciliation now retries for as long as the fence stands, it would repeat
// them forever and carry them into any support bundle. internal/voter already
// refuses to attach these errors for exactly this reason (see
// internal/voter/broadcast.go, "Do not attach err"); this is that rule applied
// at the fence boundary, where it also has to hold for adopters this package
// does not control.
//
// Categories are derived by TYPE — errors.Is / errors.As against context, net
// and encoding/json sentinels — never by matching text. internal/tx deciding
// anything from an error string is precisely what the typed Indeterminate
// signal exists to avoid, and a category is all any of this file's logic uses.
type fenceCause string

const (
	// fenceCauseTimeout: a deadline expired with no answer.
	fenceCauseTimeout fenceCause = "timeout"
	// fenceCauseCanceled: the caller (usually a closed browser tab) went away.
	fenceCauseCanceled fenceCause = "canceled"
	// fenceCauseTransport: the connection failed. The most dangerous case, and
	// the one the fence exists for: the bytes were already handed to the kernel.
	fenceCauseTransport fenceCause = "transport"
	// fenceCauseDecode: the node answered with something unreadable, so it may
	// well have accepted the transaction.
	fenceCauseDecode fenceCause = "decode"
	// fenceCauseRPC: a decoded JSON-RPC error envelope — the node answered, but
	// not about this transaction's fate.
	fenceCauseRPC fenceCause = "rpc"
	// fenceCausePanic: the resolver panicked. Kept distinct from every other
	// category because it is the one that indicts OUR code rather than the
	// network, and an operator seeing it repeatedly should be reading a stack
	// trace, not a firewall rule.
	fenceCausePanic fenceCause = "resolver_panic"
	// fenceCauseSubmitPanic: submit panicked AFTER registering its encoded
	// transaction as handed to the network (RegisterSubmittedTx), so the fence
	// was raised by the lease's panic guard rather than by an Indeterminate
	// return. Distinct from fenceCausePanic because it indicts the ADOPTER's
	// broadcast path, not the reconciliation resolver, and the stack to read is
	// the one the panic printed when it finished unwinding.
	fenceCauseSubmitPanic fenceCause = "submit_panic"
	// fenceCauseSubmitError: submit returned an ordinary error while its
	// registration was still live. Registration proves bytes reached transport;
	// without ClearSubmittedTx, the error cannot safely reopen the signer.
	fenceCauseSubmitError fenceCause = "submit_error"
	// fenceCauseNoResolver / fenceCauseNoEncodedTx: the attempt could not even
	// be made. These are the two shapes of "we have no way to ask", which is
	// emphatically not the same as "the transaction is gone" — they hold the
	// fence like everything else here, but they are labelled apart because they
	// are fixed by wiring, not by waiting.
	fenceCauseNoResolver  fenceCause = "no_resolver"
	fenceCauseNoEncodedTx fenceCause = "no_encoded_tx"
	// fenceCauseNoProver: the fence was restored from durable intent (so there
	// are no bytes to re-submit) and no proof reader is wired, so the node
	// cannot even ask whether the chain has already settled the transaction.
	// Like no_resolver this is fixed by wiring, not by waiting: the proof
	// reader is re-read on every attempt, so installing it late rescues the
	// fence.
	fenceCauseNoProver fenceCause = "no_fence_prover"
	// fenceCauseNoProof: the proof reader answered, but with nothing this fence
	// will accept — no verdict yet, or a proof about another signer,
	// transaction or allocation. Labelled apart from "pending" because the
	// transaction may well be settled on-chain; what is missing is a readable
	// proof, and the detail says which.
	fenceCauseNoProof fenceCause = "no_proof"
	// fenceCausePending: the attempt reached the node and the node declined to
	// answer definitively — a duplicate already in the mempool, a commit wait
	// that expired, a non-permanent CheckTx refusal. The healthiest of the
	// unresolved causes: it means the transaction is probably still alive.
	fenceCausePending fenceCause = "pending"
	// fenceCauseRestored: this fence was raised at startup from DURABLE INTENT,
	// not from a live submission — the previous process was killed while the
	// transaction was in flight. The signed bytes did not survive it, so
	// reconciliation cannot re-submit and the fence resolves only on a proven
	// fate: the exact hash found in a committed block, or supersession by a
	// higher committed nonce for this signer. It is labelled apart because it is
	// fixed by an operator proof, not by waiting for a resolver.
	fenceCauseRestored fenceCause = "restored_from_durable_intent"
)

// classifyFenceCause maps an error to its category by type only.
//
// Order matters. A *url.Error satisfies net.Error and usually WRAPS the real
// cause (including io.EOF), so the net check has to run before the decode check
// or every dropped connection would be filed as a decode fault and an operator
// would go looking at the wrong end of the wire.
func classifyFenceCause(err error) fenceCause {
	switch {
	case err == nil:
		return fenceCauseRPC
	case errors.Is(err, context.DeadlineExceeded):
		return fenceCauseTimeout
	case errors.Is(err, context.Canceled):
		return fenceCauseCanceled
	}
	// Checked by TYPE, before the network sentinels: "the chain has not proven
	// this yet" is a reply, not a fault, and filing it under rpc would send an
	// operator reading the log to look at connectivity during exactly the
	// healthy case (a fence whose transaction is simply still unresolved).
	var unproven *FenceLiftUnprovenError
	if errors.As(err, &unproven) {
		return fenceCauseNoProof
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return fenceCauseTimeout
		}
		return fenceCauseTransport
	}
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &syntaxErr) || errors.As(err, &typeErr) ||
		errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return fenceCauseDecode
	}
	return fenceCauseRPC
}

const (
	// fenceHexKeepMax is the longest hex run kept verbatim in fence text. A tx
	// hash and an ed25519 public key are 64 characters and a signature is 128 —
	// all of them things an operator reads — while an encoded SAGE transaction
	// is thousands. Anything longer than a signature is not an identifier.
	fenceHexKeepMax = 128
	// fenceDetailMax bounds one stored/logged detail. A fence can be held
	// indefinitely and re-reports on a timer, so an unbounded detail is an
	// unbounded log.
	fenceDetailMax = 512
	// redactedTxMarker replaces an exact-matched encoded transaction, so the log
	// says a transaction was removed rather than silently losing the sentence.
	redactedTxMarker = "[redacted signed tx]"
	// txQueryPrefix is the CometBFT broadcast query parameter. Every leak this
	// file guards against arrives through it.
	txQueryPrefix = "tx=0x"
)

// scrubFenceText removes anything that could be an encoded transaction from text
// that is about to be stored in a fence or written to the log, and bounds its
// length.
//
// Four passes: three because signed bytes reach a message three ways, and a
// fourth because pass 3 alone is evadable on purpose-built input.
//
//  1. EXACT MATCH kills the known leak: a *url.Error carrying
//     ".../broadcast_tx_commit?tx=0x<hex of exactly these bytes>".
//  2. THE tx= QUERY PARAMETER is redacted whatever follows it and however long
//     it is. This one is not redundant with the others: a SHORT transaction, or
//     one an adopter re-encoded or truncated, defeats both the exact match and
//     the long-run heuristic — and a message that still reads
//     "broadcast_tx_commit?tx=0x1a2b…" is a leak even when the bytes it carries
//     are only most of a transaction. The parameter name is the reliable signal,
//     so it is what we key on.
//  3. LONG HEX RUNS catch anything that arrives without that shape, from
//     adopter-supplied resolvers and RPC error envelopes this package does not
//     control.
//  4. CHUNKED/DENSE HEX catches deliberate evasion of pass 3: hex split into
//     sub-threshold runs by separators. See scrubChunkedHex for the exact rule
//     and its stated residual.
//
// ScrubBroadcastText is the exported entry point for adopters that build error
// text at the SAME origin this package guards — the CometBFT broadcast call.
//
// It exists because the leak is not confined to *url.Error. A JSON-RPC error
// envelope carries attacker- or proxy-controlled Message and Data fields, and a
// reverse proxy in front of CometBFT can answer 200 with an envelope echoing the
// request line, i.e. "GET /broadcast_tx_commit?tx=0x<full signed hex>". An
// adopter formatting that envelope verbatim re-opens exactly the leak class this
// file closed, at a different origin — and web/rbac_signing.go did precisely
// that while internal/tx scrubbed the identical surface. Asymmetric scrubbing of
// one hazard across two files is how a fixed leak comes back.
//
// Pass the encoded transaction when the caller has it, nil when it does not;
// only the exact-match pass depends on it.
func ScrubBroadcastText(text string, encoded []byte) string {
	return scrubFenceText(text, encoded)
}

// Passing nil encoded skips only the first pass.
func scrubFenceText(text string, encoded []byte) string {
	if text == "" {
		return ""
	}
	if len(encoded) > 0 {
		hexed := hex.EncodeToString(encoded)
		text = strings.ReplaceAll(text, hexed, redactedTxMarker)
		text = strings.ReplaceAll(text, strings.ToUpper(hexed), redactedTxMarker)
	}
	text = scrubTxQueryParams(text)
	text = scrubLongHexRuns(text)
	// The chunk pass runs AFTER the control pass on purpose: control-byte
	// separators between hex chunks have just become single spaces, which is
	// exactly the shape the reassembly rule bridges.
	text = scrubControlRunes(text)
	text = scrubChunkedHex(text)
	if len(text) > fenceDetailMax {
		cut := fenceDetailMax
		// Never cut mid-rune: a broken UTF-8 tail turns a diagnostic line into
		// mojibake in whatever collects the log.
		for cut > 0 && !utf8.RuneStart(text[cut]) {
			cut--
		}
		text = text[:cut] + "…(truncated)"
	}
	return text
}

// scrubTxQueryParams removes the payload of every "tx=0x…" query parameter,
// keeping the parameter name so the reader can see what was removed.
//
// It matches case-insensitively on the marker and consumes hex digits of ANY
// length, including zero. That deliberately over-matches: the only thing that
// ever appears after tx=0x on a CometBFT broadcast URL is an encoded
// transaction, so there is nothing here worth preserving, and under-matching
// costs a leak that repeats for as long as the fence stands.
func scrubTxQueryParams(text string) string {
	lower := strings.ToLower(text)
	var b strings.Builder
	b.Grow(len(text))
	for i := 0; i < len(text); {
		idx := strings.Index(lower[i:], txQueryPrefix)
		if idx < 0 {
			b.WriteString(text[i:])
			break
		}
		start := i + idx
		b.WriteString(text[i:start])
		b.WriteString(txQueryPrefix)
		end := start + len(txQueryPrefix)
		for end < len(text) && isHexDigit(text[end]) {
			end++
		}
		if end > start+len(txQueryPrefix) {
			b.WriteString(redactedTxMarker)
		}
		// No marker when there was nothing to remove — the exact-match pass
		// above may already have replaced this parameter's payload, and a second
		// marker would only make the line harder to read.
		i = end
	}
	return b.String()
}

// scrubLongHexRuns replaces every hex run longer than fenceHexKeepMax with a
// count. It walks bytes rather than runes deliberately: hex digits are ASCII, so
// any multi-byte rune is copied through untouched.
func scrubLongHexRuns(text string) string {
	var b strings.Builder
	b.Grow(len(text))
	runStart := -1
	flush := func(end int) {
		if runStart < 0 {
			return
		}
		if end-runStart > fenceHexKeepMax {
			fmt.Fprintf(&b, "[redacted %d hex chars]", end-runStart)
		} else {
			b.WriteString(text[runStart:end])
		}
		runStart = -1
	}
	for i := 0; i < len(text); i++ {
		if isHexDigit(text[i]) {
			if runStart < 0 {
				runStart = i
			}
			continue
		}
		flush(i)
		b.WriteByte(text[i])
	}
	flush(len(text))
	return b.String()
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// scrubChunkedHex is the backstop behind scrubLongHexRuns: it redacts a detail
// whose hex content is over the identifier budget but arrives FRAGMENTED, so
// no single run trips the long-run pass.
//
// The evasion it closes is concrete: a hostile proxy answering the
// reconciler's re-submit with an error envelope whose data is the request's
// own tx hex re-chunked every ~100 characters by separators. Each chunk is
// under fenceHexKeepMax, there is no tx= parameter and no exact match, so all
// three preceding passes wave it through — and up to fenceDetailMax bytes of
// the signed transaction land in the fence record and re-emit on every
// held-fence report for as long as the fence stands. A partial payload leak
// repeating indefinitely is no better than a full one.
//
// Two triggers, each covering the other's blind spot, and the whole detail is
// replaced rather than the fragments: by the time either fires the text is
// established to be carrying encoded material, and prose stitched between
// payload chunks is the attacker's, not worth preserving.
//
//   - REASSEMBLY: hex runs separated by a SINGLE non-hex byte are counted as
//     one sequence; a sequence over fenceHexKeepMax is an encoded payload,
//     however it was chunked. Single-byte bridging is why this runs after
//     scrubControlRunes — control-byte separators have just become single
//     spaces. Legitimate diagnostics survive because English words almost
//     always contain adjacent non-hex letters, which break the bridge.
//   - DENSITY: if hex digits are over the budget in TOTAL and also make up
//     more than 40% of the detail, it is redacted even when every gap is wide.
//     Both conjuncts are load-bearing: heights, ports, IPs and the a-f letters
//     of ordinary prose inflate a raw total, so total count alone would nuke
//     legitimate diagnostics, while density alone would nuke any short string
//     that IS a legitimate identifier (a lone hash is 100% hex).
//
// STATED RESIDUAL, not a guarantee: a peer that both keeps every gap at two or
// more bytes AND pads to under 40% density can still fit ~fenceDetailMax*0.4
// hex characters through, and one that re-encodes the payload in a non-hex
// alphabet is outside what any hex heuristic can see. Those are bounded by
// fenceDetailMax and accepted; the alternative — redacting on any hex at all —
// would blind every diagnostic to close a channel the exact-match and tx=
// passes already close for the canonical leak shapes.
func scrubChunkedHex(text string) string {
	totalHex := 0
	assembled := 0
	maxAssembled := 0
	gap := 0
	for i := 0; i < len(text); i++ {
		if isHexDigit(text[i]) {
			totalHex++
			assembled++
			if assembled > maxAssembled {
				maxAssembled = assembled
			}
			gap = 0
			continue
		}
		gap++
		if gap > 1 {
			assembled = 0
		}
	}
	if maxAssembled > fenceHexKeepMax ||
		(totalHex > fenceHexKeepMax && totalHex*5 > len(text)*2) {
		return fmt.Sprintf(
			"[redacted detail: %d hex chars in fragments exceeds the %d-char identifier budget]",
			totalHex, fenceHexKeepMax)
	}
	return text
}

// scrubControlRunes replaces every control rune and every Unicode format
// character with a space, with NO exceptions — not even \n and \t.
//
// The values this scrubber cleans are stored in fence records, surfaced
// through FencedSigners() into status payloads, and printed on log lines by
// emitFenceEvent, and control characters are how a remote party FORGES those
// surfaces: \r rewinds a terminal over the line just written, ESC starts ANSI
// sequences in any viewer, and DEL/C1 controls survive most naive filters. The
// no-exceptions rule is deliberate — a version-string injection elsewhere in
// this release was fixed by extending a character set, and the set was wrong
// again within a review cycle. Exempting "harmless" controls is the same
// mistake on layaway. The cost is that multi-line details (a stack trace)
// flatten to one line; emitFenceEvent quotes them anyway, so nothing readable
// is lost.
func scrubControlRunes(text string) string {
	unsafe := func(r rune) bool { return unicode.IsControl(r) || unicode.Is(unicode.Cf, r) }
	if !strings.ContainsFunc(text, unsafe) {
		return text
	}
	return strings.Map(func(r rune) rune {
		if unsafe(r) {
			return ' '
		}
		return r
	}, text)
}

// fencedTxNonce recovers the nonce out of the encoded transaction so a held
// fence can name the allocation it is stuck on.
//
// This is the field that makes a fence actionable: "key X is fenced" tells an
// operator an agent cannot sign, while "key X is fenced on nonce N" can be
// matched against what the chain has committed for that key. The nonce is not
// secret — it travels in the clear inside the transaction and is stored on-chain
// — so exposing it costs nothing. A transaction that will not decode simply has
// no nonce to report; that is not an error, it is one more thing wrong with a
// fence that already cannot be proven.
func fencedTxNonce(encoded []byte) (uint64, bool) {
	if len(encoded) == 0 {
		return 0, false
	}
	parsed, err := DecodeTx(encoded)
	if err != nil || parsed == nil {
		return 0, false
	}
	return parsed.Nonce, true
}

// fenceField is one key=value pair on a structured fence event.
type fenceField struct{ key, value string }

func fenceKV(key, value string) fenceField { return fenceField{key: key, value: value} }

func fenceNum(key string, n uint64) fenceField {
	return fenceField{key: key, value: strconv.FormatUint(n, 10)}
}

func fenceAge(key string, d time.Duration) fenceField {
	return fenceField{key: key, value: d.Round(time.Millisecond).String()}
}

// fenceLogW is where fence events go. It is a variable, and writes take
// fenceLogMu, for two reasons: the leak regression test has to be able to read
// back everything the fence emitted, and fence events are written from several
// background goroutines at once, so an unsynchronised writer would interleave
// two events into one unparseable line. The volume is bounded by the throttles
// in reportReconcileRetry / reportResolverPanic, so the lock is never hot.
var (
	fenceLogMu sync.Mutex
	fenceLogW  io.Writer = os.Stderr
)

// emitFenceEvent writes one structured fence event.
//
// It stays on the os.Stderr writer the rest of this package already uses instead
// of taking a logger: internal/tx is imported by internal/abci and by every
// adopter, none of which passes one in, and threading a logging dependency
// through a package on the consensus import path to print a handful of event
// types is not a trade worth making. The shape is what the observability
// requirement actually needs — a stable event= name plus typed fields, greppable
// and alertable.
//
// WHAT IS NEVER A FIELD: the signing key's FILE PATH, the encoded transaction,
// and any raw error. Every free-text value must have been through
// scrubFenceText at its source. The signer is identified by a PREFIX of the hex
// of its PUBLIC key — the same agent id SAGE prints everywhere else, so a fenced
// key can be matched to an agent without a translation step, and public by
// construction.
//
// An empty value drops its field entirely, which is what lets an optional field
// (a nonce that would not decode, a detail nobody reported) be passed
// unconditionally by the caller.
func emitFenceEvent(event string, fields ...fenceField) {
	var b strings.Builder
	b.WriteString("SAGE: nonce_fence event=")
	b.WriteString(event)
	for _, f := range fields {
		if f.value == "" {
			continue
		}
		b.WriteByte(' ')
		b.WriteString(f.key)
		b.WriteByte('=')
		if fenceValueNeedsQuoting(f.value) {
			b.WriteString(strconv.Quote(f.value))
		} else {
			b.WriteString(f.value)
		}
	}
	b.WriteByte('\n')

	// Held ACROSS the write, not just the read: two goroutines emitting at once
	// would otherwise interleave into one line that neither greps nor parses.
	fenceLogMu.Lock()
	defer fenceLogMu.Unlock()
	_, _ = fmt.Fprint(fenceLogW, b.String())
}

// fenceValueNeedsQuoting decides whether a field value goes into the event
// line raw or through strconv.Quote.
//
// This is an ALLOWLIST, not a blocklist, and that is the fix for a real
// injection: the earlier version quoted only on space/tab/newline/quote — a
// character SET, and character sets drift. CR, ESC and DEL all passed raw, and
// the values in these fields include RPC-derived details, i.e. remote text: a
// detail containing \r rewinds the terminal cursor and overwrites the line
// already emitted (a forged log entry), and ESC opens ANSI sequences in any
// terminal or log viewer. Same failure shape as the version-string injection
// fixed elsewhere this release — right mechanism, wrong set. So the test is
// inverted: anything that is not a printable, backslash-free, single-token
// rune is quoted, and strconv.Quote escapes every control and every invalid
// byte. utf8.RuneError forces quoting because ranging yields it for each raw
// invalid byte — which is how bare C1 controls (0x80–0x9F) arrive — and Quote
// renders those as \x.. escapes instead of passing the raw byte through.
func fenceValueNeedsQuoting(v string) bool {
	for _, r := range v {
		if r == ' ' || r == '"' || r == '\\' || r == utf8.RuneError || !unicode.IsPrint(r) {
			return true
		}
	}
	return false
}

// fenceGaugePublishMu serializes publishFenceGauges' read-then-set. It is a
// separate lock, not fenceMu held longer, because the SET reaches into the
// metrics package and fenceMu must never be held across a foreign call — but
// the read and the set do have to be one atomic step. Without that, two
// publishers interleave read-A, read-B, set-B, set-A and the gauges FREEZE on
// the older snapshot: a fence lift racing a periodic re-report could leave
// sage_nonce_fences_active pinned at 1 on a node with nothing held (a
// permanent false alarm), or — worse — at 0 while a fence stands (the silent
// hold this file's observability exists to prevent). The lock covers a map
// read and two gauge stores; nothing here waits on reconciliation, RPC, or a
// lease, so distinct keys stay concurrent.
var fenceGaugePublishMu sync.Mutex

// publishFenceGauges recomputes the two held-fence alarm gauges from the map.
//
// It is called on every raise, lift and periodic re-report rather than from a
// sampling ticker, so sage_nonce_fences_active tracks the map exactly and
// sage_nonce_fence_oldest_age_seconds advances while a fence is stuck instead of
// freezing at the value it had when it was raised.
//
// Callers must NOT hold fenceMu: this takes it.
func publishFenceGauges() {
	fenceGaugePublishMu.Lock()
	defer fenceGaugePublishMu.Unlock()
	now := time.Now()
	fenceMu.Lock()
	active := len(fences)
	oldest := time.Duration(0)
	for _, f := range fences {
		if age := now.Sub(f.since); age > oldest {
			oldest = age
		}
	}
	fenceMu.Unlock()
	metrics.SetNonceFenceGauges(active, oldest.Seconds())
}

// keyFence is the per-signing-key gate that holds a key closed while an
// indeterminate submission is being reconciled. ch is closed (never sent on) so
// every blocked waiter wakes at once when the fence lifts.
//
// pending counts outstanding reconciliations rather than being a bare flag.
// Today only the holder of a key's lease slot can fence it, so the count is
// always 0 or 1 — but a flag would make a second fence silently lift the first,
// and "silently lift the fence" is the precise bug this whole file exists to
// prevent. Counting makes that shape safe instead of merely unlikely.
//
// The remaining fields are DIAGNOSTICS, guarded by fenceMu like the rest. They
// exist because the corrected invariant makes an unbounded hold possible: a
// fence nobody can see is a mystery hang, and the whole argument for holding
// rather than conceding is that the failure is loud and attributable.
type keyFence struct {
	ch       chan struct{}
	pending  int
	retiring bool
	lifted   bool

	txHash   string
	nonce    uint64
	hasNonce bool
	since    time.Time
	// cause is a CATEGORY, never a message. See fenceCause: the submit error
	// that raised the fence routinely contains the whole signed transaction.
	cause      fenceCause
	attempts   int
	lastAt     time.Time
	lastCause  fenceCause
	lastDetail string
	// lastRetryLogAt / lastPanicLogAt rate-limit the per-attempt events.
	// Reconciliation retries for as long as the fence stands, which under the
	// corrected invariant can be indefinitely, so an unthrottled per-attempt
	// line is an unbounded log — and a repeating panic carries a stack trace,
	// which makes it the worst offender. They are separate clocks so a
	// throttled retry line can never swallow the first report of a panic.
	lastRetryLogAt time.Time
	lastPanicLogAt time.Time
}

// fenceMu guards fences. Like leases, the map is SPARSE — an entry exists only
// while a key is actually fenced, and the reconciliation that proves the
// transaction's fate deletes it. That is what bounds fence memory on a node that
// signs for many agent and federation keys: fences cannot accumulate one entry
// per key ever used. A key whose fate is never proven does retain its entry
// (that is the point), and it is one small struct per stuck signing key.
//
// fenceMu is never held across reconciliation, an RPC, or a lease acquisition;
// it only guards the map and the counters, so a fence on key A never delays key
// B.
var (
	fenceMu sync.Mutex
	fences  = make(map[string]*keyFence)
)

// fenceTimings bounds the reconciliation LOOP — never the fence itself. Nothing
// here can lift a fence; these only decide how often this process asks, and how
// often it complains. They are read through a mutex because reconciliation runs
// in a background goroutine and tests override them; without the guard the race
// detector would (correctly) flag the override.
type fenceTimings struct {
	// attempt caps a single resolver call so one hung RPC cannot stall the
	// retry loop. Exhausting it yields TxVerdictUnresolved and another attempt,
	// never a lift.
	attempt time.Duration
	// retry is the gap after the first unresolved attempt. It doubles per
	// consecutive unresolved attempt up to retryMax, so an unreachable node is
	// not hammered by a loop that will run for as long as the fence is held.
	retry time.Duration
	// retryMax caps that backoff.
	retryMax time.Duration
	// report is how often a still-held fence is logged. A held fence must stay
	// audible: silence is what would make it a mystery hang instead of a
	// diagnosable one.
	report time.Duration
}

var (
	fenceTimingMu sync.RWMutex
	fenceTiming   = fenceTimings{
		attempt:  30 * time.Second,
		retry:    2 * time.Second,
		retryMax: 60 * time.Second,
		report:   60 * time.Second,
	}
)

func init() {
	fenceTiming.attempt = fenceDurationFromEnv("SAGE_TX_FENCE_ATTEMPT_MS", fenceTiming.attempt)
	fenceTiming.retry = fenceDurationFromEnv("SAGE_TX_FENCE_RETRY_MS", fenceTiming.retry)
	fenceTiming.retryMax = fenceDurationFromEnv("SAGE_TX_FENCE_RETRY_MAX_MS", fenceTiming.retryMax)
	fenceTiming.report = fenceDurationFromEnv("SAGE_TX_FENCE_REPORT_MS", fenceTiming.report)
}

func fenceDurationFromEnv(name string, fallback time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	ms, err := strconv.Atoi(v)
	if err != nil || ms <= 0 {
		fmt.Fprintf(os.Stderr, "SAGE: ignoring invalid %s=%q (want positive milliseconds); using %s\n", name, v, fallback)
		return fallback
	}
	return time.Duration(ms) * time.Millisecond
}

func currentFenceTimings() fenceTimings {
	fenceTimingMu.RLock()
	defer fenceTimingMu.RUnlock()
	return fenceTiming
}

// SetFenceTimingsForTest shrinks the reconciliation schedule so a test that
// raises a fence does not pay production backoff at teardown. It returns a
// restore func; call it with defer.
//
// EXPORTED, unlike the package-internal test helper, because the callers that
// need it are in OTHER packages. web/ has many tests that raise a real fence,
// and each was paying the production 2s first-retry interval while draining.
// The env knobs cannot help them: they are read once in this package's init(),
// which runs before any foreign TestMain can set them. On a ~203s suite that
// per-test cost pushed `go test -race ./web` past the DEFAULT 10-minute package
// timeout, so the documented command failed even with every test correct — a
// green branch that looks red to anyone running the command we tell them to run.
//
// Production callers must not use this. It exists so tests can be honest about
// fence behaviour rather than avoiding fences to stay fast.
func SetFenceTimingsForTest(attempt, retry, retryMax, report time.Duration) (restore func()) {
	fenceTimingMu.Lock()
	previous := fenceTiming
	fenceTiming = fenceTimings{attempt: attempt, retry: retry, retryMax: retryMax, report: report}
	fenceTimingMu.Unlock()
	return func() {
		fenceTimingMu.Lock()
		fenceTiming = previous
		fenceTimingMu.Unlock()
	}
}

// signerHex renders a fence's map key (the raw ed25519 public key) as the hex
// agent id an operator sees everywhere else in SAGE, so a fence can be matched
// to an agent without a translation step. The PUBLIC key, never a key path:
// paths are the one signer-identifying string that must never be logged.
func signerHex(key string) string { return hex.EncodeToString([]byte(key)) }

// signerPrefixLen is how much of the agent id goes into a log line. 16 hex
// characters is what cmd/sage-gui already prints for an operator id, so a fence
// event greps against the rest of SAGE's output, and it is far past any
// collision an operator could hit on one node.
const signerPrefixLen = 16

// signerPrefix is the logged form of a signer. The full id stays available
// through FencedSigners for a status surface that needs to match a fence to an
// exact agent record.
func signerPrefix(key string) string {
	full := signerHex(key)
	if len(full) <= signerPrefixLen {
		return full
	}
	return full[:signerPrefixLen] + "…"
}

// fenceNonceField renders the fenced nonce, or nothing when the transaction did
// not decode. An empty-valued field is dropped by emitFenceEvent, so an
// undecodable transaction simply reports no nonce rather than a misleading 0 —
// 0 is a real value app-v9 treats as a permanently invalid sentinel.
func fenceNonceField(nonce uint64, ok bool) fenceField {
	if !ok {
		return fenceField{}
	}
	return fenceNum("nonce", nonce)
}

// fenceSubmission closes key behind an indeterminate submission and starts the
// background reconciliation that owns proving its fate.
//
// It MUST be called before the caller's lease release runs, so the next holder
// of the slot finds the fence already set rather than racing past it.
func fenceSubmission(key string, ind *indeterminateSubmit) {
	txHash := ""
	if len(ind.encoded) > 0 {
		hash := CometTxHash(ind.encoded)
		txHash = strings.ToUpper(hex.EncodeToString(hash[:]))
	}
	nonce, hasNonce := fencedTxNonce(ind.encoded)
	if len(ind.encoded) == 0 && ind.recordedTxHash != "" {
		// Restored from durable intent: the identity was recorded before the
		// previous process died, and there are no bytes to re-derive it from.
		txHash = strings.ToUpper(ind.recordedTxHash)
		nonce, hasNonce = ind.recordedNonce, ind.hasRecordedNonce
	}

	fenceMu.Lock()
	fence := fences[key]
	raised := fence == nil
	if raised {
		fence = &keyFence{
			ch:       make(chan struct{}),
			txHash:   txHash,
			nonce:    nonce,
			hasNonce: hasNonce,
			since:    time.Now(),
			cause:    ind.cause,
		}
		fences[key] = fence
	}
	fence.pending++
	fenceMu.Unlock()

	metrics.NonceFenceIndeterminateTotal.Inc()
	publishFenceGauges()
	// The cause is the CATEGORY, never ind.err's message: that message is the
	// broadcast error, and a broadcast URL carries the entire signed
	// transaction. See fenceCause.
	emitFenceEvent("fence_set",
		fenceKV("signer", signerPrefix(key)),
		fenceKV("tx_hash", txHash),
		fenceNonceField(nonce, hasNonce),
		fenceKV("cause", string(ind.cause)),
		fenceKV("note", fenceSetNote(ind)))

	// Reconciliation is per SUBMISSION — each one owns proving its own bytes and
	// retiring its own pending count. The alarm is per FENCE, so it starts only
	// with the fence itself; a second submission joining an existing fence would
	// otherwise double the operator's alarm for one stuck key.
	go reconcileFencedSubmission(key, fence, ind)
	if raised {
		go alarmHeldFence(key, fence)
	}
}

// fenceSetNote says what will actually settle THIS fence. The two shapes do not
// settle the same way: a live fence holds the exact bytes and reconciliation
// re-submits them, while a fence restored from durable intent has no bytes and
// is settled by reading the chain. Telling an operator that a restored fence is
// being re-submitted is the kind of small falsehood that costs an afternoon,
// because it is the line they read first.
func fenceSetNote(ind *indeterminateSubmit) string {
	if len(ind.encoded) == 0 {
		return "no transaction will be signed with this key until that exact transaction's fate is PROVEN; " +
			"the signed bytes did not survive the previous process, so this fence is settled by READING the " +
			"chain (the recorded hash in a committed block, or a committed nonce at or above the allocation)"
	}
	return "no transaction will be signed with this key until that exact transaction's fate is PROVEN; " +
		"reconciliation re-submits the identical bytes to force an answer"
}

// liftFence retires one reconciliation and opens the key once none remain.
//
// It is only ever reached from a PROVEN verdict. There is deliberately no
// deferred call to this function anywhere: lifting must be a decision taken on
// evidence, and a `defer liftFence(...)` around the reconciliation goroutine —
// which is what an earlier revision did — makes the fence lift as a side effect
// of the goroutine merely ending, including when it ends by panicking.
//
// Closing ch (rather than sending) is what makes every queued waiter wake.
// retiring selects exactly one durable cleanup before the key opens; lifted
// prevents a later proof from retiring the same fence again.
func liftFence(key string, fence *keyFence, verdict TxVerdict, detail string) {
	fenceMu.Lock()
	if fence.retiring || fence.lifted {
		fenceMu.Unlock()
		return
	}
	fence.pending--
	if fence.pending > 0 {
		fenceMu.Unlock()
		return
	}
	fence.retiring = true
	held := time.Since(fence.since)
	attempts := fence.attempts
	txHash := fence.txHash
	nonce, hasNonce := fence.nonce, fence.hasNonce
	fenceMu.Unlock()

	// Retire the durable shadow BEFORE opening the key. The store deletes by
	// signer: opening first would let a new submission save its intent, then
	// this prior retirement could delete that new record. Keep the fence in
	// the map and leave its waiter channel open until cleanup returns, without
	// holding the process-wide mutex over storage I/O. A deletion failure keeps
	// its existing logged, degraded-persistence policy.
	discardFenceIntent(key)

	fenceMu.Lock()
	fence.lifted = true
	// Only delete OUR entry: a later fence for the same key is a different
	// object, and deleting it here would open a key that is still closed.
	if fences[key] == fence {
		delete(fences, key)
	}
	close(fence.ch)
	fenceMu.Unlock()
	metrics.NonceFenceResolvedTotal.WithLabelValues(verdict.metricFate()).Inc()
	publishFenceGauges()
	// Keep the outcome visible after the fence (and the status row that shows
	// it) is gone. See FenceResolution.
	recordFenceResolution(verdict.metricFate(), signerPrefix(key), txHash, nonce, hasNonce, held, detail)
	emitFenceEvent("fence_lift",
		fenceKV("signer", signerPrefix(key)),
		fenceKV("tx_hash", txHash),
		fenceNonceField(nonce, hasNonce),
		fenceKV("fate", verdict.metricFate()),
		fenceAge("held_for", held),
		fenceNum("attempts", uint64(attempts)), // #nosec G115 -- attempts is a non-negative counter
		fenceKV("detail", detail))
}

// metricFate is the stable label/field for the outcome that lifted a fence.
// Only verdicts that are safe to lift on can appear here; "unresolved" would
// mean something lifted a fence without proof, which is the bug this file
// exists to prevent, so it is named loudly enough to be spotted in a dashboard.
//
// "abandoned" is deliberately its own label rather than folded into
// "superseded": it is an operator decision made in the absence of proof, and a
// dashboard that showed it as a proven fate would hide the one lift in this
// system whose payload the chain never gave a verdict on.
func (v TxVerdict) metricFate() string {
	switch v {
	case TxVerdictCommitted:
		return "committed"
	case TxVerdictRejected:
		return "rejected"
	case TxVerdictSuperseded:
		return "superseded"
	case TxVerdictSpent:
		return "spent"
	case TxVerdictAbandoned:
		return "abandoned"
	default:
		return "unresolved_BUG"
	}
}

// awaitFenceLifted blocks until key is open, or ctx dies first.
//
// It BLOCKS rather than failing open. Allocating a nonce past a fence is the
// exact move that kills the abandoned transaction, so a caller that cannot wait
// must be refused, and the fence must survive that refusal — hence the error
// path here leaves the fence exactly as it found it.
//
// The loop re-reads the map after each lift because the object it waited on is
// evicted on lifting; a fence set again in that window is a different object and
// must also be honored.
func awaitFenceLifted(ctx context.Context, key string) error {
	for {
		fenceMu.Lock()
		fence := fences[key]
		fenceMu.Unlock()
		if fence == nil {
			return nil
		}
		select {
		case <-fence.ch:
		case <-ctx.Done():
			// Both sentinels, so a caller can distinguish "fenced" from "my
			// deadline was already blown" without parsing the message.
			return fmt.Errorf("%w: %w", ErrSignerFenced, ctx.Err())
		}
	}
}

// keyIsFenced reports whether key currently has a fence. Used by tests; the
// production path waits on the fence rather than polling it.
func keyIsFenced(key string) bool {
	fenceMu.Lock()
	defer fenceMu.Unlock()
	_, ok := fences[key]
	return ok
}

// FenceForSigner reports the fence currently held for sk's signing key, if any.
//
// WHY A READ-ONLY ACCESSOR EXISTS. The lease's own fence wait BLOCKS, bounded
// only by the caller's context. That is right for a background producer — the
// voter, a federation drain — which would rather wait a moment behind a
// transient fence than drop its work. It is wrong for a path that has to answer
// a person or an agent: a fence held for hours consumes that caller's entire
// deadline, and a typed, actionable refusal arrives — if at all — as a bare
// client timeout, which is exactly how a fenced node read to the agents that
// reported "writes timed out, no error". Those paths ask this first and refuse
// immediately with ErrSignerFenced (HTTP 503 + Retry-After); the lease still
// covers the window where a fence appears after the check.
func FenceForSigner(sk ed25519.PrivateKey) (FencedSigner, bool) {
	if len(sk) != ed25519.PrivateKeySize {
		return FencedSigner{}, false
	}
	pub, ok := sk.Public().(ed25519.PublicKey)
	if !ok {
		return FencedSigner{}, false
	}
	key := string(pub)
	fenceMu.Lock()
	fence := fences[key]
	var held FencedSigner
	if fence != nil {
		held = fence.snapshotLocked(key, time.Now())
	}
	fenceMu.Unlock()
	return held, fence != nil
}

// FencedSigner is a diagnostic snapshot of one HELD fence.
//
// A held fence refuses every signing request for its key with ErrSignerFenced,
// and under the corrected invariant it can be held indefinitely. That is only
// defensible if it is INSPECTABLE, so this is the read side of the argument at
// the top of this file: an operator (or a status handler) can see exactly which
// key is closed, which transaction it is waiting on, how long it has waited and
// what the last reconciliation attempt reported.
type FencedSigner struct {
	// SignerPubKeyHex is hex(ed25519 public key) — the same agent id used
	// everywhere else in SAGE.
	SignerPubKeyHex string
	// SignerPubKeyPrefix is the short form used in log lines, so an event and a
	// status row can be matched by eye.
	SignerPubKeyPrefix string
	// TxHash is the CometBFT hash of the abandoned transaction, empty when the
	// adopter fenced without encoded bytes (which is itself unprovable).
	TxHash string
	// Nonce is the allocation the key is stuck on, and HasNonce is false when
	// the transaction would not decode. This is the field that makes a fence
	// ACTIONABLE: it can be compared against what the chain has committed for
	// this signer. It is not secret — it travels in the clear inside the
	// transaction and ends up on-chain.
	Nonce    uint64
	HasNonce bool
	// Cause is the TYPED CATEGORY of the failure that raised the fence
	// (timeout / canceled / transport / decode / rpc), never the underlying
	// message. The message is withheld deliberately: the broadcast error
	// contains the full request URL and therefore the entire signed
	// transaction. See fenceCause.
	Cause string
	// Resolution says HOW this fence can end, which is the difference between a
	// fence that will clear itself and one that needs an operator. It is derived
	// from the cause rather than stored, and it exists because a caller reading
	// status had no way to tell the two apart: "reconciliation is re-submitting
	// the identical bytes" is TRUE for a fence raised by a live indeterminate
	// submit and FALSE for one restored from durable intent, whose bytes did not
	// survive the process. Agents reported the false version back as "the
	// self-heal is not working" — the same symptom, two different diagnoses.
	//
	// Values: "reconciling" (live fence, bytes in hand, re-submitted until
	// consensus answers), "proof_or_operator" (restored fence, lifts on a proof
	// read from the chain or on an explicit operator abandon), or
	// "unknown" for a cause this build does not classify.
	Resolution string
	// Since / HeldFor are when the fence was raised and how long it has stood.
	Since   time.Time
	HeldFor time.Duration
	// Attempts is the number of reconciliation attempts made so far.
	Attempts int
	// LastAttemptAt / LastCause / LastDetail describe the most recent attempt,
	// which is what tells an operator whether this is a dead node, a missing
	// resolver or a transaction genuinely still pending. LastDetail is scrubbed
	// of anything that could be an encoded transaction before it is stored.
	LastAttemptAt time.Time
	LastCause     string
	LastDetail    string
}

// FencedSigners returns a snapshot of every currently held fence, oldest first.
// An empty result means no signing key is fenced.
func FencedSigners() []FencedSigner {
	now := time.Now()
	fenceMu.Lock()
	out := make([]FencedSigner, 0, len(fences))
	for key, fence := range fences {
		out = append(out, fence.snapshotLocked(key, now))
	}
	fenceMu.Unlock()

	sort.Slice(out, func(i, j int) bool {
		if out[i].Since.Equal(out[j].Since) {
			return out[i].SignerPubKeyHex < out[j].SignerPubKeyHex
		}
		return out[i].Since.Before(out[j].Since)
	})
	return out
}

// snapshotLocked renders one fence for the diagnostics surfaces and for the
// prover hook. The caller MUST hold fenceMu, and the returned value is a copy:
// handing the live struct (or its channel) to a prover would let an adopter
// observe or race the fence's own state — and no caller outside this file ever
// needs more than what the diagnostics already publish.
func (f *keyFence) snapshotLocked(key string, now time.Time) FencedSigner {
	return FencedSigner{
		SignerPubKeyHex:    signerHex(key),
		SignerPubKeyPrefix: signerPrefix(key),
		TxHash:             f.txHash,
		Nonce:              f.nonce,
		HasNonce:           f.hasNonce,
		Cause:              string(f.cause),
		Resolution:         fenceResolution(f.cause),
		Since:              f.since,
		HeldFor:            now.Sub(f.since),
		Attempts:           f.attempts,
		LastAttemptAt:      f.lastAt,
		LastCause:          string(f.lastCause),
		LastDetail:         f.lastDetail,
	}
}

// fenceResolution answers "how does this fence end?" from its cause. It is the
// one field a caller needs to tell a self-clearing fence from one that needs an
// operator, and it is derived here, beside the cause, so the two can never
// disagree.
func fenceResolution(cause fenceCause) string {
	switch cause {
	case fenceCauseRestored:
		// The bytes are gone with the process that sent them, so reconciliation
		// has nothing to re-submit: this fence lifts on a proof read from the
		// chain, or on an explicit operator abandon when no proof can exist.
		return "proof_or_operator"
	case fenceCauseNoProver, fenceCauseNoProof:
		// Only the restored path records these: its instrument is the proof
		// reader, and "the reader is missing" or "the reader sees no proof yet"
		// both mean the same thing to a caller — this fence ends on a proof, or
		// on an operator decision, never by re-submission.
		return "proof_or_operator"
	case fenceCauseTimeout, fenceCauseCanceled, fenceCauseTransport, fenceCauseDecode, fenceCauseRPC,
		fenceCausePending, fenceCauseNoResolver, fenceCauseNoEncodedTx, fenceCausePanic, fenceCauseSubmitPanic,
		fenceCauseSubmitError:
		// Everything else still holds the exact bytes that went out (or failed
		// for a reason reconciliation retries), so the reconciler keeps pushing
		// them until consensus answers.
		return "reconciling"
	default:
		return "unknown"
	}
}

// errNoTxResolver and errNoEncodedTx are the two ways a fence can be raised with
// nothing able to prove its transaction's fate. Both are held, not conceded:
// "we have no way to check" is not evidence that the transaction is gone, and
// reopening on it is the same silent Code 4 loss as reopening on a timer.
//
// NEITHER MESSAGE MAY SUGGEST A RESTART. An earlier revision of both told the
// operator to restart, on the theory that SetNonceFloorFunc re-seeds the
// allocator from the highest COMMITTED on-chain nonce. That advice was false and
// false in the direction that loses transactions: the abandoned nonce is
// unresolved precisely because it may still be in flight ABOVE the committed
// floor, so a restart drops the fence, seeds below the abandoned nonce, and
// issues one that overtakes it. See the file header.
var (
	errNoTxResolver = errors.New("no transaction resolver is wired for this signing key, " +
		"so nothing can prove this transaction's fate; install one with tx.SetTxResolverFunc — " +
		"it is re-read on every attempt, so a late install rescues this fence")
	errNoEncodedTx = errors.New("the indeterminate submission carried no encoded transaction, " +
		"so it can be neither identified nor re-submitted; the adopter must pass the exact bytes " +
		"it put on the wire to tx.Indeterminate")
	errNoFenceProver = errors.New("no fence proof reader is wired, so a fence restored from durable " +
		"intent cannot be re-checked against the chain; install one with tx.SetFenceProverFunc — " +
		"it is re-read on every attempt, so a late install rescues this fence")
)

// ErrSigningQuiesced is returned by WithNonceLease once signing has been
// quiesced for a coordinated restart. Like ErrSignerFenced it means NOTHING WAS
// SIGNED OR SENT, and it must never be reported as a consensus rejection.
//
// Why quiesce at all: a transaction signed into a teardown that is already under
// way is the most likely transaction in the node's life to end up with an
// unobserved fate, and an unobserved fate at that moment is exactly the one the
// in-process fence cannot carry across the restart. Refusing to start is free;
// losing the fence is not.
var ErrSigningQuiesced = errors.New("signing is quiesced for a coordinated restart: nothing was signed or sent")

// signingQuiesced is set while a coordinated restart is draining. It is an
// atomic rather than a mutex on purpose: WithNonceLease reads it on every
// allocation for every key, and the hard constraint that distinct keys stay
// fully concurrent rules out a shared lock on that path.
var signingQuiesced atomic.Bool

// QuiesceSigningForRestart stops new nonce allocations and returns the function
// that resumes them. Call it once a RESTART is actually committed, or when an
// ORDINARY exit (a signal or a serve error) begins draining — both teardowns are
// the most likely moment in the process's life for a submission to end with an
// unobserved outcome. Call the returned function if the restart is abandoned,
// or the node will keep running with signing off; an ordinary exit never
// resumes, because it is leaving.
//
// It does NOT wait for in-flight submissions and deliberately does not block:
// every caller fails fast with ErrSigningQuiesced instead of parking inside a
// teardown that is trying to join those very goroutines.
// It is IDEMPOTENT in both directions, and both events fire only on a real
// transition. The restart path reaches this from more than one place (the
// shutdown select and the queued-request adoption that follows it), and two call
// sites racing to announce the same state change would make the log say
// something happened twice that happened once.
func QuiesceSigningForRestart() (resume func()) {
	if signingQuiesced.CompareAndSwap(false, true) {
		emitFenceEvent("signing_quiesced", fenceKV("reason", "restart or shutdown draining"))
	}
	return func() {
		if signingQuiesced.CompareAndSwap(true, false) {
			emitFenceEvent("signing_resumed", fenceKV("reason", "coordinated restart abandoned"))
		}
	}
}

// WaitForSigningIdle blocks until no signing key has an in-flight or queued
// submission, or ctx expires first. On success every lease slot has been
// released, which means every submission that was going to raise a fence has
// already raised it (fenceSubmission runs before the lease release on the
// indeterminate path).
//
// This is the second half of the restart veto's ordering guarantee. Checking
// RestartVetoReason alone is a time-of-check race: the check can pass while a
// submission is still in flight, the drain then severs that submission's
// connection (the force-close after the HTTP shutdown budget is precisely the
// kind of event that manufactures an indeterminate outcome), and the fence it
// raises appears AFTER the only veto that was ever going to run. So the restart
// path must quiesce signing first, wait here until the in-flight population is
// zero, and only THEN evaluate the veto — at that point the fence map can no
// longer grow, and a clean veto is a clean veto for the whole drain.
//
// It deliberately counts QUEUED callers as busy, not only slot holders: a
// queued caller past the entry quiesce check still exits through the
// re-check after acquiring the slot, but until it has done so it is a
// participant whose refusal has not happened yet.
//
// Polling under leaseMu is deliberate: the alternative is threading a
// condition variable through every lease release for the benefit of a path
// that runs once per restart. 25ms against a map read is noise there, and this
// is never on a signing hot path.
func WaitForSigningIdle(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		leaseMu.Lock()
		busy := len(leases)
		leaseMu.Unlock()
		if busy == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%d signing key(s) still have an in-flight or queued submission: %w", busy, ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
}

// RestartVetoReason returns a non-empty, operator-facing reason why a
// COORDINATED restart must not proceed, or "" when there is nothing outstanding.
//
// THIS IS THE ONE PLACE THE CROSS-RESTART RESIDUAL IS ACTIVELY PREVENTED RATHER
// THAN MERELY DOCUMENTED. The fence lives in memory only, so a restart taken
// while a fence is held throws away the only record that nonce N might still be
// in flight; the allocator then re-seeds from the highest COMMITTED nonce (which
// is still below N), issues some M, M commits, and the late N is rejected
// Code 4. A crash can still do that to us. Our own updater does not have to.
//
// The caller is responsible for FAILING CLOSED — if it cannot reach this
// function at all, it must veto anyway. A veto that degrades to "proceed" when
// it malfunctions is worse than no veto, because the release notes will say the
// case is handled.
//
// The text is deliberately free of any "restart anyway to clear it" escape
// hatch, because there isn't one.
func RestartVetoReason() string {
	held := FencedSigners()
	if len(held) == 0 {
		return ""
	}
	// WHICH FENCES STILL NEED THIS VETO. Its argument is that a restart
	// "discards the only record that a transaction may still be in flight" —
	// and since durable intent landed, that is only true for a fence whose
	// record is NOT on disk. A fence shadowed by an intent row is re-raised at
	// the next start (RestoreFencesFromIntents), so the restart cannot lose the
	// record, and the allocator cannot re-seed past the abandoned nonce. What a
	// restart DOES cost is the re-submission ability, because the signed bytes
	// live in this process only — which is why the restored fence re-reads its
	// proofs and the abandon route exists.
	//
	// Leaving the veto blanket-wide made an unprovable fence an upgrade dead
	// end: the node refused the restart that would install the fix, and the fix
	// was the only thing that could clear the fence. That is the shape users
	// reported. A restart is now refused only when the record itself is at
	// stake, and the store read FAILS CLOSED: an unwired, unreadable or empty
	// store leaves every fence protected.
	if durableSigners, ok := durableFenceIntentSigners(); ok {
		unprotected := held[:0:0]
		for _, f := range held {
			if _, durable := durableSigners[strings.ToLower(f.SignerPubKeyHex)]; !durable {
				unprotected = append(unprotected, f)
			}
		}
		if len(unprotected) == 0 {
			emitFenceEvent("fence_restart_allowed_durable",
				fenceNum("fences", uint64(len(held))), // #nosec G115 -- non-negative count
				fenceKV("note", "every held fence is shadowed by a durable intent record, so this restart "+
					"re-raises them at the next start instead of discarding them; each key stays refusing to "+
					"sign until a fate is proven (the node re-reads the proofs) or an operator abandons it"))
			return ""
		}
		held = unprotected
	}
	oldest := held[0]
	detail := fmt.Sprintf("signing key %s is fenced on tx %s", oldest.SignerPubKeyPrefix, oldest.TxHash)
	if oldest.HasNonce {
		detail += fmt.Sprintf(" (nonce %d)", oldest.Nonce)
	}
	// The closing advice has to describe what will ACTUALLY end the hold. Every
	// fence that reaches this point is UNPROTECTED — no durable record could be
	// read for it — so the first question is why the record is missing, because
	// the record is what re-raises the fence after a restart.
	tail := "Restarting now would discard the only record that this transaction may still be in " +
		"flight, and a later transaction would then be rejected as a replay. Keep the node running: " +
		"reconciliation is re-submitting the identical bytes and lifts the fence the moment a fate is " +
		"proven, and the missing durable record is itself a fault to chase (fence_intent_write_failed " +
		"and fence_intent_clear_failed events in the node log)."
	if oldest.Cause == string(fenceCauseRestored) {
		tail = "Its signed bytes did not survive the previous shutdown, and its durable record could not " +
			"be read: that record is what re-raises this fence at the next start, so without it a restart " +
			"would let the allocator seed past the abandoned nonce. Check the intent store first. A proof " +
			"readable from the chain lifts this fence through POST /v1/dashboard/signer-fence/lift, and a " +
			"transaction nothing can deliver back — no peers, no mempool copy, no committed fate, unspent " +
			"allocation — through POST /v1/dashboard/signer-fence/abandon, which records the decision " +
			"rather than claiming a proof. See the node log for nonce_fence events."
	}
	return fmt.Sprintf(
		"%d signing key(s) are awaiting proof of an earlier submission's fate — %s, held for %s. %s",
		len(held), detail, oldest.HeldFor.Round(time.Second), tail)
}

// durableFenceIntentSigners reads the durable records once and reports the
// signers they cover. ok=false means the question could not be answered — no
// store wired, the read failed, or it did not finish inside the bound — and the
// caller must treat every fence as unprotected. The read is bounded because it
// sits on the restart path: a store that hangs must not hang a restart, and
// "we could not check" resolves to the safe answer (veto), never to the
// convenient one.
func durableFenceIntentSigners() (map[string]struct{}, bool) {
	store := currentFenceIntentStore()
	if store == nil {
		return nil, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	type readResult struct {
		intents []FenceIntent
		err     error
	}
	// Buffered, so a store that ignores ctx cannot leak the goroutine forever
	// on the send. The goroutine itself may outlive the deadline (a synchronous
	// SQLite call cannot be canceled); the restart path tolerates that, and the
	// read is a single small table.
	done := make(chan readResult, 1)
	go func() {
		intents, err := store.ListFenceIntents(ctx)
		done <- readResult{intents: intents, err: err}
	}()
	select {
	case res := <-done:
		if res.err != nil {
			return nil, false
		}
		signers := make(map[string]struct{}, len(res.intents))
		for _, intent := range res.intents {
			signers[strings.ToLower(strings.TrimSpace(intent.SignerPubKeyHex))] = struct{}{}
		}
		return signers, true
	case <-ctx.Done():
		return nil, false
	}
}

// ReportFencesDroppedAtShutdown writes one final fence_dropped_at_shutdown
// event per held fence, for an exit that is going to happen regardless — a
// SIGTERM/SIGINT, a serve error, or a coordinated-restart final gate that has
// already failed. It changes nothing about the exit; a plain signal is not a
// coordinated restart and the veto correctly cannot apply to it.
//
// Why the terminal record matters: the fence map dies with the process, the
// next start seeds from the committed floor, and if the unresolved transaction
// then loses the race the operator sees an unrelated later action rejected
// Code 4. The last periodic fence_held line can be up to a report interval
// (60s default) old, and without this event nothing in the log says fences
// were dropped AT the exit — so the eventual loss has no traceable cause. This
// line is what lets a post-mortem connect the Code 4 back to the shutdown that
// discarded the record.
//
// The note states the residual plainly and does NOT claim the exit is safe;
// the reason field says which exit path dropped it. Same redaction rules as
// every other fence event: signer prefix, hash, nonce — never a key path, never
// the encoded transaction.
func ReportFencesDroppedAtShutdown(reason string) {
	for _, held := range FencedSigners() {
		emitFenceEvent("fence_dropped_at_shutdown",
			fenceKV("signer", held.SignerPubKeyPrefix),
			fenceKV("tx_hash", held.TxHash),
			fenceNonceField(held.Nonce, held.HasNonce),
			fenceAge("held_for", held.HeldFor),
			fenceNum("attempts", uint64(held.Attempts)), // #nosec G115 -- attempts is a non-negative counter
			fenceKV("last_cause", held.LastCause),
			fenceKV("reason", reason),
			fenceKV("note", "this in-process record does NOT survive the exit; the next start re-seeds from the "+
				"committed floor with no knowledge of this transaction, and if it is still in flight a later "+
				"Code 4 replay rejection for this signer traces back to this drop"))
	}
}

// reconcileFencedSubmission proves the fate of one exact transaction, and lifts
// the fence only when it has. It retries FOREVER while the fence is up: this
// goroutine is the fence's reconciliation, so it living as long as the fence is
// the intended shape, not a leak. It ends exactly when it lifts.
//
// There is no budget, and that is the point. A budget expiring says nothing
// about where the transaction is, so ending reconciliation on one would reopen
// the key on a clock — the failure this file was reworked to remove.
func reconcileFencedSubmission(key string, fence *keyFence, ind *indeterminateSubmit) {
	if len(ind.encoded) == 0 && ind.recordedTxHash != "" {
		reconcileRestoredFence(key, fence)
		return
	}
	unresolved := 0
	for {
		if fenceResolved(key, fence) {
			return
		}
		timing := currentFenceTimings()

		outcome, cause, err := resolveOnce(key, fence, ind, timing.attempt)
		if err != nil {
			// A failed attempt can never be a verdict. Forcing it here means an
			// adopter's resolver cannot lift a fence by returning a verdict
			// alongside an error, deliberately or by accident.
			outcome.Verdict = TxVerdictUnresolved
			if outcome.Detail == "" {
				outcome.Detail = err.Error()
			}
		}
		// Scrub UNCONDITIONALLY, at the boundary, because everything past this
		// line is stored or logged. The resolver is caller-supplied — an adopter
		// can hand back a detail built from an *url.Error whose message is the
		// broadcast URL, i.e. the whole signed transaction — so trusting the
		// resolver to have sanitized its own text would put the leak back one
		// package away from where it can be reviewed.
		outcome.Detail = scrubFenceText(outcome.Detail, ind.encoded)
		attempt := recordFenceAttempt(fence, cause, outcome.Detail)

		switch outcome.Verdict {
		case TxVerdictCommitted, TxVerdictRejected:
			emitFenceEvent(fateEventName(outcome.Verdict),
				fenceKV("signer", signerPrefix(key)),
				fenceKV("tx_hash", fenceTxHash(fence)),
				fenceNum("attempt", uint64(attempt)), // #nosec G115 -- attempt is a non-negative counter
				fenceKV("detail", outcome.Detail))
			liftFence(key, fence, outcome.Verdict, outcome.Detail)
			return
		}

		metrics.NonceFenceReconcileFailuresTotal.WithLabelValues(string(cause)).Inc()
		unresolved++
		reportReconcileRetry(key, fence, cause, outcome.Detail, attempt)
		time.Sleep(fenceRetryDelay(timing, unresolved))
	}
}

// fenceResolved reports whether this reconciliation's fence is already over —
// lifted on a proven fate, or replaced by a later fence for the same key.
//
// WHY THE LOOP MUST ASK. Reconciliation retries for as long as the fence
// stands, and the fence can now also be lifted from OUTSIDE this goroutine:
// LiftFenceWithProof retires a restored fence on an operator-read proof, and
// AbandonUnprovableFence retires one on an explicit operator decision. Without
// this check the goroutine keeps its backoff running against a fence that no
// longer exists — on the live path it would go on RE-SUBMITTING bytes whose
// fate the lift already settled, which is pointless traffic at best. The
// counter it would decrement no longer belongs to a held fence.
func fenceResolved(key string, fence *keyFence) bool {
	fenceMu.Lock()
	defer fenceMu.Unlock()
	return fence.lifted || fences[key] != fence
}

// reconcileRestoredFence drives the only instrument a byte-less fence has: the
// process-wide proof reader (SetFenceProverFunc), re-read on the same backoff as
// live reconciliation for as long as the fence stands.
//
// WHY "PROVEN FATE" IS NOT THE SAME AS "WAITING FOR AN OPERATOR". An earlier
// revision emitted one fence_restored_waiting_for_proof event and parked on the
// fence channel. That was defensible while the only proof surface was the
// operator's POST — but the proofs the fence accepts are read from the NODE
// (the recorded hash in the transaction index, the signer's committed nonce),
// and a node is perfectly capable of reading them itself. Parking meant nobody
// did: a fence whose transaction had committed before the process died held
// its key, and refused every coordinated restart (i.e. every update), for as
// long as the node ran.
func reconcileRestoredFence(key string, fence *keyFence) {
	emitFenceEvent("fence_restored_waiting_for_proof",
		fenceKV("signer", signerPrefix(key)),
		fenceKV("tx_hash", fenceTxHash(fence)),
		fenceNonceField(fence.nonce, fence.hasNonce),
		fenceKV("note", "the signed bytes did not survive the restart, so this fence cannot be resolved "+
			"by re-submission; it lifts on a proven fate (the recorded hash in a committed block, or a "+
			"committed nonce at or above the fenced allocation), which the node re-reads on every attempt"))

	unresolved := 0
	for {
		if fenceResolved(key, fence) {
			return
		}
		timing := currentFenceTimings()

		proof, cause, detail, err := proveRestoredOnce(key, fence, timing.attempt)
		if err != nil {
			// No proof. Before recording another retry, ask whether this fence
			// can be settled WITHOUT one: a node that is caught up, has never
			// had a peer this run, holds no mempool copy and sees no committed
			// fate is the one case where the payload cannot come back, and on
			// the desktop product there is no operator standing by to declare
			// it — the node would otherwise refuse every write and every update
			// for as long as it ran.
			resolved, resolveDetail, resolveErr := autoResolveRestoredOnce(key, fence, timing.attempt)
			if resolveErr == nil && resolved {
				emitFenceEvent("fate_abandoned",
					fenceKV("signer", signerPrefix(key)),
					fenceKV("tx_hash", fenceTxHash(fence)),
					fenceNum("attempt", uint64(fenceAttempts(fence))), // #nosec G115 -- non-negative counter
					fenceKV("detail", resolveDetail))
				liftFence(key, fence, TxVerdictAbandoned, resolveDetail)
				return
			}
			// A failed proof read is never a verdict: "we could not ask" and
			// "the chain has not proven it yet" both leave the fence standing.
			//
			// WHEN THE AUTOMATIC ROUTE DECLINED, SAY WHY. Its refusal is the
			// answer to "why is this not clearing itself?" — peers connected,
			// node still catching up, or the mempool holding the transaction —
			// and it used to be computed and then dropped on the floor here, so
			// the status row and the held-fence alarm could only ever show the
			// proof-read error. That is why a node whose self-heal was refusing
			// on evidence read exactly like a node whose self-heal was broken.
			// EVERY outcome of the automatic route is recorded — a refusal AND a
			// fault. The refusal branch was added first and the error branch was
			// left out, which reproduced the exact blind spot this annotation
			// exists to close: on a node where the auto route could not even read
			// its evidence (an RPC surface answering unexpectedly, for instance),
			// the status row and the alarm stayed identical to a node whose
			// self-heal was merely waiting, and the field report read as "the
			// automatic route does not run at all".
			switch {
			case resolveErr != nil:
				detail = detail + "; the automatic resolution errored: " + scrubFenceText(resolveErr.Error(), nil)
			case !resolved && resolveDetail != "":
				detail = detail + "; the automatic resolution is declining: " + resolveDetail
			case !resolved:
				detail = detail + "; the automatic resolution declined without a reason (this is a bug: " +
					"every refusal path is expected to name its predicate)"
			}
			unresolved++
			attempt := recordFenceAttempt(fence, cause, detail)
			metrics.NonceFenceReconcileFailuresTotal.WithLabelValues(string(cause)).Inc()
			reportReconcileRetry(key, fence, cause, detail, attempt)
			time.Sleep(fenceRetryDelay(timing, unresolved))
			continue
		}

		// Re-validate against THIS fence before lifting. The prover is caller
		// supplied, and a proof for a different signer, a different transaction
		// or a lower nonce must be refused here exactly as it is on the
		// operator path — the validation is the fence's, not the prover's.
		verdict, liftDetail, validateErr := validateFenceLiftProof(signerPrefix(key), fence, proof)
		if validateErr != nil {
			unresolved++
			detail := scrubFenceText(validateErr.Error(), nil)
			attempt := recordFenceAttempt(fence, fenceCauseNoProof, detail)
			metrics.NonceFenceReconcileFailuresTotal.WithLabelValues(string(fenceCauseNoProof)).Inc()
			reportReconcileRetry(key, fence, fenceCauseNoProof, detail, attempt)
			time.Sleep(fenceRetryDelay(timing, unresolved))
			continue
		}

		emitFenceEvent(fateEventName(verdict),
			fenceKV("signer", signerPrefix(key)),
			fenceKV("tx_hash", fenceTxHash(fence)),
			fenceNum("attempt", uint64(fenceAttempts(fence))), // #nosec G115 -- non-negative counter
			fenceKV("detail", liftDetail))
		liftFence(key, fence, verdict, liftDetail)
		return
	}
}

// autoResolveRestoredOnce runs one startup-resolution attempt. A refusal from
// the resolver is NOT an error here: the common answer is "not yet" (the node
// is still catching up, or a peer has been seen), and it leaves the fence in
// place with its retry loop running.
func autoResolveRestoredOnce(key string, fence *keyFence, attempt time.Duration) (bool, string, error) {
	resolve := fenceAutoResolverFallback()
	if resolve == nil {
		return false, "", nil
	}
	fenceMu.Lock()
	held := fence.snapshotLocked(key, time.Now())
	fenceMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), attempt)
	defer cancel()
	resolved, detail, err := resolve(ctx, held)
	if err != nil {
		var refused *FenceAbandonRefusedError
		if errors.As(err, &refused) {
			// A refused resolution is information, not a fault: record WHY and
			// keep asking, so the status surface says what is holding the fence
			// rather than only that it is held.
			return false, refused.Reason, nil
		}
		return false, "", err
	}
	return resolved, detail, nil
}

// proveRestoredOnce runs one proof read under its own deadline, and converts a
// panic in the prover into an unresolved attempt exactly as resolveOnce does for
// a resolver: a panic is caller code failing, not an answer about a transaction.
func proveRestoredOnce(key string, fence *keyFence, attempt time.Duration) (proof FenceLiftProof, cause fenceCause, detail string, err error) {
	defer func() {
		if r := recover(); r != nil {
			panicked := scrubFenceText(fmt.Sprint(r), nil)
			proof = FenceLiftProof{}
			cause = fenceCausePanic
			detail = "fence prover panicked: " + panicked
			err = errors.New("fence prover panicked")
			reportResolverPanic(key, fence, panicked, scrubFenceText(string(debug.Stack()), nil))
		}
	}()

	prove := fenceProverFallback()
	if prove == nil {
		return FenceLiftProof{}, fenceCauseNoProver, errNoFenceProver.Error(), errNoFenceProver
	}
	fenceMu.Lock()
	held := fence.snapshotLocked(key, time.Now())
	fenceMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), attempt)
	defer cancel()
	proof, err = prove(ctx, held)
	if err != nil {
		return FenceLiftProof{}, classifyFenceCause(err), scrubFenceText(err.Error(), nil), err
	}
	return proof, "", "", nil
}

func fenceAttempts(fence *keyFence) int {
	fenceMu.Lock()
	defer fenceMu.Unlock()
	return fence.attempts
}

func fateEventName(verdict TxVerdict) string {
	switch verdict {
	case TxVerdictCommitted:
		return "fate_committed"
	case TxVerdictSpent:
		return "fate_spent"
	case TxVerdictAbandoned:
		return "fate_abandoned"
	default:
		// Rejected and superseded keep the event name they have always had:
		// both are "consensus refused to commit these bytes again", and no
		// operator alert or doc should have to learn a second spelling for it
		// in a bug-fix release.
		return "fate_rejected"
	}
}

func fenceTxHash(fence *keyFence) string {
	fenceMu.Lock()
	defer fenceMu.Unlock()
	return fence.txHash
}

// reportReconcileRetry emits one reconcile_retry event, RATE-LIMITED.
//
// The first few attempts always print: a fence that resolves quickly should
// still leave a trace of what it went through. After that the event is throttled
// to the same interval as the still-held alarm, because reconciliation runs for
// as long as the fence stands and a fence may now stand indefinitely — an
// unthrottled per-attempt line would turn one stuck signing key into an
// unbounded log and drown the events that matter.
const reconcileRetryVerboseAttempts = 3

func reportReconcileRetry(key string, fence *keyFence, cause fenceCause, detail string, attempt int) {
	report := currentFenceTimings().report
	now := time.Now()

	fenceMu.Lock()
	verbose := attempt <= reconcileRetryVerboseAttempts
	due := fence.lastRetryLogAt.IsZero() || now.Sub(fence.lastRetryLogAt) >= report
	if verbose || due {
		fence.lastRetryLogAt = now
	} else {
		fenceMu.Unlock()
		return
	}
	age := now.Sub(fence.since)
	txHash := fence.txHash
	nonce, hasNonce := fence.nonce, fence.hasNonce
	fenceMu.Unlock()

	emitFenceEvent("reconcile_retry",
		fenceKV("signer", signerPrefix(key)),
		fenceKV("tx_hash", txHash),
		fenceNonceField(nonce, hasNonce),
		fenceAge("fence_age", age),
		fenceNum("attempt", uint64(attempt)), // #nosec G115 -- attempt is a non-negative counter
		fenceKV("cause", string(cause)),
		fenceKV("detail", detail))
}

// fenceRetryDelay backs off exponentially from timing.retry up to
// timing.retryMax. It is recomputed from the CURRENT timings each iteration so
// a test (or an operator changing the environment before a restart) is not
// fighting a delay captured when the fence was raised.
func fenceRetryDelay(timing fenceTimings, unresolved int) time.Duration {
	shift := unresolved - 1
	if shift < 0 {
		shift = 0
	}
	if shift > 6 {
		shift = 6
	}
	delay := timing.retry << uint(shift)
	if delay <= 0 || delay > timing.retryMax {
		delay = timing.retryMax
	}
	return delay
}

// resolveOnce runs one reconciliation attempt under its own deadline, and
// converts a panic in the resolver into an UNRESOLVED attempt.
//
// A panic is caller-supplied code failing. It tells us NOTHING about the
// transaction: the bytes may be in a mempool about to commit, and lifting the
// fence here would let the next caller allocate a higher nonce and kill them.
// So it recovers (the node must survive a bad resolver), logs loudly with the
// stack (a silent recover would hide a real defect), keeps the fence, and lets
// the loop retry — the retry matters because a panic is often transient (a nil
// client during a reconnect) and the next attempt may well produce a verdict.
//
// The recovered value is SCRUBBED before it is logged. A panic value is very
// often an error, and an error from the broadcast path carries the request URL
// and therefore the signed transaction; a stack trace of a hex-formatting
// function can carry it too.
//
// The returned fenceCause labels the attempt for metrics and diagnostics. It is
// always derived from the failure's TYPE, never from its text.
func resolveOnce(key string, fence *keyFence, ind *indeterminateSubmit, attempt time.Duration) (outcome TxOutcome, cause fenceCause, err error) {
	defer func() {
		if r := recover(); r != nil {
			panicked := scrubFenceText(fmt.Sprint(r), ind.encoded)
			outcome = TxOutcome{Verdict: TxVerdictUnresolved}
			cause = fenceCausePanic
			err = errors.New("reconciliation resolver panicked: " + panicked)
			reportResolverPanic(key, fence, panicked, scrubFenceText(string(debug.Stack()), ind.encoded))
		}
	}()

	if len(ind.encoded) == 0 {
		return TxOutcome{Verdict: TxVerdictUnresolved}, fenceCauseNoEncodedTx, errNoEncodedTx
	}
	resolve := ind.resolve
	if resolve == nil {
		resolve = txResolverFallback()
	}
	if resolve == nil {
		return TxOutcome{Verdict: TxVerdictUnresolved}, fenceCauseNoResolver, errNoTxResolver
	}

	// Bounds THIS ATTEMPT only. Deliberately not called a budget: its expiry
	// produces another attempt, never a lift. The bound is COOPERATIVE — see
	// TxResolveFunc: a resolver that ignores ctx outlives it and stalls this
	// fence's loop (the alarm keeps firing; other keys are unaffected), and
	// running the resolver on a watchdog goroutine instead would not fix that
	// — the hung call would leak and a second concurrent attempt would run
	// against an adopter that was promised sequential attempts.
	ctx, cancel := context.WithTimeout(context.Background(), attempt)
	defer cancel()
	outcome, err = resolve(ctx, ind.encoded)
	return outcome, resolveAttemptCause(outcome.Verdict, err), err
}

// reportResolverPanic logs a panicking resolver loudly, and then keeps logging
// it only occasionally.
//
// LOUDLY, because a silent recover would hide a real defect and this is the one
// unresolved cause that indicts our own code rather than the network. OCCASIONALLY
// after that, because a resolver that panics usually panics every time, the fence
// retries for as long as it stands, and each report carries a stack trace — so an
// unthrottled version turns one bad resolver into gigabytes and buries every other
// fence event. The first report always fires; the throttle only applies to the
// repeats, which by construction say the same thing.
func reportResolverPanic(key string, fence *keyFence, panicked, stack string) {
	report := currentFenceTimings().report
	now := time.Now()

	fenceMu.Lock()
	if !fence.lastPanicLogAt.IsZero() && now.Sub(fence.lastPanicLogAt) < report {
		fenceMu.Unlock()
		return
	}
	fence.lastPanicLogAt = now
	txHash := fence.txHash
	fenceMu.Unlock()

	emitFenceEvent("resolver_panic",
		fenceKV("signer", signerPrefix(key)),
		fenceKV("tx_hash", txHash),
		fenceKV("panic", panicked),
		fenceKV("note", "the signing key STAYS FENCED and reconciliation will retry — a panic is the "+
			"resolver failing, not an answer about the transaction"),
		fenceKV("stack", stack))
}

// resolveAttemptCause labels one attempt. A clean unresolved answer is
// fenceCausePending — the node replied and declined to be definitive, which is
// the healthiest way to stay fenced and must not be filed next to a dead socket.
func resolveAttemptCause(verdict TxVerdict, err error) fenceCause {
	switch {
	case err != nil:
		return classifyFenceCause(err)
	case verdict == TxVerdictUnresolved:
		return fenceCausePending
	default:
		return ""
	}
}

// recordFenceAttempt updates the diagnostics a held fence is inspected through
// and returns this attempt's ordinal.
func recordFenceAttempt(fence *keyFence, cause fenceCause, detail string) int {
	fenceMu.Lock()
	defer fenceMu.Unlock()
	fence.attempts++
	fence.lastAt = time.Now()
	fence.lastCause = cause
	fence.lastDetail = detail
	return fence.attempts
}

// alarmHeldFence keeps announcing a fence that is still up, on its OWN goroutine
// and its own clock, until the fence lifts.
//
// It is deliberately not folded into the reconciliation loop. An alarm that only
// fires between attempts goes quiet exactly when it is most needed — a resolver
// that hangs, or one backing off at the retry ceiling, is a prime way for a fence
// to get stuck — and a stuck fence that stops complaining is the mystery hang
// this design promised not to produce. Like the reconciliation goroutine, it
// lives as long as the fence does; that is the fence being audible, not a leak.
func alarmHeldFence(key string, fence *keyFence) {
	for {
		timer := time.NewTimer(currentFenceTimings().report)
		select {
		case <-fence.ch:
			timer.Stop()
			return
		case <-timer.C:
		}
		if !reportHeldFence(key, fence) {
			return
		}
	}
}

// reportHeldFence emits one "still held" line and reports whether the fence is
// still up. A held fence refuses every signing request for its key, so it must
// keep saying so: the case for holding rather than conceding rests on the
// failure being loud and attributable.
// The note is worded with care. It says what is refused and what would end the
// hold, and it does NOT tell the operator to restart. An earlier revision did,
// claiming a restart re-seeded the allocator from the committed on-chain nonce
// and cleared the hold "with the chain's own evidence". That is false: the
// committed floor is BELOW the abandoned nonce — that is what unresolved means —
// so a restart drops the fence and then issues a nonce that overtakes a
// transaction which may still be in flight. Operator-facing text that recommends
// the exact action which loses the transaction is worse than no text at all.
func reportHeldFence(key string, fence *keyFence) bool {
	fenceMu.Lock()
	if fence.lifted {
		fenceMu.Unlock()
		return false
	}
	held := time.Since(fence.since)
	attempts := fence.attempts
	txHash := fence.txHash
	nonce, hasNonce := fence.nonce, fence.hasNonce
	cause := fence.lastCause
	detail := fence.lastDetail
	fenceMu.Unlock()

	publishFenceGauges()
	// The held-alarm note must describe THIS fence's route out. For a fence
	// restored from durable intent the bytes did not survive, so saying
	// "reconciliation keeps re-submitting the identical bytes" is false — and
	// it is not a harmless turn of phrase: an operator (or an agent reading the
	// log) takes it as "this clears itself", stops looking for the proof route,
	// and reads the honest wait as a broken self-heal. The status surface was
	// already given a resolution class for the same reason; the alarm now
	// matches it.
	heldNote := "every signing request for this key is refused with ErrSignerFenced until that exact " +
		"transaction is proven committed or proven permanently refused; reconciliation keeps re-submitting " +
		"the identical bytes to force that answer"
	if fence.cause == fenceCauseRestored {
		heldNote = "every signing request for this key is refused with ErrSignerFenced until that exact " +
			"transaction is proven committed or proven permanently refused. THIS fence was restored from " +
			"durable intent, so its signed bytes did NOT survive: nothing is being re-submitted, and it lifts " +
			"only on a proof read from the chain (or another allocation reaching this nonce) or on an explicit " +
			"operator abandon"
	}
	emitFenceEvent("fence_held",
		fenceKV("signer", signerPrefix(key)),
		fenceKV("tx_hash", txHash),
		fenceNonceField(nonce, hasNonce),
		fenceAge("held_for", held),
		fenceNum("attempts", uint64(attempts)), // #nosec G115 -- attempts is a non-negative counter
		fenceKV("last_cause", string(cause)),
		fenceKV("last_detail", fenceDetailOrUnknown(detail)),
		fenceKV("note", heldNote))
	return true
}

func fenceDetailOrUnknown(detail string) string {
	if detail == "" {
		return "no detail reported"
	}
	return detail
}

// CometTxHash returns the transaction hash CometBFT indexes these encoded bytes
// under: SHA-256 over the exact bytes broadcast. Reconciliation has to identify
// the ABANDONED transaction specifically, not "a transaction from this signer" —
// re-deriving the hash from the bytes is the only identifier available after the
// RPC response was lost.
func CometTxHash(encoded []byte) [sha256.Size]byte {
	return sha256.Sum256(encoded)
}

// NormalizeCometHash canonicalizes a remote-supplied transaction hash for
// comparison: trim surrounding space, lowercase, then strip one optional 0x
// prefix. The order matters — lowercasing FIRST means a nonstandard "0X"
// prefix normalizes identically to "0x". Exported because web's broadcast
// helper once re-implemented these steps in the opposite order, and the two
// proof surfaces diverged on exactly the formatting variance this function
// exists to absorb (fail-closed — a needless fence, never a false lift — but
// divergence on a single hazard across two files is how this branch re-opened
// a closed leak once already). Every surface that compares or inspects a
// remote hash must canonicalize through this one function.
func NormalizeCometHash(reported string) string {
	got := strings.ToLower(strings.TrimSpace(reported))
	return strings.TrimPrefix(got, "0x")
}

// cometReportedHashMatches reports whether a remote-supplied hash string names
// exactly the bytes this fence is holding.
//
// EVERY verdict either proof path produces must pass this check first. The RPC
// answer is remote data: a reverse proxy replaying an earlier response, or a
// node answering about a different transaction, produces an envelope that is
// syntactically perfect and factually about someone else — and a verdict
// adopted from it would lift the fence on bytes whose fate is still unknown,
// which is the silent Code 4 loss this file exists to prevent. Normalization
// (trim, optional 0x/0X prefix, case) exists because CometBFT renders hashes
// uppercase while hex.EncodeToString is lowercase; anything that survives
// normalization and still differs is a mismatch, never a formatting quirk.
func cometReportedHashMatches(reported string, want [sha256.Size]byte) bool {
	return NormalizeCometHash(reported) == hex.EncodeToString(want[:])
}

// CometReportedHashMatches is the exported face of cometReportedHashMatches,
// for the OTHER proof surface: web's broadcast helper binds every claimed
// verdict — success or rejection — through this same predicate, the way both
// sides already share HexHashPrefix, so the two files cannot drift into
// accepting or refusing different renderings of the same hash.
func CometReportedHashMatches(reported string, want [sha256.Size]byte) bool {
	return cometReportedHashMatches(reported, want)
}

// HexHashPrefix renders at most 8 characters of a remote-supplied transaction
// hash for a diagnostic. The value PURPORTS to be a hex hash, so an allowlist
// is the honest filter: after trimming, an optional 0x prefix, and lowercasing,
// every rune outside [0-9a-f] is dropped rather than echoed. That keeps
// ESC/CR/DEL — the forged-log alphabet — out of error text without a denylist
// that drifts the next time someone finds a character nobody thought of. Both
// proof surfaces (this package's reconciler and web's broadcast helper) render
// mismatched hashes through this one function so the two sides cannot diverge.
func HexHashPrefix(reported string) string {
	const max = 8
	got := strings.ToLower(strings.TrimSpace(reported))
	got = strings.TrimPrefix(got, "0x")
	kept := make([]byte, 0, max)
	for i := 0; i < len(got) && len(kept) < max; i++ {
		if c := got[i]; (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') {
			kept = append(kept, c)
		}
	}
	if len(kept) == 0 {
		return "(no hex characters)"
	}
	return string(kept)
}

// cometHashMismatchDetail renders a hash-binding failure for the fence's
// diagnostics. Only a hex-filtered short prefix of the remote value is echoed
// (HexHashPrefix): the reported string is remote-controlled text of unbounded
// length and arbitrary alphabet.
func cometHashMismatchDetail(op, reported string, want [sha256.Size]byte) string {
	return fmt.Sprintf("%s answered about a different transaction (want %s…, got %s…): not proof of this one's fate",
		op, hex.EncodeToString(want[:])[:8], HexHashPrefix(reported))
}

// cometTxLookup mirrors the /tx JSON-RPC envelope. Only the fields needed to
// tell "in a block, executed" from "in a block, rejected" from "no answer" are
// decoded.
type cometTxLookup struct {
	Result *struct {
		Hash     string      `json:"hash"`
		Height   CometHeight `json:"height"`
		TxResult *struct {
			Code *int   `json:"code"`
			Log  string `json:"log"`
		} `json:"tx_result"`
	} `json:"result"`
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    string `json:"data"`
	} `json:"error"`
}

// cometTxLookupSaysNotFound recognises the ONE error envelope that is an
// answer rather than a fault: CometBFT's -32603 "Internal error" whose data
// names the hash that was asked about and says it was not found. The hash must
// appear in the text so a generic internal error cannot be adopted as "not
// committed" — the direction that would be indistinguishable from a lookup
// that simply failed.
func cometTxLookupSaysNotFound(code int, message, data string, hash [sha256.Size]byte) bool {
	if code != cometTxNotFoundRPCErrorCode {
		return false
	}
	text := strings.ToLower(message + " " + data)
	if !strings.Contains(text, "not found") {
		return false
	}
	return strings.Contains(text, strings.ToLower(hex.EncodeToString(hash[:])))
}

// cometTxNotFoundRPCErrorCode is the JSON-RPC code CometBFT 0.38 uses for a
// transaction the node has not indexed (it reports "Internal error" with a
// "tx (HASH) not found" data string). It is NOT a unique not-found code — real
// internal errors share it — which is why the text must corroborate.
const cometTxNotFoundRPCErrorCode = -32603

// cometBroadcastCommit mirrors the /broadcast_tx_commit envelope: both the
// CheckTx and the FinalizeBlock code, because either one being non-zero is a
// definitive verdict on these exact bytes.
type cometBroadcastCommit struct {
	Result *struct {
		CheckTx *struct {
			Code      *int   `json:"code"`
			Log       string `json:"log"`
			Codespace string `json:"codespace"`
		} `json:"check_tx"`
		TxResult *struct {
			Code *int   `json:"code"`
			Log  string `json:"log"`
		} `json:"tx_result"`
		Hash   string      `json:"hash"`
		Height CometHeight `json:"height"`
	} `json:"result"`
	Error *struct {
		Message string `json:"message"`
		Data    string `json:"data"`
	} `json:"error"`
}

// CometTxResolver returns a TxResolveFunc that forces a definitive answer about
// a transaction from a CometBFT RPC endpoint. It lives here rather than in an
// adopter package so web, api/rest and the background workers all reconcile the
// same way; internal/tx still imports nothing from those packages.
//
// An empty endpoint returns nil, which means "no resolver" — the fence is then
// held (see errNoTxResolver) rather than pretending to have checked.
func CometTxResolver(cometRPC string) TxResolveFunc {
	endpoint := strings.TrimRight(strings.TrimSpace(cometRPC), "/")
	if endpoint == "" {
		return nil
	}
	return func(ctx context.Context, encoded []byte) (TxOutcome, error) {
		return cometResolve(ctx, endpoint, encoded)
	}
}

// LookupCometTxOutcome checks exact indexed bytes without re-submitting them.
// A missing transaction is unresolved, never proof of rejection.
func LookupCometTxOutcome(ctx context.Context, cometRPC string, encoded []byte) (TxOutcome, error) {
	return cometIndexedOutcome(ctx, strings.TrimRight(strings.TrimSpace(cometRPC), "/"), encoded, CometTxHash(encoded))
}

// cometResolve is the two-step reconciliation: ask whether the exact hash is in
// a block, and if that does not prove anything, RE-SUBMIT the exact bytes to
// make consensus answer.
//
// Re-submission is what keeps "never lift without proof" from meaning "fenced
// forever in practice". Passive polling cannot distinguish a transaction that
// was lost on the wire from one sitting unindexed in a mempool — CometBFT
// indexes a transaction only once it is in a block, so both answer "not found"
// — and a lost transaction never resolves itself, so a polling-only reconciler
// would hold the fence for the life of the process in the single most common
// failure, with no safe way out (a restart is not one — see the header). The
// same signed bytes carry the same nonce and the same hash, so re-broadcasting
// them is idempotent: it either gets them committed, gets them definitively
// refused, or is refused as a duplicate because they were already pending. All
// three are progress; none can create a second transaction.
func cometResolve(ctx context.Context, endpoint string, encoded []byte) (TxOutcome, error) {
	hash := CometTxHash(encoded)

	outcome, lookupErr := cometIndexedOutcome(ctx, endpoint, encoded, hash)
	if outcome.Verdict != TxVerdictUnresolved {
		return outcome, nil
	}

	resubmitted, needsIndexRecheck, submitErr := cometResubmitOutcome(ctx, endpoint, encoded)
	if needsIndexRecheck {
		// A permanent admission refusal proves the lift is safe, not whether
		// the original transaction succeeded. A vote may itself have committed
		// before its target closed, just as the code-4 nonce gate can reject a
		// self-committed transaction. Recheck the index after the resubmission
		// before choosing a fate label. If indexing is unavailable, preserve the
		// typed proof and its explicit uncertainty about the original outcome.
		if recheck, recheckErr := cometIndexedOutcome(ctx, endpoint, encoded, hash); recheckErr == nil &&
			recheck.Verdict != TxVerdictUnresolved {
			return recheck, nil
		}
		return resubmitted, nil
	}
	if resubmitted.Verdict != TxVerdictUnresolved {
		return resubmitted, nil
	}

	// Neither step proved anything. Report every reason, MOST SPECIFIC FIRST:
	// together they are what tells an operator whether the node is unreachable,
	// the transaction is still pending, or the re-submission was refused for a
	// reason that is real but not permanent.
	//
	// Order matters. The re-submit's own detail leads because it is the newest
	// and most specific evidence — notably the "refused in CheckTx, not a
	// permanent class" case, which is the one an operator most needs to see and
	// which an earlier version of this function buried under the lookup's
	// unremarkable "tx not found". The lookup's unresolved detail is included
	// too: since the hash/height binding it can carry the one fact that
	// explains a stuck fence — the index is answering about a DIFFERENT
	// transaction — and dropping it would leave that misbehavior invisible.
	parts := make([]string, 0, 4)
	if resubmitted.Detail != "" {
		parts = append(parts, resubmitted.Detail)
	}
	if submitErr != nil {
		parts = append(parts, submitErr.Error())
	}
	if outcome.Detail != "" {
		parts = append(parts, outcome.Detail)
	}
	if lookupErr != nil {
		parts = append(parts, lookupErr.Error())
	}
	return TxOutcome{Verdict: TxVerdictUnresolved, Detail: strings.Join(parts, "; ")}, nil
}

// cometIndexedOutcome reports a verdict only when the exact hash is in a block.
//
// It deliberately does NOT try to tell "never indexed" from "the RPC is broken"
// by reading the error envelope's text. Both are the same thing here — the
// absence of proof — both keep the fence up, and both are answered by the
// re-submission that follows. Removing that string match removes the last place
// this file could have decided something from a message.
func cometIndexedOutcome(ctx context.Context, endpoint string, encoded []byte, hash [sha256.Size]byte) (TxOutcome, error) {
	url := fmt.Sprintf("%s/tx?hash=0x%s", endpoint, strings.ToUpper(hex.EncodeToString(hash[:])))
	var lookup cometTxLookup
	resultOK, err := cometGetJSON(ctx, "comet tx lookup", url, encoded, &lookup)
	if err != nil {
		return TxOutcome{Verdict: TxVerdictUnresolved}, err
	}
	if lookup.Error != nil {
		// "NOT FOUND" IS AN ANSWER, NOT A FAULT. CometBFT reports a hash it has
		// not indexed as JSON-RPC -32603 "Internal error" with a data string
		// naming that hash. Read as a fault, this single line broke every route
		// that asks about a transaction that never committed — which is the
		// NORMAL state for a fence whose transaction is unproven:
		//   - the lift route answered 502 "the fate of this transaction could
		//     not be read" instead of 409 no-proof-yet;
		//   - the evidence reader classified the hash as "unavailable" instead
		//     of "not_found";
		//   - and the automatic resolution errored BEFORE reaching any of its
		//     gates, so every predicate it would have evaluated (and every
		//     refusal reason it would have recorded) was unreachable. A field
		//     node sat behind two unprovable fences for exactly this reason.
		//
		// The match is deliberately narrow: the error must be -32603 AND its
		// text must say "not found" AND it must name the hash that was asked
		// about. Anything else stays a fault, because a lookup that could not
		// be performed must never be recorded as "the transaction is not
		// committed" — that is the direction that lifts a fence on no evidence.
		if cometTxLookupSaysNotFound(lookup.Error.Code, lookup.Error.Message, lookup.Error.Data, hash) {
			return TxOutcome{
				Verdict: TxVerdictUnresolved,
				Detail: "comet tx lookup: the node has not indexed this transaction (it is in no committed " +
					"block); a lookup miss is not proof of anything, which is why the fence stays held",
			}, nil
		}
		return TxOutcome{Verdict: TxVerdictUnresolved},
			fmt.Errorf("comet tx lookup: %s", scrubFenceText(lookup.Error.Message+": "+lookup.Error.Data, encoded))
	}
	if !resultOK {
		// A 500 reaches the decoder only for its JSON-RPC error envelope; one
		// that carries a Result instead is a proxy artifact, and a proxy's 500
		// body is never proof of anything.
		return TxOutcome{
			Verdict: TxVerdictUnresolved,
			Detail:  "comet tx lookup answered 500 without a JSON-RPC error envelope: not a CometBFT answer",
		}, nil
	}
	if lookup.Result == nil || strings.TrimSpace(lookup.Result.Hash) == "" {
		return TxOutcome{Verdict: TxVerdictUnresolved}, nil
	}
	// BIND THE ANSWER TO THE QUESTION before reading any verdict out of it. The
	// /tx query named a hash, but nothing forces the response to be about that
	// hash: a proxy replaying an earlier lookup, or a confused node, returns an
	// envelope for a DIFFERENT transaction, and adopting its result field would
	// lift this fence on someone else's fate. Same rule for the height — an
	// "indexed" answer at height <= 0 claims block inclusion while naming no
	// block, which is not a claim that can be checked, so it proves nothing.
	// Both stay UNRESOLVED: the fence holds and reconciliation asks again.
	if !cometReportedHashMatches(lookup.Result.Hash, hash) {
		return TxOutcome{
			Verdict: TxVerdictUnresolved,
			Detail:  cometHashMismatchDetail("comet tx lookup", lookup.Result.Hash, hash),
		}, nil
	}
	if lookup.Result.Height <= 0 {
		return TxOutcome{
			Verdict: TxVerdictUnresolved,
			Detail:  "comet tx lookup reported an indexed transaction with no block height: not proof of inclusion",
		}, nil
	}
	if lookup.Result.TxResult == nil || lookup.Result.TxResult.Code == nil {
		return TxOutcome{
			Verdict: TxVerdictUnresolved,
			Detail:  "comet tx lookup omitted tx_result or verdict code: not proof of execution",
		}, nil
	}
	if *lookup.Result.TxResult.Code != 0 {
		// In a block and refused by FinalizeBlock. This is REAL PROOF for the
		// property the fence guards — no later allocation can be overtaken —
		// but the claim must be stated no wider than that, because an earlier
		// revision of this comment overstated it ("nothing is left in flight").
		// PRECISELY WHAT THIS PROVES, AND WHAT IT DOES NOT:
		//   - Proven: NO NONCE INVERSION IS POSSIBLE. Once the key reopens, the
		//     next allocation commits a higher nonce, and from that point the
		//     nonce gate refuses any resurfacing copy of these bytes as a
		//     replay. The Code 4 loss this file exists to prevent cannot occur.
		//   - NOT proven: "these bytes can never execute again." A FAILED
		//     transaction does not consume the signer's nonce, and CometBFT's
		//     mempool de-dup is a bounded cache, so a byte-identical copy in a
		//     peer mempool can be RE-INCLUDED in a later block. If the in-block
		//     failure was transient — e.g. the consensus path's code 4 "nonce
		//     lookup error" in internal/abci/app.go, a store fault, not the
		//     nonce gate — the re-included copy can even COMMIT, in the window
		//     before the signer's next commit closes the floor.
		// So fate_rejected means "no inversion possible", not "no effect ever":
		// an operator who redoes the action by hand can still end up applying
		// it twice, caught only by expected_revision guards. Written down
		// because a reassuring-but-overstated claim is the failure mode this
		// whole workstream exists to remove.
		return TxOutcome{
			Verdict: TxVerdictRejected,
			Detail: fmt.Sprintf("FinalizeBlock code %d at height %d: %s",
				*lookup.Result.TxResult.Code, lookup.Result.Height,
				scrubFenceText(lookup.Result.TxResult.Log, encoded)),
		}, nil
	}
	return TxOutcome{
		Verdict: TxVerdictCommitted,
		Detail:  fmt.Sprintf("committed at height %d", lookup.Result.Height),
	}, nil
}

// checkTxNonceGateCode is the one CheckTx code accepted WITHOUT a codespace.
//
// internal/abci/app.go returns it from exactly two places, both inside app-v9's
// replay gate: "nonce 0 not permitted" and "nonce too low: got N, expected > M".
// Nothing else in CheckTx uses code 4 — the neighbouring nonce LOOKUP failure is
// code 3 — so the code alone identifies the reason and no message needs parsing.
const checkTxNonceGateCode = 4

// checkTxRefusalIsPermanent reports whether a CheckTx code proves that the
// ORIGINAL in-flight copy of these bytes can never commit.
//
// THE DISTINCTION THIS ENCODES, WHICH AN EARLIER REVISION MISSED: a re-submit is
// judged against the state that exists NOW, at the node WE are talking to. The
// older copy — the one whose fate is unknown, the one the fence is actually
// about — will be judged against whatever state exists wherever and whenever IT
// arrives. So a refusal of the re-submit is evidence about the re-submit, and
// promoting it to a verdict about the original is only sound when the reason
// cannot un-happen.
//
// By numeric code alone, only the nonce gate has that property:
// a positive nonce is refused only when the signer's committed nonce is already
// at least that high, and that floor is MONOTONE NON-DECREASING; nonce zero is
// refused once the monotone app-v9 activation height has enabled the sentinel
// rule. Either refusal therefore remains true at every node, including for a
// copy that surfaces from a peer mempool an hour later. Other numeric codes
// alone do not identify a monotone predicate:
//   - code 3 is a nonce LOOKUP error — a local store fault, transient by nature;
//   - the app-v20 resource limit and code 112 are admission/backpressure, which
//     is by definition temporary;
//   - authorization, clearance, org membership and domain grants are ordinary
//     mutable state and can be granted back a block later;
//   - even decode and signature codes are fork-gated (see the app-v15 comment in
//     app.go: the same bytes are Code 1 on one binary and Code 10 on another),
//     so "statically invalid" is not static across an upgrade boundary.
//
// A separate typed codespace proves the narrow committed-memory vote case
// (see isCommittedMemoryVoteRefusal). Every other refusal stays unresolved. That costs
// liveness on a key whose transaction is genuinely dead and whose re-submit is
// refused for some other reason — a loud, inspectable stall. Getting it wrong in
// the other direction costs a transaction, silently, on some unrelated later
// request. Loud beats silent.
//
// THE ONE CAVEAT, STATED RATHER THAN GLOSSED: "monotone" is a property of the
// CHAIN, and a node whose state is rolled back behind us — a snapshot restore, a
// state-sync rewind — presents a lower committed nonce than it did a moment ago.
// A fence lifted on code 4 before such a rollback could, in principle, have its
// abandoned transaction become admissible again. This is not treated as a hole
// worth holding fences for: a rollback rewrites what "committed" means for every
// transaction on the node, not just this one, and the process that held the
// fence does not survive a restore anyway. It is written down because the
// argument above would otherwise read as stronger than it is.
func checkTxRefusalIsPermanent(code int) bool { return code == checkTxNonceGateCode }

// cometResubmitOutcome re-broadcasts the identical bytes and maps what comes
// back. needsIndexRecheck marks a permanent CheckTx refusal: the caller must
// re-check the index before settling on a fate label (see cometResolve).
//
// The mapping:
//   - CheckTx code 4 (the nonce gate) -> REJECTED, needsIndexRecheck=true. The gate is
//     monotone, so these bytes can never commit AGAIN — that is the whole lift
//     argument, and the outcome the re-submission engine is built to provoke.
//     What code 4 does NOT say is whether they never committed or already
//     committed THEMSELVES: app-v9's gate is `nonce <= committed`, so a
//     re-submit of the very transaction that closed the floor answers code 4
//     with no higher nonce ever existing. The detail states both readings;
//     claiming "a higher nonce has committed" here — as an earlier revision
//     did — misreports a committed change as refused whenever the tx index
//     cannot answer, and the operator redoes by hand work that already applied.
//   - Code 13 + CheckTxCommittedMemoryVoteCodespace, bound to a signed memory
//     vote -> REJECTED, needsIndexRecheck=true. The canonical post-v25 target
//     cannot return to proposed, so these bytes cannot succeed again. This
//     does not prove whether the original vote was recorded.
//   - CheckTx non-zero, any other untyped refusal -> UNRESOLVED. See
//     checkTxRefusalIsPermanent: it refuses THIS submission, not the copy that
//     may still be in flight.
//   - FinalizeBlock non-zero at a real height -> REJECTED. Reaching
//     FinalizeBlock means the exact hash was in a block; that is the same proof
//     an indexed lookup gives — and carries the same precise scope: it proves
//     no inversion is possible, NOT that the bytes can never execute again
//     (see cometIndexedOutcome for the re-inclusion caveat).
//   - both zero, at a real height -> COMMITTED.
//   - an error envelope -> UNRESOLVED. A duplicate refused by the mempool cache,
//     a commit wait that timed out, a full mempool and an unreachable node are
//     all indistinguishable here and none is a verdict, so the fence stays up
//     and the next attempt asks again. Not classifying them apart is
//     intentional: it would be a decision made from RPC text.
//
// NO VERDICT IS READ FROM AN ENVELOPE WHOSE HASH IS NOT OURS. Every result
// branch above assumes the response describes the bytes we just re-submitted,
// and nothing about a 200-with-result guarantees that: a replaying proxy or a
// node answering a different request produces a perfectly-shaped envelope about
// a different transaction. So the hash is bound to sha256(encoded) before any
// code is examined; a mismatch (or a missing hash) is UNRESOLVED — the fence
// holds and the next attempt asks again.
func cometResubmitOutcome(ctx context.Context, endpoint string, encoded []byte) (outcome TxOutcome, needsIndexRecheck bool, err error) {
	wantHash := CometTxHash(encoded)
	var res cometBroadcastCommit
	resultOK, err := cometBroadcastJSON(ctx, "comet re-submit", endpoint, "broadcast_tx_commit", encoded, &res)
	if err != nil {
		return TxOutcome{Verdict: TxVerdictUnresolved}, false, err
	}
	if res.Error != nil {
		return TxOutcome{Verdict: TxVerdictUnresolved}, false,
			fmt.Errorf("comet re-submit: %s", scrubFenceText(res.Error.Message+": "+res.Error.Data, encoded))
	}
	if !resultOK {
		// Same rule as the lookup: only a 200 may carry a Result. A 500 body
		// that parses into one is not CometBFT and must never become a verdict.
		return TxOutcome{
			Verdict: TxVerdictUnresolved,
			Detail:  "comet re-submit answered 500 without a JSON-RPC error envelope: not a CometBFT answer",
		}, false, nil
	}
	if res.Result == nil {
		return TxOutcome{Verdict: TxVerdictUnresolved}, false, errors.New("comet re-submit returned no result")
	}
	if res.Result.CheckTx == nil || res.Result.TxResult == nil ||
		res.Result.CheckTx.Code == nil || res.Result.TxResult.Code == nil {
		return TxOutcome{Verdict: TxVerdictUnresolved}, false,
			errors.New("comet re-submit omitted check_tx or tx_result, or omitted a verdict code: not proof of this transaction's fate")
	}
	if !cometReportedHashMatches(res.Result.Hash, wantHash) {
		return TxOutcome{
			Verdict: TxVerdictUnresolved,
			Detail:  cometHashMismatchDetail("comet re-submit", res.Result.Hash, wantHash),
		}, false, nil
	}
	if code := *res.Result.CheckTx.Code; code != 0 {
		log := scrubFenceText(res.Result.CheckTx.Log, encoded)
		if isCommittedMemoryVoteRefusal(code, res.Result.CheckTx.Codespace, encoded) {
			return TxOutcome{
				Verdict: TxVerdictRejected,
				Detail: "re-submit refused by the committed canonical memory vote gate: " +
					"these signed vote bytes cannot succeed again; whether the original vote " +
					"was recorded is unknown without the tx index",
			}, true, nil
		}
		if !checkTxRefusalIsPermanent(code) {
			// Deliberately an UNRESOLVED outcome rather than a verdict. The
			// fence stays up and reconciliation asks again, because this node
			// refusing to admit the re-submission right now says nothing about
			// where the original copy is.
			return TxOutcome{
				Verdict: TxVerdictUnresolved,
				Detail: fmt.Sprintf("re-submit refused in CheckTx (code %d, not a permanent class): %s",
					code, log),
			}, false, nil
		}
		// The hash binding above is what ties this refusal to OUR bytes; a
		// positive height is deliberately NOT demanded here, because unlike the
		// two in-block verdicts this one claims no inclusion — a CheckTx-refused
		// transaction never has a height, and requiring one would make the
		// single most common proof (supersession) unreachable. The lift stands
		// on the gate's monotonicity alone.
		return TxOutcome{
			Verdict: TxVerdictRejected,
			Detail: fmt.Sprintf("re-submit refused by the committed-nonce gate (CheckTx code %d): %s — "+
				"these bytes can never commit again; whether they were superseded by a higher committed "+
				"nonce or already committed themselves is indistinguishable without the tx index",
				code, log),
		}, true, nil
	}
	if *res.Result.TxResult.Code != 0 {
		if res.Result.Height <= 0 {
			// A FinalizeBlock code with no height is not the proof it looks
			// like: without a block there is nothing that says these bytes were
			// executed, so this is silence, not a verdict.
			return TxOutcome{
				Verdict: TxVerdictUnresolved,
				Detail: fmt.Sprintf("re-submit reported FinalizeBlock code %d with no committed height",
					*res.Result.TxResult.Code),
			}, false, nil
		}
		return TxOutcome{
			Verdict: TxVerdictRejected,
			Detail: fmt.Sprintf("re-submit rejected in FinalizeBlock (code %d) at height %d: %s",
				*res.Result.TxResult.Code, res.Result.Height, scrubFenceText(res.Result.TxResult.Log, encoded)),
		}, false, nil
	}
	if res.Result.Height <= 0 {
		// Both codes are zero but nothing says it landed: "committed" with no
		// block is not a claim that can be checked. (The hash was already bound
		// above.) Treat the silence as no answer: a synthesized "committed" here
		// would be the fail-open this rework removed.
		return TxOutcome{Verdict: TxVerdictUnresolved}, false, errors.New("comet re-submit reported no committed height")
	}
	return TxOutcome{
		Verdict: TxVerdictCommitted,
		Detail:  fmt.Sprintf("re-submit committed at height %d", res.Result.Height),
	}, false, nil
}

// cometGetJSON and cometBroadcastJSON issue one RPC and RETURN NO ERROR THAT
// COULD CONTAIN THE TRANSACTION.
//
// This is the origin of the leak that a cross-review caught, so the fix belongs
// here rather than only at the fence boundary. The historical re-submit GET URL
// carried the entire signed transaction and net/http embedded that URL in some
// errors. Re-submission now uses a bounded JSON-RPC POST, but both helpers still
// reduce transport failures to typed categories so no future request shape can
// put signed bytes into a fence record or support bundle.
//
// What survives is a TYPED CATEGORY (transport / timeout / canceled / decode /
// rpc) and, for an HTTP-level refusal, the status line. The caller already knows
// the transaction's hash and logs it as its own field, so nothing an operator
// needs is lost.
//
// resultOK reports whether the HTTP status entitles the caller to read a
// RESULT out of the decoded envelope. It is true only for 200: CometBFT
// delivers "tx not found" and mempool refusals as 500 with a JSON-RPC ERROR
// envelope, so a 500 must reach the decoder — but CometBFT never delivers a
// Result on a non-200, so a 500 whose body carries one is a proxy or an
// impostor and its Result must not become a verdict. Callers may always read
// the Error envelope; they must check resultOK before reading Result.
func cometGetJSON(ctx context.Context, op, url string, encoded []byte, out any) (resultOK bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil) // #nosec G107 -- internal CometBFT RPC
	if err != nil {
		// NOT %w: url.Error.Error() is `parse "<the whole URL>": ...`.
		return false, fmt.Errorf("%s: could not build request (%s)", op, classifyFenceCause(err))
	}
	// The tx lookup is a pure read and carries no submission, so it keeps the
	// shared pool: a transparently retried lookup costs nothing and this loop
	// polls for as long as a fence stands.
	return cometDoJSON(http.DefaultClient.Do, op, req, encoded, out)
}

func cometBroadcastJSON(ctx context.Context, op, endpoint, method string, encoded []byte, out any) (resultOK bool, err error) {
	req, err := cometBroadcastRequest(ctx, endpoint, method, encoded)
	if err != nil {
		return false, fmt.Errorf("%s: could not build request (%s)", op, classifyFenceCause(err))
	}
	// This is the reconciler's RE-SUBMIT: the same nil-body GET wire shape as a
	// first broadcast, so it carries the same at-least-once delivery exposure and
	// must go out on the non-reusing client. Fixing only comet_commit.go would
	// leave the fence's own recovery path duplicating the transaction it exists
	// to resolve.
	return cometDoJSON(DoCometSubmission, op, req, encoded, out)
}

func cometDoJSON(do func(*http.Request) (*http.Response, error), op string, req *http.Request, encoded []byte, out any) (resultOK bool, err error) {
	resp, err := do(req)
	if err != nil {
		// NOT %w, for the same reason. The category is what the retry loop and
		// the metrics label actually use.
		return false, fmt.Errorf("%s: %s", op, classifyFenceCause(err))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusInternalServerError {
		// Anything but 200/500 is a broken transport, not CometBFT.
		// resp.Status never echoes the request, but it IS the server's text and
		// a hostile proxy writes it freely — scrub it like any other remote
		// string.
		return false, fmt.Errorf("%s: comet rpc returned %s", op, scrubFenceText(resp.Status, encoded))
	}
	if err := validateCometJSONResponse(resp); err != nil {
		return false, fmt.Errorf("%s: %s", op, scrubFenceText(err.Error(), encoded))
	}
	// The body is read through a hard cap BEFORE the decoder sees it. This loop
	// retries for as long as a fence stands — that is the design — so an
	// unbounded read here is an AMPLIFIER: one node or proxy answering with a
	// huge body turns a single indeterminate transaction into unbounded
	// repeated allocation, once per attempt, forever. An oversized body is not
	// proof of anything; it fails the attempt, the fence holds, and the next
	// attempt asks again.
	limited := &io.LimitedReader{R: resp.Body, N: CometRPCMaxResponseBytes + 1}
	decoder := json.NewDecoder(limited)
	if err := decoder.Decode(out); err != nil {
		// A decode error quotes the RESPONSE, not the request, but the response
		// of a proxy or a confused node can echo anything — scrub it. A body
		// truncated by the cap fails here too (unexpected EOF), which is the
		// correct verdictless outcome.
		return false, fmt.Errorf("%s: %s", op, scrubFenceText(err.Error(), encoded))
	}
	// Decode through EOF, rather than accepting the first valid JSON value.
	// json.Decoder.Decode may return after buffering only a small prefix, so a
	// proof envelope followed by another JSON value, trailing garbage, or more
	// than the response cap otherwise bypasses both the single-document
	// protocol and the LimitedReader exhaustion check below.
	if trailingErr := decoder.Decode(&struct{}{}); trailingErr == nil {
		return false, fmt.Errorf("%s: response contained multiple JSON values", op)
	} else if !errors.Is(trailingErr, io.EOF) {
		return false, fmt.Errorf("%s: response contained trailing data: %s", op,
			scrubFenceText(trailingErr.Error(), encoded))
	}
	if limited.N <= 0 {
		// The document decoded but the body kept going past the cap: whatever
		// answered is not a CometBFT node speaking its own protocol.
		return false, fmt.Errorf("%s: response body exceeded %d bytes; refusing to treat it as a CometBFT envelope",
			op, CometRPCMaxResponseBytes)
	}
	return resp.StatusCode == http.StatusOK, nil
}

// CometRPCMaxResponseBytes caps how much of a CometBFT RPC response body any
// broadcast or reconciliation helper will read. Exported so web/'s broadcast
// helpers apply the SAME bound — asymmetric handling of one hazard across two
// packages is how this branch re-opened a closed leak once already.
//
// 8 MiB is comfortably above any legitimate envelope: CometBFT's default max
// transaction is ~1 MiB, a /tx result carries the transaction base64-encoded
// (~1.4x) plus events and proof, and a /broadcast_tx_commit envelope carries
// logs and events on top of the codes — all well under this. Anything larger is
// not CometBFT answering, and treating it as data would hand whoever controls
// the RPC path an allocation lever inside an unstoppable retry loop.
const CometRPCMaxResponseBytes = 8 << 20
