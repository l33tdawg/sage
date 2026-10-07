package web

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/tx"
)

type cometStatusTimeResult struct {
	Result struct {
		SyncInfo struct {
			LatestBlockTime time.Time `json:"latest_block_time"`
		} `json:"sync_info"`
	} `json:"result"`
}

// latestConsensusTimeWeb reads the chain's most recently committed time. The
// dashboard's app-v20 governance proof is checked against deterministic block
// time, not the host wall clock; on a recovering or CPU-starved personal node
// those clocks can differ by more than the strict five-minute proof window.
func latestConsensusTimeWeb(cometRPC string) (time.Time, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(cometRPC, "/")+"/status", nil) // #nosec G107 -- internal CometBFT RPC
	if err != nil {
		return time.Time{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return time.Time{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// resp.Status is the server's own status line: remote text, scrubbed
		// like every other remote string this file formats.
		return time.Time{}, fmt.Errorf("comet status returned %s", tx.ScrubBroadcastText(resp.Status, nil))
	}
	// Same body cap as every other CometBFT read in this file; a /status
	// envelope is a few KB, so anything near the cap is not CometBFT.
	limited := &io.LimitedReader{R: resp.Body, N: tx.CometRPCMaxResponseBytes + 1}
	var result cometStatusTimeResult
	decoder := json.NewDecoder(limited)
	if err := decoder.Decode(&result); err != nil {
		return time.Time{}, fmt.Errorf("decode comet status: %w", err)
	}
	if trailingErr := decoder.Decode(&struct{}{}); trailingErr == nil {
		return time.Time{}, fmt.Errorf("decode comet status: multiple JSON values")
	} else if !errors.Is(trailingErr, io.EOF) {
		return time.Time{}, fmt.Errorf("decode comet status: trailing data: %w", trailingErr)
	}
	if limited.N <= 0 {
		return time.Time{}, fmt.Errorf("comet status response body exceeded %d bytes", tx.CometRPCMaxResponseBytes)
	}
	if result.Result.SyncInfo.LatestBlockTime.IsZero() {
		return time.Time{}, fmt.Errorf("comet status omitted latest block time")
	}
	return result.Result.SyncInfo.LatestBlockTime, nil
}

const governanceProofClockSkew = 5 * time.Minute

// freshGovernanceProofTime chooses a timestamp that is fresh at the CheckTx
// boundary without moving behind the chain's last committed clock. An idle
// SAGE intentionally produces no heartbeat blocks, so latestBlockTime may be
// hours old even though the next block created by this transaction will use a
// current time. Reusing that old timestamp made a newly signed proof stale on
// arrival. A committed clock more than one proof window ahead of the host is a
// real clock-safety failure and remains fail-closed.
func freshGovernanceProofTime(now, latestBlockTime time.Time) (time.Time, error) {
	if latestBlockTime.After(now.Add(governanceProofClockSkew)) {
		return time.Time{}, fmt.Errorf(
			"committed consensus time is more than %s ahead of host time",
			governanceProofClockSkew,
		)
	}
	if latestBlockTime.After(now) {
		return latestBlockTime, nil
	}
	return now, nil
}

func (h *DashboardHandler) embedConsensusTimedGovernanceProof(ptx *tx.ParsedTx, operatorKey ed25519.PrivateKey, method, path string, body []byte) error {
	proofTime := time.Now()
	if h.ConsensusGovernanceClock {
		consensusTime, err := latestConsensusTimeWeb(h.CometBFTRPC)
		if err != nil {
			return fmt.Errorf("read committed consensus time: %w", err)
		}
		proofTime, err = freshGovernanceProofTime(proofTime, consensusTime)
		if err != nil {
			return fmt.Errorf("select governance proof time: %w", err)
		}
	}
	return embedDashboardGovernanceProofAt(ptx, operatorKey, method, path, body, proofTime)
}

const governanceProofAheadOfConsensus = "app-v20 governance proof timestamp is more than 5 minutes ahead of consensus time"

// retryGovernanceProofAtCommittedTime repairs the one honest race exposed by
// an idle single-validator chain. The rejected proposal mutates no consensus
// state, but its block gives us the clock the next proof must bind to.
func (h *DashboardHandler) retryGovernanceProofAtCommittedTime(
	ptx *tx.ParsedTx,
	operatorKey ed25519.PrivateKey,
	method string,
	path string,
	body []byte,
	broadcastErr error,
) (bool, error) {
	if broadcastErr == nil || !strings.Contains(broadcastErr.Error(), governanceProofAheadOfConsensus) {
		return false, nil
	}
	consensusTime, err := latestConsensusTimeWeb(h.CometBFTRPC)
	if err != nil {
		return true, fmt.Errorf("read committed consensus time after rejected proof: %w", err)
	}
	if err := embedDashboardGovernanceProofAt(
		ptx, operatorKey, method, path, body, consensusTime,
	); err != nil {
		return true, fmt.Errorf("re-sign governance proof at committed time: %w", err)
	}
	return true, nil
}

// This file is the commit-confirmed signing/broadcast plumbing for the v11.3
// RBAC reassign + access-control flow. The existing dashboard broadcast path
// (broadcastTxSync) is fire-and-forget: it cannot confirm a tx executed or
// enforce the strict propose -> executed -> reassign -> grant ordering the
// flow needs, so those handlers use the helpers here instead. Nothing here
// changes consensus; it only builds/signs/broadcasts existing tx types.

// cometCommitResult mirrors the /broadcast_tx_commit JSON envelope (a subset of
// the api/rest cometCommitResponse). It surfaces both the CheckTx and the
// FinalizeBlock (TxResult) codes so a consensus-side rejection becomes a real
// error rather than a silent success.
type cometCommitResult struct {
	Result struct {
		CheckTx *struct {
			Code *int   `json:"code"`
			Log  string `json:"log"`
		} `json:"check_tx"`
		TxResult *struct {
			Code *int   `json:"code"`
			Log  string `json:"log"`
		} `json:"tx_result"`
		Hash   string `json:"hash"`
		Height int64  `json:"height,string"`
	} `json:"result"`
	Error *struct {
		Message string `json:"message"`
		Data    string `json:"data"`
	} `json:"error"`
}

// rbacCommitTimeout bounds how long a commit-confirmed broadcast waits for
// /broadcast_tx_commit. Matches the api/rest client default (60s) so slow
// single-validator commits have headroom; overridable via SAGE_TX_COMMIT_TIMEOUT_MS.
func rbacCommitTimeout() time.Duration {
	if v := os.Getenv("SAGE_TX_COMMIT_TIMEOUT_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return 60 * time.Second
}

// broadcastTxCommitWebContext is the request-aware variant used by CEREBRUM
// mutations. A closed browser tab or an explicit client deadline must cancel
// the in-flight Comet request instead of leaving the server goroutine detached
// for the full commit timeout. Callers must still treat cancellation as an
// indeterminate result: consensus may already have accepted the transaction.
func broadcastTxCommitWebContext(parent context.Context, cometRPC string, signingKey ed25519.PrivateKey, txBytes []byte) (hash string, height int64, txLog string, err error) {
	txHex := hex.EncodeToString(txBytes)
	url := fmt.Sprintf("%s/broadcast_tx_commit?tx=0x%s", cometRPC, txHex)

	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, rbacCommitTimeout())
	defer cancel()

	req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, url, nil) // #nosec G107 -- internal CometBFT RPC
	if reqErr != nil {
		// Pre-send: the URL was never dialed, so nothing is in flight and this
		// must NOT fence. Still not %w — NewRequestWithContext fails with a
		// *url.Error whose message is `parse "<the whole URL>": ...`, and this
		// URL carries the entire signed transaction. A misconfigured CometBFTRPC
		// (a stray control character, a bad scheme) would otherwise leak the
		// full signed bytes into every 502 body and log line until the config
		// was fixed. broadcastTxSyncContext and internal/tx's cometGetJSON
		// already refuse %w here for the same reason.
		return "", 0, "", fmt.Errorf("create broadcast request: %s", commitTransportCause(reqErr))
	}
	// From this point a PANIC below is treated as exactly as ambiguous as a
	// broken connection: once Do(req) runs the node may have accepted the
	// transaction, and the recover cannot tell which side of that call it
	// interrupted. This recover is the CLEANER of two guards, not the only
	// one: after the registration a few lines down, even a panic that unwound
	// past it would be FENCED on the registered bytes by WithNonceLease's own
	// panic guard — it no longer releases the slot. What the recover adds is
	// the conversion itself (a typed indeterminate error the caller handles,
	// instead of a panic unwinding through the handler) and cover for the
	// one-line window between here and the registration, where nothing has
	// been sent yet and the conservative answer is still indeterminate. Only
	// the panic value's TYPE is reported: the value can be an error carrying
	// the request URL, i.e. the whole signed transaction. Pre-onWire panics
	// keep unwinding — nothing was sent and nothing was registered, so the
	// lease releasing the slot is the correct outcome, and fencing would have
	// reconciliation broadcast a transaction the caller was told never went
	// out.
	onWire := false
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		if !onWire {
			panic(r)
		}
		hash, height, txLog = "", 0, ""
		err = indeterminateCommit(fmt.Errorf("broadcast tx commit: panicked (%T) after the transaction was handed to the transport", r))
	}()
	onWire = true
	// Registered with the lease's panic guard at the same instant onWire flips,
	// so the recover above and the registration can never disagree about whether
	// bytes were at risk. The recover is the primary conversion; the
	// registration is what saves the case the recover cannot see — a panic in
	// code that runs AFTER this function returns but before submit does, which
	// would otherwise unwind through WithNonceLease as a releasing pre-send
	// panic while these bytes sit in a mempool. WithNonceLease clears the
	// registration on every return.
	tx.RegisterSubmittedTx(signingKey, txBytes, tx.CometTxResolver(cometRPC))
	// The three returns below are THE origin of commit ambiguity in web/, and
	// each is marked indeterminate right here rather than being re-derived from
	// its message downstream. By this point the encoded transaction has already
	// been handed to the kernel: a broken connection, an undecodable body or an
	// RPC-level error envelope all mean the node may have accepted it anyway.
	resp, doErr := tx.DoCometSubmission(req)
	if doErr != nil {
		// NOT %w on doErr. net/http returns a *url.Error whose message embeds
		// the request URL, and this request's URL is
		// /broadcast_tx_commit?tx=0x<the entire signed transaction>. Wrapping it
		// put the whole signed transaction into every error this function
		// returns — which handlers surface to operators, the node logs, and (before
		// the fence stopped keeping raw errors) repeated forever in the fence's
		// retry lines. internal/voter/broadcast.go declines to attach these
		// errors for exactly this reason. The category is what a caller acts on;
		// the transaction's identity is already available as its hash.
		return "", 0, "", indeterminateCommit(fmt.Errorf("broadcast tx commit: %s", commitTransportCause(doErr)))
	}
	defer resp.Body.Close()

	// A NON-200 IS NEVER PROOF OF ANYTHING. Nothing below examined the status
	// code, so a 500 or 502 whose body happened to parse as JSON fell straight
	// through to the success return. The transaction may well have reached the
	// mempool before whatever failed, so this is INDETERMINATE — it must fence,
	// not succeed and not be a definitive rejection.
	if resp.StatusCode != http.StatusOK {
		return "", 0, "", indeterminateCommit(fmt.Errorf(
			"broadcast tx commit: unexpected status %s", tx.ScrubBroadcastText(resp.Status, txBytes)))
	}

	// Bounded read, same cap as internal/tx's reconciliation RPC. A fenced key
	// re-reads this endpoint forever by design, so an unbounded decode here is
	// the same allocation amplifier codex flagged there — and the same hazard
	// must be handled the same way on both sides, or the asymmetry reintroduces
	// it. An oversized body proves nothing: indeterminate, so the fence holds.
	limited := &io.LimitedReader{R: resp.Body, N: tx.CometRPCMaxResponseBytes + 1}
	var result cometCommitResult
	decoder := json.NewDecoder(limited)
	if decErr := decoder.Decode(&result); decErr != nil {
		return "", 0, "", indeterminateCommit(fmt.Errorf("decode broadcast commit response: %s",
			tx.ScrubBroadcastText(decErr.Error(), txBytes)))
	}
	if trailingErr := decoder.Decode(&struct{}{}); trailingErr == nil {
		return "", 0, "", indeterminateCommit(errors.New(
			"decode broadcast commit response: multiple JSON values"))
	} else if !errors.Is(trailingErr, io.EOF) {
		return "", 0, "", indeterminateCommit(fmt.Errorf(
			"decode broadcast commit response: trailing data: %s",
			tx.ScrubBroadcastText(trailingErr.Error(), txBytes)))
	}
	if limited.N <= 0 {
		return "", 0, "", indeterminateCommit(fmt.Errorf(
			"broadcast commit response body exceeded %d bytes; refusing to treat it as a CometBFT envelope",
			tx.CometRPCMaxResponseBytes))
	}
	if result.Error != nil {
		// SCRUB THE ENVELOPE. Message and Data are remote-controlled text, and a
		// reverse proxy in front of CometBFT can answer 200 with an envelope that
		// echoes the request line — "GET /broadcast_tx_commit?tx=0x<signed hex>".
		// Formatting that verbatim would re-open the signed-byte leak at a second
		// origin: the fence record stays clean because it derives only a typed
		// cause, but this error flows into operator-facing 502 bodies and node
		// logs on every retry. internal/tx already scrubs the identical surface;
		// leaving this one raw is the asymmetry that lets a closed leak return.
		message := tx.ScrubBroadcastText(result.Error.Message, txBytes)
		if result.Error.Data != "" {
			data := tx.ScrubBroadcastText(result.Error.Data, txBytes)
			return "", 0, "", indeterminateCommit(fmt.Errorf("broadcast error: %s: %s", message, data))
		}
		return "", 0, "", indeterminateCommit(fmt.Errorf("broadcast error: %s", message))
	}
	// A valid CometBFT commit result always carries both nested verdicts. With
	// value structs, a missing or explicit-null check_tx/tx_result silently
	// zero-valued to code 0; a proxy could therefore pair the correct hash and
	// height with no execution verdict and manufacture a false success.
	if result.Result.CheckTx == nil || result.Result.TxResult == nil ||
		result.Result.CheckTx.Code == nil || result.Result.TxResult.Code == nil {
		return "", 0, "", indeterminateCommit(errors.New(
			"broadcast commit response omitted check_tx, tx_result, or verdict code: cannot prove this transaction's fate"))
	}
	// From here down the envelope claims a verdict — but NO VERDICT, SUCCESS OR
	// REJECTION, IS READ FROM AN ENVELOPE WHOSE HASH IS NOT OURS — the rule
	// internal/tx's proof paths already enforce, applied here through the SAME
	// exported predicate (tx.CometReportedHashMatches) rather than a local
	// re-implementation: a hand-rolled copy here once normalized in a different
	// order and the two proof surfaces diverged on a "0X" prefix. The
	// rejection branches used to run before the binding, so a replaying proxy
	// answering with an EARLIER transaction's CheckTx refusal — while OUR bytes
	// reached the real mempool — was adopted as a definitive verdict: the lease
	// released with no fence, the next caller's nonce overtook the in-flight
	// original, and it died to the silent Code 4 loss this file exists to
	// prevent. The dual misreport is as bad: a transaction that actually
	// committed gets reported "rejected", and the operator redoes the change by
	// hand and applies it twice.
	//
	// Requiring the binding cannot misclassify a genuine rejection, because a
	// real CometBFT node computes Hash locally from the submitted bytes on
	// EVERY /broadcast_tx_commit return, including CheckTx refusals. So a bound
	// rejection stays a plain, definitive error that releases the lease —
	// marking it indeterminate would fence the signing key on every ordinary
	// validation failure — while an unbound one is SILENCE about our
	// transaction: indeterminate, fence, and let reconciliation ask consensus
	// directly.
	sentHash := tx.CometTxHash(txBytes)
	wantHash := hex.EncodeToString(sentHash[:])
	gotHash := tx.NormalizeCometHash(result.Result.Hash)
	bound := tx.CometReportedHashMatches(result.Result.Hash, sentHash)
	if *result.Result.CheckTx.Code != 0 {
		if !bound {
			// Only hex-filtered prefixes are echoed: both values are remote text.
			return "", 0, "", indeterminateCommit(fmt.Errorf(
				"broadcast commit response reported a CheckTx rejection for a different transaction "+
					"(want %s…, got %s…): not proof of this one's fate",
				wantHash[:8], tx.HexHashPrefix(gotHash)))
		}
		// Definitively refused before any block, so nothing is in flight — the
		// registration protecting these bytes is retired NOW rather than at
		// submit's return. A panic between here and that return would otherwise
		// fence a transaction consensus just refused, and that fence is a trap
		// in both directions: while the refusal's cause persists it cannot lift
		// (re-submission keeps drawing the same non-permanent-class refusal,
		// and the index never finds a never-included transaction), and most
		// CheckTx causes are MUTABLE state — re-grant the missing access and
		// reconciliation's re-submit of these "rejected" bytes can be admitted
		// and COMMIT, executing late a transaction whose caller was told it
		// failed. See tx.ClearSubmittedTx.
		tx.ClearSubmittedTx(signingKey)
		return "", 0, "", fmt.Errorf("tx rejected in CheckTx (code %d): %s",
			*result.Result.CheckTx.Code, tx.ScrubBroadcastText(result.Result.CheckTx.Log, txBytes))
	}
	if *result.Result.TxResult.Code != 0 {
		// An in-block rejection additionally needs its block: a FinalizeBlock
		// verdict IS inclusion in a block, so code != 0 at height 0 is a shape
		// no real node produces. internal/tx's re-submit path treats a
		// heightless FinalizeBlock code as silence for the same reason.
		if !bound || result.Result.Height <= 0 {
			return "", 0, "", indeterminateCommit(fmt.Errorf(
				"broadcast commit response reported a FinalizeBlock rejection that is not bound to this "+
					"transaction (hash match %t, height %d): not proof of its fate",
				bound, result.Result.Height))
		}
		// In a block with a non-zero code: fate fully decided, nothing in
		// flight. Retired for the same panic-window reason as the CheckTx
		// branch above.
		tx.ClearSubmittedTx(signingKey)
		return "", 0, "", fmt.Errorf("tx rejected in FinalizeBlock (code %d): %s",
			*result.Result.TxResult.Code, tx.ScrubBroadcastText(result.Result.TxResult.Log, txBytes))
	}

	// ZERO CODES ARE NOT PROOF OF COMMIT. Every field above zero-values, so a
	// syntactically valid but EMPTY body — "{}", or a truncated/again-proxied
	// response — reached this point with CheckTx.Code == 0 and TxResult.Code == 0
	// and was returned as a SUCCESSFUL COMMIT carrying an empty hash and height 0.
	// That is worse than the race this whole change exists to fix: the caller is
	// told the transaction committed, the fence never engages because there is no
	// error, and the write is simply lost with a success reported to the operator.
	//
	// So success requires the same POSITIVE evidence the rejections above do:
	//   - a hash that is present AND equals sha256 of txBytes. Absent binding, a
	//     stale or mismatched RPC answer — a proxy replaying an earlier response,
	//     a node answering about a different transaction — would be accepted as
	//     proof for THIS one.
	//   - a positive height. Height 0 means no block, so nothing committed.
	// Anything short of that is INDETERMINATE and must fence: we genuinely do not
	// know whether the bytes are in flight.
	switch {
	case gotHash == "":
		return "", 0, "", indeterminateCommit(errors.New(
			"broadcast commit response carried no transaction hash: cannot prove this transaction committed"))
	case !bound:
		// Deliberately echoes only a hex-filtered short prefix of the remote
		// hash (tx.HexHashPrefix): the returned value is remote-controlled
		// text, and this error flows raw into handler logs — a raw ESC or CR
		// here would reintroduce the forged-log vector everywhere else in this
		// file is scrubbed against.
		return "", 0, "", indeterminateCommit(fmt.Errorf(
			"broadcast commit response hash does not match the transaction sent (want %s…, got %s…)",
			wantHash[:8], tx.HexHashPrefix(gotHash)))
	case result.Result.Height <= 0:
		return "", 0, "", indeterminateCommit(errors.New(
			"broadcast commit response reported no block height: cannot prove this transaction committed"))
	}
	// The returned hash is OUR canonical rendering (uppercase, CometBFT's
	// convention), not result.Result.Hash. They are proven equal modulo
	// trim/prefix/case by the switch above, and the difference is who authored
	// the string: handlers put this value into activity events and responses,
	// and the one remote-controlled degree of freedom left — its formatting —
	// is not worth carrying there. The success-path log is scrubbed for the
	// same reason: a committed transaction's log is exactly as
	// remote-controlled as a failed one's.
	return strings.ToUpper(wantHash), result.Result.Height, tx.ScrubBroadcastText(result.Result.TxResult.Log, txBytes), nil
}

// commitTransportCause renders a broadcast transport failure as a category, so
// the error this package returns can never contain the request URL and
// therefore can never contain the signed transaction.
//
// Classified by TYPE, never by matching text: the point of moving the
// classification to the origin was to stop deriving meaning from messages, and a
// category derived from a message would put that back one level down. The four
// buckets are what a caller actually distinguishes — the client gave up, the
// caller went away, or the wire broke.
func commitTransportCause(err error) string {
	switch {
	case err == nil:
		return "unknown"
	case errors.Is(err, context.DeadlineExceeded):
		return "timed out waiting for commit"
	case errors.Is(err, context.Canceled):
		return "the request was canceled"
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return "timed out waiting for commit"
		}
		return "the connection to the node failed"
	}
	return "the request to the node failed"
}

// backgroundSigningBudget bounds a submission that has no request context of its
// own (the continuity/adoption workers, federation cleanup, import). The lease
// can now BLOCK such a caller while the key is fenced, and context.Background()
// would let a worker goroutine park there indefinitely. Two commit timeouts
// leaves room for a full commit behind one queued ahead of it, and still fails
// the caller loudly instead of wedging it.
func backgroundSigningBudget() time.Duration {
	return 2 * rbacCommitTimeout()
}

// signAndBroadcastCommit stamps the nonce, adds the legacy same-key proof for
// non-governance RBAC transactions, signs the envelope, encodes it, and waits
// for commit. Governance callers either supply a modern request-bound operator
// proof before calling this helper or intentionally use the proofless direct
// compatibility lane.
func (h *DashboardHandler) signAndBroadcastCommit(ptx *tx.ParsedTx, key ed25519.PrivateKey) (hash string, height int64, txLog string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), backgroundSigningBudget())
	defer cancel()
	return h.signAndBroadcastCommitContext(ctx, ptx, key)
}

// signAndBroadcastCommitContext runs the whole stamp -> sign -> encode ->
// broadcast sequence inside a per-signing-key nonce lease.
//
// The lease is load-bearing, not defensive. Every dashboard fan-out that
// mutates several records at once (clearing a Done/Dropped board column,
// bulk-forgetting selected memories) issues N concurrent HTTP requests that all
// land here with the SAME key. Allocating the nonce and then racing to CometBFT
// meant those txs could arrive in descending nonce order, and app-v9's replay
// gate rejects the late-arriving lower nonce with Code 4 "nonce too low" — so a
// random subset of the batch failed while the rest succeeded. Serializing
// allocation with submission is what makes the emitted order match the
// allocated order. Every signAndBroadcastCommit* caller in web/ inherits this.
//
// The lease's ordering guarantee survives a lost RPC response because this
// function tells the lease WHICH KIND of failure it had. Waiting for commit is
// not enough on its own: broadcastTxCommitWebContext can return because the
// connection broke rather than because consensus answered, and releasing the
// lease on that would let the next caller's higher nonce overtake a transaction
// still in flight — Code 4 "nonce too low", the exact loss the lease exists to
// stop. So an indeterminate broadcast is handed back wrapped in
// tx.Indeterminate, which fences the signing key until that exact transaction's
// fate is PROVEN — committed, or definitively refused by consensus. Nothing else
// reopens the key: background reconciliation re-submits the identical bytes to
// force an answer, and a fence that cannot be proven is HELD (loudly, and
// visible through tx.FencedSigners) rather than conceded on a timer, because a
// wrongly reopened key loses a later transaction silently to Code 4. Sign/encode
// failures and real CheckTx or FinalizeBlock rejections are definitive, so they
// stay plain errors and release the lease normally.
//
// A handler that gets tx.ErrSignerFenced back should surface it as
// "not sent yet, ask again" — HTTP 503 with a Retry-After — and NEVER as a
// rejection: nothing was signed, so there is no verdict to report and nothing
// for the operator to undo.
//
// THE ONLY RESOLUTION IS RECONCILIATION FINISHING. Do not tell an operator to
// restart. An earlier version of this comment did, on the theory that a restart
// re-seeds the allocator from the highest committed on-chain nonce
// (tx.SetNonceFloorFunc, wired in cmd/sage-gui/node.go). That reasoning is
// wrong: the abandoned nonce is unresolved precisely because it sits ABOVE the
// committed floor and may still be in flight, so a restart discards the fence,
// seeds below it, and issues a nonce that overtakes it — the Code 4 loss the
// fence exists to prevent, now untraceable. cmd/sage-gui accordingly VETOES a
// coordinated restart while any key is fenced.
func (h *DashboardHandler) signAndBroadcastCommitContext(ctx context.Context, ptx *tx.ParsedTx, key ed25519.PrivateKey) (string, int64, string, error) {
	return h.signAndBroadcastCommitPrepared(ctx, ptx, key, nil)
}

// signAndBroadcastCommitPrepared is signAndBroadcastCommitContext for callers
// that must run their OWN pre-signature steps — a request-bound app-v20
// governance proof, an app-v23 elevation — which the generic path cannot build.
//
// prepare runs INSIDE THE LEASE, after the nonce is stamped and before the
// signature. That placement is the whole reason this variant exists: the
// governance handlers used to allocate a nonce, then embed a proof (which itself
// makes an RPC for consensus time), then sign and broadcast, all outside any
// lease — so two governance actions on one key could allocate N and N+1 and
// reach CometBFT in the other order, and the late one was rejected Code 4. A
// nonce is only meaningful while the lease that issued it is held.
//
// prepare must be a pure function of ptx and the request; anything it does is
// covered by the signature that follows, so it must not be re-run on a
// reconciliation (it is not — reconciliation re-sends the already-encoded bytes).
func (h *DashboardHandler) signAndBroadcastCommitPrepared(
	ctx context.Context,
	ptx *tx.ParsedTx,
	key ed25519.PrivateKey,
	prepare func(*tx.ParsedTx) error,
) (string, int64, string, error) {
	return h.signAndBroadcastCommitDurable(ctx, ptx, key, prepare, nil)
}

// beforeSend persists an exact-byte recovery intent inside the nonce lease.
// A persistence failure is definitive and prevents any network submission.
func (h *DashboardHandler) signAndBroadcastCommitDurable(
	ctx context.Context, ptx *tx.ParsedTx, key ed25519.PrivateKey,
	prepare func(*tx.ParsedTx) error, beforeSend func([]byte) error,
) (string, int64, string, error) {
	var (
		hash   string
		height int64
		txLog  string
		txErr  error
	)
	// Refuse IMMEDIATELY when the key is already fenced: the lease would park
	// here until this request's deadline instead of answering, and the caller is
	// an operator or an agent waiting on a response (see tx.FenceForSigner).
	// Nothing is signed either way; this only decides how long they wait to be
	// told, and the handlers already map ErrSignerFenced to 503 + Retry-After.
	if _, fenced := tx.FenceForSigner(key); fenced {
		return "", 0, "", tx.ErrSignerFenced
	}
	leaseErr := tx.WithNonceLease(ctx, key, func(nonce uint64) error {
		ptx.Nonce = nonce
		if ptx.Timestamp.IsZero() {
			ptx.Timestamp = time.Now()
		}
		if prepare != nil {
			if prepErr := prepare(ptx); prepErr != nil {
				// Definitive and pre-send: nothing reached the wire, so this
				// releases the lease normally and must never fence.
				txErr = prepErr
				return txErr
			}
		} else {
			// Direct governance is authorized by the outer operator/validator signature
			// and deliberately carries no HTTP-agent proof. App-v20+ treats any proof
			// material on governance as a modern request-bound proof (8-byte request
			// nonce + canonical request body). The generic legacy dashboard proof lacks
			// those fields and is therefore correctly rejected. Keep the legacy same-key
			// proof for non-governance RBAC transactions, whose consensus path still
			// accepts it.
			switch ptx.Type {
			case tx.TxTypeGovPropose, tx.TxTypeGovVote, tx.TxTypeGovCancel:
			default:
				embedDashboardAgentProof(ptx, key)
			}
		}
		if signErr := tx.SignTx(ptx, key); signErr != nil {
			txErr = fmt.Errorf("sign tx: %w", signErr)
			return txErr
		}
		encoded, encErr := tx.EncodeTx(ptx)
		if encErr != nil {
			txErr = fmt.Errorf("encode tx: %w", encErr)
			return txErr
		}
		if beforeSend != nil {
			if persistErr := beforeSend(encoded); persistErr != nil {
				txErr = fmt.Errorf("persist submission intent: %w", persistErr)
				return txErr
			}
		}
		hash, height, txLog, txErr = broadcastTxCommitWebContext(ctx, h.CometBFTRPC, key, encoded)
		if isIndeterminateCommitError(txErr) {
			// Fence the key on the EXACT bytes that went out. The lease needs
			// the encoded transaction, not the error: reconciliation both
			// identifies the transaction by its CometBFT hash and RE-SUBMITS
			// those identical bytes to force consensus to answer, and neither
			// is possible from the error text. Re-submitting identical bytes
			// is idempotent (same nonce, same hash), so it can never produce a
			// second transaction. txErr itself stays the plain error so every
			// caller of this function keeps seeing the message it always saw.
			return tx.Indeterminate(txErr, encoded, tx.CometTxResolver(h.CometBFTRPC))
		}
		return txErr
	})
	// txErr is still nil only when the closure never ran: the request was
	// cancelled while queued for the key, or the key was fenced and this
	// request's context expired before reconciliation lifted it. Either way
	// nothing was signed or sent, so this is a DEFINITIVE "no change" —
	// deliberately not marked indeterminate, and worded so it cannot be mistaken
	// for a consensus rejection.
	if txErr == nil && leaseErr != nil {
		if errors.Is(leaseErr, tx.ErrSignerFenced) {
			return "", 0, "", fmt.Errorf(
				"this signing key is held pending confirmation of an earlier submission whose outcome was lost; "+
					"nothing was signed or sent for this request: %w", leaseErr)
		}
		return "", 0, "", fmt.Errorf("await signing slot for this key: %w", leaseErr)
	}
	return hash, height, txLog, txErr
}

// signAndBroadcastSyncContext is the FIRE-AND-FORGET sibling of
// signAndBroadcastCommitContext, for the background producers that put an audit
// record on-chain and do not wait for a block.
//
// WHY THESE NEEDED A LEASE TOO. They used to call tx.MonotonicNonce and then
// broadcast, with nothing serializing the two — and they share h.SigningKey with
// every commit-confirmed dashboard mutation. Two of them (or one of them and one
// dashboard action) could therefore allocate N and N+1 and hand them to CometBFT
// in the other order, and app-v9's replay gate rejects the late lower nonce
// Code 4. Being fire-and-forget made that WORSE rather than harmless: nobody was
// waiting for the answer, so the loss was invisible — the agent simply never
// showed up on-chain and the dashboard rendered it as "un-synced" forever.
//
// Holding the lease across allocate -> sign -> broadcast makes the emitted order
// match the allocated order, which is what CometBFT's per-sender ascending-nonce
// reaping needs to admit a burst intact.
//
// The broadcast is /broadcast_tx_sync, so a clean return means CometBFT supplied
// a complete, exact-hash-bound CheckTx verdict — NOT that the transaction later
// committed. A non-zero CheckTx code is definitive for these bytes and retires
// their registration; every transport, status, RPC, decode, shape, trailing-data,
// or hash-binding failure stays live and fences through WithNonceLease.
func (h *DashboardHandler) signAndBroadcastSyncContext(ctx context.Context, ptx *tx.ParsedTx, key ed25519.PrivateKey) error {
	var txErr error
	// Same fast refusal as the durable commit path: a held fence must reach the
	// handler's 503 mapping now, not after the request has already timed out.
	if _, fenced := tx.FenceForSigner(key); fenced {
		return tx.ErrSignerFenced
	}
	leaseErr := tx.WithNonceLease(ctx, key, func(nonce uint64) error {
		ptx.Nonce = nonce
		if ptx.Timestamp.IsZero() {
			ptx.Timestamp = time.Now()
		}
		embedDashboardAgentProof(ptx, key)
		if signErr := tx.SignTx(ptx, key); signErr != nil {
			txErr = fmt.Errorf("sign tx: %w", signErr)
			return txErr
		}
		encoded, encErr := tx.EncodeTx(ptx)
		if encErr != nil {
			txErr = fmt.Errorf("encode tx: %w", encErr)
			return txErr
		}
		txErr = broadcastTxSyncContext(ctx, h.CometBFTRPC, key, encoded)
		if isIndeterminateCommitError(txErr) {
			return tx.Indeterminate(txErr, encoded, tx.CometTxResolver(h.CometBFTRPC))
		}
		return txErr
	})
	if txErr == nil && leaseErr != nil {
		if errors.Is(leaseErr, tx.ErrSignerFenced) {
			return fmt.Errorf(
				"this signing key is held pending confirmation of an earlier submission whose outcome was lost; "+
					"nothing was signed or sent for this request: %w", leaseErr)
		}
		return fmt.Errorf("await signing slot for this key: %w", leaseErr)
	}
	return txErr
}

// broadcastTxSyncContext uses the shared strict sync protocol. Keeping the
// decoder, single-document check, exact-hash binding, and registration lifecycle
// in internal/tx prevents web from accepting a merely-HTTP-200 response that
// proves nothing about these bytes.
func broadcastTxSyncContext(parent context.Context, cometRPC string, signingKey ed25519.PrivateKey, txBytes []byte) error {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, syncBroadcastTimeout)
	defer cancel()
	result, err := tx.BroadcastCometSync(ctx, cometRPC, signingKey, txBytes)
	if err != nil {
		return err
	}
	if result.CheckTxCode != 0 {
		return fmt.Errorf("tx rejected by CheckTx (code %d): %s", result.CheckTxCode, result.CheckTxLog)
	}
	return nil
}

// syncBroadcastTimeout bounds a fire-and-forget broadcast. It matches the value
// the pre-lease broadcastTxSync used, because the request itself has not changed
// — only what happens around it.
const syncBroadcastTimeout = 5 * time.Second

// commitIndeterminateError marks a commit-confirmed broadcast that could have
// reached consensus but did not yield a trustworthy answer to this process.
//
// It replaces the message-prefix matching this file used to do. Prefix matching
// was fragile in the direction that costs transactions: any wrapping that
// prepended context ("update agent policy: broadcast tx commit: ...") silently
// reclassified an indeterminate result as a definitive rejection, and the
// classification could only run AFTER the error had travelled out of the code
// that knew what actually happened. Marking at the origin removes both problems.
//
// Error() returns the wrapped message verbatim: several handlers surface this
// text to operators and the nonce-lease contract returns submit's error
// undecorated, so the marker must be invisible in the message.
type commitIndeterminateError struct{ err error }

func (e *commitIndeterminateError) Error() string { return e.err.Error() }

func (e *commitIndeterminateError) Unwrap() error { return e.err }

func indeterminateCommit(err error) error {
	if err == nil {
		return nil
	}
	return &commitIndeterminateError{err: err}
}

// isIndeterminateCommitError reports whether a commit-confirmed request could
// have reached consensus but did not yield a trustworthy response to this
// process. CheckTx/FinalizeBlock failures are definitive rejections and must
// remain errors; transport and RPC response faults are not proof of no change.
//
// A fenced request (tx.ErrSignerFenced) is deliberately NOT indeterminate:
// nothing was signed or sent, so there is no change for a handler to go looking
// for.
func isIndeterminateCommitError(err error) bool {
	var indeterminate *commitIndeterminateError
	return errors.As(err, &indeterminate)
}

// fencedSignerRetryAfterSeconds is the Retry-After a fenced request advertises.
// Reconciliation re-submits the identical bytes on a backoff that tops out at a
// minute, so this is roughly "after the next couple of attempts" — long enough
// not to hammer a node that is already struggling, short enough that a fence
// clearing normally is not followed by a needless wait.
const fencedSignerRetryAfterSeconds = 15

// signerHeldNotSent reports whether err is one of the two typed "NOTHING WAS
// SIGNED OR SENT" refusals — a fenced signing key, or signing quiesced for a
// coordinated restart. It exists for the surfaces that cannot answer through
// writeSignerNotSentIfHeld (JSON result objects, multi-step batch responses):
// they must still classify the refusal as "not sent, retry" rather than fold it
// into their *_rejected verdict, because reporting a deferral as a rejection
// sends an operator hunting for a change to undo that never existed.
func signerHeldNotSent(err error) bool {
	return errors.Is(err, tx.ErrSignerFenced) || errors.Is(err, tx.ErrSigningQuiesced)
}

// writeSignerNotSentIfHeld answers a request whose transaction was NEVER SIGNED
// OR SENT — the signing key is fenced, or signing is quiesced for a restart —
// and reports whether it did.
//
// Call it FIRST in any error branch that would otherwise report a consensus
// rejection. That ordering is the point: `tx.ErrSignerFenced` is deliberately
// not an "indeterminate commit" (nothing is in flight for the handler to go
// looking for), so without this guard it falls through to whatever the handler
// treats as its definitive-failure case — and telling an operator their change
// was REFUSED, when it was merely deferred, is the misreport the typed error
// exists to prevent. They would go hunting for a change to undo that does not
// exist, or redo the work by hand.
//
// 503 + Retry-After is the honest shape: not sent, ask again. Never a rejection
// status, never a suggestion to restart.
func writeSignerNotSentIfHeld(w http.ResponseWriter, err error) bool {
	switch {
	case errors.Is(err, tx.ErrSignerFenced):
		w.Header().Set("Retry-After", strconv.Itoa(fencedSignerRetryAfterSeconds))
		writeError(w, http.StatusServiceUnavailable,
			"Not sent: this signing key is waiting for confirmation of an earlier submission whose "+
				"outcome was lost. Nothing was signed or broadcast, and nothing needs undoing. Try again shortly.")
		return true
	case errors.Is(err, tx.ErrSigningQuiesced):
		w.Header().Set("Retry-After", strconv.Itoa(fencedSignerRetryAfterSeconds))
		writeError(w, http.StatusServiceUnavailable,
			"Not sent: SAGE is restarting, so no new transaction was signed. Try again once it is back.")
		return true
	default:
		return false
	}
}

// writeGovernanceBroadcastFailure maps a signing/broadcast failure onto the
// right status, keeping a FENCED request distinguishable from a REJECTED one.
//
// This distinction is the reason ErrSignerFenced is a typed error at all. A
// consensus rejection is a verdict about a transaction that reached the chain:
// the operator's change was refused and they should not retry it unchanged. A
// fence means NOTHING WAS SIGNED OR SENT — there is no verdict, nothing to undo,
// and retrying is exactly the right thing to do once reconciliation resolves the
// earlier submission. Reporting the second as the first tells an operator their
// change was refused when it was merely deferred, which is how a transient
// outage turns into someone re-doing work by hand.
//
// 503 + Retry-After is the honest HTTP shape for "not sent yet, ask again". It
// is deliberately NOT 409/422 (a verdict) and NOT 502 (the node answered fine).
// The body says nothing about restarting, because restarting discards the fence
// and loses the transaction — see signAndBroadcastCommitPrepared.
func writeGovernanceBroadcastFailure(w http.ResponseWriter, prefix string, err error) {
	if errors.Is(err, tx.ErrSignerFenced) {
		w.Header().Set("Retry-After", strconv.Itoa(fencedSignerRetryAfterSeconds))
		writeError(w, http.StatusServiceUnavailable,
			"not sent: this signing key is waiting for confirmation of an earlier submission whose outcome "+
				"was lost. Nothing was signed or broadcast, and nothing needs undoing. Try again shortly.")
		return
	}
	if errors.Is(err, tx.ErrSigningQuiesced) {
		w.Header().Set("Retry-After", strconv.Itoa(fencedSignerRetryAfterSeconds))
		writeError(w, http.StatusServiceUnavailable,
			"not sent: SAGE is restarting, so no new transaction was signed. Try again once it is back.")
		return
	}
	writeError(w, http.StatusBadGateway, prefix+err.Error())
}

// agentIDForKey returns the on-chain agent id (hex(pubkey)) for an Ed25519 key,
// matching auth.PublicKeyToAgentID. Empty for a nil/invalid key.
func agentIDForKey(key ed25519.PrivateKey) string {
	if len(key) != ed25519.PrivateKeySize {
		return ""
	}
	pub, ok := key.Public().(ed25519.PublicKey)
	if !ok {
		return ""
	}
	return hex.EncodeToString(pub)
}

// rbacPurgeRe extracts the purged-grant count from processDomainReassign's
// success log ("... purged N grants ...").
var rbacPurgeRe = regexp.MustCompile(`purged\s+(\d+)\s+grants`)

// parsePurgedGrantsWeb pulls the purged-grant count out of a DomainReassign
// FinalizeBlock log, or 0 if absent.
func parsePurgedGrantsWeb(log string) int {
	m := rbacPurgeRe.FindStringSubmatch(log)
	if len(m) < 2 {
		return 0
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return 0
	}
	return n
}

// signerFenceHealth renders the signer-fence state for the operator status
// surface (/v1/dashboard/health).
//
// WHY THIS IS PART OF THE FEATURE, NOT A NICETY. A fence refuses every signing
// request for one key and, by design, is never lifted by a timer — only by proof
// of the earlier transaction's fate. Holding a key indefinitely is only a
// defensible trade while the hold is VISIBLE: otherwise "my agent stopped being
// able to write" is a mystery hang, which is precisely the outcome the loud-
// failure argument promised to avoid.
//
// TWO TIERS, because /health is public (the CLI status command reads it
// unauthenticated):
//   - always: the COUNT and the age of the oldest hold. Both are pure liveness
//     signals — they say this node cannot sign for somebody — and neither
//     identifies a key or a transaction.
//   - operator only: which signer, which transaction, which nonce, how many
//     reconciliation attempts, and the typed cause. All of it is public on-chain
//     data or a category string, but it is per-agent detail and it belongs
//     behind the same gate as the rest of the operator view.
//
// EVERYTHING HERE IS ALREADY SANITIZED AT THE SOURCE. Cause and LastCause are
// typed categories, LastDetail has been through the fence's scrubber, and the
// signer is a PUBLIC key — never a key file path, never a raw error, never the
// encoded transaction. See internal/tx/nonce_fence.go's fenceCause.
func signerFenceHealth(operator bool) map[string]any {
	held := tx.FencedSigners()
	out := map[string]any{
		"active":             len(held),
		"oldest_age_seconds": 0,
	}
	// The last resolution is reported BEFORE the empty check on purpose: its
	// whole job is to keep a hold that ENDED visible once there is no held fence
	// left to describe. Operator-only, like the per-fence rows: it names a signer
	// and a transaction, and the public tier gets the count and the explanation
	// only.
	if operator {
		if last, ok := tx.LastFenceResolution(); ok {
			out["last_resolution"] = fenceResolutionRow(last)
		}
	}
	if len(held) == 0 {
		return out
	}
	// FencedSigners is oldest-first, so held[0] is the alarm.
	out["oldest_age_seconds"] = int(held[0].HeldFor.Round(time.Second).Seconds())
	// Said in the status payload as well as the log, because an operator looking
	// at a stuck node needs to know the hold is deliberate — and must not be
	// told to restart, which discards the fence and loses the transaction.
	//
	// The explanation NAMES THE ROUTE OUT of each held fence instead of
	// describing one. An earlier revision said reconciliation was re-submitting
	// the identical bytes for every fence, which is true only for a live one: a
	// fence restored from durable intent has no bytes to re-submit and lifts on
	// a proof or an operator decision. Callers read the blanket sentence as a
	// promise that the node would clear itself, and reported the opposite
	// symptom back as a broken self-heal.
	out["explanation"] = "one or more signing keys are waiting for proof of an earlier submission's fate; " +
		"nothing was signed or sent for the requests they refused. " + signerFenceRoutes(held)
	if !operator {
		return out
	}

	signers := make([]map[string]any, 0, len(held))
	for _, fence := range held {
		row := map[string]any{
			"signer": fence.SignerPubKeyPrefix,
			// The full public key, because the prefix above is a DISPLAY form.
			// It cannot be used as a request key: sending it to the lift or
			// abandon route answers 404 "no signer fence is held", which reads
			// like "your fence does not exist" rather than "you passed the
			// truncated form". A field report lost several attempts to exactly
			// that, and had to read the durable table to recover the key the
			// status surface had already been given.
			"signer_public_key": fence.SignerPubKeyHex,
			"tx_hash":           fence.TxHash,
			"held_seconds":      int(fence.HeldFor.Round(time.Second).Seconds()),
			"since":             fence.Since.UTC().Format(time.RFC3339),
			"attempts":          fence.Attempts,
			"cause":             fence.Cause,
			"resolution":        fence.Resolution,
			"last_cause":        fence.LastCause,
			"last_detail":       fence.LastDetail,
			"signer_agent_id":   fence.SignerPubKeyHex,
		}
		if fence.HasNonce {
			// The nonce is what makes a fence actionable: it can be compared
			// against what the chain has committed for this signer.
			//
			// A STRING, not a number, because the comparison happens in the
			// dashboard: SAGE nonces are nanosecond allocations and routinely
			// exceed JavaScript's safe integer range (2^53), where JSON.parse
			// silently rounds. A rounded nonce is worse than no nonce — the
			// operator compares it against the chain's committed value and draws
			// a wrong conclusion from the last digits being different.
			row["nonce"] = strconv.FormatUint(fence.Nonce, 10)
		}
		if !fence.LastAttemptAt.IsZero() {
			row["last_attempt_at"] = fence.LastAttemptAt.UTC().Format(time.RFC3339)
		}
		signers = append(signers, row)
	}
	out["signers"] = signers
	return out
}

// fenceResolutionRow renders the last fence outcome for the dashboard.
func fenceResolutionRow(last tx.FenceResolution) map[string]any {
	row := map[string]any{
		"mode":         last.Mode,
		"signer":       last.SignerPubKeyPrefix,
		"tx_hash":      last.TxHash,
		"held_seconds": int(last.HeldFor.Round(time.Second).Seconds()),
		"at":           last.At.UTC().Format(time.RFC3339),
		"detail":       last.Detail,
	}
	if last.HasNonce {
		// A string for the same reason the held rows use one: the dashboard
		// compares it against the chain, and JSON numbers round past 2^53.
		row["nonce"] = strconv.FormatUint(last.Nonce, 10)
	}
	return row
}

// signerFenceRoutes renders what can still end each held fence, by resolution
// class, and says what that means in one sentence. It never promises a route
// the fence does not have.
func signerFenceRoutes(held []tx.FencedSigner) string {
	reconciling, proofOrOperator := 0, 0
	for _, fence := range held {
		switch fence.Resolution {
		case "reconciling":
			reconciling++
		case "proof_or_operator":
			proofOrOperator++
		default:
			// An unclassified cause is reported as needing an operator rather
			// than as self-healing, because the safe reading of "we cannot say
			// how this ends" is that it may not end on its own.
			proofOrOperator++
		}
	}
	switch {
	case reconciling > 0 && proofOrOperator > 0:
		return fmt.Sprintf("reconciliation is re-submitting the identical bytes for %d of them; %d "+
			"restored from a previous process's durable intent did not survive with its signed bytes and lifts "+
			"only on a proof read from the chain or on an explicit operator abandon "+
			"(POST /v1/dashboard/signer-fence/abandon)", reconciling, proofOrOperator)
	case proofOrOperator > 0:
		return fmt.Sprintf("%d restored from a previous process's durable intent: the signed bytes did not "+
			"survive, so reconciliation cannot re-submit them and this fence lifts only on a proof read from "+
			"the chain or on an explicit operator abandon "+
			"(POST /v1/dashboard/signer-fence/abandon)", proofOrOperator)
	default:
		return "reconciliation is re-submitting the identical bytes to force an answer"
	}
}
