package rest

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/l33tdawg/sage/internal/tx"
)

// consensusTxStage identifies which part of a locally-signed consensus
// submission failed. REST handlers preserve their existing public error
// contracts by distinguishing local sign/encode failures from CometBFT
// admission or execution failures.
type consensusTxStage string

const (
	consensusTxLease  consensusTxStage = "lease"
	consensusTxSign   consensusTxStage = "sign"
	consensusTxEncode consensusTxStage = "encode"
	consensusTxSubmit consensusTxStage = "submit"
)

// IndeterminateSubmissionResponse is the body returned when a submission
// reached the network but this node could not observe its fate before the
// broadcast wait expired.
//
// It exists because every other answer would have been a claim we cannot
// support. A 5xx says "your request failed" about a transaction that may
// already be committed; a 2xx success would assert a verdict we do not have.
// Status 202 with retryable=false and the transaction hash states exactly what
// is true and hands over the one handle that resolves it.
type IndeterminateSubmissionResponse struct {
	Status    string  `json:"status"`
	TxHash    string  `json:"tx_hash,omitempty"`
	Nonce     *uint64 `json:"nonce,omitempty"`
	Committed bool    `json:"committed"`
	Retryable bool    `json:"retryable"`
	Message   string  `json:"message"`
}

// indeterminateSubmissionMessage is the caller-facing instruction for an
// ambiguous outcome. It is deliberately explicit about the one thing a caller
// must not do: re-signing is not a retry, it is a second write.
const indeterminateSubmissionMessage = "The transaction reached the network, but this node could not observe its fate before the broadcast wait expired. It may still commit, and this node's nonce fence is already reconciling it. Do not resubmit: look the transaction up by tx_hash and re-read the target state before deciding anything."

// writeIndeterminateBroadcast reports an ambiguous submission outcome as an
// ambiguous submission outcome, and reports whether it handled err.
//
// Only tx.ErrSubmitIndeterminate is claimed, and one definitive failure that
// arrives wearing that type is explicitly refused: a full mempool. CometBFT
// reports backpressure as an RPC-level error envelope, so the tx layer types it
// indeterminate — from a text envelope it cannot rule admission out — while the
// node has in fact refused the transaction outright. That case has its own
// documented answer (429 + Retry-After + the mempool-full problem type) and must
// keep it, or chain backpressure would be reported as an unknown outcome and
// callers would start hunting for transactions that were never accepted.
//
// Every other failure keeps the status it already had: a CheckTx or
// FinalizeBlock rejection, a sign or encode fault, a pre-send request-building
// error. Those say something about the request that this response deliberately
// does not — that no transaction is in flight.
//
// The hash and nonce are public on-chain data (see tx.IndeterminateDetails),
// and withholding them is precisely what left callers guessing.
func writeIndeterminateBroadcast(w http.ResponseWriter, err error) bool {
	if isMempoolFullErr(err) {
		return false
	}
	hash, nonce, hasNonce, ok := tx.IndeterminateDetails(err)
	if !ok {
		return false
	}
	body := IndeterminateSubmissionResponse{
		Status:    "indeterminate",
		TxHash:    hash,
		Committed: false,
		Retryable: false,
		Message:   indeterminateSubmissionMessage,
	}
	if hasNonce {
		body.Nonce = &nonce
	}
	writeJSON(w, http.StatusAccepted, body)
	return true
}

// submitConsensusTx is the single nonce-lease ownership layer for REST
// transactions signed by s.signingKey. The lease begins before nonce
// allocation and ends only after submit returns, so two same-key handlers
// cannot reach CometBFT in the reverse of their allocated nonce order.
//
// submit owns the wire protocol and may call any one of the raw REST broadcast
// variants (plain, height, context-aware, or FinalizeBlock-log preserving).
// Those raw broadcasters deliberately remain lease-free: they are nested
// wrappers, and acquiring again there would deadlock this non-reentrant lease.
func (s *Server) submitConsensusTx(
	ctx context.Context,
	parsed *tx.ParsedTx,
	submit func(encoded []byte) error,
) (consensusTxStage, error) {
	if parsed == nil {
		return consensusTxSign, fmt.Errorf("missing transaction")
	}
	if submit == nil {
		return consensusTxSubmit, fmt.Errorf("missing transaction submitter")
	}
	if len(s.signingKey) != ed25519.PrivateKeySize {
		return consensusTxSign, fmt.Errorf("validator signing key is unavailable")
	}
	// REFUSE IMMEDIATELY when this key is already fenced. The lease below would
	// otherwise park here until the request's deadline (see tx.FenceForSigner),
	// so an agent writing to a fenced node sees a bare timeout with no reason
	// instead of the typed "not sent, retry" answer. Nothing is signed either
	// way; this only decides how long the caller waits to be told.
	if _, fenced := tx.FenceForSigner(s.signingKey); fenced {
		return consensusTxLease, fmt.Errorf("%w: check tx.FenceForSigner", tx.ErrSignerFenced)
	}

	stage := consensusTxLease
	err := tx.WithNonceLease(ctx, s.signingKey, func(nonce uint64) error {
		parsed.Nonce = nonce
		// A handler may wait behind another same-key commit for several block
		// intervals. Stamp freshness only after it owns the lease.
		parsed.Timestamp = time.Now()

		stage = consensusTxSign
		if err := s.signTx(parsed); err != nil {
			return err
		}

		stage = consensusTxEncode
		encoded, err := tx.EncodeTx(parsed)
		if err != nil {
			return err
		}

		stage = consensusTxSubmit
		return submit(encoded)
	})
	return stage, err
}

func (s *Server) writeConsensusTxError(
	w http.ResponseWriter,
	stage consensusTxStage,
	operation string,
	err error,
) {
	switch stage {
	case consensusTxSign:
		s.logger.Error().Err(err).Msg("failed to sign " + operation + " tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
	case consensusTxEncode:
		s.logger.Error().Err(err).Msg("failed to encode " + operation + " tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
	case consensusTxLease:
		s.logger.Error().Err(err).Msg("failed to acquire nonce lease for " + operation + " tx")
		switch {
		case errors.Is(err, tx.ErrSignerFenced):
			w.Header().Set("Retry-After", strconv.Itoa(signerFencedRetryAfterSeconds))
			writeProblem(w, http.StatusServiceUnavailable, "Signing key temporarily held",
				s.signerFencedPublicMessage())
		case errors.Is(err, tx.ErrSigningQuiesced):
			writeProblem(w, http.StatusServiceUnavailable, "Signing paused",
				"Transaction signing is paused for a coordinated restart. Nothing was signed or sent for this request.")
		default:
			writeProblem(w, http.StatusServiceUnavailable, "Submission unavailable", "Transaction submission was canceled before it began.")
		}
	default:
		// An ambiguous outcome is not a broadcast failure, and reporting it as
		// one is what taught callers to retry a write that may already be
		// committed. Claim it before the generic mapping.
		//
		// The helper is the single authority on what it claims, and its false
		// return is load-bearing: a full mempool reaches here typed
		// indeterminate (see writeIndeterminateBroadcast) and must fall through
		// to the mapping below, not be answered with nothing at all.
		if writeIndeterminateBroadcast(w, err) {
			s.logger.Error().Err(err).Msg("broadcast outcome indeterminate for " + operation + " tx")
			return
		}
		s.logger.Error().Err(err).Msg("failed to broadcast " + operation + " tx")
		status, publicMsg := broadcastErrorPublic(err)
		writeProblem(w, status, "Broadcast error", publicMsg)
	}
}

// signerFencedRetryAfterSeconds matches the web surface's advice: long enough
// not to hammer a node that is already stuck, short enough that a fence
// clearing normally is not followed by a needless wait.
const signerFencedRetryAfterSeconds = 15

// signerFencedPublicMessage is what a caller is told when its write was refused
// because the signing key is fenced. It states the three things that matter and
// nothing else: nothing was signed or sent (so nothing needs undoing), WHY the
// key is held (the earlier submission this request is stuck behind), and where
// an operator can look. The held transaction's identity is public on-chain data
// — the same fields the health surface already prints.
func (s *Server) signerFencedPublicMessage() string {
	const base = "An earlier transaction from this signing key is still waiting to be confirmed. Nothing was " +
		"signed or sent for this request, and nothing needs undoing."
	held, fenced := tx.FenceForSigner(s.signingKey)
	if !fenced {
		return base + " Retry shortly: the node lifts the hold itself once that transaction's fate is proven."
	}
	detail := fmt.Sprintf(" The key is held on transaction %s", held.TxHash)
	if held.HasNonce {
		detail += fmt.Sprintf(" (nonce %d)", held.Nonce)
	}
	if held.HeldFor > 0 {
		detail += fmt.Sprintf(", unproven for %s", held.HeldFor.Round(time.Second))
	}
	return base + detail + ". The node re-reads the chain for a proof on its own; the operator view is " +
		"GET /v1/dashboard/health (signer_fences). Retry shortly."
}
