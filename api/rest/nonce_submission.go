package rest

import (
	"context"
	"fmt"
	"net/http"
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
		writeProblem(w, http.StatusServiceUnavailable, "Submission unavailable", "Transaction submission was canceled before it began.")
	default:
		s.logger.Error().Err(err).Msg("failed to broadcast " + operation + " tx")
		status, publicMsg := broadcastErrorPublic(err)
		writeProblem(w, status, "Broadcast error", publicMsg)
	}
}
