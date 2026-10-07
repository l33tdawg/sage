package rest

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
)

func nonceSubmissionTestTx(memoryID string) *tx.ParsedTx {
	return &tx.ParsedTx{
		Type:      tx.TxTypeMemoryVote,
		Nonce:     1,
		Timestamp: time.Unix(1, 0),
		MemoryVote: &tx.MemoryVote{
			MemoryID: memoryID,
			Decision: tx.VoteDecisionAccept,
		},
	}
}

func TestSubmitConsensusTxSerializesSameKeyThroughSubmit(t *testing.T) {
	_, signingKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := &Server{signingKey: signingKey, logger: zerolog.Nop()}

	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondEntered := make(chan struct{})
	type outcome struct {
		stage  consensusTxStage
		err    error
		parsed *tx.ParsedTx
	}
	firstDone := make(chan outcome, 1)
	secondDone := make(chan outcome, 1)

	go func() {
		parsed := nonceSubmissionTestTx("first")
		stage, submitErr := s.submitConsensusTx(context.Background(), parsed, func(encoded []byte) error {
			wire, decodeErr := tx.DecodeTx(encoded)
			if decodeErr != nil {
				return decodeErr
			}
			valid, verifyErr := tx.VerifyTx(wire)
			if verifyErr != nil {
				return verifyErr
			}
			if !valid {
				return errors.New("first submitted transaction has an invalid signature")
			}
			close(firstEntered)
			<-releaseFirst
			return nil
		})
		firstDone <- outcome{stage: stage, err: submitErr, parsed: parsed}
	}()

	select {
	case <-firstEntered:
	case first := <-firstDone:
		require.NoError(t, first.err)
		t.Fatal("first submit returned before entering its callback")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first submit callback")
	}
	go func() {
		parsed := nonceSubmissionTestTx("second")
		stage, submitErr := s.submitConsensusTx(context.Background(), parsed, func(encoded []byte) error {
			wire, decodeErr := tx.DecodeTx(encoded)
			if decodeErr != nil {
				return decodeErr
			}
			valid, verifyErr := tx.VerifyTx(wire)
			if verifyErr != nil {
				return verifyErr
			}
			if !valid {
				return errors.New("second submitted transaction has an invalid signature")
			}
			close(secondEntered)
			return nil
		})
		secondDone <- outcome{stage: stage, err: submitErr, parsed: parsed}
	}()

	select {
	case <-secondEntered:
		t.Fatal("second same-key submit entered before the first submit returned")
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFirst)

	first := <-firstDone
	second := <-secondDone
	require.NoError(t, first.err)
	require.NoError(t, second.err)
	require.Equal(t, consensusTxSubmit, first.stage)
	require.Equal(t, consensusTxSubmit, second.stage)
	require.Greater(t, second.parsed.Nonce, first.parsed.Nonce)
	require.True(t, first.parsed.Timestamp.After(time.Unix(1, 0)))
	require.True(t, second.parsed.Timestamp.After(time.Unix(1, 0)))
}

func TestSubmitConsensusTxReportsSubmitFailureAndReleasesLease(t *testing.T) {
	_, signingKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := &Server{signingKey: signingKey, logger: zerolog.Nop()}

	wantErr := errors.New("comet unavailable")
	first := nonceSubmissionTestTx("failed")
	stage, err := s.submitConsensusTx(context.Background(), first, func([]byte) error {
		return wantErr
	})
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, consensusTxSubmit, stage)

	second := nonceSubmissionTestTx("after-failure")
	stage, err = s.submitConsensusTx(context.Background(), second, func([]byte) error { return nil })
	require.NoError(t, err)
	require.Equal(t, consensusTxSubmit, stage)
	require.Greater(t, second.Nonce, first.Nonce, "an ambiguous failed submission must never recycle its nonce")
}

func TestWriteConsensusTxErrorDistinguishesFenceAndRestartQuiesce(t *testing.T) {
	s := &Server{logger: zerolog.Nop()}
	for _, tc := range []struct {
		name       string
		err        error
		title      string
		detail     string
		retryAfter string
	}{
		{
			name:       "fenced signer is actionable and retryable",
			err:        fmt.Errorf("await signer: %w", tx.ErrSignerFenced),
			title:      "Signing key temporarily held",
			detail:     "earlier transaction",
			retryAfter: "15",
		},
		{
			name:   "restart quiesce is not described as a fence",
			err:    fmt.Errorf("await signer: %w", tx.ErrSigningQuiesced),
			title:  "Signing paused",
			detail: "coordinated restart",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			s.writeConsensusTxError(rr, consensusTxLease, "test", tc.err)
			require.Equal(t, http.StatusServiceUnavailable, rr.Code)
			require.Equal(t, tc.retryAfter, rr.Header().Get("Retry-After"))
			require.Contains(t, rr.Body.String(), tc.title)
			require.Contains(t, rr.Body.String(), tc.detail)
			require.Contains(t, rr.Body.String(), "Nothing was signed or sent")
			require.False(t, strings.Contains(rr.Body.String(), "rejected"))
		})
	}
}

// TestSubmitConsensusTxRefusesImmediatelyWhileFenced pins the failure mode the
// users reported: writes against a fenced node sat on the nonce lease until the
// CALLER's deadline and arrived as a bare timeout, with no statement that
// nothing had been sent. The refusal must be immediate, and when the fence is
// inspectable the answer must say what the key is held on.
func TestSubmitConsensusTxRefusesImmediatelyWhileFenced(t *testing.T) {
	_, sk, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	pub, ok := sk.Public().(ed25519.PublicKey)
	require.True(t, ok)
	s := &Server{logger: zerolog.Nop(), signingKey: sk}

	encoded := []byte("indeterminate-bytes-for-fast-refusal-test")
	require.ErrorIs(t, tx.WithNonceLease(context.Background(), sk, func(uint64) error {
		return tx.Indeterminate(errors.New("connection reset"), encoded,
			func(context.Context, []byte) (tx.TxOutcome, error) {
				return tx.TxOutcome{Verdict: tx.TxVerdictUnresolved}, nil
			})
	}), tx.ErrSubmitIndeterminate)
	hash := tx.CometTxHash(encoded)
	t.Cleanup(func() {
		// Retire the fence through its own API so a failed assertion cannot leave
		// it behind for sibling tests in this package.
		_ = tx.LiftFenceWithProof(context.Background(), hex.EncodeToString(pub), tx.FenceLiftProof{
			Kind:   "committed",
			TxHash: strings.ToUpper(hex.EncodeToString(hash[:])),
			Detail: "test teardown",
		})
	})

	// A LONG deadline: if the fast path were missing, this call would sit here
	// for its whole budget instead of refusing.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	start := time.Now()
	stage, err := s.submitConsensusTx(ctx, nonceSubmissionTestTx("fenced-key"), func([]byte) error {
		t.Fatal("a fenced signing key must never reach submit")
		return nil
	})
	require.ErrorIs(t, err, tx.ErrSignerFenced)
	require.Equal(t, consensusTxLease, stage)
	require.Less(t, time.Since(start), 5*time.Second,
		"the refusal must be immediate rather than waiting for the caller's deadline")

	recorder := httptest.NewRecorder()
	s.writeConsensusTxError(recorder, stage, "memory submit", err)
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.Equal(t, "15", recorder.Header().Get("Retry-After"))
	require.Contains(t, recorder.Body.String(), "Nothing was signed or sent")
	require.Contains(t, recorder.Body.String(), strings.ToUpper(hex.EncodeToString(hash[:])),
		"the answer must name the transaction the key is held on")
	require.Contains(t, recorder.Body.String(), "signer_fences")
}
