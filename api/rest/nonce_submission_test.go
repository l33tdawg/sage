package rest

import (
	"context"
	"crypto/ed25519"
	"errors"
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
