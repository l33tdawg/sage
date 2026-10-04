package voter

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
)

func TestVoteBroadcastMalformedCometResponseFencesExactSigner(t *testing.T) {
	pub, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	encoded := []byte("voter-ambiguous-on-wire-bytes")
	wantHash := tx.CometTxHash(encoded)
	var allowProof atomic.Bool
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if allowProof.Load() {
			_, _ = w.Write([]byte(`{"result":{"hash":"` + strings.ToUpper(hex.EncodeToString(wantHash[:])) + `","height":"1","tx_result":{"code":0}}}`))
			return
		}
		_, _ = w.Write([]byte(`{"result":`))
	}))
	defer rpc.Close()

	err = tx.WithNonceLease(context.Background(), key, func(uint64) error {
		_, broadcastErr := broadcastVoteTx(context.Background(), rpc.URL, key, encoded, zerolog.Nop())
		return broadcastErr
	})
	require.Error(t, err)

	wantSigner := hex.EncodeToString(pub)
	for _, held := range tx.FencedSigners() {
		if held.SignerPubKeyHex == wantSigner {
			require.Equal(t, strings.ToUpper(hex.EncodeToString(wantHash[:])), held.TxHash)
			allowProof.Store(true)
			require.Eventually(t, func() bool {
				for _, current := range tx.FencedSigners() {
					if current.SignerPubKeyHex == wantSigner {
						return false
					}
				}
				return true
			}, 2*time.Second, 10*time.Millisecond)
			return
		}
	}
	t.Fatalf("malformed on-wire vote response released signer %s instead of fencing it", wantSigner)
}

func TestVoteBroadcastCancellationReconciles(t *testing.T) {
	for name, captureBeforeCancel := range map[string]bool{"before_capture": false, "after_capture": true} {
		t.Run(name, func(t *testing.T) {
			_, key, err := ed25519.GenerateKey(nil)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			callerDone := make(chan struct{})
			var captured capturedTxs
			var lookups atomic.Int64
			var canceled atomic.Bool
			handler := captureHandler(t, &captured)
			rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/tx" {
					lookups.Add(1)
				}
				if r.URL.Path == "/broadcast_tx_sync" && canceled.CompareAndSwap(false, true) {
					// Cancel before the caller can read a reply, with the bytes
					// either captured or lost. Reconciliation must resolve both.
					if captureBeforeCancel {
						handler.ServeHTTP(httptest.NewRecorder(), r)
					}
					cancel()
					<-callerDone
					return
				}
				handler.ServeHTTP(w, r)
			}))
			defer rpc.Close()

			err = tx.WithNonceLease(ctx, key, func(nonce uint64) error {
				vote := &tx.ParsedTx{Type: tx.TxTypeGovVote, Nonce: nonce, Timestamp: time.Now(),
					GovVote: &tx.GovVote{ProposalID: "prop-canceled", Decision: tx.VoteDecisionAccept}}
				if signErr := tx.SignTx(vote, key); signErr != nil {
					return signErr
				}
				encoded, encodeErr := tx.EncodeTx(vote)
				if encodeErr != nil {
					return encodeErr
				}
				_, broadcastErr := broadcastVoteTx(ctx, rpc.URL, key, encoded, zerolog.Nop())
				return broadcastErr
			})
			close(callerDone)
			require.Error(t, err)
			require.Eventually(t, func() bool {
				_, fenced := tx.FenceForSigner(key)
				return lookups.Load() > 0 && !fenced
			}, 5*time.Second, 10*time.Millisecond, "the captured vote must resolve before the mock RPC closes")
			require.Len(t, captured.all(), 1)
		})
	}
}
