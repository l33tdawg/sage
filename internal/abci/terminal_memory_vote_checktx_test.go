package abci

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestCheckTxCommittedMemoryVoteProof(t *testing.T) {
	for _, tc := range []struct {
		name, status    string
		submittedHeight int64
		currentHeight   int64
		hashless        bool
		wantProof       bool
	}{
		{"canonical committed", "committed", 11, 20, false, true},
		{"still proposed", "proposed", 11, 20, false, false},
		{"deprecated needs separate proof", "deprecated", 11, 20, false, false},
		{"challenged needs separate proof", "challenged", 11, 20, false, false},
		{"legacy committed", "committed", 10, 20, false, false},
		{"activation block", "committed", 11, 9, false, false},
		{"uncommitted submission height", "committed", 21, 20, false, false},
		{"hashless committed", "committed", 11, 20, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := setupAppV24ReanchorGovernanceFixture(t, 1)
			f.app.appV25AppliedHeight = 10
			f.app.state.Height = tc.currentHeight
			submit := makeMemorySubmitTx(t, f.root, "general", "terminal vote proof fixture")
			submit.MemorySubmit.MemoryID = "terminal-vote-proof"
			accepted := f.app.processMemorySubmit(submit, tc.submittedHeight, time.Unix(25000, 0))
			require.Zero(t, accepted.Code, accepted.Log)
			hash := submit.MemorySubmit.ContentHash
			if tc.hashless {
				hash = nil
			}
			require.NoError(t, f.app.badgerStore.SetMemoryHash(submit.MemorySubmit.MemoryID, hash, tc.status))
			vote := &tx.ParsedTx{Type: tx.TxTypeMemoryVote, Nonce: 42,
				MemoryVote: &tx.MemoryVote{MemoryID: submit.MemorySubmit.MemoryID, Decision: tx.VoteDecisionAccept}}
			require.NoError(t, tx.SignTx(vote, f.validator.priv))
			raw, err := tx.EncodeTx(vote)
			require.NoError(t, err)
			beforeNonce, err := f.app.badgerStore.GetNonce(f.validator.id)
			require.NoError(t, err)
			beforeHash, err := f.app.badgerStore.ComputeAppHashExcludingBookkeeping()
			require.NoError(t, err)
			response, err := f.app.CheckTx(context.Background(), &abcitypes.RequestCheckTx{Tx: raw})
			require.NoError(t, err)
			if tc.wantProof {
				require.Equal(t, uint32(13), response.Code, response.Log)
				require.Equal(t, "sage/memory-vote-committed/v1", response.Codespace)
				// Exercise the real ABCI response through the RPC resolver, including
				// hash binding and the ordinary missing-index response.
				rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/tx" {
						w.WriteHeader(http.StatusInternalServerError)
						_, _ = fmt.Fprint(w, `{"error":{"code":-32603,"message":"tx not indexed"}}`)
						return
					}
					encoded, decodeErr := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
					if decodeErr != nil || string(encoded) != string(raw) {
						t.Error("resolver did not resubmit the exact signed bytes")
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					checked, checkErr := f.app.CheckTx(r.Context(), &abcitypes.RequestCheckTx{Tx: encoded})
					if checkErr != nil {
						t.Error(checkErr)
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"result": map[string]any{
						"hash": fmt.Sprintf("%X", sha256.Sum256(encoded)), "height": "0",
						"check_tx": checked, "tx_result": map[string]int{"code": 0},
					}})
				}))
				defer rpc.Close()
				outcome, resolveErr := tx.CometTxResolver(rpc.URL)(context.Background(), raw)
				require.NoError(t, resolveErr)
				require.Equal(t, tx.TxVerdictRejected, outcome.Verdict, outcome.Detail)
				// The same bytes remain a consensus rejection. Admission proof must
				// not turn a stale vote into a nonce-consuming successful action.
				result := f.app.processMemoryVote(vote, tc.currentHeight+1, time.Unix(25001, 0))
				require.Equal(t, uint32(13), result.Code, result.Log)
			} else {
				require.NotEqual(t, "sage/memory-vote-committed/v1", response.Codespace)
			}
			afterNonce, err := f.app.badgerStore.GetNonce(f.validator.id)
			require.NoError(t, err)
			require.Equal(t, beforeNonce, afterNonce)
			afterHash, err := f.app.badgerStore.ComputeAppHashExcludingBookkeeping()
			require.NoError(t, err)
			require.Equal(t, beforeHash, afterHash, "admission/rejection must not mutate consensus state")
			_, status, err := f.app.badgerStore.GetMemoryHash(submit.MemorySubmit.MemoryID)
			require.NoError(t, err)
			require.Equal(t, tc.status, status)
		})
	}
}

func TestCheckTxCommittedVoteProofPreservesSignatureAndNonceGates(t *testing.T) {
	f := setupAppV24ReanchorGovernanceFixture(t, 1)
	f.app.appV9AppliedHeight = 1
	f.app.appV25AppliedHeight = 10
	f.app.state.Height = 20
	submit := makeMemorySubmitTx(t, f.root, "general", "nonce gate precedence")
	submit.MemorySubmit.MemoryID = "nonce-gated-vote"
	accepted := f.app.processMemorySubmit(submit, 11, time.Unix(25000, 0))
	require.Zero(t, accepted.Code, accepted.Log)
	require.NoError(t, f.app.badgerStore.SetMemoryStatusPreservingHash(submit.MemorySubmit.MemoryID, "committed"))
	require.NoError(t, f.app.badgerStore.SetNonce(f.validator.id, 42))
	for _, tc := range []struct {
		name         string
		nonce        uint64
		badSignature bool
		code         uint32
	}{
		{"spent nonce", 42, false, 4},
		{"zero nonce", 0, false, 4},
		{"invalid signature", 43, true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vote := &tx.ParsedTx{Type: tx.TxTypeMemoryVote, Nonce: tc.nonce,
				MemoryVote: &tx.MemoryVote{MemoryID: submit.MemorySubmit.MemoryID, Decision: tx.VoteDecisionAccept}}
			require.NoError(t, tx.SignTx(vote, f.validator.priv))
			if tc.badSignature {
				vote.Signature[0] ^= 1
			}
			raw, err := tx.EncodeTx(vote)
			require.NoError(t, err)
			response, err := f.app.CheckTx(context.Background(), &abcitypes.RequestCheckTx{Tx: raw})
			require.NoError(t, err)
			require.Equal(t, tc.code, response.Code, response.Log)
			require.Empty(t, response.Codespace)
		})
	}
}

func TestCheckTxCommittedVoteProofRequiresCompleteCanonicalState(t *testing.T) {
	for _, prefix := range []string{"memory:", "memauthor:", "memdomain:", "mem_class:", "memheight:"} {
		t.Run(prefix, func(t *testing.T) {
			f := setupAppV24ReanchorGovernanceFixture(t, 1)
			f.app.appV25AppliedHeight = 10
			f.app.state.Height = 20
			submit := makeMemorySubmitTx(t, f.root, "general", "complete committed target")
			submit.MemorySubmit.MemoryID = "incomplete-proof"
			accepted := f.app.processMemorySubmit(submit, 11, time.Unix(25000, 0))
			require.Zero(t, accepted.Code, accepted.Log)
			require.NoError(t, f.app.badgerStore.SetMemoryStatusPreservingHash(submit.MemorySubmit.MemoryID, "committed"))
			require.NoError(t, f.app.badgerStore.DB().Update(func(transaction *badger.Txn) error {
				return transaction.Delete([]byte(prefix + submit.MemorySubmit.MemoryID))
			}))
			vote := &tx.ParsedTx{Type: tx.TxTypeMemoryVote, Nonce: 42,
				MemoryVote: &tx.MemoryVote{MemoryID: submit.MemorySubmit.MemoryID, Decision: tx.VoteDecisionAccept}}
			require.NoError(t, tx.SignTx(vote, f.validator.priv))
			raw, err := tx.EncodeTx(vote)
			require.NoError(t, err)
			response, err := f.app.CheckTx(context.Background(), &abcitypes.RequestCheckTx{Tx: raw})
			require.NoError(t, err)
			require.NotEqual(t, "sage/memory-vote-committed/v1", response.Codespace)
		})
	}
}

func TestCommittedVoteProofTargetCannotBeReopenedByReplayOrAdoption(t *testing.T) {
	f := setupAppV24ReanchorGovernanceFixture(t, 1)
	f.app.appV25AppliedHeight = 10
	f.app.state.Height = 20
	submit := makeMemorySubmitTx(t, f.root, "general", "immutable ballot target")
	submit.MemorySubmit.MemoryID = "closed-ballot-proof"
	accepted := f.app.processMemorySubmit(submit, 11, time.Unix(25000, 0))
	require.Zero(t, accepted.Code, accepted.Log)
	require.NoError(t, f.app.badgerStore.SetMemoryStatusPreservingHash(submit.MemorySubmit.MemoryID, "committed"))
	state, err := f.app.badgerStore.GetMemoryDisclosureState(submit.MemorySubmit.MemoryID)
	require.NoError(t, err)
	// Governance adoption can initialize legacy envelopes, but it cannot
	// repurpose this complete target as a fresh proposed ballot.
	plan := sha256.Sum256([]byte("attempted ballot reopening"))
	_, err = f.app.badgerStore.AdoptLegacyMemories(plan[:], []store.MemoryLegacyAdoptionEntry{{
		MemoryID: submit.MemorySubmit.MemoryID, Status: "proposed", ContentHash: state.ContentHash,
		Domain: state.Domain, Author: state.Author, AuthorPrincipal: state.AuthorPrincipal,
		Classification: state.Classification,
	}})
	require.ErrorIs(t, err, store.ErrMemoryLegacyAdoptionConflict)
	// An exact ordinary replay is accepted only as a no-op. Later terminal
	// lifecycle changes likewise must not make this signed vote admissible.
	for _, status := range []string{"committed", "deprecated", "committed"} {
		require.NoError(t, f.app.badgerStore.SetMemoryStatusPreservingHash(submit.MemorySubmit.MemoryID, status))
		replayed := f.app.processMemorySubmit(submit, 21, time.Unix(25001, 0))
		require.Zero(t, replayed.Code, replayed.Log)
		_, after, hashErr := f.app.badgerStore.GetMemoryHash(submit.MemorySubmit.MemoryID)
		require.NoError(t, hashErr)
		require.Equal(t, status, after)
		result := f.app.processMemoryVote(&tx.ParsedTx{PublicKey: f.validator.pub,
			MemoryVote: &tx.MemoryVote{MemoryID: submit.MemorySubmit.MemoryID, Decision: tx.VoteDecisionAccept}}, 21, time.Unix(25001, 0))
		require.Equal(t, uint32(13), result.Code, result.Log)
	}
}
