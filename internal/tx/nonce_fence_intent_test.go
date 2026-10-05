package tx

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This file pins the DURABILITY half of the fence's contract. The fence lives
// in process memory, so before durable intent existed a restart, crash or
// SIGKILL discarded it and the next start re-seeded the allocator from the
// highest committed nonce — below the abandoned one — and signed into the gap.
// The abandoned transaction was then refused Code 4 when it finally landed,
// surfacing later as an unrelated action failing as a replay.
//
// The tests below are written so that deleting either half of the fix fails
// them: remove the write in RegisterSubmittedTx and the first test finds no
// record; remove the restore at startup and the third finds no fence and the
// key signs straight into the gap.

type memIntentStore struct {
	mu      sync.Mutex
	intents map[string]FenceIntent
	saves   int
	deletes int
}

func newMemIntentStore() *memIntentStore {
	return &memIntentStore{intents: make(map[string]FenceIntent)}
}

func (s *memIntentStore) SaveFenceIntent(_ context.Context, intent FenceIntent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.intents[strings.ToLower(intent.SignerPubKeyHex)] = intent
	s.saves++
	return nil
}

func (s *memIntentStore) DeleteFenceIntent(_ context.Context, signerHex string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.intents, strings.ToLower(signerHex))
	s.deletes++
	return nil
}

func (s *memIntentStore) ListFenceIntents(_ context.Context) ([]FenceIntent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]FenceIntent, 0, len(s.intents))
	for _, intent := range s.intents {
		out = append(out, intent)
	}
	return out, nil
}

func (s *memIntentStore) held(signerHex string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.intents[strings.ToLower(signerHex)]
	return ok
}

func intentStoreForTest(t *testing.T) *memIntentStore {
	t.Helper()
	store := newMemIntentStore()
	SetFenceIntentStore(store)
	t.Cleanup(func() {
		SetFenceIntentStore(nil)
		clearAllFencesForTest(t)
	})
	return store
}

func signerHexFor(t *testing.T, sk ed25519.PrivateKey) string {
	t.Helper()
	pub, ok := sk.Public().(ed25519.PublicKey)
	require.True(t, ok)
	return hex.EncodeToString(pub)
}

// A submit that registers its bytes and then reports success has a proven fate:
// the nonce is consumed, nothing is in flight, and the durable shadow must not
// survive to fence the next start.
func TestDurableFenceIntentIsRetiredWhenASubmitCommits(t *testing.T) {
	setFenceTimingsForTest(t, fastFenceTimings())
	store := intentStoreForTest(t)
	sk := newLeaseTestKey(t)

	require.NoError(t, WithNonceLease(context.Background(), sk, func(nonce uint64) error {
		RegisterSubmittedTx(sk, []byte("encoded-transaction-bytes"), nil)
		require.True(t, store.held(signerHexFor(t, sk)),
			"the intent must be durable BEFORE the bytes reach the transport")
		return nil
	}))

	require.False(t, store.held(signerHexFor(t, sk)),
		"a committed submit must retire its durable intent")
	require.Empty(t, FencedSigners())
}

// An indeterminate submit leaves the intent in place, and the fence lift that
// proves its fate retires it. Both halves matter: an intent that outlives a
// proven lift re-fences the key at the next startup.
func TestDurableFenceIntentSurvivesAnIndeterminateSubmitAndDiesWithTheFence(t *testing.T) {
	setFenceTimingsForTest(t, fastFenceTimings())
	store := intentStoreForTest(t)
	sk := newLeaseTestKey(t)
	key := leaseKeyFor(t, sk)

	txBytes := []byte("indeterminate-transaction-bytes")
	submitErr := WithNonceLease(context.Background(), sk, func(nonce uint64) error {
		RegisterSubmittedTx(sk, txBytes, func(context.Context, []byte) (TxOutcome, error) {
			// Never resolves: this test is about durability, not reconciliation.
			return TxOutcome{Verdict: TxVerdictUnresolved}, nil
		})
		return Indeterminate(errors.New("transport reset"), txBytes, nil)
	})
	require.ErrorIs(t, submitErr, ErrSubmitIndeterminate,
		"an indeterminate submit reports the ambiguity to its caller; the fence is the follow-up")

	held := FencedSigners()
	require.Len(t, held, 1)
	require.True(t, store.held(signerHexFor(t, sk)), "an unresolved submit keeps its intent")

	// The operator proves the fate of the exact transaction: this byte string
	// does not decode to a nonce, so the proof that applies here is the hash
	// found in a committed block. (Supersession is exercised in the restart test,
	// which can fabricate a record carrying a nonce.)
	require.NoError(t, LiftFenceWithProof(context.Background(), signerHexFor(t, sk), FenceLiftProof{
		Kind:   "committed",
		TxHash: held[0].TxHash,
		Detail: "committed at height 1234",
	}))

	require.Empty(t, FencedSigners())
	require.False(t, store.held(signerHexFor(t, sk)),
		"a proven lift must retire the durable intent")
	require.Nil(t, fences[key])
}

// The restart case, end to end: a record left by a process that died while its
// transaction was in flight must come back as a fence that REFUSES TO SIGN,
// and must resolve only on evidence.
func TestRestartRestoresFenceFromDurableIntentAndRefusesToSign(t *testing.T) {
	setFenceTimingsForTest(t, fastFenceTimings())
	store := intentStoreForTest(t)
	sk := newLeaseTestKey(t)
	signerHex := signerHexFor(t, sk)

	// What the previous process left behind: bytes were handed to the transport,
	// the fate was never proven, and the process died.
	require.NoError(t, store.SaveFenceIntent(context.Background(), FenceIntent{
		SignerPubKeyHex: signerHex,
		TxHash:          strings.Repeat("AB", 32),
		Nonce:           41,
		HasNonce:        true,
		CreatedAt:       time.Now().UTC(),
	}))

	restored, err := RestoreFencesFromIntents(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, restored)

	held := FencedSigners()
	require.Len(t, held, 1)
	require.Equal(t, signerHex, held[0].SignerPubKeyHex)
	require.Equal(t, uint64(41), held[0].Nonce)
	require.True(t, held[0].HasNonce)

	// The key must refuse to sign: this is the whole point. A short deadline
	// turns "parked on the fence" into the typed refusal instead of a hang.
	signCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	signErr := WithNonceLease(signCtx, sk, func(uint64) error {
		t.Fatal("a restored fence must never let the key allocate")
		return nil
	})
	require.ErrorIs(t, signErr, ErrSignerFenced)

	// Evidence lifts it, and the record goes with it.
	proof, err := ProveFenceLiftFromChain(context.Background(), "", func(ed25519.PublicKey) (uint64, bool) {
		return 99, true // a higher nonce has committed
	}, held[0])
	require.NoError(t, err)
	require.Equal(t, "superseded", proof.Kind)
	require.Equal(t, uint64(99), proof.CommittedNonce)

	require.NoError(t, LiftFenceWithProof(context.Background(), signerHex, proof))
	require.Empty(t, FencedSigners())
	require.False(t, store.held(signerHex))

	// With the fence lifted on proof, the key signs again.
	require.NoError(t, WithNonceLease(context.Background(), sk, func(uint64) error { return nil }))
}

// No proof, no lift. A fence that a lookup could not resolve must stay held,
// and the refusal must say what evidence is missing rather than timing out.
func TestOperatorLiftRefusesWithoutProof(t *testing.T) {
	setFenceTimingsForTest(t, fastFenceTimings())
	store := intentStoreForTest(t)
	sk := newLeaseTestKey(t)
	signerHex := signerHexFor(t, sk)

	require.NoError(t, store.SaveFenceIntent(context.Background(), FenceIntent{
		SignerPubKeyHex: signerHex,
		TxHash:          strings.Repeat("CD", 32),
		Nonce:           7,
		HasNonce:        true,
		CreatedAt:       time.Now().UTC(),
	}))
	_, err := RestoreFencesFromIntents(context.Background())
	require.NoError(t, err)
	held := FencedSigners()
	require.Len(t, held, 1)

	// A "proof" that is not above the fenced nonce proves nothing.
	require.NoError(t, store.SaveFenceIntent(context.Background(), FenceIntent{}))
	err = LiftFenceWithProof(context.Background(), signerHex, FenceLiftProof{
		Kind:           "superseded",
		CommittedNonce: 7,
	})
	var unproven *FenceLiftUnprovenError
	require.ErrorAs(t, err, &unproven)
	require.Contains(t, unproven.Reason, "not above the fenced nonce")
	require.Len(t, FencedSigners(), 1, "a refused proof must leave the fence standing")

	// The prover itself refuses when neither proof holds, and says why a lookup
	// miss is not one of them.
	_, err = ProveFenceLiftFromChain(context.Background(), "", func(ed25519.PublicKey) (uint64, bool) {
		return 3, true // below the fenced nonce
	}, held[0])
	require.ErrorAs(t, err, &unproven)
	require.Contains(t, unproven.Reason, "A missing lookup is not proof")
	require.Len(t, FencedSigners(), 1)
}
