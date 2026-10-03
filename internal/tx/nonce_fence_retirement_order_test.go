package tx

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type blockedRetirementStore struct {
	*memIntentStore
	signer  string
	entered chan struct{}
	release chan struct{}
	blocked atomic.Bool
}

func (s *blockedRetirementStore) DeleteFenceIntent(ctx context.Context, signer string) error {
	if signer == s.signer && s.blocked.CompareAndSwap(false, true) {
		close(s.entered)
		<-s.release
	}
	return s.memIntentStore.DeleteFenceIntent(ctx, signer)
}

func TestFenceRetirementCompletesBeforeTheNextSignerLease(t *testing.T) {
	sk := newLeaseTestKey(t)
	key := leaseKeyFor(t, sk)
	signer := hex.EncodeToString([]byte(key))
	disk := &blockedRetirementStore{memIntentStore: newMemIntentStore(), signer: signer, entered: make(chan struct{}), release: make(chan struct{})}
	SetFenceIntentStore(disk)
	t.Cleanup(func() { SetFenceIntentStore(nil); clearAllFencesForTest(t) })
	require.NoError(t, disk.SaveFenceIntent(context.Background(), FenceIntent{SignerPubKeyHex: signer, TxHash: strings.Repeat("ab", 32), Nonce: 42, HasNonce: true, CreatedAt: time.Now()}))
	fence := &keyFence{ch: make(chan struct{}), since: time.Now(), pending: 1, nonce: 42, hasNonce: true}
	fenceMu.Lock()
	fences[key] = fence
	fenceMu.Unlock()
	done := make(chan struct{})
	go func() { liftFence(key, fence, TxVerdictCommitted, "test chain proof"); close(done) }()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(disk.release) }); <-done }
	defer release()
	select {
	case <-disk.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("retirement did not reach the store")
	}

	// Blocked storage for this signer must not hold the process-wide fence lock.
	other := newLeaseTestKey(t)
	otherCtx, otherCancel := context.WithTimeout(context.Background(), time.Second)
	defer otherCancel()
	require.NoError(t, WithNonceLease(otherCtx, other, func(uint64) error { return nil }))

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	signed := false
	err := WithNonceLease(ctx, sk, func(uint64) error { signed = true; return errors.New("next lease reached submit") })
	require.ErrorIs(t, err, ErrSignerFenced, "a signer must remain closed until its durable retirement completes")
	require.False(t, signed, "the next intent could otherwise be deleted by the prior retirement")
	require.True(t, disk.held(signer), "the deliberately blocked durable deletion has not finished")
	require.Len(t, FencedSigners(), 1, "recovery status must remain held until retirement finishes")

	// A concurrent proof must not start a second deletion while one retires.
	liftFence(key, fence, TxVerdictCommitted, "duplicate chain proof")
	release()
	require.False(t, disk.held(signer))
	require.Empty(t, FencedSigners())
	require.NoError(t, WithNonceLease(context.Background(), sk, func(uint64) error { return nil }))
}
