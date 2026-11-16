package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func introLinkFixture(id byte, chain string) *IntroducedLink {
	h := func(b byte) string { return strings.Repeat(fmt.Sprintf("%02x", b), 32) }
	return &IntroducedLink{
		LinkID: h(id), OrgID: "org-acme", RemoteChainID: chain, PeerAgentPub: h(0xaa),
		OfferID: h(0xbb), SessionID: "S-" + h(id)[:8], Kind: IntroducedLinkKindNew, Role: "host",
		EIntro: []byte("e-intro"), HostAcceptSig: []byte("ha"), GuestAccept: []byte("ga"), GuestAckSig: []byte("gk"),
		Evidence: []byte(`{"policy_rev":3}`), Terms: []byte("terms"), PriorTermsDigest: "absent",
		StagedRef: "staged/1", CreatedAt: 1000, Deadline: 2000,
	}
}

func txHashOf(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func TestIntroducedLink_InsertAndOneInFlightPerChain(t *testing.T) {
	ctx := context.Background()
	s := newTokenStore(t)

	none, err := s.LatestIntroducedLinkByChain(ctx, "chain-b")
	require.NoError(t, err)
	require.Nil(t, none, "a chain with no row reads as nil")

	l1 := introLinkFixture(1, "chain-b")
	require.NoError(t, s.InsertIntroducedLinkIntent(ctx, l1))
	got, err := s.GetIntroducedLink(ctx, l1.LinkID)
	require.NoError(t, err)
	require.Equal(t, IntroducedLinkIntent, got.State)
	require.Equal(t, l1.EIntro, got.EIntro)
	require.Empty(t, got.TxHash)

	// A second in-flight row for the same chain is refused.
	require.ErrorIs(t, s.InsertIntroducedLinkIntent(ctx, introLinkFixture(2, "chain-b")), ErrIntroducedLinkInFlight)
	// Another chain is independent.
	require.NoError(t, s.InsertIntroducedLinkIntent(ctx, introLinkFixture(3, "chain-c")))
	// The same link id is refused as invalid.
	dup := introLinkFixture(1, "chain-d")
	require.ErrorIs(t, s.InsertIntroducedLinkIntent(ctx, dup), ErrIntroducedLinkInvalid)

	// Once the first row settles, a new intent for the chain is allowed.
	ok, err := s.TransitionIntroducedLink(ctx, IntroducedLinkTransition{
		LinkID: l1.LinkID, From: IntroducedLinkIntent, To: IntroducedLinkAborted, Reason: "stale: offer expired", At: 1100,
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, s.InsertIntroducedLinkIntent(ctx, introLinkFixture(2, "chain-b")))
	latest, err := s.LatestIntroducedLinkByChain(ctx, "chain-b")
	require.NoError(t, err)
	require.Equal(t, introLinkFixture(2, "chain-b").LinkID, latest.LinkID, "the newest row wins")
}

func TestIntroducedLink_ConcurrentIntentsOneWins(t *testing.T) {
	ctx := context.Background()
	s := newTokenStore(t)
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = s.InsertIntroducedLinkIntent(ctx, introLinkFixture(byte(10+i), "chain-b"))
		}(i)
	}
	wg.Wait()
	wins := 0
	for _, err := range errs {
		if err == nil {
			wins++
			continue
		}
		require.ErrorIs(t, err, ErrIntroducedLinkInFlight)
	}
	require.Equal(t, 1, wins, "exactly one in-flight intent per remote chain")
}

func TestIntroducedLink_IntentValidation(t *testing.T) {
	ctx := context.Background()
	s := newTokenStore(t)
	for name, mut := range map[string]func(*IntroducedLink){
		"bad link id":         func(l *IntroducedLink) { l.LinkID = "x" },
		"bad chain":           func(l *IntroducedLink) { l.RemoteChainID = "Chain B" },
		"bad kind":            func(l *IntroducedLink) { l.Kind = "manual" },
		"bad role":            func(l *IntroducedLink) { l.Role = "peer" },
		"missing host accept": func(l *IntroducedLink) { l.HostAcceptSig = nil },
		"missing guest ack":   func(l *IntroducedLink) { l.GuestAckSig = nil },
		"missing terms":       func(l *IntroducedLink) { l.Terms = nil },
		"bad prior digest":    func(l *IntroducedLink) { l.PriorTermsDigest = "none" },
		"tx bytes at intent":  func(l *IntroducedLink) { l.TxBytes = []byte("tx") },
		"deadline not after":  func(l *IntroducedLink) { l.Deadline = l.CreatedAt },
		"oversized blob":      func(l *IntroducedLink) { l.EIntro = make([]byte, MaxIntroducedLinkBlobBytes+1) },
	} {
		l := introLinkFixture(1, "chain-b")
		mut(l)
		require.ErrorIs(t, s.InsertIntroducedLinkIntent(ctx, l), ErrIntroducedLinkInvalid, name)
	}
	rows, err := s.ListIntroducedLinks(ctx, "")
	require.NoError(t, err)
	require.Empty(t, rows, "nothing written for an invalid intent")
}

func TestIntroducedLink_TransitionsAreCASAndHashBound(t *testing.T) {
	ctx := context.Background()
	s := newTokenStore(t)
	l := introLinkFixture(1, "chain-b")
	require.NoError(t, s.InsertIntroducedLinkIntent(ctx, l))

	tr := func(from, to, reason, hash string, at int64, txb []byte) (bool, error) {
		var nonce uint64
		if txb != nil {
			nonce = 7
		}
		return s.TransitionIntroducedLink(ctx, IntroducedLinkTransition{
			LinkID: l.LinkID, From: from, To: to, Reason: reason, At: at, ExpectTxHash: hash, TxBytes: txb, TxNonce: nonce,
		})
	}

	// Disallowed transitions are refused before touching the row.
	_, err := tr(IntroducedLinkIntent, IntroducedLinkActive, "", "", 1100, nil)
	require.ErrorIs(t, err, ErrIntroducedLinkInvalid)
	_, err = tr(IntroducedLinkIntent, IntroducedLinkAborted, "", "", 1100, nil)
	require.ErrorIs(t, err, ErrIntroducedLinkInvalid, "aborted needs a reason")
	_, err = tr(IntroducedLinkIntent, IntroducedLinkSigned, "", "", 1100, nil)
	require.ErrorIs(t, err, ErrIntroducedLinkInvalid, "signed needs the exact bytes")

	txb := []byte("exact tx-33 bytes")
	ok, err := tr(IntroducedLinkIntent, IntroducedLinkSigned, "", "", 1100, txb)
	require.NoError(t, err)
	require.True(t, ok)
	got, err := s.GetIntroducedLink(ctx, l.LinkID)
	require.NoError(t, err)
	require.Equal(t, txHashOf(txb), got.TxHash)
	require.Equal(t, txb, got.TxBytes)
	require.Equal(t, uint64(7), got.TxNonce)

	// A stale caller still expecting intent does nothing.
	ok, err = tr(IntroducedLinkIntent, IntroducedLinkAborted, "late", "", 1150, nil)
	require.NoError(t, err)
	require.False(t, ok)
	// Out of signed, the hash must match.
	_, err = s.TransitionIntroducedLink(ctx, IntroducedLinkTransition{LinkID: l.LinkID, From: IntroducedLinkSigned, To: IntroducedLinkCommitted, At: 1200})
	require.ErrorIs(t, err, ErrIntroducedLinkInvalid, "no hash named")
	ok, err = tr(IntroducedLinkSigned, IntroducedLinkCommitted, "", strings.Repeat("0", 64), 1200, nil)
	require.NoError(t, err)
	require.False(t, ok, "a different hash must not move the row")
	ok, err = tr(IntroducedLinkSigned, IntroducedLinkCommitted, "", got.TxHash, 1200, nil)
	require.NoError(t, err)
	require.True(t, ok)
	_, err = tr(IntroducedLinkCommitted, IntroducedLinkActive, "", got.TxHash, 1300, []byte("again"))
	require.ErrorIs(t, err, ErrIntroducedLinkInvalid, "bytes only on intent → signed")
	ok, err = tr(IntroducedLinkCommitted, IntroducedLinkActive, "", got.TxHash, 1300, nil)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = tr(IntroducedLinkActive, IntroducedLinkPaused, "peer left the allow-list", got.TxHash, 1400, nil)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = tr(IntroducedLinkPaused, IntroducedLinkRevoked, "owner revoked", got.TxHash, 1500, nil)
	require.NoError(t, err)
	require.True(t, ok)
	_, err = tr(IntroducedLinkRevoked, IntroducedLinkActive, "", got.TxHash, 1600, nil)
	require.ErrorIs(t, err, ErrIntroducedLinkInvalid, "revoked is terminal")

	hist, err := s.ListIntroducedLinkHistory(ctx, l.LinkID)
	require.NoError(t, err)
	var path []string
	for _, h := range hist {
		path = append(path, h.From+">"+h.To)
	}
	require.Equal(t, []string{">intent", "intent>signed", "signed>committed", "committed>active", "active>paused", "paused>revoked"}, path)
	require.Equal(t, "owner revoked", hist[len(hist)-1].Reason)
	final, err := s.GetIntroducedLink(ctx, l.LinkID)
	require.NoError(t, err)
	require.Equal(t, IntroducedLinkRevoked, final.State)
	require.Equal(t, "owner revoked", final.StateReason)
}

func TestIntroducedLink_LatestStatesAndRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "intro.db")
	s, err := NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	b1 := introLinkFixture(1, "chain-b")
	require.NoError(t, s.InsertIntroducedLinkIntent(ctx, b1))
	ok, err := s.TransitionIntroducedLink(ctx, IntroducedLinkTransition{LinkID: b1.LinkID, From: IntroducedLinkIntent, To: IntroducedLinkAborted, Reason: "crash_before_broadcast", At: 1100})
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, s.InsertIntroducedLinkIntent(ctx, introLinkFixture(2, "chain-b")))
	require.NoError(t, s.InsertIntroducedLinkIntent(ctx, introLinkFixture(3, "chain-c")))
	require.NoError(t, s.Close())

	// Restart on the same file: rows, history and the in-flight fence hold.
	s, err = NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	states, err := s.LatestIntroducedLinkStates(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"chain-b": IntroducedLinkIntent, "chain-c": IntroducedLinkIntent}, states)
	require.ErrorIs(t, s.InsertIntroducedLinkIntent(ctx, introLinkFixture(4, "chain-c")), ErrIntroducedLinkInFlight)
	rows, err := s.ListIntroducedLinks(ctx, "chain-b")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, IntroducedLinkIntent, rows[0].State, "newest first")
	require.Equal(t, "crash_before_broadcast", rows[1].StateReason)
}
