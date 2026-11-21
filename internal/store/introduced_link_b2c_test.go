package store

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func toState(t *testing.T, s *SQLiteStore, l *IntroducedLink, path ...string) {
	t.Helper()
	ctx := context.Background()
	from := IntroducedLinkIntent
	for _, to := range path {
		tr := IntroducedLinkTransition{LinkID: l.LinkID, From: from, To: to, At: 1100, Reason: "r"}
		if from == IntroducedLinkIntent && to == IntroducedLinkSigned {
			tr.TxBytes, tr.Reason = signedTermsTx(t, l.Terms, 7), ""
		}
		if from != IntroducedLinkIntent {
			tr.ExpectTxHash = l.TxHash
		}
		ok, err := s.TransitionIntroducedLink(ctx, tr)
		require.NoError(t, err)
		require.True(t, ok, "%s → %s", from, to)
		got, err := s.GetIntroducedLink(ctx, l.LinkID)
		require.NoError(t, err)
		l.TxHash = got.TxHash
		from = to
	}
}

// An aborted rekey never put its bytes on chain, so it does not take over
// governance of the generation from the link it failed to replace. An
// aborted kind-new attempt still governs, as in B1.
func TestIntroducedLink_AbortedRekeyDoesNotGovern(t *testing.T) {
	ctx := context.Background()
	s := newTokenStore(t)
	live := introLinkFixture(1, "chain-b")
	require.NoError(t, s.InsertIntroducedLinkIntent(ctx, live))
	toState(t, s, live, IntroducedLinkSigned, IntroducedLinkCommitted, IntroducedLinkActive, IntroducedLinkRekeyRequired)

	rk := introLinkFixture(2, "chain-b")
	rk.Kind, rk.Terms, rk.Generation = IntroducedLinkKindRekey, live.Terms, live.Generation
	require.NoError(t, s.InsertIntroducedLinkIntent(ctx, rk))
	got, err := s.IntroducedLinkForGeneration(ctx, "chain-b", live.Generation)
	require.NoError(t, err)
	require.Equal(t, rk.LinkID, got.LinkID, "an in-flight rekey governs (and denies) its generation")

	toState(t, s, rk, IntroducedLinkAborted)
	got, err = s.IntroducedLinkForGeneration(ctx, "chain-b", live.Generation)
	require.NoError(t, err)
	require.Equal(t, live.LinkID, got.LinkID, "the aborted rekey hands governance back")
	states, err := s.IntroducedLinkGenerationStates(ctx)
	require.NoError(t, err)
	require.Equal(t, IntroducedLinkRekeyRequired, states["chain-b"][live.Generation])

	// An aborted kind-new attempt keeps governing its generation.
	n := introLinkFixture(3, "chain-c")
	require.NoError(t, s.InsertIntroducedLinkIntent(ctx, n))
	toState(t, s, n, IntroducedLinkAborted)
	got, err = s.IntroducedLinkForGeneration(ctx, "chain-c", n.Generation)
	require.NoError(t, err)
	require.Equal(t, n.LinkID, got.LinkID)
}

func TestIntroducedLink_CardRecord(t *testing.T) {
	ctx := context.Background()
	s := newTokenStore(t)
	l := introLinkFixture(1, "chain-b")
	require.NoError(t, s.InsertIntroducedLinkIntent(ctx, l))
	none, err := s.GetIntroducedLinkCard(ctx, l.LinkID)
	require.NoError(t, err)
	require.Nil(t, none)

	digest := strings.Repeat("d", 64)
	ok, err := s.RecordIntroducedLinkCard(ctx, l.LinkID, IntroducedLinkActive, 2, digest, "card_rotated", 1200)
	require.NoError(t, err)
	require.False(t, ok, "a row in another state is not touched")
	ok, err = s.RecordIntroducedLinkCard(ctx, l.LinkID, IntroducedLinkIntent, 2, digest, "card_rotated: endpoint", 1200)
	require.NoError(t, err)
	require.True(t, ok)
	c, err := s.GetIntroducedLinkCard(ctx, l.LinkID)
	require.NoError(t, err)
	require.Equal(t, uint64(2), c.CardRev)
	require.Equal(t, digest, c.CardDigest)
	hist, err := s.ListIntroducedLinkHistory(ctx, l.LinkID)
	require.NoError(t, err)
	last := hist[len(hist)-1]
	require.Equal(t, IntroducedLinkIntent, last.From)
	require.Equal(t, IntroducedLinkIntent, last.To)
	require.Equal(t, "card_rotated: endpoint", last.Reason)

	_, err = s.RecordIntroducedLinkCard(ctx, l.LinkID, IntroducedLinkIntent, 3, "nothex", "x", 1300)
	require.ErrorIs(t, err, ErrIntroducedLinkInvalid)
	_, err = s.RecordIntroducedLinkCard(ctx, strings.Repeat("9", 64), IntroducedLinkIntent, 3, digest, "x", 1300)
	require.ErrorIs(t, err, ErrIntroducedLinkNotFound)
}

func TestIntroducedLink_RefusalLogIsBounded(t *testing.T) {
	ctx := context.Background()
	s := newTokenStore(t)
	require.ErrorIs(t, s.InsertIntroducedLinkRefusal(ctx, IntroducedLinkRefusal{At: 1, RemoteChainID: "chain-b"}), ErrIntroducedLinkInvalid)
	for i := 0; i < maxIntroducedLinkRefusals+5; i++ {
		chain := "chain-b"
		if i%2 == 1 {
			chain = "chain-c"
		}
		require.NoError(t, s.InsertIntroducedLinkRefusal(ctx, IntroducedLinkRefusal{
			At: int64(1000 + i), OrgID: "org-acme", OfferID: strings.Repeat("0f", 32), RemoteChainID: chain,
			Kind: "new", Role: "host", Reason: fmt.Sprintf("policy_mismatch #%d", i),
		}))
	}
	all, err := s.ListIntroducedLinkRefusals(ctx, "")
	require.NoError(t, err)
	require.Len(t, all, maxIntroducedLinkRefusals)
	require.Equal(t, fmt.Sprintf("policy_mismatch #%d", maxIntroducedLinkRefusals+4), all[0].Reason, "newest first")
	only, err := s.ListIntroducedLinkRefusals(ctx, "chain-c")
	require.NoError(t, err)
	for _, r := range only {
		require.Equal(t, "chain-c", r.RemoteChainID)
	}
}

func TestRekeySyncControlPin(t *testing.T) {
	ctx := context.Background()
	s := newTokenStore(t)
	pinA, pinB := strings.Repeat("a", 64), strings.Repeat("b", 64)
	require.Error(t, s.RekeySyncControlPin(ctx, "chain-a", pinB), "no binding")
	require.NoError(t, s.PrepareSyncControl(ctx, SyncControl{
		RemoteChainID: "chain-a", Role: "host", ControllerChainID: "chain-ctl",
		ControllerAgentID: "ctl", PeerAgentID: "peer", PolicyEpoch: "e1", RemoteCAPin: pinA,
	}))
	require.Error(t, s.RekeySyncControlPin(ctx, "chain-a", pinB), "a pending binding is not rekeyed")
	require.NoError(t, s.ActivateSyncControl(ctx, "chain-a", "e1"))
	require.NoError(t, s.RekeySyncControlPin(ctx, "chain-a", pinB))
	require.NoError(t, s.RekeySyncControlPin(ctx, "chain-a", pinB), "idempotent")
	c, err := s.GetSyncControl(ctx, "chain-a")
	require.NoError(t, err)
	require.Equal(t, pinB, c.RemoteCAPin)
	require.Equal(t, "e1", c.PolicyEpoch, "the epoch and binding stay")
	require.Equal(t, "active", c.BindingState)
	require.Error(t, s.RekeySyncControlPin(ctx, "chain-a", "short"))
}
