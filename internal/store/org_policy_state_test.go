package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOrgPolicyState_GetSetAtomic(t *testing.T) {
	ctx := context.Background()
	s := newTokenStore(t)

	got, err := s.GetOrgPolicyState(ctx, "absent")
	require.NoError(t, err)
	require.Nil(t, got, "an absent key reads as nil, not an error")

	require.NoError(t, s.SetOrgPolicyStatesAtomic(ctx, []StateWrite{
		{Key: "orgpolicy/enroll/a", Value: []byte("outcome")},
		{Key: "orgpolicy/accepted/o", Value: []byte("rev1")},
	}))
	got, err = s.GetOrgPolicyState(ctx, "orgpolicy/enroll/a")
	require.NoError(t, err)
	require.Equal(t, []byte("outcome"), got)

	// Upsert overwrites in place.
	require.NoError(t, s.SetOrgPolicyStatesAtomic(ctx, []StateWrite{{Key: "orgpolicy/accepted/o", Value: []byte("rev2")}}))
	got, err = s.GetOrgPolicyState(ctx, "orgpolicy/accepted/o")
	require.NoError(t, err)
	require.Equal(t, []byte("rev2"), got)

	require.NoError(t, s.SetOrgPolicyStatesAtomic(ctx, nil), "an empty write set is a no-op")
}

func TestOrgPolicyState_CompareAndSetIsAtomic(t *testing.T) {
	ctx := context.Background()
	s := newTokenStore(t)

	applied, err := s.CompareAndSetOrgPolicyStatesAtomic(ctx,
		[]StateCondition{{Key: "orgpolicy/enroll/a"}, {Key: "orgpolicy/accepted/o"}},
		[]StateWrite{
			{Key: "orgpolicy/enroll/a", Value: []byte("first")},
			{Key: "orgpolicy/accepted/o", Value: []byte("rev1")},
		})
	require.NoError(t, err)
	require.True(t, applied)

	applied, err = s.CompareAndSetOrgPolicyStatesAtomic(ctx,
		[]StateCondition{{Key: "orgpolicy/enroll/a"}, {Key: "orgpolicy/accepted/o"}},
		[]StateWrite{
			{Key: "orgpolicy/enroll/a", Value: []byte("second")},
			{Key: "orgpolicy/accepted/o", Value: []byte("rev2")},
		})
	require.NoError(t, err)
	require.False(t, applied)

	got, err := s.GetOrgPolicyState(ctx, "orgpolicy/enroll/a")
	require.NoError(t, err)
	require.Equal(t, []byte("first"), got, "a failed compare-and-set must write nothing")
}

func TestOrgPolicyState_ListKeysByPrefix(t *testing.T) {
	ctx := context.Background()
	s := newTokenStore(t)

	keys, err := s.ListOrgPolicyStateKeys(ctx, "orgpolicy/enroll/", 10)
	require.NoError(t, err)
	require.Empty(t, keys)

	require.NoError(t, s.SetOrgPolicyStatesAtomic(ctx, []StateWrite{
		{Key: "orgpolicy/enroll/b", Value: []byte("x")},
		{Key: "orgpolicy/enroll/a", Value: []byte("x")},
		{Key: "orgpolicy/enrollx", Value: []byte("x")},
		{Key: "orgpolicy/accepted/o", Value: []byte("x")},
	}))
	keys, err = s.ListOrgPolicyStateKeys(ctx, "orgpolicy/enroll/", 10)
	require.NoError(t, err)
	require.Equal(t, []string{"orgpolicy/enroll/a", "orgpolicy/enroll/b"}, keys)

	_, err = s.ListOrgPolicyStateKeys(ctx, "orgpolicy/enroll/", 1)
	require.Error(t, err, "an over-limit listing fails instead of truncating")
}
