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
