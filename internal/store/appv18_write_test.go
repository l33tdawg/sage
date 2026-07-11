package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHasWriteAccessMultiOrg_FederationDoesNotImplyWrite(t *testing.T) {
	bs := newTestBadger(t)
	const (
		orgA   = "org-reader"
		orgB   = "org-owner"
		reader = "reader-agent"
		owner  = "owner-agent"
	)
	require.NoError(t, bs.RegisterOrg(orgA, "Readers", "", reader, 1))
	require.NoError(t, bs.AddOrgMember(orgA, reader, 4, "member", 1))
	require.NoError(t, bs.RegisterOrg(orgB, "Owners", "", owner, 1))
	require.NoError(t, bs.AddOrgMember(orgB, owner, 4, "admin", 1))
	require.NoError(t, bs.RegisterDomain("federated.research", owner, "", 1))
	require.NoError(t, bs.SetFederation("fed-read-only", orgA, orgB, nil, 4, 0, false, "active"))

	readable, err := bs.HasAccessMultiOrg("federated.research", reader, 0, time.Unix(10, 0), true)
	require.NoError(t, err)
	assert.True(t, readable, "federation continues to authorize reads")
	writable, err := bs.HasWriteAccessMultiOrg("federated.research", reader, time.Unix(10, 0), true)
	require.NoError(t, err)
	assert.False(t, writable, "federation clearance must not invent a write verb")
}
