package store

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFederationRBACViewsListConsensusDomainsAndPeerGrants(t *testing.T) {
	bs := newTestBadger(t)

	require.NoError(t, bs.RegisterDomain("tii", "owner-a", "", 11))
	require.NoError(t, bs.RegisterDomain("tii.research", "owner-b", "tii", 12))
	require.NoError(t, bs.SetAccessGrant("tii", "peer-agent", 1, 0, "owner-a"))
	require.NoError(t, bs.SetAccessGrant("tii.research", "peer-agent", 2, 99, "owner-b"))
	require.NoError(t, bs.SetAccessGrant("other", "someone-else", 2, 0, "owner-c"))

	domains, err := bs.ListRegisteredDomains()
	require.NoError(t, err)
	require.Equal(t, []RegisteredDomain{
		{DomainName: "tii", OwnerAgentID: "owner-a", CreatedHeight: 11},
		{DomainName: "tii.research", OwnerAgentID: "owner-b", ParentDomain: "tii", CreatedHeight: 12},
	}, domains)

	grants, err := bs.ListAccessGrantsForAgent("peer-agent")
	require.NoError(t, err)
	require.Equal(t, []AgentAccessGrant{
		{Domain: "tii", AgentID: "peer-agent", Level: 1, GranterID: "owner-a"},
		{Domain: "tii.research", AgentID: "peer-agent", Level: 2, ExpiresAt: 99, GranterID: "owner-b"},
	}, grants)
}

func TestListAccessGrantsForAgentRequiresIdentity(t *testing.T) {
	bs := newTestBadger(t)
	_, err := bs.ListAccessGrantsForAgent("")
	require.Error(t, err)
}
