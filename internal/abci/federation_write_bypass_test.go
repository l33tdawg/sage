package abci

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFederationPeerPolicyCannotAuthorizeRawCometSubmit closes the important
// transport-bypass regression: an off-consensus peer policy is not consensus
// write authority. A peer that submits tx-1 directly still needs an ordinary
// consensus grant, and v11.9 refuses to mint that grant from federation Write.
func TestFederationPeerPolicyCannotAuthorizeRawCometSubmit(t *testing.T) {
	app := setupTestApp(t)
	owner := newAgentKey(t)
	peer := newAgentKey(t)
	const domain = "tii.federated-private"

	require.NoError(t, app.badgerStore.RegisterDomain(domain, owner.id, "", 1))
	require.NoError(t, app.badgerStore.SetAccessGrant(domain, owner.id, 2, 0, owner.id))

	ss, ok := app.GetOffchainStore().(*store.SQLiteStore)
	require.True(t, ok)
	_, err := ss.ReplacePeerRBACPolicy(context.Background(), store.PeerRBACPolicy{
		RemoteChainID: "peer-chain",
		PeerAgentID:   peer.id,
		PolicyEpoch:   "ceremony-epoch",
		RemoteCAPin:   strings.Repeat("ab", 32),
		PolicyVersion: store.CurrentPeerRBACPolicyVersion,
		Domains: []store.PeerRBACDomainPermission{{
			Domain: domain,
			Read:   true,
			Copy:   true,
		}},
	})
	require.NoError(t, err)

	result := app.processMemorySubmit(
		makeMemorySubmitTx(t, peer, domain, "direct federation peer bypass"),
		2,
		time.Unix(2, 0),
	)
	require.Equal(t, uint32(11), result.Code)
	assert.Contains(t, result.Log, "has no write access")
}
