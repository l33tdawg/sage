package abci

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// RFC #443 §7.3: a second tx-33 for the same remote chain is an upsert that
// replaces pin and endpoint in place (no second record, status stays active).
func TestRFC443Baseline_Tx33UpsertReplacesPinAndEndpoint(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5
	admin := newAgentKey(t)
	registerAgent(t, app, admin, "admin", "admin")

	first := termsFor("sage-b", []string{"*"})
	res := app.processCrossFedSet(crossFedSetTx(t, admin, first), 10, time.Now())
	require.Equal(t, uint32(0), res.Code, res.Log)

	second := termsFor("sage-b", []string{"*"})
	second.Endpoint = "https://peer-moved.example:9443"
	second.PeerPubKey = bytes.Repeat([]byte{7}, 32)
	res = app.processCrossFedSet(crossFedSetTx(t, admin, second), 11, time.Now())
	require.Equal(t, uint32(0), res.Code, res.Log)

	ep, pk, _, _, _, _, st, err := app.badgerStore.GetCrossFed("sage-b")
	require.NoError(t, err)
	require.Equal(t, "https://peer-moved.example:9443", ep)
	require.Equal(t, bytes.Repeat([]byte{7}, 32), pk)
	require.Equal(t, "active", st)

	all, err := app.badgerStore.ListCrossFed()
	require.NoError(t, err)
	n := 0
	for _, r := range all {
		if r.RemoteChainID == "sage-b" {
			n++
		}
	}
	require.Equal(t, 1, n, "upsert keeps one record per remote chain")
}
