package federation

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	libcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

func testRouteBundle(t *testing.T, ip string) JoinP2PBundle {
	t.Helper()
	relayKey, _, err := libcrypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	relayID, err := peer.IDFromPrivateKey(relayKey)
	require.NoError(t, err)
	peerKey, _, err := libcrypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	peerID, err := peer.IDFromPrivateKey(peerKey)
	require.NoError(t, err)
	return JoinP2PBundle{PeerID: peerID.String(), Protocol: "/sage/fed/1.0.0", Addrs: []string{
		"/ip4/" + ip + "/tcp/4001/p2p/" + relayID.String() + "/p2p-circuit/p2p/" + peerID.String(),
	}}
}

func TestAuthenticatedRouteExchangePersistsRemoteAndReturnsLocal(t *testing.T) {
	m := &Manager{}
	remote := testRouteBundle(t, "203.0.113.10")
	local := testRouteBundle(t, "203.0.113.20")
	var persistedChain string
	var persisted []string
	m.SetJoinP2PHooks(JoinP2PHooks{
		LocalBundle: func() (JoinP2PBundle, error) { return local, nil },
		Persist: func(chain string, addrs []string) error {
			persistedChain, persisted = chain, append([]string(nil), addrs...)
			return nil
		},
	})
	body, _ := json.Marshal(remote)
	req := httptest.NewRequest(http.MethodPost, "/fed/v1/p2p/routes", bytes.NewReader(body))
	ctx := context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{ChainID: "remote-chain", Agreement: &store.CrossFedRecord{}})
	rr := httptest.NewRecorder()
	m.handleP2PRoutes(rr, req.WithContext(ctx))
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "remote-chain", persistedChain)
	assert.Equal(t, remote.Addrs, persisted)
	var got JoinP2PBundle
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &got))
	assert.Equal(t, local, got)
}
