package tx

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEvidenceReaderAcceptsBothCometCountSpellings pins the tolerance that
// keeps a formatting difference from being read as "the nodes could not be
// asked". CometBFT's mempool surfaces have returned counts as quoted strings
// while its peer surfaces return bare numbers; decoding only one spelling
// turned the other into a fault, and a fault in the automatic route was (until
// the annotation fix) indistinguishable from a route that was merely waiting.
func TestEvidenceReaderAcceptsBothCometCountSpellings(t *testing.T) {
	cases := []struct {
		name         string
		peersJSON    string
		mempoolJSON  string
		wantPeers    int
		wantCount    int
		wantTotal    int
		wantErr      bool
		errSubstring string
	}{
		{
			name:        "bare numbers (the common shape)",
			peersJSON:   `{"result":{"n_peers":0}}`,
			mempoolJSON: `{"result":{"n_txs":0,"total":0,"txs":[]}}`,
		},
		{
			name:        "quoted counts (mempool surfaces have shipped this)",
			peersJSON:   `{"result":{"n_peers":"2"}}`,
			mempoolJSON: `{"result":{"n_txs":"1","total":"3","txs":[]}}`,
			wantPeers:   2,
			wantCount:   1,
			wantTotal:   3,
		},
		{
			name:         "unparseable count is a fault, never a zero",
			peersJSON:    `{"result":{"n_peers":"many"}}`,
			mempoolJSON:  `{"result":{"n_txs":0,"total":0,"txs":[]}}`,
			wantErr:      true,
			errSubstring: "neither a number nor a numeric string",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/net_info":
					_, _ = w.Write([]byte(tc.peersJSON))
				case "/status":
					_ = json.NewEncoder(w).Encode(map[string]any{
						"result": map[string]any{"sync_info": map[string]any{"catching_up": false}},
					})
				case "/unconfirmed_txs":
					_, _ = w.Write([]byte(tc.mempoolJSON))
				case "/tx":
					w.WriteHeader(http.StatusInternalServerError)
					_ = json.NewEncoder(w).Encode(map[string]any{
						"error": map[string]any{"code": -32603, "message": "Internal error", "data": "tx not found"},
					})
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(server.Close)

			ev, err := ReadFenceAbandonEvidence(context.Background(), server.URL, nil, FencedSigner{
				SignerPubKeyHex: signerHexForTestPrefix(),
				TxHash:          "ABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABAB",
				Nonce:           7,
				HasNonce:        true,
			})
			if tc.wantErr {
				require.Error(t, err, "an unreadable count must be a fault, not a silent zero")
				require.Contains(t, err.Error(), tc.errSubstring)
				return
			}
			require.NoError(t, err)
			require.True(t, ev.PeersChecked)
			require.True(t, ev.MempoolChecked)
			require.Equal(t, tc.wantPeers, ev.Peers)
			require.Equal(t, tc.wantCount, ev.MempoolCount)
			require.Equal(t, tc.wantTotal, ev.MempoolTotal)
		})
	}
}

// signerHexForTestPrefix returns a well-formed ed25519 public key hex for tests
// that only exercise the evidence reader (the signer is not resolved further).
func signerHexForTestPrefix() string {
	return "3d73cdbdffaacac7b3f2b7d0f9a56f1b6dd0f1f5a4b2e1c0d9e8f7a6b5c4d3e2"
}
