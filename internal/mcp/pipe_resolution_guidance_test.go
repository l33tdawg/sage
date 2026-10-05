package mcp

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLegacyPipeResolutionFailureGuidesExactDiscoveryWithoutSending(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusOK} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			sends := 0
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/pipe/resolve", func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_ = json.NewEncoder(w).Encode(map[string]any{"detail": "Unknown target"})
			})
			mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
				sends++
				w.WriteHeader(http.StatusInternalServerError)
			})
			ts := httptest.NewServer(mux)
			defer ts.Close()
			_, key, err := ed25519.GenerateKey(nil)
			require.NoError(t, err)
			_, err = NewServer(ts.URL, key).toolPipe(context.Background(), map[string]any{
				"to": "provider-alias", "payload": "work",
			})
			require.Error(t, err)
			for _, guidance := range []string{"exact local agent ID", "#node/agent-prefix", "agent_id@chain", "sage_find_agent", "sage_directory", "arbitrary provider aliases are not inferred"} {
				require.Contains(t, err.Error(), guidance)
			}
			require.Zero(t, sends, "a failed resolution must never reach either send service")
		})
	}
}
