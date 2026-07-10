package rest

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestEmbedAgentAuthAppV17RequestEnvelopeGate(t *testing.T) {
	for _, tc := range []struct {
		name      string
		postV17   bool
		wantBytes bool
	}{
		{name: "pre-activation preserves legacy bytes", postV17: false, wantBytes: false},
		{name: "post-activation embeds canonical request", postV17: true, wantBytes: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pub, priv, err := auth.GenerateKeypair()
			require.NoError(t, err)
			body := []byte(`{"name":"bound"}`)
			path := "/v1/domain/register?audit=true"
			ts := time.Now().Unix()
			sig := auth.SignRequest(priv, http.MethodPost, path, body, ts)

			req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
			req.Header.Set("X-Agent-ID", auth.PublicKeyToAgentID(pub))
			req.Header.Set("X-Signature", hex.EncodeToString(sig))
			req.Header.Set("X-Timestamp", fmt.Sprintf("%d", ts))

			server := &Server{}
			server.SetPostV17ForNextTxAccessor(func() bool { return tc.postV17 })
			parsed := &tx.ParsedTx{}
			handler := middleware.Ed25519AuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				server.embedAgentAuth(r.Context(), parsed)
				w.WriteHeader(http.StatusNoContent)
			}))
			handler.ServeHTTP(httptest.NewRecorder(), req)

			assert.Equal(t, []byte(pub), parsed.AgentPubKey)
			assert.Equal(t, sig, parsed.AgentSig)
			if tc.wantBytes {
				assert.Equal(t, []byte("POST "+path+"\n"+string(body)), parsed.AgentRequest)
			} else {
				assert.Empty(t, parsed.AgentRequest)
			}
		})
	}
}
