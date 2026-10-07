package rest

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type delayedResponseEmbedder struct {
	authoritativeTestEmbedder
	delay time.Duration
}

func (e delayedResponseEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	select {
	case <-time.After(e.delay):
		return e.authoritativeTestEmbedder.Embed(ctx, text)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Exercise the production HTTP server and signed submit handler, rather than a
// ResponseRecorder (which cannot enforce socket write deadlines). Scale only
// the outer deadline and fake-service latency to keep the regression fast.
func TestHTTPServerSubmitAcknowledgmentSurvivesEmbeddingAndCommit(t *testing.T) {
	t.Setenv("SAGE_EMBEDDING_TIMEOUT", "")
	t.Setenv("SAGE_EMBED_TIMEOUT", "")
	t.Setenv("SAGE_TX_COMMIT_TIMEOUT_MS", "")
	for _, secure := range []bool{false, true} {
		name := "HTTP"
		if secure {
			name = "HTTPS"
		}
		t.Run(name, func(t *testing.T) {
			var broadcasts atomic.Int32
			comet := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/broadcast_tx_commit" {
					http.Error(w, "not found", http.StatusNotFound)
					return
				}
				broadcasts.Add(1)
				time.Sleep(500 * time.Millisecond)
				writeCometCommitFixture(t, w, r, 0, "", 0, "", 7)
			}))
			defer comet.Close()
			srv, _, _ := newTestServer(t, comet.URL)
			srv.embedder = delayedResponseEmbedder{
				authoritativeTestEmbedder: authoritativeTestEmbedder{vector: []float32{1, 0}, name: "fixture"},
				delay:                     200 * time.Millisecond,
			}
			outer := srv.newHTTPServer("127.0.0.1:0", nil)
			httpTest := httptest.NewUnstartedServer(outer.Handler)
			httpTest.Config = outer
			httpTest.Config.WriteTimeout /= 100
			if secure {
				httpTest.StartTLS()
			} else {
				httpTest.Start()
			}
			defer httpTest.Close()
			body := []byte(`{"content":"response deadline regression","memory_type":"fact","domain_tag":"crypto","confidence_score":0.9}`)
			req, _ := signedRequest(t, http.MethodPost, "/v1/memory/submit", body)
			req.URL.Scheme = "http"
			if secure {
				req.URL.Scheme = "https"
			}
			req.URL.Host = httpTest.Listener.Addr().String()
			req.RequestURI = ""
			client := httpTest.Client()
			client.Timeout = 5 * time.Second
			resp, err := client.Do(req)
			require.EqualValues(t, 1, broadcasts.Load(), "an acknowledgment failure must never trigger a second submit")
			require.NoError(t, err, "the confirmed submit must retain its HTTP acknowledgment")
			defer resp.Body.Close()
			response, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, http.StatusCreated, resp.StatusCode, string(response))
			var result SubmitMemoryResponse
			require.NoError(t, json.Unmarshal(response, &result))
			require.True(t, result.Committed)
			require.EqualValues(t, 7, result.CommittedHeight)
			assertStrictTxHash(t, result.TxHash)
		})
	}
}

func TestHTTPServerWriteTimeoutTracksSubmitBudget(t *testing.T) {
	for _, tc := range []struct {
		name      string
		canonical string
		alias     string
		commit    string
		want      time.Duration
	}{
		{name: "default", want: 285750 * time.Millisecond},
		{name: "canonical 60 seconds", canonical: "60s", want: 375750 * time.Millisecond},
		{name: "canonical two minutes", canonical: "2m", want: 555750 * time.Millisecond},
		{name: "legacy alias", alias: "60s", want: 375750 * time.Millisecond},
		{name: "canonical wins", canonical: "2m", alias: "1ms", want: 555750 * time.Millisecond},
		{name: "invalid canonical uses safe default", canonical: "invalid", alias: "1ms", want: 285750 * time.Millisecond},
		{name: "zero embedding uses safe default", canonical: "0s", want: 285750 * time.Millisecond},
		{name: "negative embedding uses safe default", canonical: "-1s", want: 285750 * time.Millisecond},
		{name: "configured commit", commit: "120000", want: 345750 * time.Millisecond},
		{name: "invalid commit uses default", commit: "invalid", want: 285750 * time.Millisecond},
		{name: "zero commit uses default", commit: "0", want: 285750 * time.Millisecond},
		{name: "negative commit uses default", commit: "-1", want: 285750 * time.Millisecond},
		{name: "commit cap", commit: "600000", want: maxRESTWriteTimeout},
		{name: "commit multiplication overflow uses default", commit: "9223372036854775807", want: 285750 * time.Millisecond},
		{name: "commit parse overflow uses default", commit: "9223372036854775808", want: 285750 * time.Millisecond},
		{name: "embedding multiplication overflow", canonical: "2562047h47m16.854775807s", want: maxRESTWriteTimeout},
		{name: "bounded", canonical: "24h", want: maxRESTWriteTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SAGE_EMBEDDING_TIMEOUT", tc.canonical)
			t.Setenv("SAGE_EMBED_TIMEOUT", tc.alias)
			t.Setenv("SAGE_TX_COMMIT_TIMEOUT_MS", tc.commit)
			server := &Server{}

			plain := server.newHTTPServer("127.0.0.1:0", nil)
			secure := server.newHTTPServer("127.0.0.1:0", &tls.Config{MinVersion: tls.VersionTLS12})

			assert.Equal(t, tc.want, plain.WriteTimeout)
			assert.Equal(t, tc.want, secure.WriteTimeout)
			assert.Equal(t, 15*time.Second, plain.ReadTimeout)
			assert.Equal(t, 60*time.Second, plain.IdleTimeout)
		})
	}
}
