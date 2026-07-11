package mcp

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
)

func TestSignedRequestUsesUniqueVerifiableNonce(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	var mu sync.Mutex
	seen := make(map[string]bool)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, readErr := io.ReadAll(r.Body)
		require.NoError(t, readErr)
		nonceHex := r.Header.Get("X-Nonce")
		nonce, decodeErr := hex.DecodeString(nonceHex)
		require.NoError(t, decodeErr)
		require.Len(t, nonce, 8)
		sig, decodeErr := hex.DecodeString(r.Header.Get("X-Signature"))
		require.NoError(t, decodeErr)
		ts, parseErr := strconv.ParseInt(r.Header.Get("X-Timestamp"), 10, 64)
		require.NoError(t, parseErr)
		require.True(t, auth.VerifyRequestWithNonce(pub, r.Method, r.URL.RequestURI(), body, ts, nonce, sig))
		mu.Lock()
		require.False(t, seen[nonceHex], "every same-path request needs a fresh replay nonce")
		seen[nonceHex] = true
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(api.Close)
	server := NewServer(api.URL, priv)

	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, requestErr := server.signedRequest(context.Background(), http.MethodGet, "/v1/memory/list", nil)
			if requestErr == nil {
				_ = resp.Body.Close()
			}
			errs <- requestErr
		}()
	}
	wg.Wait()
	close(errs)
	for requestErr := range errs {
		require.NoError(t, requestErr)
	}
	require.Len(t, seen, 100)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestIdempotentReadRetriesDuringNodeStartup(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	server := NewServer("http://localhost:8080", priv)
	var attempts atomic.Int32
	server.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		if attempts.Add(1) < 3 {
			return nil, errors.New("dial tcp 127.0.0.1:8080: connect: connection refused")
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
			Header:     make(http.Header),
		}, nil
	})}
	var out map[string]any
	require.NoError(t, server.doSignedJSON(context.Background(), http.MethodGet, "/v1/status", nil, &out))
	require.Equal(t, int32(3), attempts.Load())
}
