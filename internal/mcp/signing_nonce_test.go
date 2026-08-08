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

type readErrorBody struct {
	read bool
}

func (b *readErrorBody) Read(p []byte) (int, error) {
	if b.read {
		return 0, io.ErrUnexpectedEOF
	}
	b.read = true
	return copy(p, `{"items":[`), nil
}

func (*readErrorBody) Close() error { return nil }

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
	require.NoError(t, server.doSignedJSON(context.Background(), http.MethodGet, "/v1/dashboard/stats", nil, &out))
	require.Equal(t, int32(3), attempts.Load())
}

func TestReplaySafeCanonicalReceiveRetriesUnexpectedResponseEOFWithSameBody(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	server := NewServer("http://localhost:8080", priv)
	requestBody := []byte(`{"receive_token":"stable-token","limit":5}`)
	var attempts atomic.Int32
	server.httpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		gotBody, readErr := io.ReadAll(req.Body)
		require.NoError(t, readErr)
		require.Equal(t, requestBody, gotBody, "a receive retry must preserve its exact idempotency token")
		if attempts.Add(1) == 1 {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       &readErrorBody{},
				Header:     make(http.Header),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"items":[],"count":0}`)),
			Header:     make(http.Header),
		}, nil
	})}
	var out map[string]any
	require.NoError(t, server.doSignedJSON(
		context.Background(), http.MethodPost, "/v1/messages/receive", requestBody, &out,
	))
	require.Equal(t, int32(2), attempts.Load())
}

func TestNonReplayableClaimGETDoesNotRetryUnexpectedResponseEOF(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	server := NewServer("http://localhost:8080", priv)
	var attempts atomic.Int32
	server.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		attempts.Add(1)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       &readErrorBody{},
			Header:     make(http.Header),
		}, nil
	})}
	var out map[string]any
	err = server.doSignedJSON(
		context.Background(), http.MethodGet, "/v1/pipe/inbox?limit=5", nil, &out,
	)
	require.ErrorContains(t, err, "unexpected EOF")
	require.Equal(t, int32(1), attempts.Load())
}

func TestSignedRequestReplayClassificationFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name   string
		method string
		path   string
		want   signedRequestReplaySafety
	}{
		{name: "safe dashboard read", method: http.MethodGet, path: "/v1/dashboard/stats", want: signedRequestReplaySafe},
		{name: "safe memory detail", method: http.MethodGet, path: "/v1/memory/mem-1", want: signedRequestReplaySafe},
		{name: "safe pipe detail", method: http.MethodGet, path: "/v1/pipe/pipe-1", want: signedRequestReplaySafe},
		{name: "passive pipe inbox history", method: http.MethodGet, path: "/v1/pipe/history/inbox?limit=20", want: signedRequestReplaySafe},
		{name: "passive pipe outbox history", method: http.MethodGet, path: "/v1/pipe/history/outbox?limit=20", want: signedRequestReplaySafe},
		{name: "passive results projection", method: http.MethodGet, path: "/v1/pipe/results?limit=5", want: signedRequestReplaySafe},
		{name: "passive results count probe", method: http.MethodGet, path: "/v1/pipe/results?count_only=1", want: signedRequestReplaySafe},
		{name: "canonical message status", method: http.MethodGet, path: "/v1/messages/msg-1/status", want: signedRequestReplaySafe},
		{name: "idempotent message send", method: http.MethodPost, path: "/v1/messages", want: signedRequestReplaySafe},
		{name: "idempotent message receive", method: http.MethodPost, path: "/v1/messages/receive", want: signedRequestReplaySafe},
		{name: "idempotent message reply", method: http.MethodPost, path: "/v1/messages/msg-1/reply", want: signedRequestReplaySafe},
		{name: "idempotent exact read ack", method: http.MethodPut, path: "/v1/messages/msg-1/read", want: signedRequestReplaySafe},
		{name: "idempotent exact read batch", method: http.MethodPut, path: "/v1/messages/read-batch", want: signedRequestReplaySafe},
		{name: "read-only federated challenge batch", method: http.MethodPost, path: "/v1/pipe/receipts/challenge-batch", want: signedRequestReplaySafe},
		{name: "idempotent federated receipt batch", method: http.MethodPut, path: "/v1/pipe/receipts/batch", want: signedRequestReplaySafe},
		{name: "idempotent federated claimed receipt", method: http.MethodPut, path: "/v1/pipe/pipe-1/receipt/claimed", want: signedRequestReplaySafe},
		{name: "idempotent federated read receipt", method: http.MethodPut, path: "/v1/pipe/pipe-1/receipt/read", want: signedRequestReplaySafe},
		{name: "read-only federated claimed challenge", method: http.MethodGet, path: "/v1/pipe/pipe-1/receipt/challenge/claimed", want: signedRequestReplaySafe},
		{name: "read-only federated read challenge", method: http.MethodGet, path: "/v1/pipe/pipe-1/receipt/challenge/read", want: signedRequestReplaySafe},
		{name: "unknown federated receipt kind fails closed", method: http.MethodPut, path: "/v1/pipe/pipe-1/receipt/future", want: signedRequestSingleAttempt},
		{name: "destructive pipe inbox", method: http.MethodGet, path: "/v1/pipe/inbox?limit=5", want: signedRequestSingleAttempt},
		{name: "destructive pipe updates", method: http.MethodGet, path: "/v1/pipe/updates?limit=5", want: signedRequestSingleAttempt},
		{name: "destructive task notifications", method: http.MethodGet, path: "/v1/dashboard/task-notifications?limit=5", want: signedRequestSingleAttempt},
		{name: "unknown get fails closed", method: http.MethodGet, path: "/v1/future/read", want: signedRequestSingleAttempt},
		{name: "unknown nested memory get fails closed", method: http.MethodGet, path: "/v1/memory/mem-1/future-read", want: signedRequestSingleAttempt},
		{name: "unknown nested pipe get fails closed", method: http.MethodGet, path: "/v1/pipe/pipe-1/future-read", want: signedRequestSingleAttempt},
		{name: "unknown nested message read fails closed", method: http.MethodPut, path: "/v1/messages/msg-1/future/read", want: signedRequestSingleAttempt},
		{name: "unknown post fails closed", method: http.MethodPost, path: "/v1/future/query", want: signedRequestSingleAttempt},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, classifySignedRequestReplay(test.method, test.path))
		})
	}
}

func TestDestructiveGETsNeverRetryAmbiguousFailures(t *testing.T) {
	paths := []string{
		"/v1/pipe/inbox?limit=5",
		"/v1/pipe/updates?limit=5",
		"/v1/dashboard/task-notifications?limit=5",
	}
	for _, path := range paths {
		t.Run(path+"/transport_error", func(t *testing.T) {
			_, priv, err := ed25519.GenerateKey(nil)
			require.NoError(t, err)
			server := NewServer("http://localhost:8080", priv)
			var attempts atomic.Int32
			server.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				attempts.Add(1)
				return nil, errors.New("unexpected EOF after request may have reached server")
			})}

			var out map[string]any
			require.Error(t, server.doSignedJSON(context.Background(), http.MethodGet, path, nil, &out))
			require.Equal(t, int32(1), attempts.Load())
		})

		t.Run(path+"/retryable_http_status", func(t *testing.T) {
			_, priv, err := ed25519.GenerateKey(nil)
			require.NoError(t, err)
			server := NewServer("http://localhost:8080", priv)
			var attempts atomic.Int32
			server.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				attempts.Add(1)
				return &http.Response{
					StatusCode: http.StatusServiceUnavailable,
					Body:       io.NopCloser(strings.NewReader(`{"detail":"response unavailable after mutation"}`)),
					Header:     make(http.Header),
				}, nil
			})}

			var out map[string]any
			require.Error(t, server.doSignedJSON(context.Background(), http.MethodGet, path, nil, &out))
			require.Equal(t, int32(1), attempts.Load())
		})
	}
}

func TestPassivePipelineResultsRemainsRetryable(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	server := NewServer("http://localhost:8080", priv)
	var attempts atomic.Int32
	server.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		if attempts.Add(1) == 1 {
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Body:       io.NopCloser(strings.NewReader(`{"detail":"node starting"}`)),
				Header:     make(http.Header),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"items":[],"count":0}`)),
			Header:     make(http.Header),
		}, nil
	})}

	var out map[string]any
	require.NoError(t, server.doSignedJSON(
		context.Background(), http.MethodGet, "/v1/pipe/results?limit=5", nil, &out,
	))
	require.Equal(t, int32(2), attempts.Load())
}
