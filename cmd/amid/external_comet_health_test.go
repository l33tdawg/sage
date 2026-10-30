package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/metrics"
	"github.com/stretchr/testify/require"
)

func TestExternalCometReachableRequiresSuccessfulRPCReply(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
		body string
		want bool
	}{
		{"healthy", 200, `{"jsonrpc":"2.0","id":-1,"result":{}}`, true},
		{"null_error", 200, `{"result":{},"error":null}`, true},
		{"http_error", 503, `{"result":{}}`, false},
		{"rpc_error", 200, `{"error":{"code":-32603}}`, false},
		{"result_and_error", 200, `{"result":{},"error":{"code":-32603}}`, false},
		{"missing_result", 200, `{}`, false},
		{"null_result", 200, `{"result":null}`, false},
		{"malformed", 200, `not JSON`, false},
		{"oversized", 200, `{"result":{},"padding":"` + strings.Repeat("x", 1<<20) + `"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/health", r.URL.Path)
				w.WriteHeader(tc.code)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			require.Equal(t, tc.want, externalCometReachable(context.Background(), server.Client(), server.URL))
		})
	}
}

func TestExternalCometReachableHonorsCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	require.False(t, externalCometReachable(ctx, server.Client(), server.URL))
	require.Less(t, time.Since(started), time.Second)
}

func TestExternalCometReachableRefusesStoppedEngine(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"result":{}}`))
	}))
	defer server.Close()
	client := server.Client()
	require.True(t, externalCometReachable(context.Background(), client, server.URL))
	server.Close()
	require.False(t, externalCometReachable(context.Background(), client, server.URL))
}

func TestExternalCometMonitorClearsAndRestoresHealth(t *testing.T) {
	var reachable atomic.Bool
	reachable.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !reachable.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":-1,"result":{}}`))
	}))
	defer server.Close()
	health := metrics.NewHealthChecker()
	health.SetPostgresHealth(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		monitorExternalCometHealth(ctx, server.Client(), server.URL, health, 5*time.Millisecond)
	}()
	status := func() int {
		w := httptest.NewRecorder()
		health.HealthHandler(w, httptest.NewRequest(http.MethodGet, "/health", nil))
		return w.Code
	}
	require.Eventually(t, func() bool { return status() == 200 }, time.Second, time.Millisecond)
	reachable.Store(false)
	require.Eventually(t, func() bool { return status() == 503 }, time.Second, time.Millisecond)
	reachable.Store(true)
	require.Eventually(t, func() bool { return status() == 200 }, time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("health monitor ignored cancellation")
	}
	require.Equal(t, 503, status())
}
