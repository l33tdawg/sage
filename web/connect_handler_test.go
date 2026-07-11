package web

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
)

func TestChatGPTDesktopConnectIsAppScoped(t *testing.T) {
	assert.True(t, connectAppScoped["chatgpt-desktop"])
	assert.False(t, connectFolderScoped["chatgpt-desktop"])
}

func TestConnectProviderRequiresLocalHumanCEREBRUM(t *testing.T) {
	var calls atomic.Int32
	var gotProvider, gotPath, gotToken string
	h := &DashboardHandler{ConnectFunc: func(provider, path, token string) ([]ConnectFile, error) {
		calls.Add(1)
		gotProvider, gotPath, gotToken = provider, path, token
		return []ConnectFile{{Path: "/tmp/config", Action: "created"}}, nil
	}}
	router := chi.NewRouter()
	router.Post("/v1/dashboard/connect/{provider}", h.handleConnectProvider)

	tests := []struct {
		name       string
		origin     string
		secFetch   string
		agentID    string
		host       string
		remoteAddr string
		wantStatus int
	}{
		{name: "same-origin browser", origin: "http://localhost:8080", host: "localhost:8080", remoteAddr: "127.0.0.1:54321", wantStatus: http.StatusOK},
		{name: "origin-less caller", host: "localhost:8080", wantStatus: http.StatusForbidden},
		{name: "signed agent", origin: "http://localhost:8080", host: "localhost:8080", agentID: "agent-1", wantStatus: http.StatusForbidden},
		{name: "cross-origin browser", origin: "https://evil.example", secFetch: "cross-site", host: "localhost:8080", wantStatus: http.StatusForbidden},
		{name: "non-loopback browser", origin: "http://192.168.1.10:8080", host: "192.168.1.10:8080", remoteAddr: "192.168.1.20:54321", wantStatus: http.StatusForbidden},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/connect/chatgpt-desktop", nil)
			req.Host = tc.host
			if tc.remoteAddr != "" {
				req.RemoteAddr = tc.remoteAddr
			}
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			if tc.secFetch != "" {
				req.Header.Set("Sec-Fetch-Site", tc.secFetch)
			}
			if tc.agentID != "" {
				req = req.WithContext(context.WithValue(req.Context(), verifiedDashboardAgentKey{}, tc.agentID))
			}
			rr := httptest.NewRecorder()
			router.ServeHTTP(rr, req)
			assert.Equal(t, tc.wantStatus, rr.Code, rr.Body.String())
		})
	}
	assert.Equal(t, int32(1), calls.Load(), "only the same-origin human request may write config")
	assert.Equal(t, "chatgpt-desktop", gotProvider)
	assert.Empty(t, gotPath, "app-scoped ChatGPT desktop setup must ignore caller paths")
	assert.Empty(t, gotToken)
}
