package hunch

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func server(t *testing.T, status int, body any, check func(*http.Request, map[string]any)) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		if check != nil {
			check(r, req)
		}
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(srv.Close)
	return New(srv.URL+"/", "k-test", "judge-a", 5*time.Second)
}

func TestYesNo_ReturnsProbabilities(t *testing.T) {
	c := server(t, 200, map[string]any{"model": "judge-a", "results": map[string]any{
		"lasting": map[string]any{"kind": "yesno", "p_yes": 0.93}}},
		func(r *http.Request, req map[string]any) {
			require.Equal(t, "/v1/judge", r.URL.Path)
			require.Equal(t, "Bearer k-test", r.Header.Get("Authorization"))
			require.Equal(t, "judge-a", req["model"])
			checks := req["checks"].(map[string]any)
			require.Equal(t, "yesno", checks["lasting"].(map[string]any)["kind"])
		})
	ps, err := c.YesNo(context.Background(), map[string]string{"memory": "x"}, map[string]Check{"lasting": Lasting})
	require.NoError(t, err)
	require.InDelta(t, 0.93, ps["lasting"], 1e-9)
}

func TestYesNo_FailsClosed(t *testing.T) {
	t.Run("missing probability", func(t *testing.T) {
		c := server(t, 200, map[string]any{"results": map[string]any{}}, nil)
		_, err := c.YesNo(context.Background(), "x", map[string]Check{"lasting": Lasting})
		require.True(t, errors.Is(err, ErrNoVerdict), "a missing p is no verdict, never a default")
	})
	t.Run("out of range", func(t *testing.T) {
		c := server(t, 200, map[string]any{"results": map[string]any{
			"lasting": map[string]any{"p_yes": 1.4}}}, nil)
		_, err := c.YesNo(context.Background(), "x", map[string]Check{"lasting": Lasting})
		require.True(t, errors.Is(err, ErrNoVerdict))
	})
	t.Run("service error", func(t *testing.T) {
		c := server(t, 400, map[string]any{"error": map[string]any{"code": "unknown_model", "message": "no such model"}}, nil)
		_, err := c.YesNo(context.Background(), "x", map[string]Check{"lasting": Lasting})
		require.ErrorContains(t, err, "unknown_model")
	})
	t.Run("not configured", func(t *testing.T) {
		var c *Client
		_, err := c.YesNo(context.Background(), "x", map[string]Check{"lasting": Lasting})
		require.Error(t, err)
	})
}
