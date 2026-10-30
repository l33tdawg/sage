package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/metrics"
)

const externalCometHealthTimeout = 3 * time.Second

// externalCometReachable checks RPC availability, not voting or commit progress.
func externalCometReachable(ctx context.Context, client *http.Client, cometRPC string) bool {
	if client == nil {
		return false
	}
	probeCtx, cancel := context.WithTimeout(ctx, externalCometHealthTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, strings.TrimRight(cometRPC, "/")+"/health", nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	const limit = 1 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil || len(body) > limit {
		return false
	}
	var reply struct {
		Result *struct{}       `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(body, &reply); err != nil {
		return false
	}
	return reply.Result != nil && (len(reply.Error) == 0 || string(reply.Error) == "null")
}

// Called only after the existing chain-ID bind succeeds. Losing the external
// engine must clear the initial healthy bit; a later valid reply restores it.
func monitorExternalCometHealth(ctx context.Context, client *http.Client, cometRPC string, health *metrics.HealthChecker, interval time.Duration) {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer health.SetCometBFTHealth(false)
	for {
		health.SetCometBFTHealth(externalCometReachable(ctx, client, cometRPC))
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
