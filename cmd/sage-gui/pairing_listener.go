package main

// Temporary LAN listener for the node-join ceremony (Phase 5b-3). It exists
// ONLY while a pairing is active: handleJoinHostStart binds it, and it is torn
// down on approve+fetch, abort, or session expiry. Personal-mode nodes are
// otherwise localhost-only; this is the sole window in which the node accepts
// an inbound LAN connection, and the handler behind it is gated entirely by
// proof-of-secret (see web/network_join_pairing.go).

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"
)

// startPairingListener binds an ephemeral 0.0.0.0 TCP listener serving handler
// and returns a stop func plus the chosen port. Wired to
// DashboardHandler.PairingListenerFn.
func startPairingListener(handler http.Handler) (stop func(), port int, err error) {
	//nolint:gosec // Pairing intentionally opens a short-lived LAN listener gated by proof-of-secret.
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "0.0.0.0:0")
	if err != nil {
		return nil, 0, fmt.Errorf("bind pairing listener: %w", err)
	}
	port = ln.Addr().(*net.TCPAddr).Port

	srv := &http.Server{
		Handler:           handler,
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	go func() {
		_ = srv.Serve(ln) // returns on Shutdown
	}()

	stop = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}
	return stop, port, nil
}
