package main

import "testing"

// TestDefaultCometRPC pins the precedence for `sage-gui upgrade`'s RPC target:
// explicit SAGE_COMET_RPC wins; otherwise it derives from SAGE_CMT_RPC_ADDR so a
// moved node is reached without a second env var; both unset preserves the
// historical http://127.0.0.1:26657.
func TestDefaultCometRPC(t *testing.T) {
	// SAGE_COMET_RPC wins outright, even when SAGE_CMT_RPC_ADDR is also set.
	t.Setenv("SAGE_CMT_RPC_ADDR", "tcp://127.0.0.1:36657")
	t.Setenv("SAGE_COMET_RPC", "http://example:9999")
	if got := defaultCometRPC(); got != "http://example:9999" {
		t.Errorf("defaultCometRPC() = %q, want SAGE_COMET_RPC override", got)
	}

	// No SAGE_COMET_RPC → derived from SAGE_CMT_RPC_ADDR (wildcard → loopback).
	t.Setenv("SAGE_COMET_RPC", "")
	t.Setenv("SAGE_CMT_RPC_ADDR", "tcp://0.0.0.0:36657")
	if got := defaultCometRPC(); got != "http://127.0.0.1:36657" {
		t.Errorf("defaultCometRPC() = %q, want derived http://127.0.0.1:36657", got)
	}

	// Both unset → historical default preserved.
	t.Setenv("SAGE_CMT_RPC_ADDR", "")
	if got := defaultCometRPC(); got != "http://127.0.0.1:26657" {
		t.Errorf("defaultCometRPC() = %q, want default http://127.0.0.1:26657", got)
	}
}
