package main

import "testing"

// TestCmtRPCAddr pins the SAGE_CMT_RPC_ADDR override and its default. The
// default must stay the historical tcp://127.0.0.1:26657 so existing single-node
// deployments are byte-for-byte unchanged; a set value wins verbatim so a second
// personal node can be moved off 26657 to coexist on one host.
func TestCmtRPCAddr(t *testing.T) {
	t.Setenv("SAGE_CMT_RPC_ADDR", "")
	if got := cmtRPCAddr(); got != "tcp://127.0.0.1:26657" {
		t.Errorf("default cmtRPCAddr() = %q, want tcp://127.0.0.1:26657", got)
	}
	t.Setenv("SAGE_CMT_RPC_ADDR", "tcp://127.0.0.1:36657")
	if got := cmtRPCAddr(); got != "tcp://127.0.0.1:36657" {
		t.Errorf("override cmtRPCAddr() = %q, want tcp://127.0.0.1:36657", got)
	}
}

// TestCmtRPCClientURL pins the listen-addr → dial-URL derivation: tcp:// becomes
// http://, and a 0.0.0.0 wildcard listen host is dialed as 127.0.0.1 (you can't
// reliably connect to the wildcard).
func TestCmtRPCClientURL(t *testing.T) {
	cases := []struct {
		env  string
		want string
	}{
		{"", "http://127.0.0.1:26657"},                      // default
		{"tcp://127.0.0.1:36657", "http://127.0.0.1:36657"}, // moved port
		{"tcp://0.0.0.0:26657", "http://127.0.0.1:26657"},   // wildcard → loopback dial
	}
	for _, c := range cases {
		t.Setenv("SAGE_CMT_RPC_ADDR", c.env)
		if got := cmtRPCClientURL(); got != c.want {
			t.Errorf("cmtRPCClientURL() with SAGE_CMT_RPC_ADDR=%q = %q, want %q", c.env, got, c.want)
		}
	}
}

// TestCmtP2PAddr pins the SAGE_CMT_P2P_ADDR override and that the per-mode
// fallback (loopback for personal, 0.0.0.0 for quorum) is preserved when the env
// is unset — defaults stay unchanged for both modes.
func TestCmtP2PAddr(t *testing.T) {
	t.Setenv("SAGE_CMT_P2P_ADDR", "")
	if got := cmtP2PAddr("tcp://127.0.0.1:26656"); got != "tcp://127.0.0.1:26656" {
		t.Errorf("personal default cmtP2PAddr() = %q, want tcp://127.0.0.1:26656", got)
	}
	if got := cmtP2PAddr("tcp://0.0.0.0:26656"); got != "tcp://0.0.0.0:26656" {
		t.Errorf("quorum default cmtP2PAddr() = %q, want tcp://0.0.0.0:26656", got)
	}
	t.Setenv("SAGE_CMT_P2P_ADDR", "tcp://127.0.0.1:36656")
	if got := cmtP2PAddr("tcp://0.0.0.0:26656"); got != "tcp://127.0.0.1:36656" {
		t.Errorf("override cmtP2PAddr() = %q, want tcp://127.0.0.1:36656", got)
	}
}
