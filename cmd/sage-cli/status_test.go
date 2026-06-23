package main

import (
	"reflect"
	"testing"
)

// TestStatusRPCURLs pins the SAGE_CMT_RPC_ADDR behavior of `sage-cli status`:
// unset preserves the historical scan of the local quorum dev cluster; a set
// value collapses to that one moved endpoint with the same tcp://→http:// and
// 0.0.0.0→127.0.0.1 derivation cmd/sage-gui uses.
func TestStatusRPCURLs(t *testing.T) {
	t.Setenv("SAGE_CMT_RPC_ADDR", "")
	want := []string{
		"http://localhost:26657/status",
		"http://localhost:26757/status",
		"http://localhost:26857/status",
		"http://localhost:26957/status",
	}
	if got := statusRPCURLs(); !reflect.DeepEqual(got, want) {
		t.Errorf("default statusRPCURLs() = %v, want %v", got, want)
	}

	cases := []struct {
		env  string
		want string
	}{
		{"tcp://127.0.0.1:36657", "http://127.0.0.1:36657/status"}, // moved port
		{"tcp://0.0.0.0:26657", "http://127.0.0.1:26657/status"},   // wildcard → loopback dial
	}
	for _, c := range cases {
		t.Setenv("SAGE_CMT_RPC_ADDR", c.env)
		got := statusRPCURLs()
		if len(got) != 1 || got[0] != c.want {
			t.Errorf("statusRPCURLs() with SAGE_CMT_RPC_ADDR=%q = %v, want [%q]", c.env, got, c.want)
		}
	}
}
