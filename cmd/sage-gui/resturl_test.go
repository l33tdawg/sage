package main

import "testing"

// TestRestBaseURL pins the shared helper that seed.go, mcp_token.go and vault.go
// use to turn cfg.RESTAddr into a base URL. The host:port form (the shipped
// default, "127.0.0.1:8080") must NOT get "localhost" prepended — the bug that
// vault.go's old `"http://localhost" + cfg.RESTAddr` concat produced
// ("http://localhost127.0.0.1:8080", an unconnectable host). Only the bare
// ":port" form gets localhost filled in.
func TestRestBaseURL(t *testing.T) {
	cases := []struct {
		addr string
		want string
	}{
		{"127.0.0.1:8080", "http://127.0.0.1:8080"}, // shipped default — must stay host:port
		{":8080", "http://localhost:8080"},          // bare port — localhost filled in
		{"sage.internal:8080", "http://sage.internal:8080"},
		{"0.0.0.0:9090", "http://localhost:9090"},
		{"[::]:9091", "http://localhost:9091"},
	}
	for _, c := range cases {
		if got := restBaseURL(c.addr); got != c.want {
			t.Errorf("restBaseURL(%q) = %q, want %q", c.addr, got, c.want)
		}
	}
}
