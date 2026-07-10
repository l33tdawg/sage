package netguard

import "testing"

func TestLocalLANHTTPBase(t *testing.T) {
	cases := []struct {
		name string
		in   string
		ok   bool
	}{
		{"loopback", "http://127.0.0.1:8080", true},
		{"localhost", "http://localhost:8080", true},
		{"private v4", "https://192.168.1.10:8443", true},
		{"ula v6", "https://[fd00::1]:8443", true},
		{"public metadata host", "http://169.254.169.254:80", false},
		{"public ip", "https://8.8.8.8:443", false},
		{"dns name", "https://example.com:443", false},
		{"userinfo", "http://u:p@127.0.0.1:8080", false},
		{"query", "http://127.0.0.1:8080?a=b", false},
		{"wrong scheme", "ftp://127.0.0.1:21", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LocalLANHTTPBase(tc.in, "http", "https")
			if (err == nil) != tc.ok {
				t.Fatalf("LocalLANHTTPBase(%q) err=%v, want ok=%v", tc.in, err, tc.ok)
			}
		})
	}
}

func TestLocalLANHostPort(t *testing.T) {
	if _, err := LocalLANHostPort("192.168.1.10:8080"); err != nil {
		t.Fatalf("private host:port rejected: %v", err)
	}
	if _, err := LocalLANHostPort("8.8.8.8:8080"); err == nil {
		t.Fatal("public host:port accepted")
	}
}

func TestJoinPath(t *testing.T) {
	cases := []struct {
		name string
		path string
		want string // expected full URL when ok; empty means an error is expected
	}{
		{"plain path", "/fed/v1/join/ca", "https://192.168.1.10:8444/fed/v1/join/ca"},
		// Regression: the join status poll carries a query string. Building the
		// ref via url.URL{Path: ...} percent-escaped the '?' and 404'd every
		// guest status poll (the v11.4.8/9 stuck-at-2-of-2 ceremony bug).
		{"query survives", "/fed/v1/join/status?session_id=ABC123", "https://192.168.1.10:8444/fed/v1/join/status?session_id=ABC123"},
		{"relative rejected", "fed/v1/join/ca", ""},
		{"nul rejected", "/fed\x00", ""},
		{"protocol-relative rejected", "//evil.example/fed", ""},
		{"scheme smuggle rejected", "/..//evil.example\x00", ""},
		{"fragment rejected", "/fed/v1/join/ca#frag", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := JoinPath("https://192.168.1.10:8444", tc.path)
			if tc.want == "" {
				if err == nil {
					t.Fatalf("JoinPath(%q) = %q, want error", tc.path, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("JoinPath(%q): %v", tc.path, err)
			}
			if got != tc.want {
				t.Fatalf("JoinPath(%q) = %q, want %q", tc.path, got, tc.want)
			}
		})
	}
}
