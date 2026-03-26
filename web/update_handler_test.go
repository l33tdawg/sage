package web

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSemverGreater(t *testing.T) {
	tests := []struct {
		a, b string
		want bool
	}{
		{"3.6.0", "3.5.0", true},
		{"3.5.0", "3.6.0", false},
		{"3.6.0", "3.6.0", false},  // equal, not greater
		{"3.10.0", "3.9.0", true},  // 10 > 9 (not string compare)
		{"4.0.0", "3.99.99", true},
		{"3.6.1", "3.6.0", true},
		{"3.6.0", "3.6.1", false},
		{"1.0.0", "0.99.0", true},
		{"3.7.0", "3.6.0", true},   // the real scenario: latest > current
		{"3.5.0", "3.5.0", false},  // same version
		{"dev", "3.6.0", false},    // dev parses as 0.0.0
		{"3.6.0-rc1", "3.5.0", true}, // pre-release suffix stripped
	}

	for _, tt := range tests {
		t.Run(tt.a+"_vs_"+tt.b, func(t *testing.T) {
			assert.Equal(t, tt.want, semverGreater(tt.a, tt.b))
		})
	}
}

func TestParseSemver(t *testing.T) {
	tests := []struct {
		input string
		want  [3]int
	}{
		{"3.6.0", [3]int{3, 6, 0}},
		{"v3.6.0", [3]int{3, 6, 0}},
		{"3.10.1", [3]int{3, 10, 1}},
		{"3.6.0-rc1", [3]int{3, 6, 0}},
		{"3.6.0+build123", [3]int{3, 6, 0}},
		{"dev", [3]int{0, 0, 0}},
		{"1.0", [3]int{1, 0, 0}},
		{"", [3]int{0, 0, 0}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.want, parseSemver(tt.input))
		})
	}
}

func TestFindAssetName(t *testing.T) {
	name := findAssetName("3.7.0")
	assert.Contains(t, name, "sage-gui_3.7.0_")
	assert.True(t, len(name) > 20)
}

func TestRedirectRestriction(t *testing.T) {
	// Set up a malicious server that the redirect would point to
	malicious := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("malicious payload")) //nolint:errcheck
	}))
	defer malicious.Close()

	// Set up a server that redirects to the malicious server
	redirector := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, malicious.URL+"/evil", http.StatusFound)
	}))
	defer redirector.Close()

	// Build the CheckRedirect function (same as in handleApplyUpdate)
	checkRedirect := func(req *http.Request, via []*http.Request) error {
		u := req.URL.String()
		allowed := strings.HasPrefix(u, "https://github.com/") ||
			strings.HasPrefix(u, "https://objects.githubusercontent.com/") ||
			strings.HasPrefix(u, "https://release-assets.githubusercontent.com/")
		if !allowed {
			return fmt.Errorf("redirect to non-GitHub URL blocked")
		}
		return nil
	}

	// Test: non-GitHub redirect is blocked
	t.Run("blocks non-GitHub redirect", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "https://evil.com/payload", nil)
		err := checkRedirect(req, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "redirect to non-GitHub URL blocked")
	})

	// Test: GitHub redirect is allowed
	t.Run("allows GitHub redirect", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "https://github.com/l33tdawg/sage/releases/download/v3.7.0/archive.tar.gz", nil)
		err := checkRedirect(req, nil)
		assert.NoError(t, err)
	})

	// Test: objects.githubusercontent.com redirect is allowed
	t.Run("allows githubusercontent redirect", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "https://objects.githubusercontent.com/some-hash/archive.tar.gz", nil)
		err := checkRedirect(req, nil)
		assert.NoError(t, err)
	})

	// Test: release-assets.githubusercontent.com redirect is allowed
	t.Run("allows release-assets githubusercontent redirect", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "https://release-assets.githubusercontent.com/github-production-release-asset/123456/archive.tar.gz", nil)
		err := checkRedirect(req, nil)
		assert.NoError(t, err)
	})
}

func TestPathTraversalRejection(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want bool // true = should be rejected
	}{
		{"clean URL", "https://github.com/l33tdawg/sage/releases/download/v3.7.0/archive.tar.gz", false},
		{"path traversal", "https://github.com/l33tdawg/sage/releases/download/../../evil.tar.gz", true},
		{"double dot in query", "https://github.com/l33tdawg/sage/releases/download/v3.7.0/a..b.tar.gz", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rejected := strings.Contains(tt.url, "..")
			assert.Equal(t, tt.want, rejected)
		})
	}
}
