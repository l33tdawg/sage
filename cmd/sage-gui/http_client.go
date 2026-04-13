package main

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/tlsca"
)

// tlsAwareClient returns an *http.Client that handles https:// URLs gracefully.
// For plain http:// URLs, it returns http.DefaultClient (no overhead).
// For https:// URLs, it tries to load the SAGE quorum CA from ~/.sage/certs/,
// falling back to system CAs if the certs directory doesn't exist or isn't populated.
func tlsAwareClient(baseURL string) *http.Client {
	if !strings.HasPrefix(baseURL, "https://") {
		return http.DefaultClient
	}

	// Try SAGE_CA_CERT env var first (explicit CA path).
	if caPath := os.Getenv("SAGE_CA_CERT"); caPath != "" {
		tlsCfg, err := tlsca.ClientTLSConfigFromCA(caPath)
		if err == nil {
			return &http.Client{
				Timeout:   30 * time.Second,
				Transport: &http.Transport{TLSClientConfig: tlsCfg},
			}
		}
		fmt.Fprintf(os.Stderr, "SAGE: SAGE_CA_CERT=%s failed to load: %v (falling back to certs dir)\n", caPath, err)
	}

	// Try certs directory (~/.sage/certs/).
	certsDir := filepath.Join(SageHome(), "certs")
	tlsCfg, err := tlsca.ClientTLSConfig(certsDir)
	if err == nil {
		return &http.Client{
			Timeout:   30 * time.Second,
			Transport: &http.Transport{TLSClientConfig: tlsCfg},
		}
	}

	// Fall back to system CAs — works with properly-signed certs (e.g. Let's Encrypt).
	return &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13}},
	}
}
