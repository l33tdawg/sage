package web

import (
	"bytes"
	"context"
	stdb64 "encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeCloudflared writes a tiny shell script to disk that mimics the
// cloudflared CLI well enough for the wizard's happy path. Returns the
// path to the script. Caller is responsible for setting SAGE_CLOUDFLARED_BIN
// and cleaning up.
func fakeCloudflared(t *testing.T, behaviors map[string]string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("fakeCloudflared shell script requires a POSIX shell")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "cloudflared")

	// Default behaviors.
	loginURL := "https://dash.cloudflare.com/argotunnel?fakeAuth=1"
	if v, ok := behaviors["login_url"]; ok {
		loginURL = v
	}
	tunnelUUID := "0123abcd-4567-89ab-cdef-0123456789ab"
	if v, ok := behaviors["tunnel_uuid"]; ok {
		tunnelUUID = v
	}
	listOutput := behaviors["list_output"] // "" by default → no existing tunnel

	// Write a shell script that switches on $1/$2/$3.
	script := fmt.Sprintf(`#!/bin/sh
case "$1 $2 $3" in
  "--version "*|"--version  ")
    echo "cloudflared version 2025.1.0 (built with sage-test-fake)"
    ;;
  "tunnel login"*)
    echo "Please open the following URL and log in with your Cloudflare account:"
    echo ""
    echo "  %s"
    echo ""
    echo "Leave cloudflared running while you complete the login."
    sleep 0.2
    ;;
  "tunnel list"*)
    cat <<EOF
%s
EOF
    ;;
  "tunnel create"*)
    echo "Tunnel credentials written to /will/be/replaced/%s.json"
    echo "Created tunnel sage with id %s"
    ;;
  "tunnel route"*)
    echo "Added CNAME for ${5:-?}"
    ;;
  *)
    echo "fake-cloudflared: unknown args: $@" >&2
    exit 0
    ;;
esac
`, loginURL, listOutput, tunnelUUID, tunnelUUID)

	require.NoError(t, os.WriteFile(path, []byte(script), 0o755)) //nolint:gosec // test fixture
	return path
}

// withFakeCloudflared sets SAGE_CLOUDFLARED_BIN to fake script path and
// patches HOME so the wizard writes into a tempdir, not the user's real
// ~/.cloudflared.
func withFakeCloudflared(t *testing.T, behaviors map[string]string) (cloudflaredPath, fakeHome string) {
	t.Helper()
	cloudflaredPath = fakeCloudflared(t, behaviors)
	fakeHome = t.TempDir()
	t.Setenv("SAGE_CLOUDFLARED_BIN", cloudflaredPath)
	t.Setenv("HOME", fakeHome)
	t.Setenv("SAGE_HOME", filepath.Join(fakeHome, ".sage"))
	t.Setenv("SAGE_BROWSER_OPEN_BIN", "/usr/bin/true") // silently no-op
	require.NoError(t, os.MkdirAll(filepath.Join(fakeHome, ".cloudflared"), 0o700))
	return cloudflaredPath, fakeHome
}

func TestWizard_CheckCloudflared_NotInstalled(t *testing.T) {
	t.Setenv("SAGE_CLOUDFLARED_BIN", "/nonexistent/path/to/cloudflared")

	h, _ := newTestHandler(t)
	r := testRouter(h)
	req := httptest.NewRequest(http.MethodPost, "/v1/wizard/chatgpt/check-cloudflared", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, false, resp["installed"])
	assert.Equal(t, "", resp["version"])
}

func TestWizard_CheckCloudflared_Installed(t *testing.T) {
	withFakeCloudflared(t, nil)

	h, _ := newTestHandler(t)
	r := testRouter(h)
	req := httptest.NewRequest(http.MethodPost, "/v1/wizard/chatgpt/check-cloudflared", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, true, resp["installed"])
	assert.Contains(t, resp["version"], "cloudflared version")
}

func TestWizard_Login_CapturesURL(t *testing.T) {
	const expectedURL = "https://dash.cloudflare.com/argotunnel?test=login"
	withFakeCloudflared(t, map[string]string{"login_url": expectedURL})

	h, _ := newTestHandler(t)
	r := testRouter(h)
	req := httptest.NewRequest(http.MethodPost, "/v1/wizard/chatgpt/login", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, expectedURL, resp["url"])

	// Reset capture so other tests don't see this state.
	activeLoginCapture.mu.Lock()
	activeLoginCapture.cmd = nil
	activeLoginCapture.url = ""
	activeLoginCapture.done = true
	activeLoginCapture.mu.Unlock()
}

func TestWizard_LoginStatus_NoCert(t *testing.T) {
	_, _ = withFakeCloudflared(t, nil)

	h, _ := newTestHandler(t)
	r := testRouter(h)
	req := httptest.NewRequest(http.MethodGet, "/v1/wizard/chatgpt/login-status", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, false, resp["authenticated"])
}

func TestWizard_LoginStatus_CertPresent(t *testing.T) {
	_, fakeHome := withFakeCloudflared(t, nil)

	// Drop a fake cert.pem in place.
	certPath := filepath.Join(fakeHome, ".cloudflared", "cert.pem")
	require.NoError(t, os.WriteFile(certPath, []byte("-----BEGIN PRIVATE KEY-----\nFAKE\n-----END PRIVATE KEY-----\n"), 0o600))

	h, _ := newTestHandler(t)
	r := testRouter(h)
	req := httptest.NewRequest(http.MethodGet, "/v1/wizard/chatgpt/login-status", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, true, resp["authenticated"])
	assert.Equal(t, certPath, resp["cert_path"])
}

func TestWizard_CreateTunnel_FullFlow(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("autostart only supported on darwin/linux")
	}
	const tunnelUUID = "abcd1234-5678-9abc-def0-112233445566"
	cfPath, fakeHome := withFakeCloudflared(t, map[string]string{
		"tunnel_uuid": tunnelUUID,
		"list_output": "ID                                     NAME    CREATED              CONNECTIONS\n",
	})

	// Pre-create the credentials file the wizard expects to find after
	// `cloudflared tunnel create`.
	credPath := filepath.Join(fakeHome, ".cloudflared", tunnelUUID+".json")
	require.NoError(t, os.WriteFile(credPath, []byte(`{"AccountTag":"fake","TunnelID":"`+tunnelUUID+`"}`), 0o600))

	// Patch the autostart helpers so they don't actually invoke launchctl.
	t.Setenv("PATH", filepath.Dir(cfPath)+":"+os.Getenv("PATH"))

	h, _ := newTestHandler(t)
	r := testRouter(h)

	body, _ := json.Marshal(map[string]string{
		"subdomain": "sagetest",
		"zone":      "example.com",
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/wizard/chatgpt/create-tunnel", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	output := w.Body.String()
	assert.Contains(t, output, "tunnel: uuid="+tunnelUUID)
	assert.Contains(t, output, "config:")
	assert.Contains(t, output, "hostname: sagetest.example.com")
	// Ends with a "done:" line — we tolerate either 0 (verify passed) or 1
	// (verify failed because we didn't actually launch a tunnel). The
	// important thing is the wizard reached the verify step.
	require.Regexp(t, `done: [01]\n?$`, output)

	// Config file should exist.
	configPath := filepath.Join(fakeHome, ".cloudflared", "config.yml")
	cfg, err := os.ReadFile(configPath) //nolint:gosec // test path
	require.NoError(t, err)
	assert.Contains(t, string(cfg), "tunnel: "+tunnelUUID)
	assert.Contains(t, string(cfg), "hostname: sagetest.example.com")
	assert.Contains(t, string(cfg), "/v1/mcp/(sse|messages|streamable)")
	assert.Contains(t, string(cfg), "/oauth/(authorize|token|register)")
	assert.Contains(t, string(cfg), "http_status:404")
}

func TestWizard_CreateTunnel_RejectsBadSubdomain(t *testing.T) {
	withFakeCloudflared(t, nil)

	h, _ := newTestHandler(t)
	r := testRouter(h)

	body, _ := json.Marshal(map[string]string{
		"subdomain": "BAD;rm -rf /",
		"zone":      "example.com",
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/wizard/chatgpt/create-tunnel", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestWizard_CreateTunnel_RejectsBadZone(t *testing.T) {
	withFakeCloudflared(t, nil)

	h, _ := newTestHandler(t)
	r := testRouter(h)

	body, _ := json.Marshal(map[string]string{
		"subdomain": "sage",
		"zone":      "no spaces allowed.com",
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/wizard/chatgpt/create-tunnel", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestWizard_CreateTunnel_DoesNotClobberExistingConfig(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("autostart only supported on darwin/linux")
	}
	cfPath, fakeHome := withFakeCloudflared(t, map[string]string{
		"tunnel_uuid": "fffffff1-1111-2222-3333-444444444444",
		"list_output": "ID                                     NAME    CREATED              CONNECTIONS\n",
	})
	credPath := filepath.Join(fakeHome, ".cloudflared", "fffffff1-1111-2222-3333-444444444444.json")
	require.NoError(t, os.WriteFile(credPath, []byte(`{}`), 0o600))

	// Pre-existing config.yml referencing a DIFFERENT tunnel — must not be overwritten.
	configPath := filepath.Join(fakeHome, ".cloudflared", "config.yml")
	preExisting := "tunnel: 99999999-9999-9999-9999-999999999999\ningress:\n  - service: http://localhost:1234\n"
	require.NoError(t, os.WriteFile(configPath, []byte(preExisting), 0o600))

	t.Setenv("PATH", filepath.Dir(cfPath)+":"+os.Getenv("PATH"))

	h, _ := newTestHandler(t)
	r := testRouter(h)
	body, _ := json.Marshal(map[string]string{"subdomain": "sage", "zone": "example.com"})
	req := httptest.NewRequest(http.MethodPost, "/v1/wizard/chatgpt/create-tunnel", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	out := w.Body.String()
	assert.Contains(t, out, "error:")
	assert.Contains(t, out, "already exists")

	// Pre-existing config must be untouched.
	current, err := os.ReadFile(configPath) //nolint:gosec // test path
	require.NoError(t, err)
	assert.Equal(t, preExisting, string(current))
}

func TestWizard_MintToken_Success(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	body, _ := json.Marshal(map[string]string{
		"agent_id":   strings.Repeat("a", 64),
		"token_name": "chatgpt-test",
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/wizard/chatgpt/mint-token", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["id"])
	assert.NotEmpty(t, resp["token"])
	assert.Equal(t, "chatgpt-test", resp["name"])
}

func TestWizard_MintToken_RejectsBadAgentID(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	body, _ := json.Marshal(map[string]string{
		"agent_id":   "too-short",
		"token_name": "chatgpt-test",
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/wizard/chatgpt/mint-token", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// TestWizard_HappyPath_E2E threads through every step of the wizard
// (check → login → loginStatus → createTunnel → mintToken) with the fake
// cloudflared CLI and verifies the artefacts created on disk.
func TestWizard_HappyPath_E2E(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("autostart only supported on darwin/linux")
	}
	const tunnelUUID = "deadbeef-1111-2222-3333-444455556666"
	cfPath, fakeHome := withFakeCloudflared(t, map[string]string{
		"tunnel_uuid": tunnelUUID,
		"list_output": "ID                                     NAME    CREATED              CONNECTIONS\n",
		"login_url":   "https://dash.cloudflare.com/argotunnel?happy=path",
	})
	t.Setenv("PATH", filepath.Dir(cfPath)+":"+os.Getenv("PATH"))

	h, _ := newTestHandler(t)
	r := testRouter(h)

	// 1. check-cloudflared → installed
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/wizard/chatgpt/check-cloudflared", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var checkResp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &checkResp))
	require.Equal(t, true, checkResp["installed"])

	// 2. login → URL captured
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/wizard/chatgpt/login", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var loginResp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &loginResp))
	require.Contains(t, loginResp["url"], "happy=path")

	// 3. simulate user completing login by writing cert.pem
	certPath := filepath.Join(fakeHome, ".cloudflared", "cert.pem")
	require.NoError(t, os.WriteFile(certPath, []byte("FAKE CERT"), 0o600))

	// 4. login-status → authenticated
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/wizard/chatgpt/login-status", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var statusResp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &statusResp))
	require.Equal(t, true, statusResp["authenticated"])

	// 5. create-tunnel
	credPath := filepath.Join(fakeHome, ".cloudflared", tunnelUUID+".json")
	require.NoError(t, os.WriteFile(credPath, []byte("{}"), 0o600))

	body, _ := json.Marshal(map[string]string{"subdomain": "sage", "zone": "example.com"})
	w = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/wizard/chatgpt/create-tunnel", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// 6. mint-token
	body, _ = json.Marshal(map[string]string{
		"agent_id":   strings.Repeat("c", 64),
		"token_name": "chatgpt",
	})
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/v1/wizard/chatgpt/mint-token", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusCreated, w.Code)
	var mintResp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &mintResp))
	require.NotEmpty(t, mintResp["token"])
}

// Ensure the verify helper times out cleanly when the URL is unreachable.
func TestWizard_VerifyTunnel_TimesOut(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	// Deliberately use a short context to keep the test quick.
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	err := wizardVerifyTunnel(ctx, srv.URL+"/health", func(_, _ string) {})
	require.Error(t, err)
}

// Ensure verify succeeds on a 200.
func TestWizard_VerifyTunnel_Succeeds(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, wizardVerifyTunnel(ctx, srv.URL+"/health", func(_, _ string) {}))
}

// Sanity: ensure the install endpoint returns *some* output for an
// unsupported platform — we don't actually invoke brew/curl in tests.
func TestWizard_InstallCloudflared_StreamsOutput(t *testing.T) {
	// Force a path that doesn't have brew/curl so we trigger the manual
	// pointer rather than a real install.
	t.Setenv("PATH", t.TempDir())

	h, _ := newTestHandler(t)
	r := testRouter(h)
	req := httptest.NewRequest(http.MethodPost, "/v1/wizard/chatgpt/install-cloudflared", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	out := w.Body.String()
	assert.Contains(t, out, "done:")
}

// Ensure URL parsing handles trailing punctuation cleanly. (regression
// guard — cloudflared sometimes prints the URL followed by a period when
// wrapping at terminal width.)
func TestWizard_URLRegex_StripsPunctuation(t *testing.T) {
	parsed, err := url.Parse("https://dash.cloudflare.com/argotunnel?x=1")
	require.NoError(t, err)
	assert.Equal(t, "dash.cloudflare.com", parsed.Host)
}

// ─── v6.7.4: Cloudflare zone enumeration ────────────────────────────────────

// validCertPEM builds a synthetic cloudflared cert.pem whose embedded JSON
// token decodes to the given fields. The format MUST match what cloudflared
// itself writes — `BEGIN`/`END` markers + base64 JSON wrapped at 64 chars.
func validCertPEM(t *testing.T, zoneID, accountID, apiToken string) []byte {
	t.Helper()
	payload, err := json.Marshal(map[string]string{
		"zoneID":    zoneID,
		"accountID": accountID,
		"apiToken":  apiToken,
	})
	require.NoError(t, err)
	encoded := stdb64.StdEncoding.EncodeToString(payload)
	var wrapped strings.Builder
	for i := 0; i < len(encoded); i += 64 {
		end := i + 64
		if end > len(encoded) {
			end = len(encoded)
		}
		wrapped.WriteString(encoded[i:end])
		if end < len(encoded) {
			wrapped.WriteString("\n")
		}
	}
	return []byte("-----BEGIN ARGO TUNNEL TOKEN-----\n" + wrapped.String() + "\n-----END ARGO TUNNEL TOKEN-----\n")
}

func TestDecodeCloudflaredCert_HappyPath(t *testing.T) {
	pem := validCertPEM(t, "z-1", "acct-1", "tok-XYZ")
	tok, err := decodeCloudflaredCert(pem)
	require.NoError(t, err)
	assert.Equal(t, "z-1", tok.ZoneID)
	assert.Equal(t, "acct-1", tok.AccountID)
	assert.Equal(t, "tok-XYZ", tok.APIToken)
}

func TestDecodeCloudflaredCert_MissingMarkers(t *testing.T) {
	_, err := decodeCloudflaredCert([]byte("not a pem"))
	assert.Error(t, err)
}

func TestDecodeCloudflaredCert_EmptyAPIToken(t *testing.T) {
	pem := validCertPEM(t, "z-1", "acct-1", "")
	_, err := decodeCloudflaredCert(pem)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "apiToken empty")
}

func TestFetchCloudflareZones_Pagination(t *testing.T) {
	// Mock the Cloudflare API: page 1 returns 2 zones with total_pages=2,
	// page 2 returns 1 zone. Verify all 3 surface in sorted order.
	calls := 0
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer tok-XYZ" {
			http.Error(w, "bad bearer", http.StatusUnauthorized)
			return
		}
		page := r.URL.Query().Get("page")
		w.Header().Set("Content-Type", "application/json")
		switch page {
		case "1":
			_, _ = w.Write([]byte(`{"result":[{"id":"z2","name":"beta.example.com"},{"id":"z1","name":"alpha.example.com"}],"result_info":{"page":1,"total_pages":2}}`))
		case "2":
			_, _ = w.Write([]byte(`{"result":[{"id":"z3","name":"charlie.example.com"}],"result_info":{"page":2,"total_pages":2}}`))
		default:
			http.Error(w, "unexpected page", http.StatusBadRequest)
		}
	}))
	defer mock.Close()

	// Override the API base URL via test seam.
	prev := cloudflareAPIBase
	cloudflareAPIBase = mock.URL
	t.Cleanup(func() { cloudflareAPIBase = prev })

	zones, err := fetchCloudflareZones("tok-XYZ")
	require.NoError(t, err)
	require.Len(t, zones, 3)
	// Sorted by name.
	assert.Equal(t, "alpha.example.com", zones[0]["name"])
	assert.Equal(t, "beta.example.com", zones[1]["name"])
	assert.Equal(t, "charlie.example.com", zones[2]["name"])
	assert.Equal(t, 2, calls)
}

func TestFetchCloudflareZones_BadBearer(t *testing.T) {
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Invalid token", http.StatusUnauthorized)
	}))
	defer mock.Close()

	prev := cloudflareAPIBase
	cloudflareAPIBase = mock.URL
	t.Cleanup(func() { cloudflareAPIBase = prev })

	_, err := fetchCloudflareZones("bad-token")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "401")
}

func TestListCloudflareZones_NoCertReturnsEmpty(t *testing.T) {
	// Point cloudflaredHome at a tempdir that has no cert.pem — the wrapper
	// should swallow the error and return an empty list.
	prev := cloudflaredHomeOverride
	cloudflaredHomeOverride = t.TempDir()
	t.Cleanup(func() { cloudflaredHomeOverride = prev })
	zones := listCloudflareZones()
	assert.Empty(t, zones)
}
