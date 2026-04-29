package web

// ChatGPT setup wizard — orchestrates the cloudflared install/login/tunnel
// dance from CEREBRUM so non-power-users can wire SAGE up to ChatGPT's MCP
// connector without touching a terminal.
//
// Philosophy: this wizard is **local-first orchestration**, not a managed
// service. The user owns the cloudflared tunnel, the Cloudflare account,
// and the domain. SAGE never proxies through anyone's cloud — chatgpt.com
// hits the user's domain → Cloudflare's edge → user-owned tunnel → user's
// localhost. We just collapse the 9-step manual setup into a guided UI.
//
// All endpoints sit under /v1/wizard/chatgpt/ and are gated by the existing
// dashboard auth middleware (cookie session when encryption is on, else
// no-auth same as the rest of CEREBRUM).
//
// Subprocess inputs (subdomain, zone names) are sanitized with strict
// allowlists before being passed to exec.Command. Env-var override
// `SAGE_CLOUDFLARED_BIN` is honored so tests can drop in a fake cloudflared
// CLI without touching $PATH.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"text/template"
	"time"
)

// cloudflaredBin returns the path/name of the cloudflared binary to invoke.
// Honors SAGE_CLOUDFLARED_BIN for tests; otherwise relies on $PATH lookup.
func cloudflaredBin() string {
	if v := strings.TrimSpace(os.Getenv("SAGE_CLOUDFLARED_BIN")); v != "" {
		return v
	}
	return "cloudflared"
}

// browserOpenBin returns the OS-specific binary used to open URLs in the
// user's default browser. Tests override via SAGE_BROWSER_OPEN_BIN.
func browserOpenBin() (string, []string) {
	if v := strings.TrimSpace(os.Getenv("SAGE_BROWSER_OPEN_BIN")); v != "" {
		return v, nil
	}
	switch runtime.GOOS {
	case "darwin":
		return "open", nil
	case "windows":
		return "rundll32", []string{"url.dll,FileProtocolHandler"}
	default:
		return "xdg-open", nil
	}
}

// cloudflaredHome is where cloudflared stores cert.pem, tunnel credentials,
// and config.yml. Always ~/.cloudflared/ — that's what cloudflared itself
// uses regardless of platform. Tests override via cloudflaredHomeOverride.
func cloudflaredHome() string {
	if cloudflaredHomeOverride != "" {
		return cloudflaredHomeOverride
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".cloudflared")
}

// Test seams — set by wizard_chatgpt_test.go to point at fixtures, NEVER set
// in production code paths.
var (
	cloudflaredHomeOverride string
	cloudflareAPIBase       = "https://api.cloudflare.com"
)

// loginCapture holds the in-flight `cloudflared tunnel login` subprocess
// state. Created on POST /login, cleared once cert.pem materializes.
type loginCapture struct {
	mu      sync.Mutex
	cmd     *exec.Cmd
	url     string
	started time.Time
	done    bool
	err     string
}

// activeLoginCapture is shared across the wizard endpoints — only one
// login flow can be active at a time per node.
var activeLoginCapture = &loginCapture{}

// validSubdomainRe restricts subdomains to RFC-1035-ish: lowercase letters,
// digits, hyphens, length 1-63. Hyphens cannot be leading/trailing.
var validSubdomainRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// validZoneRe restricts zone names to dotted hostnames of [a-z0-9-]+ labels.
var validZoneRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$`)

// validTunnelUUIDRe matches Cloudflare tunnel UUIDs (lowercase 8-4-4-4-12).
// Used both as a strict validator (against full strings via MatchString) and
// as a parser to extract a UUID embedded in cloudflared CLI output via
// FindString — the regex compiles without anchors, then helpers wrap it
// with anchors when strict validation is needed.
var validTunnelUUIDRe = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
var validTunnelUUIDFullRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// RegisterChatGPTWizardRoutes wires the wizard endpoints onto r. Must be
// called inside the auth-protected route group so unauthenticated callers
// can't trigger subprocess execution.
func (h *DashboardHandler) RegisterChatGPTWizardRoutes(r interface {
	Post(pattern string, handlerFn http.HandlerFunc)
	Get(pattern string, handlerFn http.HandlerFunc)
}) {
	r.Post("/v1/wizard/chatgpt/check-cloudflared", h.handleWizardCheckCloudflared)
	r.Post("/v1/wizard/chatgpt/install-cloudflared", h.handleWizardInstallCloudflared)
	r.Post("/v1/wizard/chatgpt/login", h.handleWizardLogin)
	r.Get("/v1/wizard/chatgpt/login-status", h.handleWizardLoginStatus)
	r.Post("/v1/wizard/chatgpt/create-tunnel", h.handleWizardCreateTunnel)
	r.Post("/v1/wizard/chatgpt/mint-token", h.handleWizardMintToken)
}

// ─── /check-cloudflared ──────────────────────────────────────────────────

// handleWizardCheckCloudflared reports whether `cloudflared` is on $PATH and
// its version string if so. Returns {installed: bool, version: string}.
func (h *DashboardHandler) handleWizardCheckCloudflared(w http.ResponseWriter, r *http.Request) {
	bin := cloudflaredBin()
	if _, err := exec.LookPath(bin); err != nil {
		writeJSONResp(w, http.StatusOK, map[string]any{
			"installed": false,
			"version":   "",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	out, _ := exec.CommandContext(ctx, bin, "--version").CombinedOutput() //nolint:gosec // bin is from env or literal "cloudflared"
	version := strings.TrimSpace(string(out))

	writeJSONResp(w, http.StatusOK, map[string]any{
		"installed": true,
		"version":   version,
	})
}

// ─── /install-cloudflared ────────────────────────────────────────────────

// handleWizardInstallCloudflared installs cloudflared via the platform's
// native package manager and streams stdout/stderr to the client via a
// chunked text response. We use plain text + flush rather than full SSE
// because the wizard's frontend reads .body as a stream — simpler than
// EventSource and works fine for this one-shot install.
func (h *DashboardHandler) handleWizardInstallCloudflared(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)

	writeLine := func(prefix, msg string) {
		_, _ = fmt.Fprintf(w, "%s: %s\n", prefix, msg)
		flusher.Flush()
	}

	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		// Homebrew is the canonical macOS path. If brew isn't there, give a
		// clear pointer rather than trying to compete with `installer`.
		if _, err := exec.LookPath("brew"); err != nil {
			writeLine("error", "Homebrew not found. Install it from https://brew.sh and re-run this step, or install cloudflared manually from https://github.com/cloudflare/cloudflared/releases")
			writeLine("done", "1")
			return
		}
		cmd = exec.CommandContext(r.Context(), "brew", "install", "cloudflared") //nolint:gosec // literal args
	case "linux":
		// On Debian/Ubuntu the official package URL is a moving target; safest
		// minimal install is the static binary into ~/.local/bin so we don't
		// require sudo. The user can later move it to /usr/local/bin if they
		// prefer a system-wide install.
		home, _ := os.UserHomeDir()
		dst := filepath.Join(home, ".local", "bin", "cloudflared")
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			writeLine("error", "create ~/.local/bin: "+err.Error())
			writeLine("done", "1")
			return
		}
		// Detect arch — cloudflared releases are tagged amd64/arm64/arm
		arch := runtime.GOARCH
		switch arch {
		case "amd64", "arm64":
		case "arm":
			arch = "arm"
		default:
			writeLine("error", "unsupported linux arch: "+arch+" (manual install: https://github.com/cloudflare/cloudflared/releases)")
			writeLine("done", "1")
			return
		}
		url := fmt.Sprintf("https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-%s", arch)
		cmd = exec.CommandContext(r.Context(), "curl", "-fsSL", "-o", dst, url) //nolint:gosec // url built from constant + GOARCH whitelist
	default:
		writeLine("error", "automatic install not supported on "+runtime.GOOS+" — install manually from https://github.com/cloudflare/cloudflared/releases")
		writeLine("done", "1")
		return
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		writeLine("error", "open stdout: "+err.Error())
		writeLine("done", "1")
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		writeLine("error", "open stderr: "+err.Error())
		writeLine("done", "1")
		return
	}

	if err := cmd.Start(); err != nil {
		writeLine("error", "start install: "+err.Error())
		writeLine("done", "1")
		return
	}

	streamCopy := func(label string, rd io.Reader) {
		buf := make([]byte, 4096)
		for {
			n, err := rd.Read(buf)
			if n > 0 {
				lines := strings.Split(strings.TrimRight(string(buf[:n]), "\n"), "\n")
				for _, line := range lines {
					if line != "" {
						writeLine(label, line)
					}
				}
			}
			if err != nil {
				return
			}
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); streamCopy("out", stdout) }()
	go func() { defer wg.Done(); streamCopy("err", stderr) }()
	wg.Wait()

	exitCode := 0
	if err := cmd.Wait(); err != nil {
		exitCode = 1
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			exitCode = ee.ExitCode()
		}
		writeLine("err", err.Error())
	}

	// On linux, chmod +x the downloaded binary.
	if runtime.GOOS == "linux" && exitCode == 0 {
		home, _ := os.UserHomeDir()
		dst := filepath.Join(home, ".local", "bin", "cloudflared")
		if cerr := os.Chmod(dst, 0o755); cerr != nil { //nolint:gosec // server-controlled path
			writeLine("err", "chmod +x: "+cerr.Error())
		}
	}

	writeLine("done", fmt.Sprintf("%d", exitCode))
}

// ─── /login ──────────────────────────────────────────────────────────────

// handleWizardLogin starts `cloudflared tunnel login` and captures the URL
// it prints to stdout. Returns the URL so the frontend can open it (we also
// fire `open` / `xdg-open` in the background on the local machine).
func (h *DashboardHandler) handleWizardLogin(w http.ResponseWriter, r *http.Request) {
	activeLoginCapture.mu.Lock()
	defer activeLoginCapture.mu.Unlock()

	// Idempotent: if an existing capture is still pending and recent, return
	// its URL rather than launching another cloudflared process.
	if activeLoginCapture.cmd != nil && !activeLoginCapture.done && time.Since(activeLoginCapture.started) < 5*time.Minute {
		writeJSONResp(w, http.StatusOK, map[string]any{
			"url":     activeLoginCapture.url,
			"started": activeLoginCapture.started.Format(time.RFC3339),
		})
		return
	}

	// Reset for a fresh attempt.
	activeLoginCapture.cmd = nil
	activeLoginCapture.url = ""
	activeLoginCapture.done = false
	activeLoginCapture.err = ""
	activeLoginCapture.started = time.Now()

	bin := cloudflaredBin()
	if _, err := exec.LookPath(bin); err != nil {
		writeError(w, http.StatusBadRequest, "cloudflared not on PATH — run /install-cloudflared first")
		return
	}

	// We deliberately use a background context here because cloudflared's
	// `tunnel login` is a long-running interactive process — the request
	// context is short-lived (HTTP roundtrip) but the cloudflared subprocess
	// must outlive it so the URL stays valid until the user completes the
	// browser flow. The login-status polling endpoint observes completion
	// via the cert.pem watcher, not subprocess wait.
	cmd := exec.CommandContext(context.Background(), bin, "tunnel", "login") //nolint:gosec // bin from env or literal
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "open stdout: "+err.Error())
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "open stderr: "+err.Error())
		return
	}
	if err := cmd.Start(); err != nil {
		writeError(w, http.StatusInternalServerError, "start cloudflared: "+err.Error())
		return
	}
	activeLoginCapture.cmd = cmd

	// URL extraction — cloudflared prints something like:
	//   "Please open the following URL and log in with your Cloudflare account:
	//    https://dash.cloudflare.com/argotunnel?..."
	urlRe := regexp.MustCompile(`https?://[^\s]+`)
	urlCh := make(chan string, 1)

	scan := func(rd io.Reader) {
		buf := make([]byte, 4096)
		for {
			n, err := rd.Read(buf)
			if n > 0 {
				if match := urlRe.Find(buf[:n]); match != nil {
					select {
					case urlCh <- string(match):
					default:
					}
				}
			}
			if err != nil {
				return
			}
		}
	}
	go scan(stdout)
	go scan(stderr)

	// Wait up to 10s for the URL to appear.
	select {
	case loginURL := <-urlCh:
		activeLoginCapture.url = loginURL
		// Best-effort browser open. Background ctx because the open command
		// is fire-and-forget — the user's browser must outlive this request.
		go func() {
			openBin, openArgs := browserOpenBin()
			args := append([]string{}, openArgs...)
			args = append(args, loginURL)
			_ = exec.CommandContext(context.Background(), openBin, args...).Start() //nolint:gosec,noctx // openBin from env or platform constant; fire-and-forget
		}()
		writeJSONResp(w, http.StatusOK, map[string]any{
			"url":     loginURL,
			"started": activeLoginCapture.started.Format(time.RFC3339),
		})
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		activeLoginCapture.err = "timed out waiting for cloudflared to print login URL"
		writeError(w, http.StatusGatewayTimeout, activeLoginCapture.err)
	}
}

// handleWizardLoginStatus polls for the existence of ~/.cloudflared/cert.pem
// and, once present, returns the parsed account/zone info from Cloudflare.
func (h *DashboardHandler) handleWizardLoginStatus(w http.ResponseWriter, r *http.Request) {
	certPath := filepath.Join(cloudflaredHome(), "cert.pem")
	info, err := os.Stat(certPath)
	if err != nil {
		writeJSONResp(w, http.StatusOK, map[string]any{
			"authenticated": false,
		})
		return
	}

	// Read cert.pem — it embeds the API token. We don't decode it here
	// (cloudflared's binary format is private), instead we just confirm
	// presence and return the path so the frontend knows step 3 is done.
	resp := map[string]any{
		"authenticated": true,
		"cert_path":     certPath,
		"cert_size":     info.Size(),
		"cert_modified": info.ModTime().Format(time.RFC3339),
		"zones":         listCloudflareZones(),
	}
	writeJSONResp(w, http.StatusOK, resp)
}

// listCloudflareZones decodes the apiToken embedded in cert.pem and calls the
// Cloudflare API to enumerate zones the user has access to, so the wizard can
// render a dropdown instead of a free-text input.
//
// cert.pem layout:
//
//	-----BEGIN ARGO TUNNEL TOKEN-----
//	<base64-encoded JSON: {"zoneID":"…","accountID":"…","apiToken":"…"}>
//	-----END ARGO TUNNEL TOKEN-----
//
// The apiToken is a Cloudflare-issued bearer scoped to tunnel + DNS operations
// for the user's zones — exactly what we need. Failure modes return an empty
// list so the frontend falls back to the manual-entry input.
func listCloudflareZones() []map[string]string {
	certPath := filepath.Join(cloudflaredHome(), "cert.pem")
	pem, err := os.ReadFile(certPath)
	if err != nil {
		return []map[string]string{}
	}
	tok, err := decodeCloudflaredCert(pem)
	if err != nil {
		return []map[string]string{}
	}
	zones, err := fetchCloudflareZones(tok.APIToken)
	if err != nil {
		return []map[string]string{}
	}
	return zones
}

// cloudflaredCertToken is the JSON payload embedded in cert.pem.
type cloudflaredCertToken struct {
	ZoneID    string `json:"zoneID"`
	AccountID string `json:"accountID"`
	APIToken  string `json:"apiToken"`
}

// decodeCloudflaredCert extracts and decodes the JSON token from a PEM-wrapped
// cloudflared cert. Tolerant of leading/trailing whitespace and base64 padding
// drift (older cloudflared versions sometimes drop the `=` padding).
func decodeCloudflaredCert(pem []byte) (cloudflaredCertToken, error) {
	const begin = "-----BEGIN ARGO TUNNEL TOKEN-----"
	const end = "-----END ARGO TUNNEL TOKEN-----"
	s := string(pem)
	bi := strings.Index(s, begin)
	if bi < 0 {
		return cloudflaredCertToken{}, errors.New("cert.pem: BEGIN ARGO TUNNEL TOKEN marker missing")
	}
	rest := s[bi+len(begin):]
	ei := strings.Index(rest, end)
	if ei < 0 {
		return cloudflaredCertToken{}, errors.New("cert.pem: END ARGO TUNNEL TOKEN marker missing")
	}
	b64 := strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == ' ' || r == '\t' {
			return -1
		}
		return r
	}, rest[:ei])
	// Pad to a multiple of 4 chars — base64 may be unpadded.
	if pad := len(b64) % 4; pad != 0 {
		b64 += strings.Repeat("=", 4-pad)
	}
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return cloudflaredCertToken{}, fmt.Errorf("cert.pem: base64 decode: %w", err)
	}
	var tok cloudflaredCertToken
	if jerr := json.Unmarshal(raw, &tok); jerr != nil {
		return cloudflaredCertToken{}, fmt.Errorf("cert.pem: json decode: %w", jerr)
	}
	if tok.APIToken == "" {
		return cloudflaredCertToken{}, errors.New("cert.pem: apiToken empty")
	}
	return tok, nil
}

// fetchCloudflareZones pages through GET /client/v4/zones with the given
// bearer and returns each active zone as {id, name}. Returns up to ~200 zones
// (4 pages × 50). Network/API errors surface to the caller; the public
// listCloudflareZones helper swallows them so the wizard can degrade.
func fetchCloudflareZones(apiToken string) ([]map[string]string, error) {
	httpClient := &http.Client{Timeout: 10 * time.Second}
	out := []map[string]string{}
	for page := 1; page <= 4; page++ {
		url := fmt.Sprintf("%s/client/v4/zones?per_page=50&page=%d&status=active", cloudflareAPIBase, page)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			cancel()
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiToken)
		req.Header.Set("Accept", "application/json")
		resp, err := httpClient.Do(req)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("cloudflare api call: %w", err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		cancel()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("cloudflare api status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		var page1 struct {
			Result []struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"result"`
			ResultInfo struct {
				Page       int `json:"page"`
				TotalPages int `json:"total_pages"`
			} `json:"result_info"`
		}
		if jerr := json.Unmarshal(body, &page1); jerr != nil {
			return nil, fmt.Errorf("cloudflare api json: %w", jerr)
		}
		for _, z := range page1.Result {
			out = append(out, map[string]string{"id": z.ID, "name": z.Name})
		}
		if page1.ResultInfo.Page >= page1.ResultInfo.TotalPages {
			break
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i]["name"] < out[j]["name"] })
	return out, nil
}

// ─── /create-tunnel ──────────────────────────────────────────────────────

// handleWizardCreateTunnel runs:
//   1. cloudflared tunnel create sage   (idempotent — uses existing if present)
//   2. cloudflared tunnel route dns sage <hostname>
//   3. write ~/.cloudflared/config.yml with path-restricted ingress
//   4. install launchd plist (macOS) or systemd user unit (linux)
//   5. verify tunnel reachable via HTTPS health probe
// Streams progress to the frontend as text/plain "step: msg" lines.
//
// SAFETY: when a config.yml or tunnel named "sage" already exists, we DO NOT
// overwrite it — the wizard returns the existing values and the user is
// shown a warning in the UI. This protects power users (Dhillon) who already
// hand-crafted their config.
func (h *DashboardHandler) handleWizardCreateTunnel(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Subdomain string `json:"subdomain"`
		Zone      string `json:"zone"`
		Hostname  string `json:"hostname"` // optional — overrides sub.zone if provided
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	subdomain := strings.ToLower(strings.TrimSpace(req.Subdomain))
	zone := strings.ToLower(strings.TrimSpace(req.Zone))
	if subdomain == "" {
		subdomain = "sage"
	}
	if !validSubdomainRe.MatchString(subdomain) {
		writeError(w, http.StatusBadRequest, "subdomain must be RFC-1035 (lowercase alphanumeric + hyphens, 1-63 chars)")
		return
	}
	if !validZoneRe.MatchString(zone) {
		writeError(w, http.StatusBadRequest, "zone must be a dotted hostname (e.g. example.com)")
		return
	}
	hostname := req.Hostname
	if hostname == "" {
		hostname = subdomain + "." + zone
	}
	if !validZoneRe.MatchString(strings.ToLower(hostname)) {
		writeError(w, http.StatusBadRequest, "hostname not a valid dotted hostname")
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)

	writeLine := func(step, msg string) {
		_, _ = fmt.Fprintf(w, "%s: %s\n", step, msg)
		flusher.Flush()
	}

	bin := cloudflaredBin()
	if _, err := exec.LookPath(bin); err != nil {
		writeLine("error", "cloudflared not on PATH")
		writeLine("done", "1")
		return
	}

	// Step 1: create or reuse tunnel.
	tunnelUUID, credPath, err := wizardCreateOrReuseTunnel(r.Context(), bin, "sage", writeLine)
	if err != nil {
		writeLine("error", err.Error())
		writeLine("done", "1")
		return
	}
	writeLine("tunnel", fmt.Sprintf("uuid=%s", tunnelUUID))

	// Step 2: route DNS.
	if rerr := wizardRouteDNS(r.Context(), bin, "sage", hostname, writeLine); rerr != nil {
		writeLine("error", rerr.Error())
		writeLine("done", "1")
		return
	}

	// Step 3: write config.yml — only if we don't see an existing one for a
	// different tunnel UUID. If the user already has a config that points at
	// a different tunnel, we surface that and bail rather than clobber.
	configPath := filepath.Join(cloudflaredHome(), "config.yml")
	if existing, _ := os.ReadFile(configPath); len(existing) > 0 { //nolint:gosec // path under user's home
		if !strings.Contains(string(existing), tunnelUUID) {
			writeLine("error", fmt.Sprintf("~/.cloudflared/config.yml already exists for a different tunnel — leaving it alone. Inspect it manually before retrying. Path: %s", configPath))
			writeLine("done", "1")
			return
		}
		writeLine("config", "existing config.yml matches this tunnel — skipping rewrite")
	} else {
		if werr := wizardWriteConfig(configPath, tunnelUUID, credPath, hostname); werr != nil {
			writeLine("error", "write config.yml: "+werr.Error())
			writeLine("done", "1")
			return
		}
		writeLine("config", "wrote "+configPath)
	}

	// Step 4: install autostart. NON-FATAL — the tunnel still works for
	// this session; only auto-respawn on reboot is impacted. CI runners
	// (GitHub Actions linux without a systemd user instance) trigger this
	// path and shouldn't fail the whole wizard. Surface as a warning the
	// frontend can render, but keep going.
	if aerr := wizardInstallAutostart(r.Context(), tunnelUUID, writeLine); aerr != nil {
		writeLine("warn", "autostart skipped: "+aerr.Error()+" — tunnel still works for this session, but won't auto-respawn on reboot. You can re-run the wizard later or wire autostart manually.")
	}

	// Step 5: verify reachability.
	healthURL := "https://" + hostname + "/health"
	writeLine("verify", "polling "+healthURL)
	if verr := wizardVerifyTunnel(r.Context(), healthURL, writeLine); verr != nil {
		writeLine("warn", verr.Error())
	} else {
		writeLine("verify", "tunnel is up")
	}

	writeLine("hostname", hostname)
	writeLine("tunnel_uuid", tunnelUUID)
	writeLine("done", "0")
}

// wizardCreateOrReuseTunnel runs `cloudflared tunnel list` and looks for an
// existing tunnel of the given name; if found, returns its UUID. Otherwise
// runs `cloudflared tunnel create <name>` and parses the new UUID + creds.
func wizardCreateOrReuseTunnel(ctx context.Context, bin, name string, log func(step, msg string)) (uuid, credPath string, err error) {
	// Try list first.
	listCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	listOut, _ := exec.CommandContext(listCtx, bin, "tunnel", "list").CombinedOutput() //nolint:gosec // bin from env, args constant

	// Output looks roughly like:
	//   ID                                     NAME    CREATED              CONNECTIONS
	//   328de5a1-00dd-4326-b38a-b0763781ccb6   sage    2025-...
	// We don't try to be a real parser — find a line containing the name
	// surrounded by whitespace and a UUID at the start of the line.
	for _, line := range strings.Split(string(listOut), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[1] == name && validTunnelUUIDFullRe.MatchString(fields[0]) {
			cred := filepath.Join(cloudflaredHome(), fields[0]+".json")
			if _, err := os.Stat(cred); err == nil {
				log("tunnel", "reusing existing tunnel "+fields[0])
				return fields[0], cred, nil
			}
		}
	}

	// Otherwise create it.
	createCtx, cancel2 := context.WithTimeout(ctx, 60*time.Second)
	defer cancel2()
	out, cerr := exec.CommandContext(createCtx, bin, "tunnel", "create", name).CombinedOutput() //nolint:gosec // bin from env, name validated
	if cerr != nil {
		return "", "", fmt.Errorf("tunnel create: %w (output: %s)", cerr, strings.TrimSpace(string(out)))
	}
	log("tunnel", strings.TrimSpace(string(out)))

	// Parse UUID from output.
	uuidMatch := validTunnelUUIDRe.FindString(string(out))
	if uuidMatch == "" {
		return "", "", fmt.Errorf("could not parse tunnel UUID from cloudflared output: %s", strings.TrimSpace(string(out)))
	}
	credPath = filepath.Join(cloudflaredHome(), uuidMatch+".json")
	if _, err := os.Stat(credPath); err != nil {
		return "", "", fmt.Errorf("credentials file not found at %s: %w", credPath, err)
	}
	return uuidMatch, credPath, nil
}

// wizardRouteDNS adds the CNAME from <hostname> → <tunnel>.cfargotunnel.com.
func wizardRouteDNS(ctx context.Context, bin, name, hostname string, log func(step, msg string)) error {
	dnsCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// nolint:gosec // bin is env-controlled, name + hostname pre-validated.
	out, err := exec.CommandContext(dnsCtx, bin, "tunnel", "route", "dns", name, hostname).CombinedOutput()
	msg := strings.TrimSpace(string(out))
	if err != nil {
		// "already routed" is a benign error we treat as success — re-running
		// the wizard against the same hostname must be idempotent.
		if strings.Contains(strings.ToLower(msg), "already exists") || strings.Contains(strings.ToLower(msg), "already configured") || strings.Contains(strings.ToLower(msg), "already") {
			log("dns", "DNS route already exists — reusing")
			return nil
		}
		return fmt.Errorf("route dns: %w (output: %s)", err, msg)
	}
	log("dns", msg)
	return nil
}

// configTemplate matches the format known to work — see the user's
// hand-crafted config at ~/.cloudflared/config.yml as the reference.
// First-match-wins ingress; final rule is a 404 catchall so anything not
// explicitly allowlisted is dropped at the edge.
var configTemplate = template.Must(template.New("cf").Parse(`# Generated by SAGE CEREBRUM ChatGPT setup wizard ({{.Version}}).
# Path allowlist — only the surface that external MCP clients (ChatGPT,
# Cursor, Cline) actually need. CEREBRUM dashboard, login form, ed25519
# admin endpoints, and dashboard health all stay private to localhost.
# First-match-wins; final rule is a catchall 404.
tunnel: {{.TunnelUUID}}
credentials-file: {{.CredentialsFile}}

ingress:
  # MCP transport — bearer-auth protected
  - hostname: {{.Hostname}}
    path: ^/v1/mcp/(sse|messages|streamable)(/.*)?$
    service: https://localhost:8443
    originRequest:
      noTLSVerify: true

  # OAuth wrapper for ChatGPT (v6.7.2+) — auth-code + token exchange
  - hostname: {{.Hostname}}
    path: ^/oauth/(authorize|token|register)(/.*)?$
    service: https://localhost:8443
    originRequest:
      noTLSVerify: true

  # OAuth discovery doc (RFC 8414)
  - hostname: {{.Hostname}}
    path: ^/\.well-known/oauth-authorization-server/?$
    service: https://localhost:8443
    originRequest:
      noTLSVerify: true

  # Minimal liveness probe — no chain stats, no memory counts
  - hostname: {{.Hostname}}
    path: ^/health/?$
    service: https://localhost:8443
    originRequest:
      noTLSVerify: true

  # Anything else from the public internet → 404 at the edge.
  - service: http_status:404
`))

// wizardWriteConfig writes the path-restricted cloudflared config.
func wizardWriteConfig(configPath, tunnelUUID, credPath, hostname string) error {
	if !validTunnelUUIDFullRe.MatchString(tunnelUUID) {
		return fmt.Errorf("invalid tunnel UUID")
	}
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		return err
	}
	f, err := os.Create(configPath) //nolint:gosec // configPath is user's home directory
	if err != nil {
		return err
	}
	defer f.Close()
	return configTemplate.Execute(f, map[string]string{
		"Version":         "v6.7.3",
		"TunnelUUID":      tunnelUUID,
		"CredentialsFile": credPath,
		"Hostname":        hostname,
	})
}

// launchdSagePlistTemplate is the autostart unit for cloudflared on macOS.
// Distinct label (com.cloudflared.sage, NOT com.sage.personal) so it doesn't
// collide with SAGE's own autostart.
var launchdSagePlistTemplate = template.Must(template.New("plist").Parse(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.cloudflared.sage</string>
    <key>ProgramArguments</key>
    <array>
        <string>{{.CloudflaredPath}}</string>
        <string>tunnel</string>
        <string>run</string>
        <string>sage</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>{{.LogPath}}</string>
    <key>StandardErrorPath</key>
    <string>{{.LogPath}}</string>
</dict>
</plist>
`))

// systemdSageUnitTemplate is the autostart unit for cloudflared on linux.
var systemdSageUnitTemplate = template.Must(template.New("unit").Parse(`# Generated by SAGE CEREBRUM wizard
[Unit]
Description=Cloudflare Tunnel for SAGE
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart={{.CloudflaredPath}} tunnel run sage
Restart=always
RestartSec=5
StandardOutput=append:{{.LogPath}}
StandardError=append:{{.LogPath}}

[Install]
WantedBy=default.target
`))

// wizardInstallAutostart writes & loads the platform-appropriate unit file.
func wizardInstallAutostart(ctx context.Context, _ string, log func(step, msg string)) error {
	cfPath, lookErr := exec.LookPath(cloudflaredBin())
	if lookErr != nil {
		return fmt.Errorf("cloudflared not on PATH: %w", lookErr)
	}
	logPath := filepath.Join(cloudflaredHome(), "sage.log")

	switch runtime.GOOS {
	case "darwin":
		dir := launchAgentsDir()
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		plistPath := filepath.Join(dir, "com.cloudflared.sage.plist")
		f, err := os.Create(plistPath) //nolint:gosec // plistPath is in user's home
		if err != nil {
			return err
		}
		if terr := launchdSagePlistTemplate.Execute(f, map[string]string{
			"CloudflaredPath": cfPath,
			"LogPath":         logPath,
		}); terr != nil {
			f.Close()
			return terr
		}
		f.Close()
		log("autostart", "wrote "+plistPath)
		// Load it (idempotent).
		unloadCtx, unloadCancel := context.WithTimeout(ctx, 10*time.Second)
		_ = exec.CommandContext(unloadCtx, "launchctl", "unload", plistPath).Run() //nolint:gosec // path under user's home, not user input
		unloadCancel()
		loadCtx, loadCancel := context.WithTimeout(ctx, 10*time.Second)
		defer loadCancel()
		if err := exec.CommandContext(loadCtx, "launchctl", "load", plistPath).Run(); err != nil { //nolint:gosec // path under user's home
			return fmt.Errorf("launchctl load: %w", err)
		}
		log("autostart", "launchctl loaded com.cloudflared.sage")
		return nil
	case "linux":
		home, _ := os.UserHomeDir()
		dir := filepath.Join(home, ".config", "systemd", "user")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		unitPath := filepath.Join(dir, "cloudflared-sage.service")
		f, err := os.Create(unitPath) //nolint:gosec // path under user's home
		if err != nil {
			return err
		}
		if terr := systemdSageUnitTemplate.Execute(f, map[string]string{
			"CloudflaredPath": cfPath,
			"LogPath":         logPath,
		}); terr != nil {
			f.Close()
			return terr
		}
		f.Close()
		log("autostart", "wrote "+unitPath)
		reloadCtx, reloadCancel := context.WithTimeout(ctx, 10*time.Second)
		_ = exec.CommandContext(reloadCtx, "systemctl", "--user", "daemon-reload").Run()
		reloadCancel()
		enableCtx, enableCancel := context.WithTimeout(ctx, 10*time.Second)
		defer enableCancel()
		if err := exec.CommandContext(enableCtx, "systemctl", "--user", "enable", "--now", "cloudflared-sage.service").Run(); err != nil {
			return fmt.Errorf("systemctl enable: %w", err)
		}
		log("autostart", "systemctl enabled cloudflared-sage.service")
		return nil
	default:
		return fmt.Errorf("autostart not supported on %s", runtime.GOOS)
	}
}

// wizardVerifyTunnel polls https://<hostname>/health until it returns 200
// or 30s elapses.
func wizardVerifyTunnel(ctx context.Context, healthURL string, log func(step, msg string)) error {
	deadline := time.Now().Add(30 * time.Second)
	client := &http.Client{Timeout: 5 * time.Second}
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			log("verify", fmt.Sprintf("got HTTP %d, retrying...", resp.StatusCode))
		} else {
			log("verify", err.Error())
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("tunnel did not respond 200 to %s within 30s — it may take another minute for Cloudflare DNS + edge to propagate; check `tail -f ~/.cloudflared/sage.log`", healthURL)
}

// ─── /mint-token ─────────────────────────────────────────────────────────

// handleWizardMintToken wraps the existing /v1/mcp/tokens flow with simpler
// ergonomics for the wizard. The actual token creation goes through the
// canonical api/rest handler — we just call our local SAGE node over HTTP
// (using the same admin-auth that gates the wizard).
//
// The wizard is already authenticated via the dashboard cookie; on the
// backend we proxy the token mint through the in-process /v1/mcp/tokens
// handler by calling it via the local HTTP listener with X-Agent-ID +
// X-Signature headers. Since that's invasive, we instead read the result
// from a thin in-package helper if MCP token store is present.
func (h *DashboardHandler) handleWizardMintToken(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AgentID   string `json:"agent_id"`
		TokenName string `json:"token_name"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	req.AgentID = strings.TrimSpace(req.AgentID)
	if req.TokenName == "" {
		req.TokenName = "chatgpt"
	}
	if len(req.AgentID) != 64 {
		writeError(w, http.StatusBadRequest, "agent_id must be a 64-char hex-encoded ed25519 public key")
		return
	}

	ts, ok := h.store.(mcpWizardTokenStore)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "mcp tokens unsupported on this backend")
		return
	}

	// Mint the token using the same primitives as the api/rest handler.
	token, id, createdAt, err := mintMCPTokenForWizard(r.Context(), ts, req.AgentID, req.TokenName)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "mint token: "+err.Error())
		return
	}

	writeJSONResp(w, http.StatusCreated, map[string]any{
		"id":         id,
		"agent_id":   req.AgentID,
		"name":       req.TokenName,
		"token":      token,
		"created_at": createdAt.Format(time.RFC3339),
		"use_hint":   "Save this token NOW — it will never be shown again. The OAuth flow will return it to ChatGPT at consent time.",
	})
}

// mcpWizardTokenStore is the minimal interface from store.SQLiteStore the
// wizard's mint-token endpoint needs. Defined locally so the web package
// stays decoupled from the api/rest sibling package while still issuing
// real, audit-equivalent MCP tokens.
type mcpWizardTokenStore interface {
	InsertMCPToken(ctx context.Context, id, name, agentID, tokenSHA256 string) error
}
