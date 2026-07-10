// Package tunnelclientd manages OpenAI's tunnel-client as a local sidecar for
// ChatGPT/Codex Secure MCP Tunnel connections.
package tunnelclientd

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DefaultProfile         = "sage-chatgpt"
	DefaultPort            = 8081
	releaseTag             = "v0.0.10"
	binaryName             = "tunnel-client"
	maxExtractedBinarySize = 128 << 20
)

type asset struct {
	name   string
	sha256 string
	size   int64
}

var assets = map[string]asset{
	"darwin/amd64":  {"tunnel-client-v0.0.10-darwin-amd64.zip", "1a48616e584484f8bef4c1128d515ac96cf44d0d9609c1462abccc1793f4b847", 7672583},
	"darwin/arm64":  {"tunnel-client-v0.0.10-darwin-arm64.zip", "288accc7fd20cfee1d495adb933773af9e19ebc0cdef3173f7fb544afa5065b2", 7100022},
	"linux/amd64":   {"tunnel-client-v0.0.10-linux-amd64.zip", "b9e0388a343f2d7adeff3992f411a0bd3d916a64bc56534aac5fd15ac1b20cd5", 7508561},
	"linux/arm64":   {"tunnel-client-v0.0.10-linux-arm64.zip", "b842a9b2352eebd80514cf01a1fbb1c0d400a7d24a4015e85a7ea5f1aeaa5b30", 6789903},
	"windows/amd64": {"tunnel-client-v0.0.10-windows-amd64.zip", "5e64a056f1d96786da0a6f8db1da5f5f4a03fd19a90d951a25cf2ca8d9093d00", 7658615},
	"windows/arm64": {"tunnel-client-v0.0.10-windows-arm64.zip", "08954ccda078abfeac9382f9b19d178ce0656cfe1e84f5941f0f8a5c4e91ea78", 6839760},
}

var releaseBaseURL = "https://github.com/openai/tunnel-client/releases/download/" + releaseTag + "/"

type Manager struct {
	mu      sync.Mutex
	dataDir string
	port    int
	cmd     *exec.Cmd

	dlMu       sync.Mutex
	installing bool
	dlDone     int64
	dlTotal    int64
}

type RunConfig struct {
	Profile    string
	TunnelID   string
	APIKey     string
	MCPCommand string
}

func New(dataDir string) *Manager {
	return &Manager{dataDir: dataDir, port: DefaultPort}
}

func (m *Manager) URL() string { return fmt.Sprintf("http://127.0.0.1:%d", m.port) }

func (m *Manager) engineDir() string  { return filepath.Join(m.dataDir, "tunnel-client") }
func (m *Manager) profileDir() string { return filepath.Join(m.dataDir, "tunnel-client-profiles") }
func (m *Manager) logFilePath() string {
	return filepath.Join(m.dataDir, "tunnel-client.log")
}
func (m *Manager) pidFilePath() string {
	return filepath.Join(m.dataDir, "tunnel-client.pid")
}

func exeName() string {
	if runtime.GOOS == "windows" {
		return binaryName + ".exe"
	}
	return binaryName
}

func (m *Manager) managedBinaryPath() string { return filepath.Join(m.engineDir(), exeName()) }

func (m *Manager) EngineInstalled() bool {
	st, err := os.Stat(m.managedBinaryPath())
	return err == nil && !st.IsDir()
}

func (m *Manager) InstallSupported() bool {
	_, ok := assets[runtime.GOOS+"/"+runtime.GOARCH]
	return ok
}

func (m *Manager) EngineSizeBytes() int64 {
	if a, ok := assets[runtime.GOOS+"/"+runtime.GOARCH]; ok {
		return a.size
	}
	return 0
}

func (m *Manager) BinaryPath() (string, bool) {
	if m.EngineInstalled() {
		return m.managedBinaryPath(), true
	}
	if p, err := exec.LookPath(binaryName); err == nil {
		return p, true
	}
	for _, p := range []string{
		"/opt/homebrew/bin/" + binaryName,
		"/usr/local/bin/" + binaryName,
		"/home/linuxbrew/.linuxbrew/bin/" + binaryName,
		"/usr/bin/" + binaryName,
	} {
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p, true
		}
	}
	return "", false
}

func (m *Manager) Installing() bool {
	m.dlMu.Lock()
	defer m.dlMu.Unlock()
	return m.installing
}

func (m *Manager) Progress() (done, total int64) {
	m.dlMu.Lock()
	defer m.dlMu.Unlock()
	return m.dlDone, m.dlTotal
}

func (m *Manager) Install(ctx context.Context, progress func(done, total int64)) error {
	if m.EngineInstalled() {
		return nil
	}
	m.dlMu.Lock()
	if m.installing {
		m.dlMu.Unlock()
		return fmt.Errorf("a tunnel-client install is already in progress")
	}
	m.installing = true
	m.dlDone, m.dlTotal = 0, m.EngineSizeBytes()
	m.dlMu.Unlock()
	defer func() {
		m.dlMu.Lock()
		m.installing = false
		m.dlMu.Unlock()
	}()

	a, ok := assets[runtime.GOOS+"/"+runtime.GOARCH]
	if !ok {
		return fmt.Errorf("no pinned tunnel-client build for %s/%s", runtime.GOOS, runtime.GOARCH)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, releaseBaseURL+a.name, nil)
	if err != nil {
		return err
	}
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return fmt.Errorf("download tunnel-client: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download tunnel-client: http %d", resp.StatusCode)
	}

	hasher := sha256.New()
	var buf bytes.Buffer
	var done int64
	chunk := make([]byte, 256<<10)
	for {
		n, rerr := resp.Body.Read(chunk)
		if n > 0 {
			buf.Write(chunk[:n])
			_, _ = hasher.Write(chunk[:n])
			done += int64(n)
			if done > a.size {
				return fmt.Errorf("tunnel-client archive larger than pinned size")
			}
			m.dlMu.Lock()
			m.dlDone = done
			m.dlMu.Unlock()
			if progress != nil {
				progress(done, a.size)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return fmt.Errorf("download tunnel-client: %w", rerr)
		}
	}
	if done != a.size {
		return fmt.Errorf("download incomplete: got %d of %d bytes", done, a.size)
	}
	if sum := hex.EncodeToString(hasher.Sum(nil)); sum != a.sha256 {
		return fmt.Errorf("tunnel-client checksum mismatch (got %s)", sum)
	}
	return m.installZip(buf.Bytes())
}

func (m *Manager) installZip(payload []byte) error {
	zr, err := zip.NewReader(bytes.NewReader(payload), int64(len(payload)))
	if err != nil {
		return err
	}
	var bin *zip.File
	for _, f := range zr.File {
		if filepath.Base(f.Name) == exeName() {
			bin = f
			break
		}
	}
	if bin == nil {
		return fmt.Errorf("archive did not contain %s", exeName())
	}
	rc, err := bin.Open()
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	if mkErr := os.MkdirAll(m.engineDir(), 0o755); mkErr != nil {
		return mkErr
	}
	tmp := m.managedBinaryPath() + ".part"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	limited := &io.LimitedReader{R: rc, N: maxExtractedBinarySize + 1}
	written, err := io.Copy(out, limited)
	if err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if written > maxExtractedBinarySize {
		_ = out.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("tunnel-client binary too large: %d bytes", written)
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, m.managedBinaryPath())
}

func (m *Manager) ProfilePath(profile string) string {
	return filepath.Join(m.profileDir(), cleanProfile(profile)+".yaml")
}

func (m *Manager) Start(ctx context.Context, cfg RunConfig) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Probe(ctx) {
		return m.URL(), nil
	}
	bin, ok := m.BinaryPath()
	if !ok {
		return "", fmt.Errorf("tunnel-client is not installed yet")
	}
	if strings.TrimSpace(cfg.TunnelID) == "" {
		return "", fmt.Errorf("tunnel_id is required")
	}
	if strings.TrimSpace(cfg.APIKey) == "" {
		return "", fmt.Errorf("runtime API key is required")
	}
	if strings.TrimSpace(cfg.MCPCommand) == "" {
		return "", fmt.Errorf("MCP command is required")
	}
	profile := cleanProfile(cfg.Profile)
	profilePath := m.ProfilePath(profile)
	if err := os.MkdirAll(filepath.Dir(profilePath), 0o700); err != nil {
		return "", err
	}
	if err := os.WriteFile(profilePath, []byte(profileYAML(cfg.TunnelID, cfg.MCPCommand, m.port)), 0o600); err != nil {
		return "", fmt.Errorf("write tunnel-client profile: %w", err)
	}

	logf, err := os.OpenFile(m.logFilePath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return "", fmt.Errorf("open tunnel-client log: %w", err)
	}
	cmd := exec.CommandContext(context.WithoutCancel(ctx), bin, "run", "--profile-file", profilePath)
	cmd.Stdout = logf
	cmd.Stderr = logf
	cmd.SysProcAttr = sidecarSysProcAttr()
	childEnv := make([]string, 0, len(os.Environ())+1)
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "CONTROL_PLANE_API_KEY=") {
			continue
		}
		childEnv = append(childEnv, kv)
	}
	childEnv = append(childEnv, "CONTROL_PLANE_API_KEY="+strings.TrimSpace(cfg.APIKey))
	cmd.Env = childEnv
	if err := cmd.Start(); err != nil {
		_ = logf.Close()
		return "", fmt.Errorf("start tunnel-client: %w", err)
	}
	m.cmd = cmd
	_ = os.WriteFile(m.pidFilePath(), []byte(strconv.Itoa(cmd.Process.Pid)), 0o600)

	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		_ = logf.Close()
		close(exited)
		m.mu.Lock()
		if m.cmd == cmd {
			m.cmd = nil
			_ = os.Remove(m.pidFilePath())
		}
		m.mu.Unlock()
	}()

	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			_ = m.stopLocked()
			return "", ctx.Err()
		case <-exited:
			m.cmd = nil
			_ = os.Remove(m.pidFilePath())
			return "", fmt.Errorf("tunnel-client exited during startup - see %s", m.logFilePath())
		case <-time.After(500 * time.Millisecond):
		}
		if m.Probe(ctx) {
			return m.URL(), nil
		}
	}
	_ = m.stopLocked()
	return "", fmt.Errorf("tunnel-client did not become healthy - see %s", m.logFilePath())
}

func (m *Manager) Probe(ctx context.Context) bool {
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(cctx, http.MethodGet, m.URL()+"/healthz", nil)
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopLocked()
}

func (m *Manager) stopLocked() error {
	if m.cmd != nil && m.cmd.Process != nil {
		killSidecar(m.cmd.Process.Pid)
		m.cmd = nil
		_ = os.Remove(m.pidFilePath())
		return nil
	}
	b, err := os.ReadFile(m.pidFilePath())
	if err != nil {
		return nil
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || !pidIsTunnelClient(pid) {
		_ = os.Remove(m.pidFilePath())
		return nil
	}
	killSidecar(pid)
	_ = os.Remove(m.pidFilePath())
	return nil
}

func (m *Manager) PortAvailable() bool {
	ln, err := new(net.ListenConfig).Listen(context.Background(), "tcp", fmt.Sprintf("127.0.0.1:%d", m.port))
	if err != nil {
		return false
	}
	_ = ln.Close()
	return true
}

func cleanProfile(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		s = DefaultProfile
	}
	var b strings.Builder
	for _, r := range s {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	if b.Len() == 0 {
		return DefaultProfile
	}
	return b.String()
}

func profileYAML(tunnelID, mcpCommand string, port int) string {
	return fmt.Sprintf(`config_version: 1
control_plane:
  tunnel_id: %q
  api_key: env:CONTROL_PLANE_API_KEY
health:
  listen_addr: %q
admin_ui:
  open_browser: false
log:
  level: info
  format: struct-text
mcp:
  commands:
    - channel: main
      command: %q
`, strings.TrimSpace(tunnelID), fmt.Sprintf("127.0.0.1:%d", port), strings.TrimSpace(mcpCommand))
}
