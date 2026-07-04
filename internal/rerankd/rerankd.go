// Package rerankd manages SAGE's optional reranker sidecar: a llama.cpp
// llama-server process serving bge-reranker-v2-m3 on loopback. Ollama has no
// rerank endpoint (the upstream PR died unmerged), so the "bundled the same
// way as Ollama" story is: detect the llama-server binary (guide the install
// when missing), download a pinned GGUF once, then spawn and manage the
// process. Everything runs locally; nothing leaves the machine.
package rerankd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/l33tdawg/sage/internal/embedding"
)

const (
	// Pinned model asset. gpustack maintains the reference GGUF conversions
	// for llama-box; the sha256 is the upstream LFS object hash, verified
	// after download so a tampered or truncated file never gets served.
	ModelDisplayName = "bge-reranker-v2-m3 (Q8_0)"
	modelFileName    = "bge-reranker-v2-m3-Q8_0.gguf"
	modelURL         = "https://huggingface.co/gpustack/bge-reranker-v2-m3-GGUF/resolve/main/bge-reranker-v2-m3-Q8_0.gguf"
	modelSHA256      = "a43c7c9b11a4c1517e5bf95151960e1621d1b72f7a493364b01e386cf1aaa1d3"
	// ModelSizeBytes is the exact pinned asset size - used for progress
	// totals and the cheap "already downloaded" check.
	ModelSizeBytes = 635676416

	// DefaultPort is the loopback port the managed sidecar binds. 8081 is
	// left to bring-your-own TEI servers (the Settings detect probe's
	// convention); the managed process stays out of its way.
	DefaultPort = 8082

	binaryName = "llama-server"
)

// Test seams: production values point at the pinned asset; tests override
// them to exercise the download/verify machinery against a local server.
var (
	modelSrcURL   = modelURL
	modelWantSHA  = modelSHA256
	modelWantSize = int64(ModelSizeBytes)
)

// Manager owns the sidecar lifecycle. The port is the source of truth for
// "running": node restarts happen via syscall.Exec, which orphans (not kills)
// a spawned child, so on boot we ADOPT a healthy sidecar instead of spawning
// a duplicate.
type Manager struct {
	mu      sync.Mutex
	dataDir string
	port    int
	cmd     *exec.Cmd // our spawned child, nil when adopted or not running

	dlMu        sync.Mutex
	downloading bool
}

// New returns a manager rooted at the SAGE home directory (~/.sage).
func New(dataDir string) *Manager {
	return &Manager{dataDir: dataDir, port: DefaultPort}
}

// URL is the sidecar's base URL, valid whether spawned or adopted.
func (m *Manager) URL() string { return fmt.Sprintf("http://127.0.0.1:%d", m.port) }

// ModelPath is where the pinned GGUF lives.
func (m *Manager) ModelPath() string {
	return filepath.Join(m.dataDir, "models", modelFileName)
}

func (m *Manager) pidFilePath() string { return filepath.Join(m.dataDir, "rerankd.pid") }
func (m *Manager) logFilePath() string { return filepath.Join(m.dataDir, "rerankd.log") }

// BinaryPath locates llama-server: the managed install wins (version-pinned,
// lives beside the model), then PATH, then the usual install prefixes (brew
// on macOS/Linuxbrew, plain /usr/local).
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

// ModelReady reports whether the pinned GGUF is fully present. Size match is
// the cheap check; content integrity was verified at download time.
func (m *Manager) ModelReady() bool {
	st, err := os.Stat(m.ModelPath())
	return err == nil && st.Size() == modelWantSize
}

// Downloading reports whether a model download is in flight.
func (m *Manager) Downloading() bool {
	m.dlMu.Lock()
	defer m.dlMu.Unlock()
	return m.downloading
}

// Download fetches the pinned GGUF to ModelPath with sha256 verification and
// an atomic rename. progress (optional) receives cumulative bytes and the
// total. Returns immediately if the model is already present. Concurrent
// calls beyond the first fail fast.
func (m *Manager) Download(ctx context.Context, progress func(done, total int64)) error {
	if m.ModelReady() {
		return nil
	}
	m.dlMu.Lock()
	if m.downloading {
		m.dlMu.Unlock()
		return fmt.Errorf("a download is already in progress")
	}
	m.downloading = true
	m.dlMu.Unlock()
	defer func() {
		m.dlMu.Lock()
		m.downloading = false
		m.dlMu.Unlock()
	}()

	if err := os.MkdirAll(filepath.Dir(m.ModelPath()), 0o755); err != nil {
		return fmt.Errorf("create models dir: %w", err)
	}
	tmp := m.ModelPath() + ".part"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(tmp) // no-op after the success rename
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, modelSrcURL, nil)
	if err != nil {
		return err
	}
	resp, err := (&http.Client{Timeout: 0}).Do(req) // 636MB: bound by ctx, not a fixed timeout
	if err != nil {
		return fmt.Errorf("download model: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download model: http %d", resp.StatusCode)
	}

	hasher := sha256.New()
	var done int64
	buf := make([]byte, 1<<20)
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := f.Write(buf[:n]); werr != nil {
				return fmt.Errorf("write model: %w", werr)
			}
			_, _ = hasher.Write(buf[:n])
			done += int64(n)
			if progress != nil {
				progress(done, modelWantSize)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return fmt.Errorf("download model: %w", rerr)
		}
	}
	if done != modelWantSize {
		return fmt.Errorf("download incomplete: got %d of %d bytes", done, modelWantSize)
	}
	if sum := hex.EncodeToString(hasher.Sum(nil)); sum != modelWantSHA {
		return fmt.Errorf("model checksum mismatch (got %s) - refusing to install it", sum)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close model file: %w", err)
	}
	if err := os.Rename(tmp, m.ModelPath()); err != nil {
		return fmt.Errorf("install model: %w", err)
	}
	return nil
}

// Probe reports whether something on our port answers a real llama.cpp
// rerank call. This (not a pid) is the liveness signal: it also recognizes a
// sidecar orphaned by a node self-exec restart, which we adopt rather than
// double-spawn.
func (m *Manager) Probe(ctx context.Context) bool {
	rr := embedding.NewHTTPRerankerKind(m.URL(), ModelDisplayName, embedding.RerankKindLlamaCpp, 3*time.Second)
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err := rr.Rerank(probeCtx, "sage rerankd probe", []string{"alpha"})
	return err == nil
}

// Start ensures a healthy sidecar is serving on the port and returns its URL.
// If one already answers (previous spawn surviving a node restart), it is
// adopted. Otherwise llama-server is spawned with the pinned model and
// polled until /health clears (503 = still loading the model).
func (m *Manager) Start(ctx context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Probe(ctx) {
		return m.URL(), nil // adopt
	}
	bin, ok := m.BinaryPath()
	if !ok {
		return "", fmt.Errorf("%s not found - install llama.cpp first", binaryName)
	}
	if !m.ModelReady() {
		return "", fmt.Errorf("reranker model not downloaded yet")
	}

	logf, err := os.OpenFile(m.logFilePath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return "", fmt.Errorf("open sidecar log: %w", err)
	}
	// --embedding --pooling rank --rerank is the documented trio for serving
	// a cross-encoder reranker; /v1/rerank appears only with these set.
	cmd := exec.Command(bin,
		"-m", m.ModelPath(),
		"--embedding", "--pooling", "rank", "--rerank",
		"--host", "127.0.0.1",
		"--port", strconv.Itoa(m.port),
	)
	cmd.Stdout = logf
	cmd.Stderr = logf
	cmd.SysProcAttr = sidecarSysProcAttr()
	if err := cmd.Start(); err != nil {
		_ = logf.Close()
		return "", fmt.Errorf("start %s: %w", binaryName, err)
	}
	m.cmd = cmd
	_ = os.WriteFile(m.pidFilePath(), []byte(strconv.Itoa(cmd.Process.Pid)), 0o644)
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait() // reap; the log file stays open for the process lifetime
		_ = logf.Close()
		close(exited)
	}()

	// Wait for the model to load. /health returns 503 while loading, 200
	// when ready; a small GGUF on a modern machine takes single-digit
	// seconds, cold spinning disks take longer.
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			m.stopLocked()
			return "", ctx.Err()
		case <-exited: // crashed during startup (bad flags, port in use, ...)
			m.cmd = nil
			_ = os.Remove(m.pidFilePath())
			return "", fmt.Errorf("%s exited during startup - see %s", binaryName, m.logFilePath())
		case <-time.After(500 * time.Millisecond):
		}
		if m.healthOK(ctx) {
			return m.URL(), nil
		}
	}
	m.stopLocked()
	return "", fmt.Errorf("%s did not become healthy - see %s", binaryName, m.logFilePath())
}

func (m *Manager) healthOK(ctx context.Context) bool {
	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, m.URL()+"/health", nil)
	if err != nil {
		return false
	}
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode == http.StatusOK
}

// Stop terminates the sidecar: our own child if we spawned it, otherwise the
// pidfile process from a previous node incarnation (best-effort).
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
	// Adopted process: use the pidfile left by whichever incarnation
	// spawned it. Verify the port actually serves a reranker before
	// killing anything, so a recycled pid can't take out an innocent
	// process.
	b, err := os.ReadFile(m.pidFilePath())
	if err != nil {
		return nil // nothing to stop
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || pid <= 1 {
		_ = os.Remove(m.pidFilePath())
		return nil
	}
	if m.Probe(context.Background()) {
		killSidecar(pid)
	}
	_ = os.Remove(m.pidFilePath())
	return nil
}
