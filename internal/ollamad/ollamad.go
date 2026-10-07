// Package ollamad manages SAGE's local Ollama runtime for semantic memory.
package ollamad

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	ModelName      = "nomic-embed-text"
	ModelDimension = 768
	DefaultPort    = 11434

	binaryName = "ollama"
)

// Manager owns the managed Ollama runtime lifecycle. A healthy loopback API is
// the source of truth, so it can adopt an already-running Ollama app or a
// sidecar that survived a SAGE self-restart.
type Manager struct {
	mu      sync.Mutex
	dataDir string
	port    int
	model   string
	dim     int
	cmd     *exec.Cmd

	dlMu       sync.Mutex
	installing bool
	pulling    bool
	dlDone     int64
	dlTotal    int64
	pullStatus string
}

func New(dataDir string) *Manager {
	manager, _ := NewConfigured(dataDir, ModelName, ModelDimension)
	return manager
}

// NewConfigured binds the managed runtime to the exact model and vector
// dimension used by the node embedding provider. Keeping this explicit avoids
// a custom CPU profile being declared unhealthy—or worse, silently treated as
// nomic/768—by the sidecar readiness and pull paths.
func NewConfigured(dataDir, model string, dimension int) (*Manager, error) {
	model = strings.TrimSpace(model)
	if model == "" {
		return nil, errors.New("managed Ollama model is required")
	}
	if dimension <= 0 {
		return nil, errors.New("managed Ollama dimension must be positive")
	}
	return &Manager{
		dataDir: dataDir,
		port:    DefaultPort,
		model:   model,
		dim:     dimension,
	}, nil
}

func (m *Manager) URL() string { return fmt.Sprintf("http://127.0.0.1:%d", m.port) }

func (m *Manager) engineDir() string { return filepath.Join(m.dataDir, "ollama") }
func (m *Manager) modelDir() string  { return filepath.Join(m.dataDir, "ollama-models") }
func (m *Manager) pidFilePath() string {
	return filepath.Join(m.dataDir, "ollamad.pid")
}
func (m *Manager) logFilePath() string {
	return filepath.Join(m.dataDir, "ollamad.log")
}

func (m *Manager) managedBinaryCandidates() []string {
	exe := binaryName
	if runtime.GOOS == "windows" {
		exe += ".exe"
	}
	return []string{
		filepath.Join(m.engineDir(), exe),
		filepath.Join(m.engineDir(), "bin", exe),
		filepath.Join(m.engineDir(), "ollama", exe),
	}
}

func (m *Manager) managedBinaryPath() string {
	for _, p := range m.managedBinaryCandidates() {
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p
		}
	}
	return m.managedBinaryCandidates()[0]
}

func (m *Manager) EngineInstalled() bool {
	for _, p := range m.managedBinaryCandidates() {
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return true
		}
	}
	return false
}

func (m *Manager) BinaryPath() (string, bool) {
	if m.EngineInstalled() {
		return m.managedBinaryPath(), true
	}
	if p, err := exec.LookPath(binaryName); err == nil {
		return p, true
	}
	for _, p := range []string{
		"/Applications/Ollama.app/Contents/Resources/ollama",
		"/opt/homebrew/bin/ollama",
		"/usr/local/bin/ollama",
		"/home/linuxbrew/.linuxbrew/bin/ollama",
		"/usr/bin/ollama",
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

func (m *Manager) Pulling() bool {
	m.dlMu.Lock()
	defer m.dlMu.Unlock()
	return m.pulling
}

func (m *Manager) Progress() (done, total int64) {
	m.dlMu.Lock()
	defer m.dlMu.Unlock()
	return m.dlDone, m.dlTotal
}

func (m *Manager) PullStatus() string {
	m.dlMu.Lock()
	defer m.dlMu.Unlock()
	return m.pullStatus
}

func (m *Manager) Probe(ctx context.Context) bool {
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(cctx, http.MethodGet, m.URL()+"/api/tags", nil)
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (m *Manager) ModelReady(ctx context.Context) bool {
	cctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(cctx, http.MethodGet, m.URL()+"/api/tags", nil)
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	var tags struct {
		Models []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	if json.NewDecoder(resp.Body).Decode(&tags) != nil {
		return false
	}
	for _, model := range tags.Models {
		if model.Name == m.model || strings.HasPrefix(model.Name, m.model+":") {
			return m.embedProbe(cctx)
		}
	}
	return false
}

func (m *Manager) embedProbe(ctx context.Context) bool {
	if m.tryEmbedProbe(ctx, "/api/embed", map[string]any{
		"model": m.model,
		"input": "sage semantic readiness probe",
	}) {
		return true
	}
	return m.tryEmbedProbe(ctx, "/api/embeddings", map[string]any{
		"model":  m.model,
		"prompt": "sage semantic readiness probe",
	})
}

func (m *Manager) tryEmbedProbe(ctx context.Context, path string, payload map[string]any) bool {
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.URL()+path, bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	var out struct {
		Embedding  []float32   `json:"embedding"`
		Embeddings [][]float32 `json:"embeddings"`
		Error      string      `json:"error"`
	}
	if json.NewDecoder(resp.Body).Decode(&out) != nil || out.Error != "" {
		return false
	}
	if len(out.Embedding) == m.dim {
		return true
	}
	return len(out.Embeddings) > 0 && len(out.Embeddings[0]) == m.dim
}

func (m *Manager) Start(ctx context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Probe(ctx) {
		return m.URL(), nil
	}
	bin, ok := m.BinaryPath()
	if !ok {
		return "", fmt.Errorf("ollama runtime not installed yet")
	}
	if err := os.MkdirAll(m.modelDir(), 0o755); err != nil {
		return "", fmt.Errorf("create Ollama model dir: %w", err)
	}
	logf, err := os.OpenFile(m.logFilePath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return "", fmt.Errorf("open Ollama log: %w", err)
	}
	cmd := exec.CommandContext(context.WithoutCancel(ctx), bin, "serve")
	cmd.Stdout = logf
	cmd.Stderr = logf
	cmd.SysProcAttr = sidecarSysProcAttr()
	cmd.Env = m.childEnv()
	if err := cmd.Start(); err != nil {
		_ = logf.Close()
		return "", fmt.Errorf("start Ollama: %w", err)
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

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			_ = m.stopLocked()
			return "", ctx.Err()
		case <-exited:
			m.cmd = nil
			_ = os.Remove(m.pidFilePath())
			return "", fmt.Errorf("ollama exited during startup - see %s", m.logFilePath())
		case <-time.After(500 * time.Millisecond):
		}
		if m.Probe(ctx) {
			return m.URL(), nil
		}
	}
	_ = m.stopLocked()
	return "", fmt.Errorf("ollama did not become ready - see %s", m.logFilePath())
}

func (m *Manager) PullModel(ctx context.Context, status func(string), progress func(done, total int64)) error {
	if m.ModelReady(ctx) {
		return nil
	}
	if !m.Probe(ctx) {
		return fmt.Errorf("ollama is not running")
	}
	m.dlMu.Lock()
	if m.pulling {
		m.dlMu.Unlock()
		return fmt.Errorf("a model download is already in progress")
	}
	m.pulling = true
	m.dlDone, m.dlTotal = 0, 0
	m.pullStatus = "starting"
	m.dlMu.Unlock()
	defer func() {
		m.dlMu.Lock()
		m.pulling = false
		m.dlMu.Unlock()
	}()

	body, _ := json.Marshal(map[string]any{"name": m.model, "stream": true})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.URL()+"/api/pull", strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 0}).Do(req)
	if err != nil {
		return fmt.Errorf("pull model: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("pull model: http %d", resp.StatusCode)
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for scanner.Scan() {
		var msg struct {
			Status    string `json:"status"`
			Error     string `json:"error"`
			Completed int64  `json:"completed"`
			Total     int64  `json:"total"`
		}
		if json.Unmarshal(scanner.Bytes(), &msg) != nil {
			continue
		}
		if msg.Error != "" {
			return errors.New(msg.Error)
		}
		m.dlMu.Lock()
		if msg.Status != "" {
			m.pullStatus = msg.Status
		}
		if msg.Total > 0 {
			m.dlDone, m.dlTotal = msg.Completed, msg.Total
		}
		done, total, st := m.dlDone, m.dlTotal, m.pullStatus
		m.dlMu.Unlock()
		if st != "" && status != nil {
			status(st)
		}
		if total > 0 && progress != nil {
			progress(done, total)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("pull model: %w", err)
	}
	if !m.ModelReady(ctx) {
		return fmt.Errorf("%s did not become available after download", m.model)
	}
	return nil
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
	if err != nil || !pidIsOllama(pid) {
		_ = os.Remove(m.pidFilePath())
		return nil
	}
	killSidecar(pid)
	_ = os.Remove(m.pidFilePath())
	return nil
}
