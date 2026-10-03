package ollamad

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// The pinned local memory-gate judge. Distributed as a plain file download + a sha256 the node verifies before
// install — the same verify-or-refuse discipline this package already applies to the Ollama runtime binary
// (install.go), and deliberately STRICTER than the by-name embedding-model pull (which trusts a registry tag).
// A new judge version is a new URL + tag + digest here; an existing tag is never re-pointed.
const (
	JudgeModelTag   = "sage-memory-judge:v15"
	judgeGGUFURL    = "https://huggingface.co/Infosec-Consult/sage-memory-judge/resolve/v15/sage-judge-v15-q8_0.gguf"
	JudgeGGUFSHA256 = "714ff9324133ba3b7166fe82fa362226fae5574c4f9cfbc53c054248b16f2cfd"
	// Ollama v0.31.1 rewrites the GGUF when importing it. Bind inference to
	// the separately verified registered blob, rather than the download hash.
	JudgeModelBlobSHA256 = "86bc739212ea59719839a59a1b994d720f2c9ec4010c32c8d92800db7b465169"
	judgeGGUFName        = "sage-judge-v15-q8_0.gguf"
	// Thinking is OFF by serving requirement; the judge reads the first token as the label.
	judgeModelfile = "FROM %s\nTEMPLATE {{ .Prompt }}\nRENDERER qwen3.5\nPARSER qwen3.5\n"
)

// EnsureJudgeModel makes the pinned local judge available in the managed Ollama. If the tag is already present
// it does nothing. Otherwise it downloads the GGUF from the pinned URL, VERIFIES its sha256 (refusing to install
// on any mismatch), and `ollama create`s the tag. It is safe to call on every startup; it only works once.
func (m *Manager) EnsureJudgeModel(ctx context.Context, status func(string)) error {
	if status == nil {
		status = func(string) {}
	}
	present, err := m.hasModel(ctx, JudgeModelTag)
	if err == nil && present {
		return nil
	}
	bin, ok := m.BinaryPath()
	if !ok {
		return fmt.Errorf("ollama runtime not installed yet")
	}
	if err := os.MkdirAll(m.modelDir(), 0o755); err != nil {
		return fmt.Errorf("create model dir: %w", err)
	}
	gguf := filepath.Join(m.modelDir(), judgeGGUFName)
	status("downloading judge model")
	if err := downloadVerifiedFile(ctx, judgeGGUFURL, JudgeGGUFSHA256, gguf); err != nil {
		return err
	}
	modelfile := filepath.Join(m.modelDir(), "Modelfile.judge")
	if err := os.WriteFile(modelfile, []byte(fmt.Sprintf(judgeModelfile, gguf)), 0o600); err != nil {
		return fmt.Errorf("write judge Modelfile: %w", err)
	}
	status("registering judge model")
	cmd := exec.CommandContext(ctx, bin, "create", JudgeModelTag, "-f", modelfile)
	cmd.Env = m.childEnv()
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ollama create %s: %w: %s", JudgeModelTag, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// childEnv is the environment the managed ollama binary runs under: the host and model dir pinned, and any
// inherited OLLAMA_HOST/OLLAMA_MODELS/SAGE_ vars scrubbed so nothing redirects it off the local runtime.
func (m *Manager) childEnv() []string {
	env := make([]string, 0, len(os.Environ())+3)
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "SAGE_") || strings.HasPrefix(kv, "OLLAMA_HOST=") || strings.HasPrefix(kv, "OLLAMA_MODELS=") || strings.HasPrefix(kv, "OLLAMA_NO_CLOUD=") {
			continue
		}
		env = append(env, kv)
	}
	return append(env,
		"OLLAMA_HOST=127.0.0.1:"+strconv.Itoa(m.port),
		"OLLAMA_MODELS="+m.modelDir(),
		"OLLAMA_NO_CLOUD=1",
	)
}

// hasModel reports whether a tag is already present in the managed Ollama.
func (m *Manager) hasModel(ctx context.Context, tag string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.URL()+"/api/tags", nil)
	if err != nil {
		return false, err
	}
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("list models: http %d", resp.StatusCode)
	}
	var out struct {
		Models []struct {
			Name  string `json:"name"`
			Model string `json:"model"`
		} `json:"models"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return false, err
	}
	for _, mm := range out.Models {
		if mm.Name == tag || mm.Model == tag {
			return true, nil
		}
	}
	return false, nil
}

// downloadVerifiedFile streams url to dest and refuses it unless the content's sha256 equals want. On any
// mismatch or incomplete read the partial file is removed and an error returned — never a half or wrong file
// left where the model loader would pick it up. Mirrors the runtime-binary verification in install.go.
func downloadVerifiedFile(ctx context.Context, url, want, dest string) error {
	tmp := dest + ".part"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create %s: %w", tmp, err)
	}
	cleanup := func() { _ = f.Close(); _ = os.Remove(tmp) }
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		cleanup()
		return err
	}
	resp, err := (&http.Client{Timeout: 0}).Do(req)
	if err != nil {
		cleanup()
		return fmt.Errorf("download judge model: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		cleanup()
		return fmt.Errorf("download judge model: http %d", resp.StatusCode)
	}
	hasher := sha256.New()
	if _, err := io.Copy(io.MultiWriter(f, hasher), resp.Body); err != nil {
		cleanup()
		return fmt.Errorf("download judge model: %w", err)
	}
	if sum := hex.EncodeToString(hasher.Sum(nil)); sum != want {
		cleanup()
		return fmt.Errorf("judge model checksum mismatch (got %s, want %s) - refusing to install it", sum, want)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close judge model: %w", err)
	}
	if err := os.Rename(tmp, dest); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("install judge model: %w", err)
	}
	return nil
}
