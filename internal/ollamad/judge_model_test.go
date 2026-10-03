package ollamad

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDownloadVerifiedFile(t *testing.T) {
	payload := []byte("pinned judge weights")
	sum := sha256.Sum256(payload)
	good := hex.EncodeToString(sum[:])
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	t.Run("correct digest installs", func(t *testing.T) {
		dest := filepath.Join(t.TempDir(), "m.gguf")
		require.NoError(t, downloadVerifiedFile(context.Background(), srv.URL, good, dest))
		got, err := os.ReadFile(dest)
		require.NoError(t, err)
		require.Equal(t, payload, got)
		require.NoFileExists(t, dest+".part")
	})

	t.Run("wrong digest refuses and leaves no file", func(t *testing.T) {
		dest := filepath.Join(t.TempDir(), "m.gguf")
		err := downloadVerifiedFile(context.Background(), srv.URL, "deadbeef", dest)
		require.ErrorContains(t, err, "checksum mismatch")
		require.NoFileExists(t, dest, "a model that fails verification must not be left where the loader finds it")
		require.NoFileExists(t, dest+".part")
	})

	t.Run("http error refuses", func(t *testing.T) {
		bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) }))
		defer bad.Close()
		dest := filepath.Join(t.TempDir(), "m.gguf")
		require.Error(t, downloadVerifiedFile(context.Background(), bad.URL, good, dest))
		require.NoFileExists(t, dest)
	})
}

// The pinned judge constants are internally consistent: a 64-hex sha256 and an HTTPS URL that carries the tag.
func TestJudgePinConsistency(t *testing.T) {
	require.Len(t, JudgeGGUFSHA256, 64)
	require.Contains(t, judgeGGUFURL, "https://")
	require.Contains(t, judgeGGUFURL, judgeGGUFName)
}

func TestManagedOllama_CloudDisabledDespiteInheritedSettings(t *testing.T) {
	t.Setenv("OLLAMA_NO_CLOUD", "0")
	t.Setenv("OLLAMA_HOST", "https://remote.example")
	t.Setenv("OLLAMA_MODELS", "/foreign-models")
	m := New(t.TempDir())
	values := map[string]string{}
	for _, kv := range m.childEnv() {
		k, v, _ := strings.Cut(kv, "=")
		require.NotContains(t, values, k, "child environment must not contain duplicate keys")
		values[k] = v
	}
	require.Equal(t, "1", values["OLLAMA_NO_CLOUD"])
	require.Equal(t, "127.0.0.1:11434", values["OLLAMA_HOST"])
	require.Equal(t, m.modelDir(), values["OLLAMA_MODELS"])
}
