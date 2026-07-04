package rerankd

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeTarGz builds a release-shaped tar.gz: files nested under a top dir,
// plus hostile entries (path traversal name, symlink) that must be neutered.
func makeTarGz(t *testing.T, binName string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	add := func(name string, mode int64, body []byte) {
		require.NoError(t, tw.WriteHeader(&tar.Header{Name: name, Mode: mode, Size: int64(len(body)), Typeflag: tar.TypeReg}))
		_, err := tw.Write(body)
		require.NoError(t, err)
	}
	add("llama-b9870/"+binName, 0o755, []byte("#!/bin/sh\necho fake llama-server\n"))
	add("llama-b9870/libggml.dylib", 0o644, []byte("dylib bytes"))
	add("llama-b9870/../../evil.txt", 0o644, []byte("escape attempt")) // flattens to evil.txt at worst
	// Hostile symlink: target outside the archive - must be dropped.
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "llama-b9870/link", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd"}))
	// Soname chain like the real release: libx.dylib -> libx.0.dylib -> real file.
	add("llama-b9870/libx.0.0.1.dylib", 0o644, []byte("real dylib"))
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "llama-b9870/libx.0.dylib", Typeflag: tar.TypeSymlink, Linkname: "libx.0.0.1.dylib"}))
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "llama-b9870/libx.dylib", Typeflag: tar.TypeSymlink, Linkname: "libx.0.dylib"}))
	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())
	return buf.Bytes()
}

func withFakeEngine(t *testing.T, payload []byte, format string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(payload)
	}))
	sum := sha256.Sum256(payload)
	prevURL, prevFor := engineBaseURL, engineAssetFor
	engineBaseURL = srv.URL + "/"
	engineAssetFor = func() (engineAsset, bool) {
		return engineAsset{name: "fake." + format, sha256: hex.EncodeToString(sum[:]), size: int64(len(payload)), format: format}, true
	}
	t.Cleanup(func() { engineBaseURL, engineAssetFor = prevURL, prevFor; srv.Close() })
}

func TestInstallEngine_TarGzFlattensAndVerifies(t *testing.T) {
	payload := makeTarGz(t, serverBinaryName())
	withFakeEngine(t, payload, "tar.gz")

	m := New(t.TempDir())
	require.False(t, m.EngineInstalled())
	var lastDone int64
	require.NoError(t, m.InstallEngine(context.Background(), func(done, total int64) { lastDone = done }))
	assert.Equal(t, int64(len(payload)), lastDone)
	assert.True(t, m.EngineInstalled())

	// Flattened: binary + dylib at the top of engineDir, no nested dirs, no
	// traversal escape. Intra-archive soname symlinks are materialized;
	// hostile links (absolute / outside targets) are dropped.
	entries, err := os.ReadDir(m.engineDir())
	require.NoError(t, err)
	names := map[string]bool{}
	for _, e := range entries {
		names[e.Name()] = true
		assert.False(t, e.IsDir())
	}
	assert.True(t, names[serverBinaryName()])
	assert.True(t, names["libggml.dylib"])
	assert.False(t, names["link"], "hostile symlink must be dropped")
	_, err = os.Stat(filepath.Join(m.dataDir, "..", "evil.txt"))
	assert.True(t, os.IsNotExist(err), "traversal must not escape")

	// The soname chain resolves to the real file for both link names.
	for _, ln := range []string{"libx.0.dylib", "libx.dylib"} {
		st, err := os.Stat(filepath.Join(m.engineDir(), ln)) // follows symlinks
		require.NoError(t, err, ln)
		assert.True(t, st.Mode().IsRegular(), ln)
		body, err := os.ReadFile(filepath.Join(m.engineDir(), ln))
		require.NoError(t, err)
		assert.Equal(t, "real dylib", string(body), ln)
	}

	if runtime.GOOS != "windows" {
		st, err := os.Stat(filepath.Join(m.engineDir(), serverBinaryName()))
		require.NoError(t, err)
		assert.NotZero(t, st.Mode()&0o111, "server binary must be executable")
	}

	// BinaryPath prefers the managed install.
	p, ok := m.BinaryPath()
	require.True(t, ok)
	assert.Equal(t, m.managedBinaryPath(), p)

	// Idempotent.
	require.NoError(t, m.InstallEngine(context.Background(), nil))
}

func TestInstallEngine_ZipFormat(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create(serverBinaryName())
	require.NoError(t, err)
	_, _ = w.Write([]byte("fake exe"))
	w2, err := zw.Create("ggml.dll")
	require.NoError(t, err)
	_, _ = w2.Write([]byte("dll"))
	require.NoError(t, zw.Close())
	withFakeEngine(t, buf.Bytes(), "zip")

	m := New(t.TempDir())
	require.NoError(t, m.InstallEngine(context.Background(), nil))
	assert.True(t, m.EngineInstalled())
}

func TestInstallEngine_ChecksumMismatchRefuses(t *testing.T) {
	payload := makeTarGz(t, serverBinaryName())
	withFakeEngine(t, payload, "tar.gz")
	// Sabotage the pin AFTER the seam captured the real hash.
	prev := engineAssetFor
	engineAssetFor = func() (engineAsset, bool) {
		a, _ := prev()
		a.sha256 = "1111111111111111111111111111111111111111111111111111111111111111"
		return a, true
	}
	t.Cleanup(func() { engineAssetFor = prev })

	m := New(t.TempDir())
	err := m.InstallEngine(context.Background(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "checksum mismatch")
	assert.False(t, m.EngineInstalled())
	_, statErr := os.Stat(m.engineDir())
	assert.True(t, os.IsNotExist(statErr), "nothing may be installed on mismatch")
}

func TestInstallEngine_MissingServerBinaryRefuses(t *testing.T) {
	payload := makeTarGz(t, "some-other-tool") // archive without llama-server
	withFakeEngine(t, payload, "tar.gz")

	m := New(t.TempDir())
	err := m.InstallEngine(context.Background(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "did not contain")
	assert.False(t, m.EngineInstalled())
}
