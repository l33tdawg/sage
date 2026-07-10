package tunnelclientd

import (
	"archive/zip"
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProfileYAMLUsesNonConflictingHealthPortAndEnvKey(t *testing.T) {
	y := profileYAML("tunnel_0123456789abcdef0123456789abcdef", "'/Applications/SAGE.app/Contents/MacOS/sage-gui' mcp", 8081)

	assert.Contains(t, y, `tunnel_id: "tunnel_0123456789abcdef0123456789abcdef"`)
	assert.Contains(t, y, `api_key: env:CONTROL_PLANE_API_KEY`)
	assert.Contains(t, y, `listen_addr: "127.0.0.1:8081"`)
	assert.Contains(t, y, `command: "'/Applications/SAGE.app/Contents/MacOS/sage-gui' mcp"`)
	assert.NotContains(t, y, "sk-")
	assert.NotContains(t, y, "127.0.0.1:8080")
}

func TestInstallZipInstallsBinaryOnly(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("nested/" + exeName())
	require.NoError(t, err)
	_, _ = w.Write([]byte("fake tunnel-client"))
	w2, err := zw.Create("README.md")
	require.NoError(t, err)
	_, _ = w2.Write([]byte("not installed"))
	require.NoError(t, zw.Close())

	m := New(t.TempDir())
	require.NoError(t, m.installZip(buf.Bytes()))
	assert.True(t, m.EngineInstalled())
	body, err := os.ReadFile(m.managedBinaryPath())
	require.NoError(t, err)
	assert.Equal(t, "fake tunnel-client", string(body))

	_, err = os.Stat(filepath.Join(m.engineDir(), "README.md"))
	assert.True(t, os.IsNotExist(err))
	if runtime.GOOS != "windows" {
		st, err := os.Stat(m.managedBinaryPath())
		require.NoError(t, err)
		assert.NotZero(t, st.Mode()&0o111)
	}
}
