package orchestrator

import (
	"archive/zip"
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/l33tdawg/sage/internal/store"
)

// TestGenerateBundleBlankP2PAddr asserts the generated agent config.yaml leaves
// quorum.p2p_addr blank. node.go only applies the SAGE_CMT_P2P_ADDR override (and
// its tcp://0.0.0.0:26656 default) when p2p_addr is empty, so a hardcoded value
// here would shadow the override and block two quorum agents coexisting on one
// host — the gap this change closes.
func TestGenerateBundleBlankP2PAddr(t *testing.T) {
	dir := t.TempDir()
	agent := &store.AgentEntry{Name: "alpha", AgentID: "abc123", Role: "member", Clearance: 1}

	zipPath, err := GenerateBundle(dir, agent, make([]byte, 32), "tcp://primary:26656")
	if err != nil {
		t.Fatalf("GenerateBundle: %v", err)
	}

	cfg := readZipEntry(t, zipPath, "sage-agent-alpha/config.yaml")
	if !strings.Contains(cfg, `p2p_addr: ""`) {
		t.Errorf("config.yaml should leave p2p_addr blank, got:\n%s", cfg)
	}
	if strings.Contains(cfg, `p2p_addr: "tcp://0.0.0.0:26656"`) {
		t.Errorf("config.yaml must not hardcode the P2P port (it shadows SAGE_CMT_P2P_ADDR):\n%s", cfg)
	}
}

func readZipEntry(t *testing.T, zipPath, name string) string {
	t.Helper()
	data, err := os.ReadFile(zipPath)
	if err != nil {
		t.Fatalf("read zip: %v", err)
	}
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("open zip: %v", err)
	}
	for _, f := range zr.File {
		if f.Name == name {
			rc, err := f.Open()
			if err != nil {
				t.Fatalf("open entry %q: %v", name, err)
			}
			defer rc.Close()
			b, _ := io.ReadAll(rc)
			return string(b)
		}
	}
	t.Fatalf("entry %q not found in bundle", name)
	return ""
}
