package orchestrator

import (
	"archive/zip"
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/l33tdawg/sage/internal/store"
)

// GenerateBundle creates a ZIP bundle for an agent containing keys, config, and CA cert.
// If caCertPEM is non-empty, the quorum CA certificate is included for TLS verification.
func GenerateBundle(bundleDir string, agent *store.AgentEntry, seed []byte, primaryAddr string, caCertPEM ...string) (string, error) {
	if err := os.MkdirAll(bundleDir, 0700); err != nil {
		return "", fmt.Errorf("create bundle dir: %w", err)
	}

	zipPath := filepath.Join(bundleDir, fmt.Sprintf("sage-agent-%s.zip", agent.Name))
	prefix := fmt.Sprintf("sage-agent-%s/", agent.Name)

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)

	// agent.key — Ed25519 seed (32 bytes)
	if err := addToZip(zw, prefix+"agent.key", seed); err != nil {
		return "", err
	}

	// config.yaml
	configYAML := fmt.Sprintf(`# SAGE Agent Configuration — %s
data_dir: ~/.sage/data
rest_addr: ":8080"

embedding:
  provider: hash
  dimension: 768

quorum:
  enabled: true
  p2p_addr: "tcp://0.0.0.0:26656"
  peers:
    - "%s"
`, agent.Name, primaryAddr)
	if err := addToZip(zw, prefix+"config.yaml", []byte(configYAML)); err != nil {
		return "", err
	}

	// .mcp.json — use HTTPS if CA cert is included (quorum mode with TLS).
	apiURL := "http://localhost:8080"
	if len(caCertPEM) > 0 && caCertPEM[0] != "" {
		apiURL = "https://localhost:8443"
	}
	mcpJSON := fmt.Sprintf(`{
  "mcpServers": {
    "sage": {
      "command": "sage-gui",
      "args": ["mcp"],
      "env": {
        "SAGE_API_URL": "%s"
      }
    }
  }
}`, apiURL)
	if err := addToZip(zw, prefix+".mcp.json", []byte(mcpJSON)); err != nil {
		return "", err
	}

	// SETUP.txt
	setupTxt := fmt.Sprintf(`SAGE Agent Setup — %s
================================

1. Copy this entire folder to the target machine
2. Install sage-gui: download from github.com/l33tdawg/sage/releases
3. Move agent.key to ~/.sage/agent.key
4. Move config.yaml to ~/.sage/config.yaml
5. Move .mcp.json to your project root
6. Start the agent: sage-gui serve

Agent ID: %s
Role: %s
Clearance Level: %d (%s)

This agent will connect to the primary node at %s.
After starting, the primary node will trigger a chain redeployment
to include this agent in the validator set.
`, agent.Name, agent.AgentID, agent.Role, agent.Clearance,
		clearanceLabel(agent.Clearance), primaryAddr)
	if err := addToZip(zw, prefix+"SETUP.txt", []byte(setupTxt)); err != nil {
		return "", err
	}

	// Include quorum CA certificate for TLS verification (v6.5+).
	if len(caCertPEM) > 0 && caCertPEM[0] != "" {
		if err := addToZip(zw, prefix+"ca.crt", []byte(caCertPEM[0])); err != nil {
			return "", err
		}
	}

	if err := zw.Close(); err != nil {
		return "", err
	}

	if err := os.WriteFile(zipPath, buf.Bytes(), 0600); err != nil {
		return "", err
	}

	return zipPath, nil
}

func addToZip(zw *zip.Writer, name string, data []byte) error {
	fw, err := zw.Create(name)
	if err != nil {
		return fmt.Errorf("create zip entry %s: %w", name, err)
	}
	if _, err := fw.Write(data); err != nil {
		return fmt.Errorf("write zip entry %s: %w", name, err)
	}
	return nil
}

func clearanceLabel(level int) string {
	labels := []string{"Guest", "Internal", "Confidential", "Secret", "Top Secret"}
	if level >= 0 && level < len(labels) {
		return labels[level]
	}
	return "Unknown"
}
