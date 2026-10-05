package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLanternPrivateListenerYAML(t *testing.T) {
	for _, test := range []struct {
		name, yaml string
		allowed    bool
	}{
		{"private", "federation: {enabled: false}\nquorum: {enabled: false}\n", true},
		{"federation", "federation: {enabled: true}\n", false},
		{"quorum", "federation: {enabled: false}\nquorum: {enabled: true}\n", false},
		{"omitted_defaults", "{}\n", false},
		{"null_federation", "federation: null\n", false},
		{"duplicate", "federation: {enabled: false}\nfederation: {enabled: true}\n", false},
		{"wildcard_overridden", "federation: {enabled: false}\nrest_addr: 0.0.0.0:8080\nquorum: {enabled: false, tls_addr: '0.0.0.0:8443'}\n", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("SAGE_HOME", home)
			t.Setenv("SAGE_LANTERN_PRIVATE_LISTENERS", "1")
			t.Setenv("REST_ADDR", "127.0.0.1:8080")
			t.Setenv("SAGE_TLS_ADDR", "127.0.0.1:8443")
			t.Setenv("SAGE_CMT_RPC_ADDR", "tcp://127.0.0.1:26657")
			t.Setenv("SAGE_CMT_P2P_ADDR", "tcp://127.0.0.1:26656")
			if err := os.WriteFile(filepath.Join(home, "config.yaml"), []byte(test.yaml), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig()
			if (err == nil) != test.allowed {
				t.Fatalf("allowed=%v error=%v", test.allowed, err)
			}
			if err == nil && (cfg.RESTAddr != "127.0.0.1:8080" || cfg.Quorum.TLSAddr != "127.0.0.1:8443") {
				t.Fatal("listener override lost")
			}
			entries, err := os.ReadDir(home)
			if err != nil || len(entries) != 1 {
				t.Fatal("config check created state", err)
			}
		})
	}
}

func TestLanternPrivateListenerEnvironmentRejects(t *testing.T) {
	t.Setenv("SAGE_LANTERN_PRIVATE_LISTENERS", "1")
	t.Setenv("SAGE_CMT_RPC_ADDR", "tcp://127.0.0.1:26657")
	t.Setenv("SAGE_CMT_P2P_ADDR", "tcp://127.0.0.1:26656")
	cfg := DefaultConfig(t.TempDir())
	cfg.Federation.Enabled = false
	cfg.Quorum.TLSAddr = "127.0.0.1:8443"
	for _, name := range []string{"SAGE_CMT_RPC_ADDR", "SAGE_CMT_P2P_ADDR"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "tcp://0.0.0.0:26657")
			if enforceLanternPrivateListeners(cfg) == nil {
				t.Fatal("accepted external address")
			}
		})
	}
	t.Setenv("SAGE_LANTERN_PRIVATE_LISTENERS", "false")
	if enforceLanternPrivateListeners(cfg) == nil {
		t.Fatal("accepted invalid policy")
	}
	t.Setenv("SAGE_LANTERN_PRIVATE_LISTENERS", "")
	cfg.Federation.Enabled = true
	if enforceLanternPrivateListeners(cfg) != nil {
		t.Fatal("changed generic policy")
	}
}

func TestLanternPrivateListenerRechecksConfig(t *testing.T) {
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)
	t.Setenv("SAGE_LANTERN_PRIVATE_LISTENERS", "1")
	t.Setenv("REST_ADDR", "127.0.0.1:8080")
	t.Setenv("SAGE_TLS_ADDR", "127.0.0.1:8443")
	t.Setenv("SAGE_CMT_RPC_ADDR", "tcp://127.0.0.1:26657")
	t.Setenv("SAGE_CMT_P2P_ADDR", "tcp://127.0.0.1:26656")
	if _, err := LoadConfig(); err == nil {
		t.Fatal("missing configuration accepted")
	}
	path := filepath.Join(home, "config.yaml")
	for _, enabled := range []string{"false", "true"} {
		if err := os.WriteFile(path, []byte("quorum: {enabled: false}\nfederation: {enabled: "+enabled+"}\n"), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := LoadConfig()
		if (err == nil) != (enabled == "false") {
			t.Fatalf("config was not revalidated: %v", err)
		}
	}
}
