package main

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// Config holds the sage-lite configuration.
type Config struct {
	Embedding  EmbeddingConfig `yaml:"embedding"`
	Encryption EncryptionConfig `yaml:"encryption"`
	DataDir    string           `yaml:"data_dir"`
	RESTAddr   string           `yaml:"rest_addr"`
	AgentKey   string           `yaml:"agent_key_file"`
	BlockTime  string           `yaml:"block_time"` // e.g. "1s", "3s"
}

// EncryptionConfig controls AES-256-GCM encryption of memory content at rest.
type EncryptionConfig struct {
	Enabled bool `yaml:"enabled"` // Whether encryption is active
}

// EmbeddingConfig configures the embedding provider.
type EmbeddingConfig struct {
	Provider  string `yaml:"provider"` // "ollama" or "hash"
	APIKey    string `yaml:"api_key,omitempty"`
	Model     string `yaml:"model,omitempty"`
	Dimension int    `yaml:"dimension,omitempty"`
	BaseURL   string `yaml:"base_url,omitempty"` // For Ollama
}

// DefaultConfig returns the default configuration.
func DefaultConfig(home string) *Config {
	return &Config{
		Embedding: EmbeddingConfig{
			Provider:  "hash",
			Dimension: 768,
		},
		DataDir:  filepath.Join(home, "data"),
		RESTAddr: "127.0.0.1:8080",
		AgentKey: filepath.Join(home, "agent.key"),
	}
}

// SageHome returns the SAGE home directory.
func SageHome() string {
	home := os.Getenv("SAGE_HOME")
	if home != "" {
		return home
	}
	userHome, err := os.UserHomeDir()
	if err != nil {
		return ".sage"
	}
	return filepath.Join(userHome, ".sage")
}

// LoadConfig loads configuration from ~/.sage/config.yaml.
// Returns default config if the file doesn't exist.
func LoadConfig() (*Config, error) {
	home := SageHome()
	cfg := DefaultConfig(home)

	configPath := filepath.Join(home, "config.yaml")
	data, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, fmt.Errorf("read config: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	// Expand ~ and ensure absolute paths
	cfg.DataDir = expandHome(cfg.DataDir)
	cfg.AgentKey = expandHome(cfg.AgentKey)
	if !filepath.IsAbs(cfg.DataDir) {
		cfg.DataDir = filepath.Join(home, cfg.DataDir)
	}
	if !filepath.IsAbs(cfg.AgentKey) {
		cfg.AgentKey = filepath.Join(home, cfg.AgentKey)
	}

	return cfg, nil
}

// expandHome replaces a leading ~ with the user's home directory.
func expandHome(path string) string {
	if len(path) < 2 || path[0] != '~' || path[1] != '/' {
		return path
	}
	userHome, err := os.UserHomeDir()
	if err != nil {
		return path
	}
	return filepath.Join(userHome, path[2:])
}

// SaveConfig writes the configuration to ~/.sage/config.yaml.
func SaveConfig(cfg *Config) error {
	home := SageHome()
	if err := os.MkdirAll(home, 0700); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	return os.WriteFile(filepath.Join(home, "config.yaml"), data, 0600)
}
