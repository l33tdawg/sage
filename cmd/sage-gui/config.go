package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// expandTilde replaces a leading "~" or "~/" with the actual home directory.
// This is needed because shells expand ~ but Go's os.MkdirAll does not.
func expandTilde(path string) string {
	if path == "~" || strings.HasPrefix(path, "~/") {
		if h, err := os.UserHomeDir(); err == nil {
			return filepath.Join(h, path[1:])
		}
	}
	return path
}

// Config holds the sage-gui configuration.
type Config struct {
	Embedding  EmbeddingConfig  `yaml:"embedding"`
	Encryption EncryptionConfig `yaml:"encryption"`
	Quorum     QuorumConfig     `yaml:"quorum"`
	// NOT omitempty: the default is enabled=true, so a zero FederationConfig
	// (enabled=false, the operator's explicit "off") must be written as
	// `federation: {enabled: false}`. With omitempty the off-state would be
	// stripped and LoadConfig's default-true would silently re-enable it — you
	// could never turn federation off.
	Federation FederationConfig `yaml:"federation"`
	Voter      VoterConfig      `yaml:"voter"`
	DataDir    string           `yaml:"data_dir"`
	RESTAddr   string           `yaml:"rest_addr"`
	AgentKey   string           `yaml:"agent_key_file"`
	BlockTime  string           `yaml:"block_time"` // e.g. "1s", "3s"

	// ChainID is a read-only mirror of the network's globally-unique chain_id,
	// reconciled from the authoritative CometBFT genesis on every serve (see
	// readChainIDFromGenesis). It is NOT user-editable and has no env override —
	// genesis is the source of truth; this cache just makes the id available
	// before CometBFT is up and for the federation identity/collision guard.
	ChainID string `yaml:"chain_id,omitempty"`

	// RetainBlocks is the CometBFT block-retention window: Commit reports
	// RetainHeight = height - RetainBlocks, and CometBFT prunes blocks BELOW
	// that height — the retain height itself survives, so the blockstore keeps
	// RetainBlocks+1 blocks (base = retain height through the tip, both
	// inclusive). Memory content lives in BadgerDB/SQLite, not in old blocks,
	// so pruning consensus history is safe on a personal node. 0 = mode default
	// (personal: 100000; quorum: disabled — a fresh quorum peer block-syncs
	// history from existing peers, so pruning there is opt-in). -1 = explicitly
	// keep everything. Quorum operators who do opt in should keep the window at
	// least as large as the consensus evidence max-age window (CometBFT default
	// 100000 blocks / 48h), so misbehavior evidence can still be verified
	// against retained blocks. See issue #40.
	RetainBlocks int64 `yaml:"retain_blocks,omitempty"`

	// DisableAutoUpgrade opts a personal node out of the v10.5.1 upgrade
	// auto-advance: by default a single-validator node walks the governance
	// fork ladder to the binary's compiled ceiling automatically (propose →
	// auto-vote → activate, one fork at a time), so updating the binary also
	// brings the CHAIN up to date. Quorum clusters never auto-advance
	// regardless of this knob — fork scheduling there is an operator decision.
	DisableAutoUpgrade bool `yaml:"disable_auto_upgrade,omitempty"`
}

// FederationConfig controls the v11 cross-network federation LISTENER (the
// dedicated mTLS port serving other SAGE chains). The OUTBOUND side (federated
// recall, receipt delivery) needs no listener and is always available once
// cross_fed agreements exist — this only gates the inbound surface.
type FederationConfig struct {
	Enabled bool `yaml:"enabled"` // start the mTLS federation listener
	// ListenAddr is the federation listener address (default 0.0.0.0:8444).
	// Unlike the local API this REQUIRES a verified client certificate pinned
	// to an active cross_fed agreement, so exposing it is the point.
	ListenAddr string `yaml:"listen_addr,omitempty"`
}

// VoterConfig controls the per-node memory auto-voter — the goroutine that
// signs MemoryVote/GovVote txs with the node's own consensus key so submitted
// memories reach quorum (runs-or-exits guarantee, v11).
type VoterConfig struct {
	// Enabled starts the auto-voter (default true). Disabling it is an explicit
	// operator choice — submitted memories then stay proposed until some other
	// validator votes them through — so a false here logs Info, not Warn.
	Enabled bool `yaml:"enabled"`
	// PollInterval is how often pending memories are scanned, as a Go duration
	// string (default "2s"). Unset/unparsable falls back to the default.
	PollInterval string `yaml:"poll_interval,omitempty"`
	// Required makes a voter that cannot start (missing/invalid consensus key)
	// a fatal boot error instead of a warning, so the node either votes or
	// exits — it never silently serves voter-less. Default false.
	Required bool `yaml:"required,omitempty"`
}

// QuorumConfig controls multi-validator consensus mode.
type QuorumConfig struct {
	Enabled bool     `yaml:"enabled"`            // Enable quorum mode (multi-validator)
	Peers   []string `yaml:"peers,omitempty"`    // Persistent peers (nodeID@host:port)
	P2PAddr string   `yaml:"p2p_addr,omitempty"` // P2P listen address (default: tcp://0.0.0.0:26656)
	TLSAddr string   `yaml:"tls_addr,omitempty"` // TLS REST listen address (default: 0.0.0.0:8443)
}

// EncryptionConfig controls AES-256-GCM encryption of memory content at rest.
type EncryptionConfig struct {
	Enabled bool `yaml:"enabled"` // Whether encryption is active
}

// EmbeddingConfig configures the embedding provider.
//
// Provider values:
//   - "hash"              — built-in deterministic non-semantic embeddings
//   - "ollama"            — local Ollama (POST /api/embed)
//   - "openai-compatible" — OpenAI / vLLM / LiteLLM / TEI (POST /v1/embeddings)
type EmbeddingConfig struct {
	Provider  string `yaml:"provider"` // "hash", "ollama", or "openai-compatible"
	APIKey    string `yaml:"api_key,omitempty"`
	Model     string `yaml:"model,omitempty"`
	Dimension int    `yaml:"dimension,omitempty"`
	BaseURL   string `yaml:"base_url,omitempty"` // Ollama or OpenAI-compatible base
}

// defaultVoterConfig is the voter default block. Factored out because the
// default is Enabled=TRUE (a zero VoterConfig means "voter off"), so every
// Config that isn't decoded over LoadConfig's defaults — see persistChainID's
// raw round-trip — must seed this explicitly or an absent voter block would
// silently become an explicit voter.enabled=false on the next rewrite.
func defaultVoterConfig() VoterConfig {
	return VoterConfig{
		Enabled:      true,
		PollInterval: "2s",
	}
}

// defaultFederationConfig is the federation default block. Enabled=TRUE so the
// inbound mTLS listener starts on a fresh node and the guided JOIN wizard just
// works — the listener requires a client cert pinned to an active cross_fed
// agreement (unpinned peers are rejected at the TLS handshake) and the join
// routes sit behind joinAuth, so exposing :8444 by default is the point, not a
// risk. Like the voter default, this MUST be seeded on every config that isn't
// decoded over LoadConfig's defaults (persistChainID's raw round-trip), or an
// absent federation block would re-marshal as an implicit disable and the
// listener would silently never start on the next boot — the exact bug this
// fixes.
func defaultFederationConfig() FederationConfig {
	return FederationConfig{Enabled: true}
}

// DefaultConfig returns the default configuration.
func DefaultConfig(home string) *Config {
	return &Config{
		Embedding: EmbeddingConfig{
			Provider:  "hash",
			Dimension: 768,
		},
		Voter:      defaultVoterConfig(),
		Federation: defaultFederationConfig(),
		DataDir:    filepath.Join(home, "data"),
		RESTAddr:   "127.0.0.1:8080",
		AgentKey:   filepath.Join(home, "agent.key"),
	}
}

// SageHome returns the SAGE home directory.
func SageHome() string {
	home := os.Getenv("SAGE_HOME")
	if home != "" {
		return expandTilde(home)
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
			applyEnvOverrides(cfg)
			if vErr := cfg.validate(); vErr != nil {
				return nil, vErr
			}
			return cfg, nil
		}
		return nil, fmt.Errorf("read config: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	applyEnvOverrides(cfg)
	if vErr := cfg.validate(); vErr != nil {
		return nil, vErr
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

// validate rejects contradictory configuration after the file + env merge.
// Load-time, so a misconfigured node refuses to boot instead of guessing.
func (cfg *Config) validate() error {
	if cfg.Voter.Required && !cfg.Voter.Enabled {
		return fmt.Errorf("invalid config: voter.required=true but voter.enabled=false — a required voter cannot be disabled (fix the voter block in config.yaml or SAGE_VOTER_ENABLED/SAGE_VOTER_REQUIRED)")
	}
	return nil
}

// applyEnvOverrides applies environment-variable overrides to cfg in place.
//
// Backward-compat: REST_ADDR, SAGE_EMBEDDING_PROVIDER, OLLAMA_URL, OLLAMA_MODEL
// keep their original meanings.
//
// New (for the openai-compatible provider): SAGE_EMBEDDING_BASE_URL,
// SAGE_EMBEDDING_MODEL, SAGE_EMBEDDING_API_KEY, SAGE_EMBEDDING_DIMENSION.
// The SAGE_EMBEDDING_* names take precedence over OLLAMA_* when both are set,
// because the OLLAMA_* names are misleading once a non-Ollama backend is in
// use (e.g. vLLM at /v1/embeddings).
func applyEnvOverrides(cfg *Config) {
	if envAddr := os.Getenv("REST_ADDR"); envAddr != "" {
		cfg.RESTAddr = envAddr
	}
	if envProvider := os.Getenv("SAGE_EMBEDDING_PROVIDER"); envProvider != "" {
		cfg.Embedding.Provider = envProvider
	}
	// Ollama-named overrides (legacy).
	if envURL := os.Getenv("OLLAMA_URL"); envURL != "" {
		cfg.Embedding.BaseURL = envURL
	}
	if envModel := os.Getenv("OLLAMA_MODEL"); envModel != "" {
		cfg.Embedding.Model = envModel
	}
	// Provider-agnostic overrides — preferred for openai-compatible deployments.
	if envURL := os.Getenv("SAGE_EMBEDDING_BASE_URL"); envURL != "" {
		cfg.Embedding.BaseURL = envURL
	}
	if envModel := os.Getenv("SAGE_EMBEDDING_MODEL"); envModel != "" {
		cfg.Embedding.Model = envModel
	}
	if envKey := os.Getenv("SAGE_EMBEDDING_API_KEY"); envKey != "" {
		cfg.Embedding.APIKey = envKey
	}
	if envDim := os.Getenv("SAGE_EMBEDDING_DIMENSION"); envDim != "" {
		if n, err := strconv.Atoi(envDim); err == nil && n > 0 {
			cfg.Embedding.Dimension = n
		}
	}
	// Memory auto-voter overrides (runs-or-exits guarantee, v11).
	if v := os.Getenv("SAGE_VOTER_ENABLED"); v != "" {
		if b, ok := envBool("SAGE_VOTER_ENABLED", v); ok {
			cfg.Voter.Enabled = b
		}
	}
	if envInterval := os.Getenv("SAGE_VOTER_POLL_INTERVAL"); envInterval != "" {
		cfg.Voter.PollInterval = envInterval
	}
	if v := os.Getenv("SAGE_VOTER_REQUIRED"); v != "" {
		if b, ok := envBool("SAGE_VOTER_REQUIRED", v); ok {
			cfg.Voter.Required = b
		}
	}
}

// envBool parses a boolean env override, accepting the strconv.ParseBool set plus the
// common yes/no/on/off spellings. An unrecognized non-empty value is a likely operator
// mistake — and for a fail-fast safety flag like SAGE_VOTER_REQUIRED, silently treating
// "yes" as false would leave the operator thinking the gate is armed when it isn't — so
// it warns to stderr and reports ok=false rather than defaulting quietly.
func envBool(name, val string) (value bool, ok bool) {
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "1", "t", "true", "yes", "y", "on":
		return true, true
	case "0", "f", "false", "no", "n", "off":
		return false, true
	default:
		fmt.Fprintf(os.Stderr, "SAGE: %s=%q is not a recognized boolean (use true/false); ignoring\n", name, val)
		return false, false
	}
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

// persistChainID writes ONLY the chain_id into config.yaml, preserving every
// other field in its on-disk (raw, un-expanded) form. This exists because
// LoadConfig expands DataDir/AgentKey in place (tilde/relative -> absolute), so
// SaveConfig(cfg) after a load would bake those into absolute paths and silently
// drop any tilde/relative form the operator wrote — which then breaks if the
// config is ever synced to a different home/user/SAGE_HOME. The chain_id
// reconcile-on-boot fires for every existing node's first v11 boot, so it must
// not rewrite paths. Re-reads the raw file and rewrites only the delta.
func persistChainID(chainID string) error {
	home := SageHome()
	if err := os.MkdirAll(home, 0700); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}
	configPath := filepath.Join(home, "config.yaml")

	data, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No config yet (fresh node): a new file with absolute default paths
			// is fine — there is no operator-authored tilde/relative form to lose.
			cfg := DefaultConfig(home)
			cfg.ChainID = chainID
			return SaveConfig(cfg)
		}
		return fmt.Errorf("read config: %w", err)
	}

	// Unmarshal into a RAW Config (no path expansion) so paths round-trip verbatim.
	// Voter AND federation defaults are seeded: both default to enabled=true, so
	// without the seed a config that omits either block would re-marshal as an
	// implicit disable and silently kill the auto-voter / federation listener on
	// the next boot. (Keys present in the file still win — the seed only fills
	// absent ones.)
	raw := Config{Voter: defaultVoterConfig(), Federation: defaultFederationConfig()}
	if parseErr := yaml.Unmarshal(data, &raw); parseErr != nil {
		return fmt.Errorf("parse config: %w", parseErr)
	}
	if raw.ChainID == chainID {
		return nil // no drift
	}
	raw.ChainID = chainID
	out, err := yaml.Marshal(&raw)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	return os.WriteFile(configPath, out, 0600)
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
