//go:build !windows

package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	cmtcryptoed "github.com/cometbft/cometbft/crypto/ed25519"
	cmtjson "github.com/cometbft/cometbft/libs/json"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/privval"
	cmttypes "github.com/cometbft/cometbft/types"
	sageabci "github.com/l33tdawg/sage/internal/abci"
	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v3"
)

const (
	freshNodeClaim     = ".fresh-node-init-v1.claim"
	freshNodeReady     = ".fresh-node-init-v1.ready"
	freshNodeReadyTemp = ".fresh-node-init-v1.ready.tmp"
	freshNodeSchema    = "sage.fresh-node-init.v1"
	freshNodeMaxFile   = 1 << 20
)

// A receipt is a local completion fence, not an additional consensus origin.
// Its artifact is independently authenticated by the signed genesis manifest.
type freshNodeReceipt struct {
	Schema        string                  `json:"schema"`
	GenesisSHA256 string                  `json:"genesis_sha256"`
	ChainID       string                  `json:"chain_id"`
	AppVersion    uint64                  `json:"app_version"`
	DataDir       string                  `json:"data_dir"`
	TransportPath string                  `json:"transport_path"`
	CompanionPath string                  `json:"companion_path,omitempty"`
	ValidatorID   string                  `json:"validator_id"`
	TransportID   string                  `json:"transport_id"`
	NodeID        string                  `json:"node_id"`
	BinaryVersion string                  `json:"binary_version"`
	ForkVersion   int                     `json:"fork_version"`
	ClaimSHA256   string                  `json:"claim_sha256,omitempty"`
	Inputs        map[string]freshNodePin `json:"inputs"`
}

type freshNodePin struct {
	SHA256 string `json:"sha256"`
	Mode   uint32 `json:"mode"`
	Size   int64  `json:"size"`
}

type freshNodeFS struct {
	home string
	root *os.File
}
type freshNodePrepared struct {
	cfg       *Config
	receipt   freshNodeReceipt
	manifest  sageabci.FreshGenesisManifest
	genesis   *cmttypes.GenesisDoc
	files     map[string]freshNodePin
	transport ed25519.PrivateKey
	node      *p2p.NodeKey
}

func runInitFreshNode(args []string) error {
	if len(args) != 2 || args[0] != "--genesis-sha256" || !freshNodeDigest(args[1]) {
		return errors.New("usage: sage-gui init-fresh-node --genesis-sha256 <canonical lowercase SHA-256 of the shared genesis>")
	}
	if os.Getenv("SAGE_INSTANCE_LOCK_FD") != "" {
		return errors.New("fresh-node initialization does not accept an inherited instance lock")
	}
	return initializeFreshNode(SageHome(), args[1])
}

func freshNodeDigest(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == sha256.Size && hex.EncodeToString(b) == s
}
func freshNodeSHA(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

// Resolve only the parent alias (e.g. macOS /var); the home itself must not be
// a symlink. Every descendant operation is relative to this held directory fd.
func openFreshNodeFS(home string) (*freshNodeFS, error) {
	abs, err := filepath.Abs(home)
	if err != nil {
		return nil, err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(abs))
	if err != nil {
		return nil, err
	}
	path := filepath.Join(parent, filepath.Base(abs))
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open owned fresh-node home: %w", err)
	}
	f := &freshNodeFS{home: abs, root: os.NewFile(uintptr(fd), path)}
	if homeCheckErr := freshNodeCheckFD(fd, true, true); homeCheckErr != nil {
		f.root.Close()
		return nil, homeCheckErr
	}
	return f, nil
}

func freshNodeCheckFD(fd int, directory, private bool) error {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return err
	}
	if st.Uid != uint32(os.Geteuid()) {
		return errors.New("fresh-node input must be owned by the current user")
	}
	if directory {
		if st.Mode&unix.S_IFMT != unix.S_IFDIR || st.Mode&0077 != 0 {
			return errors.New("fresh-node directories must be private directories")
		}
	} else if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Nlink != 1 || st.Mode&0111 != 0 || st.Mode&0022 != 0 || (private && st.Mode&0077 != 0) {
		return errors.New("fresh-node input must be a singly linked, non-executable regular file with safe permissions")
	}
	return nil
}

func (f *freshNodeFS) relative(path string) (string, error) {
	if !filepath.IsAbs(path) {
		path = filepath.Join(f.home, path)
	}
	rel, err := filepath.Rel(f.home, filepath.Clean(path))
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", errors.New("fresh-node inputs must be strictly inside SAGE_HOME")
	}
	return filepath.ToSlash(rel), nil
}

func (f *freshNodeFS) parent(path string) (int, string, error) {
	if path == "" || path != filepath.ToSlash(filepath.Clean(path)) || filepath.IsAbs(path) || path == ".." || strings.HasPrefix(path, "../") {
		return -1, "", errors.New("invalid relative fresh-node path")
	}
	fd, err := unix.Dup(int(f.root.Fd()))
	if err != nil {
		return -1, "", err
	}
	parts := strings.Split(path, "/")
	for _, part := range parts[:len(parts)-1] {
		next, openErr := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		closeErr := unix.Close(fd)
		if openErr != nil {
			// The open failure remains primary if closing its parent also failed.
			return -1, "", openErr
		}
		if closeErr != nil {
			_ = unix.Close(next) // Preserve the parent-close failure during cleanup.
			return -1, "", closeErr
		}
		if checkErr := freshNodeCheckFD(next, true, true); checkErr != nil {
			_ = unix.Close(next) // Preserve the validation failure during cleanup.
			return -1, "", checkErr
		}
		fd = next
	}
	return fd, parts[len(parts)-1], nil
}

func (f *freshNodeFS) read(path string, private bool) (result []byte, pin freshNodePin, resultErr error) {
	fd, name, err := f.parent(path)
	if err != nil {
		return nil, freshNodePin{}, err
	}
	defer func() {
		// Cleanup must not mask a read/validation error, or hide its own failure.
		if closeErr := unix.Close(fd); closeErr != nil && resultErr == nil {
			result, pin, resultErr = nil, freshNodePin{}, closeErr
		}
	}()
	child, err := unix.Openat(fd, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, freshNodePin{}, err
	}
	file := os.NewFile(uintptr(child), path)
	defer file.Close()
	if checkErr := freshNodeCheckFD(child, false, private); checkErr != nil {
		return nil, freshNodePin{}, checkErr
	}
	before, err := file.Stat()
	if err != nil {
		return nil, freshNodePin{}, err
	}
	if before.Size() > freshNodeMaxFile {
		return nil, freshNodePin{}, errors.New("fresh-node input exceeds 1 MiB")
	}
	data, err := io.ReadAll(io.LimitReader(file, freshNodeMaxFile+1))
	if err != nil {
		return nil, freshNodePin{}, err
	}
	after, err := file.Stat()
	if err != nil {
		return nil, freshNodePin{}, err
	}
	if len(data) > freshNodeMaxFile || int64(len(data)) != before.Size() || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return nil, freshNodePin{}, errors.New("fresh-node input changed while reading")
	}
	return data, freshNodePin{SHA256: freshNodeSHA(data), Mode: uint32(before.Mode().Perm()), Size: int64(len(data))}, nil
}

func (f *freshNodeFS) create(path string, data []byte) (resultErr error) {
	fd, name, err := f.parent(path)
	if err != nil {
		return err
	}
	defer func() {
		// Keep write/fsync failures primary; otherwise report the close failure.
		if closeErr := unix.Close(fd); resultErr == nil {
			resultErr = closeErr
		}
	}()
	child, err := unix.Openat(fd, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(child), path)
	_, writeErr := io.Copy(file, bytes.NewReader(data))
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	return unix.Fsync(fd)
}

func (f *freshNodeFS) sameHome() error {
	other, err := openFreshNodeFS(f.home)
	if err != nil {
		return err
	}
	defer other.root.Close()
	a, err := f.root.Stat()
	if err != nil {
		return err
	}
	b, err := other.root.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(a, b) {
		return errors.New("SAGE_HOME was replaced during initialization")
	}
	return nil
}

// Bound directory enumeration as well as file reads. Empty unexpected dirs,
// recovery artifacts, SQL/Badger files, symlinks and partial claims all refuse.
func (f *freshNodeFS) checkPristine(allowed map[string]bool) error {
	dirs := map[string]bool{".": true}
	for path := range allowed {
		for dir := filepath.ToSlash(filepath.Dir(path)); dir != "."; dir = filepath.ToSlash(filepath.Dir(dir)) {
			dirs[dir] = true
		}
	}
	visits := 0
	var walk func(string, int) error
	walk = func(path string, depth int) error {
		if depth > 16 {
			return errors.New("fresh-node directory depth exceeds limit")
		}
		var fd int
		if path == "." {
			var err error
			fd, err = unix.Openat(int(f.root.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
			if err != nil {
				return err
			}
		} else {
			parent, name, err := f.parent(path)
			if err != nil {
				return err
			}
			fd, err = unix.Openat(parent, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
			closeErr := unix.Close(parent)
			if err != nil {
				return err // Preserve the open failure if parent cleanup also failed.
			}
			if closeErr != nil {
				_ = unix.Close(fd) // Preserve the parent-close failure during cleanup.
				return closeErr
			}
		}
		file := os.NewFile(uintptr(fd), path)
		defer file.Close()
		if dirCheckErr := freshNodeCheckFD(fd, true, true); dirCheckErr != nil {
			return dirCheckErr
		}
		entries, err := file.ReadDir(65)
		if err != nil && err != io.EOF {
			return err
		}
		for _, entry := range entries {
			visits++
			if visits > 64 {
				return errors.New("fresh-node layout exceeds 64 entries")
			}
			child := entry.Name()
			if path != "." {
				child = path + "/" + child
			}
			if entry.IsDir() {
				if !dirs[child] {
					return fmt.Errorf("unexpected fresh-node directory %s", child)
				}
				if walkErr := walk(child, depth+1); walkErr != nil {
					return walkErr
				}
			} else {
				if !allowed[child] {
					return fmt.Errorf("unexpected fresh-node evidence %s", child)
				}
				if _, _, readErr := f.read(child, child != "config.yaml" && !strings.HasSuffix(child, "genesis.json") && !strings.HasSuffix(child, "config.toml")); readErr != nil {
					return fmt.Errorf("unsafe fresh-node file %s: %w", child, readErr)
				}
			}
		}
		return nil
	}
	return walk(".", 0)
}

func (f *freshNodeFS) config() (*Config, error) {
	data, _, err := f.read("config.yaml", true)
	if err != nil {
		return nil, err
	}
	cfg := DefaultConfig(f.home)
	private, err := privateListenerPolicy()
	if err != nil {
		return nil, err
	}
	if listenerErr := validatePrivateListenerYAML(data, private); listenerErr != nil {
		return nil, listenerErr
	}
	if configDecodeErr := yaml.Unmarshal(data, cfg); configDecodeErr != nil {
		return nil, configDecodeErr
	}
	applyEnvOverrides(cfg)
	if configValidateErr := cfg.validate(); configValidateErr != nil {
		return nil, configValidateErr
	}
	normalizeConfigPaths(cfg, f.home)
	return cfg, nil
}

func freshNodeJSON(data []byte, target any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return errors.New("trailing fresh-node JSON")
	}
	return nil
}

func (f *freshNodeFS) prepare(cfg *Config, expectedDigest string, unused bool) (*freshNodePrepared, error) {
	dataDir, err := f.relative(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	transportPath, err := f.relative(cfg.AgentKey)
	if err != nil {
		return nil, err
	}
	companionPath := ""
	if cfg.VendoredAgentBootstrap != nil {
		companionPath, err = f.relative(cfg.VendoredAgentBootstrap.AgentKeyFile)
		if err != nil {
			return nil, err
		}
	}
	base := dataDir + "/cometbft/"
	if transportPath == companionPath || transportPath == freshNodeClaim || transportPath == freshNodeReady || transportPath == freshNodeReadyTemp || transportPath == versionFile || transportPath == forkVersionFile || transportPath == "sage.instance.lock" || companionPath == freshNodeClaim || companionPath == freshNodeReady || companionPath == freshNodeReadyTemp || companionPath == versionFile || companionPath == forkVersionFile || companionPath == "sage.instance.lock" || strings.HasPrefix(transportPath, dataDir+"/") || (companionPath != "" && strings.HasPrefix(companionPath, dataDir+"/")) || transportPath == "config.yaml" || companionPath == "config.yaml" {
		return nil, errors.New("fresh-node identity paths overlap configuration or consensus data")
	}
	p := &freshNodePrepared{cfg: cfg, files: map[string]freshNodePin{}}
	read := func(path string, private bool) ([]byte, error) {
		data, pin, readErr := f.read(path, private)
		if readErr == nil {
			p.files[path] = pin
		}
		return data, readErr
	}
	genesisBytes, err := read(base+"config/genesis.json", false)
	if err != nil {
		return nil, err
	}
	if freshNodeSHA(genesisBytes) != expectedDigest {
		return nil, errors.New("shared genesis SHA-256 differs from the required artifact")
	}
	var genesis cmttypes.GenesisDoc
	if decodeErr := cmtjson.Unmarshal(genesisBytes, &genesis); decodeErr != nil {
		return nil, decodeErr
	}
	if genesisErr := genesis.ValidateAndComplete(); genesisErr != nil {
		return nil, genesisErr
	}
	// Match InitChain's fresh-origin envelope checks before claiming this home.
	if len(genesis.AppState) > 128<<10 {
		return nil, errors.New("fresh genesis app_state exceeds 128 KiB")
	}
	var appState struct {
		Sage struct {
			InitialAdmin string          `json:"initial_admin"`
			Bootstrap    json.RawMessage `json:"fresh_bootstrap"`
			Old          json.RawMessage `json:"app_v23_bootstrap"`
		} `json:"sage"`
	}
	if appStateErr := json.Unmarshal(genesis.AppState, &appState); appStateErr != nil || len(appState.Sage.Bootstrap) == 0 || bytes.Equal(bytes.TrimSpace(appState.Sage.Bootstrap), []byte("null")) {
		return nil, errors.New("shared artifact has no fresh genesis manifest")
	}
	if len(appState.Sage.Old) > 0 && string(appState.Sage.Old) != "null" {
		return nil, errors.New("fresh and app-v23 genesis manifests cannot coexist")
	}
	var manifest sageabci.FreshGenesisManifest
	if manifestDecodeErr := freshNodeJSON(appState.Sage.Bootstrap, &manifest); manifestDecodeErr != nil {
		return nil, fmt.Errorf("decode fresh genesis manifest: %w", manifestDecodeErr)
	}
	if _, manifestVerifyErr := sageabci.VerifyFreshGenesisManifest(genesis.ChainID, manifest); manifestVerifyErr != nil {
		return nil, manifestVerifyErr
	}
	if genesis.InitialHeight != 1 || genesis.ConsensusParams == nil || genesis.ConsensusParams.Version.App != manifest.AppVersion || appState.Sage.InitialAdmin != manifest.RootID || (cfg.ChainID != "" && cfg.ChainID != genesis.ChainID) || len(genesis.Validators) != len(manifest.Validators) {
		return nil, errors.New("shared genesis header differs from signed origin")
	}
	powers := map[string]int64{}
	for _, v := range manifest.Validators {
		powers[v.ValidatorID] = v.Power
	}
	for _, v := range genesis.Validators {
		pub, ok := v.PubKey.(cmtcryptoed.PubKey)
		if !ok || len(pub) != ed25519.PublicKeySize {
			return nil, errors.New("fresh validator must be Ed25519")
		}
		id := hex.EncodeToString(pub)
		if powers[id] != v.Power {
			return nil, errors.New("actual validator roster differs from signed roster")
		}
		delete(powers, id)
	}
	if len(powers) != 0 {
		return nil, errors.New("signed validator roster is incomplete")
	}
	keyBytes, err := read(base+"config/priv_validator_key.json", true)
	if err != nil {
		return nil, err
	}
	var validator privval.FilePVKey
	if validatorDecodeErr := cmtjson.Unmarshal(keyBytes, &validator); validatorDecodeErr != nil {
		return nil, validatorDecodeErr
	}
	secret, ok := validator.PrivKey.(cmtcryptoed.PrivKey)
	if !ok || len(secret) != ed25519.PrivateKeySize || !bytes.Equal(ed25519.NewKeyFromSeed(secret[:ed25519.SeedSize]), secret) || validator.PubKey == nil || !bytes.Equal(secret.PubKey().Bytes(), validator.PubKey.Bytes()) || !bytes.Equal(secret.PubKey().Address(), validator.Address) {
		return nil, errors.New("invalid local validator identity")
	}
	validatorID := hex.EncodeToString(validator.PubKey.Bytes())
	found := false
	for _, v := range manifest.Validators {
		if v.ValidatorID == validatorID {
			found = true
		}
	}
	if !found {
		return nil, errors.New("local validator is not in the signed roster")
	}
	if len(manifest.Validators) > 1 && !cfg.Quorum.Enabled {
		return nil, errors.New("multi-validator fresh genesis requires quorum.enabled")
	}
	if unused && !reflect.DeepEqual(cfg.Quorum.StateSync, QuorumStateSyncConfig{}) {
		return nil, errors.New("fresh initialization refuses state-sync or recovery configuration")
	}
	stateBytes, err := read(base+"data/priv_validator_state.json", true)
	if err != nil {
		return nil, err
	}
	if signingVerifyErr := validateVendoredValidatorStateBytes(stateBytes, cmtcryptoed.PubKey(secret.PubKey().Bytes()), genesis.ChainID); signingVerifyErr != nil {
		return nil, fmt.Errorf("verify fresh-node signing state: %w", signingVerifyErr)
	}
	if unused {
		var state privval.FilePVLastSignState
		if signingDecodeErr := cmtjson.Unmarshal(stateBytes, &state); signingDecodeErr != nil {
			return nil, signingDecodeErr
		}
		if state.Height != 0 || state.Round != 0 || state.Step != 0 || len(state.Signature) != 0 || len(state.SignBytes) != 0 {
			return nil, errors.New("fresh initializer requires unused validator signing state")
		}
	}
	transportBytes, err := read(transportPath, true)
	if err == nil {
		p.transport, err = decodeEd25519PrivateKey(transportBytes)
		if err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) || !unused {
		return nil, err
	}
	if p.transport != nil && (appV23AgentIDForKey(p.transport) == manifest.RootID || appV23AgentIDForKey(p.transport) == validatorID || (manifest.Agent != nil && appV23AgentIDForKey(p.transport) == manifest.Agent.AgentID)) {
		return nil, errors.New("node transport identity must be distinct from Root, validator and enrolled Companion")
	}
	nodeBytes, err := read(base+"config/node_key.json", true)
	if err == nil {
		var node p2p.NodeKey
		if nodeDecodeErr := cmtjson.Unmarshal(nodeBytes, &node); nodeDecodeErr != nil {
			return nil, nodeDecodeErr
		}
		key, ok := node.PrivKey.(cmtcryptoed.PrivKey)
		if !ok || len(key) != ed25519.PrivateKeySize || !bytes.Equal(ed25519.NewKeyFromSeed(key[:ed25519.SeedSize]), key) {
			return nil, errors.New("invalid node P2P identity")
		}
		if id := hex.EncodeToString(key.PubKey().Bytes()); id == manifest.RootID || id == validatorID || (manifest.Agent != nil && id == manifest.Agent.AgentID) || (p.transport != nil && id == appV23AgentIDForKey(p.transport)) {
			return nil, errors.New("P2P node key must be distinct from authority and other node identities")
		}
		p.node = &node
	} else if !errors.Is(err, os.ErrNotExist) || !unused {
		return nil, err
	}
	if companionPath != "" {
		b, companionReadErr := read(companionPath, true)
		if companionReadErr != nil {
			return nil, companionReadErr
		}
		key, companionKeyErr := decodeEd25519PrivateKey(b)
		if companionKeyErr != nil {
			return nil, companionKeyErr
		}
		a := manifest.Agent
		boot := cfg.VendoredAgentBootstrap
		if a == nil || appV23AgentIDForKey(key) != a.AgentID || a.Profile != "companion" || a.Capabilities != 15 || a.HomeDomain != boot.HomeDomain || a.Clearance != boot.Clearance {
			return nil, errors.New("configured local Companion differs from signed genesis")
		}
	}
	for _, optional := range []string{base + "config/config.toml"} {
		if _, optionalReadErr := read(optional, false); optionalReadErr != nil && !errors.Is(optionalReadErr, os.ErrNotExist) {
			return nil, optionalReadErr
		}
	}
	if _, configReadErr := read("config.yaml", true); configReadErr != nil {
		return nil, configReadErr
	}
	for _, path := range []string{transportPath, base + "config/node_key.json"} {
		fd, _, parentErr := f.parent(path)
		if parentErr != nil {
			return nil, fmt.Errorf("prepare identity parent: %w", parentErr)
		}
		if closeErr := unix.Close(fd); closeErr != nil {
			return nil, fmt.Errorf("close identity parent: %w", closeErr)
		}
	}
	p.genesis = &genesis
	p.manifest = manifest
	p.receipt = freshNodeReceipt{Schema: freshNodeSchema, GenesisSHA256: expectedDigest, ChainID: genesis.ChainID, AppVersion: manifest.AppVersion, DataDir: dataDir, TransportPath: transportPath, CompanionPath: companionPath, ValidatorID: validatorID, BinaryVersion: version, ForkVersion: ConsensusForkVersion, Inputs: p.files}
	if p.transport != nil {
		p.receipt.TransportID = appV23AgentIDForKey(p.transport)
	}
	if p.node != nil {
		p.receipt.NodeID = string(p.node.ID())
	}
	return p, nil
}

func initializeFreshNode(home, expectedDigest string) (resultErr error) {
	if !freshNodeDigest(expectedDigest) {
		return errors.New("invalid genesis digest")
	}
	f, err := openFreshNodeFS(home)
	if err != nil {
		return err
	}
	defer f.root.Close()
	// Use the same inode/lock as serve, without following a lock-file symlink.
	lockFD, err := unix.Openat(int(f.root.Fd()), "sage.instance.lock", unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return err
	}
	defer func() {
		// Never replace an initialization/unlock failure with cleanup failure.
		if closeErr := unix.Close(lockFD); resultErr == nil {
			resultErr = closeErr
		}
	}()
	if checkErr := freshNodeCheckFD(lockFD, false, true); checkErr != nil {
		return checkErr
	}
	if lockErr := unix.Flock(lockFD, unix.LOCK_EX|unix.LOCK_NB); lockErr != nil {
		return errors.New("node is already locked")
	}
	defer func() {
		// Keep initialization failures primary; otherwise report unlock failure.
		if unlockErr := unix.Flock(lockFD, unix.LOCK_UN); resultErr == nil {
			resultErr = unlockErr
		}
	}()
	cfg, err := f.config()
	if err != nil {
		return err
	}
	p, err := f.prepare(cfg, expectedDigest, true)
	if err != nil {
		return err
	}
	allowed := map[string]bool{"config.yaml": true, "sage.instance.lock": true, p.receipt.TransportPath: true}
	base := p.receipt.DataDir + "/cometbft/"
	for _, path := range []string{base + "config/genesis.json", base + "config/priv_validator_key.json", base + "config/node_key.json", base + "config/config.toml", base + "data/priv_validator_state.json"} {
		allowed[path] = true
	}
	if p.receipt.CompanionPath != "" {
		allowed[p.receipt.CompanionPath] = true
	}
	claim, _, claimErr := f.read(freshNodeClaim, true)
	ready, _, readyErr := f.read(freshNodeReady, true)
	completed := claimErr == nil && readyErr == nil
	if completed {
		for _, path := range []string{freshNodeClaim, freshNodeReady, versionFile, forkVersionFile} {
			allowed[path] = true
		}
	} else if !errors.Is(claimErr, os.ErrNotExist) || !errors.Is(readyErr, os.ErrNotExist) {
		return errors.New("fresh-node initialization is incomplete or corrupt; automatic repair is refused")
	}
	if pristineErr := f.checkPristine(allowed); pristineErr != nil {
		return pristineErr
	}
	if completed {
		var previous freshNodeReceipt
		if readyDecodeErr := freshNodeJSON(ready, &previous); readyDecodeErr != nil {
			return readyDecodeErr
		}
		var original freshNodeReceipt
		if claimDecodeErr := freshNodeJSON(claim, &original); claimDecodeErr != nil {
			return claimDecodeErr
		}
		if claimBindingErr := validateFreshNodeClaim(original, previous, claim); claimBindingErr != nil {
			return claimBindingErr
		}
		if previous.BinaryVersion != version || previous.ForkVersion != ConsensusForkVersion {
			return errors.New("completed initializer differs from this request")
		}
		if receiptErr := validateFreshNodeReceipt(f, p, previous, true); receiptErr != nil {
			return receiptErr
		}
		return nil
	}
	// Re-read under the held instance lock before claiming; no initialized
	// identity/marker is overwritten. A failed claim remains visible to serve.
	againConfig, err := f.config()
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(cfg, againConfig) {
		return errors.New("effective configuration changed before claim")
	}
	again, err := f.prepare(againConfig, expectedDigest, true)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(p.files, again.files) {
		return errors.New("fresh-node inputs changed before claim")
	}
	if homeErr := f.sameHome(); homeErr != nil {
		return homeErr
	}
	claimBytes, err := json.Marshal(p.receipt)
	if err != nil {
		return err
	}
	if claimCreateErr := f.create(freshNodeClaim, claimBytes); claimCreateErr != nil {
		return claimCreateErr
	}
	if p.transport == nil {
		_, secret, transportKeyErr := ed25519.GenerateKey(rand.Reader)
		if transportKeyErr != nil {
			return transportKeyErr
		}
		if transportCreateErr := f.create(p.receipt.TransportPath, secret.Seed()); transportCreateErr != nil {
			return transportCreateErr
		}
	}
	if p.node == nil {
		node := p2p.NodeKey{PrivKey: cmtcryptoed.GenPrivKey()}
		data, nodeMarshalErr := cmtjson.Marshal(node)
		if nodeMarshalErr != nil {
			return nodeMarshalErr
		}
		if nodeCreateErr := f.create(base+"config/node_key.json", data); nodeCreateErr != nil {
			return nodeCreateErr
		}
	}
	if versionCreateErr := f.create(versionFile, []byte(version+"\n")); versionCreateErr != nil {
		return versionCreateErr
	}
	if forkCreateErr := f.create(forkVersionFile, []byte(fmt.Sprintf("%d\n", ConsensusForkVersion))); forkCreateErr != nil {
		return forkCreateErr
	}
	postConfig, err := f.config()
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(cfg, postConfig) {
		return errors.New("effective configuration changed during publication")
	}
	post, err := f.prepare(postConfig, expectedDigest, true)
	if err != nil {
		return err
	}
	for path, pin := range p.files {
		if post.files[path] != pin {
			return errors.New("fresh-node input changed during publication")
		}
	}
	if postPristineErr := f.checkPristine(func() map[string]bool {
		allowed[freshNodeClaim] = true
		allowed[versionFile] = true
		allowed[forkVersionFile] = true
		return allowed
	}()); postPristineErr != nil {
		return postPristineErr
	}
	if homeErr := f.sameHome(); homeErr != nil {
		return homeErr
	}
	post.receipt.ClaimSHA256 = freshNodeSHA(claimBytes)
	readyBytes, err := json.Marshal(post.receipt)
	if err != nil {
		return err
	}
	if readyCreateErr := f.create(freshNodeReadyTemp, readyBytes); readyCreateErr != nil {
		return readyCreateErr
	}
	if readyLinkErr := unix.Linkat(int(f.root.Fd()), freshNodeReadyTemp, int(f.root.Fd()), freshNodeReady, 0); readyLinkErr != nil {
		return readyLinkErr
	}
	if readySyncErr := f.root.Sync(); readySyncErr != nil {
		return readySyncErr
	}
	if readyUnlinkErr := unix.Unlinkat(int(f.root.Fd()), freshNodeReadyTemp, 0); readyUnlinkErr != nil {
		return readyUnlinkErr
	}
	return f.root.Sync()
}

func validateFreshNodeClaim(c, r freshNodeReceipt, raw []byte) error {
	if c.Schema != freshNodeSchema || c.ClaimSHA256 != "" || r.ClaimSHA256 != freshNodeSHA(raw) || c.GenesisSHA256 != r.GenesisSHA256 || c.ChainID != r.ChainID || c.AppVersion != r.AppVersion || c.ValidatorID != r.ValidatorID || c.DataDir != r.DataDir || c.TransportPath != r.TransportPath || c.CompanionPath != r.CompanionPath || c.BinaryVersion != r.BinaryVersion || c.ForkVersion != r.ForkVersion || (c.TransportID != "" && c.TransportID != r.TransportID) || (c.NodeID != "" && c.NodeID != r.NodeID) {
		return errors.New("fresh-node completion claim mismatch")
	}
	for path, pin := range c.Inputs {
		if r.Inputs[path] != pin {
			return errors.New("completed fresh-node inputs differ from the original claim")
		}
	}
	return nil
}

func validateFreshNodeReceipt(f *freshNodeFS, p *freshNodePrepared, r freshNodeReceipt, pristine bool) error {
	actual := p.receipt
	if r.Schema != freshNodeSchema || r.GenesisSHA256 != actual.GenesisSHA256 || r.ChainID != actual.ChainID || r.AppVersion != actual.AppVersion || r.DataDir != actual.DataDir || r.TransportPath != actual.TransportPath || r.CompanionPath != actual.CompanionPath || r.ValidatorID != actual.ValidatorID || r.TransportID != actual.TransportID || r.NodeID != actual.NodeID {
		return errors.New("fresh-node receipt does not bind current artifact and local identities")
	}
	if pristine && !reflect.DeepEqual(r.Inputs, p.files) {
		return errors.New("completed initializer inputs have changed")
	}
	versionBytes, _, err := f.read(versionFile, true)
	if err != nil {
		return err
	}
	forkBytes, _, err := f.read(forkVersionFile, true)
	if err != nil {
		return err
	}
	if pristine {
		if string(versionBytes) != r.BinaryVersion+"\n" || string(forkBytes) != fmt.Sprintf("%d\n", r.ForkVersion) {
			return errors.New("completed initializer markers differ")
		}
	} else if strings.TrimSpace(string(versionBytes)) == "" || strings.TrimSpace(string(forkBytes)) == "" {
		return errors.New("fresh-node completion markers are missing")
	}
	return nil
}

// Called before migration/store/socket startup. Existing homes without either
// fence retain their previous startup path. Completed nodes may have advanced
// signing state, runtime databases, upgraded version stamps and address config.
func preflightFreshNodeInitialization(home string, cfg *Config) error {
	// Lstat only the two new names first: legacy home permission/layout policy
	// must not silently change merely because this feature was compiled in.
	_, claimErr := os.Lstat(filepath.Join(home, freshNodeClaim))
	_, readyErr := os.Lstat(filepath.Join(home, freshNodeReady))
	if errors.Is(claimErr, os.ErrNotExist) && errors.Is(readyErr, os.ErrNotExist) {
		return nil
	}
	f, err := openFreshNodeFS(home)
	if err != nil {
		return err
	}
	defer f.root.Close()
	claim, _, err := f.read(freshNodeClaim, true)
	if err != nil {
		return errors.New("unfinished fresh-node initialization; startup refused")
	}
	ready, _, err := f.read(freshNodeReady, true)
	if err != nil {
		return errors.New("unfinished fresh-node initialization; startup refused")
	}
	var c, r freshNodeReceipt
	if claimDecodeErr := freshNodeJSON(claim, &c); claimDecodeErr != nil {
		return claimDecodeErr
	}
	if receiptDecodeErr := freshNodeJSON(ready, &r); receiptDecodeErr != nil {
		return receiptDecodeErr
	}
	if bindingErr := validateFreshNodeClaim(c, r, claim); bindingErr != nil {
		return bindingErr
	}
	p, err := f.prepare(cfg, r.GenesisSHA256, false)
	if err != nil {
		return err
	}
	if receiptErr := validateFreshNodeReceipt(f, p, r, false); receiptErr != nil {
		return receiptErr
	}
	if cfg.ChainID == "" {
		cfg.ChainID = p.genesis.ChainID
	}
	return nil
}
