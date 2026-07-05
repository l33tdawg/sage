package main

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	cmtcryptoed "github.com/cometbft/cometbft/crypto/ed25519"
	"github.com/cometbft/cometbft/privval"
	cmttypes "github.com/cometbft/cometbft/types"
	cmttime "github.com/cometbft/cometbft/types/time"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite"

	"github.com/l33tdawg/sage/internal/tlsca"
)

// writeTestGenesis writes a genesis.json with the given chain_id and n validators,
// AND a real priv_validator_key.json whose key IS genesis validator[0] — matching a
// genuine legacy node, so the ownership guard (Guard 3b) is satisfied. Returns the
// first validator's pubkey bytes (the identity the re-mint must preserve). Extra
// validators (n>1) get throwaway keys, modelling a shared/multi-validator network.
func writeTestGenesis(t *testing.T, cometHome, chainID string, n int) []byte {
	t.Helper()
	configDir := filepath.Join(cometHome, "config")
	dataDir := filepath.Join(cometHome, "data")
	require.NoError(t, os.MkdirAll(configDir, 0700))
	require.NoError(t, os.MkdirAll(dataDir, 0700))

	// Real validator key file — priv_validator_key.json matches genesis validator[0].
	pv := privval.GenFilePV(
		filepath.Join(configDir, "priv_validator_key.json"),
		filepath.Join(dataDir, "priv_validator_state.json"),
	)
	pv.Save()
	firstPub := pv.Key.PubKey.Bytes()

	vals := []cmttypes.GenesisValidator{
		{Address: pv.Key.PubKey.Address(), PubKey: pv.Key.PubKey, Power: 10, Name: "personal"},
	}
	for i := 1; i < n; i++ {
		pub := cmtcryptoed.GenPrivKey().PubKey()
		vals = append(vals, cmttypes.GenesisValidator{Address: pub.Address(), PubKey: pub, Power: 10, Name: "personal"})
	}
	genDoc := cmttypes.GenesisDoc{
		ChainID:         chainID,
		GenesisTime:     cmttime.Now(),
		ConsensusParams: cmttypes.DefaultConsensusParams(),
		Validators:      vals,
	}
	require.NoError(t, genDoc.ValidateAndComplete())
	require.NoError(t, genDoc.SaveAs(filepath.Join(configDir, "genesis.json")))

	// A real legacy node always has committed chain state on disk. Create the block
	// and state stores so the re-mint's wipe + split-brain confirm run for real (not
	// vacuously) and can be asserted gone afterward.
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "state.db"), []byte("state"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "blockstore.db"), []byte("blocks"), 0600))
	return firstPub
}

// writeForeignValidatorGenesis writes a single-validator legacy genesis whose sole
// validator is NOT this node's key (models a Flow-3 guest that adopted the host's
// genesis): priv_validator_key.json is a fresh local key, genesis validator[0] is a
// different (host) key.
func writeForeignValidatorGenesis(t *testing.T, cometHome, chainID string) {
	t.Helper()
	configDir := filepath.Join(cometHome, "config")
	dataDir := filepath.Join(cometHome, "data")
	require.NoError(t, os.MkdirAll(configDir, 0700))
	require.NoError(t, os.MkdirAll(dataDir, 0700))

	// This node's own signing key.
	pv := privval.GenFilePV(
		filepath.Join(configDir, "priv_validator_key.json"),
		filepath.Join(dataDir, "priv_validator_state.json"),
	)
	pv.Save()

	// Genesis lists a DIFFERENT (host's) validator.
	host := cmtcryptoed.GenPrivKey().PubKey()
	genDoc := cmttypes.GenesisDoc{
		ChainID:         chainID,
		GenesisTime:     cmttime.Now(),
		ConsensusParams: cmttypes.DefaultConsensusParams(),
		Validators: []cmttypes.GenesisValidator{
			{Address: host.Address(), PubKey: host, Power: 10, Name: "host"},
		},
	}
	require.NoError(t, genDoc.ValidateAndComplete())
	require.NoError(t, genDoc.SaveAs(filepath.Join(configDir, "genesis.json")))
}

// seedTestSQLite writes a minimal but valid sage.db carrying two committed
// memories, so we can prove they survive the re-mint's chain reset.
func seedTestSQLite(t *testing.T, dataDir string) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(dataDir, 0700))
	dbPath := filepath.Join(dataDir, "sage.db")
	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	_, err = db.ExecContext(ctx, `CREATE TABLE memories (
		memory_id TEXT PRIMARY KEY,
		domain_tag TEXT,
		status TEXT,
		content TEXT,
		content_hash BLOB,
		memory_type TEXT,
		created_at TEXT
	)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO memories VALUES
		('m1','go-debugging','committed','a durable committed memory worth keeping',x'01','fact','2026-01-01'),
		('m2','sage-architecture','committed','another durable committed memory worth keeping',x'02','fact','2026-01-02')`)
	require.NoError(t, err)
	return dbPath
}

func committedCount(t *testing.T, dbPath string) int {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	var n int
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM memories WHERE status = 'committed'`).Scan(&n))
	return n
}

func chainIDOf(t *testing.T, cometHome string) string {
	t.Helper()
	id, err := readChainIDFromGenesis(cometHome)
	require.NoError(t, err)
	return id
}

// withVersion overrides the ldflags-injected package version for a test's duration
// (the re-mint, like every migration, no-ops on "dev").
func withVersion(t *testing.T, v string) {
	t.Helper()
	prev := version
	version = v
	t.Cleanup(func() { version = prev })
}

// withNoOtherInstance neutralises Guard 4's live-instance probe so tests don't
// depend on whatever is listening on the RPC port in the environment.
func withNoOtherInstance(t *testing.T) {
	t.Helper()
	prev := instanceIsServing
	instanceIsServing = func() bool { return false }
	t.Cleanup(func() { instanceIsServing = prev })
}

// writeCAWithCN generates a real CA (CN="sage-ca-<chainID>") + a node cert into
// certsDir, so CertsExist is true and the CA CN can be reconciled against a chain_id.
func writeCAWithCN(t *testing.T, certsDir, chainID string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(certsDir, 0700))
	caCert, caKey, err := tlsca.GenerateCA(chainID)
	require.NoError(t, err)
	require.NoError(t, tlsca.WriteCert(filepath.Join(certsDir, tlsca.CACertFile), caCert))
	require.NoError(t, tlsca.WriteKey(filepath.Join(certsDir, tlsca.CAKeyFile), caKey))
	nodeCert, nodeKey, err := tlsca.GenerateNodeCert(caCert, caKey, "personal", []string{"127.0.0.1"})
	require.NoError(t, err)
	require.NoError(t, tlsca.WriteCert(filepath.Join(certsDir, tlsca.NodeCertFile), nodeCert))
	require.NoError(t, tlsca.WriteKey(filepath.Join(certsDir, tlsca.NodeKeyFile), nodeKey))
}

func certsPresent(certsDir string) bool { return tlsca.CertsExist(certsDir) }

// TestReconcileCACommonName covers the boot-time CA-CN self-heal (backstops a
// partial cert rotation).
func TestReconcileCACommonName(t *testing.T) {
	t.Run("stale-cn-regenerated", func(t *testing.T) {
		certsDir := filepath.Join(t.TempDir(), "certs")
		writeCAWithCN(t, certsDir, "sage-personal") // CA CN = sage-ca-sage-personal
		removed := reconcileCACommonName(certsDir, "sage-personal-xyz234abcdef", false, zerolog.Nop())
		require.True(t, removed, "a CA whose CN doesn't match the chain_id must be rotated")
		require.False(t, certsPresent(certsDir), "all cert files must be removed so they regenerate")
	})

	t.Run("matching-cn-untouched", func(t *testing.T) {
		certsDir := filepath.Join(t.TempDir(), "certs")
		writeCAWithCN(t, certsDir, "sage-personal-xyz234abcdef")
		removed := reconcileCACommonName(certsDir, "sage-personal-xyz234abcdef", false, zerolog.Nop())
		require.False(t, removed, "a CA whose CN matches must be left alone")
		require.True(t, certsPresent(certsDir))
	})

	t.Run("quorum-untouched", func(t *testing.T) {
		certsDir := filepath.Join(t.TempDir(), "certs")
		writeCAWithCN(t, certsDir, "sage-personal")
		removed := reconcileCACommonName(certsDir, "sage-quorum-abc", true, zerolog.Nop())
		require.False(t, removed, "quorum CAs are managed by the shared-genesis flow")
		require.True(t, certsPresent(certsDir))
	})

	t.Run("no-certs-noop", func(t *testing.T) {
		certsDir := filepath.Join(t.TempDir(), "certs")
		removed := reconcileCACommonName(certsDir, "sage-personal-xyz", false, zerolog.Nop())
		require.False(t, removed, "nothing to reconcile when no certs exist")
	})

	t.Run("ca-missing-but-node-cert-present-regenerated", func(t *testing.T) {
		certsDir := filepath.Join(t.TempDir(), "certs")
		writeCAWithCN(t, certsDir, "sage-personal")
		// Simulate a partial rotation: CA removed, node cert/key left behind.
		require.NoError(t, os.Remove(filepath.Join(certsDir, tlsca.CACertFile)))
		require.NoError(t, os.Remove(filepath.Join(certsDir, tlsca.CAKeyFile)))
		require.True(t, certsPresent(certsDir), "node cert/key still present")
		removed := reconcileCACommonName(certsDir, "sage-personal-newid234567", false, zerolog.Nop())
		require.True(t, removed, "inconsistent CA-missing state must be cleared for regen")
		require.False(t, certsPresent(certsDir))
	})
}

// TestRemint_DialedPeer_Skipped: a node whose CometBFT address book records a dialed
// peer (it joined/participated in a P2P network) must not be re-minted even if its
// config flags now look standalone. NOTE: this models a node that DIALED a peer
// (e.g. an ex-guest); it does NOT model a Flow-3 host, whose address book stays empty
// (documented residual in Guard 1b — not testable because there is no disk trace).
func TestRemint_DialedPeer_Skipped(t *testing.T) {
	withVersion(t, "v11.0.3")
	withNoOtherInstance(t)
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)
	dataDir := filepath.Join(home, "data")
	cometHome := filepath.Join(dataDir, "cometbft")
	writeTestGenesis(t, cometHome, legacySharedChainID, 1) // single-validator own-key, looks standalone
	seedTestSQLite(t, dataDir)

	// Address book with a dialed peer.
	addrbook := `{"key":"abc","addrs":[{"addr":{"id":"deadbeef","ip":"192.168.1.9","port":26656}}]}`
	require.NoError(t, os.WriteFile(filepath.Join(cometHome, "config", "addrbook.json"), []byte(addrbook), 0600))

	migrated, err := remintLegacyChainID(dataDir, &Config{}, zerolog.Nop())
	require.NoError(t, err)
	require.False(t, migrated, "a node that dialed a network peer must not be re-minted")
	require.Equal(t, legacySharedChainID, chainIDOf(t, cometHome))
}

// TestHasNetworkedPeers checks the addrbook signal directly.
func TestHasNetworkedPeers(t *testing.T) {
	cometHome := t.TempDir()
	cfgDir := filepath.Join(cometHome, "config")
	require.NoError(t, os.MkdirAll(cfgDir, 0700))

	require.False(t, hasNetworkedPeers(cometHome), "absent addrbook => not networked")
	require.NoError(t, os.WriteFile(filepath.Join(cfgDir, "addrbook.json"), []byte(`{"key":"k","addrs":[]}`), 0600))
	require.False(t, hasNetworkedPeers(cometHome), "empty addrbook (pure personal) => not networked")
	require.NoError(t, os.WriteFile(filepath.Join(cfgDir, "addrbook.json"), []byte(`{"key":"k","addrs":[{"addr":{"ip":"10.0.0.2"}}]}`), 0600))
	require.True(t, hasNetworkedPeers(cometHome), "addrbook with a peer => networked")
}

// TestRemint_LegacyPersonal_ReMintsAndPreservesMemories is the happy path.
func TestRemint_LegacyPersonal_ReMintsAndPreservesMemories(t *testing.T) {
	withVersion(t, "v11.0.3")
	withNoOtherInstance(t)
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)
	dataDir := filepath.Join(home, "data")
	cometHome := filepath.Join(dataDir, "cometbft")

	origPub := writeTestGenesis(t, cometHome, legacySharedChainID, 1)
	dbPath := seedTestSQLite(t, dataDir)
	require.Equal(t, 2, committedCount(t, dbPath))

	migrated, err := remintLegacyChainID(dataDir, &Config{Quorum: QuorumConfig{Enabled: false}}, zerolog.Nop())
	require.NoError(t, err)
	require.True(t, migrated, "legacy single-validator personal node must be re-minted")

	newID := chainIDOf(t, cometHome)
	require.NotEqual(t, legacySharedChainID, newID, "chain_id must change")
	require.True(t, strings.HasPrefix(newID, legacySharedChainID+"-"), "new id keeps the sage-personal prefix, got %q", newID)

	// Validator identity preserved.
	genDoc, err := cmttypes.GenesisDocFromFile(filepath.Join(cometHome, "config", "genesis.json"))
	require.NoError(t, err)
	require.Len(t, genDoc.Validators, 1)
	require.Equal(t, origPub, genDoc.Validators[0].PubKey.Bytes(), "validator pubkey must be unchanged")

	// Memories intact.
	require.FileExists(t, dbPath)
	require.Equal(t, 2, committedCount(t, dbPath), "committed memories must survive the re-mint")

	// Chain stores wiped (exercises the wipe + the split-brain confirm for real).
	require.NoFileExists(t, filepath.Join(cometHome, "data", "state.db"))
	require.NoFileExists(t, filepath.Join(cometHome, "data", "blockstore.db"))

	// Safety artifacts.
	require.FileExists(t, filepath.Join(cometHome, "config", "genesis.json.pre-remint.bak"))
	backups, _ := filepath.Glob(filepath.Join(home, "backups", "sage-pre-upgrade-*.db"))
	require.NotEmpty(t, backups, "a verified SQLite backup must be written before the wipe")

	// TLS CA CN self-heal is covered by TestReconcileCACommonName; the re-mint itself
	// no longer touches certs (the boot-time reconcile does).
}

// TestRemint_ResetFailure_NonFatal pins the headline availability promise: when the
// chain backup/reset can't complete, the re-mint is skipped and NOTHING is wiped —
// it never returns an error that would abort boot.
func TestRemint_ResetFailure_NonFatal(t *testing.T) {
	withVersion(t, "v11.0.3")
	withNoOtherInstance(t)
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)
	dataDir := filepath.Join(home, "data")
	cometHome := filepath.Join(dataDir, "cometbft")
	writeTestGenesis(t, cometHome, legacySharedChainID, 1)

	// A garbage (non-sqlite) sage.db makes VACUUM fail -> raw copy -> verifyBackup's
	// quick_check rejects -> resetChainState errors, all BEFORE any wipe.
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "sage.db"), []byte("not a database at all"), 0600))

	migrated, err := remintLegacyChainID(dataDir, &Config{}, zerolog.Nop())
	require.NoError(t, err, "reset failure must be non-fatal — never an error that aborts boot")
	require.False(t, migrated, "re-mint must be skipped when the backup can't be verified")
	require.Equal(t, legacySharedChainID, chainIDOf(t, cometHome), "chain_id must be untouched")

	// Abort happened before destruction: chain state and the live DB are intact.
	require.FileExists(t, filepath.Join(cometHome, "data", "state.db"))
	require.FileExists(t, filepath.Join(cometHome, "data", "blockstore.db"))
	got, _ := os.ReadFile(filepath.Join(dataDir, "sage.db"))
	require.Equal(t, "not a database at all", string(got), "live sage.db must be untouched")
}

// TestRemint_Idempotent: a second boot after re-mint is a no-op.
func TestRemint_Idempotent(t *testing.T) {
	withVersion(t, "v11.0.3")
	withNoOtherInstance(t)
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)
	dataDir := filepath.Join(home, "data")
	cometHome := filepath.Join(dataDir, "cometbft")
	writeTestGenesis(t, cometHome, legacySharedChainID, 1)
	seedTestSQLite(t, dataDir)

	migrated, err := remintLegacyChainID(dataDir, &Config{}, zerolog.Nop())
	require.NoError(t, err)
	require.True(t, migrated)
	firstID := chainIDOf(t, cometHome)

	migrated2, err := remintLegacyChainID(dataDir, &Config{}, zerolog.Nop())
	require.NoError(t, err)
	require.False(t, migrated2, "second run must be a no-op")
	require.Equal(t, firstID, chainIDOf(t, cometHome), "chain_id must not churn on re-run")
}

// TestRemint_Guards covers every case that must be left untouched.
func TestRemint_Guards(t *testing.T) {
	t.Run("quorum-mode-skipped", func(t *testing.T) {
		withVersion(t, "v11.0.3")
		withNoOtherInstance(t)
		home := t.TempDir()
		t.Setenv("SAGE_HOME", home)
		dataDir := filepath.Join(home, "data")
		cometHome := filepath.Join(dataDir, "cometbft")
		writeTestGenesis(t, cometHome, legacySharedChainID, 1)

		migrated, err := remintLegacyChainID(dataDir, &Config{Quorum: QuorumConfig{Enabled: true}}, zerolog.Nop())
		require.NoError(t, err)
		require.False(t, migrated, "quorum node must never be re-minted")
		require.Equal(t, legacySharedChainID, chainIDOf(t, cometHome))
	})

	t.Run("quorum-peers-configured-skipped", func(t *testing.T) {
		withVersion(t, "v11.0.3")
		withNoOtherInstance(t)
		home := t.TempDir()
		t.Setenv("SAGE_HOME", home)
		dataDir := filepath.Join(home, "data")
		cometHome := filepath.Join(dataDir, "cometbft")
		writeTestGenesis(t, cometHome, legacySharedChainID, 1)

		migrated, err := remintLegacyChainID(dataDir, &Config{Quorum: QuorumConfig{Enabled: false, Peers: []string{"node@1.2.3.4:26656"}}}, zerolog.Nop())
		require.NoError(t, err)
		require.False(t, migrated, "a node with configured peers is networked — never re-mint")
		require.Equal(t, legacySharedChainID, chainIDOf(t, cometHome))
	})

	t.Run("foreign-validator-guest-skipped", func(t *testing.T) {
		withVersion(t, "v11.0.3")
		withNoOtherInstance(t)
		home := t.TempDir()
		t.Setenv("SAGE_HOME", home)
		dataDir := filepath.Join(home, "data")
		cometHome := filepath.Join(dataDir, "cometbft")
		writeForeignValidatorGenesis(t, cometHome, legacySharedChainID)

		migrated, err := remintLegacyChainID(dataDir, &Config{}, zerolog.Nop())
		require.NoError(t, err)
		require.False(t, migrated, "a genesis whose validator isn't our key (guest) must not be re-minted")
		require.Equal(t, legacySharedChainID, chainIDOf(t, cometHome))
	})

	t.Run("another-instance-running-skipped", func(t *testing.T) {
		withVersion(t, "v11.0.3")
		home := t.TempDir()
		t.Setenv("SAGE_HOME", home)
		dataDir := filepath.Join(home, "data")
		cometHome := filepath.Join(dataDir, "cometbft")
		writeTestGenesis(t, cometHome, legacySharedChainID, 1)

		prev := instanceIsServing
		instanceIsServing = func() bool { return true }
		t.Cleanup(func() { instanceIsServing = prev })

		migrated, err := remintLegacyChainID(dataDir, &Config{}, zerolog.Nop())
		require.NoError(t, err)
		require.False(t, migrated, "must not wipe chain state while another instance is live")
		require.Equal(t, legacySharedChainID, chainIDOf(t, cometHome))
	})

	t.Run("already-unique-id-skipped", func(t *testing.T) {
		withVersion(t, "v11.0.3")
		withNoOtherInstance(t)
		home := t.TempDir()
		t.Setenv("SAGE_HOME", home)
		dataDir := filepath.Join(home, "data")
		cometHome := filepath.Join(dataDir, "cometbft")
		unique := legacySharedChainID + "-abcdefghij234567abcdefghij"
		writeTestGenesis(t, cometHome, unique, 1)

		migrated, err := remintLegacyChainID(dataDir, &Config{}, zerolog.Nop())
		require.NoError(t, err)
		require.False(t, migrated, "already-unique id must be left alone")
		require.Equal(t, unique, chainIDOf(t, cometHome))
	})

	t.Run("legacy-quorum-literal-skipped", func(t *testing.T) {
		withVersion(t, "v11.0.3")
		withNoOtherInstance(t)
		home := t.TempDir()
		t.Setenv("SAGE_HOME", home)
		dataDir := filepath.Join(home, "data")
		cometHome := filepath.Join(dataDir, "cometbft")
		writeTestGenesis(t, cometHome, "sage-quorum", 1)

		migrated, err := remintLegacyChainID(dataDir, &Config{}, zerolog.Nop())
		require.NoError(t, err)
		require.False(t, migrated, "the sage-quorum literal is not the personal legacy id")
		require.Equal(t, "sage-quorum", chainIDOf(t, cometHome))
	})

	t.Run("multi-validator-sage-personal-skipped", func(t *testing.T) {
		withVersion(t, "v11.0.3")
		withNoOtherInstance(t)
		home := t.TempDir()
		t.Setenv("SAGE_HOME", home)
		dataDir := filepath.Join(home, "data")
		cometHome := filepath.Join(dataDir, "cometbft")
		writeTestGenesis(t, cometHome, legacySharedChainID, 3) // shared network shape

		migrated, err := remintLegacyChainID(dataDir, &Config{}, zerolog.Nop())
		require.NoError(t, err)
		require.False(t, migrated, "multi-validator sage-personal looks like a shared network — do not fork it")
		require.Equal(t, legacySharedChainID, chainIDOf(t, cometHome))
	})

	t.Run("no-genesis-fresh-install-skipped", func(t *testing.T) {
		withVersion(t, "v11.0.3")
		withNoOtherInstance(t)
		home := t.TempDir()
		t.Setenv("SAGE_HOME", home)
		dataDir := filepath.Join(home, "data")
		require.NoError(t, os.MkdirAll(dataDir, 0700))

		migrated, err := remintLegacyChainID(dataDir, &Config{}, zerolog.Nop())
		require.NoError(t, err)
		require.False(t, migrated, "no genesis => fresh install, nothing to migrate")
	})

	t.Run("dev-build-skipped", func(t *testing.T) {
		withVersion(t, "dev")
		withNoOtherInstance(t)
		home := t.TempDir()
		t.Setenv("SAGE_HOME", home)
		dataDir := filepath.Join(home, "data")
		cometHome := filepath.Join(dataDir, "cometbft")
		writeTestGenesis(t, cometHome, legacySharedChainID, 1)

		migrated, err := remintLegacyChainID(dataDir, &Config{}, zerolog.Nop())
		require.NoError(t, err)
		require.False(t, migrated, "dev builds never touch persisted state")
		require.Equal(t, legacySharedChainID, chainIDOf(t, cometHome))
	})
}
