package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	cmtcryptoed "github.com/cometbft/cometbft/crypto/ed25519"
	cryptoproto "github.com/cometbft/cometbft/proto/tendermint/crypto"
	cmttypes "github.com/cometbft/cometbft/types"
	cmttime "github.com/cometbft/cometbft/types/time"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/store"
)

// Test 4: ensureOperatorAdminID derives the canonical lowercase-hex PUBLIC key from
// both on-disk agent.key formats (32-byte seed and 64-byte private key), generates
// one if absent, and the result equals readNodeOperatorKey() and decodes to 32 bytes.
func TestIssue52_EnsureOperatorAdminID(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	want := hex.EncodeToString(pub)

	for name, keyBytes := range map[string][]byte{
		"32-byte-seed":       priv.Seed(),
		"64-byte-privatekey": priv,
	} {
		t.Run(name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("SAGE_HOME", home)
			require.NoError(t, os.WriteFile(filepath.Join(home, "agent.key"), keyBytes, 0600))

			got := ensureOperatorAdminID()
			require.Equal(t, want, got, "derived admin id must be hex(pub)")
			require.Len(t, got, 64)
			raw, err := hex.DecodeString(got)
			require.NoError(t, err)
			require.Len(t, raw, ed25519.PublicKeySize)

			rk, err := readNodeOperatorKey(filepath.Join(home, "agent.key"))
			require.NoError(t, err)
			require.Equal(t, rk, got, "must match readNodeOperatorKey")
		})
	}

	t.Run("generate-if-absent", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("SAGE_HOME", home)
		got := ensureOperatorAdminID()
		require.Len(t, got, 64, "must generate a key and return its pubkey hex")
		_, statErr := os.Stat(filepath.Join(home, "agent.key"))
		require.NoError(t, statErr, "agent.key must now exist")
		// second call is stable (reads the generated key)
		require.Equal(t, got, ensureOperatorAdminID())
	})
}

func TestIssue52_GenesisAppStateHasInitialAdmin(t *testing.T) {
	id := hex.EncodeToString(make([]byte, 32))
	require.False(t, genesisAppStateHasInitialAdmin(nil))
	require.False(t, genesisAppStateHasInitialAdmin(json.RawMessage(`{}`)))
	require.False(t, genesisAppStateHasInitialAdmin(json.RawMessage(`{"sage":{}}`)))
	require.False(t, genesisAppStateHasInitialAdmin(json.RawMessage(`{"sage":{"initial_admin":""}}`)))
	require.False(t, genesisAppStateHasInitialAdmin(json.RawMessage(`not json`)))
	require.True(t, genesisAppStateHasInitialAdmin(json.RawMessage(`{"sage":{"initial_admin":"`+id+`"}}`)))
}

// i52WriteGenesis writes a genesis.json under home/config with nVals validators and
// the given app_state (nil for none), returning the operator's admin id.
func i52WriteGenesis(t *testing.T, home string, nVals int, appState json.RawMessage) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(home, "config"), 0700))
	require.NoError(t, os.MkdirAll(filepath.Join(home, "data"), 0700))
	vals := make([]cmttypes.GenesisValidator, 0, nVals)
	for i := 0; i < nVals; i++ {
		pk := cmtcryptoed.GenPrivKey().PubKey()
		vals = append(vals, cmttypes.GenesisValidator{Address: pk.Address(), PubKey: pk, Power: 10, Name: "v"})
	}
	gd := cmttypes.GenesisDoc{
		ChainID: "sage-personal", GenesisTime: cmttime.Now(),
		ConsensusParams: cmttypes.DefaultConsensusParams(), Validators: vals, AppState: appState,
	}
	require.NoError(t, gd.ValidateAndComplete())
	require.NoError(t, gd.SaveAs(filepath.Join(home, "config", "genesis.json")))
}

func i52ReadGenesisAppState(t *testing.T, home string) json.RawMessage {
	t.Helper()
	gd, err := cmttypes.GenesisDocFromFile(filepath.Join(home, "config", "genesis.json"))
	require.NoError(t, err)
	return gd.AppState
}

// Test M6: healGenesisAdminIfReset injects the admin ONLY when the chain is at
// height-0 (no block store) and the genesis is a single-validator chain without an
// existing admin. It must NEVER touch a live chain's genesis.
func TestIssue52_HealGenesisAdminIfReset(t *testing.T) {
	// In production the CometBFT home (config/genesis.json + data/) and the SAGE home
	// (agent.key, via SageHome()) are DISTINCT directories. setup wires up that real
	// two-dir layout: agent.key under sageHome, genesis under cometHome, and returns
	// the cometHome to pass to healGenesisAdminIfReset plus the operator's admin id.
	setup := func(t *testing.T) (cometHome, admin string) {
		cometHome = t.TempDir()
		sageHome := t.TempDir()
		t.Setenv("SAGE_HOME", sageHome)
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(sageHome, "agent.key"), priv.Seed(), 0600))
		return cometHome, hex.EncodeToString(pub)
	}

	t.Run("reset-single-validator-no-admin -> injects", func(t *testing.T) {
		cometHome, admin := setup(t)
		i52WriteGenesis(t, cometHome, 1, nil) // no app_state, no blockstore/state (height 0)
		healGenesisAdminIfReset(cometHome, zerolog.Nop())
		require.True(t, genesisAppStateHasInitialAdmin(i52ReadGenesisAppState(t, cometHome)))
		require.Contains(t, string(i52ReadGenesisAppState(t, cometHome)), admin)
	})

	t.Run("LIVE chain (blockstore.db present) -> genesis untouched", func(t *testing.T) {
		cometHome, _ := setup(t)
		i52WriteGenesis(t, cometHome, 1, nil)
		require.NoError(t, os.MkdirAll(filepath.Join(cometHome, "data", "blockstore.db"), 0700)) // live
		healGenesisAdminIfReset(cometHome, zerolog.Nop())
		require.False(t, genesisAppStateHasInitialAdmin(i52ReadGenesisAppState(t, cometHome)),
			"a live chain's genesis must NOT be rewritten")
	})

	t.Run("partial reset (state.db survives) -> genesis untouched", func(t *testing.T) {
		cometHome, _ := setup(t)
		i52WriteGenesis(t, cometHome, 1, nil)
		// blockstore.db wiped but state.db survived: CometBFT loads the cached
		// (admin-less) genesis doc from state.db FIRST, so a rewrite would be silently
		// ignored and the chain would re-deadlock. heal MUST treat this as live.
		require.NoError(t, os.MkdirAll(filepath.Join(cometHome, "data", "state.db"), 0700))
		healGenesisAdminIfReset(cometHome, zerolog.Nop())
		require.False(t, genesisAppStateHasInitialAdmin(i52ReadGenesisAppState(t, cometHome)),
			"a surviving state.db must block the rewrite (cached genesis doc would win)")
	})

	t.Run("already-seeded -> no-op", func(t *testing.T) {
		cometHome, _ := setup(t)
		existing := hex.EncodeToString(make([]byte, 32))
		i52WriteGenesis(t, cometHome, 1, json.RawMessage(`{"sage":{"initial_admin":"`+existing+`"}}`))
		healGenesisAdminIfReset(cometHome, zerolog.Nop())
		require.Contains(t, string(i52ReadGenesisAppState(t, cometHome)), existing,
			"existing admin must be preserved")
	})

	t.Run("multi-validator -> not touched", func(t *testing.T) {
		cometHome, _ := setup(t)
		i52WriteGenesis(t, cometHome, 2, nil)
		healGenesisAdminIfReset(cometHome, zerolog.Nop())
		require.False(t, genesisAppStateHasInitialAdmin(i52ReadGenesisAppState(t, cometHome)))
	})

	t.Run("no genesis -> no panic", func(t *testing.T) {
		cometHome, _ := setup(t)
		require.NotPanics(t, func() { healGenesisAdminIfReset(cometHome, zerolog.Nop()) })
	})
}

// TestIssue52_HealThenInitChain_EndToEnd exercises the REAL runServe ordering on a
// reset, admin-less personal chain: initCometBFTConfig (must NOT seed an existing
// genesis) -> healGenesisAdminIfReset (must inject the seed) -> a fresh abci.SageApp
// InitChain consuming that healed genesis (must register the operator as admin). This
// ties the node-layer heal to the consensus-layer seed end-to-end, rather than testing
// each half in isolation. (migrateOnUpgrade is a no-op without a version change, so the
// meaningful ordering is initCometBFTConfig -> heal -> InitChain.)
func TestIssue52_HealThenInitChain_EndToEnd(t *testing.T) {
	cometHome := t.TempDir()
	sageHome := t.TempDir()
	t.Setenv("SAGE_HOME", sageHome)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(sageHome, "agent.key"), priv.Seed(), 0600))
	adminID := hex.EncodeToString(pub)

	// A reset chain: admin-less single-validator genesis, empty data dir (height 0).
	i52WriteGenesis(t, cometHome, 1, nil)

	// Step 1: initCometBFTConfig must short-circuit on the existing genesis and add NO
	// seed (it only seeds genesis it CREATES — never an existing one).
	require.NoError(t, initCometBFTConfig(cometHome))
	require.False(t, genesisAppStateHasInitialAdmin(i52ReadGenesisAppState(t, cometHome)),
		"initCometBFTConfig must not seed a pre-existing genesis")

	// Step 2: ensureGenesisSeed is EXACTLY what runServe calls after migrate — it must
	// heal the admin-less genesis. (Testing the helper, not the two functions
	// separately, is what guards against the heal step being dropped from serve.)
	// ensureGenesisSeed resolves agent.key through SageHome(), i.e. process-wide
	// SAGE_HOME. This package mutates that variable in 60+ places across a dozen
	// files, mixing t.Setenv with raw os.Setenv whose `defer os.Setenv(orig)`
	// writes "" back when the variable was originally unset -- and SageHome()
	// treats "" as "fall back to ~/.sage", which on a CI runner has no agent.key.
	//
	// This assertion has failed exactly once on CI (main, 2026-07-20) and has not
	// reproduced in 50 single-test runs, 10 under -race, or 3 whole-package runs
	// locally. Pin the preconditions so the next occurrence says WHICH one broke
	// instead of a bare "Should be true".
	require.Equal(t, sageHome, SageHome(),
		"SAGE_HOME leaked: ensureGenesisSeed will look for agent.key in the wrong root")
	require.FileExists(t, filepath.Join(SageHome(), "agent.key"),
		"agent.key is not readable at the resolved SAGE_HOME, so the heal cannot seed an admin")

	require.NoError(t, ensureGenesisSeed(cometHome, zerolog.Nop()))
	healed := i52ReadGenesisAppState(t, cometHome)
	require.True(t, genesisAppStateHasInitialAdmin(healed),
		"heal did not seed a chain admin; SAGE_HOME=%q resolved=%q genesis=%s",
		os.Getenv("SAGE_HOME"), SageHome(), string(healed))
	require.Contains(t, string(healed), adminID)

	// Step 3: a fresh consensus app InitChains from the healed genesis and registers
	// the operator as admin — the contract the propose gate depends on.
	bs, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })
	sq, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "off.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sq.Close() })
	app, err := abci.NewSageAppWithStores(bs, sq, zerolog.Nop())
	require.NoError(t, err)

	valPub := cmtcryptoed.GenPrivKey().PubKey().Bytes()
	_, err = app.InitChain(context.Background(), &abcitypes.RequestInitChain{
		Validators: []abcitypes.ValidatorUpdate{{
			Power:  10,
			PubKey: cryptoproto.PublicKey{Sum: &cryptoproto.PublicKey_Ed25519{Ed25519: valPub}},
		}},
		AppStateBytes: healed,
	})
	require.NoError(t, err)

	require.True(t, bs.IsAgentRegistered(adminID), "InitChain must register the healed operator admin")
	ag, err := bs.GetRegisteredAgent(adminID)
	require.NoError(t, err)
	require.Equal(t, "admin", ag.Role, "healed operator must hold the admin role end-to-end")
}

// TestIssue52_RepairChainState covers the `sage-gui repair-chain` recovery core: a
// single-validator chain stranded past app-v9 (admin-less genesis, live block/state)
// is reset and healed so the next boot re-seeds the operator as admin; a multi-validator
// chain is refused untouched; an uninitialised data dir errors.
func TestIssue52_RepairChainState(t *testing.T) {
	t.Run("single-validator deadlocked chain -> reset + heal", func(t *testing.T) {
		dataDir := t.TempDir()
		sageHome := t.TempDir()
		t.Setenv("SAGE_HOME", sageHome)
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(sageHome, "agent.key"), priv.Seed(), 0600))
		adminID := hex.EncodeToString(pub)

		cometHome := filepath.Join(dataDir, "cometbft")
		i52WriteGenesis(t, cometHome, 1, nil) // admin-less single-validator genesis
		cometData := filepath.Join(cometHome, "data")
		// Simulate the LIVE deadlocked chain: block + state stores present.
		require.NoError(t, os.MkdirAll(filepath.Join(cometData, "blockstore.db"), 0700))
		require.NoError(t, os.MkdirAll(filepath.Join(cometData, "state.db"), 0700))
		require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "badger"), 0700))

		require.NoError(t, repairChainState(dataDir, zerolog.Nop()))

		// Consensus state rebuilt (block/state wiped) ...
		_, bErr := os.Stat(filepath.Join(cometData, "blockstore.db"))
		require.True(t, os.IsNotExist(bErr), "blockstore.db must be removed")
		_, sErr := os.Stat(filepath.Join(cometData, "state.db"))
		require.True(t, os.IsNotExist(sErr), "state.db must be removed")
		// ... and the genesis healed with the operator admin so the next InitChain seeds it.
		healed := i52ReadGenesisAppState(t, cometHome)
		require.True(t, genesisAppStateHasInitialAdmin(healed))
		require.Contains(t, string(healed), adminID)
	})

	t.Run("multi-validator chain -> refused, untouched", func(t *testing.T) {
		dataDir := t.TempDir()
		sageHome := t.TempDir()
		t.Setenv("SAGE_HOME", sageHome)
		_, priv, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(sageHome, "agent.key"), priv.Seed(), 0600))

		cometHome := filepath.Join(dataDir, "cometbft")
		i52WriteGenesis(t, cometHome, 2, nil)
		cometData := filepath.Join(cometHome, "data")
		require.NoError(t, os.MkdirAll(filepath.Join(cometData, "blockstore.db"), 0700))

		require.Error(t, repairChainState(dataDir, zerolog.Nop()), "must refuse a multi-validator chain")
		_, bErr := os.Stat(filepath.Join(cometData, "blockstore.db"))
		require.NoError(t, bErr, "a multi-validator chain must NOT be reset")
		require.False(t, genesisAppStateHasInitialAdmin(i52ReadGenesisAppState(t, cometHome)))
	})

	t.Run("grown quorum (genesis=1, live validators>1) -> refused, untouched", func(t *testing.T) {
		dataDir := t.TempDir()
		sageHome := t.TempDir()
		t.Setenv("SAGE_HOME", sageHome)
		_, priv, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(sageHome, "agent.key"), priv.Seed(), 0600))

		cometHome := filepath.Join(dataDir, "cometbft")
		i52WriteGenesis(t, cometHome, 1, nil) // BORN single-validator ...
		cometData := filepath.Join(cometHome, "data")
		require.NoError(t, os.MkdirAll(filepath.Join(cometData, "blockstore.db"), 0700))
		// ... but GREW into a 2-validator quorum via governance (live validator set).
		bs, err := store.NewBadgerStore(filepath.Join(dataDir, "badger"))
		require.NoError(t, err)
		require.NoError(t, bs.SaveValidators(map[string]int64{"v1": 10, "v2": 10}))
		require.NoError(t, bs.CloseBadger())

		require.Error(t, repairChainState(dataDir, zerolog.Nop()),
			"must refuse a chain that grew into a quorum, even though genesis was single-validator")
		_, bErr := os.Stat(filepath.Join(cometData, "blockstore.db"))
		require.NoError(t, bErr, "a grown-quorum chain must NOT be reset")
	})

	t.Run("uninitialised data dir -> error", func(t *testing.T) {
		t.Setenv("SAGE_HOME", t.TempDir())
		require.Error(t, repairChainState(t.TempDir(), zerolog.Nop()))
	})

	t.Run("running node (chain index locked) -> refused, untouched", func(t *testing.T) {
		dataDir := t.TempDir()
		sageHome := t.TempDir()
		t.Setenv("SAGE_HOME", sageHome)
		_, priv, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(sageHome, "agent.key"), priv.Seed(), 0600))

		cometHome := filepath.Join(dataDir, "cometbft")
		i52WriteGenesis(t, cometHome, 1, nil)
		cometData := filepath.Join(cometHome, "data")
		require.NoError(t, os.MkdirAll(filepath.Join(cometData, "blockstore.db"), 0700))
		// Hold the chain-index lock — exactly what a serving node does.
		held, err := store.NewBadgerStore(filepath.Join(dataDir, "badger"))
		require.NoError(t, err)
		defer func() { _ = held.CloseBadger() }()

		err = repairChainState(dataDir, zerolog.Nop())
		require.Error(t, err, "must refuse while the chain index is locked (node running)")
		require.Contains(t, err.Error(), "still running")
		_, bErr := os.Stat(filepath.Join(cometData, "blockstore.db"))
		require.NoError(t, bErr, "must NOT wipe state while the node holds the lock")
	})

	t.Run("corrupt chain index (unclean crash) -> falls through, still recovers", func(t *testing.T) {
		dataDir := t.TempDir()
		sageHome := t.TempDir()
		t.Setenv("SAGE_HOME", sageHome)
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(sageHome, "agent.key"), priv.Seed(), 0600))
		adminID := hex.EncodeToString(pub)

		cometHome := filepath.Join(dataDir, "cometbft")
		i52WriteGenesis(t, cometHome, 1, nil)
		cometData := filepath.Join(cometHome, "data")
		require.NoError(t, os.MkdirAll(filepath.Join(cometData, "blockstore.db"), 0700))
		require.NoError(t, os.MkdirAll(filepath.Join(cometData, "state.db"), 0700))
		// Build a real index, then corrupt its MANIFEST so the open fails with a
		// non-lock error (the kind a kill -9 / power loss can leave behind).
		badgerPath := filepath.Join(dataDir, "badger")
		bs, err := store.NewBadgerStore(badgerPath)
		require.NoError(t, err)
		require.NoError(t, bs.CloseBadger())
		require.NoError(t, os.WriteFile(filepath.Join(badgerPath, "MANIFEST"), []byte("not a real manifest"), 0600))

		var logBuf bytes.Buffer
		require.NoError(t, repairChainState(dataDir, zerolog.New(&logBuf)),
			"a corrupt (non-locked) index must NOT block recovery — the reset rebuilds it")
		require.Contains(t, logBuf.String(), "chain index is unreadable",
			"the corrupt-index fall-through must log the warning (guards the lock-vs-corruption discrimination)")
		require.True(t, genesisAppStateHasInitialAdmin(i52ReadGenesisAppState(t, cometHome)))
		require.Contains(t, string(i52ReadGenesisAppState(t, cometHome)), adminID)
	})
}

// TestIssue52_InitCometBFTConfigSeedsNewChain covers the PREVENTION path: when
// initCometBFTConfig CREATES a brand-new chain's genesis (none exists yet), it must
// seed the operator key as app_state.sage.initial_admin so a freshly-born personal
// chain is admin-protected from the start and never strands climbing the fork ladder.
func TestIssue52_InitCometBFTConfigSeedsNewChain(t *testing.T) {
	cometHome := t.TempDir()
	sageHome := t.TempDir()
	t.Setenv("SAGE_HOME", sageHome)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(sageHome, "agent.key"), priv.Seed(), 0600))
	adminID := hex.EncodeToString(pub)

	require.NoError(t, initCometBFTConfig(cometHome)) // no genesis yet -> creates one

	as := i52ReadGenesisAppState(t, cometHome)
	require.True(t, genesisAppStateHasInitialAdmin(as), "a newly created genesis must carry the admin seed")
	require.Contains(t, string(as), adminID, "the seed must be the operator's agent.key pubkey")
}
