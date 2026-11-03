//go:build v119testfixture

package main

import (
	"os"
	"path/filepath"
	"testing"

	cmttypes "github.com/cometbft/cometbft/types"
	"github.com/stretchr/testify/require"
)

func TestFreshV30StateSyncFixtureOriginMarkers(t *testing.T) {
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)
	cfg := DefaultConfig(home)
	require.NoError(t, SaveConfig(cfg))
	require.NoError(t, initV30StateSyncFixture())
	genesis, err := cmttypes.GenesisDocFromFile(filepath.Join(cfg.DataDir, "cometbft", "config", "genesis.json"))
	require.NoError(t, err)
	require.Equal(t, uint64(30), genesis.ConsensusParams.Version.App)
	require.True(t, hasSignedFreshGenesis(genesis.AppState))
	stamp, err := os.ReadFile(filepath.Join(home, versionFile))
	require.NoError(t, err)
	require.Equal(t, version+"\n", string(stamp))
	require.Equal(t, ConsensusForkVersion, readForkVersion(filepath.Join(home, forkVersionFile)))
	require.FileExists(t, filepath.Join(home, "v30-proof-agent.key"))
	before := snapshotTestTree(t, home)
	require.ErrorContains(t, initV30StateSyncFixture(), "lineage marker exists")
	require.Equal(t, before, snapshotTestTree(t, home), "a second initializer cannot rewrite the signed origin or stamps")
}
