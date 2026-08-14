//go:build !v119testfixture

package main

import (
	"testing"

	"github.com/cometbft/cometbft/config"
	"github.com/stretchr/testify/require"
)

func TestApplyV119FixturePersistentPeerDialCeilingIsNoopInNormalBuild(t *testing.T) {
	t.Setenv("SAGE_V119_PERSISTENT_PEER_MAX_DIAL_PERIOD", "5s")
	cfg := config.DefaultP2PConfig()
	want := *cfg

	require.NoError(t, applyV119FixturePersistentPeerDialCeiling(cfg))
	require.Equal(t, want, *cfg, "fixture environment must not change normal-build P2P configuration")
	require.Equal(t, want.PersistentPeersMaxDialPeriod, cfg.PersistentPeersMaxDialPeriod)
}
