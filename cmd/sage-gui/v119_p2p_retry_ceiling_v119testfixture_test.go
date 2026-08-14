//go:build v119testfixture

package main

import (
	"testing"
	"time"

	"github.com/cometbft/cometbft/config"
	"github.com/stretchr/testify/require"
)

func TestApplyV119FixturePersistentPeerDialCeiling(t *testing.T) {
	t.Setenv(v119PersistentPeerMaxDialPeriodEnv, "5s")
	cfg := config.DefaultP2PConfig()
	require.NoError(t, applyV119FixturePersistentPeerDialCeiling(cfg))
	require.Equal(t, 5*time.Second, cfg.PersistentPeersMaxDialPeriod)
}

func TestApplyV119FixturePersistentPeerDialCeilingRejectsInvalidValues(t *testing.T) {
	for _, value := range []string{"0s", "-1s", "not-a-duration"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv(v119PersistentPeerMaxDialPeriodEnv, value)
			cfg := config.DefaultP2PConfig()
			require.Error(t, applyV119FixturePersistentPeerDialCeiling(cfg))
		})
	}
}
