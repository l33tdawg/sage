//go:build v119testfixture

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/cometbft/cometbft/config"
)

const v119PersistentPeerMaxDialPeriodEnv = "SAGE_V119_PERSISTENT_PEER_MAX_DIAL_PERIOD"

func applyV119FixturePersistentPeerDialCeiling(cfg *config.P2PConfig) error {
	raw := os.Getenv(v119PersistentPeerMaxDialPeriodEnv)
	if raw == "" {
		return nil
	}
	period, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("parse %s: %w", v119PersistentPeerMaxDialPeriodEnv, err)
	}
	if period <= 0 {
		return fmt.Errorf("%s must be positive", v119PersistentPeerMaxDialPeriodEnv)
	}
	cfg.PersistentPeersMaxDialPeriod = period
	return nil
}
