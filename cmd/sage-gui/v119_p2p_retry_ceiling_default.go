//go:build !v119testfixture

package main

import "github.com/cometbft/cometbft/config"

func applyV119FixturePersistentPeerDialCeiling(_ *config.P2PConfig) error {
	return nil
}
