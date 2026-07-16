//go:build v119testfixture

package abci

// effectiveUpgradeDelayFloorBlocks is compiled only into the isolated v11.9
// Docker fixture. It changes no state-sync, authorization, P2P, activation, or
// serving code; it only bounds the time needed for a fresh chain to perform
// the real signed app-version ladder before the transfer starts.
func effectiveUpgradeDelayFloorBlocks() int64 {
	return 3
}
