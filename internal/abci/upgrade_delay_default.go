//go:build !v119testfixture

package abci

// effectiveUpgradeDelayFloorBlocks returns the production governance safety
// delay. The v11.9 wire gate has an explicit test-only replacement so it can
// exercise the real signed upgrade ceremony without waiting hours in CI.
func effectiveUpgradeDelayFloorBlocks() int64 {
	return defaultUpgradeDelayBlocks
}
