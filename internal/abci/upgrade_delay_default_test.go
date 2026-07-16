//go:build !v119testfixture

package abci

import "testing"

func TestEffectiveUpgradeDelayFloorBlocksPinsProductionDefault(t *testing.T) {
	if defaultUpgradeDelayBlocks != 200 {
		t.Fatalf("defaultUpgradeDelayBlocks = %d, want production default 200", defaultUpgradeDelayBlocks)
	}
	if got := effectiveUpgradeDelayFloorBlocks(); got != defaultUpgradeDelayBlocks {
		t.Fatalf("production upgrade-delay floor = %d, want defaultUpgradeDelayBlocks = %d", got, defaultUpgradeDelayBlocks)
	}
}
