//go:build !v119testfixture

package governance

import "testing"

func TestEffectiveCooldownBlocksPinsProductionDefault(t *testing.T) {
	if CooldownBlocks != 50 {
		t.Fatalf("CooldownBlocks = %d, want production default 50", CooldownBlocks)
	}
	if got := effectiveCooldownBlocks(); got != CooldownBlocks {
		t.Fatalf("production proposal cooldown = %d, want CooldownBlocks = %d", got, CooldownBlocks)
	}
}
