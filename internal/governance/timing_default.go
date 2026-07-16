//go:build !v119testfixture

package governance

// effectiveCooldownBlocks returns the production governance safety cooldown.
// The v11.9 wire gate has an explicit test-only replacement so a fresh chain
// can complete its signed app-version ladder in bounded CI time.
func effectiveCooldownBlocks() int64 {
	return CooldownBlocks
}
