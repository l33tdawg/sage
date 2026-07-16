//go:build v119testfixture

package governance

// effectiveCooldownBlocks is compiled only into the isolated v11.9 Docker
// fixture. It changes no authorization, quorum, identity, state-sync, P2P, or
// serving rule; it only shortens the interval between successful proposals in
// the fixture's real signed app-version ladder.
func effectiveCooldownBlocks() int64 {
	return 1
}
