package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// ConsensusForkVersion is the chain's consensus fork tag. Bumped ONLY when
// a SAGE release introduces a consensus-breaking change that makes existing
// chain state (BadgerDB on-chain registry, CometBFT blocks) invalid under
// the new binary — e.g. tx encoding/decoding shape change, BadgerDB key
// prefix or value encoding change, ABCI semantics change, validator/quorum
// rule change, genesis incompatibility.
//
// INDEPENDENT of release semver. Patch and minor releases that don't break
// consensus do not bump it, so operators keep chain state across upgrades:
// domain registry, access grants, org memberships, validator set, agent
// identities. This is the gate that distinguishes "drag-and-drop chain
// reset is acceptable" (single-user sovereign mode) from "chain state IS
// the deployment substrate" (multi-agent / org-bootstrap / federation).
//
// History:
//
//	1 — Gate introduced in v7.5.5. All prior v7.5.x deployments are treated
//	    as fork=1 on first boot under this gate so the upgrade that adds the
//	    gate itself does not produce a spurious reset.
//
// Declared as a var (not const) so tests can stage fork transitions without
// rebuilding. Mirrors the existing `version` symbol's pattern.
var ConsensusForkVersion = 1

const forkVersionFile = "fork-version.txt"

// readForkVersion returns the consensus fork tag stamped on disk, or 0 when
// the file is absent or unparseable. 0 signals "this install predates the
// gate — adopt the current binary's fork without resetting state".
func readForkVersion(path string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0
	}
	return n
}

// stampForkVersion writes the given fork tag to path. Callers must persist
// this AFTER any reset has completed — a crash mid-migration must leave the
// next boot still seeing the old fork so the reset gets re-attempted.
func stampForkVersion(path string, fork int) error {
	return os.WriteFile(path, []byte(fmt.Sprintf("%d\n", fork)), 0600)
}
