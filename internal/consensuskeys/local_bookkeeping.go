// Package consensuskeys owns exact key identities shared by independent
// consensus-state hash implementations. Keep this package dependency-free so
// store, local snapshot verification, and recovery tooling cannot drift.
package consensuskeys

import "bytes"

const (
	AppV23GenesisVersion           uint64 = 23
	AgentOrgsIndexBackfillProgress        = "migration:index:v1:agent_orgs"
	OrgNameIndexBackfillProgress          = "migration:index:v1:org_name"
	AppV23MigrationState                  = "appv23:migration_state"
	AppV23GenesisActivation               = "appv23:genesis_activation"
	GovernanceDelegationDomainV20         = "governance_delegation_domain_v20"
)

var (
	// AppV23MigrationStagePrefix deliberately sorts after every ordinary
	// textual SAGE key. Before app-v23 activation these records are a local,
	// replay-safe preparation sidecar and are excluded from AppHash. The
	// appv23:migration_state activation record atomically promotes the exact
	// prepared set into consensus state; post-activation hash/snapshot code
	// includes the prefix in its normal lexicographic position (last).
	AppV23MigrationStagePrefix = []byte{0xff, 's', 'a', 'g', 'e', ':', 'a', 'p', 'p', 'v', '2', '3', ':', 's', 't', 'a', 'g', 'e', ':'}
	AppV23MigrationPrepareKey  = []byte{0xff, 's', 'a', 'g', 'e', ':', 'a', 'p', 'p', 'v', '2', '3', ':', 'p', 'r', 'e', 'p', 'a', 'r', 'e'}
)

func IsAppV23MigrationStageKey(key []byte) bool {
	return bytes.HasPrefix(key, AppV23MigrationStagePrefix)
}

func AppV23MigrationStageKey(target []byte) []byte {
	// Avoid arithmetic preallocation from an external key length. Appending to
	// the fixed prefix grows safely or panics before any wrapped allocation size.
	key := bytes.Clone(AppV23MigrationStagePrefix)
	return append(key, target...)
}

func AppV23MigrationStageTarget(key []byte) ([]byte, bool) {
	if !IsAppV23MigrationStageKey(key) || len(key) == len(AppV23MigrationStagePrefix) {
		return nil, false
	}
	return key[len(AppV23MigrationStagePrefix):], true
}

// IsAppHashExcludedLocalKey reports whether key is one of the two legacy
// dirty-tree startup-progress markers rather than replicated chain state.
// Production progress now lives in local sidecars, but retaining these exact
// exclusions keeps snapshot/state-sync verification defensive while startup
// durably scrubs the keys. app-v23 preparation records are also local until
// the activation marker promotes the stage prefix; callers which know that
// app-v23 is active must explicitly include IsAppV23MigrationStageKey rows.
func IsAppHashExcludedLocalKey(key []byte) bool {
	return bytes.Equal(key, []byte(AgentOrgsIndexBackfillProgress)) ||
		bytes.Equal(key, []byte(OrgNameIndexBackfillProgress)) ||
		bytes.Equal(key, AppV23MigrationPrepareKey) ||
		IsAppV23MigrationStageKey(key)
}
