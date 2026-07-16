// Package consensuskeys owns exact key identities shared by independent
// consensus-state hash implementations. Keep this package dependency-free so
// store, local snapshot verification, and recovery tooling cannot drift.
package consensuskeys

import "bytes"

const (
	AgentOrgsIndexBackfillProgress = "migration:index:v1:agent_orgs"
	OrgNameIndexBackfillProgress   = "migration:index:v1:org_name"
)

// IsAppHashExcludedLocalKey reports whether key is one of the two legacy
// dirty-tree startup-progress markers rather than replicated chain state.
// Production progress now lives in local sidecars, but retaining these exact
// exclusions keeps snapshot/state-sync verification defensive while startup
// durably scrubs the keys. Derived rows and every other migration:* key hash.
func IsAppHashExcludedLocalKey(key []byte) bool {
	return bytes.Equal(key, []byte(AgentOrgsIndexBackfillProgress)) ||
		bytes.Equal(key, []byte(OrgNameIndexBackfillProgress))
}
