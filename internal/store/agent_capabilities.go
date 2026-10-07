package store

// AgentCapabilities is the consensus-stored capability/restriction mask for an
// on-chain agent. Zero preserves the pre-app-v22 behavior exactly.
type AgentCapabilities uint32

const (
	// AgentCapabilityReadAllDomains lifts domain and submitting-agent discovery
	// filters. It does not lift the agent's numeric classification clearance.
	AgentCapabilityReadAllDomains AgentCapabilities = 1 << iota
	// AgentCapabilityDenySharedDomainWrite blocks memory creation in ownerless
	// shared domains (general, self, meta, sage-*, and dynamic shared domains).
	AgentCapabilityDenySharedDomainWrite
	// AgentCapabilityDenyDomainClaim blocks every explicit and implicit
	// first-writer path that would make the agent a domain owner.
	AgentCapabilityDenyDomainClaim
	// AgentCapabilityDenyForeignDomainWrite blocks memory creation in domains
	// owned by another agent even when a level-2 grant exists.
	AgentCapabilityDenyForeignDomainWrite
	// AgentCapabilityDenyFederatedPipe preserves local pipeline notes while
	// refusing remote-recipient lookup and delivery to another SAGE. Visibility
	// to peers in negotiated node messaging is controlled separately by the
	// connection's agent exposure policy.
	AgentCapabilityDenyFederatedPipe
)

const KnownAgentCapabilities = AgentCapabilityReadAllDomains |
	AgentCapabilityDenySharedDomainWrite |
	AgentCapabilityDenyDomainClaim |
	AgentCapabilityDenyForeignDomainWrite |
	AgentCapabilityDenyFederatedPipe

// DefaultSelfRegisteredAgentCapabilities is the fail-closed app-v22 enrollment
// posture. A freshly generated key may register itself for discoverability, but
// it cannot use that open enrollment path to write shared/foreign domains,
// claim an unowned domain, or open a federated delivery path. A global
// administrator may replace this mask after reviewing the agent.
const DefaultSelfRegisteredAgentCapabilities = AgentCapabilityDenySharedDomainWrite |
	AgentCapabilityDenyDomainClaim |
	AgentCapabilityDenyForeignDomainWrite |
	AgentCapabilityDenyFederatedPipe

func (c AgentCapabilities) Has(capability AgentCapabilities) bool {
	return c&capability != 0
}

func (c AgentCapabilities) Valid() bool {
	return c&^KnownAgentCapabilities == 0
}
