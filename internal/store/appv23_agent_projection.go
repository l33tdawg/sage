package store

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ReconcileAppV23AgentProjections repairs the node-local agent directory from
// consensus-authoritative app-v23 state. It is deliberately one-way:
// network_agents/agents receives no authority of its own, and this helper never
// registers, enrolls, approves, promotes, or grants an agent in BadgerDB.
//
// Root is not an ordinary agent and is therefore never projected. Inactive
// enrollments are also skipped; their lifecycle remains governed by consensus.
func ReconcileAppV23AgentProjections(
	ctx context.Context,
	agentStore AgentStore,
	badgerStore *BadgerStore,
) (int, error) {
	return reconcileAppV23AgentProjections(ctx, agentStore, badgerStore, "", nil)
}

// EnsureAppV23AgentProjection repairs one already-committed active ordinary
// agent. metadata may contribute display-only fields when consensus and the
// existing local row have none (for example the first Mynah self-registration
// after direct app-v23 genesis). Policy and lifecycle fields always come from
// committed app-v23 state.
func EnsureAppV23AgentProjection(
	ctx context.Context,
	agentStore AgentStore,
	badgerStore *BadgerStore,
	agentID string,
	metadata *AgentEntry,
) (*AgentEntry, error) {
	if !isCanonicalAgentID(agentID) {
		return nil, errors.New("app-v23 projection agent ID must be canonical lowercase 64-hex")
	}
	_, err := reconcileAppV23AgentProjections(
		ctx, agentStore, badgerStore, agentID, metadata,
	)
	if err != nil {
		return nil, err
	}
	projected, err := agentStore.GetAgent(ctx, agentID)
	if err != nil {
		return nil, fmt.Errorf("read repaired app-v23 agent projection: %w", err)
	}
	if projected == nil || projected.Status != "active" || projected.RemovedAt != nil {
		return nil, errors.New("app-v23 active agent projection was not materialized")
	}
	return projected, nil
}

func reconcileAppV23AgentProjections(
	ctx context.Context,
	agentStore AgentStore,
	badgerStore *BadgerStore,
	onlyAgentID string,
	metadata *AgentEntry,
) (int, error) {
	if agentStore == nil || badgerStore == nil {
		return 0, errors.New("app-v23 agent projection stores are unavailable")
	}
	root, err := badgerStore.GetAppV23Root()
	if err != nil {
		return 0, fmt.Errorf("read app-v23 Root for agent projection: %w", err)
	}
	if root == nil {
		return 0, nil
	}
	if validationErr := badgerStore.ValidateAppV23State(); validationErr != nil {
		return 0, fmt.Errorf("validate app-v23 state before agent projection: %w", validationErr)
	}

	localAgents, err := agentStore.ListAgents(ctx)
	if err != nil {
		return 0, fmt.Errorf("list local agent projection: %w", err)
	}
	existingByID := make(map[string]*AgentEntry, len(localAgents))
	for _, agent := range localAgents {
		if agent != nil {
			existingByID[agent.AgentID] = agent
		}
	}

	onChainAgents, err := badgerStore.ListRegisteredAgents()
	if err != nil {
		return 0, fmt.Errorf("list committed app-v23 agents: %w", err)
	}
	now := time.Now().UTC()
	projected := 0
	foundTarget := onlyAgentID == ""
	for i := range onChainAgents {
		onChain := &onChainAgents[i]
		if onlyAgentID != "" && onChain.AgentID != onlyAgentID {
			continue
		}
		if !isCanonicalAgentID(onChain.AgentID) {
			return projected, fmt.Errorf(
				"committed app-v23 agent %q is not canonical", onChain.AgentID,
			)
		}
		wasRoot, rootErr := badgerStore.IsAppV23RootCredential(onChain.AgentID)
		if rootErr != nil {
			return projected, fmt.Errorf(
				"read Root credential history for app-v23 agent %s: %w",
				onChain.AgentID, rootErr,
			)
		}
		if wasRoot || onChain.AgentID == root.PrincipalID ||
			onChain.AgentID == root.CredentialID {
			if onlyAgentID != "" {
				return projected, errors.New("CEREBRUM Root is not an ordinary agent projection")
			}
			continue
		}

		enrollment, enrollmentErr := badgerStore.GetAppV23Enrollment(onChain.AgentID)
		role, roleErr := badgerStore.GetAppV23Role(onChain.AgentID)
		if enrollmentErr != nil || roleErr != nil {
			return projected, fmt.Errorf(
				"read committed app-v23 policy for agent %s: %w",
				onChain.AgentID, errors.Join(enrollmentErr, roleErr),
			)
		}
		// A post-activation self-registration is intentionally discoverable
		// before Root review but has neither enrollment nor role yet.
		// ValidateAppV23State above already proved the exact bounded pending
		// shape, so the local serving projection must skip it rather than make
		// one pending agent abort startup for every active agent.
		if enrollment == nil && role == nil {
			if onlyAgentID != "" {
				return projected, ErrAppV23NeedsApproval
			}
			continue
		}
		if enrollment == nil || role == nil ||
			enrollment.AgentID != onChain.AgentID ||
			role.AgentID != onChain.AgentID {
			return projected, fmt.Errorf(
				"committed app-v23 policy is incomplete for agent %s",
				onChain.AgentID,
			)
		}
		if !enrollment.Active {
			if onlyAgentID != "" {
				return projected, ErrAppV23NeedsApproval
			}
			continue
		}
		if err := ValidateAppV23EnrollmentPolicy(
			role.Role,
			enrollment.Profile,
			enrollment.Capabilities,
			enrollment.Clearance,
			enrollment.Active,
		); err != nil {
			return projected, fmt.Errorf(
				"invalid committed app-v23 policy for agent %s: %w",
				onChain.AgentID, err,
			)
		}
		// Role, clearance, and capabilities are serving fields derived from the
		// independently committed role and enrollment records. Older upgrade
		// paths can retain a stale value in the duplicate agent-shaped record;
		// that must never turn a rebuildable local projection into a node-wide
		// startup failure. Normalize the transient source before rebuilding
		// SQLite. This deliberately does not write BadgerDB outside consensus.
		onChain.Role = role.Role
		onChain.Clearance = enrollment.Clearance
		onChain.Capabilities = enrollment.Capabilities

		entry := appV23ProjectedAgentEntry(
			onChain,
			enrollment,
			existingByID[onChain.AgentID],
			metadata,
			now,
		)
		if existingByID[onChain.AgentID] == nil {
			if createErr := agentStore.CreateAgent(ctx, entry); createErr != nil {
				// A concurrent idempotent registration may have won after the
				// initial ListAgents snapshot. Only an exact successful lookup
				// turns that insert failure into the update path.
				concurrent, lookupErr := agentStore.GetAgent(ctx, onChain.AgentID)
				if lookupErr != nil || concurrent == nil {
					return projected, fmt.Errorf(
						"create app-v23 agent projection %s: %w",
						onChain.AgentID, errors.Join(createErr, lookupErr),
					)
				}
				entry = appV23ProjectedAgentEntry(
					onChain, enrollment, concurrent, metadata, now,
				)
				if updateErr := agentStore.UpdateAgent(ctx, entry); updateErr != nil {
					return projected, fmt.Errorf(
						"update concurrent app-v23 agent projection %s: %w",
						onChain.AgentID, updateErr,
					)
				}
				if concurrent.Status != "active" || concurrent.RemovedAt != nil {
					if statusErr := agentStore.UpdateAgentStatus(
						ctx, onChain.AgentID, "active",
					); statusErr != nil {
						return projected, fmt.Errorf(
							"activate concurrent app-v23 agent projection %s: %w",
							onChain.AgentID, statusErr,
						)
					}
				}
			}
		} else {
			if updateErr := agentStore.UpdateAgent(ctx, entry); updateErr != nil {
				return projected, fmt.Errorf(
					"update app-v23 agent projection %s: %w",
					onChain.AgentID, updateErr,
				)
			}
			if entry.Status != existingByID[onChain.AgentID].Status ||
				existingByID[onChain.AgentID].RemovedAt != nil {
				if statusErr := agentStore.UpdateAgentStatus(
					ctx, onChain.AgentID, "active",
				); statusErr != nil {
					return projected, fmt.Errorf(
						"activate app-v23 agent projection %s: %w",
						onChain.AgentID, statusErr,
					)
				}
			}
			if existingByID[onChain.AgentID].FirstSeen == nil {
				if firstSeenErr := agentStore.BackfillFirstSeen(
					ctx, onChain.AgentID, now,
				); firstSeenErr != nil {
					return projected, fmt.Errorf(
						"backfill app-v23 agent first_seen %s: %w",
						onChain.AgentID, firstSeenErr,
					)
				}
			}
		}
		existingByID[onChain.AgentID] = entry
		projected++
		foundTarget = true
	}
	if !foundTarget {
		return projected, fmt.Errorf(
			"active committed app-v23 agent %s was not found", onlyAgentID,
		)
	}
	return projected, nil
}

func appV23ProjectedAgentEntry(
	onChain *OnChainAgent,
	enrollment *AppV23LocalEnrollment,
	existing *AgentEntry,
	metadata *AgentEntry,
	now time.Time,
) *AgentEntry {
	entry := &AgentEntry{
		AgentID:       onChain.AgentID,
		Role:          onChain.Role,
		Status:        "active",
		Clearance:     int(enrollment.Clearance),
		OrgID:         onChain.OrgID,
		DeptID:        onChain.DeptID,
		DomainAccess:  onChain.DomainAccess,
		OnChainHeight: onChain.RegisteredAt,
		VisibleAgents: onChain.VisibleAgents,
		Capabilities:  enrollment.Capabilities,
		CreatedAt:     now,
	}
	firstSeen := now
	entry.FirstSeen = &firstSeen
	if existing != nil {
		entry.Name = existing.Name
		entry.RegisteredName = existing.RegisteredName
		entry.Avatar = existing.Avatar
		entry.BootBio = existing.BootBio
		entry.ValidatorPubkey = existing.ValidatorPubkey
		entry.NodeID = existing.NodeID
		entry.P2PAddress = existing.P2PAddress
		entry.BundlePath = existing.BundlePath
		entry.FirstSeen = existing.FirstSeen
		entry.LastSeen = existing.LastSeen
		entry.CreatedAt = existing.CreatedAt
		entry.Provider = existing.Provider
		entry.MemoryCount = existing.MemoryCount
		entry.ClaimToken = existing.ClaimToken
		entry.ClaimExpiresAt = existing.ClaimExpiresAt
	}
	if onChain.Name != "" {
		entry.Name = onChain.Name
	}
	if onChain.RegisteredName != "" {
		entry.RegisteredName = onChain.RegisteredName
	}
	if onChain.BootBio != "" {
		entry.BootBio = onChain.BootBio
	}
	if onChain.P2PAddress != "" {
		entry.P2PAddress = onChain.P2PAddress
	}
	if onChain.Provider != "" {
		entry.Provider = onChain.Provider
	}
	if metadata != nil && metadata.AgentID == onChain.AgentID {
		if entry.Name == "" {
			entry.Name = metadata.Name
		}
		if entry.RegisteredName == "" {
			entry.RegisteredName = metadata.RegisteredName
		}
		if entry.BootBio == "" {
			entry.BootBio = metadata.BootBio
		}
		if entry.P2PAddress == "" {
			entry.P2PAddress = metadata.P2PAddress
		}
		if entry.Provider == "" {
			entry.Provider = metadata.Provider
		}
	}
	if entry.RegisteredName == "" {
		entry.RegisteredName = entry.Name
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = now
	}
	if entry.FirstSeen == nil {
		entry.FirstSeen = &firstSeen
	}
	entry.RemovedAt = nil
	return entry
}
