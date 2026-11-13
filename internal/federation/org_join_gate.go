package federation

// RFC #443 A2 JOIN integration. Organization policy stays outside the
// federation transport: the node wires one evaluator that owns the node-local
// orgpolicy state. A nil evaluator preserves the historical non-fleet path.

import (
	"context"
	"errors"
	"fmt"
)

const (
	JoinGateRequest  = "request"
	JoinGateApproval = "approval"
	JoinGateCommit   = "commit"

	JoinGateHost  = "host"
	JoinGateGuest = "guest"
)

// JoinPeerCheck is one exact host/guest eligibility decision. Now is unix
// seconds captured at the call site so policy freshness and override expiry use
// one decision clock.
type JoinPeerCheck struct {
	PeerAgentPub string
	PeerChainID  string
	SessionID    string
	Role         string
	Phase        string
	Now          int64
}

// JoinPeerDecision distinguishes an ordinary non-fleet node from a fleet node
// that admitted or rejected this peer.
type JoinPeerDecision struct {
	Managed          bool
	Allowed          bool
	Reason           string
	OrgID            string
	OverrideRevision uint64
}

// JoinPeerEvaluator is the only organization-policy dependency in the JOIN
// transport. Production wires it to internal/orgpolicy; tests provide the same
// evaluator or a deterministic stub.
type JoinPeerEvaluator func(context.Context, JoinPeerCheck) (JoinPeerDecision, error)

var errJoinPeerIneligible = errors.New("peer is not eligible for fleet JOIN")

// SetJoinPeerEvaluator installs the node's org-policy/override decision path.
// It must be called before the listener serves requests.
func (m *Manager) SetJoinPeerEvaluator(evaluator JoinPeerEvaluator) {
	m.orgJoinGateMu.Lock()
	m.orgJoinGate = evaluator
	m.orgJoinGateMu.Unlock()
}

func (m *Manager) joinPeerEvaluator() JoinPeerEvaluator {
	m.orgJoinGateMu.RLock()
	defer m.orgJoinGateMu.RUnlock()
	return m.orgJoinGate
}

// requireJoinPeerEligible fails closed for every fleet-managed node. A node
// with no EnrollmentOutcome receives Managed=false and follows the unchanged
// legacy ceremony.
func (m *Manager) requireJoinPeerEligible(
	ctx context.Context,
	peerAgentPub, peerChainID, sessionID, role, phase string,
	nowUnix int64,
) error {
	evaluator := m.joinPeerEvaluator()
	if evaluator == nil {
		return nil
	}
	decision, err := evaluator(ctx, JoinPeerCheck{
		PeerAgentPub: peerAgentPub,
		PeerChainID:  peerChainID,
		SessionID:    sessionID,
		Role:         role,
		Phase:        phase,
		Now:          nowUnix,
	})
	if err != nil {
		return fmt.Errorf("org JOIN eligibility unavailable: %w", err)
	}
	if !decision.Managed {
		return nil
	}
	if !decision.Allowed {
		reason := decision.Reason
		if reason == "" {
			reason = errJoinPeerIneligible.Error()
		}
		if decision.OrgID != "" {
			return fmt.Errorf("org %s: %s", decision.OrgID, reason)
		}
		return errors.New(reason)
	}
	return nil
}
