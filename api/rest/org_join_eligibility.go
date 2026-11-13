package rest

// RFC #443 A2 production JOIN evaluator. The federation transport calls this
// callback at request, approval, and pre-tx-33 commit boundaries; all policy,
// override, persistence, and current-Root checks remain in one path.

import (
	"context"
	"fmt"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/orgpolicy"
)

type federationJoinPeerGateSetter interface {
	SetJoinPeerEvaluator(federation.JoinPeerEvaluator)
}

func (s *Server) evaluateFederationJoinPeer(ctx context.Context, check federation.JoinPeerCheck) (federation.JoinPeerDecision, error) {
	ps := s.orgPolicyStore(ctx)
	if ps == nil {
		return federation.JoinPeerDecision{Managed: false, Allowed: true}, nil
	}
	gate := orgpolicy.JoinGate{
		Store: ps,
		Now:   func() int64 { return check.Now },
		CurrentRoot: func() (orgpolicy.CurrentRoot, error) {
			if s.badgerStore == nil {
				return orgpolicy.CurrentRoot{}, fmt.Errorf("current Root state unavailable")
			}
			root, err := s.badgerStore.GetAppV23Root()
			if err != nil {
				return orgpolicy.CurrentRoot{}, fmt.Errorf("read current Root: %w", err)
			}
			if root == nil || root.CredentialID == "" {
				return orgpolicy.CurrentRoot{}, fmt.Errorf("current Root is unavailable")
			}
			pub, err := auth.AgentIDToPublicKey(root.CredentialID)
			if err != nil {
				return orgpolicy.CurrentRoot{}, fmt.Errorf("current Root credential is invalid")
			}
			return orgpolicy.CurrentRoot{CredentialID: root.CredentialID, PublicKey: pub}, nil
		},
	}
	decision, err := gate.Evaluate(orgpolicy.JoinGateCheck{
		PeerAgentPub: check.PeerAgentPub,
		PeerChainID:  check.PeerChainID,
		SessionID:    check.SessionID,
		Role:         check.Role,
		Phase:        check.Phase,
	})
	if err != nil {
		return federation.JoinPeerDecision{}, err
	}
	return federation.JoinPeerDecision{
		Managed:          decision.Managed,
		Allowed:          decision.Allowed,
		Reason:           decision.Reason,
		OrgID:            decision.OrgID,
		OverrideRevision: decision.OverrideRevision,
	}, nil
}
