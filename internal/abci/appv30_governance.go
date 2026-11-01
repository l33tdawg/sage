package abci

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

var errAppV30TaintStateDrift = errors.New("app-v30 taint execution state changed")

type appV30TaintProposal struct {
	Action     uint8
	DecisionID string
	Apply      *tx.TaintApplyPayload
	Clear      *tx.TaintClearPayload
	Target     tx.ParentRef
	LocalChain string
}

func (app *SageApp) validateAppV30TaintProposal(
	targetID string,
	targetPubKey []byte,
	targetPower int64,
	payload []byte,
	height int64,
) (appV30TaintProposal, error) {
	var proposal appV30TaintProposal
	if !app.postAppV30Fork(height) {
		return proposal, fmt.Errorf("OpMemoryTaint is not active")
	}
	if len(targetPubKey) != 0 || targetPower != 0 {
		return proposal, fmt.Errorf("OpMemoryTaint must not carry target_pubkey or target_power")
	}
	if len(payload) < 2 {
		return proposal, fmt.Errorf("taint payload is too short")
	}
	localChainID := app.expectedConsensusChainIDValue()
	if localChainID == "" {
		return proposal, fmt.Errorf("local consensus chain ID is unavailable")
	}
	proposal.LocalChain = localChainID

	switch payload[1] {
	case tx.TaintActionApply:
		apply, err := tx.DecodeTaintApplyPayload(payload)
		if err != nil {
			return proposal, err
		}
		if apply.TargetChainID != localChainID {
			return proposal, fmt.Errorf("taint target chain is not local")
		}
		decisionID, err := tx.TaintApplyDecisionID(payload)
		if err != nil {
			return proposal, err
		}
		if targetID != decisionID {
			return proposal, fmt.Errorf("taint apply target_id does not match the canonical decision ID")
		}
		if err := app.validateAppV30TaintApplyScope(*apply); err != nil {
			return proposal, err
		}
		if err := app.validateAppV30TaintTarget(*apply); err != nil {
			return proposal, err
		}
		payloadHash := sha256.Sum256(payload)
		header, found, err := app.badgerStore.GetTaintDecision(decisionID)
		if err != nil {
			return proposal, err
		}
		if found {
			if header.Action != taint.ActionApply || header.PayloadHash != payloadHash {
				return proposal, fmt.Errorf(
					"%w: taint apply decision conflicts with existing state",
					errAppV30TaintStateDrift,
				)
			}
		} else {
			active, countErr := app.badgerStore.ActiveTaintDecisionCount()
			if countErr != nil {
				return proposal, countErr
			}
			if active >= taint.MaxActiveDecisions {
				return proposal, fmt.Errorf(
					"%w: active taint decision capacity is exhausted",
					errAppV30TaintStateDrift,
				)
			}
		}
		proposal.Action = tx.TaintActionApply
		proposal.DecisionID = decisionID
		proposal.Apply = apply
		proposal.Target = tx.ParentRef{
			SourceChainID: localChainID,
			MemoryID:      apply.TargetMemoryID,
		}
		copy(proposal.Target.ContentHash[:], apply.TargetContentHash)

	case tx.TaintActionClear:
		clear, err := tx.DecodeTaintClearPayload(payload)
		if err != nil {
			return proposal, err
		}
		clearTargetID, err := tx.TaintClearTargetID(payload)
		if err != nil {
			return proposal, err
		}
		if targetID != clearTargetID {
			return proposal, fmt.Errorf("taint clear target_id does not match the canonical clear target")
		}
		decisionID := fmt.Sprintf("%x", clear.DecisionID)
		header, found, err := app.badgerStore.GetTaintDecision(decisionID)
		if err != nil {
			return proposal, err
		}
		if !found {
			return proposal, fmt.Errorf(
				"%w: taint clear decision does not exist",
				errAppV30TaintStateDrift,
			)
		}
		if header.Action != taint.ActionApply {
			return proposal, fmt.Errorf(
				"%w: taint clear decision has an invalid action",
				errAppV30TaintStateDrift,
			)
		}
		proposal.Action = tx.TaintActionClear
		proposal.DecisionID = decisionID
		proposal.Clear = clear

	default:
		return proposal, fmt.Errorf("unsupported taint action %d", payload[1])
	}
	return proposal, nil
}

func (app *SageApp) validateAppV30TaintApplyScope(
	apply tx.TaintApplyPayload,
) error {
	switch apply.ScopeKind {
	case tx.TaintScopeDirect:
		return nil
	case tx.TaintScopeRootGeneration:
		root, err := app.badgerStore.GetAppV23Root()
		if err != nil || root == nil {
			return fmt.Errorf("Root state is unavailable")
		}
		if apply.PrincipalID != root.PrincipalID {
			return fmt.Errorf("taint Root principal does not match the permanent Root principal")
		}
		if apply.GenerationTo > root.Generation {
			return fmt.Errorf("taint Root generation range includes a future generation")
		}
		return nil
	case tx.TaintScopeCredentialSet:
		for _, credential := range apply.Credentials {
			publicKey, err := auth.AgentIDToPublicKey(credential)
			if err != nil ||
				auth.PublicKeyToAgentID(publicKey) != credential {
				return fmt.Errorf("taint credential is not a canonical agent ID")
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported taint scope %d", apply.ScopeKind)
	}
}

func (app *SageApp) validateAppV30TaintTarget(
	apply tx.TaintApplyPayload,
) error {
	state, err := app.badgerStore.GetMemoryDisclosureState(apply.TargetMemoryID)
	if err != nil {
		return fmt.Errorf("read taint target: %w", err)
	}
	if state.Status != "committed" && state.Status != "challenged" {
		return fmt.Errorf(
			"%w: taint target is not committed or challenged",
			errAppV30TaintStateDrift,
		)
	}
	if !bytes.Equal(state.ContentHash, apply.TargetContentHash) {
		return fmt.Errorf(
			"%w: taint target content hash does not match canonical state",
			errAppV30TaintStateDrift,
		)
	}
	return nil
}

func (app *SageApp) applyAppV30TaintProposal(
	proposal *governance.ProposalState,
	height int64,
) error {
	parsed, err := app.validateAppV30TaintProposal(
		proposal.TargetID,
		proposal.TargetPubKey,
		proposal.TargetPower,
		proposal.Payload,
		height,
	)
	if err != nil {
		return err
	}
	switch parsed.Action {
	case tx.TaintActionApply:
		payloadHash := sha256.Sum256(proposal.Payload)
		if _, err := app.badgerStore.ApplyTaintDecision(
			parsed.LocalChain,
			parsed.DecisionID,
			parsed.Target,
			payloadHash,
		); err != nil {
			return err
		}
		header, found, err := app.badgerStore.GetTaintDecision(parsed.DecisionID)
		if err != nil {
			return err
		}
		if !found || !header.Status.Active() {
			return nil
		}
		_, err = app.badgerStore.ExpandTaintDecision(
			parsed.LocalChain,
			parsed.DecisionID,
			taint.PerExecutionUnitBudget,
		)
		return err
	case tx.TaintActionClear:
		if _, err := app.badgerStore.ClearTaintDecision(parsed.DecisionID); err != nil {
			return err
		}
		_, err := app.badgerStore.CleanupTaintDecision(
			parsed.DecisionID,
			taint.PerExecutionUnitBudget,
		)
		return err
	default:
		return fmt.Errorf("unsupported taint action")
	}
}
