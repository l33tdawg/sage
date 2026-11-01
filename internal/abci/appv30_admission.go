package abci

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func (app *SageApp) appV30ProvenanceSigners(
	credentialID, principalID string,
	height int64,
) ([]store.AppV30ProvenanceSigner, error) {
	rootGeneration := uint64(0)
	if app.postAppV23Rules(height) {
		root, rootErr := app.badgerStore.GetAppV23Root()
		if rootErr != nil {
			return nil, fmt.Errorf("read app-v23 Root state: %w", rootErr)
		}
		if root == nil {
			return nil, fmt.Errorf("read app-v23 Root state: unavailable")
		}
		generation, found, err := app.badgerStore.GetAppV23RootCredentialGeneration(credentialID)
		if err != nil {
			return nil, fmt.Errorf("read Root credential generation: %w", err)
		}
		if found {
			rootGeneration = generation
		} else if credentialID == root.CredentialID {
			return nil, fmt.Errorf("current Root credential has no generation marker")
		}
	}
	return []store.AppV30ProvenanceSigner{{
		CredentialID:   credentialID,
		PrincipalID:    principalID,
		RootGeneration: rootGeneration,
		Exact:          true,
	}}, nil
}

func appV30CoCommitProvenanceSigners(
	root *store.AppV23RootState,
	env *tx.CoCommitSubmit,
) ([]store.AppV30ProvenanceSigner, error) {
	if root == nil || env == nil {
		return nil, fmt.Errorf("co-commit provenance requires Root state and an envelope")
	}
	signers := make([]store.AppV30ProvenanceSigner, 0, len(env.Coauthors))
	for _, coauthor := range env.Coauthors {
		credentialID := hex.EncodeToString(coauthor.PubKey)
		principalID := credentialID
		rootGeneration := uint64(0)
		if credentialID == root.CredentialID {
			principalID = root.PrincipalID
			rootGeneration = root.Generation
		}
		signers = append(signers, store.AppV30ProvenanceSigner{
			CredentialID:   credentialID,
			PrincipalID:    principalID,
			RootGeneration: rootGeneration,
			Exact:          true,
		})
	}
	return signers, nil
}

func (app *SageApp) preflightAppV30Admission(
	parsedTx *tx.ParsedTx,
	credentialID, childID string,
	parents []tx.ParentRef,
	signers []store.AppV30ProvenanceSigner,
	height int64,
	blockTime time.Time,
) (string, error) {
	if !app.postAppV30Fork(height) {
		return "", fmt.Errorf("app-v30 admission rules are not active")
	}
	localChainID := app.expectedConsensusChainIDValue()
	if len(parents) > 0 && localChainID == "" {
		return "", fmt.Errorf("local consensus chain ID is unavailable")
	}
	if len(parents) > 0 {
		if err := app.badgerStore.ValidateMemoryLineageParents(
			childID,
			localChainID,
			parents,
		); err != nil {
			if errors.Is(err, store.ErrMemoryLineageUnavailable) {
				return "", appV30LineageParentUnavailable()
			}
			return "", err
		}
		existing, err := app.badgerStore.GetMemoryLineage(childID)
		if err != nil {
			return "", err
		}
		if len(existing) > 0 {
			existingEncoded, existingErr := tx.EncodeDerivedFromExtension(existing)
			if existingErr != nil {
				return "", fmt.Errorf("existing lineage is malformed: %w", existingErr)
			}
			parentsEncoded, parentsErr := tx.EncodeDerivedFromExtension(parents)
			if parentsErr != nil {
				return "", parentsErr
			}
			if !bytes.Equal(existingEncoded, parentsEncoded) {
				return "", fmt.Errorf("memory lineage conflicts with the submitted parent set")
			}
		}
		for _, parent := range parents {
			state, stateErr := app.badgerStore.GetMemoryDisclosureState(parent.MemoryID)
			if stateErr != nil {
				return "", appV30LineageParentUnavailable()
			}
			if !state.DomainRecorded || state.Domain == "" {
				return "", appV30LineageParentUnavailable()
			}
			withinClearance, clearanceErr := app.appV23MemoryWithinClearance(
				credentialID,
				parent.MemoryID,
			)
			if clearanceErr != nil || !withinClearance {
				return "", appV30LineageParentUnavailable()
			}
			allowed, _, decisionErr := app.appV23DomainDecision(
				parsedTx,
				credentialID,
				state.Domain,
				store.AppV23VerbRead,
				height,
				blockTime,
			)
			if decisionErr != nil || !allowed {
				return "", appV30LineageParentUnavailable()
			}
		}
	}
	if _, err := app.badgerStore.ValidateTaintAdmission(
		localChainID,
		childID,
		parents,
		signers,
	); err != nil {
		return "", err
	}
	return localChainID, nil
}

func appV30LineageParentUnavailable() error {
	return fmt.Errorf(
		"%w: parent cannot be resolved for this caller",
		store.ErrMemoryLineageUnavailable,
	)
}

func (app *SageApp) applyAppV30Admission(
	localChainID, childID string,
	parents []tx.ParentRef,
	signers []store.AppV30ProvenanceSigner,
) (store.InheritedTaintResult, error) {
	if len(parents) > 0 {
		if err := app.badgerStore.SetMemoryLineage(childID, parents); err != nil {
			return store.InheritedTaintResult{}, err
		}
	}
	return app.badgerStore.ApplyTaintAdmission(
		localChainID,
		childID,
		parents,
		signers,
	)
}
