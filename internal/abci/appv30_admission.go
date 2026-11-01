package abci

import (
	"bytes"
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

func (app *SageApp) preflightAppV30Lineage(
	parsedTx *tx.ParsedTx,
	credentialID, childID string,
	parents []tx.ParentRef,
	height int64,
	blockTime time.Time,
) (string, error) {
	if len(parents) == 0 {
		return "", nil
	}
	if !app.postAppV30Fork(height) {
		return "", fmt.Errorf("derived_from extension is not active")
	}
	localChainID := app.expectedConsensusChainIDValue()
	if localChainID == "" {
		return "", fmt.Errorf("local consensus chain ID is unavailable")
	}
	if err := app.badgerStore.ValidateMemoryLineageParents(
		childID,
		localChainID,
		parents,
	); err != nil {
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
			return "", fmt.Errorf("read parent disclosure state: %w", stateErr)
		}
		if !state.DomainRecorded || state.Domain == "" {
			return "", fmt.Errorf("parent %s has no authenticated domain", parent.MemoryID)
		}
		withinClearance, clearanceErr := app.appV23MemoryWithinClearance(
			credentialID,
			parent.MemoryID,
		)
		if clearanceErr != nil || !withinClearance {
			return "", fmt.Errorf("parent %s is outside caller clearance", parent.MemoryID)
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
			return "", fmt.Errorf("parent %s is not readable by the caller", parent.MemoryID)
		}
	}
	if _, err := app.badgerStore.ValidateTaintInheritance(
		localChainID,
		childID,
		parents,
	); err != nil {
		return "", err
	}
	return localChainID, nil
}

func (app *SageApp) applyAppV30Lineage(
	localChainID, childID string,
	parents []tx.ParentRef,
) (store.InheritedTaintResult, error) {
	if len(parents) == 0 {
		return store.InheritedTaintResult{}, nil
	}
	if err := app.badgerStore.SetMemoryLineage(childID, parents); err != nil {
		return store.InheritedTaintResult{}, err
	}
	return app.badgerStore.InheritTaintMemberships(
		localChainID,
		childID,
		parents,
	)
}
