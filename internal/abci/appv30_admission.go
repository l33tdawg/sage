package abci

import (
	"fmt"

	"github.com/l33tdawg/sage/internal/store"
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
