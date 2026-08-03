package abci

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/authzdenial"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func (app *SageApp) postAppV23Fork(height int64) bool {
	return app.postAppV23GenesisRules(height) ||
		app.appV23AppliedHeight > 0 && height > app.appV23AppliedHeight
}

func (app *SageApp) postAppV23GenesisRules(height int64) bool {
	return app.appV23GenesisActive && height > 0
}

func (app *SageApp) postAppV23Rules(height int64) bool {
	return app.postAppV23Fork(height)
}

func (app *SageApp) IsAppV23ActiveForNextTx() bool {
	app.runtimeViewMu.RLock()
	defer app.runtimeViewMu.RUnlock()
	return app.isAppV23ActiveForNextTx()
}

func (app *SageApp) isAppV23ActiveForNextTx() bool {
	return app.state != nil && app.postAppV23Rules(app.state.Height+1)
}

func (app *SageApp) refreshAppV23Fork() error {
	app.appV23AppliedHeight = 0
	app.appV23GenesisActive = false
	genesis, err := app.badgerStore.GetAppV23GenesisActivation()
	if err != nil {
		return fmt.Errorf("read app-v23 genesis activation: %w", err)
	}
	rec, err := app.badgerStore.GetAppliedUpgrade(appV23UpgradeName)
	if err != nil {
		return fmt.Errorf("read applied %s record: %w", appV23UpgradeName, err)
	}
	if genesis != nil {
		if err := app.badgerStore.ValidateAppV23GenesisLineage(); err != nil {
			return fmt.Errorf("invalid app-v23 genesis lineage: %w", err)
		}
		if genesis.Version != AppV23GenesisAppVersion || genesis.Scope == "" ||
			genesis.BootstrapDigest == "" {
			return errors.New("app-v23 genesis activation marker is invalid")
		}
		root, rootErr := app.badgerStore.GetAppV23Root()
		if rootErr != nil || root == nil {
			return errors.New("app-v23 genesis activation has no root state")
		}
		if root.PrincipalID != genesis.RootID ||
			root.Scope != genesis.Scope ||
			root.BootstrapDigest != genesis.BootstrapDigest {
			return errors.New("app-v23 genesis activation does not match root state")
		}
		if app.state != nil && app.state.Height == 0 {
			validators, validatorErr := app.badgerStore.LoadValidators()
			if validatorErr != nil {
				return fmt.Errorf("read app-v23 genesis validator state: %w", validatorErr)
			}
			if len(validators) != 1 ||
				validators[genesis.ValidatorID] != genesis.ValidatorPower {
				return errors.New("app-v23 genesis validator state does not match activation provenance")
			}
		}
		domain, domainErr := app.badgerStore.GetState(governanceDelegationDomainStateKey)
		decodedScope, decodeErr := hex.DecodeString(genesis.Scope)
		if domainErr != nil || decodeErr != nil || len(decodedScope) != sha256.Size ||
			!bytes.Equal(domain, decodedScope) {
			return errors.New("app-v23 genesis activation governance domain is invalid")
		}
		if err := app.validateAppV23StateForCurrentUpgrade(); err != nil {
			return fmt.Errorf("app-v23 genesis activation has invalid local RBAC state: %w", err)
		}
		app.appV23GenesisActive = true
		return nil
	}
	if rec == nil {
		return nil
	}
	if rec.Name != appV23UpgradeName {
		return fmt.Errorf("applied %s record has name %q", appV23UpgradeName, rec.Name)
	}
	if rec.TargetAppVersion != 23 {
		return fmt.Errorf("applied %s record has target app version %d, want 23", appV23UpgradeName, rec.TargetAppVersion)
	}
	if rec.AppliedHeight <= 0 {
		return fmt.Errorf("applied %s record has non-positive height %d", appV23UpgradeName, rec.AppliedHeight)
	}
	if app.state == nil {
		return fmt.Errorf("applied %s record cannot be checked without app state", appV23UpgradeName)
	}
	if app.state.Height < rec.AppliedHeight-1 {
		return fmt.Errorf("applied %s height %d is ahead of persisted app height %d", appV23UpgradeName, rec.AppliedHeight, app.state.Height)
	}
	app.appV23AppliedHeight = rec.AppliedHeight
	return nil
}

// validateAppV23StateForCurrentUpgrade permits only the historical home-domain
// defects produced by app-v25 batch continuity, and only while app-v25 is
// applied and app-v26 is not. It performs no mutation; its sole purpose is to
// let the node reach the deterministic app-v26 repair transaction.
func (app *SageApp) validateAppV23StateForCurrentUpgrade() error {
	strictErr := app.badgerStore.ValidateAppV23State()
	if strictErr == nil {
		return nil
	}
	v26, err := app.badgerStore.GetAppliedUpgrade(appV26UpgradeName)
	if err != nil {
		return err
	}
	if v26 != nil {
		return strictErr
	}
	v25, err := app.badgerStore.GetAppliedUpgrade(appV25UpgradeName)
	if err != nil || v25 == nil || v25.TargetAppVersion != 25 {
		return strictErr
	}
	if err := app.badgerStore.ValidateAppV23StateForPreV26Recovery(); err != nil {
		return strictErr
	}
	app.logger.Warn().Err(strictErr).Msg(
		"pre-app-v26 local RBAC home defect will be repaired by app-v26 activation",
	)
	return nil
}

func (app *SageApp) validatePersistedAppV23PredecessorLadder() (int64, error) {
	lastHeight, err := app.validatePersistedAppV22PredecessorLadder()
	if err != nil {
		return 0, err
	}
	rec, err := app.badgerStore.GetAppliedUpgrade(appV22UpgradeName)
	if err != nil {
		return 0, fmt.Errorf("read applied %s predecessor: %w", appV22UpgradeName, err)
	}
	if rec == nil {
		return 0, fmt.Errorf("missing canonical applied %s predecessor", appV22UpgradeName)
	}
	if rec.Name != appV22UpgradeName || rec.TargetAppVersion != 22 || rec.AppliedHeight <= lastHeight {
		return 0, fmt.Errorf("invalid canonical applied %s predecessor", appV22UpgradeName)
	}
	if app.state != nil && rec.AppliedHeight > app.state.Height+1 {
		return 0, fmt.Errorf("applied %s predecessor height %d is ahead of persisted app height %d", appV22UpgradeName, rec.AppliedHeight, app.state.Height)
	}
	return rec.AppliedHeight, nil
}

func (app *SageApp) validateAppV23Prerequisite() error {
	if app.appV23GenesisActive {
		return nil
	}
	if app.appV23AppliedHeight <= 0 {
		return nil
	}
	lastHeight, err := app.validatePersistedAppV23PredecessorLadder()
	if err != nil {
		return fmt.Errorf("applied %s has invalid predecessor ladder: %w", appV23UpgradeName, err)
	}
	if app.appV23AppliedHeight <= lastHeight {
		return fmt.Errorf("applied %s height %d must be after applied %s predecessor height %d", appV23UpgradeName, app.appV23AppliedHeight, appV22UpgradeName, lastHeight)
	}
	migration, err := app.badgerStore.GetAppV23MigrationState()
	if err != nil {
		return fmt.Errorf("read %s migration state: %w", appV23UpgradeName, err)
	}
	if migration == nil {
		return fmt.Errorf("applied %s is missing migration state", appV23UpgradeName)
	}
	if err := app.validateAppV23StateForCurrentUpgrade(); err != nil {
		return fmt.Errorf("applied %s has invalid local RBAC state: %w", appV23UpgradeName, err)
	}
	return nil
}

func appV23Denial(code authzdenial.Code) *abcitypes.ExecTxResult {
	return &abcitypes.ExecTxResult{
		Code: 110,
		Log:  fmt.Sprintf("access denied: denial_code=%s", code),
	}
}

func appV23ControlDenied() *abcitypes.ExecTxResult {
	return &abcitypes.ExecTxResult{Code: 110, Log: "access denied"}
}

func appV23ControlPlaneType(txType tx.TxType) bool {
	switch txType {
	case tx.TxTypeAccessRequest, tx.TxTypeAccessGrant, tx.TxTypeAccessRevoke,
		tx.TxTypeDomainRegister,
		tx.TxTypeOrgRegister, tx.TxTypeOrgAddMember, tx.TxTypeOrgRemoveMember, tx.TxTypeOrgSetClearance,
		tx.TxTypeFederationPropose, tx.TxTypeFederationApprove, tx.TxTypeFederationRevoke,
		tx.TxTypeDeptRegister, tx.TxTypeDeptAddMember, tx.TxTypeDeptRemoveMember,
		tx.TxTypeAgentRegister, tx.TxTypeAgentUpdate, tx.TxTypeAgentSetPermission,
		tx.TxTypeMemoryReassign,
		tx.TxTypeGovPropose, tx.TxTypeGovVote, tx.TxTypeGovCancel,
		tx.TxTypeUpgradePropose, tx.TxTypeUpgradeCancel, tx.TxTypeUpgradeRevert,
		tx.TxTypeDomainReassign,
		tx.TxTypeCrossFedSet, tx.TxTypeCrossFedRevoke,
		tx.TxTypeLocalAgentApprove, tx.TxTypeAgentRoleChange, tx.TxTypeAccessGroupMutate:
		return true
	default:
		return false
	}
}

// enforceAppV23ControlElevation closes the host-boundary gap for every
// transaction attributable to a promoted Admin. Requiring the proof even when
// the action could have succeeded through ordinary ownership prevents a valid
// but unused proof becoming replayable after state changes. New app-v23
// mutations defer nonce consumption to their atomic store transaction.
func (app *SageApp) enforceAppV23ControlElevation(parsedTx *tx.ParsedTx, height int64) error {
	if !app.postAppV23Rules(height) {
		if parsedTx.LocalElevation != nil {
			return errors.New("local elevation unavailable before app-v23")
		}
		return nil
	}
	// These envelopes are authenticated on a non-agent identity plane:
	// MemoryVote is validator-key-only, CoCommitAttest carries its peer proof
	// inside the payload, and a direct GovVote is a validator vote. Requiring a
	// local enrollment for their outer signer would strand consensus whenever
	// the validator key is intentionally distinct from agent.key. A
	// proof-bearing governance vote is different: its embedded agent is an
	// explicit delegated authorizer and remains subject to the normal v23
	// elevation rules below.
	nonAgentActor := parsedTx.Type == tx.TxTypeMemoryVote ||
		parsedTx.Type == tx.TxTypeCoCommitAttest ||
		(parsedTx.Type == tx.TxTypeGovVote && !hasAgentProofMaterial(parsedTx))
	if nonAgentActor {
		if parsedTx.LocalElevation != nil {
			return errors.New("non-agent transaction supplied local elevation")
		}
		return nil
	}
	actorID, identityErr := verifyAgentIdentity(parsedTx)
	if identityErr != nil {
		if len(parsedTx.PublicKey) != ed25519.PublicKeySize {
			if parsedTx.LocalElevation != nil {
				return errors.New("local elevation has no authenticated principal")
			}
			return nil
		}
		actorID = auth.PublicKeyToAgentID(parsedTx.PublicKey)
	}
	root, _, role, actorErr := app.appV23Actor(actorID)
	if actorErr != nil {
		if parsedTx.LocalElevation != nil {
			return errors.New("unapproved principal supplied local elevation")
		}
		// Registration is the one control-plane operation a pending ordinary
		// principal may repeat. The transaction is self-signed and the
		// consensus handler is idempotent: it preserves the immutable canonical
		// identity and cannot grant a role, enrollment, domain, or capability.
		// This is what lets an operator reject a review request locally while
		// allowing the same key to request review again later. Root and legacy
		// Admin identities remain excluded so they cannot fall through legacy
		// role checks that still project Role=="admin".
		if errors.Is(actorErr, store.ErrAppV23NeedsApproval) &&
			parsedTx.Type == tx.TxTypeAgentRegister {
			registered, registrationErr := app.badgerStore.GetRegisteredAgent(actorID)
			if errors.Is(registrationErr, badger.ErrKeyNotFound) {
				return nil
			}
			if registrationErr != nil {
				return fmt.Errorf("read app-v23 registration state: %w", registrationErr)
			}
			if registered == nil || registered.Role != store.AppV23RoleAdmin {
				return nil
			}
		}
		if errors.Is(actorErr, store.ErrAppV23NeedsApproval) &&
			!appV23ControlPlaneType(parsedTx.Type) {
			registered, registrationErr := app.badgerStore.GetRegisteredAgent(actorID)
			if registrationErr != nil {
				return fmt.Errorf("read app-v23 registration state: %w", registrationErr)
			}
			if registered != nil && registered.Role != store.AppV23RoleAdmin {
				// Data-plane evaluators return the precise stable denial (for
				// example principal_pending_review). Only legacy Admin
				// projections are dangerous to let fall through this gate.
				return nil
			}
		}
		return fmt.Errorf("app-v23 principal is not currently authorized: %w", actorErr)
	}
	if actorID == root.CredentialID {
		if parsedTx.LocalElevation != nil {
			return errors.New("root control action must not carry delegated elevation")
		}
		return nil
	}
	if role.Role != store.AppV23RoleAdmin {
		if parsedTx.LocalElevation != nil {
			return errors.New("non-admin principal supplied local elevation")
		}
		return nil
	}
	actionBytes, err := tx.PayloadBytes(parsedTx)
	if err != nil {
		return err
	}
	use, err := app.appV23ElevationUse(
		actorID, root, parsedTx.LocalElevation, parsedTx.Type, actionBytes, height,
	)
	if err != nil {
		return err
	}
	switch parsedTx.Type {
	case tx.TxTypeLocalAgentApprove, tx.TxTypeAgentRoleChange, tx.TxTypeAccessGroupMutate:
		return nil
	default:
		return app.badgerStore.ConsumeAppV23Elevation(use, height)
	}
}

func mergeAppV23Holders(base, additional []string, limit int) ([]string, bool) {
	set := make(map[string]struct{}, len(base)+len(additional))
	for _, agentID := range base {
		set[agentID] = struct{}{}
	}
	for _, agentID := range additional {
		set[agentID] = struct{}{}
	}
	if len(set) > limit {
		return nil, true
	}
	out := make([]string, 0, len(set))
	for agentID := range set {
		out = append(out, agentID)
	}
	sort.Strings(out)
	return out, false
}

func (app *SageApp) appV23Actor(agentID string) (*store.AppV23RootState, *store.AppV23LocalEnrollment, *store.AppV23RoleState, error) {
	root, err := app.badgerStore.GetAppV23Root()
	if err != nil || root == nil {
		return nil, nil, nil, errors.New("app-v23 root state is unavailable")
	}
	policyID := agentID
	if agentID == root.CredentialID {
		policyID = root.PrincipalID
	} else {
		wasRoot, markerErr := app.badgerStore.IsAppV23RootCredential(agentID)
		if markerErr != nil {
			return nil, nil, nil, markerErr
		}
		if wasRoot {
			// Every retired generation must remain unable to fall through as an
			// ordinary principal merely because it is no longer the current key.
			return root, nil, nil, store.ErrAppV23NeedsApproval
		}
	}
	enrollment, err := app.badgerStore.GetAppV23Enrollment(policyID)
	if err != nil {
		return nil, nil, nil, err
	}
	if enrollment == nil || !enrollment.Active {
		return root, nil, nil, store.ErrAppV23NeedsApproval
	}
	role, err := app.badgerStore.GetAppV23Role(policyID)
	if err != nil {
		return nil, nil, nil, err
	}
	if role == nil ||
		store.ValidateAppV23Policy(role.Role, enrollment.Profile, enrollment.Capabilities, enrollment.Clearance) != nil {
		return root, enrollment, nil, errors.New("incompatible app-v23 role/profile state")
	}
	if policyID != root.PrincipalID && role.Role == store.AppV23RoleAdmin &&
		enrollment.RootGeneration != root.Generation {
		return root, nil, nil, store.ErrAppV23NeedsApproval
	}
	return root, enrollment, role, nil
}

func (app *SageApp) appV23ElevationUse(
	actorID string,
	root *store.AppV23RootState,
	proof *tx.LocalElevationProof,
	txType tx.TxType,
	actionBytes []byte,
	height int64,
) (*store.AppV23ElevationUse, error) {
	if actorID == root.CredentialID {
		if proof != nil {
			return nil, errors.New("root action must not carry delegated elevation")
		}
		return nil, nil
	}
	if proof == nil || proof.RootGeneration != root.Generation ||
		proof.ValidFromHeight > height || proof.ValidUntilHeight < height ||
		proof.ValidUntilHeight < proof.ValidFromHeight ||
		proof.ValidUntilHeight-proof.ValidFromHeight > store.AppV23MaxElevationWindow ||
		len(proof.Signature) != ed25519.SignatureSize {
		return nil, errors.New("invalid local CEREBRUM elevation proof")
	}
	rootKey, err := auth.AgentIDToPublicKey(root.CredentialID)
	if err != nil || !ed25519.Verify(
		rootKey,
		tx.AppV23ElevationSignBytes(root.Scope, actorID, txType, actionBytes, proof),
		proof.Signature,
	) {
		return nil, errors.New("invalid local CEREBRUM elevation signature")
	}
	return &store.AppV23ElevationUse{
		AdminID: actorID, RootGeneration: proof.RootGeneration,
		ValidFromHeight: proof.ValidFromHeight, ValidUntilHeight: proof.ValidUntilHeight,
		Nonce: proof.Nonce,
	}, nil
}

// appV23DomainDecision is the single consensus-side data-plane policy entry
// point. Explicit capability restrictions are evaluated before role/group
// allows. A non-root approved principal must retain ownership of its assigned
// non-shared home domain; losing it fails closed until an atomic re-approval.
func (app *SageApp) appV23DomainDecision(
	parsedTx *tx.ParsedTx,
	agentID, domain string,
	verb store.AppV23DomainVerb,
	height int64,
	blockTime time.Time,
) (bool, authzdenial.Code, error) {
	root, enrollment, role, actorErr := app.appV23Actor(agentID)
	if errors.Is(actorErr, store.ErrAppV23NeedsApproval) {
		return false, authzdenial.CodePrincipalPendingReview, nil
	}
	if actorErr != nil {
		return false, "", actorErr
	}
	if enrollment.Profile == store.AppV23ProfileReadOnly && verb >= store.AppV23VerbWrite {
		// Missing-write-grant would prescribe a grant that can never override a
		// profile hard deny. The stable restricted-write remedy requires the
		// operator to change the named profile.
		return false, authzdenial.CodeForeignWriteRestricted, nil
	}
	if agentID != root.CredentialID &&
		enrollment.Profile != store.AppV23ProfileReadOnly &&
		(enrollment.HomeDomain != "" ||
			!store.AppV23AllowsMigratedDomainless(
				enrollment.Profile, enrollment.Capabilities,
			)) {
		if enrollment.HomeDomain == "" || app.isSharedDomain(enrollment.HomeDomain, height) {
			return false, authzdenial.CodeNoOwnedHomeDomain, nil
		}
		owner, ownerErr := app.badgerStore.GetDomainOwner(enrollment.HomeDomain)
		if ownerErr != nil || owner != agentID {
			return false, authzdenial.CodeNoOwnedHomeDomain, nil
		}
	}
	if verb <= store.AppV23VerbModify {
		var restored bool
		var restoreErr error
		if verb == store.AppV23VerbModify {
			restored, restoreErr =
				app.badgerStore.AppV25AllowsHistoricalDomainModify(agentID, domain)
		} else {
			restored, restoreErr =
				app.badgerStore.AppV25AllowsHistoricalDomainWrite(agentID, domain)
		}
		if restoreErr != nil {
			return false, "", restoreErr
		}
		if restored {
			// Exact app-v25 continuity is evaluated only after pending,
			// Read-only, and home-integrity hard denials. Its consensus record is
			// revision- and current-policy-bound, so this bypasses only the
			// historical mask-2/mask-8 that caused the migration lockout.
			return true, "", nil
		}
	}
	shared := app.isSharedDomain(domain, height)
	if verb >= store.AppV23VerbWrite && shared &&
		enrollment.Capabilities.Has(store.AgentCapabilityDenySharedDomainWrite) {
		return false, authzdenial.CodeSharedWriteRestricted, nil
	}
	if shared {
		if verb == store.AppV23VerbRead &&
			enrollment.Capabilities.Has(store.AgentCapabilityReadAllDomains) {
			return true, "", nil
		}
		if role.Role == store.AppV23RoleAdmin {
			return true, "", nil
		}
		recoveredGroup, recoveredGroupErr :=
			app.badgerStore.AuthorizeAppV25RecoveredGroupDomain(agentID, domain, verb)
		if recoveredGroupErr != nil {
			return false, "", recoveredGroupErr
		}
		if recoveredGroup {
			return true, "", nil
		}
		if verb == store.AppV23VerbWrite {
			grandfathered, grandfatherErr :=
				app.badgerStore.AppV23AllowsGrandfatheredSharedDomainWrite(agentID, domain)
			if grandfatherErr != nil {
				return false, "", grandfatherErr
			}
			if grandfathered {
				return true, "", nil
			}
		}
		requiredLevel := uint8(1)
		switch verb {
		case store.AppV23VerbWrite:
			requiredLevel = 2
		case store.AppV23VerbModify:
			requiredLevel = 3
		}
		hasGrant, grantErr := app.badgerStore.HasAppV23AccessOrAncestor(
			domain, agentID, requiredLevel, blockTime, true,
		)
		if grantErr != nil {
			return false, "", grantErr
		}
		if hasGrant {
			return true, "", nil
		}
		if role.Role == store.AppV23RoleManager {
			if verb == store.AppV23VerbRead {
				return false, "", nil
			}
			return false, authzdenial.CodeManagerScopeDenied, nil
		}
		if verb == store.AppV23VerbRead {
			return false, "", nil
		}
		return false, authzdenial.CodeMissingWriteGrant, nil
	}
	owner, _, ownerErr := app.badgerStore.ResolveAppV23OwningAncestor(domain)
	if ownerErr != nil {
		return false, "", ownerErr
	}
	if owner == "" {
		if verb == store.AppV23VerbRead &&
			enrollment.Capabilities.Has(store.AgentCapabilityReadAllDomains) {
			// ReadAll intentionally preserves visibility of ownerless
			// historical memories. It never implies claim or mutation
			// authority, and classification is still checked separately.
			return true, "", nil
		}
		if role.Role == store.AppV23RoleAdmin {
			return true, "", nil
		}
		// The public denial taxonomy below is mutation-only. An ownerless
		// historical domain cannot be read through ownership or group scope,
		// but that is not a domain-claim attempt and must not emit a claim or
		// Manager write-scope remedy.
		if verb == store.AppV23VerbRead {
			return false, "", nil
		}
		if enrollment.Capabilities.Has(store.AgentCapabilityDenyDomainClaim) {
			return false, authzdenial.CodeDomainClaimRestricted, nil
		}
		// App-v23 migration must preserve healthy legacy mask-0 behavior:
		// an active Standard Member or Manager could claim a new non-shared
		// domain by explicit registration or first memory write before the
		// upgrade. Continue allowing that exact write verb. Companion,
		// Read-only, pending, and restricted profiles remain denied, and an
		// ownerless domain never acquires modify authority merely because a
		// principal could have claimed it with a separate write.
		if verb == store.AppV23VerbWrite &&
			(enrollment.Profile == store.AppV23ProfileStandard ||
				enrollment.Profile == store.AppV23ProfileLegacyRestricted) &&
			(role.Role == store.AppV23RoleMember ||
				role.Role == store.AppV23RoleManager) {
			return true, "", nil
		}
		if role.Role == store.AppV23RoleManager && domain != enrollment.HomeDomain {
			return false, authzdenial.CodeManagerScopeDenied, nil
		}
		return false, authzdenial.CodeDomainClaimRestricted, nil
	}
	if owner == agentID {
		return true, "", nil
	}
	if role.Role == store.AppV23RoleAdmin {
		return true, "", nil
	}
	if verb >= store.AppV23VerbWrite &&
		enrollment.Capabilities.Has(store.AgentCapabilityDenyForeignDomainWrite) {
		legacyModify := false
		if verb == store.AppV23VerbModify {
			legacyModifyAllowed, legacyModifyErr :=
				app.badgerStore.AppV23AllowsLegacyForeignModify(agentID)
			if legacyModifyErr != nil {
				return false, "", legacyModifyErr
			}
			legacyModify = legacyModifyAllowed
		}
		if !legacyModify {
			return false, authzdenial.CodeForeignWriteRestricted, nil
		}
	}
	requiredLevel := uint8(1)
	switch verb {
	case store.AppV23VerbWrite:
		requiredLevel = 2
	case store.AppV23VerbModify:
		requiredLevel = 3
	}
	hasGrant, err := app.badgerStore.HasAppV23AccessOrAncestor(
		domain, agentID, requiredLevel, blockTime, false,
	)
	if err != nil {
		return false, "", err
	}
	if hasGrant {
		return true, "", nil
	}
	decision, err := app.badgerStore.AuthorizeAppV23LocalDomain(agentID, domain, verb, false)
	if err != nil {
		return false, "", err
	}
	if decision.ExplicitDeny {
		if enrollment.Capabilities.Has(store.AgentCapabilityDenyForeignDomainWrite) {
			return false, authzdenial.CodeForeignWriteRestricted, nil
		}
		return false, "", errors.New("incompatible app-v23 authorization state")
	}
	if decision.Allowed {
		return true, "", nil
	}
	if role.Role == store.AppV23RoleManager && owner != agentID {
		if verb == store.AppV23VerbRead {
			return false, "", nil
		}
		return false, authzdenial.CodeManagerScopeDenied, nil
	}
	if verb == store.AppV23VerbRead {
		return false, "", nil
	}
	return false, authzdenial.CodeMissingWriteGrant, nil
}

// appV23MemoryWithinClearance is the consensus resource-classification gate
// for lifecycle transactions that name an existing memory by ID. Domain scope
// alone is insufficient: a Manager may hold Modify over a group while still
// lacking clearance for a classified memory in that domain.
func (app *SageApp) appV23MemoryWithinClearance(agentID, memoryID string) (bool, error) {
	_, enrollment, _, err := app.appV23Actor(agentID)
	if err != nil || enrollment == nil {
		return false, err
	}
	classification, err := app.badgerStore.GetMemoryClassification(memoryID)
	if err != nil {
		return false, err
	}
	return classification <= enrollment.Clearance, nil
}

type appV23GrantControlOutcome uint8

const (
	appV23GrantControlDenied appV23GrantControlOutcome = iota
	appV23GrantControlAllowed
	appV23GrantControlClaimAndAllow
)

func (app *SageApp) appV23GrantControlDecision(
	agentID, domain string,
) (appV23GrantControlOutcome, authzdenial.Code, error) {
	root, enrollment, role, err := app.appV23Actor(agentID)
	if errors.Is(err, store.ErrAppV23NeedsApproval) {
		return appV23GrantControlDenied, authzdenial.CodePrincipalPendingReview, nil
	}
	if err != nil {
		return appV23GrantControlDenied, "", err
	}
	if enrollment.Profile == store.AppV23ProfileReadOnly {
		return appV23GrantControlDenied, authzdenial.CodeForeignWriteRestricted, nil
	}
	owner, _, err := app.badgerStore.ResolveAppV23OwningAncestor(domain)
	if err != nil {
		return appV23GrantControlDenied, "", err
	}
	if agentID == root.CredentialID || role.Role == store.AppV23RoleAdmin {
		if owner == "" {
			return appV23GrantControlClaimAndAllow, "", nil
		}
		return appV23GrantControlAllowed, "", nil
	}
	if owner == agentID {
		return appV23GrantControlAllowed, "", nil
	}
	if owner == "" {
		if enrollment.Capabilities.Has(store.AgentCapabilityDenyDomainClaim) {
			return appV23GrantControlDenied, authzdenial.CodeDomainClaimRestricted, nil
		}
		if (role.Role == store.AppV23RoleMember ||
			role.Role == store.AppV23RoleManager) &&
			(enrollment.Profile == store.AppV23ProfileStandard ||
				enrollment.Profile == store.AppV23ProfileLegacyRestricted) {
			return appV23GrantControlClaimAndAllow, "", nil
		}
		return appV23GrantControlDenied, authzdenial.CodeDomainClaimRestricted, nil
	}
	if role.Role == store.AppV23RoleManager {
		return appV23GrantControlDenied, authzdenial.CodeManagerScopeDenied, nil
	}
	return appV23GrantControlDenied, authzdenial.CodeMissingWriteGrant, nil
}

func (app *SageApp) processLocalAgentApprove(parsedTx *tx.ParsedTx, height int64, _ time.Time) *abcitypes.ExecTxResult {
	if !app.postAppV23Rules(height) {
		return &abcitypes.ExecTxResult{Code: 10, Log: "unknown tx type"}
	}
	approval := parsedTx.LocalAgentApprove
	if approval == nil {
		return &abcitypes.ExecTxResult{Code: 110, Log: "missing local agent approval payload"}
	}
	actorID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 110, Log: "agent identity verification failed"}
	}
	root, err := app.badgerStore.GetAppV23Root()
	if err != nil || root == nil {
		return appV23ControlDenied()
	}
	if actorID != root.CredentialID {
		_, _, actorRole, actorErr := app.appV23Actor(actorID)
		if actorErr != nil || actorRole.Role != store.AppV23RoleAdmin {
			return appV23ControlDenied()
		}
	}
	if approval.AgentID == root.PrincipalID {
		return appV23ControlDenied()
	}
	if approval.Scope != root.Scope {
		return appV23ControlDenied()
	}
	capabilities := store.AgentCapabilities(approval.Capabilities)
	if approval.Profile == store.AppV23ProfileRoot ||
		approval.Profile == store.AppV23ProfileLegacyRestricted ||
		store.ValidateAppV23Policy(approval.Role, approval.Profile, capabilities, approval.Clearance) != nil {
		return appV23ControlDenied()
	}
	if (!approval.Active || approval.Profile == store.AppV23ProfileReadOnly) &&
		(approval.TransferHomeDomain || approval.ExpectedHomeDomainOwner != "") {
		return appV23ControlDenied()
	}
	if approval.Clearance > 4 {
		return appV23ControlDenied()
	}
	if approval.Active {
		if approval.Profile != store.AppV23ProfileReadOnly &&
			(approval.HomeDomain == "" || app.isSharedDomain(approval.HomeDomain, height)) {
			return appV23ControlDenied()
		}
		targetKey, keyErr := auth.AgentIDToPublicKey(approval.AgentID)
		if keyErr != nil || len(approval.TargetSignature) != ed25519.SignatureSize ||
			!ed25519.Verify(targetKey, tx.LocalAgentApprovalSignBytes(actorID, approval), approval.TargetSignature) {
			return appV23ControlDenied()
		}
	}
	enrollment := store.AppV23LocalEnrollment{
		AgentID: approval.AgentID, ApprovedBy: actorID, RootGeneration: root.Generation,
		Profile: approval.Profile, HomeDomain: approval.HomeDomain,
		ExpectedHomeDomainOwner:  approval.ExpectedHomeDomainOwner,
		TransferHomeDomain:       approval.TransferHomeDomain,
		RetireOwnedDomainsToRoot: !approval.Active && app.postAppV26Rules(height),
		Clearance:                approval.Clearance, Capabilities: capabilities,
		Active: approval.Active, UpdatedHeight: height,
	}
	elevation, err := app.appV23ElevationUse(
		actorID, root, parsedTx.LocalElevation, tx.TxTypeLocalAgentApprove,
		tx.LocalAgentApproveActionBytes(approval), height,
	)
	if err != nil {
		return appV23ControlDenied()
	}
	if err := app.badgerStore.ApproveAppV23LocalAgent(
		enrollment, approval.Role, approval.ExpectedRevision, approval.ExpectedRoleRevision,
		elevation,
	); err != nil {
		return appV23ControlDenied()
	}
	return &abcitypes.ExecTxResult{Code: 0, Log: "local agent approval updated"}
}

func (app *SageApp) processAgentRoleChangeV23(parsedTx *tx.ParsedTx, height int64, _ time.Time) *abcitypes.ExecTxResult {
	if !app.postAppV23Rules(height) {
		return &abcitypes.ExecTxResult{Code: 10, Log: "unknown tx type"}
	}
	change := parsedTx.AgentRoleChange
	if change == nil ||
		change.Profile == store.AppV23ProfileRoot ||
		change.Profile == store.AppV23ProfileLegacyRestricted ||
		store.ValidateAppV23Policy(
			change.Role, change.Profile, store.AgentCapabilities(change.Capabilities), change.Clearance,
		) != nil {
		return &abcitypes.ExecTxResult{Code: 111, Log: "invalid app-v23 role change payload"}
	}
	actorID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 111, Log: "agent identity verification failed"}
	}
	root, _, actorRole, err := app.appV23Actor(actorID)
	if errors.Is(err, store.ErrAppV23NeedsApproval) {
		return appV23ControlDenied()
	}
	if err != nil {
		return appV23ControlDenied()
	}
	current, err := app.badgerStore.GetAppV23Role(change.AgentID)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 111, Log: "role lookup failed"}
	}
	adminTransition := change.Role == store.AppV23RoleAdmin || (current != nil && current.Role == store.AppV23RoleAdmin)
	if adminTransition {
		if actorID != root.CredentialID && actorRole.Role != store.AppV23RoleAdmin {
			return appV23ControlDenied()
		}
	} else if actorID != root.CredentialID && actorRole.Role != store.AppV23RoleAdmin {
		return appV23ControlDenied()
	}
	elevation, err := app.appV23ElevationUse(
		actorID, root, parsedTx.LocalElevation, tx.TxTypeAgentRoleChange,
		tx.AgentRoleChangeActionBytes(change), height,
	)
	if err != nil {
		return appV23ControlDenied()
	}
	if err := app.badgerStore.SetAppV23Policy(
		actorID, change.AgentID, change.Role, change.ExpectedProfile, change.Profile,
		change.Clearance, store.AgentCapabilities(change.Capabilities),
		change.ExpectedRevision, change.EnrollmentRevision, height,
		elevation,
	); err != nil {
		return appV23ControlDenied()
	}
	return &abcitypes.ExecTxResult{Code: 0, Log: "agent role updated"}
}

func (app *SageApp) processAccessGroupMutateV23(parsedTx *tx.ParsedTx, height int64, _ time.Time) *abcitypes.ExecTxResult {
	if !app.postAppV23Rules(height) {
		return &abcitypes.ExecTxResult{Code: 10, Log: "unknown tx type"}
	}
	mutation := parsedTx.AccessGroupMutate
	if mutation == nil {
		return &abcitypes.ExecTxResult{Code: 112, Log: "missing access group mutation payload"}
	}
	actorID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 112, Log: "agent identity verification failed"}
	}
	root, _, role, err := app.appV23Actor(actorID)
	if errors.Is(err, store.ErrAppV23NeedsApproval) {
		return appV23ControlDenied()
	}
	if err != nil || (actorID != root.CredentialID && role.Role != store.AppV23RoleAdmin) {
		return appV23ControlDenied()
	}
	if !sort.StringsAreSorted(mutation.Members) {
		return &abcitypes.ExecTxResult{Code: 112, Log: "access group members must be canonical sorted"}
	}
	if app.postAppV26Rules(height) {
		if mutation.Delete {
			if mutation.MemberAuthority != "" {
				return appV23ControlDenied()
			}
		} else if authorityErr := store.ValidateAppV26GroupAuthority(mutation.MemberAuthority); authorityErr != nil {
			return appV23ControlDenied()
		}
	} else if mutation.MemberAuthority != "" {
		// Historical forks neither accept nor persist the appended field.
		return appV23ControlDenied()
	}
	elevation, err := app.appV23ElevationUse(
		actorID, root, parsedTx.LocalElevation, tx.TxTypeAccessGroupMutate,
		tx.AccessGroupMutateActionBytes(mutation), height,
	)
	if err != nil {
		return appV23ControlDenied()
	}
	var mutateErr error
	if app.postAppV26Rules(height) {
		mutateErr = app.badgerStore.MutateAppV26AccessGroup(
			actorID, mutation.GroupID, mutation.Name, mutation.Members,
			mutation.MemberAuthority, mutation.ExpectedRevision,
			mutation.Delete, height, elevation,
		)
	} else {
		mutateErr = app.badgerStore.MutateAppV23AccessGroup(
			actorID, mutation.GroupID, mutation.Name, mutation.Members,
			mutation.ExpectedRevision, mutation.Delete, height, elevation,
		)
	}
	if mutateErr != nil {
		return appV23ControlDenied()
	}
	return &abcitypes.ExecTxResult{Code: 0, Log: "access group updated"}
}

func (app *SageApp) processRootCredentialRotateV23(parsedTx *tx.ParsedTx, height int64, _ time.Time) *abcitypes.ExecTxResult {
	if !app.postAppV23Rules(height) {
		return &abcitypes.ExecTxResult{Code: 10, Log: "unknown tx type"}
	}
	rotation := parsedTx.RootCredentialRotate
	if rotation == nil || parsedTx.LocalElevation != nil {
		return appV23ControlDenied()
	}
	actorID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return appV23ControlDenied()
	}
	root, err := app.badgerStore.GetAppV23Root()
	if err != nil || root == nil || actorID != root.CredentialID ||
		rotation.ExpectedGeneration != root.Generation ||
		rotation.Scope != root.Scope ||
		rotation.NewCredentialID == root.CredentialID {
		return appV23ControlDenied()
	}
	newKey, err := auth.AgentIDToPublicKey(rotation.NewCredentialID)
	if err != nil || len(rotation.NewCredentialSignature) != ed25519.SignatureSize ||
		!ed25519.Verify(
			newKey,
			tx.RootCredentialRotationSignBytes(root.PrincipalID, rotation),
			rotation.NewCredentialSignature,
		) {
		return appV23ControlDenied()
	}
	if err := app.badgerStore.RotateAppV23RootCredential(
		rotation.ExpectedGeneration, rotation.NewCredentialID, height,
	); err != nil {
		return appV23ControlDenied()
	}
	return &abcitypes.ExecTxResult{Code: 0, Log: "root credential rotated"}
}
