package abci

import (
	"fmt"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/l33tdawg/sage/internal/contentvalidator"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// IsAppV31ActiveForNextTx exposes readiness without activating the fork.
func (app *SageApp) IsAppV31ActiveForNextTx() bool {
	app.runtimeViewMu.RLock()
	defer app.runtimeViewMu.RUnlock()
	return app.state != nil && app.postAppV31Fork(app.state.Height+1)
}

func (app *SageApp) decodeConsensusTx(raw []byte, height int64) (*tx.ParsedTx, error) {
	return tx.DecodeTxWithFunctionAuthority(raw, app.postAppV31Fork(height))
}

// processFunctionAuthority is a Root-only, consensus-held control plane. Domain
// ownership and self-issued grants cannot create a functional designation.
func (app *SageApp) processFunctionAuthority(parsed *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	if !app.postAppV31Fork(height) {
		return &abcitypes.ExecTxResult{Code: 1, Log: tx.ErrUnknownTxType.Error()}
	}
	a := parsed.FunctionAuthority
	if a == nil || parsed.LocalElevation != nil {
		return appV23ControlDenied()
	}
	actor, err := app.verifyCurrentAgentIdentity(parsed, height)
	if err != nil {
		return appV23ControlDenied()
	}
	root, _, _, err := app.appV23Actor(actor)
	if err != nil || root == nil || actor != root.CredentialID || a.Scope != root.Scope {
		return appV23ControlDenied()
	}
	if a.Action == "activate" {
		if a.Subject != "" || a.ExpectedRevision != 0 || a.ExpiresAt != 0 || a.ActivationHeight <= height {
			return appV23ControlDenied()
		}
		if !app.contentValidators.HasFunction(a.Domain, a.Function) {
			return &abcitypes.ExecTxResult{Code: 18, Log: "function authority activation requires a callable registered function"}
		}
		err = app.badgerStore.SetFunctionAuthorityPolicy(&store.FunctionAuthorityPolicy{
			Function: a.Function, Domain: a.Domain, ActivationHeight: a.ActivationHeight,
			CreatedHeight: height, CreatedBy: root.PrincipalID, RuleVersion: 1,
		})
	} else {
		if a.ActivationHeight != 0 || (a.Action != "assign" && a.Action != "revoke") ||
			a.ExpiresAt < 0 || (a.ExpiresAt != 0 && a.ExpiresAt <= blockTime.Unix()) {
			return appV23ControlDenied()
		}
		if a.Action == "assign" {
			_, enrollment, role, targetErr := app.appV23Actor(a.Subject)
			if targetErr != nil || enrollment == nil || role == nil || a.Subject == root.PrincipalID || a.Subject == root.CredentialID ||
				(role.Role != store.AppV23RoleMember && role.Role != store.AppV23RoleManager) {
				return appV23ControlDenied()
			}
		} else {
			previous, previousErr := app.badgerStore.GetFunctionDesignation(a.Function, a.Domain, a.Subject)
			if previousErr != nil || previous == nil || a.ExpiresAt != 0 {
				return appV23ControlDenied()
			}
		}
		err = app.badgerStore.SetFunctionDesignation(&store.FunctionDesignation{
			Function: a.Function, Domain: a.Domain, Subject: a.Subject,
			Revision: a.ExpectedRevision + 1, AssignedBy: root.PrincipalID,
			RootGeneration: root.Generation, UpdatedHeight: height,
			ExpiresAt: a.ExpiresAt, Revoked: a.Action == "revoke",
		}, a.ExpectedRevision)
	}
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 11, Log: "function authority control rejected: " + err.Error()}
	}
	return &abcitypes.ExecTxResult{Code: 0}
}

// authorizeFunctionAt is bound to the executing app, not the boot-time app.
// Every read therefore sees earlier writes in the same speculative transaction
// and disappears with that transaction on abort.
func (app *SageApp) authorizeFunctionAt(parsed *tx.ParsedTx, rec *memory.MemoryRecord, height int64, blockTime time.Time, request contentvalidator.FunctionAuthorityRequest) (contentvalidator.FunctionAuthorityResult, error) {
	result := contentvalidator.FunctionAuthorityResult{Function: request.Function, Domain: request.Domain, Subject: request.Subject}
	if parsed == nil || parsed.MemorySubmit == nil || rec == nil || blockTime.IsZero() ||
		!app.postAppV31Fork(height) || request.Subject == "" || request.Subject != rec.SubmittingAgent || request.Domain != rec.DomainTag {
		return result, nil
	}
	policy, err := app.badgerStore.GetFunctionAuthorityPolicy(request.Function, request.Domain)
	if err != nil || policy == nil || policy.RuleVersion != 1 || height < policy.ActivationHeight {
		return result, err
	}
	root, enrollment, role, err := app.appV23Actor(request.Subject)
	if err != nil || root == nil || enrollment == nil || role == nil || request.Subject == root.PrincipalID ||
		request.Subject == root.CredentialID || (role.Role != store.AppV23RoleMember && role.Role != store.AppV23RoleManager) {
		return result, nil
	}
	if policy.CreatedBy != root.PrincipalID || uint8(parsed.MemorySubmit.Classification) > enrollment.Clearance {
		return result, nil
	}
	allowed, denial, err := app.appV23DomainDecision(parsed, request.Subject, request.Domain, store.AppV23VerbWrite, height, blockTime)
	if err != nil || denial != "" || !allowed {
		return result, err
	}
	designation, err := app.badgerStore.GetFunctionDesignation(request.Function, request.Domain, request.Subject)
	if err != nil || designation == nil || designation.Revoked || designation.AssignedBy != root.PrincipalID ||
		designation.RootGeneration != root.Generation || (designation.ExpiresAt > 0 && blockTime.Unix() >= designation.ExpiresAt) {
		return result, err
	}
	level, expiry, _, err := app.badgerStore.GetAccessGrant(request.Domain, request.Subject)
	if err != nil {
		return result, err
	}
	if level < 2 || level > 3 || expiry < 0 || (expiry > 0 && blockTime.Unix() >= expiry) {
		return result, nil // a missing, expired or invalid exact grant denies
	}
	result.Allowed = true
	return result, nil
}

func (app *SageApp) validateMemoryContentAt(parsed *tx.ParsedTx, rec *memory.MemoryRecord, outcomeClass string, height int64, blockTime time.Time) (bool, string) {
	function := app.contentValidators.FunctionFor(rec.DomainTag, outcomeClass)
	if !app.postAppV31Fork(height) || function == "" {
		return app.contentValidators.Validate(rec.DomainTag, outcomeClass, rec)
	}
	policy, err := app.badgerStore.GetFunctionAuthorityPolicy(function, rec.DomainTag)
	if err != nil {
		return true, "content validator: functional policy state unavailable"
	}
	if policy == nil || height < policy.ActivationHeight {
		return app.contentValidators.Validate(rec.DomainTag, outcomeClass, rec)
	}
	if policy.RuleVersion != 1 {
		return true, fmt.Sprintf("content validator: unsupported functional rule version %d", policy.RuleVersion)
	}
	return app.contentValidators.ValidateWithContext(rec.DomainTag, outcomeClass, rec, contentvalidator.ValidationContext{
		Height: height, BlockTime: blockTime,
		AuthorizeFunction: func(request contentvalidator.FunctionAuthorityRequest) (contentvalidator.FunctionAuthorityResult, error) {
			if request.Function != function {
				return contentvalidator.FunctionAuthorityResult{Function: request.Function, Domain: request.Domain, Subject: request.Subject}, nil
			}
			return app.authorizeFunctionAt(parsed, rec, height, blockTime, request)
		},
	})
}

// functionContentValidationActive reads canonical adoption state before the
// local registry. A binary missing an adopted implementation must fail closed,
// including after a restart, rather than silently taking the legacy role path.
func (app *SageApp) functionContentValidationActive(domain, _ string, height int64) (bool, error) {
	if !app.postAppV31Fork(height) {
		return false, nil
	}
	if app.badgerStore == nil {
		return false, fmt.Errorf("functional policy store unavailable")
	}
	policies, err := app.badgerStore.ListFunctionAuthorityPoliciesForDomain(domain)
	if err != nil {
		return false, err
	}
	active := false
	for _, policy := range policies {
		if height < policy.ActivationHeight {
			continue
		}
		if policy.RuleVersion != 1 {
			return false, fmt.Errorf("unsupported functional rule version")
		}
		if !app.contentValidators.HasFunction(domain, policy.Function) {
			return false, fmt.Errorf("adopted function implementation unavailable")
		}
		active = true
	}
	return active, nil
}
