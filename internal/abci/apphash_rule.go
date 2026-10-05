package abci

import (
	"fmt"

	"github.com/l33tdawg/sage/internal/store"
)

// appHashRule identifies which of the mutually-exclusive AppHash rules is in
// force for a committed block. The rules REPLACE each other, newest first:
//
//	app-v28 (composite): the app-v13 rule over the legacy state WITHOUT the
//	  public-memory index nodes, composed with the sparse public-memory root
//	  those nodes commit to.
//	app-v13 (narrow): excludes exactly the three SaveState bookkeeping keys.
//	app-v12 (broad, FLAWED, superseded): excludes the whole state: prefix.
//	legacy: everything included.
//
// Historical blocks keep replaying under the rule that was in force at their
// own height, so this selection is a function of the height and the persisted
// activation records alone.
type appHashRule uint8

const (
	appHashRuleLegacy appHashRule = iota
	appHashRuleV12
	appHashRuleV13
	appHashRuleV28
)

func (rule appHashRule) String() string {
	switch rule {
	case appHashRuleV12:
		return "app-v12"
	case appHashRuleV13:
		return "app-v13"
	case appHashRuleV28:
		return "app-v28"
	default:
		return "legacy"
	}
}

// appHashRuleInputs are the facts that decide the rule at a height. They are
// explicit so that the consensus commit path (which caches them on SageApp)
// and the offline state-sync verification path (which has a store that is not
// being served) select the rule through the same function instead of keeping
// two copies of the precedence.
//
// Two copies is how the state-sync provider verification went stale: it kept
// computing the app-v13 hash after app-v28 had replaced the rule, so a v28
// provider refused to serve with "persisted AppHash does not match Badger
// state" while every pre-v28 chain stayed green.
type appHashRuleInputs struct {
	appV12AppliedHeight int64
	appV13AppliedHeight int64
	appV23GenesisActive bool
	appV28AppliedHeight int64
}

// selectAppHashRule is the single source of truth for AppHash-rule precedence.
// Every activation block H_act itself still hashes under the rule that was
// previously in force (strict "height > applied" comparisons), so the flip
// lands at H_act+1 and pre-upgrade replicas reproduce the activation block
// byte for byte.
func selectAppHashRule(height int64, inputs appHashRuleInputs) appHashRule {
	switch {
	case inputs.appV28AppliedHeight > 0 && height > inputs.appV28AppliedHeight:
		return appHashRuleV28
	case (inputs.appV23GenesisActive && height > 0) ||
		(inputs.appV13AppliedHeight > 0 && height > inputs.appV13AppliedHeight):
		return appHashRuleV13
	case inputs.appV12AppliedHeight > 0 && height > inputs.appV12AppliedHeight:
		return appHashRuleV12
	default:
		return appHashRuleLegacy
	}
}

// appHashRuleInputsFromApp reads the activation heights cached on a live app.
func (app *SageApp) appHashRuleInputsFromApp() appHashRuleInputs {
	return appHashRuleInputs{
		appV12AppliedHeight: app.appV12AppliedHeight,
		appV13AppliedHeight: app.appV13AppliedHeight,
		appV23GenesisActive: app.appV23GenesisActive,
		appV28AppliedHeight: app.appV28AppliedHeight,
	}
}

// appHashRuleInputsForOfflineState derives the rule inputs for a persisted
// state that the state-sync verification paths have already accepted as
// post-app-v20. Such a chain is necessarily past app-v13 — the narrow rule is
// the baseline those paths have always assumed — and the only rule that can
// have replaced it by then is app-v28's composite root. The superseded rules
// are seeded as already in force rather than read back, because a chain that
// reached app-v20 through the app-v23 direct-genesis path carries no earlier
// activation records at all, and this path is handed stores that may be
// reduced backup images. The one activation that changes the answer here is
// read from the AppHash-covered audit trail, so a store that cannot produce it
// fails closed.
func appHashRuleInputsForOfflineState(badgerStore *store.BadgerStore) (appHashRuleInputs, error) {
	inputs := appHashRuleInputs{
		appV12AppliedHeight: 1,
		appV13AppliedHeight: 1,
	}
	record, err := badgerStore.GetAppliedUpgrade(appV28UpgradeName)
	if err != nil {
		return appHashRuleInputs{}, fmt.Errorf("read applied %s record: %w", appV28UpgradeName, err)
	}
	if record == nil {
		return inputs, nil
	}
	if record.Name != appV28UpgradeName || record.TargetAppVersion != 28 || record.AppliedHeight <= 0 {
		return appHashRuleInputs{}, fmt.Errorf("invalid applied %s record", appV28UpgradeName)
	}
	inputs.appV28AppliedHeight = record.AppliedHeight
	return inputs, nil
}

// computeAppHashForRule evaluates one rule against a store. Keeping the
// rule-to-function mapping here is what lets the commit path and the
// verification paths share a single precedence decision.
func computeAppHashForRule(badgerStore *store.BadgerStore, rule appHashRule) ([]byte, error) {
	switch rule {
	case appHashRuleV28:
		return badgerStore.ComputePublicMemoryAppHash()
	case appHashRuleV13:
		return badgerStore.ComputeAppHashExcludingBookkeeping()
	case appHashRuleV12:
		return badgerStore.ComputeAppHashExcludingState()
	default:
		return ComputeAppHash(badgerStore)
	}
}
