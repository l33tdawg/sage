import test from 'node:test';
import assert from 'node:assert/strict';
import { appV31PolicyDraft, appV31PolicyChanged, appV31InspectionDefaults, appV31CapabilityIndicators, appV31InspectionNeedsRenewal } from '../web/static/js/appv31-policy.js';
import { appV23RoleDefaults, appV23ProfileDefaults } from '../web/static/js/appv23-policy.js';

const agent = { role: 'member', profile: 'companion', clearance: 1, capabilities: 15, home_domain: 'home' };
test('inspection remains absent by default and cannot be enabled before activation', () => {
    assert.equal(appV31PolicyDraft(agent, true).capabilities, 15);
    assert.throws(() => appV31InspectionDefaults(true, 15, false), /not active/);
    assert.equal(appV31PolicyDraft({...agent, capabilities:47}, false).capabilities,15);
    assert.equal(appV31CapabilityIndicators(agent, false).length,5);
});
test('explicit inspection survives a draft without changing baseline write restrictions', () => {
    const draft=appV31PolicyDraft({...agent,capabilities:47},true);
    assert.equal(draft.capabilities,47);
    assert.deepEqual(appV31CapabilityIndicators(draft,true).slice(0,5),appV31CapabilityIndicators(agent,false));
    assert.equal(appV31CapabilityIndicators(draft,true).at(-1).enabled,true);
    assert.equal(appV31PolicyChanged({...agent,capabilities:47},draft,true),false);
});
test('explicit assignment and revocation are dirty changes and retain independent restrictions', () => {
    const granted={...agent,...appV31InspectionDefaults(true,31,true)};
    assert.equal(granted.capabilities,63);
    assert.equal(appV31PolicyChanged({...agent,capabilities:31},granted,true),true);
    const revoked={...granted,...appV31InspectionDefaults(false,63,true)};
    assert.equal(revoked.capabilities,31);
    assert.equal(appV31PolicyChanged(granted,revoked,true),true);
});
test('role and profile changes do not silently retain inspection authority', () => {
    assert.equal(appV23RoleDefaults('manager',47).capabilities&32,0);
    assert.equal(appV23ProfileDefaults('read_only','member',47).capabilities&32,0);
});
test('contradictory or migration-only profiles cannot acquire inspection from raw bits', () => {
    assert.equal(appV31PolicyDraft({...agent,capabilities:32|64},true).capabilities&32,0);
    assert.equal(appV31CapabilityIndicators({...agent,profile:'legacy_restricted',capabilities:47},true).length,0);
});

test('inspection renewal does not depend on revoking ordinary agent access', () => {
    const current={...agent, capabilities:47, enrollment_active:true, inspection_needs_reauthorization:true};
    assert.equal(appV31InspectionNeedsRenewal(current,current,true),true);
    assert.equal(appV31InspectionNeedsRenewal(current,{...current,capabilities:15},true),false);
    assert.equal(appV31InspectionNeedsRenewal(current,current,false),false);
    assert.equal(current.enrollment_active,true);
});
