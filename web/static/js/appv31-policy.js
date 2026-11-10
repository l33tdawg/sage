import {
    appV23PolicyDraft,
    appV23PolicyChanged,
    appV23CapabilityIndicators,
} from './appv23-policy.js';

const INSPECT_DIRECT_GRANTS = 32;

function basePolicy(agent) {
    return { ...agent, capabilities: Number(agent?.capabilities || 0) & ~INSPECT_DIRECT_GRANTS };
}

export function appV31PolicyDraft(agent, supported = false) {
    if (!supported) return appV23PolicyDraft(agent);
    const base = basePolicy(agent);
    const draft = appV23PolicyDraft(base);
    // Keep only an explicitly recorded permission on an otherwise canonical
    // named policy. Changing a role/profile still uses the old preset reset.
    const recorded = Number(agent?.capabilities || 0);
    if (draft.profile !== 'legacy_restricted' && Number.isInteger(recorded) &&
        recorded >= 0 && base.capabilities === draft.capabilities) {
        draft.capabilities |= recorded & INSPECT_DIRECT_GRANTS;
    }
    return draft;
}

export function appV31PolicyChanged(current, next, supported = false) {
    if (!supported) return appV23PolicyChanged(current, next);
    return appV23PolicyChanged(basePolicy(current), basePolicy(next)) ||
        (appV31PolicyDraft(current, true).capabilities & INSPECT_DIRECT_GRANTS) !==
        (appV31PolicyDraft(next, true).capabilities & INSPECT_DIRECT_GRANTS);
}

export function appV31InspectionDefaults(enabled, capabilities, supported) {
    if (supported !== true) throw new TypeError('Grant inspection is not active');
    return { capabilities: enabled ? capabilities | INSPECT_DIRECT_GRANTS : capabilities & ~INSPECT_DIRECT_GRANTS };
}

export function appV31CapabilityIndicators(policy, supported = false) {
    if (!supported) return appV23CapabilityIndicators(policy);
    const indicators = appV23CapabilityIndicators(basePolicy(policy));
    if (policy?.profile === 'legacy_restricted') return indicators;
    return [...indicators, {
        bit: INSPECT_DIRECT_GRANTS,
        name: 'Direct-grant inspection',
        desc: 'Reads local grant metadata. Does not grant memory access or write authority.',
        enabledLabel: 'Assigned',
        disabledLabel: 'Not assigned',
        enabled: (appV31PolicyDraft(policy, true).capabilities & INSPECT_DIRECT_GRANTS) !== 0,
    }];
}

export function appV31InspectionNeedsRenewal(agent, draft, supported = false) {
    return supported === true && agent?.inspection_needs_reauthorization === true &&
        (Number(draft?.capabilities || 0) & INSPECT_DIRECT_GRANTS) !== 0;
}
