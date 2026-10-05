// describeMemoryGate turns the review-queue payload's `gate` block into the
// copy the Memory gate settings panel renders.
//
// COPY RULE: always say what leaves the node. When the gate is on, the TEXT of
// proposed memories in scope is sent to the configured judge service; ids,
// authors and other metadata are not. An operator must be able to read that on
// the same screen where they review the held memories.

export function describeMemoryGate(gate) {
    if (!gate || !gate.enabled) {
        return {
            enabled: false,
            headline: 'The memory gate is off.',
            detail: 'No memory text is sent anywhere and nothing is held for review: this node votes with the built-in checks only. ' +
                'To turn it on, set SAGE_HUNCH_URL (and optionally SAGE_HUNCH_MODELS, SAGE_HUNCH_INCLUDE_DOMAINS, SAGE_HUNCH_EXEMPT_DOMAINS) and restart the node.',
            scope: '',
        };
    }
    const include = Array.isArray(gate.include_domains) ? gate.include_domains : [];
    const exempt = Array.isArray(gate.exempt_domains) ? gate.exempt_domains : [];
    let scope = include.length
        ? `Only memories in domains starting with ${include.join(', ')}`
        : 'Memories in every domain';
    if (exempt.length) scope += `, except domains starting with ${exempt.join(', ')},`;
    scope += ' are judged: their text is sent to the configured judge service. No ids, authors or other metadata are sent.';
    if ((Number(gate.evidence_judges) || 0) > 0) {
        scope += ' For a memory submitted with evidence, the evidence text is sent as well, and it passes only when every judge agrees the evidence supports it.';
    }
    const judges = Number(gate.judges) || 0;
    return {
        enabled: true,
        headline: `The memory gate is on (${judges} judge${judges === 1 ? '' : 's'}).`,
        detail: 'Memories the judges were unsure about are not voted on until you decide them here. ' +
            'Your decision settles only that question: the built-in checks still run when the node votes.',
        scope,
    };
}

// reviewItemState says how one held item can be handled.
export function reviewItemState(item) {
    if (!item) return { decidable: false, text: '' };
    if (item.content_unavailable) {
        return {
            decidable: false,
            text: 'Content cannot be read on this node right now (locked or undecryptable), so it cannot be reviewed here.',
        };
    }
    return {
        decidable: true,
        text: item.content || '',
        evidence: item.evidence || '',
        evidenceNote: item.evidence_expired
            ? 'This memory was submitted with evidence, but the evidence expired on this node before the memory arrived, so whether it supports the memory could not be checked. Decide from the memory alone.'
            : '',
    };
}
