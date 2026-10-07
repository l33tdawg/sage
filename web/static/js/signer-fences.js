// describeSignerFences turns the /v1/dashboard/health `signer_fences` block into
// the shape the System Status panel renders.
//
// WHY THIS PANEL EXISTS. A held fence refuses every write from its key AND every
// coordinated restart, and the dashboard used to say nothing about it: the field
// report that produced the fence workstream read from the outside as "reads are
// fine, writes time out, no error". The health payload has carried the block
// since v11.23.3 and the per-fence detail for an operator session since
// v11.23.4; no UI rendered it, so an operator sitting in front of CEREBRUM saw
// nothing until writes broke.
//
// TWO COPY RULES, AND BOTH ARE LOAD-BEARING:
//
//   1. Never advise restarting to clear a fence. A restart discards the
//      in-process fence and loses the transaction it protects, which is the
//      exact failure the mechanism exists to prevent. The server's own health
//      explanation says so, and this module must not contradict it.
//   2. Say WHICH shape is held, because the two have different exits.
//      `reconciling`: the exact signed bytes are still being re-submitted, so it
//      settles itself once consensus answers. `proof_or_operator`: the bytes did
//      not survive the process that sent them, re-submission cannot settle it,
//      and it lifts on a proof read from the chain or on an operator decision.

const DEFAULT_EXPLANATION =
    'one or more signing keys are waiting for proof of an earlier submission\'s fate; nothing was signed ' +
    'or sent for the requests they refused, and reconciliation is re-submitting the identical bytes to force ' +
    'an answer';

// The two resolutions the node reports, plus an honest fallback for a payload
// that carries a resolution this build does not know (an older dashboard reading
// a newer node, or the reverse).
const RESOLUTION_COPY = {
    reconciling: {
        label: 'Reconciling',
        hint: 'The identical signed transaction is being re-submitted until consensus answers. This clears ' +
            'itself, and restarting to clear it would discard the fence and lose that transaction.',
    },
    proof_or_operator: {
        label: 'Waiting on proof',
        hint: 'The signed bytes did not survive the process that sent them, so re-submission cannot settle ' +
            'this one: it lifts on a proof read from the chain, or on an operator decision. SAGE settles the ' +
            'shapes it can prove by itself; `sage-gui fence list` shows what is left.',
    },
    unknown: {
        label: 'Held',
        hint: 'This build does not recognise the resolution the node reported. The hold is deliberate: the ' +
            'key will not sign until the transaction\'s fate is proven, and restarting would discard the ' +
            'fence rather than settle it.',
    },
};

function finiteNumber(value, fallback = 0) {
    const n = typeof value === 'number' ? value : Number(value);
    return Number.isFinite(n) && n >= 0 ? n : fallback;
}

// formatFenceAge renders a held duration the way an operator reads it, and never
// as a bare number of seconds once it is past a minute.
export function formatFenceAge(seconds) {
    const s = Math.floor(finiteNumber(seconds));
    if (s < 60) return `${s}s`;
    const minutes = Math.floor(s / 60);
    if (minutes < 60) return `${minutes}m`;
    const hours = Math.floor(minutes / 60);
    if (hours < 48) return `${hours}h`;
    return `${Math.floor(hours / 24)}d`;
}

// shortHash is display-only: it keeps the first eight characters of a hex value
// and marks the truncation, so nobody copies it into a request that wants the
// full key. (The payload carries the full key separately for exactly that
// reason, see signer_public_key in web/rbac_signing.go.)
function shortHash(value) {
    const text = typeof value === 'string' ? value.trim() : '';
    if (!text) return '';
    return text.length > 8 ? `${text.slice(0, 8)}…` : text;
}

function normalizeRow(raw) {
    if (!raw || typeof raw !== 'object') return null;
    const resolution = RESOLUTION_COPY[raw.resolution] ? raw.resolution : 'unknown';
    const copy = RESOLUTION_COPY[resolution];
    // The node sends the nonce as a decimal STRING (web/rbac_signing.go): a
    // nanosecond allocation exceeds JavaScript's safe integer range, and a JSON
    // number would arrive silently rounded. A number is still accepted for an
    // older node's payload, and rendered as-is.
    const nonce = raw.nonce;
    const attempts = finiteNumber(raw.attempts);
    const detail = typeof raw.last_detail === 'string' ? raw.last_detail.trim() : '';
    return {
        signer: typeof raw.signer === 'string' ? raw.signer : '',
        signerFull: typeof raw.signer_public_key === 'string' ? raw.signer_public_key : '',
        signerShort: shortHash(raw.signer),
        txHashShort: shortHash(raw.tx_hash),
        nonceText: nonce === null || nonce === undefined || nonce === '' ? '' : String(nonce),
        heldSeconds: finiteNumber(raw.held_seconds),
        heldLabel: formatFenceAge(raw.held_seconds),
        attempts,
        attemptsLabel: attempts === 1 ? '1 attempt' : `${attempts} attempts`,
        resolution,
        resolutionLabel: copy.label,
        resolutionHint: copy.hint,
        cause: typeof raw.cause === 'string' ? raw.cause : '',
        lastCause: typeof raw.last_cause === 'string' ? raw.last_cause : '',
        detail,
    };
}

export function describeSignerFences(health) {
    const block = health && typeof health === 'object' ? health.signer_fences : null;
    const active = Math.floor(finiteNumber(block?.active));
    if (active <= 0) {
        return { active: 0, oldestAgeSeconds: 0, oldestLabel: '', explanation: '', rows: [], hasRows: false };
    }
    const oldestAgeSeconds = Math.floor(finiteNumber(block?.oldest_age_seconds));
    const explanation = typeof block?.explanation === 'string' && block.explanation.trim()
        ? block.explanation.trim()
        : DEFAULT_EXPLANATION;
    const rows = (Array.isArray(block?.signers) ? block.signers : [])
        .map(normalizeRow)
        .filter(Boolean);
    return {
        active,
        oldestAgeSeconds,
        oldestLabel: formatFenceAge(oldestAgeSeconds),
        explanation,
        rows,
        hasRows: rows.length > 0,
    };
}

// fenceSummary is the one-line form used where there is no room for the table
// (a pill, a title attribute): it says how many keys and how long, and nothing
// else, because the detail belongs next to the remedy.
export function fenceSummary(fenceStatus) {
    if (!fenceStatus || fenceStatus.active <= 0) return '';
    const keys = fenceStatus.active === 1 ? '1 signing key' : `${fenceStatus.active} signing keys`;
    return fenceStatus.oldestLabel ? `${keys} held (oldest ${fenceStatus.oldestLabel})` : `${keys} held`;
}

// The fates a lift records, plus the abandoned:* modes an operator or automatic
// decision records. The rejected copy keeps the caveat the fence docs make:
// code 4 proves supersession OR self-commit, so "rejected" is not licence to
// redo the action by hand without checking the chain.
const FATE_COPY = {
    committed: {
        label: 'Resolved: committed',
        hint: 'The recorded transaction is in a committed block, so the hold ended with its payload intact.',
    },
    rejected: {
        label: 'Resolved: rejected',
        hint: 'Consensus refused these bytes. Code 4 means either a higher nonce has committed or this ' +
            'transaction did — verify the effect on-chain before redoing the action by hand.',
    },
    spent: {
        label: 'Resolved: allocation spent',
        hint: 'The signer\'s committed nonce reached the fenced allocation, so these bytes can never commit ' +
            'again. Whether they were the transaction that committed or were overtaken is what the index ' +
            'decides, and this label admits it cannot tell.',
    },
    abandoned: {
        label: 'Resolved without a proof',
        hint: 'The node or the operator ended this hold without a fate from the chain, so the payload may be ' +
            'lost. If a copy of those bytes still exists and lands before the signer\'s next commit, it ' +
            'commits and the next transaction is refused as a replay.',
    },
    unknown: {
        label: 'Resolved',
        hint: 'This build does not recognise the resolution the node reported. The log line for the fence ' +
            'carries the detail.',
    },
};

function shortTimestamp(value) {
    const text = typeof value === 'string' ? value : '';
    const parsed = Date.parse(text);
    if (Number.isNaN(parsed)) return '';
    return `${new Date(parsed).toISOString().replace('T', ' ').slice(0, 16)} UTC`;
}

// describeLastFenceResolution normalizes the operator-only `last_resolution`
// block: the fence that ended, and what ended it. Returns null when the process
// has not resolved a fence (or when the caller is not an operator session, in
// which case the node does not send the block at all).
export function describeLastFenceResolution(health) {
    const block = health?.signer_fences?.last_resolution;
    if (!block || typeof block !== 'object') return null;
    const mode = typeof block.mode === 'string' ? block.mode.trim() : '';
    if (!mode) return null;
    const key = mode.startsWith('abandoned') ? 'abandoned' : (FATE_COPY[mode] ? mode : 'unknown');
    const copy = FATE_COPY[key];
    const nonce = block.nonce;
    return {
        mode,
        label: copy.label,
        hint: copy.hint,
        abandoned: key === 'abandoned',
        signerShort: shortHash(block.signer),
        txHashShort: shortHash(block.tx_hash),
        nonceText: nonce === null || nonce === undefined || nonce === '' ? '' : String(nonce),
        heldLabel: formatFenceAge(block.held_seconds),
        atLabel: shortTimestamp(block.at),
        detail: typeof block.detail === 'string' ? block.detail.trim() : '',
    };
}
