import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import {
    describeLastFenceResolution,
    describeSignerFences,
    fenceSummary,
    formatFenceAge,
} from '../web/static/js/signer-fences.js';

const appSource = readFileSync(new URL('../web/static/js/app.js', import.meta.url), 'utf8');

// The operator payload web/rbac_signing.go's signerFenceHealth builds: shared
// fields always, per-fence rows only for an operator session.
const operatorPayload = {
    signer_fences: {
        active: 2,
        oldest_age_seconds: 3720,
        explanation: 'one or more signing keys are waiting for proof of an earlier submission\'s fate; ' +
            'nothing was signed or sent for the requests they refused, and reconciliation is re-submitting ' +
            'the identical bytes to force an answer',
        signers: [
            {
                signer: '5c9ea6448b3caf58',
                signer_public_key: '5c9ea6448b3caf58aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                tx_hash: '158107F5AA11BB22CC33DD44EE55FF6677889900AABBCCDDEEFF00112233445566',
                nonce: '1788345425350495656',
                held_seconds: 3720,
                attempts: 214,
                cause: 'restored_from_durable_intent',
                resolution: 'proof_or_operator',
                last_cause: 'no_proof',
                last_detail: 'neither proof holds yet: the transaction is not in a committed block',
            },
            {
                signer: '916058bf00112233',
                signer_public_key: '916058bf00112233bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
                tx_hash: 'ABCDEF0123456789',
                held_seconds: 45,
                attempts: 2,
                cause: 'transport',
                resolution: 'reconciling',
                last_cause: 'transport',
                last_detail: 'dial tcp 127.0.0.1:26657: connect: connection refused',
            },
        ],
    },
};

test('an absent or empty fence block renders nothing', () => {
    for (const health of [undefined, null, {}, { signer_fences: { active: 0 } }]) {
        const status = describeSignerFences(health);
        assert.equal(status.active, 0);
        assert.equal(status.rows.length, 0);
        assert.equal(status.explanation, '');
        assert.equal(fenceSummary(status), '');
    }
});

test('a summary-only payload still names the hold', () => {
    // signerFenceHealth returns active/oldest_age_seconds/explanation to a
    // non-operator caller: no rows, but the row must still say what is wrong.
    const status = describeSignerFences({
        signer_fences: { active: 1, oldest_age_seconds: 90, explanation: 'held for proof' },
    });
    assert.equal(status.active, 1);
    assert.equal(status.oldestLabel, '1m');
    assert.equal(status.explanation, 'held for proof');
    assert.equal(status.hasRows, false);
    assert.equal(fenceSummary(status), '1 signing key held (oldest 1m)');
});

test('operator rows carry the facts the panel renders', () => {
    const status = describeSignerFences(operatorPayload);
    assert.equal(status.active, 2);
    assert.equal(status.hasRows, true);
    const [restored, live] = status.rows;

    assert.equal(restored.signerShort, '5c9ea644…');
    assert.equal(restored.nonceText, '1788345425350495656');
    assert.equal(restored.heldLabel, '1h');
    assert.equal(restored.attemptsLabel, '214 attempts');
    assert.equal(restored.resolution, 'proof_or_operator');
    assert.equal(restored.resolutionLabel, 'Waiting on proof');
    assert.match(restored.detail, /not in a committed block/);

    assert.equal(live.signerShort, '916058bf…');
    assert.equal(live.nonceText, '');
    assert.equal(live.heldLabel, '45s');
    assert.equal(live.attemptsLabel, '2 attempts');
    assert.equal(live.resolution, 'reconciling');
    assert.equal(live.resolutionLabel, 'Reconciling');

    // The full key rides along for a request, the short form is display only.
    assert.equal(restored.signerFull, operatorPayload.signer_fences.signers[0].signer_public_key);
});

// The node sends the nonce as a string because it exceeds Number.MAX_SAFE_INTEGER
// (1788345425350495656 > 2^53) and JSON.parse would round it — the value is
// compared against the chain's committed nonce, so a rounded one is a wrong
// answer, not a cosmetic flaw. An older node's numeric payload still renders.
test('the nonce survives exactly and tolerates a legacy numeric payload', () => {
    const exact = describeSignerFences({
        signer_fences: { active: 1, signers: [{ signer: 'abc', nonce: '1788345425350495656' }] },
    });
    assert.equal(exact.rows[0].nonceText, '1788345425350495656');

    const legacy = describeSignerFences({
        signer_fences: { active: 1, signers: [{ signer: 'abc', nonce: 42 }] },
    });
    assert.equal(legacy.rows[0].nonceText, '42');
});

test('an unknown resolution says so instead of promising self-healing', () => {
    const status = describeSignerFences({
        signer_fences: { active: 1, signers: [{ signer: 'abc12345', resolution: 'something_new' }] },
    });
    const [row] = status.rows;
    assert.equal(row.resolution, 'unknown');
    assert.equal(row.resolutionLabel, 'Held');
    assert.match(row.resolutionHint, /does not recognise the resolution/);
    assert.doesNotMatch(row.resolutionHint, /clears itself/);
});

test('held durations read like an operator wrote them', () => {
    assert.equal(formatFenceAge(0), '0s');
    assert.equal(formatFenceAge(59), '59s');
    assert.equal(formatFenceAge(60), '1m');
    assert.equal(formatFenceAge(3600), '1h');
    assert.equal(formatFenceAge(49 * 3600), '2d');
    assert.equal(formatFenceAge(undefined), '0s');
});

// THE COPY RULE. A restart discards the in-process fence and loses the
// transaction it protects, so nothing this panel renders may suggest one as the
// remedy. The forbidden list matches the one the Go-side fence tests use
// (cmd/sage-gui/signer_fence_restart_test.go), so the two surfaces cannot drift
// into advising opposite things.
test('no rendered string advises restarting to clear the fence', () => {
    const status = describeSignerFences(operatorPayload);
    const resolved = describeLastFenceResolution({
        signer_fences: { last_resolution: { mode: 'abandoned:operator_cli', signer: '916058bf00112233' } },
    });
    const strings = [
        status.explanation,
        fenceSummary(status),
        resolved.label,
        resolved.hint,
        ...status.rows.flatMap((row) => [
            row.resolutionLabel,
            row.resolutionHint,
            row.detail,
            row.lastCause,
            row.cause,
        ]),
    ].filter(Boolean);
    for (const text of strings) {
        const lower = text.toLowerCase();
        for (const forbidden of ['restart anyway', 'restart to clear', 'force a restart', 'try restarting']) {
            assert.ok(
                !lower.includes(forbidden),
                `the fence panel advises ${JSON.stringify(forbidden)}: ${text}`,
            );
        }
    }
    // And the reconciling hint must actively refuse it, not merely omit it.
    const reconciling = status.rows.find((row) => row.resolution === 'reconciling');
    assert.match(reconciling.resolutionHint, /restarting to clear it would discard the fence and lose that transaction/);
});

test('the last resolution is absent until a fence actually ends', () => {
    for (const health of [undefined, null, {}, { signer_fences: {} }, { signer_fences: { last_resolution: {} } }]) {
        assert.equal(describeLastFenceResolution(health), null);
    }
});

// A fence that ended is the one thing the held-fence row cannot show: it is gone
// by then. The record answers "what happened to the hold I was just looking at".
test('a resolved fence keeps its fate, its identifiers and how long it was held', () => {
    const committed = describeLastFenceResolution({
        signer_fences: {
            last_resolution: {
                mode: 'committed',
                signer: '3d73cdbdffaacac7',
                tx_hash: 'AABBCCDDEEFF0011',
                nonce: '1770000000000000001',
                held_seconds: 301,
                at: '2026-09-22T11:31:04Z',
                detail: 'committed in block 5',
            },
        },
    });
    assert.equal(committed.label, 'Resolved: committed');
    assert.equal(committed.abandoned, false);
    assert.equal(committed.signerShort, '3d73cdbd…');
    assert.equal(committed.txHashShort, 'AABBCCDD…');
    assert.equal(committed.nonceText, '1770000000000000001');
    assert.equal(committed.heldLabel, '5m');
    assert.equal(committed.atLabel, '2026-09-22 11:31 UTC');
    assert.equal(committed.detail, 'committed in block 5');
});

test('a resolution with no proof says the payload may be lost', () => {
    const abandoned = describeLastFenceResolution({
        signer_fences: {
            last_resolution: { mode: 'abandoned:operator_cli', signer: '916058bf00112233', held_seconds: 3720 },
        },
    });
    assert.equal(abandoned.label, 'Resolved without a proof');
    assert.equal(abandoned.abandoned, true);
    assert.match(abandoned.hint, /payload may be lost/);

    // The automatic routes carry their detail through the same field.
    const automatic = describeLastFenceResolution({
        signer_fences: { last_resolution: { mode: 'abandoned:automatic_quiescent', signer: '916058bf00112233' } },
    });
    assert.equal(automatic.label, 'Resolved without a proof');
});

test('the rejected fate keeps the code-4 caveat', () => {
    const rejected = describeLastFenceResolution({
        signer_fences: { last_resolution: { mode: 'rejected', signer: '3d73cdbdffaacac7' } },
    });
    assert.equal(rejected.label, 'Resolved: rejected');
    assert.match(rejected.hint, /verify the effect on-chain before redoing the action/);
});

test('an unrecognised fate admits it instead of guessing', () => {
    const unknown = describeLastFenceResolution({ signer_fences: { last_resolution: { mode: 'vanished' } } });
    assert.equal(unknown.label, 'Resolved');
    assert.match(unknown.hint, /does not recognise the resolution/);
});

test('the System Status panel renders the hold and its rows', () => {
    assert.match(
        appSource,
        /import \{ describeSignerFences, describeLastFenceResolution, fenceSummary \} from '\.\/signer-fences\.js';/,
    );
    assert.match(appSource, /const fenceStatus = describeSignerFences\(health\);/);
    assert.match(appSource, /const lastFenceResolution = describeLastFenceResolution\(health\);/);

    const start = appSource.indexOf('<h3>System Status</h3>');
    assert.notEqual(start, -1, 'the System Status section must exist');
    const end = appSource.indexOf('<!-- Memory Statistics -->', start);
    assert.notEqual(end, -1, 'the System Status section must be bounded');
    const section = appSource.slice(start, end);

    assert.match(section, /fenceStatus\.active > 0 && html`/, 'the hold is rendered only while it is held');
    assert.match(section, /Signing key on hold/);
    assert.match(section, /fenceSummary\(fenceStatus\)/);
    assert.match(section, /fenceStatus\.explanation/);
    assert.match(section, /fenceStatus\.rows\.map/, 'operator rows render individually');
    assert.match(section, /row\.resolutionHint/);
    assert.match(section, /row\.detail/, 'the recorded last_detail reaches the panel');
    assert.match(section, /lastFenceResolution && html`/, 'a resolved fence renders after the hold is gone');
    assert.match(section, /lastFenceResolution\.hint/);
});
