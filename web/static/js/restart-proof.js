// Pure restart-proof helpers kept separate from the Preact application so the
// update lifecycle can be regression-tested without a browser DOM.

export const RESTART_WAIT_MS = 5 * 60 * 1000;

export function restartBaselineBootID(restartResponse) {
    return String(restartResponse?.boot_id || '');
}

export function formatRestartCountdown(remainingMs) {
    const numericMs = Number(remainingMs);
    const boundedMs = Number.isFinite(numericMs) ? Math.max(0, numericMs) : 0;
    const totalSeconds = Math.ceil(boundedMs / 1000);
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = String(totalSeconds % 60).padStart(2, '0');
    return `${minutes}:${seconds}`;
}

export function restartProgressMessage(phase) {
    switch (phase) {
        case 'requesting':
            return 'Requesting a safe restart…';
        case 'waiting':
            return 'Safe restart accepted. SAGE is checking whether in-flight work has finished.';
        case 'draining':
            return 'SAGE is finishing in-flight work before it restarts.';
        case 'restarting':
            return 'SAGE has closed its listener and is starting the replacement process.';
        default:
            return 'SAGE is restarting safely.';
    }
}

export function requestedRestartIsReady(previousBootID, health, expectedVersion = '') {
    const cleanVersion = value => String(value || '').replace(/^v/, '');
    return !!previousBootID
        && !!health?.boot_id
        && health.boot_id !== previousBootID
        && health.sage === 'running'
        && (!expectedVersion || cleanVersion(health.version) === cleanVersion(expectedVersion));
}
