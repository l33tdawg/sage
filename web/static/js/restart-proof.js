// Pure restart-proof helpers kept separate from the Preact application so the
// update lifecycle can be regression-tested without a browser DOM.

export function restartBaselineBootID(restartResponse) {
    return String(restartResponse?.boot_id || '');
}

export function requestedRestartIsReady(previousBootID, health, expectedVersion = '') {
    const cleanVersion = value => String(value || '').replace(/^v/, '');
    return !!previousBootID
        && !!health?.boot_id
        && health.boot_id !== previousBootID
        && health.sage === 'running'
        && (!expectedVersion || cleanVersion(health.version) === cleanVersion(expectedVersion));
}
