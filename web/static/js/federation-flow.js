// Small pure helpers shared by the federation ceremony UI and its behavioral
// tests. The Go API deliberately uses protocol constants such as "ABORTED";
// browser decisions should not depend on their presentation casing.
export function normalizeFederationJoinState(value) {
    return String(value || '').trim().toLowerCase();
}
