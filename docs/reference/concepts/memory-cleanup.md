# Canonical memory cleanup

CEREBRUM cleanup is a local, explicitly authorized operator policy. It uses the
existing consensus memory-challenge transaction; it does not change consensus
rules or directly deprecate SQLite rows. The node supervises the worker for its
own lifetime (`RunCanonicalCleanupWorker`, `web/cleanup.go:271`).

## Consent and scope

Only the current local Root can preview, configure, or request cleanup
(`cleanupActor`, `web/cleanup.go:83`). A saved automatic policy is bound to that
exact credential and Root generation. It defaults to OFF; old
`cleanup_enabled` preferences are deliberately not migrated into authorization.
After upgrading, review a preview and explicitly enable the feature.

Eligible records must be canonically verified, committed user memories:

- Expired observations use the observation lifetime, except session-context
  observations use their own lifetime.
- Any supported memory type may qualify below the computed confidence threshold.
  Confidence includes corroboration and type-aware decay.
- Open tasks, internal CEREBRUM records, future-dated records, and non-committed
  records do not qualify.
- Unverifiable rows are skipped and reported, never locally repaired by cleanup.

See `cleanupEligible` (`web/cleanup.go:188`). Every planned target is rechecked
immediately before signing; eligibility can change after a preview.

## Counts, execution, and recovery

The planner walks the full inventory using stable pages, with **no 500-record
total cap** (`cleanupPlan`, `web/cleanup.go:142`). Page size bounds retrieval, not
the eligible total. A full preview reports scanned, verified eligible, and
unverifiable skipped counts. These describe the scan, not a frozen database
snapshot. A scan error fails the run before any submission.

Execution queues a durable plan and submits sequentially. The API and UI separate
eligible, confirmed submissions, observed retired, observed challenged, unchanged,
and skipped counts. A challenge may require additional votes; completing cleanup
does not mean every eligible memory was retired. A later reinstatement may also
make an already-confirmed target active again.

Before sending, the exact signed bytes are durably saved inside the normal nonce
lease (`signAndBroadcastCommitDurable`, `web/rbac_signing.go:561`). A restart or
lost response reconciles those exact bytes. It never creates a replacement
transaction with a fresh nonce for an unresolved item. If authorization remains
valid, the standard reconciler may resend identical bytes. If automatic consent
or the Root credential changes, recovery only looks up the prior transaction;
it does not authorize another submission.

Turning automatic cleanup OFF prevents new automatic submissions. Already-sent
transactions can still commit. Unresolved outcomes remain visibly pending rather
than being reported as success. Manual cleanup is a separate explicitly confirmed
run, not controlled by the automatic toggle.

## Dashboard API

All routes require current local Root authority and canonical consensus support:

| Route | Behavior |
| --- | --- |
| `GET /v1/dashboard/settings/cleanup` | Returns `config`, `authorized`, `worker_available`, `last_run`, and a structured `last_result` object. |
| `POST /v1/dashboard/settings/cleanup` | Saves validated settings and binds consent to the current Root. |
| `POST /v1/dashboard/cleanup/run` | Requires explicit `dry_run: true/false`; optional `config` supplies rules for this request without saving or enabling automation. |

Preview returns HTTP 200 only after a full scan. Execution returns HTTP 202 when
queued, not when finished; poll settings for progress. A concurrent run returns
409. Invalid settings return 400; unavailable consensus/worker/storage returns
503. Clients must check HTTP status, not interpret an error JSON body as success.
See `canonicalRunCleanup` (`web/cleanup.go:208`).

Settings ranges: observation lifetime 1–90 days, session lifetime 1–30 days,
confidence threshold 0.01–0.50, interval 1–168 hours.
`auto_challenge_conflicts` must be false; arbitrary conflict discovery is not
implemented (`validCleanupConfig`, `web/cleanup.go:58`).
