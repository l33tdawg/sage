package web

// Cleanup is a local operator policy, not a new consensus rule. Every change
// uses the existing challenge transaction; this file never edits lifecycle SQL.
import (
	"context"
	"encoding/hex"
	"encoding/json"
	"math"
	"net/http"
	"time"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

const cleanupPolicyKey = "canonical_cleanup_policy_v1"
const cleanupRunKey = "canonical_cleanup_run_v1"

type cleanupPolicy struct {
	Config     memory.CleanupConfig `json:"config"`
	Credential string               `json:"credential"`
	Generation uint64               `json:"generation"`
}

type cleanupRun struct {
	State            string `json:"state"`
	Checked          int    `json:"checked"`
	Eligible         int    `json:"eligible"`
	Submitted        int    `json:"submitted"`
	Deprecated       int    `json:"deprecated"`
	Challenged       int    `json:"challenged"`
	Skipped          int    `json:"skipped"`
	Complete         bool   `json:"complete"`
	DryRun           bool   `json:"dry_run"`
	Error            string `json:"error,omitempty"`
	Started          string `json:"started_at,omitempty"`
	Finished         string `json:"finished_at,omitempty"`
	Pending          string `json:"pending_id,omitempty"`
	PendingBytes     []byte `json:"pending_bytes,omitempty"`
	PendingConfirmed bool   `json:"pending_confirmed"`
	Unchanged        int    `json:"unchanged"`
	// The immutable, complete plan is retained locally for crash reconciliation.
	IDs       []string      `json:"ids,omitempty"`
	Next      int           `json:"next"`
	Policy    cleanupPolicy `json:"policy"`
	Automatic bool          `json:"automatic"`
}

func (r cleanupRun) public() map[string]any {
	return map[string]any{"state": r.State, "checked": r.Checked, "eligible": r.Eligible,
		"submitted": r.Submitted, "deprecated": r.Deprecated, "challenged": r.Challenged,
		"skipped": r.Skipped, "unchanged": r.Unchanged, "complete": r.Complete, "dry_run": r.DryRun, "error": r.Error,
		"started_at": r.Started, "finished_at": r.Finished, "confirmation_pending": r.Pending != ""}
}

func validCleanupConfig(c memory.CleanupConfig) bool {
	return c.ObservationTTLDays >= 1 && c.ObservationTTLDays <= 90 && c.SessionTTLDays >= 1 && c.SessionTTLDays <= 30 &&
		c.CleanupIntervalHours >= 1 && c.CleanupIntervalHours <= 168 && !math.IsNaN(c.StaleThreshold) &&
		!math.IsInf(c.StaleThreshold, 0) && c.StaleThreshold >= 0.01 && c.StaleThreshold <= 0.5 && !c.AutoChallengeConflicts
}

func (h *DashboardHandler) cleanupRead(ctx context.Context, key string, dst any) error {
	value, err := h.prefStore.GetPreference(ctx, key)
	if err != nil {
		return err
	}
	if value == "" {
		return nil
	}
	return json.Unmarshal([]byte(value), dst)
}

func (h *DashboardHandler) cleanupWrite(ctx context.Context, key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return h.prefStore.SetPreference(ctx, key, string(data))
}

func (h *DashboardHandler) cleanupActor(w http.ResponseWriter, r *http.Request) *appV23ControlActor {
	if h.prefStore == nil {
		writeError(w, 503, "cleanup preferences unavailable")
		return nil
	}
	actor, ok := h.requireAppV23ControlActor(w, r, true)
	if !ok {
		return nil
	}
	if !actor.IsRoot {
		writeError(w, 403, "Memory cleanup policy requires the current local Root.")
		return nil
	}
	return actor
}

func (h *DashboardHandler) canonicalCleanupSettings(w http.ResponseWriter, r *http.Request) {
	if h.cleanupActor(w, r) == nil {
		return
	}
	h.cleanupMu.Lock()
	defer h.cleanupMu.Unlock()
	p := cleanupPolicy{Config: memory.DefaultCleanupConfig()}
	var run cleanupRun
	if err := h.cleanupRead(r.Context(), cleanupPolicyKey, &p); err != nil {
		writeError(w, 503, err.Error())
		return
	}
	if err := h.cleanupRead(r.Context(), cleanupRunKey, &run); err != nil {
		writeError(w, 503, err.Error())
		return
	}
	root, _, broker := h.appV23RootBrokerKey()
	active := broker.Available && root != nil && p.Credential == root.CredentialID && p.Generation == root.Generation
	writeJSONResp(w, 200, map[string]any{"config": p.Config, "authorized": active, "worker_available": h.cleanupWorkerReady.Load(), "last_result": run.public(), "last_run": run.Finished})
}

func (h *DashboardHandler) canonicalSaveCleanup(w http.ResponseWriter, r *http.Request) {
	actor := h.cleanupActor(w, r)
	if actor == nil {
		return
	}
	var cfg memory.CleanupConfig
	if err := decodeAppV23AccessJSON(w, r, &cfg); err != nil || !validCleanupConfig(cfg) {
		writeError(w, 400, "Invalid cleanup settings; unsupported conflict auto-challenge must be off.")
		return
	}
	h.cleanupMu.Lock()
	defer h.cleanupMu.Unlock()
	p := cleanupPolicy{Config: cfg, Credential: actor.ID, Generation: actor.Root.Generation}
	if err := h.cleanupWrite(r.Context(), cleanupPolicyKey, p); err != nil {
		writeError(w, 503, err.Error())
		return
	}
	writeJSONResp(w, 200, map[string]any{"config": cfg, "ok": true})
}

// Pages through the full inventory. A page size is never a total cap. Count
// only canonical committed user memories, with the same rules used at execution.
func (h *DashboardHandler) cleanupPlan(ctx context.Context, p cleanupPolicy) (cleanupRun, error) {
	run := cleanupRun{State: "preview", DryRun: true, Policy: p, Started: time.Now().UTC().Format(time.RFC3339Nano)}
	at := time.Now()
	err := h.walkAppV23DashboardProjectionPages(ctx, store.ListOptions{Sort: "oldest"}, func(records []*memory.MemoryRecord) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		classes, err := h.BadgerStore.ClassifyMemoryProjections(records)
		if err != nil {
			return err
		}
		ids := make([]string, 0, len(records))
		for _, rec := range records {
			if rec != nil {
				ids = append(ids, rec.MemoryID)
			}
		}
		counts, err := h.store.GetCorroborationCounts(ctx, ids)
		if err != nil {
			return err
		}
		for i, rec := range records {
			if rec == nil {
				continue
			}
			run.Checked++
			if classes[i].Err != nil {
				run.Skipped++
				continue
			}
			if cleanupEligible(rec, p.Config, at, counts[rec.MemoryID]) {
				run.IDs = append(run.IDs, rec.MemoryID)
				run.Eligible++
			}
		}
		return nil
	})
	if err != nil {
		run.State = "failed"
		run.Error = err.Error()
		return run, err
	}
	run.Complete = true
	return run, nil
}

func cleanupEligible(rec *memory.MemoryRecord, cfg memory.CleanupConfig, now time.Time, corr int) bool {
	if rec == nil || rec.Status != memory.StatusCommitted || rec.IsOpenTask() || isCerebrumInternalMemoryDomain(rec.DomainTag) || rec.CreatedAt.After(now) {
		return false
	}
	if rec.MemoryType == memory.TypeObservation {
		ttl := cfg.ObservationTTLDays
		if rec.DomainTag == "session-context" {
			ttl = cfg.SessionTTLDays
		}
		if now.Sub(rec.CreatedAt) > time.Duration(ttl)*24*time.Hour {
			return true
		}
	}
	return memory.ComputeConfidenceForRecord(rec, now, corr) < cfg.StaleThreshold
}

func cleanupActive(s string) bool {
	return s == "queued" || s == "scanning" || s == "running" || s == "confirmation_pending"
}

func (h *DashboardHandler) canonicalRunCleanup(w http.ResponseWriter, r *http.Request) {
	actor := h.cleanupActor(w, r)
	if actor == nil {
		return
	}
	var body struct {
		DryRun *bool                 `json:"dry_run"`
		Config *memory.CleanupConfig `json:"config,omitempty"`
	}
	if err := decodeAppV23AccessJSON(w, r, &body); err != nil || body.DryRun == nil {
		writeError(w, 400, "dry_run must be explicitly true or false")
		return
	}
	h.cleanupMu.Lock()
	defer h.cleanupMu.Unlock()
	p := cleanupPolicy{Config: memory.DefaultCleanupConfig()}
	if err := h.cleanupRead(r.Context(), cleanupPolicyKey, &p); err != nil {
		writeError(w, 503, err.Error())
		return
	}
	p.Credential = actor.ID
	p.Generation = actor.Root.Generation
	if body.Config != nil {
		p.Config = *body.Config
	}
	if !validCleanupConfig(p.Config) {
		writeError(w, 400, "Invalid saved cleanup policy")
		return
	}
	if *body.DryRun {
		h.cleanupMu.Unlock()
		run, err := h.cleanupPlan(r.Context(), p)
		h.cleanupMu.Lock()
		if err != nil {
			writeJSONResp(w, 503, run.public())
			return
		}
		writeJSONResp(w, 200, run.public())
		return
	}
	if !h.cleanupWorkerReady.Load() {
		writeError(w, 503, "Cleanup worker is not running")
		return
	}
	var old cleanupRun
	if err := h.cleanupRead(r.Context(), cleanupRunKey, &old); err != nil {
		writeError(w, 503, err.Error())
		return
	}
	if cleanupActive(old.State) || old.Pending != "" {
		writeJSONResp(w, 409, map[string]any{"error": "A cleanup is already active; inspect its progress.", "run": old.public()})
		return
	}
	// Plan asynchronously: neither full inventory size nor consensus latency is
	// bounded by a browser request timeout. No memory mutation happens here.
	run := cleanupRun{State: "queued", Policy: p, Started: time.Now().UTC().Format(time.RFC3339Nano)}
	if err := h.cleanupWrite(r.Context(), cleanupRunKey, run); err != nil {
		writeError(w, 503, err.Error())
		return
	}
	writeJSONResp(w, 202, run.public())
}

// RunCanonicalCleanupWorker is owned by the node's supervised worker lifecycle.
// The explicit versioned policy is fresh opt-in; legacy cleanup_enabled is ignored.
func (h *DashboardHandler) RunCanonicalCleanupWorker(ctx context.Context) {
	if !h.cleanupWorkerReady.CompareAndSwap(false, true) {
		return
	}
	defer h.cleanupWorkerReady.Store(false)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.cleanupTick(ctx)
		}
	}
}

func (h *DashboardHandler) cleanupTick(ctx context.Context) {
	if !h.appV23IsActive() || h.prefStore == nil || h.BadgerStore == nil {
		return
	}
	h.cleanupMu.Lock()
	defer h.cleanupMu.Unlock()
	var run cleanupRun
	if h.cleanupRead(ctx, cleanupRunKey, &run) != nil {
		return
	}
	if cleanupActive(run.State) && (!validCleanupConfig(run.Policy.Config) || run.Next < 0 || run.Next > len(run.IDs)) {
		run.State = "failed"
		run.Error = "Invalid durable cleanup state; operator investigation required"
		run.Finished = time.Now().UTC().Format(time.RFC3339Nano)
		_ = h.cleanupWrite(ctx, cleanupRunKey, run)
		return
	}
	if !cleanupActive(run.State) && run.Pending == "" {
		p := cleanupPolicy{Config: memory.DefaultCleanupConfig()}
		if h.cleanupRead(ctx, cleanupPolicyKey, &p) != nil || !p.Config.Enabled || !validCleanupConfig(p.Config) {
			return
		}
		last, err := time.Parse(time.RFC3339Nano, run.Finished)
		if err == nil && time.Since(last) < time.Duration(p.Config.CleanupIntervalHours)*time.Hour {
			return
		}
		run = cleanupRun{State: "queued", Policy: p, Automatic: true, Started: time.Now().UTC().Format(time.RFC3339Nano)}
	}
	root, _, broker := h.appV23RootBrokerKey()
	authorized := broker.Available && root != nil && root.CredentialID == run.Policy.Credential && root.Generation == run.Policy.Generation
	if !authorized && run.Pending == "" {
		run.State = "failed"
		run.Error = "Cleanup authorization changed or its exact local signing key is unavailable; save settings to authorize again."
		run.Finished = time.Now().UTC().Format(time.RFC3339Nano)
		_ = h.cleanupWrite(ctx, cleanupRunKey, run)
		return
	}
	if run.Automatic {
		var current cleanupPolicy
		if err := h.cleanupRead(ctx, cleanupPolicyKey, &current); err != nil {
			return
		}
		authorized = authorized && current == run.Policy
		if current != run.Policy && run.Pending == "" {
			run.State = "cancelled"
			run.Finished = time.Now().UTC().Format(time.RFC3339Nano)
			_ = h.cleanupWrite(ctx, cleanupRunKey, run)
			return
		}
	}
	if run.State == "queued" || run.State == "scanning" {
		run.State = "scanning"
		if h.cleanupWrite(ctx, cleanupRunKey, run) != nil {
			return
		}
		// Keep status reads and disabling automation responsive during a full
		// inventory scan. The durable scanning state fences concurrent runs.
		h.cleanupMu.Unlock()
		plan, err := h.cleanupPlan(ctx, run.Policy)
		h.cleanupMu.Lock()
		plan.Automatic = run.Automatic
		plan.DryRun = false
		if err != nil {
			plan.Finished = time.Now().UTC().Format(time.RFC3339Nano)
			_ = h.cleanupWrite(ctx, cleanupRunKey, plan)
			return
		}
		run = plan
		run.State = "running"
		run.Complete = false
		if h.cleanupWrite(ctx, cleanupRunKey, run) != nil {
			return
		}
	}
	// A crash or lost RPC response leaves the exact in-flight ID durable. Never
	// blindly resubmit it. Only an observed canonical terminal/challenge outcome
	// permits moving to the next item; otherwise the run remains visibly pending.
	if run.Pending != "" {
		if !run.PendingConfirmed {
			parsed, decodeErr := tx.DecodeTx(run.PendingBytes)
			valid := false
			if decodeErr == nil {
				valid, _ = tx.VerifyTx(parsed)
				valid = valid && parsed.Type == tx.TxTypeMemoryChallenge && parsed.MemoryChallenge != nil &&
					parsed.MemoryChallenge.MemoryID == run.Pending &&
					hex.EncodeToString(parsed.PublicKey) == run.Policy.Credential &&
					parsed.MemoryChallenge.Reason == "operator-authorized memory cleanup"
			}
			if !valid {
				run.Error = "Invalid exact submission intent; operator investigation required"
				_ = h.cleanupWrite(ctx, cleanupRunKey, run)
				return
			}
			attemptCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			// If consent was withdrawn, only query. Otherwise the standard
			// reconciler may resend IDENTICAL signed bytes, never a new nonce.
			var outcome tx.TxOutcome
			var err error
			if authorized && h.CometBFTRPC != "" {
				outcome, err = tx.CometTxResolver(h.CometBFTRPC)(attemptCtx, run.PendingBytes)
			} else {
				outcome, err = tx.LookupCometTxOutcome(attemptCtx, h.CometBFTRPC, run.PendingBytes)
			}
			cancel()
			if err != nil || outcome.Verdict == tx.TxVerdictUnresolved {
				run.State = "confirmation_pending"
				run.Error = "Exact transaction outcome is unresolved; no new cleanup transaction will be signed"
				_ = h.cleanupWrite(ctx, cleanupRunKey, run)
				return
			}
			if outcome.Verdict == tx.TxVerdictRejected {
				run.State = "failed"
				run.Error = "Consensus reconciliation stopped this run; inspect the transaction before retrying"
				run.Pending = ""
				run.PendingBytes = nil
				run.Finished = time.Now().UTC().Format(time.RFC3339Nano)
				_ = h.cleanupWrite(ctx, cleanupRunKey, run)
				return
			}
			run.PendingConfirmed = true
			run.Submitted++
		}
		_, status, err := h.BadgerStore.GetMemoryHash(run.Pending)
		if err != nil {
			return
		}
		switch status {
		case "deprecated":
			run.Deprecated++
		case "challenged":
			run.Challenged++
		default:
			// A later transaction may already have reinstated the memory.
			// The exact cleanup transaction is confirmed, but do not claim it
			// is currently retired or challenge it a second time in this run.
			run.Unchanged++
		}
		run.Pending = ""
		run.PendingBytes = nil
		run.PendingConfirmed = false
		run.Next++
		run.State = "running"
		run.Error = ""
	}
	// Recheck consent after reconciliation as well: disabling automation while
	// a transaction is pending must not authorize the next transaction.
	if run.Automatic {
		var current cleanupPolicy
		if h.cleanupRead(ctx, cleanupPolicyKey, &current) != nil {
			return
		}
		if current != run.Policy {
			run.State = "cancelled"
			run.Finished = time.Now().UTC().Format(time.RFC3339Nano)
			_ = h.cleanupWrite(ctx, cleanupRunKey, run)
			return
		}
	}
	if run.Next >= len(run.IDs) {
		run.State = "completed"
		run.Complete = true
		run.Finished = time.Now().UTC().Format(time.RFC3339Nano)
		run.IDs = nil
		_ = h.cleanupWrite(ctx, cleanupRunKey, run)
		return
	}
	id := run.IDs[run.Next]
	rec, err := h.store.GetMemory(ctx, id)
	if err != nil {
		run.State = "failed"
		run.Error = err.Error()
	} else {
		classes, classErr := h.BadgerStore.ClassifyMemoryProjections([]*memory.MemoryRecord{rec})
		counts, countErr := h.store.GetCorroborationCounts(ctx, []string{id})
		if classErr != nil || countErr != nil {
			run.State = "failed"
			run.Error = "Cannot verify current cleanup target"
		} else if rec == nil || classes[0].Err != nil || !cleanupEligible(rec, run.Policy.Config, time.Now(), counts[id]) {
			run.Skipped++
			run.Next++
		} else {
			// A full scan can take time; resolve the exact current credential again
			// immediately before persisting intent and signing.
			root, key, broker := h.appV23RootBrokerKey()
			if !broker.Available || root == nil || root.CredentialID != run.Policy.Credential || root.Generation != run.Policy.Generation {
				run.State = "failed"
				run.Error = "Cleanup authorization changed before submission"
				run.Finished = time.Now().UTC().Format(time.RFC3339Nano)
				_ = h.cleanupWrite(ctx, cleanupRunKey, run)
				return
			}
			sendCtx, cancelSend := context.WithTimeout(ctx, backgroundSigningBudget())
			_, _, _, sendErr := h.signAndBroadcastCommitDurable(sendCtx, &tx.ParsedTx{Type: tx.TxTypeMemoryChallenge, Timestamp: time.Now(), MemoryChallenge: &tx.MemoryChallenge{MemoryID: id, Reason: "operator-authorized memory cleanup"}}, key, nil, func(encoded []byte) error {
				run.Pending = id
				run.PendingBytes = append([]byte(nil), encoded...)
				return h.cleanupWrite(ctx, cleanupRunKey, run)
			})
			cancelSend()
			if sendErr != nil {
				run.Error = sendErr.Error()
				if isIndeterminateCommitError(sendErr) {
					run.State = "confirmation_pending"
				} else {
					run.Pending = ""
					run.PendingBytes = nil
					run.State = "failed"
				}
			} else {
				run.Submitted++
				run.PendingConfirmed = true
				// Observe canonical outcome on the next tick, never infer it from log text.
				run.State = "confirmation_pending"
			}
		}
	}
	if run.State == "failed" {
		run.Finished = time.Now().UTC().Format(time.RFC3339Nano)
	}
	_ = h.cleanupWrite(ctx, cleanupRunKey, run)
}
