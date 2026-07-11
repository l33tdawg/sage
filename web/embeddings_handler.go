package web

// Embeddings setup (Phase: embeddings-setup + re-embed). SAGE ships with a
// bundled local embedder (Ollama + nomic-embed-text) but falls back to a "hash"
// pseudo-embedder when Ollama isn't the configured provider — which gives only
// keyword matching, no semantic recall. This surface lets the operator turn the
// real embedder on from CEREBRUM: detect/guide Ollama, re-embed all existing
// memories through it (SSE progress), then switch the node to the Ollama
// provider (a restart, so every consumer picks it up).
//
// The embedder is LOCKED to Ollama + nomic-embed-text — this is not a
// "choose your embedder" screen; it just turns the bundled one on.

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/embedding"
	"github.com/l33tdawg/sage/internal/vault"
)

const (
	ollamaBaseURL   = "http://localhost:11434"
	embedModel      = "nomic-embed-text"
	embedDimension  = 768
	reembedPageSize = 100
)

type OllamadManager interface {
	BinaryPath() (string, bool)
	EngineInstalled() bool
	InstallSupported() bool
	EngineSizeBytes() int64
	Installing() bool
	Pulling() bool
	Progress() (done, total int64)
	PullStatus() string
	InstallEngine(ctx context.Context, progress func(done, total int64)) error
	Start(ctx context.Context) (string, error)
	Probe(ctx context.Context) bool
	ModelReady(ctx context.Context) bool
	PullModel(ctx context.Context, status func(string), progress func(done, total int64)) error
	URL() string
}

// RegisterEmbeddingsRoutes wires the embeddings-setup routes (authed group).
func (h *DashboardHandler) RegisterEmbeddingsRoutes(r chi.Router) {
	r.Get("/v1/dashboard/embeddings/status", h.handleEmbeddingsStatus)
	r.Post("/v1/dashboard/embeddings/check-ollama", h.handleEmbeddingsCheckOllama)
	r.Group(func(r chi.Router) {
		r.Use(h.wizardSecurityGate)
		r.Post("/v1/dashboard/embeddings/install-ollama", h.handleEmbeddingsInstallOllama)
		r.Post("/v1/dashboard/embeddings/start-ollama", h.handleEmbeddingsStartOllama)
		r.Post("/v1/dashboard/embeddings/pull-model", h.handleEmbeddingsPullModel)
	})
	r.Post("/v1/dashboard/embeddings/reembed", h.handleEmbeddingsReembed)
	r.Get("/v1/dashboard/embeddings/reembed/progress", h.handleEmbeddingsReembedProgress)
	r.Post("/v1/dashboard/embeddings/deprecate-unreadable", h.handleEmbeddingsDeprecateUnreadable)
	r.Post("/v1/dashboard/embeddings/recover-preview", h.handleEmbeddingsRecoverPreview)
	r.Post("/v1/dashboard/embeddings/recover", h.handleEmbeddingsRecover)
	r.Post("/v1/dashboard/embeddings/enable", h.handleEmbeddingsEnable)
}

// handleEmbeddingsStatus reports the current embedder + how much re-embedding is
// pending, so the frontend can drive the setup flow and surface a "turn on
// semantic search" call to action from the hash-provider status.
func (h *DashboardHandler) handleEmbeddingsStatus(w http.ResponseWriter, r *http.Request) {
	counts, err := h.store.CountMemoriesByProvider(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "count memories: "+err.Error())
		return
	}
	total := 0
	for _, n := range counts {
		total += n
	}
	current := currentEmbedProvider(h.embedder)
	ollamaUp := h.embeddingOllamaRunning(r.Context())
	modelReady := ollamaUp && h.embeddingOllamaHasModel(r.Context())
	binPath, binFound := "", false
	engineInstalled, installSupported, installing, pulling := false, false, false, false
	var engineBytes, modelDone, modelTotal int64
	pullStatus := ""
	if h.Ollamad != nil {
		binPath, binFound = h.Ollamad.BinaryPath()
		engineInstalled = h.Ollamad.EngineInstalled()
		installSupported = h.Ollamad.InstallSupported()
		engineBytes = h.Ollamad.EngineSizeBytes()
		installing = h.Ollamad.Installing()
		pulling = h.Ollamad.Pulling()
		modelDone, modelTotal = h.Ollamad.Progress()
		pullStatus = h.Ollamad.PullStatus()
	}

	writeJSONResp(w, http.StatusOK, map[string]any{
		"provider":                 current, // active embedder: "hash" | "ollama" | ...
		"is_semantic":              current == embedProviderOllama,
		"model":                    embedModel,
		"dimension":                embedDimension,
		"ollama_running":           ollamaUp,
		"model_available":          modelReady,
		"ollama_binary_found":      binFound,
		"ollama_binary_path":       binPath,
		"ollama_engine_installed":  engineInstalled,
		"ollama_install_supported": installSupported,
		"ollama_engine_bytes":      engineBytes,
		"ollama_installing":        installing,
		"ollama_pulling":           pulling,
		"ollama_model_done":        modelDone,
		"ollama_model_total":       modelTotal,
		"ollama_pull_status":       pullStatus,
		"ollama_url": func() string {
			if h.Ollamad != nil {
				return h.Ollamad.URL()
			}
			return ollamaBaseURL
		}(),
		"total_memories": total,
		"need_reembed":   counts[""], // untagged memories still needing an embedding (excludes done + skipped)
		"on_ollama":      counts[embedProviderOllama],
		"unreadable":     counts["skipped"], // undecryptable/empty — deprecation candidates
		"errored":        counts["error"],   // readable but embed failed — retryable
		"vault_locked":   h.VaultLocked.Load(),
	})
}

const embedProviderOllama = "ollama"

// currentEmbedProvider mirrors the health handler's provider dispatch.
func currentEmbedProvider(e Embedder) string {
	if e == nil {
		return "unknown"
	}
	if named, ok := e.(embedding.Named); ok {
		return named.Name()
	}
	if ep, ok := e.(embedderProvider); ok && ep.Semantic() {
		return embedProviderOllama
	}
	return "hash"
}

// handleEmbeddingsCheckOllama reports whether Ollama is reachable and whether the
// bundled model is pulled.
func (h *DashboardHandler) handleEmbeddingsCheckOllama(w http.ResponseWriter, r *http.Request) {
	up := h.embeddingOllamaRunning(r.Context())
	writeJSONResp(w, http.StatusOK, map[string]any{
		"ollama_running":  up,
		"model":           embedModel,
		"model_available": up && h.embeddingOllamaHasModel(r.Context()),
	})
}

func (h *DashboardHandler) handleEmbeddingsInstallOllama(w http.ResponseWriter, r *http.Request) {
	if h.Ollamad == nil {
		writeError(w, http.StatusNotImplemented, "managed Ollama setup not available on this node")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	_ = http.NewResponseController(w).SetWriteDeadline(time.Time{}) //nolint:errcheck
	w.WriteHeader(http.StatusOK)
	line := func(k, v string) { fmt.Fprintf(w, "%s: %s\n", k, v); flusher.Flush() }
	if h.Ollamad.Installing() {
		h.streamOllamaProgress(r.Context(), h.Ollamad.Installing, h.Ollamad.EngineInstalled, line)
		return
	}
	instCtx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()
	lastEmit := time.Time{}
	err := h.Ollamad.InstallEngine(instCtx, func(done, total int64) {
		if time.Since(lastEmit) < 250*time.Millisecond && done != total {
			return
		}
		lastEmit = time.Now()
		line("progress", fmt.Sprintf("%d %d", done, total))
	})
	if err != nil {
		if h.Ollamad.EngineInstalled() {
			line("done", "0")
			return
		}
		if h.Ollamad.Installing() {
			h.streamOllamaProgress(r.Context(), h.Ollamad.Installing, h.Ollamad.EngineInstalled, line)
			return
		}
		line("error", err.Error())
		line("done", "1")
		return
	}
	line("done", "0")
}

func (h *DashboardHandler) handleEmbeddingsStartOllama(w http.ResponseWriter, r *http.Request) {
	if h.Ollamad == nil {
		writeError(w, http.StatusNotImplemented, "managed Ollama setup not available on this node")
		return
	}
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(80 * time.Second)) //nolint:errcheck
	ctx, cancel := context.WithTimeout(r.Context(), 75*time.Second)
	defer cancel()
	url, err := h.Ollamad.Start(ctx)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	if h.prefStore != nil {
		_ = h.prefStore.SetPreference(r.Context(), "ollama_managed", "1")
		_ = h.prefStore.SetPreference(r.Context(), "ollama_url", url)
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"ok": true, "url": url})
}

// handleEmbeddingsPullModel streams `ollama pull nomic-embed-text` progress as
// text/plain lines ("status: msg", final "done: 0|1").
func (h *DashboardHandler) handleEmbeddingsPullModel(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	// Clear the http.Server's 15s WriteTimeout (an absolute per-request write
	// deadline) so the server doesn't guillotine this minutes-long model pull
	// mid-stream (same fix as web/sse.go).
	_ = http.NewResponseController(w).SetWriteDeadline(time.Time{}) //nolint:errcheck // best-effort; unsupported writers keep prior behaviour
	w.WriteHeader(http.StatusOK)
	line := func(k, v string) { fmt.Fprintf(w, "%s: %s\n", k, v); flusher.Flush() }

	if h.Ollamad != nil {
		if h.Ollamad.Pulling() {
			h.streamOllamaProgress(r.Context(), h.Ollamad.Pulling, func() bool { return h.Ollamad.ModelReady(r.Context()) }, line)
			return
		}
		pullCtx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
		defer cancel()
		err := h.Ollamad.PullModel(pullCtx,
			func(status string) { line("status", status) },
			func(done, total int64) { line("progress", fmt.Sprintf("%d %d", done, total)) },
		)
		if err != nil {
			if h.Ollamad.ModelReady(r.Context()) {
				line("done", "0")
				return
			}
			if h.Ollamad.Pulling() {
				h.streamOllamaProgress(r.Context(), h.Ollamad.Pulling, func() bool { return h.Ollamad.ModelReady(r.Context()) }, line)
				return
			}
			line("error", err.Error())
			line("done", "1")
			return
		}
		line("done", "0")
		return
	}

	body, _ := json.Marshal(map[string]any{"name": embedModel, "stream": true})
	req, _ := http.NewRequestWithContext(r.Context(), http.MethodPost, ollamaBaseURL+"/api/pull", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 10 * time.Minute}).Do(req)
	if err != nil {
		line("error", "cannot reach Ollama: "+err.Error())
		line("done", "1")
		return
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for scanner.Scan() {
		var msg struct {
			Status string `json:"status"`
			Error  string `json:"error"`
		}
		if json.Unmarshal(scanner.Bytes(), &msg) == nil {
			if msg.Error != "" {
				line("error", msg.Error)
				line("done", "1")
				return
			}
			if msg.Status != "" {
				line("status", msg.Status)
			}
		}
	}
	line("done", "0")
}

func (h *DashboardHandler) streamOllamaProgress(ctx context.Context, active func() bool, complete func() bool, line func(k, v string)) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if h.Ollamad != nil {
			if status := h.Ollamad.PullStatus(); h.Ollamad.Pulling() && status != "" {
				line("status", status)
			}
			if done, total := h.Ollamad.Progress(); total > 0 {
				line("progress", fmt.Sprintf("%d %d", done, total))
			}
		}
		if !active() {
			if complete() {
				line("done", "0")
			} else {
				line("error", "operation did not complete")
				line("done", "1")
			}
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// reembedJob is the live state of the SERVER-SIDE background re-embed job. It is
// intentionally decoupled from any HTTP request: a re-embed of thousands of
// memories takes minutes, and tying it to a streaming response made it die on any
// client hiccup (backgrounded tab, throttling, network blip) — which then
// cancelled the server loop via the request context. The job now runs on a
// background context and the frontend polls handleEmbeddingsReembedProgress.
type reembedJob struct {
	mu        sync.Mutex
	running   bool
	done      int // successfully embedded this run
	skipped   int // couldn't embed (undecryptable / empty / embed error) — tagged so they leave the set
	total     int // memories needing embedding at the start of the run
	errMsg    string
	startedAt time.Time
}

func (j *reembedJob) snapshot() map[string]any {
	j.mu.Lock()
	defer j.mu.Unlock()
	m := map[string]any{
		"running": j.running,
		"done":    j.done,
		"skipped": j.skipped,
		"total":   j.total,
		"error":   j.errMsg,
	}
	// ETA from the observed rate (processed / elapsed). Only meaningful while
	// running with some progress; the client shows it as "~N min left".
	processed := j.done + j.skipped
	if j.running && processed > 0 && !j.startedAt.IsZero() {
		elapsed := time.Since(j.startedAt).Seconds()
		if elapsed > 0 {
			rate := float64(processed) / elapsed // items/sec
			remaining := j.total - processed
			if rate > 0 && remaining > 0 {
				m["eta_seconds"] = int(float64(remaining) / rate)
			}
			m["elapsed_seconds"] = int(elapsed)
		}
	}
	return m
}

// handleEmbeddingsReembed STARTS (or re-attaches to) the background re-embed job
// and returns its current snapshot immediately. Idempotent: if a job is already
// running, it returns that job's status instead of starting a second one.
// Requires the vault unlocked (it decrypts content + re-encrypts embeddings).
// Resumable: already-Ollama memories are skipped, so a re-run finishes an
// interrupted pass.
func (h *DashboardHandler) handleEmbeddingsReembed(w http.ResponseWriter, r *http.Request) {
	if h.VaultLocked.Load() {
		writeError(w, http.StatusForbidden, "unlock the vault before re-embedding")
		return
	}
	if !h.embeddingOllamaRunning(r.Context()) || !h.embeddingOllamaHasModel(r.Context()) {
		writeError(w, http.StatusPreconditionFailed, "Ollama with "+embedModel+" is not available")
		return
	}

	h.reembed.mu.Lock()
	if h.reembed.running {
		h.reembed.mu.Unlock()
		writeJSONResp(w, http.StatusOK, h.reembed.snapshot())
		return
	}
	// Retry previously-errored embeds by clearing the 'error' tag back to '' so
	// they re-enter the work set this run (transient failures aren't stuck forever).
	_, _ = h.store.ResetErroredEmbeddings(r.Context())
	// Count what's left (untagged memories) up front for the progress bar.
	counts, _ := h.store.CountMemoriesByProvider(r.Context())
	total := counts[""]
	h.reembed.running = true
	h.reembed.done, h.reembed.skipped, h.reembed.total, h.reembed.errMsg = 0, 0, total, ""
	h.reembed.startedAt = time.Now() // for ETA
	h.reembed.mu.Unlock()

	h.runBackground(h.runReembed)

	writeJSONResp(w, http.StatusOK, h.reembed.snapshot())
}

// handleEmbeddingsReembedProgress returns the background job's current snapshot.
func (h *DashboardHandler) handleEmbeddingsReembedProgress(w http.ResponseWriter, r *http.Request) {
	writeJSONResp(w, http.StatusOK, h.reembed.snapshot())
}

// handleEmbeddingsDeprecateUnreadable soft-deprecates the memories that couldn't
// be read (embedding_provider = 'skipped'; undecryptable with the current vault
// key). Deprecated = hidden from all views, row + on-chain hash retained
// (reversible, NOT a hard delete). Only touches unreadable memories — readable
// memories that merely failed to embed ('error') are never deprecated.
func (h *DashboardHandler) handleEmbeddingsDeprecateUnreadable(w http.ResponseWriter, r *http.Request) {
	n, err := h.store.DeprecateUnreadableMemories(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "deprecate unreadable: "+err.Error())
		return
	}
	if n > 0 {
		h.SSE.Broadcast(SSEEvent{Type: EventForget})
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"deprecated": n})
}

// runReembed is the background worker. It uses context.Background() so it is NOT
// tied to the triggering request — the operator can close the modal, background
// the tab, or lose the connection and the job keeps going.
func (h *DashboardHandler) runReembed(ctx context.Context) {
	defer func() {
		h.reembed.mu.Lock()
		h.reembed.running = false
		h.reembed.mu.Unlock()
	}()

	client := embedding.NewClient(ollamaBaseURL, embedModel)
	fail := func(msg string) {
		h.reembed.mu.Lock()
		h.reembed.errMsg = msg
		h.reembed.mu.Unlock()
	}
	// markUnreadable: content can't be read (undecryptable / empty) — a deprecation
	// candidate. markErrored: content IS readable but the embed failed (retryable),
	// kept distinct so it's never deprecated. Both tag the row so it leaves the ''
	// work set and the loop converges. A FAILED tag write is returned so the caller
	// stops rather than re-fetching the same untagged row forever.
	markUnreadable := func(id string) error {
		if err := h.store.MarkMemoryEmbeddingSkipped(ctx, id); err != nil {
			return err
		}
		h.reembed.mu.Lock()
		h.reembed.skipped++
		h.reembed.mu.Unlock()
		return nil
	}
	markErrored := func(id string) error {
		if err := h.store.MarkMemoryEmbeddingError(ctx, id); err != nil {
			return err
		}
		h.reembed.mu.Lock()
		h.reembed.skipped++
		h.reembed.mu.Unlock()
		return nil
	}
	for {
		// Each call returns the NEXT batch of still-untagged memories; every one is
		// tagged below (ollama / skipped / error), so the set converges to empty.
		// ListMemoriesForReembed returns an error if the vault key is unavailable
		// (locked mid-run), so we never mis-tag readable memories as unreadable.
		mems, err := h.store.ListMemoriesForReembed(ctx, reembedPageSize)
		if err != nil {
			fail("list memories: " + err.Error())
			return
		}
		if len(mems) == 0 {
			return // no more work — converged
		}
		for _, m := range mems {
			var tagErr error
			// Unreadable: undecryptable (vault-key mismatch) or empty content.
			if !m.Decryptable || strings.TrimSpace(m.Content) == "" {
				tagErr = markUnreadable(m.MemoryID)
			} else if emb, embErr := client.Embed(ctx, m.Content); embErr != nil {
				tagErr = markErrored(m.MemoryID) // readable but embed failed — retryable
			} else if upErr := h.store.UpdateMemoryEmbedding(ctx, m.MemoryID, emb, embedProviderOllama); upErr != nil {
				tagErr = markErrored(m.MemoryID)
			} else {
				h.reembed.mu.Lock()
				h.reembed.done++
				h.reembed.mu.Unlock()
			}
			if tagErr != nil {
				// A tag write failed — the row stays untagged and would be re-fetched
				// forever. Stop with a visible error instead of spinning.
				fail("tag write failed: " + tagErr.Error())
				return
			}
		}
	}
}

// handleEmbeddingsRecoverPreview reports how many unreadable memories a given OLD
// recovery key can decrypt — WITHOUT mutating anything — so the operator can
// confirm the key is right before committing.
func (h *DashboardHandler) handleEmbeddingsRecoverPreview(w http.ResponseWriter, r *http.Request) {
	h.recoverOrphans(w, r, true)
}

// handleEmbeddingsRecover re-keys every unreadable memory the supplied old recovery
// key can decrypt: decrypts with the old key, re-encrypts under the LIVE vault in
// place, and clears embedding_provider so it re-embeds. Single key per call — run
// again with another old key for whatever remains.
func (h *DashboardHandler) handleEmbeddingsRecover(w http.ResponseWriter, r *http.Request) {
	h.recoverOrphans(w, r, false)
}

func (h *DashboardHandler) recoverOrphans(w http.ResponseWriter, r *http.Request, dryRun bool) {
	if h.VaultLocked.Load() {
		writeError(w, http.StatusForbidden, "unlock the vault before recovering memories")
		return
	}
	var body struct {
		RecoveryKey string `json:"recovery_key"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if body.RecoveryKey == "" {
		writeError(w, http.StatusBadRequest, "recovery_key is required")
		return
	}
	oldVault, err := vault.FromDataKey(body.RecoveryKey)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid recovery key format") // don't echo key bytes
		return
	}
	n, err := h.store.RekeyUnreadableMemories(r.Context(), oldVault, dryRun)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "recover: "+err.Error())
		return
	}
	counts, _ := h.store.CountMemoriesByProvider(r.Context())
	resp := map[string]any{
		"remaining_unreadable": counts["skipped"], // still-unreadable (try another key)
	}
	if dryRun {
		resp["decryptable"] = n // how many THIS key can recover
	} else {
		resp["recovered"] = n
		if n > 0 {
			h.SSE.Broadcast(SSEEvent{Type: EventForget}) // recovered content appears in views
			// The recovered rows re-entered the re-embed work set (their
			// embedding_provider was cleared). On a semantic node, actually
			// start the background job so the "queued for re-embedding" copy
			// is true - otherwise they sit unembedded until someone finds the
			// Settings CTA. Same guarded start as handleEmbeddingsReembed.
			if currentEmbedProvider(h.embedder) == embedProviderOllama &&
				h.embeddingOllamaRunning(r.Context()) && h.embeddingOllamaHasModel(r.Context()) {
				h.reembed.mu.Lock()
				if !h.reembed.running {
					counts2, _ := h.store.CountMemoriesByProvider(r.Context())
					h.reembed.running = true
					h.reembed.done, h.reembed.skipped, h.reembed.total, h.reembed.errMsg = 0, 0, counts2[""], ""
					h.reembed.startedAt = time.Now()
					h.runBackground(h.runReembed)
				}
				h.reembed.mu.Unlock()
				resp["reembed_started"] = true
			}
		}
	}
	writeJSONResp(w, http.StatusOK, resp)
}

func (h *DashboardHandler) embeddingOllamaRunning(ctx context.Context) bool {
	if h.Ollamad != nil {
		return h.Ollamad.Probe(ctx)
	}
	return ollamaRunning(ctx)
}

func (h *DashboardHandler) embeddingOllamaHasModel(ctx context.Context) bool {
	if h.Ollamad != nil {
		return h.Ollamad.ModelReady(ctx)
	}
	return ollamaHasModel(ctx, embedModel)
}

// handleEmbeddingsEnable switches the node's embedding provider to Ollama in
// config and restarts so every consumer (the /v1/embed endpoint, import, search)
// picks it up. Re-embedding should be run FIRST (while unlocked) so memories are
// already on the Ollama vector space when the switch takes effect.
func (h *DashboardHandler) handleEmbeddingsEnable(w http.ResponseWriter, r *http.Request) {
	if h.SetEmbeddingOllama == nil {
		writeError(w, http.StatusServiceUnavailable, "embedding switch not available on this node")
		return
	}
	if err := h.SetEmbeddingOllama(); err != nil {
		writeError(w, http.StatusInternalServerError, "enable ollama embeddings: "+err.Error())
		return
	}
	// The provider switch is persisted above; a restart just makes every
	// consumer pick it up. Where an in-process restart isn't supported (Windows
	// has no exec(2)), tell the operator to relaunch manually instead of
	// pretending to restart and silently doing nothing.
	if !restartInProcessSupported() {
		writeJSONResp(w, http.StatusOK, map[string]any{
			"ok":               true,
			"restart_required": true,
			"message":          "Semantic memory is on. Quit SAGE and relaunch it to finish switching.",
		})
		return
	}
	if h.RequestRestart == nil {
		writeJSONResp(w, http.StatusOK, map[string]any{
			"ok": true, "restart_required": true,
			"message": "Semantic memory is on. Fully quit SAGE and open it again to finish switching.",
		})
		return
	}
	if err := h.RequestRestart(); err != nil {
		writeJSONResp(w, http.StatusServiceUnavailable, map[string]any{
			"ok": true, "restart_required": true,
			"message": "Semantic memory is saved, but SAGE could not restart cleanly. Fully quit and reopen it.",
		})
		return
	}
	writeJSONResp(w, http.StatusAccepted, map[string]any{"ok": true, "status": "draining", "message": "Turning on semantic memory and restarting cleanly…"})
}

// ollamaRunning reports whether the local Ollama daemon answers /api/tags.
func ollamaRunning(ctx context.Context) bool {
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(cctx, http.MethodGet, ollamaBaseURL+"/api/tags", nil)
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// ollamaHasModel reports whether the given model is pulled locally.
func ollamaHasModel(ctx context.Context, model string) bool {
	cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(cctx, http.MethodGet, ollamaBaseURL+"/api/tags", nil)
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	var tags struct {
		Models []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	if json.NewDecoder(resp.Body).Decode(&tags) != nil {
		return false
	}
	for _, m := range tags.Models {
		// Ollama tags look like "nomic-embed-text:latest"; match the base name.
		if m.Name == model || strings.HasPrefix(m.Name, model+":") {
			return true
		}
	}
	return false
}
