<!-- Verified against code at SAGE v11 (branch v11-dev, commit c568c7f). Cite file:line when behavior is non-obvious. -->

# SAGE Local Engines and First-Run Setup Reference (v11)

**The v11 dashboard surface for standing up SAGE's optional local inference engines
without a terminal, plus the onboarding and recall-tuning knobs, plus the embedder
provenance column that ties each memory to the embedder that produced it.**

Everything here is **off-consensus**: the reranker only reorders recall candidates,
onboarding is a per-node UI flag, recall tuning is a per-node preference, and the
`embedding_provider` stamp is an off-chain column on the SQLite mirror. None of it
touches chain state. Normal memory submission (which does reach consensus) is
documented in [`rest-api.md`](rest-api.md) and [`concepts/memory-lifecycle.md`](concepts/memory-lifecycle.md).

All endpoints below live on the **dashboard listener** and use the dashboard's
cookie/session auth (`authMiddleware`, `web/handler.go:635`), not the Ed25519
signed-request scheme the `/v1/*` public API uses. The managed-reranker **setup**
endpoints additionally sit behind a strict same-origin gate (see below).

---

## 1. First-run onboarding - `/v1/dashboard/settings/onboarding`

A single per-node preference flag (`onboarding_done`) drives the first-run wizard.
The dashboard shows the wizard until this is set, and only on an empty brain, so
upgrades of lived-in nodes never see it; Settings > Maintenance offers a
"run setup again" entry that ignores the flag (`web/onboarding_handler.go:8-11`).
Routes are registered at `web/handler.go:417-418`.

### `GET /v1/dashboard/settings/onboarding`

Reports whether onboarding has been completed or dismissed (`handleGetOnboarding`,
`web/onboarding_handler.go:15`).

**Response** (HTTP 200): `{"done": true}`

When the node has no preference store there is nowhere to remember a dismissal, so
the handler reports `done: true` rather than nag on every load
(`web/onboarding_handler.go:16-24`).

### `POST /v1/dashboard/settings/onboarding`

Marks onboarding done (any close of the wizard) or un-done. Idempotent
(`handleSaveOnboarding`, `web/onboarding_handler.go:29`).

**Request:** `{"done": true}`
**Response** (HTTP 200): `{"done": true}`

Persists `onboarding_done` as `"1"` / `"0"` (`web/onboarding_handler.go:42-48`).
Returns HTTP 501 when the node has no preference store
(`web/onboarding_handler.go:38-40`).

---

## 2. Recall tuning - `/v1/dashboard/settings/recall`

Two per-node preferences tune the recall path the MCP `sage_turn` / `sage_recall`
tools read: `recall_top_k` and `recall_min_confidence`. Routes at
`web/handler.go:411-412`.

### `GET /v1/dashboard/settings/recall`

Returns the current values (`handleGetRecallSettings`, `web/handler.go:2439`).

**Response** (HTTP 200): `{"top_k": 5, "min_confidence": 70}`

Defaults when unset: `top_k` = 5 (`web/handler.go:2451`), `min_confidence` = 70
(percent; `web/handler.go:2458`). The 70% default catches observations (0.80+)
and inferences (0.60+), not just facts.

### `POST /v1/dashboard/settings/recall`

Saves both values, **clamped** (`handleSaveRecallSettings`, `web/handler.go:2472`).

**Request:** `{"top_k": 10, "min_confidence": 75}`
**Response** (HTTP 200): `{"ok": true, "top_k": 10, "min_confidence": 75}`

**v11 clamp bounds** (`web/handler.go:2491-2505`):

| Field | Min | Max | Why |
|-------|-----|-----|-----|
| `top_k` | 3 | 20 | The floor of 3 keeps recall from being starved into uselessness; k tops out at 20 because with the reranker on, high k stays sharp (over-sample + re-score). |
| `min_confidence` | 50 | 100 | Floor 50 is low enough to include inferences (0.60+) and the documented 70% default; the old floor of 85 made the default unrepresentable the moment anyone pressed Save. |

> This clamp is a **dashboard-only** control. It does not gate the `top_k` /
> `min_confidence` fields on the SDK / REST query endpoints (`POST /v1/memory/query`
> and friends), which pass those values through unclamped (`api/rest/memory_handler.go`).

---

## 3. Reranker configuration - `/v1/dashboard/settings/reranker`

The optional cross-encoder reranker refines the top-K after hybrid recall
(BM25 + vector via RRF). It is off by default and off-consensus. These endpoints
configure a **bring-your-own** reranker endpoint; the **managed sidecar** flow
(section 4) drives the same store hot-swap but downloads the engine itself.
Routes at `web/handler.go:413-416`. The reranker is hot-swapped LIVE on the recall
path via `SetReranker` (no restart) and persisted to preferences so it survives a
restart independent of the `SAGE_RERANK_*` env vars (`web/handler_reranker.go:63-65`).

### `GET /v1/dashboard/settings/reranker`

Returns the current reranker configuration (`handleGetReranker`,
`web/handler_reranker.go:34`). It reports the live store state, then lets persisted
preferences override it.

**Response** (HTTP 200):
```json
{"enabled": false, "url": "", "model": "BAAI/bge-reranker-v2-m3", "kind": "llamacpp"}
```

### `POST /v1/dashboard/settings/reranker`

Enables/disables and configures the reranker (`handleSaveReranker`,
`web/handler_reranker.go:66`).

**Request:**
```json
{"enabled": true, "url": "http://localhost:8081", "model": "BAAI/bge-reranker-v2-m3", "kind": "tei"}
```

| Field | Meaning |
|-------|---------|
| `enabled` | Turn the reranker on/off. When true, `url` is required (`web/handler_reranker.go:75-78`). |
| `url` | The reranker endpoint. |
| `model` | Informational model id; defaults to `BAAI/bge-reranker-v2-m3`. |
| `kind` | **v11 field.** Endpoint dialect: `"tei"` (default) or `"llamacpp"` (the managed sidecar). The Settings form omits it, which means TEI (`web/handler_reranker.go:24-29`). |

**The `kind` field** selects which wire dialect the reranker HTTP client speaks (see
section 5). Unknown / omitted values fall back to TEI. It is lower-cased and trimmed
before use (`web/handler_reranker.go:88`).

**Verify-on-enable (v11):** when `enabled` is true, the handler probes the URL with a
trivial rerank call **before** accepting the change (`web/handler_reranker.go:97-106`).
A URL that does not answer is rejected with HTTP 400
(`"reranker not reachable at that URL: ..."`) rather than left silently "On" while
every recall falls back to RRF ordering. Turning the reranker **off** never probes.

**Response** (HTTP 200): the accepted view, e.g.
`{"enabled": true, "url": "http://localhost:8081", "model": "BAAI/bge-reranker-v2-m3", "kind": "tei"}`.

**Managed-sidecar hand-off:** a manual save is the operator taking over from the
managed flow. If a managed sidecar was running (`reranker_managed == "1"`) and the
operator did not re-enter the sidecar's own URL, the handler stops the sidecar before
clearing the managed flag, so it does not outlive node restarts as an orphan
(`web/handler_reranker.go:119-123`). It always writes `reranker_managed = "0"` so the
boot path stops auto-starting the sidecar (`web/handler_reranker.go:134`).

### `POST /v1/dashboard/settings/reranker/test`

Probes a candidate endpoint with a trivial rerank call so the operator can validate a
URL before enabling it. Does NOT touch the live reranker (`handleTestReranker`,
`web/handler_reranker.go:163`).

**Request:** `{"url": "http://localhost:8081", "model": "..."}`
**Response** (HTTP 200): `{"ok": true}` or `{"ok": false, "error": "..."}`

### `GET /v1/dashboard/settings/reranker/detect` (v11)

Probes the conventional local reranker address so the dashboard can pre-fill the URL
field instead of presenting a blank one (`handleDetectReranker`,
`web/handler_reranker.go:146`). The candidate list is a **fixed** loopback set
(`http://localhost:8081`, `http://127.0.0.1:8081`) - no caller-supplied URL is
accepted, so this is a convenience probe, not an SSRF surface. Both `localhost` and
`127.0.0.1` are tried in case `localhost` resolves to `::1` while the reranker binds
IPv4 only.

**Response** (HTTP 200): `{"found": true, "url": "http://localhost:8081"}` or `{"found": false}`

> Port note: the **detect** probe targets `8081` (the bring-your-own TEI convention).
> The **managed** sidecar (section 4) binds `8082` deliberately, to stay out of a
> hand-run TEI server's way (`internal/rerankd/rerankd.go:39-42`).

---

## 4. Managed reranker setup - `/v1/dashboard/reranker/setup/*` (v11)

Ollama-style guided setup: SAGE downloads a pinned llama.cpp engine and a pinned GGUF
model itself, then spawns and manages the process - no package manager, no sudo, no
terminal. Routes are registered by `RegisterRerankerSetupRoutes`
(`web/reranker_setup_handler.go:38-44`). The frontend contract to the manager is the
`RerankdManager` interface (`web/reranker_setup_handler.go:18-34`); a nil manager
means the feature is unavailable on this node and every endpoint returns
`{"available": false}` or HTTP 501.

**Auth:** these routes carry `authMiddleware` **plus** a strict same-origin gate
(`wizardSecurityGate`, `web/handler.go:397-400`, `web/handler.go:741`). Because setup
downloads and `chmod`s a binary and spawns `llama-server` as a subprocess, the same
gate the ChatGPT / federation / network-join wizards use rejects any request whose
`Origin` / `Sec-Fetch-Site` is not local, independent of cookie or session state - a
cross-origin tab must never be able to drive subprocess execution.

### `GET /v1/dashboard/reranker/setup/status`

Drives the guided flow: which of engine / model / process are already in place, plus
the OS so the frontend shows the right guidance (`handleRerankerSetupStatus`,
`web/reranker_setup_handler.go:49`).

**Response** (HTTP 200) when available:
```json
{
  "available": true,
  "os": "darwin",
  "binary_found": true,
  "binary_path": "/Users/you/.sage/llama.cpp/llama-server",
  "engine_installed": true,
  "install_supported": true,
  "engine_bytes": 11138009,
  "model_name": "bge-reranker-v2-m3 (Q8_0)",
  "model_bytes": 635676416,
  "model_ready": true,
  "downloading": false,
  "installing": false,
  "running": true,
  "url": "http://127.0.0.1:8082",
  "managed": true
}
```

`binary_found` reflects any usable `llama-server` (managed install, PATH, or the usual
brew/`/usr/local` prefixes; `internal/rerankd/rerankd.go:91-109`); `engine_installed`
is specifically the **managed** engine. `install_supported` is false on platforms with
no pinned release asset (the UI then falls back to manual-install guidance).
`running` is a live rerank **probe** of the port, not a pid check.

### `POST /v1/dashboard/reranker/setup/install-engine`

Streams the pinned llama.cpp release download + extract (`handleRerankerSetupInstallEngine`,
`web/reranker_setup_handler.go:83`).

### `POST /v1/dashboard/reranker/setup/download`

Streams the pinned GGUF model download (`handleRerankerSetupDownload`,
`web/reranker_setup_handler.go:125`). A download detached from a closed tab keeps
running; a reopened tab **attaches** to the live download and streams its progress
rather than failing the retry with "already in progress"
(`web/reranker_setup_handler.go:148-151`, `streamRerankerDownloadProgress` at :182).

**Streaming progress protocol (both endpoints):** `Content-Type: text/plain`, one
`key: value\n` line per event, flushed as it happens (`web/reranker_setup_handler.go:101`,
`:143`). The dashboard server's 15s write deadline is cleared for these minutes-long
streams (`:99`, `:141`).

| Line | Meaning |
|------|---------|
| `progress: <done> <total>` | Bytes so far and total, throttled to ~4 lines/sec (`:107-111`, `:159-163`). |
| `done: 0` | Success terminator (`:118`, `:176`). |
| `error: <message>` followed by `done: 1` | Failure (`:114-116`, `:172-174`). |

This is the same `text/plain` "key: value" shape the embeddings pull-model flow uses.
Both downloads are detached from the request context, so a dropped browser tab does
not abort a 600MB download at 95% (`:103`, `:153`).

### `POST /v1/dashboard/reranker/setup/start`

Spawns (or adopts) the sidecar, waits for it to come healthy, then enables + persists
the reranker in **llama.cpp dialect** so it survives restarts
(`handleRerankerSetupStart`, `web/reranker_setup_handler.go:210`). `Start` blocks up
to 150s while the sidecar loads the model, so the write deadline is pushed just past
that budget (`:224`).

**Response** (HTTP 200): `{"ok": true, "url": "http://127.0.0.1:8082"}`; HTTP 502 if
the sidecar does not come up.

On success it persists `reranker_enabled=1`, `reranker_url`, `reranker_model`,
`reranker_kind=llamacpp`, and `reranker_managed=1` (`:240-246`) so node boot
re-starts the managed sidecar.

### `POST /v1/dashboard/reranker/setup/stop`

Stops the sidecar and turns the reranker off (`handleRerankerSetupStop`,
`web/reranker_setup_handler.go:251`): `SetReranker(nil, 0)`, `Stop()`, and persists
`reranker_enabled=0` + `reranker_managed=0`.

**Response** (HTTP 200): `{"ok": true}`

---

## 5. Internals: the managed reranker sidecar (`internal/rerankd`)

Package `rerankd` manages SAGE's optional reranker sidecar: a llama.cpp `llama-server`
process serving `bge-reranker-v2-m3` on loopback (`internal/rerankd/rerankd.go:1-7`).
Ollama has no rerank endpoint (the upstream PR died unmerged), so SAGE bundles
llama.cpp "the same way as Ollama": detect the binary (guide/perform the install when
missing), download a pinned GGUF once, then spawn and manage the process. Everything
runs locally; nothing leaves the machine.

### Pinned, checksum-verified assets

- **Model:** `bge-reranker-v2-m3-Q8_0.gguf`, pinned URL + `sha256` +
  `ModelSizeBytes` = 635676416 (`internal/rerankd/rerankd.go:31-37`). The download
  hashes as it writes, aborts early if the stream runs past the pinned size, and
  refuses to install on a size or checksum mismatch, then does an atomic rename
  (`rerankd.go:190-234`).
- **Engine:** the official `ggml-org/llama.cpp` GitHub release, pinned to a single
  build tag `b9870` (`internal/rerankd/install.go:27`), with a per-platform
  `{name, sha256, size, format}` table (`install.go:38-45`, six GOOS/GOARCH targets).
  `InstallEngine` buffers the (~11-17MB) archive in memory while hashing so the
  `sha256` is verified **before any byte touches disk** (`install.go:145-176`), then
  extracts into a temp dir and atomically moves it into place.
- **Archive-bomb guard:** total decompressed output is capped at 512MB
  (`install.go:47-49`); the extractor flattens every file to its basename
  (sidestepping path traversal) and materializes only intra-archive symlinks whose
  target is a regular file from the same archive, needed for the dylib soname chain
  (`install.go:210-300`).
- **Rosetta correctness:** an amd64 SAGE binary translated by Rosetta on Apple Silicon
  fetches the **native arm64** engine (the child process is separate and the arm64
  build is the one with Metal), via `effectiveArch()`
  (`internal/rerankd/install_arch_darwin.go:14`).

### Adopt-not-respawn (the port is the source of truth)

Node restarts happen via `syscall.Exec`, which orphans (not kills) a spawned child.
So on boot SAGE **adopts** a healthy sidecar instead of double-spawning it: liveness
is a real `/v1/rerank` **probe** of the port (`Probe`, `rerankd.go:241-247`), and
`Start` returns the existing URL when the probe passes (`rerankd.go:257-259`). The
sidecar binds `127.0.0.1:8082` by default (`DefaultPort`, `rerankd.go:39-42`). `Stop`
kills our own child if we spawned it, otherwise it reads the pidfile from a previous
incarnation and **verifies the pid is still `llama-server`** before signaling, so a
recycled pid can't take out an unrelated process (`rerankd.go:367-393`,
`internal/rerankd/rerankd_unix.go:31-37`).

### Secret hygiene

The third-party binary is handed a sanitized environment: every `SAGE_*` variable is
stripped so the vault passphrase (`SAGE_PASSPHRASE`) and embedding key never reach a
separate, network-listening process where they'd surface in `/proc/<pid>/environ` or a
crash dump. Every platform-critical var (PATH, HOME, TMPDIR, ...) is preserved
(`rerankd.go:283-296`). The sidecar is launched with the documented cross-encoder trio
`--embedding --pooling rank --rerank` (`rerankd.go:272-279`).

### The llama.cpp rerank dialect (beside TEI)

The reranker HTTP client (`internal/embedding/reranker.go`) speaks two dialects,
selected by `kind`:

| Dialect | Constant | Path | Request | Response |
|---------|----------|------|---------|----------|
| TEI (default) | `RerankKindTEI` = `"tei"` | `POST /rerank` | `{query, texts}` | `[{index, score}]` |
| llama.cpp | `RerankKindLlamaCpp` = `"llamacpp"` | `POST /v1/rerank` | `{model, query, documents}` | `{results: [{index, relevance_score}]}` |

Constants at `internal/embedding/reranker.go:41-44`; the request/response structs at
`:89-111`; dialect branch at `:127-130` (request) and `:162-172` (response decode).
TEI is the default so operators can drop in HuggingFace text-embeddings-inference (or
any TEI-compatible server) with no SAGE-specific adapter; llama.cpp is the dialect the
**managed** sidecar uses, since Ollama has no rerank endpoint. `BuildReranker` returns
`nil` when disabled or URL-less, which the store treats as "skip the rerank pass"
(`reranker.go:248-256`).

---

## 6. Embedding provenance: `embedding_provider` stamped at insert (v11)

Each memory record carries an `embedding_provider` string recording **which embedder**
produced its vector - distinct from `provider`, which is the submitting agent's LLM
identity (`internal/memory/model.go:49-52`). Empty string means "none / unknown, i.e.
needs re-embed"; the dashboard's re-embed flow and the embedder-coverage counts key on
this column.

**Why it is stamped at insert (v11 fix):** the off-chain record is stamped at insert
time via `SupplementaryData.EmbeddingProvider` (`internal/memory/model.go:92-96`).
Without it, every new memory would land at `embedding_provider = ''` and the dashboard
would forever count it as "needs re-embed" even though its vector is already semantic
(commit `c568c7f`).

- **Where the stamp is computed:** `Server.embedderStampFor(emb)` returns the semantic
  embedder's `Named` id (e.g. `"ollama"`) when one is active and the submission carries
  a vector, and `""` otherwise - hash pseudo-vectors deliberately stay unstamped so
  they get picked up when the operator turns semantic search on
  (`api/rest/embed_handler.go:126-142`).
- **Where it is applied:** on the submit and co-commit paths
  (`api/rest/memory_handler.go:481`, `api/rest/cocommit_handler.go:201`).
- **Where it is persisted:** `SQLiteStore.InsertMemory` writes the
  `embedding_provider` column (`internal/store/sqlite.go:843-884`). The upsert uses
  `COALESCE(NULLIF(excluded.embedding_provider, ''), memories.embedding_provider)`
  (`sqlite.go:879`) so a re-insert that omits the stamp never clobbers an existing one.
  The column is added by an idempotent migration with a supporting index
  (`sqlite.go:661-668`).

The same column carries the re-embed lifecycle states the embeddings-setup flow uses:
`''` (needs re-embed), `'skipped'` (undecryptable under the current vault, hidden from
views), and `'error'` (`internal/store/sqlite.go:1016-1034`). The re-embed backfill
that clears them is node-local and off-consensus; for the on-chain lifecycle a record
moves through (submit -> proposed -> committed / deprecated) see
[`concepts/memory-lifecycle.md`](concepts/memory-lifecycle.md).

---

## Related environment variables

The managed flow persists its choices as preferences, but the reranker can also be
configured entirely via env vars (they set the startup default the dashboard then
overrides). See [`environment-variables.md`](environment-variables.md) - notably the
v11 `SAGE_RERANK_KIND` selector alongside `SAGE_RERANK_ENABLED` / `SAGE_RERANK_URL` /
`SAGE_RERANK_MODEL` / `SAGE_RERANK_TIMEOUT_MS` / `SAGE_RERANK_OVERSAMPLE`.
