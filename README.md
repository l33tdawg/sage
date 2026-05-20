# (S)AGE — Sovereign Agent Governed Experience

**Persistent, consensus-validated memory infrastructure for AI agents.**

SAGE gives AI agents institutional memory that persists across conversations, goes through BFT consensus validation, carries confidence scores, and decays naturally over time. Not a flat file. Not a vector DB bolted onto a chat app. Infrastructure — built on the same consensus primitives as distributed ledgers.

The architecture is described in [Paper 1: Agent Memory Infrastructure](papers/Paper1%20-%20Agent%20Memory%20Infrastructure%20-%20Byzantine-Resilient%20Institutional%20Memory%20for%20Multi-Agent%20Systems.pdf).

> **Just want to install it?** [Download here](https://l33tdawg.github.io/sage/) — double-click, done. Works with any AI.

<a href="https://glama.ai/mcp/servers/l33tdawg/s-age">
  <img width="380" height="200" src="https://glama.ai/mcp/servers/l33tdawg/s-age/badge" alt="(S)AGE MCP server" />
</a>

---

## Architecture

```
Agent (Claude, ChatGPT, DeepSeek, Gemini, etc.)
  │ MCP / REST
  ▼
sage-gui
  ├── ABCI App (validation, confidence, decay, Ed25519 sigs)
  ├── App Validators (sentinel, dedup, quality, consistency — BFT 3/4 quorum)
  ├── Governance Engine (on-chain validator proposals + voting)
  ├── CometBFT consensus (single-validator or multi-agent network)
  ├── SQLite + optional AES-256-GCM encryption
  ├── CEREBRUM Dashboard (SPA, real-time SSE)
  └── Network Agent Manager (add/remove agents, key rotation, LAN pairing)
```

Personal mode runs a real CometBFT node with 4 in-process application validators — every memory write goes through pre-validation, signed vote transactions, and BFT quorum before committing. Same consensus pipeline as multi-node deployments. Add more agents from the dashboard when you're ready.

Full deployment guide (multi-agent networks, RBAC, federation, monitoring): **[Architecture docs](docs/ARCHITECTURE.md)**

---

## CEREBRUM Dashboard

![CEREBRUM — Neural network memory visualization](docs/screen-brain.png)

`http://localhost:8080/ui/` — force-directed neural graph, domain filtering, semantic search, real-time updates via SSE.

### Network Management

![Network — Multi-agent management](docs/screen-network.png)

Add agents, configure domain-level read/write permissions, manage clearance levels, rotate keys, download bundles — all from the dashboard.

### Settings

| Overview | Security | Configuration | Update |
|:---:|:---:|:---:|:---:|
| ![Overview](docs/screen-overview.png) | ![Security](docs/screen-security.png) | ![Config](docs/screen-config.png) | ![Update](docs/screen-update.png) |
| Chain health, peers, system status | Synaptic Ledger encryption, export | Boot instructions, cleanup, tooltips | One-click updates from dashboard |

---

## What's New in v7.6

Direct-write hooks across Claude Code and Codex. Sessions boot SAGE without depending on the agent to remember `sage_inception` on its own — the local SAGE node is queried directly by lifecycle hooks, recent committed memories land as initial context, and a session-lifecycle observation is written on exit. One unified install path for both agents.

- **`sage-gui hook session-start | session-end`.** New Go subcommand bundled with sage-gui. Loads the agent's Ed25519 key with the same priority chain as the MCP server (SAGE_IDENTITY_PATH → SAGE_AGENT_KEY → per-project key → default), signs REST calls against the local SAGE node, and prints a recent-memories context block on stdout (which Claude Code / Codex inject into the agent's prompt). Soft-fails with non-zero exit so the shell wrapper falls back to a static nudge when the node is unreachable.
- **`sage-gui mcp install` now ships 5 direct-write scripts** (`sage-session-start.sh`, `sage-session-end.sh`, `sage-pre-compact.sh`, `sage-user-prompt.sh`, `sage-stop.sh`) instead of the legacy 2-script nudge set. The session-start/end scripts shell out to `sage-gui hook ...` via an absolute binary path substituted at install time — no `$PATH` lookup, no Python dependency, no pynacl. All scripts respect `~/.sage/memory_mode` (full / bookend / on-demand). The hook block uses `${CLAUDE_PROJECT_DIR}` so paths resolve correctly even when Claude Code starts from a subdir.
- **`sage-gui codex install` mirrors the install for Codex.** Writes `.codex/config.toml` (MCP server registration), `.codex/hooks.json` (hook lifecycle wiring with absolute paths because Codex doesn't expand env vars in hook commands), the same 5 hook scripts under `.codex/hooks/`, and an `AGENTS.md` boot reminder. The hook scripts are identical to the Claude side — same Go binary, same signed REST protocol, same memory_mode semantics. Two agents, one substrate.
- **Self-heal migrates older installs.** Every MCP server start runs `selfHealProject`, which now detects legacy 2-script installs (sage-boot.sh / sage-turn.sh) and rewrites them to the new 5-script set, refreshes scripts when the sage-gui binary moves to a new location (a stale `__SAGE_GUI_BIN__` path triggers re-templating), and repairs missing `hooks.json` on Codex installs. No user action required after upgrading.

## What's New in v7.5

Migration substrate. Hands-off in-place chain upgrades across consensus-rule changes — no chain reset, no operator commands, no accumulated memory lost. v7.5 itself ships zero consensus-rule changes; the entire release is the plumbing that makes every subsequent release safe to land on existing chains.

- **Scheduled snapshots.** Every committed block, a background scheduler (`internal/abci/snapshot_sched.go`) decides whether to fire `internal/snapshot.Take`. Default cadence: every 10,000 blocks AND every 6 hours. Snapshots land at `~/.sage/data/snapshots/<height>/` with the running binary bundled into `binary/sage-gui-<version>` so rollback has the previous executable available locally. Atomicity via staging-dir + atomic rename + `OK` sentinel. Retention: K=5 newest + one anchor per distinct binary version (powers downgrade).
- **Snapshot verify-by-restore.** `internal/snapshot.Verify` doesn't just check file hashes — it loads the BadgerDB backup into a tmpdir and recomputes `ComputeAppHash` against the manifest, runs SQLite `PRAGMA integrity_check` + `foreign_key_check`, and walks the CometBFT tarball. The check is "this snapshot is restorable" not "this snapshot's bytes parse."
- **Snapshot encryption inherits vault posture.** Synaptic Ledger unlocked at boot → snapshots are wrapped in a streaming Argon2id + AES-256-GCM envelope keyed off the vault passphrase. Plaintext otherwise. No new operator decision.
- **Upgrade tx types + chain-computed activation height.** Three new transaction types (`TxTypeUpgradePropose` / `TxTypeUpgradeCancel` / `TxTypeUpgradeRevert`). When a proposal lands, `ActivationHeight` is computed deterministically inside `FinalizeBlock` as `req.Height + max(payload.UpgradeDelayBlocks, 200)`. Every validator computes the same number from the same inputs — no multi-validator drift. Activation happens via `ResponseFinalizeBlock.ConsensusParamUpdates.Version.App`, CometBFT-native, applied at H+1 atomically across all replicas. At-most-one pending plan at a time enforced on-chain.
- **Auto-proposal watchdog.** `cmd/sage-gui/upgrade_watchdog.go` runs as a goroutine after CometBFT boots. When the running binary's embedded `upgradeTargetAppVersion` exceeds the chain's current app version, it signs an `UpgradePropose` with the node operator's agent key and broadcasts via CometBFT RPC. Terminates on success, on "already pending", or when the chain catches up. v7.5.0 ships with `upgradeTargetAppVersion = 1` (no upgrade) so the watchdog is a no-op until the first real version bump.
- **HALT sentinel on panic.** `cmd/sage-gui/halt_writer.go::haltOnPanic` runs in `runServe`'s deferred recover. Writes a JSON sentinel to `~/.sage/data/HALT` atomically (staging file + fsync + rename), then re-panics so the original stack still surfaces.
- **Supervised rollback.** `cmd/sage-launcher --supervise` parents the chain binary. On child exit it reads the HALT sentinel, scans `~/.sage/data/snapshots/` for the latest anchor whose `BinaryVersion != FailedVersion`, calls the real `snapshot.Restore` (Verify-gated), clears the sentinel, and `syscall.Exec`s the bundled rollback binary. PID continuity preserved for outer supervisors (launchd / systemd). Crash-loop circuit breaker stops thrash at 3 non-HALT crashes within a 60s sliding window. Opt-in `--supervise` flag so the existing detached-launcher flow (macOS `.app` / Windows `.exe` double-click) is untouched.
- **Test coverage:** unit tests for every component plus three integration tests — `TestV75_EndToEnd_ProposeEncodeFinalizeActivate` (full consensus path: build signed tx → encode → decode → dispatch → persist → activate → audit), `TestV75_MultiValidatorDrift` (4 independent SageApp replicas process the same encoded tx and assert byte-identical `ConsensusParamUpdates` at activation), `TestV75_E2E_PanicToRollback` (chain writes HALT → launcher reads HALT → real `snapshot.Restore` mutates data dir back to anchor → execer invoked with the bundled rollback binary). The integration tests caught two real bugs that single-component unit tests missed: a path mismatch between snapshot producer and launcher consumer, and a `time.Time` vs `int64` wire-format mismatch in the manifest schema. Both fixed in the same commit that landed the tests.

Full architecture: [docs/ARCHITECTURE.md#upgrade-machinery-v75](docs/ARCHITECTURE.md#upgrade-machinery-v75).

## What's New in v7.1

Recall quality polish and second-benchmark coverage. Optional cross-encoder reranking, optional query expansion, the LoCoMo retrieval benchmark, and a SAGE adapter shipped upstream to mem0's open-source evaluator so the comparison runs on their published methodology unchanged.

### v7.1.1 — Honest broadcast errors on RBAC/governance writes

- **All RBAC and governance REST handlers now wait for FinalizeBlock before returning.** `POST /v1/access/grant`, `/v1/access/request`, `/v1/access/revoke`, `/v1/domain/register`, `/v1/org/*`, `/v1/dept/*`, `/v1/vote/*`, and `PATCH /v1/agent/{id}` switched from CometBFT's `broadcast_tx_sync` (CheckTx-only) to `broadcast_tx_commit` (CheckTx + FinalizeBlock). Reported by levelup: an `access_grant` from an agent who wasn't the on-chain domain owner previously returned HTTP 201 `{status: "granted"}` while the tx was silently rejected at consensus, leaving callers to discover the ghost-tx by polling `GET /v1/access/grants/{agent_id}`. Now the consensus rejection surfaces as HTTP 403 with the real reason. Same bug class as the v6.6.9 permission-handler fix; this completes the migration across the remaining 20 call sites.
- **Domain-ownership recipe + known limitations documented.** [`docs/ARCHITECTURE.md#domain-ownership-first-write-wins`](docs/ARCHITECTURE.md#domain-ownership-first-write-wins) now spells out the cross-agent write sequence (probe owner → `register_domain` → `access_grant` → write) and the v7.1 limitations: subdomain grants don't cascade from ancestors yet, and lost-owner recovery is manual. The ancestor-walk and `domain_reassign` fixes are queued behind the v7.2 upgrade-machinery work so existing chains can upgrade without a reset.

- **Cross-encoder reranking on `/v1/memory/hybrid` (env-gated, opt-in).** Hybrid recall optionally fans out an oversampled candidate pool through a TEI-compatible HTTP reranker, then keeps the top-K by cross-encoder score. Off by default - turn on with `SAGE_RERANK_ENABLED=1 SAGE_RERANK_URL=<endpoint>`. Tunables: `SAGE_RERANK_MODEL` (defaults `BAAI/bge-reranker-v2-m3`), `SAGE_RERANK_TIMEOUT_MS=2000`, `SAGE_RERANK_OVERSAMPLE=2`. Falls back to RRF ordering if the reranker fails or times out. Native MPS sidecar shipped under `bench/rerank-server/` for Apple Silicon hosts where the HuggingFace TEI container has trouble under Rosetta.
- **Query expansion on `/v1/memory/hybrid` (server-side fanout).** The hybrid endpoint accepts an `expansions: [{query, embedding}]` array and merges results across the original query plus all variants via RRF (`k=60` across variants). Callers generate variants any way they like; the bench harness uses a small LLM call (`gpt-4o-mini`) for paraphrase / entity / temporal variants. Cost vs. recall is a caller-side decision.
- **LoCoMo benchmark, n=1986.** New `bench/locomo/` harness runs the ACL 2024 long-conversation retrieval benchmark (Maharana et al., 10 conversations / 272 sessions / 5882 turns / 1986 questions) end-to-end through the same Ed25519-signed REST path as LongMemEval. Headline retrieval: **R@5 = 0.6394, R@10 = 0.7324, MRR = 0.5790, Hit@5 = 0.6954**. Strong on single-hop (cat 4, n=841, R@5 = 0.765) and temporal (cat 2, R@5 = 0.769); aggregation-heavy cat 1 is metric-structurally lower because mean evidence is 3.13 turns. Methodology, per-category breakdown, and the `make bench-locomo-fetch` reproducer in [`bench/locomo/README.md`](bench/locomo/README.md).
- **LongMemEval-S re-run with the v7.1 stack.** **R@5 = 0.8927, R@10 = 0.9461, MRR = 0.8842 (n=499).** R@10 is up 1.4 pt vs. v7.0; R@5 is down 1.3 pt - the reranker pulls more relevant items into the candidate pool at the cost of some top-5 ordering precision. v7.0 stock numbers remain the right baseline; enable the v7.1 reranker if your downstream consumer benefits from a wider candidate pool.
- **SAGE adapter for mem0's open-source LoCoMo evaluator.** Submitted upstream. The PR adds `evaluation/src/sage/` to mem0's eval suite so SAGE is a first-class storage backend alongside RAG, langmem, openai, and zep. Scored via mem0's own `evals.py` with their `gpt-4o-mini` LLM-judge for direct apples-to-apples comparison: **LLM-judge = 0.7656** (n=1540, cat 5 excluded per mem0's methodology). Curated systems publish ~0.93 on this metric. The gap is full-pipeline tuning - SAGE preserves raw turns under BFT consensus instead of running an LLM curator over the chat history. Operators who want curation layer it on top; SAGE itself stays source-faithful.
- **Node-operator read-scope bypass for hooks.** SessionStart's prefetch path now short-circuits the `resolveVisibleAgents` filter when the caller is the local node operator key, so multi-agent SAGE nodes don't silently return empty recall lists to the hook. Docs in [`docs/HOOKS.md`](docs/HOOKS.md).
- **Python SDK 7.1.0.** New `client.hybrid(query, embedding, ...)` method on `SageClient` and `AsyncSageClient` for calling the hybrid recall endpoint directly. Replaces the previous SDK gap that forced raw `httpx` calls. `pip install sage-agent-sdk>=7.1.0`.

## What's New in v7.0

Recall quality and ambient capture. Hybrid retrieval, lifecycle hooks that actually write through consensus, branch-aware tagging, and the first SAGE benchmark on a public retrieval dataset.

- **Hybrid recall (BM25 + vector via RRF).** `sage_recall` now fuses FTS5/BM25 and vector cosine results in one round trip via weighted Reciprocal Rank Fusion (RRF) instead of picking one index based on vault state. New `POST /v1/memory/hybrid` endpoint backs it, with full RBAC, multi-org, classification, and decay parity vs. the existing query and search paths. Mixed-version networks keep working: older nodes that don't expose `/v1/memory/hybrid` get an automatic fall-back to the legacy FTS5 path. Defaults `RRFK=60`, `BM25=0.4`, `Vector=0.6`, `Oversample=2`; tune at runtime via `SAGE_HYBRID_RRF_K`, `SAGE_HYBRID_BM25_WEIGHT`, `SAGE_HYBRID_VECTOR_WEIGHT`, `SAGE_HYBRID_OVERSAMPLE`. Force the legacy single-index path with `SAGE_RECALL_HYBRID=0`.
- **LongMemEval-S benchmark, 0.9053 R@5 stock defaults.** New `bench/longmemeval/` harness runs the ICLR 2025 retrieval benchmark (Wu et al., 500 questions) against a fresh Docker SAGE through the full BFT consensus pipeline. Headline: **R@5 = 0.9053, R@10 = 0.9332, MRR = 0.9041**. `single-session-assistant` saturates at 0.98; `multi-session` lands at 0.91; full per-category breakdown and per-question detail in `bench/results/longmemeval-full-48e81ec.json`. Methodology, tunables, and reproducer in [`bench/longmemeval/README.md`](bench/longmemeval/README.md).
- **Direct-write lifecycle hooks for Claude Code.** Five hooks under `.claude/hooks/`: `SessionStart` and `SessionEnd` sign REST calls to the local SAGE node directly (no LLM in the loop) using `~/.sage/agent.key`; `PreCompact`, `UserPromptSubmit`, and `Stop` cover the events where direct-write would be too noisy or high-frequency. Install guide and the read-scope caveat for multi-agent nodes in [`docs/HOOKS.md`](docs/HOOKS.md).
- **Branch-aware memory tagging.** Memories written from a git working tree are auto-tagged with `branch:<name>` so `feature/x` and `main` stay separable without manual hygiene. Detection caches for 30 s with a 750 ms hard timeout so a wedged `git` can't stall a memory write. Outside a git repo: silent no-op. Opt out per process: `SAGE_BRANCH_TAG=0`. The tag uses the existing filter, so `sage_recall ... tags=["branch:feature/x"]` works like any other tag.
- **Test coverage.** 36 new tests across `internal/store`, `api/rest`, and `internal/mcp` covering RRF fusion ordering, weight bias, env overrides, hybrid fall-back paths, git-branch detection, tag plumbing, and RBAC parity for the new endpoint. `go test ./...` green on all 22 packages.

## What's New in v6.8

- **Admin-bootstrap escape hatch (6.8.5)** — closes a cross-agent-visibility failure mode where the SQL mirror has `role='admin'` but the chain doesn't (e.g. fire-and-forget register tx silently dropped during CometBFT cold-start). ABCI now auto-registers a signed admin set-permission / memory-reassign caller from SQL when the on-chain record is missing, then proceeds with the normal auth gate. REST pre-flight gets the same fallback so a 403 doesn't short-circuit recovery. Strict trigger (`Role == "admin"` exact match); SQL admin rows can only land via operator-authenticated paths.
- **Cross-agent visibility hotfix (6.8.4)** — two bugs let `network_agents.{org_id, dept_id, domain_access, visible_agents, clearance}` get blanked in the SQL mirror on re-register. Fixed by carrying all on-chain fields through the idempotent register path and adding REST-side PATCH semantics to `PUT /v1/agent/{id}/permission` that backfill missing fields from BadgerDB.
- **Windows wizard parity (6.8.1)** — the CEREBRUM ChatGPT setup wizard now installs `cloudflared` on Windows via `winget`. Step 1 + `/v1/wizard/chatgpt/check-cloudflared` return platform-aware install hints; users without `winget` get a clear manual-install pointer.
- **Hardening release (6.8.0)** — maintenance pass on the OAuth flow, the CEREBRUM ChatGPT wizard, and long-standing RBAC ergonomics. No new features; existing workflows unchanged.

<details>
<summary>Full v6.8.0 hardening details</summary>

- **OAuth DCR is now persistent.** `/oauth/register` writes `client_id` + `redirect_uris[]` to a new `oauth_clients` table; `/authorize` and `/token` validate the inbound `redirect_uri` belongs to the registered set. HTTPS-only, no userinfo, no fragment. Per-IP rate limit on `/register`.
- **`state` is mandatory on `/oauth/authorize`**, and the consent form ships with an HMAC-signed CSRF nonce. The misleading agent-picker dropdown is gone — bearers from the OAuth flow run as the local node identity, full stop.
- **CEREBRUM wizard endpoints are gated by strict same-origin** in addition to dashboard auth. `chatgpt.com`, `cursor.sh`, and `*.anthropic.com` dropped from the dashboard CORS allowlist; HTTP MCP transport keeps its own localhost-only CORS layer.
- **Wizard subprocess seams locked down.** `SAGE_CLOUDFLARED_BIN`, `SAGE_BROWSER_OPEN_BIN`, and `cloudflareAPIBase` overrides honoured only under `go test`. Login URL validated as `https://*.cloudflare.com` before browser open. Cert / launchd plist / systemd unit / `config.yml` written 0600. Cloudflare API responses bounded with `io.LimitReader`. Ingress regex covers RFC 9728 `/.well-known/oauth-protected-resource`.
- **`processAgentSetPermission` clamps writes by caller authority.** The v6.6.9 widening (self-set / global admin / org admin) is preserved; new clearance can no longer exceed caller's own ceiling; cross-org moves require admin of both source and destination.
- **REST broadcast errors are sanitised** before reaching clients. FinalizeBlock log strings stay server-side; the client gets canonical `access denied` / `not found` / `request rejected`. SSE sessions bound to the bearer that opened them.
- **Operator note.** v6.7.5 is yanked from Releases. Upgrade to v6.8.0+.

</details>

## What's New in v6.7

- **ChatGPT MCP connector via OAuth 2.0 + PKCE (6.7.2 → 6.7.5)** — full OAuth wrapper in front of the bearer-token transport so ChatGPT's MCP connector can authenticate against SAGE. RFC 8414 discovery (`/.well-known/oauth-authorization-server`), RFC 7591 Dynamic Client Registration (6.7.5), `/oauth/authorize` consent screen, `/oauth/token` code exchange with PKCE S256, RFC 9728 protected-resource metadata (6.7.5). Cursor / Cline / Claude Desktop still use the simpler bearer scheme.
- **CEREBRUM ChatGPT setup wizard (6.7.3)** — 6-click guided flow inside the Network tab replaces nine manual terminal commands (cloudflared install → tunnel login → DNS route → config.yml → autostart → bearer mint → ChatGPT connector form). Six admin-auth-gated endpoints under `/v1/wizard/chatgpt/`. Cloudflare zone dropdown added in 6.7.4 — no more hand-typing your domain.
- **HTTPS-capable HTTP MCP transport (6.7.0)** — SSE (`/v1/mcp/sse`) and Streamable-HTTP (`/v1/mcp/streamable`) endpoints on `:8443`. Same hand-rolled JSON-RPC dispatcher serves both stdio and HTTP transports. Bearer auth via `mcp_tokens` (32 random bytes, SHA-256 digest stored). CLI: `sage-gui mcp-token create/list/revoke`. `/v1/mcp/tokens` route-shadow hotfix in 6.7.1.

<details>
<summary>Full v6.7 deep-dive — ChatGPT setup walkthroughs, file lists, OAuth boundary notes</summary>

### v6.7.5 — DCR, discovery, consent UX

- Dynamic Client Registration (`POST /oauth/register`, RFC 7591) and Protected Resource Metadata (`GET /.well-known/oauth-protected-resource`, RFC 9728). Bearer middleware emits `WWW-Authenticate: Bearer realm="sage", resource_metadata=…` on 401s from `/v1/mcp/*`. Discovery doc advertises `registration_endpoint`.
- Consent screen lists active agents in a dropdown (admins first, default-selected). SPA honors `?next=` so the OAuth wizard lands on consent directly. Static assets under `/ui/*` ship with cache-busting + build-version query strings. Trace logging at `/oauth/token` for operator diagnostics.
- **Operator note:** Cloudflare Bot Fight Mode (free-tier) blocks server-to-server OAuth callbacks from OpenAI's backend with a Managed Challenge. Disable it for the SAGE host before connecting ChatGPT.

### v6.7.4 — Cloudflare zone dropdown in the wizard

`web/wizard_chatgpt.go:listCloudflareZones` decodes the base64-JSON token in `~/.cloudflared/cert.pem` and pages `GET /client/v4/zones?status=active` to enumerate active zones. Returned via `/v1/wizard/chatgpt/login-status` as `zones: [{id, name}]`. Frontend renders `<select>` when present; falls back to manual entry on API failure. Single-zone accounts auto-select.

### v6.7.3 — Wizard endpoints

Six admin-auth-gated endpoints under `/v1/wizard/chatgpt/`: `check-cloudflared`, `install-cloudflared`, `login`, `login-status`, `create-tunnel`, `mint-token`. Each streams progress as chunked `text/plain`. All subprocess inputs strictly validated before invocation (RFC-1035 subdomains, dotted-hostname zones, canonical UUID format). Existing-state protection: refuses to overwrite a different tunnel's `config.yml`; reuses a pre-existing `sage` tunnel from `cloudflared tunnel list`.

Files: `web/wizard_chatgpt.go`, `web/wizard_chatgpt_token.go`, `web/wizard_chatgpt_test.go`. Frontend Preact components `ChatGPTSetupWizard` and `CursorSetupPanel` in `web/static/js/app.js`. SDK Python bump to 6.7.3 (no API changes — wizard is a CEREBRUM feature).

### v6.7.2 — OAuth + PKCE wrapper

Three endpoints at host root (NOT under `/v1/mcp/`):

- `GET /.well-known/oauth-authorization-server` — RFC 8414. `response_types_supported: ["code"]`, `code_challenge_methods_supported: ["S256"]`, `token_endpoint_auth_methods_supported: ["none"]`. v6.7.2 did NOT advertise `registration_endpoint`; v6.7.5 added it.
- `GET/POST /oauth/authorize` — consent screen, gated by dashboard session cookie. Validates `client_id`, `redirect_uri`, `state`, `code_challenge` (S256 only), `response_type=code`. Mints an `mcp_tokens` row, binds it via `mcp_auth_codes`, 302s back with `?code=...&state=...`.
- `POST /oauth/token` — code-for-bearer exchange. Single-use enforced (`UPDATE...WHERE used_at IS NULL`). SHA-256(verifier) matched in constant time. Returns `{"access_token", "token_type": "Bearer", "expires_in": 0}` — 0 = no expiry; `mcp_tokens` revocation is the real lifetime gate.

Auth code TTL: 5 minutes. PKCE verifier check via `crypto/subtle.ConstantTimeCompare`. Files: `api/rest/oauth_handlers.go`, `internal/store/mcp_auth_codes.go`; tests cover discovery shape, missing-params 400s, unauthed redirect, full happy-path exchange, code reuse, bad verifier, redirect mismatch, expiry, purge.

#### ChatGPT setup (v6.7.2+)

1. ChatGPT → New App / Connector → MCP Server
2. **MCP Server URL:** `https://<host>:8443/v1/mcp/sse`
3. **Authentication:** OAuth → Advanced settings (Auth URL / Token URL auto-discovered from `/.well-known/oauth-authorization-server`)
4. **OAuth Client ID:** any string (suggest `chatgpt`). **Client Secret:** leave empty. **Token endpoint auth method:** `none`.
5. Click Create. ChatGPT redirects to `/oauth/authorize` → pick the agent identity → SAGE redirects back with code → ChatGPT exchanges → live.

### v6.7.1 — `/v1/mcp/tokens` route-shadow hotfix

v6.7.0 silently broke `POST/GET/DELETE /v1/mcp/tokens` because the SSE/streamable transport was mounted via `r.Route("/v1/mcp", …)` and chi resolved that as a catch-all subrouter at `/v1/mcp/*`, shadowing the token-management routes. Fix: flat `r.With(CORS, Bearer).Get/Post(...)` for transport endpoints. Bearer middleware still applies only to SSE/streamable; ed25519 admin auth still gates token management.

### v6.7.0 — HTTPS-capable HTTP MCP transport

Endpoints under `/v1/mcp`:

- `GET /v1/mcp/sse` — SSE transport (older MCP spec, ChatGPT's connector uses this). Pair with `POST /v1/mcp/messages?sessionId=…` for client → server requests.
- `POST /v1/mcp/streamable` — Streamable-HTTP single-endpoint transport.

TLS on `:8443` inherited from the chi router; plain `:8080` for local development. Bearer auth: 32 random bytes, base64url, SHA-256 digest stored. Admin-managed via ed25519-signed `/v1/mcp/tokens` REST. CLI parity: `sage-gui mcp-token create/list/revoke`. CORS reflects request `Origin`, allows `Authorization`, `Content-Type`, `Mcp-Session-Id`.

#### Bearer-only setup (Cursor / Cline / Claude Desktop)

1. `sage-gui mcp-token create --agent <id> --name <label>`
2. Configure MCP client with `Authorization: Bearer <token>` and `https://<host>:8443/v1/mcp/sse`.

Self-signed cert note: SAGE auto-generates its own CA at `~/.sage/certs/`. ChatGPT cannot reach `localhost` and rejects self-signed certs — use cloudflared/ngrok or a Let's-Encrypted reverse proxy.

Files: `internal/mcp/http_transport.go`, `api/rest/mcp_tokens_handler.go`, `api/rest/middleware/bearer.go`, `internal/store/mcp_tokens.go`. Stdio MCP path untouched.

</details>

## What's New in v6.6

- **Tags on propose + tag-filtered semantic recall (6.6.0)** — `POST /v1/memory/submit` now accepts `tags`, and `/v1/memory/query` and `/search` accept a `tags` filter (any-match / OR semantics). MCP `toolRemember` drops the old 2-step tag dance — single atomic submit. Python SDK: `propose(tags=...)` and `query(tags=...)`.
- **Offchain SQLITE_BUSY silent-drop fix (6.6.1)** — Under sustained SQLite lock contention, the offchain `Commit()` flush could exhaust its retry budget, log CRITICAL, and silently clear pending writes while BadgerDB had already advanced — CometBFT then skipped replay on restart and the writes were lost invisibly. Now: flush runs *before* BadgerDB state is saved, retry budget raised to 30 attempts, panic on exhaustion so CometBFT replays the block. First surfaced by Level Up: 521 accepted submits with zero new visible memories across 96 hours on 6.5.5.
- **Silent-filter observability (6.6.2)** — `/v1/memory/list`, `/query`, `/search` now set an `X-SAGE-Filter-Applied` header and a `filtered` JSON envelope whenever either silent-hide filter ran. `/list` includes `total_before_filter` + `visible` counts; `/query`/`/search` include `hidden_count`. Empty-domain vs RBAC-filtered is finally distinguishable.
- **Org-clearance-as-seeAll (6.6.2)** — TopSecret (clearance=4) org members bypass the `submitting_agents` RBAC filter automatically. Per-domain access control and per-record classification gates still apply. Closes the `visible_agents="*"` boilerplate for single-org deployments.
- **Admin bootstrap playbook (6.6.2)** — New `docs/ADMIN_BOOTSTRAP.md` documents three deployment patterns (single-org, multi-org federated, homogeneous-trust legacy) with setup commands and the chain-reset visibility gotcha.
- **ABCI healthcheck + chain-bootstrap-window doc (6.6.3)** — `deploy/Dockerfile.abci` ships a HEALTHCHECK with `start_period=5m` to cover the ~3min CometBFT cold-start window on fresh data dirs, so Docker doesn't false-flag containers as unhealthy during normal bootstrap. Doc adds the 503-vs-connection-refused diagnostic and orchestrator guidance for Kubernetes `startupProbe`.
- **Root-cause SQLite pragma fix, tx serialization, post-commit context (6.6.4)** — Three cascading bugs surfaced by concurrent propose-with-tags workloads: (1) the `_journal_mode=WAL` DSN form is silently dropped by `modernc.org/sqlite` — the DB has been running in rollback-journal mode with `busy_timeout=0` since the driver switch, which is the root cause behind the 6.6.1 symptom (now fixed at source with `_pragma=journal_mode(WAL)` and explicit follow-up PRAGMAs as belt-and-braces); (2) `SetTags` + 5 other store methods opened transactions with raw `s.db.BeginTx`, bypassing the writeMu that writeExecContext/RunInTx use to serialize writes — fixed via a new `beginTxLocked` helper; (3) post-commit `SetTags` and `UpdateAgentLastSeen` ran on `r.Context()`, so a client disconnect (SIGKILL, timeout) between `broadcastTxCommit` returning and the tag write left untagged orphan rows that broke tag-based idempotency — now run under a 10s background context. Also: `POST /v1/agent/register` first-time registration now surfaces `on_chain_height` (previously only returned on the idempotent `already_registered` path, which SDK callers read as a version-drift signal). First surfaced by RAPTOR's `libexec/raptor-sage-setup` — concurrent `asyncio.gather(Semaphore(8))` proposes produced 396 SQLITE_BUSY + 197 tag-write failures on 6.6.3, zero after the fix.
- **`sage_recall` no longer surfaces a cryptic FTS5 error on vault-encrypted nodes (6.6.10)** — On a node with the synaptic-ledger vault unlocked, memory content is AES-256-GCM encrypted at rest, so SQLite FTS5 cannot text-index it. `internal/store/sqlite.go:SearchByText` correctly bails with a hard error in that case — but `internal/mcp/tools.go:toolRecall` was routing to that very FTS5 path (`POST /v1/memory/search`) whenever `isSemanticMode(ctx)` returned false. `isSemanticMode` decides from `/v1/embed/info`'s `semantic` field, which `api/rest/embed_handler.go:handleEmbedInfo` derived purely from `embedder.Semantic()` — vault-state was nowhere in the decision. Result: any vault-active node without an Ollama embedder configured silently degraded every `sage_recall` call into the literal error string `"text search unavailable: content is encrypted — use semantic search with Ollama"` bubbled verbatim through MCP to the agent, which has no `sage_recall` knob to "switch to semantic" — they just called the tool and got an error they couldn't act on. Multiple agents on the SAGE network hit this for weeks before Dhillon caught it on a freshly-registered claude-code agent. Fix is two-layered: (1) `handleEmbedInfo` now type-asserts the store to `vaultStatusReporter` (new `SQLiteStore.VaultActive()` method) and forces `semantic=true` whenever the vault is active — even if no embedder is configured, so the embed call fails with a clearer downstream error rather than the cryptic FTS5 path; (2) belt-and-braces in `toolRecall`: if the FTS5 path's `doSignedJSON` returns an error containing the marker `"text search unavailable: content is vault-encrypted"`, the handler logs a warning, warms the `semanticMode` cache to `true`, and retries via the new shared `recallSemantic` helper — so older nodes that haven't been upgraded still recover gracefully. The marker constant lives at `internal/mcp/tools.go:vaultEncryptedSearchMarker` paired with `internal/store/sqlite.go:ErrTextSearchVaultEncryptedMsg` (the canonical wording — `"text search unavailable: content is vault-encrypted; this node is in semantic-only mode"` — replaces the old "use Ollama" message that was misleading because there's nothing the caller can do at the MCP layer). Tests: `internal/store/sqlite_test.go:TestSearchByText_VaultActiveErrors` (vault-on returns the encryption error) and `internal/mcp/tools_test.go:TestSageRecall_VaultActiveForcesSemantic` + `TestSageRecall_RetriesSemanticOnVaultEncryptedFTSError` (mock `/v1/embed/info` to honestly report semantic, mock to lie and return the FTS error and verify retry) plus `api/rest/embed_handler_test.go:TestHandleEmbedInfo_VaultActiveForcesSemantic`. Boundary: `internal/store/postgres.go` is untouched (its "text search not available" error is documented as expected behavior), and the FTS5-vs-encryption architectural constraint is unchanged — we fixed routing/UX, not the indexing reality.
- **Org name lookup endpoint + SDK routing (6.6.9)** — `client.get_org("levelup")` previously 404'd because the Python SDK passed the human-readable name straight into `GET /v1/org/{org_id}`, which the REST handler treated as an opaque orgID. There was no name→orgID resolver anywhere in the stack, and `processOrgRegister` doesn't enforce name uniqueness — orgID is `sha256(adminID + ":" + name + ":" + height)` so two admins (or the same admin at different heights) can both register an org named "levelup" and land in distinct orgID slots. Fix: a one-to-many `org_name:<name>:<orgID>` reverse index in BadgerDB, maintained from `RegisterOrg` and backfilled idempotently from existing `org:*` forward entries on every `NewBadgerStore` open (so in-place upgrades work without a chain reset). New `GET /v1/org/by-name/{name}` returns `{"orgs": [...]}` — empty result is HTTP 200 (a valid answer), not 404. Python SDK `get_org(identifier)` now sniffs the input: 32-char lowercase hex routes to `/v1/org/{id}` unchanged; anything else hits the by-name endpoint and returns the single match, raising `SageAPIError(404)` for zero matches and `ValueError` for >1 matches so callers can disambiguate via `list_orgs_by_name`. **Known quirk surfaced by the new endpoint:** the same human-readable name can map to multiple orgIDs — this was already true in v6.6.8 but the SDK assumption hid it; we left `processOrgRegister` alone (name-uniqueness enforcement is a behavior change worth a separate discussion). Reported by the Level Up team on v6.6.8 validation. Tests: `internal/store/badger_multiorg_test.go` (empty, single, multi-admin, legacy backfill, store-open auto-backfill) + `api/rest/org_byname_test.go`.
- **`PUT /v1/agent/{id}/permission` no longer silently no-ops for non-admin callers (6.6.9)** — A non-prod-admin caller setting `visible_agents="*"` (or any other field) on `PUT /v1/agent/{id}/permission` previously got HTTP 200 with a real `tx_hash` while the SQL row stayed untouched. Two cascading defects: (1) the REST handler used `broadcast_tx_sync`, which only inspects CheckTx (signature/nonce) — the FinalizeBlock rejection (`code=67 "not an admin"`) was never propagated to the client, so the API confirmed success for a write the chain had refused; (2) the ABCI handler `processAgentSetPermission` hard-gated on the on-chain global `Role=="admin"`, meaning ONLY the original deployment-admin identity could ever land a permission write — so the most common deployment pattern (an agent declaring its own RBAC surface, or an org admin configuring a member) silently returned 200 + empty SQL. Reported by the Level Up team while validating v6.6.8. Fix: REST does a fail-fast pre-flight RBAC check using BadgerDB (`callerCanSetPermission`) and switches to `broadcast_tx_commit` so any consensus-side rejection still surfaces (`broadcastErrorStatus` maps "access denied" to HTTP 403); ABCI auth model widens to *self-set* OR *global admin* OR *org admin of any org the target also belongs to* (using the `agent_orgs` index from v6.6.8 + `GetMemberClearance` for role lookup). The auth model lives in code comments at both `api/rest/agent_handler.go:handleAgentSetPermission` and `internal/abci/app.go:processAgentSetPermission` so the two layers can't drift. Regression tests in `api/rest/permission_handler_test.go` cover self-set, org-admin, global-admin, unauthorized-403 (no broadcast, no tx_hash), and FinalizeBlock-rejection-surfaces-as-403; `internal/abci/app_test.go` adds matching ABCI-level coverage.
- **Multi-org membership no longer silently strips access (6.6.8)** — `BadgerStore.AddOrgMember` previously maintained an `agent_org:<agentID>` *single-slot* reverse lookup that every new add overwrote. The forward `org_member:` entries (clearance, role, height) for prior orgs survived, but `HasAccessMultiOrg` and `agentHasTopSecretClearance` only consulted the single slot — so the moment a pipeline agent was added to a second org, queries scoped against the first org's domains returned HTTP 200 with zero memories despite the agent still being a `clearance=4` member there. Reported by the Level Up agent (4 pipeline agents added to a new tenant org disappeared from prod-org recall). Fix: a one-to-many `agent_orgs:<agentID>:<orgID>` reverse index, additive on every add and surgically removed on `RemoveOrgMember` (legacy single slot rebound deterministically to the lexically smallest remaining org so federation governance auto-pickers don't break). `HasAccessMultiOrg` now iterates every org the agent belongs to and every org the domain owner belongs to, granting same-org clearance against any matching pair and falling back to a federation check across the cartesian product. `agentHasTopSecretClearance` returns true if TS in *any* org. Federation `propose`/`approve`/`revoke` ABCI handlers verify membership of the *specified* org (`IsAgentInOrg`) instead of comparing against the legacy primary slot, so multi-org admins can act on either side of a federation. `NewBadgerStore` runs an idempotent backfill from the authoritative `org_member:` forward index, so in-place upgrades from pre-v6.6.8 schemas work without a chain reset. Regression tests in `internal/store/badger_multiorg_test.go`.
- **Encrypted CA private key in quorum manifest (6.6.6/6.6.7)** — `sage-gui quorum-init` previously embedded the quorum CA private key as plaintext PEM inside `quorum-manifest.json`. Anyone who got the file (misdelivered email, Slack drop, shared backup) had the CA forever and could mint valid TLS certs for the quorum. Now: the CA key is wrapped with an Argon2id + AES-256-GCM envelope (`internal/tlsca/manifest_crypt.go`) keyed by an operator passphrase set via `SAGE_QUORUM_PASSPHRASE` env var or interactive prompt. Share the passphrase OUT-OF-BAND (different channel from the manifest file). `quorum-join` prompts for it on import; tampered envelopes (flipped salt/nonce/ciphertext bytes) fail closed via authenticated encryption. Pre-encryption manifests with plaintext `ca_key` are rejected outright with a regen prompt. v6.6.7 = v6.6.6 + golangci-lint shadow fixes that blocked the v6.6.6 release workflow.

<details>
<summary>Full v6.6.x changelog</summary>

- v6.6.10: `sage_recall` UX fix on vault-encrypted nodes — `/v1/embed/info` forces `semantic=true` when the store reports `VaultActive()`; MCP `toolRecall` adds a belt-and-braces retry that re-runs the semantic path when the FTS5 path returns the vault-encrypted marker; cleaner SQLite error wording.
- v6.6.9: Org name lookup (`GET /v1/org/by-name/{name}` + SDK hex-vs-name routing) AND `PUT /v1/agent/{id}/permission` silent-failure fix (REST pre-flight RBAC + `broadcast_tx_commit`; ABCI auth widened to self-set / global-admin / org-admin)
- v6.6.8: Multi-org membership fix — `agent_orgs` one-to-many reverse index, `HasAccessMultiOrg` iterates, federation handlers gated by `IsAgentInOrg`
- v6.6.7: Encrypted CA key in quorum manifest (lint-fix re-cut of v6.6.6)
- v6.6.6: Encrypted CA key in quorum manifest (release blocked by lint; superseded by v6.6.7)
- v6.6.5: Python SDK version alignment (PyPI publish repair for v6.6.4)
- v6.6.4: SQLite pragma root-cause fix + writeMu-guarded BeginTx + post-commit background context + first-register on_chain_height
- v6.6.3: ABCI HEALTHCHECK + chain-bootstrap-window doc
- v6.6.2: Silent-filter observability + org-clearance-as-seeAll + admin bootstrap docs
- v6.6.1: Offchain SQLITE_BUSY silent-drop fix (correctness; flush-before-badger reorder)
- v6.6.0: Tags on propose/query + `/v1/agent/register` response field rename to `on_chain_height`

</details>

### v6.5 Highlights

- **Encrypted Node-to-Node Communication (6.5.0)** — REST API TLS support for quorum mode. Per-quorum ECDSA P-256 certificate authority, auto-generated during `quorum-init`/`quorum-join`. Dual-listener pattern: TLS on `:8443` for network traffic, plain HTTP on `localhost:8080` for dashboard/MCP.
- **CometBFT P2P Already Encrypted (6.5.0)** — Verified that CometBFT v0.38.15 encrypts all validator-to-validator gossip via SecretConnection (X25519 DH + ChaCha20-Poly1305). No plaintext memories on the wire.
- **TLS Certificate Infrastructure (6.5.0)** — New `internal/tlsca/` package: CA generation, node cert generation, PEM I/O, TLS config builders. `sage-gui cert-status` CLI for expiry monitoring. Python SDK v6.1.0 adds `ca_cert` parameter.
- **Stuck-proposed deprecation + vote dedup (6.5.1)** — When all validators voted but quorum (2/3) wasn't reached (e.g. 2-2 tie), memories stayed in `proposed` forever and the validator ticker re-voted every 2 seconds (~1.4M redundant txs over 8 days for one stuck memory). Now: deprecate the memory when votes are in but quorum is missed, and track per-session voted memories to prevent re-vote.
- **`/v1/memory/{id}/forget` + SDK `forget()` (6.5.4)** — Closes a semantic gap where "forget" was the user-facing verb across MCP/dashboard/docs but only `/challenge` existed. New endpoint is a thin alias for challenge with an optional reason (defaults to "deprecated by user" — `challenge` requires a non-empty reason, `forget` is forgiving for dedup callers).
- **RBAC ownership theft fix + real broadcast errors (6.5.5)** — Two bugs masqueraded as generic "Failed to broadcast" errors when CometBFT was fine and FinalizeBlock was returning "access denied". Fix: reserve `general` and `self` as shared catch-all domains (never auto-registered), make `RegisterDomain` check-and-set instead of silent overwrite, add `TransferDomain` for explicit admin transfers, and surface the real broadcast error from REST handlers (403 on access-denied instead of generic 500).

<details>
<summary>Full v6.5.x changelog</summary>

- v6.5.5: RBAC ownership theft fix; real broadcast error surfacing
- v6.5.4: `/v1/memory/{id}/forget` endpoint + SDK `forget()` method
- v6.5.3: RBAC regression test backfill for Level Up bug reports
- v6.5.2: CI workaround for GitHub Pages duplicate-artifact errors (reverted in 6.5.3)
- v6.5.1: Deprecate stuck proposed memories when quorum cannot be reached; per-session vote dedup
- v6.5.0: TLS everywhere — encrypted REST API for quorum mode, per-quorum CA

</details>

### v6.0 Highlights

- **Dynamic Validator Governance** — Validators can now be added, removed, and have their power updated **without stopping the chain**. Admin agents submit governance proposals, validators vote on-chain with 2/3 BFT quorum, and CometBFT applies validator set changes at consensus level. Zero downtime.
- **On-Chain Governance Engine** — New `internal/governance/` package with deterministic integer-only quorum math, proposal lifecycle (voting → executed/rejected/expired/cancelled), proposer cooldown, min voting period, and power constraints. All state in BadgerDB, included in AppHash.
- **Governance Dashboard** — New Governance section in the CEREBRUM Network page. Active proposal cards with vote tally, quorum progress bar, expiry countdown, and one-click voting. Proposal history with status badges. "New Proposal" wizard for admins.
- **Security Constraints** — 1/3 max power for new validators (prevents single-add takeover), min 2 validators after removal, 50-block proposer cooldown (prevents grief), 500-block max proposal TTL (prevents governance lockup), admin-only proposals, validator-only voting.

### v5.x Highlights

- **FTS5 Full-Text Search** — Keyword-based recall fallback when embeddings aren’t semantic.
- **Docker Compose** — `docker-compose.sage-gui.yml` with Ollama sidecar for semantic embeddings.
- **Consensus-First Writes** — Memory submissions go through full BFT consensus before appearing in queries.
- **Byzantine Fault Tests in CI** — 4-validator Docker cluster with fault injection.
- **Nonce Replay Protection** — Random nonce in request signing prevents sub-second replay collisions.
- **Docker Env Vars** — `OLLAMA_URL` and `OLLAMA_MODEL` properly configure embeddings in Docker.

<details>
<summary>Full v5.x changelog</summary>

- v5.4.5: Docker env var support for OLLAMA_URL/OLLAMA_MODEL
- v5.4.4: Empty blocks fix for single-node idle timeout prevention
- v5.4.3: Null array fix (return `[]` not `null` for empty results)
- v5.4.2: Nonce verification threaded through full tx pipeline
- v5.4.1: Random nonce for replay protection
- v5.4.0: FTS5 search, Docker Compose with Ollama
- v5.3.x: Consensus-first writes, Byzantine CI tests, Docker hardening, write serialization
- v5.2.x: Immutable RegisteredName, self-updater fix, memory type guidance
- v5.1.0: Agent rename fix, self-healing name reconciliation
- v5.0.x: Agent pipeline, Python SDK, vault recovery, memory modes, MCP identity fix, Docker fix

</details>

### v4.x Highlights

- **4 Application Validators** — Sentinel, Dedup, Quality, Consistency with 3/4 BFT quorum.
- **RBAC** — Agent isolation by default, domain-level permissions, clearance levels, multi-org federation. Domains are first-write-wins owned (auto-registered to the first submitter); cross-agent writes need an explicit `POST /v1/domain/register` + `POST /v1/access/grant` from the real owner per concrete subdomain. `general`, `self`, `meta`, and `sage-*` are reserved shared namespaces. See [Domain Ownership](docs/ARCHITECTURE.md#domain-ownership-first-write-wins) for the full recipe and known limitations.
- **Synaptic Ledger** — AES-256-GCM encryption with Argon2id key derivation, vault lock/unlock.

### v3.x Highlights

- **Multi-Agent Networks** — Add agents from dashboard, LAN pairing, key rotation, redeployment orchestrator.
- **On-Chain Agent Identity** — Registration, permissions, and metadata through CometBFT consensus.
- **CEREBRUM Dashboard** — Brain graph, focus mode, timeline, search, draggable panels.

---

## Research

| Paper | Key Result |
|-------|------------|
| [Agent Memory Infrastructure](papers/Paper1%20-%20Agent%20Memory%20Infrastructure%20-%20Byzantine-Resilient%20Institutional%20Memory%20for%20Multi-Agent%20Systems.pdf) | BFT consensus architecture for agent memory |
| [Consensus-Validated Memory](papers/Paper2%20-%20Consensus-Validated%20Memory%20Improves%20Agent%20Performance%20on%20Complex%20Tasks.pdf) | 50-vs-50 study: memory agents outperform memoryless |
| [Institutional Memory](papers/Paper3%20-%20Institutional%20Memory%20as%20Organizational%20Knowledge%20-%20AI%20Agents%20That%20Learn%20Their%20Jobs%20from%20Experience%20Not%20Instructions.pdf) | Agents learn from experience, not instructions |
| [Longitudinal Learning](papers/Paper4%20-%20Longitudinal%20Learning%20in%20Governed%20Multi-Agent%20Systems%20-%20How%20Institutional%20Memory%20Improves%20Agent%20Performance%20Over%20Time.pdf) | Cumulative learning: rho=0.716 with memory vs 0.040 without |

---

## Quick Start

```bash
git clone https://github.com/l33tdawg/sage.git && cd sage
go build -o sage-gui ./cmd/sage-gui/
./sage-gui setup    # Pick your AI, get MCP config
./sage-gui serve    # SAGE + Dashboard on :8080
```

Or grab a binary: [macOS DMG](https://github.com/l33tdawg/sage/releases/latest) (signed & notarized) | [Windows EXE](https://github.com/l33tdawg/sage/releases/latest) | [Linux tar.gz](https://github.com/l33tdawg/sage/releases/latest)

### Docker

```bash
docker pull ghcr.io/l33tdawg/sage:latest
docker run -p 8080:8080 -v ~/.sage:/root/.sage ghcr.io/l33tdawg/sage:latest
```

Pin a specific version with `ghcr.io/l33tdawg/sage:6.0.0`.

### Upgrading from an older version?

If you installed SAGE before v5.0 and your AI isn't doing turn-by-turn memory updates, re-run the installer in your project directory:

```bash
cd /path/to/your/project
sage-gui mcp install
```

This installs Claude Code hooks that enforce the memory lifecycle (boot, turn, reflect) — even if your `.mcp.json` is already configured. Restart your Claude Code session after running this.

---

## Documentation

| Doc | What's in it |
|-----|-------------|
| [Architecture & Deployment](docs/ARCHITECTURE.md) | Multi-agent networks, BFT, RBAC, federation, API reference |
| [Getting Started](docs/GETTING_STARTED.md) | Setup walkthrough, embedding providers, multi-agent network guide |
| [Security FAQ](SECURITY_FAQ.md) | Threat model, encryption, auth, signature scheme |
| [Connect Your AI](https://l33tdawg.github.io/sage/connect.html) | Interactive setup wizard for any provider |

---

## Stack

Go / CometBFT v0.38 / chi / SQLite / Ed25519 + AES-256-GCM + Argon2id / MCP

---

## License

Code: [Apache 2.0](LICENSE) | Papers: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

## Author

Dhillon Andrew Kannabhiran ([@l33tdawg](https://github.com/l33tdawg))

---

<p align="center"><em>A tribute to <a href="http://phenoelit.darklab.org/fx.html">Felix 'FX' Lindner</a> — who showed us <b>how much further curiosity can go.</b></em></p>
