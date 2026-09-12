<!-- Reconciled through SAGE v11.19.20. Every variable below was located at the cited file:line via `os.Getenv` or the local env helper. When the code changes, re-verify and bump this header. -->

# SAGE Reference — Environment Variables

**Authoritative, code-verified list of every environment variable SAGE reads.**
Each entry cites the `file:line` where it is consumed and the default applied when it is unset.

Most variables are optional — SAGE runs with sane defaults. The one you almost always
care about is [`SAGE_HOME`](#core-paths--identity). Variables are read by different binaries
(`sage-gui`, the MCP server, the REST API, and the `amid` indexer); the **Read by** column
notes which.

> There is **no `SAGE_ROOT_DIR`**. The data-directory variable is `SAGE_HOME`.

---

## Core: paths & identity

| Variable | What it does | Default | Read by | Source |
|----------|--------------|---------|---------|--------|
| `SAGE_HOME` | Data directory — holds `config.yaml`, `agent.key`, `certs/`, `data/`, the `memory_mode` flag, etc. Tilde (`~`) is expanded. | `~/.sage` | all | `cmd/sage-gui/config.go:98`, `cmd/sage-gui/mcp.go:38` |
| `SAGE_API_URL` | REST base URL that the CLI / MCP server / hooks talk to. Literal IPv4 loopback avoids an IPv6 `localhost` resolution when the personal listener is IPv4-only. | `http://127.0.0.1:8080`, or `https://127.0.0.1:8443` when `$SAGE_HOME/certs/` exists | sage-gui, sage-cli, MCP | `internal/mcp/server.go`, `cmd/sage-gui/mcp.go`, `cmd/sage-cli/main.go` |
| `SAGE_IDENTITY_PATH` | Explicit identity-key path. Takes precedence over `SAGE_AGENT_KEY`. Project-local installs pin their provider-specific repository key explicitly. Shared user-level clients use workspace mode: a verified Git common root is the identity boundary, so linked/Claude-managed worktrees inherit the primary repository's same-provider `.mcp.json` signer instead of minting scratchpad agents. An explicit worktree-local path remains the opt-in isolation mechanism. | (canonical repository derivation) | sage-gui, MCP | `cmd/sage-gui/mcp.go`, `cmd/sage-gui/hook.go:219`, `cmd/sage-gui/connect.go` (`mcpIdentityPath`) |
| `SAGE_IDENTITY_MODE` | Shared MCP identity selection. `workspace` ignores inherited shell key exports and resolves through the canonical repository workspace; `pinned` honors the explicit configured key. Written by generated Claude Code/Codex config rather than intended as a general user setting. | `pinned` unless configured | sage-gui MCP | `cmd/sage-gui/mcp.go`, `cmd/sage-gui/codex.go` |
| `SAGE_AGENT_KEY` | Explicit agent-key path; overrides implicit repository derivation when `SAGE_IDENTITY_PATH` is unset. An unset MCP identity never falls back to the stable node transport/CEREBRUM Root key: workspace clients derive a provider-separated canonical repository key and app-scoped clients derive `agents/global-<provider>/agent.key`. Claude Code and Codex remain separate signers in the same repository. | (repository or provider-global ordinary-agent derivation) | sage-gui, MCP | `cmd/sage-gui/mcp.go`, `cmd/sage-gui/connect.go` |

### First-party vendored companion bootstrap

These variables are for an application such as Mynah / SAGE Voice Bridge that
creates and owns a fresh personal SAGE node. Ordinary self-registering agents
must not set them. Supplying any one makes the complete contract mandatory;
missing or invalid fields fail startup rather than silently creating a mute or
partially privileged companion.

| Variable | What it does | Default | Read by | Source |
|----------|--------------|---------|---------|--------|
| `SAGE_VENDORED_AGENT_KEY_FILE` | Stable, application-owned Ed25519 key path for a first-party companion that creates a fresh node. Genesis creates/loads it, dual-signs the chain-bound bootstrap with Root and companion, and enrolls that distinct identity directly at app-v23. It is not an upgraded-node repair selector; existing third-party or mask-30 agents require reviewed onboarding. | (unset; generic enrollment) | sage-gui | `cmd/sage-gui/config.go` (`applyEnvOverrides`), `cmd/sage-gui/node.go` (`genesisAppStateForVendoredAgent`) |
| `SAGE_VENDORED_AGENT_HOME_DOMAIN` | Non-shared domain the companion owns after fresh-genesis bootstrap, for example `voice-interface`. This is ownership, not a synthetic level-2 grant. Readiness remains fail-closed until app-v24 is active for the next admitted transaction. | (required with vendored key) | sage-gui | `cmd/sage-gui/config.go`, `cmd/sage-gui/node.go`, `cmd/sage-gui/appv23_vendored_readiness.go`, `cmd/sage-gui/vendored_appv24_readiness.go` |
| `SAGE_VENDORED_AGENT_CLEARANCE` | Maximum classification the companion may read, integer `0..4`. The app-v23 Companion profile remains fixed at role Member and capability mask `15`; this variable cannot promote it to Manager/Admin or weaken the profile. | `1` when another vendored field creates the config | sage-gui | `cmd/sage-gui/config.go`, `cmd/sage-gui/node.go` |

---

## Server & networking

| Variable | What it does | Default | Read by | Source |
|----------|--------------|---------|---------|--------|
| `REST_ADDR` | REST API listen address; overrides `config.yaml`. | `127.0.0.1:8080` | sage-gui | `cmd/sage-gui/config.go:156` |
| `SAGE_TLS_ADDR` | HTTPS/MCP REST listen address; overrides `quorum.tls_addr` even in personal mode. MCP and lifecycle-hook URL fallback honors the same port (wildcard bind hosts become `localhost`). Set this, `REST_ADDR`, both Comet addresses, and the federation listener to distinct ports when running another node on the same host. | `127.0.0.1:8443` (personal), `0.0.0.0:8443` (quorum) | sage-gui, MCP, hooks | `cmd/sage-gui/config.go` (`applyEnvOverrides`), `cmd/sage-gui/node.go` (TLS listener), `internal/mcp/server.go` (`defaultBaseURL`) |
| `SAGE_CMT_RPC_ADDR` | CometBFT RPC listen address (also the tx-broadcast client target, the web health panel, `sage-cli status`, and the `sage-gui upgrade` RPC default). Move it to run a second node on one host. | `tcp://127.0.0.1:26657` | sage-gui, sage-cli | `cmd/sage-gui/node.go:842` |
| `SAGE_CMT_P2P_ADDR` | CometBFT P2P listen address. Personal mode defaults to loopback; quorum mode defaults to `tcp://0.0.0.0:26656` when unset. Generated agent bundles leave `p2p_addr` blank so this env override applies. | `tcp://127.0.0.1:26656` | sage-gui | `cmd/sage-gui/node.go:860` |
| `SAGE_ALLOWED_CEREBRUM_HOSTS` | Comma-separated extra hostnames the CEREBRUM loopback boundary accepts in `Host` and forwarded-host headers, in addition to `localhost` and loopback IPs. Lets a loopback TLS-terminating reverse proxy pass the original Host through instead of rewriting it to `localhost`. Exact hostnames only (ports stripped, no wildcards). The connected peer and every `X-Forwarded-For` hop must still be loopback, and unconfigured hostnames still return 404. When the proxy supplies `X-Forwarded-Proto`, every field-line and comma-joined hop must be `http` or `https` and all values must agree. | (unset — loopback only) | sage-gui (CEREBRUM dashboard) | `web/trusted_hosts.go` |
| `CORS_ALLOWED_ORIGINS` | Comma-separated allowlist of origins for REST CORS. | `*` | REST | `api/rest/server.go:258-262` |
| `SAGE_COMET_RPC` | CometBFT RPC endpoint for `sage-gui upgrade`; takes precedence over `SAGE_CMT_RPC_ADDR` for that command. | (built-in) | sage-gui | `cmd/sage-gui/upgrade.go:44` |
| `SAGE_TX_COMMIT_TIMEOUT_MS` | Timeout (ms) for `broadcast_tx_commit`. Raise it for unusually slow consensus; JOIN final-confirm peer and listener budgets derive from the same value. | `60000` (60s) | REST, federation | `api/rest/memory_handler.go`, `internal/federation/broadcast.go`, `cmd/sage-gui/node.go` |
| `SAGE_NO_BROWSER` | If set to any non-empty value, don't auto-open a browser when the node starts. | (unset → opens browser) | sage-gui | `cmd/sage-gui/node.go:724` |
| `SAGE_FED_RECALL_TIMEOUT_MS` | Timeout (ms) for federated recall fanout. | `4000` (4s) | REST | `api/rest/memory_handler.go:786-791` |
| `SAGE_FED_RECEIPT_TIMEOUT_MS` | Timeout (ms) for federation receipt fetches. | `20000` (20s) | federation | `internal/federation/client.go:21-28` |
| `SAGE_UI_DIR` | Filesystem directory for serving web UI assets instead of the embedded bundle. | embedded assets | web | `web/handler.go:522-526` |
| `SAGE_GRAPH_MAX_NODES` | Maximum graph nodes returned by the web graph endpoint. | `2500` | web | `web/handler.go` (`graphMaxNodes`) |

---

## Memory auto-voter (v11.1.0)

The per-node voter turns submitted memories from `proposed` into `committed`. See
[`concepts/voter-operations.md`](concepts/voter-operations.md). Env values accept
`true/false/yes/no/on/off`; an unrecognized value warns rather than silently
disarming the flag. `sage-gui` also reads these from a `voter:` block in `config.yaml`
(env wins).

| Variable | What it does | Default | Read by | Source |
|----------|--------------|---------|---------|--------|
| `SAGE_VOTER_ENABLED` | Whether the auto-voter runs. `false` is an explicit "no auto-voter" choice — memories stay `proposed` until another validator votes them through. | `true` | sage-gui | `cmd/sage-gui/config.go` (`applyEnvOverrides`) |
| `SAGE_VOTER_POLL_INTERVAL` | Voter poll cadence (Go duration, e.g. `2s`). Unset/invalid falls back to `2s`. | `2s` | sage-gui | `cmd/sage-gui/config.go` |
| `SAGE_VOTER_REQUIRED` | If `true`, the node **refuses to boot** when no usable consensus key is available, instead of serving voterless. Guards a deployment that needs guaranteed auto-commit. | `false` | sage-gui | `cmd/sage-gui/config.go` |
| `VOTER_REQUIRED` | `amid` equivalent of `SAGE_VOTER_REQUIRED` — sets the default for the `--require-voter` flag (fatal-exit when the validator key is missing/unreadable). | `false` | amid | `cmd/amid/main.go` |
| `VALIDATOR_KEY_FILE` | `amid` socket mode: concrete `priv_validator_key.json` for both the auto-voter and REST governance gateway (in-process mode injects the key under `--home`). Without a usable live key, REST governance fails closed with 503; the random compatibility key is never accepted for governance. | (none) | amid, REST | `cmd/amid/main.go`, `api/rest/server.go` |
| `SAGE_GOVERNANCE_OPERATOR_ID` | `amid` governance gateway allowlist: one hex Ed25519 identity permitted to authorize this validator's REST propose/vote/cancel calls. Equivalent flag: `--governance-operator-id`. Empty disables governance mutations. `sage-gui` wires its local operator identity without this env variable. | (none) | amid | `cmd/amid/main.go`, `api/rest/server.go` |

### External Comet settings for app-v20

These are CometBFT `config.toml` settings, not SAGE environment variables. Before
the tagged app-v20 ceremony, every validator must run the exact v11.9 binary. A
split `amid`/external-Comet deployment must also set
`[mempool] max_tx_bytes = 1048576`; `recheck = true` is the supported release
profile. Restart external Comet after changing either setting so no entry admitted
under an older, larger bound remains in memory. SAGE deliberately drains bounded
stale entries through deterministic FinalizeBlock Code 111, so liveness does not
depend on recheck alone. Standard Comet RPC does not expose enough effective
configuration to verify this remotely; operators must verify each local file and
restart. `deploy/init-testnet.sh` and the v11.9 fault gate generate and validate
this exact profile, including an empty WAL directory for the crash oracle.

---

## Vault & secrets

| Variable | What it does | Default | Read by | Source |
|----------|--------------|---------|---------|--------|
| `SAGE_PASSPHRASE` | Vault passphrase for the encrypted store. If empty, the node prompts on a TTY. A provably single-validator personal node may start in isolated local-unlock mode; quorum, state-sync, or multi-validator nodes fail before CometBFT starts until the vault is unlocked. | (prompt) | sage-gui | `cmd/sage-gui/node.go` |
| `SAGE_QUORUM_PASSPHRASE` | Passphrase supplied non-interactively to quorum init/join. | (prompt) | sage-gui | `cmd/sage-gui/quorum.go:501` |

---

## Embeddings

These override `config.yaml`'s embedding block. Provider values: `hash` (built-in, non-semantic),
`ollama`, or `openai-compatible` (OpenAI / vLLM / LiteLLM / TEI).

| Variable | What it does | Default | Source |
|----------|--------------|---------|--------|
| `SAGE_EMBEDDING_PROVIDER` | Embedding backend. | `hash` | `cmd/sage-gui/config.go:159` |
| `SAGE_EMBEDDING_BASE_URL` | Embedding endpoint base URL. | (provider-specific) | `cmd/sage-gui/config.go:170` |
| `SAGE_EMBEDDING_MODEL` | Embedding model name. | (provider-specific) | `cmd/sage-gui/config.go:173` |
| `SAGE_EMBEDDING_API_KEY` | API key for the embedding endpoint. | (none) | `cmd/sage-gui/config.go:176` |
| `SAGE_EMBEDDING_DIMENSION` | Embedding vector dimension (int > 0). | `768` | `cmd/sage-gui/config.go:179` |
| `SAGE_EMBEDDING_TIMEOUT` | HTTP deadline for each Ollama or OpenAI-compatible embedding request. Accepts a positive Go duration such as `60s` or `2m`; invalid, zero, and negative values fall back to 30 seconds. This can be raised for CPU-only inference queues. The REST server adds 15 seconds of response headroom so its outer writer does not cut off the embed request; that outer deadline is capped at 10 minutes. | `30s` | `internal/embedding/http_config.go:13`; `api/rest/server.go:1120` |
| `SAGE_EMBED_TIMEOUT` | Legacy alias for `SAGE_EMBEDDING_TIMEOUT`. The canonical variable wins when both are set, including when its value is invalid (which falls back safely rather than consulting the alias). | (none) | `internal/embedding/http_config.go:16` |
| `OLLAMA_URL` | **Legacy** alias for the base URL. `SAGE_EMBEDDING_BASE_URL` wins when both are set. | (none) | `cmd/sage-gui/config.go:166` |
| `OLLAMA_MODEL` | **Legacy** alias for the model. `SAGE_EMBEDDING_MODEL` wins when both are set. | (none) | `cmd/sage-gui/config.go:169` |
| `OLLAMA_KEEP_ALIVE` | How long Ollama keeps the embed model resident, sent as `keep_alive` on every embed request so it isn't unloaded between turns (the fix for intermittent embed failures). Accepts a duration (`24h`) or integer seconds (`-1` pins in memory, `0` unloads); an integer is sent to Ollama as a JSON number, an unparseable value falls back to the default. | `30m` | `internal/embedding/ollama.go:81` |
| `SAGE_PROVIDER` | Provider label the MCP server reports for itself. | (empty) | `internal/mcp/server.go:95` |

`sage-gui` defaults to the built-in `hash` provider. The standalone `amid`
daemon preserves its historical default of Ollama + `nomic-embed-text` + 768
dimensions, but now consumes the same provider-agnostic `SAGE_EMBEDDING_*`
model, base URL, API key, dimension, and timeout variables. An explicit invalid
dimension, unsupported provider, or incomplete OpenAI-compatible configuration
makes `amid` fail before opening its REST listener; it never silently stamps the
legacy Ollama vector space over a requested custom one (`cmd/amid/main.go`).

---

## Hybrid recall & reranking

| Variable | What it does | Default | Source |
|----------|--------------|---------|--------|
| `SAGE_RECALL_HYBRID` | Gates the hybrid (BM25 + vector / RRF) recall path. Set `0`/`false`/`no` to force the legacy single-index path. | on | `internal/mcp/tools.go:595` |
| `SAGE_HYBRID_RRF_K` | Reciprocal-Rank-Fusion `k` constant (int > 0). | `60` | `internal/store/hybrid.go:40` |
| `SAGE_HYBRID_BM25_WEIGHT` | Weight on the BM25 rank contribution (float ≥ 0). | `0.4` | `internal/store/hybrid.go:45` |
| `SAGE_HYBRID_VECTOR_WEIGHT` | Weight on the vector rank contribution (float ≥ 0). | `0.6` | `internal/store/hybrid.go:50` |
| `SAGE_HYBRID_OVERSAMPLE` | Per-index oversample multiplier (`TopK × N`, int ≥ 1). | `2` | `internal/store/hybrid.go:55` |
| `SAGE_RERANK_ENABLED` | Truthy value turns on the cross-encoder reranker. | off | `internal/embedding/reranker.go:214` |
| `SAGE_RERANK_URL` | Reranker endpoint (required when reranking is enabled). | (none) | `internal/embedding/reranker.go:215` |
| `SAGE_RERANK_MODEL` | Reranker model. | `BAAI/bge-reranker-v2-m3` | `internal/embedding/reranker.go:216` |
| `SAGE_RERANK_KIND` | **v11.** Endpoint dialect: `tei` (default) or `llamacpp` (the managed sidecar). Trimmed + lower-cased; unknown values fall back to TEI. | `tei` | `internal/embedding/reranker.go:217` |
| `SAGE_RERANK_TIMEOUT_MS` | Reranker request timeout (ms, int > 0). | `2000` | `internal/embedding/reranker.go:224` |
| `SAGE_RERANK_OVERSAMPLE` | Candidates pulled before reranking (int ≥ 1). | `2` | `internal/embedding/reranker.go:229` |

---

## Snapshots & behavior toggles

| Variable | What it does | Default | Read by | Source |
|----------|--------------|---------|---------|--------|
| `SAGE_SNAPSHOT_KEEP` | Snapshots to retain (newest N + per-version anchors, which are never pruned). Integer ≥ 1. | `5` | sage-gui | `cmd/sage-gui/node.go:262`, `cmd/sage-gui/snapshot.go:56` |
| `SAGE_BRANCH_TAG` | Set `0`/`false`/`no` to disable branch tagging of memories. | on | MCP | `internal/mcp/branch.go:27` |
| `SAGE_CLAUDE_CHANNEL` | Controls the experimental custom Claude notification adapter. It defaults off. The shipped Claude Code host registers a handler for `notifications/claude/channel`, but delivery from a plain `.mcp.json` server through the host's plugin-scoped channel gate remains unverified; enable it only for a host whose end-to-end delivery has been confirmed. Codex is hard-disabled even under an explicit override because it cannot consume the method and must not occupy the one exact-agent wake lease. When enabled for a capable host, `sage-gui mcp` subscribes to this agent's signed `/v1/messages/wake` stream. A rejected competing adapter retries with bounded backoff and can acquire the lease after the holder disconnects. Payload-free: the host learns only a durable wake cursor, never message content or sender. An unrecognized value warns and stays off. | off | sage-gui MCP (stdio) | `claudeChannelEnabled` (`mcp.go:347`), `runClaudeChannel` (`internal/mcp/claude_channel.go:111`), `EnableRESTClaudeChannel` (`internal/mcp/claude_wake_source.go:85`) |
| `SAGE_STOP_NUDGE` | Controls the payload-free end-of-turn wake check. Claude Code and Codex project hooks enable it by default; legacy installed Stop hooks with no provider label also default on, closing the user-scope upgrade gap. Set `0`/`false`/`no` to opt out. Other named hosts remain off unless explicitly enabled. The generated Stop hook asks the signed, lease-free wake snapshot whether durable message work remains unfinished and, for a newer cursor, emits one top-level `decision:block` continuation so the agent handles it before becoming idle. It never acquires the SSE lease, never sees sender or content, never blocks `SubagentStop`, never blocks when `stop_hook_active` is set, nudges the same session again only for a newer sequence, surfaces unchanged stranded work to a fresh session, and fails open on every error—including failure to persist its one-shot cursor. It cannot resurrect a thread that is already fully idle. | on for `claude-code`, `codex`, and legacy unlabeled Stop hooks; off for other named hosts | sage-gui hook, Codex/Claude Stop | `cmd/sage-gui/hook.go` (`stopNudgeEnabled`, `runHookStopCheck`), `cmd/sage-gui/mcp.go` (`sageStopScript`) |

---

## Recall-backed compaction

Capture of harness-evicted conversation turns as governed memories. Default-off;
see [`recall-backed-compaction.md`](recall-backed-compaction.md). All knobs are
clamped to safe bounds.

| Variable | What it does | Default | Read by | Source |
|----------|--------------|---------|---------|--------|
| `SAGE_NEVERCOMPACT` | Headless / centrally-managed opt-in for capture (`1`/`true`/`yes`/`on`). Interactive hosts use `sage-gui nevercompact enable` instead. Default-off either way. | off | sage-gui hook | `cmd/sage-gui/nevercompact.go` (`neverCompactEnvOptIn`, `neverCompactCapturePermitted`) |
| `SAGE_NEVERCOMPACT_CLASSIFICATION` | Clearance level for captured chunks, clamped to `1..4`; never Public. | `2` (Confidential) | sage-gui hook | `cmd/sage-gui/hook_precompact.go` (`neverCompactClassification`) |
| `SAGE_NEVERCOMPACT_TRANSCRIPT_ROOT` | Host-specific trusted root that a transcript path must canonically resolve within. | `~/.claude/projects` | sage-gui hook | `cmd/sage-gui/hook_precompact.go` (`preCompactTranscriptRoot`) |
| `SAGE_NEVERCOMPACT_CHUNK_BYTES` | Target chunk size in bytes, clamped to `1000..60000`. | `6000` | sage-gui hook | `cmd/sage-gui/hook_precompact.go` (`preCompactChunkBytes`) |
| `SAGE_NEVERCOMPACT_MAX_UNITS` | Capture units submitted per compaction, clamped to `1..4000`; the rest defers to the next compaction (the cursor persists, so the tail is never lost). | `400` | sage-gui hook | `cmd/sage-gui/hook_precompact.go` (`preCompactMaxUnits`) |
| `SAGE_NEVERCOMPACT_BUDGET_MS` | Wall-clock budget per capture in milliseconds, clamped to `500..4500`. | `3500` | sage-gui hook | `cmd/sage-gui/hook_precompact.go` (`preCompactBudget`) |
| `SAGE_NEVERCOMPACT_RECALL_BUDGET_MS` | Wall-clock budget for the complete thread recall in milliseconds, clamped to `1000..20000`. | `8000` | sage-gui hook | `cmd/sage-gui/nevercompact_recall.go` (`recallBudget`) |

---

## Initial-admin bootstrap

Used once, when bootstrapping the very first admin identity on a fresh network.

| Variable | What it does | Source |
|----------|--------------|--------|
| `SAGE_INITIAL_ADMIN_NAME` | Display name for the initial admin agent. | `cmd/sage-gui/initial_admin.go:51` |
| `SAGE_INITIAL_ADMIN_AGENT_ID` | Agent ID to grant initial admin to. | `cmd/sage-gui/initial_admin.go:41` |

---

## TLS

| Variable | What it does | Read by | Source |
|----------|--------------|---------|--------|
| `SAGE_CA_CERT` | CA certificate path for client-side TLS verification when talking to an `https://` node. | sage-gui, MCP | `cmd/sage-gui/http_client.go:25`, `internal/mcp/server.go:615` |
| `TLS_CERT` | Default for `amid --tls-cert` (REST API server cert, PEM). | amid | `cmd/amid/main.go:48` |
| `TLS_KEY` | Default for `amid --tls-key` (REST API server key, PEM). | amid | `cmd/amid/main.go:49` |
| `TLS_CA` | Default for `amid --tls-ca` (CA cert for TLS verification, PEM). | amid | `cmd/amid/main.go:50` |

---

## `amid` indexer

The standalone `amid` binary reads these as flag defaults.

| Variable | What it does | Source |
|----------|--------------|--------|
| `POSTGRES_URL` | Default for `--postgres-url` (PostgreSQL connection URL). | `cmd/amid/main.go:41` |
| `COMETBFT_HOME` | Default for `--home` (CometBFT home directory). | `cmd/amid/main.go:40` |

---

## Not part of the supported config surface

These are read by the code but are **test-only or OS-provided** — don't rely on them for deployment configuration.

| Variable | Why it's excluded | Source |
|----------|-------------------|--------|
| `SAGE_CLOUDFLARED_BIN` | Honored **only under `go test`** (fake `cloudflared`). | `web/wizard_chatgpt.go:49` |
| `SAGE_BROWSER_OPEN_BIN` | Honored **only under `go test`** (fake browser opener). | `web/wizard_chatgpt.go:60` |
| `SAGE_TEST_POSTGRES_DSN`, `CI` | Test / CI harness internals. | various `*_test`-adjacent paths |
| `PATH`, `HOME`, `APPDATA` | OS-provided; SAGE reads them only to locate binaries / the home directory. | various |
