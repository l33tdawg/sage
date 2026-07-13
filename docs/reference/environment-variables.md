<!-- Reconciled through SAGE v11.2.1. Every variable below was located at the cited file:line via `os.Getenv` or the local env helper. When the code changes, re-verify and bump this header. -->

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
| `SAGE_API_URL` | REST base URL that the CLI / MCP server / hooks talk to. | `http://localhost:8080`, or `https://localhost:8443` when `$SAGE_HOME/certs/` exists | sage-gui, sage-cli, MCP | `internal/mcp/server.go:591`, `cmd/sage-gui/mcp.go:185`, `cmd/sage-cli/main.go:30` |
| `SAGE_IDENTITY_PATH` | Explicit identity-key path. Takes precedence over `SAGE_AGENT_KEY`. | (per-project derivation) | sage-gui, MCP | `cmd/sage-gui/mcp.go:145`, `cmd/sage-gui/hook.go:219` |
| `SAGE_AGENT_KEY` | Explicit agent-key path; overrides per-project key derivation. Used if `SAGE_IDENTITY_PATH` is unset. | `$SAGE_HOME/agent.key` | sage-gui, MCP | `cmd/sage-gui/mcp.go:147`, `cmd/sage-gui/mcp_token.go:208` |

---

## Server & networking

| Variable | What it does | Default | Read by | Source |
|----------|--------------|---------|---------|--------|
| `REST_ADDR` | REST API listen address; overrides `config.yaml`. | `127.0.0.1:8080` | sage-gui | `cmd/sage-gui/config.go:156` |
| `SAGE_CMT_RPC_ADDR` | CometBFT RPC listen address (also the tx-broadcast client target, the web health panel, `sage-cli status`, and the `sage-gui upgrade` RPC default). Move it to run a second node on one host. | `tcp://127.0.0.1:26657` | sage-gui, sage-cli | `cmd/sage-gui/node.go:842` |
| `SAGE_CMT_P2P_ADDR` | CometBFT P2P listen address. Personal mode defaults to loopback; quorum mode defaults to `tcp://0.0.0.0:26656` when unset. Generated agent bundles leave `p2p_addr` blank so this env override applies. | `tcp://127.0.0.1:26656` | sage-gui | `cmd/sage-gui/node.go:860` |
| `CORS_ALLOWED_ORIGINS` | Comma-separated allowlist of origins for REST CORS. | `*` | REST | `api/rest/server.go:258-262` |
| `SAGE_COMET_RPC` | CometBFT RPC endpoint for `sage-gui upgrade`; takes precedence over `SAGE_CMT_RPC_ADDR` for that command. | (built-in) | sage-gui | `cmd/sage-gui/upgrade.go:44` |
| `SAGE_TX_COMMIT_TIMEOUT_MS` | Timeout (ms) for `broadcast_tx_commit`. Raise it for unusually slow consensus. | `60000` (60s) | REST, federation | `api/rest/memory_handler.go:1317`, `internal/federation/broadcast.go:14-21` |
| `SAGE_NO_BROWSER` | If set to any non-empty value, don't auto-open a browser when the node starts. | (unset → opens browser) | sage-gui | `cmd/sage-gui/node.go:724` |
| `SAGE_FED_RECALL_TIMEOUT_MS` | Timeout (ms) for federated recall fanout. | `4000` (4s) | REST | `api/rest/memory_handler.go:786-791` |
| `SAGE_FED_RECEIPT_TIMEOUT_MS` | Timeout (ms) for federation receipt fetches. | `20000` (20s) | federation | `internal/federation/client.go:21-28` |
| `SAGE_UI_DIR` | Filesystem directory for serving web UI assets instead of the embedded bundle. | embedded assets | web | `web/handler.go:522-526` |
| `SAGE_GRAPH_MAX_NODES` | Maximum graph nodes returned by the web graph endpoint. | `500` | web | `web/handler.go:1238-1241` |

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
| `VALIDATOR_KEY_FILE` | `amid` socket mode: `priv_validator_key.json` for the auto-voter (in-process mode uses the key under `--home`). Without it in socket mode, no voter runs unless `--require-voter` forces a failure. | (none) | amid, REST | `cmd/amid/main.go`, `api/rest/server.go` |

---

## Vault & secrets

| Variable | What it does | Default | Read by | Source |
|----------|--------------|---------|---------|--------|
| `SAGE_PASSPHRASE` | Vault passphrase for the encrypted store. If empty, the node prompts on a TTY (and stays locked if none is available). | (prompt) | sage-gui | `cmd/sage-gui/node.go:169` |
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
| `OLLAMA_URL` | **Legacy** alias for the base URL. `SAGE_EMBEDDING_BASE_URL` wins when both are set. | (none) | `cmd/sage-gui/config.go:166` |
| `OLLAMA_MODEL` | **Legacy** alias for the model. `SAGE_EMBEDDING_MODEL` wins when both are set. | (none) | `cmd/sage-gui/config.go:169` |
| `OLLAMA_KEEP_ALIVE` | How long Ollama keeps the embed model resident, sent as `keep_alive` on every embed request so it isn't unloaded between turns (the fix for intermittent embed failures). Accepts a duration (`24h`) or integer seconds (`-1` pins in memory, `0` unloads); an integer is sent to Ollama as a JSON number, an unparseable value falls back to the default. | `30m` | `internal/embedding/ollama.go:81` |
| `SAGE_PROVIDER` | Provider label the MCP server reports for itself. | (empty) | `internal/mcp/server.go:95` |

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
| `SAGE_TEST_POSTGRES_DSN`, `VALIDATOR_KEY_FILE`, `CI` | Test / CI harness internals. | various `*_test`-adjacent paths |
| `PATH`, `HOME`, `APPDATA` | OS-provided; SAGE reads them only to locate binaries / the home directory. | various |
