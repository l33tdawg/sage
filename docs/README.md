# SAGE Documentation

The central documentation resource for **SAGE** — Sovereign Agent Governed Experience, a governed, BFT-consensus-validated institutional memory layer for AI agents.

> Looking for the project landing page? It lives at **https://l33tdawg.github.io/sage/** (served from the `gh-pages` branch). This folder is documentation only.

---

## Start here

| If you want to… | Read |
|-----------------|------|
| **Integrate an agent / look up exact API, SDK, MCP, or behavior** | **[`reference/INDEX.md`](reference/INDEX.md)** — the authoritative, code-verified reference (cites `file:line`). Point agents here. |
| Get a node running and submit your first memory | [`GETTING_STARTED.md`](GETTING_STARTED.md) |
| Understand the system design (BFT, RBAC, federation, storage) | [`ARCHITECTURE.md`](ARCHITECTURE.md) |
| Bootstrap an admin / org / clearance setup | [`ADMIN_BOOTSTRAP.md`](ADMIN_BOOTSTRAP.md) |
| Configure lifecycle hooks | [`HOOKS.md`](HOOKS.md) |

---

## The reference (`reference/`)

The code-verified source of truth. When it disagrees with anything else, it wins.

- [`reference/rest-api.md`](reference/rest-api.md) — every HTTP endpoint
- [`reference/python-sdk.md`](reference/python-sdk.md) — full `SageClient` / `AsyncSageClient` surface (package `sage-agent-sdk`)
- [`reference/mcp-tools.md`](reference/mcp-tools.md) — every `sage_*` MCP tool + the boot sequence
- [`reference/concepts/`](reference/concepts/) — memory lifecycle, clearance & classification, RBAC/orgs/federation, consensus/confidence/decay

Each file carries a `Verified against … (commit …)` header. **Never document a feature that isn't in the code yet.**

---

## Conventions

- `docs/` is public, curated documentation. Internal drafts (whitepapers, announcements, figures) are kept out of version control via `.gitignore`.
- The machine-readable OpenAPI spec lives at [`../api/openapi.yaml`](../api/openapi.yaml) and is kept in sync with `reference/rest-api.md`.
