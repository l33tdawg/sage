# v11.18.2 federation Docker acceptance

This harness gives two SAGE personal nodes separate persisted homes, separate
Docker edge networks, and one self-hosted natter relay. A temporary third
network models a shared LAN. It is intentionally isolated from developer SAGE
state and binds REST only on `127.0.0.1:28080` and `127.0.0.1:28081` by
default (`FED_NODE_A_PORT` / `FED_NODE_B_PORT` override them).

Run the reproducible topology smoke test with:

```sh
FED_ACCEPTANCE_STATE=/tmp/sage-v111800-fed \
  deploy/federation-acceptance/run.sh topology
```

`topology` proves container isolation, LAN DNS reachability, relay-only
separation, process/state survival across A/B/both restarts, and relay outage
recovery. It does **not** qualify a release.

The release gate is `run.sh full`. It is self-contained: a test-only client
inside each node signs the loopback dashboard requests and drives the real JOIN,
MCP, permission, and inbox APIs. Three environment commands remain optional
diagnostic overrides:

- `FED_ACCEPTANCE_PAIR_COMMAND`: replace the built-in real dashboard JOIN
  ceremony and explicit reciprocal Mynah agent exports. The built-in path does
  not create mirrored Access Groups, receiving domains, or manual PeerRBAC
  identity rows.
- `FED_ACCEPTANCE_MIGRATION_COMMAND`: replace the built-in v11.17.4 migration gate,
  `p2p_peers` fixture for a paired node, restart it, and assert the peer is
  retained and upgraded to a current non-expired generation-bound route.
- `FED_ACCEPTANCE_FLOW_COMMAND`: replace the built-in actual MCP stdio flow and
  prove `sage_find_agent` -> direct unique registered-name `sage_message_send`,
  canonical remote delivery, and the replier-owned immutable reply-event status;
  then stop the recipient before
  delivery; restart it; prove `sage_inbox` persistence, read/claim,
  `sage_message_reply`, and `sage_message_status` on the sender. The send must
  omit `ttl_minutes`, wait longer than the old 60-minute default in a nightly
  variant (a clock-injected shorter case is acceptable in PR CI), and assert
  the message did not expire.

The built-in full mode fails closed on every missing result or deadline. A
green topology smoke test must never be presented as federation acceptance.
Fresh nodes may spend up to 45 minutes walking the governed fork ladder through
app-v26, where vendored Mynah companions and durable receipt-v2 acknowledgement
are both active; the harness prints live app version and height while it waits
and never substitutes a pre-activated fixture.
Each fresh MCP process is bounded at 100 seconds by default (override with
`FED_ACCEPTANCE_MCP_TIMEOUT_SECONDS`), so an unavailable route fails the gate
with the phase and tool name instead of hanging CI indefinitely.

## Matrix

| Phase | Topology/fault | Required assertion |
|---|---|---|
| LAN | both nodes temporarily share `lan` | direct names resolve; JOIN and canonical flow succeed |
| Internet-like | nodes only share natter indirectly | no edge-to-edge route; relay route succeeds |
| Address churn | recreate A, recreate B, restart both | persisted agreement discovers current route; no stale endpoint pin |
| Stale snapshot | set persisted `expires_at` in the past | stale route is rejected, refreshed, and flow recovers |
| Relay outage | stop then restart natter | explicit offline/no-route while down; automatic recovery after restart |
| Upgrade | v11.17.4 `p2p_peers` fixture | peer survives load and gains current route snapshot |
| Pairwise Read | reciprocal exported Mynah agents; no mirrored Access Group or linked-reader fixture | either ordinary companion can read the other's exported owned domain; writes remain ungranted |
| Copy backfill | each source independently offers Copy; each receiver independently subscribes | receiver-local recall finds memories created before consent in both directions |
| Copy incremental | create a new source memory after consent | receiver-local recall finds the new copy in both directions |
| Copy restart | restart both nodes, then stop each source in turn | each receiver still recalls both backfilled and incremental copies locally while its source is offline |
| Offline inbox | recipient stopped after send | durable inbox delivery, read, reply, and final sender status |

The contract test is fast and does not require Docker:

```sh
node scripts/federation-acceptance-contract.test.mjs
```
