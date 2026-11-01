# Fresh chains at the latest supported app version

This is the fresh-genesis implementation in the app-v30 stack. A new, empty
SAGE chain starts with the selected app rules from block 1. It does not create
an older chain and walk through historical upgrade proposals. Personal
initialization selects the supported ceiling through
`initCometBFTConfigForFreshChain` (`cmd/sage-gui/node.go:3652`).

## Version selection

| Selection | App version in this stack | Meaning |
|-----------|---------------------------|---------|
| Omitted or `--app-version latest` | 29 | The supported, unattended ceiling. |
| `--app-version 30` | 30 | Explicit use of the compiled app-v30 rules. App-v30 remains dormant for automatic upgrades. |
| Another explicit integer | 24–30 | A compiled fresh-genesis version; values outside this range are rejected. |

`freshGenesisVersion` (`cmd/sage-gui/fresh_genesis.go:27`) resolves `latest`
through `MaxSupportedAppVersion` (`internal/abci/app.go:2815`). The separate
`MaxCompiledAppVersion` (`internal/abci/app.go:2822`) is 30. Creating an explicit
app-v30 genesis does not raise the automatic upgrade ceiling or activate
app-v30 on any existing chain.

## Create one artifact for the complete validator set

For a consumer such as VANTARAQ, the same contract supports a single validator
or a quorum of four or more validators, up to 256. Prepare a JSON array with
every validator's public Ed25519 key as 64 lowercase hexadecimal characters
and its positive voting power:

```json
[
  {"validator_id": "<first validator public key>", "power": 10},
  {"validator_id": "<second validator public key>", "power": 10}
]
```

Replace the placeholders and include the entire roster before signing. Private
validator keys do not belong in this file. Roster uniqueness, ordering, powers
and size are checked by `ValidateFreshGenesisActivation`
(`internal/store/fresh_genesis.go:54`); the CLI sorts the public roster before
signing in `freshGenesisAppStateWithKeys` (`cmd/sage-gui/fresh_genesis.go:38`).

Use an existing Root key and a unique new chain ID:

```sh
sage-gui init-genesis \
  --chain-id vantaraq-new-chain \
  --root-key /secure/root.key \
  --validators /secure/validators.json \
  --output /staging/genesis.json
```

The default is Root-only bootstrap; ordinary agents can be enrolled later. To
enroll one Companion at genesis, also provide
`--agent-key /secure/companion.key --home-domain vantaraq` and optionally
`--clearance 0..4` (default 1). The Companion key must already exist and must
differ from Root. Its manifest includes the Companion profile, capability mask
15, clearance and owned non-shared home domain. Both Root and Companion sign
the same manifest. These inputs and the output file are handled by
`runInitGenesis` (`cmd/sage-gui/fresh_genesis.go:122`).

Run this command once. It writes a new genesis file with a Root-signed manifest
and refuses to overwrite an existing output; it does not start a node. Install
the **identical genesis file** as each node's `config/genesis.json` before first
startup. Each node keeps its own validator private key, signing-state file and
node key, plus its local quorum/peer configuration. Generating separate genesis
files on each node is not the distribution procedure. The legacy mutable
`quorum-init` / `quorum-join` creation flow refuses new-chain creation through
`refuseLegacyQuorumGenesisCreation` (`cmd/sage-gui/fresh_quorum.go:12`).

## What consensus verifies

The signed manifest binds the chain ID, schema, target app version, Root,
optional agent policy and complete validator roster. Signature verification is
in `VerifyFreshGenesisManifest` (`internal/abci/fresh_genesis.go:76`). At
`InitChain`, `prevalidateFreshGenesisRequest`
(`internal/abci/fresh_genesis.go:129`) also checks the declared consensus app
version, initial height and actual CometBFT validator keys/powers against that
manifest.

`BootstrapFreshGenesis` (`internal/store/fresh_genesis.go:132`) atomically
creates the origin's authority and index prerequisites. An exact uncommitted
genesis replay is permitted; a different manifest or a contaminated store is
rejected. `ValidateFreshGenesisLineage`
(`internal/store/fresh_genesis.go:299`) rejects synthetic historical upgrade
records through the origin version. The origin is preserved across restart
and recovery; it is not a substitute upgrade ledger.

## Existing chains and historical direct-v23 origins

Keep an existing chain's genesis and history. Existing initialization returns
without replacing its genesis in `initCometBFTConfigWithGenesisVersion`
(`cmd/sage-gui/node.go:3656`). Its later app-version changes use the normal
governed upgrade path described in [Upgrading SAGE](../UPGRADING.md).

The older [direct-v23 bootstrap contract](app-v23-access-control-design.md)
remains supported for historical artifacts and their app-v24 serving boundary.
It is not the starting point for new installs using this fresh-genesis path.
