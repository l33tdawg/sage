<!-- Reconciled through SAGE v11.2.0 (registry.go:67/84/109, app.go:704-729/2688-2718, provider.go:98/107/132/139/147). -->

# Layer-2 content-validation gate (and how a deployment arms it)

Verified against code at SAGE v11.2.0.

SAGE ships an optional, deployment-agnostic **content-validation gate**: a
consensus-path hook that can REJECT a memory submission based on the *shape of
its content*, keyed by `(domain, outcome_class)`. SAGE itself ships ZERO
validators — a stock build never rejects anything on this layer. A deployment
supplies its own validators and arms the gate without patching any SAGE source
it doesn't own.

This doc is the authoritative reference for the gate and, in particular, the
**arming seam** a downstream deployment uses so it never re-patches the `cmd`
entrypoints on a SAGE bump.

---

## What the gate is

A `ContentValidator` is a pure function of one record:

```go
type ContentValidator func(rec *memory.MemoryRecord) error // nil = accept, err = REJECT
```

The error string becomes the on-chain reject `Log`, so it MUST be deterministic
across every validator (no node-local or non-reproducible state) or validators
will diverge on `AppHash`.

Validators are held in a `ContentValidatorRegistry`, populated once at boot and
treated as frozen thereafter (`internal/contentvalidator/registry.go`):

- `RegisterContentValidator(domain, outcomeClass, v)` — bind a validator to an
  exact `(domain, outcome_class)` pair. (`registry.go:67`)
- `RegisterClosedDomain(domain)` — mark a domain closed-schema: once it has at
  least one validator, a submission whose `outcome_class` has NO registered
  validator is REJECTED instead of passing through. This closes the fail-open
  hole where a malformed/cross-class/router-miss body would commit unvalidated.
  (`registry.go:84`)
- `Validate(domain, outcomeClass, rec) (rejected, reason)` — the runtime call.
  A nil receiver or empty registry is pass-through. (`registry.go:109`)

The routing key is read from the submission body by `parseOutcomeClass`
(`internal/abci/app.go:1890`), which decodes ONLY the `outcome_class` field and
ignores every sibling.

### When enforcement is actually live

Enforcement requires BOTH (`internal/abci/app.go:2701`):

1. a registry is installed (`app.contentValidators != nil`), AND
2. `postAppV7Fork(height)` is true.

There is no separate enable flag. A registry compiled in past the fork is enough;
enforcement is then chain-wide and driven by consensus state, not a per-node
toggle. Current code makes this a bounded consensus window: the gate is live after
app-v7 activation and turns off again after app-v14 activation
(`internal/abci/app.go:704-729`). A node in the live window with NO registry stays
bootable (a generic-only fleet is valid) but emits an advisory — see
`ContentValidationEnforcementWarning` (`app.go:1238-1258`). A MIXED fleet
(some nodes with a registry, some without) during the live window is the
real hazard: it forks `AppHash`. Every validator in a fleet must run the SAME
registry.

---

## Arming the gate (the extension point)

`internal/contentvalidator/provider.go` is the release-stable seam. A deployment
registers a `Provider` and the ABCI constructors install it automatically — no
`SetContentValidators` call site, no edit to `cmd/amid` or `cmd/sage-gui`.

```go
type Provider func() *ContentValidatorRegistry

func SetProvider(p Provider)                       // register (boot-only; nil clears)
func HasProvider() bool                            // is one registered?
func BuildFromProvider() *ContentValidatorRegistry // build one, or nil
```

Both constructors (`NewSageApp`, `NewSageAppWithStores`) call
`armContentValidatorsFromProvider` (`internal/abci/app.go`), which installs the
provider's registry **only if** no registry was already wired via
`SetContentValidators` (explicit wiring wins).

### Recommended pattern (zero cmd edits)

Mirror `database/sql` driver registration: drop ONE additive `.go` file into a
package the binary already compiles (this `contentvalidator` package always is)
and register from its `init()`:

```go
package contentvalidator // additive file, e.g. arming_mydeploy.go

func init() {
    SetProvider(func() *ContentValidatorRegistry {
        r := NewContentValidatorRegistry()
        r.RegisterContentValidator("my-domain", "my-class", myValidator)
        r.RegisterClosedDomain("my-domain")
        return r
    })
}
```

Because the file is *additive* (a new file, not an edit to an existing one), it
does not conflict on rebase across SAGE releases the way patching
`cmd/amid/main.go` + `cmd/sage-gui/node.go` did. The provider API is the stable
contract; the surrounding entrypoint code is free to move.

> If you prefer to keep validators in your own package, register from that
> package's `init()` and add a single blank import (`import _ ".../yourpkg"`) to
> a compiled package so the `init()` runs. Dropping the arming file directly into
> an already-compiled package avoids even that one import.

### Stateful validators (context-aware arming)

The no-arg `Provider` above is perfect for **stateless** validators (shape checks
on the record body alone). Some validators need live, read-only chain state at
arm time — most commonly a **signer-authority** check: a record's self-asserted
`agent_role` is forgeable, so it may only be trusted if the *on-chain signer*
actually holds that role. That decision needs a lookup over chain state, which a
no-arg provider can't reach — forcing a deployment back onto per-release `cmd`
patches (`app.SetContentValidators(reg)` wired with `app.RoleResolver()`).

The **context-aware** variant closes that gap. It is purely additive and opt-in;
the no-arg `Provider` is unchanged and stock builds still register nothing.

```go
type ArmContext interface {
    RoleResolver() func(agentID string) string // "" = unknown/unregistered signer
}

type ProviderWithContext func(ArmContext) *ContentValidatorRegistry

func SetProviderWithContext(p ProviderWithContext)        // register (boot-only; nil clears)
func HasProviderWithContext() bool                        // is one registered?
func BuildFromProviderWithContext(c ArmContext) *ContentValidatorRegistry
```

(`provider.go:98/107/132/139/147`.) The constructor passes an `ArmContext` backed
by the app, so the whole arming collapses to one additive `init()` — no `cmd`
edits, signer authority preserved:

```go
func init() {
    contentvalidator.SetProviderWithContext(func(c contentvalidator.ArmContext) *contentvalidator.ContentValidatorRegistry {
        reg := contentvalidator.NewContentValidatorRegistry()
        resolve := c.RoleResolver() // capture ONCE at arm time
        reg.RegisterContentValidator("red", "detect", func(rec *memory.MemoryRecord) error {
            if resolve(rec.SubmittingAgent) != "red-agent" {
                return fmt.Errorf("signer %s lacks red-agent authority", rec.SubmittingAgent)
            }
            return nil // ...plus the shape checks
        })
        return reg
    })
}
```

What `ArmContext` exposes — and why it stays deterministic:

- `RoleResolver()` returns **the same per-call, read-only on-chain role lookup the
  gate already consumes inside `FinalizeBlock`** (`app.RoleResolver()`,
  `app.go:987`). Given an agent ID it returns the RAW on-chain role, or `""` when
  the signer is unknown or the read errors — so an authority check fails closed on
  an unknown signer. Capture it once at arm time; call it per record. Nothing the
  enforcement path doesn't already read is exposed, so the seam adds **no new
  nondeterminism surface** (no time, no network, no writes, no goroutines).
- The `ArmContext` handed to a provider is a **narrow adapter, not the `*SageApp`**
  (`app.go:939/943`). A provider therefore cannot downcast back into mutable app
  internals — the seam is a real decoupling boundary, not an ergonomic alias.

**Precedence.** If BOTH a context-aware and a no-arg provider are registered, the
context-aware one wins (it is the richer registration, consulted first at
`app.go:917`) and the constructor logs a warning so the no-arg registration is
never *silently* dropped. An explicit `SetContentValidators` still beats both —
the early-return guard at `app.go:909` leaves a pre-wired registry untouched.

### Contract / gotchas

- **Boot-only.** Call `SetProvider` from an `init()` or before constructing the
  ABCI app. The registry is frozen once blocks are processing; mutating it (or
  registering concurrently with `FinalizeBlock`) is non-deterministic.
- **Determinism.** Every validator's accept/reject decision and error string must
  be identical across all validators. Non-determinism forks `AppHash`.
- **Whole-fleet parity.** Ship the same provider to every validator. A registry
  on some nodes but not others diverges state.
- **Stock SAGE registers nothing.** No provider ⇒ gate inert ⇒ identical to a
  build without this seam. Backward compatible by construction.
