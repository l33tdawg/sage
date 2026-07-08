# Natter — SAGE's libp2p Connectivity Service

Natter is the author-operated, **optional** connectivity service for SAGE
federation (reference deployment: `natter.sage.delivery`). It gives SAGE
nodes behind NAT/firewalls:

- **Peer rendezvous** — a stable, well-known peer ID + multiaddrs that SAGE
  nodes bootstrap against
- **Reachability assistance** — an AutoNAT dial-back service so nodes can
  detect whether they are publicly reachable
- **DCUtR hole-punch coordination** — the coordination streams between two
  NAT'd peers ride over relayed connections
- **Circuit Relay v2 fallback** — E2E-encrypted relayed connectivity when
  hole punching fails

It is a single small Go binary in its own module
(`github.com/l33tdawg/sage/natter`), fully config-driven, with no hardcoded
infrastructure — running your own natter is a first-class setup (see
[Self-hosting](#self-hosting)).

## Security model

**Natter sits OUTSIDE the SAGE federation trust boundary.** It forwards
end-to-end-encrypted libp2p streams (Noise/TLS between the federation peers
themselves) and is architecturally unable to:

- read plaintext federation traffic,
- impersonate a federation peer, or
- modify messages in transit (integrity is end-to-end).

A fully compromised natter can affect **availability and connection metadata
only** (who connected to whom, when, and how many bytes). No SAGE memory,
consensus, or identity material ever depends on it. That is why relay limits
are a bandwidth-budget problem, not a confidentiality one — and why you can
point your nodes at anyone's natter (or none) without extending trust.

## Build

Requires Go 1.25+ (see `go.mod`).

```sh
cd natter
make build            # -> bin/natter
make test             # unit tests (host boot, peer ID stability, limits)
make lint             # golangci-lint if installed, else go vet
make build-linux-arm64  # cross-compile for the Hetzner CAX11 (arm64)
```

## Configure

```sh
cp natter.example.yaml natter.yaml
$EDITOR natter.yaml
```

Everything is documented inline in `natter.example.yaml`: listen multiaddrs
(QUIC/TCP/WSS), identity key path, relay resource limits (with the 20 TB/mo
budget math), AutoNAT rate limits, and WSS cert paths. Every key has a sane
default; with no config file at all, natter runs with QUIC+TCP on port 4001
and a `./natter.key` identity.

## Run

```sh
./bin/natter -config natter.yaml
```

On startup natter prints a copy-paste-ready bootstrap block:

```
==================== NATTER BOOTSTRAP INFO ====================
peer id: 12D3KooW...
multiaddrs (publish these in SAGE bootstrap config):
  /ip4/203.0.113.7/udp/4001/quic-v1/p2p/12D3KooW...
  /ip4/203.0.113.7/tcp/4001/p2p/12D3KooW...
===============================================================
```

Those multiaddrs are exactly what gets published as SAGE bootstrap config.
The peer ID is derived from the persistent ed25519 key at
`identity_key_path` (created on first run, mode 0600) and is **stable across
restarts — back that file up**; losing it changes the relay's identity and
strands every node that pinned it.

Health check: `curl -s http://127.0.0.1:8090/healthz` (local only).
Shutdown: SIGTERM/SIGINT are handled gracefully.

## Reference deployment (Hetzner CAX11, natter.sage.delivery)

The CAX11 is an arm64 (Ampere) box with ~20 TB/mo traffic included — the
default relay limits are tuned against that budget (math in the example
config).

1. **Provision** a CAX11 (Ubuntu LTS). Create the service user and dirs:

   ```sh
   sudo useradd --system --home /var/lib/natter --shell /usr/sbin/nologin natter
   sudo mkdir -p /etc/natter /var/lib/natter
   sudo chown natter:natter /var/lib/natter
   ```

2. **DNS** — add an `A`/`AAAA` record: `natter.sage.delivery -> <server IP>`.

3. **Firewall** — allow inbound:

   | Port | Proto | Purpose                    |
   |------|-------|----------------------------|
   | 4001 | UDP   | QUIC (preferred transport) |
   | 4001 | TCP   | TCP fallback               |
   | 443  | TCP   | WSS (egress-443-only nodes)|
   | 443  | UDP   | QUIC on 443 (optional alt) |

   ```sh
   sudo ufw allow 4001/udp && sudo ufw allow 4001/tcp
   sudo ufw allow 443/tcp  && sudo ufw allow 443/udp
   ```

4. **Let's Encrypt** for the WSS listener (natter terminates TLS itself, so
   use the standalone authenticator before natter binds 443, or DNS-01):

   ```sh
   sudo certbot certonly --standalone -d natter.sage.delivery
   ```

   Point `wss.cert_file` / `wss.key_file` at
   `/etc/letsencrypt/live/natter.sage.delivery/{fullchain,privkey}.pem`,
   grant the `natter` user read access to them, and add a certbot deploy
   hook: `systemctl restart natter` (certs are loaded at startup; hot-reload
   is a TODO).

5. **Deploy the binary + unit** (cross-compile locally, ship the artifact):

   ```sh
   make build-linux-arm64
   scp bin/natter-linux-arm64 root@natter.sage.delivery:/usr/local/bin/natter
   scp natter.example.yaml    root@natter.sage.delivery:/etc/natter/natter.yaml   # then edit
   scp deploy/natter.service  root@natter.sage.delivery:/etc/systemd/system/
   ssh root@natter.sage.delivery 'systemctl daemon-reload && systemctl enable --now natter'
   ```

   The unit runs as the non-root `natter` user with `ProtectSystem=strict`,
   `NoNewPrivileges`, and only `CAP_NET_BIND_SERVICE` (for :443). State is
   confined to `/var/lib/natter`.

6. **Publish** the bootstrap block from `journalctl -u natter` into SAGE
   bootstrap config.

## Self-hosting

Nothing in natter (or SAGE) is pinned to `natter.sage.delivery`. To run your
own connectivity service for your federation:

1. Build (or cross-compile) the binary and deploy it on any box with a
   public IP — the systemd unit and config template above work unchanged.
2. Skip the WSS section if your nodes don't need an egress-443 escape hatch;
   QUIC+TCP on 4001 is a complete setup.
3. Point your SAGE nodes' bootstrap config at YOUR peer ID + multiaddrs from
   the startup banner.

Because natter is outside the trust boundary, federations can even share a
relay run by a third party — the worst a malicious operator can do is drop
your connections.

## Rendezvous status (deliberate scope cut)

SAGE nodes currently bootstrap against natter's **static peer ID +
multiaddrs**; once connected, peers learn about each other via identify and
the relay's peerstore. A dedicated rendezvous protocol server (namespace
register/discover) is **deferred to Sprint 3 as client-side work** — the
lighter static-bootstrap approach covers a single author-operated node, and
go-libp2p's DHT would be overkill (and extra attack/maintenance surface) for
one relay.

## Module layout

```
natter/
├── main.go              # wiring: flags, banner, /healthz, graceful shutdown
├── config.go            # YAML config, defaults, validation
├── host.go              # identity persistence, libp2p host, relay service
├── natter_test.go       # peer ID stability, relay limits, config tests
├── natter.example.yaml  # commented reference config
├── deploy/natter.service
└── Makefile
```
