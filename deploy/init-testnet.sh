#!/usr/bin/env bash
set -euo pipefail

# Generate 4-node CometBFT testnet configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GENESIS_DIR="${SCRIPT_DIR}/genesis"
NUM_VALIDATORS=4

# The amid/ABCI container (deploy/Dockerfile.abci) runs as the unprivileged
# system user 'sage' (uid/gid 100/101 on Alpine). cometbft writes
# priv_validator_key.json as 0600 owned by the generating user, so that in-
# container user cannot read its own signing key and the per-node memory
# auto-voter silently disables itself ("permission denied"). Normalize the key
# to 0640 owned by the amid uid/gid so amid can read it; the cometbft container
# runs as root (deploy/Dockerfile.node has no USER) and reads it regardless.
# Override these if your image runs amid as a different uid/gid.
AMID_UID="${AMID_UID:-100}"
AMID_GID="${AMID_GID:-101}"

echo "==> Generating ${NUM_VALIDATORS}-node testnet configuration..."

# Clean existing configs
rm -rf "${GENESIS_DIR}"
mkdir -p "${GENESIS_DIR}"

# Prefer a host-built cometbft from the standard Go install location so we skip the
# in-container build entirely when the developer already has one installed.
if [ -x "${HOME}/go/bin/cometbft" ]; then
    PATH="${HOME}/go/bin:${PATH}"
fi

# Check if cometbft binary is available
if ! command -v cometbft &> /dev/null; then
    echo "cometbft binary not found. Building from Docker..."
    HOST_UID=$(id -u)
    HOST_GID=$(id -g)
    # NOTE: 'set -e' inside the container so a failed clone/build aborts loudly here
    # instead of leaking through as a confusing "cometbft: not found" from the
    # testnet call below. The build's stderr is intentionally NOT suppressed.
    docker run --rm -v "${GENESIS_DIR}:/genesis" \
        golang:1.22-alpine sh -c '
        set -e
        apk add --no-cache git make >/dev/null 2>&1
        git clone --branch v0.38.15 --depth 1 https://github.com/cometbft/cometbft.git /tmp/cometbft 2>/dev/null
        cd /tmp/cometbft
        if ! CGO_ENABLED=0 go build -o /usr/local/bin/cometbft ./cmd/cometbft; then
            echo "ERROR: failed to build cometbft v0.38.15 in-container" >&2
            exit 1
        fi
        cometbft testnet \
            --v '"${NUM_VALIDATORS}"' \
            --o /genesis \
            --hostname-prefix cometbft \
            --populate-persistent-peers
        chown -R '"${HOST_UID}:${HOST_GID}"' /genesis
        # Make each node validator signing key readable by the in-container amid
        # user (0640, owned by the amid uid/gid). Done AFTER the recursive chown
        # above so it is not overwritten. Root here can chown to any id.
        for d in /genesis/node*/config; do
            chmod 0640 "$d/priv_validator_key.json" 2>/dev/null || true
            chown '"${AMID_UID}:${AMID_GID}"' "$d/priv_validator_key.json" 2>/dev/null || true
        done
    '
else
    cometbft testnet \
        --v ${NUM_VALIDATORS} \
        --o "${GENESIS_DIR}" \
        --hostname-prefix cometbft \
        --populate-persistent-peers
    # cometbft ran as the host user; normalize the signing keys so the amid
    # container (uid/gid ${AMID_UID}/${AMID_GID}) can read them. chown needs
    # privilege the host user may lack — fall back to a precise instruction.
    for i in $(seq 0 $((NUM_VALIDATORS - 1))); do
        KEY="${GENESIS_DIR}/node${i}/config/priv_validator_key.json"
        [ -f "$KEY" ] || continue
        chmod 0640 "$KEY"
        if ! chown "${AMID_UID}:${AMID_GID}" "$KEY" 2>/dev/null; then
            echo "==> NOTE: could not chown ${KEY} to ${AMID_UID}:${AMID_GID} (needs root)."
            echo "    The amid container runs as that uid/gid and must read its signing key."
            echo "    If the auto-voter logs 'permission denied', run:"
            echo "      sudo chown ${AMID_UID}:${AMID_GID} ${GENESIS_DIR}/node*/config/priv_validator_key.json"
            echo "      sudo chmod 0640 ${GENESIS_DIR}/node*/config/priv_validator_key.json"
        fi
    done
fi

# Patch config.toml for each node
for i in $(seq 0 $((NUM_VALIDATORS - 1))); do
    CONFIG="${GENESIS_DIR}/node${i}/config/config.toml"

    echo "==> Patching node${i} config..."

    # Disable PEX (use persistent peers only)
    sed -i.bak 's/pex = true/pex = false/' "$CONFIG"

    # Allow non-routable addresses (Docker)
    sed -i.bak 's/addr_book_strict = true/addr_book_strict = false/' "$CONFIG"

    # Allow duplicate IPs (Docker)
    sed -i.bak 's/allow_duplicate_ip = false/allow_duplicate_ip = true/' "$CONFIG"

    # Set block time
    sed -i.bak 's/timeout_commit = ".*"/timeout_commit = "3s"/' "$CONFIG"

    # Enable Prometheus metrics
    sed -i.bak 's/prometheus = false/prometheus = true/' "$CONFIG"

    # Set proxy_app for ABCI connection (TCP to separate ABCI container)
    sed -i.bak "s|proxy_app = \".*\"|proxy_app = \"tcp://abci${i}:26658\"|" "$CONFIG"

    # Set listen addresses to bind all interfaces
    sed -i.bak 's|laddr = "tcp://127.0.0.1:26657"|laddr = "tcp://0.0.0.0:26657"|' "$CONFIG"
    sed -i.bak 's|laddr = "tcp://0.0.0.0:26656"|laddr = "tcp://0.0.0.0:26656"|' "$CONFIG"

    # Clean up backup files
    rm -f "${CONFIG}.bak"
done

# Patch genesis.json to set chain_id
GENESIS="${GENESIS_DIR}/node0/config/genesis.json"
if command -v python3 &> /dev/null; then
    python3 -c "
import json
with open('${GENESIS}') as f:
    g = json.load(f)
g['chain_id'] = 'sage-testnet-1'
with open('${GENESIS}', 'w') as f:
    json.dump(g, f, indent=2)
"
    # Copy updated genesis to all nodes
    for i in $(seq 1 $((NUM_VALIDATORS - 1))); do
        cp "${GENESIS}" "${GENESIS_DIR}/node${i}/config/genesis.json"
    done
    echo "==> Chain ID set to: sage-testnet-1"
fi

echo "==> Testnet configuration generated in ${GENESIS_DIR}"
echo "==> Validators: ${NUM_VALIDATORS}"
echo "==> Run 'make up' to start the network"
