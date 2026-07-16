#!/usr/bin/env bash
#
# Integrated v11.9 authorized state-sync wire gate.
#
# This deliberately uses independent `sage-gui serve` processes. The provider,
# read-only observer, pristine receiver, and unauthorized receiver attempt each
# own a distinct Comet node key and private-validator key. Only the provider's
# validator key is in genesis; the observer and receivers are NON-VALIDATOR full
# nodes and therefore cannot sign or double-sign the chain.
#
# The fixture-tagged sage-gui shortens two governance timing bounds: the
# governed upgrade-delay floor is three blocks instead of 200, and the proposer
# cooldown is one block instead of 50. It also recognizes a test-only dormant
# pre-publication pause hook used to place the exact crash below. A fresh
# provider still performs the real signed auto-advance ceremony through app-v20,
# at a positive activation height, with the chain-derived governance domain.
# Authorization, quorum, identity, state-sync, P2P, receiver-session,
# activation, Comet handoff, seal, REST admission, and restart paths are
# production code.

set -euo pipefail
cd "$(dirname "$0")/../.."

ABCI_IMAGE=${V119_STATE_SYNC_ABCI_IMAGE:-sage-v119-chaos-abci:local}
NODE_IMAGE=${V119_STATE_SYNC_NODE_IMAGE:-sage-v119-chaos-node:local}
REBUILD=${V119_STATE_SYNC_REBUILD:-${V119_CHAOS_REBUILD:-1}}
KEEP=${V119_STATE_SYNC_KEEP:-0}
TIMEOUT=${V119_STATE_SYNC_TIMEOUT:-240}

for setting in "REBUILD=${REBUILD}" "KEEP=${KEEP}"; do
  case "${setting#*=}" in
    0|1) ;;
    *)
      echo "ERROR: ${setting%%=*} must be 0 or 1" >&2
      exit 1
      ;;
  esac
done

TMP_ROOT=$(cd "${TMPDIR:-/tmp}" && pwd -P)
WORKDIR=$(mktemp -d "${TMP_ROOT}/sage-v119-state-sync.XXXXXX")
WORKDIR=$(cd "${WORKDIR}" && pwd -P)
MARKER=${WORKDIR}/.sage-v119-state-sync-owner
printf '%s\n' "${WORKDIR}" >"${MARKER}"

TOKEN=$(basename "${WORKDIR}" | tr -cd '[:alnum:]')
RPC_NETWORK="sage-v119-ss-rpc-${TOKEN}"
P2P_NETWORK="sage-v119-ss-p2p-${TOKEN}"
PROVIDER="sage-v119-ss-provider-${TOKEN}"
OBSERVER="sage-v119-ss-observer-${TOKEN}"
RECEIVER="sage-v119-ss-receiver-${TOKEN}"
SUCCESS_RECEIVER="sage-v119-ss-success-receiver-${TOKEN}"
ATTACKER="sage-v119-ss-unauthorized-${TOKEN}"
P2P_PLACEHOLDER="sage-v119-ss-p2p-placeholder-${TOKEN}"
CONTAINERS=("${PROVIDER}" "${OBSERVER}" "${RECEIVER}" "${SUCCESS_RECEIVER}" "${ATTACKER}" "${P2P_PLACEHOLDER}")

PROVIDER_HOME=${WORKDIR}/provider
OBSERVER_HOME=${WORKDIR}/observer
RECEIVER_HOME=${WORKDIR}/receiver
SUCCESS_RECEIVER_HOME=${WORKDIR}/success-receiver
ATTACKER_HOME=${WORKDIR}/unauthorized
PRE_PUBLISH_MARKER=/sage/v119-state-sync-pre-publish.marker
READINESS_VIOLATION=/sage/v119-state-sync-premature-ready
READINESS_PROBE_PID=

validate_workdir() {
  [ -n "${WORKDIR:-}" ] && [ "${WORKDIR}" != / ] && [ -d "${WORKDIR}" ] &&
    [ ! -L "${WORKDIR}" ] && [ -f "${MARKER}" ] && [ ! -L "${MARKER}" ] &&
    [ "$(cat "${MARKER}")" = "${WORKDIR}" ] &&
    [ "$(dirname "${WORKDIR}")" = "${TMP_ROOT}" ]
}

dump_diagnostics() {
  local container
  for container in "${CONTAINERS[@]}"; do
    if docker inspect "${container}" >/dev/null 2>&1; then
      echo "--- ${container} ---" >&2
      docker logs --tail=160 "${container}" >&2 || true
    fi
  done
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if [ -n "${READINESS_PROBE_PID:-}" ]; then
    kill "${READINESS_PROBE_PID}" >/dev/null 2>&1 || true
    wait "${READINESS_PROBE_PID}" >/dev/null 2>&1 || true
    READINESS_PROBE_PID=
  fi
  if [ "${status}" -ne 0 ]; then
    dump_diagnostics
  fi
  if [ "${KEEP}" = 1 ]; then
    echo "v11.9 state-sync fixture retained at ${WORKDIR}" >&2
    echo "containers: ${CONTAINERS[*]}" >&2
    exit "${status}"
  fi
  docker rm -f "${CONTAINERS[@]}" >/dev/null 2>&1 || true
  docker network rm "${P2P_NETWORK}" >/dev/null 2>&1 || true
  docker network rm "${RPC_NETWORK}" >/dev/null 2>&1 || true
  if validate_workdir; then
    # Container-owned Badger/Comet files may be mode 0700 on Linux. Remove only
    # this marker-validated run directory through the already-pinned local image.
    docker run --rm --pull never --network none \
      -v "${WORKDIR}:/owned" "${NODE_IMAGE}" sh -ec \
      'find /owned -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +' >/dev/null 2>&1 || true
    rmdir "${WORKDIR}" >/dev/null 2>&1 || true
  fi
  exit "${status}"
}
on_signal() {
  local status=$1
  trap - INT TERM
  exit "${status}"
}
trap cleanup EXIT
trap 'on_signal 130' INT
trap 'on_signal 143' TERM

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "ERROR: required command not found: $1" >&2
    exit 1
  }
}
require_command docker
require_command git
require_command python3

SOURCE_ID=$(python3 deploy/scripts/v11.9-source-id.py)
if [ "${#SOURCE_ID}" -ne 64 ]; then
  echo "ERROR: source identity is not a canonical SHA-256 digest" >&2
  exit 1
fi
case "${SOURCE_ID}" in
  *[!0-9a-f]*)
    echo "ERROR: source identity is not lowercase hexadecimal" >&2
    exit 1
    ;;
esac

if [ "${REBUILD}" = 1 ]; then
  docker build --target v119-state-sync-fixture \
    --build-arg "V119_STATE_SYNC_SOURCE_ID=${SOURCE_ID}" \
    -f deploy/Dockerfile.abci -t "${ABCI_IMAGE}" .
  docker build --build-arg "V119_STATE_SYNC_SOURCE_ID=${SOURCE_ID}" \
    -f deploy/Dockerfile.node -t "${NODE_IMAGE}" .
else
  docker image inspect "${ABCI_IMAGE}" >/dev/null
  docker image inspect "${NODE_IMAGE}" >/dev/null
fi
for image in "${ABCI_IMAGE}" "${NODE_IMAGE}"; do
  image_source_id=$(docker image inspect --format '{{ index .Config.Labels "dev.sage.v119-state-sync.source-id" }}' "${image}")
  if [ "${image_source_id}" != "${SOURCE_ID}" ]; then
    echo "ERROR: ${image} source identity ${image_source_id:-<missing>} does not match current tree ${SOURCE_ID}; rebuild with V119_STATE_SYNC_REBUILD=1" >&2
    exit 1
  fi
done
if ! docker run --rm --pull never --network none --entrypoint sh "${ABCI_IMAGE}" \
  -ec 'test -x /app/amid && test -x /app/sage-gui-v119-fixture'; then
  echo "ERROR: ${ABCI_IMAGE} is not the v119-state-sync-fixture target; rebuild with V119_STATE_SYNC_REBUILD=1" >&2
  exit 1
fi

docker network create --internal "${RPC_NETWORK}" >/dev/null
docker network create --internal "${P2P_NETWORK}" >/dev/null
mkdir -p "${PROVIDER_HOME}" "${OBSERVER_HOME}" "${RECEIVER_HOME}" "${SUCCESS_RECEIVER_HOME}" "${ATTACKER_HOME}"
chmod 0777 "${PROVIDER_HOME}" "${OBSERVER_HOME}" "${RECEIVER_HOME}" "${SUCCESS_RECEIVER_HOME}" "${ATTACKER_HOME}"

write_agent_key() {
  python3 - "$1" <<'PY'
import os
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
path.write_bytes(os.urandom(32))
path.chmod(0o644)  # isolated fixture; readable by the unprivileged container UID
PY
}

for home in "${PROVIDER_HOME}" "${OBSERVER_HOME}" "${RECEIVER_HOME}" "${SUCCESS_RECEIVER_HOME}" "${ATTACKER_HOME}"; do
  write_agent_key "${home}/agent.key"
done

write_provider_personal_config() {
  cat >"${PROVIDER_HOME}/config.yaml" <<'YAML'
embedding:
  provider: hash
  dimension: 768
encryption:
  enabled: false
quorum:
  enabled: false
federation:
  enabled: false
  p2p_enabled: false
  p2p_force_private: false
voter:
  enabled: true
  poll_interval: 500ms
data_dir: /sage/data
rest_addr: 0.0.0.0:8080
agent_key_file: /sage/agent.key
retain_blocks: -1
disable_auto_upgrade: false
YAML
  chmod 0644 "${PROVIDER_HOME}/config.yaml"
}

write_ordinary_config() {
  local home=$1
  local peer=${2:-}
  local voter=${3:-false}
  cat >"${home}/config.yaml" <<YAML
embedding:
  provider: hash
  dimension: 768
encryption:
  enabled: false
quorum:
  enabled: true
  peers:
YAML
  if [ -n "${peer}" ]; then
    printf '    - "%s"\n' "${peer}" >>"${home}/config.yaml"
  else
    printf '    []\n' >>"${home}/config.yaml"
  fi
  cat >>"${home}/config.yaml" <<YAML
  p2p_addr: tcp://0.0.0.0:26656
federation:
  enabled: false
  p2p_enabled: false
  p2p_force_private: false
voter:
  enabled: ${voter}
  poll_interval: 500ms
data_dir: /sage/data
rest_addr: 0.0.0.0:8080
agent_key_file: /sage/agent.key
retain_blocks: 0
disable_auto_upgrade: true
YAML
  chmod 0644 "${home}/config.yaml"
}

write_provider_serving_config() {
  local provider_id=$1
  local receiver_id=$2
  cat >"${PROVIDER_HOME}/config.yaml" <<YAML
embedding:
  provider: hash
  dimension: 768
encryption:
  enabled: false
quorum:
  enabled: true
  peers: []
  p2p_addr: tcp://0.0.0.0:26656
  state_sync:
    serving: true
    snapshot_dir: state-sync/snapshots
    authorization_file: /sage/state-sync-authorization.json
    authorized_peer_ids:
      - "${provider_id}"
      - "${receiver_id}"
    chunk_size: 65536
federation:
  enabled: false
  p2p_enabled: false
  p2p_force_private: false
voter:
  enabled: true
  poll_interval: 500ms
data_dir: /sage/data
rest_addr: 0.0.0.0:8080
agent_key_file: /sage/agent.key
retain_blocks: 0
disable_auto_upgrade: true
YAML
  chmod 0644 "${PROVIDER_HOME}/config.yaml"
}

write_receiving_config() {
  local home=$1
  local local_id=$2
  local provider_id=$3
  local trust_height=$4
  local trust_hash=$5
  local startup_timeout=$6
  cat >"${home}/config.yaml" <<YAML
embedding:
  provider: hash
  dimension: 768
encryption:
  enabled: false
quorum:
  enabled: true
  peers:
    - "${provider_id}@provider-p2p:26656"
  p2p_addr: tcp://0.0.0.0:26656
  state_sync:
    receiving: true
    snapshot_dir: state-sync/receiving
    authorization_file: /sage/state-sync-authorization.json
    authorized_peer_ids:
      - "${provider_id}"
      - "${local_id}"
    rpc_servers:
      - http://provider-rpc:26657
      - http://observer-rpc:26657
    trust_height: ${trust_height}
    trust_hash: "${trust_hash}"
    trust_period: 1h
    startup_timeout: "${startup_timeout}"
    chunk_size: 65536
federation:
  enabled: false
  p2p_enabled: false
  p2p_force_private: false
voter:
  enabled: false
data_dir: /sage/data
rest_addr: 0.0.0.0:8080
agent_key_file: /sage/agent.key
retain_blocks: 0
disable_auto_upgrade: true
YAML
  chmod 0644 "${home}/config.yaml"
}

create_sage() {
  local name=$1
  local home=$2
  local rpc_alias=$3
  local pre_publish_pause_file=
  if [ "${name}" = "${RECEIVER}" ]; then
    pre_publish_pause_file=${PRE_PUBLISH_MARKER}
  fi
  mkdir -p "${home}/tmp"
  chmod 0777 "${home}/tmp"
  docker create --name "${name}" \
    --network "${RPC_NETWORK}" --network-alias "${rpc_alias}" \
    -e SAGE_HOME=/sage \
    -e TMPDIR=/sage/tmp \
    -e SAGE_CMT_RPC_ADDR=tcp://0.0.0.0:26657 \
    -e SAGE_CMT_P2P_ADDR=tcp://0.0.0.0:26656 \
    -e SAGE_NO_BROWSER=1 \
    -e "SAGE_V119_STATE_SYNC_PRE_PUBLISH_PAUSE_FILE=${pre_publish_pause_file}" \
    -v "${home}:/sage" \
    "${ABCI_IMAGE}" ./sage-gui-v119-fixture serve >/dev/null
}

start_sage() {
  if ! docker inspect "$1" >/dev/null 2>&1; then
    create_sage "$1" "$2" "$3"
  fi
  docker start "$1" >/dev/null
}

stop_sage() {
  docker stop -t 25 "$1" >/dev/null
}

rpc_json() {
  local container=$1
  local path=$2
  docker exec "${container}" wget -qO- "http://127.0.0.1:26657${path}"
}

rpc_json_request() {
  local container=$1
  local request=$2
  docker exec "${container}" wget -qO- \
    --header='Content-Type: application/json' \
    --post-data="${request}" \
    http://127.0.0.1:26657/
}

rest_ready() {
  docker exec "$1" wget -qO- "http://127.0.0.1:8080/ready" >/dev/null 2>&1
}

rest_json() {
  docker exec "$1" wget -qO- "http://127.0.0.1:8080$2"
}

start_readiness_probe() {
  if [ -n "${READINESS_PROBE_PID:-}" ]; then
    echo "ERROR: receiver readiness probe is already running" >&2
    return 1
  fi
  docker exec "${RECEIVER}" sh -ec '
    violation=$1
    rm -f "${violation}"
    while :; do
      if wget -qO- http://127.0.0.1:8080/ready >/dev/null 2>&1; then
        printf "%s\n" "REST /ready opened before the durable pre-publication boundary" >"${violation}"
        exit 42
      fi
      sleep 1
    done
  ' sh "${READINESS_VIOLATION}" &
  READINESS_PROBE_PID=$!
}

stop_readiness_probe() {
  if [ -z "${READINESS_PROBE_PID:-}" ]; then
    return 0
  fi
  kill "${READINESS_PROBE_PID}" >/dev/null 2>&1 || true
  wait "${READINESS_PROBE_PID}" >/dev/null 2>&1 || true
  READINESS_PROBE_PID=
}

wait_pre_publish_marker() {
  local deadline=$((SECONDS + TIMEOUT))
  until docker exec "${RECEIVER}" test -s "${PRE_PUBLISH_MARKER}" >/dev/null 2>&1; do
    if [ "$(docker inspect --format '{{.State.Running}}' "${RECEIVER}" 2>/dev/null || true)" != true ]; then
      echo "ERROR: authorized receiver exited before the durable pre-publication boundary" >&2
      return 1
    fi
    if [ "${SECONDS}" -ge "${deadline}" ]; then
      echo "ERROR: authorized receiver did not reach the durable pre-publication boundary" >&2
      return 1
    fi
    sleep 1
  done
}

assert_scoped_projection_ready() {
  rest_json "$1" /ready | python3 -c '
import json
import sys

body = json.load(sys.stdin)
scoped = body.get("scoped_projection") or {}
status = body.get("status")
if status != "ready":
    raise SystemExit(f"node readiness is {status!r}")
want = {"checked": True, "required": True, "ok": True, "records": 1, "rebuilt": 1}
for key, value in want.items():
    if scoped.get(key) != value:
        raise SystemExit(f"scoped_projection.{key}={scoped.get(key)!r}, want {value!r}")
'
}

rpc_height() {
  rpc_json "$1" /status | python3 -c 'import json,sys; print(json.load(sys.stdin)["result"]["sync_info"]["latest_block_height"])'
}

rpc_status_app_hash() {
  rpc_json "$1" /status | python3 -c 'import json,sys; print(json.load(sys.stdin)["result"]["sync_info"]["latest_app_hash"].lower())'
}

rpc_latest_block_hash() {
  rpc_json "$1" /status | python3 -c 'import json,sys; print(json.load(sys.stdin)["result"]["sync_info"]["latest_block_hash"].lower())'
}

rpc_earliest_height() {
  rpc_json "$1" /status | python3 -c 'import json,sys; print(json.load(sys.stdin)["result"]["sync_info"]["earliest_block_height"])'
}

rpc_catching_up() {
  rpc_json "$1" /status | python3 -c 'import json,sys; print(str(json.load(sys.stdin)["result"]["sync_info"]["catching_up"]).lower())'
}

rpc_node_id() {
  rpc_json "$1" /status | python3 -c 'import json,sys; print(json.load(sys.stdin)["result"]["node_info"]["id"])'
}

rpc_voting_power() {
  rpc_json "$1" /status | python3 -c 'import json,sys; print(json.load(sys.stdin)["result"]["validator_info"]["voting_power"])'
}

assert_nonvalidator() {
  local container=$1
  local power
  power=$(rpc_voting_power "${container}")
  if [ "${power}" != 0 ]; then
    echo "ERROR: synchronized receiver ${container} has voting power ${power}; signed governance admission was not performed" >&2
    return 1
  fi
}

rpc_app_version() {
  rpc_json "$1" /abci_info | python3 -c 'import json,sys; print(json.load(sys.stdin)["result"]["response"]["app_version"])'
}

rpc_app_height() {
  rpc_json "$1" /abci_info | python3 -c 'import json,sys; print(json.load(sys.stdin)["result"]["response"]["last_block_height"])'
}

rpc_app_hash() {
  rpc_json "$1" /abci_info | python3 -c 'import base64,json,sys; print(base64.b64decode(json.load(sys.stdin)["result"]["response"]["last_block_app_hash"], validate=True).hex())'
}

rpc_block_hash() {
  local container=$1
  local height=$2
  rpc_json "${container}" "/block?height=${height}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["result"]["block_id"]["hash"].lower())'
}

is_canonical_hash() {
  local value=$1
  [ "${#value}" -eq 64 ] &&
    case "${value}" in
      *[!0-9a-f]*) false ;;
      *) true ;;
    esac
}

strip_ansi() {
  python3 -c 'import re,sys; sys.stdout.write(re.sub(r"\x1b\[[0-9;]*m", "", sys.stdin.read()))'
}

rpc_peer_ids() {
  rpc_json "$1" /net_info | python3 -c 'import json,sys; print(" ".join(sorted(p["node_info"]["id"] for p in json.load(sys.stdin)["result"]["peers"])))'
}

rpc_p2p_filter_code() {
  local container=$1
  local node_id=$2
  rpc_json "${container}" "/abci_query?path=%22%2Fp2p%2Ffilter%2Fid%2F${node_id}%22" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["result"]["response"].get("code", 0))'
}

authenticated_outbound_eofs() {
  local container=$1
  local peer_id=$2
  docker logs "${container}" 2>&1 |
    python3 -c 'import sys
peer_id = sys.argv[1]
print(sum(peer_id in line and " out}" in line and "err=EOF" in line for line in sys.stdin))
' "${peer_id}"
}

wait_rpc() {
  local container=$1
  local deadline=$((SECONDS + TIMEOUT))
  until rpc_json "${container}" /status >/dev/null 2>&1; do
    if [ "${SECONDS}" -ge "${deadline}" ]; then
      echo "ERROR: ${container} RPC did not become ready" >&2
      return 1
    fi
    sleep 1
  done
}

wait_rest() {
  local container=$1
  local deadline=$((SECONDS + TIMEOUT))
  until rest_ready "${container}"; do
    if [ "${SECONDS}" -ge "${deadline}" ]; then
      echo "ERROR: ${container} REST did not become ready" >&2
      return 1
    fi
    sleep 1
  done
}

wait_app_version() {
  local container=$1
  local want=$2
  local deadline=$((SECONDS + TIMEOUT))
  local got=0
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    got=$(rpc_app_version "${container}" 2>/dev/null || printf 0)
    if [ "${got}" = "${want}" ]; then
      return 0
    fi
    sleep 2
  done
  echo "ERROR: ${container} app version ${got}, want ${want}" >&2
  return 1
}

wait_height_at_least() {
  local container=$1
  local want=$2
  local deadline=$((SECONDS + TIMEOUT))
  local got=0
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    got=$(rpc_height "${container}" 2>/dev/null || printf 0)
    if [ "${got}" -ge "${want}" ]; then
      return 0
    fi
    sleep 1
  done
  echo "ERROR: ${container} height ${got}, want >= ${want}" >&2
  return 1
}

wait_convergence() {
  local left=$1
  local right=$2
  local deadline=$((SECONDS + TIMEOUT))
  local lh=0 rh=0 la=0 ra=0
  local left_app_hash="" right_app_hash="" left_block_hash="" right_block_hash=""
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    lh=$(rpc_height "${left}" 2>/dev/null || printf 0)
    rh=$(rpc_height "${right}" 2>/dev/null || printf 0)
    la=$(rpc_app_height "${left}" 2>/dev/null || printf 0)
    ra=$(rpc_app_height "${right}" 2>/dev/null || printf 0)
    left_app_hash=$(rpc_app_hash "${left}" 2>/dev/null || true)
    right_app_hash=$(rpc_app_hash "${right}" 2>/dev/null || true)
    left_block_hash=$(rpc_block_hash "${left}" "${lh}" 2>/dev/null || true)
    right_block_hash=$(rpc_block_hash "${right}" "${rh}" 2>/dev/null || true)
    if [ "${lh}" -gt 0 ] && [ "${rh}" -gt 0 ] &&
       [ "${la}" -gt 0 ] && [ "${ra}" -gt 0 ] &&
       [ "${lh}" = "${rh}" ] && [ "${la}" = "${lh}" ] && [ "${ra}" = "${rh}" ] &&
       [ "$(rpc_catching_up "${left}" 2>/dev/null || true)" = false ] &&
       [ "$(rpc_catching_up "${right}" 2>/dev/null || true)" = false ] &&
       is_canonical_hash "${left_app_hash}" &&
       is_canonical_hash "${right_app_hash}" &&
       [ "${left_app_hash}" = "${right_app_hash}" ] &&
       is_canonical_hash "${left_block_hash}" &&
       is_canonical_hash "${right_block_hash}" &&
       [ "${left_block_hash}" = "${right_block_hash}" ]; then
      return 0
    fi
    sleep 2
  done
  echo "ERROR: ${left}/${right} did not converge (rpc ${lh}/${rh}, app ${la}/${ra})" >&2
  return 1
}

init_pristine_comet_home() {
  local home=$1
  mkdir -p "${home}/data/cometbft"
  chmod 0777 "${home}/data" "${home}/data/cometbft"
  docker run --rm --pull never --network none \
    -v "${home}/data/cometbft:/cometbft" "${NODE_IMAGE}" \
    cometbft init --home /cometbft >/dev/null
  # The integrated fixture runs as uid/gid 100/101. Normalize only this owned
  # test home; the authorization trust root is created separately as 0644.
  docker run --rm --pull never --network none \
    -v "${home}/data/cometbft:/cometbft" "${NODE_IMAGE}" sh -ec \
    'chown -R 100:101 /cometbft; find /cometbft -type d -exec chmod 0700 {} +; chmod 0600 /cometbft/config/*.json /cometbft/data/priv_validator_state.json' >/dev/null
}

node_id_from_home() {
  docker run --rm --pull never --network none \
    -v "$1/data/cometbft:/cometbft:ro" "${NODE_IMAGE}" \
    cometbft show-node-id --home /cometbft
}

read_private_comet_json() {
  local home=$1
  local relative_path=$2
  case "${relative_path}" in
    config/genesis.json | config/priv_validator_key.json) ;;
    *)
      echo "ERROR: refusing unexpected private Comet JSON path ${relative_path}" >&2
      return 1
      ;;
  esac
  # These files deliberately remain mode 0600 and container-owned. Read them
  # through the root NODE_IMAGE helper so this works on Linux CI without ever
  # loosening private-validator key permissions for the host runner account.
  docker run --rm --pull never --network none \
    -v "${home}/data/cometbft:/cometbft:ro" "${NODE_IMAGE}" sh -ec \
    'cat "/cometbft/$1"' sh "${relative_path}"
}

validator_pubkey_from_home() {
  read_private_comet_json "$1" config/priv_validator_key.json |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["pub_key"]["value"])'
}

write_authorization() {
  local path=$1
  local chain_id=$2
  local joining_id=$3
  local validator_pubkey=$4
  local provider_id=$5
  local floor=$6
  local expiry
  expiry=$(python3 -c 'import datetime; print((datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(minutes=30)).isoformat().replace("+00:00","Z"))')
  cat >"${path}" <<JSON
{
  "chain_id": "${chain_id}",
  "joining_node_id": "${joining_id}",
  "validator_public_key": "${validator_pubkey}",
  "app_version": 20,
  "expires_at": "${expiry}",
  "snapshot_height_floor": ${floor},
  "validator_node_ids": ["${provider_id}"],
  "provider_node_ids": ["${provider_id}"]
}
JSON
  chmod 0644 "${path}"
}

copy_provider_genesis() {
  local target_home=$1
  docker run --rm --pull never --network none \
    -v "${PROVIDER_HOME}/data/cometbft/config/genesis.json:/source/genesis.json:ro" \
    -v "${target_home}/data/cometbft/config:/target" "${NODE_IMAGE}" sh -ec \
    'cp /source/genesis.json /target/genesis.json; chown 100:101 /target/genesis.json; chmod 0600 /target/genesis.json' >/dev/null
}

seed_memories() {
  local container=$1
  local file=$2
  docker exec "${container}" ./sage-gui-v119-fixture seed "${file}" --domain v119-state-sync >/dev/null
}

echo "=== v11.9 integrated authorized state-sync wire gate ==="
echo "source: ${SOURCE_ID} (both image labels verified before topology start)"
echo "fixture: one validator provider; observer/receiver/unauthorized peers are distinct non-validator full nodes"

# 1. Drive a fresh real chain through the signed app-v20 ladder.
write_provider_personal_config
cat >"${PROVIDER_HOME}/post-v20.txt" <<'EOF'
This committed post-app-v20 fixture record proves the provider snapshot is beyond the positive activation height.
EOF
cat >"${PROVIDER_HOME}/advance.txt" <<'EOF'
This first state-sync eligibility record advances the provider beyond the exported snapshot height.

This second state-sync eligibility record supplies additional committed blocks for the H plus two light-client window.
EOF
chmod 0644 "${PROVIDER_HOME}/post-v20.txt" "${PROVIDER_HOME}/advance.txt"

start_sage "${PROVIDER}" "${PROVIDER_HOME}" provider-rpc
docker network connect --alias provider-p2p "${P2P_NETWORK}" "${PROVIDER}"
wait_rpc "${PROVIDER}"
wait_rest "${PROVIDER}"
wait_app_version "${PROVIDER}" 20
pre_seed_height=$(rpc_height "${PROVIDER}")
seed_memories "${PROVIDER}" /sage/post-v20.txt
wait_height_at_least "${PROVIDER}" "$((pre_seed_height + 1))"
scoped_memory_id=$(docker exec "${PROVIDER}" ./sage-gui-v119-fixture \
  v119-state-sync-fixture install-scoped-proof)
if [ -z "${scoped_memory_id}" ]; then
  echo "ERROR: app-v20 governed scope fixture did not return a scoped memory ID" >&2
  exit 1
fi
echo "governed app-v20 scoped projection fixture committed as ${scoped_memory_id}"
stop_sage "${PROVIDER}"

chain_id=$(read_private_comet_json "${PROVIDER_HOME}" config/genesis.json |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["chain_id"])')
provider_id=$(node_id_from_home "${PROVIDER_HOME}")

# 2. Create three truly pristine, independently keyed full-node homes and copy
# only the provider's configured genesis. Their validator keys are not in it.
for home in "${OBSERVER_HOME}" "${RECEIVER_HOME}" "${SUCCESS_RECEIVER_HOME}" "${ATTACKER_HOME}"; do
  init_pristine_comet_home "${home}"
  copy_provider_genesis "${home}"
done
observer_id=$(node_id_from_home "${OBSERVER_HOME}")
receiver_id=$(node_id_from_home "${RECEIVER_HOME}")
success_receiver_id=$(node_id_from_home "${SUCCESS_RECEIVER_HOME}")
attacker_id=$(node_id_from_home "${ATTACKER_HOME}")
receiver_pubkey=$(validator_pubkey_from_home "${RECEIVER_HOME}")
success_receiver_pubkey=$(validator_pubkey_from_home "${SUCCESS_RECEIVER_HOME}")
attacker_pubkey=$(validator_pubkey_from_home "${ATTACKER_HOME}")

python3 - "${provider_id}" "${observer_id}" "${receiver_id}" "${success_receiver_id}" "${attacker_id}" <<'PY'
import sys

node_ids = sys.argv[1:]
if len(node_ids) != len(set(node_ids)):
    raise SystemExit("ERROR: fixture node IDs are not independent")
PY

# 3. Let the independent non-validator observer block-sync from genesis through
# integrated sage-gui. It becomes the second, genuinely independent RPC origin.
provider_peer_for_observer="${provider_id}@provider-p2p:26656"
observer_peer_for_provider="${observer_id}@observer-p2p:26656"
write_ordinary_config "${PROVIDER_HOME}" "${observer_peer_for_provider}" true
write_ordinary_config "${OBSERVER_HOME}" "${provider_peer_for_observer}" false
docker start "${PROVIDER}" >/dev/null
start_sage "${OBSERVER}" "${OBSERVER_HOME}" observer-rpc
docker network connect --alias observer-p2p "${P2P_NETWORK}" "${OBSERVER}"
wait_rpc "${PROVIDER}"
wait_rpc "${OBSERVER}"
wait_convergence "${PROVIDER}" "${OBSERVER}"
snapshot_height=$(rpc_height "${PROVIDER}")
snapshot_app_hash=$(rpc_app_hash "${PROVIDER}")
if [ "${snapshot_height}" -le 1 ]; then
  echo "ERROR: provider did not reach a positive post-app-v20 snapshot height" >&2
  exit 1
fi
if ! is_canonical_hash "${snapshot_app_hash}"; then
  echo "ERROR: provider snapshot app hash is not canonical" >&2
  exit 1
fi
stop_sage "${OBSERVER}"
stop_sage "${PROVIDER}"

write_authorization "${PROVIDER_HOME}/state-sync-authorization.json" "${chain_id}" \
  "${receiver_id}" "${receiver_pubkey}" "${provider_id}" "${snapshot_height}"
write_authorization "${RECEIVER_HOME}/state-sync-authorization.json" "${chain_id}" \
  "${receiver_id}" "${receiver_pubkey}" "${provider_id}" "${snapshot_height}"
write_authorization "${ATTACKER_HOME}/state-sync-authorization.json" "${chain_id}" \
  "${attacker_id}" "${attacker_pubkey}" "${provider_id}" "${snapshot_height}"

# 4. Boot the provider once in the authorized serving role to export H, then
# return provider+observer to ordinary mode and commit H+1/H+2. A final serving
# boot exports the tip too, but the catalog must expose only the old eligible H.
write_provider_serving_config "${provider_id}" "${receiver_id}"
docker start "${PROVIDER}" >/dev/null
wait_rpc "${PROVIDER}"
wait_rest "${PROVIDER}"
if ! export_height=$(rpc_height "${PROVIDER}"); then
  echo "ERROR: provider RPC failed while verifying the initial snapshot height" >&2
  exit 1
fi
if [ "${export_height}" -ne "${snapshot_height}" ]; then
  echo "ERROR: provider moved while exporting the initial snapshot" >&2
  exit 1
fi
stop_sage "${PROVIDER}"

write_ordinary_config "${PROVIDER_HOME}" "${observer_peer_for_provider}" true
docker start "${PROVIDER}" >/dev/null
docker start "${OBSERVER}" >/dev/null
wait_rest "${PROVIDER}"
wait_rpc "${OBSERVER}"
seed_memories "${PROVIDER}" /sage/advance.txt
wait_height_at_least "${PROVIDER}" "$((snapshot_height + 2))"
wait_convergence "${PROVIDER}" "${OBSERVER}"
latest_height=$(rpc_height "${PROVIDER}")
if [ "${latest_height}" -lt "$((snapshot_height + 2))" ]; then
  echo "ERROR: provider snapshot H=${snapshot_height} is not H+2 eligible at ${latest_height}" >&2
  exit 1
fi
stop_sage "${OBSERVER}"
stop_sage "${PROVIDER}"

write_provider_serving_config "${provider_id}" "${receiver_id}"
docker start "${PROVIDER}" >/dev/null
# Observer is RPC-only during transfer: no provider alias and no signing role.
docker network disconnect "${P2P_NETWORK}" "${OBSERVER}" >/dev/null
docker start "${OBSERVER}" >/dev/null
wait_rpc "${PROVIDER}"
wait_rest "${PROVIDER}"
wait_rpc "${OBSERVER}"

if [ "$(rpc_node_id "${PROVIDER}")" != "${provider_id}" ] ||
   [ "$(rpc_node_id "${OBSERVER}")" != "${observer_id}" ]; then
  echo "ERROR: light-client RPC origins do not belong to the independent expected nodes" >&2
  exit 1
fi
for height in "${snapshot_height}" "$((snapshot_height + 1))" "$((snapshot_height + 2))"; do
  provider_hash=$(rpc_block_hash "${PROVIDER}" "${height}")
  observer_hash=$(rpc_block_hash "${OBSERVER}" "${height}")
  if [ -z "${provider_hash}" ] || [ "${provider_hash}" != "${observer_hash}" ]; then
    echo "ERROR: independent RPC origins disagree at light block ${height}" >&2
    exit 1
  fi
done
trust_hash=$(rpc_block_hash "${PROVIDER}" "${snapshot_height}")

# 5. Both receiver attempts can reach the two independent RPC origins. Give
# provider-p2p a real DNS answer backed by a deliberately closed port first:
# peer-profile validation succeeds and Comet RPC comes up, while normal REST
# remains unbound behind the seal. Then swap that placeholder for the provider.
write_receiving_config "${RECEIVER_HOME}" "${receiver_id}" "${provider_id}" \
  "${snapshot_height}" "${trust_hash}" 3m
write_receiving_config "${ATTACKER_HOME}" "${attacker_id}" "${provider_id}" \
  "${snapshot_height}" "${trust_hash}" 45s
docker network disconnect "${P2P_NETWORK}" "${PROVIDER}" >/dev/null
docker run -d --pull never --name "${P2P_PLACEHOLDER}" \
  --network "${P2P_NETWORK}" --network-alias provider-p2p \
  "${NODE_IMAGE}" sh -ec 'exec tail -f /dev/null' >/dev/null
create_sage "${RECEIVER}" "${RECEIVER_HOME}" receiver-rpc
create_sage "${ATTACKER}" "${ATTACKER_HOME}" unauthorized-rpc
docker network connect --alias receiver-p2p "${P2P_NETWORK}" "${RECEIVER}"
docker network connect --alias unauthorized-p2p "${P2P_NETWORK}" "${ATTACKER}"
docker start "${RECEIVER}" "${ATTACKER}" >/dev/null
wait_rpc "${RECEIVER}"
wait_rpc "${ATTACKER}"
start_readiness_probe
sleep 2
for candidate in "${RECEIVER}" "${ATTACKER}"; do
  if ! docker exec "${candidate}" busybox nslookup provider-p2p >/dev/null 2>&1; then
    echo "ERROR: ${candidate} could not resolve the closed provider-p2p placeholder" >&2
    exit 1
  fi
  if docker exec "${candidate}" busybox nc -z -w 1 provider-p2p 26656; then
    echo "ERROR: ${candidate} reached a P2P listener before provider exposure" >&2
    exit 1
  fi
done
if rest_ready "${RECEIVER}" || rest_ready "${ATTACKER}"; then
  echo "ERROR: an unsealed receiver exposed REST before its authorized P2P path existed" >&2
  exit 1
fi

# Connect the unauthorized node first. Its own independently valid local ticket
# approves the provider, but the provider's reciprocal profile excludes this
# joining ID from the exact unconditional capacity set. With ordinary inbound
# capacity zero, Comet rejects it after authentication and before addPeer; the
# explicit live ABCI query below separately proves FilterPeers returns Code 111.
# An outbound switch may transiently list its locally approved provider before
# that remote admission rejection and EOF arrive.
docker network disconnect "${P2P_NETWORK}" "${RECEIVER}" >/dev/null
docker rm -f "${P2P_PLACEHOLDER}" >/dev/null
authenticated_eofs_before=$(authenticated_outbound_eofs "${ATTACKER}" "${provider_id}")
docker network connect --alias provider-p2p "${P2P_NETWORK}" "${PROVIDER}"
tcp_deadline=$((SECONDS + 15))
until docker exec "${ATTACKER}" busybox nc -z -w 2 provider-p2p 26656; do
  if [ "${SECONDS}" -ge "${tcp_deadline}" ]; then
    echo "ERROR: unauthorized fixture did not have a real DNS/TCP path to provider P2P" >&2
    exit 1
  fi
  sleep 1
done
# Reset the persistent-peer reconnect backoff after replacing the closed
# placeholder. sage-gui intentionally emits only Comet Error logs, so observe
# the attacker's exact authenticated provider-ID outbound EOF rather than the
# provider's Info-level zero-capacity line before counting denial samples.
stop_sage "${ATTACKER}"
docker start "${ATTACKER}" >/dev/null
wait_rpc "${ATTACKER}"
rejection_deadline=$((SECONDS + 15))
while :; do
  authenticated_eofs_after=$(authenticated_outbound_eofs "${ATTACKER}" "${provider_id}")
  if [ "${authenticated_eofs_after}" -gt "${authenticated_eofs_before}" ]; then
    break
  fi
  if [ "${SECONDS}" -ge "${rejection_deadline}" ]; then
    echo "ERROR: unauthorized receiver did not observe the provider's authenticated rejection EOF" >&2
    exit 1
  fi
  sleep 1
done
# Consume the complete log stream. With `pipefail`, grep -q may close early on
# a successful match and turn docker logs' resulting SIGPIPE into a false
# negative.
if ! docker logs "${ATTACKER}" 2>&1 | grep -F 'authorized one-shot validator state-sync receiver armed' >/dev/null; then
  echo "ERROR: unauthorized fixture failed before its independently valid local receiver authorization armed" >&2
  exit 1
fi
attacker_filter_code=$(rpc_p2p_filter_code "${PROVIDER}" "${attacker_id}" 2>/dev/null || true)
receiver_filter_code=$(rpc_p2p_filter_code "${PROVIDER}" "${receiver_id}" 2>/dev/null || true)
if [ "${attacker_filter_code}" != 111 ] || [ "${receiver_filter_code}" != 0 ]; then
  echo "ERROR: live provider P2P filter returned attacker/receiver codes ${attacker_filter_code:-<missing>}/${receiver_filter_code:-<missing>}, want 111/0" >&2
  exit 1
fi

attack_samples=0
attack_deadline=$((SECONDS + 8))
while [ "${SECONDS}" -lt "${attack_deadline}" ]; do
  if ! provider_peer_ids=$(rpc_peer_ids "${PROVIDER}"); then
    echo "ERROR: provider /net_info failed during unauthorized-peer proof" >&2
    exit 1
  fi
  if [ -n "${provider_peer_ids}" ]; then
    echo "ERROR: provider accepted a peer before the authorized receiver connected: ${provider_peer_ids}" >&2
    exit 1
  fi
  attacker_height=$(rpc_height "${ATTACKER}" 2>/dev/null || printf unavailable)
  if [ "${attacker_height}" != 0 ]; then
    echo "ERROR: unauthorized receiver advanced to height ${attacker_height}" >&2
    exit 1
  fi
  if docker logs "${ATTACKER}" 2>&1 | grep -F 'authorized state-sync session assembled and app-v20 candidate verified' >/dev/null; then
    echo "ERROR: unauthorized receiver assembled a state-sync session" >&2
    exit 1
  fi
  if rest_ready "${ATTACKER}"; then
    echo "ERROR: unauthorized receiver reached normal serving" >&2
    exit 1
  fi
  attack_samples=$((attack_samples + 1))
  sleep 1
done
if [ "${attack_samples}" -lt 5 ]; then
  echo "ERROR: unauthorized-peer denial proof collected only ${attack_samples} samples" >&2
  exit 1
fi

# Stop the persistent redial path, then require both switches to drain before
# introducing the authorized receiver. This distinguishes a transient local
# outbound entry from an accepted provider-side session without racing the
# approved transfer against stale transport state.
docker network disconnect "${P2P_NETWORK}" "${ATTACKER}" >/dev/null
drain_deadline=$((SECONDS + 15))
while :; do
  provider_peer_ids=$(rpc_peer_ids "${PROVIDER}" 2>/dev/null || printf unavailable)
  attacker_peer_ids=$(rpc_peer_ids "${ATTACKER}" 2>/dev/null || printf unavailable)
  if [ -z "${provider_peer_ids}" ] && [ -z "${attacker_peer_ids}" ]; then
    break
  fi
  if [ "${SECONDS}" -ge "${drain_deadline}" ]; then
    echo "ERROR: unauthorized transport did not drain (provider=${provider_peer_ids:-<empty>} attacker=${attacker_peer_ids:-<empty>})" >&2
    exit 1
  fi
  sleep 1
done
stop_sage "${ATTACKER}"

docker network connect --alias receiver-p2p "${P2P_NETWORK}" "${RECEIVER}"
wait_pre_publish_marker
if rest_ready "${RECEIVER}"; then
  echo "ERROR: receiver exposed REST while its durable activation was still unpublished" >&2
  exit 1
fi
if docker exec "${RECEIVER}" test -e "${READINESS_VIOLATION}"; then
  echo "ERROR: concurrent readiness probe observed REST before runtime publication" >&2
  exit 1
fi
if [ "$(docker exec "${RECEIVER}" ./sage-gui-v119-fixture v119-state-sync-fixture receiving)" != false ]; then
  echo "ERROR: durable activation did not disarm the one-shot receiver role before publication" >&2
  exit 1
fi

# The runtime remains intentionally paused behind its exclusive activation gate
# here. Its fsynced fixture marker carries the exact tuple that production code
# has already matched across the activated app, Comet StateStore, +2/3 seen
# commit, and block-sync handoff. Upstream Comet `/status` is BlockStore-derived,
# so it must still be empty and catching up until H+1 is materialized after the
# restart below; `/block?height=H` must likewise be unavailable. Do not call
# `/abci_info` while the runtime write lease is held: it is expected to wait
# behind the same gate we are proving.
pre_publish_evidence=$(docker exec "${RECEIVER}" cat "${PRE_PUBLISH_MARKER}")
python3 - "${snapshot_height}" "${snapshot_app_hash}" 20 "${pre_publish_evidence}" <<'PY'
import json
import sys

want_height = int(sys.argv[1])
want_hash = sys.argv[2]
want_version = int(sys.argv[3])
try:
    evidence = json.loads(sys.argv[4])
except (json.JSONDecodeError, TypeError) as exc:
    raise SystemExit(f"invalid pre-publication evidence: {exc}") from exc
want = {"height": want_height, "app_hash": want_hash, "app_version": want_version}
if evidence != want:
    raise SystemExit(f"pre-publication evidence {evidence!r}, want {want!r}")
PY
receiver_rpc_height=$(rpc_height "${RECEIVER}" 2>/dev/null || printf 0)
receiver_status_app_hash=$(rpc_status_app_hash "${RECEIVER}" 2>/dev/null || true)
receiver_block_hash=$(rpc_latest_block_hash "${RECEIVER}" 2>/dev/null || true)
receiver_earliest_height=$(rpc_earliest_height "${RECEIVER}" 2>/dev/null || printf 0)
receiver_catching_up=$(rpc_catching_up "${RECEIVER}" 2>/dev/null || true)
receiver_h_block_response=$(rpc_json_request "${RECEIVER}" \
  "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"block\",\"params\":{\"height\":\"${snapshot_height}\"}}" 2>&1 || true)
receiver_h_block_state=$(python3 - "${snapshot_height}" "${receiver_h_block_response}" <<'PY'
import json
import sys

try:
    want_height = int(sys.argv[1])
    body = json.loads(sys.argv[2])
except (json.JSONDecodeError, TypeError):
    print("invalid")
    raise SystemExit
result = body.get("result")
if isinstance(result, dict) and isinstance(result.get("block_id"), dict):
    print("available")
elif body.get("error") == {
    "code": -32603,
    "message": "Internal error",
    "data": f"height {want_height} must be less than or equal to the current blockchain height 0",
}:
    print("unavailable")
else:
    print("invalid")
PY
)
if [ "${receiver_rpc_height}" != 0 ] ||
   [ -n "${receiver_status_app_hash}" ] ||
   [ -n "${receiver_block_hash}" ] ||
   [ "${receiver_earliest_height}" != 0 ] ||
   [ "${receiver_catching_up}" != true ] ||
   [ "${receiver_h_block_state}" != unavailable ]; then
  echo "ERROR: paused receiver BlockStore is not the expected empty/catching-up state" >&2
  echo "observed status: latest_height=${receiver_rpc_height:-<empty>} latest_app_hash=${receiver_status_app_hash:-<empty>} latest_block_hash=${receiver_block_hash:-<empty>} earliest_height=${receiver_earliest_height:-<empty>} catching_up=${receiver_catching_up:-<empty>} H_block=${receiver_h_block_state:-<empty>}" >&2
  echo "H=${snapshot_height} block response: ${receiver_h_block_response:-<empty>}" >&2
  exit 1
fi

provider_peers=$(rpc_peer_ids "${PROVIDER}")
if [ "${provider_peers}" != "${receiver_id}" ]; then
  echo "ERROR: provider peer set ${provider_peers:-<empty>}, want exactly authorized receiver ${receiver_id}" >&2
  exit 1
fi
if rest_ready "${ATTACKER}"; then
  echo "ERROR: unauthorized receiver exposed REST after the authorized transfer" >&2
  exit 1
fi

session_line=$(docker logs "${RECEIVER}" 2>&1 | grep -n 'authorized state-sync session assembled and app-v20 candidate verified' | tail -1 | cut -d: -f1)
session_height=$(docker logs "${RECEIVER}" 2>&1 | grep 'authorized state-sync session assembled and app-v20 candidate verified' | tail -1 | strip_ansi | sed -n 's/.*height=\([0-9][0-9]*\).*/\1/p')
if [ -z "${session_line}" ] || [ "${session_height}" != "${snapshot_height}" ]; then
  echo "ERROR: receiver session height ${session_height:-<unknown>}, want H+2-eligible ${snapshot_height}" >&2
  exit 1
fi

# The fixture hook is after the durable journal/config/directory completion but
# before the runtime publishes Sealed. Kill exactly there, then prove the raw
# config disarm makes the same process boot as an ordinary synchronized node
# without an operator rewriting its configuration.
docker kill --signal KILL "${RECEIVER}" >/dev/null
stop_readiness_probe
docker start "${RECEIVER}" >/dev/null
wait_rest "${RECEIVER}"
if [ "$(docker exec "${RECEIVER}" ./sage-gui-v119-fixture v119-state-sync-fixture receiving)" != false ]; then
  echo "ERROR: receiver re-armed its completed one-shot role after pre-publication SIGKILL" >&2
  exit 1
fi
if docker exec "${RECEIVER}" test -e "${READINESS_VIOLATION}"; then
  echo "ERROR: readiness probe recorded premature serving before pre-publication SIGKILL" >&2
  exit 1
fi
assert_scoped_projection_ready "${RECEIVER}"
docker exec "${RECEIVER}" ./sage-gui-v119-fixture \
  v119-state-sync-fixture verify-scoped-projection "${scoped_memory_id}"
wait_convergence "${PROVIDER}" "${RECEIVER}"
assert_nonvalidator "${RECEIVER}"
receiver_app_version=$(rpc_app_version "${RECEIVER}" 2>/dev/null || true)
receiver_app_hash=$(rpc_app_hash "${RECEIVER}" 2>/dev/null || true)
provider_app_hash=$(rpc_app_hash "${PROVIDER}" 2>/dev/null || true)
if [ "${receiver_app_version}" != 20 ] ||
   ! is_canonical_hash "${receiver_app_hash}" ||
   ! is_canonical_hash "${provider_app_hash}" ||
   [ "${receiver_app_hash}" != "${provider_app_hash}" ]; then
  echo "ERROR: restarted receiver did not activate and converge on exact app-v20 provider state" >&2
  exit 1
fi

ready_line=$(docker logs "${RECEIVER}" 2>&1 | grep -n 'SAGE Personal ready' | tail -1 | cut -d: -f1)
if [ -z "${ready_line}" ] || [ "${session_line}" -ge "${ready_line}" ]; then
  echo "ERROR: post-crash REST admission did not follow authorized session verification" >&2
  exit 1
fi

earliest=$(rpc_earliest_height "${RECEIVER}")
echo "authorized receiver durably completed H=${snapshot_height}, survived pre-publication SIGKILL, caught up to ${latest_height}, earliest retained block=${earliest}"

# 6. Re-authorize a second pristine non-validator and let this one complete the
# non-crashing runtime publication path. This preserves the exact
# session -> sealed -> REST log-order proof separately from the receiver killed
# at the durable pre-publication boundary above.
stop_sage "${RECEIVER}"
docker network disconnect "${P2P_NETWORK}" "${RECEIVER}" >/dev/null 2>&1 || true
stop_sage "${PROVIDER}"
write_authorization "${PROVIDER_HOME}/state-sync-authorization.json" "${chain_id}" \
  "${success_receiver_id}" "${success_receiver_pubkey}" "${provider_id}" "${snapshot_height}"
write_authorization "${SUCCESS_RECEIVER_HOME}/state-sync-authorization.json" "${chain_id}" \
  "${success_receiver_id}" "${success_receiver_pubkey}" "${provider_id}" "${snapshot_height}"
write_provider_serving_config "${provider_id}" "${success_receiver_id}"
write_receiving_config "${SUCCESS_RECEIVER_HOME}" "${success_receiver_id}" "${provider_id}" \
  "${snapshot_height}" "${trust_hash}" 3m
docker start "${PROVIDER}" >/dev/null
wait_rpc "${PROVIDER}"
wait_rest "${PROVIDER}"
create_sage "${SUCCESS_RECEIVER}" "${SUCCESS_RECEIVER_HOME}" success-receiver-rpc
docker network connect --alias success-receiver-p2p "${P2P_NETWORK}" "${SUCCESS_RECEIVER}"
docker start "${SUCCESS_RECEIVER}" >/dev/null
wait_rpc "${SUCCESS_RECEIVER}"
wait_rest "${SUCCESS_RECEIVER}"
wait_convergence "${PROVIDER}" "${SUCCESS_RECEIVER}"
assert_nonvalidator "${SUCCESS_RECEIVER}"

if [ "$(docker exec "${SUCCESS_RECEIVER}" ./sage-gui-v119-fixture v119-state-sync-fixture receiving)" != false ]; then
  echo "ERROR: successful receiver did not durably disarm its one-shot role" >&2
  exit 1
fi
assert_scoped_projection_ready "${SUCCESS_RECEIVER}"
docker exec "${SUCCESS_RECEIVER}" ./sage-gui-v119-fixture \
  v119-state-sync-fixture verify-scoped-projection "${scoped_memory_id}"
if [ "$(rpc_peer_ids "${PROVIDER}")" != "${success_receiver_id}" ]; then
  echo "ERROR: provider peer set is not exactly the successful authorized receiver" >&2
  exit 1
fi

success_receiver_app_version=$(rpc_app_version "${SUCCESS_RECEIVER}" 2>/dev/null || true)
success_receiver_app_hash=$(rpc_app_hash "${SUCCESS_RECEIVER}" 2>/dev/null || true)
provider_app_hash=$(rpc_app_hash "${PROVIDER}" 2>/dev/null || true)
if [ "${success_receiver_app_version}" != 20 ] ||
   ! is_canonical_hash "${success_receiver_app_hash}" ||
   ! is_canonical_hash "${provider_app_hash}" ||
   [ "${success_receiver_app_hash}" != "${provider_app_hash}" ]; then
  echo "ERROR: successful receiver did not publish exact app-v20 provider state" >&2
  exit 1
fi

success_session_line=$(docker logs "${SUCCESS_RECEIVER}" 2>&1 | grep -n 'authorized state-sync session assembled and app-v20 candidate verified' | tail -1 | cut -d: -f1)
success_seal_line=$(docker logs "${SUCCESS_RECEIVER}" 2>&1 | grep -n 'authorized validator state-sync activation sealed before service admission' | tail -1 | cut -d: -f1)
success_ready_line=$(docker logs "${SUCCESS_RECEIVER}" 2>&1 | grep -n 'SAGE Personal ready' | tail -1 | cut -d: -f1)
success_session_height=$(docker logs "${SUCCESS_RECEIVER}" 2>&1 | grep 'authorized state-sync session assembled and app-v20 candidate verified' | tail -1 | strip_ansi | sed -n 's/.*height=\([0-9][0-9]*\).*/\1/p')
success_sealed_height=$(docker logs "${SUCCESS_RECEIVER}" 2>&1 | grep 'authorized validator state-sync activation sealed before service admission' | tail -1 | strip_ansi | sed -n 's/.*height=\([0-9][0-9]*\).*/\1/p')
if [ -z "${success_session_line}" ] || [ -z "${success_seal_line}" ] || [ -z "${success_ready_line}" ] ||
   [ "${success_session_line}" -ge "${success_seal_line}" ] || [ "${success_seal_line}" -ge "${success_ready_line}" ]; then
  echo "ERROR: successful receiver did not log session verification, seal, and REST admission in order" >&2
  exit 1
fi
if [ "${success_session_height}" != "${snapshot_height}" ] || [ "${success_sealed_height}" != "${snapshot_height}" ]; then
  echo "ERROR: successful receiver session/seal heights ${success_session_height:-<unknown>}/${success_sealed_height:-<unknown>}, want ${snapshot_height}" >&2
  exit 1
fi

success_earliest=$(rpc_earliest_height "${SUCCESS_RECEIVER}")
echo "successful receiver published sealed H=${snapshot_height}, earliest retained block=${success_earliest}"

# 7. A successfully synchronized node is still a non-validator until a later
# governance action adds its Ed25519 validator key. Crash/restart the live
# provider and require a fresh committed block to converge exactly again.
docker kill --signal KILL "${PROVIDER}" >/dev/null
docker start "${PROVIDER}" >/dev/null
wait_rest "${PROVIDER}"
before_restart_seed=$(rpc_height "${PROVIDER}")
cat >"${PROVIDER_HOME}/restart.txt" <<'EOF'
This post-restart committed record proves the synchronized non-validator full node resumes ordinary block catch-up.
EOF
chmod 0644 "${PROVIDER_HOME}/restart.txt"
seed_memories "${PROVIDER}" /sage/restart.txt
wait_height_at_least "${PROVIDER}" "$((before_restart_seed + 1))"
wait_convergence "${PROVIDER}" "${SUCCESS_RECEIVER}"
assert_nonvalidator "${SUCCESS_RECEIVER}"

if [ "$(rpc_peer_ids "${PROVIDER}")" != "${success_receiver_id}" ]; then
  echo "ERROR: exact authorized provider peer set changed after crash/restart" >&2
  exit 1
fi

completion_source_id=$(python3 deploy/scripts/v11.9-source-id.py)
if [ "${completion_source_id}" != "${SOURCE_ID}" ]; then
  echo "ERROR: source tree changed during the cold gate (${SOURCE_ID} -> ${completion_source_id}); rerun from one frozen tree" >&2
  exit 1
fi
for image in "${ABCI_IMAGE}" "${NODE_IMAGE}"; do
  completion_image_source_id=$(docker image inspect --format '{{ index .Config.Labels "dev.sage.v119-state-sync.source-id" }}' "${image}")
  if [ "${completion_image_source_id}" != "${SOURCE_ID}" ]; then
    echo "ERROR: ${image} source identity changed during the cold gate (${SOURCE_ID} -> ${completion_image_source_id:-<missing>})" >&2
    exit 1
  fi
done

echo "=== v11.9 AUTHORIZED STATE-SYNC WIRE GATE PASSED ==="
echo "PASS: frozen source ${SOURCE_ID}; real signed app-v20 scope+memory; exact projection rebuild; independent provider+observer light origins; H+2 snapshot; exact P2P authorization; approved-sender sessions; concurrent seal-before-REST proof; unauthorized rejection; receiver pre-publication SIGKILL with automatic ordinary restart; separate session<seal<REST completion; provider SIGKILL; block/AppHash convergence"
echo "ROLE: receiver is intentionally a synchronized NON-VALIDATOR full node; validator-set admission remains a separate signed governance action"
