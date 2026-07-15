#!/usr/bin/env bash
#
# Real multi-process v11.9 fault gate over an isolated four-validator
# CometBFT + ABCI Docker network.
#
# Proves, with bounded waits and matched-height AppHash checks:
#   1. SIGKILL of a complete validator process pair (Comet + ABCI), restart,
#      block catch-up, and AppHash convergence.
#   2. A live Comet process isolated by a P2P-only firewall while 3/4
#      continues, then firewall removal/catch-up/AppHash convergence.
#   3. A stable-IP 2+2 P2P firewall partition does not relax consensus:
#      both live halves stall until the firewall heals, then resume and
#      converge on the same block and application state.
#
# It does not claim ABCI state-sync endpoint or app-v20 scoped-reconfiguration
# coverage. The fresh deploy testnet starts below app-v20. Set
# V119_REQUIRE_SCOPED_RECONFIG=1 and/or
# V119_REQUIRE_AUTHORIZED_STATE_SYNC=1 to make either missing proof a hard,
# explicit failure instead of silently treating the topology proof as enough.

set -euo pipefail
cd "$(dirname "$0")/../.."

PROJECT=sage-v119-chaos
NETWORK=${PROJECT}_sagenet
FIREWALL_CHAIN=SAGE_V119
P2P_PORT=26656
P2P_TCP_RETRIES2=3
COMETBFT_SOURCE_COMMIT=feb2aea4dc271d612129afc958cb844713ec792b
COMETBFT_RUNTIME_VERSION="0.38.22+${COMETBFT_SOURCE_COMMIT}"
# Compose interpolates the required password for every command, not only `up`.
# Export one isolated test value so kill/start/ps/logs/down use the same complete
# project configuration on clean CI runners that have no deploy/.env file.
export POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-ci_test_password}
COMPOSE=(docker compose -p "${PROJECT}"
  -f deploy/docker-compose.yml
  -f deploy/docker-compose.test.yml
  -f deploy/docker-compose.v119-chaos.yml)
RPC_PORTS=(37657 37757 37857 37957)
REBUILD=${V119_CHAOS_REBUILD:-1}
KEEP=${V119_CHAOS_KEEP:-0}
CHAOS_WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/sage-v119-chaos.XXXXXX")
CHAOS_WORKDIR=$(cd "${CHAOS_WORKDIR}" && pwd -P)
CHAOS_WORKDIR_MARKER="${CHAOS_WORKDIR}/.sage-testnet-genesis-owner"
touch "${CHAOS_WORKDIR_MARKER}"
export V119_CHAOS_GENESIS_DIR="${CHAOS_WORKDIR}/genesis"
export V119_CHAOS_DATA_DIR="${CHAOS_WORKDIR}/data"
mkdir -p "${V119_CHAOS_DATA_DIR}/postgres"
for index in 0 1 2 3; do
  mkdir -p "${V119_CHAOS_DATA_DIR}/abci${index}"
  # The temp parent is mode 0700. World-writable child directories let the
  # fixed non-root ABCI UID write through Docker bind mounts on Linux/macOS
  # without granting access through the host parent.
  chmod 0777 "${V119_CHAOS_DATA_DIR}/abci${index}"
done

dump_diagnostics() {
  "${COMPOSE[@]}" ps -a || true
  "${COMPOSE[@]}" logs --tail=120 postgres cometbft0 cometbft1 cometbft2 cometbft3 abci0 abci1 abci2 abci3 || true
}

cleanup() {
  rc=$?
  trap - EXIT INT TERM
  if [ "${rc}" -ne 0 ]; then
    echo "--- v11.9 chaos gate failed; diagnostics follow ---"
    dump_diagnostics
  fi
  if [ "${KEEP}" = "1" ]; then
    echo "--- V119_CHAOS_KEEP=1: leaving ${PROJECT} running ---"
    echo "--- isolated genesis retained at ${V119_CHAOS_GENESIS_DIR} ---"
  else
    cleanup_failed=0
    if ! "${COMPOSE[@]}" down -v --remove-orphans; then
      echo "ERROR: ${PROJECT} teardown failed; retaining ${CHAOS_WORKDIR} for recovery" >&2
      cleanup_failed=1
    fi

    remaining=
    if [ "${cleanup_failed}" = "0" ]; then
      if ! remaining=$("${COMPOSE[@]}" ps -a -q 2>/dev/null); then
        echo "ERROR: could not verify ${PROJECT} teardown; retaining ${CHAOS_WORKDIR}" >&2
        cleanup_failed=1
      elif [ -n "${remaining}" ]; then
        echo "ERROR: ${PROJECT} still has containers after teardown (${remaining}); retaining ${CHAOS_WORKDIR}" >&2
        cleanup_failed=1
      fi
    fi

    if [ "${cleanup_failed}" = "0" ]; then
      if [ ! -f "${CHAOS_WORKDIR_MARKER}" ] || [ -L "${CHAOS_WORKDIR_MARKER}" ]; then
        echo "ERROR: refusing to remove unowned chaos work directory ${CHAOS_WORKDIR}" >&2
        cleanup_failed=1
      elif ! rm -rf -- "${CHAOS_WORKDIR}"; then
        echo "ERROR: failed to remove chaos work directory ${CHAOS_WORKDIR}" >&2
        cleanup_failed=1
      fi
    fi

    if [ "${cleanup_failed}" != "0" ]; then
      echo "ERROR: cleanup incomplete; isolated genesis retained at ${V119_CHAOS_GENESIS_DIR}" >&2
      if [ "${rc}" -eq 0 ]; then
        rc=1
      fi
    fi
  fi
  exit "${rc}"
}
trap cleanup EXIT INT TERM

rpc_json() {
  port=$1
  path=$2
  curl -fsS --max-time 3 "http://127.0.0.1:${port}${path}"
}

rpc_height() {
  rpc_json "$1" /status | python3 -c 'import json,sys; print(int(json.load(sys.stdin)["result"]["sync_info"]["latest_block_height"]))'
}

rpc_app_version() {
  rpc_json "$1" /abci_info | python3 -c 'import json,sys; print(int(json.load(sys.stdin)["result"]["response"]["app_version"]))'
}

rpc_status_tuple() {
  rpc_json "$1" /status | python3 -c '
import json, sys
sync = json.load(sys.stdin)["result"]["sync_info"]
height = int(sync["latest_block_height"])
catching_up = str(bool(sync["catching_up"])).lower()
block_hash = sync.get("latest_block_hash") or "EMPTY"
print(f"{height}|{catching_up}|{block_hash}")'
}

rpc_abci_tuple() {
  rpc_json "$1" /abci_info | python3 -c '
import json, sys
response = json.load(sys.stdin)["result"]["response"]
height = int(response["last_block_height"])
app_hash = response["last_block_app_hash"] or "EMPTY"
print(f"{height}|{app_hash}")'
}

rpc_node_id() {
  rpc_json "$1" /status | python3 -c '
import json, sys
node_id = str(json.load(sys.stdin)["result"]["node_info"]["id"]).lower()
if len(node_id) != 40 or any(c not in "0123456789abcdef" for c in node_id):
    raise SystemExit("invalid CometBFT node ID")
print(node_id)'
}

rpc_peer_ids() {
  rpc_json "$1" /net_info | python3 -c '
import json, sys
peers = json.load(sys.stdin)["result"]["peers"]
ids = sorted(str(peer["node_info"]["id"]).lower() for peer in peers)
print(",".join(ids) if ids else "NONE")'
}

expected_peer_ids() {
  if [ "$#" -eq 0 ]; then
    echo NONE
    return 0
  fi
  python3 -c 'import sys; print(",".join(sorted(sys.argv[1:])))' "$@"
}

wait_rpc() {
  port=$1
  timeout=${2:-120}
  deadline=$((SECONDS + timeout))
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    if rpc_height "${port}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "ERROR: RPC ${port} did not become ready within ${timeout}s" >&2
  return 1
}

wait_all_rpc() {
  timeout=${1:-180}
  for port in "${RPC_PORTS[@]}"; do
    wait_rpc "${port}" "${timeout}"
  done
}

wait_progress() {
  port=$1
  start=$2
  delta=${3:-2}
  timeout=${4:-90}
  deadline=$((SECONDS + timeout))
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    height=$(rpc_height "${port}" 2>/dev/null || echo -1)
    if [ "${height}" -ge $((start + delta)) ]; then
      echo "RPC ${port}: height ${start} -> ${height}"
      return 0
    fi
    sleep 1
  done
  echo "ERROR: RPC ${port} did not advance ${delta} blocks from ${start}" >&2
  return 1
}

assert_matched_apphash() {
  label=$1
  timeout=${2:-120}
  deadline=$((SECONDS + timeout))
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    status_heights=()
    abci_heights=()
    app_hashes=()
    block_hashes=()
    available=1
    for port in "${RPC_PORTS[@]}"; do
      status_tuple=$(rpc_status_tuple "${port}" 2>/dev/null || echo '-1|error|ERROR')
      IFS='|' read -r status_height catching_up block_hash <<< "${status_tuple}"
      abci_tuple=$(rpc_abci_tuple "${port}" 2>/dev/null || echo '-1|ERROR')
      IFS='|' read -r abci_height app_hash <<< "${abci_tuple}"
      if [ "${status_height}" -lt 2 ] || [ "${abci_height}" -ne "${status_height}" ] ||
         [ "${catching_up}" != "false" ] || [ "${app_hash}" = "ERROR" ] || [ "${app_hash}" = "EMPTY" ] ||
         [ "${block_hash}" = "ERROR" ] || [ "${block_hash}" = "EMPTY" ]; then
        available=0
      fi
      status_heights+=("${status_height}")
      abci_heights+=("${abci_height}")
      app_hashes+=("${app_hash}")
      block_hashes+=("${block_hash}")
    done
    if [ "${available}" = "1" ]; then
      if [ "${status_heights[0]}" = "${status_heights[1]}" ] &&
         [ "${status_heights[0]}" = "${status_heights[2]}" ] &&
         [ "${status_heights[0]}" = "${status_heights[3]}" ] &&
         [ "${abci_heights[0]}" = "${abci_heights[1]}" ] &&
         [ "${abci_heights[0]}" = "${abci_heights[2]}" ] &&
         [ "${abci_heights[0]}" = "${abci_heights[3]}" ] &&
         [ "${app_hashes[0]}" = "${app_hashes[1]}" ] &&
         [ "${app_hashes[0]}" = "${app_hashes[2]}" ] &&
         [ "${app_hashes[0]}" = "${app_hashes[3]}" ] &&
         [ "${block_hashes[0]}" = "${block_hashes[1]}" ] &&
         [ "${block_hashes[0]}" = "${block_hashes[2]}" ] &&
         [ "${block_hashes[0]}" = "${block_hashes[3]}" ]; then
        echo "${label}: live convergence height=${abci_heights[0]} block=${block_hashes[0]} AppHash=${app_hashes[0]} catching_up=false"
        return 0
      fi
    fi
    sleep 1
  done
  echo "ERROR: ${label}: validators did not reach exact live block/ABCI height/AppHash convergence with catching_up=false" >&2
  return 1
}

service_container() {
  "${COMPOSE[@]}" ps -q "$1"
}

assert_service_running() {
  local service=$1
  local id running
  id=$(service_container "${service}")
  [ -n "${id}" ] || { echo "ERROR: no container for ${service}" >&2; return 1; }
  running=$(docker inspect -f '{{.State.Running}}' "${id}")
  [ "${running}" = "true" ] || { echo "ERROR: ${service} is not running" >&2; return 1; }
}

comet_network_ip() {
  local service=$1
  local id ip
  id=$(service_container "${service}")
  [ -n "${id}" ] || { echo "ERROR: no container for ${service}" >&2; return 1; }
  ip=$(docker inspect -f "{{with index .NetworkSettings.Networks \"${NETWORK}\"}}{{.IPAddress}}{{end}}" "${id}")
  [ -n "${ip}" ] || { echo "ERROR: no ${NETWORK} address for ${service}" >&2; return 1; }
  echo "${ip}"
}

wait_exact_peer_set() {
  local port=$1
  local expected=$2
  local timeout=${3:-90}
  local deadline=$((SECONDS + timeout))
  local actual=ERROR
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    actual=$(rpc_peer_ids "${port}" 2>/dev/null || echo ERROR)
    if [ "${actual}" = "${expected}" ]; then
      echo "RPC ${port}: exact live peer set ${actual}"
      return 0
    fi
    sleep 1
  done
  echo "ERROR: RPC ${port} peer set ${actual}; expected exactly ${expected}" >&2
  return 1
}

install_partition_firewall() {
  local service=$1
  shift
  local id blocked_ip expected_rules actual_rules
  id=$(service_container "${service}")
  [ -n "${id}" ] || { echo "ERROR: no container for ${service}" >&2; return 1; }

  docker exec "${id}" iptables -w 5 -N "${FIREWALL_CHAIN}"
  for blocked_ip in "$@"; do
    [ -n "${blocked_ip}" ] || { echo "ERROR: empty firewall peer IP for ${service}" >&2; return 1; }
    # A Comet P2P flow has 26656 at either end depending on which peer dialed.
    # Cover both directions without touching RPC (26657) or ABCI (26658).
    docker exec "${id}" iptables -w 5 -A "${FIREWALL_CHAIN}" -p tcp -s "${blocked_ip}" --sport "${P2P_PORT}" -j REJECT --reject-with tcp-reset
    docker exec "${id}" iptables -w 5 -A "${FIREWALL_CHAIN}" -p tcp -s "${blocked_ip}" --dport "${P2P_PORT}" -j REJECT --reject-with tcp-reset
    docker exec "${id}" iptables -w 5 -A "${FIREWALL_CHAIN}" -p tcp -d "${blocked_ip}" --sport "${P2P_PORT}" -j REJECT --reject-with tcp-reset
    docker exec "${id}" iptables -w 5 -A "${FIREWALL_CHAIN}" -p tcp -d "${blocked_ip}" --dport "${P2P_PORT}" -j REJECT --reject-with tcp-reset
  done
  docker exec "${id}" iptables -w 5 -I INPUT 1 -j "${FIREWALL_CHAIN}"
  docker exec "${id}" iptables -w 5 -I OUTPUT 1 -j "${FIREWALL_CHAIN}"
  docker exec "${id}" iptables -w 5 -C INPUT -j "${FIREWALL_CHAIN}"
  docker exec "${id}" iptables -w 5 -C OUTPUT -j "${FIREWALL_CHAIN}"
  expected_rules=$((4 * $#))
  actual_rules=$(docker exec "${id}" iptables -w 5 -S "${FIREWALL_CHAIN}" | awk '$1 == "-A" { count++ } END { print count + 0 }')
  if [ "${actual_rules}" -ne "${expected_rules}" ]; then
    echo "ERROR: ${service} installed ${actual_rules} P2P firewall rules; expected ${expected_rules}" >&2
    return 1
  fi
  echo "installed ${FIREWALL_CHAIN} P2P firewall on ${service} (${id}, rules=${actual_rules})"
}

partition_firewall_packets() {
  local service=$1
  local id
  id=$(service_container "${service}")
  [ -n "${id}" ] || return 1
  docker exec "${id}" iptables -w 5 -nvxL "${FIREWALL_CHAIN}" |
    awk '$3 == "REJECT" { packets += $1 } END { print packets + 0 }'
}

wait_partition_firewall_exercised() {
  local service=$1
  local timeout=${2:-30}
  local deadline=$((SECONDS + timeout))
  local packets=0
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    packets=$(partition_firewall_packets "${service}" 2>/dev/null || echo 0)
    if [ "${packets}" -gt 0 ]; then
      echo "${service}: ${FIREWALL_CHAIN} rejected ${packets} P2P packets"
      return 0
    fi
    sleep 1
  done
  echo "ERROR: ${service} P2P firewall did not reject any packets within ${timeout}s" >&2
  return 1
}

assert_tcp_eviction_tuning() {
  local service=$1
  local id actual
  id=$(service_container "${service}")
  [ -n "${id}" ] || { echo "ERROR: no container for ${service}" >&2; return 1; }
  actual=$(docker exec "${id}" sysctl -n net.ipv4.tcp_retries2)
  if [ "${actual}" != "${P2P_TCP_RETRIES2}" ]; then
    echo "ERROR: ${service} net.ipv4.tcp_retries2=${actual}; expected ${P2P_TCP_RETRIES2}" >&2
    return 1
  fi
}

remove_partition_firewall() {
  local service=$1
  local id
  id=$(service_container "${service}")
  [ -n "${id}" ] || { echo "ERROR: no container for ${service}" >&2; return 1; }
  docker exec "${id}" iptables -w 5 -D INPUT -j "${FIREWALL_CHAIN}"
  docker exec "${id}" iptables -w 5 -D OUTPUT -j "${FIREWALL_CHAIN}"
  docker exec "${id}" iptables -w 5 -F "${FIREWALL_CHAIN}"
  docker exec "${id}" iptables -w 5 -X "${FIREWALL_CHAIN}"
  if docker exec "${id}" iptables -w 5 -C INPUT -j "${FIREWALL_CHAIN}" >/dev/null 2>&1 ||
     docker exec "${id}" iptables -w 5 -C OUTPUT -j "${FIREWALL_CHAIN}" >/dev/null 2>&1; then
    echo "ERROR: ${service} retained a ${FIREWALL_CHAIN} jump after healing" >&2
    return 1
  fi
  echo "removed ${FIREWALL_CHAIN} P2P firewall from ${service} (${id})"
}

all_rpc_heights() {
  local heights=()
  local port height
  for port in "${RPC_PORTS[@]}"; do
    height=$(rpc_height "${port}") || return 1
    [ "${height}" -ge 0 ] || return 1
    heights+=("${height}")
  done
  echo "${heights[*]}"
}

wait_all_heights_stable() {
  local stable_seconds=${1:-8}
  local timeout=${2:-90}
  local deadline=$((SECONDS + timeout))
  local stable_since=${SECONDS}
  local previous=
  local snapshot
  local heights=()

  while [ "${SECONDS}" -lt "${deadline}" ]; do
    snapshot=$(all_rpc_heights 2>/dev/null || true)
    read -r -a heights <<< "${snapshot}"
    if [ "${#heights[@]}" -eq 4 ] &&
       [ "${heights[0]}" = "${heights[1]}" ] &&
       [ "${heights[2]}" = "${heights[3]}" ]; then
      if [ "${snapshot}" != "${previous}" ]; then
        previous=${snapshot}
        stable_since=${SECONDS}
      elif [ $((SECONDS - stable_since)) -ge "${stable_seconds}" ]; then
        echo "${snapshot}"
        return 0
      fi
    else
      previous=
      stable_since=${SECONDS}
    fi
    sleep 1
  done

  echo "ERROR: partition halves did not reach equal, unchanged heights for ${stable_seconds}s (last=${snapshot:-unavailable})" >&2
  return 1
}

if ! "${COMPOSE[@]}" down -v --remove-orphans >/dev/null; then
  echo "ERROR: could not remove stale ${PROJECT} resources before the run" >&2
  exit 1
fi

for spec in "sage-v119-chaos-abci:local deploy/Dockerfile.abci" \
            "sage-v119-chaos-node:local deploy/Dockerfile.node"; do
  tag=${spec%% *}
  dockerfile=${spec##* }
  if [ "${REBUILD}" = "1" ] || ! docker image inspect "${tag}" >/dev/null 2>&1; then
    echo "--- building ${tag} from current tree ---"
    docker build -f "${dockerfile}" -t "${tag}" .
  else
    echo "--- reusing ${tag}; V119_CHAOS_REBUILD=1 rebuilds current tree ---"
  fi
done

node_version=$(docker run --rm sage-v119-chaos-node:local cometbft version | head -n 1 | sed 's/^v//')
if [ "${node_version}" != "${COMETBFT_RUNTIME_VERSION}" ]; then
	echo "ERROR: v11.9 chaos gate requires v0.38.23 source commit ${COMETBFT_SOURCE_COMMIT}, got ${node_version:-unknown}" >&2
	exit 1
fi
echo "validated CometBFT v0.38.23 source runtime ${node_version}"

echo "--- generating a fresh isolated four-validator testnet ---"
SAGE_TESTNET_GENESIS_DIR="${V119_CHAOS_GENESIS_DIR}" \
SAGE_TESTNET_GENESIS_OWNER_MARKER="${CHAOS_WORKDIR_MARKER}" \
SAGE_TESTNET_ABCI_HOST_SUFFIX="-local" \
COMETBFT_DOCKER_IMAGE="sage-v119-chaos-node:local" \
  bash deploy/init-testnet.sh

echo "--- starting isolated real CometBFT/ABCI processes ---"
"${COMPOSE[@]}" up -d --no-build \
  postgres abci0 abci1 abci2 abci3 cometbft0 cometbft1 cometbft2 cometbft3
wait_all_rpc 180
baseline=$(rpc_height "${RPC_PORTS[0]}")
wait_progress "${RPC_PORTS[0]}" "${baseline}" 2 90
assert_matched_apphash "baseline" 120

COMET_IPS=()
NODE_IDS=()
for i in 0 1 2 3; do
  COMET_IPS+=("$(comet_network_ip "cometbft${i}")")
  NODE_IDS+=("$(rpc_node_id "${RPC_PORTS[$i]}")")
  assert_tcp_eviction_tuning "cometbft${i}"
done
python3 -c '
import ipaddress, sys
ips = [str(ipaddress.ip_address(value)) for value in sys.argv[1:5]]
node_ids = sys.argv[5:9]
if len(set(ips)) != 4:
    raise SystemExit("CometBFT common-network IPs are not unique")
if len(set(node_ids)) != 4:
    raise SystemExit("CometBFT node IDs are not unique")
' "${COMET_IPS[@]}" "${NODE_IDS[@]}"
echo "validated stable Comet identities and ${NETWORK} IPs"
echo "validated container-local P2P socket expiry (tcp_retries2=${P2P_TCP_RETRIES2})"

echo "--- fault 1: SIGKILL a complete validator pair, then restart ---"
before_kill=$(rpc_height "${RPC_PORTS[0]}")
"${COMPOSE[@]}" kill -s KILL cometbft3 abci3
wait_progress "${RPC_PORTS[0]}" "${before_kill}" 2 90
"${COMPOSE[@]}" start abci3 cometbft3
wait_rpc "${RPC_PORTS[3]}" 120
assert_matched_apphash "post-SIGKILL recovery" 180

echo "--- fault 2: live one-validator P2P firewall partition, then heal ---"
before_one_partition=$(rpc_height "${RPC_PORTS[0]}")
install_partition_firewall cometbft0 "${COMET_IPS[3]}"
install_partition_firewall cometbft1 "${COMET_IPS[3]}"
install_partition_firewall cometbft2 "${COMET_IPS[3]}"
install_partition_firewall cometbft3 "${COMET_IPS[0]}" "${COMET_IPS[1]}" "${COMET_IPS[2]}"
for service in cometbft0 cometbft1 cometbft2 cometbft3; do
  wait_partition_firewall_exercised "${service}" 30
done
wait_exact_peer_set "${RPC_PORTS[0]}" "$(expected_peer_ids "${NODE_IDS[1]}" "${NODE_IDS[2]}")" 90
wait_exact_peer_set "${RPC_PORTS[1]}" "$(expected_peer_ids "${NODE_IDS[0]}" "${NODE_IDS[2]}")" 90
wait_exact_peer_set "${RPC_PORTS[2]}" "$(expected_peer_ids "${NODE_IDS[0]}" "${NODE_IDS[1]}")" 90
wait_exact_peer_set "${RPC_PORTS[3]}" "$(expected_peer_ids)" 90
assert_service_running cometbft3
if ! isolated_abci_tuple=$(rpc_abci_tuple "${RPC_PORTS[3]}"); then
  echo "ERROR: isolated validator lost its private ABCI path during the P2P-only partition" >&2
  exit 1
fi
echo "isolated validator remained live with private ABCI state ${isolated_abci_tuple}"
wait_progress "${RPC_PORTS[0]}" "${before_one_partition}" 2 90
one_partition_live_height=$(rpc_height "${RPC_PORTS[0]}")
one_partition_isolated_height=$(rpc_height "${RPC_PORTS[3]}")
if [ "${one_partition_isolated_height}" -ge "${one_partition_live_height}" ]; then
  echo "ERROR: isolated validator did not fall behind (${one_partition_isolated_height} vs live ${one_partition_live_height})" >&2
  exit 1
fi
for service in cometbft0 cometbft1 cometbft2 cometbft3; do
  remove_partition_firewall "${service}"
done
assert_matched_apphash "post-one-validator partition" 180

echo "--- fault 3: stable-IP 2+2 P2P firewall partition must halt on both live halves ---"
install_partition_firewall cometbft0 "${COMET_IPS[2]}" "${COMET_IPS[3]}"
install_partition_firewall cometbft1 "${COMET_IPS[2]}" "${COMET_IPS[3]}"
install_partition_firewall cometbft2 "${COMET_IPS[0]}" "${COMET_IPS[1]}"
install_partition_firewall cometbft3 "${COMET_IPS[0]}" "${COMET_IPS[1]}"
for service in cometbft0 cometbft1 cometbft2 cometbft3; do
  wait_partition_firewall_exercised "${service}" 30
done
wait_exact_peer_set "${RPC_PORTS[0]}" "$(expected_peer_ids "${NODE_IDS[1]}")" 90
wait_exact_peer_set "${RPC_PORTS[1]}" "$(expected_peer_ids "${NODE_IDS[0]}")" 90
wait_exact_peer_set "${RPC_PORTS[2]}" "$(expected_peer_ids "${NODE_IDS[3]}")" 90
wait_exact_peer_set "${RPC_PORTS[3]}" "$(expected_peer_ids "${NODE_IDS[2]}")" 90

halt_window=${V119_HALT_WINDOW_SECONDS:-12}
case "${halt_window}" in
  ''|*[!0-9]*)
    echo "ERROR: V119_HALT_WINDOW_SECONDS must be an integer of at least 12" >&2
    exit 1
    ;;
esac
if [ "${halt_window}" -lt 12 ]; then
  echo "ERROR: V119_HALT_WINDOW_SECONDS must be at least 12" >&2
  exit 1
fi

stable_snapshot=$(wait_all_heights_stable 8 90)
read -r -a halt_start <<< "${stable_snapshot}"
sleep "${halt_window}"
end_snapshot=$(all_rpc_heights)
read -r -a halt_end <<< "${end_snapshot}"
if [ "${#halt_end[@]}" -ne 4 ]; then
  echo "ERROR: could not read all validator heights after the 2+2 halt window" >&2
  exit 1
fi
for i in 0 1 2 3; do
  if [ "${halt_end[$i]}" -ne "${halt_start[$i]}" ]; then
    echo "ERROR: 2+2 partition advanced validator ${i} from ${halt_start[$i]} to ${halt_end[$i]}; quorum may have relaxed" >&2
    exit 1
  fi
done
echo "2+2 live partition held both halves strictly at A=${halt_start[0]}, B=${halt_start[2]} for ${halt_window}s"
for service in cometbft0 cometbft1 cometbft2 cometbft3; do
  remove_partition_firewall "${service}"
done
wait_progress "${RPC_PORTS[0]}" "${halt_end[0]}" 2 120
assert_matched_apphash "post-majority-partition heal" 180

versions=()
for port in "${RPC_PORTS[@]}"; do
  versions+=("$(rpc_app_version "${port}")")
done
echo "application versions after fault phases: ${versions[*]}"
missing_required_proof=0
if [ "${V119_REQUIRE_SCOPED_RECONFIG:-0}" = "1" ]; then
  app_v20_active=1
  for version in "${versions[@]}"; do
    if [ "${version}" -lt 20 ]; then
      app_v20_active=0
      break
    fi
  done
  if [ "${app_v20_active}" = "0" ]; then
    echo "ERROR: scoped reconfiguration gate requires an app-v20-initialized multi-validator fixture; fresh deploy testnet reports app versions ${versions[*]}" >&2
    echo "ERROR: do not count crash/partition success as the v11.9 pinned-ballot reconfiguration proof" >&2
  else
    echo "ERROR: app-v20 is active, but the real signed scope formation/revision driver is not yet implemented in this Docker harness" >&2
  fi
  missing_required_proof=1
fi
if [ "${V119_REQUIRE_AUTHORIZED_STATE_SYNC:-0}" = "1" ]; then
  echo "ERROR: authorized provider-to-pristine-receiver state sync is not yet implemented in this Docker harness" >&2
  echo "ERROR: do not count topology recovery as the v11.9 endpoint/P2P/restart proof" >&2
  missing_required_proof=1
fi
if [ "${missing_required_proof}" = "1" ]; then
  exit 1
fi

echo "=== v11.9 REAL MULTI-PROCESS FAULT GATE PASSED ==="
echo "PASS: SIGKILL/restart, stable-IP P2P firewall partition/heal, strict 2+2 halt, exact live block/ABCI height/AppHash convergence"
echo "OPEN: authorized state-sync transfer and real app-v20 pinned-scope reconfiguration (v11.9 release mode requires both open gates)"
