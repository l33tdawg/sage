#!/usr/bin/env bash
#
# Real multi-process v11.9 fault gate over an isolated four-process
# CometBFT + ABCI Docker network.
#
# Proves, with bounded waits and matched-height AppHash checks:
#   1. A real signed app-v20 ceremony starts with three genesis validators and
#      one non-validator process, then adds that process, updates its power,
#      removes a different validator, and proves every H+2 Comet set transition
#      plus restart persistence/no resurrection.
#   2. SIGKILL of complete Comet + ABCI process pairs after add, update, and
#      remove, followed by block catch-up and AppHash convergence.
#   3. A live lower-power validator isolated by a P2P-only firewall while the
#      remaining greater-than-two-thirds power continues, then heals/catches up.
#   4. A stable-IP 2+2 P2P firewall partition does not relax consensus:
#      both live halves stall until the firewall heals, then resume and
#      converge on the same block and application state.
#
# This topology proves real Comet TCP behavior; it does not pretend that the
# deterministic subprocess oracle's held replica is a network partition. The
# scoped-reconfiguration release gate composes both independent proofs. A
# direct V119_REQUIRE_SCOPED_RECONFIG=1 run therefore invokes the signed
# app-v20 subprocess oracle after this Docker topology succeeds. CI runs that
# oracle in its prerequisite job and routes the flag there to avoid duplication.
# V119_REQUIRE_AUTHORIZED_STATE_SYNC=1 independently invokes the integrated
# sage-gui provider-to-receiver wire proof.

set -euo pipefail
cd "$(dirname "$0")/../.."

PROJECT=sage-v119-chaos
NETWORK=${PROJECT}_sagenet
FIREWALL_CHAIN=SAGE_V119
P2P_PORT=26656
P2P_TCP_RETRIES2=3
APP_V20_MEMPOOL_MAX_TX_BYTES=1048576
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
REST_PORTS=(18190 18191 18192 18193)
REBUILD=${V119_CHAOS_REBUILD:-1}
KEEP=${V119_CHAOS_KEEP:-0}
for setting in \
  "V119_CHAOS_REBUILD=${REBUILD}" \
  "V119_CHAOS_KEEP=${KEEP}" \
  "V119_REQUIRE_SCOPED_RECONFIG=${V119_REQUIRE_SCOPED_RECONFIG:-0}" \
  "V119_REQUIRE_AUTHORIZED_STATE_SYNC=${V119_REQUIRE_AUTHORIZED_STATE_SYNC:-0}"; do
  case "${setting#*=}" in
    0|1) ;;
    *)
      echo "ERROR: ${setting%%=*} must be 0 or 1" >&2
      exit 1
      ;;
  esac
done
CHAOS_TMP_ROOT=$(cd "${TMPDIR:-/tmp}" && pwd -P)
CHAOS_WORKDIR=
CHAOS_WORKDIR_MARKER=
CHAOS_WORKDIR_READY=0
V119_CHAOS_GENESIS_DIR=
V119_CHAOS_DATA_DIR=
V119_GOVERNANCE_OPERATOR_KEY=
export V119_GOVERNANCE_OPERATOR_ID=

dump_diagnostics() {
  "${COMPOSE[@]}" ps -a || true
  "${COMPOSE[@]}" logs --tail=120 postgres cometbft0 cometbft1 cometbft2 cometbft3 abci0 abci1 abci2 abci3 || true
}

validate_chaos_workdir() {
  local parent name marker_value

  if [ -z "${CHAOS_WORKDIR:-}" ] || [ "${CHAOS_WORKDIR}" = "/" ] ||
     [ ! -d "${CHAOS_WORKDIR}" ] || [ -L "${CHAOS_WORKDIR}" ]; then
    echo "ERROR: refusing invalid chaos work directory ${CHAOS_WORKDIR:-<unset>}" >&2
    return 1
  fi

  parent=$(cd "$(dirname "${CHAOS_WORKDIR}")" && pwd -P) || return 1
  name=$(basename "${CHAOS_WORKDIR}")
  case "${name}" in
    sage-v119-chaos.*) ;;
    *)
      echo "ERROR: refusing unexpected chaos work directory name ${CHAOS_WORKDIR}" >&2
      return 1
      ;;
  esac
  if [ "${parent}" != "${CHAOS_TMP_ROOT}" ]; then
    echo "ERROR: refusing chaos work directory outside ${CHAOS_TMP_ROOT}: ${CHAOS_WORKDIR}" >&2
    return 1
  fi

  if [ ! -f "${CHAOS_WORKDIR_MARKER}" ] || [ -L "${CHAOS_WORKDIR_MARKER}" ]; then
    echo "ERROR: refusing unowned chaos work directory ${CHAOS_WORKDIR}" >&2
    return 1
  fi
  marker_value=$(cat "${CHAOS_WORKDIR_MARKER}") || return 1
  if [ "${marker_value}" != "${CHAOS_WORKDIR}" ]; then
    echo "ERROR: chaos work directory marker does not match ${CHAOS_WORKDIR}" >&2
    return 1
  fi
}

clear_chaos_workdir_contents() {
  local cleanup_image=sage-v119-chaos-node:local
  local residue

  validate_chaos_workdir || return 1

  if docker image inspect "${cleanup_image}" >/dev/null 2>&1; then
    # CometBFT and PostgreSQL deliberately write their bind-mounted state as
    # container UIDs (root and postgres respectively). A host rm cannot descend
    # through that state on Linux. Reuse the already-built, pinned Alpine image
    # as a tightly confined cleanup helper: no network, read-only rootfs, and
    # only this run's validated temp directory mounted writable.
    if ! docker run --rm --pull never --network none --read-only \
      --cap-drop ALL --cap-add DAC_OVERRIDE --cap-add FOWNER \
      --security-opt no-new-privileges=true \
      --user 0:0 \
      --mount "type=bind,source=${CHAOS_WORKDIR},target=/sage-chaos-cleanup" \
      --env "CHAOS_EXPECTED_WORKDIR=${CHAOS_WORKDIR}" \
      --entrypoint /bin/sh "${cleanup_image}" -euc '
        root=/sage-chaos-cleanup
        marker=${root}/.sage-testnet-genesis-owner
        test -d "${root}"
        test -f "${marker}"
        test ! -L "${marker}"
        test "$(cat "${marker}")" = "${CHAOS_EXPECTED_WORKDIR}"
        find "${root}" -mindepth 1 -maxdepth 1 \
          ! -name .sage-testnet-genesis-owner -exec rm -rf -- {} \;
        test -z "$(find "${root}" -mindepth 1 -maxdepth 1 \
          ! -name .sage-testnet-genesis-owner -print -quit)"
        rm -f -- "${marker}"
        test -z "$(find "${root}" -mindepth 1 -maxdepth 1 -print -quit)"
      '; then
      echo "ERROR: container cleanup failed for ${CHAOS_WORKDIR}" >&2
      return 1
    fi
  else
    # Before the validator image exists, no container has written into this
    # directory. Keep an early-failure path that does not pull an image.
    find "${CHAOS_WORKDIR}" -mindepth 1 -maxdepth 1 \
      ! -name .sage-testnet-genesis-owner -exec rm -rf -- {} \;
    residue=$(find "${CHAOS_WORKDIR}" -mindepth 1 -maxdepth 1 \
      ! -name .sage-testnet-genesis-owner -print -quit) || return 1
    if [ -n "${residue}" ]; then
      echo "ERROR: host cleanup left residue in ${CHAOS_WORKDIR}: ${residue}" >&2
      return 1
    fi
    rm -f -- "${CHAOS_WORKDIR_MARKER}" || return 1
  fi

  residue=$(find "${CHAOS_WORKDIR}" -mindepth 1 -maxdepth 1 -print -quit) || return 1
  if [ -n "${residue}" ]; then
    echo "ERROR: cleanup left residue in ${CHAOS_WORKDIR}: ${residue}" >&2
    return 1
  fi
  if ! rmdir -- "${CHAOS_WORKDIR}"; then
    echo "ERROR: failed to remove empty chaos work directory ${CHAOS_WORKDIR}" >&2
    return 1
  fi
  if [ -e "${CHAOS_WORKDIR}" ] || [ -L "${CHAOS_WORKDIR}" ]; then
    echo "ERROR: chaos work directory still exists after cleanup: ${CHAOS_WORKDIR}" >&2
    return 1
  fi
}

cleanup_unready_workdir() {
  local parent name marker marker_value

  [ -n "${CHAOS_WORKDIR:-}" ] || return 0
  if [ "${CHAOS_WORKDIR}" = "/" ] || [ ! -d "${CHAOS_WORKDIR}" ] ||
     [ -L "${CHAOS_WORKDIR}" ]; then
    echo "ERROR: refusing invalid unready chaos work directory ${CHAOS_WORKDIR}" >&2
    return 1
  fi
  parent=$(cd "$(dirname "${CHAOS_WORKDIR}")" && pwd -P) || return 1
  name=$(basename "${CHAOS_WORKDIR}")
  case "${name}" in
    sage-v119-chaos.*) ;;
    *)
      echo "ERROR: refusing unexpected unready chaos work directory ${CHAOS_WORKDIR}" >&2
      return 1
      ;;
  esac
  if [ "${parent}" != "${CHAOS_TMP_ROOT}" ]; then
    echo "ERROR: refusing unready chaos work directory outside ${CHAOS_TMP_ROOT}: ${CHAOS_WORKDIR}" >&2
    return 1
  fi

  marker=${CHAOS_WORKDIR_MARKER:-${CHAOS_WORKDIR}/.sage-testnet-genesis-owner}
  if [ -e "${marker}" ] || [ -L "${marker}" ]; then
    if [ ! -f "${marker}" ] || [ -L "${marker}" ]; then
      echo "ERROR: refusing unexpected unready owner marker ${marker}" >&2
      return 1
    fi
    marker_value=$(cat "${marker}") || return 1
    if [ "${marker_value}" != "${CHAOS_WORKDIR}" ]; then
      echo "ERROR: unready owner marker does not match ${CHAOS_WORKDIR}" >&2
      return 1
    fi
    rm -f -- "${marker}" || return 1
  fi
  if ! rmdir -- "${CHAOS_WORKDIR}"; then
    echo "ERROR: unready chaos work directory is not empty: ${CHAOS_WORKDIR}" >&2
    return 1
  fi
}

cleanup() {
  rc=$?
  trap - EXIT INT TERM
  if [ "${CHAOS_WORKDIR_READY:-0}" != "1" ]; then
    if ! cleanup_unready_workdir && [ "${rc}" -eq 0 ]; then
      rc=1
    fi
    exit "${rc}"
  fi
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

    if [ "${cleanup_failed}" = "0" ] && ! clear_chaos_workdir_contents; then
      cleanup_failed=1
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
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

CHAOS_WORKDIR=$(mktemp -d "${CHAOS_TMP_ROOT}/sage-v119-chaos.XXXXXX")
canonical_workdir=$(cd "${CHAOS_WORKDIR}" && pwd -P)
CHAOS_WORKDIR=${canonical_workdir}
CHAOS_WORKDIR_MARKER="${CHAOS_WORKDIR}/.sage-testnet-genesis-owner"
export V119_CHAOS_GENESIS_DIR="${CHAOS_WORKDIR}/genesis"
export V119_CHAOS_DATA_DIR="${CHAOS_WORKDIR}/data"
printf '%s\n' "${CHAOS_WORKDIR}" > "${CHAOS_WORKDIR_MARKER}"
CHAOS_WORKDIR_READY=1
mkdir -p "${V119_CHAOS_DATA_DIR}/postgres"
for index in 0 1 2 3; do
  mkdir -p "${V119_CHAOS_DATA_DIR}/abci${index}"
  # The temp parent is mode 0700. World-writable child directories let the
  # fixed non-root ABCI UID write through Docker bind mounts on Linux/macOS
  # without granting access through the host parent.
  chmod 0777 "${V119_CHAOS_DATA_DIR}/abci${index}"
done

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

rpc_chain_id() {
  rpc_json "$1" /status | python3 -c '
import json, sys
chain_id = json.load(sys.stdin)["result"]["node_info"]["network"]
if not isinstance(chain_id, str) or not chain_id:
    raise SystemExit("invalid empty CometBFT chain ID")
print(chain_id)'
}

rpc_peer_ids() {
  rpc_json "$1" /net_info | python3 -c '
import json, sys
peers = json.load(sys.stdin)["result"]["peers"]
ids = sorted(str(peer["node_info"]["id"]).lower() for peer in peers)
print(",".join(ids) if ids else "NONE")'
}

rpc_validator_set() {
  local port=$1
  local height=$2
  rpc_json "${port}" "/validators?height=${height}&page=1&per_page=100" | python3 -c '
import base64, json, sys
result = json.load(sys.stdin)["result"]
validators = result["validators"]
if int(result["total"]) != len(validators) or len(validators) > 100:
    raise SystemExit("validator response is incomplete")
items = []
for validator in validators:
    raw = base64.b64decode(validator["pub_key"]["value"], validate=True)
    if len(raw) != 32:
        raise SystemExit("validator has a non-Ed25519 public key")
    power = int(validator["voting_power"])
    if power <= 0:
        raise SystemExit("validator has non-positive power")
    items.append(f"{raw.hex()}={power}")
print(",".join(sorted(items)))'
}

rpc_validator_address_for_pub() {
  local port=$1
  local height=$2
  local pubkey=$3
  rpc_json "${port}" "/validators?height=${height}&page=1&per_page=100" | python3 -c '
import base64, json, sys
want = sys.argv[1]
matches = []
for validator in json.load(sys.stdin)["result"]["validators"]:
    raw = base64.b64decode(validator["pub_key"]["value"], validate=True).hex()
    if raw == want:
        matches.append(str(validator["address"]).lower())
if len(matches) != 1:
    raise SystemExit(f"wanted one validator address for {want}, got {len(matches)}")
print(matches[0])' "${pubkey}"
}

rpc_tx_height() {
  local port=$1
  local hash=$2
  case "${hash}" in
    ''|*[!0-9A-Fa-f]*)
      echo "ERROR: invalid transaction hash ${hash:-<empty>}" >&2
      return 1
      ;;
  esac
  rpc_json "${port}" "/tx?hash=0x${hash}&prove=false" | python3 -c '
import json, sys
print(int(json.load(sys.stdin)["result"]["height"]))'
}

rpc_commit_signers() {
  local port=$1
  local height=$2
  rpc_json "${port}" "/commit?height=${height}" | python3 -c '
import json, sys
signatures = json.load(sys.stdin)["result"]["signed_header"]["commit"]["signatures"]
addresses = sorted(str(item.get("validator_address") or "").lower() for item in signatures)
print(",".join(address for address in addresses if address))'
}

json_field() {
  local field=$1
  python3 -c 'import json,sys; value=json.load(sys.stdin)[sys.argv[1]]; print(value)' "${field}"
}

canonical_validator_set() {
  python3 -c '
import sys
if len(sys.argv[1:]) % 2:
    raise SystemExit("validator set requires pubkey/power pairs")
items = []
for index in range(1, len(sys.argv), 2):
    pubkey = sys.argv[index]
    power = int(sys.argv[index + 1])
    if len(pubkey) != 64 or any(c not in "0123456789abcdef" for c in pubkey) or power <= 0:
        raise SystemExit("invalid validator set member")
    items.append(f"{pubkey}={power}")
if len(items) != len(set(items)):
    raise SystemExit("duplicate validator set member")
print(",".join(sorted(items)))
' "$@"
}

fixture_client() {
  docker run --rm --pull never --network "${NETWORK}" \
    --mount "type=bind,source=${V119_GOVERNANCE_OPERATOR_KEY},target=/fixture/operator.key,readonly" \
    --entrypoint /app/v119-governance-fixture \
    sage-v119-chaos-abci:local --key /fixture/operator.key "$@"
}

fixture_request() {
  local index=$1
  local method=$2
  local path=$3
  local body=${4:-}
  fixture_client \
    --node "${index}" \
    --method "${method}" \
    --path "${path}" \
    --body "${body}" \
    request
}

fixture_upgrade() {
  local target=$1
  docker run --rm --pull never --network "${NETWORK}" \
    --mount "type=bind,source=${V119_GOVERNANCE_OPERATOR_KEY},target=/fixture/operator.key,readonly" \
    --entrypoint /app/sage-gui-v119-fixture \
    sage-v119-chaos-abci:local upgrade propose \
      --target "${target}" --yes --wait \
      --rpc http://cometbft0:26657 \
      --agent-key /fixture/operator.key
}

governance_context() {
  local index=$1
  fixture_request "${index}" GET /v1/governance/context
}

governance_propose() {
  local index=$1
  local operation=$2
  local target_id=$3
  local target_pubkey=$4
  local target_power=$5
  local context validator_id domain active body
  context=$(governance_context "${index}")
  IFS='|' read -r validator_id domain active <<< "$(printf '%s' "${context}" | python3 -c '
import json, sys
ctx = json.load(sys.stdin)
print("{}|{}|{}".format(ctx["validator_id"], ctx["governance_domain"], str(bool(ctx["app_v20_active"])).lower()))')"
  if [ "${active}" != true ] || [ -z "${validator_id}" ] || [ -z "${domain}" ]; then
    echo "ERROR: validator ${index} returned an inactive governance context" >&2
    return 1
  fi
  body=$(python3 -c '
import json, sys
payload = {
    "validator_id": sys.argv[1],
    "governance_domain": sys.argv[2],
    "operation": sys.argv[3],
    "target_id": sys.argv[4],
    "reason": "v11.9 real-Comet validator lifecycle release proof",
}
if sys.argv[5]:
    payload["target_pubkey"] = sys.argv[5]
power = int(sys.argv[6])
if power:
    payload["target_power"] = power
print(json.dumps(payload, separators=(",", ":")))
' "${validator_id}" "${domain}" "${operation}" "${target_id}" "${target_pubkey}" "${target_power}")
  fixture_request "${index}" POST /v1/governance/propose "${body}"
}

governance_vote() {
  local index=$1
  local proposal_id=$2
  local context validator_id domain active body
  context=$(governance_context "${index}")
  IFS='|' read -r validator_id domain active <<< "$(printf '%s' "${context}" | python3 -c '
import json, sys
ctx = json.load(sys.stdin)
print("{}|{}|{}".format(ctx["validator_id"], ctx["governance_domain"], str(bool(ctx["app_v20_active"])).lower()))')"
  if [ "${active}" != true ] || [ -z "${validator_id}" ] || [ -z "${domain}" ]; then
    echo "ERROR: validator ${index} returned an inactive governance context" >&2
    return 1
  fi
  body=$(python3 -c '
import json, sys
print(json.dumps({
    "validator_id": sys.argv[1],
    "governance_domain": sys.argv[2],
    "proposal_id": sys.argv[3],
    "decision": "accept",
}, separators=(",", ":")))
' "${validator_id}" "${domain}" "${proposal_id}")
  fixture_request "${index}" POST /v1/governance/vote "${body}"
}

governance_cancel() {
  local index=$1
  local proposal_id=$2
  local context validator_id domain active body
  context=$(governance_context "${index}")
  IFS='|' read -r validator_id domain active <<< "$(printf '%s' "${context}" | python3 -c '
import json, sys
ctx = json.load(sys.stdin)
print("{}|{}|{}".format(ctx["validator_id"], ctx["governance_domain"], str(bool(ctx["app_v20_active"])).lower()))')"
  if [ "${active}" != true ] || [ -z "${validator_id}" ] || [ -z "${domain}" ]; then
    echo "ERROR: validator ${index} returned an inactive governance context" >&2
    return 1
  fi
  body=$(python3 -c '
import json, sys
print(json.dumps({
    "validator_id": sys.argv[1],
    "governance_domain": sys.argv[2],
    "proposal_id": sys.argv[3],
}, separators=(",", ":")))
' "${validator_id}" "${domain}" "${proposal_id}")
  fixture_request "${index}" POST /v1/governance/cancel "${body}"
}

assert_abci_validator_state() {
  local label=$1
  local expected_set=$2
  shift 2
  local expected_active=("$@")
  local timeout=120
  local deadline=$((SECONDS + timeout))
  local index context got_set got_id got_active all_match
  if [ "${#expected_active[@]}" -ne 4 ]; then
    echo "ERROR: ${label}: expected four ABCI activity flags" >&2
    return 1
  fi
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    all_match=1
    for index in 0 1 2 3; do
      context=$(governance_context "${index}" 2>/dev/null || true)
      if [ -z "${context}" ]; then
        all_match=0
        break
      fi
      IFS='|' read -r got_id got_active got_set <<< "$(printf '%s' "${context}" | python3 -c '
import json, sys
ctx = json.load(sys.stdin)
items = []
for validator in ctx.get("active_validators", []):
    validator_id = str(validator["validator_id"])
    power = int(validator["voting_power"])
    if len(validator_id) != 64 or any(c not in "0123456789abcdef" for c in validator_id) or power <= 0:
        raise SystemExit("invalid persisted ABCI validator readiness member")
    items.append(f"{validator_id}={power}")
if len(items) != len(set(items)):
    raise SystemExit("duplicate persisted ABCI validator readiness member")
print("{}|{}|{}".format(
    ctx["validator_id"],
    str(bool(ctx["validator_active"])).lower(),
    ",".join(sorted(items)),
))')" || {
        all_match=0
        break
      }
      if [ "${got_id}" != "${NODE_PUBKEYS[$index]}" ] ||
         [ "${got_active}" != "${expected_active[$index]}" ] ||
         [ "${got_set}" != "${expected_set}" ]; then
        all_match=0
        break
      fi
    done
    if [ "${all_match}" = 1 ]; then
      echo "${label}: all four ABCIs report exact persisted roster ${expected_set} (active=${expected_active[*]})"
      return 0
    fi
    sleep 1
  done
  echo "ERROR: ${label}: ABCI${index} readiness id=${got_id:-unavailable} active=${got_active:-unavailable} set=${got_set:-unavailable}; want id=${NODE_PUBKEYS[$index]} active=${expected_active[$index]} set=${expected_set}" >&2
  return 1
}

governance_heartbeat() {
  fixture_request 0 PUT /v1/agent/update \
    '{"name":"v11.9-chaos-operator","boot_bio":"bounded live validator lifecycle proof"}'
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

wait_rest() {
  local port=$1
  local timeout=${2:-120}
  local deadline=$((SECONDS + timeout))
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    if curl -fsS --max-time 3 "http://127.0.0.1:${port}/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "ERROR: REST ${port} did not become ready within ${timeout}s" >&2
  return 1
}

wait_all_rest() {
  local timeout=${1:-180}
  local port
  for port in "${REST_PORTS[@]}"; do
    wait_rest "${port}" "${timeout}"
  done
}

wait_all_governance_domain_bindings() {
  local timeout=${1:-30}
  local expected_chain_id=
  local chain_id port index deadline bound

  for port in "${RPC_PORTS[@]}"; do
    chain_id=$(rpc_chain_id "${port}")
    if [ -z "${expected_chain_id}" ]; then
      expected_chain_id=${chain_id}
    elif [ "${chain_id}" != "${expected_chain_id}" ]; then
      echo "ERROR: CometBFT chain IDs diverged before governance binding: ${expected_chain_id} != ${chain_id}" >&2
      return 1
    fi
  done

  for index in 0 1 2 3; do
    deadline=$((SECONDS + timeout))
    bound=0
    while [ "${SECONDS}" -lt "${deadline}" ]; do
      if "${COMPOSE[@]}" logs --no-color "abci${index}" 2>/dev/null | python3 -c '
import re, sys
chain_id = sys.argv[1]
message = "app-v20 upgrade voter bound to authoritative CometBFT chain-id"
formats = (
    f"chain_id={chain_id}",
    f"chain_id=\"{chain_id}\"",
    f"\"chain_id\":\"{chain_id}\"",
)
ansi_escape = re.compile(r"\x1b\[[0-9;]*m")
if not any(
    message in (line := ansi_escape.sub("", raw))
    and any(field in line for field in formats)
    for raw in sys.stdin
):
    raise SystemExit(1)
' "${expected_chain_id}"; then
        echo "ABCI${index}: governance voter bound to chain ${expected_chain_id}"
        bound=1
        break
      fi
      sleep 1
    done
    if [ "${bound}" != 1 ]; then
      echo "ERROR: ABCI${index} did not bind its governance voter to chain ${expected_chain_id} within ${timeout}s" >&2
      "${COMPOSE[@]}" logs --tail=80 "abci${index}" || true
      return 1
    fi
  done
}

wait_all_app_version() {
  local target=$1
  local timeout=${2:-180}
  local deadline=$((SECONDS + timeout))
  local versions=()
  local port version all_match
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    versions=()
    all_match=1
    for port in "${RPC_PORTS[@]}"; do
      version=$(rpc_app_version "${port}" 2>/dev/null || echo -1)
      versions+=("${version}")
      if [ "${version}" -ne "${target}" ]; then
        all_match=0
      fi
    done
    if [ "${all_match}" = 1 ]; then
      echo "all four applications reached app-v${target}"
      return 0
    fi
    sleep 1
  done
  echo "ERROR: applications did not all reach app-v${target} (last=${versions[*]:-unavailable})" >&2
  return 1
}

wait_all_height_at_least() {
  local target=$1
  local timeout=${2:-180}
  local deadline=$((SECONDS + timeout))
  local port height all_ready
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    all_ready=1
    for port in "${RPC_PORTS[@]}"; do
      height=$(rpc_height "${port}" 2>/dev/null || echo -1)
      if [ "${height}" -lt "${target}" ]; then
        all_ready=0
      fi
    done
    if [ "${all_ready}" = 1 ]; then
      return 0
    fi
    sleep 1
  done
  echo "ERROR: all processes did not reach height ${target}" >&2
  return 1
}

assert_validator_set_at() {
  local label=$1
  local height=$2
  local expected=$3
  local timeout=${4:-180}
  local deadline=$((SECONDS + timeout))
  local port got all_match
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    all_match=1
    for port in "${RPC_PORTS[@]}"; do
      got=$(rpc_validator_set "${port}" "${height}" 2>/dev/null || echo ERROR)
      if [ "${got}" != "${expected}" ]; then
        all_match=0
        break
      fi
    done
    if [ "${all_match}" = 1 ]; then
      echo "${label}: exact validator set at height ${height}: ${expected}"
      return 0
    fi
    sleep 1
  done
  echo "ERROR: ${label}: validator set at height ${height} was ${got:-unavailable}; want ${expected}" >&2
  return 1
}

advance_governance_to_execution() {
  local proposal_height=$1
  local target_height=$((proposal_height + 10))
  local response tx_hash current_height
  current_height=$(rpc_height "${RPC_PORTS[0]}")
  while [ "${current_height}" -lt "${target_height}" ]; do
    response=$(governance_heartbeat)
    tx_hash=$(printf '%s' "${response}" | json_field tx_hash)
    current_height=$(rpc_tx_height "${RPC_PORTS[0]}" "${tx_hash}")
  done
  wait_all_height_at_least "${target_height}" 120
  # Quorum is present before this boundary and ProcessBlock runs at every
  # height, so the immutable minimum-voting boundary is the execution height
  # even if an empty block races the final heartbeat RPC response.
  echo "${target_height}"
}

restart_pair_and_converge() {
  local label=$1
  local index=$2
  local progress_port_index=$3
  local before
  before=$(rpc_height "${RPC_PORTS[$progress_port_index]}")
  "${COMPOSE[@]}" kill -s KILL "cometbft${index}" "abci${index}"
  # app-v20 suppresses idle blocks. Two authenticated idempotent operator
  # updates prove the surviving voting power can still commit while the pair is
  # down, instead of mistaking a quiet chain for progress.
  governance_heartbeat >/dev/null
  governance_heartbeat >/dev/null
  wait_progress "${RPC_PORTS[$progress_port_index]}" "${before}" 2 120
  "${COMPOSE[@]}" start "abci${index}" "cometbft${index}"
  wait_rpc "${RPC_PORTS[$index]}" 120
  wait_rest "${REST_PORTS[$index]}" 120
  assert_matched_apphash "${label}" 180
}

LAST_EXECUTION_HEIGHT=0
execute_validator_change() {
  local label=$1
  local operation=$2
  local target_id=$3
  local target_pubkey=$4
  local target_power=$5
  local old_set=$6
  local new_set=$7
  local proposed proposal_id proposal_hash proposal_height
  local vote vote_hash vote_height execution_height response

  proposed=$(governance_propose 0 "${operation}" "${target_id}" "${target_pubkey}" "${target_power}")
  proposal_id=$(printf '%s' "${proposed}" | json_field proposal_id)
  proposal_hash=$(printf '%s' "${proposed}" | json_field tx_hash)
  proposal_height=$(rpc_tx_height "${RPC_PORTS[0]}" "${proposal_hash}")
  if [ -z "${proposal_id}" ] || [ "${proposal_height}" -le 0 ]; then
    echo "ERROR: ${label}: governance proposal response was incomplete" >&2
    return 1
  fi

  # The outer proposal belongs to validator0 and carries its automatic accept.
  # Validators1/2 independently authorize exact operator-signed vote requests;
  # all three votes are committed before the immutable ten-block floor.
  for voter_index in 1 2; do
    vote=$(governance_vote "${voter_index}" "${proposal_id}")
    vote_hash=$(printf '%s' "${vote}" | json_field tx_hash)
    vote_height=$(rpc_tx_height "${RPC_PORTS[$voter_index]}" "${vote_hash}")
    if [ "${vote_height}" -ge $((proposal_height + 10)) ]; then
      echo "ERROR: ${label}: validator${voter_index} vote missed the minimum-voting boundary" >&2
      return 1
    fi
  done

  execution_height=$(advance_governance_to_execution "${proposal_height}")
  # Materialize both delayed Comet validator-set heights after the ABCI update.
  for _ in 1 2; do
    response=$(governance_heartbeat)
    vote_hash=$(printf '%s' "${response}" | json_field tx_hash)
    rpc_tx_height "${RPC_PORTS[0]}" "${vote_hash}" >/dev/null
  done
  wait_all_height_at_least "$((execution_height + 2))" 180
  assert_validator_set_at "${label} pre-effective" "$((execution_height + 1))" "${old_set}" 180
  assert_validator_set_at "${label} effective" "$((execution_height + 2))" "${new_set}" 180
  assert_matched_apphash "${label} AppHash convergence" 180
  LAST_EXECUTION_HEIGHT=${execution_height}
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

SOURCE_ID=$(python3 deploy/scripts/v11.9-source-id.py)
if [ "${#SOURCE_ID}" -ne 64 ]; then
  echo "ERROR: v11.9 source identity must be a 64-character SHA-256 digest" >&2
  exit 1
fi
case "${SOURCE_ID}" in
  *[!0-9a-f]*)
    echo "ERROR: v11.9 source identity must be lowercase hexadecimal" >&2
    exit 1
    ;;
esac

for spec in "sage-v119-chaos-abci:local deploy/Dockerfile.abci" \
            "sage-v119-chaos-node:local deploy/Dockerfile.node"; do
  tag=${spec%% *}
  dockerfile=${spec##* }
  if [ "${REBUILD}" = "1" ] || ! docker image inspect "${tag}" >/dev/null 2>&1; then
    echo "--- building ${tag} from current tree ---"
    if [ "${tag}" = "sage-v119-chaos-abci:local" ]; then
      docker build --target v119-state-sync-fixture \
        --build-arg "V119_STATE_SYNC_SOURCE_ID=${SOURCE_ID}" \
        -f "${dockerfile}" -t "${tag}" .
    else
      docker build \
        --build-arg "V119_STATE_SYNC_SOURCE_ID=${SOURCE_ID}" \
        -f "${dockerfile}" -t "${tag}" .
    fi
  else
    echo "--- reusing ${tag}; V119_CHAOS_REBUILD=1 rebuilds current tree ---"
  fi
done

for image in sage-v119-chaos-abci:local sage-v119-chaos-node:local; do
  image_source_id=$(docker image inspect --format '{{ index .Config.Labels "dev.sage.v119-state-sync.source-id" }}' "${image}")
  if [ "${image_source_id}" != "${SOURCE_ID}" ]; then
    echo "ERROR: ${image} source identity ${image_source_id:-<missing>} does not match current tree ${SOURCE_ID}" >&2
    exit 1
  fi
done
if ! docker run --rm --pull never --network none --entrypoint sh \
  sage-v119-chaos-abci:local -ec \
  'test -x /app/amid && test -x /app/amid-v119-fixture && test -x /app/sage-gui-v119-fixture && test -x /app/v119-governance-fixture'; then
  echo "ERROR: chaos ABCI image is not the explicit v11.9 fixture target" >&2
  exit 1
fi

V119_GOVERNANCE_OPERATOR_KEY=${CHAOS_WORKDIR}/governance-operator.key
python3 - "${V119_GOVERNANCE_OPERATOR_KEY}" <<'PY'
import os
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
path.write_bytes(os.urandom(32))
path.chmod(0o644)
PY
export V119_GOVERNANCE_OPERATOR_ID=$(docker run --rm --pull never --network none \
  --mount "type=bind,source=${V119_GOVERNANCE_OPERATOR_KEY},target=/fixture/operator.key,readonly" \
  --entrypoint /app/v119-governance-fixture \
  sage-v119-chaos-abci:local --key /fixture/operator.key identity)
if [ "${#V119_GOVERNANCE_OPERATOR_ID}" -ne 64 ]; then
  echo "ERROR: per-run governance operator ID has an invalid length" >&2
  exit 1
fi
case "${V119_GOVERNANCE_OPERATOR_ID}" in
  *[!0-9a-f]*)
    echo "ERROR: per-run governance operator ID is not lowercase hexadecimal" >&2
    exit 1
    ;;
esac
echo "validated frozen source ${SOURCE_ID} and generated an isolated per-run governance operator"

node_version=$(docker run --rm sage-v119-chaos-node:local cometbft version | head -n 1 | sed 's/^v//')
if [ "${node_version}" != "${COMETBFT_RUNTIME_VERSION}" ]; then
	echo "ERROR: v11.9 chaos gate requires v0.38.23 source commit ${COMETBFT_SOURCE_COMMIT}, got ${node_version:-unknown}" >&2
	exit 1
fi
echo "validated CometBFT v0.38.23 source runtime ${node_version}"

echo "--- generating four process identities; node3 starts outside the three-validator genesis ---"
SAGE_TESTNET_GENESIS_DIR="${V119_CHAOS_GENESIS_DIR}" \
SAGE_TESTNET_GENESIS_OWNER_MARKER="${CHAOS_WORKDIR_MARKER}" \
SAGE_TESTNET_ABCI_HOST_SUFFIX="-local" \
COMETBFT_DOCKER_IMAGE="sage-v119-chaos-node:local" \
  bash deploy/init-testnet.sh

python3 - "${APP_V20_MEMPOOL_MAX_TX_BYTES}" \
  "${V119_CHAOS_GENESIS_DIR}/node0/config/config.toml" \
  "${V119_CHAOS_GENESIS_DIR}/node1/config/config.toml" \
  "${V119_CHAOS_GENESIS_DIR}/node2/config/config.toml" \
  "${V119_CHAOS_GENESIS_DIR}/node3/config/config.toml" <<'PY'
import pathlib
import re
import sys

max_tx_bytes = int(sys.argv[1])
for raw_path in sys.argv[2:]:
    path = pathlib.Path(raw_path)
    text = path.read_text()

    def one(pattern: str, want: str) -> None:
        values = re.findall(pattern, text, flags=re.MULTILINE)
        if values != [want]:
            raise SystemExit(f"{path}: wanted exactly one {pattern!r} value {want!r}, got {values!r}")

    one(r"^recheck\s*=\s*(\S+)\s*$", "true")
    one(r"^max_tx_bytes\s*=\s*(\d+)\s*$", str(max_tx_bytes))
    one(r'^wal_dir\s*=\s*("[^"]*")\s*$', '""')
PY
echo "validated external Comet mempool transition profile (recheck=true, max_tx_bytes=${APP_V20_MEMPOOL_MAX_TX_BYTES}, wal_dir empty)"

NODE_PUBKEYS=()
NODE_PUBKEYS_B64=()
NODE_ADDRESSES=()
INITIAL_POWER=
for i in 0 1 2 3; do
  private_key_json=$(docker run --rm --pull never --network none \
    --mount "type=bind,source=${V119_CHAOS_GENESIS_DIR}/node${i}/config,target=/validator,readonly" \
    sage-v119-chaos-node:local sh -ec 'cat /validator/priv_validator_key.json')
  key_tuple=$(printf '%s' "${private_key_json}" | python3 -c '
import base64, hashlib, json, sys
value = json.load(sys.stdin)["pub_key"]["value"]
raw = base64.b64decode(value, validate=True)
if len(raw) != 32:
    raise SystemExit("private validator key is not Ed25519")
print(f"{raw.hex()}|{value}|{hashlib.sha256(raw).digest()[:20].hex()}")')
  IFS='|' read -r key_hex key_b64 key_address <<< "${key_tuple}"
  NODE_PUBKEYS+=("${key_hex}")
  NODE_PUBKEYS_B64+=("${key_b64}")
  NODE_ADDRESSES+=("${key_address}")
  key_power=$(python3 - "${V119_CHAOS_GENESIS_DIR}/node0/config/genesis.json" "${key_b64}" <<'PY'
import json
import pathlib
import sys

genesis = json.loads(pathlib.Path(sys.argv[1]).read_text())
matches = [int(v["power"]) for v in genesis["validators"] if v["pub_key"]["value"] == sys.argv[2]]
if len(matches) != 1 or matches[0] <= 0:
    raise SystemExit("validator key is absent or duplicated in generated genesis")
print(matches[0])
PY
)
  if [ -z "${INITIAL_POWER}" ]; then
    INITIAL_POWER=${key_power}
  elif [ "${key_power}" -ne "${INITIAL_POWER}" ]; then
    echo "ERROR: generated validators do not have equal power" >&2
    exit 1
  fi
done

# CometBFT's generated default power is not a stable fixture contract (some
# supported generators emit 1, which cannot represent a positive 20% integer
# update). Normalize the three pre-start genesis validators to an explicit
# power so this gate always exercises the same bounded update and partition
# geometry. This mutates only the marker-owned, not-yet-started fixture homes.
FIXTURE_INITIAL_POWER=100
python3 - "${NODE_PUBKEYS_B64[3]}" "${FIXTURE_INITIAL_POWER}" \
  "${V119_CHAOS_GENESIS_DIR}/node0/config/genesis.json" \
  "${V119_CHAOS_GENESIS_DIR}/node1/config/genesis.json" \
  "${V119_CHAOS_GENESIS_DIR}/node2/config/genesis.json" \
  "${V119_CHAOS_GENESIS_DIR}/node3/config/genesis.json" <<'PY'
import json
import os
import pathlib
import sys

candidate = sys.argv[1]
normalized_power = int(sys.argv[2])
if normalized_power <= 0:
    raise SystemExit("normalized fixture power must be positive")
reference = None
for raw_path in sys.argv[3:]:
    path = pathlib.Path(raw_path)
    genesis = json.loads(path.read_text())
    validators = genesis.get("validators")
    if not isinstance(validators, list) or len(validators) != 4:
        raise SystemExit(f"{path}: expected four generated validators")
    kept = [v for v in validators if v.get("pub_key", {}).get("value") != candidate]
    if len(kept) != 3:
        raise SystemExit(f"{path}: candidate was absent or duplicated")
    for validator in kept:
        validator["power"] = str(normalized_power)
    genesis["validators"] = kept
    encoded = json.dumps(genesis, indent=2).encode() + b"\n"
    if reference is None:
        reference = encoded
    elif encoded != reference:
        raise SystemExit(f"{path}: generated genesis differs across nodes")
    temporary = path.with_suffix(path.suffix + ".v119.tmp")
    with temporary.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
PY

INITIAL_POWER=${FIXTURE_INITIAL_POWER}

POWER_DELTA=$((INITIAL_POWER / 5))
if [ "${POWER_DELTA}" -lt 1 ]; then
  POWER_DELTA=1
fi
UPDATED_POWER=$((INITIAL_POWER + POWER_DELTA))
python3 - "${INITIAL_POWER}" "${UPDATED_POWER}" <<'PY'
import sys

base, updated = map(int, sys.argv[1:])
if base <= 0 or updated <= base or updated - base > base // 2:
    raise SystemExit("unsafe fixture power delta")
# After node2 removal, isolating one base-power validator leaves base+updated,
# which must remain strictly greater than two thirds of 2*base+updated.
if 3 * (base + updated) <= 2 * (2 * base + updated):
    raise SystemExit("post-removal one-validator partition lacks >2/3 power")
# The later {node0,node1}|{removed-node2,node3} split must leave neither side
# strictly above two thirds.
total = 2 * base + updated
if 3 * (2 * base) > 2 * total or 3 * updated > 2 * total:
    raise SystemExit("post-removal 2+2 split would retain consensus power")
PY

INITIAL_SET=$(canonical_validator_set \
  "${NODE_PUBKEYS[0]}" "${INITIAL_POWER}" \
  "${NODE_PUBKEYS[1]}" "${INITIAL_POWER}" \
  "${NODE_PUBKEYS[2]}" "${INITIAL_POWER}")
ADDED_SET=$(canonical_validator_set \
  "${NODE_PUBKEYS[0]}" "${INITIAL_POWER}" \
  "${NODE_PUBKEYS[1]}" "${INITIAL_POWER}" \
  "${NODE_PUBKEYS[2]}" "${INITIAL_POWER}" \
  "${NODE_PUBKEYS[3]}" "${INITIAL_POWER}")
UPDATED_SET=$(canonical_validator_set \
  "${NODE_PUBKEYS[0]}" "${INITIAL_POWER}" \
  "${NODE_PUBKEYS[1]}" "${INITIAL_POWER}" \
  "${NODE_PUBKEYS[2]}" "${INITIAL_POWER}" \
  "${NODE_PUBKEYS[3]}" "${UPDATED_POWER}")
REMOVED_SET=$(canonical_validator_set \
  "${NODE_PUBKEYS[0]}" "${INITIAL_POWER}" \
  "${NODE_PUBKEYS[1]}" "${INITIAL_POWER}" \
  "${NODE_PUBKEYS[3]}" "${UPDATED_POWER}")
echo "prepared governed validator lifecycle: 3 equal -> add node3 -> node3 ${INITIAL_POWER}->${UPDATED_POWER} -> remove node2"

echo "--- starting isolated real CometBFT/ABCI processes ---"
"${COMPOSE[@]}" up -d --no-build \
  postgres abci0 abci1 abci2 abci3 cometbft0 cometbft1 cometbft2 cometbft3
wait_all_rpc 180
wait_all_rest 180
wait_all_governance_domain_bindings 30
baseline=$(rpc_height "${RPC_PORTS[0]}")
wait_progress "${RPC_PORTS[0]}" "${baseline}" 2 90
assert_matched_apphash "baseline" 120
baseline=$(rpc_height "${RPC_PORTS[0]}")
assert_validator_set_at "three-validator genesis" "${baseline}" "${INITIAL_SET}" 120

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

echo "--- registering the isolated per-run operator before the app-v9 admin gate ---"
register_response=$(fixture_request 0 POST /v1/agent/register \
  '{"name":"v11.9-chaos-operator","role":"admin","boot_bio":"bounded live validator lifecycle proof","provider":"v11.9-fixture","p2p_address":""}')
python3 - "${V119_GOVERNANCE_OPERATOR_ID}" "${register_response}" <<'PY'
import json
import sys

operator = sys.argv[1]
response = json.loads(sys.argv[2])
if response.get("agent_id") != operator or response.get("role") != "admin":
    raise SystemExit("operator registration did not commit the exact per-run admin identity")
if int(response.get("on_chain_height") or 0) <= 0:
    raise SystemExit("operator registration omitted a positive committed height")
PY

echo "--- driving the real three-validator chain through signed app-v20 governance ---"
# Fresh fixture stores begin at app-v1. Every fork gate is independent and a
# skipped version can never be activated after a higher version commits, so the
# real-process oracle must prove the complete one-at-a-time ladder.
for target in 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
  fixture_upgrade "${target}"
  wait_all_app_version "${target}" 240
done
assert_matched_apphash "post-app-v20 activation" 180

governance_domain=
for i in 0 1 2 3; do
  context=$(governance_context "${i}")
  context_tuple=$(printf '%s' "${context}" | python3 -c '
import json, sys
ctx = json.load(sys.stdin)
print("{}|{}|{}".format(ctx["validator_id"], ctx["governance_domain"], str(bool(ctx["app_v20_active"])).lower()))')
  IFS='|' read -r context_validator context_domain context_active <<< "${context_tuple}"
  if [ "${context_active}" != true ] || [ "${context_validator}" != "${NODE_PUBKEYS[$i]}" ]; then
    echo "ERROR: ABCI${i} governance context is not bound to its live validator key" >&2
    exit 1
  fi
  if [ -z "${governance_domain}" ]; then
    governance_domain=${context_domain}
  elif [ "${context_domain}" != "${governance_domain}" ]; then
    echo "ERROR: ABCI governance domains diverged across the chain" >&2
    exit 1
  fi
done
echo "validated four independently keyed gateways against one quorum-committed app-v20 domain"

echo "--- lifecycle 1: signed add of the live node3 full process, then complete-pair SIGKILL/restart ---"
execute_validator_change "governed add node3" add_validator \
  "${NODE_PUBKEYS[3]}" "${NODE_PUBKEYS[3]}" "${INITIAL_POWER}" \
  "${INITIAL_SET}" "${ADDED_SET}"
ADD_EXECUTION_HEIGHT=${LAST_EXECUTION_HEIGHT}
assert_abci_validator_state "governed add node3 effective ABCI state" "${ADDED_SET}" true true true true
restart_pair_and_converge "post-add node3 SIGKILL recovery" 3 0
assert_validator_set_at "post-add restarted historical H+2" \
  "$((ADD_EXECUTION_HEIGHT + 2))" "${ADDED_SET}" 120
post_add_height=$(rpc_height "${RPC_PORTS[0]}")
assert_validator_set_at "post-add restart persistence" "${post_add_height}" "${ADDED_SET}" 120
assert_abci_validator_state "post-add restart persisted ABCI state" "${ADDED_SET}" true true true true

echo "--- lifecycle 2: bounded node3 power update, then complete-pair SIGKILL/restart ---"
execute_validator_change "governed update node3" update_power \
  "${NODE_PUBKEYS[3]}" "" "${UPDATED_POWER}" \
  "${ADDED_SET}" "${UPDATED_SET}"
UPDATE_EXECUTION_HEIGHT=${LAST_EXECUTION_HEIGHT}
assert_abci_validator_state "governed update node3 effective ABCI state" "${UPDATED_SET}" true true true true
restart_pair_and_converge "post-update node3 SIGKILL recovery" 3 0
assert_validator_set_at "post-update restarted historical H+2" \
  "$((UPDATE_EXECUTION_HEIGHT + 2))" "${UPDATED_SET}" 120
post_update_height=$(rpc_height "${RPC_PORTS[0]}")
assert_validator_set_at "post-update restart persistence" "${post_update_height}" "${UPDATED_SET}" 120
assert_abci_validator_state "post-update restart persisted ABCI state" "${UPDATED_SET}" true true true true

echo "--- lifecycle 3: signed node2 removal, removed-pair SIGKILL/restart, and no resurrection ---"
execute_validator_change "governed remove node2" remove_validator \
  "${NODE_PUBKEYS[2]}" "" 0 \
  "${UPDATED_SET}" "${REMOVED_SET}"
REMOVE_EXECUTION_HEIGHT=${LAST_EXECUTION_HEIGHT}
assert_abci_validator_state "governed remove node2 effective ABCI state" "${REMOVED_SET}" true true false true
restart_pair_and_converge "post-remove node2 SIGKILL recovery" 2 0
assert_validator_set_at "post-remove restarted historical H+2" \
  "$((REMOVE_EXECUTION_HEIGHT + 2))" "${REMOVED_SET}" 120
post_remove_height=$(rpc_height "${RPC_PORTS[0]}")
assert_validator_set_at "post-remove restart persistence" "${post_remove_height}" "${REMOVED_SET}" 120
assert_abci_validator_state "post-remove restart persisted ABCI state" "${REMOVED_SET}" true true false true

REMOVED_COMET_ADDRESS=$(rpc_validator_address_for_pub \
  "${RPC_PORTS[0]}" "$((REMOVE_EXECUTION_HEIGHT + 1))" "${NODE_PUBKEYS[2]}")
if [ "${REMOVED_COMET_ADDRESS}" != "${NODE_ADDRESSES[2]}" ]; then
  echo "ERROR: removed node2 Comet address ${REMOVED_COMET_ADDRESS} does not match its generated key ${NODE_ADDRESSES[2]}" >&2
  exit 1
fi
for sample in 1 2 3; do
  response=$(governance_heartbeat)
  tx_hash=$(printf '%s' "${response}" | json_field tx_hash)
  sample_height=$(rpc_tx_height "${RPC_PORTS[0]}" "${tx_hash}")
  assert_validator_set_at "removed node2 remains absent after restart sample ${sample}" \
    "${sample_height}" "${REMOVED_SET}" 120
  commit_signers=$(rpc_commit_signers "${RPC_PORTS[0]}" "${sample_height}")
  case ",${commit_signers}," in
    *,${REMOVED_COMET_ADDRESS},*)
      echo "ERROR: removed node2 signed commit ${sample_height} after restart" >&2
      exit 1
      ;;
  esac
done
assert_matched_apphash "post-remove no-resurrection convergence" 180
echo "proved node2 absent from three effective validator sets and three post-restart commits"

echo "--- proving the restarted removed node2 key cannot cast a governance vote ---"
removed_vote_probe=$(governance_propose 0 add_validator \
  "${V119_GOVERNANCE_OPERATOR_ID}" "${V119_GOVERNANCE_OPERATOR_ID}" "${INITIAL_POWER}")
removed_vote_probe_id=$(printf '%s' "${removed_vote_probe}" | json_field proposal_id)
if removed_vote_error=$(governance_vote 2 "${removed_vote_probe_id}" 2>&1); then
  echo "ERROR: removed node2 governance gateway contributed a vote after restart" >&2
  exit 1
fi
case "${removed_vote_error}" in
  *"HTTP 400"*"request rejected"*) ;;
  *)
    echo "ERROR: removed node2 vote failed for an unexpected reason: ${removed_vote_error}" >&2
    exit 1
    ;;
esac
cancel_response=$(governance_cancel 0 "${removed_vote_probe_id}")
cancel_hash=$(printf '%s' "${cancel_response}" | json_field tx_hash)
rpc_tx_height "${RPC_PORTS[0]}" "${cancel_hash}" >/dev/null
assert_abci_validator_state "post-rejected-removed-vote ABCI state" "${REMOVED_SET}" true true false true
echo "proved removed node2's restarted key is rejected by consensus as a governance voter"

echo "--- fault 1: isolate lower-power validator1; node0+updated-node3 retain strict >2/3 ---"
before_one_partition=$(rpc_height "${RPC_PORTS[0]}")
install_partition_firewall cometbft0 "${COMET_IPS[1]}"
install_partition_firewall cometbft1 "${COMET_IPS[0]}" "${COMET_IPS[2]}" "${COMET_IPS[3]}"
install_partition_firewall cometbft2 "${COMET_IPS[1]}"
install_partition_firewall cometbft3 "${COMET_IPS[1]}"
for service in cometbft0 cometbft1 cometbft2 cometbft3; do
  wait_partition_firewall_exercised "${service}" 30
done
wait_exact_peer_set "${RPC_PORTS[0]}" "$(expected_peer_ids "${NODE_IDS[2]}" "${NODE_IDS[3]}")" 90
wait_exact_peer_set "${RPC_PORTS[1]}" "$(expected_peer_ids)" 90
wait_exact_peer_set "${RPC_PORTS[2]}" "$(expected_peer_ids "${NODE_IDS[0]}" "${NODE_IDS[3]}")" 90
wait_exact_peer_set "${RPC_PORTS[3]}" "$(expected_peer_ids "${NODE_IDS[0]}" "${NODE_IDS[2]}")" 90
assert_service_running cometbft1
if ! isolated_abci_tuple=$(rpc_abci_tuple "${RPC_PORTS[1]}"); then
  echo "ERROR: isolated validator lost its private ABCI path during the P2P-only partition" >&2
  exit 1
fi
echo "isolated validator remained live with private ABCI state ${isolated_abci_tuple}"
governance_heartbeat >/dev/null
governance_heartbeat >/dev/null
wait_progress "${RPC_PORTS[0]}" "${before_one_partition}" 2 90
one_partition_live_height=$(rpc_height "${RPC_PORTS[0]}")
one_partition_isolated_height=$(rpc_height "${RPC_PORTS[1]}")
if [ "${one_partition_isolated_height}" -ge "${one_partition_live_height}" ]; then
  echo "ERROR: isolated validator did not fall behind (${one_partition_isolated_height} vs live ${one_partition_live_height})" >&2
  exit 1
fi
for service in cometbft0 cometbft1 cometbft2 cometbft3; do
  remove_partition_firewall "${service}"
done
assert_matched_apphash "post-one-validator partition" 180

echo "--- fault 2: post-removal stable-IP 2+2 split must halt both live halves ---"
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
wait_exact_peer_set "${RPC_PORTS[0]}" "$(expected_peer_ids "${NODE_IDS[1]}" "${NODE_IDS[2]}" "${NODE_IDS[3]}")" 90
wait_exact_peer_set "${RPC_PORTS[1]}" "$(expected_peer_ids "${NODE_IDS[0]}" "${NODE_IDS[2]}" "${NODE_IDS[3]}")" 90
wait_exact_peer_set "${RPC_PORTS[2]}" "$(expected_peer_ids "${NODE_IDS[0]}" "${NODE_IDS[1]}" "${NODE_IDS[3]}")" 90
wait_exact_peer_set "${RPC_PORTS[3]}" "$(expected_peer_ids "${NODE_IDS[0]}" "${NODE_IDS[1]}" "${NODE_IDS[2]}")" 90
governance_heartbeat >/dev/null
governance_heartbeat >/dev/null
wait_progress "${RPC_PORTS[0]}" "${halt_end[0]}" 2 120
assert_matched_apphash "post-majority-partition heal" 180

versions=()
for port in "${RPC_PORTS[@]}"; do
  versions+=("$(rpc_app_version "${port}")")
done
echo "application versions after fault phases: ${versions[*]}"
for version in "${versions[@]}"; do
  if [ "${version}" -ne 20 ]; then
    echo "ERROR: live validator lifecycle/partition gate ended below app-v20" >&2
    exit 1
  fi
done
final_height=$(rpc_height "${RPC_PORTS[0]}")
assert_validator_set_at "final post-partition validator set" "${final_height}" "${REMOVED_SET}" 120

completion_source_id=$(python3 deploy/scripts/v11.9-source-id.py)
if [ "${completion_source_id}" != "${SOURCE_ID}" ]; then
  echo "ERROR: source tree changed during chaos gate (${SOURCE_ID} -> ${completion_source_id})" >&2
  exit 1
fi
if [ "${V119_REQUIRE_SCOPED_RECONFIG:-0}" = "1" ]; then
  echo "--- running signed app-v20 scope formation/revision subprocess oracle ---"
  V119_REQUIRE_SCOPED_RECONFIG=1 bash deploy/scripts/run-v11.9-multiprocess.sh
  echo "PASS: composed signed scope-reconfiguration + real-Comet partition gate"
fi
if [ "${V119_REQUIRE_AUTHORIZED_STATE_SYNC:-0}" = "1" ]; then
  echo "--- running integrated authorized sage-gui state-sync wire gate ---"
  V119_STATE_SYNC_REBUILD=0 bash deploy/scripts/run-v11.9-state-sync.sh
fi

final_source_id=$(python3 deploy/scripts/v11.9-source-id.py)
if [ "${final_source_id}" != "${SOURCE_ID}" ]; then
  echo "ERROR: source tree changed before the composite gate completed (${SOURCE_ID} -> ${final_source_id})" >&2
  exit 1
fi
for image in sage-v119-chaos-abci:local sage-v119-chaos-node:local; do
  final_image_source_id=$(docker image inspect --format '{{ index .Config.Labels "dev.sage.v119-state-sync.source-id" }}' "${image}")
  if [ "${final_image_source_id}" != "${SOURCE_ID}" ]; then
    echo "ERROR: ${image} source identity changed before the composite gate completed (${SOURCE_ID} -> ${final_image_source_id:-<missing>})" >&2
    exit 1
  fi
done

echo "=== v11.9 REAL MULTI-PROCESS FAULT GATE PASSED ==="
echo "PASS: frozen source ${SOURCE_ID}; signed app-v20 add/update/remove; exact H+2 validator powers; add/update/remove SIGKILL persistence; removed-key no-resurrection; stable-IP P2P partition/heal; strict post-removal 2+2 halt; exact live block/ABCI height/AppHash convergence"
echo "SCOPE: deploy/scripts/run-v11.9-multiprocess.sh supplies signed app-v20 formation/revision and pinned-ballot semantics"
echo "STATE SYNC: set V119_REQUIRE_AUTHORIZED_STATE_SYNC=1 to require the integrated authorized provider-to-pristine-full-node transfer"
