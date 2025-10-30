#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

IMAGE="tinkerpop/gremlin-server:3.7.4"
CONTAINER_NAME="cmdb-gremlin"
HOST="localhost"
PORT="8182"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# Dataset lives at docs/upgrade/spec/cmdb-data.gremlin relative to this script
DATASET_PATH="${SCRIPT_DIR}/../../spec/cmdb-data.gremlin"

usage() {
  cat <<EOF
Usage: $(basename "$0") <command>

Commands:
  up           Start Gremlin Server Docker container (${IMAGE}) on port ${PORT}
  wait         Wait until Gremlin Server is ready to accept requests
  seed         Load dataset from ${DATASET_PATH} into the running server
  reset        Recreate container (down + up) and seed dataset
  down         Stop and remove the Gremlin Server container
  logs         Tail container logs
  status       Show container status

Environment overrides:
  IMAGE            Docker image (default: ${IMAGE})
  CONTAINER_NAME   Container name (default: ${CONTAINER_NAME})
  HOST             Hostname for HTTP (default: ${HOST})
  PORT             Host port for HTTP (default: ${PORT})
EOF
}

container_exists() { docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; }
container_running() { docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; }

cmd_up() {
  if container_running; then
    echo "[INFO] Container ${CONTAINER_NAME} already running"
    return 0
  fi
  if container_exists; then
    echo "[INFO] Starting existing container ${CONTAINER_NAME}"
    docker start "${CONTAINER_NAME}" >/dev/null
  else
    echo "[INFO] Pulling image ${IMAGE} (if needed)"
    docker pull "${IMAGE}" >/dev/null || true
    echo "[INFO] Creating and starting container ${CONTAINER_NAME}"
    docker run -d --name "${CONTAINER_NAME}" -p "${PORT}:8182" "${IMAGE}" conf/gremlin-server.yaml >/dev/null
  fi
  echo "[INFO] Container is starting..."
}

cmd_wait() {
  local timeout=90
  local start_ts
  start_ts=$(date +%s)
  echo "[INFO] Waiting for Gremlin Server at http://${HOST}:${PORT}/gremlin (timeout ${timeout}s)"
  while true; do
    # Require explicit HTTP 200 from the /gremlin endpoint
    local resp
    resp=$(curl -sS -w "%{http_code}" -H 'Content-Type: application/json' \
      -X POST "http://${HOST}:${PORT}/gremlin" \
      --data '{"gremlin":"g.inject(1).count()"}')
    local http_code
    http_code="${resp: -3}"
    if [[ "$http_code" == "200" ]]; then
      break
    fi
    sleep 2
    local now
    now=$(date +%s)
    if (( now - start_ts > timeout )); then
      echo "[ERROR] Timed out waiting for Gremlin Server to become ready (last HTTP code: ${http_code})" >&2
      exit 1
    fi
  done
  echo "[INFO] Gremlin Server is ready"
}

cmd_seed() {
  if ! container_running; then
    echo "[ERROR] Container ${CONTAINER_NAME} is not running; start it with: $0 up && $0 wait" >&2
    exit 1
  fi
  if [[ ! -f "${DATASET_PATH}" ]]; then
    echo "[ERROR] Dataset not found at ${DATASET_PATH}" >&2
    exit 1
  fi
  echo "[INFO] Loading dataset from ${DATASET_PATH}"

  # Clear existing graph first to ensure a clean, deterministic state
  clear_payload=$(printf '%s' "g.V().drop().iterate()" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g')
  clear_resp=$(curl -sS -w "%{http_code}" -H 'Content-Type: application/json' \
    -X POST "http://${HOST}:${PORT}/gremlin" \
    --data "{\"gremlin\":\"${clear_payload}\"}")
  clear_code="${clear_resp: -3}"
  clear_body="${clear_resp::-3}"
  if [[ "$clear_code" != "200" ]]; then
    echo "[ERROR] Failed to clear graph with g.V().drop().iterate()" >&2
    echo "[ERROR] HTTP ${clear_code} Response: ${clear_body}" >&2
    exit 1
  fi

  # Post each non-empty, non-comment line to the /gremlin endpoint
  local line_no=0
  while IFS= read -r raw_line || [[ -n "$raw_line" ]]; do
    line_no=$((line_no + 1))
    # Trim leading/trailing whitespace
    line="${raw_line%%[[:space:]]*}"
    line="${raw_line%$'\r'}" # strip CR if present
    trimmed="$(echo "$raw_line" | sed -e 's/^\s\+//' -e 's/\s\+$//')"

    # Skip empty lines and comments (// ... or # ...)
    if [[ -z "${trimmed}" ]] || [[ "${trimmed}" =~ ^// ]] || [[ "${trimmed}" =~ ^# ]]; then
      continue
    fi

    # Escape JSON special chars in the Gremlin line: backslash and quotes
    esc_line=$(printf '%s' "$trimmed" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g')

    # Submit
    resp=$(curl -sS -w "%{http_code}" -H 'Content-Type: application/json' \
      -X POST "http://${HOST}:${PORT}/gremlin" \
      --data "{\"gremlin\":\"${esc_line}\"}")

    http_code="${resp: -3}"
    body="${resp::-3}"

    if [[ "$http_code" != "200" ]]; then
      echo "[ERROR] Failed at line ${line_no}: ${trimmed}" >&2
      echo "[ERROR] HTTP ${http_code} Response: ${body}" >&2
      exit 1
    fi
  done < "${DATASET_PATH}"

  echo "[INFO] Dataset load complete"
}

cmd_reset() {
  cmd_down || true
  cmd_up
  cmd_wait
  cmd_seed
}

cmd_down() {
  if container_exists; then
    echo "[INFO] Stopping and removing ${CONTAINER_NAME}"
    docker rm -f "${CONTAINER_NAME}" >/dev/null || true
  else
    echo "[INFO] No existing container named ${CONTAINER_NAME}"
  fi
}

cmd_logs() {
  if container_exists; then
    docker logs -f "${CONTAINER_NAME}"
  else
    echo "[INFO] No existing container named ${CONTAINER_NAME}"
  fi
}

cmd_status() {
  if container_running; then
    echo "running"
  elif container_exists; then
    echo "stopped"
  else
    echo "absent"
  fi
}

main() {
  local cmd="${1:-}" || true
  case "${cmd}" in
    up) cmd_up ;;
    wait) cmd_wait ;;
    seed) cmd_seed ;;
    reset) cmd_reset ;;
    down) cmd_down ;;
    logs) cmd_logs ;;
    status) cmd_status ;;
    -h|--help|help|"") usage ;;
    *) echo "Unknown command: ${cmd}"; usage; exit 1 ;;
  esac
}

main "$@"
