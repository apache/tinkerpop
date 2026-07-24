#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Validate the agent-friendly documentation (agentdocsspec.com) — the Markdown mirror, the
# llms.txt discovery index, page sizes, link resolution, and the per-page directive — using the
# spec's own tool, `npx afdocs check`.
#
# Usage:
#   bin/validate-llms-txt.sh                 # check the published current docs
#                                            #   (https://tinkerpop.apache.org/docs/current/)
#   bin/validate-llms-txt.sh -v 3.7.7        # check a published version
#   bin/validate-llms-txt.sh --version 3.7.7 #   (https://tinkerpop.apache.org/docs/3.7.7/)
#   bin/validate-llms-txt.sh -v local        # check the LOCAL build: replicate the publish layout
#                                            #   (markdown + html + images together) under
#                                            #   target/docs/serve/, serve it, and check localhost
#
# Any extra arguments after the mode are passed through to `afdocs check` (e.g. --fixes, --verbose,
# --format json, --skip-checks ...).
#
# NOTE: afdocs requires Node.js >= 22; this script warns if the active node is older. A plain local
# static server cannot exercise the hosting-dependent checks (content-negotiation, redirects,
# soft-404, cache headers, auth) — those are only meaningful against the real published site.

set -e

cd "$(dirname "$0")/.."
TP_HOME="$(pwd)"

BASE_URL="https://tinkerpop.apache.org/docs"
VERSION="current"

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
PASSTHROUGH=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -v|--version) VERSION="$2"; shift 2 ;;
    -h|--help)
      sed -n '19,40p' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) PASSTHROUGH+=("$1"); shift ;;
  esac
done

# ---------------------------------------------------------------------------
# Node version advisory (afdocs requires >= 22)
# ---------------------------------------------------------------------------
if command -v node >/dev/null 2>&1; then
  NODE_MAJOR=$(node -v | sed 's/^v//; s/\..*//')
  if [ "${NODE_MAJOR:-0}" -lt 22 ]; then
    echo "WARNING: afdocs requires Node.js >= 22 but found $(node -v)."
    echo "         Results may be unreliable. Install a newer node (e.g. 'mise install node@22')."
  fi
else
  echo "ERROR: node/npx not found on PATH; afdocs needs Node.js >= 22."
  exit 1
fi

# ---------------------------------------------------------------------------
# Local mode: replicate the publish layout and serve it
# ---------------------------------------------------------------------------
HTTP_PID=""
cleanup() {
  set +e
  [ -n "${HTTP_PID}" ] && kill "${HTTP_PID}" 2>/dev/null
}
trap cleanup EXIT INT TERM

run_afdocs() {
  local url="$1"; shift
  local extra=("$@")
  echo "Running: npx --yes afdocs check ${url} ${extra[*]} ${PASSTHROUGH[*]}"
  echo "-----------------------------------------------------------------------"
  npx --yes afdocs@latest check "${url}" "${extra[@]}" "${PASSTHROUGH[@]}"
}

if [ "${VERSION}" == "local" ]; then
  MD_DIR="target/docs/markdown"
  HTML_DIR="target/docs/htmlsingle"
  SERVE_DIR="target/docs/serve"

  if [ ! -d "${MD_DIR}" ] || [ ! -f "${MD_DIR}/llms.txt" ]; then
    echo "ERROR: No local Markdown docs found at ${MD_DIR} (with llms.txt)."
    echo "       Generate them first, e.g.:  bin/process-docs.sh --dryRun"
    echo "       (a --dryRun build produces the Markdown mirror + llms.txt without a live server;"
    echo "        a full 'bin/process-docs.sh' additionally fills in executed ==> output.)"
    exit 1
  fi

  echo "Replicating publish layout into ${SERVE_DIR}/ (markdown + html + images together)..."
  rm -rf "${SERVE_DIR}"
  mkdir -p "${SERVE_DIR}"
  # HTML first (brings images/, stylesheets, and the .html pages), then overlay the Markdown mirror
  # so .md sits beside .html and llms.txt lands at the served root — mirroring bin/publish-docs.sh.
  if [ -d "${HTML_DIR}" ]; then
    cp -R "${HTML_DIR}/." "${SERVE_DIR}/"
  else
    echo "NOTE: ${HTML_DIR} not found; serving Markdown only (HTML/parity checks will be limited)."
  fi
  cp -R "${MD_DIR}/." "${SERVE_DIR}/"

  # Pick a free port up front so we can bake the absolute base URL into llms.txt before serving.
  PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
  LOCAL_URL="http://127.0.0.1:${PORT}/"

  # Regenerate the served llms.txt with ABSOLUTE URLs (http://127.0.0.1:PORT/...). The spec's
  # link-resolution/coverage checks only recognize full http(s):// links, so this mirrors what
  # publish-docs.sh does with the canonical site URL and lets the local run exercise those checks.
  EXT_CLASSES="docs/tinkeradoc-extension/target/classes"
  if [ -d "${EXT_CLASSES}" ]; then
    echo "Regenerating served llms.txt with absolute URLs for ${LOCAL_URL} ..."
    java -cp "${EXT_CLASSES}" org.apache.tinkerpop.tinkeradoc.LlmsTxtGenerator \
      --prefix "${LOCAL_URL}" --out "${SERVE_DIR}/llms.txt" "${SERVE_DIR}"
  else
    echo "NOTE: ${EXT_CLASSES} not found; serving llms.txt as-is (relative links won't satisfy"
    echo "      the link-resolution/coverage checks). Build the extension to enable absolute URLs."
  fi

  echo "Serving ${SERVE_DIR}/ at ${LOCAL_URL} ..."
  # A threaded server: afdocs issues concurrent requests, and python's default single-threaded
  # http.server can drop connections mid-response (undici in afdocs then aborts). ThreadingHTTPServer
  # handles the concurrency cleanly.
  python3 -c "
import sys, http.server, socketserver, functools
port = int(sys.argv[1]); root = sys.argv[2]
handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=root)
class T(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True
T(('127.0.0.1', port), handler).serve_forever()
" "${PORT}" "${SERVE_DIR}" >/dev/null 2>&1 &
  HTTP_PID=$!

  # Wait for readiness (loopback works within this single script invocation).
  for i in $(seq 1 30); do
    if curl -sf "http://127.0.0.1:${PORT}/llms.txt" >/dev/null 2>&1; then break; fi
    if ! kill -0 "${HTTP_PID}" 2>/dev/null; then
      echo "ERROR: local web server exited during startup."; exit 1
    fi
    [ "$i" -eq 30 ] && { echo "ERROR: local web server did not become ready."; exit 1; }
    sleep 0.5
  done

  # Local-run options (each skippable by passing your own):
  #  --canonical-origin: tells afdocs the localhost origin is canonical, so the absolute llms.txt
  #      links (baked in above) count as same-origin and its link-resolution checks run.
  #  --max-concurrency 1 / --request-delay: afdocs' HTTP client (undici) can abort on many-page
  #      concurrent crawls against a lightweight local server; serial requests are reliable.
  LOCAL_OPTS=()
  [[ " ${PASSTHROUGH[*]} " == *" --canonical-origin "* ]] || LOCAL_OPTS+=(--canonical-origin "http://127.0.0.1:${PORT}")
  [[ " ${PASSTHROUGH[*]} " == *" --max-concurrency "* ]] || LOCAL_OPTS+=(--max-concurrency 1)
  [[ " ${PASSTHROUGH[*]} " == *" --request-delay "* ]] || LOCAL_OPTS+=(--request-delay 20)
  run_afdocs "${LOCAL_URL}" "${LOCAL_OPTS[@]}"
  echo "-----------------------------------------------------------------------"
  echo "NOTE: hosting-dependent checks (content-negotiation, redirects, soft-404, cache headers,"
  echo "      auth) are not meaningful against this local static server; validate those against"
  echo "      the published site."
else
  run_afdocs "${BASE_URL}/${VERSION}/"
fi
