#!/usr/bin/env bash
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

# Verify TinkerPop development environment prerequisites.
# Usage: bash .skills/tinkerpop-dev/scripts/check-env.sh

set -uo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

pass=0
warn=0
fail=0

ok()   { echo -e "  ${GREEN}✓${NC} $1"; pass=$((pass + 1)); }
skip() { echo -e "  ${YELLOW}○${NC} $1"; warn=$((warn + 1)); }
bad()  { echo -e "  ${RED}✗${NC} $1"; fail=$((fail + 1)); }

echo "TinkerPop Development Environment Check"
echo "========================================"
echo ""

# --- Java ---
echo "Core prerequisites:"
if command -v java &>/dev/null; then
    java_version=$(java -version 2>&1 | head -1 | sed 's/.*"\(.*\)".*/\1/' | cut -d. -f1)
    if [[ "$java_version" -eq 11 || "$java_version" -eq 17 ]]; then
        ok "Java $java_version (11 or 17 required)"
    elif [[ "$java_version" -gt 17 ]]; then
        bad "Java $java_version found — only Java 11 or 17 are supported (use sdkman.io to switch)"
    else
        bad "Java $java_version found — version 11 or 17 required (use sdkman.io to install)"
    fi
else
    bad "Java not found — install Java 11 or 17 (use sdkman.io)"
fi

# --- Maven ---
if command -v mvn &>/dev/null; then
    mvn_version=$(mvn --version 2>/dev/null | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)
    ok "Maven $mvn_version (3.5.3+ required)"
else
    bad "Maven not found — install Maven 3.5.3+ (try sdkman.io)"
fi

# --- Docker ---
if command -v docker &>/dev/null; then
    if docker info &>/dev/null; then
        ok "Docker installed and running"
    else
        bad "Docker installed but not running — start Docker Desktop or the daemon"
    fi
else
    bad "Docker not found — install Docker Desktop (includes Compose)"
fi

# --- Docker Compose ---
if docker compose version &>/dev/null 2>&1; then
    ok "Docker Compose available"
elif command -v docker-compose &>/dev/null; then
    ok "Docker Compose (standalone) available"
else
    skip "Docker Compose not found — needed for GLV tests"
fi

echo ""
echo "Optional (GLV development):"

# --- Python ---
if command -v python3 &>/dev/null; then
    py_version=$(python3 --version 2>&1 | grep -oE '[0-9]+\.[0-9]+')
    ok "Python $py_version (3.10+ recommended for gremlin-python local dev)"
else
    skip "Python 3 not found — Docker handles test execution, but local dev needs it"
fi

# --- Node.js ---
if command -v node &>/dev/null; then
    node_version=$(node --version 2>/dev/null | sed 's/v//')
    ok "Node.js $node_version (22+ recommended for gremlin-js local dev)"
else
    skip "Node.js not found — Maven downloads a local copy automatically"
fi

# --- .NET SDK ---
if command -v dotnet &>/dev/null; then
    dotnet_version=$(dotnet --version 2>/dev/null)
    ok ".NET SDK $dotnet_version (8.0+ recommended for gremlin-dotnet local dev)"
else
    skip ".NET SDK not found — Docker handles test execution"
fi

# --- Go ---
if command -v go &>/dev/null; then
    go_version=$(go version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?')
    ok "Go $go_version (1.25+ recommended for gremlin-go local dev)"
else
    skip "Go not found — Docker handles test execution"
fi

echo ""
echo "GLV activation status:"

# --- .glv sentinel files ---
check_glv() {
    local name="$1"
    shift
    local found=false
    for f in "$@"; do
        if [[ -f "$f" ]]; then
            found=true
        fi
    done
    if $found; then
        ok "$name — activated (.glv present)"
    else
        skip "$name — not activated (create .glv to enable in standard builds)"
    fi
}

check_glv "gremlin-python" "gremlin-python/.glv"
check_glv "gremlin-dotnet" "gremlin-dotnet/src/.glv" "gremlin-dotnet/test/.glv"
check_glv "gremlin-go" "gremlin-go/.glv"

echo ""
echo "----------------------------------------"
echo -e "Results: ${GREEN}$pass passed${NC}, ${YELLOW}$warn skipped${NC}, ${RED}$fail failed${NC}"

if [[ $fail -gt 0 ]]; then
    echo ""
    echo "Fix the failed items above before building. See references/dev-environment-setup.md"
    exit 1
fi
