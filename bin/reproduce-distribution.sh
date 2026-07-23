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

# =============================================================================
# Reproducible Build Validation Script
# =============================================================================
# Independently rebuilds TinkerPop release distributions from a git tag inside
# a pinned Docker toolchain and compares the rebuilt artifacts entry-by-entry
# against the published artifacts on dist.apache.org.
#
# This establishes trust WITHOUT relying on the release manager's checksums or
# signatures — the threat model is that the release was built on untrusted or
# compromised hardware.
#
# Usage: reproduce-distribution.sh <GIT_TAG> [DIST_URL_BASE]
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Platform-specific SHA-512 (macOS compatibility)
# ---------------------------------------------------------------------------
if [[ "$OSTYPE" == "darwin"* ]]; then
  sha512sum() { shasum -a 512 "$@"; }
fi

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DOCKER_IMAGE_PREFIX="tinkerpop-reproducible-build"
DOCKERFILE_PATH="${REPO_ROOT}/docker/reproducible-build/Dockerfile"

# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------

usage() {
  cat <<EOF
Usage: $(basename "$0") <GIT_TAG> [DIST_URL_BASE]

  GIT_TAG        Release tag to reproduce (e.g. 3.7.6)
  DIST_URL_BASE  Base URL for published artifacts
                 (default: https://dist.apache.org/repos/dist/release/tinkerpop/<GIT_TAG>)

This script rebuilds TinkerPop distribution artifacts from a git tag inside a
pinned Docker container and compares them entry-by-entry against the published
artifacts. It requires Docker and git to be installed.

EOF
  exit 1
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

info() {
  echo "* $*"
}

warn() {
  echo "  [WARN] $*"
}

check_prerequisites() {
  command -v docker >/dev/null 2>&1 || die "docker is required but not found on PATH"
  command -v git >/dev/null 2>&1 || die "git is required but not found on PATH"
  command -v curl >/dev/null 2>&1 || die "curl is required but not found on PATH"
  command -v unzip >/dev/null 2>&1 || die "unzip is required but not found on PATH"

  docker info >/dev/null 2>&1 || die "docker daemon is not running"
}

# Determine if a JAR filename belongs to TinkerPop (org.apache.tinkerpop)
is_tinkerpop_jar() {
  local filename="$1"
  # TinkerPop JARs: gremlin-*, tinkergraph-*, spark-gremlin*, hadoop-gremlin*,
  # neo4j-gremlin*, tinkerpop-*
  if [[ "$filename" =~ ^(gremlin-|tinkergraph-|spark-gremlin|hadoop-gremlin|neo4j-gremlin|tinkerpop-) ]]; then
    return 0
  fi
  return 1
}

# Compare inner entries of two JARs (recursive comparison)
compare_jar_entries() {
  local jar_rebuilt="$1"
  local jar_published="$2"
  local jar_name="$3"
  local tmp_rebuilt tmp_published
  local has_diff=0

  tmp_rebuilt=$(mktemp -d)
  tmp_published=$(mktemp -d)

  unzip -q -o "$jar_rebuilt" -d "$tmp_rebuilt" 2>/dev/null || true
  unzip -q -o "$jar_published" -d "$tmp_published" 2>/dev/null || true

  # Get sorted entry lists
  local entries_rebuilt entries_published
  entries_rebuilt=$(cd "$tmp_rebuilt" && find . -type f | sort)
  entries_published=$(cd "$tmp_published" && find . -type f | sort)

  # Find entries only in one side
  local only_rebuilt only_published
  only_rebuilt=$(comm -23 <(echo "$entries_rebuilt") <(echo "$entries_published"))
  only_published=$(comm -13 <(echo "$entries_rebuilt") <(echo "$entries_published"))

  if [[ -n "$only_rebuilt" ]]; then
    echo "    Entries only in rebuilt ${jar_name}:"
    echo "$only_rebuilt" | sed 's/^/      /'
    has_diff=1
  fi

  if [[ -n "$only_published" ]]; then
    echo "    Entries only in published ${jar_name}:"
    echo "$only_published" | sed 's/^/      /'
    has_diff=1
  fi

  # Compare common entries by content hash
  local common_entries
  common_entries=$(comm -12 <(echo "$entries_rebuilt") <(echo "$entries_published"))
  while IFS= read -r entry; do
    [[ -z "$entry" ]] && continue
    local hash_r hash_p
    hash_r=$(sha512sum "${tmp_rebuilt}/${entry}" | awk '{print $1}')
    hash_p=$(sha512sum "${tmp_published}/${entry}" | awk '{print $1}')
    if [[ "$hash_r" != "$hash_p" ]]; then
      echo "    DIFFERS: ${jar_name}!${entry}"
      has_diff=1
    fi
  done <<< "$common_entries"

  rm -rf "$tmp_rebuilt" "$tmp_published"
  return $has_diff
}

# Compare two ZIP artifacts entry-by-entry
# Returns 0 if PASS, 1 if FAIL (TinkerPop entries differ), 2 if only external deps differ
compare_zip_entries() {
  local zip_rebuilt="$1"
  local zip_published="$2"
  local artifact_name="$3"
  local has_tp_diff=0
  local has_ext_diff=0

  # Extract entry listings: path, size, CRC from unzip -v
  # unzip -v format: Length Method Size Cmpr Date Time CRC-32 Name
  local tmp_rebuilt_list tmp_published_list
  tmp_rebuilt_list=$(mktemp)
  tmp_published_list=$(mktemp)

  # Get sorted list of entries with size and CRC-32
  # unzip -v format: Length Method Size Cmpr Date Time CRC-32 Name
  # Note: $NF is used for path which assumes no spaces in entry names (safe for Maven artifacts)
  unzip -v "$zip_rebuilt" 2>/dev/null | awk '/^[- ]*$/{p=!p;next} p && NF>=8 {print $NF, $1, $7}' | sort > "$tmp_rebuilt_list"
  unzip -v "$zip_published" 2>/dev/null | awk '/^[- ]*$/{p=!p;next} p && NF>=8 {print $NF, $1, $7}' | sort > "$tmp_published_list"

  # Compare entry paths
  local paths_rebuilt paths_published
  paths_rebuilt=$(awk '{print $1}' "$tmp_rebuilt_list")
  paths_published=$(awk '{print $1}' "$tmp_published_list")

  local only_rebuilt only_published
  only_rebuilt=$(comm -23 <(echo "$paths_rebuilt") <(echo "$paths_published"))
  only_published=$(comm -13 <(echo "$paths_rebuilt") <(echo "$paths_published"))

  if [[ -n "$only_rebuilt" ]]; then
    echo "  Entries only in rebuilt ${artifact_name}:"
    echo "$only_rebuilt" | head -20 | sed 's/^/    /'
    local count
    count=$(echo "$only_rebuilt" | wc -l | tr -d ' ')
    [[ $count -gt 20 ]] && echo "    ... and $((count - 20)) more"
    has_tp_diff=1
  fi

  if [[ -n "$only_published" ]]; then
    echo "  Entries only in published ${artifact_name}:"
    echo "$only_published" | head -20 | sed 's/^/    /'
    local count
    count=$(echo "$only_published" | wc -l | tr -d ' ')
    [[ $count -gt 20 ]] && echo "    ... and $((count - 20)) more"
    has_tp_diff=1
  fi

  # Compare entries present in both (size + CRC)
  local common_paths differing_entries=""
  common_paths=$(comm -12 <(echo "$paths_rebuilt") <(echo "$paths_published"))

  while IFS= read -r path; do
    [[ -z "$path" ]] && continue
    local info_r info_p
    info_r=$(grep -F "$path " "$tmp_rebuilt_list" | head -1)
    info_p=$(grep -F "$path " "$tmp_published_list" | head -1)
    if [[ "$info_r" != "$info_p" ]]; then
      differing_entries="${differing_entries}${path}\n"
    fi
  done <<< "$common_paths"

  rm -f "$tmp_rebuilt_list" "$tmp_published_list"

  if [[ -z "$differing_entries" ]] && [[ $has_tp_diff -eq 0 ]]; then
    return 0
  fi

  # Classify differing entries as TinkerPop or external
  if [[ -n "$differing_entries" ]]; then
    local tp_diffs="" ext_diffs=""
    while IFS= read -r entry_path; do
      [[ -z "$entry_path" ]] && continue
      local basename_entry
      basename_entry=$(basename "$entry_path")
      if [[ "$basename_entry" == *.jar ]] && ! is_tinkerpop_jar "$basename_entry"; then
        ext_diffs="${ext_diffs}${entry_path}\n"
      else
        tp_diffs="${tp_diffs}${entry_path}\n"
      fi
    done < <(echo -e "$differing_entries")

    if [[ -n "$ext_diffs" ]]; then
      echo "  External dependencies (from Maven Central — not built from source, skipped):"
      echo -e "$ext_diffs" | head -10 | sed 's/^/    /'
      local ext_count
      ext_count=$(echo -e "$ext_diffs" | grep -c . || true)
      [[ $ext_count -gt 10 ]] && echo "    ... and $((ext_count - 10)) more"
      has_ext_diff=1
    fi

    if [[ -n "$tp_diffs" ]]; then
      echo "  TinkerPop entries with differing size/CRC:"
      echo -e "$tp_diffs" | sed 's/^/    /'
      has_tp_diff=1

      # Recurse into differing TinkerPop JARs
      local tmp_r_extract tmp_p_extract
      tmp_r_extract=$(mktemp -d)
      tmp_p_extract=$(mktemp -d)
      unzip -q -o "$zip_rebuilt" -d "$tmp_r_extract" 2>/dev/null || true
      unzip -q -o "$zip_published" -d "$tmp_p_extract" 2>/dev/null || true

      while IFS= read -r entry_path; do
        [[ -z "$entry_path" ]] && continue
        local basename_entry
        basename_entry=$(basename "$entry_path")
        if [[ "$basename_entry" == *.jar ]] && is_tinkerpop_jar "$basename_entry"; then
          local jar_r="${tmp_r_extract}/${entry_path}"
          local jar_p="${tmp_p_extract}/${entry_path}"
          if [[ -f "$jar_r" ]] && [[ -f "$jar_p" ]]; then
            echo "  Recursing into TinkerPop JAR: ${basename_entry}"
            compare_jar_entries "$jar_r" "$jar_p" "$basename_entry" || true
          fi
        fi
      done < <(echo -e "$tp_diffs")

      rm -rf "$tmp_r_extract" "$tmp_p_extract"
    fi
  fi

  if [[ $has_tp_diff -ne 0 ]]; then
    return 1
  elif [[ $has_ext_diff -ne 0 ]]; then
    return 2
  fi
  return 0
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

# Parse arguments
GIT_TAG="${1:-}"
if [[ -z "$GIT_TAG" ]]; then
  usage
fi

# Validate GIT_TAG format to prevent injection via docker build -t or git clone --branch
if ! [[ "$GIT_TAG" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-rc[0-9]+)?$ ]]; then
  die "Invalid GIT_TAG '${GIT_TAG}': must match pattern <major>.<minor>.<patch> with optional -rcN suffix (e.g. 3.7.6, 3.7.7-rc1)"
fi

DIST_URL_BASE="${2:-https://dist.apache.org/repos/dist/release/tinkerpop/${GIT_TAG}}"

echo "============================================================================="
echo " Reproducible Build Validation for Apache TinkerPop ${GIT_TAG}"
echo "============================================================================="
echo ""
echo "  Git tag:    ${GIT_TAG}"
echo "  Dist URL:   ${DIST_URL_BASE}"
echo ""

# Preconditions
info "checking prerequisites ..."
check_prerequisites
echo "  OK"

# Create working directory
WORK_DIR=$(mktemp -d)
info "working directory: ${WORK_DIR}"

cleanup() {
  local exit_code=$?
  if [[ $exit_code -ne 0 ]]; then
    echo ""
    echo "Build artifacts preserved for inspection at: ${WORK_DIR}"
  else
    rm -rf "$WORK_DIR"
  fi
  # Remove the Docker image built for this validation run (non-fatal if it fails)
  if [[ -n "${DOCKER_TAG:-}" ]]; then
    docker rmi "$DOCKER_TAG" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Step 1: Build the Docker image
# ---------------------------------------------------------------------------
DOCKER_TAG="${DOCKER_IMAGE_PREFIX}:${GIT_TAG}"
info "building Docker image (${DOCKER_TAG}) ..."

if [[ ! -f "$DOCKERFILE_PATH" ]]; then
  die "Dockerfile not found at ${DOCKERFILE_PATH}. Run Phase 5 first."
fi

docker build -t "$DOCKER_TAG" -f "$DOCKERFILE_PATH" "$(dirname "$DOCKERFILE_PATH")" \
  > "${WORK_DIR}/docker-build.log" 2>&1 || {
  echo "  FAILED (see ${WORK_DIR}/docker-build.log)"
  cat "${WORK_DIR}/docker-build.log"
  exit 1
}
echo "  OK"

# ---------------------------------------------------------------------------
# Step 2: Check out the tag into a temp directory
# ---------------------------------------------------------------------------
SOURCE_DIR="${WORK_DIR}/source"
info "checking out tag ${GIT_TAG} ..."

git clone --depth 1 --branch "$GIT_TAG" "file://${REPO_ROOT}" "$SOURCE_DIR" \
  > "${WORK_DIR}/git-clone.log" 2>&1 || {
  echo "  FAILED — tag '${GIT_TAG}' not found in repository"
  cat "${WORK_DIR}/git-clone.log"
  exit 1
}
echo "  OK"

# ---------------------------------------------------------------------------
# Step 3: Build inside Docker
# ---------------------------------------------------------------------------
BUILD_LOG="${WORK_DIR}/mvn-build.log"
info "building from source inside Docker (this may take a while) ..."

# -Papache-release is retained because it activates the source-release assembly that produces the
# src zip we compare.  GPG signing is skipped (-Dgpg.skip=true) because signatures are detached
# .asc files that are not part of the compared artifacts and would otherwise require the release
# manager's private signing key, which a validator must not need.
docker run --rm \
  -v "${SOURCE_DIR}:/build:rw" \
  -v "${WORK_DIR}/m2-repo:/root/.m2/repository:rw" \
  -w /build \
  "$DOCKER_TAG" \
  mvn clean install -Papache-release -DskipTests -DskipImageBuild -Dgpg.skip=true \
  > "$BUILD_LOG" 2>&1 || {
  echo "  FAILED (see ${BUILD_LOG})"
  echo ""
  echo "  Last 30 lines of build log:"
  tail -30 "$BUILD_LOG" | sed 's/^/    /'
  exit 1
}
echo "  OK"

# ---------------------------------------------------------------------------
# Step 4: Locate rebuilt artifacts and map to published names
# ---------------------------------------------------------------------------
info "locating rebuilt artifacts ..."

# The rebuilt artifacts use Maven assembly finalName; published ones may be renamed.
# Assembly finalName for console/server: apache-tinkerpop-${artifactId}-${version}
# Mapping to published names (per release.asciidoc):
#   apache-tinkerpop-gremlin-console-<ver>-distribution.zip -> apache-tinkerpop-gremlin-console-<ver>-bin.zip
#   apache-tinkerpop-gremlin-server-<ver>-distribution.zip  -> apache-tinkerpop-gremlin-server-<ver>-bin.zip
#   tinkerpop-<ver>-source-release.zip                      -> apache-tinkerpop-<ver>-src.zip

CONSOLE_REBUILT="${SOURCE_DIR}/gremlin-console/target/apache-tinkerpop-gremlin-console-${GIT_TAG}-distribution.zip"
SERVER_REBUILT="${SOURCE_DIR}/gremlin-server/target/apache-tinkerpop-gremlin-server-${GIT_TAG}-distribution.zip"
SOURCE_REBUILT="${SOURCE_DIR}/target/tinkerpop-${GIT_TAG}-source-release.zip"

# Published names on dist.apache.org
CONSOLE_PUBLISHED_NAME="apache-tinkerpop-gremlin-console-${GIT_TAG}-bin.zip"
SERVER_PUBLISHED_NAME="apache-tinkerpop-gremlin-server-${GIT_TAG}-bin.zip"
SOURCE_PUBLISHED_NAME="apache-tinkerpop-${GIT_TAG}-src.zip"

# Verify rebuilt artifacts exist
for artifact in "$CONSOLE_REBUILT" "$SERVER_REBUILT" "$SOURCE_REBUILT"; do
  if [[ ! -f "$artifact" ]]; then
    die "Expected rebuilt artifact not found: ${artifact}"
  fi
done
echo "  OK — all three artifacts found"

# ---------------------------------------------------------------------------
# Step 5: Download published artifacts
# ---------------------------------------------------------------------------
PUBLISHED_DIR="${WORK_DIR}/published"
mkdir -p "$PUBLISHED_DIR"

info "downloading published artifacts from ${DIST_URL_BASE} ..."

for name in "$CONSOLE_PUBLISHED_NAME" "$SERVER_PUBLISHED_NAME" "$SOURCE_PUBLISHED_NAME"; do
  echo -n "  * ${name} ... "
  curl -Lsf "${DIST_URL_BASE}/${name}" -o "${PUBLISHED_DIR}/${name}" || {
    echo "FAILED"
    die "Failed to download ${DIST_URL_BASE}/${name}"
  }
  echo "OK"
done

# ---------------------------------------------------------------------------
# Step 6: Compare rebuilt vs published
# ---------------------------------------------------------------------------
echo ""
echo "============================================================================="
echo " Comparison Results"
echo "============================================================================="
echo ""

# Declare artifact pairs: rebuilt_path:published_path:display_name
declare -a ARTIFACT_PAIRS=(
  "${CONSOLE_REBUILT}:${PUBLISHED_DIR}/${CONSOLE_PUBLISHED_NAME}:gremlin-console"
  "${SERVER_REBUILT}:${PUBLISHED_DIR}/${SERVER_PUBLISHED_NAME}:gremlin-server"
  "${SOURCE_REBUILT}:${PUBLISHED_DIR}/${SOURCE_PUBLISHED_NAME}:source"
)

declare -a RESULTS=()
OVERALL_EXIT=0

for pair in "${ARTIFACT_PAIRS[@]}"; do
  IFS=':' read -r rebuilt_path published_path display_name <<< "$pair"

  echo "--- ${display_name} ---"
  echo ""

  # Whole-file SHA-512 comparison
  hash_rebuilt=$(sha512sum "$rebuilt_path" | awk '{print $1}')
  hash_published=$(sha512sum "$published_path" | awk '{print $1}')

  if [[ "$hash_rebuilt" == "$hash_published" ]]; then
    echo "  SHA-512: MATCH (bit-for-bit identical)"
    echo ""
    RESULTS+=("${display_name}: PASS (bit-for-bit)")
  else
    echo "  SHA-512: MISMATCH"
    echo "    rebuilt:   ${hash_rebuilt}"
    echo "    published: ${hash_published}"
    echo ""
    echo "  Performing entry-by-entry comparison ..."
    echo ""

    set +e
    compare_zip_entries "$rebuilt_path" "$published_path" "$display_name"
    cmp_result=$?
    set -e

    case $cmp_result in
      0)
        echo "  Entry comparison: all entries match"
        RESULTS+=("${display_name}: PASS (entries match)")
        ;;
      2)
        echo "  Entry comparison: TinkerPop entries match; only external deps differ"
        RESULTS+=("${display_name}: PASS (entries match, only external deps differ)")
        ;;
      1)
        echo "  Entry comparison: TinkerPop entries DIFFER"
        RESULTS+=("${display_name}: FAIL")
        OVERALL_EXIT=1
        ;;
    esac

    # Optionally use diffoscope for richer diff
    if command -v diffoscope >/dev/null 2>&1; then
      echo ""
      echo "  Running diffoscope for detailed diff ..."
      diffoscope_out="${WORK_DIR}/diffoscope-${display_name}.txt"
      diffoscope "$rebuilt_path" "$published_path" > "$diffoscope_out" 2>&1 || true
      echo "  Diffoscope output saved to: ${diffoscope_out}"
    fi
  fi

  echo ""
done

# ---------------------------------------------------------------------------
# Step 7: Final summary
# ---------------------------------------------------------------------------
echo "============================================================================="
echo " SUMMARY"
echo "============================================================================="
echo ""
for result in "${RESULTS[@]}"; do
  echo "  ${result}"
done
echo ""

if [[ $OVERALL_EXIT -eq 0 ]]; then
  echo "  OVERALL: PASS — all artifacts are reproducible"
else
  echo "  OVERALL: FAIL — one or more artifacts could not be reproduced"
  echo ""
  echo "  Build artifacts preserved at: ${WORK_DIR}"
fi

echo ""
exit $OVERALL_EXIT
