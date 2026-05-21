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

# Orchestration script for the TinkerPop documentation build.
#
# Full build (--fullRun, default):
#   Validates console/server distributions, installs plugins, starts
#   Gremlin Server and Gephi mock, then invokes Maven with the
#   AsciidoctorJ extension to render docs with live code execution.
#
# Dry-run (--dryRun):
#   Invokes Maven with -Dgremlin-docs-dryrun=true. No server, console,
#   or plugins required.

set -e

cd "$(dirname "$0")/.."
TP_HOME="$(pwd)"

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
MODE="full"
NOCLEAN=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --fullRun|-f)  MODE="full";  shift ;;
    --dryRun|-d)   MODE="dry";   shift ;;
    --noClean|-n)  NOCLEAN="1";  shift ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# ---------------------------------------------------------------------------
# Cleanup trap
# ---------------------------------------------------------------------------
GREMLIN_SERVER_PID=""
GEPHI_MOCK_PID=""

cleanup() {
  set +e
  [ -n "${GREMLIN_SERVER_PID}" ] && kill "${GREMLIN_SERVER_PID}" 2>/dev/null
  [ -n "${GEPHI_MOCK_PID}" ] && kill "${GEPHI_MOCK_PID}" 2>/dev/null
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Dry-run mode
# ---------------------------------------------------------------------------
if [ "${MODE}" == "dry" ]; then
  if [ -z "${NOCLEAN}" ]; then
    rm -rf target/postprocess-asciidoc target/doc-source target/docs 2>/dev/null || true
  fi
  echo "Copying docs sources to target/postprocess-asciidoc/..."
  mkdir -p target/postprocess-asciidoc
  cp -r docs/{static,stylesheets} target/postprocess-asciidoc/
  cp -r docs/src/* target/postprocess-asciidoc/
  mvn process-resources -Dasciidoc -Dgremlin.docs.dryrun=true
  exit $?
fi

# ---------------------------------------------------------------------------
# Full build mode
# ---------------------------------------------------------------------------

# Resolve version from pom.xml
TP_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout 2>/dev/null || \
  grep -A1 '<artifactId>tinkerpop</artifactId>' pom.xml | grep -o 'version>[^<]*' | grep -o '>.*' | cut -d '>' -f2 | head -n1)

# 1. Validate console distribution
CONSOLE_DIR=$(ls -d gremlin-console/target/apache-tinkerpop-gremlin-console-*-standalone 2>/dev/null | head -n1)
if [ -z "${CONSOLE_DIR}" ] || [ ! -d "${CONSOLE_DIR}" ]; then
  echo "ERROR: Gremlin Console distribution not found."
  echo "Build it first: mvn clean install -pl :gremlin-console -am -DskipTests"
  exit 1
fi
CONSOLE_HOME="$(cd "${CONSOLE_DIR}" && pwd)"

# 2. Validate server distribution
SERVER_DIR=$(ls -d gremlin-server/target/apache-tinkerpop-gremlin-server-*-standalone 2>/dev/null | head -n1)
if [ -z "${SERVER_DIR}" ] || [ ! -d "${SERVER_DIR}" ]; then
  echo "ERROR: Gremlin Server distribution not found."
  echo "Build it first: mvn clean install -pl :gremlin-server -am -DskipTests"
  exit 1
fi
SERVER_HOME="$(cd "${SERVER_DIR}" && pwd)"

# 3. Install plugins into console
echo "Installing plugins into console..."
PLUGINS="hadoop-gremlin spark-gremlin neo4j-gremlin sparql-gremlin"
for plugin in ${PLUGINS}; do
  PLUGIN_DIR="${plugin}/target/${plugin}-${TP_VERSION}-standalone"
  if [ -d "${PLUGIN_DIR}" ]; then
    echo " * installing ${plugin} (standalone)"
    cp -r "${PLUGIN_DIR}" "${CONSOLE_HOME}/ext/${plugin}"
  elif [ -f "${plugin}/target/${plugin}-${TP_VERSION}.jar" ]; then
    echo " * installing ${plugin} (jar + dependencies)"
    mkdir -p "${CONSOLE_HOME}/ext/${plugin}/lib"
    cp "${plugin}/target/${plugin}-${TP_VERSION}.jar" "${CONSOLE_HOME}/ext/${plugin}/lib/"
    cp "${plugin}"/target/dependency/*.jar "${CONSOLE_HOME}/ext/${plugin}/lib/" 2>/dev/null || \
      mvn dependency:copy-dependencies -pl "${plugin}" -DoutputDirectory="${CONSOLE_HOME}/ext/${plugin}/lib" -q
  else
    echo " * WARNING: ${plugin} not found"
  fi
done

# 4. Copy hadoop config to console classpath
HADOOP_CONF_SRC="tools/tinkerpop-docs/src/main/resources/hadoop-conf"
cp "${HADOOP_CONF_SRC}/core-site.xml" "${CONSOLE_HOME}/conf/"
cp "${HADOOP_CONF_SRC}/hadoop-docs.properties" "${CONSOLE_HOME}/conf/"

# 5. Start Gremlin Server
echo "Starting Gremlin Server..."
mkdir -p target
pushd "${SERVER_HOME}" > /dev/null
bin/gremlin-server.sh conf/gremlin-server-modern.yaml > "${TP_HOME}/target/gremlin-server-docs.log" 2>&1 &
GREMLIN_SERVER_PID=$!
popd > /dev/null

# Wait for server to be ready (port 8182)
echo -n "Waiting for Gremlin Server on port 8182..."
for i in $(seq 1 30); do
  if nc -z localhost 8182 2>/dev/null; then
    echo " ready."
    break
  fi
  if [ $i -eq 30 ]; then
    echo " TIMEOUT"
    echo "ERROR: Gremlin Server failed to start within 30 seconds."
    exit 1
  fi
  sleep 1
  echo -n "."
done

# 6. Start Gephi mock (port 8080)
if ! nc -z localhost 8080 2>/dev/null; then
  echo "Starting Gephi mock on port 8080..."
  bin/gephi-mock.py > /dev/null 2>&1 &
  GEPHI_MOCK_PID=$!
fi

# 7. Resolve HADOOP_GREMLIN_LIBS path
HADOOP_GREMLIN_LIBS="${CONSOLE_HOME}/ext/hadoop-gremlin/lib"

# 8. Copy source docs to staging area (replaces old preprocessor copy)
echo "Copying docs sources to target/postprocess-asciidoc/..."
mkdir -p target/postprocess-asciidoc
cp -r docs/{static,stylesheets} target/postprocess-asciidoc/
cp -r docs/src/* target/postprocess-asciidoc/

# 9. Invoke Maven with AsciidoctorJ extension attributes
echo "Running documentation build..."
if [ -z "${NOCLEAN}" ]; then
  rm -r target/doc-source target/docs 2>/dev/null || true
fi
set +e
mvn process-resources -Dasciidoc \
  -Dgremlin.docs.console.home="${CONSOLE_HOME}" \
  -Dgremlin.docs.hadoop.libs="${HADOOP_GREMLIN_LIBS}"
ec=$?
set -e

if [ ${ec} -eq 0 ]; then
  echo "Documentation build complete. Output: target/docs/htmlsingle/"
else
  echo "ERROR: Documentation build failed."
fi

exit ${ec}
