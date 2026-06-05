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
  mvn process-resources -pl . -Dasciidoc -Dgremlin.docs.dryrun=true
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

# Clear any plugin directories parked aside by a previous (possibly interrupted) run. The
# extension moves ext/<plugin> to ext-disabled/<plugin> when excluding a plugin per-book; a
# leftover ext-disabled/ from a crashed build would otherwise shadow the freshly-installed
# plugins and break the next run's console restarts.
rm -rf "${CONSOLE_HOME}/ext-disabled"

# 3. Install plugins into console
echo "Installing plugins into console..."

# Copy a plugin's dependency jars onto the console classpath via ext/<plugin>/plugin/ (which
# bin/gremlin.sh globs) rather than the shared lib/. This keeps each plugin's transitive deps
# isolatable so the docs extension can exclude conflicting plugins per-book (e.g. Neo4j's
# Scala 2.11 vs Spark's 2.12) by moving the plugin directory off the classpath. Jars already
# present in lib/ (core gremlin deps) and slf4j/logback-classic are skipped to avoid duplicate
# classpath entries and logger bindings -- mirroring the console's own :install (DependencyGrabber).
copy_deps_to_plugin() {
  local src_dir="$1" plugin="$2"
  local plugin_dir="${CONSOLE_HOME}/ext/${plugin}/plugin"
  mkdir -p "${plugin_dir}"
  local jar base
  for jar in "${src_dir}"/*.jar; do
    [ -e "${jar}" ] || continue
    base=$(basename "${jar}")
    case "${base}" in slf4j-*|logback-classic-*) continue ;; esac
    [ -e "${CONSOLE_HOME}/lib/${base}" ] && continue
    cp "${jar}" "${plugin_dir}/" 2>/dev/null
  done
}

PLUGINS="hadoop-gremlin spark-gremlin neo4j-gremlin sparql-gremlin"
for plugin in ${PLUGINS}; do
  PLUGIN_DIR="${plugin}/target/${plugin}-${TP_VERSION}-standalone"
  if [ -d "${PLUGIN_DIR}" ]; then
    echo " * installing ${plugin} (standalone)"
    cp -r "${PLUGIN_DIR}" "${CONSOLE_HOME}/ext/${plugin}"
    mkdir -p "${CONSOLE_HOME}/ext/${plugin}/plugin"
    cp "${plugin}/target/${plugin}-${TP_VERSION}.jar" "${CONSOLE_HOME}/ext/${plugin}/plugin/" 2>/dev/null
    copy_deps_to_plugin "${CONSOLE_HOME}/ext/${plugin}/lib" "${plugin}"
  elif [ -f "${plugin}/target/${plugin}-${TP_VERSION}.jar" ]; then
    echo " * installing ${plugin} (jar + dependencies)"
    mkdir -p "${CONSOLE_HOME}/ext/${plugin}/lib"
    mkdir -p "${CONSOLE_HOME}/ext/${plugin}/plugin"
    cp "${plugin}/target/${plugin}-${TP_VERSION}.jar" "${CONSOLE_HOME}/ext/${plugin}/lib/"
    cp "${plugin}/target/${plugin}-${TP_VERSION}.jar" "${CONSOLE_HOME}/ext/${plugin}/plugin/"
    cp "${plugin}"/target/dependency/*.jar "${CONSOLE_HOME}/ext/${plugin}/lib/" 2>/dev/null || \
      mvn dependency:copy-dependencies -pl "${plugin}" -DoutputDirectory="${CONSOLE_HOME}/ext/${plugin}/lib" -q
    copy_deps_to_plugin "${CONSOLE_HOME}/ext/${plugin}/lib" "${plugin}"
  else
    echo " * WARNING: ${plugin} not found"
  fi
done

# 3b. Resolve the Neo4j database implementation onto the console classpath.
# neo4j-gremlin declares neo4j-tinkerpop-api-impl as a Gremlin-Plugin-Dependencies manifest
# entry (test-scoped / gated behind the includeNeo4j profile), so it is not bundled by the
# local jar copy above. The old console ':install' flow fetched it via DependencyGrabber;
# here we resolve it (and its transitive deps) from Maven so Neo4jGraph blocks can execute.
NEO4J_IMPL_VERSION="0.9-3.4.0"
NEO4J_PLUGIN_LIB="${CONSOLE_HOME}/ext/neo4j-gremlin/lib"
if [ -d "${NEO4J_PLUGIN_LIB}" ] && ! ls "${NEO4J_PLUGIN_LIB}"/neo4j-tinkerpop-api-impl-*.jar >/dev/null 2>&1; then
  echo " * resolving Neo4j implementation (neo4j-tinkerpop-api-impl:${NEO4J_IMPL_VERSION})"
  NEO4J_POM="${TP_HOME}/target/neo4j-impl-pom.xml"
  cat > "${NEO4J_POM}" <<POM
<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>
<groupId>org.apache.tinkerpop.docs</groupId><artifactId>neo4j-impl-resolver</artifactId><version>1</version>
<dependencies><dependency><groupId>org.neo4j</groupId><artifactId>neo4j-tinkerpop-api-impl</artifactId><version>${NEO4J_IMPL_VERSION}</version></dependency></dependencies></project>
POM
  mvn -q -f "${NEO4J_POM}" dependency:copy-dependencies -DoutputDirectory="${NEO4J_PLUGIN_LIB}"
  # Drop ONLY the conflicting io.netty 4.x jar that Neo4j pulls in (netty-all-4.1.24): it
  # contains an older io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker13 that
  # shadows the console driver's 4.1.125 class and breaks ':remote' server connections with a
  # NoSuchMethodError. Keep netty-3.9.x (org.jboss.netty package) -- it does NOT conflict and
  # is required by Neo4j 3.4's IO layer.
  rm -f "${NEO4J_PLUGIN_LIB}"/netty-all-4.*.jar
  copy_deps_to_plugin "${NEO4J_PLUGIN_LIB}" "neo4j-gremlin"
fi

# 4. Register plugins in console
echo "Registering plugins..."
# Write plugins.txt deterministically rather than appending to whatever state a prior run left:
# the console rewrites this file to the set of successfully-activated plugins on shutdown, so a
# previous (possibly failed) run can leave it missing TinkerGraph/Credentials, which would fail
# the first doc block with "No such property: TinkerFactory". Lightweight built-in plugins are
# listed before the heavy graph plugins so activation order is stable.
cat > "${CONSOLE_HOME}/ext/plugins.txt" <<'EOF'
org.apache.tinkerpop.gremlin.console.jsr223.DriverGremlinPlugin
org.apache.tinkerpop.gremlin.console.jsr223.UtilitiesGremlinPlugin
org.apache.tinkerpop.gremlin.tinkergraph.jsr223.TinkerGraphGremlinPlugin
org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphGremlinPlugin
org.apache.tinkerpop.gremlin.hadoop.jsr223.HadoopGremlinPlugin
org.apache.tinkerpop.gremlin.spark.jsr223.SparkGremlinPlugin
org.apache.tinkerpop.gremlin.neo4j.jsr223.Neo4jGremlinPlugin
org.apache.tinkerpop.gremlin.sparql.jsr223.SparqlGremlinPlugin
EOF

# 5. Copy hadoop config to console classpath
HADOOP_CONF_SRC="tools/tinkerpop-docs/src/main/resources/hadoop-conf"
cp "${HADOOP_CONF_SRC}/core-site.xml" "${CONSOLE_HOME}/conf/"
cp "${HADOOP_CONF_SRC}/hadoop-docs.properties" "${CONSOLE_HOME}/conf/"

# 5. Start Gremlin Server
echo "Starting Gremlin Server..."
mkdir -p target

# Fail fast if port 8182 is already in use. The docs ':remote connect' blocks
# target localhost:8182, so any other service on that port (a stale Gremlin
# Server, or an unrelated process that happens to claim 8182) will cause our
# server to fail binding while the readiness check still passes -- the console
# then connects to the wrong service and WebSocket handshakes fail, dumping
# large stacktraces into the rendered docs.
if nc -z localhost 8182 2>/dev/null; then
  echo "ERROR: Port 8182 is already in use by another process."
  echo "       Gremlin Server needs this port for the docs ':remote' examples."
  echo "       Identify the process with 'lsof -nP -iTCP:8182' and stop it,"
  echo "       then re-run the docs build."
  exit 1
fi

pushd "${SERVER_HOME}" > /dev/null
bin/gremlin-server.sh conf/gremlin-server-modern.yaml > "${TP_HOME}/target/gremlin-server-docs.log" 2>&1 &
GREMLIN_SERVER_PID=$!
popd > /dev/null

# Wait for server to be ready (port 8182)
echo -n "Waiting for Gremlin Server on port 8182..."
for i in $(seq 1 30); do
  # Detect early server failure (e.g. bind error) so we don't wait the full timeout
  if ! kill -0 "${GREMLIN_SERVER_PID}" 2>/dev/null; then
    echo " FAILED"
    echo "ERROR: Gremlin Server process exited during startup. See target/gremlin-server-docs.log"
    exit 1
  fi
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
mvn process-resources -pl . -Dasciidoc \
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
