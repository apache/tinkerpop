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

# Builds TinkerPop documentation using the gremlin-docs AsciidoctorJ extension.
# The extension delegates Gremlin code execution to a real Gremlin Console process,
# then generates language variant tabs via the ANTLR-based GremlinTranslator.
#
# Usage:
#   bin/process-docs.sh              # full build with live gremlin execution
#   bin/process-docs.sh --dry-run    # skip gremlin execution (fast, for layout checks)

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${PROJECT_ROOT}"

TP_VERSION=$(cat pom.xml | grep -A1 '<artifactId>tinkerpop</artifactId>' | grep '<version>' | sed -e 's/.*<version>//' -e 's/<\/version>.*//')

if [ -z "${TP_VERSION}" ]; then
    echo "ERROR: Could not determine TinkerPop version from pom.xml"
    exit 1
fi

ASCIIDOC_ATTRS=""
DRYRUN=false
if [ "$1" = "--dry-run" ]; then
    DRYRUN=true
    ASCIIDOC_ATTRS="-Dasciidoctor.attributes.gremlin-docs-dryrun=true"
    echo "Dry-run mode: gremlin blocks will not be executed"
fi

echo "Building docs for TinkerPop ${TP_VERSION}..."
echo "Source: docs/src/"
echo "Output: target/docs/htmlsingle/"

# build and install the gremlin-docs extension (not part of the main reactor)
echo "Installing gremlin-docs extension..."
mvn install -f gremlin-docs/pom.xml -DskipTests -Denforcer.skip=true -q

GREMLIN_SERVER_PID=""
GEPHI_MOCK_PID=""

function cleanup() {
    if [ -n "${GREMLIN_SERVER_PID}" ]; then
        echo "Stopping Gremlin Server (PID ${GREMLIN_SERVER_PID})..."
        kill ${GREMLIN_SERVER_PID} 2>/dev/null || true
        wait ${GREMLIN_SERVER_PID} 2>/dev/null || true
    fi
    if [ -n "${GEPHI_MOCK_PID}" ]; then
        kill ${GEPHI_MOCK_PID} 2>/dev/null || true
        wait ${GEPHI_MOCK_PID} 2>/dev/null || true
    fi
    # clean up conf/hadoop from console home if we created it
    if [ -n "${CONSOLE_HOME}" ] && [ -d "${CONSOLE_HOME}/conf/hadoop" ]; then
        rm -rf "${CONSOLE_HOME}/conf/hadoop"
    fi
}

trap cleanup EXIT

if [ "${DRYRUN}" = "false" ]; then
    # locate the console distribution (must be built already via mvn install -pl :gremlin-console -am)
    CONSOLE_HOME=$(ls -d "${PROJECT_ROOT}"/gremlin-console/target/apache-tinkerpop-gremlin-console-*-standalone 2>/dev/null | head -1)

    if [ -z "${CONSOLE_HOME}" ] || [ ! -d "${CONSOLE_HOME}" ]; then
        echo "ERROR: Gremlin Console distribution not found."
        echo "Build it first: mvn clean install -pl :gremlin-console -am -DskipTests"
        exit 1
    fi

    echo "Using console: ${CONSOLE_HOME}"

    # install plugins needed for doc examples
    # NOTE: neo4j-gremlin is excluded by default because its Spark jars conflict with
    # spark-gremlin on the classpath. Neo4j examples will fall back to dry-run output.
    # The old AWK pipeline handled this by swapping plugins per-document.
    PLUGIN_DIR="${CONSOLE_HOME}/ext"
    plugins=("hadoop-gremlin" "spark-gremlin" "sparql-gremlin")
    for pluginName in "${plugins[@]}"; do
        if [ ! -d "${PLUGIN_DIR}/${pluginName}" ]; then
            echo "Installing plugin: ${pluginName}..."
            pushd "${CONSOLE_HOME}" > /dev/null
            bin/gremlin.sh -e <(echo ":install org.apache.tinkerpop ${pluginName} ${TP_VERSION}") 2>/dev/null || true
            popd > /dev/null
        else
            echo "Plugin already installed: ${pluginName}"
        fi
    done

    # activate plugins in plugins.txt if not already present
    for pluginName in "${plugins[@]}"; do
        # derive class name: hadoop-gremlin -> HadoopGremlinPlugin
        className=""
        for part in $(tr '-' '\n' <<< "${pluginName}"); do
            className="${className}$(tr '[:lower:]' '[:upper:]' <<< "${part:0:1}")${part:1}"
        done
        pluginClassFile=$(find . -name "${className}Plugin.java" 2>/dev/null | head -1)
        if [ -n "${pluginClassFile}" ]; then
            pluginClass=$(sed -e 's@.*src/main/java/@@' -e 's/\.java$//' <<< "${pluginClassFile}" | tr '/' '.')
            if ! grep -q "${pluginClass}" "${PLUGIN_DIR}/plugins.txt" 2>/dev/null; then
                echo "${pluginClass}" >> "${PLUGIN_DIR}/plugins.txt"
            fi
        fi
    done

    # start Gremlin Server for remote connection examples
    SERVER_HOME=$(ls -d "${PROJECT_ROOT}"/gremlin-server/target/apache-tinkerpop-gremlin-server-*-standalone 2>/dev/null | head -1)
    if [ -n "${SERVER_HOME}" ] && [ -d "${SERVER_HOME}" ]; then
        # check for port conflict before starting
        if nc -z localhost 8182 2>/dev/null; then
            echo "ERROR: Port 8182 is already in use. Stop the process using it before building docs."
            exit 1
        fi

        echo "Starting Gremlin Server..."
        mkdir -p target/docs-logs
        pushd "${SERVER_HOME}" > /dev/null
        bin/gremlin-server.sh conf/gremlin-server-modern.yaml > "${PROJECT_ROOT}/target/docs-logs/gremlin-server.log" 2>&1 &
        GREMLIN_SERVER_PID=$!
        popd > /dev/null

        # wait for server to be ready (up to 30 seconds)
        echo -n "Waiting for Gremlin Server on port 8182"
        for i in $(seq 1 30); do
            if nc -z localhost 8182 2>/dev/null; then
                echo " ready."
                break
            fi
            echo -n "."
            sleep 1
        done
        if ! nc -z localhost 8182 2>/dev/null; then
            echo " WARNING: Gremlin Server may not have started. Remote connection examples may fail."
        fi
    else
        echo "WARNING: Gremlin Server distribution not found. Remote connection examples will fail."
        echo "Build it first: mvn clean install -pl :gremlin-server -am -DskipTests"
    fi

    # set up conf/hadoop inside the console home so GraphFactory.open('conf/hadoop/...') resolves
    # (the console process runs with CONSOLE_HOME as its working directory)
    mkdir -p "${CONSOLE_HOME}/conf/hadoop"
    cp "${PROJECT_ROOT}"/hadoop-gremlin/conf/* "${CONSOLE_HOME}/conf/hadoop/" 2>/dev/null || true

    # start Gephi mock server for Gephi plugin examples (listens on port 8080)
    if ! nc -z localhost 8080 2>/dev/null; then
        "${PROJECT_ROOT}/bin/gephi-mock.py" > /dev/null 2>&1 &
        GEPHI_MOCK_PID=$!
    fi

    HADOOP_LIBS="${CONSOLE_HOME}/ext/tinkergraph-gremlin/lib"
    ASCIIDOC_ATTRS="${ASCIIDOC_ATTRS} -Dasciidoctor.attributes.gremlin-docs-console-home=${CONSOLE_HOME}"
    ASCIIDOC_ATTRS="${ASCIIDOC_ATTRS} -Dasciidoctor.attributes.gremlin-docs-hadoop-libs=${HADOOP_LIBS}"
fi

# copy static assets that live outside docs/src/ into the staging area
mkdir -p target/doc-source
cp -r docs/static target/doc-source/ 2>/dev/null || true
cp -r docs/stylesheets target/doc-source/ 2>/dev/null || true

# run asciidoctor with the gremlin-docs extension
mvn process-resources \
    -Dasciidoc \
    -Drat.skip=true \
    ${ASCIIDOC_ATTRS}

# post-process: replace version placeholder
echo "Post-processing: replacing x.y.z with ${TP_VERSION}..."
find target/docs/htmlsingle -name '*.html' | while IFS= read -r f; do
    sed "s/x\.y\.z/${TP_VERSION}/g" "$f" > "$f.tmp" && mv "$f.tmp" "$f"
done

echo "Done. Output in target/docs/htmlsingle/"
