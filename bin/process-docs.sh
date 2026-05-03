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
# This bypasses the old AWK preprocessing pipeline and processes [gremlin-*] blocks
# directly during Asciidoctor rendering.
#
# Usage:
#   bin/process-docs-new.sh              # full build with live gremlin execution
#   bin/process-docs-new.sh --dry-run    # skip gremlin execution (fast, for layout checks)

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${PROJECT_ROOT}"

TP_VERSION=$(cat pom.xml | grep -A1 '<artifactId>tinkerpop</artifactId>' | grep '<version>' | sed -e 's/.*<version>//' -e 's/<\/version>.*//')

if [ -z "${TP_VERSION}" ]; then
    echo "ERROR: Could not determine TinkerPop version from pom.xml"
    exit 1
fi

ASCIIDOC_ATTRS=""
if [ "$1" = "--dry-run" ]; then
    ASCIIDOC_ATTRS="-Dasciidoctor.attributes.gremlin-docs-dryrun=true"
    echo "Dry-run mode: gremlin blocks will not be executed"
fi

echo "Building docs for TinkerPop ${TP_VERSION}..."
echo "Source: docs/src/"
echo "Output: target/docs/htmlsingle/"

# build and install the gremlin-docs extension (not part of the main reactor)
echo "Installing gremlin-docs extension..."
mvn install -f gremlin-docs/pom.xml -DskipTests -Denforcer.skip=true -q

# copy static assets that live outside docs/src/ into the staging area
# (Maven's copy-docs-to-work-area handles docs/src/ itself)
mkdir -p target/doc-source
cp -r docs/static target/doc-source/ 2>/dev/null || true
cp -r docs/stylesheets target/doc-source/ 2>/dev/null || true

# set up conf/hadoop so GraphFactory.open('conf/hadoop/...') resolves during build
mkdir -p conf/hadoop
cp hadoop-gremlin/conf/* conf/hadoop/ 2>/dev/null || true

# run asciidoctor with the gremlin-docs extension, pointing at raw sources
mvn process-resources \
    -Dasciidoc \
    -Drat.skip=true \
    ${ASCIIDOC_ATTRS}

# clean up
rm -rf conf/hadoop
rmdir conf 2>/dev/null || true

# post-process: replace version placeholder
echo "Post-processing: replacing x.y.z with ${TP_VERSION}..."
find target/docs/htmlsingle -name '*.html' | while IFS= read -r f; do
    sed "s/x\.y\.z/${TP_VERSION}/g" "$f" > "$f.tmp" && mv "$f.tmp" "$f"
done

echo "Done. Output in target/docs/htmlsingle/"
