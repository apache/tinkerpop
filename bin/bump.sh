#!/bin/bash

# This script bumps version in the various files that reference the current TinkerPop version files (e.g. pom.xml)
# in preparation for release. Usage:
#
# bin/bump.sh "version"

VERSION="$1"
SCRIPT_PATH="$0"
SCRIPT_DIR=`dirname "${SCRIPT_PATH}"`
PROJECT_DIR="${SCRIPT_DIR}/.."

# switch to project directory (allows us to call bump.sh from everywhere and still use relative paths within the script)
pushd "$PROJECT_DIR" > /dev/null

# update pom.xml
for pom in $(find . -name pom.xml); do
  cat "$pom" | grep -n -A2 -B2 '<groupId>com.tinkerpop</groupId>' \
             | grep -A2 -B2 '<artifactId>tinkerpop</artifactId>'  \
             | grep '<version>' | cut -f1 -d '-' | xargs -n1 -I{} sed -i.bak "{}s@>.*<@>${VERSION}<@" "$pom" && rm -f "${pom}.bak"
done

# YAML configuration
INPUT="gremlin-server/conf/gremlin-server-neo4j.yaml"
sed -i.bak 's/\[com.tinkerpop, neo4j-gremlin, ".*"\]/\[com.tinkerpop, neo4j-gremlin, "'"${VERSION}"'"\]/' "${INPUT}" && rm -f "${INPUT}.bak"

# README
INPUT="README.asciidoc"
sed -i.bak 's/\(http:\/\/tinkerpop.com\/.*docs\/\)[A-Za-z0-9.-]*\/\(.*\)/\1'"${VERSION}"'\/\2/' "${INPUT}" && rm -f "${INPUT}.bak"

# switch back to initial directory
popd > /dev/null