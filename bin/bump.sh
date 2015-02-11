#!/bin/bash
#
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
  cat "$pom" | grep -n -A2 -B2 '<groupId>com.apache.tinkerpop</groupId>' \
             | grep -A2 -B2 '<artifactId>tinkerpop</artifactId>'  \
             | grep '<version>' | cut -f1 -d '-' | xargs -n1 -I{} sed -i.bak "{}s@>.*<@>${VERSION}<@" "$pom" && rm -f "${pom}.bak"
done

# README
INPUT="README.asciidoc"
sed -i.bak 's/\(http:\/\/tinkerpop.com\/.*docs\/\)[A-Za-z0-9.-]*\/\(.*\)/\1'"${VERSION}"'\/\2/' "${INPUT}" && rm -f "${INPUT}.bak"

# switch back to initial directory
popd > /dev/null