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

pushd "$(dirname $0)/../.." > /dev/null

if [ ! -f bin/gremlin.sh ]; then
  echo "Gremlin REPL is not available. Cannot preprocess AsciiDoc files."
  popd > /dev/null
  exit 1
fi

function directory {
  d1=`pwd`
  cd $1
  d2=`pwd`
  cd $d1
  echo "$d2"
}

mkdir -p target/postprocess-asciidoc
rm -rf target/postprocess-asciidoc/*
mkdir target/postprocess-asciidoc/tmp
cp -R docs/{static,stylesheets} target/postprocess-asciidoc/

TP_HOME=`pwd`
CONSOLE_HOME=`directory "${TP_HOME}/gremlin-console/target/apache-gremlin-console-*-standalone"`
PLUGIN_DIR="${CONSOLE_HOME}/ext"
TP_VERSION=$(cat pom.xml | grep -A1 '<artifactId>tinkerpop</artifactId>' | grep -o 'version>[^<]*' | grep -o '>.*' | cut -d '>' -f2 | head -n1)
TMP_DIR="/tmp/tp-docs-preprocessor"

trap cleanup INT

function cleanup() {
  echo -ne "\r\n\n"
  docs/preprocessor/uninstall-plugins.sh "${CONSOLE_HOME}" "${TMP_DIR}"
  find "${TP_HOME}/docs/src/" -name "*.asciidoc.groovy" | xargs rm -f
  rm -rf ${TMP_DIR}
}

mkdir -p ${TMP_DIR}

# install plugins
echo
echo "=========================="
echo "+   Installing Plugins   +"
echo "=========================="
echo
docs/preprocessor/install-plugins.sh "${CONSOLE_HOME}" "${TP_VERSION}" "${TMP_DIR}"

if [ ${PIPESTATUS[0]} -ne 0 ]; then
  cleanup
  exit 1
else
  echo
fi

# process *.asciidoc files
echo
echo "============================"
echo "+   Processing AsciiDocs   +"
echo "============================"
find "${TP_HOME}/docs/src/" -name "*.asciidoc" | xargs -n1 ${TP_HOME}/docs/preprocessor/preprocess-file.sh "${CONSOLE_HOME}"

if [ ${PIPESTATUS[1]} -ne 0 ]; then
  cleanup
  exit 1
else
  echo
fi

cleanup

popd > /dev/null
