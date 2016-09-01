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

DRYRUN_DOCS="$1"
FULLRUN_DOCS="$2"

pushd "$(dirname $0)/../.." > /dev/null

if [ "${DRYRUN_DOCS}" != "*" ]; then

  if [ ! -f bin/gremlin.sh ]; then
    echo "Gremlin REPL is not available. Cannot preprocess AsciiDoc files."
    popd > /dev/null
    exit 1
  fi

  for daemon in "NameNode" "DataNode" "ResourceManager" "NodeManager"
  do
    running=`jps | cut -d ' ' -f2 | grep -c ${daemon}`
    if [ ${running} -eq 0 ]; then
      echo "Hadoop is not running, be sure to start it before processing the docs."
      exit 1
    fi
  done

  netstat -an | awk '{print $4}' | grep -o '[0-9]*$' | grep 8182 > /dev/null && {
    echo "The port 8182 is required for Gremlin Server, but already in use. Be sure to close the application that currently uses the port before processing the docs."
    exit 1
  }

  if [ -e /tmp/neo4j ]; then
    echo "The directory '/tmp/neo4j' is required by the pre-processor, be sure to delete it before processing the docs."
    exit 1
  fi

  if [ -e /tmp/tinkergraph.kryo ]; then
    echo "The file '/tmp/tinkergraph.kryo' is required by the pre-processor, be sure to delete it before processing the docs."
    exit 1
  fi
fi

function directory {
  d1=`pwd`
  cd $1
  d2=`pwd`
  cd $d1
  echo "$d2"
}

mkdir -p target/postprocess-asciidoc/tmp
mkdir -p target/postprocess-asciidoc/logs
cp -R docs/{static,stylesheets} target/postprocess-asciidoc/

TP_HOME=`pwd`
CONSOLE_HOME=`directory "${TP_HOME}/gremlin-console/target/apache-tinkerpop-gremlin-console-*-standalone"`
PLUGIN_DIR="${CONSOLE_HOME}/ext"
TP_VERSION=$(cat pom.xml | grep -A1 '<artifactId>tinkerpop</artifactId>' | grep -o 'version>[^<]*' | grep -o '>.*' | cut -d '>' -f2 | head -n1)
TMP_DIR="/tmp/tp-docs-preprocessor"

mkdir -p "${TMP_DIR}"

HISTORY_FILE=".gremlin_groovy_history"
[ -f ~/${HISTORY_FILE} ] && cp ~/${HISTORY_FILE} ${TMP_DIR}

pushd gremlin-server/target/apache-tinkerpop-gremlin-server-*-standalone > /dev/null
bin/gremlin-server.sh conf/gremlin-server-modern.yaml > ${TP_HOME}/target/postprocess-asciidoc/logs/gremlin-server.log 2>&1 &
GREMLIN_SERVER_PID=$!
popd > /dev/null

function cleanup() {
  echo -ne "\r\n\n"
  docs/preprocessor/uninstall-plugins.sh "${CONSOLE_HOME}" "${TMP_DIR}"
  [ -f ${TMP_DIR}/plugins.txt.orig ] && mv ${TMP_DIR}/plugins.txt.orig ${CONSOLE_HOME}/ext/plugins.txt
  find ${TP_HOME}/docs/src/ -name "*.asciidoc.groovy" | xargs rm -f
  [ -f ${TMP_DIR}/${HISTORY_FILE} ] &&  mv ${TMP_DIR}/${HISTORY_FILE} ~/
  rm -rf ${TMP_DIR}
  kill ${GREMLIN_SERVER_PID} &> /dev/null
  popd &> /dev/null
}

trap cleanup EXIT

if [ "${DRYRUN_DOCS}" != "*" ] || [ ! -z "${FULLRUN_DOCS}" ]; then

  # install plugins
  echo
  echo "=========================="
  echo "+   Installing Plugins   +"
  echo "=========================="
  echo
  cp ${CONSOLE_HOME}/ext/plugins.txt ${TMP_DIR}/plugins.txt.orig
  docs/preprocessor/install-plugins.sh "${CONSOLE_HOME}" "${TP_VERSION}" "${TMP_DIR}"

  if [ $? -ne 0 ]; then
    exit 1
  else
    echo
  fi

fi

# process *.asciidoc files
COLS=${COLUMNS}
[[ ${COLUMNS} -lt 240 ]] && stty cols 240

tput rmam

echo
echo "============================"
echo "+   Processing AsciiDocs   +"
echo "============================"

ec=0
for subdir in $(find "${TP_HOME}/docs/src/" -name index.asciidoc | xargs -n1 dirname)
do
  find "${subdir}" -maxdepth 1 -name "*.asciidoc" |
       xargs -n1 basename |
       xargs -n1 -I {} echo "echo -ne {}' '; (grep -n {} ${subdir}/index.asciidoc || echo 0) | head -n1 | cut -d ':' -f1" | /bin/bash | sort -nk2 | cut -d ' ' -f1 |
       xargs -n1 -I {} echo "${subdir}/{}" |
       xargs -n1 ${TP_HOME}/docs/preprocessor/preprocess-file.sh "${CONSOLE_HOME}" "${DRYRUN_DOCS}" "${FULLRUN_DOCS}"

  ps=(${PIPESTATUS[@]})
  for i in {0..7}; do
    ec=${ps[i]}
    [ ${ec} -eq 0 ] || break
  done
  [ ${ec} -eq 0 ] || break
done

tput smam
[[ "${COLUMNS}" != "" ]] && stty cols ${COLS}

rm -rf /tmp/neo4j /tmp/tinkergraph.kryo

[ ${ec} -eq 0 ] || exit 1

echo
