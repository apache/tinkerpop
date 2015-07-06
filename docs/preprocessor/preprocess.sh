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

for daemon in "NameNode" "DataNode" "JobTracker" "TaskTracker"
do
  running=`jps | cut -d ' ' -f2 | grep -c ${daemon}`
  if [ ${running} -eq 0 ]; then
    echo "Hadoop is not running, be sure to start it before processing the docs."
    exit 1
  fi
done

nc -z localhost 2181 || (
  echo "ZooKeeper is not running, be sure to start it before processing the docs."
  exit 1
)

if [ -e /tmp/neo4j ]; then
  echo "The directory '/tmp/neo4j' is required by the pre-processor, be sure to delete it before processing the docs."
  exit 1
fi

function directory {
  d1=`pwd`
  cd $1
  d2=`pwd`
  cd $d1
  echo "$d2"
}

mkdir -p target/postprocess-asciidoc/tmp
cp -R docs/{static,stylesheets} target/postprocess-asciidoc/

TP_HOME=`pwd`
CONSOLE_HOME=`directory "${TP_HOME}/gremlin-console/target/apache-gremlin-console-*-standalone"`
PLUGIN_DIR="${CONSOLE_HOME}/ext"
TP_VERSION=$(cat pom.xml | grep -A1 '<artifactId>tinkerpop</artifactId>' | grep -o 'version>[^<]*' | grep -o '>.*' | cut -d '>' -f2 | head -n1)
TMP_DIR="/tmp/tp-docs-preprocessor"

HISTORY_FILE=`find . -name Console.groovy | xargs grep HISTORY_FILE | head -n1 | cut -d '=' -f2 | xargs echo`
[ ${HISTORY_FILE} ] || HISTORY_FILE=".gremlin_groovy_history"
[ -f ~/${HISTORY_FILE} ] && cp ~/${HISTORY_FILE} /tmp/

GREMLIN_SERVER=$(netstat -a | grep -o ':8182[0-9]*' | grep -cx ':8182')

if [ ${GREMLIN_SERVER} -eq 0 ]; then
  pushd gremlin-server/target/apache-gremlin-server-*-standalone > /dev/null
  bin/gremlin-server.sh conf/gremlin-server-modern.yaml > /dev/null 2> /dev/null &
  GREMLIN_SERVER_PID=$!
  popd > /dev/null
fi

trap cleanup INT

function cleanup() {
  echo -ne "\r\n\n"
  docs/preprocessor/uninstall-plugins.sh "${CONSOLE_HOME}" "${TMP_DIR}"
  find "${TP_HOME}/docs/src/" -name "*.asciidoc.groovy" | xargs rm -f
  [ -f /tmp/${HISTORY_FILE} ] &&  mv /tmp/${HISTORY_FILE} ~/
  rm -rf ${TMP_DIR}
  [ ${GREMLIN_SERVER} -eq 0 ] && kill ${GREMLIN_SERVER_PID}
  popd > /dev/null
}

mkdir -p ${TMP_DIR}

# install plugins
echo
echo "=========================="
echo "+   Installing Plugins   +"
echo "=========================="
echo
docs/preprocessor/install-plugins.sh "${CONSOLE_HOME}" "${TP_VERSION}" "${TMP_DIR}"

if [ $? -ne 0 ]; then
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
find "${TP_HOME}/docs/src/" -name "*.asciidoc" |
     xargs -n1 basename |
     xargs -n1 -I {} echo "echo -ne {}' '; (grep -n {} ${TP_HOME}/docs/src/index.asciidoc || echo 0) | cut -d ':' -f1" | /bin/bash | sort -nk2 | cut -d ' ' -f1 |
     xargs -n1 -I {} echo "${TP_HOME}/docs/src/{}" |
     xargs -n1 ${TP_HOME}/docs/preprocessor/preprocess-file.sh "${CONSOLE_HOME}"

ps=(${PIPESTATUS[@]})
for i in {0..7}; do
  ec=${ps[i]}
  [ ${ec} -eq 0 ] || break
done

if [ ${ec} -ne 0 ]; then
  cleanup
  exit 1
else
  echo
fi

cleanup
