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

CONSOLE_HOME=$1
TP_VERSION=$2
TMP_DIR=$3
INSTALL_TEMPLATE="docs/preprocessor/install-plugins.groovy"
INSTALL_FILE="${TMP_DIR}/install-plugins.groovy"

plugins=("hadoop-gremlin" "spark-gremlin" "neo4j-gremlin", "sparql-gremlin")
# plugins=()
pluginsCount=${#plugins[@]}

i=0

cp ${INSTALL_TEMPLATE} ${INSTALL_FILE}

while [ ${i} -lt ${pluginsCount} ]; do
  pluginName=${plugins[$i]}
  className=""
  for part in $(tr '-' '\n' <<< ${pluginName}); do
    className="${className}$(tr '[:lower:]' '[:upper:]' <<< ${part:0:1})${part:1}"
  done
  pluginClassFile=$(find . -name "${className}Plugin.java")
  pluginClass=`sed -e 's@.*src/main/java/@@' -e 's/\.java$//' <<< ${pluginClassFile} | tr '/' '.'`
  installed=`grep -c "${pluginClass}" ${CONSOLE_HOME}/ext/plugins.txt`
  if [ ${installed} -eq 0 ]; then
    echo "installPlugin(new Artifact(\"org.apache.tinkerpop\", \"${pluginName}\", \"${TP_VERSION}\"))" >> ${INSTALL_FILE}
    echo "${pluginName}" >> ${TMP_DIR}/plugins.dir
    echo "${pluginClass}" >> ${TMP_DIR}/plugins.txt
  else
    echo " * skipping ${pluginName} (already installed)"
  fi
  ((i++))
done

echo "installPlugin(new Artifact(\"org.apache.tinkerpop\", \"gremlin-python\", \"${TP_VERSION}\"))" >> ${INSTALL_FILE}
echo "gremlin-python" >> ${TMP_DIR}/plugins.dir

echo "System.exit(0)" >> ${INSTALL_FILE}
echo -ne " * tinkerpop-sugar ... "

pushd ${CONSOLE_HOME} > /dev/null

mkdir -p ~/.java/.userPrefs
chmod 700 ~/.java/.userPrefs

bin/gremlin.sh -e ${INSTALL_FILE} > /dev/null

if [ ${PIPESTATUS[0]} -ne 0 ]; then
  popd > /dev/null
  exit 1
fi

if [ -f "${TMP_DIR}/plugins.txt" ]; then
  cat ${TMP_DIR}/plugins.txt >> ${CONSOLE_HOME}/ext/plugins.txt
fi

popd > /dev/null
