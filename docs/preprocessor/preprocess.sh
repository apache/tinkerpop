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

mkdir -p target/postprocess-asciidoc
rm -rf target/postprocess-asciidoc/*
mkdir target/postprocess-asciidoc/tmp
cp -R docs/{static,stylesheets} target/postprocess-asciidoc/

TP_HOME=`pwd`
CONSOLE_HOME=`readlink -f ${TP_HOME}/gremlin-console/target/apache-gremlin-console-*-standalone`
PLUGIN_DIR="${CONSOLE_HOME}/ext"
TP_VERSION=$(cat pom.xml | grep -A1 '<artifactId>tinkerpop</artifactId>' | grep -Po '(?<=<version>).*(?=</version>)')

# install Hadoop plugin
hadoopPlugin=$(find . -name "HadoopGremlinPlugin.java")
hadoopPluginName=`echo ${hadoopPlugin} | cut -d '/' -f2`
hadoopPluginClass=`echo ${hadoopPlugin} | grep -Po '(?<=src/main/java/).*(?=\.java)' | tr '/' '.'`
hadoopPluginDirectory="${PLUGIN_DIR}/${hadoopPluginName}"
match=`grep -c "$hadoopPluginClass" ${PLUGIN_DIR}/plugins.txt`

trap cleanup INT

function cleanup() {
  # remove Hadoop plugin if it wasn't installed prior pre-processing
  if [ "${rmHadoopPlugin}" == "1" ]; then
    rm -rf "${PLUGIN_DIR}/${hadoopPluginName}"
    sed -e "/${hadoopPluginClass}/d" "${PLUGIN_DIR}/plugins.txt" > "${PLUGIN_DIR}/plugins.txt."
    mv "${PLUGIN_DIR}/plugins.txt." "${PLUGIN_DIR}/plugins.txt"
  fi
  find "${TP_HOME}/docs/src/" -name "*.asciidoc.groovy" | xargs rm -f
}

if [ ! -z "${hadoopPluginName}" -a ! -d "${hadoopPluginDirectory}" -a ${match} -eq 0 ]; then
  rmHadoopPlugin=1
  echo -e "\n${hadoopPluginClass}" >> "${PLUGIN_DIR}/plugins.txt"
  mkdir -p "${PLUGIN_DIR}/${hadoopPluginName}/"{lib,plugin}
  #cp ${hadoopPluginName}/target/*${TP_VERSION}.jar "${PLUGIN_DIR}/${hadoopPluginName}/lib"
  cp ${hadoopPluginName}/target/*${TP_VERSION}.jar "${PLUGIN_DIR}/${hadoopPluginName}/plugin"
  libdir=`readlink -f ${hadoopPluginName}/target/*-standalone/lib/`
  if [ -d "${libdir}" ]; then
    #cp ${libdir}/*.jar "${PLUGIN_DIR}/${hadoopPluginName}/lib"
    cp ${libdir}/*.jar "${PLUGIN_DIR}/${hadoopPluginName}/plugin"
  fi
  cp */target/*${TP_VERSION}.jar "${PLUGIN_DIR}/${hadoopPluginName}/lib"
  for libdir in $(find . -name lib | grep -v ext); do
    cp ${libdir}/*.jar "${PLUGIN_DIR}/${hadoopPluginName}/lib"
  done
  #rm -f "${PLUGIN_DIR}/hadoop-gremlin"/*/slf4j-*.jar
  rm -f "${PLUGIN_DIR}/hadoop-gremlin"/plugin/slf4j-*.jar
  echo "System.exit(0)" > "${PLUGIN_DIR}/${hadoopPluginName}/init.groovy"
  ${CONSOLE_HOME}/bin/gremlin.sh "${PLUGIN_DIR}/${hadoopPluginName}/init.groovy" > /dev/null 2> /dev/null
fi

# copy Gremlin-Hadoop configuration files
cp ${TP_HOME}/hadoop-gremlin/conf/* "${CONSOLE_HOME}/conf/"

# process *.asciidoc files
#find "${TP_HOME}/docs/src/" -name "*.asciidoc" | xargs -n1 -P0 "${TP_HOME}/docs/preprocessor/preprocess-file.sh"
find "${TP_HOME}/docs/src/" -name "implementations.asciidoc" | xargs -n1 -P0 "${TP_HOME}/docs/preprocessor/preprocess-file.sh"

cleanup

popd > /dev/null
