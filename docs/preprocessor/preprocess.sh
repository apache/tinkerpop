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
TP_VERSION=$(cat pom.xml | grep -A1 '<artifactId>tinkerpop</artifactId>' | grep -Po '(?<=<version>).*(?=</version>)')

# install Hadoop plugin
hadoopPlugin=$(find . -name "HadoopGremlinPlugin.java")
hadoopPluginName=`echo $hadoopPlugin | cut -d '/' -f2`
hadoopPluginClass=`echo $hadoopPlugin | grep -Po '(?<=src/main/java/).*(?=\.java)' | tr '/' '.'`
match=`grep -c "$hadoopPluginClass" ${CONSOLE_HOME}/ext/plugins.txt`
if [ ! -z "${hadoopPluginName}" -a ! -d "${CONSOLE_HOME}/ext/${hadoopPluginName}" -a $match -eq 0 ]; then
  rmHadoopPlugin=1
  echo -e "\n${hadoopPluginClass}" >> "${CONSOLE_HOME}/ext/plugins.txt"
  mkdir -p "${CONSOLE_HOME}/ext/${hadoopPluginName}/"{lib,plugin}
  cp ${hadoopPluginName}/target/*${TP_VERSION}.jar "${CONSOLE_HOME}/ext/${hadoopPluginName}/lib"
  cp ${hadoopPluginName}/target/*${TP_VERSION}.jar "${CONSOLE_HOME}/ext/${hadoopPluginName}/plugin"
  libdir=`readlink -f ${hadoopPluginName}/target/*-standalone/lib/`
  if [ -d "${libdir}" ]; then
    cp ${libdir}/*.jar "${CONSOLE_HOME}/ext/${hadoopPluginName}/lib"
    cp ${libdir}/*.jar "${CONSOLE_HOME}/ext/${hadoopPluginName}/plugin"
  fi
  rm -f "${CONSOLE_HOME}/ext/hadoop-gremlin"/*/slf4j-*.jar
fi

for input in $(find "${TP_HOME}/docs/src/" -name "*.asciidoc")
do
  name=`basename $input`
  output="${TP_HOME}/target/postprocess-asciidoc/${name}"
  echo "${input} > ${output}"
  if [ $(grep -c '^\[gremlin' $input) -gt 0 ]; then
    pushd "${CONSOLE_HOME}" > /dev/null
    bin/gremlin.sh -e ${TP_HOME}/docs/preprocessor/processor.groovy $input > $input.groovy
    ec=${PIPESTATUS[0]}
    if [ $ec -eq 0 ]; then
      HADOOP_GREMLIN_LIBS=`pwd`/ext/hadoop-gremlin/lib bin/gremlin.sh $input.groovy | awk 'BEGIN {b=1} /\1IGNORE/ {b=!b} !/\1IGNORE/ {if(a&&b)print} /\1START/ {a=1}' | grep -v '^WARN ' | sed 's/^==>\x01//' > $output
      ec=${PIPESTATUS[0]}
      rm -f $input.groovy
      popd > /dev/null
    fi
    rm -f $input.groovy
    if [ $ec -ne 0 ]; then
      popd > /dev/null
      exit $ec
    fi
  else
    cp $input $output
  fi
done

if [ "{$rmHadoopPlugin}" == "1" ]; then
  rm -rf "${CONSOLE_HOME}/ext/${hadoopPluginName}"
  sed -e "/${hadoopClassName}/d" "${CONSOLE_HOME}/ext/plugins.txt" > "${CONSOLE_HOME}/ext/plugins.txt."
  mv "${CONSOLE_HOME}/ext/plugins.txt." "${CONSOLE_HOME}/ext/plugins.txt"
fi

popd > /dev/null
