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

TP_HOME=`pwd`
CONSOLE_HOME=$1
AWK_SCRIPTS="${TP_HOME}/docs/preprocessor/awk"

IFS=',' read -r -a DRYRUN_DOCS <<< "$2"
IFS=',' read -r -a FULLRUN_DOCS <<< "$3"

dryRun () {
  local doc
  yes=0
  no=1
  doDryRun=${no}
  if [ "${DRYRUN_DOCS}" == "*" ]; then
    doDryRun=${yes}
  else
    for doc in "${DRYRUN_DOCS[@]}"; do
      if [ "${doc}" == "$1" ]; then
        doDryRun=${yes}
        break
      fi
    done
  fi
  if [ ${doDryRun} ]; then
    for doc in "${FULLRUN_DOCS[@]}"; do
      if [ "${doc}" == "$1" ]; then
        doDryRun=${no}
        break
      fi
    done
  fi
  return ${doDryRun}
}

input=$4
output=`sed 's@/docs/src/@/target/postprocess-asciidoc/@' <<< "${input}"`

SKIP=
if dryRun ${input}; then
  SKIP=1
fi

mkdir -p `dirname ${output}`

if hash stdbuf 2> /dev/null; then
  lb="stdbuf -oL"
else
  lb=""
fi

trap cleanup INT

function cleanup {
  if [ -f "${output}" ]; then
    if [ `wc -l "${output}" | awk '{print $1}'` -gt 0 ]; then
      echo -e "\n\e[1mLast 10 lines of ${output}:\e[0m\n"
      tail -n10 ${output}
      echo
      echo "Opening ${output} for full inspection"
      sleep 5
      less ${output}
    fi
  fi
  rm -rf ${output} ${CONSOLE_HOME}/.ext
  exit 255
}

function processed {
  echo -ne "\r   progress: [====================================================================================================] 100%\n"
}

echo
echo " * source:   ${input}"
echo "   target:   ${output}"
echo -ne "   progress: initializing"

if [ ! ${SKIP} ] && [ $(grep -c '^\[gremlin' ${input}) -gt 0 ]; then
  if [ ${output} -nt ${input} ]; then
    processed
    exit 0
  fi
  pushd "${CONSOLE_HOME}" > /dev/null

  doc=`basename ${input} .asciidoc`

  case "${doc}" in
    "implementations-neo4j")
      # deactivate Spark plugin to prevent version conflicts between TinkerPop's Spark jars and Neo4j's Spark jars
      mkdir .ext
      mv ext/spark-gremlin .ext/
      cat ext/plugins.txt | tee .ext/plugins.all | grep -Fv 'SparkGremlinPlugin' > .ext/plugins.txt
      ;;
    "implementations-hadoop-start" | "implementations-hadoop-end" | "implementations-spark" | "implementations-giraph" | "olap-spark-yarn")
      # deactivate Neo4j plugin to prevent version conflicts between TinkerPop's Spark jars and Neo4j's Spark jars
      mkdir .ext
      mv ext/neo4j-gremlin .ext/
      cat ext/plugins.txt | tee .ext/plugins.all | grep -Fv 'Neo4jGremlinPlugin' > .ext/plugins.txt
      ;;
    "gremlin-variants")
      # deactivate plugin to prevent version conflicts
      mkdir .ext
      mv ext/neo4j-gremlin .ext/
      mv ext/spark-gremlin .ext/
      mv ext/giraph-gremlin .ext/
      mv ext/hadoop-gremlin .ext/
      cat ext/plugins.txt | tee .ext/plugins.all | grep -v 'Neo4jGremlinPlugin\|SparkGremlinPlugin\|GiraphGremlinPlugin\|HadoopGremlinPlugin' > .ext/plugins.txt
      ;;
  esac

  if [ -d ".ext" ]; then
    mv .ext/plugins.txt ext/
  fi

  sed 's/\t/    /g' ${input} |
  awk -f ${AWK_SCRIPTS}/tabify.awk |
  awk -f ${AWK_SCRIPTS}/prepare.awk |
  awk -f ${AWK_SCRIPTS}/init-code-blocks.awk -v TP_HOME="${TP_HOME}" -v PYTHONPATH="${TP_HOME}/gremlin-python/target/classes/Lib" |
  awk -f ${AWK_SCRIPTS}/progressbar.awk -v tpl=${AWK_SCRIPTS}/progressbar.groovy.template |
  HADOOP_GREMLIN_LIBS="${CONSOLE_HOME}/ext/giraph-gremlin/lib:${CONSOLE_HOME}/ext/tinkergraph-gremlin/lib" bin/gremlin.sh |
  ${lb} awk -f ${AWK_SCRIPTS}/ignore.awk   |
  ${lb} awk -f ${AWK_SCRIPTS}/prettify.awk |
  ${lb} awk -f ${AWK_SCRIPTS}/cleanup.awk  |
  ${lb} awk -f ${AWK_SCRIPTS}/language-variants.awk > ${output}

  # check exit code for each of the previously piped commands
  ps=(${PIPESTATUS[@]})
  for i in {0..9}; do
    ec=${ps[i]}
    [ ${ec} -eq 0 ] || break
  done

  if [ -d ".ext" ]; then
    mv .ext/plugins.all ext/plugins.txt
    mv .ext/* ext/
    rm -r .ext/
  fi

  if [ ${ec} -eq 0 ]; then
    tail -n1 ${output} | grep -F '// LAST LINE' > /dev/null
    ec=$?
  fi

  if [ ${ec} -eq 0 ]; then
    processed
  fi

  echo
  popd > /dev/null
  if [ ${ec} -ne 0 ]; then
    cleanup
  fi
else
  cp ${input} ${output}
  processed
fi
