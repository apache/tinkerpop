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

input=$2
output=`sed 's@/docs/src/@/target/postprocess-asciidoc/@' <<< "${input}"`

mkdir -p `dirname ${output}`

if hash stdbuf 2> /dev/null; then
  lb="stdbuf -oL"
else
  lb=""
fi

trap cleanup INT

function cleanup {
  rm -rf ${output}
  exit 255
}

function processed {
  echo -ne "\r   progress: [====================================================================================================] 100%\n"
}

echo
echo " * source:   ${input}"
echo "   target:   ${output}"
echo -ne "   progress: initializing"

if [ $(grep -c '^\[gremlin' ${input}) -gt 0 ]; then
  if [ ${output} -nt ${input} ]; then
    processed
    exit 0
  fi
  pushd "${CONSOLE_HOME}" > /dev/null

  awk -f ${AWK_SCRIPTS}/prepare.awk ${input} |
  awk -f ${AWK_SCRIPTS}/init-code-blocks.awk |
  awk -f ${AWK_SCRIPTS}/progressbar.awk -v tpl=${AWK_SCRIPTS}/progressbar.groovy.template | HADOOP_GREMLIN_LIBS="${CONSOLE_HOME}/ext/giraph-gremlin/lib:${CONSOLE_HOME}/ext/tinkergraph-gremlin/lib" bin/gremlin.sh |
  ${lb} awk -f ${AWK_SCRIPTS}/ignore.awk   |
  ${lb} awk -f ${AWK_SCRIPTS}/prettify.awk |
  ${lb} awk -f ${AWK_SCRIPTS}/cleanup.awk  > ${output}

  ps=(${PIPESTATUS[@]})
  for i in {0..6}; do
    ec=${ps[i]}
    [ ${ec} -eq 0 ] || break
  done

  if [ ${ec} -eq 0 ]; then
    ec=`grep -c '\bpb([0-9][0-9]*);' ${output}`
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
