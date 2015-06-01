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

input=$2
name=`basename ${input}`
output="${TP_HOME}/target/postprocess-asciidoc/${name}"

echo "${input} > ${output}"

if [ $(grep -c '^\[gremlin' ${input}) -gt 0 ]; then
  pushd "${CONSOLE_HOME}" > /dev/null
  bin/gremlin.sh -e ${TP_HOME}/docs/preprocessor/processor.groovy ${input} > ${input}.groovy
  ec=${PIPESTATUS[0]}
  if [ ${ec} -eq 0 ]; then
    HADOOP_GREMLIN_LIBS="${CONSOLE_HOME}/ext/hadoop-gremlin/lib" bin/gremlin.sh ${input}.groovy | awk 'BEGIN {b=1} /¶IGNORE/ {b=!b} !/¶IGNORE/ {if(a&&b)print} /¶START/ {a=1}' | grep -v '^WARN ' | sed 's/^==>¶//' > ${output}
    ec=${PIPESTATUS[0]}
    rm -f ${input}.groovy
  fi
  rm -f ${input}.groovy
  popd > /dev/null
  exit ${ec}
else
  cp ${input} ${output}
fi
