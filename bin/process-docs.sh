#!/bin/bash
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

pushd "$(dirname $0)/.." > /dev/null

NOCLEAN=

DRYRUN=
DRYRUN_DOCS=
FULLRUN_DOCS=

while [[ $# -gt 0 ]]
do
  key="$1"
  case $key in
    -n|--noClean)
      NOCLEAN=1
      shift
      ;;
    -d|--dryRun)
      DRYRUN=1
      shift
      if [[ $# -gt 0 ]] && [[ $1 != -* ]]; then
        DRYRUN_DOCS="$1"
        shift
      else
        DRYRUN_DOCS="*"
      fi
      ;;
    -f|--fullRun)
      DRYRUN=1
      DRYRUN_DOCS=${DRYRUN_DOCS:-"*"}
      shift
      FULLRUN_DOCS="$1"
      shift
      ;;
    *)
      # unknown option
      shift
      ;;
  esac
done

if [ -z ${NOCLEAN} ]; then
  rm -rf ~/.groovy/grapes/org.apache.tinkerpop/
  if hash hadoop 2> /dev/null; then
    hadoop fs -rm -r "hadoop-gremlin-*-libs" > /dev/null 2>&1
  fi
fi

if [ ${DRYRUN} ] && [ "${DRYRUN_DOCS}" == "*" ] && [ -z "${FULLRUN_DOCS}" ]; then

  mkdir -p target/postprocess-asciidoc/tmp
  cp -R docs/{static,stylesheets} target/postprocess-asciidoc/
  cp -R docs/src/. target/postprocess-asciidoc/
  ec=$?

else

  GEPHI_MOCK=

  trap cleanup EXIT

  function cleanup() {
    [ ${GEPHI_MOCK} ] && kill ${GEPHI_MOCK}
  }

  nc -z localhost 8080 || (
    bin/gephi-mock.py > /dev/null 2>&1 &
    GEPHI_MOCK=$!
  )

  docs/preprocessor/preprocess.sh "${DRYRUN_DOCS}" "${FULLRUN_DOCS}"
  ec=$?
fi

if [ $ec == 0 ]; then
  mvn process-resources -Dasciidoc && docs/postprocessor/postprocess.sh
  ec=$?
fi

popd > /dev/null

exit ${ec}
