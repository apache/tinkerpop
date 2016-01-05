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

TMP_DIR="/tmp/tpdv"

VERSION=${1}
URL=${2}
TYPE=${3}

if [ -z ${VERSION} ]; then
  echo -e "\nUsage:\n\t${0} {VERSION}\n"
  exit 1
fi

if [ -z ${URL} ]; then

  CONSOLE_URL="https://dist.apache.org/repos/dist/dev/incubator/tinkerpop/${VERSION}/apache-gremlin-console-${VERSION}-bin.zip"
  SERVER_URL="https://dist.apache.org/repos/dist/dev/incubator/tinkerpop/${VERSION}/apache-gremlin-server-${VERSION}-bin.zip"
  SOURCE_URL="https://dist.apache.org/repos/dist/dev/incubator/tinkerpop/${VERSION}/apache-tinkerpop-${VERSION}-src.zip"

  echo -e "\nValidating binary distributions\n"

  ${0} ${VERSION} ${CONSOLE_URL} "CONSOLE"
  EXIT_CODE=$?

  if [ ${EXIT_CODE} -eq 0 ]; then
    echo
    ${0} ${VERSION} ${SERVER_URL} "SERVER"
    EXIT_CODE=$?
  fi

  if [ ${EXIT_CODE} -eq 0 ]; then
    echo -e "\nValidating source distribution\n"
    ${0} ${VERSION} ${SOURCE_URL} "SOURCE"
    EXIT_CODE=$?
  fi

  [[ ${EXIT_CODE} -eq 0 ]] && rm -rf ${TMP_DIR}

  echo && exit ${EXIT_CODE}
fi

mkdir -p ${TMP_DIR}
rm -rf ${TMP_DIR}/*
cd ${TMP_DIR}

# validate downloads
ZIP_FILENAME=`grep -o '[^/]*$' <<< ${URL}`
DIR_NAME=`sed -e 's/-[^-]*$//' <<< ${ZIP_FILENAME}`
COMPONENT=`tr '-' $'\n' <<< ${ZIP_FILENAME} | head -n3 | awk '{for (i = 1; i <= NF; i++) sub(/./, toupper(substr($i, 1, 1)), $1); print}' | paste -sd ' ' -`

if [ "${TYPE}" = "SOURCE" ]; then
  DIR_NAME=`sed -e 's/^[^-]*-//' <<< ${DIR_NAME}`
fi

echo -n "* downloading ${COMPONENT} (${ZIP_FILENAME})... "
curl -Lsf ${URL} -o ${ZIP_FILENAME} || { echo "Failed to download ${COMPONENT}" ; exit 1; }
for ext in "asc" "md5" "sha1"
do
  curl -Lsf ${URL}.${ext} -o ${ZIP_FILENAME}.${ext} || { echo "Failed to download ${COMPONENT} (${ext})" ; exit 1 ; }
done
echo "OK"

# validate zip file
echo "* validating signatures and checksums ... "

echo -n "  * PGP signature ... "
[ `gpg ${ZIP_FILENAME}.asc 2>&1 | grep -c '^gpg: Good signature from "Stephen Mallette <spmallette@apache.org>"$'` -eq 1 ] || \
[ `gpg ${ZIP_FILENAME}.asc 2>&1 | grep -c '^gpg: Good signature from "Marko Rodriguez <okram@apache.org>"$'` -eq 1 ] || \
{ echo "failed"; exit 1; }
echo "OK"

echo -n "  * MD5 checksum ... "
EXPECTED=`cat ${ZIP_FILENAME}.md5`
ACTUAL=`md5sum ${ZIP_FILENAME} | awk '{print $1}'`
[ "$ACTUAL" = "${EXPECTED}" ] || { echo "failed"; exit 1; }
echo "OK"

echo -n "  * SHA1 chacksum ... "
EXPECTED=`cat ${ZIP_FILENAME}.sha1`
ACTUAL=`sha1sum ${ZIP_FILENAME} | awk '{print $1}'`
[ "$ACTUAL" = "${EXPECTED}" ] || { echo "failed"; exit 1; }
echo "OK"

echo -n "* unzipping ${COMPONENT} ... "
unzip -q ${ZIP_FILENAME} || { echo "Failed to unzip ${COMPONENT}"; exit 1; }
[ -d ${DIR_NAME} ] || { echo "${COMPONENT} was not extracted into the expected directory"; exit 1; }
echo "OK"

if [ "${TYPE}" = "SOURCE" ]; then
cd ${DIR_NAME}
echo -n "* building project ... "
mvn clean install -Dmaven.test.skip=true 2>&1 > /dev/null || { echo "failed"; exit 1; }
echo "OK"
exit 0
fi

# validate docs/ and javadocs/ directories
echo -n "* validating ${COMPONENT}'s docs ... "
cd ${DIR_NAME}
[ -d "docs" ] && [ -f "docs/index.html" ] && [ -d "docs/images" ] || { echo "docs/ directory is incomplete or not present"; exit 1; }
[ -d "javadocs/core" ] && [ -d "javadocs/full" ] || { echo "javadocs/ directory is incomplete or not present"; exit 1; }
x=`find javadocs -name 'GraphTraversal.html' | wc -l`
[[ ${x} -eq 4 ]] || { echo "${COMPONENT}'s javadocs/ directory is incomplete"; exit 1; }
echo "OK"

echo -n "* validating ${COMPONENT}'s binaries ... "
[ -d "bin" ] || { echo "bin/ directory is not present"; exit 1; }

GREMLIN_SHELL_SCRIPT=`find bin/ -name "gremlin*.sh"`
GREMLIN_BATCH_SCRIPT=`find bin/ -name "gremlin*.bat"`

[ ! -z ${GREMLIN_SHELL_SCRIPT} ] && [ -s ${GREMLIN_SHELL_SCRIPT} ] || { echo "Gremlin shell script is not present or empty"; exit 1; }
[ ! -z ${GREMLIN_BATCH_SCRIPT} ] && [ -s ${GREMLIN_BATCH_SCRIPT} ] || { echo "Gremlin batch script is not present or empty"; exit 1; }
echo "OK"

echo "* validating ${COMPONENT}'s legal files ... "
for file in "LICENSE" "NOTICE" "DISCLAIMER"
do
  echo -n "  * ${file} ... "
  [ -s ${file} ] || { echo "${file} is not present or empty"; exit 1; }
  echo "OK"
done

echo -n "* validating ${COMPONENT}'s plugin directory ... "
[ -d "ext" ] || { echo "ext/ directory is not present"; exit 1; }
if [ "${TYPE}" = "CONSOLE" ] || [[ `tr -d '.' <<< ${VERSION} | sed -e 's/-.*//'` -gt 301 ]]; then
  [ -d "ext/gremlin-groovy" ] && [ -d "ext/tinkergraph-gremlin" ] && ([ "${TYPE}" = "SERVER" ] || [ -s "ext/plugins.txt" ]) || { echo "ext/ directory is not present or incomplete"; exit 1; }
fi
echo "OK"

echo -n "* validating ${COMPONENT}'s lib directory ... "
[ -d "lib" ] && [[ `du lib | cut -f1 | wc -c` -ge 6 ]] || { echo "lib/ directory is not present or incomplete"; exit 1; }
echo "OK"

if [ "${TYPE}" = "CONSOLE" ]; then
  echo -n "* testing script evaluation ... "
  SCRIPT="x = org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory.createModern().traversal().V().count().next(); println x; x"
  SCRIPT_FILENAME="${TMP_DIR}/test.groovy"
  echo ${SCRIPT} > ${SCRIPT_FILENAME}
  [[ `bin/gremlin.sh <<< ${SCRIPT} | grep '^==>' | sed -e 's/^==>//'` -eq 6 ]] || { echo "failed to evaluate sample script"; exit 1; }
  [[ `bin/gremlin.sh -e ${SCRIPT_FILENAME}` -eq 6 ]] || { echo "failed to evaluate sample script using -e option"; exit 1; }
  echo "OK"
fi
