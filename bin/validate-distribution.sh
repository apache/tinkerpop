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

  CONSOLE_URL="https://www.apache.org/dist/incubator/tinkerpop/${VERSION}/apache-gremlin-console-${VERSION}-bin.zip"
  SERVER_URL="https://www.apache.org/dist/incubator/tinkerpop/${VERSION}/apache-gremlin-server-${VERSION}-bin.zip"

  echo -e "\nValidating binary distribution\n"

  ${0} ${VERSION} ${CONSOLE_URL} "CONSOLE" && ${0} ${VERSION} ${SERVER_URL} "SERVER"

  EXIT_CODE=$?

  [[ ${EXIT_CODE} -eq 0 ]] && rm -rf ${TMP_DIR}

  echo && exit ${EXIT_CODE}
fi

mkdir -p ${TMP_DIR}
rm -rf ${TMP_DIR}/*
cd ${TMP_DIR}

# validate downloads
ZIP_FILENAME=`grep -o '[^/]*$' <<< ${URL}`
DIR_NAME=`sed 's/-[^-]*$//' <<< ${ZIP_FILENAME}`
COMPONENT=`tr '-' $'\n' <<< ${ZIP_FILENAME} | head -n3 | while read word ; do echo "${word^}" ; done | paste -sd ' '`

echo -n "* downloading ${COMPONENT} ... "
wget -q ${URL} ${URL}.asc ${URL}.md5 ${URL}.sha1 || (echo "Failed to download ${COMPONENT}" ; exit 1)
echo "OK"

# validate zip file
echo -n "* validating checksums ... "
[ `gpg ${ZIP_FILENAME}.asc 2>&1 | grep -c '^gpg: Good signature from "Stephen Mallette <spmallette@apache.org>"$'` -eq 1 ] || \
[ `gpg ${ZIP_FILENAME}.asc 2>&1 | grep -c '^gpg: Good signature from "Marko Rodriguez <okram@apache.org>"$'` -eq 1 ] || \
{ echo "${COMPONENT}'s PGP checksum verification failed"; exit 1; }
EXPECTED=`cat ${ZIP_FILENAME}.md5`
ACTUAL=`md5sum ${ZIP_FILENAME} | awk '{print $1}'`
[ "$ACTUAL" = "${EXPECTED}" ] || { echo "${COMPONENT}'s MD5 checksum verification failed"; exit 1; }
EXPECTED=`cat ${ZIP_FILENAME}.sha1`
ACTUAL=`sha1sum ${ZIP_FILENAME} | awk '{print $1}'`
[ "$ACTUAL" = "${EXPECTED}" ] || { echo "${COMPONENT}'s SHA1 checksum verification failed"; exit 1; }
echo "OK"

echo -n "* unzipping ${COMPONENT} ... "
unzip -q ${ZIP_FILENAME} || { echo "Failed to unzip ${COMPONENT}"; exit 1; }
[ -d ${DIR_NAME} ] || { echo "${COMPONENT} was not extracted into the expected directory"; exit 1; }
echo "OK"

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

echo -n "* validating ${COMPONENT}'s legal files ... "
for file in "LICENSE" "NOTICE" "DISCLAIMER"
do
  [ -f ${file} ] || { echo "${file} is not present"; exit 1; }
  [ -s ${file} ] || { echo "${file} is empty"; exit 1; }
done
echo "OK"

echo -n "* validating ${COMPONENT}'s plugin directory ... "
[ -d "ext" ] || { echo "ext/ directory is not present"; exit 1; }
if [ "${TYPE}" = "CONSOLE" ] || [[ `tr -d '.' <<< ${VERSION} | sed 's/-.*//'` -gt 301 ]]; then
  [ -d "ext/gremlin-groovy" ] && [ -d "ext/tinkergraph-gremlin" ] && [ -s "ext/plugins.txt" ] || { echo "ext/ directory is not present or incomplete"; exit 1; }
fi
echo "OK"

echo -n "* validating ${COMPONENT}'s lib directory ... "
[ -d "lib" ] && [[ `du lib | cut -f1 | wc -c` -ge 6 ]] || { echo "lib/ directory is not present or incomplete"; exit 1; }
echo "OK"

if [ "${TYPE}" = "CONSOLE" ]; then
  echo -n "* testing script evaluation ... "
  [[ `bin/gremlin.sh <<< 'TinkerFactory.createModern().traversal().V().count()' | grep '^==>' | sed 's/^==>//'` -eq 6 ]] || { echo "failed to evaluate sample script"; exit 1; }
  echo "OK"
fi
