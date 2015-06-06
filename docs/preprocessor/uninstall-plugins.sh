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
TMP_DIR=$2

if [ -f "${TMP_DIR}/plugins.dir" ]; then
  for pluginDirectory in $(cat ${TMP_DIR}/plugins.dir); do
    rm -rf ${CONSOLE_HOME}/ext/${pluginDirectory}
  done
fi

if [ -f "${TMP_DIR}/plugins.txt" ]; then
  for className in $(cat ${TMP_DIR}/plugins.txt); do
    sed -e "/${className}/d" ${CONSOLE_HOME}/ext/plugins.txt > ${CONSOLE_HOME}/ext/plugins.txt.
    mv ${CONSOLE_HOME}/ext/plugins.txt. ${CONSOLE_HOME}/ext/plugins.txt
  done
fi
