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

HADOOP_BASENAME="hadoop-${HADOOP_VERSION}"

[ -d "/usr/local/lib/${HADOOP_BASENAME}" ] && exit

APACHE_MIRROR=$(curl -s http://www.apache.org/dyn/closer.cgi | grep -o '<a href=".*"><strong>' | cut -f2 -d '"' | head -n1)
HADOOP_DOWNLOAD_URL="${APACHE_MIRROR}hadoop/common/${HADOOP_BASENAME}/${HADOOP_BASENAME}.tar.gz"
ALT_HADOOP_DOWNLOAD_URL="https://archive.apache.org/dist/hadoop/common/${HADOOP_BASENAME}/${HADOOP_BASENAME}.tar.gz"

pushd /usr/local/lib > /dev/null
(curl -f ${HADOOP_DOWNLOAD_URL} || curl ${ALT_HADOOP_DOWNLOAD_URL}) | tar xz
popd > /dev/null

cat >> ~/.bashrc <<EOF
export USER=`whoami`
export HADOOP_PREFIX=/usr/local/lib/${HADOOP_BASENAME}
export PATH="\${PATH}:\${HADOOP_PREFIX}/bin"
export CLASSPATH="\${CLASSPATH}:\${HADOOP_PREFIX}/etc/hadoop"
EOF
