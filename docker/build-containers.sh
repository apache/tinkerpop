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

DIR=`dirname $0`
FORCE=
HADOOP_VERSION=

function cleanup {
  rm -f ${DIR}/hadoop/Dockerfile
}
trap cleanup EXIT

while [ "$1" ]; do
  case "$1" in
    -f | --force ) FORCE=true; shift ;;
    -h | --hadoop ) HADOOP_VERSION="$2"; shift 2 ;;
  esac
done

pushd ${DIR} > /dev/null

function findByTag {
  docker images tinkerpop | tail -n+2 | awk "{if (\$2==\"$1\") print \$3}"
}

if [ ! -z ${FORCE} ]; then
  for tag in "hadoop-${HADOOP_VERSION}" "base"; do
    if [ ! -z $(findByTag "${tag}") ]; then
      docker rmi tinkerpop:${tag}
    fi
  done
fi

if [ -z $(findByTag "base") ]; then
  docker build -t tinkerpop:base .
fi

if [ -z $(findByTag "hadoop-${HADOOP_VERSION}") ]; then
  sed -e "s/HADOOP_VERSION\$/${HADOOP_VERSION}/" hadoop/Dockerfile.template > hadoop/Dockerfile
  docker build -t tinkerpop:hadoop-${HADOOP_VERSION} hadoop
fi

popd > /dev/null
