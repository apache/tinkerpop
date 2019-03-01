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

# Build docker images first
# Build gremlin-server:  mvn clean install -pl :gremlin-server -am

DIR=`dirname $0`
PROJECT_HOME=${DIR}/../

TIMESTAMP=$(date +%s)
BUILD_TAG="gremlin-server-test-${TIMESTAMP}"

function cleanup {
  BUILD_IMAGE=$(docker images tinkerpop | awk "{if (\$2 == \"${BUILD_TAG}\") print \$3}")
  [ ! -z ${BUILD_IMAGE} ] && docker rmi ${BUILD_IMAGE}
  rm -f ${PROJECT_HOME}/Dockerfile
}
trap cleanup EXIT

REMOVE_CONTAINER="--rm"
[[ -n ${KEEP_CONTAINER} ]] && unset REMOVE_CONTAINER

pushd ${PROJECT_HOME} > /dev/null

#mvn clean package -DskipTests=true -pl :gremlin-server || mvn clean install -pl :gremlin-server -am

HADOOP_VERSION=$(cat pom.xml | grep -o '<hadoop.version>[^<]*' | head -n1 | cut -d '>' -f2)

docker/build-containers.sh -h "${HADOOP_VERSION}"

sed -e "s/HADOOP_VERSION\$/${HADOOP_VERSION}/" docker/gremlin-server/Dockerfile.template > Dockerfile

docker build -t tinkerpop:${BUILD_TAG} .
docker run ${TINKERPOP_DOCKER_OPTS} ${REMOVE_CONTAINER} -ti tinkerpop:${BUILD_TAG}

status=$?
popd > /dev/null
exit $status
