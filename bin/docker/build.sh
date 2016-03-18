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
TINKERPOP_BUILD_OPTIONS=${@:-"-DskipIntegrationTests=false -DincludeNeo4j"}

function cleanup {
  if [[ "$(docker images -q tinkerpop:build 2> /dev/null)" == "" ]]; then
    docker rmi tinkerpop:build
  fi
  rm -f ${DIR}/../../Dockerfile
}
trap cleanup EXIT

pushd ${DIR}/../.. > /dev/null

cp bin/docker/Dockerfile.build Dockerfile
cat >> Dockerfile <<EOF
CMD ["sh", "-c", "mvn clean install ${TINKERPOP_BUILD_OPTIONS}"]
EOF

docker build -t tinkerpop:build .
docker run --rm tinkerpop:build

popd > /dev/null
