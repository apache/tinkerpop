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

if [ -z "$1" ]; then
  echo 'Missing required parameter. Expected a server version. E.g. 3.7.0'
  exit 1
fi

export CONTAINER_TIMEOUT=300.0 # Timeout needed in case tests hang.
export ABS_PROJECT_HOME=$PWD
export GREMLIN_SERVER=$1 # Server version.

# Need the following because gremlin-test jar with server version needed for KDC.
# This is currently how it is specified in the Dockerfiles.
git reset --hard > /dev/null
git checkout $GREMLIN_SERVER > /dev/null
mvn install -pl gremlin-test > /dev/null

# Docker Compose files were added in 3.5.5
for GREMLIN_DRIVER in $(git tag --list 3.5.[5-9]* 3.[6-9]*); do
  [ "$GREMLIN_DRIVER" == "$GREMLIN_SERVER" ] && break

  echo "Testing driver ${GREMLIN_DRIVER} with server ${GREMLIN_SERVER}"

  git reset --hard > /dev/null
  git checkout $GREMLIN_DRIVER > /dev/null
  [ $? -ne 0 ] && exit $?
  git checkout $GREMLIN_SERVER gremlin-server/ docker/ > /dev/null
  [ $? -ne 0 ] && exit $?

  log_directory=logs/${GREMLIN_DRIVER}driver_${GREMLIN_SERVER}server
  mkdir -p $log_directory
  cd ./gremlin-javascript/src/main/javascript/gremlin-javascript
  timeout --foreground $CONTAINER_TIMEOUT docker-compose up --build --exit-code-from gremlin-js-integration-tests > ${ABS_PROJECT_HOME}/${log_directory}/javascript.log 2>&1
  status=$?
  docker-compose down
  [ $status -ne 0 ] && echo "Failed. ${GREMLIN_DRIVER} driver is incompatible with ${GREMLIN_SERVER} server." && exit $status
  cd ../../../../..

  cd ./gremlin-dotnet/
  timeout --foreground $CONTAINER_TIMEOUT docker-compose up --build --exit-code-from gremlin-dotnet-integration-tests > ${ABS_PROJECT_HOME}/${log_directory}/dotnet.log 2>&1
  status=$?
  docker-compose down
  [ $status -ne 0 ] && echo "Failed. ${GREMLIN_DRIVER} driver is incompatible with ${GREMLIN_SERVER} server." && exit $status
  cd ..

  cd ./gremlin-go/
  timeout --foreground $CONTAINER_TIMEOUT docker-compose up --build --exit-code-from gremlin-go-integration-tests > ${ABS_PROJECT_HOME}/${log_directory}/go.log 2>&1
  status=$?
  docker-compose down
  [ $status -ne 0 ] && echo "Failed. ${GREMLIN_DRIVER} driver is incompatible with ${GREMLIN_SERVER} server." && exit $status
  cd ..

  cd gremlin-python
  mkdir tmp_test_dir # Copy src to temp directory to prevent polluting src directories with extra test files
  cp -R src/main/python/* tmp_test_dir
  export BUILD_DIR=$PWD/tmp_test_dir
  timeout --foreground $CONTAINER_TIMEOUT docker-compose up --build --abort-on-container-exit gremlin-server-test-python gremlin-python-integration-tests > ${ABS_PROJECT_HOME}/${log_directory}/python.log 2>&1
  status=$?
  rm -r tmp_test_dir
  docker-compose down
  [ $status -ne 0 ] && echo "Failed. ${GREMLIN_DRIVER} driver is incompatible with ${GREMLIN_SERVER} server." && exit $status
  cd ..

  echo "Success. ${GREMLIN_DRIVER} driver is compatible with ${GREMLIN_SERVER} server."
done