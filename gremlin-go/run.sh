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

function usage {
  echo -e "\nUsage: $(basename "$0") <server-version> [OPTIONS]" \
          "\n\nRun the Gremlin-Go test suite in Docker." \
          "\n\nOptions:" \
          "\n\t<server-version> \t Optional value, if unspecified the test suite will run with the current version
           \t\t\t of Gremlin server and Gremlin Neo4j, as specified in the TinkerPop pom.xml file." \
          "\n\t-h, --help \t\t Show this message." \
          "\n\nExamples:" \
          "\n\tRunning the default: ./run.sh" \
          "\n\tThe default requires a SNAPSHOT server image to be built using:
          mvn clean install -pl :gremlin-server -DskipTests -DskipIntegrationTests=true -am && mvn install -Pdocker-images -pl :gremlin-server" \
          "\n\tRunning a prebuilt local SNAPSHOT build: ./run.sh 3.x.x-SNAPSHOT" \
          "\n\tRunning a released version: ./run.sh 3.5.3" \
          "\n"
}

if [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
    usage
    exit 0
fi

# Parses current gremlin server version from project pom.xml file using perl regex
GREMLIN_SERVER_VERSION=$(grep tinkerpop -A2 pom.xml | grep -Po '(?<=<version>)([0-9]+\.?){3}(-SNAPSHOT)?(?=<)')
export GREMLIN_SERVER="${1:-$GREMLIN_SERVER_VERSION}"
echo "Running server version: $GREMLIN_SERVER"

ABS_PROJECT_HOME=$(dirname $(realpath "$0"))/..
export ABS_PROJECT_HOME

# Passes current gremlin server version into docker compose as environment variable
docker-compose up --build --exit-code-from gremlin-go-integration-tests
EXIT_CODE=$?
# Removes all service containers
docker-compose down
exit $EXIT_CODE
