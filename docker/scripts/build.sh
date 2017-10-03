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

RUN_TESTS=
RUN_INTEGRATION_TESTS=
INCLUDE_NEO4J=
BUILD_JAVA_DOCS=
BUILD_USER_DOCS=

function usage {
  echo -e "\nUsage: `basename $0` [OPTIONS]" \
          "\nBuild the current local TinkerPop project in a Docker container." \
          "\n\nOptions are:\n" \
          "\n\t-t, --tests              run standard test suite" \
          "\n\t-i, --integration-tests  run integration tests" \
          "\n\t-n, --neo4j              include Neo4j" \
          "\n\t-j, --java-docs          build Java docs" \
          "\n\t-d, --docs               build user docs" \
          "\n\t-h, --help               show this message" \
          "\n"
}

while [ ! -z "$1" ]; do
  case "$1" in
    -t | --tests ) RUN_TESTS=tue; shift ;;
    -i | --integration-tests ) RUN_INTEGRATION_TESTS=true; shift ;;
    -n | --neo4j ) INCLUDE_NEO4J=true; shift ;;
    -j | --java-docs ) BUILD_JAVA_DOCS=true; shift ;;
    -d | --docs ) BUILD_USER_DOCS=true; shift ;;
    -h | --help ) usage; exit 0 ;;
    *) usage 1>&2; exit 1 ;;
  esac
done

TINKERPOP_BUILD_OPTIONS=""

[ -z "${RUN_TESTS}" ] && TINKERPOP_BUILD_OPTIONS="${TINKERPOP_BUILD_OPTIONS} -DskipTests"
[ -z "${RUN_INTEGRATION_TESTS}" ] || TINKERPOP_BUILD_OPTIONS="${TINKERPOP_BUILD_OPTIONS} -DskipIntegrationTests=false"
[ -z "${INCLUDE_NEO4J}" ] || TINKERPOP_BUILD_OPTIONS="${TINKERPOP_BUILD_OPTIONS} -DincludeNeo4j"
[ -z "${BUILD_JAVA_DOCS}" ] && TINKERPOP_BUILD_OPTIONS="${TINKERPOP_BUILD_OPTIONS} -Dmaven.javadoc.skip=true"

# If the tmpfs (in-memory filesystem exists, use it)
TINKERMEM_PATH=$(cd .. ; echo `pwd`/tinkermem)
if [ -d "${TINKERMEM_PATH}" ]; then
  echo "Moving source to in-memory tmpfs"
  rsync --remove-source-files -a . ${TINKERMEM_PATH}
  cd ..
  rm -rf ${OLDPWD}
  ln -s ${TINKERMEM_PATH} ${OLDPWD}
  cd ${TINKERMEM_PATH}
fi

touch gremlin-python/.glv
touch gremlin-dotnet/src/.glv
touch gremlin-dotnet/test/.glv

# use a custom maven settings.xml
if [ -r "settings.xml" ]; then
  echo "Copying settings.xml"
  mkdir -p ~/.m2
  cp settings.xml ~/.m2/
fi

mvn clean install process-resources ${TINKERPOP_BUILD_OPTIONS} || exit 1
[ -z "${BUILD_JAVA_DOCS}" ] || mvn process-resources -Djavadoc || exit 1

if [ ! -z "${BUILD_USER_DOCS}" ]; then

  source ~/.bashrc

  echo -e "Host *\n  UserKnownHostsFile /dev/null\n  StrictHostKeyChecking no" > ~/.ssh/config
  service ssh start

  # start Hadoop
  cp docker/hadoop/resources/* ${HADOOP_PREFIX}/etc/hadoop/
  hdfs namenode -format
  ${HADOOP_PREFIX}/sbin/start-dfs.sh
  hdfs dfs -mkdir /user
  hdfs dfs -mkdir /user/${USER}
  ${HADOOP_PREFIX}/sbin/start-yarn.sh

  # build docs
  mkdir -p ~/.groovy
  cp docker/resources/groovy/grapeConfig.xml ~/.groovy/
  rm -rf /tmp/neo4j
  grep -l 'http://tinkerpop.apache.org/docs/x.y.z' $(find docs/src -name "*.asciidoc" | grep -v '^.docs/src/upgrade/') | xargs sed -i 's@http://tinkerpop.apache.org/docs/x.y.z@/docs/x.y.z@g'
  bin/process-docs.sh || exit 1

  # emulate published directory structure
  VERSION=$(cat pom.xml | grep -A1 '<artifactId>tinkerpop</artifactId>' | grep '<version>' | awk -F '>' '{print $2}' | awk -F '<' '{print $1}')
  mkdir target/docs/htmlsingle/docs
  ln -s .. target/docs/htmlsingle/docs/${VERSION}

  # start a simple HTTP server
  IP=$(ifconfig | grep -o 'inet addr:[0-9.]*' | cut -f2 -d ':' | head -n1)
  cd target/docs/htmlsingle/
  if [ -z "${BUILD_JAVA_DOCS}" ]; then
    echo -e "\nUser Docs can be viewed under http://${IP}/\n"
    python -m SimpleHTTPServer 80
  else
    echo -e "\nUser Docs can be viewed under http://${IP}/"
    echo -e "Java Docs can be viewed under http://${IP}:81/\n"
    python -m SimpleHTTPServer 80 &
    cd ../../site/apidocs/full/
    python -m SimpleHTTPServer 81
  fi

elif [ ! -z "${BUILD_JAVA_DOCS}" ]; then

  IP=$(ifconfig | grep -o 'inet addr:[0-9.]*' | cut -f2 -d ':' | head -n1)
  echo -e "\nJava Docs can be viewed under http://${IP}/\n"
  cd target/docs/htmlsingle/
  python -m SimpleHTTPServer 80

fi
