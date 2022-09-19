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
INCLUDE_GO=
INCLUDE_PYTHON=
INCLUDE_DOTNET=
INCLUDE_JAVASCRIPT=
INCLUDE_CONSOLE=

while [ ! -z "$1" ]; do
  case "$1" in
    -t  | --tests ) RUN_TESTS=true; shift ;;
    -i  | --integration-tests ) RUN_INTEGRATION_TESTS=true; shift ;;
    -n  | --neo4j ) INCLUDE_NEO4J=true; shift ;;
    -j  | --java-docs ) BUILD_JAVA_DOCS=true; shift ;;
    -d  | --docs ) BUILD_USER_DOCS=true; shift ;;
    -go | --golang ) INCLUDE_GO=true; shift ;;
    -py | --python ) INCLUDE_PYTHON=true; shift ;;
    -dn | --dotnet ) INCLUDE_DOTNET=true; shift ;;
    -js | --javascript ) INCLUDE_JAVASCRIPT=true; shift ;;
    -c  | --console ) INCLUDE_CONSOLE=true; shift ;;
  esac
done

# To prevent Docker in Docker, skip building images and deactivate Docker profiles.
TINKERPOP_BUILD_OPTIONS="-DskipImageBuild -P -glv-js,-glv-go,-glv-python"

[ -z "${RUN_TESTS}" ] && TINKERPOP_BUILD_OPTIONS="${TINKERPOP_BUILD_OPTIONS} -DskipTests"
[ -z "${RUN_INTEGRATION_TESTS}" ] || TINKERPOP_BUILD_OPTIONS="${TINKERPOP_BUILD_OPTIONS} -DskipIntegrationTests=false"
[ -z "${INCLUDE_NEO4J}" ] || TINKERPOP_BUILD_OPTIONS="${TINKERPOP_BUILD_OPTIONS} -DincludeNeo4j"
[ -z "${BUILD_JAVA_DOCS}" ] && TINKERPOP_BUILD_OPTIONS="${TINKERPOP_BUILD_OPTIONS} -Dmaven.javadoc.skip=true"

# If any of these GLVs are selected then create a minimal build to reduce build/test times
# as the user is likely not interested in the other modules.
if [[ -n ${INCLUDE_GO} || -n ${INCLUDE_PYTHON} || -n ${INCLUDE_DOTNET} || -n ${INCLUDE_JAVASCRIPT} || -n ${INCLUDE_CONSOLE} ]]; then
  # gremlin-server, gremlin-test and neo4j-gremlin are minimal dependencies needed to start a server for GLV testing.
  TINKERPOP_BUILD_OPTIONS="${TINKERPOP_BUILD_OPTIONS} -am -pl gremlin-server,gremlin-test,neo4j-gremlin"

  [ -n "${INCLUDE_GO}" ] && TINKERPOP_BUILD_OPTIONS="${TINKERPOP_BUILD_OPTIONS},gremlin-go"
  [ -n "${INCLUDE_PYTHON}" ] && TINKERPOP_BUILD_OPTIONS="${TINKERPOP_BUILD_OPTIONS},gremlin-python"
  [ -n "${INCLUDE_DOTNET}" ] && TINKERPOP_BUILD_OPTIONS="${TINKERPOP_BUILD_OPTIONS},gremlin-dotnet,:gremlin-dotnet-source"
  [ -n "${INCLUDE_JAVASCRIPT}" ] && TINKERPOP_BUILD_OPTIONS="${TINKERPOP_BUILD_OPTIONS},gremlin-javascript"
  [ -n "${INCLUDE_CONSOLE}" ] && TINKERPOP_BUILD_OPTIONS="${TINKERPOP_BUILD_OPTIONS},gremlin-console"
else
  # When building everything, deactivate dotnet tests to prevent Docker in Docker.
  TINKERPOP_BUILD_OPTIONS="${TINKERPOP_BUILD_OPTIONS} -pl -:gremlin-dotnet-tests"
fi

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

touch gremlin-dotnet/src/.glv

# use a custom maven settings.xml
if [ -r "settings.xml" ]; then
  echo "Copying settings.xml"
  mkdir -p ~/.m2
  cp settings.xml ~/.m2/
fi

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
mvn clean install process-resources --batch-mode ${TINKERPOP_BUILD_OPTIONS} || exit 1

[ -z "${BUILD_JAVA_DOCS}" ] || mvn process-resources -Djavadoc || exit 1

if [ ! -z "${BUILD_USER_DOCS}" ]; then

  source ~/.bashrc

  echo -e "Host *\n  UserKnownHostsFile /dev/null\n  StrictHostKeyChecking no" > ~/.ssh/config
  service ssh start

  # start Hadoop
  echo "export JAVA_HOME=$JAVA_HOME" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh
  cp docker/hadoop/resources/* ${HADOOP_HOME}/etc/hadoop/
  hdfs namenode -format
  export HDFS_DATANODE_USER=root
  export HDFS_DATANODE_SECURE_USER=hdfs
  export HDFS_NAMENODE_USER=root
  export HDFS_SECONDARYNAMENODE_USER=root
  export YARN_RESOURCEMANAGER_USER=root
  export YARN_NODEMANAGER_USER=root
  ${HADOOP_HOME}/sbin/start-dfs.sh
  hdfs dfs -mkdir /user
  hdfs dfs -mkdir /user/${USER}
  ${HADOOP_HOME}/sbin/start-yarn.sh

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
  IP=$(ip -4 address show | grep -Pv '\blo\b' | grep -o 'inet [0-9.]*' | cut -f2 -d ' ' | head -n1)
  cd target/docs/htmlsingle/
  if [ -z "${BUILD_JAVA_DOCS}" ]; then
    echo -e "\nUser Docs can be viewed under http://${IP}/\n"
    python3 -m http.server 80
  else
    echo -e "\nUser Docs can be viewed under http://${IP}/"
    echo -e "Java Docs can be viewed under http://${IP}:81/\n"
    python3 -m http.server 80 &
    cd ../../site/apidocs/full/
    python3 -m http.server 81
  fi

elif [ ! -z "${BUILD_JAVA_DOCS}" ]; then

  IP=$(ip -4 address show | grep -Pv '\blo\b' | grep -o 'inet [0-9.]*' | cut -f2 -d ' ' | head -n1)
  echo -e "\nJava Docs can be viewed under http://${IP}/\n"
  cd target/site/apidocs/full/
  python3 -m http.server 80

fi
