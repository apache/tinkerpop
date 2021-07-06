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

TINKERPOP_HOME=/opt/gremlin-server
cp /opt/test/scripts/* ${TINKERPOP_HOME}/scripts

IP=$(hostname -i)

INCLUDE_NEO4J=

function usage {
  echo -e "\nUsage: `basename $0` <version> [OPTIONS]" \
          "\nStart Gremlin Server instances that match the Maven integration test environment." \
          "\n\nOptions are:\n" \
          "\n\t<version> This value is optional and if unspecified will build the current version" \
          "\n\t-n, --neo4j              include Neo4j to make transactions testable" \
          "\n\t-h, --help               show this message" \
          "\n"
}

while [ ! -z "$1" ]; do
  case "$1" in
    -n | --neo4j ) INCLUDE_NEO4J=true; shift ;;
    -h | --help ) usage; exit 0 ;;
    *) usage 1>&2; exit 1 ;;
  esac
done

echo "#############################################################################"
echo IP is $IP
echo
echo Available Gremlin Server instances:
echo "ws://${IP}:45940/gremlin with anonymous access"
echo "ws://${IP}:45941/gremlin with basic authentication (stephen/password)"
echo "ws://${IP}:45942/gremlin with kerberos authentication (stephen/password)"
echo
if [ ! -z "${INCLUDE_NEO4J}" ]; then
  echo Installing Neo4j to the environment: transactions are testable on port 45940
  echo
fi
echo "See docker/gremlin-server/docker-entrypoints.sh for transcripts per GLV."
echo "#############################################################################"

cp *.yaml ${TINKERPOP_HOME}/conf/

java -version

# dynamically installs Neo4j libraries so that we can test variants with transactions,
# but only only port 45940 is configured with the neo4j graph as the neo4j-empty.properties
# is statically pointing at a temp directory and that space can only be accessed by one
# graph at a time
if [ ! -z "${INCLUDE_NEO4J}" ]; then
  sed -i 's/graphs: {/graphs: {\n  tx: conf\/neo4j-empty.properties,/' ${TINKERPOP_HOME}/conf/gremlin-server-integration.yaml
  /opt/gremlin-server/bin/gremlin-server.sh install org.apache.tinkerpop neo4j-gremlin ${GREMLIN_SERVER_VERSION}
fi

/opt/gremlin-server/bin/gremlin-server.sh conf/gremlin-server-integration.yaml &

/opt/gremlin-server/bin/gremlin-server.sh conf/gremlin-server-integration-secure.yaml &

java -cp /opt/gremlin-test/gremlin-test-${GREMLIN_SERVER_VERSION}-jar-with-dependencies.jar \
     -Dlogback.configurationFile="file:/opt/gremlin-server/conf/logback.xml" \
     org.apache.tinkerpop.gremlin.server.KdcFixture /opt/gremlin-server &

export JAVA_OPTIONS="-Xms512m -Xmx4096m -Djava.security.krb5.conf=/opt/gremlin-server/target/kdc/krb5.conf"
exec /opt/gremlin-server/bin/gremlin-server.sh conf/gremlin-server-integration-krb5.yaml


#######################################################################
# Transcripts for connecting to gremlin-server-test using various GLV's
#######################################################################
#
# cd ${APACHE_TINKERPOP}                              # first terminal: location of cloned gitrepo
# docker/gremlin-server.sh

# cd ${APACHE_TINKERPOP}                              # second terminal
# export KRB5_CONFIG=`pwd`/docker/gremlin-server/krb5.conf
# echo 'password' | kinit stephen
# klist

# Gremlin-Groovy
# --------------
# KRB5_OPTION="-Djava.security.krb5.conf=`pwd`/docker/gremlin-server/krb5.conf"
# JAAS_OPTION="-Djava.security.auth.login.config=`pwd`/docker/gremlin-server/gremlin-console-jaas.conf"
# export JAVA_OPTIONS="${KRB5_OPTION} ${JAAS_OPTION}"
# cd gremlin-console/target/apache-tinkerpop-gremlin-console-3.x.y-SNAPSHOT-standalone
# If necessary (versions 3.4.2-3.4.6): in bin/gremlin.sh replace JVM_OPTS+=( "${JAVA_OPTIONS}" ) by JVM_OPTS+=( ${JAVA_OPTIONS} )
# bin/gremlin.sh
# gremlin> cluster = Cluster.build("172.17.0.2").port(45940).create()
# gremlin> cluster = Cluster.build("172.17.0.2").port(45941).credentials("stephen", "password").create()
# gremlin> cluster = Cluster.build("172.17.0.2").port(45942).addContactPoint("gremlin-server-test").protocol("test-service").jaasEntry("GremlinConsole").create()
# gremlin> g = traversal().withRemote(DriverRemoteConnection.using(cluster, "gmodern"))
# gremlin> g.V()

# Gremlin-Python
# --------------
# cd gremlin-python/target/python3
# source env/bin/activate
# python
# >>> from gremlin_python.process.anonymous_traversal import traversal
# >>> from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
# >>> g = traversal().withRemote(DriverRemoteConnection('ws://172.17.0.2:45940/gremlin','gmodern'))
# >>> g = traversal().withRemote(DriverRemoteConnection('ws://172.17.0.2:45941/gremlin','gmodern', username='stephen', password='password'))
# >>> g = traversal().withRemote(DriverRemoteConnection('ws://172.17.0.2:45942/gremlin','gmodern', kerberized_service='test-service@gremlin-server-test'))
# >>> g.V().toList()
