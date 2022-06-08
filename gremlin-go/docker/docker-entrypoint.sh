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

echo "#############################################################################"
echo IP is $IP
echo
echo Available Gremlin Server instances:
echo "ws://${IP}:45940/gremlin with anonymous access"
echo "wss://${IP}:45941/gremlin with basic authentication (stephen/password)"
echo Installing Neo4j to the environment: transactions are testable on port 45940
echo "#############################################################################"

cp *.yaml ${TINKERPOP_HOME}/conf/

java -version

/opt/gremlin-server/bin/gremlin-server.sh install org.apache.tinkerpop neo4j-gremlin "$GREMLIN_SERVER_VER"

/opt/gremlin-server/bin/gremlin-server.sh conf/gremlin-server-integration.yaml &

/opt/gremlin-server/bin/gremlin-server.sh conf/gremlin-server-integration-secure.yaml
