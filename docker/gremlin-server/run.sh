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

TINKERPOP_HOME=$(find . -type d -name 'apache-tinkerpop-gremlin-server-*' -print -quit)

cp /opt/test/scripts/* ${TINKERPOP_HOME}/scripts

IP=$(hostname -I)
echo "#######################"
echo IP is $IP
echo "#######################"

cat /opt/test/resources/org/apache/tinkerpop/gremlin/server/gremlin-server-integration.yaml | sed "s/host: localhost/host: 0.0.0.0/" > ${TINKERPOP_HOME}/conf/gremlin-server-integration.yaml

cd ${TINKERPOP_HOME}
./bin/gremlin-server.sh conf/gremlin-server-integration.yaml
