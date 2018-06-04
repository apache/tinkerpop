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

set -e

CONF_FILE=$1

# IP substitution hack borrowed from:
# https://github.com/htaox/NEAT/blob/94a004831cf89767e116d955192fc14ac82e5069/docker-scripts/gremlin-server-3.0.0/files/default_cmd#L5
IP=$(ip -o -4 addr list eth0 | perl -n -e 'if (m{inet\s([\d\.]+)\/\d+\s}xms) { print $1 }')
sed -i "s|^host:.*|host: $IP|" $CONF_FILE

exec /opt/gremlin-server/bin/gremlin-server.sh "$@"