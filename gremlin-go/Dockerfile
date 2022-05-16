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

ARG GREMLIN_SERVER_VERSION
FROM tinkerpop/gremlin-server:${GREMLIN_SERVER_VERSION:-3.6.1-SNAPSHOT}

USER root
RUN mkdir -p /opt
WORKDIR /opt
COPY gremlin-server/src/test /opt/test/
COPY gremlin-go/docker/generate-all.groovy /opt/test/scripts/
COPY gremlin-go/docker/docker-entrypoint.sh gremlin-go/docker/*.yaml /opt/
RUN chmod 755 /opt/docker-entrypoint.sh

# Installing dos2unix to avoid errors in running the entrypoint script on Windows machines where their
# carriage return is \r\n instead of the \n needed for linux/unix containers
RUN apk update && apk add dos2unix
RUN dos2unix /opt/docker-entrypoint.sh && apk del dos2unix

ARG NEO4J_VERSION
# Installs Neo4j libraries to this image so that we can test variants with transactions,
# but only only port 45940 is configured with the neo4j graph as the neo4j-empty.properties
# is statically pointing at a temp directory and that space can only be accessed by one
# graph at a time.
RUN /opt/gremlin-server/bin/gremlin-server.sh install org.apache.tinkerpop neo4j-gremlin ${NEO4J_VERSION:-3.6.1}

# Gremlin server and neo4j versions are set to 3.6.1 by default, to change their versions, add args under the
# docker-compose build step, e.g.:
#      args:
#        GREMLIN_SERVER_VERSION: 3.5.x-SNAPSHOT
#        NEO4J_VERSION: 3.5.x

EXPOSE 45940 45941

ENTRYPOINT ["bash", "/opt/docker-entrypoint.sh"]
CMD []
