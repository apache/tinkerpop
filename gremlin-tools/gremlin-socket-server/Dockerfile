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

FROM alpine

LABEL maintainer="dev@tinkerpop.apache.org"

ARG SOCKET_SERVER_DIR
ARG SOCKET_SERVER_VERSION

RUN apk add --no-cache --update \
    bash \
    openjdk11-jdk

COPY ${SOCKET_SERVER_DIR}/gremlin-socket-server-${SOCKET_SERVER_VERSION}.jar /opt/gremlin-socket-server/gremlin-socket-server.jar
COPY ${SOCKET_SERVER_DIR}/libs/ /opt/gremlin-socket-server/libs/
COPY ${SOCKET_SERVER_DIR}/../conf/ opt/gremlin-socket-server/conf/

WORKDIR /opt/gremlin-socket-server

EXPOSE 45943

ENTRYPOINT ["java","-jar","gremlin-socket-server.jar"]
