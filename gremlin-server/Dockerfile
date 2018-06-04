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

FROM openjdk:8u151-jre-alpine3.7

ARG GREMLIN_SERVER_DIR

RUN apk add --update \
    bash \
    perl \
    && rm -rf /var/cache/apk/*

COPY src/main/docker/docker-entrypoint.sh /
COPY ${GREMLIN_SERVER_DIR} /opt/gremlin-server

WORKDIR /opt/gremlin-server

EXPOSE 8182

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["conf/gremlin-server.yaml"]