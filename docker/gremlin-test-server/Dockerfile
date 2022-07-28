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

ARG GREMLIN_SERVER
FROM tinkerpop/gremlin-server:$GREMLIN_SERVER

USER root
RUN mkdir -p /opt
WORKDIR /opt
COPY gremlin-server/src/test /opt/test/
COPY docker/gremlin-server/docker-entrypoint.sh docker/gremlin-server/*.yaml /opt/
RUN chmod 755 /opt/docker-entrypoint.sh

# Installing dos2unix to avoid errors in running the entrypoint script on Windows machines where their
# carriage return is \r\n instead of the \n needed for linux/unix containers
RUN apk update && apk add dos2unix
RUN dos2unix /opt/docker-entrypoint.sh && apk del dos2unix

ARG GREMLIN_SERVER
ENV GREMLIN_SERVER_VERSION=$GREMLIN_SERVER

EXPOSE 45940-45942 4588

ENTRYPOINT ["/opt/docker-entrypoint.sh", "-n"]
CMD []
