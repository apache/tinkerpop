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

# Image used for running integration tests for gremlin-console.
FROM eclipse-temurin:11-jre AS jre

FROM python:3.10-bookworm

LABEL maintainer="dev@tinkerpop.apache.org"

COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN apt-get update && apt-get install -y --no-install-recommends dos2unix && rm -rf /var/lib/apt/lists/*

# Test dependencies were previously pulled in via setup.py's tests_require and run
# through pytest-runner (setup.py test). That path was removed from modern setuptools,
# so the tests are now driven by pytest directly with the dependencies installed here.
RUN pip install --no-cache-dir \
    'pytest>=6.2.5,<8.0.0' \
    'mock>=3.0.5,<4.0.0' \
    'more-itertools<11.0.0' \
    'pexpect==4.8.0'

WORKDIR /console_app
CMD dos2unix ./gremlin-console/bin/gremlin.sh && python3 -m pytest; \
EXIT_CODE=$?; chown -R `stat -c '%u:%g' .` .; exit $EXIT_CODE
