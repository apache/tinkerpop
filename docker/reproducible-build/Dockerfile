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

# =============================================================================
# Reproducible-Build Validation Dockerfile
# =============================================================================
# Purpose: Provides a hermetic, version-pinned build environment for validating
# that TinkerPop release artifacts can be reproduced bit-for-bit from source.
#
# All tool versions are pinned exactly so that builds performed months or years
# apart produce identical output — this is the foundation of reproducibility.
# =============================================================================

# JDK pinned to Eclipse Temurin 11.0.22_7 on Ubuntu Jammy.
# - JDK 11 is used because TinkerPop releases are built with Java 11 (per README).
# - The exact patch version is pinned to eliminate any behavioral differences
#   between JDK micro-releases that could affect compiled bytecode or timestamps.
FROM eclipse-temurin:11.0.22_7-jdk-jammy

# Maven version pinned exactly. Different Maven versions may produce different
# artifact metadata, plugin resolution order, or archive entry ordering.
ARG MAVEN_VERSION=3.9.6

# SHA-512 of apache-maven-3.9.6-bin.tar.gz from:
# https://archive.apache.org/dist/maven/maven-3/3.9.6/binaries/apache-maven-3.9.6-bin.tar.gz.sha512
# This ensures the downloaded archive has not been tampered with.
ARG MAVEN_SHA512=706f01b20dec0305a822ab614d51f32b07ee11d0218175e55450242e49d2156386483b506b3a4e8a03ac8611bae96395fd5eec15f50d3013d5deed6d1ee18224

# Install minimal required packages:
#   curl  - download Maven archive
#   git   - checkout source at a specific tag
#   unzip - inspect/compare ZIP and JAR artifacts
#   diffutils - diff for entry-by-entry comparison
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        git \
        unzip \
        diffutils && \
    rm -rf /var/lib/apt/lists/*

# Install pinned Maven with checksum verification.
# Using the ASF archive URL ensures long-term availability of this exact version.
RUN curl -fsSL https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
        -o /tmp/maven.tar.gz && \
    echo "${MAVEN_SHA512}  /tmp/maven.tar.gz" | sha512sum -c - && \
    tar xzf /tmp/maven.tar.gz -C /opt && \
    rm /tmp/maven.tar.gz && \
    ln -s /opt/apache-maven-${MAVEN_VERSION}/bin/mvn /usr/local/bin/mvn

# Set JAVA_HOME explicitly for tools that need it.
ENV JAVA_HOME=/opt/java/openjdk
ENV MAVEN_HOME=/opt/apache-maven-${MAVEN_VERSION}

WORKDIR /build
