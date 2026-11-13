/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * The Java driver for Gremlin Server. The {@code driver.handler}, {@code driver.stream}, {@code driver.util} and
 * {@code driver.simple} packages are internal and not exported.
 */
module org.apache.tinkerpop.gremlin.driver {
    requires transitive org.apache.tinkerpop.gremlin.core;
    requires transitive org.apache.tinkerpop.gremlin.util;
    requires io.netty.transport;
    // Cluster.Builder takes a Netty SslContext
    requires transitive io.netty.handler;
    // Auth.sigv4() takes an AwsCredentialsProvider
    requires transitive software.amazon.awssdk.auth;

    requires io.netty.common;
    requires io.netty.buffer;
    requires io.netty.codec;
    requires io.netty.codec.http;
    requires io.netty.handler.proxy;
    requires software.amazon.awssdk.http;
    requires software.amazon.awssdk.http.auth.aws;
    requires software.amazon.awssdk.http.auth.spi;
    // Sigv4 hands the credentials to the signer as an AwsCredentialsIdentity
    requires software.amazon.awssdk.identity.spi;
    // the AWS signer loads its checksum implementation and its Reactive Streams API while signing a request
    requires software.amazon.awssdk.checksums.spi;
    requires software.amazon.awssdk.checksums;
    requires org.reactivestreams;
    requires software.amazon.awssdk.utils;
    requires org.yaml.snakeyaml;
    requires org.apache.commons.lang3;
    // Connection looks up jdk.net.ExtendedSocketOptions reflectively to set TCP_KEEPIDLE, which only works if
    // jdk.net is resolved, and nothing else makes that happen for an application on the module path
    requires jdk.net;
    // UserAgent only looks up the application name through JNDI when it is available
    requires static java.naming;

    exports org.apache.tinkerpop.gremlin.driver;
    exports org.apache.tinkerpop.gremlin.driver.auth;
    exports org.apache.tinkerpop.gremlin.driver.exception;
    exports org.apache.tinkerpop.gremlin.driver.remote;
}
