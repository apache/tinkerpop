/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.driver;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;

/**
 * Describes an HTTP proxy that the driver routes connections through. When configured on the {@link Cluster.Builder}
 * via {@link Cluster.Builder#proxy(ProxyOptions)} a Netty {@code HttpProxyHandler} is inserted into the pipeline
 * before the SSL handler so that the {@code CONNECT} tunnel is established prior to the TLS handshake.
 */
public final class ProxyOptions {

    private final SocketAddress address;
    private final String username;
    private final String password;

    private ProxyOptions(final SocketAddress address, final String username, final String password) {
        this.address = Objects.requireNonNull(address, "proxy address cannot be null");
        this.username = username;
        this.password = password;
    }

    /**
     * Creates a proxy configuration for the given host and port without authentication.
     */
    public static ProxyOptions create(final String host, final int port) {
        return new ProxyOptions(InetSocketAddress.createUnresolved(host, port), null, null);
    }

    /**
     * Creates a proxy configuration for the given host and port using basic proxy authentication.
     */
    public static ProxyOptions create(final String host, final int port, final String username, final String password) {
        return new ProxyOptions(InetSocketAddress.createUnresolved(host, port), username, password);
    }

    /**
     * Creates a proxy configuration for the given address without authentication.
     */
    public static ProxyOptions create(final SocketAddress address) {
        return new ProxyOptions(address, null, null);
    }

    /**
     * Creates a proxy configuration for the given address using basic proxy authentication.
     */
    public static ProxyOptions create(final SocketAddress address, final String username, final String password) {
        return new ProxyOptions(address, username, password);
    }

    public SocketAddress getAddress() {
        return address;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    /**
     * Determines if basic proxy authentication credentials were supplied.
     */
    public boolean hasCredentials() {
        return username != null;
    }
}
