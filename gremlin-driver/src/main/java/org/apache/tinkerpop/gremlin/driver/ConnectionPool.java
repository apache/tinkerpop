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
package org.apache.tinkerpop.gremlin.driver;

import io.netty.channel.group.ChannelGroup;

import java.net.ConnectException;
import java.nio.channels.Channel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

/**
 * Connection pool is responsible for maintaining re-usable resources required to send
 * requests to a specific server. It is also the gatekeeper for the number of simultaneous requests
 * to the server.
 *
 * More specifically, it associates a Netty {@link Channel} with a {@link Connection}.
 *
 * A typical workflow for the lifetime of a Gremlin request would be as follows:
 * 1. Connection pool is set up attached to a host on initialization.
 * 2. Client borrows a connection from the connection pool.
 * 3. Connection pool chooses a healthy & available channel (or creates one if necessary) and
 *    creates a {@link Connection} associated to it.
 * 4. The lifecycle of the {@link Connection} ends when the results have been read and it is returned back to
 *    the pool.
 * 5. Connection pool reclaims the channel which is now free to be used with another request.
 */
public interface ConnectionPool {
    int DEFAULT_MAX_POOL_SIZE = 8;
    /**
     * @deprecated As of release 3.4.3, not replaced, this setting is ignored.
     * @see <a href="https://issues.apache.org/jira/browse/TINKERPOP-2205">TINKERPOP-2205</a>
     */
    @Deprecated
    int DEFAULT_MIN_POOL_SIZE = 2;
    /**
     * @deprecated As of release 3.4.3, not replaced, this setting is ignored.
     * @see <a href="https://issues.apache.org/jira/browse/TINKERPOP-2205">TINKERPOP-2205</a>
     */
    @Deprecated
    int DEFAULT_MIN_SIMULTANEOUS_USAGE_PER_CONNECTION = 8;
    /**
     * @deprecated As of release 3.4.3, replaced by {@link ConnectionPool#DEFAULT_MAX_POOL_SIZE}. For backward
     * compatibility it is still used to approximate the amount of parallelism required. In future versions, the
     * approximation logic will be removed and dependency on this parameter will be completely eliminated.
     * To disable the dependency on this parameter right now, explicitly set the value of
     * {@link Settings.ConnectionPoolSettings#maxInProcessPerConnection} and {@link Settings.ConnectionPoolSettings#maxSimultaneousUsagePerConnection}
     * to 0.
     *
     * @see ConnectionPoolImpl#calculateMaxPoolSize(Settings.ConnectionPoolSettings) for approximation
     * logic.
     * @see <a href="https://issues.apache.org/jira/browse/TINKERPOP-2205">TINKERPOP-2205</a>
     */
    @Deprecated
    int DEFAULT_MAX_SIMULTANEOUS_USAGE_PER_CONNECTION = 16;
    /**
     * Borrow a connection from the connection pool which would execute the request. Connection pool ensures
     * that the connection is backed by a healthy {@link Channel} and WebSocket handshake is already complete.
     *
     * @return {@link Connection} which is backed by an active {@link Channel}
     *         and could be used to send request.
     *
     * @throws TimeoutException When the connection could not be set timely
     * @throws ConnectException When there is a connectivity problem associated with the server
     */
    Connection prepareConnection() throws TimeoutException, ConnectException;

    /**
     * Get all the {@link Channel}s which are currently in-use.
     */
    ChannelGroup getActiveChannels();

    /**
     * Release the connection and associated resources (like channel) so that the resources can be re-used.
     */
    CompletableFuture<Void> releaseConnection(Connection conn);
    /**
     * Close the connection pool and all associated resources gracefully.
     * This method should be made idempotent and thread safe.
     */
    CompletableFuture<Void> closeAsync();

    ScheduledExecutorService executor();
    /**
     * @return {@link Host} associated with the connection pool
     */
    Host getHost();
    /**
     * @return {@link Cluster} containing the {@link Host}
     */
    Cluster getCluster();
}
