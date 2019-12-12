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

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;

import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;

import java.util.concurrent.CompletableFuture;

/**
 * Connection is a misnomer here and has been kept for historical purpose. This entity represents the lifetime of a
 * single Gremlin interaction to the server. Each connection uses a persistent WebSocket {@link Channel}
 * to send/receive data from the server. The associated {@link Channel} is released when the request associated
 * with this connection has been completed and all the results have been read.
 *
 * The management of a Connection is done using a {@link ConnectionPool}.
 *
 * @see ConnectionPool
 */
public interface Connection {

    int DEFAULT_MAX_WAIT_FOR_CONNECTION = 30000;
    int DEFAULT_MAX_WAIT_FOR_SESSION_CLOSE = 3000;
    int DEFAULT_MAX_CONTENT_LENGTH = 65536;
    int DEFAULT_RECONNECT_INTERVAL = 1000;
    int DEFAULT_RESULT_ITERATION_BATCH_SIZE = 64;
    long DEFAULT_KEEP_ALIVE_INTERVAL = 1800000;

    /**
     * Write a Gremlin request to the server.
     *
     * @param requestMessage Gremlin {@link RequestMessage} that is being sent to the server.
     * @param resultQueueFuture Future that will contain the {@link ResultSet} which would be used to
     *                          stream the result to the {@link RequestMessage}
     * @return ChannelPromise Promise which represents a successful send (I/O event) of request to the server.
     */
    ChannelPromise write(RequestMessage requestMessage, CompletableFuture<ResultSet> resultQueueFuture);

    /**
     * @return Channel The underlying Netty {@link Channel} which is used by this connection.
     */
    Channel getChannel();

    /**
     * @return Host The {@link Host} this connection is connected to.
     */
    Host getHost();
}