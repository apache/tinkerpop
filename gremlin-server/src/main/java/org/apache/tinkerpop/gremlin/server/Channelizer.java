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
package org.apache.tinkerpop.gremlin.server;

import io.netty.channel.ChannelHandler;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;

/**
 * An interface that makes it possible to plugin different Netty pipelines to Gremlin Server, enabling the use of
 * different protocols, mapper security and other such functions.  A {@code Channelizer} implementation can be
 * configured in Gremlin Server with the {@code channelizer} setting in the configuration file.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @see AbstractChannelizer
 */
public interface Channelizer extends ChannelHandler {

    /**
     * This method is called just after the {@code Channelizer} is initialized.
     */
    public void init(final ServerGremlinExecutor serverGremlinExecutor);

    /**
     * Create a message to send to seemingly dead clients to see if they respond back. The message sent will be
     * dependent on the implementation. For example, a websocket implementation would create a "ping" message.
     * This method will only be used if {@link #supportsIdleMonitor()} is {@code true}.
     */
    public default Object createIdleDetectionMessage() {
        return null;
    }

    /**
     * Determines if the channelizer supports a method for keeping the connection alive and auto-closing zombie
     * channels.
     */
    public default boolean supportsIdleMonitor() {
        return false;
    }
}
