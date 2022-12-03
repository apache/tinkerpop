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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketServerCompressionHandler;

/**
 * Class that holds the channelizers that can used for testing with the SimpleSocketServer.
 */
public class TestChannelizers {

    /**
     * A base ChannelInitializer that setups the pipeline for HTTP handling. This class should be sub-classed by a
     * handler that handles the actual data being received.
     */
    public static abstract class TestHttpServerInitializer extends ChannelInitializer<SocketChannel> {
        protected static final String WEBSOCKET_PATH = "/gremlin";

        @Override
        public void initChannel(SocketChannel ch) {
            final ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(new HttpServerCodec());
            pipeline.addLast(new HttpObjectAggregator(65536));
        }
    }

    /**
     * A vanilla WebSocket server Initializer implementation using Netty. This initializer would configure the server
     * for WebSocket handshake and decoding incoming WebSocket frames.
     */
    public static abstract class TestWebSocketServerInitializer extends TestChannelizers.TestHttpServerInitializer {

        @Override
        public void initChannel(SocketChannel ch) {
            super.initChannel(ch);

            final ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(new WebSocketServerCompressionHandler());
            pipeline.addLast(new WebSocketServerProtocolHandler(WEBSOCKET_PATH, null, true));
            this.postInit(ch.pipeline());
        }

        public abstract void postInit(final ChannelPipeline ch);
    }

    /**
     * An initializer that adds a handler that will drop WebSocket frames.
     */
    public static class TestWSNoOpInitializer extends TestHttpServerInitializer {

        @Override
        public void initChannel(SocketChannel ch) {
            super.initChannel(ch);
            ch.pipeline().addLast(new TestHandlers.NoOpWebSocketServerHandler(WEBSOCKET_PATH));
        }
    }

    /**
     * An initializer that adds a handler to mimics a server that is throttling connections.
     */
    public static class TestConnectionThrottlingInitializer extends TestHttpServerInitializer {
        int connectionCount = 0;

        @Override
        public void initChannel(SocketChannel ch) {
            super.initChannel(ch);

            final ChannelPipeline pipeline = ch.pipeline();
            if (connectionCount < 1) {
                pipeline.addLast(new WebSocketServerCompressionHandler());
                pipeline.addLast(new WebSocketServerProtocolHandler(WEBSOCKET_PATH, null, true));
                pipeline.addLast(new TestWSGremlinInitializer.ClientTestConfigurableHandler());
            } else {
                pipeline.addLast(new TestHandlers.NoOpWebSocketServerHandler(WEBSOCKET_PATH));
            }

            connectionCount++;
        }
    }
}
