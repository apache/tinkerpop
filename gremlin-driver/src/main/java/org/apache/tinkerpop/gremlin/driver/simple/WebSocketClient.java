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
package org.apache.tinkerpop.gremlin.driver.simple;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.http.HttpHeaders;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.handler.WebSocketClientHandler;
import org.apache.tinkerpop.gremlin.driver.handler.WebSocketGremlinRequestEncoder;
import org.apache.tinkerpop.gremlin.driver.handler.WebSocketGremlinResponseDecoder;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 * A simple, non-thread safe Gremlin Server client using websockets.  Typical use is for testing and demonstration.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class WebSocketClient extends AbstractClient {
    private final Channel channel;

    public WebSocketClient() {
        this(URI.create("ws://localhost:8182/gremlin"));
    }

    public WebSocketClient(final URI uri) {
        super("ws-client-%d");
        final Bootstrap b = new Bootstrap().group(group);
        b.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

        final String protocol = uri.getScheme();
        if (!"ws".equals(protocol))
            throw new IllegalArgumentException("Unsupported protocol: " + protocol);

        try {
            final WebSocketClientHandler wsHandler =
                    new WebSocketClientHandler(
                            WebSocketClientHandshakerFactory.newHandshaker(
                                    uri, WebSocketVersion.V13, null, false, HttpHeaders.EMPTY_HEADERS, 65536));
            final MessageSerializer serializer = new GryoMessageSerializerV3d0();
            b.channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(final SocketChannel ch) {
                            final ChannelPipeline p = ch.pipeline();
                            p.addLast(
                                    new HttpClientCodec(),
                                    new HttpObjectAggregator(65536),
                                    wsHandler,
                                    new WebSocketGremlinRequestEncoder(true, serializer),
                                    new WebSocketGremlinResponseDecoder(serializer),
                                    callbackResponseHandler);
                        }
                    });

            channel = b.connect(uri.getHost(), uri.getPort()).sync().channel();
            wsHandler.handshakeFuture().get(10000, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void writeAndFlush(final RequestMessage requestMessage) throws Exception {
        channel.writeAndFlush(requestMessage).get();
    }

    @Override
    public void close() throws IOException {
        try {
            channel.close().get();
        } catch (Exception ignored) {

        } finally {
            group.shutdownGracefully().awaitUninterruptibly();
        }
    }
}
