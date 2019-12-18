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
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.handler.NioGremlinRequestEncoder;
import org.apache.tinkerpop.gremlin.driver.handler.NioGremlinResponseDecoder;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1;

import java.io.IOException;
import java.net.URI;

/**
 * A simple, non-thread safe Gremlin Server client using NIO.  Typical use is for testing and demonstration.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.3.10, not replaced, use {@link WebSocketClient}.
 */
@Deprecated
public class NioClient extends AbstractClient {
    private final Channel channel;

    public NioClient() {
        this(URI.create("gs://localhost:8182"));
    }

    public NioClient(final URI uri) {
        super("nio-client-%d");
        final Bootstrap b = new Bootstrap().group(group);
        b.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

        try {
            final MessageSerializer serializer = new GraphBinaryMessageSerializerV1();
            b.channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(final SocketChannel ch) {
                            final ChannelPipeline p = ch.pipeline();
                            p.addLast(
                                    new NioGremlinResponseDecoder(serializer),
                                    new NioGremlinRequestEncoder(true, serializer),
                                    callbackResponseHandler);
                        }
                    });

            channel = b.connect(uri.getHost(), uri.getPort()).sync().channel();
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
