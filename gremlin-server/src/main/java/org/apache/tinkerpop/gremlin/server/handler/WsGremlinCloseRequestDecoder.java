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
package org.apache.tinkerpop.gremlin.server.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * Handles close requests over web sockets.  If the request is session-based then the close operation will not only close
 * the channel but also close the session and rollback uncommitted transactions.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@ChannelHandler.Sharable
public class WsGremlinCloseRequestDecoder extends MessageToMessageDecoder<CloseWebSocketFrame> {
    private static final Logger logger = LoggerFactory.getLogger(WsGremlinCloseRequestDecoder.class);

    private static final Charset UTF8 = Charset.forName("UTF-8");
    private final Map<String, MessageSerializer<?>> serializers;

    public WsGremlinCloseRequestDecoder(final Map<String, MessageSerializer<?>> serializers) {
        this.serializers = serializers;
    }

    @Override
    protected void decode(final ChannelHandlerContext channelHandlerContext, final CloseWebSocketFrame frame, final List<Object> objects) throws Exception {
        final ByteBuf messageBytes = frame.content();
        final byte len = messageBytes.readByte();
        if (len <= 0) {
            objects.add(RequestMessage.INVALID);
            return;
        }

        final ByteBuf contentTypeBytes = channelHandlerContext.alloc().buffer(len);
        try {
            messageBytes.readBytes(contentTypeBytes);
            final String contentType = contentTypeBytes.toString(UTF8);
            final MessageSerializer<?> serializer = select(contentType, ServerSerializers.DEFAULT_BINARY_SERIALIZER);

            // it's important to re-initialize these channel attributes as they apply globally to the channel. in
            // other words, the next request to this channel might not come with the same configuration and mixed
            // state can carry through from one request to the next
            channelHandlerContext.channel().attr(StateKey.SESSION).set(null);
            channelHandlerContext.channel().attr(StateKey.SERIALIZER).set(serializer);
            channelHandlerContext.channel().attr(StateKey.USE_BINARY).set(true);

            try {
                objects.add(serializer.deserializeRequest(messageBytes.discardReadBytes()));
            } catch (SerializationException se) {
                objects.add(RequestMessage.INVALID);
            }
        } finally {
            contentTypeBytes.release();
        }
    }

    private MessageSerializer<?> select(final String mimeType, final MessageSerializer<?> defaultSerializer) {
        if (logger.isWarnEnabled() && !serializers.containsKey(mimeType))
            logger.warn("Gremlin Server is not configured with a serializer for the requested mime type [{}] - using {} by default",
                    mimeType, defaultSerializer.getClass().getName());

        return serializers.getOrDefault(mimeType, defaultSerializer);
    }
}
