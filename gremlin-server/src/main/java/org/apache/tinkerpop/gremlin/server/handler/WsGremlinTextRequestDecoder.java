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

import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.ser.MessageTextSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Decodes the contents of a {@link TextWebSocketFrame}.  Text-based frames are always assumed to be
 * "application/json" when it comes to serialization.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@ChannelHandler.Sharable
public class WsGremlinTextRequestDecoder extends MessageToMessageDecoder<TextWebSocketFrame> {
    private static final Logger logger = LoggerFactory.getLogger(WsGremlinTextRequestDecoder.class);

    private final Map<String, MessageSerializer<?>> serializers;

    public WsGremlinTextRequestDecoder(final Map<String, MessageSerializer<?>> serializers) {
        this.serializers = serializers;
    }

    @Override
    protected void decode(final ChannelHandlerContext channelHandlerContext, final TextWebSocketFrame frame, final List<Object> objects) throws Exception {
        try {
            // the default serializer must be a MessageTextSerializer instance to be compatible with this decoder
            final MessageTextSerializer<?> serializer = (MessageTextSerializer<?>) select("application/json", ServerSerializers.DEFAULT_TEXT_SERIALIZER);

            // it's important to re-initialize these channel attributes as they apply globally to the channel. in
            // other words, the next request to this channel might not come with the same configuration and mixed
            // state can carry through from one request to the next
            channelHandlerContext.channel().attr(StateKey.SESSION).set(null);
            channelHandlerContext.channel().attr(StateKey.SERIALIZER).set(serializer);
            channelHandlerContext.channel().attr(StateKey.USE_BINARY).set(false);

            objects.add(serializer.deserializeRequest(frame.text()));
        } catch (SerializationException se) {
            objects.add(RequestMessage.INVALID);
        }
    }

    private MessageSerializer<?> select(final String mimeType, final MessageSerializer<?> defaultSerializer) {
        if (logger.isWarnEnabled() && !serializers.containsKey(mimeType))
            logger.warn("Gremlin Server is not configured with a serializer for the requested mime type [{}] - using {} by default",
                    mimeType, defaultSerializer.getClass().getName());

        return serializers.getOrDefault(mimeType, defaultSerializer);
    }
}
