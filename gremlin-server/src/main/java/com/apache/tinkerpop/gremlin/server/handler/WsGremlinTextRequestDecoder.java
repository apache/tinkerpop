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
package com.apache.tinkerpop.gremlin.server.handler;

import com.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import com.apache.tinkerpop.gremlin.driver.ser.MessageTextSerializer;
import com.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import com.apache.tinkerpop.gremlin.driver.ser.Serializers;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import java.util.List;

/**
 * Decodes the contents of a {@link TextWebSocketFrame}.  Text-based frames are always assumed to be
 * "application/json" when it comes to serialization.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@ChannelHandler.Sharable
public class WsGremlinTextRequestDecoder extends MessageToMessageDecoder<TextWebSocketFrame> {

    @Override
    protected void decode(final ChannelHandlerContext channelHandlerContext, final TextWebSocketFrame frame, final List<Object> objects) throws Exception {
        // the default serializer must be a MessageTextSerializer instance to be compatible with this decoder
        final MessageTextSerializer serializer = (MessageTextSerializer) Serializers.DEFAULT_REQUEST_SERIALIZER;
        channelHandlerContext.channel().attr(StateKey.SERIALIZER).set(serializer);
        channelHandlerContext.channel().attr(StateKey.USE_BINARY).set(false);

        try {
            objects.add(serializer.deserializeRequest(frame.text()));
        } catch (SerializationException se) {
            objects.add(RequestMessage.INVALID);
        }
    }
}
