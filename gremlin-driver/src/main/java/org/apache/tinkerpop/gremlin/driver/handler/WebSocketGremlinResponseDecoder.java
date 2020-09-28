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
package org.apache.tinkerpop.gremlin.driver.handler;

import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.MessageTextSerializer;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;

import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@ChannelHandler.Sharable
public final class WebSocketGremlinResponseDecoder extends MessageToMessageDecoder<WebSocketFrame> {
    private final MessageSerializer serializer;

    public WebSocketGremlinResponseDecoder(final MessageSerializer serializer) {
        this.serializer = serializer;
    }

    @Override
    protected void decode(final ChannelHandlerContext channelHandlerContext, final WebSocketFrame webSocketFrame, final List<Object> objects) throws Exception {
        if (webSocketFrame instanceof BinaryWebSocketFrame) {
            final BinaryWebSocketFrame tf = (BinaryWebSocketFrame) webSocketFrame;
            objects.add(serializer.deserializeResponse(tf.content()));
        } else if (webSocketFrame instanceof TextWebSocketFrame) {
            final TextWebSocketFrame tf = (TextWebSocketFrame) webSocketFrame;
            final MessageTextSerializer textSerializer = (MessageTextSerializer) serializer;
            objects.add(textSerializer.deserializeResponse(tf.text()));
        } else {
            throw new RuntimeException(String.format("WebSocket channel does not handle this type of message: %s", webSocketFrame.getClass().getName()));
        }
    }
}
