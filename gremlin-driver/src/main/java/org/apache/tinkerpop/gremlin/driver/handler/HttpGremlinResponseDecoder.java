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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultHttpObject;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.AttributeMap;
import io.netty.util.CharsetUtil;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.SerTokens;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;

import java.util.List;

import static org.apache.tinkerpop.gremlin.driver.Channelizer.HttpChannelizer.LAST_CONTENT_READ_RESPONSE;
import static org.apache.tinkerpop.gremlin.driver.handler.HttpGremlinResponseStreamDecoder.IS_BULKED;

public class HttpGremlinResponseDecoder extends MessageToMessageDecoder<FullHttpResponse> {
    private static final String MESSAGE_NAME = "message";
    private final MessageSerializer<?> serializer;
    private final ObjectMapper mapper = new ObjectMapper();

    public HttpGremlinResponseDecoder(final MessageSerializer<?> serializer) {
        this.serializer = serializer;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, FullHttpResponse msg, List<Object> out) throws Exception {
        // A value must be set in order to signal to the InactiveChannelHandler that data has been received.
        ctx.channel().attr(InactiveChannelHandler.BYTES_READ).set(0);

        final ByteBuf content = msg.content();
        ResponseMessage response;
        
        try {
            // no more chunks expected
            if (isError(msg.status()) && !serializer.mimeTypesSupported()[0].equals(msg.headers().get(HttpHeaderNames.CONTENT_TYPE))) {
                final String json = content.toString(CharsetUtil.UTF_8);
                final JsonNode node = mapper.readTree(json);
                final String message = node.has(MESSAGE_NAME) ? node.get(MESSAGE_NAME).asText() : "";
                response = ResponseMessage.build()
                        .code(msg.status())
                        .statusMessage(message.isEmpty() ? msg.status().reasonPhrase() : message)
                        .create();
            } else {
                response = serializer.deserializeBinaryResponse(content);
            }

            ctx.channel().attr(IS_BULKED).set(response.getResult().isBulked());
            out.add(response);
            out.add(LAST_CONTENT_READ_RESPONSE);
        } catch (SerializationException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isError(final HttpResponseStatus status) {
        return status != HttpResponseStatus.OK;
    }
}