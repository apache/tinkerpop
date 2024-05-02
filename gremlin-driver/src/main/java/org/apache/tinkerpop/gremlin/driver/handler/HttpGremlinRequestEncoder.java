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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.AttributeMap;
import org.apache.tinkerpop.gremlin.driver.UserAgent;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.util.MessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.message.RequestMessageV4;
import org.apache.tinkerpop.gremlin.util.ser.SerTokens;

import java.util.List;
import java.util.UUID;
import java.util.function.UnaryOperator;

/**
 * Converts {@link RequestMessageV4} to a {@code HttpRequest}.
 */
@ChannelHandler.Sharable
public final class HttpGremlinRequestEncoder extends MessageToMessageEncoder<RequestMessageV4> {

    //todo: move
    public static final AttributeKey<UUID> REQUEST_ID = AttributeKey.valueOf("requestId");
    private final MessageSerializerV4<?> serializer;
    private final boolean userAgentEnabled;
    private final UnaryOperator<FullHttpRequest> interceptor;

    @Deprecated
    public HttpGremlinRequestEncoder(final MessageSerializerV4<?> serializer, final UnaryOperator<FullHttpRequest> interceptor) {
        this.serializer = serializer;
        this.interceptor = interceptor;
        this.userAgentEnabled = true;
    }

    public HttpGremlinRequestEncoder(final MessageSerializerV4<?> serializer, final UnaryOperator<FullHttpRequest> interceptor, boolean userAgentEnabled) {
        this.serializer = serializer;
        this.interceptor = interceptor;
        this.userAgentEnabled = userAgentEnabled;
    }

    @Override
    protected void encode(final ChannelHandlerContext channelHandlerContext, final RequestMessageV4 requestMessage, final List<Object> objects) throws Exception {
        final Attribute<UUID> requestIdAttribute = ((AttributeMap) channelHandlerContext).attr(REQUEST_ID);
        requestIdAttribute.set(requestMessage.getRequestId());
        System.out.println("HttpGremlinRequestEncoder set requestId: " + requestIdAttribute.get());

        final String mimeType = serializer.mimeTypesSupported()[0];
        // only GraphSON3 and GraphBinary recommended for serialization of Bytecode requests
        if (requestMessage.getField("gremlin") instanceof Bytecode &&
                !mimeType.equals(SerTokens.MIME_GRAPHSON_V4) &&
                !mimeType.equals(SerTokens.MIME_GRAPHBINARY_V4)) {
            // todo: correct status code !!!
            throw new ResponseException(HttpResponseStatus.INTERNAL_SERVER_ERROR, String.format(
                    "An error occurred during serialization of this request [%s] - it could not be sent to the server - Reason: only GraphSON3 and GraphBinary recommended for serialization of Bytecode requests, but used %s",
                    requestMessage, serializer.getClass().getName()));
        }

        try {
            final ByteBuf buffer = serializer.serializeRequestAsBinary(requestMessage, channelHandlerContext.alloc());
            final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/", buffer);
            request.headers().add(HttpHeaderNames.CONTENT_TYPE, mimeType);
            request.headers().add(HttpHeaderNames.CONTENT_LENGTH, buffer.readableBytes());
            request.headers().add(HttpHeaderNames.ACCEPT, mimeType);
            if (userAgentEnabled) {
                request.headers().add(HttpHeaderNames.USER_AGENT, UserAgent.USER_AGENT);
            }
            objects.add(interceptor.apply(request));

            System.out.println("----------------------------");
        } catch (Exception ex) {
            // todo: correct status code !!!
            throw new ResponseException(HttpResponseStatus.INTERNAL_SERVER_ERROR, String.format(
                    "An error occurred during serialization of this request [%s] - it could not be sent to the server - Reason: %s",
                    requestMessage, ex));
        }
    }
}
