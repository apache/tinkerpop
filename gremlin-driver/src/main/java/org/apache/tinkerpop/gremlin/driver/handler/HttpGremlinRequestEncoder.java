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
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.tinkerpop.gremlin.driver.RequestInterceptor;
import org.apache.tinkerpop.gremlin.driver.UserAgent;
import org.apache.tinkerpop.gremlin.driver.auth.Auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.util.MessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.message.RequestMessageV4;
import org.apache.tinkerpop.gremlin.util.ser.SerTokensV4;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;

import java.net.InetSocketAddress;
import java.util.List;

import static org.apache.tinkerpop.gremlin.driver.handler.SslCheckHandler.REQUEST_SENT;

/**
 * Converts {@link RequestMessageV4} to a {@code HttpRequest}.
 */
@ChannelHandler.Sharable
public final class HttpGremlinRequestEncoder extends MessageToMessageEncoder<RequestMessageV4> {

    private final MessageSerializerV4<?> serializer;
    private final boolean userAgentEnabled;
    private final List<RequestInterceptor> interceptors;

    public HttpGremlinRequestEncoder(final MessageSerializerV4<?> serializer, final List<RequestInterceptor> interceptors, boolean userAgentEnabled) {
        this.serializer = serializer;
        this.interceptors = interceptors;
        this.userAgentEnabled = userAgentEnabled;
    }

    @Override
    protected void encode(final ChannelHandlerContext channelHandlerContext, final RequestMessageV4 requestMessage, final List<Object> objects) throws Exception {
        final String mimeType = serializer.mimeTypesSupported()[0];
        if (requestMessage.getField("gremlin") instanceof GremlinLang) {
            throw new ResponseException(HttpResponseStatus.BAD_REQUEST, String.format(
                    "An error occurred during serialization of this request [%s] - it could not be sent to the server - Reason: GremlinLang is not intended to be send as query.",
                    requestMessage));
        }

        try {
            final ByteBuf buffer = serializer.serializeRequestAsBinary(requestMessage, channelHandlerContext.alloc());
            FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/", buffer);
            request.headers().add(HttpHeaderNames.CONTENT_TYPE, mimeType);
            request.headers().add(HttpHeaderNames.CONTENT_LENGTH, buffer.readableBytes());
            request.headers().add(HttpHeaderNames.ACCEPT, mimeType);
            request.headers().add(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.DEFLATE);
            request.headers().add(HttpHeaderNames.HOST, ((InetSocketAddress) channelHandlerContext.channel().remoteAddress()).getAddress().getHostAddress());
            if (userAgentEnabled) {
                request.headers().add(HttpHeaderNames.USER_AGENT, UserAgent.USER_AGENT);
            }

            for (final RequestInterceptor interceptor : interceptors) {
                request = interceptor.apply(request);
            }
            objects.add(request);
            channelHandlerContext.channel().attr(REQUEST_SENT).set(true);
        } catch (SerializationException ex) {
            throw new ResponseException(HttpResponseStatus.BAD_REQUEST, String.format(
                    "An error occurred during serialization of this request [%s] - it could not be sent to the server - Reason: %s",
                    requestMessage, ex));
        } catch (AuthenticationException ex) {
            throw new ResponseException(HttpResponseStatus.BAD_REQUEST, String.format(
                    "An error occurred during authentication [%s] - it could not be sent to the server - Reason: %s",
                    requestMessage, ex));
        }
    }
}
