/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.tinkerpop.gremlin.driver.HttpRequest;
import org.apache.tinkerpop.gremlin.driver.RequestInterceptor;
import org.apache.tinkerpop.gremlin.driver.UserAgent;
import org.apache.tinkerpop.gremlin.driver.auth.Auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.driver.handler.InactiveChannelHandler.REQUEST_SENT;

/**
 * Converts {@link RequestMessage} to an HTTP request, running interceptors and serializing the body to JSON.
 */
@ChannelHandler.Sharable
public final class HttpGremlinRequestEncoder extends MessageToMessageEncoder<RequestMessage> {

    private final MessageSerializer<?> serializer;
    private final boolean userAgentEnabled;
    private final boolean bulkResults;
    private final boolean compressionEnabled;
    private final List<RequestInterceptor> interceptors;
    private final URI uri;

    public HttpGremlinRequestEncoder(final MessageSerializer<?> serializer,
                                     final List<RequestInterceptor> interceptors,
                                     final boolean userAgentEnabled, boolean bulkResults,
                                     final boolean compressionEnabled, final URI uri) {
        this.serializer = serializer;
        this.interceptors = interceptors;
        this.userAgentEnabled = userAgentEnabled;
        this.bulkResults = bulkResults;
        this.compressionEnabled = compressionEnabled;
        this.uri = uri;
    }

    @Override
    protected void encode(final ChannelHandlerContext channelHandlerContext, final RequestMessage requestMessage, final List<Object> objects) throws Exception {
        if (requestMessage.getField("gremlin") instanceof GremlinLang) {
            throw new ResponseException(HttpResponseStatus.BAD_REQUEST, String.format(
                    "An error occurred during serialization of this request [%s] - it could not be sent to the server - Reason: GremlinLang is not intended to be send as query.",
                    requestMessage));
        }

        final InetSocketAddress remoteAddress = getRemoteAddress(channelHandlerContext.channel());
        try {
            final Map<String, String> headersMap = new HashMap<>();
            headersMap.put(HttpRequest.Headers.HOST, remoteAddress.getAddress().getHostAddress());
            // Accept header uses the response serializer's mime type (GraphBinary for responses)
            headersMap.put(HttpRequest.Headers.ACCEPT, serializer.mimeTypesSupported()[0]);
            if (compressionEnabled) {
                headersMap.put(HttpRequest.Headers.ACCEPT_ENCODING, HttpRequest.Headers.DEFLATE);
            }
            if (userAgentEnabled) {
                headersMap.put(HttpRequest.Headers.USER_AGENT, UserAgent.USER_AGENT);
            }
            if (bulkResults) {
                headersMap.put(Tokens.BULK_RESULTS, "true");
            }

            // Promote transactionId to HTTP header for dual transmission (header and body)
            final String transactionId = requestMessage.getField(Tokens.ARGS_TRANSACTION_ID);
            if (transactionId != null) {
                headersMap.put(Tokens.Headers.TRANSACTION_ID, transactionId);
            }

            final HttpRequest gremlinRequest = new HttpRequest(headersMap, requestMessage, uri);

            for (final RequestInterceptor interceptor : interceptors) {
                interceptor.intercept(gremlinRequest);
            }

            // Auto-serialize if interceptors did not already produce bytes
            gremlinRequest.serializeBody();

            final byte[] bodyBytes = (byte[]) gremlinRequest.getBody();

            final FullHttpRequest finalRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                    uri.getPath(), Unpooled.wrappedBuffer(bodyBytes));
            gremlinRequest.headers().forEach((k, v) -> finalRequest.headers().add(k, v));

            objects.add(finalRequest);
            channelHandlerContext.channel().attr(REQUEST_SENT).set(true);
        } catch (AuthenticationException ex) {
            throw new ResponseException(HttpResponseStatus.BAD_REQUEST, String.format(
                    "An error occurred during authentication [%s] - it could not be sent to the server - Reason: %s",
                    requestMessage, ex));
        }
    }

    private static InetSocketAddress getRemoteAddress(Channel channel) {
        final InetSocketAddress remoteAddress = (InetSocketAddress) channel.remoteAddress();
        if (remoteAddress == null) {
            final Throwable sslException = channel.attr(GremlinResponseHandler.INBOUND_SSL_EXCEPTION).get();
            if (sslException != null) {
                throw new RuntimeException("Request cannot be serialized because the channel is not connected due to an ssl error.", sslException);
            }
            throw new RuntimeException("Request cannot be serialized because the channel is not connected");
        }
        return remoteAddress;
    }
}
