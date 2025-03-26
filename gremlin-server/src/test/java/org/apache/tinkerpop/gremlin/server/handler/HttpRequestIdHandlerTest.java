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
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import org.junit.Test;

import java.util.UUID;

import static org.apache.tinkerpop.gremlin.server.handler.HttpRequestIdHandler.REQUEST_ID_HEADER_NAME;
import static org.junit.Assert.assertTrue;

public class HttpRequestIdHandlerTest {
    @Test
    public void shouldProvideRequestIdToFollowingHandlers() {
        final HttpRequestIdHandler httpRequestIdHandler = new HttpRequestIdHandler();
        final EmbeddedChannel testChannel = new EmbeddedChannel(new HttpServerCodec(), httpRequestIdHandler);

        final ByteBuf buffer = testChannel.alloc().buffer();
        buffer.writeCharSequence("abc", CharsetUtil.UTF_8);
        final FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/gremlin", buffer);

        testChannel.writeInbound(httpRequest);
        testChannel.finish();

        assertTrue(testChannel.readInbound() instanceof HttpRequest);
        assertTrue(testChannel.attr(StateKey.REQUEST_ID) != null);

        buffer.release();
    }

    @Test
    public void shouldInjectRequestIdHeaderToOutgoingWrites() {
        final HttpRequestIdHandler httpRequestIdHandler = new HttpRequestIdHandler();
        final EmbeddedChannel testServerChannel = new EmbeddedChannel(httpRequestIdHandler);

        final ByteBuf buffer = testServerChannel.alloc().buffer();
        buffer.writeCharSequence("abc", CharsetUtil.UTF_8);
        final FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/gremlin", buffer);

        final FullHttpResponse httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

        testServerChannel.writeInbound(httpRequest);
        testServerChannel.writeOutbound(httpResponse);
        final HttpResponse updatedResponse = testServerChannel.readOutbound();

        testServerChannel.finish();
        assertTrue(updatedResponse.headers().contains(REQUEST_ID_HEADER_NAME));
        UUID.fromString(updatedResponse.headers().get(REQUEST_ID_HEADER_NAME));

        httpRequest.release();
        httpResponse.release();
    }
}
