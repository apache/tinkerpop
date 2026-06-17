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
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import java.util.Collections;

import io.netty.util.AttributeKey;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HttpGremlinResponseStreamDecoderTest {

    @Test
    public void shouldDecodeResponseRegardlessOfSize() throws SerializationException {
        final String largeResponse = RandomStringUtils.random(3000);
        final FullHttpResponse httpResponse = createResponse(largeResponse);
        final EmbeddedChannel testChannel = initializeChannel();

        testChannel.writeInbound(httpResponse);
        final ResponseMessage inbound = testChannel.readInbound();
        assertEquals(largeResponse, inbound.getResult().getData().get(0));
    }

    @Test
    public void shouldSetBulkedFlagCtxValueWithEachResponse() throws SerializationException {
        final EmbeddedChannel testChannel = initializeChannel();

        final FullHttpResponse httpResponse1 = createBulkedResponse(true);
        testChannel.writeInbound(httpResponse1);
        final boolean bulked1 = (boolean) testChannel.pipeline().channel().attr(AttributeKey.valueOf("isBulked")).get();
        assertTrue(bulked1);

        final FullHttpResponse httpResponse2 = createBulkedResponse(false);
        testChannel.writeInbound(httpResponse2);
        final boolean bulked2 = (boolean) testChannel.pipeline().channel().attr(AttributeKey.valueOf("isBulked")).get();
        assertFalse(bulked2);

        final FullHttpResponse httpResponse3 = createBulkedResponse(true);
        testChannel.writeInbound(httpResponse3);
        final boolean bulked3 = (boolean) testChannel.pipeline().channel().attr(AttributeKey.valueOf("isBulked")).get();
        assertTrue(bulked3);

        final FullHttpResponse httpResponse4 = createBulkedResponse(false);
        testChannel.writeInbound(httpResponse4);
        final boolean bulked4 = (boolean) testChannel.pipeline().channel().attr(AttributeKey.valueOf("isBulked")).get();
        assertFalse(bulked4);
    }

    private FullHttpResponse createResponse(String content) throws SerializationException {
        final ResponseMessage response = ResponseMessage.build().code(HttpResponseStatus.OK).result(Collections.singletonList(content)).create();
        final ByteBuf buffer = Serializers.GRAPHBINARY_V4.simpleInstance().serializeResponseAsBinary(response, ByteBufAllocator.DEFAULT);
        return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, buffer, new DefaultHttpHeaders(), new DefaultHttpHeaders());
    }

    private FullHttpResponse createBulkedResponse(final boolean bulkedFlag) throws SerializationException {
        final ResponseMessage response = ResponseMessage.build().code(HttpResponseStatus.OK).bulked(bulkedFlag).result(Collections.singletonList("test bulked")).create();
        final ByteBuf buffer = Serializers.GRAPHBINARY_V4.simpleInstance().serializeResponseAsBinary(response, ByteBufAllocator.DEFAULT);
        return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, buffer, new DefaultHttpHeaders(), new DefaultHttpHeaders());
    }

    private EmbeddedChannel initializeChannel() {
        final HttpGremlinResponseStreamDecoder decoder = new HttpGremlinResponseStreamDecoder(Serializers.GRAPHBINARY_V4.simpleInstance());
        final EmbeddedChannel testChannel = new EmbeddedChannel(new HttpServerCodec(), new HttpObjectAggregator(Integer.MAX_VALUE), decoder);
        return testChannel;
    }

}