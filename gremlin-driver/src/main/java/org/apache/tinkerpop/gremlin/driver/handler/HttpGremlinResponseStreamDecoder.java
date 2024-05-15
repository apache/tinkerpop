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
import io.netty.handler.codec.http.DefaultHttpObject;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.AttributeMap;
import io.netty.util.CharsetUtil;
import org.apache.tinkerpop.gremlin.util.MessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessageV4;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Objects;

public class HttpGremlinResponseStreamDecoder extends MessageToMessageDecoder<DefaultHttpObject> {

    private static final AttributeKey<Boolean> IS_FIRST_CHUNK = AttributeKey.valueOf("isFirstChunk");
    private static final AttributeKey<HttpResponseStatus> RESPONSE_STATUS = AttributeKey.valueOf("responseStatus");

    private final MessageSerializerV4<?> serializer;
    private final ObjectMapper mapper = new ObjectMapper();

    public HttpGremlinResponseStreamDecoder(MessageSerializerV4<?> serializer) {
        this.serializer = serializer;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, DefaultHttpObject msg, List<Object> out) throws Exception {
        final Attribute<Boolean> isFirstChunk = ((AttributeMap) ctx).attr(IS_FIRST_CHUNK);
        final Attribute<HttpResponseStatus> responseStatus = ((AttributeMap) ctx).attr(RESPONSE_STATUS);

        System.out.println("HttpGremlinResponseStreamDecoder start");

        if (msg instanceof HttpResponse) {
            responseStatus.set(((HttpResponse) msg).status());

            if (isError(((HttpResponse) msg).status())) {
                System.out.println("Error: " + ((HttpResponse) msg).status());
                return;
            }

            isFirstChunk.set(true);
        }

        if (msg instanceof HttpContent) {
            try {
                // with error status we can get json in response
                // no more chunks expected
                if (isError(responseStatus.get())) {
                    final JsonNode node = mapper.readTree(((HttpContent) msg).content().toString(CharsetUtil.UTF_8));
                    final String message = node.get("message").asText();
                    final ResponseMessageV4 response = ResponseMessageV4.build()
                            .code(responseStatus.get()).statusMessage(message)
                            .create();

                    out.add(response);
                    return;
                }

                if (isFirstChunk.get()) {
                    System.out.println("first chunk");
                } else {
                    System.out.println("not first chunk");
                }

                System.out.println("payload size in bytes: " + ((HttpContent) msg).content().readableBytes());
                System.out.println(((HttpContent) msg).content());

                final ResponseMessageV4 chunk = serializer.readChunk(((HttpContent) msg).content(), isFirstChunk.get());

                if (chunk.getResult().getData() != null) {
                    System.out.println("payload size: " + ((List) chunk.getResult().getData()).size());
                }

                if (msg instanceof LastHttpContent) {
                    final HttpHeaders trailingHeaders = ((LastHttpContent) msg).trailingHeaders();

                    System.out.println("final chunk, trailing headers:");
                    System.out.println(trailingHeaders);

                    if (!Objects.equals(trailingHeaders.get("code"), "200")) {
                        throw new Exception(trailingHeaders.get("message"));
                    }
                }

                isFirstChunk.set(false);

                out.add(chunk);
            } catch (SerializationException e) {
                System.out.println("Ex: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }

        System.out.println("----------------------------");
    }

    private static boolean isError(final HttpResponseStatus status) {
        return status != HttpResponseStatus.OK && status != HttpResponseStatus.NO_CONTENT;
    }
}
