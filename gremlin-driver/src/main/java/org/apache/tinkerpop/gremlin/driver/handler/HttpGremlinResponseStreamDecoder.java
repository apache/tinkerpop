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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpObject;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.AttributeMap;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.MessageChunkSerializer;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;

import java.util.List;
import java.util.Objects;

public class HttpGremlinResponseStreamDecoder extends MessageToMessageDecoder<DefaultHttpObject> {

    // todo: move out
    public static final AttributeKey<Boolean> IS_FIRST_CHUNK = AttributeKey.valueOf("isFirstChunk");

    private final MessageChunkSerializer<?> serializer;

    public HttpGremlinResponseStreamDecoder(MessageChunkSerializer<?> serializer) {

        this.serializer = serializer;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, DefaultHttpObject msg, List<Object> out) throws Exception {
        final Attribute<Boolean> isFirstChunk = ((AttributeMap) ctx).attr(IS_FIRST_CHUNK);

        if (msg instanceof DefaultHttpResponse) {
            isFirstChunk.set(true);
        }

        if (msg instanceof DefaultHttpContent) {
            try {
                if (ctx.attr(IS_FIRST_CHUNK).get()) {
                    System.out.println("first chunk");
                } else {
                    System.out.println("not first chunk");
                }

                final ResponseMessage chunk = serializer.readChunk(((DefaultHttpContent) msg).content(), isFirstChunk.get());

                if (chunk.getResult().getData() != null) {
                    System.out.println("payload size: " + ((List) chunk.getResult().getData()).size());
                }

                if (msg instanceof DefaultLastHttpContent) {
                    final HttpHeaders trailingHeaders = ((DefaultLastHttpContent) msg).trailingHeaders();

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
    }
}
