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

public class HttpGremlinResponseStreamDecoder extends MessageToMessageDecoder<DefaultHttpObject> {

    public static final AttributeKey<Boolean> IS_BULKED = AttributeKey.valueOf("isBulked");
    private static final AttributeKey<Boolean> IS_FIRST_CHUNK = AttributeKey.valueOf("isFirstChunk");
    private static final AttributeKey<HttpResponseStatus> RESPONSE_STATUS = AttributeKey.valueOf("responseStatus");
    private static final AttributeKey<String> RESPONSE_ENCODING = AttributeKey.valueOf("responseSerializer");
    private static final AttributeKey<Long> BYTES_READ = AttributeKey.valueOf("bytesRead");

    private final MessageSerializer<?> serializer;
    private final long maxResponseContentLength;
    private final ObjectMapper mapper = new ObjectMapper();

    public HttpGremlinResponseStreamDecoder(final MessageSerializer<?> serializer, final long maxResponseContentLength) {
        this.serializer = serializer;
        this.maxResponseContentLength = maxResponseContentLength;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, DefaultHttpObject msg, List<Object> out) throws Exception {
        final Attribute<Boolean> isFirstChunk = ((AttributeMap) ctx).attr(IS_FIRST_CHUNK);
        final Attribute<Boolean> isBulked = ((AttributeMap) ctx).attr(IS_BULKED);
        final Attribute<HttpResponseStatus> responseStatus = ((AttributeMap) ctx).attr(RESPONSE_STATUS);
        final Attribute<String> responseEncoding = ((AttributeMap) ctx).attr(RESPONSE_ENCODING);

        if (msg instanceof HttpResponse) {
            ctx.channel().attr(BYTES_READ).set(0L);

            final HttpResponse resp = (HttpResponse) msg;
            responseStatus.set(resp.status());
            responseEncoding.set(resp.headers().get(HttpHeaderNames.CONTENT_TYPE));

            isFirstChunk.set(true);
        }

        if (msg instanceof HttpContent) {
            ByteBuf content = ((HttpContent) msg).content();
            Attribute<Long> bytesRead = ctx.channel().attr(BYTES_READ);
            bytesRead.set(bytesRead.get() + content.readableBytes());
            if (maxResponseContentLength > 0 && bytesRead.get() > maxResponseContentLength) {
                throw new TooLongFrameException("Response exceeded " + maxResponseContentLength + " bytes.");
            }

            try {
                // no more chunks expected
                if (isError(responseStatus.get()) && !SerTokens.MIME_GRAPHBINARY_V4.equals(responseEncoding.get())) {
                    final JsonNode node = mapper.readTree(content.toString(CharsetUtil.UTF_8));
                    final String message = node.get("message").asText();
                    final ResponseMessage response = ResponseMessage.build()
                            .code(responseStatus.get()).statusMessage(message)
                            .create();

                    out.add(response);
                } else {
                    final ResponseMessage chunk = serializer.readChunk(content, isFirstChunk.get());
                    if (isFirstChunk.get()){
                        isBulked.set(chunk.getResult().isBulked());
                    }
                    isFirstChunk.set(false);
                    out.add(chunk);
                }

                if (msg instanceof LastHttpContent) {
                    out.add(LAST_CONTENT_READ_RESPONSE);
                }
            } catch (SerializationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static boolean isError(final HttpResponseStatus status) {
        return status != HttpResponseStatus.OK;
    }
}
