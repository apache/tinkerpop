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

import com.codahale.metrics.Meter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.CharsetUtil;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.util.GremlinError;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.util.ser.MessageTextSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.SerTokens;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import static com.codahale.metrics.MetricRegistry.name;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Provides methods shared by the HTTP handlers.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class HttpHandlerUtil {
    private static final Logger logger = LoggerFactory.getLogger(HttpHandlerUtil.class);
    static final Meter errorMeter = MetricManager.INSTANCE.getMeter(name(GremlinServer.class, "errors"));

    /**
     * A generic mapper to return JSON errors in specific cases.
     */
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Helper method to send errors back as JSON. Only to be used when the RequestMessage couldn't be parsed, because
     * a proper serialized ResponseMessage should be sent in that case.
     *
     * @param ctx     The netty channel context.
     * @param status  The HTTP error status code.
     * @param message The error message to contain the body.
     */
    public static void sendError(final ChannelHandlerContext ctx, final HttpResponseStatus status, final String message) {
        logger.warn(String.format("Invalid request - responding with %s and %s", status, message));
        errorMeter.mark();

        final ObjectNode node = mapper.createObjectNode();
        node.put("message", message);

        final FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, status, Unpooled.copiedBuffer(node.toString(), CharsetUtil.UTF_8));
        response.headers().set(CONTENT_TYPE, "application/json");
        HttpUtil.setContentLength(response, response.content().readableBytes());
        ctx.writeAndFlush(response);
    }

    static void writeError(final Context context, final ResponseMessage responseMessage, final MessageSerializer<?> serializer) {
        try {
            final ChannelHandlerContext ctx = context.getChannelHandlerContext();
            final ByteBuf ByteBuf = context.getRequestState() == HttpGremlinEndpointHandler.RequestState.STREAMING
                    ? ((MessageTextSerializerV4) serializer).writeErrorFooter(responseMessage, ctx.alloc())
                    : serializer.serializeResponseAsBinary(responseMessage, ctx.alloc());

            context.setRequestState(HttpGremlinEndpointHandler.RequestState.ERROR);
            ctx.writeAndFlush(new DefaultHttpContent(ByteBuf));

            sendTrailingHeaders(ctx, responseMessage.getStatus().getCode(), responseMessage.getStatus().getMessage());
        } catch (SerializationException se) {
            logger.warn("Unable to serialize ResponseMessage: {} ", responseMessage);
        }
    }

    static void writeError(final Context context, final GremlinError error, final MessageSerializer<?> serializer) {
        final ResponseMessage responseMessage = ResponseMessage.buildV4()
                .code(error.getCode())
                .statusMessage(error.getMessage())
                .exception(error.getException())
                .create();

        writeError(context, responseMessage, serializer);
    }

    static void sendTrailingHeaders(final ChannelHandlerContext ctx, final ResponseStatusCode statusCode, final String message) {
        final DefaultLastHttpContent defaultLastHttpContent = new DefaultLastHttpContent();
        defaultLastHttpContent.trailingHeaders().add(SerTokens.TOKEN_CODE, statusCode.getValue());
        try {
            defaultLastHttpContent.trailingHeaders().add(
                    SerTokens.TOKEN_MESSAGE, URLEncoder.encode(message, StandardCharsets.UTF_8.name()));
        } catch (UnsupportedEncodingException uee) {
            // This should never occur since we use UTF-8 so just log rather than handle.
            logger.info(StandardCharsets.UTF_8.name() + " encoding not supported", uee);
        }
        ctx.writeAndFlush(defaultLastHttpContent);
    }
}
