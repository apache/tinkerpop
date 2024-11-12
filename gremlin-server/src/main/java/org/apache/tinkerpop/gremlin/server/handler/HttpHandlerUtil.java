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
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.CharsetUtil;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.util.GremlinError;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.SerTokens;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.codahale.metrics.MetricRegistry.name;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderNames.TRANSFER_ENCODING;
import static io.netty.handler.codec.http.HttpHeaderValues.CHUNKED;
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
     * @param ctx           The netty channel context.
     * @param status        The HTTP error status code.
     * @param message       The error message to contain the body.
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

    /**
     * Writes and flushes a {@link ResponseMessage} that contains an error back to the client. Can be used to send
     * errors while streaming or when no response chunk has been sent. This serves as the end of a response.
     *
     * @param context           The netty context.
     * @param responseMessage   The response to send back.
     * @param serializer        The serializer to use to serialize the error response.
     */
    static void writeError(final Context context, final ResponseMessage responseMessage, final MessageSerializer<?> serializer) {
        try {
            final ChannelHandlerContext ctx = context.getChannelHandlerContext();
            final ByteBuf ByteBuf = context.getRequestState() == HttpGremlinEndpointHandler.RequestState.STREAMING
                    ? serializer.writeErrorFooter(responseMessage, ctx.alloc())
                    : serializer.serializeResponseAsBinary(responseMessage, ctx.alloc());

            context.setRequestState(HttpGremlinEndpointHandler.RequestState.ERROR);

            if (!ctx.channel().attr(StateKey.HTTP_RESPONSE_SENT).get()) {
                final HttpResponse responseHeader = new DefaultHttpResponse(HTTP_1_1, responseMessage.getStatus().getCode());
                responseHeader.headers().set(TRANSFER_ENCODING, CHUNKED); // Set this to make it "keep alive" eligible.
                responseHeader.headers().set(HttpHeaderNames.CONTENT_TYPE, ctx.channel().attr(StateKey.SERIALIZER).get().getValue0());
                ctx.writeAndFlush(responseHeader);
                ctx.channel().attr(StateKey.HTTP_RESPONSE_SENT).set(true);
            }

            ctx.writeAndFlush(new DefaultHttpContent(ByteBuf));

            sendTrailingHeaders(ctx, responseMessage.getStatus().getCode(), responseMessage.getStatus().getException());
        } catch (SerializationException se) {
            logger.warn("Unable to serialize ResponseMessage: {} ", responseMessage);
        }
    }

    /**
     * Writes a {@link GremlinError} into the status object of a {@link ResponseMessage} and then flushes it. Used to
     * send specific errors back to the client. This serves as the end of a response.
     *
     * @param context       The netty context.
     * @param error         The GremlinError used to populate the status.
     * @param serializer    The serializer to use to serialize the error response.
     */
    static void writeError(final Context context, final GremlinError error, final MessageSerializer<?> serializer) {
        final ResponseMessage responseMessage = ResponseMessage.build()
                .code(error.getCode())
                .statusMessage(error.getMessage())
                .exception(error.getException())
                .create();

        writeError(context, responseMessage, serializer);
    }

    /**
     * Adds trailing headers specified in the arguments to a {@link DefaultLastHttpContent} and then flushes it. This
     * serves as the end of a response.
     *
     * @param ctx           The netty context.
     * @param statusCode    The status code to include in the trailers.
     * @param exceptionType The type of exception to include in the trailers. Leave blank or null if no error occurred.
     */
    static void sendTrailingHeaders(final ChannelHandlerContext ctx, final HttpResponseStatus statusCode, final String exceptionType) {
        final DefaultLastHttpContent defaultLastHttpContent = new DefaultLastHttpContent();
        defaultLastHttpContent.trailingHeaders().add(SerTokens.TOKEN_CODE, statusCode.code());
        if (exceptionType != null && !exceptionType.isEmpty()) {
            defaultLastHttpContent.trailingHeaders().add(SerTokens.TOKEN_EXCEPTION, exceptionType);
        }

        ctx.writeAndFlush(defaultLastHttpContent);
    }
}
