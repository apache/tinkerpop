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
package org.apache.tinkerpop.gremlin.socket.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.SerTokens;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;

import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Handles HTTP Gremlin requests for the test socket server, responding with canned results
 * based on the gremlin string in the request.
 */
public class TestHttpGremlinHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4();
    private static final Graph graph = TinkerFactory.createModern();
    private static final Vertex singleVertex = graph.traversal().V().hasLabel("person").next();

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final FullHttpRequest request) throws Exception {
        final RequestMessage msg = serializer.deserializeBinaryRequest(request.content());
        final String gremlin = msg.getGremlin();

        switch (gremlin) {
            case SocketServerConstants.GREMLIN_SINGLE_VERTEX:
                writeVertexResponse(ctx);
                break;
            case SocketServerConstants.GREMLIN_CLOSE_CONNECTION:
                ctx.executor().schedule(() -> ctx.close(), 1, TimeUnit.SECONDS);
                break;
            case SocketServerConstants.GREMLIN_VERTEX_THEN_CLOSE:
                writeVertexResponse(ctx);
                ctx.executor().schedule(() -> ctx.close(), 2, TimeUnit.SECONDS);
                break;
            case SocketServerConstants.GREMLIN_FAIL_AFTER_DELAY:
                ctx.executor().schedule(() -> {
                    try {
                        writeErrorResponse(ctx, INTERNAL_SERVER_ERROR, "Server error");
                    } catch (SerializationException e) {
                        ctx.close();
                    }
                }, 1, TimeUnit.SECONDS);
                break;
            case SocketServerConstants.GREMLIN_PARTIAL_CONTENT_CLOSE:
                writePartialContentClose(ctx);
                break;
            case SocketServerConstants.GREMLIN_MALFORMED_RESPONSE:
                writeMalformedResponse(ctx);
                break;
            case SocketServerConstants.GREMLIN_NO_RESPONSE:
                // Do nothing — let the client timeout.
                break;
            case SocketServerConstants.GREMLIN_SLOW_RESPONSE:
                writeSlowResponse(ctx);
                break;
            case SocketServerConstants.GREMLIN_EMPTY_BODY:
                writeEmptyBody(ctx);
                break;
            default:
                writeErrorResponse(ctx, BAD_REQUEST, "Unknown test scenario");
                break;
        }
    }

    private void writeVertexResponse(final ChannelHandlerContext ctx) throws SerializationException {
        final ResponseMessage responseMessage = ResponseMessage.build()
                .result(Collections.singletonList(singleVertex))
                .code(OK)
                .create();
        final ByteBuf serialized = serializer.serializeResponseAsBinary(responseMessage, ctx.alloc());

        final DefaultHttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, SerTokens.MIME_GRAPHBINARY_V4);
        response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        ctx.write(response);
        ctx.write(new DefaultHttpContent(serialized));
        ctx.writeAndFlush(new DefaultLastHttpContent());
    }

    private void writeErrorResponse(final ChannelHandlerContext ctx, final HttpResponseStatus status,
                                    final String message) throws SerializationException {
        final ResponseMessage responseMessage = ResponseMessage.build()
                .result(Collections.emptyList())
                .code(status)
                .statusMessage(message)
                .create();
        final ByteBuf serialized = serializer.serializeResponseAsBinary(responseMessage, ctx.alloc());

        final DefaultHttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, SerTokens.MIME_GRAPHBINARY_V4);
        response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        ctx.write(response);
        ctx.write(new DefaultHttpContent(serialized));
        ctx.writeAndFlush(new DefaultLastHttpContent());
    }

    private void writePartialContentClose(final ChannelHandlerContext ctx) throws SerializationException {
        final ResponseMessage responseMessage = ResponseMessage.build()
                .result(Collections.singletonList(singleVertex))
                .create();

        final DefaultHttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, SerTokens.MIME_GRAPHBINARY_V4);
        response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        ctx.write(response);
        ctx.writeAndFlush(new DefaultHttpContent(serializer.writeHeader(responseMessage, ctx.alloc())));
        ctx.close();
    }

    private void writeMalformedResponse(final ChannelHandlerContext ctx) {
        final DefaultHttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, SerTokens.MIME_GRAPHBINARY_V4);
        response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        ctx.write(response);

        final byte[] garbage = new byte[64];
        new Random().nextBytes(garbage);
        ctx.write(new DefaultHttpContent(ctx.alloc().buffer(64).writeBytes(garbage)));
        ctx.writeAndFlush(new DefaultLastHttpContent());
    }

    private void writeSlowResponse(final ChannelHandlerContext ctx) {
        final DefaultHttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, SerTokens.MIME_GRAPHBINARY_V4);
        response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        ctx.writeAndFlush(response);

        final ResponseMessage headerMsg = ResponseMessage.build()
                .result(Collections.singletonList(singleVertex))
                .create();

        ctx.executor().schedule(() -> {
            try {
                ctx.writeAndFlush(new DefaultHttpContent(serializer.writeHeader(headerMsg, ctx.alloc())));
            } catch (SerializationException e) {
                ctx.close();
            }
        }, 500, TimeUnit.MILLISECONDS);

        ctx.executor().schedule(() -> {
            try {
                ctx.writeAndFlush(new DefaultHttpContent(
                        serializer.writeChunk(Collections.singletonList(singleVertex), ctx.alloc())));
            } catch (SerializationException e) {
                ctx.close();
            }
        }, 1000, TimeUnit.MILLISECONDS);

        ctx.executor().schedule(() -> {
            try {
                final ResponseMessage footerMsg = ResponseMessage.build()
                        .result(Collections.singletonList(singleVertex))
                        .code(OK)
                        .create();
                ctx.write(new DefaultHttpContent(serializer.writeFooter(footerMsg, ctx.alloc())));
                ctx.writeAndFlush(new DefaultLastHttpContent());
            } catch (SerializationException e) {
                ctx.close();
            }
        }, 1500, TimeUnit.MILLISECONDS);
    }

    private void writeEmptyBody(final ChannelHandlerContext ctx) {
        final DefaultHttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, SerTokens.MIME_GRAPHBINARY_V4);
        response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        ctx.write(response);
        ctx.writeAndFlush(new DefaultLastHttpContent());
    }
}
