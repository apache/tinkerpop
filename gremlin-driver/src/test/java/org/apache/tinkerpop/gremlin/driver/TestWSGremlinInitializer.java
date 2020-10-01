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
package org.apache.tinkerpop.gremlin.driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV2d0;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import java.util.List;
import java.util.UUID;


/**
 * Initializer which partially mimics the Gremlin Server. This initializer injects a handler in the
 * server pipeline that can be modified to send the desired response for a test case.
 */
public class TestWSGremlinInitializer extends TestWebSocketServerInitializer {
    private static final Logger logger = LoggerFactory.getLogger(TestWSGremlinInitializer.class);
    /**
     * If a request with this ID comes to the server, the server responds back with a single vertex picked from Modern
     * graph.
     */
    public static final UUID SINGLE_VERTEX_REQUEST_ID =
            UUID.fromString("6457272A-4018-4538-B9AE-08DD5DDC0AA1");

    /**
     * If a request with this ID comes to the server, the server responds back with a single vertex picked from Modern
     * graph. After some delay, server sends a Close WebSocket frame on the same connection.
     */
    public static final UUID SINGLE_VERTEX_DELAYED_CLOSE_CONNECTION_REQUEST_ID =
            UUID.fromString("3cb39c94-9454-4398-8430-03485d08bdae");

    public static final UUID FAILED_AFTER_DELAY_REQUEST_ID =
            UUID.fromString("edf79c8b-1d32-4102-a5d2-a5feeca40864");
    public static final UUID CLOSE_CONNECTION_REQUEST_ID =
            UUID.fromString("0150143b-00f9-48a7-a268-28142d902e18");
    public static final UUID CLOSE_CONNECTION_REQUEST_ID_2 =
            UUID.fromString("3c4cf18a-c7f2-4dad-b9bf-5c701eb33000");
    public static final UUID RESPONSE_CONTAINS_SERVER_ERROR_REQUEST_ID =
            UUID.fromString("0d333b1d-6e91-4807-b915-50b9ad721d20");

    /**
     * Gremlin serializer used for serializing/deserializing the request/response. This should be same as client.
     */
    private static final GraphSONMessageSerializerV2d0 SERIALIZER = new GraphSONMessageSerializerV2d0();

    @Override
    public void postInit(ChannelPipeline pipeline) {
        pipeline.addLast(new ClientTestConfigurableHandler());
    }

    /**
     * Handler introduced in the server pipeline to configure expected response for test cases.
     */
    private static class ClientTestConfigurableHandler extends MessageToMessageDecoder<BinaryWebSocketFrame> {
        @Override
        protected void decode(final ChannelHandlerContext ctx, final BinaryWebSocketFrame frame, final List<Object> objects)
                throws Exception {
            final ByteBuf messageBytes = frame.content();
            final byte len = messageBytes.readByte();
            if (len <= 0) {
                objects.add(RequestMessage.INVALID);
                return;
            }

            final ByteBuf contentTypeBytes = ctx.alloc().buffer(len);
            try {
                messageBytes.readBytes(contentTypeBytes);
            } finally {
                contentTypeBytes.release();
            }
            final RequestMessage msg = SERIALIZER.deserializeRequest(messageBytes.discardReadBytes());

            if (msg.getRequestId().equals(SINGLE_VERTEX_DELAYED_CLOSE_CONNECTION_REQUEST_ID)) {
                logger.info("sending vertex result frame");
                ctx.channel().writeAndFlush(new TextWebSocketFrame(returnSingleVertexResponse(
                        SINGLE_VERTEX_DELAYED_CLOSE_CONNECTION_REQUEST_ID)));
                logger.info("waiting for 2 sec");
                Thread.sleep(2000);
                logger.info("sending close frame");
                ctx.channel().writeAndFlush(new CloseWebSocketFrame());
            } else if (msg.getRequestId().equals(SINGLE_VERTEX_REQUEST_ID)) {
                logger.info("sending vertex result frame");
                ctx.channel().writeAndFlush(new TextWebSocketFrame(returnSingleVertexResponse(SINGLE_VERTEX_REQUEST_ID)));
            } else if (msg.getRequestId().equals(FAILED_AFTER_DELAY_REQUEST_ID)) {
                logger.info("waiting for 2 sec");
                Thread.sleep(1000);
                final ResponseMessage responseMessage = ResponseMessage.build(msg)
                        .code(ResponseStatusCode.SERVER_ERROR)
                        .statusAttributeException(new RuntimeException()).create();
                ctx.channel().writeAndFlush(new TextWebSocketFrame(SERIALIZER.serializeResponseAsString(responseMessage)));
            } else if (msg.getRequestId().equals(CLOSE_CONNECTION_REQUEST_ID)) {
                Thread.sleep(1000);
                ctx.channel().writeAndFlush(new CloseWebSocketFrame());
            } else if (msg.getRequestId().equals(RESPONSE_CONTAINS_SERVER_ERROR_REQUEST_ID)) {
                Thread.sleep(1000);
                ctx.channel().writeAndFlush(new CloseWebSocketFrame());
            }
        }

        private String returnSingleVertexResponse(final UUID requestID) throws SerializationException {
            final TinkerGraph graph = TinkerFactory.createClassic();
            final GraphTraversalSource g = graph.traversal();
            final Vertex t = g.V().limit(1).next();

            return SERIALIZER.serializeResponseAsString(ResponseMessage.build(requestID).result(t).create());
        }
    }
}
