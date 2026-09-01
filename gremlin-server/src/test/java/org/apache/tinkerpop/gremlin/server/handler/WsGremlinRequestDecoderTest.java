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
package org.apache.tinkerpop.gremlin.server.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Covers the frame size the WebSocket decoders record for {@link StateKey#REQUEST_SIZE}.
 */
public class WsGremlinRequestDecoderTest {

    private final GraphBinaryMessageSerializerV1 binarySerializer = new GraphBinaryMessageSerializerV1();
    private final GraphSONMessageSerializerV3 textSerializer = new GraphSONMessageSerializerV3();

    @Test
    public void shouldRecordTheFullFrameSizeOfABinaryFrame() throws Exception {
        final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                .addArg(Tokens.ARGS_GREMLIN, "g.V()").create();
        final ByteBuf content = binarySerializer.serializeRequestAsBinary(request, UnpooledByteBufAllocator.DEFAULT);
        final int frameSize = content.readableBytes();

        final EmbeddedChannel channel = new EmbeddedChannel(new WsGremlinBinaryRequestDecoder(serializers(binarySerializer)));
        try {
            channel.writeInbound(new BinaryWebSocketFrame(content));

            assertEquals(request.getRequestId(), ((RequestMessage) channel.readInbound()).getRequestId());
            assertEquals(frameSize, requestSize(channel));

            // not the remainder that is left once the mime type length byte has been read
            assertTrue(frameSize > 1);
            assertNotEquals(frameSize - 1, requestSize(channel));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void shouldRecordTheFullFrameSizeOfACloseFrame() throws Exception {
        final RequestMessage request = RequestMessage.build(Tokens.OPS_CLOSE).create();
        final ByteBuf content = binarySerializer.serializeRequestAsBinary(request, UnpooledByteBufAllocator.DEFAULT);
        final int frameSize = content.readableBytes();

        final EmbeddedChannel channel = new EmbeddedChannel(new WsGremlinCloseRequestDecoder(serializers(binarySerializer)));
        try {
            channel.writeInbound(new CloseWebSocketFrame(true, 0, content));

            assertEquals(request.getRequestId(), ((RequestMessage) channel.readInbound()).getRequestId());
            assertEquals(frameSize, requestSize(channel));

            // not the remainder that is left once the mime type length byte has been read
            assertTrue(frameSize > 1);
            assertNotEquals(frameSize - 1, requestSize(channel));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void shouldRecordTheFullFrameSizeOfATextFrame() throws Exception {
        // the non-ascii argument makes the frame longer than the character count of its text
        final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                .addArg(Tokens.ARGS_GREMLIN, "g.V().has('name','é')").create();
        final String text = textSerializer.serializeRequestAsString(request, UnpooledByteBufAllocator.DEFAULT);
        final int frameSize = text.getBytes(StandardCharsets.UTF_8).length;

        final EmbeddedChannel channel = new EmbeddedChannel(new WsGremlinTextRequestDecoder(serializers(textSerializer)));
        try {
            channel.writeInbound(new TextWebSocketFrame(text));

            assertEquals(request.getRequestId(), ((RequestMessage) channel.readInbound()).getRequestId());
            assertEquals(frameSize, requestSize(channel));
            assertNotEquals(text.length(), requestSize(channel));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static int requestSize(final EmbeddedChannel channel) {
        return channel.attr(StateKey.REQUEST_SIZE).get();
    }

    private static Map<String, MessageSerializer<?>> serializers(final MessageSerializer<?> serializer) {
        final Map<String, MessageSerializer<?>> serializers = new HashMap<>();

        for (final String mimeType : serializer.mimeTypesSupported()) {
            serializers.put(mimeType, serializer);
        }

        return serializers;
    }
}
